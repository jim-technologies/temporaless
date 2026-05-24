//! Bounded fire-and-forget tokio-task pool for gRPC-shaped handlers.
//!
//! Complements [`crate::workflow::run`] (synchronous + durable) with an
//! asynchronous, in-process path for side effects whose result the caller
//! doesn't need to wait on — webhook notifications, telemetry pushes,
//! best-effort vendor pings, fan-out where the caller wants its own
//! request to return quickly.
//!
//! Mirrors `adapters/go/dispatch` and `temporaless.dispatch` (Python):
//! same `register` / `do_async` / `shutdown` shape, same 15-second default
//! drain, same "always wait for every spawned task" guarantee.
//!
//! # Semantics (intentional)
//!
//! In-process only. A handler invocation lives inside a tokio task spawned
//! on the current runtime. If the process dies before the handler
//! finishes, the work is lost. This is the deliberate tradeoff vs.
//! [`crate::workflow::run`] — when you need durability across crashes,
//! write a workflow instead; this module is for at-most-once + best-effort.
//!
//! # Graceful shutdown
//!
//! [`Dispatcher::shutdown`] stops accepting new submissions, then:
//!
//! 1. **Drain window** (default 15s): wait for in-flight tasks to finish
//!    on their own.
//! 2. **Cancel**: if any are still running, `abort()` them. Tokio aborts
//!    cause the next `.await` point to yield a `JoinError` — equivalent
//!    to context cancellation in Go.
//! 3. **Final wait**: continue draining until every task has returned.
//!    Never abandon a task — orphaning a handler mid-vendor-call is worse
//!    than waiting a few extra seconds for it to notice cancellation.
//!
//! # Usage
//!
//! ```no_run
//! use std::time::Duration;
//! use std::sync::Arc;
//! use prost_types::StringValue; // any prost::Message
//! use temporaless::dispatch::{Dispatcher, DispatcherOptions};
//!
//! # async fn example() {
//! let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
//!     drain_timeout: Duration::from_secs(15),
//!     ..Default::default()
//! }));
//!
//! dispatcher.register::<StringValue, _, _>(
//!     "/payments.Charges/Charge",
//!     |req: StringValue| async move {
//!         // perform side effect
//!         let _ = req;
//!         Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
//!     },
//! );
//!
//! // Fire-and-forget — returns immediately:
//! dispatcher.do_async("/payments.Charges/Charge", StringValue { value: "hi".into() }).unwrap();
//!
//! // SIGTERM handler:
//! dispatcher.shutdown().await;
//! # }
//! ```
//!
//! Handler errors flow through [`DispatcherOptions::on_error`] (default:
//! log at WARN via `eprintln!` since we don't pull in a logging facade).

use std::any::Any;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tokio::task::{JoinHandle, JoinSet};

/// Default drain window — matches the Go / Python defaults and common
/// SIGTERM grace periods (Kubernetes preStop / terminationGracePeriodSeconds).
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(15);

/// Errors surfaced synchronously by [`Dispatcher::do_async`]. Handler-side
/// failures (returning an `Err`, panicking, being aborted at shutdown) go
/// through [`DispatcherOptions::on_error`] instead — the caller has long
/// since moved on.
#[derive(Debug, Error)]
pub enum DispatchError {
    /// The dispatcher has begun or completed [`Dispatcher::shutdown`]. New
    /// submissions are rejected; the process is going away.
    #[error("dispatcher is shutting down")]
    ShuttingDown,
    /// No handler was registered for the requested method.
    #[error("no handler registered for method {method:?}")]
    UnknownMethod {
        /// The method name that was requested.
        method: String,
    },
    /// The supplied request value isn't the type the registered handler
    /// expects. Recovers from method-name typos that happen to collide
    /// with another registered method.
    #[error("handler {method:?} got the wrong request type")]
    TypeMismatch {
        /// The method whose handler refused the request type.
        method: String,
    },
}

/// What [`Dispatcher::on_error`] sees. `None` is propagated for the
/// success path so a single channel can report both completion and error
/// — we choose to keep the surface simple: only errors flow through.
pub type OnErrorHook =
    Arc<dyn Fn(&str, Box<dyn StdError + Send + Sync>) + Send + Sync + 'static>;

/// Options for constructing a [`Dispatcher`]. Default values mirror the
/// Go and Python defaults.
pub struct DispatcherOptions {
    /// How long [`Dispatcher::shutdown`] waits for in-flight tasks before
    /// aborting them. Zero or negative falls back to [`DEFAULT_DRAIN_TIMEOUT`].
    pub drain_timeout: Duration,
    /// Invoked when a handler returns `Err` or is aborted at shutdown.
    /// Default: write a one-line WARN to stderr.
    pub on_error: Option<OnErrorHook>,
}

impl Default for DispatcherOptions {
    fn default() -> Self {
        Self {
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            on_error: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

type AnyBox = Box<dyn Any + Send>;
type ErasedFuture = Pin<Box<dyn Future<Output = HandlerResult> + Send>>;
type HandlerResult = Result<(), Box<dyn StdError + Send + Sync>>;

/// Type-erased handler: takes the boxed request `Any` (which downcasts to
/// the original `Req`) and returns an erased future.
type ErasedHandler = Arc<dyn Fn(AnyBox) -> ErasedFuture + Send + Sync + 'static>;

struct HandlerEntry {
    invoke: ErasedHandler,
}

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------

/// Bounded fire-and-forget tokio-task pool keyed by gRPC-style method names.
///
/// Construct once, wrap in an `Arc`, share across the process. All methods
/// take `&self` so concurrent submissions from many tasks are fine.
pub struct Dispatcher {
    drain_timeout: Duration,
    on_error: OnErrorHook,
    handlers: RwLock<HashMap<String, HandlerEntry>>,
    tasks: Mutex<JoinSet<HandlerOutcome>>,
    closed: AtomicBool,
}

/// What each spawned task reports back through the [`JoinSet`]. Carries
/// the method name so the on-error hook can identify the source.
struct HandlerOutcome {
    method: String,
    result: HandlerResult,
}

impl Dispatcher {
    /// Construct a [`Dispatcher`] from an options struct. Pass
    /// `DispatcherOptions::default()` for the standard 15-second drain
    /// window and stderr error logging.
    pub fn new(mut opts: DispatcherOptions) -> Self {
        if opts.drain_timeout.is_zero() {
            opts.drain_timeout = DEFAULT_DRAIN_TIMEOUT;
        }
        let on_error = opts.on_error.unwrap_or_else(default_on_error);
        Self {
            drain_timeout: opts.drain_timeout,
            on_error,
            handlers: RwLock::new(HashMap::new()),
            tasks: Mutex::new(JoinSet::new()),
            closed: AtomicBool::new(false),
        }
    }

    /// Register an async handler under `method`. `method` should be the
    /// gRPC fully-qualified method (`"/package.Service/Method"`) so the
    /// same identity used at the wire layer routes here too.
    ///
    /// Re-registering the same method overwrites silently — last writer
    /// wins.
    ///
    /// `Req` is the typed request the handler accepts. [`do_async`] type-
    /// checks against this at the call site and rejects mismatches with
    /// [`DispatchError::TypeMismatch`].
    ///
    /// [`do_async`]: Self::do_async
    pub fn register<Req, F, Fut>(&self, method: impl Into<String>, handler: F)
    where
        Req: Message + Send + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + Send + 'static,
    {
        let method = method.into();
        if method.is_empty() {
            panic!("Dispatcher::register: method is required");
        }
        let handler = Arc::new(handler);
        let method_for_err = method.clone();
        let erased: ErasedHandler = Arc::new(move |any_req: AnyBox| {
            let handler = handler.clone();
            let method_for_err = method_for_err.clone();
            Box::pin(async move {
                let typed: Box<Req> = any_req.downcast::<Req>().map_err(|_| -> Box<dyn StdError + Send + Sync> {
                    Box::new(DispatchError::TypeMismatch { method: method_for_err })
                })?;
                handler(*typed).await
            })
        });
        // Use try_write so synchronous-context callers don't deadlock; if
        // contended, fall back to blocking_write — register is called from
        // setup code, contention is unlikely.
        let mut handlers = match self.handlers.try_write() {
            Ok(g) => g,
            Err(_) => self.handlers.blocking_write(),
        };
        handlers.insert(method, HandlerEntry { invoke: erased });
    }

    /// Look up the handler for `method`, type-check `req` against the
    /// handler's request type, and spawn the work as a tokio task.
    /// Returns immediately after spawning. Returns an error ONLY for the
    /// pre-dispatch failures ([`DispatchError`]) — handler errors flow
    /// through [`DispatcherOptions::on_error`].
    ///
    /// Must be called from within a tokio runtime (otherwise `spawn` will
    /// panic).
    pub fn do_async<Req>(&self, method: &str, req: Req) -> Result<(), DispatchError>
    where
        Req: Message + Send + 'static,
    {
        if self.closed.load(Ordering::SeqCst) {
            return Err(DispatchError::ShuttingDown);
        }
        let invoke = {
            // Block briefly on the read lock — register is called rarely
            // and from setup code, so the lock is uncontended at steady
            // state. blocking_read is safe inside an async fn because the
            // critical section is microseconds.
            let handlers = match self.handlers.try_read() {
                Ok(g) => g,
                Err(_) => self.handlers.blocking_read(),
            };
            let entry = handlers
                .get(method)
                .ok_or_else(|| DispatchError::UnknownMethod {
                    method: method.to_string(),
                })?;
            entry.invoke.clone()
        };

        let method_for_task = method.to_string();
        let any_req: AnyBox = Box::new(req);

        // Spawn into the JoinSet so shutdown can drain everything in one
        // place. JoinSet takes &mut self for spawn so we briefly lock the
        // mutex; the spawn call itself returns immediately.
        let mut tasks = match self.tasks.try_lock() {
            Ok(g) => g,
            Err(_) => self.tasks.blocking_lock(),
        };
        tasks.spawn(async move {
            let result = invoke(any_req).await;
            HandlerOutcome {
                method: method_for_task,
                result,
            }
        });
        Ok(())
    }

    /// Stop accepting new submissions; drain in-flight tasks.
    ///
    /// 1. Marks the dispatcher closed so further [`do_async`] calls
    ///    return [`DispatchError::ShuttingDown`].
    /// 2. Waits up to `drain_timeout` for tasks to finish on their own.
    /// 3. If any are still running, calls [`tokio::task::JoinHandle::abort`]
    ///    on each. Tokio injects a cancellation at the next `.await` —
    ///    equivalent to context cancellation in Go.
    /// 4. Waits for the remaining tasks to actually return. Never abandons
    ///    a task.
    ///
    /// Safe to call twice; the second call observes the already-closed
    /// state and returns once any remaining tasks finish.
    ///
    /// [`do_async`]: Self::do_async
    pub async fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let mut tasks = self.tasks.lock().await;

        // Phase 1: best-effort drain. Wait either for the JoinSet to
        // empty or for the drain timer to fire.
        let drain = async {
            while let Some(joined) = tasks.join_next().await {
                self.handle_joined(joined);
            }
        };
        match tokio::time::timeout(self.drain_timeout, drain).await {
            Ok(()) => return, // clean drain
            Err(_) => {
                // Drain window expired — abort the rest.
                tasks.abort_all();
            }
        }

        // Phase 2: wait for every aborted task to actually return. The
        // JoinError from abort surfaces here; we route it through
        // on_error as a "cancelled" notice for observability.
        while let Some(joined) = tasks.join_next().await {
            self.handle_joined(joined);
        }
    }

    fn handle_joined(
        &self,
        joined: Result<HandlerOutcome, tokio::task::JoinError>,
    ) {
        match joined {
            Ok(outcome) => {
                if let Err(err) = outcome.result {
                    (self.on_error)(&outcome.method, err);
                }
            }
            Err(join_err) => {
                // join_err is either a panic or a cancellation. We can't
                // recover the method name from JoinError; report what we
                // have.
                let label = if join_err.is_cancelled() {
                    "<cancelled-at-shutdown>"
                } else {
                    "<panicked>"
                };
                let boxed: Box<dyn StdError + Send + Sync> = Box::new(JoinErrorWrapper(join_err));
                (self.on_error)(label, boxed);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Wrapper so `tokio::task::JoinError` can be coerced into a `Box<dyn Error>`
/// (JoinError implements Error already, but the trait object dance needs a
/// `Send + Sync` bound that's satisfied by wrapping in a fresh type).
#[derive(Debug, Error)]
#[error("join error: {0}")]
struct JoinErrorWrapper(tokio::task::JoinError);

fn default_on_error() -> OnErrorHook {
    Arc::new(|method: &str, err: Box<dyn StdError + Send + Sync>| {
        eprintln!("dispatch: handler {method:?} returned error: {err}");
    })
}

// The `JoinHandle` type alias is kept exported in case downstream wants to
// rebuild the same shape with finer-grained control.
#[allow(dead_code)]
type DispatchedHandle = JoinHandle<HandlerOutcome>;
