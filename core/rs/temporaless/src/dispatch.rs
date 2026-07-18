//! Fire-and-forget dispatcher for gRPC-shaped handlers.
//!
//! Complements [`crate::workflow::run`] (synchronous + durable) with an
//! asynchronous path for side effects whose result the caller doesn't
//! need to wait on — webhook notifications, telemetry pushes,
//! best-effort vendor pings, fan-out where the caller wants its own
//! request to return quickly.
//!
//! Two backends, selected via [`Queue`]:
//!
//! - **Default in-process**: each [`Dispatcher::do_async`] spawns a tokio
//!   task on the current runtime, with managed graceful shutdown and an
//!   optional `max_inflight` cap. Not durable across crashes — if the
//!   process dies before the handler finishes, the work is lost.
//! - **External queue** (Kafka, RabbitMQ, NATS, SQS, Redis Streams, ...):
//!   implement [`Queue`] once; the dispatcher proto-marshals each
//!   request deterministically and hands `(method, payload)` to the
//!   queue. Consumers pull bytes off the bus and call
//!   [`Dispatcher::invoke`] to run the registered handler. Durability
//!   comes from the queue's native ack / nack semantics.
//!
//! Mirrors `adapters/go/dispatch` and `temporaless.dispatch` (Python):
//! same `register` / `do_async` / `invoke` / `shutdown` shape, same
//! 15-second default drain, same "always wait for every spawned task"
//! guarantee.
//!
//! Use [`crate::workflow::run`] when you need at-least-once delivery
//! across crashes; this module is for at-most-once + best-effort.
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
//! 4. **Producer drain + close**: give admitted external submissions one
//!    drain window, close the producer exactly once in an owned task, then
//!    give admitted submissions a final drain window. Close failures and
//!    timeouts flow through [`DispatcherOptions::on_error`] under stable
//!    `"<queue-close>"` and `"<queue-submit>"` labels.
//!
//! # Usage
//!
//! ```no_run
//! use std::sync::Arc;
//! use temporaless::dispatch::{Dispatcher, DispatcherOptions};
//! use temporaless::v1;
//!
//! # #[derive(Clone, PartialEq, prost::Message)]
//! # struct StringValue { #[prost(string, tag = "1")] value: String }
//! # async fn example() {
//! let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
//!     // Proto-driven knobs — load from config / env / CLI flag.
//!     proto: Some(v1::DispatchOptions {
//!         max_inflight: 100,
//!         ..Default::default()
//!     }),
//!     // queue: Some(my_kafka_queue),  // plug in Kafka / Rabbit / SQS here
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
//! // Fire-and-forget — returns once the task is spawned (in-process)
//! // or the bytes are handed to the queue (external).
//! dispatcher
//!     .do_async("/payments.Charges/Charge", StringValue { value: "hi".into() })
//!     .await
//!     .unwrap();
//!
//! // SIGTERM handler:
//! dispatcher.shutdown().await;
//! # }
//! ```
//!
//! Handler errors and external queue-close failures flow through
//! [`DispatcherOptions::on_error`] (default: log at WARN via `eprintln!`
//! since we don't pull in a logging facade).

use std::any::Any;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;
use thiserror::Error;
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::JoinSet;

use crate::v1;

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
    /// The configured external [`Queue`] returned an error from
    /// `submit`. Reason carries the queue's own error message; the
    /// underlying error type stays language-local so the framework
    /// doesn't have to enumerate every possible queue adapter's errors.
    #[error("queue submit failed: {0}")]
    Queue(String),
}

/// What [`DispatcherOptions::on_error`] sees. Handler failures use their
/// registered method name. External queue-close failures and timeouts use
/// the stable `"<queue-close>"` label; submissions that remain active after
/// the post-close drain window use `"<queue-submit>"`.
pub type OnErrorHook = Arc<dyn Fn(&str, Box<dyn StdError + Send + Sync>) + Send + Sync + 'static>;

/// Options for constructing a [`Dispatcher`]. The serializable knobs
/// (`drain_timeout`, `max_inflight`) come from the proto-declared
/// [`v1::DispatchOptions`] so a single config file / env var / CLI
/// flag drives them identically across Go, Python, and Rust. Runtime
/// hooks that can't be expressed as data (`on_error`, `queue`) stay
/// language-local here.
#[derive(Default)]
pub struct DispatcherOptions {
    /// Proto-declared serializable config. `None` applies defaults
    /// ([`DEFAULT_DRAIN_TIMEOUT`], unbounded).
    pub proto: Option<v1::DispatchOptions>,
    /// Pluggable queue backend. Default: in-process tokio-task pool.
    /// Implement [`Queue`] to plug in Kafka, RabbitMQ, NATS, SQS, etc.
    pub queue: Option<Arc<dyn Queue>>,
    /// Invoked when a handler returns `Err`, is aborted at shutdown, an
    /// external queue fails or times out while closing, or an admitted queue
    /// submission remains active after close. Default: write a one-line WARN
    /// to stderr.
    pub on_error: Option<OnErrorHook>,
}

// ---------------------------------------------------------------------------
// Queue — the producer-side adapter point for external message buses.
// ---------------------------------------------------------------------------

/// Producer interface external message buses plug into. A [`Queue`]
/// receives a method name + the proto-marshaled request payload; what
/// it does with them is up to the implementation: write to a Kafka
/// topic, publish to a RabbitMQ exchange, SQS SendMessage, NATS publish,
/// Redis Streams XADD, etc.
///
/// The consumer side is the implementation's concern — the framework
/// only standardizes the producer interface and the wire format (method
/// name + deterministic proto bytes). Consumers built on this should
/// pull messages off their queue and feed `(method, payload)` into
/// [`Dispatcher::invoke`] to look up the registered handler and run it
/// on the consumer task; the queue's native ack/nack drives delivery
/// semantics.
///
/// In-process: the default implementation spawns a tokio task and runs
/// the handler immediately, applying [`DispatcherOptions::proto`]'s
/// `max_inflight` / `drain_timeout`. See [`Dispatcher::new`] for how to
/// swap in an external queue.
#[async_trait]
pub trait Queue: Send + Sync + 'static {
    /// Submit pushes (method, payload) onto the backing queue. Returns
    /// once the message is handed off (queue's native producer-ack, or
    /// for the in-process queue, once the tokio task is spawned).
    async fn submit(
        &self,
        method: &str,
        payload: &[u8],
    ) -> Result<(), Box<dyn StdError + Send + Sync>>;

    /// Close releases any resources held by the queue. Called exactly once
    /// by [`Dispatcher::shutdown`] in an owned producer-finalization task,
    /// concurrently with tracked-handler drain, and bounded by the configured
    /// drain timeout. External queues should flush pending sends and close
    /// producer connections. An error or timeout is reported through
    /// [`DispatcherOptions::on_error`] as `"<queue-close>"`.
    ///
    /// A [`submit`](Queue::submit) call admitted just before shutdown may
    /// still be awaiting its producer acknowledgement when close begins.
    /// Implementations must make that overlap safe and release or fail the
    /// pending send. Temporaless reports `"<queue-submit>"` and completes
    /// shutdown if the send remains active for a second drain window.
    ///
    /// Once polling begins, shutdown never retries this method. Cancelling an
    /// individual shutdown waiter does not cancel close; the owned task keeps
    /// running and later shutdown calls share its completion. A close timeout
    /// does drop the close future, so implementations must make timeout
    /// cancellation safe.
    async fn close(&self) -> Result<(), Box<dyn StdError + Send + Sync>>;
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

/// Type-erased request decoder: takes payload bytes, returns the
/// concrete Req boxed as `Any` for the same `ErasedHandler` to consume.
type ErasedDecoder =
    Arc<dyn Fn(&[u8]) -> Result<AnyBox, prost::DecodeError> + Send + Sync + 'static>;

struct HandlerEntry {
    invoke: ErasedHandler,
    decode: ErasedDecoder,
}

struct SubmissionTracker {
    active: AtomicUsize,
    drained: Notify,
}

impl SubmissionTracker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            active: AtomicUsize::new(0),
            drained: Notify::new(),
        })
    }

    fn admit(self: &Arc<Self>) -> SubmissionGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        SubmissionGuard {
            tracker: self.clone(),
        }
    }

    async fn wait(&self) {
        loop {
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            let notified = self.drained.notified();
            tokio::pin!(notified);
            // Register before the second state check. Without `enable`, a
            // final submission could finish between that check and the first
            // poll, losing notify_waiters and parking shutdown forever.
            notified.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct SubmissionGuard {
    tracker: Arc<SubmissionTracker>,
}

impl Drop for SubmissionGuard {
    fn drop(&mut self) {
        if self.tracker.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.tracker.drained.notify_waiters();
        }
    }
}

struct AsyncCompletion {
    done: AtomicBool,
    notify: Notify,
}

impl AsyncCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            done: AtomicBool::new(false),
            notify: Notify::new(),
        })
    }

    async fn wait(&self) {
        loop {
            if self.done.load(Ordering::Acquire) {
                return;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

struct CompletionGuard(Arc<AsyncCompletion>);

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::Release);
        self.0.notify.notify_waiters();
    }
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
    /// Serializes the complete shutdown lifecycle. Concurrent callers wait
    /// for the first caller to finish draining handlers and closing the
    /// external queue instead of running competing finalizers.
    shutdown: Mutex<()>,
    submissions: Arc<SubmissionTracker>,
    /// `Some` when `max_inflight > 0`; the in-process path awaits a
    /// permit from this semaphore before spawning. Held as
    /// `Arc<Semaphore>` so the permit's lifetime can outlive
    /// `do_async` and bind to the spawned task.
    sem: Option<Arc<Semaphore>>,
    closed: AtomicBool,
    /// Optional external queue. When `Some`, `do_async` proto-marshals
    /// the request and delegates to `queue.submit(method, payload)`.
    /// When `None`, the built-in in-process tokio-task pool is used.
    queue: Option<Arc<dyn Queue>>,
    /// Set immediately before the owned external queue-close task is spawned.
    /// This is separate from `closed`: `closed` gates new submissions, while
    /// this flag makes queue finalization exactly-once across every shutdown
    /// call, including a later waiter after a cancelled shutdown future.
    queue_close_started: AtomicBool,
    queue_close_completion: Arc<AsyncCompletion>,
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
    pub fn new(opts: DispatcherOptions) -> Self {
        // Pull serializable bits from the proto, applying defaults for
        // anything unset.
        let mut drain_timeout = opts
            .proto
            .as_ref()
            .and_then(|p| p.drain_timeout.as_ref())
            .map(|d| Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32))
            .unwrap_or(DEFAULT_DRAIN_TIMEOUT);
        if drain_timeout.is_zero() {
            drain_timeout = DEFAULT_DRAIN_TIMEOUT;
        }
        let max_inflight = opts
            .proto
            .as_ref()
            .map(|p| p.max_inflight as usize)
            .unwrap_or(0);
        let on_error = opts.on_error.unwrap_or_else(default_on_error);
        let sem = if max_inflight > 0 {
            Some(Arc::new(Semaphore::new(max_inflight)))
        } else {
            None
        };
        Self {
            drain_timeout,
            on_error,
            handlers: RwLock::new(HashMap::new()),
            tasks: Mutex::new(JoinSet::new()),
            shutdown: Mutex::new(()),
            submissions: SubmissionTracker::new(),
            sem,
            closed: AtomicBool::new(false),
            queue: opts.queue,
            queue_close_started: AtomicBool::new(false),
            queue_close_completion: AsyncCompletion::new(),
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
        Req: Message + Default + Send + 'static,
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
                let typed: Box<Req> =
                    any_req
                        .downcast::<Req>()
                        .map_err(|_| -> Box<dyn StdError + Send + Sync> {
                            Box::new(DispatchError::TypeMismatch {
                                method: method_for_err,
                            })
                        })?;
                handler(*typed).await
            })
        });
        // The decoder constructs a fresh typed Req from payload bytes —
        // exposed via `invoke` for the external-queue consumer path.
        // Default-then-merge gives us a zero-allocation builder pattern
        // even for messages without a no-arg constructor on prost.
        let decode: ErasedDecoder = Arc::new(|payload: &[u8]| {
            let req = Req::decode(payload)?;
            Ok(Box::new(req) as AnyBox)
        });
        // Use try_write so synchronous-context callers don't deadlock; if
        // contended, fall back to blocking_write — register is called from
        // setup code, contention is unlikely.
        let mut handlers = match self.handlers.try_write() {
            Ok(g) => g,
            Err(_) => self.handlers.blocking_write(),
        };
        handlers.insert(
            method,
            HandlerEntry {
                invoke: erased,
                decode,
            },
        );
    }

    /// Look up the handler for `method`, type-check `req` against the
    /// handler's request type, and spawn the work as a tokio task.
    ///
    /// When [`DispatcherOptions::max_inflight`] is set, awaits a
    /// semaphore permit before spawning — natural backpressure for bursty
    /// callers. Two escape hatches while awaiting: the dispatcher
    /// beginning [`shutdown`](Self::shutdown) (returns
    /// [`DispatchError::ShuttingDown`]) or the awaiter itself being
    /// cancelled. With `max_inflight == 0` the call returns as soon as
    /// the task is spawned.
    ///
    /// Returns an error ONLY for the pre-dispatch failures
    /// ([`DispatchError`]) — handler errors flow through
    /// [`DispatcherOptions::on_error`].
    ///
    /// Must be called from within a tokio runtime (otherwise `spawn` will
    /// panic).
    pub async fn do_async<Req>(&self, method: &str, req: Req) -> Result<(), DispatchError>
    where
        Req: Message + Send + 'static,
    {
        if self.closed.load(Ordering::SeqCst) {
            return Err(DispatchError::ShuttingDown);
        }
        // Look up the handler — both paths (in-process and external
        // queue) need the entry to exist so a typo isn't silently
        // enqueued.
        let invoke = {
            let handlers = self.handlers.read().await;
            let entry = handlers
                .get(method)
                .ok_or_else(|| DispatchError::UnknownMethod {
                    method: method.to_string(),
                })?;
            entry.invoke.clone()
        };

        // External queue path: marshal req → bytes → queue.submit. The
        // type check is implicit: the producer's Req type comes from the
        // caller's generic instantiation, not validated against the
        // registered handler's type here (mismatch surfaces on the
        // consumer side when invoke() decodes and downcasts).
        if let Some(queue) = &self.queue {
            let payload = req.encode_to_vec();
            let submission = self.submissions.admit();
            // Two-phase admission closes the race between the initial check
            // and incrementing the active counter. If shutdown won that race,
            // retract the count without touching the queue. Otherwise this
            // guard remains live across the full producer acknowledgement.
            if self.closed.load(Ordering::SeqCst) {
                drop(submission);
                return Err(DispatchError::ShuttingDown);
            }
            let result = queue.submit(method, &payload).await;
            drop(submission);
            result.map_err(|e| DispatchError::Queue(e.to_string()))?;
            return Ok(());
        }

        // In-process path: acquire a concurrency permit when bounded.
        // Shutdown closes the semaphore, which persistently wakes both
        // already-parked and not-yet-polled acquisitions.
        let permit: Option<OwnedSemaphorePermit> = if let Some(sem) = &self.sem {
            match sem.clone().acquire_owned().await {
                Ok(permit) => Some(permit),
                Err(_) => return Err(DispatchError::ShuttingDown),
            }
        } else {
            None
        };

        let method_for_task = method.to_string();
        let any_req: AnyBox = Box::new(req);

        let mut tasks = self.tasks.lock().await;
        // Re-check `closed` under the tasks lock. Without this, a
        // submission that passed the initial `closed` check could spawn
        // a task AFTER shutdown's drain loop has emptied the JoinSet
        // and returned — orphaning the work. The lock is the barrier:
        // shutdown holds it for the entire drain, so any submitter that
        // takes it next observes the finalised `closed=true`.
        if self.closed.load(Ordering::SeqCst) {
            return Err(DispatchError::ShuttingDown);
        }
        tasks.spawn(async move {
            // Hold the permit for the lifetime of the task. Dropped on
            // task exit (success / error / abort) so the slot is always
            // released exactly once.
            let _slot = permit;
            let result = invoke(any_req).await;
            HandlerOutcome {
                method: method_for_task,
                result,
            }
        });
        Ok(())
    }

    /// Decode `payload` as the request type registered for `method` and
    /// run the registered handler on the caller's task. Intended for
    /// queue-backed consumers: pull a message off Kafka / Rabbit / NATS /
    /// SQS, hand its method-name + payload here, use the returned
    /// `Result` to drive ack / nack on the underlying queue.
    ///
    /// Unlike [`do_async`](Self::do_async), `invoke` runs the handler
    /// synchronously and uses the caller's task. The producer-side
    /// concurrency cap and drain semantics don't apply here — bound
    /// your consumer's concurrency at the queue's prefetch / consumer-
    /// pool layer instead.
    pub async fn invoke(
        &self,
        method: &str,
        payload: &[u8],
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (invoke, decode) = {
            let handlers = self.handlers.read().await;
            let entry =
                handlers
                    .get(method)
                    .ok_or_else(|| -> Box<dyn StdError + Send + Sync> {
                        Box::new(DispatchError::UnknownMethod {
                            method: method.to_string(),
                        })
                    })?;
            (entry.invoke.clone(), entry.decode.clone())
        };
        let any_req =
            decode(payload).map_err(|e| -> Box<dyn StdError + Send + Sync> { Box::new(e) })?;
        invoke(any_req).await
    }

    /// Stop accepting new submissions; drain in-flight tasks.
    ///
    /// 1. Marks the dispatcher closed so further [`do_async`] calls return
    ///    [`DispatchError::ShuttingDown`] and starts the owned external
    ///    producer finalizer.
    /// 2. Waits up to `drain_timeout` for tasks to finish on their own.
    /// 3. If any are still running, calls [`tokio::task::JoinHandle::abort`]
    ///    on each. Tokio injects a cancellation at the next `.await` —
    ///    equivalent to context cancellation in Go.
    /// 4. Waits for the remaining tasks to actually return. Never abandons
    ///    a task.
    /// 5. In parallel, gives admitted external submissions one drain window,
    ///    closes the queue exactly once, then gives active submissions a
    ///    second drain window. Close and submission-drain failures are routed
    ///    through [`DispatcherOptions::on_error`].
    ///
    /// Repeated and concurrent calls are safe. One caller performs
    /// finalization while the others wait for it to finish.
    ///
    /// Cancelling one shutdown waiter does not cancel queue close: later
    /// callers await the same owned finalizer. Close itself remains bounded by
    /// `drain_timeout`; a timeout drops the queue's close future, so adapters
    /// must make timeout cancellation safe.
    ///
    /// [`do_async`]: Self::do_async
    pub async fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        // Semaphore closure is persistent: it wakes current waiters and makes
        // every future acquire fail, so there is no one-shot notification
        // window for a bounded submitter to miss.
        if let Some(sem) = &self.sem {
            sem.close();
        }
        // Spawn producer finalization before this method's first await. Once a
        // shutdown future has been polled, cancelling that individual waiter
        // cannot cancel or skip external queue close.
        self.start_queue_finalizer();

        let _shutdown = self.shutdown.lock().await;
        let mut tasks = self.tasks.lock().await;

        // Phase 1: best-effort drain. Wait either for the JoinSet to
        // empty or for the drain timer to fire.
        let drain = async {
            while let Some(joined) = tasks.join_next().await {
                self.handle_joined(joined);
            }
        };
        match tokio::time::timeout(self.drain_timeout, drain).await {
            Ok(()) => {} // clean drain
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
        drop(tasks);

        if self.queue.is_some() {
            self.queue_close_completion.wait().await;
        }
    }

    fn start_queue_finalizer(&self) {
        let Some(queue) = &self.queue else {
            return;
        };
        if self
            .queue_close_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let queue = queue.clone();
            let on_error = self.on_error.clone();
            let drain_timeout = self.drain_timeout;
            let submissions = self.submissions.clone();
            let completion = CompletionGuard(self.queue_close_completion.clone());
            let finalizer = tokio::spawn(async move {
                // This guard is captured before spawning, so runtime
                // cancellation still publishes completion to every waiter.
                let _completion = completion;
                // Give producer acknowledgements one natural drain window.
                // Close may be what releases a stuck send, so expiry here
                // deliberately proceeds instead of completing shutdown.
                let _ = tokio::time::timeout(drain_timeout, submissions.wait()).await;

                let mut close = tokio::spawn(async move { queue.close().await });
                let close_error = match tokio::time::timeout(drain_timeout, &mut close).await {
                    Ok(Ok(Ok(()))) => None,
                    Ok(Ok(Err(err))) => Some(err),
                    Ok(Err(join_err)) => {
                        Some(Box::new(JoinErrorWrapper(join_err)) as Box<dyn StdError + Send + Sync>)
                    }
                    Err(_) => {
                        close.abort();
                        let _ = close.await;
                        Some(Box::new(QueueCloseTimeout(drain_timeout))
                            as Box<dyn StdError + Send + Sync>)
                    }
                };

                // Queue::close is the adapter's cancellation/flush boundary.
                // Give any send it released one final bounded window. A
                // broken adapter must not hang process shutdown forever.
                let submission_error =
                    match tokio::time::timeout(drain_timeout, submissions.wait()).await {
                        Ok(()) => None,
                        Err(_) => Some(Box::new(QueueSubmissionDrainTimeout(drain_timeout))
                            as Box<dyn StdError + Send + Sync>),
                    };
                if let Some(err) = close_error {
                    on_error("<queue-close>", err);
                }
                if let Some(err) = submission_error {
                    on_error("<queue-submit>", err);
                }
            });
            // Dropping a Tokio JoinHandle detaches the owned close task. The
            // completion object above is the shared, cancellation-safe join
            // point for this and every later shutdown caller.
            drop(finalizer);
        }
    }

    fn handle_joined(&self, joined: Result<HandlerOutcome, tokio::task::JoinError>) {
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

#[derive(Debug, Error)]
#[error("queue close exceeded drain timeout of {0:?}")]
struct QueueCloseTimeout(Duration);

#[derive(Debug, Error)]
#[error("queue submissions remained active after post-close drain timeout of {0:?}")]
struct QueueSubmissionDrainTimeout(Duration);

fn default_on_error() -> OnErrorHook {
    Arc::new(|method: &str, err: Box<dyn StdError + Send + Sync>| {
        eprintln!("dispatch: handler {method:?} returned error: {err}");
    })
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    struct FailingCloseQueue {
        close_calls: AtomicUsize,
    }

    #[async_trait]
    impl Queue for FailingCloseQueue {
        async fn submit(
            &self,
            _method: &str,
            _payload: &[u8],
        ) -> Result<(), Box<dyn StdError + Send + Sync>> {
            Ok(())
        }

        async fn close(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Err(Box::new(io::Error::other("flush failed")))
        }
    }

    #[tokio::test]
    async fn queue_close_error_does_not_skip_active_handler_drain() {
        let handler_finished = Arc::new(AtomicBool::new(false));
        let queue = Arc::new(FailingCloseQueue {
            close_calls: AtomicUsize::new(0),
        });
        let errors = Arc::new(StdMutex::new(Vec::<(String, String)>::new()));
        let errors_for_hook = errors.clone();
        let dispatcher = Dispatcher::new(DispatcherOptions {
            queue: Some(queue.clone()),
            on_error: Some(Arc::new(move |method, err| {
                errors_for_hook
                    .lock()
                    .unwrap()
                    .push((method.to_string(), err.to_string()));
            })),
            ..Default::default()
        });

        let finished_for_task = handler_finished.clone();
        dispatcher.tasks.lock().await.spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            finished_for_task.store(true, Ordering::SeqCst);
            HandlerOutcome {
                method: "/x/Active".to_string(),
                result: Ok(()),
            }
        });

        dispatcher.shutdown().await;

        assert!(handler_finished.load(Ordering::SeqCst));
        assert_eq!(queue.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            errors.lock().unwrap().as_slice(),
            &[("<queue-close>".to_string(), "flush failed".to_string())]
        );
    }

    #[tokio::test]
    async fn shutdown_semaphore_close_persists_for_late_waiter() {
        let dispatcher = Dispatcher::new(DispatcherOptions {
            proto: Some(v1::DispatchOptions {
                max_inflight: 1,
                ..Default::default()
            }),
            ..Default::default()
        });
        let sem = dispatcher.sem.as_ref().unwrap().clone();
        let held = sem.clone().acquire_owned().await.unwrap();
        // Construct the acquisition before shutdown but deliberately do not
        // poll it until after the shutdown signal. A one-shot Notify could be
        // lost in this gap; Semaphore::close is persistent.
        let late_waiter = sem.clone().acquire_owned();

        dispatcher.shutdown().await;

        assert!(sem.is_closed());
        assert!(late_waiter.await.is_err());
        drop(held);
    }
}
