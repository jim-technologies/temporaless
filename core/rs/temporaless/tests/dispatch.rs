//! Tests for `temporaless::dispatch`.
//!
//! Mirrors `adapters/go/dispatch/dispatch_test.go` and
//! `core/py/tests/test_dispatch.py`.

use std::error::Error as StdError;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message;
use prost_types::Duration as ProtoDuration;
use temporaless::dispatch::{DispatchError, Dispatcher, DispatcherOptions, Queue};
use temporaless::v1;
use tokio::sync::Notify;

fn drain_opts(d: Duration) -> v1::DispatchOptions {
    v1::DispatchOptions {
        drain_timeout: Some(ProtoDuration {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        }),
        ..v1::DispatchOptions::default()
    }
}

fn opts_with(d: Duration, max_inflight: u32) -> v1::DispatchOptions {
    v1::DispatchOptions {
        drain_timeout: Some(ProtoDuration {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        }),
        max_inflight,
        ..v1::DispatchOptions::default()
    }
}

// Hand-rolled minimal prost message — same trick the workflow tests use,
// avoids pulling a separate proto crate for fixture types.
#[derive(Clone, PartialEq, Message)]
struct StringValue {
    #[prost(string, tag = "1")]
    value: String,
}

fn sv(v: &str) -> StringValue {
    StringValue { value: v.into() }
}

#[derive(Clone, PartialEq, Message)]
struct Int32Value {
    #[prost(int32, tag = "1")]
    value: i32,
}

#[tokio::test]
async fn do_async_runs_handler_in_background() {
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions::default()));
    let started = Arc::new(Notify::new());
    let can_finish = Arc::new(Notify::new());
    let done = Arc::new(AtomicBool::new(false));

    let started_h = started.clone();
    let can_finish_h = can_finish.clone();
    let done_h = done.clone();
    dispatcher.register::<StringValue, _, _>("/x/Slow", move |_req| {
        let started = started_h.clone();
        let can_finish = can_finish_h.clone();
        let done = done_h.clone();
        async move {
            started.notify_one();
            can_finish.notified().await;
            done.store(true, Ordering::SeqCst);
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        }
    });

    let t0 = Instant::now();
    dispatcher.do_async("/x/Slow", sv("hi")).await.unwrap();
    assert!(
        t0.elapsed() < Duration::from_millis(50),
        "do_async should return immediately"
    );

    // Wait for the handler to have actually started.
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("handler should start within 1s");

    can_finish.notify_one();
    dispatcher.shutdown().await;
    assert!(done.load(Ordering::SeqCst));
}

#[tokio::test]
async fn do_async_rejects_unknown_method() {
    let dispatcher = Dispatcher::new(DispatcherOptions::default());
    dispatcher.register::<StringValue, _, _>("/x/Known", |_| async {
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let err = dispatcher
        .do_async("/x/Missing", sv("hi"))
        .await
        .unwrap_err();
    assert!(matches!(err, DispatchError::UnknownMethod { ref method } if method == "/x/Missing"));
    dispatcher.shutdown().await;
}

#[tokio::test]
async fn do_async_type_mismatch_surfaces_via_on_error() {
    // Rust can't statically type-check the call (each Req type is a
    // separate generic instantiation), so the mismatch surfaces inside
    // the spawned task via on_error.
    let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_h = seen.clone();
    let dispatcher = Dispatcher::new(DispatcherOptions {
        on_error: Some(Arc::new(move |method, err| {
            seen_h
                .lock()
                .unwrap()
                .push((method.to_string(), err.to_string()));
        })),
        ..Default::default()
    });
    dispatcher.register::<StringValue, _, _>("/x/Strict", |_| async {
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    dispatcher
        .do_async("/x/Strict", Int32Value { value: 7 })
        .await
        .unwrap();
    dispatcher.shutdown().await;

    let s = seen.lock().unwrap();
    assert_eq!(s.len(), 1, "type mismatch should surface once");
    assert_eq!(s[0].0, "/x/Strict");
    assert!(
        s[0].1.contains("wrong request type"),
        "error message should mention type mismatch, got: {}",
        s[0].1
    );
}

#[tokio::test]
async fn do_async_rejects_after_shutdown() {
    let dispatcher = Dispatcher::new(DispatcherOptions::default());
    dispatcher.register::<StringValue, _, _>("/x/Want", |_| async {
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    dispatcher.shutdown().await;
    let err = dispatcher.do_async("/x/Want", sv("hi")).await.unwrap_err();
    assert!(matches!(err, DispatchError::ShuttingDown));
}

#[tokio::test]
async fn shutdown_drains_running_tasks() {
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
        proto: Some(drain_opts(Duration::from_secs(2))),
        ..Default::default()
    }));
    let completed = Arc::new(AtomicBool::new(false));
    let completed_h = completed.clone();
    dispatcher.register::<StringValue, _, _>("/x/Work", move |_| {
        let completed = completed_h.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            completed.store(true, Ordering::SeqCst);
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        }
    });
    dispatcher.do_async("/x/Work", sv("hi")).await.unwrap();

    let t0 = Instant::now();
    dispatcher.shutdown().await;
    let elapsed = t0.elapsed();

    assert!(
        completed.load(Ordering::SeqCst),
        "shutdown returned before the handler completed"
    );
    assert!(
        elapsed >= Duration::from_millis(100),
        "shutdown returned in {:?} but handler needs ~150ms",
        elapsed
    );
}

#[tokio::test]
async fn shutdown_aborts_after_drain_timeout() {
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
        proto: Some(drain_opts(Duration::from_millis(50))),
        ..Default::default()
    }));
    let returned = Arc::new(AtomicBool::new(false));
    let returned_h = returned.clone();
    dispatcher.register::<StringValue, _, _>("/x/Long", move |_| {
        let returned = returned_h.clone();
        async move {
            // Would block for 5s; tokio task abort should hit the await
            // and surface as a cancellation before the store below runs.
            tokio::time::sleep(Duration::from_secs(5)).await;
            returned.store(true, Ordering::SeqCst);
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        }
    });
    dispatcher.do_async("/x/Long", sv("hi")).await.unwrap();

    let t0 = Instant::now();
    dispatcher.shutdown().await;
    let elapsed = t0.elapsed();

    // The sleep gets aborted, so the line after it never runs.
    assert!(
        !returned.load(Ordering::SeqCst),
        "handler should not have completed naturally — it was aborted"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "shutdown took {:?} — abort should kick in fast",
        elapsed
    );
}

#[tokio::test]
async fn handler_errors_flow_through_on_error() {
    let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_h = seen.clone();
    let dispatcher = Dispatcher::new(DispatcherOptions {
        on_error: Some(Arc::new(move |method, err| {
            seen_h
                .lock()
                .unwrap()
                .push((method.to_string(), err.to_string()));
        })),
        ..Default::default()
    });
    dispatcher.register::<StringValue, _, _>("/x/Boom", |_| async {
        Err::<(), Box<dyn StdError + Send + Sync>>("kaboom".into())
    });

    dispatcher.do_async("/x/Boom", sv("hi")).await.unwrap();
    dispatcher.shutdown().await;

    let s = seen.lock().unwrap();
    assert_eq!(s.len(), 1);
    assert_eq!(s[0].0, "/x/Boom");
    assert_eq!(s[0].1, "kaboom");
}

#[tokio::test]
async fn shutdown_is_idempotent() {
    let dispatcher = Dispatcher::new(DispatcherOptions::default());
    dispatcher.register::<StringValue, _, _>("/x/Any", |_| async {
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });
    dispatcher.shutdown().await;
    dispatcher.shutdown().await; // must not panic / deadlock
}

#[tokio::test]
async fn many_concurrent_submissions_all_complete() {
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
        proto: Some(drain_opts(Duration::from_secs(5))),
        ..Default::default()
    }));
    let count = Arc::new(AtomicUsize::new(0));
    let count_h = count.clone();
    dispatcher.register::<StringValue, _, _>("/x/Quick", move |_| {
        let count = count_h.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        }
    });

    const N: usize = 200;
    for i in 0..N {
        dispatcher
            .do_async("/x/Quick", sv(&i.to_string()))
            .await
            .unwrap();
    }
    dispatcher.shutdown().await;
    assert_eq!(count.load(Ordering::SeqCst), N);
}

#[tokio::test]
async fn max_inflight_caps_concurrent_handlers() {
    // Handlers naturally finish after a brief sleep — avoids the
    // notify_waiters() race (waiters that park AFTER notify_waiters()
    // never wake). The cap is verified by observing inflight at the
    // sample point and the running max across the whole burst.
    let cap = 3u32;
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
        proto: Some(opts_with(Duration::from_secs(5), cap)),
        ..Default::default()
    }));

    let inflight = Arc::new(AtomicUsize::new(0));
    let max_observed = Arc::new(AtomicUsize::new(0));
    let inflight_h = inflight.clone();
    let max_h = max_observed.clone();
    dispatcher.register::<StringValue, _, _>("/x/Bounded", move |_| {
        let inflight = inflight_h.clone();
        let max = max_h.clone();
        async move {
            let cur = inflight.fetch_add(1, Ordering::SeqCst) + 1;
            let mut prev = max.load(Ordering::SeqCst);
            while cur > prev {
                match max.compare_exchange(prev, cur, Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(_) => break,
                    Err(actual) => prev = actual,
                }
            }
            // Each handler holds its slot for ~120ms so the burst spans
            // long enough to observe + verify the cap.
            tokio::time::sleep(Duration::from_millis(120)).await;
            inflight.fetch_sub(1, Ordering::SeqCst);
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        }
    });

    let total = 10usize;
    let submitters: Vec<_> = (0..total)
        .map(|i| {
            let d = dispatcher.clone();
            tokio::spawn(async move { d.do_async("/x/Bounded", sv(&i.to_string())).await })
        })
        .collect();

    // 50ms in: the first `cap` are inside the body; the rest are parked
    // on the semaphore in do_async.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        inflight.load(Ordering::SeqCst),
        cap as usize,
        "inflight should be capped at {cap} mid-burst"
    );

    // Let the natural drain complete. With 10 submissions × 120ms
    // and cap=3 the total wall is ~120ms × ceil(10/3) = ~480ms; give
    // headroom for scheduling.
    for s in submitters {
        s.await.unwrap().unwrap();
    }
    dispatcher.shutdown().await;
    assert!(
        max_observed.load(Ordering::SeqCst) <= cap as usize,
        "max observed concurrency {} > cap {}",
        max_observed.load(Ordering::SeqCst),
        cap
    );
}

#[tokio::test]
async fn max_inflight_unblocks_on_shutdown() {
    // First handler holds the only slot. Second submitter parks on the
    // permit; shutdown wakes it with ShuttingDown rather than letting
    // it wait forever for a permit that's never coming.
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
        proto: Some(opts_with(Duration::from_millis(50), 1)),
        ..Default::default()
    }));
    // tokio::sync::Semaphore as a 1-slot gate the handler waits on.
    // Permit-based (unlike Notify::notify_waiters) so adding the permit
    // before the handler hits .acquire() still wakes it.
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let gate_h = gate.clone();
    dispatcher.register::<StringValue, _, _>("/x/Hog", move |_| {
        let gate = gate_h.clone();
        async move {
            let _ = gate.acquire().await;
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        }
    });
    dispatcher.do_async("/x/Hog", sv("first")).await.unwrap();

    let d2 = dispatcher.clone();
    let second = tokio::spawn(async move { d2.do_async("/x/Hog", sv("second")).await });
    // Give the second submitter time to park on the dispatcher's permit
    // semaphore in do_async.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let d3 = dispatcher.clone();
    let shutdown = tokio::spawn(async move {
        d3.shutdown().await;
    });
    // Yield until shutdown has actually run far enough to set `closed`
    // and fire the notify — otherwise releasing the holder first lets
    // the parked submitter grab the freed permit before it sees the
    // shutdown signal.
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Release the holder so shutdown can drain its handler.
    gate.add_permits(1);

    let err = second.await.unwrap().unwrap_err();
    assert!(
        matches!(err, DispatchError::ShuttingDown),
        "expected ShuttingDown, got {err:?}"
    );
    shutdown.await.unwrap();
}

#[tokio::test]
async fn invoke_runs_registered_handler_from_bytes() {
    // External-queue consumer path: bytes → method lookup → typed
    // handler. The producer (do_async or an external publisher) writes
    // proto.encode_to_vec(); the consumer (this test) calls invoke.
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions::default()));
    let echoed = Arc::new(Mutex::new(String::new()));
    let echoed_h = echoed.clone();
    dispatcher.register::<StringValue, _, _>("/x/Echo", move |req| {
        let echoed = echoed_h.clone();
        async move {
            *echoed.lock().unwrap() = req.value;
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        }
    });

    let payload = sv("hello").encode_to_vec();
    dispatcher.invoke("/x/Echo", &payload).await.unwrap();
    assert_eq!(echoed.lock().unwrap().as_str(), "hello");
    dispatcher.shutdown().await;
}

#[tokio::test]
async fn invoke_unknown_method() {
    let dispatcher = Dispatcher::new(DispatcherOptions::default());
    let err = dispatcher.invoke("/x/Missing", &[]).await.unwrap_err();
    assert!(err.to_string().contains("no handler registered"));
}

#[tokio::test]
async fn custom_queue_receives_submission() {
    use async_trait::async_trait;

    // A test Queue that captures (method, payload) without running the
    // handler. The contract Kafka/Rabbit/SQS adapters follow.
    type CapturedLog = Arc<Mutex<Vec<(String, Vec<u8>)>>>;
    struct CapturingQueue {
        captured: CapturedLog,
    }
    #[async_trait]
    impl Queue for CapturingQueue {
        async fn submit(
            &self,
            method: &str,
            payload: &[u8],
        ) -> Result<(), Box<dyn StdError + Send + Sync>> {
            self.captured
                .lock()
                .unwrap()
                .push((method.to_string(), payload.to_vec()));
            Ok(())
        }
        async fn close(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
            Ok(())
        }
    }
    let captured: CapturedLog = Arc::new(Mutex::new(Vec::new()));
    let queue = Arc::new(CapturingQueue {
        captured: captured.clone(),
    });

    let dispatcher = Dispatcher::new(DispatcherOptions {
        queue: Some(queue),
        ..Default::default()
    });
    dispatcher.register::<StringValue, _, _>("/x/Submit", |_| async {
        panic!("handler should NOT run when a custom queue is configured");
    });

    dispatcher
        .do_async("/x/Submit", sv("payload"))
        .await
        .unwrap();

    let cap = captured.lock().unwrap();
    assert_eq!(cap.len(), 1);
    assert_eq!(cap[0].0, "/x/Submit");
    // Round-trip the captured payload to prove producer + consumer
    // share the wire format.
    let decoded = StringValue::decode(cap[0].1.as_slice()).unwrap();
    assert_eq!(decoded.value, "payload");
}

struct ShutdownQueue {
    close_calls: AtomicUsize,
    close_started: Option<Arc<Notify>>,
    close_release: Option<Arc<Notify>>,
}

#[async_trait::async_trait]
impl Queue for ShutdownQueue {
    async fn submit(
        &self,
        _method: &str,
        _payload: &[u8],
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        Ok(())
    }

    async fn close(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(started) = &self.close_started {
            started.notify_one();
        }
        if let Some(release) = &self.close_release {
            release.notified().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn repeated_shutdown_closes_queue_once() {
    let queue = Arc::new(ShutdownQueue {
        close_calls: AtomicUsize::new(0),
        close_started: None,
        close_release: None,
    });
    let dispatcher = Dispatcher::new(DispatcherOptions {
        queue: Some(queue.clone()),
        ..Default::default()
    });

    dispatcher.shutdown().await;
    dispatcher.shutdown().await;

    assert_eq!(queue.close_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_shutdown_closes_queue_once() {
    let close_started = Arc::new(Notify::new());
    let close_release = Arc::new(Notify::new());
    let queue = Arc::new(ShutdownQueue {
        close_calls: AtomicUsize::new(0),
        close_started: Some(close_started.clone()),
        close_release: Some(close_release.clone()),
    });
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
        queue: Some(queue.clone()),
        ..Default::default()
    }));

    let first_dispatcher = dispatcher.clone();
    let first = tokio::spawn(async move {
        first_dispatcher.shutdown().await;
    });
    close_started.notified().await;

    let second_dispatcher = dispatcher.clone();
    let second = tokio::spawn(async move {
        second_dispatcher.shutdown().await;
    });
    tokio::task::yield_now().await;
    assert_eq!(
        queue.close_calls.load(Ordering::SeqCst),
        1,
        "the concurrent finalizer must wait instead of closing again"
    );

    close_release.notify_one();
    first.await.unwrap();
    second.await.unwrap();
    dispatcher.shutdown().await;

    assert_eq!(queue.close_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_shutdown_waiter_does_not_cancel_queue_close() {
    let close_started = Arc::new(Notify::new());
    let close_release = Arc::new(Notify::new());
    let queue = Arc::new(ShutdownQueue {
        close_calls: AtomicUsize::new(0),
        close_started: Some(close_started.clone()),
        close_release: Some(close_release.clone()),
    });
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
        proto: Some(drain_opts(Duration::from_secs(2))),
        queue: Some(queue.clone()),
        ..Default::default()
    }));

    let first_dispatcher = dispatcher.clone();
    let first = tokio::spawn(async move {
        first_dispatcher.shutdown().await;
    });
    close_started.notified().await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    let second_dispatcher = dispatcher.clone();
    let second = tokio::spawn(async move {
        second_dispatcher.shutdown().await;
    });
    tokio::task::yield_now().await;
    assert!(
        !second.is_finished(),
        "later shutdown must share the still-running queue finalizer"
    );

    close_release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), second)
        .await
        .expect("shared queue close should finish")
        .unwrap();
    assert_eq!(queue.close_calls.load(Ordering::SeqCst), 1);
}

struct BlockedSubmitQueue {
    submit_started: Arc<Notify>,
    submit_release: Arc<Notify>,
    close_started: Arc<Notify>,
    close_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl Queue for BlockedSubmitQueue {
    async fn submit(
        &self,
        _method: &str,
        _payload: &[u8],
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.submit_started.notify_one();
        self.submit_release.notified().await;
        Ok(())
    }

    async fn close(&self) -> Result<(), Box<dyn StdError + Send + Sync>> {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
        self.close_started.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_waits_after_close_for_admitted_external_submission() {
    let submit_started = Arc::new(Notify::new());
    let submit_release = Arc::new(Notify::new());
    let close_started = Arc::new(Notify::new());
    let queue = Arc::new(BlockedSubmitQueue {
        submit_started: submit_started.clone(),
        submit_release: submit_release.clone(),
        close_started: close_started.clone(),
        close_calls: AtomicUsize::new(0),
    });
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
        proto: Some(drain_opts(Duration::from_millis(25))),
        queue: Some(queue.clone()),
        ..Default::default()
    }));
    dispatcher.register::<StringValue, _, _>("/x/BlockedSubmit", |_| async {
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let submit_dispatcher = dispatcher.clone();
    let submit = tokio::spawn(async move {
        submit_dispatcher
            .do_async("/x/BlockedSubmit", sv("payload"))
            .await
    });
    submit_started.notified().await;

    let shutdown_dispatcher = dispatcher.clone();
    let shutdown = tokio::spawn(async move {
        shutdown_dispatcher.shutdown().await;
    });
    close_started.notified().await;
    tokio::task::yield_now().await;
    assert!(
        !shutdown.is_finished(),
        "shutdown returned while an admitted Queue::submit was still active"
    );

    submit_release.notify_one();
    submit.await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(1), shutdown)
        .await
        .expect("shutdown should finish once the admitted submit returns")
        .unwrap();
    assert_eq!(queue.close_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stuck_external_submission_is_bounded_and_reported() {
    let submit_started = Arc::new(Notify::new());
    let submit_release = Arc::new(Notify::new());
    let close_started = Arc::new(Notify::new());
    let queue = Arc::new(BlockedSubmitQueue {
        submit_started: submit_started.clone(),
        submit_release,
        close_started,
        close_calls: AtomicUsize::new(0),
    });
    let errors = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let errors_for_hook = errors.clone();
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
        proto: Some(drain_opts(Duration::from_millis(25))),
        queue: Some(queue.clone()),
        on_error: Some(Arc::new(move |method, err| {
            errors_for_hook
                .lock()
                .unwrap()
                .push((method.to_string(), err.to_string()));
        })),
    }));
    dispatcher.register::<StringValue, _, _>("/x/StuckSubmit", |_| async {
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let submit_dispatcher = dispatcher.clone();
    let submit = tokio::spawn(async move {
        submit_dispatcher
            .do_async("/x/StuckSubmit", sv("payload"))
            .await
    });
    submit_started.notified().await;

    tokio::time::timeout(Duration::from_secs(1), dispatcher.shutdown())
        .await
        .expect("a broken queue must not hang shutdown forever");
    assert!(
        !submit.is_finished(),
        "fixture submission should still be stuck after queue close"
    );
    assert_eq!(queue.close_calls.load(Ordering::SeqCst), 1);
    {
        let errors = errors.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "<queue-submit>");
        assert!(
            errors[0]
                .1
                .contains("queue submissions remained active after post-close drain timeout"),
            "unexpected submission timeout: {}",
            errors[0].1
        );
    }

    submit.abort();
    assert!(submit.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn queue_close_timeout_is_bounded_and_reported_once() {
    let close_release = Arc::new(Notify::new());
    let queue = Arc::new(ShutdownQueue {
        close_calls: AtomicUsize::new(0),
        close_started: None,
        close_release: Some(close_release),
    });
    let errors = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let errors_for_hook = errors.clone();
    let dispatcher = Dispatcher::new(DispatcherOptions {
        proto: Some(drain_opts(Duration::from_millis(25))),
        queue: Some(queue.clone()),
        on_error: Some(Arc::new(move |method, err| {
            errors_for_hook
                .lock()
                .unwrap()
                .push((method.to_string(), err.to_string()));
        })),
    });

    let started = Instant::now();
    dispatcher.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "queue close must be bounded by drain_timeout"
    );
    dispatcher.shutdown().await;

    assert_eq!(queue.close_calls.load(Ordering::SeqCst), 1);
    let errors = errors.lock().unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, "<queue-close>");
    assert!(
        errors[0].1.contains("queue close exceeded drain timeout"),
        "unexpected close timeout: {}",
        errors[0].1
    );
}
