//! Tests for `temporaless::dispatch`.
//!
//! Mirrors `adapters/go/dispatch/dispatch_test.go` and
//! `core/py/tests/test_dispatch.py`.

use std::error::Error as StdError;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message;
use temporaless::dispatch::{DispatchError, Dispatcher, DispatcherOptions};
use tokio::sync::Notify;

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
    dispatcher.do_async("/x/Slow", sv("hi")).unwrap();
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

    let err = dispatcher.do_async("/x/Missing", sv("hi")).unwrap_err();
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
    let err = dispatcher.do_async("/x/Want", sv("hi")).unwrap_err();
    assert!(matches!(err, DispatchError::ShuttingDown));
}

#[tokio::test]
async fn shutdown_drains_running_tasks() {
    let dispatcher = Arc::new(Dispatcher::new(DispatcherOptions {
        drain_timeout: Duration::from_secs(2),
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
    dispatcher.do_async("/x/Work", sv("hi")).unwrap();

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
        drain_timeout: Duration::from_millis(50),
        ..Default::default()
    }));
    let returned = Arc::new(AtomicBool::new(false));
    let returned_h = returned.clone();
    dispatcher.register::<StringValue, _, _>("/x/Long", move |_| {
        let returned = returned_h.clone();
        async move {
            // Would block for 5s; tokio task abort should hit the await
            // and surface as a cancellation.
            let res = tokio::time::sleep(Duration::from_secs(5)).await;
            returned.store(true, Ordering::SeqCst);
            let _ = res;
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        }
    });
    dispatcher.do_async("/x/Long", sv("hi")).unwrap();

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

    dispatcher.do_async("/x/Boom", sv("hi")).unwrap();
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
        drain_timeout: Duration::from_secs(5),
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
            .unwrap();
    }
    dispatcher.shutdown().await;
    assert_eq!(count.load(Ordering::SeqCst), N);
}
