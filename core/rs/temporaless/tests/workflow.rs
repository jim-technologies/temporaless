//! Rust workflow runtime tests — mirror the Go (`core/go/workflow/
//! workflow_test.go`) and Python (`core/py/tests/test_workflow.py`) suites
//! so we can confirm parity on the branches that ship in Rust today.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use opendal::{Operator, services::Fs};
use prost::{Message, Name};
use tempfile::TempDir;
use temporaless::storage::OpenDALStore;
use temporaless::workflow::{
    ActivityError, ActivityOptions, RetryPolicy, RunError, Workflow, WorkflowOptions, annotate,
    current, execute_activity, run,
};

fn new_store() -> (TempDir, Arc<OpenDALStore>) {
    let tmp = TempDir::new().unwrap();
    let builder = Fs::default().root(tmp.path().to_str().unwrap());
    let op = Operator::new(builder).unwrap();
    (tmp, Arc::new(OpenDALStore::new(op)))
}

// A trivial workflow body: identity over a wrapper StringValue-equivalent.
// Hand-rolled to mirror `google.protobuf.StringValue` byte-for-byte (same
// field number, same type) so the records this test produces are wire-
// compatible with what Go and Python write — and `Name::full_name()`
// returns the same `google.protobuf.StringValue` string those SDKs do.
#[derive(Clone, PartialEq, Message)]
struct StringValue {
    #[prost(string, tag = "1")]
    value: String,
}

impl Name for StringValue {
    const NAME: &'static str = "StringValue";
    const PACKAGE: &'static str = "google.protobuf";
}

fn s(value: &str) -> StringValue {
    StringValue {
        value: value.into(),
    }
}

#[tokio::test]
async fn run_completes_a_fresh_workflow() {
    let (_tmp, store) = new_store();
    let options = WorkflowOptions::new("wf", "r-1").with_code_version("test");
    let executions = Arc::new(AtomicUsize::new(0));
    let executions2 = executions.clone();
    let result: StringValue = run(store, options, s("AAPL"), |_w: Workflow, input| {
        let executions = executions2.clone();
        async move {
            executions.fetch_add(1, Ordering::SeqCst);
            Ok::<StringValue, RunError>(s(&format!("ok:{}", input.value)))
        }
    })
    .await
    .unwrap();
    assert_eq!(result.value, "ok:AAPL");
    assert_eq!(executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn run_replays_completed_workflow_without_re_executing_body() {
    let (_tmp, store) = new_store();
    let options = WorkflowOptions::new("wf", "r-1").with_code_version("test");
    let executions = Arc::new(AtomicUsize::new(0));

    let body = |_w: Workflow, input: StringValue| {
        let executions = executions.clone();
        async move {
            executions.fetch_add(1, Ordering::SeqCst);
            Ok::<StringValue, RunError>(s(&format!("ok:{}", input.value)))
        }
    };
    let r1: StringValue = run(store.clone(), options.clone(), s("x"), body)
        .await
        .unwrap();
    assert_eq!(r1.value, "ok:x");

    let body2 = |_w: Workflow, input: StringValue| {
        let executions = executions.clone();
        async move {
            executions.fetch_add(1, Ordering::SeqCst);
            Ok::<StringValue, RunError>(s(&format!("ok:{}", input.value)))
        }
    };
    let r2: StringValue = run(store, options, s("x"), body2).await.unwrap();
    assert_eq!(r2.value, "ok:x");
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "replay should not re-execute the body"
    );
}

#[tokio::test]
async fn run_replays_stored_result_even_when_input_bytes_differ() {
    // User-supplied workflow_id + run_id is the de-duplication contract.
    // Replaying the same key with a different input must NOT re-execute and
    // must NOT error — the stored result wins. Callers wanting a distinct
    // execution choose a distinct run_id.
    let (_tmp, store) = new_store();
    let options = WorkflowOptions::new("wf", "r-1").with_code_version("test");
    let executions = Arc::new(AtomicUsize::new(0));
    {
        let executions = executions.clone();
        let body = move |_w: Workflow, input: StringValue| {
            let executions = executions.clone();
            async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok::<StringValue, RunError>(s(&format!("ok:{}", input.value)))
            }
        };
        let r1: StringValue = run(store.clone(), options.clone(), s("AAPL"), body)
            .await
            .unwrap();
        assert_eq!(r1.value, "ok:AAPL");
    }

    let body2 = {
        let executions = executions.clone();
        move |_w: Workflow, input: StringValue| {
            let executions = executions.clone();
            async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok::<StringValue, RunError>(s(&format!("ok:{}", input.value)))
            }
        }
    };
    let r2: StringValue = run(store, options, s("MSFT"), body2).await.unwrap();
    assert_eq!(r2.value, "ok:AAPL", "stored result must be replayed");
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "second invocation must not re-execute the body"
    );
}

#[tokio::test]
async fn execute_activity_runs_and_replays() {
    let (_tmp, store) = new_store();
    let options = WorkflowOptions::new("wf", "r-1").with_code_version("test");
    let executions = Arc::new(AtomicUsize::new(0));

    let result: StringValue = run(store.clone(), options.clone(), s("AAPL"), {
        let executions = executions.clone();
        move |_w, input| {
            let executions = executions.clone();
            async move {
                let resp: StringValue =
                    execute_activity(ActivityOptions::new("fetch"), input, |req: StringValue| {
                        let executions = executions.clone();
                        async move {
                            executions.fetch_add(1, Ordering::SeqCst);
                            Ok::<StringValue, ActivityError>(s(&format!("activity:{}", req.value)))
                        }
                    })
                    .await?;
                Ok::<StringValue, RunError>(resp)
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(result.value, "activity:AAPL");
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    // Force re-run by re-storing IN_PROGRESS — for now just check that
    // running with same inputs replays via the workflow record.
    let result2: StringValue = run(store, options, s("AAPL"), {
        let executions = executions.clone();
        move |_w, input| {
            let executions = executions.clone();
            async move {
                let resp: StringValue =
                    execute_activity(ActivityOptions::new("fetch"), input, |req: StringValue| {
                        let executions = executions.clone();
                        async move {
                            executions.fetch_add(1, Ordering::SeqCst);
                            Ok::<StringValue, ActivityError>(s(&format!("activity:{}", req.value)))
                        }
                    })
                    .await?;
                Ok::<StringValue, RunError>(resp)
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(result2.value, "activity:AAPL");
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "replay short-circuits on the workflow record, not even hitting execute_activity"
    );
}

#[tokio::test]
async fn execute_activity_retries_and_succeeds() {
    let (_tmp, store) = new_store();
    let options = WorkflowOptions::new("wf", "r-1").with_code_version("test");
    let attempts = Arc::new(AtomicUsize::new(0));

    let result: StringValue = run(store, options, s("x"), {
        let attempts = attempts.clone();
        move |_w, input| {
            let attempts = attempts.clone();
            async move {
                let policy = RetryPolicy {
                    maximum_attempts: 3,
                    initial_interval: Duration::from_millis(1),
                    backoff_coefficient: 1.0,
                    maximum_interval: Duration::from_millis(1),
                    non_retryable_error_codes: Vec::new(),
                };
                let resp: StringValue = execute_activity(
                    ActivityOptions::new("flaky").with_retry_policy(policy),
                    input,
                    |req: StringValue| {
                        let attempts = attempts.clone();
                        async move {
                            let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                            if n < 3 {
                                Err(ActivityError::new("transient", "retry"))
                            } else {
                                Ok(s(&format!("ok:{}", req.value)))
                            }
                        }
                    },
                )
                .await?;
                Ok(resp)
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(result.value, "ok:x");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn execute_activity_terminal_failure_after_exhausted_retries() {
    let (_tmp, store) = new_store();
    let options = WorkflowOptions::new("wf", "r-failed").with_code_version("test");
    let attempts = Arc::new(AtomicUsize::new(0));

    let err = run::<StringValue, StringValue, _, _>(store, options, s("x"), {
        let attempts = attempts.clone();
        move |_w, input| {
            let attempts = attempts.clone();
            async move {
                let policy = RetryPolicy {
                    maximum_attempts: 2,
                    initial_interval: Duration::from_millis(1),
                    backoff_coefficient: 1.0,
                    maximum_interval: Duration::from_millis(1),
                    non_retryable_error_codes: Vec::new(),
                };
                execute_activity(
                    ActivityOptions::new("always_fail").with_retry_policy(policy),
                    input,
                    |_req: StringValue| {
                        let attempts = attempts.clone();
                        async move {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            Err::<StringValue, _>(ActivityError::new("nope", "fail"))
                        }
                    },
                )
                .await
            }
        }
    })
    .await
    .err()
    .unwrap();
    assert!(matches!(err, RunError::Activity(_)));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn retry_after_overrides_short_interval() {
    let (_tmp, store) = new_store();
    let options = WorkflowOptions::new("wf", "r-ra").with_code_version("test");
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = std::time::Instant::now();

    let result: StringValue = run(store, options, s("x"), {
        let attempts = attempts.clone();
        move |_w, input| {
            let attempts = attempts.clone();
            async move {
                let policy = RetryPolicy {
                    maximum_attempts: 2,
                    initial_interval: Duration::from_millis(1),
                    backoff_coefficient: 1.0,
                    maximum_interval: Duration::from_millis(1),
                    non_retryable_error_codes: Vec::new(),
                };
                let resp: StringValue = execute_activity(
                    ActivityOptions::new("with_retry_after").with_retry_policy(policy),
                    input,
                    |req: StringValue| {
                        let attempts = attempts.clone();
                        async move {
                            let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                            if n == 1 {
                                Err(ActivityError::new("rate_limited", "429")
                                    .with_retry_after(Duration::from_millis(100)))
                            } else {
                                Ok(s(&format!("ok:{}", req.value)))
                            }
                        }
                    },
                )
                .await?;
                Ok(resp)
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(result.value, "ok:x");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(90),
        "Retry-After should have made the runtime wait ~100ms; only slept {elapsed:?}",
    );
}

#[tokio::test]
async fn non_retryable_error_codes_skip_remaining_retries() {
    let (_tmp, store) = new_store();
    let options = WorkflowOptions::new("wf", "r-nrc").with_code_version("test");
    let attempts = Arc::new(AtomicUsize::new(0));

    let err = run::<StringValue, StringValue, _, _>(store, options, s("x"), {
        let attempts = attempts.clone();
        move |_w, input| {
            let attempts = attempts.clone();
            async move {
                let policy = RetryPolicy {
                    maximum_attempts: 5,
                    initial_interval: Duration::from_millis(1),
                    backoff_coefficient: 1.0,
                    maximum_interval: Duration::from_millis(1),
                    non_retryable_error_codes: vec!["invalid_argument".into()],
                };
                execute_activity(
                    ActivityOptions::new("invalid").with_retry_policy(policy),
                    input,
                    |_req: StringValue| {
                        let attempts = attempts.clone();
                        async move {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            Err::<StringValue, _>(ActivityError::new(
                                "invalid_argument",
                                "bad input",
                            ))
                        }
                    },
                )
                .await
            }
        }
    })
    .await
    .err()
    .unwrap();
    assert!(matches!(err, RunError::Activity(_)));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "non-retryable codes must short-circuit after the first attempt"
    );
}

#[tokio::test]
async fn annotate_persists_on_activity_record() {
    let (_tmp, store) = new_store();
    let options = WorkflowOptions::new("wf", "r-ann").with_code_version("test");
    let _: StringValue = run(store.clone(), options, s("x"), |_w, input| async move {
        let resp: StringValue = execute_activity(
            ActivityOptions::new("annotated"),
            input,
            |req: StringValue| async move {
                annotate("model", "claude-opus");
                annotate("tokens", "42");
                Ok::<StringValue, ActivityError>(s(&format!("ok:{}", req.value)))
            },
        )
        .await?;
        Ok(resp)
    })
    .await
    .unwrap();

    // Read the stored activity record back and verify annotations survived.
    use temporaless::storage::{ActivityKey, Store as _};
    let record = store
        .get_activity(&ActivityKey::new("wf", "r-ann", "annotated"))
        .await
        .unwrap()
        .expect("present");
    assert_eq!(
        record.annotations.get("model").map(String::as_str),
        Some("claude-opus")
    );
    assert_eq!(
        record.annotations.get("tokens").map(String::as_str),
        Some("42")
    );
}

#[tokio::test]
async fn current_workflow_accessor_works_from_activity_body() {
    let (_tmp, store) = new_store();
    let options = WorkflowOptions::new("wf-cur", "r-1").with_code_version("test");
    let _: StringValue = run(store, options, s("x"), |_w, input| async move {
        let resp: StringValue = execute_activity(
            ActivityOptions::new("uses_current"),
            input,
            |req: StringValue| async move {
                let w = current();
                assert_eq!(w.workflow_id(), "wf-cur");
                assert_eq!(w.run_id(), "r-1");
                assert_eq!(w.code_version(), "test");
                Ok::<StringValue, ActivityError>(s(&format!(
                    "seen:{}:{}",
                    w.workflow_id(),
                    req.value
                )))
            },
        )
        .await?;
        Ok(resp)
    })
    .await
    .unwrap();
}
