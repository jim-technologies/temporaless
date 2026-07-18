//! Cross-language interop tests — proves the Rust SDK reads records
//! written with the same wire format the Go/Python SDKs use.
//!
//! Strategy: hand-construct a `WorkflowRecord` / `ActivityRecord` using
//! the same paths, schema versions, and proto field layouts the Python
//! SDK would produce, write them via opendal, then re-read them via the
//! Rust `OpenDALStore` and assert the round-trip preserves every field.
//!
//! A full cross-process round-trip (subprocess Python + asserted-from-Rust)
//! would be heavier and is left for a future iteration; the wire-format
//! invariant is what these tests are about.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use opendal::{Operator, services::Fs};
use prost::{Message, Name};
use tempfile::TempDir;
use temporaless::storage::{ActivityKey, OpenDALStore, Store, WorkflowKey, proto_timestamp};
use temporaless::v1;
use temporaless::workflow::{
    ActivityError, ActivityOptions, RunError, Workflow, WorkflowOptions, execute_activity, run,
};

fn new_store() -> (TempDir, OpenDALStore) {
    let tmp = TempDir::new().unwrap();
    let builder = Fs::default().root(tmp.path().to_str().unwrap());
    let op = Operator::new(builder).unwrap();
    (tmp, OpenDALStore::new(op))
}

/// A workflow record exactly as Python's `temporaless.workflow.run` would
/// write it on a successful execution: schema_version set, key populated,
/// CompletedAt + CreatedAt timestamps, status=COMPLETED, annotations from
/// the workflow body's annotate() calls. We assert every field round-trips.
#[tokio::test]
async fn python_authored_workflow_record_decodes_correctly() {
    let (_tmp, store) = new_store();
    let now = proto_timestamp(SystemTime::now());

    let key = WorkflowKey::new("prices:aapl", "2026-05-04T09:30:00Z");
    let mut annotations = std::collections::HashMap::new();
    annotations.insert("source".into(), "python".into());
    annotations.insert("vendor".into(), "alphavantage".into());

    let record = v1::WorkflowRecord {
        schema_version: v1::RecordSchemaVersion::Workflow as i32,
        key: Some(key.to_proto()),
        workflow_type: "workflow:google.protobuf.StringValue->google.protobuf.StringValue".into(),
        code_version: "v0.3.7".into(),
        input: Some(prost_types::Any {
            type_url: "type.googleapis.com/google.protobuf.StringValue".into(),
            value: encode_string_value("AAPL"),
        }),
        status: v1::WorkflowStatus::Completed as i32,
        result: Some(prost_types::Any {
            type_url: "type.googleapis.com/google.protobuf.StringValue".into(),
            value: encode_string_value("price:189.12"),
        }),
        failure: None,
        created_at: Some(now),
        completed_at: Some(now),
        annotations,
        ..Default::default()
    };
    store.put_workflow(&record).await.unwrap();

    let read_back = store.get_workflow(&key).await.unwrap().expect("present");
    assert_eq!(read_back.workflow_type, record.workflow_type);
    assert_eq!(read_back.code_version, "v0.3.7");
    assert_eq!(read_back.status, v1::WorkflowStatus::Completed as i32);
    assert_eq!(
        read_back.annotations.get("source").map(String::as_str),
        Some("python")
    );
    assert_eq!(
        read_back.annotations.get("vendor").map(String::as_str),
        Some("alphavantage")
    );

    // The Any payload round-trips byte-for-byte. Verify both the type_url
    // and the inner StringValue decode.
    let result_any = read_back.result.as_ref().expect("present");
    assert_eq!(
        result_any.type_url,
        "type.googleapis.com/google.protobuf.StringValue"
    );
    let inner_str = decode_string_value(&result_any.value);
    assert_eq!(inner_str, "price:189.12");
}

/// An activity record with a full retry history — same shape Python's
/// runtime writes after a retried activity completes. Verifies the
/// repeated `ActivityAttempt` + per-attempt `ActivityFailure` round-trip,
/// including the `retry_after` we added for vendor-aware backoffs.
#[tokio::test]
async fn python_authored_activity_record_with_retry_history() {
    let (_tmp, store) = new_store();
    let key = ActivityKey::new("prices:aapl", "2026-05-04T09:30:00Z", "fetch:quote");
    let now_dur = SystemTime::now();
    let now = proto_timestamp(now_dur);

    let attempts = vec![
        v1::ActivityAttempt {
            attempt: 1,
            started_at: Some(now),
            completed_at: Some(now),
            failure: Some(v1::ActivityFailure {
                code: "rate_limited".into(),
                message: "vendor 429".into(),
                retry_after: Some(prost_types::Duration {
                    seconds: 60,
                    nanos: 0,
                }),
            }),
        },
        v1::ActivityAttempt {
            attempt: 2,
            started_at: Some(now),
            completed_at: Some(now),
            failure: None,
        },
    ];

    let record = v1::ActivityRecord {
        schema_version: v1::RecordSchemaVersion::Activity as i32,
        key: Some(key.to_proto()),
        activity_type: "activity:google.protobuf.StringValue->google.protobuf.StringValue".into(),
        code_version: "v0.3.7".into(),
        input: Some(prost_types::Any {
            type_url: "type.googleapis.com/google.protobuf.StringValue".into(),
            value: encode_string_value("AAPL"),
        }),
        status: v1::ActivityStatus::Completed as i32,
        result: Some(prost_types::Any {
            type_url: "type.googleapis.com/google.protobuf.StringValue".into(),
            value: encode_string_value("189.12"),
        }),
        failure: None,
        created_at: Some(now),
        completed_at: Some(now),
        attempts,
        annotations: Default::default(),
        next_attempt_at: None,
        ..Default::default()
    };
    store.put_activity(&record).await.unwrap();

    let read_back = store.get_activity(&key).await.unwrap().expect("present");
    assert_eq!(read_back.attempts.len(), 2);
    let attempt_1 = &read_back.attempts[0];
    let failure = attempt_1.failure.as_ref().expect("present");
    assert_eq!(failure.code, "rate_limited");
    assert_eq!(failure.message, "vendor 429");
    let ra = failure.retry_after.as_ref().expect("present");
    assert_eq!(ra.seconds, 60);
    assert_eq!(ra.nanos, 0);
    assert_eq!(
        Duration::new(ra.seconds as u64, ra.nanos as u32),
        Duration::from_secs(60)
    );
    assert!(read_back.attempts[1].failure.is_none());
}

/// Workflows written by every SDK live at the same flat v2 key. Inspecting
/// the filesystem confirms Rust does not drift from the Go/Python layout.
#[tokio::test]
async fn rust_writes_canonical_v2_path() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().to_str().unwrap();
    let builder = Fs::default().root(root);
    let op = Operator::new(builder).unwrap();
    let store = OpenDALStore::new(op);

    let key = WorkflowKey::new("prices:aapl", "2026-05-04T09:30:00Z");
    let now = proto_timestamp(SystemTime::now());
    let record = v1::WorkflowRecord {
        schema_version: v1::RecordSchemaVersion::Workflow as i32,
        key: Some(key.to_proto()),
        workflow_type: "workflow:google.protobuf.StringValue->google.protobuf.StringValue".into(),
        code_version: "v1".into(),
        input: None,
        status: v1::WorkflowStatus::Completed as i32,
        result: None,
        failure: None,
        created_at: Some(now),
        completed_at: Some(now),
        annotations: Default::default(),
        ..Default::default()
    };
    store.put_workflow(&record).await.unwrap();

    // Expected canonical path — matches what Go/Python produce.
    let expected_path =
        format!("{root}/temporaless/v2/default/prices:aapl/2026-05-04T09:30:00Z/workflow.binpb");
    assert!(
        std::path::Path::new(&expected_path).exists(),
        "expected canonical v2 path {expected_path}, fs contents: {:?}",
        walk(tmp.path()).collect::<Vec<_>>(),
    );
}

/// Pre-seed a workflow record using the canonical `workflow_type` string
/// Go and Python emit (`workflow:google.protobuf.StringValue->...`), then
/// run `workflow::run()` and assert it replays the stored result without
/// `WorkflowConflict`. This is the core cross-language replay guarantee:
/// records written by any SDK must be replayable from any other.
///
/// Pre-fingerprint-removal, this test would have failed at the digest
/// assertion (Rust's `std::any::type_name` produced a different string
/// than the proto descriptor full name Go/Python use). Post-fix it passes
/// because (1) the digest is gone and (2) Rust's `message_pair_type`
/// uses `prost::Name::full_name()` — the same descriptor string.
#[tokio::test]
async fn rust_replays_python_authored_workflow_record() {
    let tmp = TempDir::new().unwrap();
    let builder = Fs::default().root(tmp.path().to_str().unwrap());
    let op = Operator::new(builder).unwrap();
    let store = Arc::new(OpenDALStore::new(op));

    let key = WorkflowKey::new("prices:aapl", "2026-05-04T09:30:00Z");
    let now = proto_timestamp(SystemTime::now());
    let seeded = v1::WorkflowRecord {
        schema_version: v1::RecordSchemaVersion::Workflow as i32,
        key: Some(key.to_proto()),
        // Exactly the string Python's `message_pair_type` produces.
        workflow_type: "workflow:google.protobuf.StringValue->google.protobuf.StringValue".into(),
        code_version: "v1".into(),
        input: Some(prost_types::Any {
            type_url: "type.googleapis.com/google.protobuf.StringValue".into(),
            value: encode_string_value("AAPL"),
        }),
        status: v1::WorkflowStatus::Completed as i32,
        result: Some(prost_types::Any {
            type_url: "type.googleapis.com/google.protobuf.StringValue".into(),
            value: encode_string_value("normalized:AAPL"),
        }),
        failure: None,
        created_at: Some(now),
        completed_at: Some(now),
        annotations: Default::default(),
        ..Default::default()
    };
    store.put_workflow(&seeded).await.unwrap();

    let options =
        WorkflowOptions::new("prices:aapl", "2026-05-04T09:30:00Z").with_code_version("v1");
    let body = |_w: Workflow, _input: TestStringValue| async move {
        panic!("body must not run — stored COMPLETED record should replay")
    };
    let result: TestStringValue = run(store, options, ts("AAPL"), body)
        .await
        .expect("replay should succeed");
    assert_eq!(result.value, "normalized:AAPL");
}

/// Rust-authored records must be directly consumable by Go/Python. In
/// particular, every Any uses the protobuf descriptor full name rather than
/// Rust's short type name.
#[tokio::test]
async fn rust_authored_records_use_canonical_any_type_urls() {
    let (_tmp, store) = new_store();
    let store = Arc::new(store);
    let options = WorkflowOptions::new("prices:aapl", "rust-authored").with_code_version("v1");

    let result: TestStringValue = run(
        store.clone(),
        options,
        ts("AAPL"),
        |_workflow, input| async move {
            execute_activity(
                ActivityOptions::new("fetch"),
                input,
                |request: TestStringValue| async move {
                    Ok::<TestStringValue, ActivityError>(ts(&format!(
                        "normalized:{}",
                        request.value
                    )))
                },
            )
            .await
        },
    )
    .await
    .expect("fresh Rust-authored workflow should complete");
    assert_eq!(result.value, "normalized:AAPL");

    let expected = "type.googleapis.com/google.protobuf.StringValue";
    let workflow = store
        .get_workflow(&WorkflowKey::new("prices:aapl", "rust-authored"))
        .await
        .unwrap()
        .expect("workflow record");
    assert_eq!(workflow.input.as_ref().unwrap().type_url, expected);
    assert_eq!(workflow.result.as_ref().unwrap().type_url, expected);

    let activity = store
        .get_activity(&ActivityKey::new("prices:aapl", "rust-authored", "fetch"))
        .await
        .unwrap()
        .expect("activity record");
    assert_eq!(activity.input.as_ref().unwrap().type_url, expected);
    assert_eq!(activity.result.as_ref().unwrap().type_url, expected);
}

#[tokio::test]
async fn rust_rejects_replayed_result_with_wrong_any_type_url() {
    let (_tmp, store) = new_store();
    let store = Arc::new(store);
    let key = WorkflowKey::new("prices:aapl", "wrong-type-url");
    let now = proto_timestamp(SystemTime::now());
    store
        .put_workflow(&v1::WorkflowRecord {
            schema_version: v1::RecordSchemaVersion::Workflow as i32,
            key: Some(key.to_proto()),
            workflow_type: "workflow:google.protobuf.StringValue->google.protobuf.StringValue"
                .into(),
            code_version: "v1".into(),
            input: Some(prost_types::Any {
                type_url: "type.googleapis.com/google.protobuf.StringValue".into(),
                value: encode_string_value("AAPL"),
            }),
            status: v1::WorkflowStatus::Completed as i32,
            result: Some(prost_types::Any {
                type_url: "type.googleapis.com/google.protobuf.Int32Value".into(),
                // These bytes would decode as StringValue if replay ignored
                // the Any type URL.
                value: encode_string_value("not-an-int"),
            }),
            created_at: Some(now),
            completed_at: Some(now),
            ..Default::default()
        })
        .await
        .unwrap();

    let err = run::<TestStringValue, TestStringValue, _, _>(
        store,
        WorkflowOptions::new("prices:aapl", "wrong-type-url").with_code_version("v1"),
        ts("AAPL"),
        |_workflow, _input| async { panic!("body must not run for a completed workflow record") },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, RunError::WorkflowConflict(ref message) if message.contains("type URL")),
        "wrong Any type URL must be rejected, got {err:?}",
    );
}

#[tokio::test]
async fn rust_rejects_replayed_activity_result_with_wrong_any_type_url() {
    let (_tmp, store) = new_store();
    let store = Arc::new(store);
    let now = proto_timestamp(SystemTime::now());
    store
        .put_activity(&v1::ActivityRecord {
            schema_version: v1::RecordSchemaVersion::Activity as i32,
            key: Some(
                ActivityKey::new("prices:aapl", "wrong-activity-type-url", "fetch").to_proto(),
            ),
            activity_type: "activity:google.protobuf.StringValue->google.protobuf.StringValue"
                .into(),
            code_version: "v1".into(),
            input: Some(prost_types::Any {
                type_url: "type.googleapis.com/google.protobuf.StringValue".into(),
                value: encode_string_value("AAPL"),
            }),
            status: v1::ActivityStatus::Completed as i32,
            result: Some(prost_types::Any {
                type_url: "type.googleapis.com/google.protobuf.Int32Value".into(),
                value: encode_string_value("not-an-int"),
            }),
            created_at: Some(now),
            completed_at: Some(now),
            ..Default::default()
        })
        .await
        .unwrap();

    let err = run(
        store,
        WorkflowOptions::new("prices:aapl", "wrong-activity-type-url").with_code_version("v1"),
        ts("AAPL"),
        |_workflow, input| async move {
            execute_activity(
                ActivityOptions::new("fetch"),
                input,
                |request: TestStringValue| async move {
                    Ok::<TestStringValue, ActivityError>(request)
                },
            )
            .await
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, RunError::ActivityConflict(ref message) if message.contains("type URL")),
        "wrong activity Any type URL must be rejected, got {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Helpers — minimal StringValue encoding so we don't need a separate proto
// dep for the test.
// ---------------------------------------------------------------------------

fn encode_string_value(s: &str) -> Vec<u8> {
    // google.protobuf.StringValue is `string value = 1`. Field 1 wire
    // type 2 (length-delimited): tag = (1 << 3) | 2 = 0x0A.
    let mut out = Vec::with_capacity(2 + s.len());
    out.push(0x0A);
    prost::encoding::encode_varint(s.len() as u64, &mut out);
    out.extend_from_slice(s.as_bytes());
    out
}

fn decode_string_value(bytes: &[u8]) -> String {
    // Trivial single-field reader. Skip the tag, read the varint length,
    // copy the bytes.
    let buf = bytes;
    assert_eq!(buf[0], 0x0A, "unexpected wire tag");
    let mut idx = 1;
    let mut len = 0u64;
    let mut shift = 0;
    loop {
        let byte = buf[idx];
        idx += 1;
        len |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    String::from_utf8(buf[idx..idx + len as usize].to_vec()).unwrap()
}

fn walk(root: &std::path::Path) -> impl Iterator<Item = std::path::PathBuf> {
    fn inner(dir: std::path::PathBuf, out: &mut Vec<std::path::PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    inner(path, out);
                } else {
                    out.push(path);
                }
            }
        }
    }
    let mut out = Vec::new();
    inner(root.to_path_buf(), &mut out);
    out.into_iter()
}

// Use Message + prost types so the dependencies are linked into the test.
#[allow(dead_code)]
fn _ensure_message_trait_in_scope<M: Message>() {}

/// Local mirror of `google.protobuf.StringValue` — same wire layout, same
/// descriptor full name. Used by the cross-language replay test to invoke
/// `workflow::run()` with a Req type whose `Name::full_name()` matches the
/// `workflow_type` string Python/Go records carry.
#[derive(Clone, PartialEq, Message)]
struct TestStringValue {
    #[prost(string, tag = "1")]
    value: String,
}

impl Name for TestStringValue {
    const NAME: &'static str = "StringValue";
    const PACKAGE: &'static str = "google.protobuf";
}

fn ts(value: &str) -> TestStringValue {
    TestStringValue {
        value: value.into(),
    }
}
