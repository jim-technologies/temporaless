//! Storage hot-path benchmarks for the Rust SDK.
//!
//! Mirrors `core/go/storage/benchmark_test.go` and `core/py/benchmarks/
//! bench_storage.py` so cross-language costs are directly comparable. Output
//! format is Go's testing.B style:
//!
//! ```text
//! BenchmarkName                              N    ns/op
//! ```
//!
//! Run with `cargo run --release --bin bench-storage`.

use std::path::PathBuf;
use std::process::id;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use opendal::{Operator, services::Fs};
use prost::Message;
use temporaless::storage::{ActivityKey, OpenDALStore, Store, WorkflowKey, proto_timestamp};
use temporaless::v1;

const TARGET_DURATION_NS: u128 = 1_000_000_000;

fn temp_root(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!("temporaless-bench-rs-{label}-{}-{nanos}", id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn new_store(label: &str) -> (PathBuf, OpenDALStore) {
    let root = temp_root(label);
    let builder = Fs::default().root(root.to_str().unwrap());
    let op = Operator::new(builder).unwrap();
    (root, OpenDALStore::new(op))
}

fn workflow_record(workflow_id: &str, run_id: &str) -> v1::WorkflowRecord {
    v1::WorkflowRecord {
        schema_version: v1::RecordSchemaVersion::Workflow as i32,
        key: Some(WorkflowKey::new(workflow_id, run_id).to_proto()),
        workflow_type: "workflow:google.protobuf.StringValue->google.protobuf.StringValue".into(),
        code_version: "bench".into(),
        input: None,
        status: v1::WorkflowStatus::Completed as i32,
        result: None,
        failure: None,
        created_at: Some(proto_timestamp(SystemTime::now())),
        completed_at: Some(proto_timestamp(SystemTime::now())),
        annotations: Default::default(),
        ..Default::default()
    }
}

fn activity_record(workflow_id: &str, run_id: &str, activity_id: &str) -> v1::ActivityRecord {
    v1::ActivityRecord {
        schema_version: v1::RecordSchemaVersion::Activity as i32,
        key: Some(ActivityKey::new(workflow_id, run_id, activity_id).to_proto()),
        activity_type: "activity:google.protobuf.StringValue->google.protobuf.StringValue".into(),
        code_version: "bench".into(),
        input: None,
        status: v1::ActivityStatus::Completed as i32,
        result: None,
        failure: None,
        created_at: Some(proto_timestamp(SystemTime::now())),
        completed_at: Some(proto_timestamp(SystemTime::now())),
        attempts: vec![],
        annotations: Default::default(),
        next_attempt_at: None,
        ..Default::default()
    }
}

/// Measure how long it takes to run `n` iterations of `body`. Auto-scales
/// `n` until elapsed wall-clock crosses ~1s (matching the Go/Python
/// harnesses), then reports `ns/op`.
async fn run_benchmark<F, Fut>(name: &str, body: F)
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut n: usize = 1;
    let mut last_n = 1usize;
    let mut last_ns_per_op: u128 = 0;
    for _ in 0..30 {
        let start = Instant::now();
        body(n).await;
        let elapsed = start.elapsed().as_nanos();
        last_n = n;
        last_ns_per_op = elapsed / n as u128;
        if elapsed >= TARGET_DURATION_NS {
            break;
        }
        if elapsed == 0 {
            n *= 100;
            continue;
        }
        let estimate = (n as u128 * TARGET_DURATION_NS / elapsed * 12 / 10) as usize;
        n = (n + 1).max(estimate.min(n * 100));
        if n > 100_000_000 {
            break;
        }
    }
    println!("{:<55} {:>10}  {:>10} ns/op", name, last_n, last_ns_per_op);
}

async fn bench_put_get_workflow() {
    let (_root, store) = new_store("put-get-wf");
    run_benchmark("BenchmarkPutGetWorkflow", |n| {
        let store = &store;
        async move {
            for i in 0..n {
                let workflow_id = "bench:wf";
                let run_id = format!("r{i}");
                let record = workflow_record(workflow_id, &run_id);
                store.put_workflow(&record).await.unwrap();
                let _ = store
                    .get_workflow(&WorkflowKey::new(workflow_id, run_id))
                    .await
                    .unwrap();
            }
        }
    })
    .await;
}

async fn bench_put_get_activity() {
    let (_root, store) = new_store("put-get-act");
    run_benchmark("BenchmarkPutGetActivity", |n| {
        let store = &store;
        async move {
            for i in 0..n {
                let activity_id = format!("act:{i}");
                let record = activity_record("bench:wf", "r1", &activity_id);
                store.put_activity(&record).await.unwrap();
                let _ = store
                    .get_activity(&ActivityKey::new("bench:wf", "r1", activity_id))
                    .await
                    .unwrap();
            }
        }
    })
    .await;
}

async fn bench_list_activities_under_run() {
    let (_root, store) = new_store("list-acts");
    // Seed 100 activities, then time how long a single ListActivities takes.
    for i in 0..100 {
        let r = activity_record("bench:wf", "r1", &format!("act:{i}"));
        store.put_activity(&r).await.unwrap();
    }
    let key = WorkflowKey::new("bench:wf", "r1");
    run_benchmark("BenchmarkListActivitiesUnderRun_100", |n| {
        let store = &store;
        let key = key.clone();
        async move {
            for _ in 0..n {
                let _ = store.list_activities(&key).await.unwrap();
            }
        }
    })
    .await;
}

async fn bench_encode_decode_activity() {
    let record = activity_record("bench:wf", "r1", "act:1");
    let bytes = record.encode_to_vec();
    run_benchmark("BenchmarkEncodeActivity", |n| {
        let record = &record;
        async move {
            for _ in 0..n {
                let _ = record.encode_to_vec();
            }
        }
    })
    .await;
    run_benchmark("BenchmarkDecodeActivity", |n| {
        let bytes = bytes.clone();
        async move {
            for _ in 0..n {
                let _ = v1::ActivityRecord::decode(bytes.as_slice()).unwrap();
            }
        }
    })
    .await;
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    bench_put_get_workflow().await;
    bench_put_get_activity().await;
    bench_list_activities_under_run().await;
    bench_encode_decode_activity().await;
}
