//! Storage tests — same shape as the Go (`core/go/storage/opendal_test.go`)
//! and Python (`core/py/tests/test_workflow.py` storage paths) test suites,
//! so we can confirm the Rust SDK reads/writes the same wire format.

use std::time::{Duration, SystemTime};

use opendal::{services::Fs, Operator};
use tempfile::TempDir;
use temporaless::storage::{proto_timestamp, ActivityKey, OpenDALStore, Store, WorkflowKey};
use temporaless::v1;

fn new_store() -> (TempDir, OpenDALStore) {
    let tmp = TempDir::new().unwrap();
    let builder = Fs::default().root(tmp.path().to_str().unwrap());
    let op = Operator::new(builder).unwrap().finish();
    (tmp, OpenDALStore::new(op))
}

fn now_ts() -> prost_types::Timestamp {
    proto_timestamp(SystemTime::now())
}

fn mk_workflow_record(
    workflow_id: &str,
    run_id: &str,
    status: v1::WorkflowStatus,
) -> v1::WorkflowRecord {
    let key = WorkflowKey::new(workflow_id, run_id).to_proto();
    v1::WorkflowRecord {
        schema_version: v1::RecordSchemaVersion::Workflow as i32,
        key: Some(key),
        workflow_type: "workflow:google.protobuf.StringValue->google.protobuf.StringValue".into(),
        code_version: "test".into(),
        input: None,
        status: status as i32,
        result: None,
        failure: None,
        created_at: Some(now_ts()),
        completed_at: if matches!(
            status,
            v1::WorkflowStatus::Completed | v1::WorkflowStatus::Failed
        ) {
            Some(now_ts())
        } else {
            None
        },
        annotations: Default::default(),
    }
}

#[tokio::test]
async fn workflow_roundtrip() {
    let (_tmp, store) = new_store();
    let key = WorkflowKey::new("wf-a", "run-1");
    let record = mk_workflow_record("wf-a", "run-1", v1::WorkflowStatus::Completed);
    store.put_workflow(&record).await.unwrap();

    let got = store.get_workflow(&key).await.unwrap().expect("present");
    assert_eq!(got.workflow_type, record.workflow_type);
    assert_eq!(got.code_version, record.code_version);
    assert_eq!(got.status, v1::WorkflowStatus::Completed as i32);
}

#[tokio::test]
async fn get_workflow_returns_none_when_absent() {
    let (_tmp, store) = new_store();
    let got = store
        .get_workflow(&WorkflowKey::new("missing", "x"))
        .await
        .unwrap();
    assert!(got.is_none());
}

#[tokio::test]
async fn delete_workflow_idempotent() {
    let (_tmp, store) = new_store();
    let key = WorkflowKey::new("wf", "r");
    let record = mk_workflow_record("wf", "r", v1::WorkflowStatus::Completed);
    store.put_workflow(&record).await.unwrap();
    assert!(store.delete_workflow(&key).await.unwrap());
    assert!(!store.delete_workflow(&key).await.unwrap());
    assert!(store.get_workflow(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn list_workflows_filters_by_status() {
    let (_tmp, store) = new_store();
    store
        .put_workflow(&mk_workflow_record(
            "wf-a",
            "r1",
            v1::WorkflowStatus::Completed,
        ))
        .await
        .unwrap();
    store
        .put_workflow(&mk_workflow_record(
            "wf-b",
            "r2",
            v1::WorkflowStatus::Failed,
        ))
        .await
        .unwrap();

    let all = store
        .list_workflows("", "", v1::WorkflowStatus::Unspecified)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);

    let failed = store
        .list_workflows("", "", v1::WorkflowStatus::Failed)
        .await
        .unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].status, v1::WorkflowStatus::Failed as i32);
}

#[tokio::test]
async fn activity_roundtrip() {
    let (_tmp, store) = new_store();
    let record = v1::ActivityRecord {
        schema_version: v1::RecordSchemaVersion::Activity as i32,
        key: Some(ActivityKey::new("wf-a", "r1", "act:1").to_proto()),
        activity_type: "activity:google.protobuf.StringValue->google.protobuf.StringValue".into(),
        code_version: "test".into(),
        input: None,
        status: v1::ActivityStatus::Completed as i32,
        result: None,
        failure: None,
        created_at: Some(now_ts()),
        completed_at: Some(now_ts()),
        attempts: vec![],
        annotations: Default::default(),
        next_attempt_at: None,
    };
    store.put_activity(&record).await.unwrap();

    let got = store
        .get_activity(&ActivityKey::new("wf-a", "r1", "act:1"))
        .await
        .unwrap()
        .expect("present");
    assert_eq!(got.activity_type, record.activity_type);
    assert_eq!(got.status, v1::ActivityStatus::Completed as i32);
}

#[tokio::test]
async fn list_activities_returns_all_under_run() {
    let (_tmp, store) = new_store();
    for i in 0..5 {
        store
            .put_activity(&v1::ActivityRecord {
                schema_version: v1::RecordSchemaVersion::Activity as i32,
                key: Some(ActivityKey::new("wf-a", "r1", format!("act:{i}")).to_proto()),
                activity_type: "activity:google.protobuf.StringValue->google.protobuf.StringValue"
                    .into(),
                code_version: "test".into(),
                input: None,
                status: v1::ActivityStatus::Completed as i32,
                result: None,
                failure: None,
                created_at: Some(now_ts()),
                completed_at: Some(now_ts()),
                attempts: vec![],
                annotations: Default::default(),
                next_attempt_at: None,
            })
            .await
            .unwrap();
    }
    let activities = store
        .list_activities(&WorkflowKey::new("wf-a", "r1"))
        .await
        .unwrap();
    assert_eq!(activities.len(), 5);
}

#[tokio::test]
async fn sweep_removes_old_completed_runs() {
    let (_tmp, store) = new_store();
    // Two completed: one old, one new.
    let mut old = mk_workflow_record("wf-old", "r1", v1::WorkflowStatus::Completed);
    let past = SystemTime::now() - Duration::from_secs(2 * 3600);
    old.completed_at = Some(proto_timestamp(past));
    store.put_workflow(&old).await.unwrap();

    store
        .put_workflow(&mk_workflow_record(
            "wf-new",
            "r2",
            v1::WorkflowStatus::Completed,
        ))
        .await
        .unwrap();

    let deleted = store
        .sweep("", SystemTime::now(), Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    assert!(store
        .get_workflow(&WorkflowKey::new("wf-old", "r1"))
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_workflow(&WorkflowKey::new("wf-new", "r2"))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn due_timers_returns_only_scheduled_under_in_progress() {
    let (_tmp, store) = new_store();
    // IN_PROGRESS workflow with a due (past fire_at) SCHEDULED timer.
    store
        .put_workflow(&mk_workflow_record(
            "wf-a",
            "r1",
            v1::WorkflowStatus::InProgress,
        ))
        .await
        .unwrap();

    use temporaless::storage::TimerKey;
    let past = proto_timestamp(SystemTime::now() - Duration::from_secs(60));
    store
        .put_timer(&v1::TimerRecord {
            schema_version: v1::RecordSchemaVersion::Timer as i32,
            key: Some(TimerKey::new("wf-a", "r1", "t1").to_proto()),
            timer_kind: v1::TimerKind::Sleep as i32,
            code_version: "test".into(),
            duration: None,
            status: v1::TimerStatus::Scheduled as i32,
            fire_at: Some(past),
            created_at: Some(now_ts()),
            fired_at: None,
        })
        .await
        .unwrap();

    let due = store.due_timers("", SystemTime::now()).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].key.timer_id, "t1");
}

#[tokio::test]
async fn claim_create_only_semantics() {
    let (_tmp, store) = new_store();
    use temporaless::storage::ClaimKey;
    let claim = v1::ClaimRecord {
        schema_version: v1::RecordSchemaVersion::Claim as i32,
        key: Some(ClaimKey::new("wf", "r", "claim:1").to_proto()),
        owner_id: "worker-a".into(),
        resource_type: v1::ClaimResourceType::Activity as i32,
        resource_id: "act:1".into(),
        code_version: "test".into(),
        lease_expires_at: Some(proto_timestamp(SystemTime::now() + Duration::from_secs(60))),
        created_at: Some(now_ts()),
        heartbeat_at: Some(now_ts()),
    };
    assert!(store.try_create_claim(&claim).await.unwrap());
    assert!(!store.try_create_claim(&claim).await.unwrap()); // already exists
    assert!(store
        .delete_claim(&ClaimKey::new("wf", "r", "claim:1"))
        .await
        .unwrap());
    assert!(store.try_create_claim(&claim).await.unwrap()); // can re-create after delete
}
