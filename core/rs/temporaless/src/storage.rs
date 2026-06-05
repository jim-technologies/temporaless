//! OpenDAL-backed storage for temporaless records (Rust SDK).
//!
//! Mirrors `core/go/storage/opendal.go` and
//! `core/py/src/temporaless/storage.py` — same Hive-partitioned paths, same
//! protobuf records, same on-disk bytes. Workflows written by the Go or
//! Python SDK are fully readable here and vice-versa.
//!
//! Path layout (strict Hive partitioning — every directory level is
//! `key=value`):
//!
//! ```text
//! temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=workflow/record.binpb
//! temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=activity/activity_id={aid}/record.binpb
//! temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=timer/timer_id={tid}/record.binpb
//! temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=event/event_id={eid}/record.binpb
//! temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=claim/claim_id={cid}/record.binpb
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opendal::Operator;
use prost::Message;
use thiserror::Error;

use crate::v1;

pub const DEFAULT_NAMESPACE: &str = "default";
pub const STORAGE_ROOT_PREFIX: &str = "temporaless/v1";

/// All errors the storage layer can surface to callers. The opendal /
/// prost errors are flattened so application code never has to depend on
/// those crates directly.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("opendal: {0}")]
    OpenDal(#[from] opendal::Error),
    #[error("decode proto: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("encode proto: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("invalid key: {0}")]
    InvalidKey(String),
}

/// `workflow_id + run_id` addresses one workflow execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkflowKey {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: String,
}

impl WorkflowKey {
    pub fn new(workflow_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.into(),
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
        }
    }

    fn path(&self) -> String {
        format!(
            "{prefix}/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=workflow/record.binpb",
            prefix = STORAGE_ROOT_PREFIX,
            ns = self.namespace,
            wf = self.workflow_id,
            rid = self.run_id,
        )
    }

    pub fn to_proto(&self) -> v1::WorkflowKey {
        v1::WorkflowKey {
            namespace: self.namespace.clone(),
            workflow_id: self.workflow_id.clone(),
            run_id: self.run_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActivityKey {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: String,
    pub activity_id: String,
}

impl ActivityKey {
    pub fn new(
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
        activity_id: impl Into<String>,
    ) -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.into(),
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
            activity_id: activity_id.into(),
        }
    }

    fn path(&self) -> String {
        format!(
            "{prefix}/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=activity/activity_id={aid}/record.binpb",
            prefix = STORAGE_ROOT_PREFIX,
            ns = self.namespace,
            wf = self.workflow_id,
            rid = self.run_id,
            aid = self.activity_id,
        )
    }

    fn dir_path(&self) -> String {
        format!(
            "{prefix}/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=activity/",
            prefix = STORAGE_ROOT_PREFIX,
            ns = self.namespace,
            wf = self.workflow_id,
            rid = self.run_id,
        )
    }

    pub fn to_proto(&self) -> v1::ActivityKey {
        v1::ActivityKey {
            namespace: self.namespace.clone(),
            workflow_id: self.workflow_id.clone(),
            run_id: self.run_id.clone(),
            activity_id: self.activity_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimerKey {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: String,
    pub timer_id: String,
}

impl TimerKey {
    pub fn new(
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
        timer_id: impl Into<String>,
    ) -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.into(),
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
            timer_id: timer_id.into(),
        }
    }

    fn path(&self) -> String {
        format!(
            "{prefix}/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=timer/timer_id={tid}/record.binpb",
            prefix = STORAGE_ROOT_PREFIX,
            ns = self.namespace,
            wf = self.workflow_id,
            rid = self.run_id,
            tid = self.timer_id,
        )
    }

    fn dir_path(&self) -> String {
        format!(
            "{prefix}/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=timer/",
            prefix = STORAGE_ROOT_PREFIX,
            ns = self.namespace,
            wf = self.workflow_id,
            rid = self.run_id,
        )
    }

    pub fn to_proto(&self) -> v1::TimerKey {
        v1::TimerKey {
            namespace: self.namespace.clone(),
            workflow_id: self.workflow_id.clone(),
            run_id: self.run_id.clone(),
            timer_id: self.timer_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventKey {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: String,
    pub event_id: String,
}

impl EventKey {
    pub fn new(
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
        event_id: impl Into<String>,
    ) -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.into(),
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
            event_id: event_id.into(),
        }
    }

    fn path(&self) -> String {
        format!(
            "{prefix}/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=event/event_id={eid}/record.binpb",
            prefix = STORAGE_ROOT_PREFIX,
            ns = self.namespace,
            wf = self.workflow_id,
            rid = self.run_id,
            eid = self.event_id,
        )
    }

    fn dir_path(&self) -> String {
        format!(
            "{prefix}/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=event/",
            prefix = STORAGE_ROOT_PREFIX,
            ns = self.namespace,
            wf = self.workflow_id,
            rid = self.run_id,
        )
    }

    pub fn to_proto(&self) -> v1::EventKey {
        v1::EventKey {
            namespace: self.namespace.clone(),
            workflow_id: self.workflow_id.clone(),
            run_id: self.run_id.clone(),
            event_id: self.event_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClaimKey {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: String,
    pub claim_id: String,
}

impl ClaimKey {
    pub fn new(
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
        claim_id: impl Into<String>,
    ) -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.into(),
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
            claim_id: claim_id.into(),
        }
    }

    fn path(&self) -> String {
        format!(
            "{prefix}/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=claim/claim_id={cid}/record.binpb",
            prefix = STORAGE_ROOT_PREFIX,
            ns = self.namespace,
            wf = self.workflow_id,
            rid = self.run_id,
            cid = self.claim_id,
        )
    }

    pub fn to_proto(&self) -> v1::ClaimKey {
        v1::ClaimKey {
            namespace: self.namespace.clone(),
            workflow_id: self.workflow_id.clone(),
            run_id: self.run_id.clone(),
            claim_id: self.claim_id.clone(),
        }
    }
}

/// Generic store contract — the surface the future Rust workflow runtime
/// will depend on. For now [`OpenDALStore`] is the only implementation;
/// a ConnectStore client wrapper would slot in here cleanly.
#[allow(async_fn_in_trait)]
pub trait Store {
    async fn get_workflow(
        &self,
        key: &WorkflowKey,
    ) -> Result<Option<v1::WorkflowRecord>, StoreError>;
    async fn put_workflow(&self, record: &v1::WorkflowRecord) -> Result<(), StoreError>;
    async fn list_workflows(
        &self,
        namespace: &str,
        workflow_id: &str,
        status: v1::WorkflowStatus,
    ) -> Result<Vec<v1::WorkflowRecord>, StoreError>;
    async fn delete_workflow(&self, key: &WorkflowKey) -> Result<bool, StoreError>;

    async fn get_activity(
        &self,
        key: &ActivityKey,
    ) -> Result<Option<v1::ActivityRecord>, StoreError>;
    async fn put_activity(&self, record: &v1::ActivityRecord) -> Result<(), StoreError>;
    async fn list_activities(
        &self,
        key: &WorkflowKey,
    ) -> Result<Vec<v1::ActivityRecord>, StoreError>;
    async fn delete_activity(&self, key: &ActivityKey) -> Result<bool, StoreError>;

    async fn get_timer(&self, key: &TimerKey) -> Result<Option<v1::TimerRecord>, StoreError>;
    async fn put_timer(&self, record: &v1::TimerRecord) -> Result<(), StoreError>;
    async fn list_timers(
        &self,
        key: &WorkflowKey,
        status: v1::TimerStatus,
    ) -> Result<Vec<v1::TimerRecord>, StoreError>;
    async fn delete_timer(&self, key: &TimerKey) -> Result<bool, StoreError>;

    async fn get_event(&self, key: &EventKey) -> Result<Option<v1::EventRecord>, StoreError>;
    async fn put_event(&self, record: &v1::EventRecord) -> Result<(), StoreError>;
    async fn list_events(&self, key: &WorkflowKey) -> Result<Vec<v1::EventRecord>, StoreError>;
    async fn delete_event(&self, key: &EventKey) -> Result<bool, StoreError>;

    async fn get_claim(&self, key: &ClaimKey) -> Result<Option<v1::ClaimRecord>, StoreError>;
    async fn try_create_claim(&self, record: &v1::ClaimRecord) -> Result<bool, StoreError>;
    async fn delete_claim(&self, key: &ClaimKey) -> Result<bool, StoreError>;

    async fn sweep(
        &self,
        namespace: &str,
        now: SystemTime,
        max_age: Duration,
    ) -> Result<u32, StoreError>;

    async fn due_timers(
        &self,
        namespace: &str,
        now: SystemTime,
    ) -> Result<Vec<DueTimer>, StoreError>;
}

/// A SCHEDULED timer whose `fire_at` has passed, paired with the workflow
/// that owns it. Returned by [`Store::due_timers`] so the operator can
/// re-invoke the workflow handler.
#[derive(Debug, Clone)]
pub struct DueTimer {
    pub key: TimerKey,
    pub record: v1::TimerRecord,
    pub workflow: v1::WorkflowRecord,
}

/// OpenDAL-backed Store implementation. Holds an [`Operator`] (any
/// OpenDAL service: fs, s3, gcs, azblob, ...) and translates each
/// record-level method into get / put / list / delete on the operator.
pub struct OpenDALStore {
    op: Operator,
}

impl OpenDALStore {
    pub fn new(op: Operator) -> Self {
        Self { op }
    }
}

/// Single-read path: try to read, treat NotFound as absent. Same pattern
/// the Python SDK uses to dodge TOCTOU races between get and concurrent
/// delete (e.g. concurrency-slot churn).
async fn read_optional(
    op: &Operator,
    path: &str,
) -> Result<Option<prost::bytes::Bytes>, StoreError> {
    match op.read(path).await {
        Ok(buf) => Ok(Some(buf.to_bytes())),
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn write_bytes(op: &Operator, path: &str, data: &[u8]) -> Result<(), StoreError> {
    op.write(path, data.to_vec()).await?;
    Ok(())
}

async fn delete_if_exists(op: &Operator, path: &str) -> Result<bool, StoreError> {
    match op.stat(path).await {
        Ok(_) => {
            op.delete(path).await?;
            Ok(true)
        }
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Walk a path prefix and collect every `*.binpb` leaf the operator can see.
async fn walk_binpb(op: &Operator, root: &str) -> Result<Vec<String>, StoreError> {
    let entries = match op.list_with(root).recursive(true).await {
        Ok(es) => es,
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    Ok(entries
        .into_iter()
        .map(|e| e.path().to_string())
        .filter(|p| p.ends_with(".binpb"))
        .collect())
}

impl Store for OpenDALStore {
    async fn get_workflow(
        &self,
        key: &WorkflowKey,
    ) -> Result<Option<v1::WorkflowRecord>, StoreError> {
        let path = key.path();
        match read_optional(&self.op, &path).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(v1::WorkflowRecord::decode(bytes)?)),
        }
    }

    async fn put_workflow(&self, record: &v1::WorkflowRecord) -> Result<(), StoreError> {
        let key = workflow_key_from_proto(&record.key.clone().unwrap_or_default());
        write_bytes(&self.op, &key.path(), &record.encode_to_vec()).await
    }

    async fn list_workflows(
        &self,
        namespace: &str,
        workflow_id: &str,
        status: v1::WorkflowStatus,
    ) -> Result<Vec<v1::WorkflowRecord>, StoreError> {
        let root = if namespace.is_empty() {
            format!("{STORAGE_ROOT_PREFIX}/")
        } else if workflow_id.is_empty() {
            format!("{STORAGE_ROOT_PREFIX}/namespace={namespace}/")
        } else {
            format!("{STORAGE_ROOT_PREFIX}/namespace={namespace}/workflow_id={workflow_id}/")
        };
        let paths = walk_binpb(&self.op, &root).await?;
        let match_workflow_id = if namespace.is_empty() && !workflow_id.is_empty() {
            Some(workflow_id)
        } else {
            None
        };
        let mut out = Vec::new();
        for path in paths {
            if !path.ends_with("/kind=workflow/record.binpb") {
                continue;
            }
            if let Some(parsed) = parse_workflow_path(&path) {
                if let Some(want) = match_workflow_id {
                    if parsed.workflow_id != want {
                        continue;
                    }
                }
                if let Some(record) = self.get_workflow(&parsed).await? {
                    if status == v1::WorkflowStatus::Unspecified || record.status == status as i32 {
                        out.push(record);
                    }
                }
            }
        }
        Ok(out)
    }

    async fn delete_workflow(&self, key: &WorkflowKey) -> Result<bool, StoreError> {
        delete_if_exists(&self.op, &key.path()).await
    }

    async fn get_activity(
        &self,
        key: &ActivityKey,
    ) -> Result<Option<v1::ActivityRecord>, StoreError> {
        let path = key.path();
        match read_optional(&self.op, &path).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(v1::ActivityRecord::decode(bytes)?)),
        }
    }

    async fn put_activity(&self, record: &v1::ActivityRecord) -> Result<(), StoreError> {
        let key = activity_key_from_proto(&record.key.clone().unwrap_or_default());
        write_bytes(&self.op, &key.path(), &record.encode_to_vec()).await
    }

    async fn list_activities(
        &self,
        key: &WorkflowKey,
    ) -> Result<Vec<v1::ActivityRecord>, StoreError> {
        // Build a sample ActivityKey just for the dir_path.
        let dir = ActivityKey {
            namespace: key.namespace.clone(),
            workflow_id: key.workflow_id.clone(),
            run_id: key.run_id.clone(),
            activity_id: "placeholder".into(),
        }
        .dir_path();
        let paths = walk_binpb(&self.op, &dir).await?;
        let mut out = Vec::new();
        for path in paths {
            if let Some(activity_id) = extract_id_from_hive_path(&path, &dir, "activity_id=") {
                let k = ActivityKey {
                    namespace: key.namespace.clone(),
                    workflow_id: key.workflow_id.clone(),
                    run_id: key.run_id.clone(),
                    activity_id,
                };
                if let Some(record) = self.get_activity(&k).await? {
                    out.push(record);
                }
            }
        }
        Ok(out)
    }

    async fn delete_activity(&self, key: &ActivityKey) -> Result<bool, StoreError> {
        delete_if_exists(&self.op, &key.path()).await
    }

    async fn get_timer(&self, key: &TimerKey) -> Result<Option<v1::TimerRecord>, StoreError> {
        match read_optional(&self.op, &key.path()).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(v1::TimerRecord::decode(bytes)?)),
        }
    }

    async fn put_timer(&self, record: &v1::TimerRecord) -> Result<(), StoreError> {
        let key = timer_key_from_proto(&record.key.clone().unwrap_or_default());
        write_bytes(&self.op, &key.path(), &record.encode_to_vec()).await
    }

    async fn list_timers(
        &self,
        key: &WorkflowKey,
        status: v1::TimerStatus,
    ) -> Result<Vec<v1::TimerRecord>, StoreError> {
        let dir = TimerKey {
            namespace: key.namespace.clone(),
            workflow_id: key.workflow_id.clone(),
            run_id: key.run_id.clone(),
            timer_id: "placeholder".into(),
        }
        .dir_path();
        let paths = walk_binpb(&self.op, &dir).await?;
        let mut out = Vec::new();
        for path in paths {
            if let Some(timer_id) = extract_id_from_hive_path(&path, &dir, "timer_id=") {
                let k = TimerKey {
                    namespace: key.namespace.clone(),
                    workflow_id: key.workflow_id.clone(),
                    run_id: key.run_id.clone(),
                    timer_id,
                };
                if let Some(record) = self.get_timer(&k).await? {
                    if status == v1::TimerStatus::Unspecified || record.status == status as i32 {
                        out.push(record);
                    }
                }
            }
        }
        Ok(out)
    }

    async fn delete_timer(&self, key: &TimerKey) -> Result<bool, StoreError> {
        delete_if_exists(&self.op, &key.path()).await
    }

    async fn get_event(&self, key: &EventKey) -> Result<Option<v1::EventRecord>, StoreError> {
        match read_optional(&self.op, &key.path()).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(v1::EventRecord::decode(bytes)?)),
        }
    }

    async fn put_event(&self, record: &v1::EventRecord) -> Result<(), StoreError> {
        let key = event_key_from_proto(&record.key.clone().unwrap_or_default());
        write_bytes(&self.op, &key.path(), &record.encode_to_vec()).await
    }

    async fn list_events(&self, key: &WorkflowKey) -> Result<Vec<v1::EventRecord>, StoreError> {
        let dir = EventKey {
            namespace: key.namespace.clone(),
            workflow_id: key.workflow_id.clone(),
            run_id: key.run_id.clone(),
            event_id: "placeholder".into(),
        }
        .dir_path();
        let paths = walk_binpb(&self.op, &dir).await?;
        let mut out = Vec::new();
        for path in paths {
            if let Some(event_id) = extract_id_from_hive_path(&path, &dir, "event_id=") {
                let k = EventKey {
                    namespace: key.namespace.clone(),
                    workflow_id: key.workflow_id.clone(),
                    run_id: key.run_id.clone(),
                    event_id,
                };
                if let Some(record) = self.get_event(&k).await? {
                    out.push(record);
                }
            }
        }
        Ok(out)
    }

    async fn delete_event(&self, key: &EventKey) -> Result<bool, StoreError> {
        delete_if_exists(&self.op, &key.path()).await
    }

    async fn get_claim(&self, key: &ClaimKey) -> Result<Option<v1::ClaimRecord>, StoreError> {
        match read_optional(&self.op, &key.path()).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(v1::ClaimRecord::decode(bytes)?)),
        }
    }

    async fn try_create_claim(&self, record: &v1::ClaimRecord) -> Result<bool, StoreError> {
        let key = claim_key_from_proto(&record.key.clone().unwrap_or_default());
        let path = key.path();
        // Best-effort create-only: check existence, then write. The
        // OpenDAL `fs` backend doesn't expose conditional writes via the
        // stable `write` API; for production multi-process atomicity, S3 /
        // GCS native preconditions are needed (same caveat as Go's
        // OpenDAL store — see docs/hard-cases.md).
        match self.op.stat(&path).await {
            Ok(_) => Ok(false),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                write_bytes(&self.op, &path, &record.encode_to_vec()).await?;
                Ok(true)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn delete_claim(&self, key: &ClaimKey) -> Result<bool, StoreError> {
        delete_if_exists(&self.op, &key.path()).await
    }

    async fn sweep(
        &self,
        namespace: &str,
        now: SystemTime,
        max_age: Duration,
    ) -> Result<u32, StoreError> {
        if max_age.is_zero() {
            return Err(StoreError::InvalidKey("max_age must be > 0".into()));
        }
        let cutoff = now - max_age;
        let completed = self
            .list_workflows(namespace, "", v1::WorkflowStatus::Completed)
            .await?;
        let mut deleted = 0u32;
        for record in completed {
            let completed_at = match &record.completed_at {
                Some(ts) => system_time_from_proto(ts),
                None => continue,
            };
            if completed_at > cutoff {
                continue;
            }
            let key = workflow_key_from_proto(&record.key.clone().unwrap_or_default());

            // Sweep children first so the parent's existence reflects "still
            // partly there" until everything under it is gone.
            for a in self.list_activities(&key).await? {
                let ak = activity_key_from_proto(&a.key.unwrap_or_default());
                self.delete_activity(&ak).await?;
            }
            for t in self.list_timers(&key, v1::TimerStatus::Unspecified).await? {
                let tk = timer_key_from_proto(&t.key.unwrap_or_default());
                self.delete_timer(&tk).await?;
            }
            for e in self.list_events(&key).await? {
                let ek = event_key_from_proto(&e.key.unwrap_or_default());
                self.delete_event(&ek).await?;
            }
            self.delete_workflow(&key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    async fn due_timers(
        &self,
        namespace: &str,
        now: SystemTime,
    ) -> Result<Vec<DueTimer>, StoreError> {
        let in_flight = self
            .list_workflows(namespace, "", v1::WorkflowStatus::InProgress)
            .await?;
        let mut out = Vec::new();
        for workflow in in_flight {
            let key = workflow_key_from_proto(&workflow.key.clone().unwrap_or_default());
            let timers = self.list_timers(&key, v1::TimerStatus::Scheduled).await?;
            for timer in timers {
                if let Some(ts) = &timer.fire_at {
                    if system_time_from_proto(ts) > now {
                        continue;
                    }
                    let tk = timer_key_from_proto(&timer.key.clone().unwrap_or_default());
                    out.push(DueTimer {
                        key: tk,
                        record: timer,
                        workflow: workflow.clone(),
                    });
                }
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Path parsing + proto key conversions. Same conventions as Go's
// `parseWorkflowPath` and Python's `_parse_workflow_path`.
// ---------------------------------------------------------------------------

fn parse_workflow_path(path: &str) -> Option<WorkflowKey> {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() != 7 {
        return None;
    }
    if parts[0] != "temporaless" || parts[1] != "v1" {
        return None;
    }
    if parts[5] != "kind=workflow" || parts[6] != "record.binpb" {
        return None;
    }
    let namespace = parts[2].strip_prefix("namespace=")?.to_string();
    let workflow_id = parts[3].strip_prefix("workflow_id=")?.to_string();
    let run_id = parts[4].strip_prefix("run_id=")?.to_string();
    Some(WorkflowKey {
        namespace,
        workflow_id,
        run_id,
    })
}

fn extract_id_from_hive_path(path: &str, dir: &str, id_prefix: &str) -> Option<String> {
    let rel = path.strip_prefix(dir)?;
    let inner = rel.strip_suffix("/record.binpb")?;
    Some(inner.strip_prefix(id_prefix)?.to_string())
}

fn workflow_key_from_proto(k: &v1::WorkflowKey) -> WorkflowKey {
    WorkflowKey {
        namespace: if k.namespace.is_empty() {
            DEFAULT_NAMESPACE.into()
        } else {
            k.namespace.clone()
        },
        workflow_id: k.workflow_id.clone(),
        run_id: k.run_id.clone(),
    }
}

fn activity_key_from_proto(k: &v1::ActivityKey) -> ActivityKey {
    ActivityKey {
        namespace: if k.namespace.is_empty() {
            DEFAULT_NAMESPACE.into()
        } else {
            k.namespace.clone()
        },
        workflow_id: k.workflow_id.clone(),
        run_id: k.run_id.clone(),
        activity_id: k.activity_id.clone(),
    }
}

fn timer_key_from_proto(k: &v1::TimerKey) -> TimerKey {
    TimerKey {
        namespace: if k.namespace.is_empty() {
            DEFAULT_NAMESPACE.into()
        } else {
            k.namespace.clone()
        },
        workflow_id: k.workflow_id.clone(),
        run_id: k.run_id.clone(),
        timer_id: k.timer_id.clone(),
    }
}

fn event_key_from_proto(k: &v1::EventKey) -> EventKey {
    EventKey {
        namespace: if k.namespace.is_empty() {
            DEFAULT_NAMESPACE.into()
        } else {
            k.namespace.clone()
        },
        workflow_id: k.workflow_id.clone(),
        run_id: k.run_id.clone(),
        event_id: k.event_id.clone(),
    }
}

fn claim_key_from_proto(k: &v1::ClaimKey) -> ClaimKey {
    ClaimKey {
        namespace: if k.namespace.is_empty() {
            DEFAULT_NAMESPACE.into()
        } else {
            k.namespace.clone()
        },
        workflow_id: k.workflow_id.clone(),
        run_id: k.run_id.clone(),
        claim_id: k.claim_id.clone(),
    }
}

fn system_time_from_proto(ts: &prost_types::Timestamp) -> SystemTime {
    if ts.seconds >= 0 {
        UNIX_EPOCH + Duration::new(ts.seconds as u64, ts.nanos.max(0) as u32)
    } else {
        UNIX_EPOCH - Duration::new((-ts.seconds) as u64, 0)
    }
}

/// Helper: build a `prost_types::Timestamp` from a `SystemTime`. Exposed so
/// tests and benchmarks can construct records without depending on prost
/// directly.
pub fn proto_timestamp(t: SystemTime) -> prost_types::Timestamp {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    prost_types::Timestamp {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}
