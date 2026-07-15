//! OpenDAL-backed storage for temporaless records (Rust SDK).
//!
//! Uses the same flat v2 paths, protobuf records, and on-disk bytes as Go and
//! Python, so Rust can read their canonical records and its workflow/activity
//! records are readable by the first-class runtimes. Rust workflow writes do
//! not yet update the `_latest` pointer, its timer writes do not maintain the
//! required `_due` write-ahead record, and its claim writes do not implement
//! create-if-absent coordination. Do not treat these low-level point writers as
//! substitutes for the first-class derived/conditional storage boundaries.
//!
//! Paths are constructed from validated protobuf key fields. Runtime code
//! never parses identity back out of an object path; list operations fetch
//! each protobuf payload and validate its embedded key and schema.
//!
//! ```text
//! temporaless/v2/{namespace}/{workflow_id}/{run_id}/workflow.binpb
//! temporaless/v2/{namespace}/{workflow_id}/{run_id}/activity/{activity_id}.binpb
//! temporaless/v2/{namespace}/{workflow_id}/{run_id}/timer/{timer_id}.binpb
//! temporaless/v2/{namespace}/{workflow_id}/{run_id}/event/{event_id}.binpb
//! temporaless/v2/{namespace}/{workflow_id}/{run_id}/claim/{claim_id}.binpb
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opendal::Operator;
use prost::Message;
use thiserror::Error;

use crate::{reserved_names, v1};

pub const DEFAULT_NAMESPACE: &str = "default";
pub const STORAGE_ROOT_PREFIX: &str = "temporaless/v2";

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
    #[error("corrupt storage record: {0}")]
    CorruptRecord(String),
}

fn validate_component(
    name: &str,
    value: &str,
    reserve_system_prefix: bool,
) -> Result<(), StoreError> {
    if value.is_empty() {
        return Err(StoreError::InvalidKey(format!("{name} must not be empty")));
    }
    if value == "." || value == ".." {
        return Err(StoreError::InvalidKey(format!(
            "{name} must not be {value}"
        )));
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'=')
    }) {
        return Err(StoreError::InvalidKey(format!(
            "{name} must contain only ASCII letters, numbers, '.', '_', '-', ':', or '='"
        )));
    }
    if reserve_system_prefix && value.starts_with('_') {
        return Err(StoreError::InvalidKey(format!(
            "{name} values beginning with '_' are reserved"
        )));
    }
    Ok(())
}

fn normalized_namespace(namespace: &str) -> &str {
    if namespace.is_empty() {
        DEFAULT_NAMESPACE
    } else {
        namespace
    }
}

fn run_prefix(namespace: &str, workflow_id: &str, run_id: &str) -> String {
    format!("{STORAGE_ROOT_PREFIX}/{namespace}/{workflow_id}/{run_id}")
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

    fn validate(&self) -> Result<(), StoreError> {
        validate_component("namespace", normalized_namespace(&self.namespace), true)?;
        validate_component("workflow_id", &self.workflow_id, true)?;
        validate_component("run_id", &self.run_id, false)
    }

    fn path(&self) -> Result<String, StoreError> {
        self.validate()?;
        Ok(format!(
            "{}/workflow.binpb",
            run_prefix(
                normalized_namespace(&self.namespace),
                &self.workflow_id,
                &self.run_id
            )
        ))
    }

    pub fn to_proto(&self) -> v1::WorkflowKey {
        v1::WorkflowKey {
            namespace: normalized_namespace(&self.namespace).to_string(),
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

    fn validate(&self) -> Result<(), StoreError> {
        validate_component("namespace", normalized_namespace(&self.namespace), true)?;
        validate_component("workflow_id", &self.workflow_id, true)?;
        validate_component("run_id", &self.run_id, false)?;
        validate_component("activity_id", &self.activity_id, false)
    }

    fn path(&self) -> Result<String, StoreError> {
        self.validate()?;
        Ok(format!(
            "{}/activity/{}.binpb",
            run_prefix(
                normalized_namespace(&self.namespace),
                &self.workflow_id,
                &self.run_id
            ),
            self.activity_id
        ))
    }

    fn dir_path(&self) -> Result<String, StoreError> {
        self.validate()?;
        Ok(format!(
            "{}/activity/",
            run_prefix(
                normalized_namespace(&self.namespace),
                &self.workflow_id,
                &self.run_id
            )
        ))
    }

    pub fn to_proto(&self) -> v1::ActivityKey {
        v1::ActivityKey {
            namespace: normalized_namespace(&self.namespace).to_string(),
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

    fn validate(&self) -> Result<(), StoreError> {
        validate_component("namespace", normalized_namespace(&self.namespace), true)?;
        validate_component("workflow_id", &self.workflow_id, true)?;
        validate_component("run_id", &self.run_id, false)?;
        validate_component("timer_id", &self.timer_id, false)
    }

    fn path(&self) -> Result<String, StoreError> {
        self.validate()?;
        Ok(format!(
            "{}/timer/{}.binpb",
            run_prefix(
                normalized_namespace(&self.namespace),
                &self.workflow_id,
                &self.run_id
            ),
            self.timer_id
        ))
    }

    fn dir_path(&self) -> Result<String, StoreError> {
        self.validate()?;
        Ok(format!(
            "{}/timer/",
            run_prefix(
                normalized_namespace(&self.namespace),
                &self.workflow_id,
                &self.run_id
            )
        ))
    }

    pub fn to_proto(&self) -> v1::TimerKey {
        v1::TimerKey {
            namespace: normalized_namespace(&self.namespace).to_string(),
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

    fn validate(&self) -> Result<(), StoreError> {
        validate_component("namespace", normalized_namespace(&self.namespace), true)?;
        validate_component("workflow_id", &self.workflow_id, true)?;
        validate_component("run_id", &self.run_id, false)?;
        validate_component("event_id", &self.event_id, false)
    }

    fn path(&self) -> Result<String, StoreError> {
        self.validate()?;
        Ok(format!(
            "{}/event/{}.binpb",
            run_prefix(
                normalized_namespace(&self.namespace),
                &self.workflow_id,
                &self.run_id
            ),
            self.event_id
        ))
    }

    fn dir_path(&self) -> Result<String, StoreError> {
        self.validate()?;
        Ok(format!(
            "{}/event/",
            run_prefix(
                normalized_namespace(&self.namespace),
                &self.workflow_id,
                &self.run_id
            )
        ))
    }

    pub fn to_proto(&self) -> v1::EventKey {
        v1::EventKey {
            namespace: normalized_namespace(&self.namespace).to_string(),
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

    fn validate(&self) -> Result<(), StoreError> {
        validate_component("namespace", normalized_namespace(&self.namespace), true)?;
        validate_component("workflow_id", &self.workflow_id, false)?;
        if self.workflow_id.starts_with('_')
            && self.workflow_id != reserved_names::CONCURRENCY_WORKFLOW_ID
        {
            return Err(StoreError::InvalidKey(
                "workflow_id values beginning with '_' are reserved".into(),
            ));
        }
        validate_component("run_id", &self.run_id, false)?;
        validate_component("claim_id", &self.claim_id, false)
    }

    fn path(&self) -> Result<String, StoreError> {
        self.validate()?;
        Ok(format!(
            "{}/claim/{}.binpb",
            run_prefix(
                normalized_namespace(&self.namespace),
                &self.workflow_id,
                &self.run_id
            ),
            self.claim_id
        ))
    }

    fn dir_path(&self) -> Result<String, StoreError> {
        self.validate()?;
        Ok(format!(
            "{}/claim/",
            run_prefix(
                normalized_namespace(&self.namespace),
                &self.workflow_id,
                &self.run_id
            )
        ))
    }

    pub fn to_proto(&self) -> v1::ClaimKey {
        v1::ClaimKey {
            namespace: normalized_namespace(&self.namespace).to_string(),
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

    async fn list_claims_for_run(
        &self,
        key: &WorkflowKey,
    ) -> Result<Vec<v1::ClaimRecord>, StoreError> {
        key.validate()?;
        let dir = ClaimKey {
            namespace: key.namespace.clone(),
            workflow_id: key.workflow_id.clone(),
            run_id: key.run_id.clone(),
            claim_id: "placeholder".into(),
        }
        .dir_path()?;
        let paths = walk_binpb(&self.op, &dir).await?;
        let mut out = Vec::new();
        for path in paths {
            let Some(bytes) = read_optional(&self.op, &path).await? else {
                continue;
            };
            let record = v1::ClaimRecord::decode(bytes)
                .map_err(|error| corrupt_decode("claim", &path, error))?;
            let actual = claim_record_key(&record)?;
            let actual_run = WorkflowKey {
                namespace: actual.namespace.clone(),
                workflow_id: actual.workflow_id.clone(),
                run_id: actual.run_id.clone(),
            };
            ensure_workflow_key("claim", &actual_run, key)?;
            let expected_path = actual.path().map_err(|error| corrupt_key("claim", error))?;
            ensure_path("claim", expected_path, &path)?;
            out.push(record);
        }
        Ok(out)
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
    let mut paths: Vec<String> = entries
        .into_iter()
        .map(|e| e.path().to_string())
        .filter(|p| p.ends_with(".binpb"))
        .collect();
    paths.sort();
    Ok(paths)
}

fn is_workflow_object(path: &str) -> bool {
    path.starts_with(&format!("{STORAGE_ROOT_PREFIX}/"))
        && path.ends_with("/workflow.binpb")
        // The three variable segments are namespace, workflow_id, and run_id.
        // This recognizes the record kind without reading any segment as
        // identity; the decoded protobuf key remains authoritative.
        && path.split('/').count() == 6
}

fn corrupt_key(record_kind: &str, error: StoreError) -> StoreError {
    StoreError::CorruptRecord(format!("{record_kind} payload has invalid key: {error}"))
}

fn corrupt_decode(record_kind: &str, path: &str, error: prost::DecodeError) -> StoreError {
    StoreError::CorruptRecord(format!("decode {record_kind} payload at {path}: {error}"))
}

fn ensure_path(record_kind: &str, actual: String, expected: &str) -> Result<(), StoreError> {
    if actual != expected {
        return Err(StoreError::CorruptRecord(format!(
            "{record_kind} payload key does not match its object location"
        )));
    }
    Ok(())
}

fn ensure_workflow_key(
    record_kind: &str,
    actual: &WorkflowKey,
    expected: &WorkflowKey,
) -> Result<(), StoreError> {
    if normalized_namespace(&actual.namespace) != normalized_namespace(&expected.namespace)
        || actual.workflow_id != expected.workflow_id
        || actual.run_id != expected.run_id
    {
        return Err(StoreError::CorruptRecord(format!(
            "{record_kind} payload key does not match requested key"
        )));
    }
    Ok(())
}

fn ensure_activity_key(actual: &ActivityKey, expected: &ActivityKey) -> Result<(), StoreError> {
    if normalized_namespace(&actual.namespace) != normalized_namespace(&expected.namespace)
        || actual.workflow_id != expected.workflow_id
        || actual.run_id != expected.run_id
        || actual.activity_id != expected.activity_id
    {
        return Err(StoreError::CorruptRecord(
            "activity payload key does not match requested key".into(),
        ));
    }
    Ok(())
}

fn ensure_timer_key(actual: &TimerKey, expected: &TimerKey) -> Result<(), StoreError> {
    if normalized_namespace(&actual.namespace) != normalized_namespace(&expected.namespace)
        || actual.workflow_id != expected.workflow_id
        || actual.run_id != expected.run_id
        || actual.timer_id != expected.timer_id
    {
        return Err(StoreError::CorruptRecord(
            "timer payload key does not match requested key".into(),
        ));
    }
    Ok(())
}

fn ensure_event_key(actual: &EventKey, expected: &EventKey) -> Result<(), StoreError> {
    if normalized_namespace(&actual.namespace) != normalized_namespace(&expected.namespace)
        || actual.workflow_id != expected.workflow_id
        || actual.run_id != expected.run_id
        || actual.event_id != expected.event_id
    {
        return Err(StoreError::CorruptRecord(
            "event payload key does not match requested key".into(),
        ));
    }
    Ok(())
}

fn ensure_claim_key(actual: &ClaimKey, expected: &ClaimKey) -> Result<(), StoreError> {
    if normalized_namespace(&actual.namespace) != normalized_namespace(&expected.namespace)
        || actual.workflow_id != expected.workflow_id
        || actual.run_id != expected.run_id
        || actual.claim_id != expected.claim_id
    {
        return Err(StoreError::CorruptRecord(
            "claim payload key does not match requested key".into(),
        ));
    }
    Ok(())
}

fn workflow_record_key(record: &v1::WorkflowRecord) -> Result<WorkflowKey, StoreError> {
    if record.schema_version != v1::RecordSchemaVersion::Workflow as i32 {
        return Err(StoreError::CorruptRecord(format!(
            "workflow payload has schema_version {}, want {}",
            record.schema_version,
            v1::RecordSchemaVersion::Workflow as i32
        )));
    }
    let proto = record
        .key
        .as_ref()
        .ok_or_else(|| StoreError::CorruptRecord("workflow payload key is required".into()))?;
    let key = workflow_key_from_proto(proto);
    key.validate()
        .map_err(|error| corrupt_key("workflow", error))?;
    Ok(key)
}

fn activity_record_key(record: &v1::ActivityRecord) -> Result<ActivityKey, StoreError> {
    if record.schema_version != v1::RecordSchemaVersion::Activity as i32 {
        return Err(StoreError::CorruptRecord(format!(
            "activity payload has schema_version {}, want {}",
            record.schema_version,
            v1::RecordSchemaVersion::Activity as i32
        )));
    }
    let proto = record
        .key
        .as_ref()
        .ok_or_else(|| StoreError::CorruptRecord("activity payload key is required".into()))?;
    let key = activity_key_from_proto(proto);
    key.validate()
        .map_err(|error| corrupt_key("activity", error))?;
    Ok(key)
}

fn timer_record_key(record: &v1::TimerRecord) -> Result<TimerKey, StoreError> {
    if record.schema_version != v1::RecordSchemaVersion::Timer as i32 {
        return Err(StoreError::CorruptRecord(format!(
            "timer payload has schema_version {}, want {}",
            record.schema_version,
            v1::RecordSchemaVersion::Timer as i32
        )));
    }
    let proto = record
        .key
        .as_ref()
        .ok_or_else(|| StoreError::CorruptRecord("timer payload key is required".into()))?;
    let key = timer_key_from_proto(proto);
    key.validate()
        .map_err(|error| corrupt_key("timer", error))?;
    Ok(key)
}

fn event_record_key(record: &v1::EventRecord) -> Result<EventKey, StoreError> {
    if record.schema_version != v1::RecordSchemaVersion::Event as i32 {
        return Err(StoreError::CorruptRecord(format!(
            "event payload has schema_version {}, want {}",
            record.schema_version,
            v1::RecordSchemaVersion::Event as i32
        )));
    }
    let proto = record
        .key
        .as_ref()
        .ok_or_else(|| StoreError::CorruptRecord("event payload key is required".into()))?;
    let key = event_key_from_proto(proto);
    key.validate()
        .map_err(|error| corrupt_key("event", error))?;
    Ok(key)
}

fn claim_record_key(record: &v1::ClaimRecord) -> Result<ClaimKey, StoreError> {
    if record.schema_version != v1::RecordSchemaVersion::Claim as i32 {
        return Err(StoreError::CorruptRecord(format!(
            "claim payload has schema_version {}, want {}",
            record.schema_version,
            v1::RecordSchemaVersion::Claim as i32
        )));
    }
    let proto = record
        .key
        .as_ref()
        .ok_or_else(|| StoreError::CorruptRecord("claim payload key is required".into()))?;
    let key = claim_key_from_proto(proto);
    key.validate()
        .map_err(|error| corrupt_key("claim", error))?;
    Ok(key)
}

impl Store for OpenDALStore {
    async fn get_workflow(
        &self,
        key: &WorkflowKey,
    ) -> Result<Option<v1::WorkflowRecord>, StoreError> {
        let path = key.path()?;
        match read_optional(&self.op, &path).await? {
            None => Ok(None),
            Some(bytes) => {
                let record = v1::WorkflowRecord::decode(bytes)
                    .map_err(|error| corrupt_decode("workflow", &path, error))?;
                let actual = workflow_record_key(&record)?;
                ensure_workflow_key("workflow", &actual, key)?;
                Ok(Some(record))
            }
        }
    }

    async fn put_workflow(&self, record: &v1::WorkflowRecord) -> Result<(), StoreError> {
        let key = workflow_record_key(record)?;
        write_bytes(&self.op, &key.path()?, &record.encode_to_vec()).await
    }

    async fn list_workflows(
        &self,
        namespace: &str,
        workflow_id: &str,
        status: v1::WorkflowStatus,
    ) -> Result<Vec<v1::WorkflowRecord>, StoreError> {
        if !namespace.is_empty() {
            validate_component("namespace", namespace, true)?;
        }
        if !workflow_id.is_empty() {
            validate_component("workflow_id", workflow_id, true)?;
        }
        let root = match (namespace.is_empty(), workflow_id.is_empty()) {
            (true, _) => format!("{STORAGE_ROOT_PREFIX}/"),
            (false, true) => format!("{STORAGE_ROOT_PREFIX}/{namespace}/"),
            (false, false) => format!("{STORAGE_ROOT_PREFIX}/{namespace}/{workflow_id}/"),
        };
        let paths = walk_binpb(&self.op, &root).await?;
        let mut out = Vec::new();
        for path in paths {
            if !is_workflow_object(&path) {
                continue;
            }
            let Some(bytes) = read_optional(&self.op, &path).await? else {
                continue;
            };
            let record = v1::WorkflowRecord::decode(bytes)
                .map_err(|error| corrupt_decode("workflow", &path, error))?;
            let key = workflow_record_key(&record)?;
            let expected_path = key.path().map_err(|error| corrupt_key("workflow", error))?;
            ensure_path("workflow", expected_path, &path)?;
            if (!namespace.is_empty() && normalized_namespace(&key.namespace) != namespace)
                || (!workflow_id.is_empty() && key.workflow_id != workflow_id)
            {
                continue;
            }
            if status == v1::WorkflowStatus::Unspecified || record.status == status as i32 {
                out.push(record);
            }
        }
        Ok(out)
    }

    async fn delete_workflow(&self, key: &WorkflowKey) -> Result<bool, StoreError> {
        delete_if_exists(&self.op, &key.path()?).await
    }

    async fn get_activity(
        &self,
        key: &ActivityKey,
    ) -> Result<Option<v1::ActivityRecord>, StoreError> {
        let path = key.path()?;
        match read_optional(&self.op, &path).await? {
            None => Ok(None),
            Some(bytes) => {
                let record = v1::ActivityRecord::decode(bytes)
                    .map_err(|error| corrupt_decode("activity", &path, error))?;
                let actual = activity_record_key(&record)?;
                ensure_activity_key(&actual, key)?;
                Ok(Some(record))
            }
        }
    }

    async fn put_activity(&self, record: &v1::ActivityRecord) -> Result<(), StoreError> {
        let key = activity_record_key(record)?;
        write_bytes(&self.op, &key.path()?, &record.encode_to_vec()).await
    }

    async fn list_activities(
        &self,
        key: &WorkflowKey,
    ) -> Result<Vec<v1::ActivityRecord>, StoreError> {
        key.validate()?;
        let dir = ActivityKey {
            namespace: key.namespace.clone(),
            workflow_id: key.workflow_id.clone(),
            run_id: key.run_id.clone(),
            activity_id: "placeholder".into(),
        }
        .dir_path()?;
        let paths = walk_binpb(&self.op, &dir).await?;
        let mut out = Vec::new();
        for path in paths {
            let Some(bytes) = read_optional(&self.op, &path).await? else {
                continue;
            };
            let record = v1::ActivityRecord::decode(bytes)
                .map_err(|error| corrupt_decode("activity", &path, error))?;
            let actual = activity_record_key(&record)?;
            let actual_run = WorkflowKey {
                namespace: actual.namespace.clone(),
                workflow_id: actual.workflow_id.clone(),
                run_id: actual.run_id.clone(),
            };
            ensure_workflow_key("activity", &actual_run, key)?;
            let expected_path = actual
                .path()
                .map_err(|error| corrupt_key("activity", error))?;
            ensure_path("activity", expected_path, &path)?;
            out.push(record);
        }
        Ok(out)
    }

    async fn delete_activity(&self, key: &ActivityKey) -> Result<bool, StoreError> {
        delete_if_exists(&self.op, &key.path()?).await
    }

    async fn get_timer(&self, key: &TimerKey) -> Result<Option<v1::TimerRecord>, StoreError> {
        let path = key.path()?;
        match read_optional(&self.op, &path).await? {
            None => Ok(None),
            Some(bytes) => {
                let record = v1::TimerRecord::decode(bytes)
                    .map_err(|error| corrupt_decode("timer", &path, error))?;
                let actual = timer_record_key(&record)?;
                ensure_timer_key(&actual, key)?;
                Ok(Some(record))
            }
        }
    }

    async fn put_timer(&self, record: &v1::TimerRecord) -> Result<(), StoreError> {
        let key = timer_record_key(record)?;
        write_bytes(&self.op, &key.path()?, &record.encode_to_vec()).await
    }

    async fn list_timers(
        &self,
        key: &WorkflowKey,
        status: v1::TimerStatus,
    ) -> Result<Vec<v1::TimerRecord>, StoreError> {
        key.validate()?;
        let dir = TimerKey {
            namespace: key.namespace.clone(),
            workflow_id: key.workflow_id.clone(),
            run_id: key.run_id.clone(),
            timer_id: "placeholder".into(),
        }
        .dir_path()?;
        let paths = walk_binpb(&self.op, &dir).await?;
        let mut out = Vec::new();
        for path in paths {
            let Some(bytes) = read_optional(&self.op, &path).await? else {
                continue;
            };
            let record = v1::TimerRecord::decode(bytes)
                .map_err(|error| corrupt_decode("timer", &path, error))?;
            let actual = timer_record_key(&record)?;
            let actual_run = WorkflowKey {
                namespace: actual.namespace.clone(),
                workflow_id: actual.workflow_id.clone(),
                run_id: actual.run_id.clone(),
            };
            ensure_workflow_key("timer", &actual_run, key)?;
            let expected_path = actual.path().map_err(|error| corrupt_key("timer", error))?;
            ensure_path("timer", expected_path, &path)?;
            if status == v1::TimerStatus::Unspecified || record.status == status as i32 {
                out.push(record);
            }
        }
        Ok(out)
    }

    async fn delete_timer(&self, key: &TimerKey) -> Result<bool, StoreError> {
        delete_if_exists(&self.op, &key.path()?).await
    }

    async fn get_event(&self, key: &EventKey) -> Result<Option<v1::EventRecord>, StoreError> {
        let path = key.path()?;
        match read_optional(&self.op, &path).await? {
            None => Ok(None),
            Some(bytes) => {
                let record = v1::EventRecord::decode(bytes)
                    .map_err(|error| corrupt_decode("event", &path, error))?;
                let actual = event_record_key(&record)?;
                ensure_event_key(&actual, key)?;
                Ok(Some(record))
            }
        }
    }

    async fn put_event(&self, record: &v1::EventRecord) -> Result<(), StoreError> {
        let key = event_record_key(record)?;
        write_bytes(&self.op, &key.path()?, &record.encode_to_vec()).await
    }

    async fn list_events(&self, key: &WorkflowKey) -> Result<Vec<v1::EventRecord>, StoreError> {
        key.validate()?;
        let dir = EventKey {
            namespace: key.namespace.clone(),
            workflow_id: key.workflow_id.clone(),
            run_id: key.run_id.clone(),
            event_id: "placeholder".into(),
        }
        .dir_path()?;
        let paths = walk_binpb(&self.op, &dir).await?;
        let mut out = Vec::new();
        for path in paths {
            let Some(bytes) = read_optional(&self.op, &path).await? else {
                continue;
            };
            let record = v1::EventRecord::decode(bytes)
                .map_err(|error| corrupt_decode("event", &path, error))?;
            let actual = event_record_key(&record)?;
            let actual_run = WorkflowKey {
                namespace: actual.namespace.clone(),
                workflow_id: actual.workflow_id.clone(),
                run_id: actual.run_id.clone(),
            };
            ensure_workflow_key("event", &actual_run, key)?;
            let expected_path = actual.path().map_err(|error| corrupt_key("event", error))?;
            ensure_path("event", expected_path, &path)?;
            out.push(record);
        }
        Ok(out)
    }

    async fn delete_event(&self, key: &EventKey) -> Result<bool, StoreError> {
        delete_if_exists(&self.op, &key.path()?).await
    }

    async fn get_claim(&self, key: &ClaimKey) -> Result<Option<v1::ClaimRecord>, StoreError> {
        let path = key.path()?;
        match read_optional(&self.op, &path).await? {
            None => Ok(None),
            Some(bytes) => {
                let record = v1::ClaimRecord::decode(bytes)
                    .map_err(|error| corrupt_decode("claim", &path, error))?;
                let actual = claim_record_key(&record)?;
                ensure_claim_key(&actual, key)?;
                Ok(Some(record))
            }
        }
    }

    async fn try_create_claim(&self, record: &v1::ClaimRecord) -> Result<bool, StoreError> {
        let key = claim_record_key(record)?;
        let path = key.path()?;
        match self
            .op
            .write_with(&path, record.encode_to_vec())
            .if_not_exists(true)
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if matches!(
                    error.kind(),
                    opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::ConditionNotMatch
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn delete_claim(&self, key: &ClaimKey) -> Result<bool, StoreError> {
        delete_if_exists(&self.op, &key.path()?).await
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
        let cutoff = now
            .checked_sub(max_age)
            .ok_or_else(|| StoreError::InvalidKey("max_age precedes the system epoch".into()))?;
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
            let key = workflow_record_key(&record)?;

            // Validate the complete deletion plan before mutating storage. A
            // misplaced/corrupt child must never leave a partially deleted
            // run. Claims go first during deletion so a later failure cannot
            // strand coordination state after its target record is gone.
            let claims = self.list_claims_for_run(&key).await?;
            let activities = self.list_activities(&key).await?;
            let timers = self.list_timers(&key, v1::TimerStatus::Unspecified).await?;
            let events = self.list_events(&key).await?;

            for claim in claims {
                let claim_key = claim_record_key(&claim)?;
                self.delete_claim(&claim_key).await?;
            }
            for a in activities {
                let ak = activity_record_key(&a)?;
                self.delete_activity(&ak).await?;
            }
            for t in timers {
                let tk = timer_record_key(&t)?;
                self.delete_timer(&tk).await?;
            }
            for e in events {
                let ek = event_record_key(&e)?;
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
            let key = workflow_record_key(&workflow)?;
            let timers = self.list_timers(&key, v1::TimerStatus::Scheduled).await?;
            for timer in timers {
                if let Some(ts) = &timer.fire_at {
                    if system_time_from_proto(ts) > now {
                        continue;
                    }
                    let tk = timer_record_key(&timer)?;
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
// Proto key conversions. Object paths are never parsed back into identity.
// ---------------------------------------------------------------------------

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
