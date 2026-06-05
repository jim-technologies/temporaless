//! Workflow + activity runtime for the Rust SDK.
//!
//! Mirrors the Go (`core/go/workflow`) and Python (`core/py/src/temporaless/
//! workflow.py`) runtimes — same replay semantics, same record shapes,
//! same proto-descriptor-derived `workflow_type` / `activity_type` strings
//! (via `prost::Name::full_name()`). A workflow authored in Python and
//! replayed in Rust returns the same stored result; a workflow authored
//! in Rust and inspected from Python sees the same record layout.
//!
//! # Scope (intentional)
//!
//! Ships today: `run()` with the three replay branches (COMPLETED short-
//! circuit, FAILED replay, IN_PROGRESS resume, fresh execution),
//! `execute_activity()` with full in-process retry + `Retry-After`,
//! `activity()` ergonomic helper, `annotate()` durable metadata.
//!
//! Not yet shipped (read `docs/sdks.md` for the gap table): durable retry
//! backoffs, concurrency keys, claims, `Sleep`, `WaitEvent`, ConnectRPC
//! integration. Workflows that don't use those primitives are fully
//! functional today; workflows that do should run in Go or Python.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use prost::{Message, Name};
use thiserror::Error;

use crate::storage::{proto_timestamp, ActivityKey, OpenDALStore, Store, StoreError, WorkflowKey};
use crate::v1;

// ---------------------------------------------------------------------------
// Options — mirror the protobuf shape so they're trivially constructible
// from the generated v1::WorkflowOptions / v1::ActivityOptions when the
// caller already has those (e.g. inside a tonic handler).
// ---------------------------------------------------------------------------

/// One workflow execution's identity + replay context.
#[derive(Debug, Clone)]
pub struct WorkflowOptions {
    pub workflow_id: String,
    pub run_id: String,
    pub code_version: String,
}

impl WorkflowOptions {
    pub fn new(workflow_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        let cv = std::env::var("TEMPORALESS_CODE_VERSION").unwrap_or_else(|_| "dev".into());
        Self {
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
            code_version: cv,
        }
    }

    pub fn with_code_version(mut self, cv: impl Into<String>) -> Self {
        self.code_version = cv.into();
        self
    }
}

/// One activity invocation's identity + retry behavior.
#[derive(Debug, Clone)]
pub struct ActivityOptions {
    pub activity_id: String,
    pub retry_policy: Option<RetryPolicy>,
}

impl ActivityOptions {
    pub fn new(activity_id: impl Into<String>) -> Self {
        Self {
            activity_id: activity_id.into(),
            retry_policy: None,
        }
    }

    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }
}

/// Retry behavior for a single activity. Same field semantics as the
/// proto `RetryPolicy` — see `api/temporaless/v1/temporaless.proto`.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub maximum_attempts: u32,
    pub initial_interval: Duration,
    pub backoff_coefficient: f64,
    pub maximum_interval: Duration,
    pub non_retryable_error_codes: Vec<String>,
}

impl RetryPolicy {
    /// Single-attempt policy (no retries).
    pub fn single_attempt() -> Self {
        Self {
            maximum_attempts: 1,
            initial_interval: Duration::ZERO,
            backoff_coefficient: 1.0,
            maximum_interval: Duration::ZERO,
            non_retryable_error_codes: Vec::new(),
        }
    }
}

/// Sensible default for the ergonomic `activity()` helper. 3 attempts, 1s
/// initial, 2x backoff, 30s max. Mirrors the Go and Python defaults.
pub fn default_retry_policy() -> RetryPolicy {
    RetryPolicy {
        maximum_attempts: 3,
        initial_interval: Duration::from_secs(1),
        backoff_coefficient: 2.0,
        maximum_interval: Duration::from_secs(30),
        non_retryable_error_codes: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// What an activity body returns on failure. Matches `ActivityFailure` on
/// the wire: a stable `code`, a human-readable `message`, and an optional
/// `retry_after` (vendor-supplied minimum wait — `Retry-After` header).
#[derive(Debug, Clone, Error)]
#[error("activity error [{code}]: {message}")]
pub struct ActivityError {
    pub code: String,
    pub message: String,
    pub retry_after: Option<Duration>,
}

impl ActivityError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retry_after: None,
        }
    }

    /// Attach a vendor-supplied minimum wait (HTTP `Retry-After`,
    /// `x-ratelimit-reset`, etc.). The runtime uses
    /// `max(computed_interval, retry_after)` for the next attempt.
    pub fn with_retry_after(mut self, retry_after: Duration) -> Self {
        self.retry_after = Some(retry_after);
        self
    }
}

/// Top-level error type for the runtime. Surfaces storage errors,
/// identity conflicts (workflow_type / activity_type / code_version
/// changes incompatible with a stored record), and terminal activity
/// failures.
#[derive(Debug, Error)]
pub enum RunError {
    #[error("storage: {0}")]
    Storage(#[from] StoreError),
    #[error("encode proto: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("decode proto: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("workflow conflict: {0}")]
    WorkflowConflict(String),
    #[error("activity conflict: {0}")]
    ActivityConflict(String),
    #[error("activity failed: {0}")]
    Activity(#[from] ActivityError),
    #[error("workflow result was missing on a completed record")]
    MissingResult,
}

// ---------------------------------------------------------------------------
// Workflow context — task-local so `current()` and `annotate()` work from
// any depth in the activity body without threading the workflow through.
// ---------------------------------------------------------------------------

/// In-flight workflow context handed to the body. Holds the store and
/// identity; provides accessors used by `execute_activity`.
#[derive(Clone)]
pub struct Workflow {
    pub(crate) store: Arc<OpenDALStore>,
    pub(crate) workflow_id: String,
    pub(crate) run_id: String,
    pub(crate) code_version: String,
    pub(crate) annotations: Arc<Mutex<HashMap<String, String>>>,
}

impl Workflow {
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
    pub fn code_version(&self) -> &str {
        &self.code_version
    }
}

tokio::task_local! {
    static CURRENT: Workflow;
}

/// Return the in-flight `Workflow` for the current task. Panics if called
/// outside a `workflow::run` scope — mirrors Python's `current_workflow()`
/// "fail fast" stance for programmer errors.
pub fn current() -> Workflow {
    CURRENT.with(|w| w.clone())
}

/// Attach a key/value to the in-flight record (activity if called from
/// inside `execute_activity`'s body, workflow otherwise). Survives replay.
pub fn annotate(key: impl Into<String>, value: impl Into<String>) {
    if let Ok(w) = CURRENT.try_with(|w| w.annotations.clone()) {
        if let Ok(mut map) = w.lock() {
            map.insert(key.into(), value.into());
        }
    }
}

// ---------------------------------------------------------------------------
// Workflow entry point
// ---------------------------------------------------------------------------

/// Run a workflow body against the store.
///
/// Replay semantics (identical to Go's `workflow.Run` and Python's `run()`):
///
/// 1. **COMPLETED record found** → decode the stored result and return it.
///    The body does NOT run.
/// 2. **FAILED record found** → return the stored failure.
/// 3. **IN_PROGRESS record found** → verify identity (workflow_type +
///    code_version), then re-run the body. Activities short-circuit on
///    their own stored records.
/// 4. **No record** → write IN_PROGRESS, run the body, write the terminal
///    record (COMPLETED or FAILED).
///
/// On replay the runtime checks `(workflow_type, code_version)`. The
/// `workflow_id` itself is the de-duplication key: same id replays the
/// stored result regardless of input bytes — the caller chose the id and
/// owns its semantics. A shape change (request/response message types) or
/// a bumped `code_version` triggers `RunError::WorkflowConflict` so a
/// caller can't accidentally replay against incompatible code.
pub async fn run<Req, Resp, F, Fut>(
    store: Arc<OpenDALStore>,
    options: WorkflowOptions,
    input: Req,
    execute: F,
) -> Result<Resp, RunError>
where
    Req: Message + Name + Default,
    Resp: Message + Name + Default,
    F: FnOnce(Workflow, Req) -> Fut,
    Fut: Future<Output = Result<Resp, RunError>>,
{
    let key = WorkflowKey::new(&options.workflow_id, &options.run_id);
    let workflow_type = message_pair_type::<Req, Resp>("workflow");

    // Replay branches.
    let existing = store.get_workflow(&key).await?;
    let created_at = match existing {
        Some(record) if record.status == v1::WorkflowStatus::Completed as i32 => {
            assert_workflow_identity(&record, &workflow_type, &options.code_version)?;
            return decode_workflow_result::<Resp>(&record);
        }
        Some(record) if record.status == v1::WorkflowStatus::Failed as i32 => {
            assert_workflow_identity(&record, &workflow_type, &options.code_version)?;
            return Err(RunError::Activity(ActivityError::new(
                record
                    .failure
                    .as_ref()
                    .map(|f| f.code.clone())
                    .unwrap_or_default(),
                record
                    .failure
                    .as_ref()
                    .map(|f| f.message.clone())
                    .unwrap_or_default(),
            )));
        }
        Some(ref record) if record.status == v1::WorkflowStatus::InProgress as i32 => {
            assert_workflow_identity(record, &workflow_type, &options.code_version)?;
            record.created_at
        }
        Some(record) => {
            return Err(RunError::WorkflowConflict(format!(
                "stored workflow has unknown status {}",
                record.status
            )));
        }
        None => None,
    };

    // Write IN_PROGRESS on fresh runs.
    let created_at = created_at.unwrap_or_else(|| proto_timestamp(SystemTime::now()));
    let input_any = pack_any(&input)?;
    if existing.is_none() {
        let in_progress = v1::WorkflowRecord {
            schema_version: v1::RecordSchemaVersion::Workflow as i32,
            key: Some(key.to_proto()),
            workflow_type: workflow_type.clone(),
            code_version: options.code_version.clone(),
            input: Some(input_any.clone()),
            status: v1::WorkflowStatus::InProgress as i32,
            result: None,
            failure: None,
            created_at: Some(created_at),
            completed_at: None,
            annotations: Default::default(),
        };
        store.put_workflow(&in_progress).await?;
    }

    // Run the body inside the task-local workflow context.
    let annotations = Arc::new(Mutex::new(HashMap::new()));
    let workflow = Workflow {
        store: store.clone(),
        workflow_id: options.workflow_id.clone(),
        run_id: options.run_id.clone(),
        code_version: options.code_version.clone(),
        annotations: annotations.clone(),
    };
    let result = CURRENT
        .scope(
            workflow.clone(),
            async move { execute(workflow, input).await },
        )
        .await;

    // Persist the terminal record.
    match result {
        Ok(resp) => {
            let result_any = pack_any(&resp)?;
            let snapshot = take_annotations(&annotations);
            let completed = v1::WorkflowRecord {
                schema_version: v1::RecordSchemaVersion::Workflow as i32,
                key: Some(key.to_proto()),
                workflow_type,
                code_version: options.code_version,
                input: Some(input_any),
                status: v1::WorkflowStatus::Completed as i32,
                result: Some(result_any),
                failure: None,
                created_at: Some(created_at),
                completed_at: Some(proto_timestamp(SystemTime::now())),
                annotations: snapshot,
            };
            store.put_workflow(&completed).await?;
            Ok(resp)
        }
        Err(err) => {
            let failure = match &err {
                RunError::Activity(a) => Some(v1::ActivityFailure {
                    code: a.code.clone(),
                    message: a.message.clone(),
                    retry_after: a.retry_after.map(prost_duration),
                }),
                _ => Some(v1::ActivityFailure {
                    code: String::new(),
                    message: err.to_string(),
                    retry_after: None,
                }),
            };
            let snapshot = take_annotations(&annotations);
            let failed = v1::WorkflowRecord {
                schema_version: v1::RecordSchemaVersion::Workflow as i32,
                key: Some(key.to_proto()),
                workflow_type,
                code_version: options.code_version,
                input: Some(input_any),
                status: v1::WorkflowStatus::Failed as i32,
                result: None,
                failure,
                created_at: Some(created_at),
                completed_at: Some(proto_timestamp(SystemTime::now())),
                annotations: snapshot,
            };
            store.put_workflow(&failed).await?;
            Err(err)
        }
    }
}

// ---------------------------------------------------------------------------
// Activity execution
// ---------------------------------------------------------------------------

/// Run an activity body, replaying from storage if possible. Honors the
/// supplied retry policy (in-process retries with exponential backoff +
/// `Retry-After` floor). Returns the activity result or a terminal
/// `ActivityError`.
pub async fn execute_activity<Req, Resp, F, Fut>(
    options: ActivityOptions,
    input: Req,
    execute: F,
) -> Result<Resp, RunError>
where
    Req: Message + Name + Default + Clone,
    Resp: Message + Name + Default,
    F: Fn(Req) -> Fut,
    Fut: Future<Output = Result<Resp, ActivityError>>,
{
    let workflow = current();
    let activity_type = message_pair_type::<Req, Resp>("activity");
    let key = ActivityKey::new(
        &workflow.workflow_id,
        &workflow.run_id,
        &options.activity_id,
    );

    // Replay branches.
    let existing = workflow.store.get_activity(&key).await?;
    if let Some(record) = &existing {
        match v1::ActivityStatus::try_from(record.status).unwrap_or(v1::ActivityStatus::Unspecified)
        {
            v1::ActivityStatus::Completed => {
                assert_activity_identity(record, &activity_type, &workflow.code_version)?;
                return decode_activity_result::<Resp>(record);
            }
            v1::ActivityStatus::Failed => {
                assert_activity_identity(record, &activity_type, &workflow.code_version)?;
                let failure = record.failure.clone().unwrap_or_default();
                return Err(RunError::Activity(ActivityError::new(
                    failure.code,
                    failure.message,
                )));
            }
            v1::ActivityStatus::Retrying => {
                assert_activity_identity(record, &activity_type, &workflow.code_version)?;
                // Resume from len(attempts) + 1.
            }
            _ => {
                return Err(RunError::ActivityConflict(format!(
                    "stored activity has unknown status {}",
                    record.status
                )));
            }
        }
    }

    let plan = options
        .retry_policy
        .clone()
        .unwrap_or_else(RetryPolicy::single_attempt);
    if plan.maximum_attempts == 0 {
        return Err(RunError::ActivityConflict(
            "retry policy maximum_attempts must be > 0".into(),
        ));
    }

    let input_any = pack_any(&input)?;
    let mut attempts: Vec<v1::ActivityAttempt> = existing
        .as_ref()
        .map(|r| r.attempts.clone())
        .unwrap_or_default();
    let mut interval = plan.initial_interval;
    let start_attempt = attempts.len() as u32 + 1;
    let annotations = Arc::new(Mutex::new(HashMap::new()));
    // Restore prior annotations from RETRYING record so per-attempt metadata
    // survives cross-invocation resumes.
    if let Some(record) = &existing {
        if let Ok(mut map) = annotations.lock() {
            for (k, v) in &record.annotations {
                map.insert(k.clone(), v.clone());
            }
        }
    }

    for attempt_idx in start_attempt..=plan.maximum_attempts {
        let started_at = SystemTime::now();
        // Scope a fresh activity-level annotations bag so annotate() calls
        // inside the body land on the ActivityRecord, not the parent
        // workflow's annotations.
        let activity_workflow = Workflow {
            store: workflow.store.clone(),
            workflow_id: workflow.workflow_id.clone(),
            run_id: workflow.run_id.clone(),
            code_version: workflow.code_version.clone(),
            annotations: annotations.clone(),
        };
        let exec_input = input.clone();
        let result = CURRENT.scope(activity_workflow, execute(exec_input)).await;
        let completed_at = SystemTime::now();

        match result {
            Ok(resp) => {
                attempts.push(v1::ActivityAttempt {
                    attempt: attempt_idx,
                    started_at: Some(proto_timestamp(started_at)),
                    completed_at: Some(proto_timestamp(completed_at)),
                    failure: None,
                });
                let result_any = pack_any(&resp)?;
                let snapshot = take_annotations(&annotations);
                let record = v1::ActivityRecord {
                    schema_version: v1::RecordSchemaVersion::Activity as i32,
                    key: Some(key.to_proto()),
                    activity_type,
                    code_version: workflow.code_version.clone(),
                    input: Some(input_any),
                    status: v1::ActivityStatus::Completed as i32,
                    result: Some(result_any),
                    failure: None,
                    created_at: attempts.first().and_then(|a| a.started_at),
                    completed_at: Some(proto_timestamp(completed_at)),
                    attempts,
                    annotations: snapshot,
                    next_attempt_at: None,
                };
                workflow.store.put_activity(&record).await?;
                return Ok(resp);
            }
            Err(err) => {
                attempts.push(v1::ActivityAttempt {
                    attempt: attempt_idx,
                    started_at: Some(proto_timestamp(started_at)),
                    completed_at: Some(proto_timestamp(completed_at)),
                    failure: Some(v1::ActivityFailure {
                        code: err.code.clone(),
                        message: err.message.clone(),
                        retry_after: err.retry_after.map(prost_duration),
                    }),
                });

                // Retry-After overrides the configured interval when it's
                // longer — vendor pacing wins over our exponential floor.
                if let Some(ra) = err.retry_after {
                    if ra > interval {
                        interval = ra;
                    }
                }

                let non_retryable = plan
                    .non_retryable_error_codes
                    .iter()
                    .any(|c| c == &err.code);
                if attempt_idx >= plan.maximum_attempts || non_retryable {
                    let snapshot = take_annotations(&annotations);
                    let record = v1::ActivityRecord {
                        schema_version: v1::RecordSchemaVersion::Activity as i32,
                        key: Some(key.to_proto()),
                        activity_type,
                        code_version: workflow.code_version.clone(),
                        input: Some(input_any),
                        status: v1::ActivityStatus::Failed as i32,
                        result: None,
                        failure: Some(v1::ActivityFailure {
                            code: err.code.clone(),
                            message: err.message.clone(),
                            retry_after: err.retry_after.map(prost_duration),
                        }),
                        created_at: attempts.first().and_then(|a| a.started_at),
                        completed_at: Some(proto_timestamp(completed_at)),
                        attempts,
                        annotations: snapshot,
                        next_attempt_at: None,
                    };
                    workflow.store.put_activity(&record).await?;
                    return Err(RunError::Activity(err));
                }

                // Persist RETRYING so a process death during the backoff
                // sleep doesn't lose the attempt history.
                let snapshot = annotations.lock().map(|m| m.clone()).unwrap_or_default();
                let retrying = v1::ActivityRecord {
                    schema_version: v1::RecordSchemaVersion::Activity as i32,
                    key: Some(key.to_proto()),
                    activity_type: activity_type.clone(),
                    code_version: workflow.code_version.clone(),
                    input: Some(input_any.clone()),
                    status: v1::ActivityStatus::Retrying as i32,
                    result: None,
                    failure: Some(v1::ActivityFailure {
                        code: err.code.clone(),
                        message: err.message.clone(),
                        retry_after: err.retry_after.map(prost_duration),
                    }),
                    created_at: attempts.first().and_then(|a| a.started_at),
                    completed_at: None,
                    attempts: attempts.clone(),
                    annotations: snapshot,
                    next_attempt_at: None,
                };
                workflow.store.put_activity(&retrying).await?;

                tokio::time::sleep(interval).await;
                interval = next_interval(interval, &plan);
            }
        }
    }

    // Reachable only if maximum_attempts overflowed — defensive.
    Err(RunError::ActivityConflict(format!(
        "activity {:?} exhausted retry plan",
        options.activity_id
    )))
}

/// Ergonomic shortcut: auto-derives `activity_id` from the function's
/// type name and applies `default_retry_policy()`. Pass [`execute_activity`]
/// directly when you need explicit control.
pub async fn activity<Req, Resp, F, Fut>(input: Req, execute: F) -> Result<Resp, RunError>
where
    Req: Message + Name + Default + Clone,
    Resp: Message + Name + Default,
    F: Fn(Req) -> Fut,
    Fut: Future<Output = Result<Resp, ActivityError>>,
{
    let activity_id = infer_activity_id::<F>();
    let options = ActivityOptions::new(activity_id).with_retry_policy(default_retry_policy());
    execute_activity(options, input, execute).await
}

/// Pull a path-safe activity_id out of `F`'s type name. Closures whose
/// generated names contain forbidden characters fall back to a generic
/// `closure` label; pass an explicit `activity_id` for stable replay.
fn infer_activity_id<F>() -> String {
    let raw = std::any::type_name::<F>();
    // Take only the last `::`-separated segment so the activity_id stays
    // short and stable across `cargo` rebuilds (the full type name
    // contains the package path).
    let short = raw.rsplit("::").next().unwrap_or(raw);
    // Strip generic args (e.g. "fetch_quote<...>") for stability.
    let short = short.split('<').next().unwrap_or(short);
    // Sanitize to the framework's ID regex [A-Za-z0-9._:-].
    let sanitized: String = short
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "closure".into()
    } else {
        sanitized
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn message_pair_type<Req: Name, Resp: Name>(kind: &str) -> String {
    // `prost::Name::full_name()` returns the proto descriptor full name
    // (`google.protobuf.StringValue`, `temporaless.v1.WorkflowRecord`, etc.) —
    // the same string Go's `proto.Message.ProtoReflect().Descriptor().FullName()`
    // and Python's `message.DESCRIPTOR.full_name` produce. Records written
    // from any SDK are therefore replayable from any other.
    format!("{kind}:{}->{}", Req::full_name(), Resp::full_name())
}

/// Guard against shape changes that would make the stored record
/// incompatible with the current code path: a swapped request/response
/// message type (which changes `workflow_type`) or a bumped `code_version`.
/// The `workflow_id` itself is the de-duplication key; same id + same shape
/// + same code_version replays the stored result regardless of input bytes.
fn assert_workflow_identity(
    record: &v1::WorkflowRecord,
    workflow_type: &str,
    code_version: &str,
) -> Result<(), RunError> {
    if record.workflow_type != workflow_type {
        return Err(RunError::WorkflowConflict(format!(
            "workflow type changed from {:?} to {:?}",
            record.workflow_type, workflow_type
        )));
    }
    if record.code_version != code_version {
        return Err(RunError::WorkflowConflict(format!(
            "code version changed from {:?} to {:?}",
            record.code_version, code_version
        )));
    }
    Ok(())
}

/// See [`assert_workflow_identity`] for the de-duplication contract.
fn assert_activity_identity(
    record: &v1::ActivityRecord,
    activity_type: &str,
    code_version: &str,
) -> Result<(), RunError> {
    if record.activity_type != activity_type {
        return Err(RunError::ActivityConflict(format!(
            "activity type changed from {:?} to {:?}",
            record.activity_type, activity_type
        )));
    }
    if record.code_version != code_version {
        return Err(RunError::ActivityConflict(format!(
            "code version changed from {:?} to {:?}",
            record.code_version, code_version
        )));
    }
    Ok(())
}

fn decode_workflow_result<Resp: Message + Default>(
    record: &v1::WorkflowRecord,
) -> Result<Resp, RunError> {
    let any = record.result.as_ref().ok_or(RunError::MissingResult)?;
    Resp::decode(any.value.as_slice()).map_err(RunError::from)
}

fn decode_activity_result<Resp: Message + Default>(
    record: &v1::ActivityRecord,
) -> Result<Resp, RunError> {
    let any = record.result.as_ref().ok_or(RunError::MissingResult)?;
    Resp::decode(any.value.as_slice()).map_err(RunError::from)
}

fn pack_any<M: Message>(message: &M) -> Result<prost_types::Any, RunError> {
    let bytes = message.encode_to_vec();
    Ok(prost_types::Any {
        type_url: format!("type.googleapis.com/{}", message_type_url::<M>()),
        value: bytes,
    })
}

/// Best-effort proto type-URL from a Rust type name. The wire format only
/// cares that this round-trips; for cross-language replay where another
/// SDK reads our `Any`, pass `Resp` types that share full_name with the
/// proto descriptor (which prost types do via `Default::default()`).
fn message_type_url<M: Message>() -> String {
    // Strip the Rust path so the URL contains just the short type name.
    // prost doesn't expose ProtoMessage::full_name, so this is a best-effort
    // shim that's still good enough for round-tripping within Rust.
    let raw = std::any::type_name::<M>();
    raw.rsplit("::").next().unwrap_or(raw).to_string()
}

fn next_interval(prev: Duration, plan: &RetryPolicy) -> Duration {
    let next = prev.mul_f64(plan.backoff_coefficient);
    if !plan.maximum_interval.is_zero() && next > plan.maximum_interval {
        plan.maximum_interval
    } else {
        next
    }
}

fn prost_duration(d: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

fn take_annotations(bag: &Arc<Mutex<HashMap<String, String>>>) -> HashMap<String, String> {
    bag.lock()
        .map(|guard| {
            if guard.is_empty() {
                HashMap::new()
            } else {
                guard.clone()
            }
        })
        .unwrap_or_default()
}
