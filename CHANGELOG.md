# Changelog

All notable changes to Temporaless are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project aims to follow [Semantic Versioning](https://semver.org/) once it ships a tagged release.

The framework is **pre-1.0** — wire-format changes will be called out clearly when they happen, and there's no backwards-compatibility commitment until v1.0.0.

## [Unreleased]

## [0.2.0] — 2026-05-24

The framework remains **pre-1.0**. This release bakes a per-submission
task tracker into `dispatch` so consumers stop having to roll their own
(as ghdrive did). Go-only for now; **Python and Rust parity is a
follow-up release**.

### Added

- **`Dispatcher.Status(taskID)` (Go)**. Returns a `*TaskInfo` describing
  the current lifecycle state of one `DoAsync` submission: PENDING →
  RUNNING → DONE (with handler response wrapped in `google.protobuf.Any`)
  or FAILED (with error message). Unknown / TTL-evicted ids return
  `(nil, false)` so callers can distinguish "no such task" from a
  terminal state.
- **`TaskStatus` enum + `TaskInfo` message** in `temporaless.v1` proto.
  Cross-language wire shape; Python and Rust SDKs will mirror in the
  next release.
- **`DispatchOptions.task_ttl`** (`google.protobuf.Duration`). How long
  completed task records stay queryable before the GC sweep evicts
  them. Default 1 hour. In-flight (PENDING/RUNNING) records never
  evict — only terminal ones age out. Tracking itself is always on:
  the framework is opinionated about this, the cost is one map entry
  per submission.

### Changed (breaking)

- **`Dispatcher.DoAsync` signature (Go)**. Returns `(taskID string, err
  error)` instead of `error`. Callers that don't want the id just
  underscore it: `_, err := disp.DoAsync(...)`. The task_id is the
  primary key for `Status()` polling.
- **`Options.OnError` signature (Go)**. Now `func(method, taskID
  string, err error)`. The default slog handler logs the task_id
  alongside the method so failed handlers are grep-able by id.
- **`Queue.Submit` signature (Go)**. Now `Submit(ctx, method, taskID,
  payload []byte) error`. External queue adapters should propagate
  the task_id alongside the payload (message header / attribute) so
  consumers can correlate.

### Internal

- Tracker is a `sync.RWMutex`-guarded map with a GC goroutine sweeping
  at `taskTTL / 2`. Shutdown stops the GC last (after handler drain)
  so terminal-state writes during drain land in the map before it
  closes.
- New tests: `TestStatusTracksLifecycle`, `TestStatusFailedSurfacesError`,
  `TestStatusUnknownIDReturnsFalse`. All under `-race`.

## [0.1.0] — 2026-05-24

First tagged release. The framework is **pre-1.0** — wire-format changes
will continue to be called out clearly when they happen, and there's no
backwards-compatibility commitment until v1.0.0.

### Added

- **`dispatch` bounded concurrency (`MaxInflight`) + pluggable `Queue`**.
  Two related additions to the pool that landed in the previous entry:
  - **Bounded concurrency**: new `max_inflight` knob on the proto
    `DispatchOptions`. When > 0, `DoAsync` blocks until a slot frees up
    — natural producer-side backpressure for bursty callers, respecting
    caller ctx / asyncio cancel / tokio shutdown notification. Zero
    (default) is unbounded — current behavior preserved.
  - **Proto-driven options**: `DrainTimeout` and `MaxInflight` moved into
    `temporaless.v1.DispatchOptions` so a single config file / env / CLI
    flag drives them identically across Go, Python, and Rust. Runtime
    hooks (`OnError`, `Queue`) stay language-local on the constructor.
    Small breaking change to the brand-new dispatcher Options struct in
    all three SDKs — no external users to migrate yet.
  - **`Queue` interface**: new producer-only abstraction `Submit(method,
    payload []byte)` + `Close()`. Default is in-process (current
    goroutine-pool behavior). External users implement `Queue` against
    Kafka / RabbitMQ / NATS / SQS / Redis Streams / etc.; the dispatcher
    proto-marshals the request deterministically and hands `(method,
    payload)` to the queue. Consumer-side helper `Dispatcher.Invoke(ctx,
    method, payload)` looks up the registered handler, decodes the
    bytes, and runs it — so the user's worker loop is just "pull from
    queue, call Invoke, ack on Ok / nack on Err". Producer-side type
    check catches mismatched request types BEFORE the bytes hit the
    queue, so a typo doesn't get durably enqueued and then dead-lettered
    later.
  - In Python and Rust, `do_async` is now `async` (it was sync) because
    the semaphore wait needs `await`. Go's signature is unchanged.

- **`dispatch` adapter — fire-and-forget pool for gRPC-shaped handlers**,
  in all three SDKs (`adapters/go/dispatch/`,
  `core/py/src/temporaless/dispatch.py`,
  `core/rs/temporaless/src/dispatch.rs`). Identical shape: `register(method,
  handler)` wires a typed handler under its gRPC fully-qualified method
  name, `do_async(method, req)` spawns it as a goroutine / asyncio task /
  tokio task and returns immediately, `shutdown()` stops accepting new
  submissions and drains in-flight work for up to `drain_timeout`
  (default 15s — matches common SIGTERM grace windows) before cancelling.
  Always waits for every spawned task to actually return; orphaning a
  handler mid-vendor-call is the failure mode we're avoiding.
  - In-process only; not durable across crashes. Use `workflow.run` when
    you need at-least-once delivery — `dispatch` is at-most-once +
    best-effort for side effects whose result the caller doesn't need to
    wait on (webhook notifications, telemetry pushes, fan-out where the
    caller wants its own request to return quickly).
  - Handler errors flow through `OnError` (default: WARN log). Panicking
    handlers are recovered in Go (`recover()`) and surfaced via the same
    path so a single bad call can't take the process down.

### Changed (breaking)

- **Rust `workflow_type` / `activity_type` now use the proto descriptor
  full name.** Replaces `std::any::type_name::<T>()` (which produced
  Rust-shaped strings like `prost_types::wrappers::StringValue`) with
  `prost::Name::full_name()` (the canonical proto name
  `google.protobuf.StringValue`, matching Go's
  `proto.Message.ProtoReflect().Descriptor().FullName()` and Python's
  `message.DESCRIPTOR.full_name`). With the digest gone, `workflow_type`
  is now the cross-language shape gate — so Rust must agree with the other
  SDKs on what that string looks like, otherwise a Python-authored record
  cannot be replayed from Rust.
  - Generic bounds widened: `workflow::run`, `execute_activity`, and the
    `activity()` helper now require `Req: Message + Name` and
    `Resp: Message + Name`. Generated types from `prost-build` with
    `enable_type_names()` (which the framework's `build.rs` now sets)
    satisfy this automatically. Hand-rolled prost messages in tests /
    examples need a one-line `impl Name`.
  - New test: `tests/interop.rs::rust_replays_python_authored_workflow_record`
    pre-seeds a `WorkflowRecord` with `workflow_type =
    "workflow:google.protobuf.StringValue->..."` (the canonical
    Python/Go form) and asserts the Rust runtime replays it without
    `WorkflowConflict`.
- **`input_digest` removed.** The SHA-256 fingerprint over deterministic
  input bytes is gone from every record kind (`WorkflowRecord`,
  `ActivityRecord`, `TimerRecord`, `ClaimRecord`) and from every SDK.
  Field numbers 5 (workflow/activity/timer) and 7 (claim) are now
  `reserved`. The de-duplication contract is the caller-supplied id:
  same `workflow_id+run_id` (workflow) or `activity_id` under the run
  (activity) replays the stored result regardless of new input bytes.
  Replay still rejects shape changes (request/response type changed →
  stored `workflow_type`/`activity_type` mismatches) and `code_version`
  changes — those are the explicit invalidation levers.
  - Motivation: the digest punished correct usage. With user-supplied
    ids, `workflow.run("prices:aapl", "2026-05-04T09:30:00Z", req)` was
    already the dedup key; the digest just added cross-language fragility
    (Rust's `std::any::type_name` vs Go/Python's proto descriptor name
    produced different bytes for the same wire input) and locked
    integrators into recomputing the hash to author records out-of-band.
  - Migration: nothing required for new records. Existing records keep
    their stored bytes — the field is unknown on decode and silently
    dropped. Code that previously relied on `ErrActivityConflict` /
    `ActivityConflictError` to detect "same id, different input" now
    sees the stored result returned; pick a distinct id if you want
    distinct executions.
  - Old `activity_digest` / `timer_digest` / `execution_digest` helpers
    and `_assert_*_fingerprint` functions are removed from the public
    Python surface. Tests and tooling that imported them need to drop
    the import; the new identity guards are internal.

### Added

- **Rust workflow runtime** — `workflow::run()`, `workflow::execute_activity()`,
  ergonomic `workflow::activity()` helper, `current()` accessor (via
  `tokio::task_local!`), `annotate()` durable metadata. Implements the
  same three replay branches Go/Python do (COMPLETED short-circuit, FAILED
  replay, IN_PROGRESS resume, fresh execution). In-process retries with
  exponential backoff + `ActivityError::with_retry_after(...)` honoring
  vendor pacing. Workflows authored in any SDK produce identical record
  bytes (same protobuf wire format, same Hive paths).
  - Not yet in Rust: claims, concurrency keys, durable timer backoffs,
    `Sleep`/`WaitEvent`, ConnectRPC handler integration. Track per-feature
    parity in `docs/sdks.md`.
- **`docs/sdks.md`** — single-page cross-SDK compatibility audit.
  Surface-by-surface comparison (construct store, workflow body, activity
  dispatch) showing identical-where-possible / idiomatic-where-needed.
  Capability matrix: what each SDK ships today vs the runway.
- **Cross-language interop tests** (`core/rs/temporaless/tests/interop.rs`)
  proving wire-format identity: hand-construct records the way Python's
  runtime would, write via the Rust `OpenDALStore`, read back, assert
  every field round-trips including nested `Any` payloads, retry history,
  and annotations. Also verifies Rust writes to the canonical Hive path.
- **Rust SDK (storage layer)** at `core/rs/temporaless/`. Native `opendal`
  crate, prost-generated proto types, full read/write of every record kind
  (workflow, activity, timer, event, claim) at the same Hive-partitioned
  paths and protobuf wire format the Go/Python SDKs use. Workflows
  authored in Python or Go are fully readable from Rust and vice versa.
  - **Scope is storage-only for now.** No workflow.Run, no claims runtime,
    no cron/timer/janitor adapters, no ConnectStore client. Those layers
    will come as the Rust SDK matures; the storage layer is the
    prerequisite that lets Rust-native tooling (analytics, MCP servers,
    future runtime) interoperate.
  - **Cross-language storage benchmark.** `scripts/bench-rs` mirrors the
    Go/Python benchmark output format (`BenchmarkName N ns/op`). On the
    `fs` backend, Rust runs `PutGetWorkflow` and `PutGetActivity` at
    ~270k ns/op — roughly **1.9× faster than Go** (~510k ns/op) and **59×
    faster than Python** (~16M ns/op). The Rust SDK uses the native
    `opendal` crate directly; Go/Python go through FFI bindings, which
    explains the gap. See `docs/benchmarks.md` for the full table.
  - **Edition 2023 downgrade in build.rs.** `prost-build` + `protox` don't
    yet parse edition 2023 (as of mid-2026), so the Rust build script
    preprocesses the canonical proto into a proto3-equivalent for Rust
    codegen only — same wire bytes, same field numbers, same RPCs. The
    `ReservedNames` defaults are emitted as Rust constants in
    `$OUT_DIR/reserved_names.rs`, so the canonical proto remains the
    single source of truth.
- **Rust toolchain in Flox manifest.** `rustc`, `cargo`, `rustfmt`,
  `clippy` added alongside the existing Go / Python / `uv` / `buf`
  packages — same thin-manifest rationale.
- **`scripts/check` runs Rust tests + `cargo fmt --check` + `cargo
  clippy -D warnings`** alongside the existing Go/Python checks. All
  three SDKs land in one gate.

### Changed

- **Proto migrated from `syntax = "proto3"` to `edition = "2023"`.**
  Enables string-typed field defaults via `[default = "..."]`. The file-level
  `features.field_presence = IMPLICIT` keeps proto3-style scalar codegen
  (value types, not pointers) so existing struct literals and `GetX()` calls
  are unchanged. The new `ReservedNames` message uses explicit-presence
  fields so the defaults actually fire.
- **Framework-reserved string literals now live in proto.** The synthetic
  `__concurrency__` workflow_id, the `activity-retry:` timer-id prefix, and
  the `slot:` concurrency-slot claim_id prefix are declared as default
  values on `temporaless.v1.ReservedNames`. Both SDKs read them from the
  generated code at module load:
  - Go: `temporalessv1.Default_ReservedNames_ConcurrencyWorkflowId` (and the
    matching `_ActivityRetryTimerIdPrefix`, `_ConcurrencySlotIdPrefix`
    package-level constants).
  - Python: `temporaless_pb2.ReservedNames().concurrency_workflow_id`
    (etc.) via a module-level `_RESERVED_NAMES` singleton.
  Renaming any reserved string is now a one-line proto change plus
  regenerate; no SDK constants can drift. When invariantprotocol generates
  the CLI / MCP / HTTP wrappers, the reserved strings are visible to those
  too — fully protobuf-driven.

### Fixed

- TOCTOU race in `OpenDALStore.get_claim` (Python): the exists-then-read
  pattern occasionally raised `NotFound` when a concurrent
  acquire/release deleted the claim mid-call. Replaced with a single
  `read` that treats `NotFound` as "absent" — also one fewer round-trip.

### Added

- **Concurrency keys** (`WorkflowOptions.concurrency_key` + `concurrency_limit`).
  Pre-emptive cluster-wide cap on in-flight `workflow.Run` invocations
  sharing a key. The runtime acquires one of N slot claims before executing
  the workflow body; all slots full → `ConcurrencyBusyError` (mapped to
  RESOURCE_EXHAUSTED) and no IN_PROGRESS record is written. Slots are
  released on terminal status, failure, and pending. Built for vendor
  rate-limit pre-emption: every workflow sharing
  `concurrency_key="vendor:openai"` with `concurrency_limit=5` has at most
  5 active invocations cluster-wide, regardless of how many workers
  dispatch. Pairs naturally with the existing `Retry-After` and
  `durable_backoff_threshold` for the full vendor-friendly story.
  - Proto: `WorkflowOptions.concurrency_key/concurrency_limit` with a
    paired-CEL validation, `ClaimResourceType.CLAIM_RESOURCE_TYPE_CONCURRENCY_KEY`
    enum value, new `DeleteClaim` RPC on `RecordStoreService`.
  - All shared types/options/enums live in proto — when invariantprotocol
    generates the CLI / MCP / HTTP wrappers, the concurrency config rides
    along automatically.
  - Distributed-safe by design: acquire arbitrates via `TryCreateClaim`
    (the storage backend's native atomic precondition — S3 `If-None-Match`,
    GCS `ifGenerationMatch=0`, OpenDAL `if_not_exists`); no app-level locks.
    Crash recovery via owner-id check: a re-invocation re-acquires its own
    stale slot rather than consuming a second one.
- **`background` workers helper** (`adapters/go/background` /
  `temporaless.background`): opt-in toggles for in-process cron / timer
  scanner / janitor loops. Solves "every replica polling the bucket is
  wasteful" — deployers configure background loops on one "operator"
  replica; the rest are handler-only and skip the helper. Each loop is
  independently toggleable; absence in `Config` means disabled.
  Deliberately not leader-elected: the framework's replay-via-storage
  catches duplicate dispatches as a no-op, so opt-in is purely efficiency.
  Skip the helper entirely when the platform provides scheduled
  invocations (EventBridge, Cloud Scheduler, Kubernetes CronJob, …).
  `docs/deployment.md` gains a section walking through the pattern.
- **Ergonomic activity helper**: `Workflow.activity()` (Python) and
  `workflow.Activity[Req, Resp](...)` (Go) collapse the most common
  callsite to roughly what a plain function call already requires —
  pass the function and its argument. Defaults applied unless overridden:
  - `activity_id` ← inferred from the function's qualified name
    (`func.__qualname__` in Python, `runtime.FuncForPC` in Go). Use
    `WithActivityID(...)` / `activity_id=` to override when two callsites
    share a function but should produce distinct records.
  - `retry_policy` ← `DefaultRetryPolicy()` / `default_retry_policy()`
    (3 attempts, 1s initial, 2× backoff, 30s max, 30s durable threshold) —
    tuned for the framework's stated LLM / vendor / quant workloads.
    Override via `WithRetryPolicy(...)` / `retry_policy=`.
  - `result_type` (Python) inferred from the function's return annotation;
    Go infers from the `Resp` generic parameter directly.
  The existing `execute_activity` / `ExecuteActivity` APIs are unchanged
  — the helper is opt-in. Cuts per-activity boilerplate roughly in half
  in the examples.
- **`docs/analytics.md`** — DuckDB / Trino / BigQuery queries against the
  Hive-partitioned bucket. Shows how to read records directly without our
  service (Option 1) or via the `export` CLI (Option 2). Lean-in on the
  storage-first audit differentiator: every other workflow framework
  requires you to query *their* database / API to learn what happened;
  with Temporaless your data warehouse owns the audit trail.
- **CLI `export --kind {workflow,activity,timer,event}`** — bulk-decode
  records to JSON Lines for ingestion into warehouses that don't speak
  protobuf natively (BigQuery, Snowflake, Redshift). Transitional surface:
  once `invariantprotocol` generates the CLI from the proto, this command
  comes for free and the bundled binary retires.
- **Vendor-aware retries via `ActivityError.retry_after` /
  `ActivityFailure.retry_after`.** Activity bodies can surface a vendor-supplied
  minimum wait (HTTP `Retry-After`, OpenAI `x-ratelimit-reset`, etc.); the
  retry planner uses `max(computed_interval, retry_after)` so vendor pacing
  wins over the configured exponential schedule. Combined with the durable
  backoff threshold, a long `Retry-After` value automatically becomes a
  durable timer instead of an in-process sleep.
- **CLI: `stale-workflows --older-than DURATION`** — list IN_PROGRESS
  workflows whose `created_at` is older than the threshold. Wire into
  alerting to catch stuck timer-scanners, missing event deliveries, or
  claim leaks.
- **CLI: `tail`** — stream new workflow records as they are written (poll
  loop; `--poll-interval` configurable, default 2s). Operator surface for
  babysitting a backfill or freshly-deployed pipeline. Honors `--json`,
  `--status`, `--namespace`, `--workflow-id` filters.
- **Durable retry backoffs** (`RetryPolicy.durable_backoff_threshold`).
  When the next retry interval crosses the configured threshold, the runtime
  persists the wait as a `TIMER_KIND_ACTIVITY_RETRY` timer + writes the
  `ActivityRecord` with `next_attempt_at`, then surfaces `TimerPendingError`
  so the workflow stays IN_PROGRESS. The bundled timer scanner re-invokes
  the workflow after `fire_at` and the retry loop resumes from the next
  attempt. Designed for LLM workflows whose rate-limit windows (30s–10min)
  exceed serverless request timeouts. Zero (default) preserves the existing
  in-process retry behavior. Resolves PRD D2.
- **Outbox idempotency-key helper** (`adapters/go/outbox` /
  `core/py/src/temporaless/outbox.py`). Derives a stable
  `temporaless-{32-hex}` key from `(namespace, workflow_id, run_id, activity_id)`
  that activity bodies pass to vendor APIs (HTTP `Idempotency-Key`, DB upsert
  keys, S3 object names) so retries after a mid-flight failure dedupe
  vendor-side instead of double-charging. Closes the side-effect-safety gap
  documented in `docs/hard-cases.md`.
- **Run-scoped replay cache** in both Go and Python. On replay, the runtime
  prefetches all activity, timer, and event records under the run in parallel
  (3 `List` calls) and serves subsequent get-by-key reads from memory. A
  workflow with N fan-out activities that previously issued N individual
  `GetActivity` round-trips per replay now issues 0 — a strict win on
  ConnectStore / S3 / GCS backends and a no-op on fresh runs (no prefetch
  triggered). Negative-cache entries short-circuit get-by-key for records
  that didn't exist at prefetch time. Out-of-scope reads (cross-pipeline
  dependencies, inspector adapters) pass straight through.
- **`temporaless` operator CLI** (`cmd/temporaless`) — thin wrapper over the
  inspector / janitor adapters. Subcommands: `list-workflows`,
  `list-activities`, `get-workflow`, `reset-workflow`, `reset-activity`,
  `reset-event`, `sweep`. Supports text and `--json` (protojson) output.
  Storage backend is selected via `--store-scheme` (`fs` by default; extend
  by importing additional `opendal-go-services/*` schemes). This is a
  transitional surface — once the protobuf service is migrated to
  `invariantprotocol`, the CLI (and MCP) will be generated automatically and
  this binary retires.
- Cross-language parity for backfill + cross-pipeline dependencies:
  - Go: `adapters/go/backfill` (`Backfill[Resp](ctx, runIDs, Options, invoke)`) and `adapters/go/dependencies` (`WaitForWorkflow[Resp](ctx, store, key, newResult)`).
  - Typed errors `WorkflowDependencyPendingError` / `WorkflowDependencyFailedError` in core, mapped through `ErrorToConnectCode` to `Unavailable` / `Internal`.
  - `(*Workflow).Store()` accessor so adapter helpers can read records without reaching into private state.
- Stress tests covering 100 concurrent workflows, 100-workflow replay, 50 parallel activities in one workflow, and high-concurrency backfill with poison-pill isolation.
- `examples/{go,py}/production-server` — tested production wiring (bearer-token auth, `/healthz` + `/readyz`, structured JSON logs, graceful shutdown).
- `Dockerfile` — multi-stage Python 3.13-slim image, ~187MB, non-root user, health check wired.
- `.github/workflows/ci.yml` — runs the full gate on push/PR.
- `docs/runbook.md` — operator runbook for common production incidents.
- `docs/production-checklist.md` — pre-launch checklist.
- Prefect 3 compatibility adapter (`adapters/py/prefectcompat`).
- Auto error-mapping in `HandleConnect` (Go) / `wrap_workflow_method` (Python) — framework typed errors now translate to `*connect.Error` / `ConnectError` automatically with the original error preserved via wrapping.
- Production-server smoke tests in both languages that spawn the binary as a subprocess and verify auth + health + RPC behavior.

### Changed

- **Storage paths use strict Hive partitioning.** Every directory level is `key=value`. Spark / Trino / DuckDB / Athena pointed at `temporaless/v1/` auto-discovers `namespace`, `workflow_id`, `run_id`, `kind`, and the per-kind id as partition columns.
  - Old: `temporaless/v1/namespaces/{ns}/workflows/{wf}/runs/{rid}/workflow.binpb`
  - New: `temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=workflow/record.binpb`
- Error messages clarified for `wrap options are ambiguous` and `input digest changed` — both now state the fix, not just the failure.
- Type-checker switched from `pyright` to `ty` (Astral). `pyright` and `pyrefly` retired.

### Removed

- `deploy/k8s/` — the framework is platform-agnostic (Lambda / Cloud Run / Modal / Fly Machines / VMs+cron / K8s); shipping K8s manifests advertised the wrong default. Deployment guidance in `docs/deployment.md` and `docs/production-checklist.md` lists all options as equal.

### Known limitations

- Dagster compat adapter blocked upstream — Dagster pins `protobuf<7`, this project uses `>=7.34.1`. Tracked in `PRD.md` D11. Will revisit when Dagster unpins.
- Long retry backoffs (>30s) currently sleep in-process. Durable retry timers are designed but gated; see `PRD.md` D2.
- Temporal SDK test fixtures download an embedded test-server binary on first run. The temporalcompat tests skip cleanly with a clear message when Temporal's CDN is unreachable.
