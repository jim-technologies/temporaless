# Changelog

All notable changes to Temporaless are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project aims to follow [Semantic Versioning](https://semver.org/) once it ships a tagged release.

The framework is **pre-1.0** — wire-format changes will be called out clearly when they happen, and there's no backwards-compatibility commitment until v1.0.0.

## [Unreleased]

### Added

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
