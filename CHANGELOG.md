# Changelog

All notable changes to Temporaless are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project aims to follow [Semantic Versioning](https://semver.org/) once it ships a tagged release.

The framework is **pre-1.0** — wire-format changes will be called out clearly when they happen, and there's no backwards-compatibility commitment until v1.0.0.

## [Unreleased]

### Added

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
