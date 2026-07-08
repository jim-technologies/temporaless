# Temporaless

A storage-first, language-agnostic, serverless workflow framework for Go, Python, Rust, and TypeScript clients.

The core idea: every workflow boundary (start, activity, durable timer, signal, claim) is a protobuf record at a deterministic path in object storage, keyed by a caller-supplied id. When workflow code reaches a stored boundary, the runtime first looks for a matching record. If one exists, the stored result is reused. If not, the boundary is executed and stored. There is no engine, no control plane, no central server — the storage backend is the source of truth, and processes are interchangeable.

> Not a Temporal replacement. A narrower framework for data workflows where activities are mostly fetch / normalize / persist and storage can be the durable coordination point.

- [`docs/getting-started.md`](docs/getting-started.md) — single-page walkthrough: store, workflow, retries, sleep, events, schedule, inspect, sweep
- [`docs/deployment.md`](docs/deployment.md) — production patterns (S3/GCS, ConnectRPC, multi-process, multi-region)

## Application Service Adoption

Temporaless is optional infrastructure for work that benefits from durable replay: idempotent, retriable, scheduled, or long-running operations. Application services should keep normal API reads and routine request/response actions on a direct in-process path.

The direct handler should remain the canonical implementation. A Temporaless workflow wrapper may call that same unary protobuf handler when durability is needed, but ordinary reads/actions must not require Temporaless storage, the timer scanner, the query index, or background operators to be healthy. If Temporaless is disabled or unavailable, services should still serve normal APIs directly and only reject, defer, or explicitly fall back for the durable background operation.

Good Temporaless candidates are discovery, refresh, sync, migration, bootstrap,
and other orchestration paths where retries and replay are part of the product
contract. Simple product reads and one-shot synchronous actions should stay on
the direct service path.

## Layout

```text
api/                  protobuf API definitions only
core/{go,py}/         Runtime + generated protobuf + OpenDAL store + ConnectRPC service
core/rs/              Rust SDK — storage layer only (native opendal crate); runtime layers TBD
core/ts/              TypeScript SDK — generated protobuf, ConnectRPC wrappers, invariantprotocol projection
adapters/{go,py}/     Adapters: claims, schedulers, inspectors, retention, Temporal compat
examples/{go,py}/     Runnable demos: fetch-prices, llm-completion, production-server, quant-service, stocks-pipeline, twitter-webhook
docs/                 Architecture and design notes
```

## Install From Git

Temporaless packages are designed for direct git consumption, not registry
publishing. Pin a commit SHA for production builds.

```sh
go get github.com/jim-technologies/temporaless@main
pip install "temporaless @ git+ssh://git@github.com/jim-technologies/temporaless.git@main#subdirectory=core/py"
npm install "github:jim-technologies/temporaless#main"
```

Rust consumers can depend on the workspace crate from git:

```toml
temporaless = { git = "ssh://git@github.com/jim-technologies/temporaless.git", branch = "main", package = "temporaless" }
```

The TypeScript package entry lives at the repository root so npm git installs
work without a package registry; its source stays under `core/ts`.

## Adapters

| Adapter | Purpose |
|---|---|
| [`adapters/go/connectstore`](adapters/go/connectstore) | ConnectRPC store adapter; v2 regenerated-stub parity is a 0.3 follow-up |
| [`adapters/go/gocdkclaims`](adapters/go/gocdkclaims) | Create-only activity claims via GoCDK Blob `IfNotExist` (S3, GCS native atomicity) |
| [`adapters/go/temporalcompat`](adapters/go/temporalcompat) | Run Temporaless-shaped handlers on the real Temporal Go SDK (worker direction) |
| [`adapters/py/temporalcompat`](adapters/py/temporalcompat) | Same for Python via `temporalio` |
| [`adapters/py/prefectcompat`](adapters/py/prefectcompat) | Run Temporaless-shaped handlers as Prefect 3 flows / tasks; keep Prefect's UI + scheduling, keep our storage-first replay |
| [`adapters/py/indexstore`](adapters/py/indexstore) | Optional SQLite query index for workflow/activity listing, retention sweeps, and indexed due-timer queries |
| [`adapters/go/timerscanner`](adapters/go/timerscanner) | Find due durable timers belonging to in-flight workflows |
| [`adapters/go/cronscheduler`](adapters/go/cronscheduler) | In-process cron scheduler with stateless seeding from existing runs |
| [`adapters/go/inspector`](adapters/go/inspector) | List in-flight / failed workflows, reset records for re-execution |
| [`adapters/go/janitor`](adapters/go/janitor) | Sweep COMPLETED runs older than a max-age threshold |
| [`adapters/go/backfill`](adapters/go/backfill) | Run a workflow over many run_ids with bounded concurrency + per-run status (Dagster/Prefect/Airflow-style backfill) |
| [`adapters/go/dependencies`](adapters/go/dependencies) | Cross-pipeline durable wait — `WaitForWorkflow(store, key, newResult)` returns upstream's result or a typed pending/failed error |
| [`adapters/go/dispatch`](adapters/go/dispatch) | Fire-and-forget goroutine pool for gRPC-shaped handlers — `DoAsync(method, req)` + `Shutdown(ctx)` that drains in-flight goroutines (default 15s) before cancelling. In-process only; pair with workflows when you need durability. |

Python equivalents for the operations adapters live in `core/py/src/temporaless/{timerscanner,cronscheduler,inspector,janitor,backfill,dependencies,dispatch}.py` (no separate uv project — they have no third-party deps). `backfill` runs a workflow over many run_ids with bounded concurrency; `dependencies.wait_for_workflow` is the cross-pipeline durable wait primitive; `dispatch.Dispatcher` is the fire-and-forget asyncio-task pool that mirrors `adapters/go/dispatch`. Rust users get the same shape via `temporaless::dispatch`.

## Examples

| Example | Demonstrates |
|---|---|
| [`examples/go/fetch-prices`](examples/go/fetch-prices) / [`examples/py/fetch_prices.py`](examples/py/fetch_prices.py) | Hello-world workflow with one activity |
| [`examples/go/llm-completion`](examples/go/llm-completion) / [`examples/py/llm_completion.py`](examples/py/llm_completion.py) | Retry policy + annotations + replay |
| [`examples/go/stocks-pipeline`](examples/go/stocks-pipeline) | Cron scheduler + multi-activity workflow + replay |
| [`examples/go/twitter-webhook`](examples/go/twitter-webhook) | `WaitEvent` + external `SendEvent`, workflow stays IN_PROGRESS until signal arrives |
| [`examples/py/quant_signals.py`](examples/py/quant_signals.py) | `asyncio.gather` over 8 parallel symbol fetches + serial signal compose, full replay |
| [`examples/go/quant-service`](examples/go/quant-service) / [`examples/py/quant_service.py`](examples/py/quant_service.py) | **Canonical ConnectRPC service**: methods with `connect.Request`/`connect.Response` shape wrapped by `workflow.HandleConnect` (Go) or `@wrap_workflow_method` (Python) — production deploys mount this on any ConnectRPC mux / `asgi_application` |
| [`examples/py/stocks_cron.py`](examples/py/stocks_cron.py) | Cron-driven workflow: scheduler dispatches per-symbol workflows, statelessly seeded from existing run records |
| [`examples/py/approval_workflow.py`](examples/py/approval_workflow.py) | **Long-running**: durable sleep + wait-for-event + multi-step replay across simulated process deaths |
| [`examples/py/data_pipeline.py`](examples/py/data_pipeline.py) | **Airflow-style ETL**: extract → parallel transform (fan-out) → validate → conditional branch → load → notify, with backfill and replay |
| [`examples/go/production-server`](examples/go/production-server) / [`examples/py/production_server.py`](examples/py/production_server.py) | **Production server**: Python is current for v2 storage; Go regenerated-stub parity is a 0.3 follow-up. Shows ConnectStore + bearer-token auth interceptor + `/healthz` & `/readyz` endpoints + structured JSON logs with correlation IDs + graceful shutdown on SIGTERM. |

## Docs

- [`docs/philosophy.md`](docs/philosophy.md) — design tenets in one page (read first)
- [`docs/comparisons.md`](docs/comparisons.md) — honest comparison vs Temporal / n8n / Prefect / Dagster
- [`docs/getting-started.md`](docs/getting-started.md) — single-page walkthrough
- [`docs/deployment.md`](docs/deployment.md) — production deployment patterns
- [`docs/production-checklist.md`](docs/production-checklist.md) — pre-launch checklist (storage, ConnectStore, workflow service, operators, observability, failure modes)
- [`docs/runbook.md`](docs/runbook.md) — operator runbook for common incidents (stuck workflows, claim leaks, storage outages, DR)
- [`docs/architecture.md`](docs/architecture.md) — goals and core model
- [`docs/storage-rpc.md`](docs/storage-rpc.md) — `RecordStoreService` contract
- [`docs/scheduling.md`](docs/scheduling.md) — durable timers, cron, scanner, distribution
- [`docs/claims.md`](docs/claims.md) — claim coordination tiers
- [`docs/hard-cases.md`](docs/hard-cases.md) — concurrency, retries, side effects, backend atomicity
- [`docs/adapter-contract.md`](docs/adapter-contract.md) — what adapters must declare
- [`docs/temporal-adapter.md`](docs/temporal-adapter.md) — strict compatibility position
- [`docs/orchestrator-adapters.md`](docs/orchestrator-adapters.md) — Dagster / Prefect adapter notes
- [`docs/dependencies.md`](docs/dependencies.md) — what lives where (Flox vs go.mod vs uv)
- [`docs/benchmarks.md`](docs/benchmarks.md) — Go and Python benchmark suites with cross-language baseline numbers
- [`docs/analytics.md`](docs/analytics.md) — bucket archive, optional query index, and offline protobuf analytics
- [`docs/sdks.md`](docs/sdks.md) — cross-SDK surface comparison + capability matrix (Go / Python / Rust / TypeScript)

## Development

```sh
flox activate
flox activate -- scripts/check       # proto + Python core/adapters gate
TEMPORALESS_CHECK_GO_RUST=1 flox activate -- scripts/check  # include explicitly gated Go/Rust parity checks
npm run check                        # TypeScript client build + tests
flox activate -- scripts/bench-go    # Go benchmarks (storage + workflow hot paths)
flox activate -- scripts/bench-py    # Python benchmarks (same suite, same output format)
```

The Flox manifest intentionally stays thin: just `go`, `python313`, `uv`, `buf`, plus the C runtime libs OpenDAL and Protovalidate need. Everything else lives in `go.mod`, `Cargo.toml`, `core/py/uv.lock`, root `package-lock.json`, or Buf remote plugin config.

## Storage convention

Records are stored as protobuf binary at deterministic v2 keys:

```text
temporaless/v2/{namespace}/{workflow_id}/{run_id}/workflow.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/activity/{activity_id}.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/timer/{timer_id}.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/event/{event_id}.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/claim/{claim_id}.binpb
```

Keys are constructed from protobuf keys; runtime code never parses paths back into record identity. A run's records stay co-located under one prefix so replay prefetch, run deletion, and bucket lifecycle rules remain simple.

IDs may contain letters, numbers, `.`, `_`, `-`, `:`, and `=`. Slashes are rejected because object keys are path-like. Namespace and workflow ID values beginning with `_` are reserved for Temporaless system prefixes such as `_latest` and `_due`. The framework does not generate IDs — workflow IDs, run IDs, activity IDs, timer IDs, claim owner IDs, and event IDs are application-owned and must be passed explicitly.

The core storage contract is generated from `temporaless.v1.RecordStoreService`: point GET/PUT/DELETE, run-scoped lists for replay prefetch and run deletion, create-if-absent claims, latest-run pointers, and the compact due-timer ledger. Cross-run search lives on optional `RecordQueryService` implementations such as `temporaless-indexstore`; the bucket remains the source of truth and the index is rebuildable.

## What you do and do not get

**Generic core (in scope):** workflow + activity replay, retry policy with `RETRYING`-record persistence, durable timers and signals via `EventRecord`, claim-based coordination tiers, durable structured annotations, in-process scheduler with O(1) latest-run pointer seeding, run-scoped prefetch/deletion, and comprehensive proto-typed errors.

**Temporal-flavored knobs (adapter-only or out of scope):** activity timeouts, heartbeats, sticky task queues, signal-channel select, workflow-level retry policy, child workflows, payload converters. Use `adapters/{go,py}/temporalcompat` if you need any of those.

**Search and retention (optional index):** bucket-only deployments run workflows, scheduling, durable timers, and lifecycle-based retention without a database. Listing workflows, inspector views, and indexed sweeps require a derived query index or an offline scan.

**Stateful processes (none required):** the core runtime is a pure function of `(input, store_state)`. The cron scheduler keeps last-fires in memory but exposes `Snapshot()`/`Restore()` for explicit migration, plus `LastFireFromRuns` for deriving state from latest-run pointer objects when run_ids embed fire times. Timer resumption uses a compact due ledger, not a full bucket walk.

**Python is async-only.** Workflow bodies, activity bodies, and the runtime entry points (`run`, `execute_activity`, `wait_event`, `sleep`) are all `async def`. Sync callables are rejected at wrap time. The framework's I/O-bound workloads (LLM, HTTP, vendor APIs) are a natural fit for async; aligning with the Temporal Python SDK removes impedance. Go stays sync — goroutines + sync function signatures are idiomatic Go and there is no equivalent of `async/await`.

## License

Apache 2.0 — see [`LICENSE`](LICENSE) for the full text.
