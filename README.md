# Temporaless

A storage-first, language-agnostic, serverless workflow framework for Go, Python, and Rust (storage layer).

The core idea: every workflow boundary (start, activity, durable timer, signal, claim) is a protobuf record at a deterministic path in object storage. When workflow code reaches a stored boundary, the runtime first looks for a matching record. If the input fingerprint matches, the stored result is reused. If not, the boundary is executed and stored. There is no engine, no control plane, no central server — the storage backend is the source of truth, and processes are interchangeable.

> Not a Temporal replacement. A narrower framework for data workflows where activities are mostly fetch / normalize / persist and storage can be the durable coordination point.

- [`docs/getting-started.md`](docs/getting-started.md) — single-page walkthrough: store, workflow, retries, sleep, events, schedule, inspect, sweep
- [`docs/deployment.md`](docs/deployment.md) — production patterns (S3/GCS, ConnectRPC, multi-process, multi-region)

## Layout

```text
api/                  protobuf API definitions only
core/{go,py}/         Runtime + generated protobuf + OpenDAL store + ConnectRPC service
core/rs/              Rust SDK — storage layer only (native opendal crate); runtime layers TBD
adapters/{go,py}/     Adapters: claims, schedulers, inspectors, retention, Temporal compat
examples/{go,py}/     Runnable demos: fetch-prices, llm-completion, production-server, quant-service, stocks-pipeline, twitter-webhook
docs/                 Architecture and design notes
```

## Adapters

| Adapter | Purpose |
|---|---|
| [`adapters/go/connectstore`](adapters/go/connectstore) | Expose `RecordStoreService` over ConnectRPC; client wraps the service back as a `storage.Store` |
| [`adapters/go/gocdkclaims`](adapters/go/gocdkclaims) | Create-only activity claims via GoCDK Blob `IfNotExist` (S3, GCS native atomicity) |
| [`adapters/go/temporalcompat`](adapters/go/temporalcompat) | Run Temporaless-shaped handlers on the real Temporal Go SDK (worker direction) |
| [`adapters/py/temporalcompat`](adapters/py/temporalcompat) | Same for Python via `temporalio` |
| [`adapters/py/prefectcompat`](adapters/py/prefectcompat) | Run Temporaless-shaped handlers as Prefect 3 flows / tasks; keep Prefect's UI + scheduling, keep our storage-first replay |
| [`adapters/go/timerscanner`](adapters/go/timerscanner) | Find due durable timers belonging to in-flight workflows |
| [`adapters/go/cronscheduler`](adapters/go/cronscheduler) | In-process cron scheduler with stateless seeding from existing runs |
| [`adapters/go/inspector`](adapters/go/inspector) | List in-flight / failed workflows, reset records for re-execution |
| [`adapters/go/janitor`](adapters/go/janitor) | Sweep COMPLETED runs older than a max-age threshold |
| [`adapters/go/backfill`](adapters/go/backfill) | Run a workflow over many run_ids with bounded concurrency + per-run status (Dagster/Prefect/Airflow-style backfill) |
| [`adapters/go/dependencies`](adapters/go/dependencies) | Cross-pipeline durable wait — `WaitForWorkflow(store, key, newResult)` returns upstream's result or a typed pending/failed error |

Python equivalents for the operations adapters live in `core/py/src/temporaless/{timerscanner,cronscheduler,inspector,janitor,backfill,dependencies}.py` (no separate uv project — they have no third-party deps). `backfill` runs a workflow over many run_ids with bounded concurrency; `dependencies.wait_for_workflow` is the cross-pipeline durable wait primitive.

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
| [`examples/go/production-server`](examples/go/production-server) / [`examples/py/production_server.py`](examples/py/production_server.py) | **Production server (Go + Python parity)**: ConnectStore + bearer-token auth interceptor + `/healthz` & `/readyz` endpoints + structured JSON logs with correlation IDs + graceful shutdown on SIGTERM. The wiring you copy into your own service. |

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
- [`docs/analytics.md`](docs/analytics.md) — DuckDB / Trino / BigQuery queries against the Hive-partitioned bucket (storage-first audit story in practice)

## Development

```sh
flox activate
flox activate -- scripts/check       # full local gate: buf lint/format/generate, go test, ruff, ty, pytest
flox activate -- scripts/bench-go    # Go benchmarks (storage + workflow hot paths)
flox activate -- scripts/bench-py    # Python benchmarks (same suite, same output format)
```

The Flox manifest intentionally stays thin: just `go`, `python313`, `uv`, `buf`, plus the C runtime libs OpenDAL and Protovalidate need. Everything else lives in `go.mod`, `core/py/uv.lock`, or Buf remote plugin config.

## Storage convention

Records are stored as protobuf binary at:

```text
temporaless/v1/namespace={namespace}/workflow_id={workflow_id}/run_id={run_id}/kind=workflow/record.binpb
temporaless/v1/namespace={namespace}/workflow_id={workflow_id}/run_id={run_id}/kind=activity/activity_id={activity_id}/record.binpb
temporaless/v1/namespace={namespace}/workflow_id={workflow_id}/run_id={run_id}/kind=timer/timer_id={timer_id}/record.binpb
temporaless/v1/namespace={namespace}/workflow_id={workflow_id}/run_id={run_id}/kind=event/event_id={event_id}/record.binpb
temporaless/v1/namespace={namespace}/workflow_id={workflow_id}/run_id={run_id}/kind=claim/claim_id={claim_id}/record.binpb
```

Paths follow strict **Hive partitioning** (every directory level is `key=value`). Pointing Spark / Trino / DuckDB / Athena at the bucket auto-discovers `namespace`, `workflow_id`, `run_id`, `kind`, and the per-kind id (`activity_id` / `timer_id` / `event_id` / `claim_id`) as partition columns — predicates push down to the bucket so `WHERE namespace='default' AND kind='activity'` only fetches activity records.

IDs may contain letters, numbers, `.`, `_`, `-`, and `:` only. Slashes and `=` are rejected so the Hive-style paths stay predictable. The framework does not generate IDs — workflow IDs, run IDs, activity IDs, timer IDs, claim owner IDs, and event IDs are application-owned and must be passed explicitly.

The storage service contract is generated from `temporaless.v1.RecordStoreService`. Record schema versions, timer kinds, claim resource types, and claim capabilities are protobuf enums, not handwritten per-language string constants.

## What you do and do not get

**Generic core (in scope):** workflow + activity replay, retry policy with `RETRYING`-record persistence, durable timers and signals via `EventRecord`, claim-based coordination tiers, durable structured annotations, in-process scheduler with stateless seeding from storage, full CRUD+list RPC surface for every record kind, comprehensive proto-typed errors.

**Temporal-flavored knobs (adapter-only or out of scope):** activity timeouts, heartbeats, sticky task queues, signal-channel select, workflow-level retry policy, child workflows, payload converters. Use `adapters/{go,py}/temporalcompat` if you need any of those.

**Stateful processes (none required):** the core runtime is a pure function of `(input, store_state)`. Inspector, janitor, timerscanner are fully stateless. The cron scheduler keeps last-fires in memory but exposes `Snapshot()`/`Restore()` for explicit migration, plus `LastFireFromRuns` for deriving state from existing workflow records — fully stateless when run_ids embed fire times.

**Python is async-only.** Workflow bodies, activity bodies, and the runtime entry points (`run`, `execute_activity`, `wait_event`, `sleep`) are all `async def`. Sync callables are rejected at wrap time. The framework's I/O-bound workloads (LLM, HTTP, vendor APIs) are a natural fit for async; aligning with the Temporal Python SDK removes impedance. Go stays sync — goroutines + sync function signatures are idiomatic Go and there is no equivalent of `async/await`.

## License

Apache 2.0 — see [`LICENSE`](LICENSE) for the full text.
