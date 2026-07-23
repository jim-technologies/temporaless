# SDKs

Temporaless ships three runtime SDKs that share the same wire format: Go,
Python, and Rust. It also ships a TypeScript SDK with generated protobuf
types, ConnectRPC store/query clients, and an optional invariantprotocol
projection subpath.

This page documents:

1. The compatibility invariant (what "shares the same wire format" means).
2. The user-facing surfaces, side by side.
3. What each runtime SDK ships today vs. what's still on the runway.
4. What the TypeScript package provides.

## Compatibility invariant

The runtime SDKs encode protobuf records identically and write them at
identical v2 flat keys:

```text
temporaless/v2/{ns}/{wf}/{rid}/workflow.binpb
temporaless/v2/{ns}/{wf}/{rid}/activity/{aid}.binpb
temporaless/v2/{ns}/{wf}/{rid}/timer/{tid}.binpb
temporaless/v2/{ns}/{wf}/{rid}/event/{eid}.binpb
temporaless/v2/{ns}/{wf}/{rid}/claim/{cid}.binpb
```

The replay contract is identical across SDKs: a terminal workflow record is
reused when `(workflow_id, run_id, workflow_type)` matches—likewise a
completed/failed activity record matching `(workflow_id, run_id, activity_id,
activity_type)`. Pending records instead drive resume with the current handler.
The user-supplied IDs are the de-duplication key; the runtime does NOT
fingerprint input bytes. If you want a distinct execution, choose a distinct
ID.

`workflow_type` and `activity_type` are formed from the protobuf
descriptor's full name (`google.protobuf.StringValue`,
`temporaless.v1.WorkflowRecord`, etc.) — the exact string Go's
`proto.Message.ProtoReflect().Descriptor().FullName()`, Python's
`message.DESCRIPTOR.full_name`, and Rust's `prost::Name::full_name()`
all produce. Rust message types must implement `prost::Name`; the
framework's `build.rs` enables this for generated types via
`prost-build`'s `enable_type_names()`. For Rust-only hand-rolled
test/example types, `impl Name { const NAME = "..."; const PACKAGE = "...";
}` is one line.

This means:

- A workflow that runs in Python and writes its `WorkflowRecord` can be
  read AND replayed by Rust or Go without re-encoding — same `workflow_type`
  string and same IDs ⇒ the runtime returns the stored result on the receiving
  side.
- A workflow that runs partially in Python, crashes, and is resumed by a
  Go worker re-reading the same bucket completes cleanly — same ids, same
  shape, same replay.
- Inspector tooling written in any SDK or TypeScript client works against any
  exposed `RecordStoreService` / `RecordQueryService`.

The cross-language replay test
(`rust_replays_python_authored_workflow_record` in
`core/rs/temporaless/tests/interop.rs`) pre-seeds a `WorkflowRecord` with
the canonical Python-style `workflow_type` and asserts the Rust runtime
replays it without `WorkflowConflict`.

## Surface comparison

The framework's thesis is "a workflow is a decorated gRPC handler" — so the
runtime surfaces converge on three concepts: **construct the store**,
**decorate the handler**, **call activities from the body**. Everything else is
language-idiomatic glue.

### Construct the store

| Go | Python | Rust |
|---|---|---|
| `storage.NewOpenDALStore(op)` | `OpenDALStore(operator)` | `OpenDALStore::new(op)` |

Each SDK uses the OpenDAL binding native to its language (Go via
`opendal-go-services`, Python via the `opendal` PyO3 binding, Rust via the
native `opendal` crate).

### Workflow body

| Concept | Go | Python | Rust |
|---|---|---|---|
| Entry point | `workflow.Run(ctx, store, options, claimStore, input, newResult, execute)` | `await run(store, options, input, ResultType, execute)` | `workflow::run(store, options, input, execute).await` |
| Handler-style | `connectworkflow.Handle(ctx, req, opts)` (`adapters/go/connectworkflow`) | `@wrap_workflow_method(WorkflowMethodWrapOptions(...))` (`adapters/py/connectworkflow`) | (tonic / connect-rs integration: next iteration) |
| Current workflow in nested calls | `workflow.Current(ctx)` | `current_workflow()` | `workflow::current()` (tokio task-local) |
| Annotate | `workflow.Annotate(ctx, key, value)` | `annotate(key, value)` | `workflow::annotate(key, value)` |

Each language uses its idiomatic concurrency primitive: Go's `ctx`, Python's
`contextvars`, Rust's `tokio::task_local!`.

### Activity dispatch

| Concept | Go | Python | Rust |
|---|---|---|---|
| Lowest-level (explicit options) | `workflow.ExecuteActivity(ctx, opts, input, newResult, fn)` | `workflow.execute_activity(opts, input, result_type, fn)` | `workflow::execute_activity(opts, input, fn).await` |
| Ergonomic helper (explicit IDs + default retry) | `workflow.Activity(ctx, fn, input, WithActivityID(...), WithRetryTimerID(...))` | `workflow.activity(fn, input, activity_id=..., retry_timer_id=...)` | — |
| Default retry policy | `workflow.DefaultRetryPolicy()` | `default_retry_policy()` | — |

Every activity ID and durable retry timer ID is application-owned and explicit.
Use the same deterministic IDs across replay and across languages; the runtime
never derives identity from a function name.

## What ships today

Go and Python are first-class and run in every repository gate. The narrower
Rust surface is also formatted, linted, compiled, and tested by the release
gate; its smaller capability matrix, rather than test status, is what keeps it
from being first-class.

| Capability | Go | Python | Rust |
|---|:-:|:-:|:-:|
| **Canonical storage reads (all record kinds)** | ✓ | ✓ | ✓ |
| **Canonical point writes plus required derived/conditional invariants** | ✓ | ✓ | partial — shared point bytes only; latest pointer, timer WAL, and conditional claims are not implemented |
| `workflow.run` + replay (terminal short-circuit, IN_PROGRESS resume, fresh execution) | ✓ | ✓ | ✓ |
| `execute_activity` + replay | ✓ | ✓ | ✓ |
| Ergonomic activity helper (explicit IDs + default retry) | ✓ | ✓ | — |
| Retry policy (attempts, backoff, max interval, non-retryable codes) | ✓ | ✓ | ✓ |
| `Retry-After` from `ActivityFailure.retry_after` | ✓ | ✓ | ✓ |
| Durable retry backoffs (`RetryPolicy.durable_backoff_threshold` → timer record) | ✓ | ✓ | — |
| Concurrency keys (cluster-wide caps via claim slots) | ✓ | ✓ | — |
| Claims (workflow single-flight + activity, GoCDK / OpenDAL backend) | ✓ | ✓ | — |
| Durable timers (`workflow.Sleep`) | ✓ | ✓ | — |
| Manual event/dependency waits | ✓ | ✓ | — |
| Optional durable polling (`PollOptions` + timer scanner) | ✓ | ✓ | — |
| Atomic create-once event delivery | ✓ via capable `EventDeliveryStore` / remote `ConnectStore`; direct Go OpenDAL reports unsupported | ✓ when the OpenDAL operator advertises conditional create | — |
| Annotate (per-record durable key/value) | ✓ | ✓ | ✓ |
| Outbox idempotency-key helper | ✓ | ✓ | — |
| ConnectRPC handler shape | ✓ | ✓ | — |
| ConnectStore (RPC over storage) | ✓ | ✓ | — |
| Cron scheduler | ✓ | ✓ | — |
| Timer scanner | ✓ | ✓ | — |
| Janitor | ✓ | ✓ | — |
| Inspector / backfill / dependencies / prefectcompat / temporalcompat | ✓ | partial | — |
| Visual plan validation/digest + plan-versus-record projection | ✓ | ✓ | — |
| Background workers helper (opt-in cron + scanner + janitor in-process) | ✓ | ✓ | — |
| Replay prefetch cache (one List per kind on resume) | ✓ | ✓ | — |

Waits are manual unless the call supplies `PollOptions`: no hidden timer or
scheduler subscription is created by default. A polled wait uses a stable
caller-supplied timer ID and the same at-least-once due scanner as sleeps and
durable activity retries. Event delivery is a separate create-once operation;
polling controls when a run rereads the event, not how the payload is written.

The Rust SDK is **storage + minimal workflow runtime** today. The runtime
layers above (claims, durable timers, retries-as-timers, concurrency keys,
ConnectRPC, the operator adapters) need their own iterations. Reading every
canonical record kind is supported. Rust workflow/activity records use the
shared format, but its low-level timer and claim writers are not safe
substitutes for the Go/Python latest-pointer, timer-WAL, and conditional-claim
boundaries.

The TypeScript package is not in the runtime matrix. Its root export ships
generated `temporaless.v1` protobuf types plus `ConnectStore` /
`ConnectQueryStore` wrappers for browser or Node Connect transports, including
visual-plan validation/digest and run projection helpers. Its
`@jim-technologies/temporaless/invariant` subpath uses
`@jim-technologies/invariant-protocol` to project the same descriptor into MCP,
CLI, HTTP/Connect, and descriptor-backed tool catalogs. It is for clients,
inspectors, dashboards, and application services that need the canonical RPC
contract without executing workflows locally.

## Install from Git

Git is the only distribution channel for Temporaless-owned packages; they are
not published to PyPI, the npm registry, crates.io, or another language
registry. Install every SDK directly from the same immutable Git revision:

```sh
go get github.com/jim-technologies/temporaless@COMMIT_SHA
pip install "temporaless @ git+ssh://git@github.com/jim-technologies/temporaless.git@COMMIT_SHA#subdirectory=core/py"
npm install --allow-git=all "github:jim-technologies/temporaless#COMMIT_SHA"
```

```toml
temporaless = { git = "ssh://git@github.com/jim-technologies/temporaless.git", rev = "COMMIT_SHA", package = "temporaless" }
```

Replace `COMMIT_SHA` with the exact revision you deploy so every language uses
the same immutable source revision.

All Temporaless packages share the release version in the root `VERSION` file.
A release has one repository tag, `vX.Y.Z`, which applies to Go, Python core,
every Python adapter, Rust, and TypeScript together. There are no per-language
or per-adapter tag streams. Use `make version-set VERSION=X.Y.Z` to prepare a
new version; the repository gate rejects any manifest, lockfile, dependency
constraint, or tag that drifts from it.

## Adapter audit

All adapters either ship for both Go and Python today or are tracked as
runway. Nothing is on the kill list — every adapter has a clear, narrow
reason to exist (storage RPC, claim coordination, scheduling primitive,
compatibility target, operations helper).

| Adapter | Purpose | Go | Python | Rust |
|---|---|:-:|:-:|:-:|
| `connectstore` | `RecordStoreService` over ConnectRPC; client wraps service back as a `Store` | ✓ | ✓ | — |
| `gocdkclaims` (Go) / `OpenDALStore.try_create_claim` (Py) | Create-only claims via blob `IfNotExist` (S3/GCS native atomicity) | ✓ | ✓ | — |
| `temporalcompat` | Run Temporaless-shaped handlers on the real Temporal SDK | ✓ | ✓ | — |
| `prefectcompat` | Run Temporaless-shaped handlers as Prefect 3 flows/tasks | — | ✓ | — |
| `timerscanner` | Find due sleep, activity-retry, and poll timers belonging to in-flight workflows | ✓ | ✓ | — |
| `cronscheduler` | In-process cron with stateless seeding from existing runs | ✓ | ✓ | — |
| `inspector` | List in-flight/failed workflows, reset records for re-execution | ✓ | ✓ | — |
| `visualization` | Validate/digest optional `WorkflowPlan`, inspect a run, and project node IDs onto durable record evidence | ✓ | ✓ | — |
| `janitor` | Sweep COMPLETED runs older than max-age | ✓ | ✓ | — |
| `backfill` | Run a workflow over many run_ids with bounded concurrency + report | ✓ | ✓ | — |
| `dependencies` | Cross-pipeline durable wait — `WaitForWorkflow(...)` | ✓ | ✓ | — |
| `outbox` | Stable idempotency key per `(workflow_id, run_id, activity_id)` | ✓ | ✓ | — |
| `background` | Opt-in cron + scanner + janitor in-process per replica | ✓ | ✓ | — |
| `dispatch` | Fire-and-forget pool for gRPC-shaped handlers — `DoAsync(method, req)` + graceful drain on shutdown (15s default). Options come from proto `DispatchOptions`. Default in-process; pluggable `Queue` interface lets users plug Kafka / Rabbit / NATS / SQS via a ~50-line adapter. | ✓ | ✓ | ✓ |

Python's operations adapters (`timerscanner`, `cronscheduler`, `inspector`,
`visualization`, `janitor`, `backfill`, `dependencies`, `outbox`, `background`) live inside
`core/py/src/temporaless/` rather than `adapters/py/` because they have
no third-party deps; `prefectcompat` and `temporalcompat` need their own
heavyweight deps so they ship as separate uv projects under
`adapters/py/`. The smaller `connectworkflow` transport boundary is also a
separate uv project so the core replay package does not own ConnectRPC handler
policy.

## Choosing a language

- **Vendor-bound LLM / quant / ML workflows** — Python. The per-record
  overhead is dwarfed by network round-trips; you get the full runtime
  including durable retries, concurrency keys, all the adapters.
- **High-rate webhook receivers / ingest pipelines** — Go. Same full
  runtime; ~30× faster than Python at the storage layer.
- **Rust-native tooling on the bucket** — Rust. Analytics CLIs, MCP
  servers, inspector dashboards, custom adapters. ~2× faster than Go at
  the storage layer, and you can author + replay simple workflows today.
- **Browser/Node clients and inspectors** — TypeScript. Generated protobuf
  types and ConnectRPC clients for app code; Node-only invariantprotocol
  projection for MCP/CLI/HTTP tool surfaces. No local workflow replay runtime.
- **Mixed deployments** — pick per workflow. The wire format is the
  contract; the SDK is the implementation choice.

## Versioning the SDKs

The proto definitions in `api/temporaless/v1/temporaless.proto` are the
contract. SDKs follow the proto: any time you add a field, generated SDKs and
client packages inherit it via `buf generate`. Most ergonomic helpers (the `activity()`
shortcut, the outbox idempotency-key helper, the ConnectRPC wrappers) are
purely language-local — they're allowed to diverge in idiom, but their
output (the records they write) must match.

When in doubt, the protobuf is the source of truth. SDKs are convenient
ways to write and read the same bytes.
