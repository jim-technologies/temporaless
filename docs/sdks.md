# SDKs

Temporaless ships three SDKs that share the same wire format. Workflow
records authored in any SDK are readable from any other.

This page documents:

1. The compatibility invariant (what "shares the same wire format" means).
2. The user-facing surfaces, side by side.
3. What each SDK ships today vs. what's still on the runway.

## Compatibility invariant

All three SDKs encode protobuf records identically and write them at
identical Hive-partitioned paths:

```text
temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=workflow/record.binpb
temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=activity/activity_id={aid}/record.binpb
temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=timer/timer_id={tid}/record.binpb
temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=event/event_id={eid}/record.binpb
temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=claim/claim_id={cid}/record.binpb
```

The replay contract is identical across SDKs: a stored record is reused
when `(workflow_id, run_id, code_version, workflow_type)` matches —
likewise `(workflow_id, run_id, activity_id, code_version, activity_type)`
for activities. The user-supplied IDs are the de-duplication key; the
runtime does NOT fingerprint input bytes. If you want a distinct
execution, choose a distinct id.

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
  string, same `code_version`, same id ⇒ the runtime returns the stored
  result on the receiving side.
- A workflow that runs partially in Python, crashes, and is resumed by a
  Go worker re-reading the same bucket completes cleanly — same ids, same
  shape, same replay.
- Inspector tooling written in any SDK works against any bucket.

The cross-language replay test
(`rust_replays_python_authored_workflow_record` in
`core/rs/temporaless/tests/interop.rs`) pre-seeds a `WorkflowRecord` with
the canonical Python-style `workflow_type` and asserts the Rust runtime
replays it without `WorkflowConflict`.

## Surface comparison

The framework's thesis is "a workflow is a decorated gRPC handler" — so
the surfaces converge on three concepts: **construct the store**, **decorate
the handler**, **call activities from the body**. Everything else is
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
| Handler-style | `workflow.HandleConnect(ctx, req, opts)` (ConnectRPC) | `@wrap_workflow_method(options=...)` decorator | (tonic / connect-rs integration: next iteration) |
| Current workflow in nested calls | `workflow.Current(ctx)` | `current_workflow()` | `workflow::current()` (tokio task-local) |
| Annotate | `workflow.Annotate(ctx, key, value)` | `annotate(key, value)` | `workflow::annotate(key, value)` |

Each language uses its idiomatic concurrency primitive: Go's `ctx`, Python's
`contextvars`, Rust's `tokio::task_local!`.

### Activity dispatch

| Concept | Go | Python | Rust |
|---|---|---|---|
| Lowest-level (explicit options) | `workflow.ExecuteActivity(ctx, opts, input, newResult, fn)` | `workflow.execute_activity(opts, input, result_type, fn)` | `workflow::execute_activity(opts, input, fn).await` |
| Ergonomic helper (auto-id + default retry) | `workflow.Activity(ctx, fn, input, opts...)` | `workflow.activity(fn, input)` | `workflow::activity(fn, input).await` |
| Auto-id source | `runtime.FuncForPC` (qualified Go function name) | `func.__qualname__` | `std::any::type_name::<F>()` |
| Default retry policy | `workflow.DefaultRetryPolicy()` | `default_retry_policy()` | `workflow::default_retry_policy()` |

The auto-inferred IDs are sanitized per language to fit the framework's
ID regex (`[A-Za-z0-9._:-]+`). The exact string is language-specific —
two workflows referring to the same activity_id by auto-inference WILL
diverge between Go and Python by design. The activity_id is part of
storage identity; an activity authored in Python should be explicitly
named (`activity_id="fetch:quote"`) if you intend to run replays of it
from another language.

## What ships today

| Capability | Go | Python | Rust |
|---|:-:|:-:|:-:|
| **Storage (read/write all record kinds)** | ✓ | ✓ | ✓ |
| `workflow.run` + replay (terminal short-circuit, IN_PROGRESS resume, fresh execution) | ✓ | ✓ | ✓ |
| `execute_activity` + replay | ✓ | ✓ | ✓ |
| Ergonomic activity helper (auto-id + default retry) | ✓ | ✓ | ✓ |
| Retry policy (attempts, backoff, max interval, non-retryable codes) | ✓ | ✓ | ✓ |
| `Retry-After` from `ActivityFailure.retry_after` | ✓ | ✓ | ✓ |
| Durable retry backoffs (`RetryPolicy.durable_backoff_threshold` → timer record) | ✓ | ✓ | — |
| Concurrency keys (cluster-wide caps via claim slots) | ✓ | ✓ | — |
| Claims (activity-level, GoCDK / OpenDAL backend) | ✓ | ✓ | — |
| Durable timers (`workflow.Sleep`) | ✓ | ✓ | — |
| Wait for event (`workflow.WaitEvent`) | ✓ | ✓ | — |
| Annotate (per-record durable key/value) | ✓ | ✓ | ✓ |
| Outbox idempotency-key helper | ✓ | ✓ | — |
| ConnectRPC handler shape | ✓ | ✓ | — |
| ConnectStore (RPC over storage) | ✓ | ✓ | — |
| Cron scheduler | ✓ | ✓ | — |
| Timer scanner | ✓ | ✓ | — |
| Janitor | ✓ | ✓ | — |
| Inspector / backfill / dependencies / prefectcompat / temporalcompat | ✓ | partial | — |
| Background workers helper (opt-in cron + scanner + janitor in-process) | ✓ | ✓ | — |
| Replay prefetch cache (one List per kind on resume) | ✓ | ✓ | — |

The Rust SDK is **storage + minimal workflow runtime** today. The runtime
layers above (claims, durable timers, retries-as-timers, concurrency keys,
ConnectRPC, the operator adapters) need their own iterations. Read the
storage records from Rust today; run a Rust-authored workflow against
those records as the rest of the SDK lands.

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
| `timerscanner` | Find due durable timers belonging to in-flight workflows | ✓ | ✓ | — |
| `cronscheduler` | In-process cron with stateless seeding from existing runs | ✓ | ✓ | — |
| `inspector` | List in-flight/failed workflows, reset records for re-execution | ✓ | ✓ | — |
| `janitor` | Sweep COMPLETED runs older than max-age | ✓ | ✓ | — |
| `backfill` | Run a workflow over many run_ids with bounded concurrency + report | ✓ | ✓ | — |
| `dependencies` | Cross-pipeline durable wait — `WaitForWorkflow(...)` | ✓ | ✓ | — |
| `outbox` | Stable idempotency key per `(workflow_id, run_id, activity_id)` | ✓ | ✓ | — |
| `background` | Opt-in cron + scanner + janitor in-process per replica | ✓ | ✓ | — |
| `dispatch` | Fire-and-forget pool for gRPC-shaped handlers — `DoAsync(method, req)` + graceful drain on shutdown (15s default). Options come from proto `DispatchOptions`. Default in-process; pluggable `Queue` interface lets users plug Kafka / Rabbit / NATS / SQS via a ~50-line adapter. | ✓ | ✓ | ✓ |

Python's operations adapters (`timerscanner`, `cronscheduler`, `inspector`,
`janitor`, `backfill`, `dependencies`, `outbox`, `background`) live inside
`core/py/src/temporaless/` rather than `adapters/py/` because they have
no third-party deps; `prefectcompat` and `temporalcompat` need their own
heavyweight deps so they ship as separate uv projects under
`adapters/py/`.

## Choosing a language

- **Vendor-bound LLM / quant / ML workflows** — Python. The per-record
  overhead is dwarfed by network round-trips; you get the full runtime
  including durable retries, concurrency keys, all the adapters.
- **High-rate webhook receivers / ingest pipelines** — Go. Same full
  runtime; ~30× faster than Python at the storage layer.
- **Rust-native tooling on the bucket** — Rust. Analytics CLIs, MCP
  servers, inspector dashboards, custom adapters. ~2× faster than Go at
  the storage layer, and you can author + replay simple workflows today.
- **Mixed deployments** — pick per workflow. The wire format is the
  contract; the SDK is the implementation choice.

## Versioning the SDKs

The proto definitions in `api/temporaless/v1/temporaless.proto` are the
contract. SDKs follow the proto: any time you add a field, all three SDKs
inherit it via `buf generate`. Most ergonomic helpers (the `activity()`
shortcut, the outbox idempotency-key helper, the ConnectRPC wrappers) are
purely language-local — they're allowed to diverge in idiom, but their
output (the records they write) must match.

When in doubt, the protobuf is the source of truth. SDKs are convenient
ways to write and read the same bytes.
