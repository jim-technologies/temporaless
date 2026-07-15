# Getting Started

Temporaless is a thin, storage-first workflow framework. There is no engine, no control plane, no central server — workflows are protobuf-shaped functions, every boundary writes a protobuf record, and replay decides whether to re-execute.

This guide walks the whole shape end-to-end. Examples lead with Python (async-only) and follow with the Go equivalent. The shapes are identical at every call site.

## 1. Set up the dev environment

```sh
flox activate
```

Flox owns Go, Python 3.14, uv, buf, and the C runtime libs OpenDAL and Protovalidate need. Everything else lives in `go.mod` or `core/py/uv.lock`.

## 2. Pick a storage backend

The default is OpenDAL's filesystem driver. Workflow records, activities, timers, events, and claims all serialize to protobuf binary at predictable paths. Swap the scheme for any cloud object store (S3, GCS, Azure Blob) when going to production.

```python
import opendal
from temporaless import OpenDALStore

operator = opendal.AsyncOperator("fs", root="/tmp/temporaless")
store = OpenDALStore(operator)
```

```go
import (
    "github.com/apache/opendal-go-services/fs"
    opendal "github.com/apache/opendal/bindings/go"
    "github.com/jim-technologies/temporaless/core/go/storage"
)

operator, _ := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": "/tmp/temporaless"})
store := storage.NewOpenDALStore(operator)
```

## 3. Write a workflow

A workflow is a unary protobuf RPC handler: one request message in, one response message out. In Python, it's `async def`. The activity inside is the same shape — request in, response out.

```python
from google.protobuf.wrappers_pb2 import StringValue
from temporaless import ActivityOptions, Workflow


async def call_the_model(prompt: StringValue) -> StringValue:
    # Real LLM call here.
    return StringValue(value=f"answered:{prompt.value}")


async def answer(workflow: Workflow, prompt: StringValue) -> StringValue:
    return await workflow.execute_activity(
        ActivityOptions(activity_id="llm:complete"),
        prompt,
        StringValue,
        call_the_model,
    )
```

```go
import (
    "context"
    "github.com/jim-technologies/temporaless/core/go/workflow"
    "google.golang.org/protobuf/types/known/wrapperspb"
)

func answer(ctx context.Context, prompt *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
    return workflow.ExecuteActivity(
        ctx,
        &workflow.ActivityOptions{ActivityId: "llm:complete"},
        prompt,
        func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
        callTheModel,
    )
}
```

Every workflow and activity is identified by caller-provided IDs. The framework validates IDs as path-safe ASCII but never generates them — they belong to your domain (`prices:aapl`, `tweet:12345`, `llm:answer`).

## 4. Run it

```python
from temporaless import Options, run

result = await run(
    store,
    Options(
        workflow_id="llm:answer",
        run_id="2026-05-04-r1",
        code_version="v1",
    ),
    StringValue(value="Why is the sky blue?"),
    StringValue,
    answer,
)
```

```go
result, err := workflow.Run(
    ctx,
    store,
    &workflow.Options{
        WorkflowId:  "llm:answer",
        RunId:       "2026-05-04-r1",
        CodeVersion: "v1",
    },
    nil, // no claim store
    wrapperspb.String("Why is the sky blue?"),
    func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
    answer,
)
```

What happens on first call:

1. `run` writes a `WorkflowRecord{status: IN_PROGRESS}`.
2. The workflow body executes. Each `execute_activity` either replays a stored result or runs the activity and writes an `ActivityRecord{status: COMPLETED, result: ...}`.
3. `run` updates the workflow record to `COMPLETED` with the result.

Re-invoking the same `(workflow_id, run_id, code_version)` short-circuits everything: the stored result is returned without re-executing the body or any activity. The user-supplied ids are the contract — pick distinct ids when you want distinct executions.

## 5. Retry transient failures

Activities take an optional `RetryPolicy`. Authors raise typed errors with stable codes for retry-policy matching.

```python
from datetime import timedelta
from google.protobuf.duration_pb2 import Duration
from temporaless import ActivityError, RetryPolicy

initial = Duration()
initial.FromTimedelta(timedelta(seconds=1))


async def call_the_model_retrying(prompt: StringValue) -> StringValue:
    if rate_limited():
        raise ActivityError("rate_limited", "vendor 429")
    return await call_the_model(prompt)


await workflow.execute_activity(
    ActivityOptions(
        activity_id="llm:complete",
        retry_policy=RetryPolicy(
            maximum_attempts=3,
            initial_interval=initial,
            backoff_coefficient=2.0,
            non_retryable_error_codes=["invalid_argument"],
        ),
    ),
    prompt,
    StringValue,
    call_the_model_retrying,
)
```

```go
workflow.ExecuteActivity(ctx, &workflow.ActivityOptions{
    ActivityId: "llm:complete",
    RetryPolicy: &temporalessv1.RetryPolicy{
        MaximumAttempts:        3,
        InitialInterval:        durationpb.New(time.Second),
        BackoffCoefficient:     2.0,
        NonRetryableErrorCodes: []string{"invalid_argument"},
    },
}, prompt, newReply, callTheModelRetrying)
```

Each attempt is recorded on `ActivityRecord.attempts`. If retries are exhausted, the runtime writes `ActivityRecord{status: FAILED, failure: ..., attempts: [...]}`. Subsequent invocations replay the failure rather than re-executing.

A `RETRYING` record is persisted between attempts, so a process death mid-retry doesn't lose the attempt history — the next invocation resumes from the next attempt index.

For a partitioned job, assign one stable activity ID per batch. Completed
batches replay from their records, while only failed/incomplete activities use
their remaining retries. If an activity exhausts retries, the workflow is
terminally failed; to operator-retry only those partitions, reset their failed
activity records and paired retry timers while the run is quiesced, reset the
parent workflow record last, then invoke the same run once. Core does not add
an implicit workflow-level retry loop. See the exact repair/reset sequence in
`docs/runbook.md`; valid `RETRYING` records should be re-invoked for automatic
timer repair before an operator deletes them.

## 6. Durable sleep

Long pauses do not block a process. `workflow.sleep` writes a `TimerRecord` and raises `TimerPendingError`; a scheduler reinvokes the workflow after the timer fires.

```python
from datetime import timedelta

await workflow.sleep("wait:vendor-window", timedelta(hours=1))
```

```go
if err := workflow.Sleep(ctx, "wait:vendor-window", time.Hour); err != nil {
    return nil, err
}
```

The bundled timer scanner walks due timers and lets the caller dispatch reinvocations. It's a thin wrapper over the `DueTimers` RPC on `RecordStoreService` — the same shape works locally or against a remote `ConnectStore`.

```python
from temporaless.timerscanner import due_timers

for timer in await due_timers(store, datetime.now(UTC)):
    # Re-invoke run() for timer.workflow's key.
    ...
```

## 7. Wait for an external event

`Workflow.wait_event` raises `EventPendingError` until an external service writes the corresponding `EventRecord`. The signal flow stays storage-first: webhook handler → `send_event` → workflow resumes on its next reinvocation.

```python
from temporaless import EventKey, send_event

# Inside the workflow body:
decision = await workflow.wait_event("approval", StringValue)

# In the webhook handler that delivers the approval:
await send_event(
    store,
    EventKey(workflow_id="twitter:moderate", run_id="tweet-12345", event_id="approval"),
    StringValue(value="approved"),
)
```

## 8. Schedule it

For interval-driven workflows (stocks, periodic LLM batches), the bundled cron scheduler dispatches at fire times. Convention: run IDs embed the fire time so reruns and backfills are explicit.

```python
from temporaless.cronscheduler import Schedule, Scheduler


async def dispatch(schedule_id: str, fire_time):
    await run(
        store,
        Options(
            workflow_id=schedule_id,
            run_id=fire_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            code_version="v1",
        ),
        StringValue(value="AAPL"),
        StringValue,
        fetch_prices,
    )


scheduler = Scheduler(
    [Schedule(id="prices:aapl", expression="* * * * *")], dispatch
)
await scheduler.tick(datetime.now(UTC))
```

## 9. Annotate for observability

Activities and workflow bodies attach durable structured metadata to their record. Annotations survive replay because they live on the stored record.

```python
from temporaless import annotate

annotate("model", "claude-opus-4-7")
annotate("tokens", "1024")
```

```go
workflow.Annotate(ctx, "model", "claude-opus-4-7")
workflow.Annotate(ctx, "tokens", "1024")
```

For real-time tracing / metrics / structured logging, use **standard ConnectRPC interceptors** on the workflow trigger surface — workflows ARE gRPC methods, so anything you'd write for a normal gRPC service drops in unchanged. See `docs/deployment.md` for the recipe. The framework deliberately does not provide a parallel observer surface.

## 10. Operator visibility

`temporaless.inspector` uses the optional query index for cross-run visibility and the core store for reset operations:

```python
from temporaless.inspector import (
    list_in_flight_workflows,
    list_failed_workflows,
    list_activities,
    reset_workflow,
    reset_activity,
)
from temporaless_indexstore import IndexedStore

query_store = IndexedStore.from_opendal(operator, "/var/temporaless/index.sqlite")

in_flight = await list_in_flight_workflows(query_store)
failed = await list_failed_workflows(query_store)
activities = await list_activities(store, workflow_key)

# With the run quiesced, reset failed children first; preserve successful ones.
# Delete a failed activity's validated paired retry timer first, when present.
await reset_activity(store, activity_key)
await reset_workflow(store, workflow_key)  # parent is always deleted last
```

Point operations are reachable as gRPC RPCs on `RecordStoreService` (via `ConnectStore`). Cross-run listing is reachable through optional `RecordQueryService` when you deploy a query index.

## 11. Retention

Bucket-only deployments can use conservative lifecycle rules for archive
retention, but those rules must preserve active run records and `_due`
write-ahead records for at least the maximum timer horizon plus the
scheduler-outage/recovery grace period. If timer duration is unbounded, exempt
them. If you deploy the optional
query index, `temporaless.janitor.sweep` deletes COMPLETED runs older than a
max-age threshold by selecting indexed metadata and mirroring deletes to the
bucket:

```python
from datetime import UTC, datetime, timedelta
from temporaless.janitor import sweep

deleted = await sweep(query_store, datetime.now(UTC), timedelta(days=7))
```

## 12. Trigger surface

Workflows are protobuf RPC handlers. Mount them behind anything that speaks protobuf:

- **ConnectRPC service**: decorate a normal service method with `temporaless_connectworkflow.wrap_workflow_method` (Python) or use `connectworkflow.Handle(...)` (Go). The method signature stays standard ConnectRPC; the transport adapter adds replay semantics through the core workflow wrapper.
- **Cloud function**: invoke `run` from the function entrypoint with IDs derived from the request.
- **Cron / queue worker**: same pattern.

```python
from temporaless import ActivityOptions, Options, Store, current_workflow
from temporaless_connectworkflow import WorkflowMethodWrapOptions, wrap_workflow_method


def _store_of(svc): return svc._store
def _options_for(_svc, req): return Options(workflow_id=f"prices:{req.value}", run_id="2026-05-04", code_version="v1")


class PriceService:
    def __init__(self, store: Store) -> None:
        self._store = store

    @wrap_workflow_method(
        WorkflowMethodWrapOptions(
            store=_store_of,
            result_type=StringValue,
            options_for=_options_for,
        )
    )
    async def fetch_prices(self, request: StringValue, ctx) -> StringValue:
        return await current_workflow().execute_activity(
            ActivityOptions(activity_id=f"vendor:{request.value}"),
            request, StringValue, _vendor_fetch,
        )


# Mount on uvicorn:
from temporaless import asgi_application
import uvicorn
uvicorn.run(asgi_application(PriceService(store)._store), port=8080)
```

The "any server can trigger a workflow" model is literal — there is no Temporaless server. Standard ConnectRPC interceptors (auth, rate-limit, tracing) plug in unchanged.

## What you do not get

Temporaless is intentionally smaller than Temporal. It does not offer:

- Multi-signal channels with `select` semantics — events are one-shot, consumed once.
- Query / update RPCs against running workflows.
- Child workflows.
- Sub-second timer accuracy (scanner cadence is ~1 minute by default).
- A UI / control plane / task queue server.
- Asset graph / lineage tracking.

If you need any of those, layer them on as adapters or use a different tool — see `docs/comparisons.md`.

## Examples

| Example | Demonstrates |
|---|---|
| `examples/py/fetch_prices.py` / `examples/go/fetch-prices/` | Hello-world workflow with one activity |
| `examples/py/llm_completion.py` / `examples/go/llm-completion/` | Retry policy + annotations + replay |
| `examples/py/quant_signals.py` | Parallel fan-out via `asyncio.gather` |
| `examples/py/quant_service.py` | Canonical: ConnectRPC service of decorated workflow methods |
| `examples/py/stocks_cron.py` / `examples/go/stocks-pipeline/` | Cron-driven workflows |
| `examples/py/approval_workflow.py` | Long-running: durable sleep + wait-for-event + replay |
| `examples/go/twitter-webhook/` | `WaitEvent` + `SendEvent` |

## Reference

- `docs/philosophy.md` — design tenets in one page (read first)
- `docs/comparisons.md` — Temporaless vs Temporal / Prefect / Dagster / n8n
- `docs/architecture.md` — goals and core model
- `docs/deployment.md` — production deployment patterns
- `docs/storage-rpc.md` — `RecordStoreService` contract
- `docs/scheduling.md` — timer + cron model
- `docs/claims.md` — claim coordination
- `docs/hard-cases.md` — concurrency, retries, side effects
- `docs/adapter-contract.md` — what adapters must declare
- `docs/benchmarks.md` — Go and Python benchmark suites
