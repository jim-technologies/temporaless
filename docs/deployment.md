# Deployment

Temporaless has no engine to deploy. What you deploy is:

1. A **Store** backend (S3, GCS, Azure Blob, etc.) shared across processes.
2. **Triggers** — any process that calls `workflow.Run` (a gRPC handler, a cloud function, a queue worker).
3. Optional periodic jobs — **cron scheduler**, **timer scanner**, and, when you use an index, **janitor** — run as cron jobs, Kubernetes CronJobs, EventBridge schedules, etc.
4. Optional **query index** for workflow listing, inspector views, and indexed retention sweeps.

This doc walks through the standard production layout.

## 1. Production Store

Swap the OpenDAL `fs` scheme for a cloud one. Atomic write semantics — important for concurrent writers — come from the cloud object store's native API.

### Go

```go
import (
    s3 "github.com/apache/opendal-go-services/s3"
    opendal "github.com/apache/opendal/bindings/go"
    "github.com/jim-technologies/temporaless/core/go/storage"
)

operator, _ := opendal.NewOperator(s3.Scheme, opendal.OperatorOptions{
    "bucket":   "prod-temporaless",
    "region":   "us-east-1",
    "endpoint": "https://s3.amazonaws.com",
})
store := storage.NewOpenDALStore(operator)
```

### Python (async-only end-to-end)

```python
import asyncio
import opendal

from temporaless import OpenDALStore, Options, Workflow, run

operator = opendal.AsyncOperator(
    "s3",
    bucket="prod-temporaless",
    region="us-east-1",
    endpoint="https://s3.amazonaws.com",
)
store = OpenDALStore(operator)


async def my_workflow(workflow: Workflow, input):
    ...


asyncio.run(run(store, Options(...), input, ResultType, my_workflow))
```

Python workflow bodies, activity bodies, and the entire storage / ConnectRPC surface are `async def`. Sync callables are rejected at wrap time. See `AGENTS.md` for the rationale.

For the storage RPC server, mount `asgi_application(store)` on any ASGI runner — `uvicorn`, `hypercorn`, etc:

```python
import uvicorn

from temporaless import asgi_application

app = asgi_application(store)
uvicorn.run(app, host="0.0.0.0", port=8080)
```

`asgi_application(store)` exposes the core `RecordStoreService`: point
GET/PUT/DELETE operations, run-scoped lists, latest-run pointer reads, and the
due-timer ledger. If you also deploy a query index, mount
`query_asgi_application(indexed_store)` separately for `RecordQueryService`.

### Auth, rate limiting, tracing — standard ConnectRPC interceptors

Both the server (`asgi_application`) and the client (`ConnectStore.from_address`) forward the standard ConnectRPC `interceptors=[...]` slot. Anything you'd already write for a gRPC/ConnectRPC service drops in unchanged — the framework's storage surface is just another ConnectRPC service.

`connectrpc.interceptor.Interceptor` is a *Union* of the four interceptor Protocols (`UnaryInterceptor`, `MetadataInterceptor`, `ServerStreamInterceptor`, `BidiStreamInterceptor`) — implement the one that matches your need. For auth, `MetadataInterceptor` is the right shape: it only reads headers, fires before the handler, and lets you reject unauthenticated calls without touching request/response bodies.

```python
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.request import RequestContext


class BearerTokenAuth:
    """Implements the MetadataInterceptor Protocol structurally — there is no
    base class to subclass. Reject before the handler runs."""

    def __init__(self, token: str) -> None:
        self._token = token

    async def on_start(self, ctx: RequestContext) -> dict:
        authz = ctx.request_headers().get("authorization", "")
        if not authz.startswith("Bearer ") or authz[7:] != self._token:
            raise ConnectError(Code.UNAUTHENTICATED, "missing or invalid token")
        return {}

    async def on_end(self, _token: dict, _ctx: RequestContext, _error: Exception | None) -> None:
        return


# Server side: every RecordStoreService RPC flows through it.
app = asgi_application(store, interceptors=[BearerTokenAuth(token=secret), RateLimit()])

# Client side: outgoing requests carry whatever the interceptor adds.
remote = ConnectStore.from_address(
    "https://prod-temporaless.internal",
    interceptors=[BearerTokenClientInterceptor(token=read_token())],
    timeout_ms=5_000,
)
```

A complete runnable production server — auth, structured JSON logging with correlation IDs, `/healthz` + `/readyz` for any platform's health probes, graceful shutdown on SIGTERM — lives at `examples/py/production_server.py`.

The trigger surface — your own `WorkflowService` that calls `workflow.run` — is also a normal ConnectRPC service, so the same interceptors apply there. There's no framework-specific auth model to learn.

### Why cloud over fs

S3 and GCS provide atomic `PutObject` (the new object becomes visible only after upload). They also support native `If-None-Match: *` / `ifGenerationMatch=0` — true atomic create-if-absent. Without those, concurrent writers can corrupt records or break claim coordination.

The OpenDAL `fs` scheme is intentionally not safe for concurrent writers; it's for development and small single-process deployments only. See `docs/hard-cases.md` for details.

### Claim coordination in production

Set `WorkflowOptions.claim_owner_id` to opt a run into workflow-execution and activity claims. The runtime creates `claim/workflow:execution.binpb` before entering missing or `IN_PROGRESS` workflow work. Another live invocation of the same `(namespace, workflow_id, run_id)` gets `ClaimBusyError` (`ALREADY_EXISTS`); a terminal workflow record still replays without waiting for the claim. Supplying a claim store alone does not opt in, and an empty `claim_owner_id` retains at-least-once overlapping execution.

The runtime first checks `GetStoreCapabilities`. A store reporting `NO_CLAIMS` or `UNSPECIFIED` rejects `claim_owner_id` and `concurrency_key` with a failed-precondition error; requested coordination is never silently ignored. `concurrency_key` requires `claim_owner_id`, and that caller-owned value is stored on its slot claim too. It is diagnostic metadata, not a re-entry token—matching owners still contend.

For claim coordination across processes:

- Bundled `adapters/go/gocdkclaims` uses GoCDK Blob's `WriterOptions.IfNotExist`. Driver-dependent atomicity:
  - GCS, S3: native atomic — multi-process safe
  - fileblob: process-local mutex compensates for the Stat-then-Rename race; multi-process not safe

- For S3-native preconditions without the GoCDK indirection: write a thin `storage.ClaimStore` against the AWS SDK using `If-None-Match: *`. Same pattern for GCS with `ifGenerationMatch=0`. Either is small.

The bundled stores are create-only. Workflow claims are deleted on orderly exits. Activity claims are deleted after a terminal record or fully persisted retry boundary, but retained when cancellation or persistence failure makes an activity outcome ambiguous. Cleanup errors are always surfaced. An activity-claim cleanup failure interrupts the body and leaves the workflow `IN_PROGRESS`; an invocation-claim cleanup failure after terminal persistence leaves the stored workflow `COMPLETED` or `FAILED`. A process crash or failed cleanup can leave a claim behind, and its `lease_expires_at` timestamp does not make takeover safe. Verify that no worker is still live and delete the exact claim through `ClaimStore.delete_claim` / `DeleteClaim`. CAS renewal and takeover are not implemented by the current core.

## 2. Mounting workflows as gRPC / ConnectRPC methods

The framework's design is: **you write a normal gRPC handler, the framework decorates it as a workflow**. There is no Temporaless-specific handler shape. The same handler can be triggered over gRPC, ConnectRPC, gRPC-Web, or — once an `invariantprotocol`-style adapter is wrapped — CLI / HTTP / MCP.

For application services, keep that normal handler callable directly. Temporaless should be an opt-in durable wrapper around idempotent, retriable, scheduled, or long-running work; it should not sit on the critical path for ordinary API reads or routine synchronous actions. If the Temporaless store, timer scanner, query index, or background operators are unavailable, the service should continue serving direct in-process APIs and return an explicit unavailable/deferred result only for the workflow-backed operation.

### Go: `workflow.HandleConnect`

```go
import (
    "connectrpc.com/connect"
    "net/http"
    pricesv1connect "your/package/pricesv1connect"
    "github.com/jim-technologies/temporaless/core/go/workflow"
)

type pricesService struct {
    store storage.Store
}

func (s *pricesService) FetchPrices(
    ctx context.Context,
    req *connect.Request[pricesv1.FetchRequest],
) (*connect.Response[pricesv1.FetchResponse], error) {
    return workflow.HandleConnect(ctx, req, workflow.WorkflowWrapOptions[*pricesv1.FetchRequest, *pricesv1.FetchResponse]{
        Store: s.store,
        OptionsFor: func(_ context.Context, r *pricesv1.FetchRequest) (*workflow.Options, error) {
            return &workflow.Options{
                WorkflowId:  "prices:" + r.GetSymbol(),
                RunId:       r.GetRunId(),  // caller-provided
                CodeVersion: codeVersion(),
            }, nil
        },
        NewResult: func() *pricesv1.FetchResponse { return &pricesv1.FetchResponse{} },
        Execute:   fetchPricesBody, // func(ctx, *FetchRequest) (*FetchResponse, error)
    })
}

mux := http.NewServeMux()
mux.Handle(pricesv1connect.NewPricesServiceHandler(&pricesService{store: store}))
http.ListenAndServe(":8080", mux)
```

`HandleConnect` unwraps the `connect.Request`, runs `workflow.Run` with replay semantics, and wraps the response — saving the boilerplate while keeping the handler signature 100% standard ConnectRPC. Inside `fetchPricesBody`, call `workflow.ExecuteActivity(ctx, ...)`, `workflow.Sleep`, `workflow.WaitEvent` like any other workflow — the in-flight workflow rides on `ctx`.

It also auto-maps framework typed errors to `*connect.Error` with the right gRPC code (`TimerPendingError`/`EventPendingError` → `Unavailable`, `ClaimBusyError` → `AlreadyExists`, claim-release failures / activity failures → `Internal`, claim-capability and record conflicts → `FailedPrecondition`). The original error is preserved via wrapping, so `errors.As(err, &workflow.TimerPendingError{...})` still recovers it. Unknown errors pass through unchanged.

### Python: `@wrap_workflow_method`

```python
from connectrpc.request import RequestContext

from temporaless import (
    ActivityOptions,
    Options,
    Store,
    current_workflow,
    wrap_workflow_method,
)


class PriceService:
    """Mount on a ConnectRPC ASGI app exactly like any other service. The
    decorator handles workflow registration; the method signature stays
    standard ConnectRPC."""

    def __init__(self, store: Store) -> None:
        self._store = store

    @wrap_workflow_method(
        store=lambda self: self._store,
        result_type=FetchResponse,
        options_for=lambda self, r: Options(
            workflow_id=f"prices:{r.symbol}",
            run_id=r.run_id,
            code_version="v1",
        ),
    )
    async def fetch_prices(
        self, request: FetchRequest, ctx: RequestContext
    ) -> FetchResponse:
        # Inside the body, current_workflow() returns the active Workflow so
        # activities, sleeps, and waits compose naturally.
        return await current_workflow().execute_activity(
            ActivityOptions(activity_id=f"vendor:{request.symbol}"),
            request,
            FetchResponse,
            _vendor_fetch,
        )
```

Any client speaking gRPC / ConnectRPC / gRPC-Web can now trigger the workflow. Terminal duplicate calls with the same `workflow_id + run_id` replay from storage. To prevent two live calls from entering a missing or `IN_PROGRESS` run together, configure a claim store and set a caller-provided `claim_owner_id`; the loser receives `ALREADY_EXISTS`. Without that opt-in, overlapping execution is at-least-once. The "any server can trigger a workflow" model is literal — the framework only provides decoration.

`@wrap_workflow_method` also auto-maps framework typed errors to `ConnectError` (`TimerPendingError`/`EventPendingError` → `UNAVAILABLE`, `ClaimBusyError` → `ALREADY_EXISTS`, claim-release failures / activity failures → `INTERNAL`, claim-capability and record conflicts → `FAILED_PRECONDITION`). The original exception is attached via `__cause__`, so `except ConnectError as e: e.__cause__` recovers the underlying type. Unknown exceptions propagate unchanged so application errors keep their full traceback.

### Mapping framework errors to gRPC codes

When the workflow body raises one of the framework's typed errors, `HandleConnect` (Go) and `@wrap_workflow_method` (Python) translate to the standard mapping below. If you drive `WrapWorkflow` / `run` directly from a non-Connect transport, call `workflow.ErrorToConnectCode` (Go) or `temporaless.workflow_error_to_connect_code` (Python) yourself.

| Framework error                               | gRPC code            |
|-----------------------------------------------|----------------------|
| `TimerPendingError`, `EventPendingError`      | `UNAVAILABLE`        |
| `ClaimBusyError`                              | `ALREADY_EXISTS`     |
| `ClaimReleaseError` / Go `ErrClaimRelease`   | `INTERNAL`           |
| `ClaimCapabilityError`                       | `FAILED_PRECONDITION`|
| `WorkflowConflictError`, `ActivityConflictError`, `TimerConflictError` | `FAILED_PRECONDITION` |
| `ActivityError`                               | `INTERNAL`           |

## 3. Periodic jobs

### Cron scheduler

For interval-driven workflows (hourly stocks fetches, daily summaries):

```go
scheduler, _ := cronscheduler.New(
    []cronscheduler.Schedule{
        {ID: "prices:aapl", Expression: "* * * * 1-5"},
        {ID: "prices:tsla", Expression: "*/5 * * * 1-5"},
    },
    func(ctx context.Context, scheduleID string, fireTime time.Time) error {
        _, err := workflow.Run(ctx, store, &workflow.Options{
            WorkflowId:  scheduleID,
            RunId:       fireTime.UTC().Format(time.RFC3339),
            CodeVersion: codeVersion(),
        }, nil, /* input */, /* newResult */, /* body */)
        return err
    },
)

// Stateless seeding from latest-run pointer objects — no separate persistence.
snapshot, _ := cronscheduler.LastFiresFromRuns(ctx, store, "",
    []string{"prices:aapl", "prices:tsla"}, time.RFC3339)
scheduler.Restore(snapshot)

// Run a Tick on a 1-minute cron / Kubernetes CronJob / EventBridge schedule.
scheduler.Tick(ctx, time.Now())
```

The Python equivalent lives in `temporaless.cronscheduler` and uses the same Snapshot / Restore / LastFiresFromRuns vocabulary. The dispatcher is `async def` because Python is async-only:

```python
from datetime import UTC, datetime

from temporaless.cronscheduler import (
    Schedule,
    Scheduler,
    last_fires_from_runs,
)
from temporaless.workflow import Options, run


async def dispatch(schedule_id: str, fire_time: datetime) -> None:
    await run(
        store,
        Options(
            workflow_id=schedule_id,
            run_id=fire_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            code_version=code_version(),
        ),
        input_message,
        ResultType,
        workflow_body,
    )


scheduler = Scheduler(
    [
        Schedule(id="prices:aapl", expression="* * * * 1-5"),
        Schedule(id="prices:tsla", expression="*/5 * * * 1-5"),
    ],
    dispatch,
)

# Stateless seeding from latest-run pointer objects — no separate persistence.
snapshot = await last_fires_from_runs(
    store, "", ["prices:aapl", "prices:tsla"], "%Y-%m-%dT%H:%M:%SZ"
)
scheduler.restore(snapshot)

# Run a tick on a 1-minute cron / Kubernetes CronJob / EventBridge schedule.
await scheduler.tick(datetime.now(UTC))
```

Two cron-scheduler processes may dispatch the same fire concurrently. Terminal reruns replay, but suppressing overlapping first execution requires the dispatched `WorkflowOptions` to include `claim_owner_id` and an atomic claim store. Without that opt-in, scheduler delivery is at-least-once and activities must remain idempotent.

### Timer scanner

For workflows that use `workflow.Sleep` (durable timers), the core writes a
small due-ledger entry alongside each pending timer. Scanners list that ledger,
not the whole workflow tree:

```go
due, _ := timerscanner.DueTimers(ctx, store, time.Now())
for _, t := range due {
    // Re-invoke the workflow handler. Do this however your trigger surface works:
    // call your ConnectRPC method, push to a queue, invoke a cloud function, etc.
    triggerWorkflowResume(ctx, t.Workflow.GetKey())
}
```

Run on a cadence (every minute, every 30s — depends on the smallest timer interval you care about). The ledger contains only pending timers and self-prunes when timers fire or are cancelled.

### Janitor

Bucket-only deployments should prefer bucket lifecycle rules for broad
retention. Exact "delete completed runs older than X" is a cross-run query and
requires `RecordQueryService` / an index-backed `QueryStore`.

```go
deleted, _ := janitor.Sweep(ctx, queryStore, claimStore, &temporalessv1.SweepRequest{
    Now:    timestamppb.Now(),
    MaxAge: durationpb.New(7 * 24 * time.Hour),
})
log.Printf("janitor swept %d completed runs older than 7 days", deleted)
```

Pass `nil` for `claimStore` only when the query store itself exposes claim
coordination or the deployment declares `NO_CLAIMS`. With a separate GoCDK/S3/GCS
claim backend, pass it explicitly. The janitor prevalidates all eligible run
snapshots and removes run-scoped claims before records. It is not an execution
fence or transaction: externally quiesce eligible runs before the sweep, then
run it daily.

### Operator-vs-handler replicas

If you run periodic jobs in-process (instead of from your platform's
scheduler), every replica polling the bucket is wasteful. Use the
`background` helper to opt in per replica — typically one "operator"
replica enables the loops; the rest are "handler" replicas with no
periodic work.

**Operator replica** (one process per pool, enables all background work):

```go
import "github.com/jim-technologies/temporaless/adapters/go/background"

workers, _ := background.New(store, background.Config{
    Cron:         &background.CronConfig{Scheduler: scheduler},
    TimerScanner: &background.TimerScannerConfig{Dispatch: dispatchDueTimer},
    Janitor:      &background.JanitorConfig{
        MaxAge:    7 * 24 * time.Hour,
        ClaimStore: claimStore, // omit only for NO_CLAIMS or auto-detection
    },
})
if err := workers.Start(ctx); err != nil { /* ... */ }
defer workers.Stop()
// ... serve workflow RPCs ...
```

```python
from temporaless.background import (
    BackgroundWorkers, CronConfig, TimerScannerConfig, JanitorConfig,
)

workers = BackgroundWorkers(
    store,
    query_store=indexed_store,
    cron=CronConfig(scheduler=scheduler),
    timer_scanner=TimerScannerConfig(dispatch=dispatch_due_timer),
    janitor=JanitorConfig(max_age=timedelta(days=7)),
)
await workers.start()
try:
    await server.serve()
finally:
    await workers.stop()
```

**Handler replicas**: skip the helper entirely, or construct it with no
config structs — `Start` becomes a no-op. They just serve workflow RPCs.

Each loop is independently toggleable: a deployment might enable only
the timer scanner on every replica (because durable sleeps are
latency-sensitive) and keep cron + janitor on a single operator replica.

**Why this is opt-in, not leader-elected.** Coordination dances add
complexity the framework explicitly rejects. The simpler answer: deployers
choose which replicas run periodic work. If you mis-configure and two replicas
both run a loop, terminal workflow records replay and indexed sweeps mirror
idempotent run-prefix deletes. Overlapping first execution is serialized only
when those workflow calls opt into `claim_owner_id` with an atomic claim
store; otherwise the duplicate dispatch is at-least-once and must be safe at
the activity boundary.

**Skip the helper entirely** when your platform already provides
scheduled invocations: Lambda + EventBridge, Cloud Run + Cloud Scheduler,
Kubernetes CronJob, GitHub Actions schedule. The platform's
one-fire-per-tick semantics is exactly what this helper provides
in-process; don't pay for both.

## 4. Multi-process and multi-region

### Multi-process within one region

Terminal records serialize replayed results. When `claim_owner_id` is set, the deterministic `workflow:execution` claim also serializes live invocations and activity claims coordinate missing activity work. Without that opt-in, multiple workflow workers, schedulers, and scanners may overlap with at-least-once execution even when they share the same Store. Indexed janitors additionally use the query store. Recommended setup: one process pool per role (Lambda function, Cloud Run service, K8s Deployment, etc.), each scaled horizontally.

### Multi-region active/active

The Store must replicate across regions. S3 cross-region replication, GCS multi-region buckets, etc. Eventual consistency is acceptable: workflows are keyed by `workflow_id + run_id`, so two regions writing the same record converge.

Conflict points:
- **Claim creation**: native `If-None-Match` / generation matches are atomic per-region but not cross-region. For active/active claims, partition workflows by region (route `workflow_id` prefix → home region) and let CRR catch up the records.
- **Workflow records during execution**: do not rely on last-writer-wins to suppress side effects. Route each workflow identity to one home region or use a coordination system with cross-region atomicity.

### Cold standby

If multi-region is overkill, run hot in one region with regular S3/GCS replication to a backup region. On failover: point the Store at the backup. No state migration needed — records are the state.

## 5. Code versioning

Set `TEMPORALESS_CODE_VERSION` to an immutable build identity (git SHA, release tag). On rolling deploys, both old and new versions run concurrently for a window:

- Each version writes records with its own `code_version`.
- Records written by old code are not replayable by new code if the workflow type or input shape changed (mismatch fails loudly).
- For breaking workflow changes, bump `workflow_id` (e.g. `prices:aapl@v2`) to fork the run history.

## 6. Observability

Two surfaces, each at the right layer:

### Durable annotations (persisted on the record)

```python
from temporaless import annotate

annotate("model", "claude-opus-4-7")
annotate("tokens_in", "1024")
annotate("vendor", "anthropic")
```

```go
workflow.Annotate(ctx, "model", "claude-opus-4-7")
```

Stored on the `ActivityRecord` / `WorkflowRecord` and visible via `inspector.ListWorkflowsByStatus`, `ListActivities`, etc. Use for fields you want to query *after* the workflow completes — audit, billing, LLM token accounting.

### Real-time tracing / metrics / logging

Workflow triggers are gRPC methods (`@wrap_workflow_method` / `workflow.HandleConnect`). Use **standard ConnectRPC interceptors** for per-call observability — auth, rate limit, tracing, metrics, structured logging all use the same surface that the rest of your gRPC service mesh uses. There is nothing Temporaless-specific to learn.

```python
from connectrpc.interceptor import Interceptor
from temporaless import asgi_application

class TracingInterceptor:
    async def intercept_unary(self, call_next, request, ctx):
        with tracer.start_as_current_span(ctx.method.name):
            return await call_next(request, ctx)

app = asgi_application(store, interceptors=[TracingInterceptor()])
```

For activity-level spans inside a workflow body, use your tracer directly inline (`with tracer.start_as_current_span(...)`) — the same way you would in any other Python async function. The framework intentionally does not provide a parallel observer surface; gRPC interceptors and inline tracer calls cover every case.

## 7. Failure modes

- **Process crash mid-activity**: activity record stays missing. Next invocation re-executes (or claims block if configured).
- **Process crash mid-retry**: `RETRYING` record is persisted between attempts. Next invocation resumes from the next attempt index, attempts list preserved.
- **Process crash mid-workflow without execution claims**: `IN_PROGRESS` remains and the next invocation re-executes the body; completed activities replay from their records.
- **Process crash with a create-only execution claim**: `workflow:execution` may remain and later invocations return `ClaimBusyError`, even after its lease timestamp. After confirming the old worker is gone, delete the exact claim through `ClaimStore.delete_claim` / `DeleteClaim`. CAS takeover is future-only.
- **Storage unavailable**: `workflow.Run` returns the storage error. Caller's responsibility to retry.
- **Schedule missed during outage**: `cronscheduler.Tick` catches up — it dispatches every fire time between `last_fire` and `now`.
- **Query index unavailable**: workflow execution, cron pointer seeding, and durable timer resumption continue. Listing, inspector search, and indexed sweeps fail until the index is back or rebuilt.

## 8. Deployment shape

The framework is platform-agnostic. The pieces you actually deploy:

```
Workflow service     N replicas    serves the ConnectRPC trigger surface
Cron scheduler tick  1/min         calls Scheduler.tick(now)
Timer scanner tick   1/min         calls DueTimers + dispatches re-invocations
Janitor (optional)   1/day         quiesces eligible runs, then calls claim-aware RecordQueryService.Sweep
Query index           optional      SQLite adapter for search and indexed retention
Code version env     git short SHA bumped on breaking workflow body changes
Storage credentials  S3 / GCS / Azure Blob via your secret manager
Bucket               production-temporaless
```

Pick whichever platform you already operate:

- **Serverless (recommended for new deploys):** AWS Lambda + API Gateway + EventBridge schedules, or Cloud Run + Cloud Scheduler, or Modal, or Fly Machines. The 3 "tick" jobs map to scheduled functions.
- **Containers:** ECS / Cloud Run / Knative — the same image runs the workflow service; the ticks are scheduled invocations.
- **VMs + cron:** the workflow service is a long-lived `uvicorn` process; the ticks are cron(8) entries.
- **Kubernetes:** Deployment for the workflow service, three CronJobs for the ticks. Most heavyweight option; pick only if you already operate K8s for unrelated reasons.

The state lives in the bucket. The processes are interchangeable.
