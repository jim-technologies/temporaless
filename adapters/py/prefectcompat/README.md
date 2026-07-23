# Prefect Python Compatibility Adapter

Strict compatibility adapter that exposes Temporaless-shaped unary protobuf handlers as Prefect flows and tasks.

It does not emulate the Prefect server. It wraps a Temporaless handler so the same code runs inside Prefect's orchestration (run tracking, UI visibility, scheduling) without changing the handler.

## Why use this

If your team already has Prefect's UI, alerting, and deployment surface set up,
this adapter lets you reuse it for Temporaless-shaped handlers. Temporaless
storage-first replay is present only when the handler body invokes the
Temporaless runtime or a Temporaless Connect boundary; wrapping a plain handler
does not add replay by itself.

```python
from temporaless_prefectcompat import (
    ActivityWrapOptions,
    WorkflowWrapOptions,
    wrap_activity,
    wrap_workflow,
)

# Before: plain Temporaless protobuf handlers
async def fetch_price(req: FetchRequest) -> FetchResponse:
    ...

async def price_workflow(req: PriceRequest) -> PriceResponse:
    ...

# After: register as Prefect flows / tasks
fetch_price_task = wrap_activity(
    fetch_price,
    ActivityWrapOptions(name="fetch_price"),
)
PriceFlow = wrap_workflow(
    price_workflow,
    WorkflowWrapOptions(name="PriceFlow", retries=2),
)

# Run via Prefect — shows up in the UI, scheduled, retried per Prefect's rules
await PriceFlow(PriceRequest(symbol="AAPL"))
```

The wrapped functions retain their unary protobuf shape; Prefect handles run
tracking, retries, scheduling, and UI visibility. A handler that calls
`current_workflow().execute_activity` retains Temporaless storage-first replay.

## Deployment parameters

`wrap_workflow` derives the concrete request message class from the handler's
annotation. Direct calls still accept that message. Before Prefect persists a
deployment or schedule parameter, the wrapped flow converts it to a strict
JSON-safe envelope containing the protobuf full name and base64-encoded,
deterministically serialized protobuf binary. A worker decodes that envelope
before calling the unchanged unary protobuf handler.

The flow's `parameters=`, `schedule=`, `schedules=`, and deployment `triggers=`
paths do this automatically. For parameters supplied directly to Prefect's
`run_deployment`, serialize them through the flow first:

```python
from prefect.deployments import run_deployment

parameters = PriceFlow.serialize_parameters(
    {"input_message": PriceRequest(symbol="AAPL")}
)
await run_deployment(name="prices/PriceFlow", parameters=parameters)
```

Passing a raw protobuf message directly to `run_deployment` bypasses the flow
and is unsupported. The adapter does not use protobuf JSON or a generic object
codec; protobuf binary remains the canonical payload.

Prefect workers reload deployment entrypoints in a fresh process. Therefore, a
deployable or served wrapped flow must have a module-global binding in the
same module that defines its handler, recreated when that module is imported,
such as
`PriceFlow = wrap_workflow(price_workflow, options)`. Local or dynamically
constructed flows—and wrappers created in a separate deployment module around
an imported handler—remain usable for direct execution and subflows, but their
deployment and serve paths are rejected.

## Supported

- one protobuf workflow request and one protobuf workflow response
- one protobuf activity request and one protobuf activity response
- concrete protobuf request and response annotations at both wrapper boundaries
- async-only handlers (sync callables rejected at wrap time)
- explicit `ActivityWrapOptions` / `WorkflowWrapOptions` fields for `name`,
  `retries`, and `retry_delay_seconds`

This package targets exactly Prefect 3.7.8. It subclasses `Flow` and preserves
Prefect's async deployment dispatch contract; a Prefect upgrade requires
promoting the pin together with this adapter's compatibility suite.

## Not in scope

- Forwarding arbitrary Prefect decorator keyword arguments. Use a native
  Prefect flow or task when you need settings outside the adapter's explicit
  compatibility surface. `Flow.with_options` is rejected; set the supported
  name and retry fields in `WorkflowWrapOptions`.
- Mapping Temporaless typed errors (`TimerPendingError`, `EventPendingError`)
  to Prefect retry semantics. Prefect tracks its own run state, while the
  underlying Temporaless workflow stays `IN_PROGRESS`. Sleeps and waits that
  explicitly use `PollOptions` can be re-invoked through the Temporaless timer
  scanner; a manual event/dependency wait requires the application delivery or
  completion path to invoke it again. This adapter does not invent either
  policy.
- Persisting Prefect run state into Temporaless records — Prefect's database is canonical for its run tracking.
- Two-way migration of existing Prefect `@flow` code to Temporaless storage — that's a separate, larger project.

## Direction

Like `temporalcompat`, this is the *outbound* direction (Temporaless-shaped handlers running on Prefect infrastructure). The inverse direction (existing Prefect code running on Temporaless storage) would require emulating Prefect's task runner; not currently in scope.
