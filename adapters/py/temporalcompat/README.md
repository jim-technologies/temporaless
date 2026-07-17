# Temporal Python Compatibility Adapter

Strict compatibility adapter that runs Temporaless-shaped unary protobuf handlers on the real Temporal Python SDK.

It does not emulate the Temporal server. It delegates activities, retries,
timeouts, and durable timers to `temporalio`. Async unary protobuf business and
activity functions can be wrapped without rewriting their bodies. Workflow
orchestration must already use this adapter's Temporal execution helpers or be
rewritten mechanically from another runtime's helpers.

## Why use this

If your existing code base has activities and workflows that already follow the Temporaless convention — one protobuf request, one protobuf response, errors raised normally — you can run them on a Temporal cluster by changing only the registration:

```python
from temporaless_temporalcompat import (
    ActivityWrapOptions,
    WorkflowWrapOptions,
    wrap_activity,
    wrap_workflow,
)

# Before: plain Python functions
async def fetch_price(req: FetchRequest) -> FetchResponse:
    ...

async def price_workflow(req: PriceRequest) -> PriceResponse:
    response = await execute_activity(
        ActivityCall(activity=fetch_price_activity, result_type=FetchResponse, ...),
        FetchRequest(symbol=req.symbol),
    )
    ...

# After: register on a Temporal worker
fetch_price_activity = wrap_activity(
    fetch_price,
    ActivityWrapOptions(name="fetch_price"),
)
PriceWorkflow = wrap_workflow(
    price_workflow,
    WorkflowWrapOptions(name="PriceWorkflow"),
)

# Worker setup as usual
async with Worker(client, task_queue="...", workflows=[PriceWorkflow], activities=[fetch_price_activity]):
    ...
```

The wrapped functions retain their unary protobuf shape; Temporal handles
execution, retries, timeouts, sleeps, and history. Temporaless object storage,
claims, and replay records are not involved in this mode unless the
application separately invokes a Temporaless workflow boundary.

## Supported

- one protobuf workflow request and one protobuf workflow response
- one protobuf activity request and one protobuf activity response
- async-only activity and workflow bodies; sync callables are rejected at wrap time
- one explicit `ActivityWrapOptions` / `WorkflowWrapOptions` object per wrapper boundary
- Temporal SDK activity scheduling through `workflow.execute_activity`
- Temporal SDK durable timers through `workflow.sleep`
- Temporal SDK `RetryPolicy`
- Temporal SDK activity timeouts (start-to-close, schedule-to-close, schedule-to-start, heartbeat)
- per-call options forwarded unchanged via `ActivityCall`

## Python Workflow Sandbox

`wrap_workflow` builds a Temporal workflow class via the `type()` builtin and applies `@workflow.run` to a cloned function (because Temporal rejects locally-scoped classes for `@workflow.run`). The generated class runs with Temporal's Python workflow sandbox **disabled** because dynamically generated classes are not globally importable in the way the sandbox expects.

If a workflow needs full Temporal Python sandbox behavior, define a native `@workflow.defn` class directly and use `execute_activity`, `sleep`, and wrapped activities inside it.

## Rejected

- multiple workflow or activity arguments
- non-protobuf payloads
- custom payload converter behavior hidden behind Temporaless APIs
- child workflows, signals, queries, updates, cancellation scopes, side effects

Those features should use the Temporal SDK directly until this adapter can prove exact compatibility for them.

## Compatibility position

This adapter is compatible **by wiring to the Temporal SDK** rather than approximating Temporal semantics in Temporaless core. It is intentionally narrow: it lets a Temporaless unary protobuf handler shape run inside a Temporal worker, and it keeps Temporal-specific behavior out of the core runtime.

The inverse direction—running arbitrary Temporal-shaped code against a
Temporaless storage backend—is not supported. Temporal signals, queries,
children, converters, history, and implicit SDK-generated identity do not have
an import-only mapping to Temporaless's explicit protobuf record model.

## Testing

The adapter tests async workflow/activity bodies, retry policies, durable
timers, timeouts, input validation, and dynamic class identity. They run
against `temporalio.testing.WorkflowEnvironment.start_time_skipping()` so no
Temporal server is required.

```sh
uv run --project adapters/py/temporalcompat pytest adapters/py/temporalcompat/tests
```
