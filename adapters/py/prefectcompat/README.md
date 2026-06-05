# Prefect Python Compatibility Adapter

Strict compatibility adapter that exposes Temporaless-shaped unary protobuf handlers as Prefect flows and tasks.

It does not emulate the Prefect server. It wraps a Temporaless handler so the same code runs inside Prefect's orchestration (run tracking, UI visibility, scheduling) without changing the handler.

## Why use this

If your team already has Prefect's UI, alerting, and deployment surface set up, this adapter lets you reuse all of it while keeping Temporaless's storage-first replay underneath. Existing Temporaless handlers register as Prefect flows by changing only one line.

```python
from temporaless_prefectcompat import wrap_activity, wrap_workflow

# Before: plain Temporaless protobuf handlers
async def fetch_price(req: FetchRequest) -> FetchResponse:
    ...

async def price_workflow(req: PriceRequest) -> PriceResponse:
    ...

# After: register as Prefect flows / tasks
fetch_price_task = wrap_activity(fetch_price, name="fetch_price")
PriceFlow = wrap_workflow(price_workflow, name="PriceFlow", retries=2)

# Run via Prefect — shows up in the UI, scheduled, retried per Prefect's rules
await PriceFlow(PriceRequest(symbol="AAPL"))
```

The wrapped functions retain their unary protobuf shape; Prefect handles run tracking, retries, scheduling, and UI visibility. The handler itself can still call `current_workflow().execute_activity` for Temporaless storage-first replay.

## Supported

- one protobuf workflow request and one protobuf workflow response
- one protobuf activity request and one protobuf activity response
- async-only handlers (sync callables rejected at wrap time)
- arbitrary `flow_kwargs` / `task_kwargs` forwarded to Prefect (retries, cache_policy, persist_result, tags, etc.)

## Not in scope

- Mapping Temporaless typed errors (`TimerPendingError`, `EventPendingError`) to Prefect retry semantics — Prefect tracks the run state, but the underlying workflow stays IN_PROGRESS in Temporaless storage and resumes via the timer scanner.
- Persisting Prefect run state into Temporaless records — Prefect's database is canonical for its run tracking.
- Two-way migration of existing Prefect `@flow` code to Temporaless storage — that's a separate, larger project.

## Direction

Like `temporalcompat`, this is the *outbound* direction (Temporaless-shaped handlers running on Prefect infrastructure). The inverse direction (existing Prefect code running on Temporaless storage) would require emulating Prefect's task runner; not currently in scope.
