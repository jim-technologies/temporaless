# Temporaless Connect workflow adapter

`temporaless-connectworkflow` adapts an async unary ConnectRPC service method
to Temporaless replay semantics. It is transport-specific by design; the core
workflow runtime does not import ConnectRPC.

```python
from temporaless import Options
from temporaless_connectworkflow import WorkflowMethodWrapOptions, wrap_workflow_method


class PriceService:
    @wrap_workflow_method(
        WorkflowMethodWrapOptions(
            store=lambda service: service.store,
            result_type=FetchResponse,
            options_for=lambda _service, request: Options(
                workflow_id=f"prices:{request.symbol}",
                run_id=request.run_id,
            ),
        )
    )
    async def fetch(self, request: FetchRequest, ctx: object = None) -> FetchResponse:
        ...
```

The adapter automatically translates Temporaless pending, contention,
conflict, and terminal activity errors into the documented ConnectRPC codes.
Unknown application exceptions propagate unchanged.

| Temporaless error | ConnectRPC code |
|---|---|
| timer, event, or workflow-dependency pending; infrastructure outage | `UNAVAILABLE` |
| claim busy | `ALREADY_EXISTS` |
| concurrency cap busy | `RESOURCE_EXHAUSTED` |
| claim capability or stored-record conflict | `FAILED_PRECONDITION` |
| claim release, activity failure, or failed workflow dependency | `INTERNAL` |

Only async unary methods with protobuf result classes are accepted. The
application continues to provide every workflow/run ID through core
`WorkflowOptions`; the adapter never generates identity or serializes a custom
payload.

For backfills that invoke a remote ConnectRPC service, opt into transport
status classification explicitly:

```python
from temporaless.backfill import backfill
from temporaless_connectworkflow import is_pending_error

report = await backfill(invoke, run_ids, pending_error=is_pending_error)
```

This keeps generic backfills transport-neutral while preserving the remote
`UNAVAILABLE` / `ALREADY_EXISTS` / `RESOURCE_EXHAUSTED` behavior.
