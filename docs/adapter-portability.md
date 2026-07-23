# Adapter Portability

How portable is your code between Temporaless and Temporal / Dagster / Prefect
/ Airflow? An async unary protobuf activity or business body is portable when
it does not call framework-specific APIs. Workflow orchestration needs a small,
mechanical shim per runtime because each framework has its own activity,
sleep, event, and identity semantics. General import-only migration is not
possible.

The framework adapter contract is documented in `docs/adapter-contract.md`. This page is the practical "I want to move my code" guide.

## What's portable: activity bodies

Activity bodies are typed protobuf-RPC handlers — one input message, one output message, plain `async def`. Nothing in the body references a specific framework:

```python
from google.protobuf.wrappers_pb2 import StringValue


async def fetch_price(symbol: StringValue) -> StringValue:
    """Vanilla async function. Lives in your code; no framework imports."""
    # ... real vendor SDK call here ...
    return StringValue(value=f"{symbol.value} 100.00")
```

This body can be reused as:

- A Temporaless activity (`Workflow.execute_activity` with `_fetch_price` as the body).
- A Temporal SDK activity (`@temporalio.activity.defn` decoration via `temporaless_temporalcompat.wrap_activity`).
- A Prefect `@task` through `temporaless_prefectcompat.wrap_activity`.
- Code behind a Dagster or Airflow boundary, provided that process owns
  compatible generated application protobuf types. Dagster currently cannot
  import Temporaless in the same Python environment.

If your activity bodies don't import framework-specific helpers (no `temporalio.workflow.now()`, no Dagster context, no Prefect logger), you've got runtime-portable activity code by construction.

## What needs a shim: workflow bodies

The way a workflow body **invokes** an activity is framework-specific. Here's the same workflow expressed in each system, side-by-side. The activity body (`fetch_price` above) is the same function in all four.

### Temporaless

```python
from temporaless import (
    ActivityOptions,
    OpenDALStore,
    Options,
    Workflow,
    run,
)

async def price_workflow(workflow: Workflow, request: StringValue) -> StringValue:
    return await workflow.execute_activity(
        ActivityOptions(activity_id="fetch:price"),
        request,
        StringValue,
        fetch_price,  # ← portable activity body, unchanged
    )

# Trigger:
result = await run(
    store, Options(workflow_id="prices:aapl", run_id="2026-05-04"),
    StringValue(value="AAPL"), StringValue, price_workflow,
)
```

### Temporal (via `adapters/py/temporalcompat`)

```python
from datetime import timedelta
from temporaless_temporalcompat import (
    ActivityCall, ActivityWrapOptions, WorkflowWrapOptions,
    execute_activity, wrap_activity, wrap_workflow,
)

# Adapter wraps the SAME body as a Temporal activity:
fetch_price_activity = wrap_activity(
    fetch_price, ActivityWrapOptions(name="fetch_price")
)


async def price_workflow_body(request: StringValue) -> StringValue:
    return await execute_activity(
        ActivityCall(
            activity=fetch_price_activity,
            result_type=StringValue,
            start_to_close_timeout=timedelta(seconds=10),
        ),
        request,
    )


PriceWorkflow = wrap_workflow(
    price_workflow_body, WorkflowWrapOptions(name="PriceWorkflow")
)

# Trigger via Temporal SDK:
result = await client.execute_workflow(
    PriceWorkflow.run, StringValue(value="AAPL"),
    id="prices:aapl/2026-05-04", task_queue="prices",
    result_type=StringValue,
)
```

### Dagster (process-isolated)

```python
import dagster
from price.v1.price_connect import PriceServiceClient
from price.v1.price_pb2 import FetchPriceRequest


@dagster.op
async def fetch_price_op(context) -> str:
    async with PriceServiceClient("https://workflow.example.com") as client:
        response = await client.fetch_price(
            FetchPriceRequest(
                symbol="AAPL",
                workflow_id="prices:AAPL",
                run_id=context.run_id,
            )
        )
    return response.price
```

Dagster 1.13.14 requires `protobuf<7`; Temporaless requires protobuf 7.35.1
or newer. Generate the Dagster client and Temporaless server independently
from the same application `.proto`, run them in separate environments, and
exchange only protobuf wire bytes. Dagster remains responsible for asset/job
state and lineage; Temporaless remains responsible for workflow replay.

### Prefect (via `adapters/py/prefectcompat`)

```python
from temporaless_prefectcompat import (
    ActivityWrapOptions,
    WorkflowWrapOptions,
    wrap_activity,
    wrap_workflow,
)

# Same activity body — wrapped as a Prefect task with explicit supported options.
fetch_price_task = wrap_activity(
    fetch_price,
    ActivityWrapOptions(name="fetch_price", retries=3, retry_delay_seconds=10),
)


async def price_flow_body(symbol: StringValue) -> StringValue:
    return await fetch_price_task(symbol)


# Prefect flow wrapper around the runtime-specific orchestration body.
PriceFlow = wrap_workflow(
    price_flow_body,
    WorkflowWrapOptions(name="PriceFlow"),
)

# Direct and deployed calls enter Prefect with a protobuf-safe parameter.
result = await PriceFlow(StringValue(value="AAPL"))
```

Prefect owns the flow/task run in this example. Temporaless object-store replay
applies only if `price_flow_body` invokes `temporaless.run` or calls a
canonical Temporaless ConnectRPC workflow service. The Prefect adapter enforces
the protobuf contract and uses a protobuf-binary deployment envelope; it does
not silently turn a normal Prefect flow into a Temporaless workflow.

### Airflow (illustrative pattern)

```python
from airflow import DAG
from airflow.operators.python import PythonOperator


def call_fetch(symbol: str, **_kwargs) -> str:
    import asyncio
    return asyncio.run(fetch_price(StringValue(value=symbol))).value


with DAG("prices", schedule_interval="@daily") as dag:
    fetch = PythonOperator(
        task_id="fetch", python_callable=call_fetch,
        op_kwargs={"symbol": "AAPL"},
    )
```

The Airflow DAG declaration replaces the workflow body; activity bodies are reusable.

## What does NOT port across runtimes

Things that work in Temporaless but not in others (or vice versa):

| Feature | Temporaless | Temporal | Dagster | Prefect | Airflow |
|---|---|---|---|---|---|
| Storage-first records on S3 | ✓ | server's history | server's DB | server's DB | server's DB |
| `current_workflow()` contextvar accessor | ✓ | use Temporal's `workflow.now()`/`info()` | Dagster context | Prefect context | jinja templating |
| Multi-signal `select` | ✗ | ✓ | n/a | n/a | n/a |
| Asset graph / lineage | ✗ | ✗ | ✓ | partial | n/a |
| `adapters/py/connectworkflow` decorator | ✓ | per-Temporal patterns | `@op` | `@task` | `Operator` |
| Historical application-build pinning | ✗ — current handler resumes | history compatibility versioning | code location versions | deployments | DAG versioning |

## Practical migration recipe

Going from another framework to Temporaless:

1. **Keep your activity bodies as-is.** They're vanilla async functions; no rewrite needed.
2. **Rewrite the workflow / DAG glue.** That's the mechanical part: replace `@op`/`@task`/operator definitions with `Workflow.execute_activity` calls inside an `async def workflow_body(workflow, request)`.
3. **Pick caller-provided IDs.** Where the other framework auto-generates run IDs, decide your `(workflow_id, run_id)` scheme — usually `<entity>:<id>` and `<date>` or `<event_uuid>`.
4. **Replace retry config.** Their retry decorators map to `RetryPolicy` on `ActivityOptions`. Same fields: max attempts, initial interval, backoff coefficient.
5. **Replace the trigger surface.** Their server endpoint uses `temporaless_connectworkflow.wrap_workflow_method` on a ConnectRPC service, mounted on uvicorn. Existing gRPC interceptors carry over.

Going from Temporaless to another framework:

- Activity bodies port unchanged.
- Workflow body becomes the other framework's flow/DAG/job glue.
- If you used `wait_event`, replace with the other framework's signal/sensor primitive.
- If you used `claims`, replace with the other framework's lock or remove if their state model handles it.

## What's tested

- `adapters/py/temporalcompat/tests` exercises the outbound
  Temporaless-to-Temporal direction with the real SDK test environment,
  including registration, activity execution, retries, sleep, timeouts, and
  validation.
- `adapters/go/temporalcompat` registers values returned by both wrapper
  functions in Temporal's SDK test environment and proves execution through
  the wrapped activity.
- `adapters/py/prefectcompat/tests` exercises direct flows/tasks, retries,
  Temporaless replay composition, and protobuf deployment-parameter
  serialization.
- `adapters/py/connectworkflow/tests` sends a binary protobuf request through
  a generated ConnectRPC ASGI service method and proves terminal replay. That
  transport boundary is also the supported Dagster integration boundary.

There is no same-process Dagster test because the official dependency ranges
are unsatisfiable. Do not install with `--no-deps` or suppress that conflict.
