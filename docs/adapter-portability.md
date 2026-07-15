# Adapter Portability

How portable is your code between Temporaless and Temporal / Dagster / Prefect / Airflow? **Activity bodies are 100% portable** — they're vanilla async functions. **Workflow bodies need a small, mechanical shim per runtime** because each framework has its own way to invoke activities, sleep, and wait for events. This page shows what changes and what doesn't.

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

This same function works as:
- A Temporaless activity (`Workflow.execute_activity` with `_fetch_price` as the body).
- A Temporal SDK activity (`@temporalio.activity.defn` decoration via `temporaless_temporalcompat.wrap_activity`).
- A Dagster `@op` (wrap into the framework's task model).
- A Prefect `@task` (same).
- An Airflow `PythonOperator` (call directly from a `python_callable`).

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
    store, Options(workflow_id="prices:aapl", run_id="2026-05-04", code_version="v1"),
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

### Dagster (illustrative pattern; bring your own `dagster` install)

```python
import dagster

@dagster.op
async def fetch_price_op(context, symbol: str) -> str:
    # The same activity body, adapted to Dagster's positional arg shape.
    result = await fetch_price(StringValue(value=symbol))
    return result.value


@dagster.job
def price_job():
    fetch_price_op()
```

For replay / persistence semantics that match Temporaless, you'd `@op` the activity bodies and let Dagster's IO managers persist outputs. Dagster's asset-graph mode is what you give up by switching to Temporaless; the workflow-shaped subset is mechanical.

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


# Same workflow body — wrapped as a Prefect flow. Visible in Prefect UI;
# Temporaless storage-first replay still applies inside the body.
PriceFlow = wrap_workflow(
    price_flow_body,
    WorkflowWrapOptions(name="PriceFlow"),
)

# Run via Prefect's runtime — Prefect tracks the run, storage records still go to S3/GCS.
result = await PriceFlow(StringValue(value="AAPL"))
```

Prefect's `@flow` / `@task` model maps cleanly to the `adapters/py/connectworkflow` trigger wrapper plus core `execute_activity`; the adapter does the wiring. The protobuf contract is enforced both ways. The compatibility surface deliberately exposes only names and retry settings; use native Prefect definitions for other decorator options. See `adapters/py/prefectcompat/tests/test_integration.py` for backfill + cross-pipeline-dep composition tested end-to-end through Prefect.

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
| `code_version` fingerprinting | ✓ | history compatibility versioning | code location versions | deployments | DAG versioning |

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

- `adapters/py/temporalcompat/tests/test_adapter.py` exercises the Temporaless→Temporal direction end-to-end with `WorkflowEnvironment.start_time_skipping()`. Activity bodies, workflow bodies, retries, sleep, timeout, async activity bodies, validation paths — all ten scenarios.
- `adapters/py/temporalcompat/tests/test_portability.py` (this iteration) shows the **same activity body** running under both Temporaless's `Workflow.execute_activity` and Temporal SDK's worker, asserting identical outputs.

If you need Dagster / Prefect / Airflow integration tests, they're a small `@op` / `@task` / `PythonOperator` shim away — copy the patterns above and bring the framework's package as a dev dep.
