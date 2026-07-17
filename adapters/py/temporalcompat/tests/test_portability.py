"""Portability proof: the same activity body runs under both Temporaless and
Temporal SDK with identical outputs.

This locks in the contract documented in `docs/adapter-portability.md`:
activity bodies are vanilla async functions; only the workflow-body wiring
differs per runtime. If we ever break that property (e.g. add framework-only
helpers that an activity body must call), this test fails.

The activity body ``_fetch_price`` is defined once and re-used:

- Temporaless side: invoked via ``Workflow.execute_activity`` inside a real
  ``run()`` call against an ``OpenDALStore``.
- Temporal side: wrapped via ``temporaless_temporalcompat.wrap_activity`` and
  invoked from a workflow body running on Temporal's time-skipping test env.

Both invocations get the same input and must produce the same output.
"""

from __future__ import annotations

import asyncio
import tempfile
from datetime import timedelta
from uuid import uuid4

import opendal
import temporaless
from google.protobuf.wrappers_pb2 import StringValue
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporaless_temporalcompat import (
    ActivityCall,
    ActivityWrapOptions,
    WorkflowWrapOptions,
    execute_activity,
    wrap_activity,
    wrap_workflow,
)

# ---- the portable activity body --------------------------------------------


async def _fetch_price(symbol: StringValue) -> StringValue:
    """Vanilla async function. No framework imports inside the body — this
    is the property that makes it portable. Defined once; consumed twice."""
    return StringValue(value=f"{symbol.value} 100.00")


# ---- runtime A: Temporaless ------------------------------------------------


async def _temporaless_run(symbol: StringValue) -> StringValue:
    operator = opendal.AsyncOperator("fs", root=tempfile.mkdtemp(prefix="temporaless-portability-"))
    store = temporaless.OpenDALStore(operator)

    async def workflow_body(workflow: temporaless.Workflow, request: StringValue) -> StringValue:
        return await workflow.execute_activity(
            temporaless.ActivityOptions(activity_id="fetch:price"),
            request,
            StringValue,
            _fetch_price,  # ← same body
        )

    return await temporaless.run(
        store,
        temporaless.Options(
            workflow_id="prices:portability",
            run_id="2026-05-04",
            code_version="test",
        ),
        symbol,
        StringValue,
        workflow_body,
    )


# ---- runtime B: Temporal SDK via temporalcompat ----------------------------

_fetch_price_temporal_activity = wrap_activity(
    _fetch_price, ActivityWrapOptions(name="fetch_price_portable")
)


async def _temporal_workflow_body(symbol: StringValue) -> StringValue:
    result = await execute_activity(
        ActivityCall(
            activity=_fetch_price_temporal_activity,
            result_type=StringValue,
            start_to_close_timeout=timedelta(seconds=10),
        ),
        symbol,
    )
    assert isinstance(result, StringValue)
    return result


_TemporalWorkflow = wrap_workflow(
    _temporal_workflow_body, WorkflowWrapOptions(name="PortabilityWorkflow")
)


async def _temporal_run(symbol: StringValue) -> StringValue:
    env = await WorkflowEnvironment.start_time_skipping()
    async with (
        env,
        Worker(
            env.client,
            task_queue="temporaless-portability",
            workflows=[_TemporalWorkflow],
            activities=[_fetch_price_temporal_activity],
        ),
    ):
        result = await env.client.execute_workflow(
            _TemporalWorkflow.run,  # ty: ignore[unresolved-attribute]
            symbol,
            id=f"portability-{uuid4()}",
            task_queue="temporaless-portability",
            result_type=StringValue,
        )
        assert isinstance(result, StringValue)
        return result


# ---- the test --------------------------------------------------------------


def test_same_activity_body_runs_identically_on_temporaless_and_temporal() -> None:
    """The locked invariant: identical input + identical activity body →
    identical output, regardless of runtime."""
    symbol = StringValue(value="AAPL")

    temporaless_result = asyncio.run(_temporaless_run(symbol))
    temporal_result = asyncio.run(_temporal_run(symbol))

    assert temporaless_result.value == "AAPL 100.00"
    assert temporal_result.value == "AAPL 100.00"
    assert temporaless_result.value == temporal_result.value
