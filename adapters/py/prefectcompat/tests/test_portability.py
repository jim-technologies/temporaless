"""Portability proof: the same activity/workflow body runs under both
Temporaless and Prefect with identical outputs.

This locks in the contract documented in `docs/adapter-portability.md`:
activity and workflow bodies are vanilla async functions; only the
dispatch wiring differs per runtime. If we ever break that property
(e.g. add framework-only helpers a body must call), this test fails.

The bodies are defined once and consumed twice:

- Temporaless side: invoked via ``Workflow.execute_activity`` / ``run`` against
  an ``OpenDALStore``.
- Prefect side: wrapped via ``temporaless_prefectcompat`` and invoked through
  Prefect's flow engine.

Both invocations get the same input and produce the same output.
"""

from __future__ import annotations

import asyncio
import tempfile

import opendal
import temporaless
from google.protobuf.wrappers_pb2 import StringValue

from temporaless_prefectcompat import wrap_activity, wrap_workflow

# ---- the portable body -------------------------------------------------


async def _fetch_price(symbol: StringValue) -> StringValue:
    """Vanilla async function. No framework imports inside the body — this
    is the property that makes it portable. Defined once; consumed twice."""
    return StringValue(value=f"{symbol.value} 100.00")


# ---- runtime A: Temporaless --------------------------------------------


async def _temporaless_run(symbol: StringValue) -> StringValue:
    operator = opendal.AsyncOperator(
        "fs", root=tempfile.mkdtemp(prefix="temporaless-portability-prefect-")
    )
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
            workflow_id="prices:portability-prefect",
            run_id="2026-05-04",
            code_version="test",
        ),
        symbol,
        StringValue,
        workflow_body,
    )


# ---- runtime B: Prefect via prefectcompat ------------------------------

_fetch_price_prefect_task = wrap_activity(_fetch_price, name="fetch_price_portable_prefect")


async def _prefect_workflow_body(symbol: StringValue) -> StringValue:
    # Same body as temporaless side, just wrapped as a Prefect flow externally.
    result = await _fetch_price_prefect_task(symbol)
    assert isinstance(result, StringValue)
    return result


_PrefectFlow = wrap_workflow(_prefect_workflow_body, name="PortabilityPrefectFlow")


async def _prefect_run(symbol: StringValue) -> StringValue:
    result = await _PrefectFlow(symbol)
    assert isinstance(result, StringValue)
    return result


# ---- the test -----------------------------------------------------------


def test_same_body_runs_identically_on_temporaless_and_prefect() -> None:
    """The locked invariant: identical input + identical body → identical
    output, regardless of runtime."""
    symbol = StringValue(value="AAPL")

    temporaless_result = asyncio.run(_temporaless_run(symbol))
    prefect_result = asyncio.run(_prefect_run(symbol))

    assert temporaless_result.value == "AAPL 100.00"
    assert prefect_result.value == "AAPL 100.00"
    assert temporaless_result.value == prefect_result.value
