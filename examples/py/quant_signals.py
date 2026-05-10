"""Quant-flavored example: parallel symbol fetch + serial signal compute.

Demonstrates the new async win — ``asyncio.gather`` over ``Workflow.execute_activity``
fans out independent vendor calls so wall time is ``max(per-symbol latency)``
instead of ``sum``. On replay each per-symbol activity short-circuits from its
stored record, so re-running the workflow is one ``Get`` per symbol.

Run with ``uv run --project core/py python examples/py/quant_signals.py``.
"""

from __future__ import annotations

import asyncio
import random
import tempfile
import time

import opendal
from google.protobuf.wrappers_pb2 import StringValue

from temporaless import (
    ActivityOptions,
    OpenDALStore,
    Options,
    Workflow,
    annotate,
    run,
)

SYMBOLS = ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META", "AMD"]


async def _fake_vendor_fetch(request: StringValue) -> StringValue:
    """Stand-in for a real vendor API. Sleeps to simulate network latency."""
    await asyncio.sleep(random.uniform(0.05, 0.15))
    annotate("vendor", "alpha")
    annotate("symbol", request.value)
    return StringValue(value=f"{request.value}:100.0+{random.uniform(-5, 5):.2f}")


async def _compose_signal(request: StringValue) -> StringValue:
    """Stand-in for a downstream signal computation. CPU-bound — no parallel win."""
    annotate("kind", "signal")
    return StringValue(value=f"signal({request.value})")


async def quant_pipeline(workflow: Workflow, _request: StringValue) -> StringValue:
    annotate("symbols", str(len(SYMBOLS)))

    # Parallel fan-out: 8 activities, ~max(latency) wall time, not sum.
    async def fetch_one(symbol: str) -> StringValue:
        return await workflow.execute_activity(
            ActivityOptions(activity_id=f"fetch:{symbol}"),
            StringValue(value=symbol),
            StringValue,
            _fake_vendor_fetch,
        )

    prices = await asyncio.gather(*(fetch_one(s) for s in SYMBOLS))

    # Serial composition: one activity that consumes the joined output.
    joined = StringValue(value=",".join(p.value for p in prices))
    return await workflow.execute_activity(
        ActivityOptions(activity_id="compose:signal"),
        joined,
        StringValue,
        _compose_signal,
    )


async def main() -> None:
    operator = opendal.AsyncOperator(
        "fs", root=tempfile.mkdtemp(prefix="temporaless-quant-")
    )
    store = OpenDALStore(operator)
    options = Options(
        workflow_id="quant:signals", run_id="2026-05-04", code_version="example"
    )
    seed = StringValue(value="batch")

    print(
        f"first invocation: parallel fetch of {len(SYMBOLS)} symbols + signal compose"
    )
    started = time.perf_counter()
    answer = await run(store, options, seed, StringValue, quant_pipeline)
    print(f"  wall time: {(time.perf_counter() - started) * 1000:.1f}ms")
    print(f"  result: {answer.value!r}")

    print("\nsecond invocation: every activity replays from storage")
    started = time.perf_counter()
    answer = await run(store, options, seed, StringValue, quant_pipeline)
    print(
        f"  wall time: {(time.perf_counter() - started) * 1000:.1f}ms (no vendor calls)"
    )
    print(f"  result: {answer.value!r}")


if __name__ == "__main__":
    asyncio.run(main())
