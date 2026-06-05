"""Common-workflow coverage for the Prefect compat adapter.

Beyond the basic shape test, these exercise patterns that real users hit:

- parallel fan-out via ``asyncio.gather``
- error propagation from activity → flow
- nested-flow composition (a flow calling another flow)
- Prefect's own retry semantics on the wrapped task
- per-call task overrides via ``.with_options``

If any of these break, real users break.
"""

from __future__ import annotations

import asyncio

import pytest
from google.protobuf.wrappers_pb2 import StringValue

from temporaless_prefectcompat import wrap_activity, wrap_workflow

# ---- parallel fan-out -------------------------------------------------------


async def _fetch_one(symbol: StringValue) -> StringValue:
    return StringValue(value=f"{symbol.value}:100.00")


_fetch_one_task = wrap_activity(_fetch_one, name="fetch_one")


async def _fan_out_body(_request: StringValue) -> StringValue:
    symbols = ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA"]
    results = await asyncio.gather(*(_fetch_one_task(StringValue(value=s)) for s in symbols))
    return StringValue(value=",".join(r.value for r in results))


_FanOutFlow = wrap_workflow(_fan_out_body, name="FanOutFlow")


async def test_asyncio_gather_fan_out_works_inside_prefect_flow() -> None:
    """``asyncio.gather`` over a wrapped task — Prefect must allow concurrent
    task runs from the same flow body."""
    result = await _FanOutFlow(StringValue(value="batch"))
    assert isinstance(result, StringValue)
    assert "AAPL:100.00" in result.value
    assert "NVDA:100.00" in result.value
    assert result.value.count(",") == 4  # 5 symbols → 4 commas


# ---- error propagation ------------------------------------------------------


class _VendorBroke(RuntimeError):
    pass


async def _flaky_fetch(symbol: StringValue) -> StringValue:
    if symbol.value == "BAD":
        raise _VendorBroke(f"vendor returned 5xx for {symbol.value}")
    return StringValue(value=f"{symbol.value}:ok")


_flaky_fetch_task = wrap_activity(_flaky_fetch, name="flaky_fetch")


async def _error_propagation_body(request: StringValue) -> StringValue:
    return await _flaky_fetch_task(request)


_ErrorFlow = wrap_workflow(_error_propagation_body, name="ErrorFlow")


async def test_activity_error_propagates_through_flow() -> None:
    """An exception raised in a wrapped task must surface to the flow caller —
    Prefect shouldn't swallow user-defined exceptions silently."""
    with pytest.raises(_VendorBroke, match="BAD"):
        await _ErrorFlow(StringValue(value="BAD"))


async def test_partial_failure_in_gather_propagates() -> None:
    """In ``asyncio.gather``, the first task to raise wins — the flow surfaces
    that exception. Other tasks may continue but the flow result is the error."""

    async def gather_body(_request: StringValue) -> StringValue:
        results = await asyncio.gather(
            _flaky_fetch_task(StringValue(value="AAPL")),
            _flaky_fetch_task(StringValue(value="BAD")),  # raises
            _flaky_fetch_task(StringValue(value="MSFT")),
        )
        return StringValue(value=",".join(r.value for r in results))

    flow = wrap_workflow(gather_body, name="PartialFailureFlow")
    with pytest.raises(_VendorBroke):
        await flow(StringValue(value="batch"))


# ---- Prefect retry semantics -----------------------------------------------


_attempts: dict[str, int] = {}


async def _retry_eventually_succeeds(symbol: StringValue) -> StringValue:
    _attempts[symbol.value] = _attempts.get(symbol.value, 0) + 1
    if _attempts[symbol.value] < 3:
        raise RuntimeError("transient")
    return StringValue(value=f"{symbol.value}:after-{_attempts[symbol.value]}")


# Forward Prefect's retries kwarg through wrap_activity.
_retry_task = wrap_activity(
    _retry_eventually_succeeds, name="retry_eventually_succeeds", retries=2, retry_delay_seconds=0
)


async def _retry_workflow_body(request: StringValue) -> StringValue:
    return await _retry_task(request)


_RetryFlow = wrap_workflow(_retry_workflow_body, name="RetryFlow")


async def test_prefect_task_retries_via_forwarded_kwargs() -> None:
    """retries=2 means up to 3 attempts (initial + 2 retries). The body
    succeeds on attempt 3 — Prefect's retry policy must honor that."""
    _attempts.clear()
    result = await _RetryFlow(StringValue(value="AAPL"))
    assert _attempts["AAPL"] == 3
    assert result.value == "AAPL:after-3"


# ---- nested flow composition ------------------------------------------------


async def _inner_body(request: StringValue) -> StringValue:
    return StringValue(value=f"inner({request.value})")


_InnerFlow = wrap_workflow(_inner_body, name="InnerFlow")


async def _outer_body(request: StringValue) -> StringValue:
    intermediate = await _InnerFlow(StringValue(value=f"step1:{request.value}"))
    assert isinstance(intermediate, StringValue)
    return StringValue(value=f"outer({intermediate.value})")


_OuterFlow = wrap_workflow(_outer_body, name="OuterFlow")


async def test_nested_flows_compose() -> None:
    """A wrap_workflow flow that calls another wrap_workflow flow — Prefect
    treats this as a subflow, the wrapper preserves protobuf semantics through
    the chain."""
    result = await _OuterFlow(StringValue(value="AAPL"))
    assert isinstance(result, StringValue)
    assert result.value == "outer(inner(step1:AAPL))"
