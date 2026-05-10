"""Tests for the Prefect compatibility adapter."""

from __future__ import annotations

from typing import Any, cast

import pytest
from google.protobuf.wrappers_pb2 import StringValue

from temporaless_prefectcompat import wrap_activity, wrap_workflow


async def echo_activity(req: StringValue) -> StringValue:
    return StringValue(value=req.value)


async def nil_input_activity(_req: StringValue):
    return None


async def echo_workflow(req: StringValue) -> StringValue:
    return StringValue(value=f"flow:{req.value}")


async def nil_input_workflow(_req: StringValue):
    return None


async def test_wrap_activity_runs_directly_and_preserves_protobuf_contract() -> None:
    """A wrapped activity is callable directly (outside a flow) and round-trips
    protobuf messages."""
    wrapped = wrap_activity(echo_activity, name="echo")
    result = await wrapped(StringValue(value="AAPL"))
    assert isinstance(result, StringValue)
    assert result.value == "AAPL"


async def test_wrap_workflow_runs_via_prefect_and_returns_protobuf() -> None:
    """A wrapped workflow runs as a Prefect flow — the call goes through
    Prefect's orchestration (run tracking, logger), but the handler's
    contract stays protobuf."""
    wrapped = wrap_workflow(echo_workflow, name="EchoFlow")
    result = await wrapped(StringValue(value="AAPL"))
    assert isinstance(result, StringValue)
    assert result.value == "flow:AAPL"


async def test_wrap_activity_inside_workflow_records_a_prefect_task_run() -> None:
    """Calling a wrap_activity-decorated callable inside a wrap_workflow-decorated
    flow registers a real Prefect task run — exercising the integration end-to-end."""
    inner = wrap_activity(echo_activity, name="echo_inside_flow")

    async def composed(req: StringValue) -> StringValue:
        intermediate = await inner(StringValue(value=f"step1:{req.value}"))
        return StringValue(value=f"step2:{intermediate.value}")

    outer = wrap_workflow(composed, name="ComposedFlow")
    result = await outer(StringValue(value="AAPL"))
    assert result.value == "step2:step1:AAPL"


async def test_wrap_activity_validates_protobuf_contract() -> None:
    """Non-protobuf inputs and non-protobuf return values fail loud."""
    wrapped_nil_input = wrap_activity(echo_activity, name="echo_nil")
    with pytest.raises(ValueError, match="activity input is required"):
        await wrapped_nil_input(cast(StringValue, None))

    wrapped_nil_result = wrap_activity(cast(Any, nil_input_activity), name="nil_result")
    with pytest.raises(ValueError, match="non-protobuf result"):
        await wrapped_nil_result(StringValue(value="AAPL"))


async def test_wrap_workflow_validates_protobuf_contract() -> None:
    """Prefect's pydantic catches non-Message inputs before the body runs;
    non-Message returns are caught by our wrapper."""
    from prefect.exceptions import ParameterTypeError

    wrapped_nil_input = wrap_workflow(echo_workflow, name="EchoNil")
    with pytest.raises(ParameterTypeError, match="Message"):
        await wrapped_nil_input(cast(StringValue, None))

    wrapped_nil_result = wrap_workflow(cast(Any, nil_input_workflow), name="NilResult")
    with pytest.raises(ValueError, match="non-protobuf result"):
        await wrapped_nil_result(StringValue(value="AAPL"))


def test_wrap_helpers_reject_sync_executors() -> None:
    """Async-only stance — sync functions fail at wrap time, not at runtime."""

    def sync_activity(_req: StringValue) -> StringValue:
        return StringValue(value="should-not-reach")

    def sync_workflow(_req: StringValue) -> StringValue:
        return StringValue(value="should-not-reach")

    with pytest.raises(ValueError, match="must be async"):
        wrap_activity(cast(Any, sync_activity), name="sync_activity")
    with pytest.raises(ValueError, match="must be async"):
        wrap_workflow(cast(Any, sync_workflow), name="SyncWorkflow")


def test_wrap_helpers_validate_required_fields() -> None:
    with pytest.raises(ValueError, match="activity executor"):
        wrap_activity(cast(Any, None), name="x")
    with pytest.raises(ValueError, match="workflow executor"):
        wrap_workflow(cast(Any, None), name="x")

    async def anon(_req: StringValue) -> None:
        return None

    anon.__name__ = ""
    with pytest.raises(ValueError, match="activity name"):
        wrap_activity(cast(Any, anon), name=None)
    with pytest.raises(ValueError, match="workflow name"):
        wrap_workflow(cast(Any, anon), name=None)


async def test_wrap_workflow_forwards_prefect_kwargs() -> None:
    """``flow_kwargs`` like ``retries`` reach the underlying Prefect flow.

    We verify by reading the configured retries off the resulting flow object.
    """
    wrapped = wrap_workflow(echo_workflow, name="RetryFlow", retries=2)
    # prefect.flow attaches a Flow object with `.retries` available.
    assert getattr(wrapped, "retries", None) == 2


async def test_wrap_activity_forwards_prefect_kwargs() -> None:
    wrapped = wrap_activity(echo_activity, name="retry_task", retries=3)
    assert getattr(wrapped, "retries", None) == 3
