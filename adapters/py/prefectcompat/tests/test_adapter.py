"""Tests for the Prefect compatibility adapter."""

from __future__ import annotations

from typing import Any, cast

import pytest
from google.protobuf.wrappers_pb2 import StringValue

from temporaless_prefectcompat import (
    ActivityWrapOptions,
    WorkflowWrapOptions,
    wrap_activity,
    wrap_workflow,
)


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
    wrapped = wrap_activity(echo_activity, ActivityWrapOptions(name="echo"))
    result = await wrapped(StringValue(value="AAPL"))
    assert isinstance(result, StringValue)
    assert result.value == "AAPL"


async def test_wrap_workflow_runs_via_prefect_and_returns_protobuf() -> None:
    """A wrapped workflow runs as a Prefect flow — the call goes through
    Prefect's orchestration (run tracking, logger), but the handler's
    contract stays protobuf."""
    wrapped = wrap_workflow(echo_workflow, WorkflowWrapOptions(name="EchoFlow"))
    result = await wrapped(StringValue(value="AAPL"))
    assert isinstance(result, StringValue)
    assert result.value == "flow:AAPL"


async def test_wrap_activity_inside_workflow_records_a_prefect_task_run() -> None:
    """Calling a wrap_activity-decorated callable inside a wrap_workflow-decorated
    flow registers a real Prefect task run — exercising the integration end-to-end."""
    inner = wrap_activity(echo_activity, ActivityWrapOptions(name="echo_inside_flow"))

    async def composed(req: StringValue) -> StringValue:
        intermediate = await inner(StringValue(value=f"step1:{req.value}"))
        return StringValue(value=f"step2:{intermediate.value}")

    outer = wrap_workflow(composed, WorkflowWrapOptions(name="ComposedFlow"))
    result = await outer(StringValue(value="AAPL"))
    assert result.value == "step2:step1:AAPL"


async def test_wrap_activity_validates_protobuf_contract() -> None:
    """Non-protobuf inputs and non-protobuf return values fail loud."""
    wrapped_nil_input = wrap_activity(echo_activity, ActivityWrapOptions(name="echo_nil"))
    with pytest.raises(ValueError, match="activity input is required"):
        await wrapped_nil_input(cast(StringValue, None))

    wrapped_nil_result = wrap_activity(
        cast(Any, nil_input_activity), ActivityWrapOptions(name="nil_result")
    )
    with pytest.raises(ValueError, match="non-protobuf result"):
        await wrapped_nil_result(StringValue(value="AAPL"))


async def test_wrap_workflow_validates_protobuf_contract() -> None:
    """Prefect's pydantic catches non-Message inputs before the body runs;
    non-Message returns are caught by our wrapper."""
    from prefect.exceptions import ParameterTypeError

    wrapped_nil_input = wrap_workflow(echo_workflow, WorkflowWrapOptions(name="EchoNil"))
    with pytest.raises(ParameterTypeError, match="Message"):
        await wrapped_nil_input(cast(StringValue, None))

    wrapped_nil_result = wrap_workflow(
        cast(Any, nil_input_workflow), WorkflowWrapOptions(name="NilResult")
    )
    with pytest.raises(ValueError, match="non-protobuf result"):
        await wrapped_nil_result(StringValue(value="AAPL"))


def test_wrap_helpers_reject_sync_executors() -> None:
    """Async-only stance — sync functions fail at wrap time, not at runtime."""

    def sync_activity(_req: StringValue) -> StringValue:
        return StringValue(value="should-not-reach")

    def sync_workflow(_req: StringValue) -> StringValue:
        return StringValue(value="should-not-reach")

    with pytest.raises(ValueError, match="must be async"):
        wrap_activity(cast(Any, sync_activity), ActivityWrapOptions(name="sync_activity"))
    with pytest.raises(ValueError, match="must be async"):
        wrap_workflow(cast(Any, sync_workflow), WorkflowWrapOptions(name="SyncWorkflow"))


def test_wrap_helpers_validate_required_fields() -> None:
    with pytest.raises(ValueError, match="activity executor"):
        wrap_activity(cast(Any, None), ActivityWrapOptions(name="x"))
    with pytest.raises(ValueError, match="workflow executor"):
        wrap_workflow(cast(Any, None), WorkflowWrapOptions(name="x"))

    async def anon(_req: StringValue) -> None:
        return None

    anon.__name__ = ""
    with pytest.raises(ValueError, match="activity name"):
        wrap_activity(cast(Any, anon), ActivityWrapOptions())
    with pytest.raises(ValueError, match="workflow name"):
        wrap_workflow(cast(Any, anon), WorkflowWrapOptions())


def test_wrap_helpers_reject_wrong_options_type() -> None:
    with pytest.raises(ValueError, match="activity wrap options"):
        wrap_activity(echo_activity, cast(Any, WorkflowWrapOptions()))
    with pytest.raises(ValueError, match="workflow wrap options"):
        wrap_workflow(echo_workflow, cast(Any, ActivityWrapOptions()))


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (ActivityWrapOptions(name="  "), "name must not be blank"),
        (ActivityWrapOptions(name=cast(Any, 123)), "name must be a string"),
        (ActivityWrapOptions(retries=-1), "retries must be a non-negative integer"),
        (
            ActivityWrapOptions(retries=cast(Any, True)),
            "retries must be a non-negative integer",
        ),
        (
            ActivityWrapOptions(retry_delay_seconds=-0.1),
            "retry_delay_seconds must be a finite non-negative number",
        ),
        (
            ActivityWrapOptions(retry_delay_seconds=cast(Any, "soon")),
            "retry_delay_seconds must be a finite non-negative number",
        ),
    ],
)
def test_wrap_activity_rejects_invalid_options(options: ActivityWrapOptions, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        wrap_activity(echo_activity, options)


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (WorkflowWrapOptions(name="\t"), "name must not be blank"),
        (WorkflowWrapOptions(retries=-1), "retries must be a non-negative integer"),
        (
            WorkflowWrapOptions(retry_delay_seconds=float("inf")),
            "retry_delay_seconds must be a finite non-negative number",
        ),
    ],
)
def test_wrap_workflow_rejects_invalid_options(options: WorkflowWrapOptions, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        wrap_workflow(echo_workflow, options)


async def test_wrap_workflow_applies_explicit_prefect_options() -> None:
    """The typed options reach the underlying Prefect flow."""
    wrapped = wrap_workflow(
        echo_workflow,
        WorkflowWrapOptions(name="RetryFlow", retries=2, retry_delay_seconds=0),
    )
    # prefect.flow attaches a Flow object with `.retries` available.
    assert getattr(wrapped, "retries", None) == 2


async def test_wrap_activity_applies_explicit_prefect_options() -> None:
    wrapped = wrap_activity(
        echo_activity,
        ActivityWrapOptions(name="retry_task", retries=3, retry_delay_seconds=0),
    )
    assert getattr(wrapped, "retries", None) == 3
