from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, cast
from uuid import uuid4

import pytest
from google.protobuf.wrappers_pb2 import StringValue
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporaless_temporalcompat import (
    ActivityCall,
    ActivityWrapOptions,
    WorkflowWrapOptions,
    execute_activity,
    sleep,
    wrap_activity,
    wrap_workflow,
)

TASK_QUEUE = "temporaless-python-temporalcompat"
retry_attempts = 0


def test_wrappers_reject_non_temporaless_shape() -> None:
    async def run_cases() -> None:
        tests = [
            (
                "activity nil input",
                lambda: wrapped_echo_activity(cast(StringValue, None)),
                "activity input is required",
            ),
            (
                "activity nil result",
                lambda: nil_result_activity(StringValue(value="AAPL")),
                "activity returned a non-protobuf result",
            ),
            (
                "workflow nil input",
                lambda: EchoWorkflow().run(None),
                "workflow input is required",
            ),
            (
                "workflow nil result",
                lambda: NilWorkflow().run(StringValue(value="AAPL")),
                "workflow returned a non-protobuf result",
            ),
        ]

        for _name, run, want in tests:
            with pytest.raises(ValueError, match=want):
                await run()

    asyncio.run(run_cases())


def test_execute_activity_uses_temporal_sdk() -> None:
    result = asyncio.run(run_temporal_workflow(PriceWorkflow, [fetch_price_activity]))

    assert result.value == "AAPL 100.00"


def test_sleep_uses_temporal_sdk_timer() -> None:
    result = asyncio.run(run_temporal_workflow(SleepWorkflow, []))

    assert result.value == "done:AAPL"


def test_retry_policy_uses_temporal_sdk() -> None:
    global retry_attempts
    retry_attempts = 0

    result = asyncio.run(run_temporal_workflow(RetryWorkflow, [flaky_price_activity]))

    assert result.value == "attempts:3"
    assert retry_attempts == 3


def test_timeout_uses_temporal_sdk() -> None:
    result = asyncio.run(run_temporal_workflow(TimeoutWorkflow, []))

    assert result.value == "timeout"


def test_wrap_rejects_sync_executors() -> None:
    """Async-only is the framework stance. Sync functions fail loud at wrap
    time rather than being silently tolerated via runtime hedges."""

    def sync_activity(_input: StringValue) -> StringValue:
        return StringValue(value="should-not-reach")

    def sync_wf(_input: StringValue) -> StringValue:
        return StringValue(value="should-not-reach")

    with pytest.raises(ValueError, match="must be async"):
        wrap_activity(cast(Any, sync_activity), ActivityWrapOptions(name="sync_activity"))
    with pytest.raises(ValueError, match="must be async"):
        wrap_workflow(cast(Any, sync_wf), WorkflowWrapOptions(name="SyncWorkflow"))


def test_wrap_helpers_validate_inputs() -> None:
    with pytest.raises(ValueError, match="activity executor"):
        wrap_activity(cast(Any, None), ActivityWrapOptions(name="x"))
    with pytest.raises(ValueError, match="activity name"):

        async def anon(_input: StringValue) -> None:
            return None

        anon.__name__ = ""
        wrap_activity(cast(Any, anon), ActivityWrapOptions())
    with pytest.raises(ValueError, match="workflow executor"):
        wrap_workflow(cast(Any, None), WorkflowWrapOptions(name="x"))
    with pytest.raises(ValueError, match="workflow name"):

        async def anon_wf(_input: StringValue) -> None:
            return None

        anon_wf.__name__ = ""
        wrap_workflow(cast(Any, anon_wf), WorkflowWrapOptions())

    with pytest.raises(ValueError, match="activity wrap options"):
        wrap_activity(echo_activity, cast(Any, object()))
    with pytest.raises(ValueError, match="workflow wrap options"):
        wrap_workflow(echo_workflow, cast(Any, object()))
    with pytest.raises(ValueError, match="activity name"):
        wrap_activity(echo_activity, ActivityWrapOptions(name=" "))
    with pytest.raises(ValueError, match="workflow name"):
        wrap_workflow(echo_workflow, WorkflowWrapOptions(name=" "))


def test_wrap_workflow_returns_distinct_classes_per_call() -> None:
    """Each wrap_workflow call must return a fresh class — Temporal requires
    distinct workflow types for distinct registrations."""
    a = wrap_workflow(echo_workflow, WorkflowWrapOptions(name="A"))
    b = wrap_workflow(echo_workflow, WorkflowWrapOptions(name="B"))
    assert a is not b
    assert a.__name__ == "A"
    assert b.__name__ == "B"


def test_async_activity_body_executes_via_temporal_sdk() -> None:
    """An async activity body — tests the await path in the activity wrapper."""
    result = asyncio.run(run_temporal_workflow(AsyncActivityWorkflow, [async_fetch_price_activity]))
    assert result.value == "AAPL 100.00"


def test_execute_activity_validates_call_inputs() -> None:
    async def cases() -> None:
        with pytest.raises(ValueError, match="activity is required"):
            await execute_activity(
                ActivityCall(activity=cast(Any, None), result_type=StringValue),
                StringValue(value="x"),
            )
        with pytest.raises(ValueError, match="activity input is required"):
            await execute_activity(
                ActivityCall(activity=fetch_price_activity, result_type=StringValue),
                cast(StringValue, None),
            )
        with pytest.raises(ValueError, match="result type is required"):
            await execute_activity(
                ActivityCall(
                    activity=fetch_price_activity,
                    result_type=cast(type[StringValue], None),
                ),
                StringValue(value="x"),
            )

    asyncio.run(cases())


async def run_temporal_workflow(workflow_type: type, activities: list) -> StringValue:
    try:
        env = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as exc:
        # Temporal's WorkflowEnvironment.start_time_skipping downloads its
        # embedded test-server binary from temporal.download on first run.
        # When the CDN is unreachable the bridge raises "Failed starting
        # test server: error sending request". That's a network/CDN issue,
        # not a framework regression — skip cleanly so CI stays green.
        if "Failed starting test server" in str(exc) or "error sending request" in str(exc):
            pytest.skip(f"temporal test server unavailable (network): {exc}")
        raise
    async with (
        env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[workflow_type],
            activities=activities,
        ),
    ):
        result = await env.client.execute_workflow(
            workflow_type.run,  # ty: ignore[unresolved-attribute]
            StringValue(value="AAPL"),
            id=f"temporaless-{uuid4()}",
            task_queue=TASK_QUEUE,
            result_type=StringValue,
        )
        assert isinstance(result, StringValue)
        return result


async def echo_activity(input_message: StringValue) -> StringValue:
    return StringValue(value=input_message.value)


async def nil_activity(_input_message: StringValue):
    return None


async def echo_workflow(input_message: StringValue) -> StringValue:
    return StringValue(value=input_message.value)


async def nil_workflow(_input_message: StringValue):
    return None


async def fetch_price(input_message: StringValue) -> StringValue:
    return StringValue(value=f"{input_message.value} 100.00")


async def flaky_price(_input_message: StringValue) -> StringValue:
    global retry_attempts
    retry_attempts += 1
    if retry_attempts < 3:
        raise ApplicationError("vendor unavailable", type="VendorUnavailable")
    return StringValue(value=f"attempts:{retry_attempts}")


async def price_workflow(input_message: StringValue) -> StringValue:
    result = await execute_activity(
        ActivityCall(
            activity=fetch_price_activity,
            result_type=StringValue,
            start_to_close_timeout=timedelta(seconds=10),
        ),
        input_message,
    )
    assert isinstance(result, StringValue)
    return result


async def sleep_workflow(input_message: StringValue) -> StringValue:
    await sleep(timedelta(hours=1))
    return StringValue(value=f"done:{input_message.value}")


async def retry_workflow(input_message: StringValue) -> StringValue:
    result = await execute_activity(
        ActivityCall(
            activity=flaky_price_activity,
            result_type=StringValue,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=1),
                backoff_coefficient=1,
                maximum_attempts=3,
            ),
        ),
        input_message,
    )
    assert isinstance(result, StringValue)
    return result


async def async_fetch_price(input_message: StringValue) -> StringValue:
    return StringValue(value=f"{input_message.value} 100.00")


async def async_activity_workflow(input_message: StringValue) -> StringValue:
    result = await execute_activity(
        ActivityCall(
            activity=async_fetch_price_activity,
            result_type=StringValue,
            start_to_close_timeout=timedelta(seconds=10),
        ),
        input_message,
    )
    assert isinstance(result, StringValue)
    return result


async def timeout_workflow(input_message: StringValue) -> StringValue:
    try:
        await execute_activity(
            ActivityCall(
                activity=fetch_price_activity,
                result_type=StringValue,
                task_queue=f"{TASK_QUEUE}-missing",
                schedule_to_start_timeout=timedelta(seconds=1),
                schedule_to_close_timeout=timedelta(seconds=1),
                retry_policy=RetryPolicy(maximum_attempts=1),
            ),
            input_message,
        )
    except ActivityError as err:
        if isinstance(err.cause, TimeoutError):
            return StringValue(value="timeout")
        raise
    return StringValue(value="unexpected")


wrapped_echo_activity = wrap_activity(echo_activity, ActivityWrapOptions(name="echo_activity"))
# `nil_activity` and `nil_workflow` deliberately violate the Message-return
# contract to exercise the runtime guard. Cast around the type checker.
nil_result_activity = wrap_activity(
    cast(Any, nil_activity), ActivityWrapOptions(name="nil_activity")
)
fetch_price_activity = wrap_activity(fetch_price, ActivityWrapOptions(name="fetch_price_activity"))
flaky_price_activity = wrap_activity(flaky_price, ActivityWrapOptions(name="flaky_price_activity"))

async_fetch_price_activity = wrap_activity(
    async_fetch_price, ActivityWrapOptions(name="async_fetch_price_activity")
)

EchoWorkflow = wrap_workflow(echo_workflow, WorkflowWrapOptions(name="EchoWorkflow"))
NilWorkflow = wrap_workflow(cast(Any, nil_workflow), WorkflowWrapOptions(name="NilWorkflow"))
PriceWorkflow = wrap_workflow(price_workflow, WorkflowWrapOptions(name="PriceWorkflow"))
SleepWorkflow = wrap_workflow(sleep_workflow, WorkflowWrapOptions(name="SleepWorkflow"))
RetryWorkflow = wrap_workflow(retry_workflow, WorkflowWrapOptions(name="RetryWorkflow"))
TimeoutWorkflow = wrap_workflow(timeout_workflow, WorkflowWrapOptions(name="TimeoutWorkflow"))
AsyncActivityWorkflow = wrap_workflow(
    async_activity_workflow, WorkflowWrapOptions(name="AsyncActivityWorkflow")
)
