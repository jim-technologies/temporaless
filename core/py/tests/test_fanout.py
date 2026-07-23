from __future__ import annotations

import asyncio
from datetime import timedelta

import opendal
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.wrappers_pb2 import StringValue

from temporaless import (
    ActivityError,
    ActivityOptions,
    OpenDALStore,
    Options,
    RetryPolicy,
    TimerPendingError,
    Workflow,
    gather_activities,
    run,
)
from temporaless.storage import ActivityKey, WorkflowKey
from temporaless.v1 import temporaless_pb2


@pytest.fixture
def store(tmp_path) -> OpenDALStore:
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


async def test_gather_activities_preserves_call_order() -> None:
    async def branch(value: str, delay: float) -> StringValue:
        await asyncio.sleep(delay)
        return StringValue(value=value)

    results = await gather_activities(
        branch("first", 0.02),
        branch("second", 0),
    )

    assert [result.value for result in results] == ["first", "second"]


async def test_gather_activities_waits_for_slow_sibling_before_workflow_fails(
    store: OpenDALStore,
) -> None:
    slow_started = asyncio.Event()
    fast_failed = asyncio.Event()
    release_slow = asyncio.Event()
    slow_finished = asyncio.Event()

    async def workflow_body(workflow: Workflow, _request: StringValue) -> StringValue:
        async def slow(_request: StringValue) -> StringValue:
            slow_started.set()
            await release_slow.wait()
            slow_finished.set()
            return StringValue(value="slow:done")

        async def fail(_request: StringValue) -> StringValue:
            await slow_started.wait()
            fast_failed.set()
            raise ActivityError("fatal", "fast branch failed")

        await gather_activities(
            workflow.execute_activity(
                ActivityOptions(activity_id="slow"),
                StringValue(value="slow"),
                StringValue,
                slow,
            ),
            workflow.execute_activity(
                ActivityOptions(activity_id="fast-failure"),
                StringValue(value="fast"),
                StringValue,
                fail,
            ),
        )
        return StringValue(value="unreachable")

    invocation = asyncio.create_task(
        run(
            store,
            Options(workflow_id="fanout:settle", run_id="run"),
            StringValue(value="request"),
            StringValue,
            workflow_body,
        )
    )
    await asyncio.wait_for(fast_failed.wait(), timeout=1)
    await asyncio.sleep(0)
    assert not invocation.done()
    assert not slow_finished.is_set()

    release_slow.set()
    with pytest.raises(ActivityError, match="fast branch failed"):
        await invocation

    assert slow_finished.is_set()
    slow_record = await store.get_activity(
        ActivityKey(workflow_id="fanout:settle", run_id="run", activity_id="slow")
    )
    failed_record = await store.get_activity(
        ActivityKey(
            workflow_id="fanout:settle",
            run_id="run",
            activity_id="fast-failure",
        )
    )
    assert slow_record is not None
    assert slow_record.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert failed_record is not None
    assert failed_record.status == temporaless_pb2.ACTIVITY_STATUS_FAILED


async def test_gather_activities_continuation_keeps_workflow_in_progress(
    store: OpenDALStore,
) -> None:
    retry_interval = Duration()
    retry_interval.FromTimedelta(timedelta(hours=1))
    durable_threshold = Duration()
    durable_threshold.FromTimedelta(timedelta(seconds=1))
    durable_policy = RetryPolicy(
        maximum_attempts=2,
        initial_interval=retry_interval,
        durable_backoff_threshold=durable_threshold,
    )

    async def workflow_body(workflow: Workflow, _request: StringValue) -> StringValue:
        async def permanent_failure(_request: StringValue) -> StringValue:
            raise ActivityError("permanent", "terminal branch")

        async def retry_later(_request: StringValue) -> StringValue:
            raise ActivityError("transient", "pending branch")

        await gather_activities(
            workflow.execute_activity(
                ActivityOptions(activity_id="terminal"),
                StringValue(value="terminal"),
                StringValue,
                permanent_failure,
            ),
            workflow.execute_activity(
                ActivityOptions(
                    activity_id="pending",
                    retry_policy=durable_policy,
                    retry_timer_id="retry:pending",
                ),
                StringValue(value="pending"),
                StringValue,
                retry_later,
            ),
        )
        return StringValue(value="unreachable")

    options = Options(
        workflow_id="fanout:pending",
        run_id="run",
    )
    with pytest.raises(TimerPendingError):
        await run(
            store,
            options,
            StringValue(value="request"),
            StringValue,
            workflow_body,
        )

    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    terminal_record = await store.get_activity(
        ActivityKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
            activity_id="terminal",
        )
    )
    pending_record = await store.get_activity(
        ActivityKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
            activity_id="pending",
        )
    )
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    assert terminal_record is not None
    assert terminal_record.status == temporaless_pb2.ACTIVITY_STATUS_FAILED
    assert pending_record is not None
    assert pending_record.status == temporaless_pb2.ACTIVITY_STATUS_RETRYING


async def test_gather_activities_aggregates_terminal_failures() -> None:
    async def fail(code: str) -> StringValue:
        raise ActivityError(code, f"{code} failed")

    with pytest.raises(ExceptionGroup) as exc_info:
        await gather_activities(fail("one"), fail("two"))

    assert [str(exc) for exc in exc_info.value.exceptions] == [
        "activity error [one]: one failed",
        "activity error [two]: two failed",
    ]


async def test_gather_activities_propagates_base_exception_after_settling() -> None:
    class StopFanout(BaseException):
        pass

    settled: list[str] = []

    async def fail_terminally() -> StringValue:
        settled.append("terminal")
        raise ActivityError("failed", "ordinary failure")

    async def stop() -> StringValue:
        await asyncio.sleep(0)
        settled.append("stop")
        raise StopFanout

    with pytest.raises(StopFanout):
        await gather_activities(fail_terminally(), stop())

    assert sorted(settled) == ["stop", "terminal"]


async def test_gather_activities_cancellation_drains_every_child() -> None:
    started = [asyncio.Event(), asyncio.Event()]
    drained: list[int] = []

    async def block(index: int) -> StringValue:
        started[index].set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            drained.append(index)
            raise
        return StringValue(value="unreachable")

    fanout = asyncio.create_task(gather_activities(block(0), block(1)))
    await asyncio.gather(*(event.wait() for event in started))
    fanout.cancel()

    with pytest.raises(asyncio.CancelledError):
        await fanout
    assert sorted(drained) == [0, 1]


async def test_gather_activities_accepts_empty_fanout() -> None:
    assert await gather_activities() == []
