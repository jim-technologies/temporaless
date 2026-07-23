from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.storage import OpenDALStore, WorkflowKey
from temporaless.timerscanner import due_timers
from temporaless.workflow import Options, TimerPendingError, Workflow, run


@pytest.fixture
def root(tmp_path):
    return str(tmp_path)


@pytest.fixture
def operator(root):
    return opendal.AsyncOperator("fs", root=root)


@pytest.fixture
def store(operator):
    return OpenDALStore(operator)


async def test_due_timers_finds_scheduled_timers_inflight(
    operator: opendal.AsyncOperator, store: OpenDALStore
) -> None:
    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        await workflow.sleep("wait:vendor-window", timedelta(hours=1))
        return StringValue(value=f"done:{request.value}")

    with pytest.raises(TimerPendingError):
        await run(
            store,
            Options(
                workflow_id="prices:scanner",
                run_id="2026-05-02",
            ),
            StringValue(value="AAPL"),
            StringValue,
            execute,
        )

    before = datetime.now(UTC) + timedelta(minutes=1)
    assert await due_timers(store, before) == []

    after = datetime.now(UTC) + timedelta(hours=2)
    due = await due_timers(store, after)
    assert len(due) == 1
    assert due[0].key.timer_id == "wait:vendor-window"
    assert due[0].workflow is not None
    assert due[0].workflow.key.workflow_id == "prices:scanner"


async def test_due_timers_skips_fired_timers(
    operator: opendal.AsyncOperator, store: OpenDALStore
) -> None:
    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        await workflow.sleep("wait:zero", timedelta(seconds=0))
        return StringValue(value=f"done:{request.value}")

    await run(
        store,
        Options(
            workflow_id="prices:scanner-fired",
            run_id="2026-05-02",
        ),
        StringValue(value="AAPL"),
        StringValue,
        execute,
    )

    assert await due_timers(store, datetime.now(UTC) + timedelta(hours=1)) == []


async def test_due_timers_skips_timer_under_completed_workflow(
    operator: opendal.AsyncOperator, store: OpenDALStore
) -> None:
    """Correctness check: a SCHEDULED timer whose parent workflow already
    COMPLETED is not a real pending timer — the workflow has moved past it.
    DueTimers must scope to IN_PROGRESS workflows only.
    """
    from google.protobuf.timestamp_pb2 import Timestamp

    from temporaless.storage import (
        TIMER_RECORD_SCHEMA_VERSION,
        WORKFLOW_RECORD_SCHEMA_VERSION,
        TimerKey,
    )
    from temporaless.v1 import temporaless_pb2  # noqa: PLC0415

    workflow_key = WorkflowKey(workflow_id="prices:done", run_id="2026-05-04")
    completed_at = Timestamp()
    completed_at.GetCurrentTime()
    await store.put_workflow(
        temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=workflow_key.to_proto(),
            workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
            status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=completed_at,
        )
    )

    fire_at = Timestamp()
    fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(
        temporaless_pb2.TimerRecord(
            schema_version=TIMER_RECORD_SCHEMA_VERSION,
            key=TimerKey(
                workflow_id="prices:done", run_id="2026-05-04", timer_id="orphan"
            ).to_proto(),
            timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
            status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
            fire_at=fire_at,
        )
    )

    assert await due_timers(store, datetime.now(UTC)) == []


async def test_due_timers_on_empty_store(
    operator: opendal.AsyncOperator, store: OpenDALStore
) -> None:
    """No workflows → no timers. Idempotent baseline."""
    assert await due_timers(store, datetime.now(UTC)) == []
