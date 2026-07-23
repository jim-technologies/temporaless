"""Tests for the BackgroundWorkers helper.

The helper wires the existing periodic adapters (cron scheduler, timer
scanner, janitor) into asyncio.Task loops the workflow service process can
opt into per-replica. Tests cover: opt-in/opt-out, tick cadence, clean
shutdown, error resilience.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import opendal
import pytest
from temporaless_indexstore import IndexedStore

from temporaless.background import (
    BackgroundWorkers,
    CronConfig,
    JanitorConfig,
    TimerScannerConfig,
)
from temporaless.cronscheduler import Schedule, Scheduler


@pytest.fixture
def store(tmp_path):
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    return IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")


async def test_no_config_start_is_noop(store):
    """Construct with no config structs → start/stop is a no-op."""
    workers = BackgroundWorkers(store)
    await workers.start()
    # No tasks should be running.
    await workers.stop()  # safe, doesn't hang


async def test_cron_loop_ticks(store):
    """Cron config drives scheduler.tick() at the configured interval."""
    dispatches: list[tuple[str, datetime]] = []

    async def dispatch(schedule_id: str, fire_time: datetime) -> None:
        dispatches.append((schedule_id, fire_time))

    scheduler = Scheduler(
        [Schedule(id="hourly", expression="0 * * * *")],
        dispatch,
    )
    # Seed last_fire well in the past so each tick fires the schedule.
    scheduler.seed("hourly", datetime(2020, 1, 1, tzinfo=UTC))

    workers = BackgroundWorkers(
        store,
        cron=CronConfig(scheduler=scheduler, interval=timedelta(milliseconds=50)),
    )
    await workers.start()
    await asyncio.sleep(0.2)  # ~3 ticks
    await workers.stop()
    assert len(dispatches) > 0, "expected at least one dispatch"


async def test_timer_scanner_loop_dispatches(store):
    """Timer scanner invokes dispatch for each due timer; transient
    dispatcher errors don't kill the loop."""
    seen: list[str] = []
    dispatched_both = asyncio.Event()

    async def dispatch(timer) -> None:
        seen.append(timer.key.timer_id)
        if {"good", "bad"}.issubset(seen):
            dispatched_both.set()
        if "bad" in timer.key.timer_id:
            raise RuntimeError("simulated dispatch failure")

    # Seed an in-progress workflow with a due timer.
    from google.protobuf.timestamp_pb2 import Timestamp

    from temporaless.storage import (
        TIMER_RECORD_SCHEMA_VERSION,
        WORKFLOW_RECORD_SCHEMA_VERSION,
        TimerKey,
        WorkflowKey,
    )
    from temporaless.v1 import temporaless_pb2

    now = Timestamp()
    now.GetCurrentTime()
    past = Timestamp()
    past.FromDatetime(datetime.now(UTC) - timedelta(minutes=1))

    workflow_record = temporaless_pb2.WorkflowRecord(
        schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
        key=WorkflowKey(workflow_id="wf", run_id="r").to_proto(),
        workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
        status=temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        created_at=now,
    )
    await store.put_workflow(workflow_record)
    for tid in ("good", "bad"):
        timer = temporaless_pb2.TimerRecord(
            schema_version=TIMER_RECORD_SCHEMA_VERSION,
            key=TimerKey(workflow_id="wf", run_id="r", timer_id=tid).to_proto(),
            timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
            status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
            fire_at=past,
            created_at=now,
        )
        await store.put_timer(timer)

    workers = BackgroundWorkers(
        store,
        timer_scanner=TimerScannerConfig(dispatch=dispatch, interval=timedelta(milliseconds=50)),
    )
    await workers.start()
    try:
        await asyncio.wait_for(dispatched_both.wait(), timeout=2)
    finally:
        await workers.stop()

    assert "good" in seen, "scanner should have dispatched the good timer"
    assert "bad" in seen, "scanner should have attempted the bad timer"
    # The good timer may have been seen multiple times (still SCHEDULED) — that's
    # fine; the scanner is idempotent.


async def test_janitor_loop_runs(store):
    """Janitor config runs Store.sweep on a loop."""
    from google.protobuf.timestamp_pb2 import Timestamp

    from temporaless.storage import WORKFLOW_RECORD_SCHEMA_VERSION, WorkflowKey
    from temporaless.v1 import temporaless_pb2

    # Seed an old COMPLETED workflow that should be swept.
    old = Timestamp()
    old.FromDatetime(datetime.now(UTC) - timedelta(hours=2))
    record = temporaless_pb2.WorkflowRecord(
        schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
        key=WorkflowKey(workflow_id="wf", run_id="old").to_proto(),
        workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
        status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        created_at=old,
        completed_at=old,
    )
    await store.put_workflow(record)

    workers = BackgroundWorkers(
        store,
        query_store=store,
        janitor=JanitorConfig(max_age=timedelta(hours=1), interval=timedelta(milliseconds=50)),
    )
    await workers.start()

    async def wait_until_swept() -> None:
        key = WorkflowKey(workflow_id="wf", run_id="old")
        while await store.get_workflow(key) is not None:
            await asyncio.sleep(0.01)

    try:
        await asyncio.wait_for(wait_until_swept(), timeout=2)
    finally:
        await workers.stop()

    swept = await store.get_workflow(WorkflowKey(workflow_id="wf", run_id="old"))
    assert swept is None, "janitor should have swept the old COMPLETED workflow"


async def test_stop_is_idempotent(store):
    workers = BackgroundWorkers(store)
    await workers.stop()  # before start
    await workers.start()
    await workers.stop()
    await workers.stop()  # after stop


async def test_start_is_idempotent(store):
    """Calling start() twice doesn't double-spawn tasks."""

    async def noop_dispatch(schedule_id, fire_time):
        pass

    scheduler = Scheduler([], noop_dispatch)
    workers = BackgroundWorkers(
        store,
        cron=CronConfig(scheduler=scheduler, interval=timedelta(seconds=1)),
    )
    await workers.start()
    task_count = len(workers._tasks)  # type: ignore[reportPrivateUsage]
    await workers.start()
    assert len(workers._tasks) == task_count, "second start() must not spawn duplicates"
    await workers.stop()


async def test_validation_rejects_bad_intervals(store):
    """Intervals must be > 0; negative max_age rejected."""

    async def noop_dispatch(schedule_id, fire_time):
        pass

    scheduler = Scheduler([], noop_dispatch)
    with pytest.raises(ValueError, match="interval"):
        BackgroundWorkers(
            store,
            cron=CronConfig(scheduler=scheduler, interval=timedelta(0)),
        )
    with pytest.raises(ValueError, match="max_age"):
        BackgroundWorkers(
            store,
            janitor=JanitorConfig(max_age=timedelta(0)),
        )


async def test_loop_iteration_error_does_not_kill_worker(store):
    """A transient error inside a tick must not kill the worker — log and
    continue is the contract. Use a tick-counting fake that always raises so
    we can verify the loop iterates regardless of cron's internal state."""

    class AlwaysFailingScheduler:
        def __init__(self) -> None:
            self.tick_count = 0

        async def tick(self, now: datetime) -> int:
            self.tick_count += 1
            raise RuntimeError("boom")

    scheduler = AlwaysFailingScheduler()
    workers = BackgroundWorkers(
        store,
        cron=CronConfig(scheduler=scheduler, interval=timedelta(milliseconds=30)),  # type: ignore[arg-type]
    )
    await workers.start()
    await asyncio.sleep(0.15)
    await workers.stop()
    # Multiple ticks happened despite each one raising.
    assert scheduler.tick_count >= 2, (
        f"expected loop to recover after errors; tick_count={scheduler.tick_count}"
    )
