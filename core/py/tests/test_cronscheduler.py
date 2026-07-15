import asyncio
from datetime import UTC, datetime

import opendal
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.cronscheduler import (
    Schedule,
    Scheduler,
    last_fire_from_runs,
    last_fires_from_runs,
)
from temporaless.storage import OpenDALStore
from temporaless.workflow import Options, run


async def test_tick_fires_due_schedules_after_seed() -> None:
    fired: list[tuple[str, datetime]] = []

    async def dispatch(schedule_id: str, fire_time: datetime) -> None:
        fired.append((schedule_id, fire_time))

    scheduler = Scheduler(
        [Schedule(id="every-minute", expression="* * * * *")],
        dispatch,
    )
    scheduler.seed("every-minute", datetime(2026, 5, 2, 9, 30, tzinfo=UTC))

    count = await scheduler.tick(datetime(2026, 5, 2, 9, 33, 5, tzinfo=UTC))
    assert count == 3
    assert [time for _, time in fired] == [
        datetime(2026, 5, 2, 9, 31, tzinfo=UTC),
        datetime(2026, 5, 2, 9, 32, tzinfo=UTC),
        datetime(2026, 5, 2, 9, 33, tzinfo=UTC),
    ]


@pytest.mark.parametrize(
    ("failed_minute", "committed_minute", "retried_minutes"),
    [
        (31, 30, [31, 32, 33]),
        (32, 31, [32, 33]),
    ],
)
async def test_tick_commits_only_successful_dispatches(
    failed_minute: int,
    committed_minute: int,
    retried_minutes: list[int],
) -> None:
    attempted: list[datetime] = []
    fail_once = True

    async def dispatch(_schedule_id: str, fire_time: datetime) -> None:
        nonlocal fail_once
        attempted.append(fire_time)
        if fail_once and fire_time.minute == failed_minute:
            fail_once = False
            raise RuntimeError("dispatch failed")

    scheduler = Scheduler(
        [Schedule(id="every-minute", expression="* * * * *")],
        dispatch,
    )
    scheduler.seed("every-minute", datetime(2026, 5, 2, 9, 30, tzinfo=UTC))
    now = datetime(2026, 5, 2, 9, 33, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="dispatch failed"):
        await scheduler.tick(now)

    assert scheduler.last_fire("every-minute") == datetime(
        2026, 5, 2, 9, committed_minute, tzinfo=UTC
    )

    attempted.clear()
    count = await scheduler.tick(now)

    assert count == len(retried_minutes)
    assert [fire_time.minute for fire_time in attempted] == retried_minutes
    assert scheduler.last_fire("every-minute") == datetime(2026, 5, 2, 9, 33, tzinfo=UTC)


async def test_concurrent_ticks_do_not_dispatch_the_same_fire_twice() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    fired: list[datetime] = []

    async def dispatch(_schedule_id: str, fire_time: datetime) -> None:
        fired.append(fire_time)
        entered.set()
        await release.wait()

    scheduler = Scheduler(
        [Schedule(id="every-minute", expression="* * * * *")],
        dispatch,
    )
    scheduler.seed("every-minute", datetime(2026, 5, 2, 9, 30, tzinfo=UTC))
    now = datetime(2026, 5, 2, 9, 31, tzinfo=UTC)

    first = asyncio.create_task(scheduler.tick(now))
    await asyncio.wait_for(entered.wait(), timeout=1)
    second = asyncio.create_task(scheduler.tick(now))
    await asyncio.sleep(0)
    release.set()

    assert await first == 1
    assert await second == 0
    assert fired == [now]


async def test_tick_without_seed_anchors_to_first_tick() -> None:
    dispatched = 0

    async def dispatch(_id: str, _t: datetime) -> None:
        nonlocal dispatched
        dispatched += 1

    scheduler = Scheduler([Schedule(id="every-minute", expression="* * * * *")], dispatch)

    first = await scheduler.tick(datetime(2026, 5, 2, 9, 30, tzinfo=UTC))
    assert first == 0
    assert dispatched == 0

    second = await scheduler.tick(datetime(2026, 5, 2, 9, 31, 30, tzinfo=UTC))
    assert second == 1
    assert dispatched == 1


async def test_weekday_schedule_skips_weekend() -> None:
    fired: list[datetime] = []

    async def dispatch(_id: str, fire_time: datetime) -> None:
        fired.append(fire_time)

    scheduler = Scheduler(
        [Schedule(id="weekday-open", expression="30 9 * * 1-5")],
        dispatch,
    )
    scheduler.seed("weekday-open", datetime(2026, 5, 2, 0, 0, tzinfo=UTC))  # Saturday

    count = await scheduler.tick(datetime(2026, 5, 4, 9, 35, tzinfo=UTC))  # Monday
    assert count == 1
    assert fired == [datetime(2026, 5, 4, 9, 30, tzinfo=UTC)]


async def _noop_dispatch(_id: str, _t: datetime) -> None:
    return None


@pytest.mark.parametrize(
    "schedules",
    [
        [Schedule(id="x", expression="* * * * *"), Schedule(id="x", expression="0 9 * * *")],
        [Schedule(id="x", expression="not a cron")],
        [Schedule(id="", expression="* * * * *")],
    ],
)
def test_constructor_rejects_bad_input(schedules: list[Schedule]) -> None:
    with pytest.raises(ValueError):
        Scheduler(schedules, _noop_dispatch)


async def test_snapshot_and_restore_carry_state_across_processes() -> None:
    dispatched: list[datetime] = []

    async def dispatch(_id: str, fire_time: datetime) -> None:
        dispatched.append(fire_time)

    first = Scheduler([Schedule(id="every-minute", expression="* * * * *")], dispatch)
    first.seed("every-minute", datetime(2026, 5, 4, 9, 30, tzinfo=UTC))
    await first.tick(datetime(2026, 5, 4, 9, 32, 30, tzinfo=UTC))
    assert len(dispatched) == 2

    snapshot = first.snapshot()
    assert snapshot["every-minute"] == datetime(2026, 5, 4, 9, 32, tzinfo=UTC)

    dispatched.clear()
    revived = Scheduler([Schedule(id="every-minute", expression="* * * * *")], dispatch)
    revived.restore(snapshot)
    await revived.tick(datetime(2026, 5, 4, 9, 33, 30, tzinfo=UTC))
    assert dispatched == [datetime(2026, 5, 4, 9, 33, tzinfo=UTC)]


async def _ok_workflow(_w, _r):
    return StringValue(value="ok")


def _timestamp(value: datetime) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromDatetime(value)
    return timestamp


async def test_last_fire_from_runs_derives_state_from_storage(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    fire_times = [
        datetime(2026, 5, 4, 9, 30, tzinfo=UTC),
        datetime(2026, 5, 4, 9, 31, tzinfo=UTC),
        datetime(2026, 5, 4, 9, 32, tzinfo=UTC),
    ]
    for fire_time in fire_times:
        await run(
            store,
            Options(
                workflow_id="prices:aapl",
                run_id=fire_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                code_version="test",
                run_order_time=_timestamp(fire_time),
            ),
            StringValue(value="AAPL"),
            StringValue,
            _ok_workflow,
        )

    last = await last_fire_from_runs(store, "", "prices:aapl")
    assert last is not None
    assert last.replace(tzinfo=UTC) == datetime(2026, 5, 4, 9, 32, tzinfo=UTC)


async def test_last_fire_from_runs_reads_latest_pointer_once(tmp_path) -> None:
    class CountingStore(OpenDALStore):
        def __init__(self, operator: opendal.AsyncOperator) -> None:
            super().__init__(operator)
            self.latest_reads = 0

        async def get_latest_workflow_run(self, namespace: str, workflow_id: str):
            self.latest_reads += 1
            return await super().get_latest_workflow_run(namespace, workflow_id)

    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = CountingStore(operator)
    await run(
        store,
        Options(
            workflow_id="prices:aapl",
            run_id="2026-05-04T09:32:00Z",
            code_version="test",
            run_order_time=_timestamp(datetime(2026, 5, 4, 9, 32, tzinfo=UTC)),
        ),
        StringValue(value="AAPL"),
        StringValue,
        _ok_workflow,
    )
    store.latest_reads = 0

    last = await last_fire_from_runs(store, "", "prices:aapl")

    assert last == datetime(2026, 5, 4, 9, 32, tzinfo=UTC)
    assert store.latest_reads == 1


async def test_last_fires_from_runs_skips_unknown_schedules(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    await run(
        store,
        Options(
            workflow_id="prices:aapl",
            run_id="2026-05-04T09:32:00Z",
            code_version="test",
            run_order_time=_timestamp(datetime(2026, 5, 4, 9, 32, tzinfo=UTC)),
        ),
        StringValue(value="AAPL"),
        StringValue,
        _ok_workflow,
    )

    snapshot = await last_fires_from_runs(
        store,
        "",
        ["prices:aapl", "prices:never-ran"],
    )
    assert "prices:aapl" in snapshot
    assert "prices:never-ran" not in snapshot
