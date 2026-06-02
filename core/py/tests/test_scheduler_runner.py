import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from temporaless import scheduler_runner
from temporaless.cronscheduler import Schedule, Scheduler


async def test_runner_ticks_and_fires_until_stopped() -> None:
    fired: list[tuple[str, datetime]] = []

    async def dispatch(schedule_id: str, fire_time: datetime) -> None:
        fired.append((schedule_id, fire_time))

    scheduler = Scheduler([Schedule(id="m", expression="* * * * *")], dispatch)
    base = datetime(2026, 5, 2, 9, 30, tzinfo=UTC)
    scheduler.seed("m", base)

    stop = asyncio.Event()
    calls = {"n": 0}

    def now() -> datetime:
        calls["n"] += 1
        if calls["n"] >= 4:
            stop.set()
        return base + timedelta(minutes=calls["n"])

    ticks = await scheduler_runner.run(scheduler, 0.001, stop=stop, now=now)
    assert ticks >= 3
    # seeded at base; the minute-cadence ticks at +1/+2/+3 each fire one slot.
    assert len(fired) >= 3


async def test_runner_survives_tick_error() -> None:
    calls = {"n": 0}

    async def dispatch(schedule_id: str, fire_time: datetime) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")

    scheduler = Scheduler([Schedule(id="m", expression="* * * * *")], dispatch)
    base = datetime(2026, 5, 2, 9, 30, tzinfo=UTC)
    scheduler.seed("m", base)

    stop = asyncio.Event()
    it = {"n": 0}

    def now() -> datetime:
        it["n"] += 1
        if it["n"] >= 5:
            stop.set()
        return base + timedelta(minutes=it["n"])

    ticks = await scheduler_runner.run(scheduler, 0.001, stop=stop, now=now)
    assert ticks >= 4  # a failing dispatch is logged, not fatal — the loop lives


async def test_runner_rejects_nonpositive_interval() -> None:
    async def dispatch(schedule_id: str, fire_time: datetime) -> None:
        return None

    scheduler = Scheduler([Schedule(id="m", expression="* * * * *")], dispatch)
    with pytest.raises(ValueError):
        await scheduler_runner.run(scheduler, 0)
