"""Tiny cron-style scheduler for the schedule-driven trigger half.

Callers hand in a list of cron schedules and a dispatcher callback;
``tick(now)`` computes which schedules are due since the last fire and invokes
the dispatcher with the schedule ID and fire time.

The scheduler is stateful but the state is fully serializable. For distributed
or restartable use:

- Call ``snapshot()`` to extract the current last-fires map.
- Persist it externally (storage, SQL, KV).
- On next boot, call ``restore()`` with the persisted map.

For fully storage-derived state (no separate persistence), pair the scheduler
with ``last_fire_from_runs``: it reads the schedule's latest-run pointer and
uses its protobuf ``run_order_time``. Dispatchers set that field to the
scheduled fire time when constructing ``WorkflowOptions``.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from croniter import croniter

from temporaless.storage import DEFAULT_NAMESPACE, Store

Dispatcher = Callable[[str, datetime], Awaitable[None]]


@dataclass(frozen=True)
class Schedule:
    id: str
    expression: str


class Scheduler:
    def __init__(self, schedules: list[Schedule], dispatch: Dispatcher) -> None:
        seen: set[str] = set()
        self._schedules: list[Schedule] = []
        self._cron: dict[str, croniter] = {}
        for schedule in schedules:
            if not schedule.id:
                raise ValueError("schedule id is required")
            if schedule.id in seen:
                raise ValueError(f"duplicate schedule id {schedule.id!r}")
            seen.add(schedule.id)
            try:
                self._cron[schedule.id] = croniter(schedule.expression)
            except (ValueError, KeyError) as exc:
                raise ValueError(f"schedule {schedule.id!r}: {exc}") from exc
            self._schedules.append(schedule)
        self._dispatch = dispatch
        self._last_fires: dict[str, datetime] = {}
        self._lock = threading.Lock()
        self._tick_lock = asyncio.Lock()

    def seed(self, schedule_id: str, last_fire: datetime) -> None:
        if last_fire.tzinfo is None:
            raise ValueError("last_fire must be timezone-aware")
        with self._lock:
            if schedule_id not in self._cron:
                raise ValueError(f"unknown schedule {schedule_id!r}")
            self._last_fires[schedule_id] = last_fire

    def last_fire(self, schedule_id: str) -> datetime | None:
        with self._lock:
            return self._last_fires.get(schedule_id)

    async def tick(self, now: datetime) -> int:
        """Dispatch every schedule due since the last fire. Returns the number
        of dispatches.

        The dispatcher is awaited sequentially per schedule fire — if you need
        parallel dispatch, wrap the dispatcher with ``asyncio.create_task``.
        Catch-up is generated one fire at a time so a long outage cannot build
        an unbounded in-memory plan before the first dispatch.
        """
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        # Serialize ticks so two concurrent callers cannot plan the same fire.
        # Never hold the thread lock while awaiting user code: dispatchers may
        # safely inspect or update scheduler state.
        async with self._tick_lock:
            dispatched = 0
            for schedule in self._schedules:
                with self._lock:
                    anchor = self._last_fires.get(schedule.id)
                    if anchor is None:
                        self._last_fires[schedule.id] = now
                        continue

                next_fire = croniter(schedule.expression, anchor).get_next(datetime)
                while next_fire <= now:
                    await self._dispatch(schedule.id, next_fire)
                    # A fire is durable scheduler state only after dispatch
                    # succeeds. If dispatch raises, the failed fire remains due
                    # on the next tick while earlier successes stay committed.
                    with self._lock:
                        current = self._last_fires.get(schedule.id)
                        if current is None or next_fire > current:
                            current = next_fire
                            self._last_fires[schedule.id] = current
                    dispatched += 1
                    next_fire = croniter(schedule.expression, current).get_next(datetime)
            return dispatched

    def snapshot(self) -> dict[str, datetime]:
        """Return a copy of the current last-fire map for external persistence."""
        with self._lock:
            return dict(self._last_fires)

    def restore(self, snapshot: dict[str, datetime]) -> None:
        """Replace the in-memory last-fire map with ``snapshot``.

        Schedules in the snapshot but not in the scheduler are silently ignored.
        Schedules in the scheduler but not in the snapshot keep whatever state
        they had (typically: nothing — the first ``tick`` will anchor them to
        ``now``).
        """
        with self._lock:
            for schedule_id, fire_time in snapshot.items():
                if fire_time.tzinfo is None:
                    raise ValueError(f"snapshot[{schedule_id!r}] must be timezone-aware")
                if schedule_id not in self._cron:
                    continue
                self._last_fires[schedule_id] = fire_time


async def last_fire_from_runs(
    store: Store,
    namespace: str,
    schedule_id: str,
) -> datetime | None:
    """Read ``schedule_id``'s latest-run pointer and return its logical
    run-order time.

    Schedule dispatchers set ``WorkflowOptions.run_order_time`` to the fire
    time. The pointer persists that protobuf timestamp, so SDKs never parse
    opaque caller-owned run IDs or depend on language-specific date formats.

    Returns ``None`` when no valid referenced run exists yet.
    """
    if not schedule_id:
        raise ValueError("schedule_id is required")

    pointer = await store.get_latest_workflow_run(namespace or DEFAULT_NAMESPACE, schedule_id)
    if pointer is None or not pointer.HasField("run_order_time"):
        return None
    try:
        return pointer.run_order_time.ToDatetime(tzinfo=UTC)
    except OverflowError, ValueError:
        return None


async def last_fires_from_runs(
    store: Store,
    namespace: str,
    schedule_ids: list[str],
) -> dict[str, datetime]:
    """Multi-schedule convenience: returns a snapshot suitable for
    ``Scheduler.restore``."""
    out: dict[str, datetime] = {}
    for schedule_id in schedule_ids:
        last = await last_fire_from_runs(store, namespace, schedule_id)
        if last is not None:
            out[schedule_id] = last
    return out
