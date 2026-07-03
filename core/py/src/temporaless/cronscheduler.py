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
parses the pointed run_id as a timestamp. This is the recommended pattern when
run_ids follow the
``2026-05-04T09:30:00Z`` convention.
"""

from __future__ import annotations

import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from croniter import croniter

from temporaless.storage import DEFAULT_NAMESPACE, QueryStore, Store
from temporaless.v1 import temporaless_pb2

Dispatcher = Callable[[str, datetime], Awaitable[None]]
_QUERY_FALLBACK_PAGE_SIZE = 32
_QUERY_FALLBACK_MAX_PAGES = 8


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
        """
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        # Plan dispatches under the lock so ``snapshot``/``seed`` from other
        # threads see a consistent view, then dispatch outside the lock so
        # awaiting the dispatcher doesn't block them.
        plan: list[tuple[str, datetime]] = []
        with self._lock:
            for schedule in self._schedules:
                anchor = self._last_fires.get(schedule.id)
                if anchor is None:
                    self._last_fires[schedule.id] = now
                    continue
                iterator = croniter(schedule.expression, anchor)
                next_fire = iterator.get_next(datetime)
                while next_fire <= now:
                    plan.append((schedule.id, next_fire))
                    self._last_fires[schedule.id] = next_fire
                    next_fire = iterator.get_next(datetime)

        for schedule_id, fire_time in plan:
            await self._dispatch(schedule_id, fire_time)
        return len(plan)

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
    run_id_format: str,
    *,
    query: QueryStore | None = None,
) -> datetime | None:
    """Read ``schedule_id``'s latest-run pointer and return the fire time
    parsed from its run_id using ``run_id_format``.

    This is the recommended path to seed the scheduler statelessly: when
    run_ids embed the schedule fire time, a small bucket pointer carries the
    scheduler's "memory". No separate persistence needed.

    Returns ``None`` when no parseable run exists yet. If a query index is
    passed and the pointer is missing or unparseable, the index is used as a
    fallback.
    """
    if not schedule_id:
        raise ValueError("schedule_id is required")
    if not run_id_format:
        raise ValueError("run_id_format is required (e.g. '%Y-%m-%dT%H:%M:%S%z')")

    pointer = await store.get_latest_workflow_run(namespace or DEFAULT_NAMESPACE, schedule_id)
    if pointer is not None:
        parsed = _parse_run_fire_time(pointer.key.run_id, run_id_format)
        if parsed is not None:
            return parsed

    if query is None:
        return None

    page_token = ""
    for _ in range(_QUERY_FALLBACK_MAX_PAGES):
        records, page_token = await query.list_workflows(
            namespace,
            schedule_id,
            temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
            order_by="created_at desc",
            page_size=_QUERY_FALLBACK_PAGE_SIZE,
            page_token=page_token,
        )
        for record in records:
            parsed = _parse_run_fire_time(record.key.run_id, run_id_format)
            if parsed is not None:
                return parsed
        if not page_token:
            break
    return None


async def last_fires_from_runs(
    store: Store,
    namespace: str,
    schedule_ids: list[str],
    run_id_format: str,
    *,
    query: QueryStore | None = None,
) -> dict[str, datetime]:
    """Multi-schedule convenience: returns a snapshot suitable for
    ``Scheduler.restore``."""
    out: dict[str, datetime] = {}
    for schedule_id in schedule_ids:
        last = await last_fire_from_runs(store, namespace, schedule_id, run_id_format, query=query)
        if last is not None:
            out[schedule_id] = last
    return out


def _parse_run_fire_time(run_id: str, run_id_format: str) -> datetime | None:
    try:
        fire_time = datetime.strptime(run_id, run_id_format)
    except ValueError:
        return None
    if fire_time.tzinfo is None:
        fire_time = fire_time.replace(tzinfo=UTC)
    return fire_time
