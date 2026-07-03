"""Background workers helper — wires the periodic adapters (cron scheduler,
timer scanner, janitor) into the workflow service process as toggleable
asyncio.Task loops.

**Why this exists.** Every replica driving cron, timer, or indexed janitor
work is wasteful. Deployers typically want one "operator" replica running these
loops while N "handler" replicas just serve workflow RPCs. This module makes
that wiring tidy without adding new concepts — each loop is opt-in via its
config dataclass; absence means disabled.

**Why not leader election.** Coordination dances (lease + heartbeat) add
complexity the framework explicitly rejects. The simpler answer: deployers
configure only the replicas they want to run background work.

**Safety net if you mis-configure.** If two replicas accidentally both run the
same loop, the framework's replay model still produces correct results — the
second ``workflow.run`` short-circuits via stored records, and indexed sweeps
mirror idempotent run-prefix deletes. The opt-in is purely an efficiency
optimization, not a correctness one.

Typical wiring (operator replica)::

    workers = BackgroundWorkers(
        store,
        query_store=indexed_store,
        cron=CronConfig(scheduler=my_scheduler),
        timer_scanner=TimerScannerConfig(dispatch=dispatch_due_timer),
        janitor=JanitorConfig(max_age=timedelta(days=7)),
    )
    await workers.start()
    try:
        await server.serve()
    finally:
        await workers.stop()

Handler-only replica: skip ``BackgroundWorkers`` entirely. Or construct it with
no config structs — ``start()`` becomes a no-op.

For platforms with their own scheduler (Lambda + EventBridge, Cloud Run + Cloud
Scheduler, Kubernetes CronJob), use those instead — they already provide the
"one-fire-per-tick" semantics this module gives you in-process.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from temporaless.cronscheduler import Scheduler
from temporaless.janitor import sweep as janitor_sweep
from temporaless.storage import DueTimer, QueryStore, Store
from temporaless.timerscanner import due_timers as scan_due_timers

logger = logging.getLogger(__name__)


@dataclass
class CronConfig:
    """Run ``scheduler.tick(now)`` on a loop.

    The scheduler is responsible for invoking its dispatcher per fired
    schedule — this config just drives the tick cadence.
    """

    scheduler: Scheduler
    interval: timedelta = timedelta(seconds=60)


# Callback invoked once per due timer the scanner finds. Typically re-invokes
# the workflow handler so a durable ``workflow.sleep`` resumes. Return cleanly
# from your dispatcher; the loop logs and continues on per-timer errors so one
# bad workflow doesn't stall the whole scanner.
DueTimerDispatcher = Callable[[DueTimer], Awaitable[None]]


@dataclass
class TimerScannerConfig:
    """Poll ``store.due_timers(now)`` and invoke ``dispatch`` for each timer."""

    dispatch: DueTimerDispatcher
    interval: timedelta = timedelta(seconds=60)
    namespace: str = ""  # empty = all namespaces


@dataclass
class JanitorConfig:
    """Periodically sweep COMPLETED runs older than ``max_age`` via a query index."""

    max_age: timedelta
    interval: timedelta = timedelta(hours=24)
    namespace: str = ""  # empty = all namespaces


class BackgroundWorkers:
    """Container for opt-in background loops within the workflow service.

    Construct with the config structs for the loops you want enabled on this
    replica. ``start()`` spawns the asyncio.Tasks; ``stop()`` cancels them and
    awaits clean shutdown.

    Idempotent: ``start()`` after ``start()`` is a no-op; ``stop()`` before
    ``start()`` is a no-op.
    """

    def __init__(
        self,
        store: Store,
        *,
        query_store: QueryStore | None = None,
        cron: CronConfig | None = None,
        timer_scanner: TimerScannerConfig | None = None,
        janitor: JanitorConfig | None = None,
    ) -> None:
        if cron is not None and cron.interval <= timedelta(0):
            raise ValueError("cron.interval must be > 0")
        if timer_scanner is not None and timer_scanner.interval <= timedelta(0):
            raise ValueError("timer_scanner.interval must be > 0")
        if janitor is not None:
            if janitor.interval <= timedelta(0):
                raise ValueError("janitor.interval must be > 0")
            if janitor.max_age <= timedelta(0):
                raise ValueError("janitor.max_age must be > 0")
            if query_store is None:
                raise ValueError("query_store is required when janitor is configured")
        self._store = store
        self._query_store = query_store
        self._cron = cron
        self._timer_scanner = timer_scanner
        self._janitor = janitor
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        """Spawn asyncio.Tasks for each enabled loop. Returns immediately;
        tasks run until ``stop()`` is called (or the loop is cancelled)."""
        if self._tasks:
            return  # already started
        self._stop_event.clear()
        if self._cron is not None:
            self._tasks.append(asyncio.create_task(self._run_cron(self._cron)))
        if self._timer_scanner is not None:
            self._tasks.append(asyncio.create_task(self._run_timer_scanner(self._timer_scanner)))
        if self._janitor is not None:
            self._tasks.append(asyncio.create_task(self._run_janitor(self._janitor)))

    async def stop(self) -> None:
        """Signal stop, then await all tasks. Safe to call when already
        stopped or never started."""
        if not self._tasks:
            return
        self._stop_event.set()
        # Cancel for prompt shutdown — the loops check the event each iteration,
        # but cancellation also unwinds awaits that may be sleeping inside the
        # adapter call itself.
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _run_cron(self, cfg: CronConfig) -> None:
        await self._loop(
            "cron",
            cfg.interval,
            lambda: cfg.scheduler.tick(datetime.now(UTC)),
        )

    async def _run_timer_scanner(self, cfg: TimerScannerConfig) -> None:
        async def tick() -> None:
            due = await scan_due_timers(self._store, datetime.now(UTC), namespace=cfg.namespace)
            for timer in due:
                if self._stop_event.is_set():
                    return
                try:
                    await cfg.dispatch(timer)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # One bad workflow shouldn't stall the scanner. Logged so
                    # operators can find it; framework replay catches anything
                    # the dispatch missed on the next tick.
                    logger.exception(
                        "timer_scanner dispatch failed for %s/%s/%s",
                        timer.key.workflow_id,
                        timer.key.run_id,
                        timer.key.timer_id,
                    )

        await self._loop("timer_scanner", cfg.interval, tick)

    async def _run_janitor(self, cfg: JanitorConfig) -> None:
        if self._query_store is None:
            raise RuntimeError("query_store is required when janitor is configured")
        query_store = self._query_store
        await self._loop(
            "janitor",
            cfg.interval,
            lambda: janitor_sweep(
                query_store, datetime.now(UTC), cfg.max_age, namespace=cfg.namespace
            ),
        )

    async def _loop(
        self,
        name: str,
        interval: timedelta,
        body: Callable[[], Awaitable[object]],
    ) -> None:
        seconds = interval.total_seconds()
        try:
            while not self._stop_event.is_set():
                try:
                    await body()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Log and keep looping — a transient store error shouldn't
                    # kill the worker for the rest of the deployment's lifetime.
                    logger.exception("%s loop iteration failed", name)
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
                    return  # stop requested during sleep
                except TimeoutError:
                    continue
        except asyncio.CancelledError:
            return
