"""Resident driver for the cron :class:`~temporaless.cronscheduler.Scheduler` —
the optional "serverful" half of scheduling.

The scheduler is serverless on its own: :meth:`Scheduler.tick` is a stateless
step you drive from any external clock (OS cron, a cloud scheduler, a one-shot
function), and ``last_fire_from_runs`` lets bucket latest-run pointers be the
only durable state.
This module is the opposite trade-off, for callers who want one always-on
process: a small loop that ticks the scheduler on a fixed interval until
stopped. It adds **no scheduling logic** — it only owns the clock and the loop,
so it stays out of the core workflow engine (it imports nothing from
``workflow`` or ``storage``).

Single-process by design: the loop relies on the scheduler's in-memory
last-fire state. For multi-process or restart-safe scheduling, prefer the
stateless ``tick`` + ``last_fire_from_runs`` path, where storage is the only
state and any number of interchangeable processes can drive it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from temporaless.cronscheduler import Scheduler

log = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(UTC)


async def run(
    scheduler: Scheduler,
    interval_seconds: float,
    *,
    stop: asyncio.Event | None = None,
    now: Callable[[], datetime] = _utc_now,
) -> int:
    """Tick ``scheduler`` every ``interval_seconds`` until ``stop`` is set.

    Each iteration calls ``await scheduler.tick(now())``. The scheduler's own
    catch-up loop dispatches every slot due since the last tick, so a delayed
    iteration (slow dispatch, a paused process) self-corrects on the next tick
    rather than dropping fires. A failing tick is logged and the loop
    continues — a resident scheduler should not die on one bad dispatch.

    Wire ``stop`` to your shutdown signal (e.g. set it from a SIGTERM handler)
    for graceful exit; the inter-tick sleep is interrupted the moment it is set.

    Returns the number of ticks performed (useful for tests and shutdown logs).
    """
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    stop = stop or asyncio.Event()
    ticks = 0
    while not stop.is_set():
        try:
            await scheduler.tick(now())
        except Exception:
            log.exception("scheduler tick failed")
        ticks += 1
        # Sleep until the next tick, but wake immediately if asked to stop.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
    return ticks
