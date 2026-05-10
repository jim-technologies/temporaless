"""Find due timer records for in-flight workflows so a serverless or worker
process can re-invoke the workflow handler.

Thin wrapper around :meth:`Store.due_timers` so callers that imported
``timerscanner.due_timers`` directly keep working. Backend-agnostic — works
against any Store, including a remote ConnectStore (which forwards to the
server-side ``DueTimers`` RPC in a single round-trip).

Stale timers under COMPLETED or FAILED workflows are intentionally skipped:
the workflow has already moved past them.
"""

from __future__ import annotations

from datetime import datetime

from temporaless.storage import DueTimer, Store


async def due_timers(store: Store, now: datetime, *, namespace: str = "") -> list[DueTimer]:
    """Return SCHEDULED timers belonging to IN_PROGRESS workflows whose
    ``fire_at`` is at or before ``now``.
    """
    return await store.due_timers(namespace, now)


__all__ = ["DueTimer", "due_timers"]
