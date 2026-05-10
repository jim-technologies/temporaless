"""Sweep completed workflow runs older than a max-age threshold.

Thin wrapper around :meth:`Store.sweep` so callers that imported
``janitor.sweep`` directly keep working. Backend-agnostic — operates over the
Store interface so it works for any backend, including remote ConnectStore
(which forwards to the server-side ``Sweep`` RPC in a single round-trip).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from temporaless.storage import Store


async def sweep(store: Store, now: datetime, max_age: timedelta, *, namespace: str = "") -> int:
    """Delete COMPLETED workflow runs older than ``max_age``. Returns the
    number of runs deleted."""
    return await store.sweep(namespace, now, max_age)
