"""Sweep completed workflow runs older than a max-age threshold.

Retention sweep is a cross-run query. Use an index-backed QueryStore for the
online janitor; bucket-only deployments should use lifecycle rules or an
offline scan.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from temporaless.storage import QueryStore


async def sweep(
    query: QueryStore, now: datetime, max_age: timedelta, *, namespace: str = ""
) -> int:
    """Delete COMPLETED workflow runs older than ``max_age``. Returns the
    number of runs deleted."""
    return await query.sweep(namespace, now, max_age)
