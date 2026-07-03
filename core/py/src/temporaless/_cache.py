"""Run-scoped record cache for workflow replay.

Mirrors ``core/go/workflow/cache.go``. Wraps a :class:`Store` and serves
get_activity / get_timer / get_event / get_workflow calls from memory after a
single bulk prefetch. Writes go through to the underlying store and update the
cache (write-through). Out-of-scope reads (different workflow_id / run_id)
pass straight through — useful for cross-pipeline dependencies adapters.

Replay flow on a workflow with N parallel-fan-out activities:

- Without cache: each re-invocation issues N individual ``get_activity``
  round-trips against the store.
- With cache: one ``list_activities`` call up front, then every
  ``get_activity`` hits memory. Same for timers and events.

The wrapper exposes the same Protocol shape as the underlying store and is
intentionally private — workflow.run constructs it, no caller needs to.
"""

from __future__ import annotations

import asyncio
from datetime import datetime

from temporaless.storage import (
    ActivityKey,
    ClaimCapability,
    ClaimKey,
    ClaimStore,
    DueTimer,
    EventKey,
    Store,
    TimerKey,
    WorkflowKey,
    activity_key_from_proto,
    event_key_from_proto,
    timer_key_from_proto,
    workflow_key_from_proto,
)
from temporaless.v1 import temporaless_pb2


class RunScopedCache:
    """Wraps a Store with an in-memory cache scoped to one ``WorkflowKey``.

    Constructed by :func:`temporaless.workflow.run` and never exposed
    directly. Implements the full Store protocol so it can transparently
    substitute for the underlying store in the workflow body.
    """

    def __init__(self, inner: Store, scope: WorkflowKey) -> None:
        self._inner = inner
        self._scope = scope
        self._lock = asyncio.Lock()

        self._workflow_known: bool = False
        self._workflow: temporaless_pb2.WorkflowRecord | None = None

        self._activities_listed: bool = False
        # Value None records a negative-cache entry ("we looked, not present").
        self._activities: dict[str, temporaless_pb2.ActivityRecord | None] = {}

        self._timers_listed: bool = False
        self._timers: dict[str, temporaless_pb2.TimerRecord | None] = {}

        self._events_listed: bool = False
        self._events: dict[str, temporaless_pb2.EventRecord | None] = {}

    async def prefetch(self) -> None:
        """Issue list_activities + list_timers + list_events in parallel and
        populate the cache. After prefetch, get-by-key for a record not in the
        result short-circuits to None without an underlying round-trip.

        Worth calling only when the workflow record exists in IN_PROGRESS
        state — a fresh run has nothing to prefetch.
        """
        activities, timers, events = await asyncio.gather(
            self._inner.list_activities(self._scope),
            self._inner.list_timers(self._scope, temporaless_pb2.TIMER_STATUS_UNSPECIFIED),
            self._inner.list_events(self._scope),
        )
        async with self._lock:
            for record in activities:
                self._activities[activity_key_from_proto(record.key).activity_id] = record
            self._activities_listed = True
            for record in timers:
                self._timers[timer_key_from_proto(record.key).timer_id] = record
            self._timers_listed = True
            for record in events:
                self._events[event_key_from_proto(record.key).event_id] = record
            self._events_listed = True

    def _in_scope(self, namespace: str, workflow_id: str, run_id: str) -> bool:
        return (
            namespace == self._scope.namespace
            and workflow_id == self._scope.workflow_id
            and run_id == self._scope.run_id
        )

    # WorkflowStore ---------------------------------------------------------

    async def get_workflow(self, key: WorkflowKey) -> temporaless_pb2.WorkflowRecord | None:
        if not self._in_scope(key.namespace, key.workflow_id, key.run_id):
            return await self._inner.get_workflow(key)
        async with self._lock:
            if self._workflow_known:
                return self._workflow
        record = await self._inner.get_workflow(key)
        async with self._lock:
            self._workflow_known = True
            self._workflow = record
        return record

    async def put_workflow(self, record: temporaless_pb2.WorkflowRecord) -> None:
        await self._inner.put_workflow(record)
        key = workflow_key_from_proto(record.key)
        if self._in_scope(key.namespace, key.workflow_id, key.run_id):
            async with self._lock:
                self._workflow_known = True
                self._workflow = record

    async def get_latest_workflow_run(
        self, namespace: str, workflow_id: str
    ) -> temporaless_pb2.LatestWorkflowRunPointer | None:
        return await self._inner.get_latest_workflow_run(namespace, workflow_id)

    async def delete_workflow(self, key: WorkflowKey) -> bool:
        deleted = await self._inner.delete_workflow(key)
        if self._in_scope(key.namespace, key.workflow_id, key.run_id):
            async with self._lock:
                self._workflow_known = True
                self._workflow = None
        return deleted

    async def delete_run(self, key: WorkflowKey) -> int:
        deleted = await self._inner.delete_run(key)
        if self._in_scope(key.namespace, key.workflow_id, key.run_id):
            async with self._lock:
                self._workflow_known = True
                self._workflow = None
                self._activities.clear()
                self._activities_listed = True
                self._timers.clear()
                self._timers_listed = True
                self._events.clear()
                self._events_listed = True
        return deleted

    # ActivityStore ---------------------------------------------------------

    async def get_activity(self, key: ActivityKey) -> temporaless_pb2.ActivityRecord | None:
        if not self._in_scope(key.namespace, key.workflow_id, key.run_id):
            return await self._inner.get_activity(key)
        async with self._lock:
            if key.activity_id in self._activities:
                return self._activities[key.activity_id]
            listed = self._activities_listed
        if listed:
            return None
        record = await self._inner.get_activity(key)
        async with self._lock:
            self._activities[key.activity_id] = record
        return record

    async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None:
        await self._inner.put_activity(record)
        key = activity_key_from_proto(record.key)
        if self._in_scope(key.namespace, key.workflow_id, key.run_id):
            async with self._lock:
                self._activities[key.activity_id] = record

    async def list_activities(self, key: WorkflowKey) -> list[temporaless_pb2.ActivityRecord]:
        if not self._in_scope(key.namespace, key.workflow_id, key.run_id):
            return await self._inner.list_activities(key)
        async with self._lock:
            if self._activities_listed:
                return [r for r in self._activities.values() if r is not None]
        records = await self._inner.list_activities(key)
        async with self._lock:
            for record in records:
                self._activities[activity_key_from_proto(record.key).activity_id] = record
            self._activities_listed = True
        return records

    async def delete_activity(self, key: ActivityKey) -> bool:
        deleted = await self._inner.delete_activity(key)
        if self._in_scope(key.namespace, key.workflow_id, key.run_id):
            async with self._lock:
                self._activities[key.activity_id] = None
        return deleted

    # TimerStore ------------------------------------------------------------

    async def get_timer(self, key: TimerKey) -> temporaless_pb2.TimerRecord | None:
        if not self._in_scope(key.namespace, key.workflow_id, key.run_id):
            return await self._inner.get_timer(key)
        async with self._lock:
            if key.timer_id in self._timers:
                return self._timers[key.timer_id]
            listed = self._timers_listed
        if listed:
            return None
        record = await self._inner.get_timer(key)
        async with self._lock:
            self._timers[key.timer_id] = record
        return record

    async def put_timer(self, record: temporaless_pb2.TimerRecord) -> None:
        await self._inner.put_timer(record)
        key = timer_key_from_proto(record.key)
        if self._in_scope(key.namespace, key.workflow_id, key.run_id):
            async with self._lock:
                self._timers[key.timer_id] = record

    async def list_timers(
        self,
        key: WorkflowKey,
        status: temporaless_pb2.TimerStatus,
    ) -> list[temporaless_pb2.TimerRecord]:
        if not self._in_scope(key.namespace, key.workflow_id, key.run_id):
            return await self._inner.list_timers(key, status)
        async with self._lock:
            if self._timers_listed:
                return [
                    r
                    for r in self._timers.values()
                    if r is not None
                    and (status == temporaless_pb2.TIMER_STATUS_UNSPECIFIED or r.status == status)
                ]
        # A status-filtered list would hide records the body might still look
        # up by id; only the unfiltered call populates the cache.
        if status != temporaless_pb2.TIMER_STATUS_UNSPECIFIED:
            return await self._inner.list_timers(key, status)
        records = await self._inner.list_timers(key, status)
        async with self._lock:
            for record in records:
                self._timers[timer_key_from_proto(record.key).timer_id] = record
            self._timers_listed = True
        return records

    async def delete_timer(self, key: TimerKey) -> bool:
        deleted = await self._inner.delete_timer(key)
        if self._in_scope(key.namespace, key.workflow_id, key.run_id):
            async with self._lock:
                self._timers[key.timer_id] = None
        return deleted

    # EventStore ------------------------------------------------------------

    async def get_event(self, key: EventKey) -> temporaless_pb2.EventRecord | None:
        if not self._in_scope(key.namespace, key.workflow_id, key.run_id):
            return await self._inner.get_event(key)
        async with self._lock:
            if key.event_id in self._events:
                return self._events[key.event_id]
            listed = self._events_listed
        if listed:
            return None
        record = await self._inner.get_event(key)
        async with self._lock:
            self._events[key.event_id] = record
        return record

    async def put_event(self, record: temporaless_pb2.EventRecord) -> None:
        await self._inner.put_event(record)
        key = event_key_from_proto(record.key)
        if self._in_scope(key.namespace, key.workflow_id, key.run_id):
            async with self._lock:
                self._events[key.event_id] = record

    async def list_events(self, key: WorkflowKey) -> list[temporaless_pb2.EventRecord]:
        if not self._in_scope(key.namespace, key.workflow_id, key.run_id):
            return await self._inner.list_events(key)
        async with self._lock:
            if self._events_listed:
                return [r for r in self._events.values() if r is not None]
        records = await self._inner.list_events(key)
        async with self._lock:
            for record in records:
                self._events[event_key_from_proto(record.key).event_id] = record
            self._events_listed = True
        return records

    async def delete_event(self, key: EventKey) -> bool:
        deleted = await self._inner.delete_event(key)
        if self._in_scope(key.namespace, key.workflow_id, key.run_id):
            async with self._lock:
                self._events[key.event_id] = None
        return deleted

    # ClaimStore (pass-through — claim contention has its own concurrency
    # story, no cache value). Only meaningful when the underlying store is
    # a ClaimStore; otherwise the methods raise the same TypeError a direct
    # caller would get from a non-claim store.
    # ---------------------------------------------------------------------

    async def claim_capability(self) -> ClaimCapability:
        inner = self._require_claim_store()
        return await inner.claim_capability()

    async def get_claim(self, key: ClaimKey) -> temporaless_pb2.ClaimRecord | None:
        inner = self._require_claim_store()
        return await inner.get_claim(key)

    async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
        inner = self._require_claim_store()
        return await inner.try_create_claim(record)

    async def delete_claim(self, key: ClaimKey) -> bool:
        inner = self._require_claim_store()
        return await inner.delete_claim(key)

    def _require_claim_store(self) -> ClaimStore:
        if not isinstance(self._inner, ClaimStore):
            raise TypeError(
                "underlying store does not support claims (does not implement ClaimStore)"
            )
        return self._inner

    # Runtime scanner method passes straight through -----------------------

    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        return await self._inner.due_timers(namespace, now)
