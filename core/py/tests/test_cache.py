"""Tests for the run-scoped replay cache.

Mirrors core/go/workflow/cache_test.go. The cache wraps a Store and serves
get-by-key reads from memory after a bulk prefetch. On replay, a workflow
with N activities issues 1 list_activities call instead of N get_activity
round-trips.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import Int32Value

from temporaless._cache import RunScopedCache
from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    ActivityKey,
    OpenDALStore,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ActivityOptions,
    Options,
    run,
)


class CountingStore:
    """Wraps a Store and tallies each method call. Used by cache tests to
    assert that replay paths issue list_* calls once and serve subsequent
    get_* from cache."""

    def __init__(self, inner):
        self._inner = inner
        self.get_activity_calls = 0
        self.list_activity_calls = 0
        self.put_activity_calls = 0
        self.get_workflow_calls = 0
        self.put_workflow_calls = 0
        self.list_workflow_calls = 0
        self.get_timer_calls = 0
        self.list_timer_calls = 0
        self.put_timer_calls = 0
        self.get_event_calls = 0
        self.list_event_calls = 0
        self.put_event_calls = 0

    async def get_activity(self, key):
        self.get_activity_calls += 1
        return await self._inner.get_activity(key)

    async def put_activity(self, record):
        self.put_activity_calls += 1
        return await self._inner.put_activity(record)

    async def list_activities(self, key):
        self.list_activity_calls += 1
        return await self._inner.list_activities(key)

    async def delete_activity(self, key):
        return await self._inner.delete_activity(key)

    async def get_workflow(self, key):
        self.get_workflow_calls += 1
        return await self._inner.get_workflow(key)

    async def put_workflow(self, record):
        self.put_workflow_calls += 1
        return await self._inner.put_workflow(record)

    async def list_workflows(self, namespace, workflow_id, status):
        self.list_workflow_calls += 1
        return await self._inner.list_workflows(namespace, workflow_id, status)

    async def delete_workflow(self, key):
        return await self._inner.delete_workflow(key)

    async def get_timer(self, key):
        self.get_timer_calls += 1
        return await self._inner.get_timer(key)

    async def put_timer(self, record):
        self.put_timer_calls += 1
        return await self._inner.put_timer(record)

    async def list_timers(self, key, status):
        self.list_timer_calls += 1
        return await self._inner.list_timers(key, status)

    async def delete_timer(self, key):
        return await self._inner.delete_timer(key)

    async def get_event(self, key):
        self.get_event_calls += 1
        return await self._inner.get_event(key)

    async def put_event(self, record):
        self.put_event_calls += 1
        return await self._inner.put_event(record)

    async def list_events(self, key):
        self.list_event_calls += 1
        return await self._inner.list_events(key)

    async def delete_event(self, key):
        return await self._inner.delete_event(key)

    async def sweep(self, namespace, now, max_age):
        return await self._inner.sweep(namespace, now, max_age)

    async def due_timers(self, namespace, now):
        return await self._inner.due_timers(namespace, now)


@pytest.fixture
def inner_store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


@pytest.fixture
def counter(inner_store):
    return CountingStore(inner_store)


async def _run_fanout(store, run_id: str, n: int, executions_counter: list[int]) -> Int32Value:
    """Drive a workflow with N activities. Activity i takes Int32Value(i),
    returns Int32Value(i * 2). The workflow body sums the results."""

    async def workflow_body(workflow, request: Int32Value) -> Int32Value:
        total = 0
        for i in range(request.value):
            result = await workflow.execute_activity(
                ActivityOptions(activity_id=f"act:{i}"),
                Int32Value(value=i),
                Int32Value,
                _double,
            )
            total += result.value

        async def _identity_input(_: Int32Value) -> None:  # type: ignore[reportUnusedFunction]
            return None

        del _identity_input  # quiet unused
        return Int32Value(value=total)

    async def _double(req: Int32Value) -> Int32Value:
        executions_counter[0] += 1
        return Int32Value(value=req.value * 2)

    return await run(
        store,
        Options(workflow_id="fanout", run_id=run_id, code_version="test"),
        Int32Value(value=n),
        Int32Value,
        workflow_body,
    )


async def test_replay_uses_cached_activities(inner_store, counter):
    """Core promise: on replay, the runtime issues a single list_activities
    call up front and serves every get_activity from cache. Without the
    cache, replay would issue N get_activity round-trips."""
    activity_count = 8
    executions = [0]

    # First run: fresh, all activities execute.
    await _run_fanout(counter, "run-1", activity_count, executions)
    assert executions[0] == activity_count

    # Reset counters before the replay.
    counter.get_activity_calls = 0
    counter.list_activity_calls = 0
    counter.list_timer_calls = 0
    counter.list_event_calls = 0
    executions[0] = 0

    # Force replay by flipping COMPLETED -> IN_PROGRESS.
    key = WorkflowKey(workflow_id="fanout", run_id="run-1")
    completed = await inner_store.get_workflow(key)
    assert completed is not None
    completed.status = temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    completed.ClearField("completed_at")
    completed.ClearField("result")
    await inner_store.put_workflow(completed)

    await _run_fanout(counter, "run-1", activity_count, executions)
    assert executions[0] == 0, "replay should not re-execute activities"
    # Big assertion: replay must NOT issue N get_activity round-trips.
    assert counter.get_activity_calls == 0, (
        f"replay get_activity calls = {counter.get_activity_calls}, want 0"
    )
    assert counter.list_activity_calls == 1
    assert counter.list_timer_calls == 1
    assert counter.list_event_calls == 1


async def test_fresh_run_skips_prefetch(counter):
    """A fresh run (no prior records) doesn't issue an unnecessary
    list_activities/list_timers/list_events trio."""
    executions = [0]
    await _run_fanout(counter, "fresh-run", 3, executions)
    assert counter.list_activity_calls == 0
    assert counter.list_timer_calls == 0
    assert counter.list_event_calls == 0


async def test_write_through_and_readback(inner_store, counter):
    """After put_activity via the cache, get_activity for the same key
    returns the just-written record without hitting the inner store."""
    scope = WorkflowKey(workflow_id="w", run_id="r")
    cache = RunScopedCache(counter, scope)

    record = temporaless_pb2.ActivityRecord(
        schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
        key=ActivityKey(
            workflow_id=scope.workflow_id,
            run_id=scope.run_id,
            activity_id="a",
        ).to_proto(),
        activity_type="activity:google.protobuf.StringValue->google.protobuf.StringValue",
        code_version="test",
        input_digest="deadbeef",
        status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
    )
    record.created_at.GetCurrentTime()
    record.completed_at.GetCurrentTime()

    await cache.put_activity(record)
    got = await cache.get_activity(
        ActivityKey(workflow_id=scope.workflow_id, run_id=scope.run_id, activity_id="a")
    )
    assert got is not None
    assert got.input_digest == "deadbeef"
    assert counter.get_activity_calls == 0, "cached read should not hit inner store"
    assert counter.put_activity_calls == 1

    # Inner is authoritative — a separate read sees the same record.
    inner_got = await inner_store.get_activity(
        ActivityKey(workflow_id=scope.workflow_id, run_id=scope.run_id, activity_id="a")
    )
    assert inner_got is not None
    assert inner_got.input_digest == "deadbeef"


async def test_out_of_scope_read_passes_through(inner_store, counter):
    """Cross-pipeline reads (different workflow_id) must hit the underlying
    store — the cache should not interfere with dependencies.wait_for_workflow."""
    scope = WorkflowKey(workflow_id="active-workflow", run_id="r")
    cache = RunScopedCache(counter, scope)
    await cache.prefetch()

    other_key = WorkflowKey(workflow_id="other-workflow", run_id="r")
    other_record = temporaless_pb2.WorkflowRecord(
        schema_version=temporaless_pb2.RECORD_SCHEMA_VERSION_WORKFLOW,
        key=other_key.to_proto(),
        workflow_type="workflow:google.protobuf.Int32Value->google.protobuf.Int32Value",
        code_version="test",
        input_digest="abc",
        status=temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
    )
    other_record.created_at.GetCurrentTime()
    await inner_store.put_workflow(other_record)
    counter.get_workflow_calls = 0

    got = await cache.get_workflow(other_key)
    assert got is not None
    assert got.workflow_type == other_record.workflow_type
    assert counter.get_workflow_calls == 1, "cross-pipeline read should pass through"


async def test_negative_cache_after_prefetch(inner_store, counter):
    """After prefetch, a get for a non-existent activity_id returns None
    without hitting the inner store — prefetch already proved nothing was
    there."""
    scope = WorkflowKey(workflow_id="w", run_id="r")
    cache = RunScopedCache(counter, scope)
    await cache.prefetch()
    counter.get_activity_calls = 0

    got = await cache.get_activity(
        ActivityKey(
            workflow_id=scope.workflow_id,
            run_id=scope.run_id,
            activity_id="never-existed",
        )
    )
    assert got is None
    assert counter.get_activity_calls == 0


async def test_delete_invalidates_cache(inner_store, counter):
    """delete_activity via the cache clears the cached entry."""
    scope = WorkflowKey(workflow_id="w", run_id="r")
    cache = RunScopedCache(counter, scope)

    record = temporaless_pb2.ActivityRecord(
        schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
        key=ActivityKey(
            workflow_id=scope.workflow_id, run_id=scope.run_id, activity_id="a"
        ).to_proto(),
        activity_type="activity:google.protobuf.Int32Value->google.protobuf.Int32Value",
        code_version="test",
        input_digest="abc",
        status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
    )
    record.created_at.GetCurrentTime()
    record.completed_at.GetCurrentTime()
    await cache.put_activity(record)
    assert await cache.delete_activity(
        ActivityKey(workflow_id=scope.workflow_id, run_id=scope.run_id, activity_id="a")
    )
    got = await cache.get_activity(
        ActivityKey(workflow_id=scope.workflow_id, run_id=scope.run_id, activity_id="a")
    )
    assert got is None, "delete should have cleared the cache"


async def test_semantic_equivalence_against_raw_store(inner_store):
    """Replay produces the same result via the cache as a fresh execution
    would. Guards against the cache drifting from the underlying-store
    contract."""
    executions = [0]
    result = await _run_fanout(inner_store, "eq-1", 4, executions)
    assert result.value == 0 + 2 + 4 + 6
    assert executions[0] == 4

    key = WorkflowKey(workflow_id="fanout", run_id="eq-1")
    completed = await inner_store.get_workflow(key)
    assert completed is not None
    completed.status = temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    completed.ClearField("completed_at")
    completed.ClearField("result")
    await inner_store.put_workflow(completed)

    executions[0] = 0
    result = await _run_fanout(inner_store, "eq-1", 4, executions)
    assert result.value == 0 + 2 + 4 + 6
    assert executions[0] == 0


async def test_cache_concurrent_get(inner_store, counter):
    """Mutex protects concurrent reads from the workflow body — important
    because asyncio.gather callers may issue many parallel get calls."""
    import asyncio

    scope = WorkflowKey(workflow_id="w", run_id="r")
    for i in range(50):
        record = temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=ActivityKey(
                workflow_id=scope.workflow_id,
                run_id=scope.run_id,
                activity_id=f"act:{i}",
            ).to_proto(),
            activity_type="activity:google.protobuf.Int32Value->google.protobuf.Int32Value",
            code_version="test",
            input_digest="d",
            status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
        record.created_at.GetCurrentTime()
        record.completed_at.GetCurrentTime()
        await inner_store.put_activity(record)

    cache = RunScopedCache(counter, scope)
    await cache.prefetch()
    counter.get_activity_calls = 0

    async def fetch(i):
        return await cache.get_activity(
            ActivityKey(
                workflow_id=scope.workflow_id,
                run_id=scope.run_id,
                activity_id=f"act:{i}",
            )
        )

    results = await asyncio.gather(*(fetch(i) for i in range(50)))
    assert all(r is not None for r in results)
    assert counter.get_activity_calls == 0


# Silence "unused import" for Timestamp / datetime / timedelta — they may be
# imported by code-paths future-extending this test.
_ = (Timestamp, datetime, timedelta, UTC)
