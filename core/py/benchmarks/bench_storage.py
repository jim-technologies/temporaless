"""Storage hot-path benchmarks: point ops, run-scoped lists, and query indexes.

Mirrors ``core/go/storage/benchmark_test.go`` so cross-language costs are
directly comparable.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from collections.abc import AsyncIterable
from datetime import UTC, datetime, timedelta

import opendal
from google.protobuf.any_pb2 import Any
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue
from temporaless_indexstore import IndexedStore

from benchmarks._harness import Bench, main
from temporaless.cronscheduler import last_fire_from_runs
from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    WORKFLOW_RECORD_SCHEMA_VERSION,
    ActivityKey,
    OpenDALStore,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2


def _new_store() -> tuple[OpenDALStore, opendal.AsyncOperator, str]:
    tmp = tempfile.mkdtemp(prefix="temporaless-bench-")
    operator = opendal.AsyncOperator("fs", root=tmp)
    return OpenDALStore(operator), operator, tmp


def _new_indexed_store() -> tuple[IndexedStore, opendal.AsyncOperator, str]:
    tmp = tempfile.mkdtemp(prefix="temporaless-bench-")
    operator = opendal.AsyncOperator("fs", root=tmp)
    return IndexedStore.from_opendal(operator, f"{tmp}/index.sqlite"), operator, tmp


def _result_any(value: str) -> Any:
    payload = Any()
    payload.Pack(StringValue(value=value))
    return payload


async def bench_put_get_workflow(b: Bench) -> None:
    store, _operator, tmp = _new_store()
    try:
        record = temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=WorkflowKey(workflow_id="bench:wf", run_id="run").to_proto(),
            workflow_type="test:type",
            code_version="v1",
            status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            result=_result_any("benchmark-result"),
        )
        key = WorkflowKey(workflow_id="bench:wf", run_id="run")
        b.reset_timer()
        for _ in range(b.n):
            await store.put_workflow(record)
            await store.get_workflow(key)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def bench_put_get_activity(b: Bench) -> None:
    store, _operator, tmp = _new_store()
    try:
        record = temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=ActivityKey(workflow_id="bench:wf", run_id="run", activity_id="fetch").to_proto(),
            activity_type="test:activity",
            code_version="v1",
            status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
            result=_result_any("benchmark-result"),
        )
        key = ActivityKey(workflow_id="bench:wf", run_id="run", activity_id="fetch")
        b.reset_timer()
        for _ in range(b.n):
            await store.put_activity(record)
            await store.get_activity(key)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def _populated_scoped_store(
    schedules: int, runs_per_schedule: int
) -> tuple[IndexedStore, opendal.AsyncOperator, str]:
    store, operator, tmp = _new_indexed_store()
    for s in range(schedules):
        for r in range(runs_per_schedule):
            ts = _timestamp(r)
            record = temporaless_pb2.WorkflowRecord(
                schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
                key=WorkflowKey(workflow_id=f"schedule-{s:03d}", run_id=f"run-{r:05d}").to_proto(),
                workflow_type="test:type",
                code_version="v1",
                status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
                created_at=ts,
                completed_at=ts,
            )
            await store.put_workflow(record)
    return store, operator, tmp


_TOTAL_SCHEDULES = 10
_RUNS_PER_SCHEDULE = 5
_LAST_FIRE_RUNS = int(os.environ.get("TEMPORALESS_BENCH_LAST_FIRE_RUNS", "10000"))


async def bench_index_list_workflows_filtered(b: Bench) -> None:
    store, _operator, tmp = await _populated_scoped_store(_TOTAL_SCHEDULES, _RUNS_PER_SCHEDULE)
    try:
        b.reset_timer()
        for _ in range(b.n):
            records, token = await store.list_workflows(
                "",
                "schedule-005",
                temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
                order_by="created_at desc",
                page_size=_RUNS_PER_SCHEDULE,
            )
            if len(records) != _RUNS_PER_SCHEDULE or token:
                raise AssertionError("indexed list returned an unexpected page")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def bench_legacy_bucket_walk_list_workflows_filtered(b: Bench) -> None:
    _store, operator, tmp = await _populated_scoped_store(_TOTAL_SCHEDULES, _RUNS_PER_SCHEDULE)
    try:
        b.reset_timer()
        for _ in range(b.n):
            records = await _legacy_workflow_scan(
                operator,
                workflow_id="schedule-005",
                status=temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
            )
            if len(records) != _RUNS_PER_SCHEDULE:
                raise AssertionError("legacy scan returned an unexpected count")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def bench_last_fire_pointer_get(b: Bench) -> None:
    store, _operator, tmp = _new_store()
    try:
        await _seed_schedule_runs(store, "bench:schedule", _LAST_FIRE_RUNS)
        b.reset_timer()
        for _ in range(b.n):
            last = await last_fire_from_runs(store, "", "bench:schedule")
            if last != _fire_time(_LAST_FIRE_RUNS - 1):
                raise AssertionError(f"got {last!r}, want {_fire_time(_LAST_FIRE_RUNS - 1)!r}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def bench_legacy_bucket_walk_last_fire(b: Bench) -> None:
    store, operator, tmp = _new_store()
    try:
        await _seed_schedule_runs(store, "bench:schedule", _LAST_FIRE_RUNS)
        b.reset_timer()
        for _ in range(b.n):
            last = await _legacy_last_fire_scan(operator, "bench:schedule", "%Y-%m-%dT%H:%M:%SZ")
            if last != _fire_time(_LAST_FIRE_RUNS - 1):
                raise AssertionError(f"got {last!r}, want {_fire_time(_LAST_FIRE_RUNS - 1)!r}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def bench_run_scoped_prefetch_activities(b: Bench) -> None:
    store, _operator, tmp = _new_store()
    try:
        scope = WorkflowKey(workflow_id="bench:fanout", run_id="run")
        for i in range(50):
            record = temporaless_pb2.ActivityRecord(
                schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
                key=ActivityKey(
                    workflow_id=scope.workflow_id,
                    run_id=scope.run_id,
                    activity_id=f"act:{i:03d}",
                ).to_proto(),
                activity_type="test:activity",
                code_version="v1",
                status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
                result=_result_any("benchmark-result"),
            )
            await store.put_activity(record)
        b.reset_timer()
        for _ in range(b.n):
            records = await store.list_activities(scope)
            if len(records) != 50:
                raise AssertionError(f"got {len(records)} records, want 50")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


_FANOUT = 50


def _fanout_record(idx: int) -> temporaless_pb2.WorkflowRecord:
    return temporaless_pb2.WorkflowRecord(
        schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
        key=WorkflowKey(workflow_id="bench:fanout", run_id=f"run-{idx:05d}").to_proto(),
        workflow_type="test:type",
        code_version="v1",
        status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        result=_result_any("benchmark-result"),
    )


async def bench_put_workflow_serial_50(b: Bench) -> None:
    """Baseline: 50 puts awaited one at a time inside a single iteration —
    mirrors how the framework writes records during sequential workflow steps.
    """
    store, _operator, tmp = _new_store()
    try:
        records = [_fanout_record(i) for i in range(_FANOUT)]
        b.reset_timer()
        for _ in range(b.n):
            for record in records:
                await store.put_workflow(record)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def bench_put_workflow_parallel_50(b: Bench) -> None:
    """Async win: same 50 puts via ``asyncio.gather``. The Tokio thread pool
    overlaps the syscalls; on a network backend (S3/GCS) this is the difference
    between 50× sequential round-trips and one round-trip's worth of wall time.
    """
    store, _operator, tmp = _new_store()
    try:
        records = [_fanout_record(i) for i in range(_FANOUT)]
        b.reset_timer()
        for _ in range(b.n):
            await asyncio.gather(*(store.put_workflow(r) for r in records))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _timestamp(idx: int) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(_fire_time(idx))
    return ts


def _fire_time(idx: int) -> datetime:
    return datetime(2026, 5, 4, 9, 0, tzinfo=UTC) + timedelta(minutes=idx)


async def _seed_schedule_runs(store: OpenDALStore, workflow_id: str, count: int) -> None:
    for idx in range(count):
        fire_time = _fire_time(idx)
        ts = Timestamp()
        ts.FromDatetime(fire_time)
        record = temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=WorkflowKey(
                workflow_id=workflow_id,
                run_id=fire_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ).to_proto(),
            workflow_type="test:type",
            code_version="v1",
            status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            created_at=ts,
            completed_at=ts,
            run_order_time=ts,
        )
        await store.put_workflow(record)


async def _legacy_workflow_scan(
    operator: opendal.AsyncOperator,
    *,
    workflow_id: str,
    status: temporaless_pb2.WorkflowStatus,
) -> list[temporaless_pb2.WorkflowRecord]:
    records: list[temporaless_pb2.WorkflowRecord] = []
    async for path in _walk_binpb(operator, "temporaless/v2/"):
        if not path.endswith("/workflow.binpb"):
            continue
        record = await _read_workflow(operator, path)
        if record.key.workflow_id != workflow_id:
            continue
        if status != temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED and record.status != status:
            continue
        records.append(record)
    return records


async def _legacy_last_fire_scan(
    operator: opendal.AsyncOperator, workflow_id: str, run_id_layout: str
) -> datetime | None:
    last: datetime | None = None
    for record in await _legacy_workflow_scan(
        operator,
        workflow_id=workflow_id,
        status=temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
    ):
        parsed = datetime.strptime(record.key.run_id, run_id_layout).replace(tzinfo=UTC)
        if last is None or parsed > last:
            last = parsed
    return last


async def _read_workflow(
    operator: opendal.AsyncOperator, path: str
) -> temporaless_pb2.WorkflowRecord:
    data = bytes(await operator.read(path))
    record = temporaless_pb2.WorkflowRecord()
    record.ParseFromString(data)
    return record


async def _walk_binpb(operator: opendal.AsyncOperator, root: str) -> AsyncIterable[str]:
    queue = [root]
    while queue:
        current = queue.pop(0)
        try:
            entries = sorted(
                [entry async for entry in await operator.list(current)], key=lambda e: e.path
            )
        except opendal.exceptions.NotFound:
            continue
        for entry in entries:
            path = entry.path
            if path == current:
                continue
            if path.endswith("/"):
                queue.append(path)
            elif path.endswith(".binpb"):
                yield path


if __name__ == "__main__":
    main(
        ("BenchmarkPutGetWorkflow", bench_put_get_workflow),
        ("BenchmarkPutGetActivity", bench_put_get_activity),
        (
            "BenchmarkLastFireSeeding/pointer_get",
            bench_last_fire_pointer_get,
        ),
        (
            "BenchmarkLastFireSeeding/legacy_full_bucket_walk",
            bench_legacy_bucket_walk_last_fire,
        ),
        ("BenchmarkRunScopedPrefetchActivities50", bench_run_scoped_prefetch_activities),
        (
            "BenchmarkListWorkflowsFiltered/index",
            bench_index_list_workflows_filtered,
        ),
        (
            "BenchmarkListWorkflowsFiltered/legacy_full_bucket_walk",
            bench_legacy_bucket_walk_list_workflows_filtered,
        ),
        ("BenchmarkPutWorkflowSerial50", bench_put_workflow_serial_50),
        ("BenchmarkPutWorkflowParallel50", bench_put_workflow_parallel_50),
    )
