"""Storage hot-path benchmarks: round-trip put/get and list scan costs.

Mirrors ``core/go/storage/benchmark_test.go`` so cross-language costs are
directly comparable.
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from collections.abc import Awaitable, Callable

import opendal
from google.protobuf.any_pb2 import Any
from google.protobuf.wrappers_pb2 import StringValue

from benchmarks._harness import Bench, main
from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    WORKFLOW_RECORD_SCHEMA_VERSION,
    ActivityKey,
    OpenDALStore,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2


def _new_store() -> tuple[OpenDALStore, str]:
    tmp = tempfile.mkdtemp(prefix="temporaless-bench-")
    operator = opendal.AsyncOperator("fs", root=tmp)
    return OpenDALStore(operator), tmp


def _result_any(value: str) -> Any:
    payload = Any()
    payload.Pack(StringValue(value=value))
    return payload


async def bench_put_get_workflow(b: Bench) -> None:
    store, tmp = _new_store()
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
    store, tmp = _new_store()
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


async def _populated_workflows_store(count: int) -> tuple[OpenDALStore, str]:
    store, tmp = _new_store()
    for i in range(count):
        record = temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=WorkflowKey(workflow_id="bench:wf", run_id=f"run-{i:05d}").to_proto(),
            workflow_type="test:type",
            code_version="v1",
            status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        )
        await store.put_workflow(record)
    return store, tmp


def _list_workflows_scan(count: int) -> Callable[[Bench], Awaitable[None]]:
    async def fn(b: Bench) -> None:
        store, tmp = await _populated_workflows_store(count)
        try:
            b.reset_timer()
            for _ in range(b.n):
                records = await store.list_workflows(
                    "", "", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED
                )
                if len(records) != count:
                    raise AssertionError(f"got {len(records)} records, want {count}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    return fn


async def _populated_scoped_store(
    schedules: int, runs_per_schedule: int
) -> tuple[OpenDALStore, str]:
    store, tmp = _new_store()
    for s in range(schedules):
        for r in range(runs_per_schedule):
            record = temporaless_pb2.WorkflowRecord(
                schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
                key=WorkflowKey(workflow_id=f"schedule-{s:03d}", run_id=f"run-{r:05d}").to_proto(),
                workflow_type="test:type",
                code_version="v1",
                status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            )
            await store.put_workflow(record)
    return store, tmp


_TOTAL_SCHEDULES = 50
_RUNS_PER_SCHEDULE = 10


async def bench_list_workflows_unscoped(b: Bench) -> None:
    store, tmp = await _populated_scoped_store(_TOTAL_SCHEDULES, _RUNS_PER_SCHEDULE)
    try:
        want = _TOTAL_SCHEDULES * _RUNS_PER_SCHEDULE
        b.reset_timer()
        for _ in range(b.n):
            records = await store.list_workflows(
                "", "", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED
            )
            if len(records) != want:
                raise AssertionError(f"got {len(records)} records, want {want}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def bench_list_workflows_scoped_by_id(b: Bench) -> None:
    store, tmp = await _populated_scoped_store(_TOTAL_SCHEDULES, _RUNS_PER_SCHEDULE)
    try:
        b.reset_timer()
        for _ in range(b.n):
            records = await store.list_workflows(
                "", "schedule-025", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED
            )
            if len(records) != _RUNS_PER_SCHEDULE:
                raise AssertionError(f"got {len(records)} records, want {_RUNS_PER_SCHEDULE}")
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
    store, tmp = _new_store()
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
    store, tmp = _new_store()
    try:
        records = [_fanout_record(i) for i in range(_FANOUT)]
        b.reset_timer()
        for _ in range(b.n):
            await asyncio.gather(*(store.put_workflow(r) for r in records))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main(
        ("BenchmarkPutGetWorkflow", bench_put_get_workflow),
        ("BenchmarkPutGetActivity", bench_put_get_activity),
        ("BenchmarkListWorkflowsScan/workflows=10", _list_workflows_scan(10)),
        ("BenchmarkListWorkflowsScan/workflows=100", _list_workflows_scan(100)),
        ("BenchmarkListWorkflowsScan/workflows=500", _list_workflows_scan(500)),
        ("BenchmarkListWorkflowsScopedByID/unscoped", bench_list_workflows_unscoped),
        (
            "BenchmarkListWorkflowsScopedByID/scoped_by_workflow_id",
            bench_list_workflows_scoped_by_id,
        ),
        ("BenchmarkPutWorkflowSerial50", bench_put_workflow_serial_50),
        ("BenchmarkPutWorkflowParallel50", bench_put_workflow_parallel_50),
    )
