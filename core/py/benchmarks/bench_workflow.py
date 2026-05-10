"""Workflow runtime hot-path benchmarks: fresh execution, replay, retry loop.

Mirrors ``core/go/workflow/benchmark_test.go`` so cross-language costs are
directly comparable.
"""

from __future__ import annotations

import shutil
import tempfile
from datetime import timedelta

import opendal
from google.protobuf.duration_pb2 import Duration
from google.protobuf.wrappers_pb2 import StringValue

from benchmarks._harness import Bench, main
from temporaless.storage import OpenDALStore
from temporaless.workflow import (
    ActivityError,
    Options,
    RetryPolicy,
    Workflow,
    run,
)


def _new_store() -> tuple[OpenDALStore, str]:
    tmp = tempfile.mkdtemp(prefix="temporaless-bench-")
    operator = opendal.AsyncOperator("fs", root=tmp)
    return OpenDALStore(operator), tmp


async def _fetch_activity(workflow: Workflow, request: StringValue) -> StringValue:
    async def execute() -> StringValue:
        return StringValue(value=f"ok:{request.value}")

    return await workflow.run_activity(
        "fetch",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        request,
        StringValue,
        execute,
    )


async def bench_workflow_run_fresh_execution(b: Bench) -> None:
    store, tmp = _new_store()
    try:
        b.reset_timer()
        for i in range(b.n):
            await run(
                store,
                Options(
                    workflow_id="bench:fresh",
                    run_id=f"run-{i:05d}",
                    code_version="v1",
                ),
                StringValue(value="input"),
                StringValue,
                _fetch_activity,
            )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def bench_workflow_run_replay(b: Bench) -> None:
    store, tmp = _new_store()
    try:
        options = Options(workflow_id="bench:replay", run_id="shared", code_version="v1")
        await run(store, options, StringValue(value="input"), StringValue, _fetch_activity)
        b.reset_timer()
        for _ in range(b.n):
            await run(store, options, StringValue(value="input"), StringValue, _fetch_activity)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def bench_retry_loop_in_process(b: Bench) -> None:
    store, tmp = _new_store()
    try:
        duration = Duration()
        duration.FromTimedelta(timedelta(milliseconds=1))
        policy = RetryPolicy(maximum_attempts=3, initial_interval=duration)

        async def workflow_body(workflow: Workflow, request: StringValue) -> StringValue:
            calls = 0

            async def execute() -> StringValue:
                nonlocal calls
                calls += 1
                if calls < 3:
                    raise ActivityError("rate_limited", "transient")
                return StringValue(value=f"ok:{request.value}")

            return await workflow.run_activity(
                "fetch",
                "activity:google.protobuf.StringValue->google.protobuf.StringValue",
                request,
                StringValue,
                execute,
                policy,
            )

        b.reset_timer()
        for i in range(b.n):
            await run(
                store,
                Options(
                    workflow_id="bench:retry",
                    run_id=f"run-{i:05d}",
                    code_version="v1",
                ),
                StringValue(value="input"),
                StringValue,
                workflow_body,
            )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main(
        ("BenchmarkWorkflowRunFreshExecution", bench_workflow_run_fresh_execution),
        ("BenchmarkWorkflowRunReplay", bench_workflow_run_replay),
        ("BenchmarkRetryLoopInProcess", bench_retry_loop_in_process),
    )
