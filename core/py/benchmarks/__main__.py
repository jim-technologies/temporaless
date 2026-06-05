"""Run every benchmark suite in one shot.

uv run --project core/py python -m benchmarks
"""

from __future__ import annotations

import asyncio

from benchmarks._harness import run_benchmark
from benchmarks.bench_storage import (
    _list_workflows_scan,
    bench_list_workflows_scoped_by_id,
    bench_list_workflows_unscoped,
    bench_put_get_activity,
    bench_put_get_workflow,
    bench_put_workflow_parallel_50,
    bench_put_workflow_serial_50,
)
from benchmarks.bench_workflow import (
    bench_retry_loop_in_process,
    bench_workflow_run_fresh_execution,
    bench_workflow_run_replay,
)


async def _run_all() -> None:
    suite = [
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
        ("BenchmarkWorkflowRunFreshExecution", bench_workflow_run_fresh_execution),
        ("BenchmarkWorkflowRunReplay", bench_workflow_run_replay),
        ("BenchmarkRetryLoopInProcess", bench_retry_loop_in_process),
        ("BenchmarkPutWorkflowSerial50", bench_put_workflow_serial_50),
        ("BenchmarkPutWorkflowParallel50", bench_put_workflow_parallel_50),
    ]
    for name, fn in suite:
        await run_benchmark(name, fn)


if __name__ == "__main__":
    asyncio.run(_run_all())
