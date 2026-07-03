"""Run every benchmark suite in one shot.

uv run --project core/py python -m benchmarks
"""

from __future__ import annotations

import asyncio

from benchmarks._harness import run_benchmark
from benchmarks.bench_storage import (
    bench_index_list_workflows_filtered,
    bench_last_fire_pointer_get,
    bench_legacy_bucket_walk_last_fire,
    bench_legacy_bucket_walk_list_workflows_filtered,
    bench_put_get_activity,
    bench_put_get_workflow,
    bench_put_workflow_parallel_50,
    bench_put_workflow_serial_50,
    bench_run_scoped_prefetch_activities,
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
