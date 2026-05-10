"""Airflow / Dagster / Prefect-style data pipeline as a Temporaless workflow.

The classic ETL DAG pattern:

    extract  ──▶  transform  ──▶  validate  ──▶  load  ──▶  notify
                                       │
                                       └──(if too few rows)──▶  alert + halt

Translated to Temporaless: the whole DAG is **one workflow body** composed
with normal Python `await` and `if`/`else`. Each box is an activity. There
is no DAG declaration language — Python control flow IS the DAG. Replay is
fingerprint-based: re-running the same `(workflow_id, run_id)` skips every
step that already completed and resumes where it left off, which is what
Airflow's "clear-and-rerun" semantics feel like.

What this example demonstrates that maps to data-pipelining frameworks:

- **Sequential dependencies** (extract → transform → load): plain `await`.
- **Fan-out / fan-in** (parallel partition processing): `asyncio.gather`
  over per-partition activities.
- **Conditional branching**: regular Python `if` around an activity call.
- **Idempotent re-runs / backfill**: replay short-circuits via stored
  records. Run for a different ``run_id`` to backfill a different date.
- **Sensor / wait-for-condition**: `workflow.sleep` for time-based waits;
  `workflow.wait_event` for external triggers (file-arrived signals,
  upstream pipeline completion notifications).
- **Resource-bounded concurrency** (e.g. "only 5 LLM calls at once"): use
  ``asyncio.Semaphore`` inline — that's standard Python, not a framework
  concept.

What this example does NOT try to provide (use a different tool):

- Asset / lineage graph (Dagster's identity).
- Visual DAG editor (Airflow / n8n).
- Pool-based resource managers (Airflow Pools).

Run:

    uv run --project core/py python examples/py/data_pipeline.py
"""

from __future__ import annotations

import asyncio
import random
import tempfile
from datetime import UTC, datetime

import opendal
from google.protobuf.wrappers_pb2 import Int64Value, StringValue

from temporaless import (
    ActivityOptions,
    OpenDALStore,
    Options,
    Workflow,
    annotate,
    run,
)

# ---- activities ------------------------------------------------------------


async def _extract(date: StringValue) -> StringValue:
    """Extract: pull a partition list for the date. Returns comma-separated IDs."""
    annotate("step", "extract")
    annotate("date", date.value)
    # In production: vendor SDK call, S3 list, BigQuery query, etc.
    partitions = [f"p{i:02d}" for i in range(8)]
    return StringValue(value=",".join(partitions))


async def _transform_partition(partition: StringValue) -> Int64Value:
    """Per-partition transform: returns row count after cleaning."""
    annotate("step", "transform")
    annotate("partition", partition.value)
    await asyncio.sleep(random.uniform(0.02, 0.08))  # simulate work
    return Int64Value(value=random.randint(50, 500))


async def _validate(total_rows: Int64Value) -> StringValue:
    """Quality gate: enough rows? Returns 'ok' or 'too_few'."""
    annotate("step", "validate")
    annotate("rows", str(total_rows.value))
    if total_rows.value < 200:
        return StringValue(value="too_few")
    return StringValue(value="ok")


async def _load(total_rows: Int64Value) -> StringValue:
    """Load: write the day's output to the sink (DB / Parquet / etc.)."""
    annotate("step", "load")
    annotate("rows", str(total_rows.value))
    return StringValue(value=f"loaded:{total_rows.value}")


async def _notify(message: StringValue) -> StringValue:
    annotate("step", "notify")
    return StringValue(value=f"notified:{message.value}")


async def _alert(message: StringValue) -> StringValue:
    annotate("step", "alert")
    return StringValue(value=f"alerted:{message.value}")


# ---- the DAG, expressed as a workflow body ---------------------------------


async def daily_pipeline(workflow: Workflow, date: StringValue) -> StringValue:
    """One workflow run per (pipeline_id, date). Backfill = call run() with a
    different date. Re-run after a transient failure = call run() again with
    the same date — completed steps short-circuit from storage.
    """
    annotate("pipeline", "stocks_daily")

    # Stage 1: extract (single activity)
    partitions_csv = await workflow.execute_activity(
        ActivityOptions(activity_id="extract"), date, StringValue, _extract
    )
    partitions = partitions_csv.value.split(",")

    # Stage 2: parallel transform (fan-out / fan-in via asyncio.gather)
    async def transform_one(partition: str) -> int:
        result = await workflow.execute_activity(
            ActivityOptions(activity_id=f"transform:{partition}"),
            StringValue(value=partition),
            Int64Value,
            _transform_partition,
        )
        return result.value

    # Bound parallelism: at most 4 concurrent partition transforms. Standard
    # Python — no framework primitive, just a semaphore.
    semaphore = asyncio.Semaphore(4)

    async def transform_with_limit(partition: str) -> int:
        async with semaphore:
            return await transform_one(partition)

    counts = await asyncio.gather(*(transform_with_limit(p) for p in partitions))
    total_rows = Int64Value(value=sum(counts))

    # Stage 3: validation gate (regular Python if/else, branches are activities)
    verdict = await workflow.execute_activity(
        ActivityOptions(activity_id="validate"),
        total_rows,
        StringValue,
        _validate,
    )

    if verdict.value == "too_few":
        # Halt the DAG: alert and return without loading.
        return await workflow.execute_activity(
            ActivityOptions(activity_id="alert"),
            StringValue(value=f"row count {total_rows.value} below threshold"),
            StringValue,
            _alert,
        )

    # Stage 4: load (sequential after validate)
    loaded = await workflow.execute_activity(
        ActivityOptions(activity_id="load"), total_rows, StringValue, _load
    )

    # Stage 5: notify (sequential after load)
    return await workflow.execute_activity(
        ActivityOptions(activity_id="notify"), loaded, StringValue, _notify
    )


# ---- driver ----------------------------------------------------------------


async def main() -> None:
    operator = opendal.AsyncOperator(
        "fs", root=tempfile.mkdtemp(prefix="temporaless-pipeline-")
    )
    store = OpenDALStore(operator)

    print("=== first run (full DAG executes) ===")
    started = datetime.now(UTC)
    result = await run(
        store,
        Options(
            workflow_id="pipeline:stocks_daily",
            run_id="2026-05-04",
            code_version="example",
        ),
        StringValue(value="2026-05-04"),
        StringValue,
        daily_pipeline,
    )
    print(f"  result: {result.value!r}")
    print(f"  wall time: {(datetime.now(UTC) - started).total_seconds() * 1000:.1f}ms")

    print("\n=== re-run (every step replays from storage; no work done) ===")
    started = datetime.now(UTC)
    result = await run(
        store,
        Options(
            workflow_id="pipeline:stocks_daily",
            run_id="2026-05-04",
            code_version="example",
        ),
        StringValue(value="2026-05-04"),
        StringValue,
        daily_pipeline,
    )
    print(f"  result: {result.value!r}")
    print(f"  wall time: {(datetime.now(UTC) - started).total_seconds() * 1000:.1f}ms")

    print("\n=== backfill: run for a different date ===")
    backfill_result = await run(
        store,
        Options(
            workflow_id="pipeline:stocks_daily",
            run_id="2026-05-03",
            code_version="example",
        ),
        StringValue(value="2026-05-03"),
        StringValue,
        daily_pipeline,
    )
    print(f"  result: {backfill_result.value!r}")


if __name__ == "__main__":
    asyncio.run(main())
