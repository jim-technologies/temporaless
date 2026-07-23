"""Airflow / Dagster / Prefect-style data pipeline as a Temporaless workflow.

The classic ETL DAG pattern:

    extract  ──▶  transform  ──▶  validate  ──▶  load  ──▶  notify
                                       │
                                       └──(if too few rows)──▶  alert + halt

Translated to Temporaless: the whole DAG is **one workflow body** composed
with normal Python `await` and `if`/`else`. Each box is an activity. There
is no DAG declaration language — Python control flow IS the DAG. Replay is
id-based: re-running the same `(workflow_id, run_id)` skips every step
that already completed (stored records keyed by activity_id) and resumes
where it left off, which is what Airflow's "clear-and-rerun" semantics
feel like.

What this example demonstrates that maps to data-pipelining frameworks:

- **Sequential dependencies** (extract → transform → load): plain `await`.
- **Fan-out / fan-in** (parallel partition processing):
  ``gather_activities`` over per-partition activities.
- **Conditional branching**: regular Python `if` around an activity call.
- **Visual plan projection**: ``pipeline_plan`` describes the same stable
  nodes and edges for approval/UI rendering; execution remains ordinary code.
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
- A finished visual editor or integration catalog (Airflow / n8n).
- Pool-based resource managers (Airflow Pools).

Run:

    uv run --project core/py python examples/py/data_pipeline.py
"""

from __future__ import annotations

import asyncio
import tempfile
from datetime import UTC, datetime

import opendal
from google.protobuf.wrappers_pb2 import Int64Value, StringValue

from temporaless import (
    ActivityOptions,
    OpenDALStore,
    Options,
    Workflow,
    WorkflowKey,
    annotate,
    gather_activities,
    inspect_run,
    plan_digest,
    project_workflow_run,
    run,
    validate_plan,
)
from temporaless.v1 import temporaless_pb2

# ---- activities ------------------------------------------------------------


async def _extract(date: StringValue) -> StringValue:
    """Extract: pull a partition list for the date. Returns comma-separated IDs."""
    annotate("step", "extract")
    annotate("date", date.value)
    # In production: vendor SDK call, S3 list, BigQuery query, etc.
    partitions = [f"{date.value}:p{i:02d}" for i in range(8)]
    return StringValue(value=",".join(partitions))


async def _transform_partition(partition: StringValue) -> Int64Value:
    """Per-partition transform: returns row count after cleaning."""
    annotate("step", "transform")
    annotate("partition", partition.value)
    await asyncio.sleep(0.02)  # simulate work
    date, _partition_id = partition.value.split(":", maxsplit=1)
    # Deterministic fixture: the current run follows the load branch while the
    # previous-day backfill follows the alert branch. A branch decision must
    # derive from recorded inputs/results, not fresh randomness during replay.
    return Int64Value(value=250 if date.endswith("-04") else 75)


async def _validate(total_rows: Int64Value) -> StringValue:
    """Quality gate: enough rows? Returns 'ok' or 'too_few'."""
    annotate("step", "validate")
    annotate("rows", str(total_rows.value))
    if total_rows.value < 1_000:
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


def pipeline_plan(date: str) -> temporaless_pb2.WorkflowPlan:
    """The same topology a visual editor or AI planner would render.

    The plan is metadata, not an interpreter. Its stable node IDs are reused
    unchanged by the workflow body as activity IDs, allowing the UI to overlay
    durable records without trying to infer Python control flow.
    """
    string_type = "google.protobuf.StringValue"
    integer_type = "google.protobuf.Int64Value"
    partitions = [f"{date}:p{i:02d}" for i in range(8)]
    nodes = [
        temporaless_pb2.WorkflowPlanNode(
            node_id="extract",
            display_name="Extract partitions",
            kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
            operation="pipeline.v1.DailyPipeline.Extract",
            request_type=string_type,
            response_type=string_type,
        ),
        temporaless_pb2.WorkflowPlanNode(
            node_id="transform",
            display_name="Transform partitions",
            kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_FAN_OUT,
        ),
        *[
            temporaless_pb2.WorkflowPlanNode(
                node_id=f"transform:{partition}",
                display_name=f"Transform {partition}",
                kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
                operation="pipeline.v1.DailyPipeline.TransformPartition",
                request_type=string_type,
                response_type=integer_type,
            )
            for partition in partitions
        ],
        temporaless_pb2.WorkflowPlanNode(
            node_id="validate",
            display_name="Validate row count",
            kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_BRANCH,
            operation="pipeline.v1.DailyPipeline.Validate",
            request_type=integer_type,
            response_type=string_type,
        ),
        temporaless_pb2.WorkflowPlanNode(
            node_id="load",
            display_name="Load output",
            kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
            operation="pipeline.v1.DailyPipeline.Load",
            request_type=integer_type,
            response_type=string_type,
        ),
        temporaless_pb2.WorkflowPlanNode(
            node_id="notify",
            display_name="Notify success",
            kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
            operation="pipeline.v1.DailyPipeline.Notify",
            request_type=string_type,
            response_type=string_type,
        ),
        temporaless_pb2.WorkflowPlanNode(
            node_id="alert",
            display_name="Alert and halt",
            kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
            operation="pipeline.v1.DailyPipeline.Alert",
            request_type=string_type,
            response_type=string_type,
        ),
    ]
    edges = [
        temporaless_pb2.WorkflowPlanEdge(
            source_node_id="extract",
            target_node_id="transform",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONTROL,
        ),
        *[
            temporaless_pb2.WorkflowPlanEdge(
                source_node_id="transform",
                target_node_id=f"transform:{partition}",
                kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONTROL,
            )
            for partition in partitions
        ],
        *[
            temporaless_pb2.WorkflowPlanEdge(
                source_node_id=f"transform:{partition}",
                target_node_id="validate",
                kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONTROL,
            )
            for partition in partitions
        ],
        temporaless_pb2.WorkflowPlanEdge(
            source_node_id="validate",
            target_node_id="load",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL,
            label="ok",
        ),
        temporaless_pb2.WorkflowPlanEdge(
            source_node_id="validate",
            target_node_id="alert",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL,
            label="too_few",
        ),
        temporaless_pb2.WorkflowPlanEdge(
            source_node_id="load",
            target_node_id="notify",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_DATA,
        ),
    ]
    return temporaless_pb2.WorkflowPlan(
        plan_id=f"stocks:daily:{date}",
        revision=1,
        nodes=nodes,
        edges=edges,
        annotations={"date": date},
    )


def render_plan(plan: temporaless_pb2.WorkflowPlan) -> None:
    """A tiny stand-in for the boxes and arrows a real UI would render."""
    print(f"  plan: {plan.plan_id} revision={plan.revision}")
    print(f"  candidate SHA-256: {plan_digest(plan)}")
    for node in plan.nodes:
        kind = temporaless_pb2.WorkflowPlanNodeKind.Name(node.kind).removeprefix(
            "WORKFLOW_PLAN_NODE_KIND_"
        )
        print(f"    [{kind:9}] {node.node_id}: {node.display_name}")
    for edge in plan.edges:
        kind = temporaless_pb2.WorkflowPlanEdgeKind.Name(edge.kind).removeprefix(
            "WORKFLOW_PLAN_EDGE_KIND_"
        )
        label = f":{edge.label}" if edge.label else ""
        print(f"    {edge.source_node_id} --{kind}{label}--> {edge.target_node_id}")


async def render_run(
    store: OpenDALStore,
    plan: temporaless_pb2.WorkflowPlan,
    run_id: str,
) -> None:
    """Overlay authoritative records on the approved plan."""
    inspection = await inspect_run(
        store,
        WorkflowKey(workflow_id="pipeline:stocks_daily", run_id=run_id),
    )
    projection = project_workflow_run(plan, inspection)
    print(f"  durable evidence for run {run_id}:")
    for projected in projection.nodes:
        evidence: list[str] = []
        if projected.activity is not None:
            status = temporaless_pb2.ActivityStatus.Name(
                projected.activity.status
            ).removeprefix("ACTIVITY_STATUS_")
            evidence.append(f"activity={status}")
        evidence.extend(
            "timer="
            + temporaless_pb2.TimerStatus.Name(timer.status).removeprefix(
                "TIMER_STATUS_"
            )
            for timer in projected.timers
        )
        if projected.event is not None:
            evidence.append("event=DELIVERED")
        if projected.claims:
            evidence.append(f"claims={len(projected.claims)}")
        print(
            f"    {projected.node.node_id}: "
            f"{', '.join(evidence) if evidence else 'no direct record'}"
        )
    unplanned = (
        len(projection.unplanned_activities)
        + len(projection.unplanned_timers)
        + len(projection.unplanned_events)
        + len(projection.unplanned_claims)
    )
    print(f"  unplanned durable records: {unplanned}")


# ---- the DAG, expressed as a workflow body ---------------------------------


async def daily_pipeline(workflow: Workflow, date: StringValue) -> StringValue:
    """One workflow run per (pipeline_id, date, approved plan revision).

    Backfill = call run() with a different date. Re-run after a transient
    failure = call run() again with the same run ID — completed steps
    short-circuit from storage.
    """
    annotate("pipeline", "stocks_daily")

    # Stage 1: extract (single activity)
    partitions_csv = await workflow.execute_activity(
        ActivityOptions(activity_id="extract"), date, StringValue, _extract
    )
    partitions = partitions_csv.value.split(",")

    # Stage 2: parallel transform (structured fan-out / fan-in)
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

    counts = await gather_activities(*(transform_with_limit(p) for p in partitions))
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

    current_date = "2026-05-04"
    current_plan = pipeline_plan(current_date)
    current_run_id = f"{current_date}:plan-r{current_plan.revision}"
    validate_plan(current_plan)
    print("=== visual plan (render this before execution) ===")
    render_plan(current_plan)
    # A real UI persists this value only after the user presses Approve. Build
    # the execution candidate again and verify its deterministic bytes at the
    # trust boundary rather than treating rendering as approval.
    approved_digest = plan_digest(current_plan)
    execution_plan = pipeline_plan(current_date)
    validate_plan(execution_plan)
    if plan_digest(execution_plan) != approved_digest:
        raise RuntimeError("workflow plan changed after approval")
    current_plan = execution_plan
    print(f"  approved SHA-256: {approved_digest}")

    print("=== first run (full DAG executes) ===")
    started = datetime.now(UTC)
    result = await run(
        store,
        Options(
            workflow_id="pipeline:stocks_daily",
            run_id=current_run_id,
        ),
        StringValue(value=current_date),
        StringValue,
        daily_pipeline,
    )
    print(f"  result: {result.value!r}")
    print(f"  wall time: {(datetime.now(UTC) - started).total_seconds() * 1000:.1f}ms")
    await render_run(store, current_plan, current_run_id)

    print("\n=== re-run (every step replays from storage; no work done) ===")
    started = datetime.now(UTC)
    result = await run(
        store,
        Options(
            workflow_id="pipeline:stocks_daily",
            run_id=current_run_id,
        ),
        StringValue(value=current_date),
        StringValue,
        daily_pipeline,
    )
    print(f"  result: {result.value!r}")
    print(f"  wall time: {(datetime.now(UTC) - started).total_seconds() * 1000:.1f}ms")

    print("\n=== backfill: run for a different date ===")
    backfill_date = "2026-05-03"
    backfill_plan = pipeline_plan(backfill_date)
    backfill_run_id = f"{backfill_date}:plan-r{backfill_plan.revision}"
    backfill_result = await run(
        store,
        Options(
            workflow_id="pipeline:stocks_daily",
            run_id=backfill_run_id,
        ),
        StringValue(value=backfill_date),
        StringValue,
        daily_pipeline,
    )
    print(f"  result: {backfill_result.value!r}")
    await render_run(store, backfill_plan, backfill_run_id)


if __name__ == "__main__":
    asyncio.run(main())
