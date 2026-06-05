"""Stress tests — proof the framework holds under concurrent production-shape load.

These exist to give operators concrete numbers they can quote ("we tested 200
concurrent workflows against one store and they all succeeded"). Marked slow;
opt in via ``-m stress`` or run as part of the gate.

Each test exercises a different production-relevant axis:

- Many concurrent *workflows* against the same store (``asyncio.gather`` over
  100+ ``run`` calls). Verifies storage doesn't deadlock under fan-out.
- Many concurrent *activities* inside one workflow body. Verifies in-workflow
  fan-out works at scale.
- Replay at scale: re-run all 100+ workflows; every one must short-circuit
  via storage replay (zero new vendor calls).
- Halt-on-error backfill at high concurrency: a single bad run_id must not
  starve the rest.
"""

from __future__ import annotations

import asyncio
import time

import opendal
import pytest
from google.protobuf.wrappers_pb2 import StringValue

from temporaless import (
    ActivityOptions,
    Options,
    Workflow,
    current_workflow,
    run,
    wrap_workflow_method,
)
from temporaless.backfill import backfill
from temporaless.storage import OpenDALStore


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


# ---- 100+ concurrent workflows against one store ----------------------------


@pytest.mark.stress
async def test_one_hundred_concurrent_workflows_complete(store: OpenDALStore) -> None:
    """100 concurrent workflow runs, each with one activity, all against the
    same Store. Every workflow must complete with the right result. This is
    the canonical "fan-out fetch" pattern at production scale."""
    activity_calls: dict[str, int] = {}

    async def vendor(req: StringValue) -> StringValue:
        activity_calls[req.value] = activity_calls.get(req.value, 0) + 1
        return StringValue(value=f"price:{req.value}")

    async def workflow_body(_wf: Workflow, request: StringValue) -> StringValue:
        return await current_workflow().execute_activity(
            ActivityOptions(activity_id=f"vendor:{request.value}"),
            request,
            StringValue,
            vendor,
        )

    n = 100
    run_ids = [f"sym-{i:03d}" for i in range(n)]

    started = time.perf_counter()
    results = await asyncio.gather(
        *(
            run(
                store,
                Options(
                    workflow_id="prices",
                    run_id=run_id,
                    code_version="v1",
                ),
                StringValue(value=run_id),
                StringValue,
                workflow_body,
            )
            for run_id in run_ids
        )
    )
    elapsed = time.perf_counter() - started

    assert len(results) == n
    for run_id, result in zip(run_ids, results, strict=True):
        assert result.value == f"price:{run_id}"
    # Each workflow's activity ran exactly once.
    assert all(count == 1 for count in activity_calls.values()), (
        f"some activities ran more than once: {activity_calls}"
    )
    per = elapsed * 1000 / n
    print(f"\n[stress] 100 concurrent workflows: {elapsed:.2f}s  ({per:.1f} ms/workflow)")


@pytest.mark.stress
async def test_one_hundred_workflows_replay_with_zero_vendor_calls(store: OpenDALStore) -> None:
    """After 100 workflows complete, re-running all of them must short-circuit
    via storage replay — the activity body should NOT fire again. This is the
    replay invariant at production scale."""
    activity_calls = 0

    async def vendor(req: StringValue) -> StringValue:
        nonlocal activity_calls
        activity_calls += 1
        return StringValue(value=f"price:{req.value}")

    async def workflow_body(_wf: Workflow, request: StringValue) -> StringValue:
        return await current_workflow().execute_activity(
            ActivityOptions(activity_id=f"vendor:{request.value}"),
            request,
            StringValue,
            vendor,
        )

    n = 100
    run_ids = [f"sym-{i:03d}" for i in range(n)]

    async def run_all() -> list[StringValue]:
        return await asyncio.gather(
            *(
                run(
                    store,
                    Options(workflow_id="prices", run_id=rid, code_version="v1"),
                    StringValue(value=rid),
                    StringValue,
                    workflow_body,
                )
                for rid in run_ids
            )
        )

    # First pass: every workflow runs once.
    first = await run_all()
    assert activity_calls == n

    # Second pass: every workflow short-circuits via stored record.
    started = time.perf_counter()
    second = await run_all()
    replay_elapsed = time.perf_counter() - started

    assert len(second) == n
    assert activity_calls == n, f"replay re-fired the activity ({activity_calls} > {n})"
    for first_result, second_result in zip(first, second, strict=True):
        assert first_result.value == second_result.value
    per = replay_elapsed * 1000 / n
    print(f"\n[stress] 100-workflow replay: {replay_elapsed:.2f}s  ({per:.1f} ms/workflow)")


# ---- many concurrent activities inside one workflow ------------------------


@pytest.mark.stress
async def test_fifty_parallel_activities_inside_one_workflow(store: OpenDALStore) -> None:
    """One workflow, 50 parallel activities via asyncio.gather. Verifies the
    in-workflow fan-out path scales — every activity gets its own claim and
    record without contention."""
    activity_calls: dict[str, int] = {}

    async def fetch(req: StringValue) -> StringValue:
        activity_calls[req.value] = activity_calls.get(req.value, 0) + 1
        return StringValue(value=f"v:{req.value}")

    async def workflow_body(_wf: Workflow, _request: StringValue) -> StringValue:
        async def fetch_one(symbol: str) -> StringValue:
            return await current_workflow().execute_activity(
                ActivityOptions(activity_id=f"fetch:{symbol}"),
                StringValue(value=symbol),
                StringValue,
                fetch,
            )

        symbols = [f"sym-{i:03d}" for i in range(50)]
        results = await asyncio.gather(*(fetch_one(s) for s in symbols))
        return StringValue(value=",".join(r.value for r in results))

    started = time.perf_counter()
    result = await run(
        store,
        Options(workflow_id="fanout", run_id="2026-05-08", code_version="v1"),
        StringValue(value="batch"),
        StringValue,
        workflow_body,
    )
    elapsed = time.perf_counter() - started

    assert result.value.count("v:") == 50
    assert all(count == 1 for count in activity_calls.values())
    print(f"\n[stress] 50 parallel activities in 1 workflow: {elapsed:.2f}s")


# ---- backfill at high concurrency with a poison pill ----------------------


@pytest.mark.stress
async def test_backfill_at_high_concurrency_isolates_failures(store: OpenDALStore) -> None:
    """Backfill 100 run_ids with concurrency=20, one of them poisoned. Must
    end with 99 SUCCEEDED + 1 FAILED — a single bad partition can't starve
    or block the rest. The framework's per-run isolation is what makes
    backfill safe."""

    class _Service:
        def __init__(self, s: OpenDALStore) -> None:
            self._store = s

        @wrap_workflow_method(
            store=lambda self: self._store,  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            result_type=StringValue,
            options_for=lambda _self, request: Options(
                workflow_id="backfill",
                run_id=request.value,
                code_version="v1",
            ),
        )
        async def fetch(self, request: StringValue, _ctx: object = None) -> StringValue:
            if request.value == "POISON-50":
                raise RuntimeError("upstream broke for poison-50")
            return StringValue(value=f"ok:{request.value}")

    service = _Service(store)
    run_ids = [f"sym-{i:03d}" for i in range(100)]
    run_ids[50] = "POISON-50"

    async def invoke(rid: str) -> StringValue:
        return await service.fetch(StringValue(value=rid))

    started = time.perf_counter()
    report = await backfill(invoke, run_ids, concurrency=20)
    elapsed = time.perf_counter() - started

    assert len(report.entries) == 100
    assert len(report.succeeded()) == 99
    assert len(report.failed()) == 1
    assert report.failed()[0].run_id == "POISON-50"
    print(f"\n[stress] backfill 100 @ concurrency=20: {elapsed:.2f}s, {report}")
