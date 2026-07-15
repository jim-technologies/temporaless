"""End-to-end interop: Temporaless adapters (backfill, wait_for_workflow)
composed with prefectcompat-wrapped flows.

This is the load-bearing compatibility proof. If you wrap a workflow as a
Prefect flow, the rest of the framework — backfill, cross-workflow
dependencies, replay short-circuits — must continue to work without
ceremony. If any of these break, the adapter is a leaky abstraction.

Each test sets up:
- A Temporaless OpenDALStore (real records, real replay).
- A workflow body that uses the framework primitives (current_workflow,
  execute_activity, sleep, store).
- The same body wrapped as a Prefect flow.
- Then drives it via the framework's adapters.
"""

from __future__ import annotations

import opendal
import pytest
from google.protobuf.wrappers_pb2 import StringValue
from temporaless import (
    ActivityOptions,
    Options,
    Workflow,
    current_workflow,
    run,
)
from temporaless.backfill import backfill
from temporaless.dependencies import wait_for_workflow
from temporaless.storage import OpenDALStore
from temporaless_connectworkflow import WorkflowMethodWrapOptions, wrap_workflow_method

from temporaless_prefectcompat import WorkflowWrapOptions
from temporaless_prefectcompat import wrap_workflow as prefect_wrap_workflow


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


# ---- backfill over prefect-wrapped flows -----------------------------------


async def test_backfill_drives_prefect_wrapped_flows(store: OpenDALStore) -> None:
    """The backfill helper iterates over `invoke(run_id)`. When `invoke`
    triggers a prefect-wrapped flow that internally calls `temporaless.run`,
    every layer composes: Prefect tracks the dispatch, our store records
    the workflow record, replay short-circuits on the second invocation."""
    invocation_count = 0

    async def _flow_body(request: StringValue) -> StringValue:
        nonlocal invocation_count
        invocation_count += 1

        async def workflow_body(_wf: Workflow, req: StringValue) -> StringValue:
            return StringValue(value=f"price:{req.value}")

        # Each Prefect flow run drives a Temporaless workflow run with the
        # request value as run_id. This is the canonical pattern: Prefect
        # gives us the dispatch + UI, Temporaless gives us replay.
        return await run(
            store,
            Options(workflow_id="prices", run_id=request.value, code_version="v1"),
            request,
            StringValue,
            workflow_body,
        )

    PrefectFlow = prefect_wrap_workflow(_flow_body, WorkflowWrapOptions(name="BackfillTargetFlow"))

    async def invoke(run_id: str) -> StringValue:
        return await PrefectFlow(StringValue(value=run_id))

    run_ids = ["2026-05-01", "2026-05-02", "2026-05-03"]
    first = await backfill(invoke, run_ids, concurrency=1)
    assert len(first.succeeded()) == 3
    assert invocation_count == 3

    # Second backfill: every Temporaless workflow record exists, so the
    # workflow body short-circuits via replay. Prefect still spawns flow
    # runs (it doesn't know about our replay), so the prefect body
    # invocation count goes up — but the *vendor activity* call count
    # would be unchanged. Here, we count flow body invocations to
    # demonstrate Prefect's role.
    second = await backfill(invoke, run_ids, concurrency=1)
    assert len(second.succeeded()) == 3
    # Body fires again (Prefect side), but inner run() short-circuits via
    # storage replay — that's the actual replay invariant we care about.
    assert invocation_count == 6


# ---- cross-pipeline deps inside prefect-wrapped flows -----------------------


async def test_wait_for_workflow_inside_prefect_wrapped_flow(store: OpenDALStore) -> None:
    """A prefect-wrapped flow whose body calls wait_for_workflow against
    Temporaless storage. The dependency is COMPLETED → result returned
    cleanly. The Prefect wrapping doesn't interfere with our typed errors."""

    # Seed the upstream workflow.
    async def upstream_body(_wf: Workflow, _req: StringValue) -> StringValue:
        return StringValue(value="upstream:done")

    await run(
        store,
        Options(workflow_id="upstream", run_id="2026-05-04", code_version="v1"),
        StringValue(value="seed"),
        StringValue,
        upstream_body,
    )

    async def downstream(request: StringValue) -> StringValue:
        # Inside a Prefect flow body, we still use the framework's primitives.
        upstream = await wait_for_workflow(
            store,
            workflow_id="upstream",
            run_id=request.value,
            result_factory=StringValue,
        )
        return StringValue(value=f"downstream({upstream.value})")

    DownstreamFlow = prefect_wrap_workflow(downstream, WorkflowWrapOptions(name="DownstreamFlow"))

    result = await DownstreamFlow(StringValue(value="2026-05-04"))
    assert isinstance(result, StringValue)
    assert result.value == "downstream(upstream:done)"


# ---- wrap_workflow_method composes with prefect_wrap_workflow ---------------


async def test_canonical_grpc_handler_wrapped_as_prefect_flow(store: OpenDALStore) -> None:
    """The fully composed shape: a `@wrap_workflow_method`-decorated gRPC
    handler is then wrapped by `prefectcompat.wrap_workflow`. Calling the
    Prefect-wrapped form drives the gRPC handler, which drives Temporaless
    replay. Three layers, all transparent."""
    vendor_calls = 0

    class PriceService:
        def __init__(self, s: OpenDALStore) -> None:
            self._store = s

        @wrap_workflow_method(
            WorkflowMethodWrapOptions(
                store=lambda self: self._store,  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                result_type=StringValue,
                options_for=lambda _self, request: Options(
                    workflow_id="prices",
                    run_id=request.value,
                    code_version="v1",
                ),
            )
        )
        async def fetch_prices(self, request: StringValue, _ctx: object = None) -> StringValue:
            async def vendor(req: StringValue) -> StringValue:
                nonlocal vendor_calls
                vendor_calls += 1
                return StringValue(value=f"vendor:{req.value}")

            return await current_workflow().execute_activity(
                ActivityOptions(activity_id=f"fetch:{request.value}"),
                request,
                StringValue,
                vendor,
            )

    service = PriceService(store)

    async def gRPC_handler_as_flow(req: StringValue) -> StringValue:
        return await service.fetch_prices(req)

    PrefectFetchPrices = prefect_wrap_workflow(
        gRPC_handler_as_flow, WorkflowWrapOptions(name="PrefectFetchPrices")
    )

    first = await PrefectFetchPrices(StringValue(value="2026-05-04"))
    assert first.value == "vendor:2026-05-04"
    assert vendor_calls == 1

    # Second call: Prefect runs the body again, but Temporaless replay
    # short-circuits the inner gRPC handler — vendor doesn't fire.
    second = await PrefectFetchPrices(StringValue(value="2026-05-04"))
    assert second.value == "vendor:2026-05-04"
    assert vendor_calls == 1  # ← replay invariant holds through Prefect
