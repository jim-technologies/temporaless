"""Tests for ``temporaless.dependencies.wait_for_workflow``."""

from __future__ import annotations

import opendal
import pytest
from google.protobuf.wrappers_pb2 import StringValue

from temporaless import (
    Options,
    Workflow,
    WorkflowDependencyFailedError,
    WorkflowDependencyPendingError,
    current_workflow,
    run,
)
from temporaless.dependencies import wait_for_workflow
from temporaless.storage import OpenDALStore
from temporaless.workflow import WorkflowConflictError


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


async def _seed_completed_upstream(store: OpenDALStore, run_id: str, value: str) -> None:
    async def body(_workflow: Workflow, request: StringValue) -> StringValue:
        return StringValue(value=value)

    await run(
        store,
        Options(workflow_id="upstream", run_id=run_id, code_version="v1"),
        StringValue(value="seed"),
        StringValue,
        body,
    )


async def test_wait_for_workflow_returns_completed_upstream_result(
    store: OpenDALStore,
) -> None:
    await _seed_completed_upstream(store, "2026-05-04", "AAPL:100")

    result = await wait_for_workflow(
        store,
        workflow_id="upstream",
        run_id="2026-05-04",
        result_factory=StringValue,
    )
    assert result.value == "AAPL:100"


async def test_wait_for_workflow_raises_pending_when_no_upstream_record(
    store: OpenDALStore,
) -> None:
    with pytest.raises(WorkflowDependencyPendingError) as excinfo:
        await wait_for_workflow(
            store,
            workflow_id="upstream",
            run_id="missing",
            result_factory=StringValue,
        )
    assert excinfo.value.workflow_id == "upstream"
    assert excinfo.value.run_id == "missing"


async def test_wait_for_workflow_raises_failed_when_upstream_failed(
    store: OpenDALStore,
) -> None:
    """Seed a FAILED upstream record by running a body that raises."""

    async def failing_body(_workflow: Workflow, _request: StringValue) -> StringValue:
        raise RuntimeError("upstream broke")

    with pytest.raises(RuntimeError, match="upstream broke"):
        await run(
            store,
            Options(workflow_id="upstream", run_id="2026-05-04", code_version="v1"),
            StringValue(value="seed"),
            StringValue,
            failing_body,
        )

    with pytest.raises(WorkflowDependencyFailedError) as excinfo:
        await wait_for_workflow(
            store,
            workflow_id="upstream",
            run_id="2026-05-04",
            result_factory=StringValue,
        )
    assert excinfo.value.workflow_id == "upstream"
    assert excinfo.value.run_id == "2026-05-04"


async def test_wait_for_workflow_inside_workflow_body_uses_current_store(
    store: OpenDALStore,
) -> None:
    """Realistic usage: a workflow body waits on an upstream workflow via
    ``current_workflow().store``. Replay short-circuits on the second call."""
    await _seed_completed_upstream(store, "2026-05-04", "AAPL:100")

    downstream_calls = 0

    async def downstream(_workflow: Workflow, request: StringValue) -> StringValue:
        nonlocal downstream_calls
        downstream_calls += 1
        wf = current_workflow()
        upstream = await wait_for_workflow(
            wf.store,
            workflow_id="upstream",
            run_id=request.value,
            result_factory=StringValue,
        )
        return StringValue(value=f"signal({upstream.value})")

    first = await run(
        store,
        Options(workflow_id="signal", run_id="2026-05-04", code_version="v1"),
        StringValue(value="2026-05-04"),
        StringValue,
        downstream,
    )
    assert first.value == "signal(AAPL:100)"
    assert downstream_calls == 1

    # Replay: workflow record exists, body doesn't re-execute.
    second = await run(
        store,
        Options(workflow_id="signal", run_id="2026-05-04", code_version="v1"),
        StringValue(value="2026-05-04"),
        StringValue,
        downstream,
    )
    assert second.value == "signal(AAPL:100)"
    assert downstream_calls == 1


async def test_wait_for_workflow_raises_conflict_on_result_type_mismatch(
    store: OpenDALStore,
) -> None:
    """If the upstream's stored result type doesn't match the requested
    factory, surface as a typed conflict so the bug is loud."""
    from google.protobuf.wrappers_pb2 import Int32Value

    await _seed_completed_upstream(store, "2026-05-04", "AAPL:100")

    with pytest.raises(WorkflowConflictError, match="result type"):
        await wait_for_workflow(
            store,
            workflow_id="upstream",
            run_id="2026-05-04",
            result_factory=Int32Value,
        )
