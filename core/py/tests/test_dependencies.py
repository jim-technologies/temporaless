"""Tests for ``temporaless.dependencies.wait_for_workflow``."""

from __future__ import annotations

from datetime import timedelta

import opendal
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.wrappers_pb2 import StringValue
from protovalidate import ValidationError

from temporaless import (
    Options,
    PollOptions,
    Workflow,
    WorkflowDependencyFailedError,
    WorkflowDependencyPendingError,
    current_workflow,
    run,
)
from temporaless.dependencies import wait_for_workflow
from temporaless.storage import (
    OpenDALStore,
    RunRecordValidationError,
    TimerKey,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import WorkflowConflictError, WorkflowInfrastructureError


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


async def _seed_completed_upstream(store: OpenDALStore, run_id: str, value: str) -> None:
    async def body(_workflow: Workflow, request: StringValue) -> StringValue:
        return StringValue(value=value)

    await run(
        store,
        Options(workflow_id="upstream", run_id=run_id),
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
            Options(workflow_id="upstream", run_id="2026-05-04"),
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
        Options(workflow_id="signal", run_id="2026-05-04"),
        StringValue(value="2026-05-04"),
        StringValue,
        downstream,
    )
    assert first.value == "signal(AAPL:100)"
    assert downstream_calls == 1

    # Replay: workflow record exists, body doesn't re-execute.
    second = await run(
        store,
        Options(workflow_id="signal", run_id="2026-05-04"),
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


@pytest.mark.parametrize("corruption", ["missing", "malformed"])
async def test_wait_for_workflow_rejects_corrupt_completed_result(
    store: OpenDALStore,
    corruption: str,
) -> None:
    await _seed_completed_upstream(store, corruption, "AAPL:100")
    key = WorkflowKey(workflow_id="upstream", run_id=corruption)
    record = await store.get_workflow(key)
    assert record is not None
    if corruption == "missing":
        record.ClearField("result")
    else:
        record.result.type_url = "type.googleapis.com/google.protobuf.StringValue"
        record.result.value = b"\xff"
    await store.put_workflow(record)

    with pytest.raises(RunRecordValidationError):
        await wait_for_workflow(
            store,
            workflow_id="upstream",
            run_id=corruption,
            result_factory=StringValue,
        )


async def test_wait_for_workflow_poll_schedules_and_terminally_acknowledges(
    store: OpenDALStore,
) -> None:
    interval = Duration()
    interval.FromTimedelta(timedelta(hours=1))
    poll = PollOptions(timer_id="poll:upstream", interval=interval)
    options = Options(workflow_id="downstream", run_id="run")

    async def downstream(_workflow: Workflow, _request: StringValue) -> StringValue:
        return await wait_for_workflow(
            current_workflow().store,
            workflow_id="upstream",
            run_id="partition",
            result_factory=StringValue,
            poll_options=poll,
        )

    with pytest.raises(WorkflowDependencyPendingError) as captured:
        await run(
            store,
            options,
            StringValue(value="request"),
            StringValue,
            downstream,
        )
    assert captured.value.wake_at is not None
    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id=poll.timer_id,
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.timer_kind == temporaless_pb2.TIMER_KIND_POLL
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED

    await _seed_completed_upstream(store, "partition", "ready")
    result = await run(
        store,
        options,
        StringValue(value="request"),
        StringValue,
        downstream,
    )
    assert result.value == "ready"
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_FIRED


async def test_wait_for_workflow_read_outage_leaves_parent_in_progress(
    store: OpenDALStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_get = store.get_workflow

    async def fail_upstream(key: WorkflowKey):
        if key.workflow_id == "upstream":
            raise RuntimeError("dependency store unavailable")
        return await original_get(key)

    monkeypatch.setattr(store, "get_workflow", fail_upstream)
    options = Options(workflow_id="downstream", run_id="outage")

    async def downstream(_workflow: Workflow, _request: StringValue) -> StringValue:
        return await wait_for_workflow(
            current_workflow().store,
            workflow_id="upstream",
            run_id="partition",
            result_factory=StringValue,
        )

    with pytest.raises(WorkflowInfrastructureError, match="dependency store unavailable"):
        await run(
            store,
            options,
            StringValue(value="request"),
            StringValue,
            downstream,
        )
    record = await original_get(WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id))
    assert record is not None
    assert record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS


async def test_wait_for_workflow_validates_key_and_factory_before_read(
    store: OpenDALStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reads = 0

    async def count_read(_key: WorkflowKey):
        nonlocal reads
        reads += 1
        return None

    monkeypatch.setattr(store, "get_workflow", count_read)
    with pytest.raises(ValidationError):
        await wait_for_workflow(
            store,
            workflow_id=".",
            run_id="run",
            result_factory=StringValue,
        )
    assert reads == 0

    with pytest.raises(TypeError, match="protobuf message"):
        await wait_for_workflow(
            store,
            workflow_id="upstream",
            run_id="run",
            result_factory=lambda: "not-protobuf",  # type: ignore[return-value]
        )
    assert reads == 0
