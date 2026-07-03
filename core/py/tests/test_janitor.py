from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue
from temporaless_indexstore import IndexedStore

from temporaless.janitor import sweep
from temporaless.storage import Store, WorkflowKey
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ActivityOptions,
    Options,
    TimerPendingError,
    Workflow,
    run,
)


@pytest.fixture
def root(tmp_path):
    return str(tmp_path)


@pytest.fixture
def operator(root):
    return opendal.AsyncOperator("fs", root=root)


@pytest.fixture
def store(operator, tmp_path):
    return IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")


async def test_sweep_deletes_old_completed_runs(
    operator: opendal.AsyncOperator, store: Store
) -> None:
    # Run 1: backdated to 48h ago, should be swept.
    await _run_workflow(store, "prices:old", "2026-05-03")
    await _backdate_completed(
        store, "prices:old", "2026-05-03", datetime.now(UTC) - timedelta(hours=48)
    )

    # Run 2: completed just now, should be kept.
    await _run_workflow(store, "prices:fresh", "2026-05-04")

    # Run 3: still in progress (TimerPendingError leaves IN_PROGRESS), should be kept.
    await _leave_in_progress(store, "prices:waiting", "2026-05-04")

    deleted = await sweep(store, datetime.now(UTC), timedelta(hours=24))
    assert deleted == 1

    assert (
        await store.get_workflow(WorkflowKey(workflow_id="prices:old", run_id="2026-05-03")) is None
    )
    assert (
        await store.get_workflow(WorkflowKey(workflow_id="prices:fresh", run_id="2026-05-04"))
        is not None
    )
    assert (
        await store.get_workflow(WorkflowKey(workflow_id="prices:waiting", run_id="2026-05-04"))
        is not None
    )


async def test_sweep_rejects_bad_input(operator: opendal.AsyncOperator, store: Store) -> None:
    with pytest.raises(ValueError):
        await sweep(store, datetime.now(UTC), timedelta(0))


async def test_sweep_skips_in_progress_and_failed_records(
    operator: opendal.AsyncOperator, store: Store
) -> None:
    """Sweep only deletes COMPLETED runs older than max_age. IN_PROGRESS and
    FAILED records are kept regardless of age — operators audit FAILED, and
    IN_PROGRESS workflows might still resume.
    """
    # Three runs all aged 48h:
    #   - prices:done    COMPLETED → swept
    #   - prices:running IN_PROGRESS → kept
    #   - prices:broken  FAILED → kept

    await _run_workflow(store, "prices:done", "2026-05-03")
    backdate = datetime.now(UTC) - timedelta(hours=48)
    await _backdate_completed(store, "prices:done", "2026-05-03", backdate)

    await _leave_in_progress(store, "prices:running", "2026-05-03")

    from temporaless.workflow import ActivityError

    async def boom(_w: Workflow, _r: StringValue) -> StringValue:
        raise ActivityError("upstream", "fail")

    with pytest.raises(ActivityError):
        await run(
            store,
            Options(workflow_id="prices:broken", run_id="2026-05-03", code_version="test"),
            StringValue(value="AAPL"),
            StringValue,
            boom,
        )

    deleted = await sweep(store, datetime.now(UTC), timedelta(hours=24))
    assert deleted == 1, "only the COMPLETED+old run should be swept"

    assert (
        await store.get_workflow(WorkflowKey(workflow_id="prices:done", run_id="2026-05-03"))
    ) is None
    assert (
        await store.get_workflow(WorkflowKey(workflow_id="prices:running", run_id="2026-05-03"))
    ) is not None
    assert (
        await store.get_workflow(WorkflowKey(workflow_id="prices:broken", run_id="2026-05-03"))
    ) is not None


async def test_sweep_on_empty_store_returns_zero(
    operator: opendal.AsyncOperator, store: Store
) -> None:
    """No COMPLETED records → sweep is a no-op returning 0."""
    deleted = await sweep(store, datetime.now(UTC), timedelta(hours=24))
    assert deleted == 0


async def _run_workflow(store: Store, workflow_id: str, run_id: str) -> None:
    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        async def fetch(r: StringValue) -> StringValue:
            return StringValue(value=f"ok:{r.value}")

        return await workflow.execute_activity(
            ActivityOptions(activity_id="fetch:" + request.value),
            request,
            StringValue,
            fetch,
        )

    await run(
        store,
        Options(workflow_id=workflow_id, run_id=run_id, code_version="test"),
        StringValue(value="AAPL"),
        StringValue,
        execute,
    )


async def _leave_in_progress(store: Store, workflow_id: str, run_id: str) -> None:
    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        await workflow.sleep("wait", timedelta(hours=1))
        return StringValue(value="done")

    with pytest.raises(TimerPendingError):
        await run(
            store,
            Options(workflow_id=workflow_id, run_id=run_id, code_version="test"),
            StringValue(value="AAPL"),
            StringValue,
            execute,
        )


async def _backdate_completed(
    store: Store, workflow_id: str, run_id: str, completed_at: datetime
) -> None:
    record = await store.get_workflow(WorkflowKey(workflow_id=workflow_id, run_id=run_id))
    assert record is not None
    assert record.status == temporaless_pb2.WORKFLOW_STATUS_COMPLETED
    timestamp = Timestamp()
    timestamp.FromDatetime(completed_at)
    record.completed_at.CopyFrom(timestamp)
    await store.put_workflow(record)
