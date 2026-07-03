from datetime import timedelta

import opendal
import pytest
from google.protobuf.wrappers_pb2 import StringValue
from temporaless_indexstore import IndexedStore

from temporaless.inspector import (
    list_activities,
    list_failed_workflows,
    list_in_flight_workflows,
    list_workflows_by_status,
    reset_activity,
    reset_event,
    reset_workflow,
)
from temporaless.storage import (
    ActivityKey,
    EventKey,
    Store,
    WorkflowKey,
    send_event,
)
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ActivityError,
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


async def test_list_activities_and_reset_helpers(
    operator: opendal.AsyncOperator, store: Store
) -> None:
    calls = 0

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        async def fetch() -> StringValue:
            nonlocal calls
            calls += 1
            return StringValue(value=f"ok:{request.value}")

        return await workflow.run_activity(
            "fetch:" + request.value,
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            request,
            StringValue,
            fetch,
        )

    options = Options(workflow_id="prices:reset", run_id="2026-05-04", code_version="test")
    await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    wf_key = WorkflowKey(workflow_id="prices:reset", run_id="2026-05-04")
    activities = await list_activities(store, wf_key)
    assert [a.key.activity_id for a in activities] == ["fetch:AAPL"]

    await reset_workflow(store, wf_key)
    await reset_activity(
        store,
        ActivityKey(
            workflow_id="prices:reset",
            run_id="2026-05-04",
            activity_id="fetch:AAPL",
        ),
    )
    await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert calls == 2


async def test_reset_event_clears_delivered_event(
    operator: opendal.AsyncOperator, store: Store
) -> None:
    key = EventKey(workflow_id="prices:event-reset", run_id="2026-05-04", event_id="approval")
    await send_event(store, key, StringValue(value="manager"))
    assert await store.get_event(key) is not None

    await reset_event(store, key)
    assert await store.get_event(key) is None


async def test_reset_is_idempotent_on_missing_path(store: Store) -> None:
    await reset_workflow(
        store,
        WorkflowKey(workflow_id="missing", run_id="missing"),
    )


async def test_list_in_flight_and_failed_workflows(
    operator: opendal.AsyncOperator, store: Store
) -> None:
    # completed
    async def done(_w: Workflow, _r: StringValue) -> StringValue:
        return StringValue(value="ok")

    await run(
        store,
        Options(workflow_id="prices:done", run_id="2026-05-04", code_version="test"),
        StringValue(value="AAPL"),
        StringValue,
        done,
    )

    # in-flight via sleep
    async def waiting(workflow: Workflow, _r: StringValue) -> StringValue:
        await workflow.sleep("wait", timedelta(hours=1))
        return StringValue(value="ok")

    with pytest.raises(TimerPendingError):
        await run(
            store,
            Options(workflow_id="prices:waiting", run_id="2026-05-04", code_version="test"),
            StringValue(value="AAPL"),
            StringValue,
            waiting,
        )

    # failed
    async def boom(_w: Workflow, _r: StringValue) -> StringValue:
        raise ActivityError("upstream_5xx", "boom")

    with pytest.raises(ActivityError):
        await run(
            store,
            Options(workflow_id="prices:broken", run_id="2026-05-04", code_version="test"),
            StringValue(value="AAPL"),
            StringValue,
            boom,
        )

    in_flight = await list_in_flight_workflows(store)
    assert [r.key.workflow_id for r in in_flight] == ["prices:waiting"]

    failed = await list_failed_workflows(store)
    assert [r.key.workflow_id for r in failed] == ["prices:broken"]
    assert failed[0].failure.code == "upstream_5xx"

    completed = await list_workflows_by_status(store, temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    assert [r.key.workflow_id for r in completed] == ["prices:done"]
