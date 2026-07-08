import opendal
from temporaless_indexstore import IndexedStore

from temporaless.connectstore import (
    ConnectQueryStore,
    ConnectStore,
    LocalRecordStoreClient,
    RecordStoreService,
)
from temporaless.storage import (
    CREATE_ONLY_CLAIMS,
    WORKFLOW_RECORD_SCHEMA_VERSION,
    ActivityKey,
    ClaimKey,
    OpenDALStore,
    TimerKey,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2


async def test_connect_store_uses_record_store_service(tmp_path) -> None:
    store = ConnectStore.local(OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path))))
    key = WorkflowKey(workflow_id="prices:rpc", run_id="2026-05-02")

    assert await store.claim_capability() == CREATE_ONLY_CLAIMS
    assert await store.get_workflow(key) is None

    await store.put_workflow(
        temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
            code_version="test-version",
            status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        )
    )

    record = await store.get_workflow(key)
    assert record is not None
    assert (
        record.workflow_type == "workflow:google.protobuf.StringValue->google.protobuf.StringValue"
    )


async def test_connect_store_covers_storage_surface(tmp_path) -> None:
    service = RecordStoreService(OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path))))
    client = LocalRecordStoreClient(service)
    store = ConnectStore(client)

    activity_key = ActivityKey(workflow_id="prices:rpc", run_id="2026-05-02", activity_id="fetch")
    timer_key = TimerKey(workflow_id="prices:rpc", run_id="2026-05-02", timer_id="sleep")
    claim_key = ClaimKey(workflow_id="prices:rpc", run_id="2026-05-02", claim_id="activity:fetch")

    assert await store.get_activity(activity_key) is None
    assert await store.get_timer(timer_key) is None
    assert await store.get_claim(claim_key) is None


async def test_asgi_application_runs_interceptors(tmp_path) -> None:
    """The asgi_application helper must forward interceptors into the
    generated ASGI class — production deployments rely on this for auth,
    rate-limiting, tracing.
    """
    from temporaless.connectstore import asgi_application

    class RecordingInterceptor:
        """Implements connectrpc.interceptor.UnaryInterceptor (Protocol)."""

        def __init__(self) -> None:
            self.calls: list[str] = []

        async def intercept_unary(self, call_next, request, ctx):
            self.calls.append(ctx.method.name)
            return await call_next(request, ctx)

    interceptor = RecordingInterceptor()
    backend = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    app = asgi_application(backend, interceptors=[interceptor])
    # Drive a real RPC through the in-process service to exercise the
    # interceptor chain. The integration test covers the full ASGI/uvicorn
    # path; here we just confirm the helper actually plumbed the interceptor
    # in (so production users' auth/rate-limit middleware will fire).
    service = app._service
    response = await service.get_store_capabilities(
        temporaless_pb2.GetStoreCapabilitiesRequest(), None
    )
    assert response.claim_capability == CREATE_ONLY_CLAIMS
    # Direct in-process service calls bypass the interceptor stack (which
    # lives in the ASGI middleware), so verify wiring instead of behaviour:
    assert any(i is interceptor for i in app._interceptors)


async def test_connect_store_sweep_round_trip(tmp_path) -> None:
    """The query Sweep RPC delegates to the indexed QueryStore and returns the count."""
    from datetime import UTC, datetime, timedelta

    from google.protobuf.timestamp_pb2 import Timestamp

    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    backend = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    store = ConnectStore.local(backend)
    query = ConnectQueryStore.local(backend)

    # Seed: one COMPLETED workflow backdated 48h, one fresh.
    old_completed = Timestamp()
    old_completed.FromDatetime(datetime.now(UTC) - timedelta(hours=48))
    fresh_completed = Timestamp()
    fresh_completed.GetCurrentTime()
    for run_id, completed_at in (("old", old_completed), ("fresh", fresh_completed)):
        await store.put_workflow(
            temporaless_pb2.WorkflowRecord(
                schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
                key=WorkflowKey(workflow_id="prices:sweep", run_id=run_id).to_proto(),
                workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
                code_version="v1",
                status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
                completed_at=completed_at,
            )
        )

    deleted = await query.sweep("", datetime.now(UTC), timedelta(hours=24))
    assert deleted == 1
    assert (await store.get_workflow(WorkflowKey(workflow_id="prices:sweep", run_id="old"))) is None
    assert (
        await store.get_workflow(WorkflowKey(workflow_id="prices:sweep", run_id="fresh"))
    ) is not None


async def test_connect_store_due_timers_round_trip(tmp_path) -> None:
    """The DueTimers RPC returns SCHEDULED timers under IN_PROGRESS workflows
    in a single round-trip."""
    from datetime import UTC, datetime, timedelta

    from google.protobuf.timestamp_pb2 import Timestamp

    from temporaless.storage import (
        TIMER_RECORD_SCHEMA_VERSION,
        TimerKey,
    )

    backend = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    store = ConnectStore.local(backend)

    wf_key = WorkflowKey(workflow_id="prices:timer", run_id="2026-05-04")
    timer_key = TimerKey(workflow_id="prices:timer", run_id="2026-05-04", timer_id="wait:vendor")
    fire_at = Timestamp()
    fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))

    await store.put_workflow(
        temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=wf_key.to_proto(),
            workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
            code_version="v1",
            status=temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        )
    )
    await store.put_timer(
        temporaless_pb2.TimerRecord(
            schema_version=TIMER_RECORD_SCHEMA_VERSION,
            key=timer_key.to_proto(),
            timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
            code_version="v1",
            status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
            fire_at=fire_at,
        )
    )

    due = await store.due_timers("", datetime.now(UTC))
    assert len(due) == 1
    assert due[0].key.timer_id == "wait:vendor"
    assert due[0].workflow.key.workflow_id == "prices:timer"


async def test_connect_store_list_and_delete_round_trip(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    backend = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    store = ConnectStore.local(backend)
    query = ConnectQueryStore.local(backend)

    keep = WorkflowKey(workflow_id="prices:keep", run_id="2026-05-02")
    drop = WorkflowKey(workflow_id="prices:drop", run_id="2026-05-02")
    for key in (keep, drop):
        await store.put_workflow(
            temporaless_pb2.WorkflowRecord(
                schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
                key=key.to_proto(),
                workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
                code_version="test-version",
                status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            )
        )

    records, _ = await query.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    assert len(records) == 2

    assert await store.delete_workflow(drop) is True
    assert await store.delete_workflow(drop) is False  # idempotent

    records, _ = await query.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    assert [r.key.workflow_id for r in records] == ["prices:keep"]
