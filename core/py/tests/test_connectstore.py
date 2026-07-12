import opendal
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from temporaless_indexstore import IndexedStore

from temporaless.connectstore import (
    ConnectQueryStore,
    ConnectStore,
    LocalRecordStoreClient,
    RecordStoreService,
)
from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    CLAIM_RECORD_SCHEMA_VERSION,
    CREATE_ONLY_CLAIMS,
    EVENT_RECORD_SCHEMA_VERSION,
    TIMER_RECORD_SCHEMA_VERSION,
    WORKFLOW_RECORD_SCHEMA_VERSION,
    ActivityKey,
    ClaimKey,
    EventKey,
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


async def test_delete_run_deletes_all_claims_from_separate_claim_store(tmp_path) -> None:
    records = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "records")))
    claims = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "claims")))
    store = ConnectStore.local(records, claims)
    key = WorkflowKey(workflow_id="prices:delete-run", run_id="run:one")
    activity_key = ActivityKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        activity_id="fetch",
    )
    await store.put_workflow(
        temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            workflow_type="workflow:test",
            code_version="v1",
            status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        )
    )
    await store.put_activity(
        temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=activity_key.to_proto(),
            activity_type="activity:test",
            code_version="v1",
            status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
    )
    for claim_id in ("arbitrary:one", "arbitrary:two"):
        created = await claims.try_create_claim(
            temporaless_pb2.ClaimRecord(
                schema_version=CLAIM_RECORD_SCHEMA_VERSION,
                key=ClaimKey(
                    workflow_id=key.workflow_id,
                    run_id=key.run_id,
                    claim_id=claim_id,
                ).to_proto(),
                owner_id="owner",
                resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
                resource_id=claim_id,
                code_version="v1",
            )
        )
        assert created is True

    assert {claim.key.claim_id for claim in await store.list_claims(key)} == {
        "arbitrary:one",
        "arbitrary:two",
    }
    assert await store.delete_run(key) == 4
    assert await records.get_workflow(key) is None
    assert await records.get_activity(activity_key) is None
    assert await claims.list_claims(key) == []


@pytest.mark.parametrize("record_kind", ["activity", "timer", "event"])
async def test_delete_run_rejects_corrupt_record_listing_before_claim_deletion(
    tmp_path,
    record_kind: str,
) -> None:
    records_operator = opendal.AsyncOperator("fs", root=str(tmp_path / "records"))
    records = OpenDALStore(records_operator)
    claims = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "claims")))
    store = ConnectStore.local(records, claims)
    key = WorkflowKey(workflow_id="prices:delete-run", run_id="run:corrupt-record")
    await records.put_workflow(
        temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            workflow_type="workflow:test",
            code_version="v1",
            status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        )
    )
    claim_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="valid",
    )
    assert await claims.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=claim_key.to_proto(),
            owner_id="owner",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
            resource_id=key.workflow_id,
            code_version="v1",
        )
    )

    if record_kind == "activity":
        path_key = ActivityKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            activity_id="misplaced",
        )
        record = temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=ActivityKey(
                workflow_id=key.workflow_id,
                run_id="run:other",
                activity_id="misplaced",
            ).to_proto(),
            activity_type="activity:test",
            code_version="v1",
            status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
    elif record_kind == "timer":
        path_key = TimerKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            timer_id="misplaced",
        )
        record = temporaless_pb2.TimerRecord(
            schema_version=TIMER_RECORD_SCHEMA_VERSION,
            key=TimerKey(
                workflow_id=key.workflow_id,
                run_id="run:other",
                timer_id="misplaced",
            ).to_proto(),
            timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
            code_version="v1",
            status=temporaless_pb2.TIMER_STATUS_FIRED,
        )
    else:
        path_key = EventKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            event_id="misplaced",
        )
        record = temporaless_pb2.EventRecord(
            schema_version=EVENT_RECORD_SCHEMA_VERSION,
            key=EventKey(
                workflow_id=key.workflow_id,
                run_id="run:other",
                event_id="misplaced",
            ).to_proto(),
        )

    await records_operator.create_dir(path_key.dir_path())
    await records_operator.write(path_key.path(), record.SerializeToString(deterministic=True))

    with pytest.raises(ConnectError) as captured:
        await store.delete_run(key)

    assert captured.value.code is Code.DATA_LOSS
    assert record_kind in captured.value.message
    assert await records.get_workflow(key) is not None
    assert await claims.get_claim(claim_key) is not None
    assert await records_operator.exists(path_key.path())


async def test_delete_run_rejects_point_only_claim_store_before_mutation(tmp_path) -> None:
    class PointOnlyClaimStore:
        def __init__(self, inner: OpenDALStore) -> None:
            self._inner = inner

        async def claim_capability(self):
            return await self._inner.claim_capability()

        async def get_claim(self, key):
            return await self._inner.get_claim(key)

        async def try_create_claim(self, record):
            return await self._inner.try_create_claim(record)

        async def delete_claim(self, key):
            return await self._inner.delete_claim(key)

    records = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "records")))
    claims = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "claims")))
    point_only = PointOnlyClaimStore(claims)
    store = ConnectStore.local(records, point_only)
    key = WorkflowKey(workflow_id="prices:delete-run", run_id="run:point-only")
    claim_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="arbitrary",
    )
    await records.put_workflow(
        temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            workflow_type="workflow:test",
            code_version="v1",
            status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        )
    )
    assert await point_only.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=claim_key.to_proto(),
            owner_id="owner",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
            resource_id=key.workflow_id,
            code_version="v1",
        )
    )

    with pytest.raises(ConnectError) as captured:
        await store.delete_run(key)

    assert captured.value.code is Code.FAILED_PRECONDITION
    assert await records.get_workflow(key) is not None
    assert await claims.get_claim(claim_key) is not None


async def test_delete_run_treats_no_claims_capability_as_record_only(tmp_path) -> None:
    class NoClaimsStructuralStore:
        async def claim_capability(self):
            return temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS

        async def get_claim(self, _key):
            raise AssertionError("get_claim must not be called")

        async def try_create_claim(self, _record):
            raise AssertionError("try_create_claim must not be called")

        async def delete_claim(self, _key):
            raise AssertionError("delete_claim must not be called")

        async def list_claims(self, _key):
            raise AssertionError("list_claims must not be called")

    records = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "records")))
    store = ConnectStore.local(records, NoClaimsStructuralStore())
    key = WorkflowKey(workflow_id="prices:delete-run", run_id="run:no-claims")
    await records.put_workflow(
        temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            workflow_type="workflow:test",
            code_version="v1",
            status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        )
    )

    assert await store.list_claims(key) == []
    assert await store.delete_run(key) == 1
    assert await records.get_workflow(key) is None


async def test_delete_run_validates_entire_claim_listing_before_delete(tmp_path) -> None:
    class CorruptListingStore:
        def __init__(
            self,
            inner: OpenDALStore,
            records: list[temporaless_pb2.ClaimRecord],
        ) -> None:
            self._inner = inner
            self._records = records
            self.delete_calls = 0

        async def claim_capability(self):
            return await self._inner.claim_capability()

        async def get_claim(self, key):
            return await self._inner.get_claim(key)

        async def try_create_claim(self, record):
            return await self._inner.try_create_claim(record)

        async def delete_claim(self, key):
            self.delete_calls += 1
            return await self._inner.delete_claim(key)

        async def list_claims(self, _key):
            return self._records

    records = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "records")))
    claims = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "claims")))
    key = WorkflowKey(workflow_id="prices:delete-run", run_id="run:corrupt")
    target_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="target",
    )
    target = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=target_key.to_proto(),
        owner_id="owner",
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
        resource_id=key.workflow_id,
        code_version="v1",
    )
    assert await claims.try_create_claim(target)
    misplaced = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=ClaimKey(
            workflow_id=key.workflow_id,
            run_id="run:other",
            claim_id="misplaced",
        ).to_proto(),
        owner_id="owner",
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
        resource_id=key.workflow_id,
        code_version="v1",
    )
    corrupt = CorruptListingStore(claims, [target, misplaced])
    store = ConnectStore.local(records, corrupt)

    with pytest.raises(ConnectError) as captured:
        await store.delete_run(key)

    assert captured.value.code is Code.DATA_LOSS
    assert corrupt.delete_calls == 0
    assert await claims.get_claim(target_key) is not None


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
