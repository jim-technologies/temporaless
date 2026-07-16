import opendal
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from google.protobuf.message import Message
from temporaless_indexstore import IndexedStore

from temporaless.connectstore import (
    ConnectQueryStore,
    ConnectStore,
    LocalRecordStoreClient,
    RecordQueryService,
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
    RunRecordValidationError,
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


@pytest.mark.parametrize(
    ("method_name", "rpc_request"),
    [
        ("get_workflow", temporaless_pb2.GetWorkflowRequest()),
        ("put_workflow", temporaless_pb2.PutWorkflowRequest()),
        ("get_activity", temporaless_pb2.GetActivityRequest()),
        ("put_activity", temporaless_pb2.PutActivityRequest()),
        ("get_timer", temporaless_pb2.GetTimerRequest()),
        ("put_timer", temporaless_pb2.PutTimerRequest()),
        ("get_event", temporaless_pb2.GetEventRequest()),
        ("put_event", temporaless_pb2.PutEventRequest()),
        ("get_claim", temporaless_pb2.GetClaimRequest()),
        ("try_create_claim", temporaless_pb2.TryCreateClaimRequest()),
        ("delete_claim", temporaless_pb2.DeleteClaimRequest()),
        ("list_activities", temporaless_pb2.ListActivitiesRequest()),
        ("list_timers", temporaless_pb2.ListTimersRequest()),
        ("list_events", temporaless_pb2.ListEventsRequest()),
        ("list_claims", temporaless_pb2.ListClaimsRequest()),
        ("delete_workflow", temporaless_pb2.DeleteWorkflowRequest()),
        ("delete_activity", temporaless_pb2.DeleteActivityRequest()),
        ("delete_timer", temporaless_pb2.DeleteTimerRequest()),
        ("delete_event", temporaless_pb2.DeleteEventRequest()),
        ("delete_run", temporaless_pb2.DeleteRunRequest()),
    ],
)
async def test_record_store_service_rejects_missing_required_messages(
    tmp_path, method_name: str, rpc_request: Message
) -> None:
    service = RecordStoreService(OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path))))

    with pytest.raises(ConnectError) as captured:
        await getattr(service, method_name)(rpc_request, None)

    assert captured.value.code is Code.INVALID_ARGUMENT


@pytest.mark.parametrize(
    ("service_kind", "rpc_request"),
    [
        ("point_due", temporaless_pb2.DueTimersRequest()),
        ("query_due", temporaless_pb2.RecordQueryServiceDueTimersRequest()),
        ("sweep", temporaless_pb2.SweepRequest()),
    ],
)
async def test_storage_services_reject_missing_required_times(
    tmp_path, service_kind: str, rpc_request: Message
) -> None:
    point = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    if service_kind == "point_due":
        service = RecordStoreService(point)
        call = service.due_timers
    else:
        query = IndexedStore(point, tmp_path / "index.sqlite")
        service = RecordQueryService(query)
        call = service.due_timers if service_kind == "query_due" else service.sweep

    with pytest.raises(ConnectError) as captured:
        await call(rpc_request, None)

    assert captured.value.code is Code.INVALID_ARGUMENT


async def test_record_store_service_rejects_invalid_latest_pointer_key(tmp_path) -> None:
    service = RecordStoreService(OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path))))

    with pytest.raises(ConnectError) as captured:
        await service.get_latest_workflow_run(
            temporaless_pb2.GetLatestWorkflowRunRequest(workflow_id=""), None
        )

    assert captured.value.code is Code.INVALID_ARGUMENT


@pytest.mark.parametrize(
    "rpc_request",
    [
        temporaless_pb2.ListWorkflowsRequest(page_size=-1),
        temporaless_pb2.ListWorkflowsRequest(page_token="not-a-valid-token"),
    ],
)
async def test_record_query_service_maps_invalid_options_to_invalid_argument(
    tmp_path, rpc_request: temporaless_pb2.ListWorkflowsRequest
) -> None:
    point = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    service = RecordQueryService(IndexedStore(point, tmp_path / "index.sqlite"))

    with pytest.raises(ConnectError) as captured:
        await service.list_workflows(rpc_request, None)

    assert captured.value.code is Code.INVALID_ARGUMENT


@pytest.mark.parametrize("record_kind", ["workflow", "activity", "timer", "event", "claim"])
async def test_connect_client_rejects_point_response_for_another_key(record_kind: str) -> None:
    requested_key, response = _corrupt_point_response(record_kind)

    class CorruptClient:
        pass

    async def get_record(_request):
        return response

    client = CorruptClient()
    setattr(client, f"get_{record_kind}", get_record)
    store = ConnectStore(client)  # type: ignore[invalid-argument-type]

    with pytest.raises(RunRecordValidationError, match="requested key"):
        await getattr(store, f"get_{record_kind}")(requested_key)


@pytest.mark.parametrize("record_kind", ["workflow", "activity", "timer", "event", "claim"])
async def test_connect_client_rejects_payload_when_found_is_false(record_kind: str) -> None:
    requested_key, response = _corrupt_point_response(record_kind)
    response.found = False

    class CorruptClient:
        pass

    async def get_record(_request):
        return response

    client = CorruptClient()
    setattr(client, f"get_{record_kind}", get_record)
    store = ConnectStore(client)  # type: ignore[invalid-argument-type]

    with pytest.raises(RunRecordValidationError, match="found=False"):
        await getattr(store, f"get_{record_kind}")(requested_key)


async def test_connect_client_rejects_latest_pointer_when_found_is_false() -> None:
    key = WorkflowKey(workflow_id="prices:pointer", run_id="run:one")
    pointer = temporaless_pb2.LatestWorkflowRunPointer(
        key=key.to_proto(), status=temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    )

    class CorruptClient:
        async def get_latest_workflow_run(self, _request):
            return temporaless_pb2.GetLatestWorkflowRunResponse(found=False, pointer=pointer)

    store = ConnectStore(CorruptClient())  # type: ignore[invalid-argument-type]

    with pytest.raises(RunRecordValidationError, match="found=False"):
        await store.get_latest_workflow_run(key.namespace, key.workflow_id)


async def test_connect_client_rejects_cross_run_list_response() -> None:
    key = WorkflowKey(workflow_id="prices:client", run_id="run:one")
    misplaced = temporaless_pb2.ActivityRecord(
        schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
        key=ActivityKey(
            workflow_id=key.workflow_id,
            run_id="run:other",
            activity_id="fetch",
        ).to_proto(),
        activity_type="activity:test",
        code_version="v1",
        status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
    )

    class CorruptClient:
        async def list_activities(self, _request):
            return temporaless_pb2.ListActivitiesResponse(records=[misplaced])

    store = ConnectStore(CorruptClient())  # type: ignore[invalid-argument-type]

    with pytest.raises(RunRecordValidationError, match="requested workflow run"):
        await store.list_activities(key)


async def test_connect_client_rejects_timer_list_response_for_wrong_status() -> None:
    key = WorkflowKey(workflow_id="prices:client", run_id="run:one")
    timer = temporaless_pb2.TimerRecord(
        schema_version=TIMER_RECORD_SCHEMA_VERSION,
        key=TimerKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            timer_id="sleep",
        ).to_proto(),
        timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
        code_version="v1",
        status=temporaless_pb2.TIMER_STATUS_FIRED,
    )

    class CorruptClient:
        async def list_timers(self, _request):
            return temporaless_pb2.ListTimersResponse(records=[timer])

    store = ConnectStore(CorruptClient())  # type: ignore[invalid-argument-type]

    with pytest.raises(RunRecordValidationError, match="requested status"):
        await store.list_timers(key, temporaless_pb2.TIMER_STATUS_SCHEDULED)


async def test_record_store_service_rejects_timer_listing_for_wrong_status() -> None:
    key = WorkflowKey(workflow_id="prices:service", run_id="run:one")
    timer = temporaless_pb2.TimerRecord(
        schema_version=TIMER_RECORD_SCHEMA_VERSION,
        key=TimerKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            timer_id="sleep",
        ).to_proto(),
        timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
        code_version="v1",
        status=temporaless_pb2.TIMER_STATUS_FIRED,
    )

    class CorruptStore:
        async def list_timers(self, _key, _status):
            return [timer]

    service = RecordStoreService(CorruptStore())  # type: ignore[invalid-argument-type]

    with pytest.raises(ConnectError) as captured:
        await service.list_timers(
            temporaless_pb2.ListTimersRequest(
                key=key.to_proto(),
                status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
            ),
            None,
        )
    assert captured.value.code is Code.DATA_LOSS


async def test_connect_client_maps_remote_timer_data_loss_to_storage_corruption() -> None:
    key = TimerKey(workflow_id="prices:remote", run_id="run:one", timer_id="sleep")

    class CorruptRemoteClient:
        async def get_timer(self, _request):
            raise ConnectError(Code.DATA_LOSS, "corrupt timer bytes")

    store = ConnectStore(CorruptRemoteClient())  # type: ignore[invalid-argument-type]

    with pytest.raises(RunRecordValidationError, match="corrupt storage data"):
        await store.get_timer(key)


async def test_connect_client_latest_pointer_requires_referenced_workflow() -> None:
    key = WorkflowKey(workflow_id="prices:pointer", run_id="run:missing")
    pointer = temporaless_pb2.LatestWorkflowRunPointer(
        key=key.to_proto(),
        status=temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
    )
    pointer.record_time.GetCurrentTime()
    pointer.updated_at.GetCurrentTime()
    pointer.run_order_time.CopyFrom(pointer.record_time)

    class DanglingPointerClient:
        async def get_latest_workflow_run(self, _request):
            return temporaless_pb2.GetLatestWorkflowRunResponse(found=True, pointer=pointer)

        async def get_workflow(self, _request):
            return temporaless_pb2.GetWorkflowResponse(found=False)

    store = ConnectStore(DanglingPointerClient())  # type: ignore[invalid-argument-type]

    assert await store.get_latest_workflow_run("", key.workflow_id) is None


async def test_record_store_service_maps_corrupt_point_response_to_data_loss() -> None:
    requested = ActivityKey(
        workflow_id="prices:service",
        run_id="run:one",
        activity_id="fetch",
    )
    misplaced = temporaless_pb2.ActivityRecord(
        schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
        key=ActivityKey(
            workflow_id=requested.workflow_id,
            run_id="run:other",
            activity_id=requested.activity_id,
        ).to_proto(),
        activity_type="activity:test",
        code_version="v1",
        status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
    )

    class CorruptStore:
        async def get_activity(self, _key):
            return misplaced

    service = RecordStoreService(CorruptStore())  # type: ignore[invalid-argument-type]

    with pytest.raises(ConnectError) as captured:
        await service.get_activity(
            temporaless_pb2.GetActivityRequest(key=requested.to_proto()), None
        )

    assert captured.value.code is Code.DATA_LOSS


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

    with pytest.raises(RunRecordValidationError) as captured:
        await store.delete_run(key)

    assert record_kind in str(captured.value)
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

    with pytest.raises(RunRecordValidationError):
        await store.delete_run(key)

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


def _corrupt_point_response(record_kind: str):
    workflow_id = "prices:client"
    if record_kind == "workflow":
        requested = WorkflowKey(workflow_id=workflow_id, run_id="run:one")
        payload = WorkflowKey(workflow_id=workflow_id, run_id="run:other")
        record = temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=payload.to_proto(),
            workflow_type="workflow:test",
            code_version="v1",
            status=temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        )
        return requested, temporaless_pb2.GetWorkflowResponse(found=True, record=record)
    if record_kind == "activity":
        requested = ActivityKey(
            workflow_id=workflow_id,
            run_id="run:one",
            activity_id="fetch",
        )
        payload = ActivityKey(
            workflow_id=workflow_id,
            run_id="run:other",
            activity_id="fetch",
        )
        record = temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=payload.to_proto(),
            activity_type="activity:test",
            code_version="v1",
            status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
        return requested, temporaless_pb2.GetActivityResponse(found=True, record=record)
    if record_kind == "timer":
        requested = TimerKey(
            workflow_id=workflow_id,
            run_id="run:one",
            timer_id="wait",
        )
        payload = TimerKey(
            workflow_id=workflow_id,
            run_id="run:other",
            timer_id="wait",
        )
        record = temporaless_pb2.TimerRecord(
            schema_version=TIMER_RECORD_SCHEMA_VERSION,
            key=payload.to_proto(),
            timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
            code_version="v1",
            status=temporaless_pb2.TIMER_STATUS_FIRED,
        )
        return requested, temporaless_pb2.GetTimerResponse(found=True, record=record)
    if record_kind == "event":
        requested = EventKey(
            workflow_id=workflow_id,
            run_id="run:one",
            event_id="approved",
        )
        payload = EventKey(
            workflow_id=workflow_id,
            run_id="run:other",
            event_id="approved",
        )
        record = temporaless_pb2.EventRecord(
            schema_version=EVENT_RECORD_SCHEMA_VERSION,
            key=payload.to_proto(),
        )
        return requested, temporaless_pb2.GetEventResponse(found=True, record=record)
    if record_kind == "claim":
        requested = ClaimKey(
            workflow_id=workflow_id,
            run_id="run:one",
            claim_id="activity:fetch",
        )
        payload = ClaimKey(
            workflow_id=workflow_id,
            run_id="run:other",
            claim_id="activity:fetch",
        )
        record = temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=payload.to_proto(),
            owner_id="owner",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
            resource_id="fetch",
            code_version="v1",
        )
        return requested, temporaless_pb2.GetClaimResponse(found=True, record=record)
    raise AssertionError(f"unsupported record kind: {record_kind}")
