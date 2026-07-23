from __future__ import annotations

import opendal
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.connectstore import ConnectStore, LocalRecordStoreClient, RecordStoreService
from temporaless.storage import (
    CLAIM_RECORD_SCHEMA_VERSION,
    ClaimKey,
    OpenDALStore,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ClaimCapabilityError,
    Options,
    Workflow,
    run,
)


class _CapabilityStore(OpenDALStore):
    def __init__(self, operator: opendal.AsyncOperator, capability: int) -> None:
        super().__init__(operator)
        self.capability = capability
        self.capability_calls = 0
        self.create_calls = 0
        self.fail_capability = False

    async def claim_capability(self) -> temporaless_pb2.ClaimCapability:
        self.capability_calls += 1
        if self.fail_capability:
            raise AssertionError("terminal replay queried claim capability")
        return self.capability

    async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
        self.create_calls += 1
        raise AssertionError(f"unsupported capability attempted claim create: {record}")


@pytest.mark.parametrize(
    ("options", "capability", "want_option"),
    [
        pytest.param(
            Options(
                workflow_id="capability:owner",
                run_id="run",
                claim_owner_id="worker",
            ),
            temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS,
            "claim_owner_id",
            id="claim_owner_no_claims",
        ),
        pytest.param(
            Options(
                workflow_id="capability:concurrency",
                run_id="run",
                claim_owner_id="worker",
                concurrency_key="vendor",
                concurrency_limit=1,
            ),
            temporaless_pb2.CLAIM_CAPABILITY_UNSPECIFIED,
            "concurrency_key",
            id="concurrency_unspecified",
        ),
        pytest.param(
            Options(
                workflow_id="capability:cas",
                run_id="run",
                claim_owner_id="worker",
            ),
            temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS,
            "claim_owner_id",
            id="claim_owner_reserved_cas",
        ),
    ],
)
async def test_run_rejects_unsupported_claim_capability_before_writing(
    tmp_path,
    options: Options,
    capability: temporaless_pb2.ClaimCapability,
    want_option: str,
) -> None:
    store = _CapabilityStore(opendal.AsyncOperator("fs", root=str(tmp_path)), capability)
    body_called = False

    async def body(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal body_called
        body_called = True
        return StringValue(value="unexpected")

    with pytest.raises(ClaimCapabilityError) as exc_info:
        await run(store, options, StringValue(value="request"), StringValue, body)

    assert exc_info.value.capability == capability
    assert exc_info.value.option == want_option
    assert body_called is False
    assert store.create_calls == 0
    assert (
        await store.get_workflow(
            WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
        )
        is None
    )


async def test_terminal_replay_does_not_require_claim_capability(tmp_path) -> None:
    store = _CapabilityStore(
        opendal.AsyncOperator("fs", root=str(tmp_path)),
        temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS,
    )
    base = Options(workflow_id="capability:replay", run_id="run")

    async def body(_workflow: Workflow, _request: StringValue) -> StringValue:
        return StringValue(value="stored")

    first = await run(store, base, StringValue(value="request"), StringValue, body)
    assert first.value == "stored"
    store.fail_capability = True

    async def replay_body(_workflow: Workflow, _request: StringValue) -> StringValue:
        raise AssertionError("terminal replay executed workflow body")

    replayed = await run(
        store,
        Options(
            workflow_id=base.workflow_id,
            run_id=base.run_id,
            claim_owner_id="worker",
        ),
        StringValue(value="request"),
        StringValue,
        replay_body,
    )
    assert replayed.value == "stored"
    assert store.capability_calls == 0


async def test_direct_workflow_activity_rejects_unsupported_capability(tmp_path) -> None:
    store = _CapabilityStore(
        opendal.AsyncOperator("fs", root=str(tmp_path)),
        temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS,
    )
    workflow = Workflow(
        store,
        Options(
            workflow_id="capability:activity",
            run_id="run",
            claim_owner_id="worker",
        ),
    )

    async def activity() -> StringValue:
        return StringValue(value="unexpected")

    with pytest.raises(ClaimCapabilityError):
        await workflow.run_activity(
            "fetch",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="request"),
            StringValue,
            activity,
        )
    assert store.create_calls == 0


async def test_connect_store_no_claim_backend_fails_with_core_typed_error() -> None:
    class RecordOnlyStore:
        async def get_workflow(self, _key: WorkflowKey):
            return None

    remote = ConnectStore.local(RecordOnlyStore())  # type: ignore[arg-type]

    async def body(_workflow: Workflow, _request: StringValue) -> StringValue:
        raise AssertionError("workflow body executed without claim capability")

    with pytest.raises(ClaimCapabilityError):
        await run(
            remote,
            Options(
                workflow_id="capability:remote",
                run_id="run",
                claim_owner_id="worker",
            ),
            StringValue(value="request"),
            StringValue,
            body,
        )


@pytest.mark.parametrize(
    ("capability", "want"),
    [
        pytest.param(
            temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS,
            temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS,
            id="no_claims",
        ),
        pytest.param(
            temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS,
            temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS,
            id="create_only",
        ),
        pytest.param(
            temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS,
            None,
            id="reserved_cas",
        ),
    ],
)
async def test_connect_store_exposes_only_current_claim_capabilities(
    tmp_path,
    capability: temporaless_pb2.ClaimCapability,
    want: temporaless_pb2.ClaimCapability | None,
) -> None:
    backend = _CapabilityStore(
        opendal.AsyncOperator("fs", root=str(tmp_path)),
        capability,
    )
    service = RecordStoreService(backend)
    store = ConnectStore(LocalRecordStoreClient(service))

    if want is None:
        with pytest.raises(ConnectError) as service_error:
            await service.get_store_capabilities(
                temporaless_pb2.GetStoreCapabilitiesRequest(),
                None,
            )
        assert service_error.value.code is Code.FAILED_PRECONDITION

        with pytest.raises(ConnectError) as client_error:
            await store.claim_capability()
        assert client_error.value.code is Code.FAILED_PRECONDITION
        return

    response = await service.get_store_capabilities(
        temporaless_pb2.GetStoreCapabilitiesRequest(),
        None,
    )
    assert response.claim_capability == want
    assert await store.claim_capability() == want


async def test_connect_delete_claim_rejects_reserved_cas_before_mutation(
    tmp_path,
) -> None:
    backend = _CapabilityStore(
        opendal.AsyncOperator("fs", root=str(tmp_path)),
        temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS,
    )
    key = ClaimKey(
        workflow_id="prices:cas",
        run_id="run",
        claim_id="workflow:execution",
    )
    record = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=key.to_proto(),
        owner_id="worker",
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
        resource_id=key.workflow_id,
    )
    assert await OpenDALStore.try_create_claim(backend, record)

    service = RecordStoreService(backend)
    with pytest.raises(ConnectError) as service_error:
        await service.delete_claim(
            temporaless_pb2.DeleteClaimRequest(key=key.to_proto()),
            None,
        )
    assert service_error.value.code is Code.FAILED_PRECONDITION
    assert await backend.get_claim(key) == record

    store = ConnectStore(LocalRecordStoreClient(service))
    with pytest.raises(ConnectError) as client_error:
        await store.delete_claim(key)
    assert client_error.value.code is Code.FAILED_PRECONDITION
    assert await backend.get_claim(key) == record
