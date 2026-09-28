import json
from pathlib import Path

import opendal
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from google.protobuf import json_format
from google.protobuf.duration_pb2 import Duration
from google.protobuf.wrappers_pb2 import StringValue
from protovalidate import ValidationError, validate

from temporaless.connectstore import ConnectStore, RecordStoreService
from temporaless.execution import FencedExecutionUnsupportedError
from temporaless.storage import OpenDALStore
from temporaless.v1 import temporaless_pb2 as pb
from temporaless.workflow import Options, run

CASES = json.loads(
    (Path(__file__).resolve().parents[3] / "testdata/fenced-execution-contract.json").read_text()
)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_shared_fenced_contract(case):
    message = json_format.ParseDict(case["message"], getattr(pb, case["type"])())
    if case["valid"]:
        validate(message)
    else:
        with pytest.raises(ValidationError):
            validate(message)


@pytest.mark.parametrize("claims", [False, True])
@pytest.mark.parametrize("empty", [False, True])
async def test_runtime_refuses_fencing_without_entering_store_or_body(tmp_path, claims, empty):
    class UnreadableStore(OpenDALStore):
        async def get_workflow(self, key):
            raise AssertionError("unsupported fencing entered the store")

    store = UnreadableStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    options = Options(
        workflow_id="workflow", run_id="run", fenced_execution=pb.FencedExecutionOptions()
    )
    if not empty:
        options.fenced_execution.CopyFrom(
            pb.FencedExecutionOptions(
                owner_id="worker", acquisition_id="invocation", lease_duration=Duration(seconds=30)
            )
        )
    if claims:
        options.claim_owner_id = "worker"

    async def body(_workflow, _request):
        raise AssertionError("unsupported fencing entered the body")

    with pytest.raises(FencedExecutionUnsupportedError):
        await run(store, options, StringValue(), StringValue, body)
    assert list(tmp_path.rglob("*.binpb")) == []


@pytest.mark.parametrize(
    "method,request_type",
    [
        ("acquire_execution", pb.AcquireExecutionRequest),
        ("renew_execution", pb.RenewExecutionRequest),
        ("release_execution", pb.ReleaseExecutionRequest),
        ("apply_execution_mutations", pb.ApplyExecutionMutationsRequest),
        ("get_execution_operation", pb.GetExecutionOperationRequest),
    ],
)
async def test_reserved_rpc_never_falls_through_to_legacy_store(tmp_path, method, request_type):
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    service = RecordStoreService(store)
    response = await service.get_store_capabilities(pb.GetStoreCapabilitiesRequest(), None)
    assert response.fenced_execution_capability == pb.FENCED_EXECUTION_CAPABILITY_UNSUPPORTED
    assert response.fenced_execution_store_incarnation == ""
    with pytest.raises(ConnectError) as captured:
        await getattr(service, method)(request_type(), None)
    assert captured.value.code == Code.UNIMPLEMENTED
    assert list(tmp_path.rglob("*.binpb")) == []


@pytest.mark.parametrize("capability", [0, 1, 2, 99])
async def test_remote_capability_never_silently_enables_fencing(capability):
    class Client:
        async def get_store_capabilities(self, _request):
            return pb.GetStoreCapabilitiesResponse(fenced_execution_capability=capability)

    store = ConnectStore(Client())
    if capability in (0, 1):
        assert (
            await store.fenced_execution_capability() == pb.FENCED_EXECUTION_CAPABILITY_UNSUPPORTED
        )
    else:
        with pytest.raises(FencedExecutionUnsupportedError):
            await store.fenced_execution_capability()
