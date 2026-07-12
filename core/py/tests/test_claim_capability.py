from __future__ import annotations

import opendal
import pytest
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.connectstore import ConnectStore
from temporaless.storage import OpenDALStore, WorkflowKey
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
                code_version="v1",
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
                code_version="v1",
                claim_owner_id="worker",
                concurrency_key="vendor",
                concurrency_limit=1,
            ),
            temporaless_pb2.CLAIM_CAPABILITY_UNSPECIFIED,
            "concurrency_key",
            id="concurrency_unspecified",
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
    base = Options(workflow_id="capability:replay", run_id="run", code_version="v1")

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
            code_version=base.code_version,
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
            code_version="v1",
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
                code_version="v1",
                claim_owner_id="worker",
            ),
            StringValue(value="request"),
            StringValue,
            body,
        )
