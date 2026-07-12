"""Storage-backed single-flight behavior for live workflow invocations."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.any_pb2 import Any
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.storage import (
    CLAIM_RECORD_SCHEMA_VERSION,
    CREATE_ONLY_CLAIMS,
    ClaimKey,
    OpenDALStore,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    WORKFLOW_EXECUTION_CLAIM_ID,
    ClaimBusyError,
    ClaimReleaseError,
    EventPendingError,
    Options,
    TimerPendingError,
    Workflow,
    WorkflowDependencyPendingError,
    run,
)


@pytest.fixture
def store(tmp_path) -> OpenDALStore:
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


def _options(*, run_id: str, owner_id: str) -> Options:
    return Options(
        workflow_id="prices:singleflight",
        run_id=run_id,
        code_version="test",
        claim_owner_id=owner_id,
    )


def _claim_key(options: Options) -> ClaimKey:
    return ClaimKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        claim_id=WORKFLOW_EXECUTION_CLAIM_ID,
    )


async def _put_workflow_claim(
    store: OpenDALStore,
    options: Options,
    *,
    owner_id: str,
    expires_delta: timedelta = timedelta(minutes=15),
) -> temporaless_pb2.ClaimRecord:
    now = datetime.now(UTC)
    created_at = Timestamp()
    created_at.FromDatetime(now)
    expires_at = Timestamp()
    expires_at.FromDatetime(now + expires_delta)
    record = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=_claim_key(options).to_proto(),
        owner_id=owner_id,
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
        resource_id=options.workflow_id,
        code_version=options.code_version,
        lease_expires_at=expires_at,
        created_at=created_at,
        heartbeat_at=created_at,
    )
    assert await store.try_create_claim(record) is True
    return record


@pytest.mark.parametrize(
    "duplicate_owner",
    [
        pytest.param("first-owner", id="same-owner"),
        pytest.param("second-owner", id="distinct-owner"),
    ],
)
async def test_live_duplicate_is_busy_even_for_same_owner(
    store: OpenDALStore,
    duplicate_owner: str,
) -> None:
    options = _options(run_id=f"live-{duplicate_owner}", owner_id="first-owner")
    duplicate_options = _options(run_id=options.run_id, owner_id=duplicate_owner)
    entered = asyncio.Event()
    release = asyncio.Event()
    duplicate_entries = 0

    async def first_body(_workflow: Workflow, request: StringValue) -> StringValue:
        entered.set()
        await release.wait()
        return StringValue(value=f"done:{request.value}")

    async def duplicate_body(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal duplicate_entries
        duplicate_entries += 1
        return StringValue(value="duplicate-ran")

    first = asyncio.create_task(
        run(store, options, StringValue(value="AAPL"), StringValue, first_body)
    )
    await asyncio.wait_for(entered.wait(), timeout=1)

    claim_key = _claim_key(options)
    claim = await store.get_claim(claim_key)
    assert claim is not None
    assert claim.schema_version == temporaless_pb2.RECORD_SCHEMA_VERSION_CLAIM
    assert claim.key == claim_key.to_proto()
    assert claim.owner_id == "first-owner"
    assert claim.resource_type == temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW
    assert claim.resource_id == options.workflow_id
    assert claim.code_version == "test"
    assert claim.HasField("created_at")
    assert claim.HasField("heartbeat_at")
    assert claim.HasField("lease_expires_at")
    assert claim.lease_expires_at.ToDatetime() > claim.created_at.ToDatetime()

    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS

    try:
        with pytest.raises(ClaimBusyError) as exc_info:
            await run(
                store,
                duplicate_options,
                StringValue(value="AAPL"),
                StringValue,
                duplicate_body,
            )
        assert exc_info.value.claim_id == WORKFLOW_EXECUTION_CLAIM_ID
        assert exc_info.value.owner_id == "first-owner"
        assert exc_info.value.capability == CREATE_ONLY_CLAIMS
        assert duplicate_entries == 0
    finally:
        release.set()

    result = await first
    assert result.value == "done:AAPL"
    assert await store.get_claim(claim_key) is None


async def test_terminal_replay_ignores_stale_workflow_claim(store: OpenDALStore) -> None:
    options = _options(run_id="terminal-replay", owner_id="first-owner")

    async def first_body(_workflow: Workflow, request: StringValue) -> StringValue:
        return StringValue(value=f"stored:{request.value}")

    first = await run(store, options, StringValue(value="AAPL"), StringValue, first_body)
    assert first.value == "stored:AAPL"
    assert await store.get_claim(_claim_key(options)) is None

    stale = await _put_workflow_claim(store, options, owner_id="stale-owner")
    replay_entries = 0

    async def replay_body(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal replay_entries
        replay_entries += 1
        return StringValue(value="should-not-run")

    replayed = await run(
        store,
        _options(run_id=options.run_id, owner_id="second-owner"),
        StringValue(value="MSFT"),
        StringValue,
        replay_body,
    )
    assert replayed.value == "stored:AAPL"
    assert replay_entries == 0

    # Terminal replay never acquired this claim, so it must not delete another
    # owner's leaked create-only claim.
    remaining = await store.get_claim(_claim_key(options))
    assert remaining is not None
    assert remaining == stale


async def test_expired_create_only_workflow_claim_remains_busy(
    store: OpenDALStore,
) -> None:
    options = _options(run_id="expired-create-only", owner_id="new-owner")
    expired = await _put_workflow_claim(
        store,
        options,
        owner_id="stale-owner",
        expires_delta=timedelta(minutes=-1),
    )
    body_entries = 0

    async def body(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal body_entries
        body_entries += 1
        return StringValue(value="should-not-run")

    with pytest.raises(ClaimBusyError) as exc_info:
        await run(store, options, StringValue(value="request"), StringValue, body)
    assert exc_info.value.owner_id == "stale-owner"
    assert exc_info.value.lease_expires_at is not None
    assert exc_info.value.lease_expires_at < datetime.now(UTC)
    assert body_entries == 0
    assert await store.get_claim(_claim_key(options)) == expired


async def test_claim_store_without_owner_keeps_at_least_once_execution(
    store: OpenDALStore,
) -> None:
    options = Options(
        workflow_id="prices:singleflight",
        run_id="no-owner",
        code_version="test",
    )
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    body_entries = 0

    async def body(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal body_entries
        body_entries += 1
        call = body_entries
        if call == 1:
            first_entered.set()
            await release_first.wait()
        return StringValue(value=f"call:{call}")

    first = asyncio.create_task(
        run(store, options, StringValue(value="request"), StringValue, body)
    )
    await asyncio.wait_for(first_entered.wait(), timeout=1)
    second = await run(store, options, StringValue(value="request"), StringValue, body)
    assert second.value == "call:2"
    release_first.set()
    await first

    assert body_entries == 2
    assert (
        await store.get_claim(
            ClaimKey(
                workflow_id=options.workflow_id,
                run_id=options.run_id,
                claim_id=WORKFLOW_EXECUTION_CLAIM_ID,
            )
        )
        is None
    )


@pytest.mark.parametrize("lose_create", [True, False], ids=["lost-create", "acquired"])
async def test_terminal_state_is_refreshed_around_claim_acquisition(
    tmp_path,
    lose_create: bool,
) -> None:
    options = _options(run_id=f"terminal-race-{lose_create}", owner_id="racing-owner")
    input_any = Any()
    input_any.Pack(StringValue(value="request"))
    result_any = Any()
    result_any.Pack(StringValue(value="stored:race"))
    now = Timestamp()
    now.GetCurrentTime()
    terminal = temporaless_pb2.WorkflowRecord(
        schema_version=temporaless_pb2.RECORD_SCHEMA_VERSION_WORKFLOW,
        key=WorkflowKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
        ).to_proto(),
        workflow_type=("workflow:google.protobuf.StringValue->google.protobuf.StringValue"),
        code_version=options.code_version,
        input=input_any,
        status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        result=result_any,
        created_at=now,
        completed_at=now,
    )

    class TerminalRaceStore(OpenDALStore):
        async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
            if record.resource_type == temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW:
                await self.put_workflow(terminal)
                if lose_create:
                    return False
            return await super().try_create_claim(record)

    race_store = TerminalRaceStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    body_entries = 0

    async def body(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal body_entries
        body_entries += 1
        return StringValue(value="should-not-run")

    result = await run(
        race_store,
        options,
        StringValue(value="request"),
        StringValue,
        body,
    )
    assert result.value == "stored:race"
    assert body_entries == 0
    assert await race_store.get_claim(_claim_key(options)) is None


@pytest.mark.parametrize(
    ("outcome", "error_type", "want_status"),
    [
        pytest.param(
            "timer-pending",
            TimerPendingError,
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
            id="timer-pending",
        ),
        pytest.param(
            "event-pending",
            EventPendingError,
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
            id="event-pending",
        ),
        pytest.param(
            "dependency-pending",
            WorkflowDependencyPendingError,
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
            id="dependency-pending-remains-in-progress",
        ),
        pytest.param(
            "claim-busy",
            ClaimBusyError,
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
            id="activity-claim-busy",
        ),
        pytest.param(
            "failure",
            RuntimeError,
            temporaless_pb2.WORKFLOW_STATUS_FAILED,
            id="application-failure",
        ),
    ],
)
async def test_workflow_claim_is_released_on_pending_and_failure(
    store: OpenDALStore,
    outcome: str,
    error_type: type[BaseException],
    want_status: temporaless_pb2.WorkflowStatus,
) -> None:
    options = _options(run_id=outcome, owner_id="worker-owner")

    async def body(_workflow: Workflow, _request: StringValue) -> StringValue:
        if outcome == "timer-pending":
            raise TimerPendingError("wait:vendor", datetime.now(UTC) + timedelta(hours=1))
        if outcome == "event-pending":
            raise EventPendingError("approval")
        if outcome == "dependency-pending":
            raise WorkflowDependencyPendingError("upstream:prices", "2026-07-11")
        if outcome == "claim-busy":
            raise ClaimBusyError("activity:send", owner_id="activity-worker")
        raise RuntimeError("application failed")

    with pytest.raises(error_type):
        await run(store, options, StringValue(value="AAPL"), StringValue, body)

    assert await store.get_claim(_claim_key(options)) is None
    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == want_status


async def test_workflow_claim_is_released_on_cancellation_and_run_can_resume(
    store: OpenDALStore,
) -> None:
    options = _options(run_id="cancel-resume", owner_id="first-owner")
    entered = asyncio.Event()
    never = asyncio.Event()

    async def canceled_body(_workflow: Workflow, _request: StringValue) -> StringValue:
        entered.set()
        await never.wait()
        return StringValue(value="never")

    task = asyncio.create_task(
        run(store, options, StringValue(value="AAPL"), StringValue, canceled_body)
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    assert await store.get_claim(_claim_key(options)) is not None

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await store.get_claim(_claim_key(options)) is None
    pending = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert pending is not None
    assert pending.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS

    resume_entries = 0

    async def resumed_body(_workflow: Workflow, request: StringValue) -> StringValue:
        nonlocal resume_entries
        resume_entries += 1
        return StringValue(value=f"resumed:{request.value}")

    resumed = await run(
        store,
        _options(run_id=options.run_id, owner_id="second-owner"),
        StringValue(value="AAPL"),
        StringValue,
        resumed_body,
    )
    assert resumed.value == "resumed:AAPL"
    assert resume_entries == 1
    assert await store.get_claim(_claim_key(options)) is None

    completed = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert completed is not None
    assert completed.status == temporaless_pb2.WORKFLOW_STATUS_COMPLETED


async def test_cancellation_during_claim_create_waits_for_and_releases_acquisition(
    tmp_path,
) -> None:
    options = _options(run_id="cancel-during-create", owner_id="cancelled-owner")
    claim_created = asyncio.Event()
    allow_create_to_return = asyncio.Event()

    class BlockingCreateStore(OpenDALStore):
        async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
            created = await super().try_create_claim(record)
            if record.resource_type == temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW:
                claim_created.set()
                await allow_create_to_return.wait()
            return created

    blocking_store = BlockingCreateStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    body_entries = 0

    async def body(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal body_entries
        body_entries += 1
        return StringValue(value="should-not-run")

    task = asyncio.create_task(
        run(
            blocking_store,
            options,
            StringValue(value="request"),
            StringValue,
            body,
        )
    )
    await asyncio.wait_for(claim_created.wait(), timeout=1)
    assert await blocking_store.get_claim(_claim_key(options)) is not None

    task.cancel()
    allow_create_to_return.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert body_entries == 0
    assert await blocking_store.get_claim(_claim_key(options)) is None
    assert (
        await blocking_store.get_workflow(
            WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
        )
        is None
    )


async def test_repeated_cancellation_waits_for_claim_cleanup(tmp_path) -> None:
    options = _options(run_id="cancel-during-cleanup", owner_id="cancelled-owner")
    delete_started = asyncio.Event()
    allow_delete = asyncio.Event()

    class BlockingDeleteStore(OpenDALStore):
        async def delete_claim(self, key: ClaimKey) -> bool:
            if key.claim_id == WORKFLOW_EXECUTION_CLAIM_ID:
                delete_started.set()
                await allow_delete.wait()
            return await super().delete_claim(key)

    blocking_store = BlockingDeleteStore(opendal.AsyncOperator("fs", root=str(tmp_path)))

    async def body(_workflow: Workflow, _request: StringValue) -> StringValue:
        return StringValue(value="completed")

    task = asyncio.create_task(
        run(
            blocking_store,
            options,
            StringValue(value="request"),
            StringValue,
            body,
        )
    )
    await asyncio.wait_for(delete_started.wait(), timeout=1)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    allow_delete.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await blocking_store.get_claim(_claim_key(options)) is None
    completed = await blocking_store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert completed is not None
    assert completed.status == temporaless_pb2.WORKFLOW_STATUS_COMPLETED


async def test_claim_cleanup_failure_is_surfaced(tmp_path) -> None:
    options = _options(run_id="cleanup-failure", owner_id="worker-owner")

    class FailingDeleteStore(OpenDALStore):
        async def delete_claim(self, key: ClaimKey) -> bool:
            if key.claim_id == WORKFLOW_EXECUTION_CLAIM_ID:
                raise OSError("claim backend unavailable")
            return await super().delete_claim(key)

    failing_store = FailingDeleteStore(opendal.AsyncOperator("fs", root=str(tmp_path)))

    async def body(_workflow: Workflow, _request: StringValue) -> StringValue:
        raise TimerPendingError("wait", datetime.now(UTC) + timedelta(hours=1))

    with pytest.raises(ClaimReleaseError, match="workflow execution claim") as exc:
        await run(
            failing_store,
            options,
            StringValue(value="request"),
            StringValue,
            body,
        )
    assert isinstance(exc.value.__cause__, OSError)
    assert await failing_store.get_claim(_claim_key(options)) is not None

    pending = await failing_store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert pending is not None
    assert pending.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
