"""Activity-claim contention, replay refresh, and lifecycle boundaries."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.any_pb2 import Any
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue

from temporaless._cache import RunScopedCache
from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    CLAIM_RECORD_SCHEMA_VERSION,
    ActivityKey,
    ClaimKey,
    OpenDALStore,
    TimerKey,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ACTIVITY_CLAIM_ID_PREFIX,
    ActivityError,
    ClaimBusyError,
    ClaimReleaseError,
    Options,
    RetryPolicy,
    TimerPendingError,
    Workflow,
    WorkflowInfrastructureError,
    run,
)

_ACTIVITY_TYPE = "activity:google.protobuf.StringValue->google.protobuf.StringValue"


@pytest.fixture
def store(tmp_path) -> OpenDALStore:
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


def _workflow(store: OpenDALStore, *, run_id: str, owner_id: str) -> Workflow:
    return Workflow(
        store,
        Options(
            workflow_id="prices:activity-claims",
            run_id=run_id,
            code_version="test",
            claim_owner_id=owner_id,
        ),
    )


def _activity_key(run_id: str, activity_id: str = "fetch") -> ActivityKey:
    return ActivityKey(
        workflow_id="prices:activity-claims",
        run_id=run_id,
        activity_id=activity_id,
    )


def _claim_key(run_id: str, activity_id: str = "fetch") -> ClaimKey:
    return ClaimKey(
        workflow_id="prices:activity-claims",
        run_id=run_id,
        claim_id=f"{ACTIVITY_CLAIM_ID_PREFIX}{activity_id}",
    )


def _duration(value: timedelta) -> Duration:
    duration = Duration()
    duration.FromTimedelta(value)
    return duration


def _durable_policy() -> RetryPolicy:
    return RetryPolicy(
        initial_interval=_duration(timedelta(hours=1)),
        backoff_coefficient=1,
        maximum_attempts=3,
        durable_backoff_threshold=_duration(timedelta(minutes=1)),
    )


def _retry_timer_id(activity_id: str) -> str:
    return f"retry:{activity_id}"


async def _rewind_retry(store: OpenDALStore, *, run_id: str, activity_id: str = "fetch") -> None:
    past = datetime.now(UTC) - timedelta(seconds=1)
    activity = await store.get_activity(_activity_key(run_id, activity_id))
    assert activity is not None
    activity.next_attempt_at.FromDatetime(past)
    await store.put_activity(activity)

    timer_key = TimerKey(
        workflow_id="prices:activity-claims",
        run_id=run_id,
        timer_id=_retry_timer_id(activity_id),
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.fire_at.FromDatetime(past)
    await store.put_timer(timer)


@pytest.mark.parametrize("second_owner", ["same-owner", "other-owner"])
async def test_live_activity_claim_is_busy_for_same_and_distinct_owner(
    store: OpenDALStore,
    second_owner: str,
) -> None:
    run_id = f"live-{second_owner}"
    first_workflow = _workflow(store, run_id=run_id, owner_id="same-owner")
    second_workflow = _workflow(store, run_id=run_id, owner_id=second_owner)
    entered = asyncio.Event()
    release = asyncio.Event()
    duplicate_entries = 0

    async def first_body() -> StringValue:
        entered.set()
        await release.wait()
        return StringValue(value="first")

    async def duplicate_body() -> StringValue:
        nonlocal duplicate_entries
        duplicate_entries += 1
        return StringValue(value="duplicate")

    first = asyncio.create_task(
        first_workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            first_body,
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)

    try:
        with pytest.raises(ClaimBusyError) as exc_info:
            await second_workflow.run_activity(
                "fetch",
                _ACTIVITY_TYPE,
                StringValue(value="AAPL"),
                StringValue,
                duplicate_body,
            )
        assert exc_info.value.owner_id == "same-owner"
        assert duplicate_entries == 0
    finally:
        release.set()

    assert (await first).value == "first"
    assert await store.get_claim(_claim_key(run_id)) is None


async def test_terminal_activity_failure_releases_claim(store: OpenDALStore) -> None:
    run_id = "terminal-failure"
    workflow = _workflow(store, run_id=run_id, owner_id="worker")

    async def fail() -> StringValue:
        raise ActivityError("invalid", "bad request")

    with pytest.raises(ActivityError, match="bad request"):
        await workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            fail,
        )

    record = await store.get_activity(_activity_key(run_id))
    assert record is not None
    assert record.status == temporaless_pb2.ACTIVITY_STATUS_FAILED
    assert await store.get_claim(_claim_key(run_id)) is None


async def test_durable_retry_releases_claim_and_resumes_with_new_owner(
    store: OpenDALStore,
) -> None:
    run_id = "durable-new-owner"
    first_workflow = _workflow(store, run_id=run_id, owner_id="first-owner")

    async def rate_limited() -> StringValue:
        raise ActivityError("rate_limited", "retry later")

    with pytest.raises(TimerPendingError):
        await first_workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            rate_limited,
            _durable_policy(),
            retry_timer_id=_retry_timer_id("fetch"),
        )
    assert await store.get_claim(_claim_key(run_id)) is None

    await _rewind_retry(store, run_id=run_id)
    second_workflow = _workflow(store, run_id=run_id, owner_id="second-owner")
    executions = 0

    async def succeed() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="ok")

    result = await second_workflow.run_activity(
        "fetch",
        _ACTIVITY_TYPE,
        StringValue(value="AAPL"),
        StringValue,
        succeed,
        _durable_policy(),
        retry_timer_id=_retry_timer_id("fetch"),
    )
    assert result.value == "ok"
    assert executions == 1
    assert await store.get_claim(_claim_key(run_id)) is None

    record = await store.get_activity(_activity_key(run_id))
    assert record is not None
    assert record.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert len(record.attempts) == 2


async def test_post_claim_refresh_observes_timer_published_after_cached_miss(tmp_path) -> None:
    run_id = "post-claim-timer"
    activity_id = "fetch"
    retry_timer_id = _retry_timer_id(activity_id)

    class TimerBeforeClaimStore(OpenDALStore):
        activity_claim_attempts = 0

        async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
            if record.resource_type == temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY:
                self.activity_claim_attempts += 1
            if self.activity_claim_attempts == 1:
                winner = temporaless_pb2.ClaimRecord()
                winner.CopyFrom(record)
                winner.owner_id = "prior-owner"
                assert await super().try_create_claim(winner)
                now = datetime.now(UTC)
                fire_at = Timestamp()
                fire_at.FromDatetime(now + timedelta(hours=1))
                created_at = Timestamp()
                created_at.FromDatetime(now)
                await self.put_timer(
                    temporaless_pb2.TimerRecord(
                        schema_version=temporaless_pb2.RECORD_SCHEMA_VERSION_TIMER,
                        key=TimerKey(
                            workflow_id="prices:activity-claims",
                            run_id=run_id,
                            timer_id=retry_timer_id,
                        ).to_proto(),
                        timer_kind=temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY,
                        code_version="test",
                        duration=_duration(timedelta(hours=1)),
                        status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
                        fire_at=fire_at,
                        created_at=created_at,
                        retry_activity_id=activity_id,
                    )
                )
                assert await super().delete_claim(_claim_key(run_id, activity_id))
                return False
            return await super().try_create_claim(record)

    raw_store = TimerBeforeClaimStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    scope = WorkflowKey(workflow_id="prices:activity-claims", run_id=run_id)
    cache = RunScopedCache(raw_store, scope)
    workflow = Workflow(
        cache,
        Options(
            workflow_id=scope.workflow_id,
            run_id=scope.run_id,
            code_version="test",
            claim_owner_id="new-owner",
        ),
    )
    executions = 0

    async def should_not_run() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="unexpected")

    with pytest.raises(TimerPendingError):
        await workflow.run_activity(
            activity_id,
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            should_not_run,
            _durable_policy(),
            retry_timer_id=retry_timer_id,
        )

    assert executions == 0
    assert raw_store.activity_claim_attempts == 2
    assert await raw_store.get_claim(_claim_key(run_id, activity_id)) is None
    timer = await raw_store.get_timer(
        TimerKey(
            workflow_id=scope.workflow_id,
            run_id=scope.run_id,
            timer_id=retry_timer_id,
        )
    )
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED


async def test_busy_activity_claim_does_not_consume_due_retry_timer(
    store: OpenDALStore,
) -> None:
    run_id = "busy-due-timer"
    first_workflow = _workflow(store, run_id=run_id, owner_id="first-owner")

    async def rate_limited() -> StringValue:
        raise ActivityError("rate_limited", "retry later")

    with pytest.raises(TimerPendingError):
        await first_workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            rate_limited,
            _durable_policy(),
            retry_timer_id=_retry_timer_id("fetch"),
        )
    await _rewind_retry(store, run_id=run_id)

    now = Timestamp()
    now.GetCurrentTime()
    expires = Timestamp()
    expires.FromDatetime(datetime.now(UTC) + timedelta(minutes=15))
    assert await store.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=_claim_key(run_id).to_proto(),
            owner_id="stale-owner",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
            resource_id="fetch",
            code_version="test",
            lease_expires_at=expires,
            created_at=now,
            heartbeat_at=now,
        )
    )

    workflow = _workflow(store, run_id=run_id, owner_id="new-owner")

    async def should_not_run() -> StringValue:
        raise AssertionError("busy activity executed")

    with pytest.raises(ClaimBusyError):
        await workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            should_not_run,
            _durable_policy(),
            retry_timer_id=_retry_timer_id("fetch"),
        )

    timer = await store.get_timer(
        TimerKey(
            workflow_id="prices:activity-claims",
            run_id=run_id,
            timer_id=_retry_timer_id("fetch"),
        )
    )
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED


async def test_cancellation_during_due_retry_retains_claim_and_scheduled_wakeup(
    store: OpenDALStore,
) -> None:
    run_id = "cancel-due-retry"
    first = _workflow(store, run_id=run_id, owner_id="first-owner")

    async def rate_limited() -> StringValue:
        raise ActivityError("rate_limited", "retry later")

    with pytest.raises(TimerPendingError):
        await first.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            rate_limited,
            _durable_policy(),
            retry_timer_id=_retry_timer_id("fetch"),
        )
    await _rewind_retry(store, run_id=run_id)

    entered = asyncio.Event()
    never = asyncio.Event()
    resumed = _workflow(store, run_id=run_id, owner_id="resumed-owner")

    async def ambiguous_body() -> StringValue:
        entered.set()
        await never.wait()
        return StringValue(value="never")

    task = asyncio.create_task(
        resumed.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            ambiguous_body,
            _durable_policy(),
            retry_timer_id=_retry_timer_id("fetch"),
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)

    timer_key = TimerKey(
        workflow_id="prices:activity-claims",
        run_id=run_id,
        timer_id=_retry_timer_id("fetch"),
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await store.get_claim(_claim_key(run_id)) is not None

    retry = _workflow(store, run_id=run_id, owner_id="third-owner")
    with pytest.raises(ClaimBusyError):
        await retry.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            ambiguous_body,
            _durable_policy(),
            retry_timer_id=_retry_timer_id("fetch"),
        )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED


async def test_activity_cancellation_retains_ambiguous_claim(store: OpenDALStore) -> None:
    run_id = "cancel-retains"
    workflow = _workflow(store, run_id=run_id, owner_id="worker")
    entered = asyncio.Event()
    never = asyncio.Event()

    async def body() -> StringValue:
        entered.set()
        await never.wait()
        return StringValue(value="never")

    task = asyncio.create_task(
        workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            body,
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    claim = await store.get_claim(_claim_key(run_id))
    assert claim is not None
    assert claim.owner_id == "worker"
    assert await store.get_activity(_activity_key(run_id)) is None

    with pytest.raises(ClaimBusyError):
        await workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            body,
        )


async def test_cancellation_during_persisted_backoff_releases_claim_and_resumes(
    tmp_path,
) -> None:
    retry_persisted = asyncio.Event()

    class BackoffStore(OpenDALStore):
        async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None:
            await super().put_activity(record)
            if record.status == temporaless_pb2.ACTIVITY_STATUS_RETRYING:
                retry_persisted.set()

    backoff_store = BackoffStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    workflow = _workflow(backoff_store, run_id="cancel-backoff", owner_id="worker")
    attempts = 0

    async def body() -> StringValue:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ActivityError("transient", "retry")
        return StringValue(value="resumed")

    policy = RetryPolicy(
        initial_interval=_duration(timedelta(minutes=30)),
        backoff_coefficient=1,
        maximum_attempts=2,
        durable_backoff_threshold=_duration(timedelta(hours=1)),
    )
    task = asyncio.create_task(
        workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            body,
            policy,
            retry_timer_id=_retry_timer_id("fetch"),
        )
    )
    await asyncio.wait_for(retry_persisted.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    retrying = await backoff_store.get_activity(_activity_key("cancel-backoff"))
    assert retrying is not None
    assert retrying.status == temporaless_pb2.ACTIVITY_STATUS_RETRYING
    assert await backoff_store.get_claim(_claim_key("cancel-backoff")) is None

    result = await workflow.run_activity(
        "fetch",
        _ACTIVITY_TYPE,
        StringValue(value="AAPL"),
        StringValue,
        body,
        policy,
        retry_timer_id=_retry_timer_id("fetch"),
    )

    assert result.value == "resumed"
    assert attempts == 2
    assert await backoff_store.get_claim(_claim_key("cancel-backoff")) is None


async def test_cancellation_during_activity_claim_create_releases_acquired_claim(
    tmp_path,
) -> None:
    claim_created = asyncio.Event()
    allow_create_to_return = asyncio.Event()

    class BlockingCreateStore(OpenDALStore):
        async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
            created = await super().try_create_claim(record)
            if record.resource_type == temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY:
                claim_created.set()
                await allow_create_to_return.wait()
            return created

    blocking_store = BlockingCreateStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    workflow = _workflow(blocking_store, run_id="cancel-create", owner_id="worker")
    body_entries = 0

    async def body() -> StringValue:
        nonlocal body_entries
        body_entries += 1
        return StringValue(value="should-not-run")

    task = asyncio.create_task(
        workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            body,
        )
    )
    await asyncio.wait_for(claim_created.wait(), timeout=1)
    task.cancel()
    allow_create_to_return.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert body_entries == 0
    assert await blocking_store.get_claim(_claim_key("cancel-create")) is None


async def test_cancellation_during_activity_claim_create_surfaces_release_failure(
    tmp_path,
) -> None:
    claim_created = asyncio.Event()
    allow_create_to_return = asyncio.Event()

    class FailingReleaseStore(OpenDALStore):
        async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
            created = await super().try_create_claim(record)
            if record.resource_type == temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY:
                claim_created.set()
                await allow_create_to_return.wait()
            return created

        async def delete_claim(self, key: ClaimKey) -> bool:
            raise OSError("claim release unavailable")

    store = FailingReleaseStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    workflow = _workflow(store, run_id="cancel-create-release-fails", owner_id="worker")
    body_entries = 0

    async def body() -> StringValue:
        nonlocal body_entries
        body_entries += 1
        return StringValue(value="should-not-run")

    task = asyncio.create_task(
        workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            body,
        )
    )
    await asyncio.wait_for(claim_created.wait(), timeout=1)
    task.cancel()
    allow_create_to_return.set()
    with pytest.raises(ClaimReleaseError, match="claim release unavailable") as exc_info:
        await task

    assert isinstance(exc_info.value.__cause__, OSError)
    assert body_entries == 0
    assert await store.get_claim(_claim_key("cancel-create-release-fails")) is not None


async def test_activity_storage_failure_retains_claim(tmp_path) -> None:
    class FailingPutStore(OpenDALStore):
        async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None:
            raise OSError("activity store unavailable")

    failing_store = FailingPutStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    workflow = _workflow(failing_store, run_id="storage-failure", owner_id="worker")

    async def body() -> StringValue:
        return StringValue(value="side-effect-completed")

    with pytest.raises(
        WorkflowInfrastructureError,
        match="activity store unavailable",
    ) as exc_info:
        await workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
            StringValue,
            body,
        )
    assert isinstance(exc_info.value.__cause__, OSError)
    assert await failing_store.get_claim(_claim_key("storage-failure")) is not None


async def test_claim_loss_refreshes_cached_terminal_activity(tmp_path) -> None:
    input_any = Any()
    input_any.Pack(StringValue(value="AAPL"))
    result_any = Any()
    result_any.Pack(StringValue(value="stored"))

    class TerminalRaceStore(OpenDALStore):
        async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
            if record.resource_type == temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY:
                now = Timestamp()
                now.GetCurrentTime()
                await self.put_activity(
                    temporaless_pb2.ActivityRecord(
                        schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
                        key=_activity_key("terminal-race").to_proto(),
                        activity_type=_ACTIVITY_TYPE,
                        code_version="test",
                        input=input_any,
                        status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
                        result=result_any,
                        created_at=now,
                        completed_at=now,
                    )
                )
                return False
            return await super().try_create_claim(record)

    race_store = TerminalRaceStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    activity_entries = 0

    async def workflow_body(workflow: Workflow, request: StringValue) -> StringValue:
        async def activity_body() -> StringValue:
            nonlocal activity_entries
            activity_entries += 1
            return StringValue(value="should-not-run")

        return await workflow.run_activity(
            "fetch", _ACTIVITY_TYPE, request, StringValue, activity_body
        )

    result = await run(
        race_store,
        Options(
            workflow_id="prices:activity-claims",
            run_id="terminal-race",
            code_version="test",
            claim_owner_id="worker",
        ),
        StringValue(value="AAPL"),
        StringValue,
        workflow_body,
    )
    assert result.value == "stored"
    assert activity_entries == 0


async def test_activity_claim_release_failure_leaves_workflow_in_progress(tmp_path) -> None:
    class FailingDeleteStore(OpenDALStore):
        async def delete_claim(self, key: ClaimKey) -> bool:
            if key.claim_id.startswith(ACTIVITY_CLAIM_ID_PREFIX):
                raise OSError("activity claim delete failed")
            return await super().delete_claim(key)

    failing_store = FailingDeleteStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    options = Options(
        workflow_id="prices:activity-claims",
        run_id="release-failure",
        code_version="test",
        claim_owner_id="worker",
    )

    async def workflow_body(workflow: Workflow, request: StringValue) -> StringValue:
        async def activity_body() -> StringValue:
            return StringValue(value="done")

        return await workflow.run_activity(
            "fetch", _ACTIVITY_TYPE, request, StringValue, activity_body
        )

    with pytest.raises(ClaimReleaseError, match="activity claim"):
        await run(
            failing_store,
            options,
            StringValue(value="AAPL"),
            StringValue,
            workflow_body,
        )

    workflow_record = await failing_store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    activity_record = await failing_store.get_activity(_activity_key(options.run_id))
    assert activity_record is not None
    assert activity_record.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert await failing_store.get_claim(_claim_key(options.run_id)) is not None
    assert (
        await failing_store.get_claim(
            ClaimKey(
                workflow_id=options.workflow_id,
                run_id=options.run_id,
                claim_id="workflow:execution",
            )
        )
        is None
    )


async def test_repeated_cancellation_waits_for_safe_activity_claim_release(tmp_path) -> None:
    delete_started = asyncio.Event()
    allow_delete = asyncio.Event()

    class BlockingDeleteStore(OpenDALStore):
        async def delete_claim(self, key: ClaimKey) -> bool:
            if key.claim_id.startswith(ACTIVITY_CLAIM_ID_PREFIX):
                delete_started.set()
                await allow_delete.wait()
            return await super().delete_claim(key)

    blocking_store = BlockingDeleteStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    workflow = _workflow(blocking_store, run_id="cancel-cleanup", owner_id="worker")

    async def body() -> StringValue:
        return StringValue(value="done")

    task = asyncio.create_task(
        workflow.run_activity(
            "fetch",
            _ACTIVITY_TYPE,
            StringValue(value="AAPL"),
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
    assert await blocking_store.get_claim(_claim_key("cancel-cleanup")) is None
    record = await blocking_store.get_activity(_activity_key("cancel-cleanup"))
    assert record is not None
    assert record.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
