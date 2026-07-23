import asyncio
from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.any_pb2 import Any
from google.protobuf.duration_pb2 import Duration
from google.protobuf.message import DecodeError
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import Int32Value, StringValue
from protovalidate import ValidationError

from temporaless.storage import (
    CLAIM_RECORD_SCHEMA_VERSION,
    CREATE_ONLY_CLAIMS,
    CREATE_ONLY_EVENT_DELIVERY,
    EVENT_RECORD_SCHEMA_VERSION,
    ActivityKey,
    ClaimKey,
    EventDeliveryConflictError,
    EventKey,
    OpenDALStore,
    RunRecordValidationError,
    TimerKey,
    WorkflowKey,
    _due_entry_path,
    deliver_event,
)
from temporaless.timerscanner import due_timers
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ActivityConflictError,
    ActivityError,
    ActivityOptions,
    ActivityWrapOptions,
    ClaimBusyError,
    EventPendingError,
    Options,
    PollOptions,
    RetryPolicy,
    TimerConflictError,
    TimerPendingError,
    Workflow,
    WorkflowConflictError,
    WorkflowInfrastructureError,
    WorkflowWrapOptions,
    annotate,
    gather_activities,
    run,
    wrap_activity,
    wrap_workflow,
)


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


def _duration(value: timedelta) -> Duration:
    duration = Duration()
    duration.FromTimedelta(value)
    return duration


# User-supplied activity_id is the de-duplication contract. Same id replays
# the stored result regardless of input bytes — the caller chose the id and
# is responsible for picking distinct ids when they want distinct executions.
@pytest.mark.parametrize(
    ("first_input", "next_input", "want", "want_error"),
    [
        ("AAPL", "AAPL", "stored:AAPL", None),
        ("AAPL", "MSFT", "stored:AAPL", None),
    ],
)
async def test_activity_replay(
    store: OpenDALStore,
    first_input: str,
    next_input: str,
    want: str | None,
    want_error: type[Exception] | None,
) -> None:
    workflow = Workflow(
        store,
        Options(workflow_id="prices:symbol", run_id="2026-05-02"),
    )
    executions = 0

    async def execute() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value=f"stored:{first_input}")

    first = await workflow.run_activity(
        "fetch:symbol",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value=first_input),
        StringValue,
        execute,
    )
    assert first.value == f"stored:{first_input}"

    if want_error is not None:
        with pytest.raises(want_error):
            await workflow.run_activity(
                "fetch:symbol",
                "activity:google.protobuf.StringValue->google.protobuf.StringValue",
                StringValue(value=next_input),
                StringValue,
                execute,
            )
        assert executions == 1
        return

    second = await workflow.run_activity(
        "fetch:symbol",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value=next_input),
        StringValue,
        execute,
    )
    assert second.value == want
    assert executions == 1


async def test_activity_replay_rejects_failed_record_without_failure(
    store: OpenDALStore,
) -> None:
    workflow = Workflow(
        store,
        Options(workflow_id="prices:malformed-activity", run_id="run"),
    )
    key = ActivityKey(
        workflow_id="prices:malformed-activity",
        run_id="run",
        activity_id="fetch:symbol",
    )
    await store.put_activity(
        temporaless_pb2.ActivityRecord(
            schema_version=temporaless_pb2.RECORD_SCHEMA_VERSION_ACTIVITY,
            key=key.to_proto(),
            activity_type=("activity:google.protobuf.StringValue->google.protobuf.StringValue"),
            status=temporaless_pb2.ACTIVITY_STATUS_FAILED,
        )
    )
    executions = 0

    async def should_not_run() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="unexpected")

    with pytest.raises(ActivityConflictError, match="has no failure"):
        await workflow.run_activity(
            key.activity_id,
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="AAPL"),
            StringValue,
            should_not_run,
        )
    assert executions == 0


@pytest.mark.parametrize(
    ("expires_delta", "want_executions"),
    [
        (timedelta(minutes=5), 0),
        (timedelta(seconds=-1), 0),
    ],
)
async def test_activity_claim_busy_and_expired(
    store: OpenDALStore,
    expires_delta: timedelta,
    want_executions: int,
) -> None:
    created_at = Timestamp()
    created_at.GetCurrentTime()
    expires_at = Timestamp()
    expires_at.FromDatetime(datetime.now(UTC) + expires_delta)
    claim_key = ClaimKey(
        workflow_id="prices:claims",
        run_id="2026-05-02",
        claim_id="activity:fetch:symbol",
    )
    claim = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=claim_key.to_proto(),
        owner_id="other-owner",
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
        resource_id="fetch:symbol",
        lease_expires_at=expires_at,
        created_at=created_at,
        heartbeat_at=created_at,
    )
    assert await store.try_create_claim(claim) is True

    workflow = Workflow(
        store,
        Options(
            workflow_id="prices:claims",
            run_id="2026-05-02",
            claim_owner_id="this-owner",
        ),
    )
    executions = 0

    async def execute() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="stored:AAPL")

    with pytest.raises(ClaimBusyError) as captured:
        await workflow.run_activity(
            "fetch:symbol",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="AAPL"),
            StringValue,
            execute,
        )
    assert captured.value.capability == CREATE_ONLY_CLAIMS
    assert executions == want_executions


async def test_claim_store_declares_capability(store: OpenDALStore) -> None:
    assert await store.claim_capability() == CREATE_ONLY_CLAIMS


@pytest.mark.parametrize(
    ("first_input", "next_input", "want", "want_error"),
    [
        ("AAPL", "AAPL", "workflow:normalized:AAPL", None),
        ("AAPL", "MSFT", "workflow:normalized:AAPL", None),
    ],
)
async def test_workflow_replay(
    store: OpenDALStore,
    first_input: str,
    next_input: str,
    want: str | None,
    want_error: type[Exception] | None,
) -> None:
    executions = 0

    async def execute(workflow: Workflow, input_message: StringValue) -> StringValue:
        nonlocal executions
        executions += 1

        async def normalize() -> StringValue:
            return StringValue(value=f"normalized:{input_message.value}")

        activity_result = await workflow.run_activity(
            "normalize:symbol",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            input_message,
            StringValue,
            normalize,
        )
        return StringValue(value=f"workflow:{activity_result.value}")

    first = await run(
        store,
        Options(workflow_id="prices:symbol", run_id="2026-05-02"),
        StringValue(value=first_input),
        StringValue,
        execute,
    )
    assert first.value == f"workflow:normalized:{first_input}"

    if want_error is not None:
        with pytest.raises(want_error):
            await run(
                store,
                Options(workflow_id="prices:symbol", run_id="2026-05-02"),
                StringValue(value=next_input),
                StringValue,
                execute,
            )
        assert executions == 1
        return

    second = await run(
        store,
        Options(workflow_id="prices:symbol", run_id="2026-05-02"),
        StringValue(value=next_input),
        StringValue,
        execute,
    )
    assert second.value == want
    assert executions == 1


@pytest.mark.parametrize(
    ("case_id", "returned"),
    [
        ("wrong-protobuf", Int32Value(value=7)),
        ("non-protobuf", "not-a-protobuf"),
    ],
)
async def test_activity_rejects_wrong_response_type_without_persisting_success(
    store: OpenDALStore,
    case_id: str,
    returned: object,
) -> None:
    workflow = Workflow(
        store,
        Options(
            workflow_id=f"prices:wrong-activity-result:{case_id}",
            run_id="run",
        ),
    )
    executions = 0

    async def execute(_request: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        return returned  # type: ignore[return-value]

    with pytest.raises(ActivityError, match="expected google.protobuf.StringValue"):
        await workflow.execute_activity(
            ActivityOptions(activity_id="fetch"),
            StringValue(value="AAPL"),
            StringValue,
            execute,
        )

    key = ActivityKey(
        workflow_id=workflow.workflow_id,
        run_id=workflow.run_id,
        activity_id="fetch",
    )
    record = await store.get_activity(key)
    assert record is not None
    assert record.status == temporaless_pb2.ACTIVITY_STATUS_FAILED
    assert not record.HasField("result")
    assert len(record.attempts) == 1
    assert executions == 1

    async def should_not_run(_request: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="unexpected")

    with pytest.raises(ActivityError, match="expected google.protobuf.StringValue"):
        await workflow.execute_activity(
            ActivityOptions(activity_id="fetch"),
            StringValue(value="AAPL"),
            StringValue,
            should_not_run,
        )
    assert executions == 1


@pytest.mark.parametrize(
    ("case_id", "returned"),
    [
        ("wrong-protobuf", Int32Value(value=7)),
        ("non-protobuf", "not-a-protobuf"),
    ],
)
async def test_workflow_rejects_wrong_response_type_and_replays_terminal_failure(
    store: OpenDALStore,
    case_id: str,
    returned: object,
) -> None:
    options = Options(
        workflow_id=f"prices:wrong-workflow-result:{case_id}",
        run_id="run",
    )
    executions = 0

    async def execute(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        return returned  # type: ignore[return-value]

    with pytest.raises(TypeError, match="expected google.protobuf.StringValue"):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    key = WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    record = await store.get_workflow(key)
    assert record is not None
    assert record.status == temporaless_pb2.WORKFLOW_STATUS_FAILED
    assert not record.HasField("result")
    assert executions == 1

    with pytest.raises(ActivityError, match="expected google.protobuf.StringValue"):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert executions == 1


@pytest.mark.parametrize("primitive", ["activity", "timer", "event"])
async def test_corrupt_primitive_record_is_terminal_not_retryable_infrastructure(
    store: OpenDALStore,
    primitive: str,
) -> None:
    options = Options(
        workflow_id=f"prices:corrupt-{primitive}",
        run_id="run",
    )
    if primitive == "activity":
        corrupt_key = ActivityKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
            activity_id="corrupt",
        )
    elif primitive == "timer":
        corrupt_key = TimerKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
            timer_id="corrupt",
        )
    else:
        corrupt_key = EventKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
            event_id="corrupt",
        )
    await store._operator.write(corrupt_key.path(), b"\xff")

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        if primitive == "activity":

            async def activity(_request: StringValue) -> StringValue:
                pytest.fail("activity body must not run after corrupt record read")

            return await workflow.execute_activity(
                ActivityOptions(activity_id="corrupt"),
                request,
                StringValue,
                activity,
            )
        if primitive == "timer":
            await workflow.sleep("corrupt", timedelta(hours=1))
            return request
        return await workflow.wait_event("corrupt", StringValue)

    with pytest.raises(DecodeError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_FAILED


async def test_run_order_time_is_persisted_and_immutable(store: OpenDALStore) -> None:
    first_order = Timestamp()
    first_order.FromDatetime(datetime(2026, 5, 4, 9, 30, tzinfo=UTC))
    changed_order = Timestamp()
    changed_order.FromDatetime(datetime(2026, 5, 4, 9, 31, tzinfo=UTC))
    ready = False

    async def execute(_workflow: Workflow, _request: StringValue) -> StringValue:
        if not ready:
            raise EventPendingError("approval")
        return StringValue(value="ok")

    options = Options(
        workflow_id="prices:ordered",
        run_id="opaque:run",
        run_order_time=first_order,
    )
    with pytest.raises(EventPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    record = await store.get_workflow(
        WorkflowKey(workflow_id="prices:ordered", run_id="opaque:run")
    )
    assert record is not None
    assert record.run_order_time == first_order

    ready = True
    result = await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert result.value == "ok"

    with pytest.raises(WorkflowConflictError, match="run_order_time changed"):
        await run(
            store,
            Options(
                workflow_id="prices:ordered",
                run_id="opaque:run",
                run_order_time=changed_order,
            ),
            StringValue(value="AAPL"),
            StringValue,
            execute,
        )


@pytest.mark.parametrize("workflow_id", ["prices/aapl", "", ".", ".."])
async def test_activity_key_rejects_invalid_workflow_ids(workflow_id: str) -> None:
    with pytest.raises(ValidationError):
        ActivityKey(workflow_id=workflow_id, run_id="2026-05-02", activity_id="fetch").path()


@pytest.mark.parametrize(
    ("options", "want_error"),
    [
        (Options(workflow_id="prices:ids", run_id=""), ValidationError),
        (
            Options(
                workflow_id="prices:ids",
                run_id="2026-05-02",
                claim_owner_id=".",
            ),
            ValidationError,
        ),
    ],
)
async def test_run_rejects_missing_required_ids(
    store: OpenDALStore, options: Options, want_error: type[Exception]
) -> None:
    async def should_not_run(_workflow, _input):
        return StringValue(value="should-not-run")

    with pytest.raises(want_error):
        await run(
            store,
            options,
            StringValue(value="AAPL"),
            StringValue,
            should_not_run,
        )


@pytest.mark.parametrize(
    ("decorator_name", "first_input", "next_input", "want_second", "want_executions"),
    [
        ("fixed", "AAPL", "AAPL", "wrapped:AAPL", 1),
        ("options_from_request", "AAPL", "MSFT", "wrapped:MSFT", 2),
    ],
)
async def test_rpc_workflow_decorators(
    store: OpenDALStore,
    decorator_name: str,
    first_input: str,
    next_input: str,
    want_second: str,
    want_executions: int,
) -> None:
    executions = 0

    async def execute(request: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value=f"wrapped:{request.value}")

    if decorator_name == "fixed":
        handler = wrap_workflow(
            WorkflowWrapOptions[StringValue](
                store=store,
                options=Options(
                    workflow_id="prices:wrapped",
                    run_id="2026-05-02",
                    claim_owner_id="decorator-worker",
                ),
            ),
            StringValue,
        )(execute)
    else:
        handler = wrap_workflow(
            WorkflowWrapOptions[StringValue](
                store=store,
                options_for=lambda request: Options(
                    workflow_id=f"prices:{request.value}",
                    run_id="2026-05-02",
                ),
            ),
            StringValue,
        )(execute)

    first = await handler(StringValue(value=first_input))
    second = await handler(StringValue(value=next_input))

    assert first.value == f"wrapped:{first_input}"
    assert second.value == want_second
    assert executions == want_executions


@pytest.mark.parametrize(
    ("decorator_name", "first_input", "next_input", "want_second", "want_executions"),
    [
        ("fixed", "AAPL", "AAPL", "activity:AAPL", 1),
        ("id_from_request", "AAPL", "MSFT", "activity:MSFT", 2),
    ],
)
async def test_rpc_activity_decorators(
    store: OpenDALStore,
    decorator_name: str,
    first_input: str,
    next_input: str,
    want_second: str,
    want_executions: int,
) -> None:
    workflow = Workflow(
        store,
        Options(
            workflow_id="prices:activity-wrapper",
            run_id=decorator_name,
        ),
    )
    executions = 0

    async def execute(request: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value=f"activity:{request.value}")

    if decorator_name == "fixed":
        handler = wrap_activity(
            ActivityWrapOptions[StringValue](
                workflow=workflow,
                options=ActivityOptions(activity_id="fetch:symbol"),
            ),
            StringValue,
        )(execute)
    else:
        handler = wrap_activity(
            ActivityWrapOptions[StringValue](
                workflow=workflow,
                options_for=lambda request: ActivityOptions(
                    activity_id=f"fetch:{request.value}",
                ),
            ),
            StringValue,
        )(execute)

    first = await handler(StringValue(value=first_input))
    second = await handler(StringValue(value=next_input))

    assert first.value == f"activity:{first_input}"
    assert second.value == want_second
    assert executions == want_executions


@pytest.mark.parametrize(
    ("duration", "want_error"),
    [
        (timedelta(seconds=0), None),
        (timedelta(hours=1), TimerPendingError),
    ],
)
async def test_sleep(
    store: OpenDALStore, duration: timedelta, want_error: type[Exception] | None
) -> None:
    executions = 0

    async def execute(workflow: Workflow, input_message: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        await workflow.sleep("wait:vendor-window", duration)
        return StringValue(value=f"done:{input_message.value}")

    if want_error is not None:
        with pytest.raises(want_error):
            await run(
                store,
                Options(workflow_id="prices:sleep", run_id="2026-05-02"),
                StringValue(value="AAPL"),
                StringValue,
                execute,
            )
        assert executions == 1
        return

    result = await run(
        store,
        Options(workflow_id="prices:sleep", run_id="2026-05-02"),
        StringValue(value="AAPL"),
        StringValue,
        execute,
    )
    assert result.value == "done:AAPL"
    assert executions == 1


async def test_sleep_rejects_negative_duration_without_writing_timer(
    store: OpenDALStore,
) -> None:
    workflow = Workflow(
        store,
        Options(workflow_id="prices:negative-sleep", run_id="run"),
    )

    with pytest.raises(ValueError, match="must not be negative"):
        await workflow.sleep("wait:invalid", timedelta(microseconds=-1))

    assert (
        await store.get_timer(
            TimerKey(
                workflow_id="prices:negative-sleep",
                run_id="run",
                timer_id="wait:invalid",
            )
        )
        is None
    )


@pytest.mark.parametrize(
    ("corruption", "message"),
    [
        ("missing_duration", "has no duration"),
        ("invalid_duration", "has invalid duration"),
        ("negative_duration", "has negative duration"),
        ("missing_fire_at", "has no fire_at"),
        ("invalid_fire_at", "has an invalid timestamp"),
        ("missing_created_at", "has no created_at"),
        ("invalid_created_at", "has an invalid timestamp"),
        ("scheduled_fired_at", "SCHEDULED sleep timer has fired_at"),
        ("fired_missing_fired_at", "FIRED sleep timer has no fired_at"),
        ("fired_invalid_fired_at", "FIRED sleep timer has invalid fired_at"),
        ("canceled_fired_at", "CANCELED sleep timer has fired_at"),
        ("retry_activity", "belongs to an activity retry"),
        ("unknown_status", "unknown status"),
    ],
)
async def test_sleep_replay_rejects_malformed_timer_state(
    tmp_path,
    corruption: str,
    message: str,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    workflow = Workflow(
        store,
        Options(workflow_id="prices:malformed-sleep", run_id="run"),
    )
    timer_id = "wait:malformed"
    duration = timedelta(hours=1)

    with pytest.raises(TimerPendingError):
        await workflow.sleep(timer_id, duration)

    key = TimerKey(
        workflow_id="prices:malformed-sleep",
        run_id="run",
        timer_id=timer_id,
    )
    record = await store.get_timer(key)
    assert record is not None
    if corruption == "missing_duration":
        record.ClearField("duration")
    elif corruption == "invalid_duration":
        record.duration.seconds = 315_576_000_001
    elif corruption == "negative_duration":
        record.duration.seconds = -1
    elif corruption == "missing_fire_at":
        record.ClearField("fire_at")
    elif corruption == "invalid_fire_at":
        record.fire_at.seconds = 253_402_300_800
    elif corruption == "missing_created_at":
        record.ClearField("created_at")
    elif corruption == "invalid_created_at":
        record.created_at.seconds = 253_402_300_800
    elif corruption == "scheduled_fired_at":
        record.fired_at.GetCurrentTime()
    elif corruption == "fired_missing_fired_at":
        record.status = temporaless_pb2.TIMER_STATUS_FIRED
        record.ClearField("fired_at")
    elif corruption == "fired_invalid_fired_at":
        record.status = temporaless_pb2.TIMER_STATUS_FIRED
        record.fired_at.seconds = 253_402_300_800
    elif corruption == "canceled_fired_at":
        record.status = temporaless_pb2.TIMER_STATUS_CANCELED
        record.fired_at.GetCurrentTime()
    elif corruption == "retry_activity":
        record.retry_activity_id = "fetch:vendor"
    elif corruption == "unknown_status":
        record.status = temporaless_pb2.TIMER_STATUS_UNSPECIFIED
    else:
        raise AssertionError(f"unknown corruption case {corruption}")
    # Remove the recovery shadow as part of this deliberate corruption. With a
    # valid ledger present, the store treats a lone point mismatch as an
    # interrupted/corrupt point write and serves the exact shadow instead.
    await operator.delete(_due_entry_path(key))
    await operator.write(key.path(), record.SerializeToString(deterministic=True))

    with pytest.raises(TimerConflictError, match=message):
        await workflow.sleep(timer_id, duration)


async def test_sleep_resumes_after_stored_timer_is_due(store: OpenDALStore) -> None:
    executions = 0

    async def execute(workflow: Workflow, input_message: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        await workflow.sleep("wait:vendor-window", timedelta(hours=1))
        return StringValue(value=f"done:{input_message.value}")

    with pytest.raises(TimerPendingError):
        await run(
            store,
            Options(workflow_id="prices:sleep", run_id="2026-05-02"),
            StringValue(value="AAPL"),
            StringValue,
            execute,
        )

    key = TimerKey(
        workflow_id="prices:sleep",
        run_id="2026-05-02",
        timer_id="wait:vendor-window",
    )
    record = await store.get_timer(key)
    assert record is not None
    record.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(record)

    result = await run(
        store,
        Options(workflow_id="prices:sleep", run_id="2026-05-02"),
        StringValue(value="AAPL"),
        StringValue,
        execute,
    )
    assert result.value == "done:AAPL"
    assert executions == 2


async def test_due_sleep_stays_redeliverable_through_crash_and_claim_busy(
    store: OpenDALStore,
) -> None:
    options = Options(
        workflow_id="prices:sleep-redelivery",
        run_id="2026-05-02",
        claim_owner_id="worker:one",
    )
    entered_suffix = asyncio.Event()
    release_suffix = asyncio.Event()

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        await workflow.sleep("wait:vendor-window", timedelta(hours=1))
        entered_suffix.set()
        await release_suffix.wait()
        return StringValue(value=f"done:{request.value}")

    with pytest.raises(TimerPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id="wait:vendor-window",
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(timer)

    resumed = asyncio.create_task(
        run(store, options, StringValue(value="AAPL"), StringValue, execute)
    )
    await asyncio.wait_for(entered_suffix.wait(), timeout=2)

    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    assert [item.key.timer_id for item in await due_timers(store, datetime.now(UTC))] == [
        "wait:vendor-window"
    ]

    # A duplicate dispatch cannot acquire the workflow claim, and must not
    # consume the only wakeup while the current body is still ambiguous.
    with pytest.raises(ClaimBusyError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED

    # Simulate the resumed worker disappearing before a durable successor.
    resumed.cancel()
    with pytest.raises(asyncio.CancelledError):
        await resumed
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS

    release_suffix.set()
    result = await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert result.value == "done:AAPL"
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_FIRED
    assert await due_timers(store, datetime.now(UTC)) == []


async def test_activity_claim_busy_after_due_sleep_keeps_wakeup_scheduled(
    store: OpenDALStore,
) -> None:
    options = Options(
        workflow_id="prices:sleep-activity-busy",
        run_id="2026-05-02",
        claim_owner_id="workflow-worker",
    )
    activity_calls = 0

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        await workflow.sleep("wait:activity", timedelta(hours=1))

        async def activity(value: StringValue) -> StringValue:
            nonlocal activity_calls
            activity_calls += 1
            return StringValue(value=f"done:{value.value}")

        return await workflow.execute_activity(
            ActivityOptions(activity_id="fetch"),
            request,
            StringValue,
            activity,
        )

    with pytest.raises(TimerPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id="wait:activity",
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(timer)

    now = Timestamp()
    now.GetCurrentTime()
    expires_at = Timestamp()
    expires_at.FromDatetime(datetime.now(UTC) + timedelta(minutes=15))
    activity_claim_key = ClaimKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        claim_id="activity:fetch",
    )
    assert await store.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=activity_claim_key.to_proto(),
            owner_id="other-activity-worker",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
            resource_id="fetch",
            lease_expires_at=expires_at,
            created_at=now,
            heartbeat_at=now,
        )
    )

    with pytest.raises(ClaimBusyError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert activity_calls == 0
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    assert [item.key.timer_id for item in await due_timers(store, datetime.now(UTC))] == [
        "wait:activity"
    ]

    assert await store.delete_claim(activity_claim_key)
    result = await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert result.value == "done:AAPL"
    assert activity_calls == 1
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_FIRED


async def test_later_sleep_is_durable_successor_before_due_timer_ack(
    store: OpenDALStore,
) -> None:
    options = Options(
        workflow_id="prices:sleep-successor",
        run_id="2026-05-02",
    )

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        await workflow.sleep("wait:first", timedelta(hours=1))
        await workflow.sleep("wait:second", timedelta(hours=2))
        return StringValue(value=f"done:{request.value}")

    with pytest.raises(TimerPendingError, match="wait:first"):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    first_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id="wait:first",
    )
    first = await store.get_timer(first_key)
    assert first is not None
    first.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(first)

    with pytest.raises(TimerPendingError, match="wait:second"):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    first = await store.get_timer(first_key)
    second = await store.get_timer(
        TimerKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
            timer_id="wait:second",
        )
    )
    assert first is not None
    assert second is not None
    assert first.status == temporaless_pb2.TIMER_STATUS_FIRED
    assert second.status == temporaless_pb2.TIMER_STATUS_SCHEDULED


@pytest.mark.parametrize(
    ("body_fails", "terminal_status"),
    [
        (False, temporaless_pb2.WORKFLOW_STATUS_COMPLETED),
        (True, temporaless_pb2.WORKFLOW_STATUS_FAILED),
    ],
)
async def test_failed_terminal_write_leaves_due_sleep_redeliverable(
    store: OpenDALStore,
    monkeypatch: pytest.MonkeyPatch,
    body_fails: bool,
    terminal_status: temporaless_pb2.WorkflowStatus,
) -> None:
    options = Options(
        workflow_id="prices:sleep-terminal-write",
        run_id="2026-05-02",
    )

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        await workflow.sleep("wait:failure", timedelta(hours=1))
        if body_fails:
            raise ValueError("body failed")
        return StringValue(value="done")

    with pytest.raises(TimerPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id="wait:failure",
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(timer)

    original_put_workflow = store.put_workflow

    async def fail_terminal_write(record: temporaless_pb2.WorkflowRecord) -> None:
        if record.status == terminal_status:
            raise RuntimeError("workflow store unavailable")
        await original_put_workflow(record)

    monkeypatch.setattr(store, "put_workflow", fail_terminal_write)
    with pytest.raises(RuntimeError, match="workflow store unavailable"):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    assert [item.key.timer_id for item in await due_timers(store, datetime.now(UTC))] == [
        "wait:failure"
    ]

    monkeypatch.setattr(store, "put_workflow", original_put_workflow)
    if body_fails:
        with pytest.raises(ValueError, match="body failed"):
            await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    else:
        result = await run(store, options, StringValue(value="AAPL"), StringValue, execute)
        assert result.value == "done"

    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_FIRED
    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == terminal_status


@pytest.mark.parametrize(
    ("body_fails", "terminal_status"),
    [
        (False, temporaless_pb2.WORKFLOW_STATUS_COMPLETED),
        (True, temporaless_pb2.WORKFLOW_STATUS_FAILED),
    ],
)
async def test_sleep_ack_failure_does_not_replace_terminal_outcome(
    store: OpenDALStore,
    monkeypatch: pytest.MonkeyPatch,
    body_fails: bool,
    terminal_status: temporaless_pb2.WorkflowStatus,
) -> None:
    options = Options(
        workflow_id="prices:sleep-ack-failure",
        run_id="2026-05-02",
    )

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        await workflow.sleep("wait:cleanup", timedelta(hours=1))
        if body_fails:
            raise ValueError("authoritative body failure")
        return StringValue(value="authoritative result")

    with pytest.raises(TimerPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id="wait:cleanup",
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(timer)

    original_put_timer = store.put_timer

    async def fail_fired_write(record: temporaless_pb2.TimerRecord) -> None:
        if (
            record.key.timer_id == "wait:cleanup"
            and record.status == temporaless_pb2.TIMER_STATUS_FIRED
        ):
            raise RuntimeError("timer store unavailable")
        await original_put_timer(record)

    monkeypatch.setattr(store, "put_timer", fail_fired_write)
    if body_fails:
        with pytest.raises(ValueError, match="authoritative body failure"):
            await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    else:
        result = await run(store, options, StringValue(value="AAPL"), StringValue, execute)
        assert result.value == "authoritative result"

    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == terminal_status
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED

    # The scanner treats the terminal workflow as authoritative even though
    # best-effort timer cleanup failed.
    assert await due_timers(store, datetime.now(UTC)) == []


async def test_annotations_persist_on_workflow_and_activity(store: OpenDALStore) -> None:
    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        annotate("request_symbol", request.value)

        async def fetch() -> StringValue:
            annotate("model", "claude-opus-4-7")
            annotate("tokens", "128")
            return StringValue(value=f"ok:{request.value}")

        return await workflow.run_activity(
            "fetch:annotated",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            request,
            StringValue,
            fetch,
        )

    await run(
        store,
        Options(
            workflow_id="prices:annotations",
            run_id="2026-05-02",
        ),
        StringValue(value="AAPL"),
        StringValue,
        execute,
    )

    wf_record = await store.get_workflow(
        WorkflowKey(workflow_id="prices:annotations", run_id="2026-05-02")
    )
    assert wf_record is not None
    assert wf_record.annotations["request_symbol"] == "AAPL"
    assert "model" not in wf_record.annotations

    act_record = await store.get_activity(
        ActivityKey(
            workflow_id="prices:annotations",
            run_id="2026-05-02",
            activity_id="fetch:annotated",
        )
    )
    assert act_record is not None
    assert act_record.annotations["model"] == "claude-opus-4-7"
    assert act_record.annotations["tokens"] == "128"


@pytest.mark.parametrize(
    "pending_error",
    [
        TimerPendingError("wait", datetime(2030, 1, 1, tzinfo=UTC)),
        EventPendingError("approval"),
    ],
)
async def test_workflow_annotations_survive_continuation_replay(
    store: OpenDALStore,
    pending_error: RuntimeError,
) -> None:
    executions = 0
    options = Options(
        workflow_id=f"annotations:{type(pending_error).__name__}",
        run_id="run",
    )

    async def execute(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        if executions == 1:
            annotate("phase", "planned")
            raise pending_error
        return StringValue(value="done")

    with pytest.raises(type(pending_error)):
        await run(store, options, StringValue(value="request"), StringValue, execute)

    key = WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    pending = await store.get_workflow(key)
    assert pending is not None
    assert pending.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    assert pending.annotations["phase"] == "planned"

    result = await run(store, options, StringValue(value="request"), StringValue, execute)
    assert result.value == "done"
    completed = await store.get_workflow(key)
    assert completed is not None
    assert completed.annotations["phase"] == "planned"


async def test_workflow_accessors_expose_ids(store: OpenDALStore) -> None:
    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        assert workflow.workflow_id == "prices:accessors"
        assert workflow.run_id == "2026-05-02"
        return StringValue(value="ok")

    await run(
        store,
        Options(
            workflow_id="prices:accessors",
            run_id="2026-05-02",
        ),
        StringValue(value="AAPL"),
        StringValue,
        execute,
    )


async def test_send_event_delivers_waitable_event(store: OpenDALStore) -> None:
    from temporaless.storage import send_event

    key = EventKey(
        workflow_id="prices:send-event",
        run_id="2026-05-02",
        event_id="approval",
    )
    await send_event(store, key, StringValue(value="manager"))

    record = await store.get_event(key)
    assert record is not None
    got = StringValue()
    record.payload.Unpack(got)
    assert got.value == "manager"
    assert record.received_at.seconds > 0


async def test_deliver_event_is_create_once_idempotent_and_canonical(
    store: OpenDALStore,
) -> None:
    assert await store.event_delivery_capability() == CREATE_ONLY_EVENT_DELIVERY
    key = EventKey(
        workflow_id="prices:deliver-event",
        run_id="2026-05-02",
        event_id="approval",
    )
    first_payload = Struct()
    first_payload.update(
        {
            "symbol": "AAPL",
            "prices": {"open": 200.0, "close": 204.0},
        }
    )
    first = await deliver_event(store, key, first_payload)
    assert first == temporaless_pb2.EVENT_DELIVERY_DISPOSITION_CREATED
    original = await store.get_event(key)
    assert original is not None

    retry_payload = Struct()
    retry_payload.update(
        {
            "prices": {"close": 204.0, "open": 200.0},
            "symbol": "AAPL",
        }
    )
    retry = await deliver_event(store, key, retry_payload)
    assert retry == temporaless_pb2.EVENT_DELIVERY_DISPOSITION_IDEMPOTENT
    retained = await store.get_event(key)
    assert retained is not None
    assert retained.received_at == original.received_at

    with pytest.raises(EventDeliveryConflictError) as captured:
        await deliver_event(store, key, StringValue(value="different"))
    assert captured.value.key == key


async def test_deliver_event_concurrent_conflict_has_one_winner(
    store: OpenDALStore,
) -> None:
    key = EventKey(
        workflow_id="prices:deliver-race",
        run_id="2026-05-02",
        event_id="approval",
    )
    outcomes = await asyncio.gather(
        deliver_event(store, key, StringValue(value="approved")),
        deliver_event(store, key, StringValue(value="rejected")),
        return_exceptions=True,
    )
    assert (
        sum(
            item == temporaless_pb2.EVENT_DELIVERY_DISPOSITION_CREATED
            for item in outcomes
            if isinstance(item, int)
        )
        == 1
    )
    conflicts = [item for item in outcomes if isinstance(item, EventDeliveryConflictError)]
    assert len(conflicts) == 1
    assert conflicts[0].key == key


async def test_wait_event_returns_pending_then_resumes(store: OpenDALStore) -> None:
    executions = 0

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        payload = await workflow.wait_event("approval", StringValue)
        return StringValue(value=f"approved:{payload.value}")

    options = Options(workflow_id="prices:event", run_id="2026-05-02")
    with pytest.raises(EventPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    record = await store.get_workflow(WorkflowKey(workflow_id="prices:event", run_id="2026-05-02"))
    assert record is not None
    assert record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS

    payload = Any()
    payload.Pack(StringValue(value="manager"))
    received_at = Timestamp()
    received_at.GetCurrentTime()
    event_key = EventKey(
        workflow_id="prices:event",
        run_id="2026-05-02",
        event_id="approval",
    )
    await store.put_event(
        temporaless_pb2.EventRecord(
            schema_version=EVENT_RECORD_SCHEMA_VERSION,
            key=event_key.to_proto(),
            payload=payload,
            received_at=received_at,
        )
    )

    result = await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert result.value == "approved:manager"
    assert executions == 2


async def test_wait_event_without_poll_remains_manual(store: OpenDALStore) -> None:
    options = Options(
        workflow_id="prices:event-manual",
        run_id="run",
    )

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        return await workflow.wait_event("approval", StringValue)

    with pytest.raises(EventPendingError) as captured:
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert captured.value.wake_at is None
    assert (
        await store.list_timers(
            WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id),
            temporaless_pb2.TIMER_STATUS_UNSPECIFIED,
        )
        == []
    )


async def test_wait_event_poll_schedules_reuses_and_rearms_due_timer(
    store: OpenDALStore,
) -> None:
    options = Options(
        workflow_id="prices:event-poll",
        run_id="run",
    )
    poll = PollOptions(timer_id="poll:approval", interval=_duration(timedelta(hours=1)))

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        return await workflow.wait_event("approval", StringValue, poll)

    async def run_once() -> EventPendingError:
        with pytest.raises(EventPendingError) as captured:
            await run(store, options, StringValue(value="AAPL"), StringValue, execute)
        return captured.value

    first = await run_once()
    assert first.wake_at is not None
    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id=poll.timer_id,
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.timer_kind == temporaless_pb2.TIMER_KIND_POLL
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    first_fire_at = timer.fire_at.ToDatetime(tzinfo=UTC)
    due = await due_timers(store, first_fire_at + timedelta(seconds=1))
    assert [item.key for item in due] == [timer_key]
    assert due[0].workflow.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS

    second = await run_once()
    assert second.wake_at == first.wake_at
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.fire_at.ToDatetime(tzinfo=UTC) == first_fire_at

    timer.fire_at.FromDatetime(datetime.now(UTC) - timedelta(minutes=1))
    await store.put_timer(timer)
    rearm_started = datetime.now(UTC)
    third = await run_once()
    assert third.wake_at is not None
    assert third.wake_at > rearm_started
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    assert not timer.HasField("fired_at")
    assert timer.fire_at.ToDatetime(tzinfo=UTC) == third.wake_at


async def test_resolved_poll_retains_crash_wake_then_terminal_acknowledges(
    store: OpenDALStore,
) -> None:
    from temporaless.storage import send_event

    options = Options(
        workflow_id="prices:event-poll-resolve",
        run_id="run",
    )
    poll = PollOptions(timer_id="poll:approval", interval=_duration(timedelta(hours=1)))

    async def pending(workflow: Workflow, _request: StringValue) -> StringValue:
        return await workflow.wait_event("approval", StringValue, poll)

    with pytest.raises(EventPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, pending)
    await send_event(
        store,
        EventKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
            event_id="approval",
        ),
        StringValue(value="approved"),
    )

    async def crash_after_resolve(
        workflow: Workflow,
        _request: StringValue,
    ) -> StringValue:
        await workflow.wait_event("approval", StringValue, poll)
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run(
            store,
            options,
            StringValue(value="AAPL"),
            StringValue,
            crash_after_resolve,
        )
    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id=poll.timer_id,
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED

    result = await run(store, options, StringValue(value="AAPL"), StringValue, pending)
    assert result.value == "approved"
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_FIRED
    assert timer.HasField("fired_at")


@pytest.mark.parametrize(
    ("kind", "next_interval"),
    [
        (temporaless_pb2.TIMER_KIND_SLEEP, timedelta(hours=1)),
        (temporaless_pb2.TIMER_KIND_POLL, timedelta(hours=2)),
    ],
    ids=["kind-collision", "interval-drift"],
)
async def test_wait_event_poll_rejects_collision_and_drift(
    store: OpenDALStore,
    kind: temporaless_pb2.TimerKind,
    next_interval: timedelta,
) -> None:
    options = Options(
        workflow_id="prices:event-poll-conflict",
        run_id=str(kind),
    )
    initial = PollOptions(
        timer_id="poll:approval",
        interval=_duration(timedelta(hours=1)),
    )

    async def first(workflow: Workflow, _request: StringValue) -> StringValue:
        return await workflow.wait_event("approval", StringValue, initial)

    with pytest.raises(EventPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, first)
    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id=initial.timer_id,
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.timer_kind = kind
    await store.put_timer(timer)
    changed = PollOptions(
        timer_id=initial.timer_id,
        interval=_duration(next_interval),
    )

    async def replay(workflow: Workflow, _request: StringValue) -> StringValue:
        return await workflow.wait_event("approval", StringValue, changed)

    with pytest.raises(TimerConflictError):
        await run(store, options, StringValue(value="AAPL"), StringValue, replay)


@pytest.mark.parametrize(
    "invalid",
    ["submicrosecond-interval", "non-message-factory"],
)
async def test_wait_event_poll_rejects_invalid_boundary_before_timer_mutation(
    store: OpenDALStore,
    invalid: str,
) -> None:
    options = Options(
        workflow_id="prices:event-poll-validation",
        run_id=invalid,
    )
    if invalid == "submicrosecond-interval":
        poll = PollOptions(
            timer_id="poll:approval",
            interval=Duration(nanos=1),
        )

        async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
            return await workflow.wait_event("approval", StringValue, poll)

        expected: type[BaseException] = ValueError
    else:
        poll = PollOptions(
            timer_id="poll:approval",
            interval=_duration(timedelta(hours=1)),
        )

        async def execute(workflow: Workflow, _request: StringValue) -> StringValue:  # type: ignore[no-redef]
            return await workflow.wait_event(  # type: ignore[invalid-return-type]
                "approval",
                lambda: "not-protobuf",  # type: ignore[return-value]
                poll,
            )

        expected = TypeError

    with pytest.raises(expected):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert (
        await store.list_timers(
            WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id),
            temporaless_pb2.TIMER_STATUS_UNSPECIFIED,
        )
        == []
    )


async def test_resolved_poll_rejects_corrupt_timer_without_acknowledging(
    store: OpenDALStore,
) -> None:
    from temporaless.storage import send_event

    options = Options(
        workflow_id="prices:event-poll-corrupt-resolve",
        run_id="run",
    )
    poll = PollOptions(timer_id="poll:approval", interval=_duration(timedelta(hours=1)))

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        return await workflow.wait_event("approval", StringValue, poll)

    with pytest.raises(EventPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id=poll.timer_id,
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.ClearField("created_at")
    await store.put_timer(timer)
    await send_event(
        store,
        EventKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
            event_id="approval",
        ),
        StringValue(value="approved"),
    )

    with pytest.raises(TimerConflictError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED


async def test_wait_event_rejects_corrupt_event_before_resolving_poll(
    store: OpenDALStore,
) -> None:
    options = Options(
        workflow_id="prices:event-poll-corrupt-event",
        run_id="run",
    )
    poll = PollOptions(timer_id="poll:approval", interval=_duration(timedelta(hours=1)))

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        return await workflow.wait_event("approval", StringValue, poll)

    with pytest.raises(EventPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    payload = Any()
    payload.Pack(StringValue(value="approved"), deterministic=True)
    await store.put_event(
        temporaless_pb2.EventRecord(
            schema_version=EVENT_RECORD_SCHEMA_VERSION,
            key=EventKey(
                workflow_id=options.workflow_id,
                run_id=options.run_id,
                event_id="approval",
            ).to_proto(),
            payload=payload,
            # Missing received_at is accepted only by low-level put_event.
        )
    )

    with pytest.raises(RunRecordValidationError) as captured:
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert "received_at is required" in str(captured.value)
    timer = await store.get_timer(
        TimerKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
            timer_id=poll.timer_id,
        )
    )
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED


@pytest.mark.parametrize("commit_before_error", [False, True])
async def test_poll_ambiguous_write_is_verified_and_remains_resumable(
    store: OpenDALStore,
    monkeypatch: pytest.MonkeyPatch,
    commit_before_error: bool,
) -> None:
    options = Options(
        workflow_id="prices:event-poll-ambiguous",
        run_id=str(commit_before_error),
    )
    poll = PollOptions(timer_id="poll:approval", interval=_duration(timedelta(hours=1)))
    original_put_timer = store.put_timer
    injected = False

    async def ambiguous_put(record: temporaless_pb2.TimerRecord) -> None:
        nonlocal injected
        if (
            not injected
            and record.timer_kind == temporaless_pb2.TIMER_KIND_POLL
            and record.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
        ):
            injected = True
            if commit_before_error:
                await original_put_timer(record)
            raise RuntimeError("ambiguous poll timer write")
        await original_put_timer(record)

    monkeypatch.setattr(store, "put_timer", ambiguous_put)

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        return await workflow.wait_event("approval", StringValue, poll)

    with pytest.raises(WorkflowInfrastructureError, match="ambiguous poll timer write"):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id=poll.timer_id,
    )
    assert (await store.get_timer(timer_key) is not None) is commit_before_error

    with pytest.raises(EventPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED


async def test_run_writes_in_progress_before_execution(store: OpenDALStore) -> None:
    captured: dict[str, temporaless_pb2.WorkflowStatus] = {}

    async def execute(_workflow: Workflow, request: StringValue) -> StringValue:
        record = await store.get_workflow(
            WorkflowKey(workflow_id="prices:in-progress", run_id="2026-05-02")
        )
        assert record is not None
        captured["status"] = record.status
        return StringValue(value=f"done:{request.value}")

    result = await run(
        store,
        Options(
            workflow_id="prices:in-progress",
            run_id="2026-05-02",
        ),
        StringValue(value="AAPL"),
        StringValue,
        execute,
    )
    assert result.value == "done:AAPL"
    assert captured["status"] == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS

    final = await store.get_workflow(
        WorkflowKey(workflow_id="prices:in-progress", run_id="2026-05-02")
    )
    assert final is not None
    assert final.status == temporaless_pb2.WORKFLOW_STATUS_COMPLETED


async def test_run_stores_failed_record_on_non_pending_error(store: OpenDALStore) -> None:
    async def execute(_workflow: Workflow, _request: StringValue) -> StringValue:
        raise ActivityError("boom", "explicit failure")

    with pytest.raises(ActivityError):
        await run(
            store,
            Options(workflow_id="prices:fails", run_id="2026-05-02"),
            StringValue(value="AAPL"),
            StringValue,
            execute,
        )

    record = await store.get_workflow(WorkflowKey(workflow_id="prices:fails", run_id="2026-05-02"))
    assert record is not None
    assert record.status == temporaless_pb2.WORKFLOW_STATUS_FAILED
    assert record.failure.code == "boom"

    executions = 0

    async def replay_execute(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="should-not-run")

    with pytest.raises(ActivityError) as captured:
        await run(
            store,
            Options(workflow_id="prices:fails", run_id="2026-05-02"),
            StringValue(value="AAPL"),
            StringValue,
            replay_execute,
        )
    assert executions == 0
    assert captured.value.code == "boom"


async def test_run_rejects_failed_record_without_failure(store: OpenDALStore) -> None:
    key = WorkflowKey(workflow_id="prices:malformed-failure", run_id="run")
    await store.put_workflow(
        temporaless_pb2.WorkflowRecord(
            schema_version=temporaless_pb2.RECORD_SCHEMA_VERSION_WORKFLOW,
            key=key.to_proto(),
            workflow_type=("workflow:google.protobuf.StringValue->google.protobuf.StringValue"),
            status=temporaless_pb2.WORKFLOW_STATUS_FAILED,
        )
    )
    executions = 0

    async def should_not_run(_workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="unexpected")

    with pytest.raises(WorkflowConflictError, match="has no failure"):
        await run(
            store,
            Options(workflow_id=key.workflow_id, run_id=key.run_id),
            StringValue(value="AAPL"),
            StringValue,
            should_not_run,
        )
    assert executions == 0


async def test_run_sleep_leaves_in_progress_for_resume(store: OpenDALStore) -> None:
    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        await workflow.sleep("wait:resume", timedelta(hours=1))
        return StringValue(value=f"done:{request.value}")

    with pytest.raises(TimerPendingError):
        await run(
            store,
            Options(workflow_id="prices:resume", run_id="2026-05-02"),
            StringValue(value="AAPL"),
            StringValue,
            execute,
        )

    record = await store.get_workflow(WorkflowKey(workflow_id="prices:resume", run_id="2026-05-02"))
    assert record is not None
    assert record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS


async def test_in_progress_run_resumes_with_current_handler(store: OpenDALStore) -> None:
    options = Options(workflow_id="prices:current-handler", run_id="run")
    old_activity_calls = 0
    new_activity_calls = 0
    current_handler_calls = 0

    async def old_handler(workflow: Workflow, request: StringValue) -> StringValue:
        async def old_activity(_request: StringValue) -> StringValue:
            nonlocal old_activity_calls
            old_activity_calls += 1
            return StringValue(value="stored-by-old-handler")

        await workflow.execute_activity(
            ActivityOptions(activity_id="fetch"),
            request,
            StringValue,
            old_activity,
        )
        await workflow.wait_event("approval", StringValue)
        return StringValue(value="old-handler-finished")

    with pytest.raises(EventPendingError):
        await run(
            store,
            options,
            StringValue(value="AAPL"),
            StringValue,
            old_handler,
        )

    await deliver_event(
        store,
        EventKey(
            workflow_id=options.workflow_id,
            run_id=options.run_id,
            event_id="approval",
        ),
        StringValue(value="approved"),
    )

    async def current_handler(workflow: Workflow, request: StringValue) -> StringValue:
        nonlocal current_handler_calls, new_activity_calls
        current_handler_calls += 1

        async def new_activity(_request: StringValue) -> StringValue:
            nonlocal new_activity_calls
            new_activity_calls += 1
            return StringValue(value="new-handler-activity")

        replayed = await workflow.execute_activity(
            ActivityOptions(activity_id="fetch"),
            request,
            StringValue,
            new_activity,
        )
        approval = await workflow.wait_event("approval", StringValue)
        return StringValue(value=f"current:{replayed.value}:{approval.value}")

    result = await run(
        store,
        options,
        StringValue(value="AAPL"),
        StringValue,
        current_handler,
    )

    assert result.value == "current:stored-by-old-handler:approved"
    assert old_activity_calls == 1
    assert new_activity_calls == 0
    assert current_handler_calls == 1


@pytest.mark.parametrize(
    ("failures", "max_attempts", "want_attempts"),
    [(0, 3, 1), (1, 3, 2), (2, 3, 3)],
)
async def test_activity_retries_until_success(
    store: OpenDALStore, failures: int, max_attempts: int, want_attempts: int
) -> None:
    workflow = Workflow(
        store,
        Options(
            workflow_id="prices:retry",
            run_id=f"retry-success-{failures}",
        ),
    )
    calls = 0

    async def execute() -> StringValue:
        nonlocal calls
        calls += 1
        if calls <= failures:
            raise ActivityError("rate_limited", "vendor 429")
        return StringValue(value="ok")

    duration = Duration()
    duration.FromTimedelta(timedelta(milliseconds=1))
    result = await workflow.run_activity(
        "fetch:retry",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value="AAPL"),
        StringValue,
        execute,
        RetryPolicy(maximum_attempts=max_attempts, initial_interval=duration),
    )
    assert result.value == "ok"
    assert calls == want_attempts

    record = await store.get_activity(
        ActivityKey(
            workflow_id="prices:retry",
            run_id=f"retry-success-{failures}",
            activity_id="fetch:retry",
        )
    )
    assert record is not None
    assert record.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert len(record.attempts) == want_attempts


async def test_activity_retries_exhausted_surfaces_failure(store: OpenDALStore) -> None:
    workflow = Workflow(
        store,
        Options(
            workflow_id="prices:retry-exhausted",
            run_id="2026-05-02",
        ),
    )
    calls = 0

    async def execute() -> StringValue:
        nonlocal calls
        calls += 1
        raise ActivityError("upstream_5xx", f"attempt {calls}")

    duration = Duration()
    duration.FromTimedelta(timedelta(milliseconds=1))

    with pytest.raises(ActivityError) as captured:
        await workflow.run_activity(
            "fetch:exhausted",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="AAPL"),
            StringValue,
            execute,
            RetryPolicy(maximum_attempts=3, initial_interval=duration),
        )
    assert captured.value.code == "upstream_5xx"
    assert calls == 3

    record = await store.get_activity(
        ActivityKey(
            workflow_id="prices:retry-exhausted",
            run_id="2026-05-02",
            activity_id="fetch:exhausted",
        )
    )
    assert record is not None
    assert record.status == temporaless_pb2.ACTIVITY_STATUS_FAILED
    assert len(record.attempts) == 3

    replay_calls = 0

    async def replay_execute() -> StringValue:
        nonlocal replay_calls
        replay_calls += 1
        return StringValue(value="should-not-run")

    with pytest.raises(ActivityError) as replay_captured:
        await workflow.run_activity(
            "fetch:exhausted",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="AAPL"),
            StringValue,
            replay_execute,
            RetryPolicy(maximum_attempts=3, initial_interval=duration),
        )
    assert replay_calls == 0
    assert replay_captured.value.code == "upstream_5xx"


async def test_activity_persists_retrying_record_between_attempts(store: OpenDALStore) -> None:
    """When an attempt fails with retries remaining, a RETRYING record carrying
    the attempts so far is persisted before the next attempt's sleep — so a
    process death between attempts doesn't lose the attempt history."""
    workflow = Workflow(
        store,
        Options(
            workflow_id="prices:retry-persist",
            run_id="2026-05-04",
        ),
    )

    duration = Duration()
    duration.FromTimedelta(timedelta(milliseconds=1))
    policy = RetryPolicy(maximum_attempts=3, initial_interval=duration)

    # Capture the activity record state observed mid-flight by reading it
    # from inside the second attempt's execute callback — at that point the
    # RETRYING record from attempt 1 must already be in storage.
    snapshot: list[temporaless_pb2.ActivityRecord] = []
    calls = 0

    async def execute() -> StringValue:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ActivityError("rate_limited", "transient")
        if calls == 2:
            current = await store.get_activity(
                ActivityKey(
                    workflow_id="prices:retry-persist",
                    run_id="2026-05-04",
                    activity_id="fetch:retry",
                )
            )
            assert current is not None
            snapshot.append(current)
            raise ActivityError("rate_limited", "still transient")
        return StringValue(value="ok")

    result = await workflow.run_activity(
        "fetch:retry",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value="AAPL"),
        StringValue,
        execute,
        policy,
    )
    assert result.value == "ok"
    assert calls == 3

    # The mid-flight snapshot must be RETRYING with one attempt persisted.
    assert snapshot, "expected to read the RETRYING record during attempt 2"
    mid = snapshot[0]
    assert mid.status == temporaless_pb2.ACTIVITY_STATUS_RETRYING
    assert len(mid.attempts) == 1

    final = await store.get_activity(
        ActivityKey(
            workflow_id="prices:retry-persist",
            run_id="2026-05-04",
            activity_id="fetch:retry",
        )
    )
    assert final is not None
    assert final.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert len(final.attempts) == 3


async def test_annotations_persist_across_retry_resume(store: OpenDALStore) -> None:
    """Annotations recorded in attempt 1 must survive a process death and
    appear on the final COMPLETED record after attempt 2."""
    workflow = Workflow(
        store,
        Options(
            workflow_id="prices:annotated-resume",
            run_id="2026-05-04",
        ),
    )
    activity_id = "fetch:annotated-resume"
    activity_type = "activity:google.protobuf.StringValue->google.protobuf.StringValue"
    request = StringValue(value="AAPL")

    # Seed a RETRYING record with annotations from a "previous" invocation.
    seeded_attempt = temporaless_pb2.ActivityAttempt(
        attempt=1,
        failure=temporaless_pb2.ActivityFailure(code="rate_limited", message="transient"),
    )
    seeded_attempt.started_at.GetCurrentTime()
    seeded_attempt.completed_at.GetCurrentTime()
    input_any = Any()
    input_any.Pack(request)
    policy = RetryPolicy(
        maximum_attempts=3,
        initial_interval=_duration(timedelta(milliseconds=1)),
        backoff_coefficient=1.0,
    )
    seeded = temporaless_pb2.ActivityRecord(
        schema_version=temporaless_pb2.RECORD_SCHEMA_VERSION_ACTIVITY,
        key=ActivityKey(
            workflow_id="prices:annotated-resume",
            run_id="2026-05-04",
            activity_id=activity_id,
        ).to_proto(),
        activity_type=activity_type,
        input=input_any,
        status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
        failure=seeded_attempt.failure,
        retry_policy=policy,
        attempts=[seeded_attempt],
        annotations={"vendor": "alpha", "model": "claude-haiku-4-5"},
    )
    seeded.created_at.GetCurrentTime()
    await store.put_activity(seeded)

    async def execute() -> StringValue:
        # New invocation only annotates new keys; pre-existing ones must be preserved.
        annotate("attempt_index", "2")
        return StringValue(value="ok")

    result = await workflow.run_activity(
        activity_id, activity_type, request, StringValue, execute, policy
    )
    assert result.value == "ok"

    final = await store.get_activity(
        ActivityKey(
            workflow_id="prices:annotated-resume",
            run_id="2026-05-04",
            activity_id=activity_id,
        )
    )
    assert final is not None
    assert final.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert final.annotations["vendor"] == "alpha"
    assert final.annotations["model"] == "claude-haiku-4-5"
    assert final.annotations["attempt_index"] == "2"


async def test_activity_resumes_retry_from_seeded_retrying_record(store: OpenDALStore) -> None:
    """Seed a RETRYING record with one attempt; run_activity resumes from
    attempt 2 instead of restarting from attempt 1."""
    workflow = Workflow(
        store,
        Options(
            workflow_id="prices:retry-resume",
            run_id="2026-05-04",
        ),
    )

    activity_id = "fetch:resume"
    activity_type = "activity:google.protobuf.StringValue->google.protobuf.StringValue"
    request = StringValue(value="AAPL")

    seeded_attempt = temporaless_pb2.ActivityAttempt(
        attempt=1,
        failure=temporaless_pb2.ActivityFailure(code="rate_limited", message="transient"),
    )
    seeded_attempt.started_at.GetCurrentTime()
    seeded_attempt.completed_at.GetCurrentTime()

    input_any = Any()
    input_any.Pack(request)
    policy = RetryPolicy(
        maximum_attempts=3,
        initial_interval=_duration(timedelta(milliseconds=1)),
        backoff_coefficient=1.0,
    )
    seeded = temporaless_pb2.ActivityRecord(
        schema_version=temporaless_pb2.RECORD_SCHEMA_VERSION_ACTIVITY,
        key=ActivityKey(
            workflow_id="prices:retry-resume",
            run_id="2026-05-04",
            activity_id=activity_id,
        ).to_proto(),
        activity_type=activity_type,
        input=input_any,
        status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
        failure=seeded_attempt.failure,
        retry_policy=policy,
        attempts=[seeded_attempt],
    )
    seeded.created_at.GetCurrentTime()
    await store.put_activity(seeded)

    calls = 0

    async def execute() -> StringValue:
        nonlocal calls
        calls += 1
        return StringValue(value="ok")

    result = await workflow.run_activity(
        activity_id,
        activity_type,
        request,
        StringValue,
        execute,
        policy,
    )
    assert result.value == "ok"
    assert calls == 1, "expected resume from attempt 2, only one new call needed"

    final = await store.get_activity(
        ActivityKey(
            workflow_id="prices:retry-resume",
            run_id="2026-05-04",
            activity_id=activity_id,
        )
    )
    assert final is not None
    assert final.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert len(final.attempts) == 2  # seeded a1 + new a2


async def test_activity_non_retryable_error_fails_fast(store: OpenDALStore) -> None:
    workflow = Workflow(
        store,
        Options(
            workflow_id="prices:non-retryable",
            run_id="2026-05-02",
        ),
    )
    calls = 0

    async def execute() -> StringValue:
        nonlocal calls
        calls += 1
        raise ActivityError("invalid_argument", "bad symbol")

    duration = Duration()
    duration.FromTimedelta(timedelta(milliseconds=1))

    with pytest.raises(ActivityError):
        await workflow.run_activity(
            "fetch:non-retryable",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="AAPL"),
            StringValue,
            execute,
            RetryPolicy(
                maximum_attempts=5,
                initial_interval=duration,
                non_retryable_error_codes=["invalid_argument"],
            ),
        )
    assert calls == 1


@pytest.mark.parametrize(
    ("policy", "message"),
    [
        (RetryPolicy(), "maximum_attempts"),
        (RetryPolicy(maximum_attempts=3), "initial_interval"),
        (
            RetryPolicy(
                maximum_attempts=1,
                initial_interval=_duration(timedelta(seconds=-1)),
            ),
            "initial_interval must be >= 0",
        ),
        (
            RetryPolicy(
                maximum_attempts=2,
                initial_interval=_duration(timedelta(seconds=1)),
                backoff_coefficient=-1,
            ),
            "backoff_coefficient",
        ),
        (
            RetryPolicy(
                maximum_attempts=2,
                initial_interval=_duration(timedelta(seconds=1)),
                backoff_coefficient=float("nan"),
            ),
            "backoff_coefficient",
        ),
        (
            RetryPolicy(
                maximum_attempts=2,
                initial_interval=_duration(timedelta(seconds=1)),
                backoff_coefficient=float("inf"),
            ),
            "backoff_coefficient",
        ),
        (
            RetryPolicy(
                maximum_attempts=2,
                initial_interval=_duration(timedelta(seconds=1)),
                maximum_interval=_duration(timedelta(seconds=-1)),
            ),
            "maximum_interval must be >= 0",
        ),
        (
            RetryPolicy(
                maximum_attempts=2,
                initial_interval=_duration(timedelta(seconds=10)),
                maximum_interval=_duration(timedelta(seconds=5)),
            ),
            "maximum_interval must be >= initial_interval",
        ),
        (
            RetryPolicy(
                maximum_attempts=2,
                initial_interval=_duration(timedelta(seconds=1)),
                durable_backoff_threshold=_duration(timedelta(seconds=-1)),
            ),
            "durable_backoff_threshold",
        ),
    ],
)
async def test_activity_invalid_retry_policy_rejected(
    store: OpenDALStore,
    policy: RetryPolicy,
    message: str,
) -> None:
    workflow = Workflow(
        store,
        Options(
            workflow_id="prices:bad-policy",
            run_id=f"bad-policy-{policy.maximum_attempts}",
        ),
    )
    with pytest.raises(ValueError, match=message):
        await workflow.run_activity(
            "fetch:bad",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="AAPL"),
            StringValue,
            lambda: StringValue(value="ok"),
            policy,
        )


async def test_try_create_claim_is_atomic_create_only(store: OpenDALStore) -> None:
    created_at = Timestamp()
    created_at.GetCurrentTime()
    expires_at = Timestamp()
    expires_at.FromDatetime(datetime.now(UTC) + timedelta(minutes=5))
    key = ClaimKey(
        workflow_id="prices:claim",
        run_id="2026-05-02",
        claim_id="activity:fetch:symbol",
    )

    first = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=key.to_proto(),
        owner_id="first-owner",
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
        resource_id="fetch:symbol",
        lease_expires_at=expires_at,
        created_at=created_at,
        heartbeat_at=created_at,
    )
    second = temporaless_pb2.ClaimRecord()
    second.CopyFrom(first)
    second.owner_id = "second-owner"

    assert await store.try_create_claim(first) is True
    assert await store.try_create_claim(second) is False

    stored = await store.get_claim(key)
    assert stored is not None
    assert stored.owner_id == "first-owner"


async def test_partial_fanout_persists_completed_activities_when_one_branch_fails(
    store: OpenDALStore,
) -> None:
    """A structured fan-out where one branch's activity fails
    after retries leaves the SUCCEEDED branches with persisted COMPLETED
    records. The workflow as a whole becomes FAILED, but on reset+rerun the
    succeeded branches replay from storage rather than re-executing.

    This is the production case for parallel-fanout pipelines: a single bad
    partition shouldn't redo the entire fan-out.
    """
    duration = Duration()
    duration.FromTimedelta(timedelta(milliseconds=1))
    policy = RetryPolicy(maximum_attempts=2, initial_interval=duration)

    fetch_calls: dict[str, int] = {"AAPL": 0, "MSFT": 0, "BAD": 0}

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        async def fetch_one(symbol: str) -> StringValue:
            async def fetch(req: StringValue) -> StringValue:
                fetch_calls[req.value] += 1
                if req.value == "BAD":
                    raise ActivityError("upstream_5xx", "permanent vendor failure")
                return StringValue(value=f"price:{req.value}")

            return await workflow.execute_activity(
                ActivityOptions(activity_id=f"fetch:{symbol}", retry_policy=policy),
                StringValue(value=symbol),
                StringValue,
                fetch,
            )

        results = await gather_activities(
            fetch_one("AAPL"),
            fetch_one("MSFT"),
            fetch_one("BAD"),
        )
        return StringValue(value=",".join(r.value for r in results))

    options = Options(
        workflow_id="prices:partial-gather",
        run_id="2026-05-04",
    )
    with pytest.raises(ActivityError):
        await run(store, options, StringValue(value="batch"), StringValue, execute)

    # AAPL and MSFT each should have completed exactly once. BAD is the one
    # that exhausted retries (max 2 attempts).
    assert fetch_calls == {"AAPL": 1, "MSFT": 1, "BAD": 2}

    # The workflow record is now FAILED.
    wf_record = await store.get_workflow(
        WorkflowKey(workflow_id="prices:partial-gather", run_id="2026-05-04")
    )
    assert wf_record is not None
    assert wf_record.status == temporaless_pb2.WORKFLOW_STATUS_FAILED

    # AAPL and MSFT activity records are COMPLETED — they aren't re-executed
    # if we re-invoke the workflow before resetting it. (Replay sees FAILED
    # and short-circuits to the failure.)
    activities = await store.list_activities(
        WorkflowKey(workflow_id="prices:partial-gather", run_id="2026-05-04")
    )
    activity_status = {a.key.activity_id: a.status for a in activities}
    assert activity_status["fetch:AAPL"] == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert activity_status["fetch:MSFT"] == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert activity_status["fetch:BAD"] == temporaless_pb2.ACTIVITY_STATUS_FAILED


async def test_cross_workflow_dependency_via_record_read(
    store: OpenDALStore,
) -> None:
    """Pattern documented in docs/comparisons.md (data-pipelining patterns):
    workflow B reads workflow A's record. If A isn't COMPLETED yet, B raises
    EventPendingError and stays IN_PROGRESS. When A completes, B's next
    invocation finds the COMPLETED record and proceeds.
    """
    upstream_options = Options(workflow_id="upstream:fetch", run_id="2026-05-04")
    downstream_options = Options(workflow_id="downstream:transform", run_id="2026-05-04")

    async def upstream_workflow(workflow: Workflow, request: StringValue) -> StringValue:
        async def fetch() -> StringValue:
            return StringValue(value=f"raw:{request.value}")

        return await workflow.run_activity(
            "fetch",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            request,
            StringValue,
            fetch,
        )

    async def downstream_workflow(workflow: Workflow, request: StringValue) -> StringValue:
        upstream_record = await store.get_workflow(
            WorkflowKey(workflow_id="upstream:fetch", run_id="2026-05-04")
        )
        if (
            upstream_record is None
            or upstream_record.status != temporaless_pb2.WORKFLOW_STATUS_COMPLETED
        ):
            raise EventPendingError("upstream:fetch")

        upstream_value = StringValue()
        upstream_record.result.Unpack(upstream_value)

        async def transform() -> StringValue:
            return StringValue(value=f"transformed:{upstream_value.value}|{request.value}")

        return await workflow.run_activity(
            "transform",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            request,
            StringValue,
            transform,
        )

    # Process 1: downstream runs before upstream — raises EventPendingError.
    with pytest.raises(EventPendingError):
        await run(
            store,
            downstream_options,
            StringValue(value="batch"),
            StringValue,
            downstream_workflow,
        )

    # Process 2: upstream completes.
    upstream_result = await run(
        store,
        upstream_options,
        StringValue(value="AAPL"),
        StringValue,
        upstream_workflow,
    )
    assert upstream_result.value == "raw:AAPL"

    # Process 3: downstream resumes, finds upstream's record, proceeds.
    downstream_result = await run(
        store,
        downstream_options,
        StringValue(value="batch"),
        StringValue,
        downstream_workflow,
    )
    assert downstream_result.value == "transformed:raw:AAPL|batch"


async def test_long_running_workflow_durable_across_simulated_process_deaths(
    store: OpenDALStore,
) -> None:
    """Locks in the long-running durable workflow invariant: a workflow that
    fetches → sleeps → waits-on-event → finalizes survives multiple "process
    deaths" between boundaries. Each ``run()`` invocation is a separate
    process; storage is the only state that crosses them.

    This is the proof-by-test that we support long-running workflows. The
    same shape works for 7-day approval flows and multi-day vendor reconciliation.
    """
    from datetime import UTC, datetime, timedelta

    from temporaless.storage import TimerKey, send_event

    options = Options(
        workflow_id="prices:long-running",
        run_id="2026-05-04",
    )

    fetch_calls = 0
    finalize_calls = 0

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        async def fetch(req: StringValue) -> StringValue:
            nonlocal fetch_calls
            fetch_calls += 1
            return StringValue(value=f"fetched:{req.value}")

        intermediate = await workflow.execute_activity(
            ActivityOptions(activity_id="fetch:initial"),
            request,
            StringValue,
            fetch,
        )

        # Durable sleep — workflow stays IN_PROGRESS, body re-enters on resume.
        await workflow.sleep("cooldown", timedelta(hours=1))

        # Durable wait — blocks until external service delivers the event.
        approval = await workflow.wait_event("approval", StringValue)

        async def finalize(req: StringValue) -> StringValue:
            nonlocal finalize_calls
            finalize_calls += 1
            return StringValue(value=f"final:{intermediate.value}:{approval.value}:{req.value}")

        return await workflow.execute_activity(
            ActivityOptions(activity_id="finalize:run"),
            request,
            StringValue,
            finalize,
        )

    # Process 1: runs the initial activity, hits the sleep, raises TimerPendingError.
    with pytest.raises(TimerPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert fetch_calls == 1
    assert finalize_calls == 0

    # Process death between calls. State lives only in storage.
    # Process 2: timer scanner fires the timer, workflow re-invoked.
    timer_key = TimerKey(
        workflow_id="prices:long-running", run_id="2026-05-04", timer_id="cooldown"
    )
    timer_record = await store.get_timer(timer_key)
    assert timer_record is not None
    timer_record.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(timer_record)

    # Now the workflow body re-runs: fetch short-circuits (replay), the due
    # sleep returns while retaining its wakeup, and wait_event raises
    # EventPendingError. There is no durable event-wait record, so the sleep
    # timer remains scanner-visible until a later invocation reaches a durable
    # successor or terminal workflow record.
    with pytest.raises(EventPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert fetch_calls == 1, "fetch must NOT re-execute on resume"
    assert finalize_calls == 0
    timer_record = await store.get_timer(timer_key)
    assert timer_record is not None
    assert timer_record.status == temporaless_pb2.TIMER_STATUS_SCHEDULED

    # Process death again. External service delivers the approval event.
    await send_event(
        store,
        EventKey(
            workflow_id="prices:long-running",
            run_id="2026-05-04",
            event_id="approval",
        ),
        StringValue(value="manager"),
    )

    # Process 3: workflow re-invoked, runs to completion. fetch + sleep replay
    # from records; wait_event finds the delivered event; finalize runs.
    result = await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert result.value == "final:fetched:AAPL:manager:AAPL"
    assert fetch_calls == 1
    assert finalize_calls == 1
    timer_record = await store.get_timer(timer_key)
    assert timer_record is not None
    assert timer_record.status == temporaless_pb2.TIMER_STATUS_FIRED

    # Process 4: replay returns the cached result with no body re-execution.
    async def assert_no_replay(_w: Workflow, _r: StringValue) -> StringValue:
        raise AssertionError("workflow body must not re-execute after COMPLETED")

    replayed = await run(store, options, StringValue(value="AAPL"), StringValue, assert_no_replay)
    assert replayed.value == "final:fetched:AAPL:manager:AAPL"


async def test_current_workflow_outside_workflow_raises() -> None:
    """``current_workflow()`` is a programming-error guard — calling it
    outside ``run`` should fail fast, not return a stale value."""
    from temporaless.workflow import current_workflow

    with pytest.raises(RuntimeError, match="outside a workflow"):
        current_workflow()


async def test_current_workflow_propagates_through_asyncio_gather(
    store: OpenDALStore,
) -> None:
    """The contextvar carrying the in-flight Workflow must propagate into tasks
    spawned by ``asyncio.gather``. This is load-bearing for the parallel-fanout
    pattern — every per-symbol activity branch needs to find its own Workflow.
    """
    import asyncio

    from temporaless.workflow import current_workflow

    seen: list[str] = []

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        async def branch(symbol: str) -> StringValue:
            # current_workflow() inside a gather-spawned task must return the
            # SAME Workflow that the parent execute is bound to.
            assert current_workflow() is workflow
            seen.append(symbol)

            async def fetch(req: StringValue) -> StringValue:
                return StringValue(value=f"price:{req.value}")

            return await current_workflow().execute_activity(
                ActivityOptions(activity_id=f"fetch:{symbol}"),
                StringValue(value=symbol),
                StringValue,
                fetch,
            )

        results = await asyncio.gather(*(branch(s) for s in ("AAPL", "MSFT", "GOOG")))
        return StringValue(value=",".join(r.value for r in results))

    options = Options(workflow_id="prices:gather", run_id="2026-05-04")
    result = await run(store, options, StringValue(value="batch"), StringValue, execute)
    assert result.value == "price:AAPL,price:MSFT,price:GOOG"
    assert sorted(seen) == ["AAPL", "GOOG", "MSFT"]


async def test_cancellation_does_not_persist_failed_records(store: OpenDALStore) -> None:
    """Asyncio cancellation is a shutdown signal, not a vendor failure. The
    workflow record should stay IN_PROGRESS (resumable on next invocation),
    and no FAILED activity record should be written."""
    import asyncio

    entered = asyncio.Event()

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        async def slow_fetch(req: StringValue) -> StringValue:
            entered.set()
            await asyncio.sleep(60)
            return StringValue(value=f"never:{req.value}")

        return await workflow.execute_activity(
            ActivityOptions(activity_id="fetch:cancel"),
            request,
            StringValue,
            slow_fetch,
        )

    options = Options(workflow_id="prices:cancel", run_id="2026-05-04")
    task = asyncio.create_task(run(store, options, StringValue(value="AAPL"), StringValue, execute))
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Workflow record should be IN_PROGRESS (resumable), not FAILED.
    wf_record = await store.get_workflow(
        WorkflowKey(workflow_id="prices:cancel", run_id="2026-05-04")
    )
    assert wf_record is not None
    assert wf_record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS

    # No activity record should have been persisted (no completed attempt).
    act_record = await store.get_activity(
        ActivityKey(
            workflow_id="prices:cancel",
            run_id="2026-05-04",
            activity_id="fetch:cancel",
        )
    )
    assert act_record is None


async def test_parallel_activities_via_asyncio_gather(store: OpenDALStore) -> None:
    """Quant/ML fanout: ``asyncio.gather`` over ``Workflow.execute_activity`` runs
    activities concurrently. Each activity has a distinct activity_id so replay
    semantics still hold per-activity. On replay all activities short-circuit
    from their stored records — confirms gather composes with replay correctly.
    """
    import asyncio

    symbols = ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA"]
    fetch_count = 0

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        async def fetch_one(symbol: str) -> StringValue:
            async def fetch(req: StringValue) -> StringValue:
                nonlocal fetch_count
                fetch_count += 1
                return StringValue(value=f"price:{req.value}")

            return await workflow.execute_activity(
                ActivityOptions(activity_id=f"fetch:{symbol}"),
                StringValue(value=symbol),
                StringValue,
                fetch,
            )

        prices = await asyncio.gather(*(fetch_one(s) for s in symbols))
        return StringValue(value=",".join(p.value for p in prices))

    options = Options(workflow_id="prices:fanout", run_id="2026-05-04")
    first = await run(store, options, StringValue(value="batch"), StringValue, execute)
    assert first.value == "price:AAPL,price:MSFT,price:GOOG,price:TSLA,price:NVDA"
    assert fetch_count == len(symbols)

    # Replay: every activity short-circuits on its stored record. No new fetches.
    second = await run(store, options, StringValue(value="batch"), StringValue, execute)
    assert second.value == first.value
    assert fetch_count == len(symbols)


@pytest.mark.parametrize(
    ("commit_before_error", "first_error_type", "first_timer_exists"),
    [
        (False, WorkflowInfrastructureError, False),
        (True, TimerPendingError, True),
    ],
    ids=["before-commit", "after-commit"],
)
async def test_sleep_ambiguous_write_is_verified_and_remains_resumable(
    store: OpenDALStore,
    monkeypatch: pytest.MonkeyPatch,
    commit_before_error: bool,
    first_error_type: type[Exception],
    first_timer_exists: bool,
) -> None:
    options = Options(
        workflow_id=f"prices:sleep-ambiguous:{commit_before_error}",
        run_id="run",
    )
    original_put_timer = store.put_timer
    injected = False

    async def ambiguous_put(record: temporaless_pb2.TimerRecord) -> None:
        nonlocal injected
        if (
            not injected
            and record.timer_kind == temporaless_pb2.TIMER_KIND_SLEEP
            and record.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
        ):
            injected = True
            if commit_before_error:
                await original_put_timer(record)
            raise RuntimeError("ambiguous sleep timer write")
        await original_put_timer(record)

    monkeypatch.setattr(store, "put_timer", ambiguous_put)

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        await workflow.sleep("wake", timedelta(hours=1))
        return StringValue(value="done")

    with pytest.raises(first_error_type) as first_error:
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    if commit_before_error:
        assert isinstance(first_error.value.__cause__, WorkflowInfrastructureError)
    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS

    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id="wake",
    )
    assert (await store.get_timer(timer_key) is not None) is first_timer_exists

    # The requester's retry repairs a definite before-commit miss; a verified
    # after-commit write replays the same wake. Both paths become scheduler
    # visible and do not depend on a live worker process.
    with pytest.raises(TimerPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await original_put_timer(timer)
    due = await due_timers(store, datetime.now(UTC))
    assert [item.key for item in due] == [timer_key]


async def test_sleep_ledger_first_crash_replays_exact_original_deadline(
    store: OpenDALStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = Options(
        workflow_id="prices:sleep-ledger-first",
        run_id="run",
    )
    duration = timedelta(days=30)
    captured = temporaless_pb2.TimerRecord()
    injected = False
    original_put_timer = store.put_timer

    async def crash_between_ledger_and_point(
        record: temporaless_pb2.TimerRecord,
    ) -> None:
        nonlocal injected
        if not injected and record.status == temporaless_pb2.TIMER_STATUS_SCHEDULED:
            injected = True
            captured.CopyFrom(record)
            await store._put_due_entry(record)
            raise RuntimeError("process died after the ledger write")
        await original_put_timer(record)

    monkeypatch.setattr(store, "put_timer", crash_between_ledger_and_point)

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        await workflow.sleep("wake", duration)
        return StringValue(value="done")

    with pytest.raises(TimerPendingError) as first_pending:
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    timer_key = TimerKey(
        workflow_id=options.workflow_id,
        run_id=options.run_id,
        timer_id="wake",
    )
    assert not await store._operator.exists(timer_key.path())
    recovered = await store.get_timer(timer_key)
    assert recovered is not None
    assert recovered == captured
    assert recovered.timer_kind == temporaless_pb2.TIMER_KIND_SLEEP
    assert recovered.duration.ToTimedelta() == duration
    assert recovered.fire_at == captured.fire_at
    assert first_pending.value.wake_at == captured.fire_at.ToDatetime().replace(tzinfo=UTC)
    assert await store.list_timers(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id),
        temporaless_pb2.TIMER_STATUS_UNSPECIFIED,
    ) == [captured]

    # A new invocation prefetches the ledger-only record. It must use the
    # original deadline rather than treating the missing canonical point as a
    # new 30-day sleep starting at replay time.
    monkeypatch.setattr(store, "put_timer", original_put_timer)
    with pytest.raises(TimerPendingError) as replay_pending:
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert replay_pending.value.wake_at == first_pending.value.wake_at
    assert not await store._operator.exists(timer_key.path())


@pytest.mark.parametrize("primitive", ["activity-read", "activity-write", "event"])
async def test_workflow_primitive_storage_failure_remains_in_progress(
    store: OpenDALStore,
    monkeypatch: pytest.MonkeyPatch,
    primitive: str,
) -> None:
    options = Options(
        workflow_id=f"prices:primitive-storage:{primitive}",
        run_id="run",
    )

    async def fail_read(_key: ActivityKey | EventKey) -> None:
        raise RuntimeError("record store unavailable")

    async def fail_activity_write(_record: temporaless_pb2.ActivityRecord) -> None:
        raise RuntimeError("record store unavailable")

    if primitive == "activity-read":
        monkeypatch.setattr(store, "get_activity", fail_read)
    elif primitive == "activity-write":
        monkeypatch.setattr(store, "put_activity", fail_activity_write)
    else:
        monkeypatch.setattr(store, "get_event", fail_read)

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        if primitive == "event":
            return await workflow.wait_event("approval", StringValue)

        async def activity(_request: StringValue) -> StringValue:
            if primitive == "activity-read":
                pytest.fail("activity body must not run after its storage read failed")
            return StringValue(value="completed-before-write-failed")

        return await workflow.execute_activity(
            ActivityOptions(activity_id="fetch"),
            request,
            StringValue,
            activity,
        )

    with pytest.raises(WorkflowInfrastructureError, match="record store unavailable"):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS


async def test_activity_business_error_cannot_masquerade_as_workflow_pending(
    store: OpenDALStore,
) -> None:
    options = Options(
        workflow_id="prices:activity-business-pending",
        run_id="run",
    )

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        async def activity(_request: StringValue) -> StringValue:
            raise TimerPendingError("user-value", datetime.now(UTC) + timedelta(hours=1))

        return await workflow.execute_activity(
            ActivityOptions(activity_id="business"),
            request,
            StringValue,
            activity,
        )

    with pytest.raises(ActivityError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)

    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)
    )
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_FAILED
