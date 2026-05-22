from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.any_pb2 import Any
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue
from protovalidate import ValidationError

from temporaless.storage import (
    CLAIM_RECORD_SCHEMA_VERSION,
    CREATE_ONLY_CLAIMS,
    EVENT_RECORD_SCHEMA_VERSION,
    ActivityKey,
    ClaimKey,
    EventKey,
    OpenDALStore,
    TimerKey,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ActivityConflictError,
    ActivityError,
    ActivityOptions,
    ActivityWrapOptions,
    ClaimBusyError,
    EventPendingError,
    Options,
    RetryPolicy,
    TimerPendingError,
    Workflow,
    WorkflowConflictError,
    WorkflowWrapOptions,
    annotate,
    run,
    wrap_activity,
    wrap_workflow,
)


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


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
        Options(workflow_id="prices:symbol", run_id="2026-05-02", code_version="test"),
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
        code_version="test",
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
            code_version="test",
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
        Options(workflow_id="prices:symbol", run_id="2026-05-02", code_version="test"),
        StringValue(value=first_input),
        StringValue,
        execute,
    )
    assert first.value == f"workflow:normalized:{first_input}"

    if want_error is not None:
        with pytest.raises(want_error):
            await run(
                store,
                Options(workflow_id="prices:symbol", run_id="2026-05-02", code_version="test"),
                StringValue(value=next_input),
                StringValue,
                execute,
            )
        assert executions == 1
        return

    second = await run(
        store,
        Options(workflow_id="prices:symbol", run_id="2026-05-02", code_version="test"),
        StringValue(value=next_input),
        StringValue,
        execute,
    )
    assert second.value == want
    assert executions == 1


@pytest.mark.parametrize("workflow_id", ["prices/aapl", "", ".", ".."])
async def test_activity_key_rejects_invalid_workflow_ids(workflow_id: str) -> None:
    with pytest.raises(ValidationError):
        ActivityKey(workflow_id=workflow_id, run_id="2026-05-02", activity_id="fetch").path()


@pytest.mark.parametrize(
    ("options", "want_error"),
    [
        (Options(workflow_id="prices:ids", run_id="", code_version="test"), ValidationError),
        (
            Options(
                workflow_id="prices:ids",
                run_id="2026-05-02",
                code_version="test",
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
                    code_version="test",
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
                    code_version="test",
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
            code_version="test",
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
                Options(workflow_id="prices:sleep", run_id="2026-05-02", code_version="test"),
                StringValue(value="AAPL"),
                StringValue,
                execute,
            )
        assert executions == 1
        return

    result = await run(
        store,
        Options(workflow_id="prices:sleep", run_id="2026-05-02", code_version="test"),
        StringValue(value="AAPL"),
        StringValue,
        execute,
    )
    assert result.value == "done:AAPL"
    assert executions == 1


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
            Options(workflow_id="prices:sleep", run_id="2026-05-02", code_version="test"),
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
        Options(workflow_id="prices:sleep", run_id="2026-05-02", code_version="test"),
        StringValue(value="AAPL"),
        StringValue,
        execute,
    )
    assert result.value == "done:AAPL"
    assert executions == 2


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
            code_version="test",
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


async def test_workflow_accessors_expose_ids(store: OpenDALStore) -> None:
    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        assert workflow.workflow_id == "prices:accessors"
        assert workflow.run_id == "2026-05-02"
        assert workflow.code_version == "v42"
        return StringValue(value="ok")

    await run(
        store,
        Options(
            workflow_id="prices:accessors",
            run_id="2026-05-02",
            code_version="v42",
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


async def test_wait_event_returns_pending_then_resumes(store: OpenDALStore) -> None:
    executions = 0

    async def execute(workflow: Workflow, _request: StringValue) -> StringValue:
        nonlocal executions
        executions += 1
        payload = await workflow.wait_event("approval", StringValue)
        return StringValue(value=f"approved:{payload.value}")

    options = Options(workflow_id="prices:event", run_id="2026-05-02", code_version="test")
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
            code_version="test",
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
            Options(workflow_id="prices:fails", run_id="2026-05-02", code_version="test"),
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
            Options(workflow_id="prices:fails", run_id="2026-05-02", code_version="test"),
            StringValue(value="AAPL"),
            StringValue,
            replay_execute,
        )
    assert executions == 0
    assert captured.value.code == "boom"


async def test_run_sleep_leaves_in_progress_for_resume(store: OpenDALStore) -> None:
    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        await workflow.sleep("wait:resume", timedelta(hours=1))
        return StringValue(value=f"done:{request.value}")

    with pytest.raises(TimerPendingError):
        await run(
            store,
            Options(workflow_id="prices:resume", run_id="2026-05-02", code_version="test"),
            StringValue(value="AAPL"),
            StringValue,
            execute,
        )

    record = await store.get_workflow(WorkflowKey(workflow_id="prices:resume", run_id="2026-05-02"))
    assert record is not None
    assert record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS


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
            code_version="test",
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
            code_version="test",
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
            code_version="test",
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
            code_version="test",
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
    seeded = temporaless_pb2.ActivityRecord(
        schema_version=temporaless_pb2.RECORD_SCHEMA_VERSION_ACTIVITY,
        key=ActivityKey(
            workflow_id="prices:annotated-resume",
            run_id="2026-05-04",
            activity_id=activity_id,
        ).to_proto(),
        activity_type=activity_type,
        code_version="test",
        input=input_any,
        status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
        attempts=[seeded_attempt],
        annotations={"vendor": "alpha", "model": "claude-haiku-4-5"},
    )
    seeded.created_at.GetCurrentTime()
    await store.put_activity(seeded)

    duration = Duration()
    duration.FromTimedelta(timedelta(milliseconds=1))
    policy = RetryPolicy(maximum_attempts=3, initial_interval=duration)

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
            code_version="test",
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
    seeded = temporaless_pb2.ActivityRecord(
        schema_version=temporaless_pb2.RECORD_SCHEMA_VERSION_ACTIVITY,
        key=ActivityKey(
            workflow_id="prices:retry-resume",
            run_id="2026-05-04",
            activity_id=activity_id,
        ).to_proto(),
        activity_type=activity_type,
        code_version="test",
        input=input_any,
        status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
        attempts=[seeded_attempt],
    )
    seeded.created_at.GetCurrentTime()
    await store.put_activity(seeded)

    duration = Duration()
    duration.FromTimedelta(timedelta(milliseconds=1))
    policy = RetryPolicy(maximum_attempts=3, initial_interval=duration)

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
            code_version="test",
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
    "policy",
    [
        RetryPolicy(),
        RetryPolicy(maximum_attempts=3),
    ],
)
async def test_activity_invalid_retry_policy_rejected(
    store: OpenDALStore, policy: RetryPolicy
) -> None:
    workflow = Workflow(
        store,
        Options(
            workflow_id="prices:bad-policy",
            run_id=f"bad-policy-{policy.maximum_attempts}",
            code_version="test",
        ),
    )
    with pytest.raises(ValueError):
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
        code_version="test",
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


async def test_wrap_workflow_method_decorates_grpc_handler(store: OpenDALStore) -> None:
    """The ``wrap_workflow_method`` decorator turns a ConnectRPC unary method
    `(self, req, ctx) -> resp` into a workflow without touching the method
    signature. Inside the body, ``current_workflow()`` returns the active
    Workflow so activities can be called without threading it through.
    """
    from temporaless.workflow import current_workflow, wrap_workflow_method

    class PriceService:
        """Mimics a ConnectRPC service. The handler shape is unchanged —
        only the decorator is added."""

        def __init__(self, store: OpenDALStore) -> None:
            self._store = store
            self.vendor_calls = 0

        @wrap_workflow_method(
            store=lambda self: self._store,  # type: ignore[attr-defined]
            result_type=StringValue,
            options_for=lambda _self, request: Options(
                workflow_id=f"prices:{request.value}",
                run_id="2026-05-04",
                code_version="v1",
            ),
        )
        async def fetch_prices(self, request: StringValue, ctx: object = None) -> StringValue:
            async def vendor(req: StringValue) -> StringValue:
                self.vendor_calls += 1
                return StringValue(value=f"vendor:{req.value}")

            return await current_workflow().execute_activity(
                ActivityOptions(activity_id=f"fetch:{request.value}"),
                request,
                StringValue,
                vendor,
            )

    service = PriceService(store)

    # First invocation: vendor activity runs, workflow + activity records persisted.
    first = await service.fetch_prices(StringValue(value="AAPL"))
    assert first.value == "vendor:AAPL"
    assert service.vendor_calls == 1

    # Second invocation with the same fingerprint: replay short-circuits.
    # No vendor calls, but the result matches.
    second = await service.fetch_prices(StringValue(value="AAPL"))
    assert second.value == "vendor:AAPL"
    assert service.vendor_calls == 1


async def test_workflow_error_to_connect_code_maps_each_error_type() -> None:
    """The helper produces (Code, message) for the typed workflow errors so
    ConnectRPC handlers can re-raise as ConnectError consistently."""
    from connectrpc.code import Code

    from temporaless.workflow import workflow_error_to_connect_code

    cases: list[tuple[BaseException, object]] = [
        (TimerPendingError("t1", datetime.now(UTC)), Code.UNAVAILABLE),
        (EventPendingError("e1"), Code.UNAVAILABLE),
        (ClaimBusyError("activity:fetch"), Code.ALREADY_EXISTS),
        (WorkflowConflictError("digest mismatch"), Code.FAILED_PRECONDITION),
        (ActivityConflictError("status mismatch"), Code.FAILED_PRECONDITION),
        (ActivityError("rate_limited", "vendor 429"), Code.INTERNAL),
    ]
    for exc, want_code in cases:
        result = workflow_error_to_connect_code(exc)
        assert result is not None, f"expected mapping for {type(exc).__name__}"
        code, message = result
        assert code is want_code, f"{type(exc).__name__}: got {code}, want {want_code}"
        assert message  # message is non-empty

    # Unknown exception types return None — caller decides.
    assert workflow_error_to_connect_code(ValueError("foo")) is None


async def test_wrap_workflow_method_auto_maps_timer_pending_to_connect_error(
    store: OpenDALStore,
) -> None:
    """``wrap_workflow_method`` translates framework typed errors to
    ``ConnectError`` automatically — Connect clients see the right gRPC code
    without users having to remember to wrap. The original ``__cause__`` is
    preserved so callers can introspect the underlying type."""
    from connectrpc.code import Code
    from connectrpc.errors import ConnectError

    from temporaless.workflow import current_workflow, wrap_workflow_method

    class SleepingService:
        def __init__(self, s: OpenDALStore) -> None:
            self._store = s

        @wrap_workflow_method(
            store=lambda self: self._store,  # type: ignore[attr-defined]
            result_type=StringValue,
            options_for=lambda _self, request: Options(
                workflow_id=f"sleep:{request.value}",
                run_id="2026-05-04",
                code_version="v1",
            ),
        )
        async def take_a_nap(self, _request: StringValue, _ctx: object = None) -> StringValue:
            await current_workflow().sleep("wait", timedelta(hours=1))
            return StringValue(value="never")

    service = SleepingService(store)

    with pytest.raises(ConnectError) as excinfo:
        await service.take_a_nap(StringValue(value="AAPL"))

    assert excinfo.value.code is Code.UNAVAILABLE
    # Underlying typed error preserved via raise … from exc.
    assert isinstance(excinfo.value.__cause__, TimerPendingError)


async def test_wrap_workflow_method_passes_through_unknown_exceptions(
    store: OpenDALStore,
) -> None:
    """Non-framework exceptions are not wrapped — they propagate so users can
    see their own application errors with full traceback."""
    from temporaless.workflow import wrap_workflow_method

    class BrokenService:
        def __init__(self, s: OpenDALStore) -> None:
            self._store = s

        @wrap_workflow_method(
            store=lambda self: self._store,  # type: ignore[attr-defined]
            result_type=StringValue,
            options_for=lambda _self, request: Options(
                workflow_id=f"broken:{request.value}",
                run_id="2026-05-04",
                code_version="v1",
            ),
        )
        async def break_things(self, _request: StringValue, _ctx: object = None) -> StringValue:
            raise RuntimeError("custom application error")

    service = BrokenService(store)
    with pytest.raises(RuntimeError, match="custom application error"):
        await service.break_things(StringValue(value="AAPL"))


async def test_partial_gather_persists_completed_activities_when_one_branch_fails(
    store: OpenDALStore,
) -> None:
    """A workflow body using asyncio.gather where one branch's activity fails
    after retries leaves the SUCCEEDED branches with persisted COMPLETED
    records. The workflow as a whole becomes FAILED, but on reset+rerun the
    succeeded branches replay from storage rather than re-executing.

    This is the production case for parallel-fanout pipelines: a single bad
    partition shouldn't redo the entire fan-out.
    """
    import asyncio

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

        results = await asyncio.gather(
            fetch_one("AAPL"),
            fetch_one("MSFT"),
            fetch_one("BAD"),
            return_exceptions=False,
        )
        return StringValue(value=",".join(r.value for r in results))

    options = Options(
        workflow_id="prices:partial-gather",
        run_id="2026-05-04",
        code_version="test",
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
    upstream_options = Options(
        workflow_id="upstream:fetch", run_id="2026-05-04", code_version="test"
    )
    downstream_options = Options(
        workflow_id="downstream:transform", run_id="2026-05-04", code_version="test"
    )

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
        code_version="test",
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

    # Now the workflow body re-runs: fetch short-circuits (replay), sleep
    # short-circuits (timer already fired), wait_event raises EventPendingError.
    with pytest.raises(EventPendingError):
        await run(store, options, StringValue(value="AAPL"), StringValue, execute)
    assert fetch_calls == 1, "fetch must NOT re-execute on resume"
    assert finalize_calls == 0

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

    options = Options(workflow_id="prices:gather", run_id="2026-05-04", code_version="test")
    result = await run(store, options, StringValue(value="batch"), StringValue, execute)
    assert result.value == "price:AAPL,price:MSFT,price:GOOG"
    assert sorted(seen) == ["AAPL", "GOOG", "MSFT"]


async def test_cancellation_does_not_persist_failed_records(store: OpenDALStore) -> None:
    """Asyncio cancellation is a shutdown signal, not a vendor failure. The
    workflow record should stay IN_PROGRESS (resumable on next invocation),
    and no FAILED activity record should be written."""
    import asyncio

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        async def slow_fetch(req: StringValue) -> StringValue:
            await asyncio.sleep(60)
            return StringValue(value=f"never:{req.value}")

        return await workflow.execute_activity(
            ActivityOptions(activity_id="fetch:cancel"),
            request,
            StringValue,
            slow_fetch,
        )

    options = Options(workflow_id="prices:cancel", run_id="2026-05-04", code_version="test")
    task = asyncio.create_task(run(store, options, StringValue(value="AAPL"), StringValue, execute))
    await asyncio.sleep(0.05)  # let the activity start its sleep
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

    options = Options(workflow_id="prices:fanout", run_id="2026-05-04", code_version="test")
    first = await run(store, options, StringValue(value="batch"), StringValue, execute)
    assert first.value == "price:AAPL,price:MSFT,price:GOOG,price:TSLA,price:NVDA"
    assert fetch_count == len(symbols)

    # Replay: every activity short-circuits on its stored record. No new fetches.
    second = await run(store, options, StringValue(value="batch"), StringValue, execute)
    assert second.value == first.value
    assert fetch_count == len(symbols)
