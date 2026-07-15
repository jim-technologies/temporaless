"""Tests for D2: durable retry backoffs.

Mirrors core/go/workflow/durable_retry_test.go.

When RetryPolicy.durable_backoff_threshold > 0 and the next retry interval
crosses it, the runtime persists the wait as a TIMER_KIND_ACTIVITY_RETRY timer
plus an ActivityRecord with next_attempt_at, then raises TimerPendingError so
the workflow stays IN_PROGRESS. A downstream scanner re-invokes after the
timer's fire_at and the retry loop resumes.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    TIMER_RECORD_SCHEMA_VERSION,
    ActivityKey,
    ClaimKey,
    OpenDALStore,
    TimerKey,
    WorkflowKey,
)
from temporaless.timerscanner import due_timers
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ACTIVITY_CLAIM_ID_PREFIX,
    ActivityConflictError,
    ActivityError,
    ActivityOptions,
    Options,
    RetryPolicy,
    TimerPendingError,
    Workflow,
    run,
)


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


def _workflow(store) -> Workflow:
    return Workflow(store, Options(workflow_id="wf", run_id="r", code_version="test"))


def _make_duration(td: timedelta) -> Duration:
    d = Duration()
    d.FromTimedelta(td)
    return d


def _retry_timer_id(activity_id: str) -> str:
    """Application-owned deterministic timer ID used by these tests."""
    return f"retry:{activity_id}"


async def _rewind_retry(store, activity_id: str) -> None:
    past = datetime.now(UTC) - timedelta(seconds=1)
    activity_key = ActivityKey(workflow_id="wf", run_id="r", activity_id=activity_id)
    activity = await store.get_activity(activity_key)
    assert activity is not None
    activity.next_attempt_at.FromDatetime(past)
    await store.put_activity(activity)

    timer_key = TimerKey(workflow_id="wf", run_id="r", timer_id=_retry_timer_id(activity_id))
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.fire_at.FromDatetime(past)
    await store.put_timer(timer)


async def test_short_backoff_stays_in_process(store):
    """When the interval never crosses the threshold, the runtime retries
    in-process — no TIMER_KIND_ACTIVITY_RETRY record is written."""
    wf = _workflow(store)
    attempts = [0]

    async def execute() -> StringValue:
        attempts[0] += 1
        if attempts[0] < 3:
            raise ActivityError("flaky", "transient")
        return StringValue(value="ok")

    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(milliseconds=10)),
        backoff_coefficient=1.0,
        maximum_interval=_make_duration(timedelta(milliseconds=10)),
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(hours=1)),
    )
    result = await wf.run_activity(
        "act:short",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value="x"),
        StringValue,
        execute,
        retry_policy=policy,
        retry_timer_id=_retry_timer_id("act:short"),
    )
    assert result.value == "ok"
    assert attempts[0] == 3

    timer_key = TimerKey(workflow_id="wf", run_id="r", timer_id=_retry_timer_id("act:short"))
    assert await store.get_timer(timer_key) is None


async def test_long_backoff_persists_and_bails(store):
    """First failure with a long backoff persists RETRYING + a SCHEDULED
    timer, and raises TimerPendingError without retrying in-process."""
    wf = _workflow(store)
    attempts = [0]

    async def execute() -> StringValue:
        attempts[0] += 1
        raise ActivityError("rate_limited", "vendor 429")

    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=30)),
        backoff_coefficient=1.0,
        maximum_interval=_make_duration(timedelta(minutes=30)),
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    start = datetime.now(UTC)
    with pytest.raises(TimerPendingError) as info:
        await wf.run_activity(
            "act:long",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            execute,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:long"),
        )
    assert attempts[0] == 1, "should bail after the first failure"
    assert info.value.timer_id == _retry_timer_id("act:long")
    assert info.value.wake_at >= start + timedelta(minutes=29)

    activity_key = ActivityKey(workflow_id="wf", run_id="r", activity_id="act:long")
    record = await store.get_activity(activity_key)
    assert record is not None
    assert record.status == temporaless_pb2.ACTIVITY_STATUS_RETRYING
    assert record.HasField("next_attempt_at")
    assert len(record.attempts) == 1

    timer_key = TimerKey(workflow_id="wf", run_id="r", timer_id=_retry_timer_id("act:long"))
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.timer_kind == temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    # fire_at must match the activity's next_attempt_at exactly.
    assert timer.fire_at.ToDatetime() == record.next_attempt_at.ToDatetime()


async def test_replay_before_fire_at_returns_pending(store):
    """A re-invocation that lands BEFORE next_attempt_at must not run the
    activity body — it raises TimerPendingError again."""
    wf = _workflow(store)
    future = datetime.now(UTC) + timedelta(minutes=10)
    next_at = Timestamp()
    next_at.FromDatetime(future)
    started_at = Timestamp()
    started_at.GetCurrentTime()
    completed_at = Timestamp()
    completed_at.GetCurrentTime()
    created_at = Timestamp()
    created_at.GetCurrentTime()
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1.0,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    key = ActivityKey(workflow_id="wf", run_id="r", activity_id="act:wait")
    failure = temporaless_pb2.ActivityFailure(code="transient", message="retry")
    await store.put_activity(
        temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            activity_type="activity:google.protobuf.StringValue->google.protobuf.StringValue",
            code_version="test",
            status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
            next_attempt_at=next_at,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:wait"),
            created_at=created_at,
            failure=failure,
            attempts=[
                temporaless_pb2.ActivityAttempt(
                    attempt=1,
                    started_at=started_at,
                    completed_at=completed_at,
                    failure=failure,
                )
            ],
        )
    )

    executions = [0]

    async def execute() -> StringValue:
        executions[0] += 1
        return StringValue(value="ok")

    with pytest.raises(TimerPendingError):
        await wf.run_activity(
            "act:wait",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            execute,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:wait"),
        )
    assert executions[0] == 0


async def test_replay_after_fire_at_resumes(store):
    """A re-invocation past next_attempt_at runs the next attempt, the
    activity completes, attempt history is preserved, paired timer becomes
    FIRED."""
    wf = _workflow(store)
    past = datetime.now(UTC) - timedelta(minutes=1)
    next_at = Timestamp()
    next_at.FromDatetime(past)
    started_at = Timestamp()
    started_at.GetCurrentTime()
    completed_at = Timestamp()
    completed_at.GetCurrentTime()
    created_at = Timestamp()
    created_at.GetCurrentTime()
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=1)),
        backoff_coefficient=1.0,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    activity_key = ActivityKey(workflow_id="wf", run_id="r", activity_id="act:resume")
    failure = temporaless_pb2.ActivityFailure(code="transient", message="retry")
    await store.put_activity(
        temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=activity_key.to_proto(),
            activity_type="activity:google.protobuf.StringValue->google.protobuf.StringValue",
            code_version="test",
            status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
            next_attempt_at=next_at,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:resume"),
            created_at=created_at,
            failure=failure,
            attempts=[
                temporaless_pb2.ActivityAttempt(
                    attempt=1,
                    started_at=started_at,
                    completed_at=completed_at,
                    failure=failure,
                )
            ],
        )
    )
    timer_key = TimerKey(workflow_id="wf", run_id="r", timer_id=_retry_timer_id("act:resume"))
    fire_at_ts = Timestamp()
    fire_at_ts.FromDatetime(past)
    duration_ts = Duration()
    duration_ts.FromTimedelta(timedelta(minutes=1))
    await store.put_timer(
        temporaless_pb2.TimerRecord(
            schema_version=TIMER_RECORD_SCHEMA_VERSION,
            key=timer_key.to_proto(),
            timer_kind=temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY,
            code_version="test",
            duration=duration_ts,
            status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
            fire_at=fire_at_ts,
            created_at=created_at,
            retry_activity_id="act:resume",
        )
    )

    executions = [0]

    async def execute() -> StringValue:
        executions[0] += 1
        return StringValue(value="ok")

    result = await wf.run_activity(
        "act:resume",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value="x"),
        StringValue,
        execute,
        retry_policy=policy,
        retry_timer_id=_retry_timer_id("act:resume"),
    )
    assert result.value == "ok"
    assert executions[0] == 1, "should run exactly attempt 2"

    record = await store.get_activity(activity_key)
    assert record is not None
    assert record.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert len(record.attempts) == 2

    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_FIRED


async def test_second_long_backoff_overwrites_timer(store):
    """A successive durable retry overwrites the previously scheduled timer
    with a later fire_at — exactly one timer record per activity at any time."""
    wf = _workflow(store)
    attempts = [0]

    async def always_fail() -> StringValue:
        attempts[0] += 1
        raise ActivityError("rate_limited", "vendor 429")

    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=2.0,
        maximum_interval=_make_duration(timedelta(minutes=40)),
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    with pytest.raises(TimerPendingError):
        await wf.run_activity(
            "act:multi",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            always_fail,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:multi"),
        )

    timer_key = TimerKey(workflow_id="wf", run_id="r", timer_id=_retry_timer_id("act:multi"))
    timer1 = await store.get_timer(timer_key)
    assert timer1 is not None
    first_fire_at = timer1.fire_at.ToDatetime()
    assert timer1.duration.ToTimedelta() == timedelta(minutes=10)

    # Rewind the stored next_attempt_at into the past so the next invocation
    # resumes immediately into another failure → second durable wait.
    await _rewind_retry(store, "act:multi")

    with pytest.raises(TimerPendingError):
        await wf.run_activity(
            "act:multi",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            always_fail,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:multi"),
        )

    timer2 = await store.get_timer(timer_key)
    assert timer2 is not None
    assert timer2.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    assert timer2.duration.ToTimedelta() == timedelta(minutes=20)
    assert timer2.fire_at.ToDatetime() > first_fire_at


async def test_durable_resume_caps_policy_backoff_without_compounding_retry_after(store):
    """Retry-After is a floor for one failed attempt, not the base of the
    exponential policy. The policy sequence remains 10m, 20m, then caps at
    25m even when attempt 1 asks the vendor for a 30m wait."""
    wf = _workflow(store)
    attempts = 0

    async def always_fail() -> StringValue:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ActivityError(
                "rate_limited",
                "vendor 429",
                retry_after=timedelta(minutes=30),
            )
        raise ActivityError("rate_limited", "vendor 429")

    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=2.0,
        maximum_interval=_make_duration(timedelta(minutes=25)),
        maximum_attempts=4,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    timer_key = TimerKey(
        workflow_id="wf",
        run_id="r",
        timer_id=_retry_timer_id("act:retry-after-history"),
    )

    for expected in (
        timedelta(minutes=30),
        timedelta(minutes=20),
        timedelta(minutes=25),
    ):
        with pytest.raises(TimerPendingError):
            await wf.run_activity(
                "act:retry-after-history",
                "activity:google.protobuf.StringValue->google.protobuf.StringValue",
                StringValue(value="x"),
                StringValue,
                always_fail,
                retry_policy=policy,
                retry_timer_id=_retry_timer_id("act:retry-after-history"),
            )
        timer = await store.get_timer(timer_key)
        assert timer is not None
        assert timer.duration.ToTimedelta() == expected
        if expected != timedelta(minutes=25):
            await _rewind_retry(store, "act:retry-after-history")


async def test_retrying_record_uses_normalized_policy_and_rejects_drift(store):
    wf = _workflow(store)
    initial_policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        # Zero is the documented linear-backoff shorthand and normalizes to 1.
        backoff_coefficient=0,
        maximum_attempts=3,
        non_retryable_error_codes=["z", "a", "a"],
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )

    async def fail() -> StringValue:
        raise ActivityError("transient", "retry")

    with pytest.raises(TimerPendingError):
        await wf.run_activity(
            "act:policy",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail,
            retry_policy=initial_policy,
            retry_timer_id=_retry_timer_id("act:policy"),
        )

    key = ActivityKey(workflow_id="wf", run_id="r", activity_id="act:policy")
    retrying = await store.get_activity(key)
    assert retrying is not None
    assert retrying.retry_policy.backoff_coefficient == 1.0
    assert list(retrying.retry_policy.non_retryable_error_codes) == ["a", "z"]
    await _rewind_retry(store, "act:policy")

    equivalent_policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1,
        maximum_attempts=3,
        non_retryable_error_codes=["a", "z"],
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )

    async def succeed() -> StringValue:
        return StringValue(value="ok")

    result = await wf.run_activity(
        "act:policy",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value="x"),
        StringValue,
        succeed,
        retry_policy=equivalent_policy,
        retry_timer_id=_retry_timer_id("act:policy"),
    )
    assert result.value == "ok"

    # A fresh RETRYING record must reject a semantic policy change under the
    # same activity identity rather than silently changing remaining waits.
    with pytest.raises(TimerPendingError):
        await wf.run_activity(
            "act:policy-drift",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail,
            retry_policy=equivalent_policy,
            retry_timer_id=_retry_timer_id("act:policy-drift"),
        )
    await _rewind_retry(store, "act:policy-drift")
    changed_policy = RetryPolicy()
    changed_policy.CopyFrom(equivalent_policy)
    changed_policy.backoff_coefficient = 2
    executions = 0

    async def should_not_run() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="unexpected")

    with pytest.raises(ActivityConflictError, match="retry policy changed"):
        await wf.run_activity(
            "act:policy-drift",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            should_not_run,
            retry_policy=changed_policy,
            retry_timer_id=_retry_timer_id("act:policy-drift"),
        )
    assert executions == 0
    timer = await store.get_timer(
        TimerKey(
            workflow_id="wf",
            run_id="r",
            timer_id=_retry_timer_id("act:policy-drift"),
        )
    )
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED


@pytest.mark.parametrize(
    ("corruption", "message"),
    [
        ("no_attempts", "has no attempts"),
        ("ordinal", "attempt 1 is out of sequence"),
        ("no_failure", "attempt 1 has no failure"),
        ("exhausted", "has exhausted its retry policy"),
        ("retry_after", "attempt 1 has invalid retry_after"),
        ("negative_retry_after", "attempt 1 has negative retry_after"),
        ("failure_mismatch", "failure does not match its latest attempt"),
        ("non_retryable_failure", "ends with a non-retryable failure"),
        ("missing_next_attempt_at", "next_attempt_at must be present"),
        ("unexpected_next_attempt_at", "next_attempt_at must be absent"),
        ("next_attempt_at", "has invalid next_attempt_at"),
    ],
)
async def test_retrying_replay_rejects_malformed_persisted_state(
    store: OpenDALStore,
    corruption: str,
    message: str,
) -> None:
    wf = _workflow(store)
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )

    async def fail() -> StringValue:
        raise ActivityError("transient", "retry")

    with pytest.raises(TimerPendingError):
        await wf.run_activity(
            "act:malformed",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:malformed"),
        )

    key = ActivityKey(workflow_id="wf", run_id="r", activity_id="act:malformed")
    record = await store.get_activity(key)
    assert record is not None
    if corruption == "no_attempts":
        del record.attempts[:]
    elif corruption == "ordinal":
        record.attempts[0].attempt = 2
    elif corruption == "no_failure":
        record.attempts[0].ClearField("failure")
    elif corruption == "exhausted":
        record.attempts.extend(
            [
                temporaless_pb2.ActivityAttempt(
                    attempt=2,
                    failure=temporaless_pb2.ActivityFailure(message="retry"),
                ),
                temporaless_pb2.ActivityAttempt(
                    attempt=3,
                    failure=temporaless_pb2.ActivityFailure(message="retry"),
                ),
            ]
        )
    elif corruption == "retry_after":
        record.attempts[0].failure.retry_after.seconds = 315_576_000_001
    elif corruption == "negative_retry_after":
        record.attempts[0].failure.retry_after.seconds = -1
    elif corruption == "failure_mismatch":
        record.failure.message = "different failure"
    elif corruption == "non_retryable_failure":
        policy.non_retryable_error_codes.append("transient")
        record.retry_policy.CopyFrom(policy)
    elif corruption == "missing_next_attempt_at":
        record.ClearField("next_attempt_at")
    elif corruption == "unexpected_next_attempt_at":
        policy.durable_backoff_threshold.CopyFrom(_make_duration(timedelta(hours=1)))
        record.retry_policy.CopyFrom(policy)
    elif corruption == "next_attempt_at":
        record.next_attempt_at.seconds = 253_402_300_800
    else:
        raise AssertionError(f"unknown corruption case {corruption}")
    await store.put_activity(record)

    executions = 0

    async def should_not_run() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="unexpected")

    with pytest.raises(ActivityConflictError, match=message):
        await wf.run_activity(
            "act:malformed",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            should_not_run,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:malformed"),
        )
    assert executions == 0


async def test_retrying_replay_repairs_missing_timer_before_returning_pending(
    tmp_path,
) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    workflow = _workflow(store)
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=2,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    calls = 0

    async def fail() -> StringValue:
        nonlocal calls
        calls += 1
        raise ActivityError("transient", "retry")

    with pytest.raises(TimerPendingError):
        await workflow.run_activity(
            "act:repair-missing",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:repair-missing"),
        )

    timer_key = TimerKey(
        workflow_id="wf",
        run_id="r",
        timer_id=_retry_timer_id("act:repair-missing"),
    )
    assert await store.delete_timer(timer_key)
    assert await store.get_timer(timer_key) is None

    with pytest.raises(TimerPendingError):
        await workflow.run_activity(
            "act:repair-missing",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:repair-missing"),
        )
    assert calls == 1
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED

    # A RETRYING record is authoritative over an accidentally consumed paired
    # timer as well; replay restores the wake before returning pending.
    timer.status = temporaless_pb2.TIMER_STATUS_FIRED
    timer.fired_at.GetCurrentTime()
    await store.put_timer(timer)
    with pytest.raises(TimerPendingError):
        await workflow.run_activity(
            "act:repair-missing",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:repair-missing"),
        )
    assert calls == 1
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED


async def test_retry_timer_write_failure_is_nonterminal_and_redeliverable(
    tmp_path,
) -> None:
    class FailFirstRetryTimerStore(OpenDALStore):
        fail_retry_timer = True

        async def put_timer(self, record: temporaless_pb2.TimerRecord) -> None:
            if (
                self.fail_retry_timer
                and record.timer_kind == temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY
                and record.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
            ):
                self.fail_retry_timer = False
                raise RuntimeError("injected retry timer write failure")
            await super().put_timer(record)

    store = FailFirstRetryTimerStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    calls = 0

    async def activity(_request: StringValue) -> StringValue:
        nonlocal calls
        calls += 1
        raise ActivityError("transient", "retry")

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        return await workflow.execute_activity(
            ActivityOptions(
                activity_id="act:timer-write",
                retry_policy=policy,
                retry_timer_id=_retry_timer_id("act:timer-write"),
            ),
            request,
            StringValue,
            activity,
        )

    options = Options(workflow_id="wf", run_id="r", code_version="test")
    with pytest.raises(TimerPendingError) as first:
        await run(store, options, StringValue(value="x"), StringValue, execute)
    assert isinstance(first.value.__cause__, RuntimeError)
    workflow_record = await store.get_workflow(WorkflowKey(workflow_id="wf", run_id="r"))
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    assert calls == 1

    # Downstream request redelivery repeats the known-failed attempt at-least
    # once, then persists both timer-first wake intent and retry history.
    with pytest.raises(TimerPendingError):
        await run(store, options, StringValue(value="x"), StringValue, execute)
    assert calls == 2
    timer = await store.get_timer(
        TimerKey(
            workflow_id="wf",
            run_id="r",
            timer_id=_retry_timer_id("act:timer-write"),
        )
    )
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED


async def test_timer_first_partial_write_honors_wake_before_reexecution(tmp_path) -> None:
    class FailFirstRetryRecordStore(OpenDALStore):
        fail_retry_record = True

        async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None:
            if self.fail_retry_record and record.status == temporaless_pb2.ACTIVITY_STATUS_RETRYING:
                self.fail_retry_record = False
                raise RuntimeError("injected retry record write failure")
            await super().put_activity(record)

    store = FailFirstRetryRecordStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    workflow = _workflow(store)
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    activity_id = "act:timer-first"
    retry_timer_id = _retry_timer_id(activity_id)
    calls = 0

    async def fail_then_succeed() -> StringValue:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ActivityError("transient", "retry")
        return StringValue(value="ok")

    with pytest.raises(TimerPendingError) as first:
        await workflow.run_activity(
            activity_id,
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail_then_succeed,
            retry_policy=policy,
            retry_timer_id=retry_timer_id,
        )
    assert isinstance(first.value.__cause__, RuntimeError)
    assert (
        await store.get_activity(ActivityKey(workflow_id="wf", run_id="r", activity_id=activity_id))
        is None
    )

    # Immediate request redelivery observes the timer-first prepare record and
    # does not bypass the vendor's future backoff.
    with pytest.raises(TimerPendingError):
        await workflow.run_activity(
            activity_id,
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail_then_succeed,
            retry_policy=policy,
            retry_timer_id=retry_timer_id,
        )
    assert calls == 1

    timer_key = TimerKey(workflow_id="wf", run_id="r", timer_id=retry_timer_id)
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(timer)

    result = await workflow.run_activity(
        activity_id,
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value="x"),
        StringValue,
        fail_then_succeed,
        retry_policy=policy,
        retry_timer_id=retry_timer_id,
    )
    assert result.value == "ok"
    assert calls == 2
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_FIRED


@pytest.mark.parametrize(
    ("corruption", "message"),
    [
        ("missing_duration", "has no duration"),
        ("invalid_duration", "has invalid duration"),
        ("negative_duration", "has negative duration"),
        ("too_short", "below its required retry interval"),
        ("missing_created_at", "has no created_at"),
        ("invalid_created_at", "has invalid created_at"),
        ("scheduled_fired_at", "SCHEDULED activity retry timer has fired_at"),
        ("fired_missing_fired_at", "FIRED activity retry timer has no fired_at"),
        ("fired_invalid_fired_at", "FIRED activity retry timer has invalid fired_at"),
    ],
)
async def test_prepared_retry_timer_rejects_malformed_state(
    store: OpenDALStore,
    corruption: str,
    message: str,
) -> None:
    workflow = _workflow(store)
    activity_id = f"act:prepared-invalid:{corruption}"
    retry_timer_id = _retry_timer_id(activity_id)
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    now = datetime.now(UTC)
    timer = temporaless_pb2.TimerRecord(
        schema_version=TIMER_RECORD_SCHEMA_VERSION,
        key=TimerKey(workflow_id="wf", run_id="r", timer_id=retry_timer_id).to_proto(),
        timer_kind=temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY,
        code_version="test",
        duration=_make_duration(timedelta(minutes=10)),
        status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
        retry_activity_id=activity_id,
    )
    timer.fire_at.FromDatetime(now + timedelta(hours=1))
    timer.created_at.FromDatetime(now)
    if corruption == "missing_duration":
        timer.ClearField("duration")
    elif corruption == "invalid_duration":
        timer.duration.seconds = 315_576_000_001
    elif corruption == "negative_duration":
        timer.duration.FromTimedelta(timedelta(seconds=-1))
    elif corruption == "too_short":
        timer.duration.FromTimedelta(timedelta(seconds=10))
    elif corruption == "missing_created_at":
        timer.ClearField("created_at")
    elif corruption == "invalid_created_at":
        timer.created_at.seconds = 253_402_300_800
    elif corruption == "scheduled_fired_at":
        timer.fired_at.FromDatetime(now)
    elif corruption == "fired_missing_fired_at":
        timer.status = temporaless_pb2.TIMER_STATUS_FIRED
    elif corruption == "fired_invalid_fired_at":
        timer.status = temporaless_pb2.TIMER_STATUS_FIRED
        timer.fired_at.seconds = 253_402_300_800
    else:
        raise AssertionError(f"unknown corruption case {corruption}")
    # Persist only the deliberately malformed point. A valid deterministic
    # shadow would correctly recover/overlay a corrupt point and prevent this
    # test from reaching the workflow-level malformed-state checks.
    await store._put_canonical_timer(timer)
    executions = 0

    async def should_not_run() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="unexpected")

    with pytest.raises(ActivityConflictError, match=message):
        await workflow.run_activity(
            activity_id,
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            should_not_run,
            retry_policy=policy,
            retry_timer_id=retry_timer_id,
        )
    assert executions == 0


async def test_fired_prepared_retry_timer_is_rearmed_before_waiting(
    store: OpenDALStore,
) -> None:
    workflow = _workflow(store)
    activity_id = "act:prepared-fired"
    retry_timer_id = _retry_timer_id(activity_id)
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    now = datetime.now(UTC)
    timer = temporaless_pb2.TimerRecord(
        schema_version=TIMER_RECORD_SCHEMA_VERSION,
        key=TimerKey(workflow_id="wf", run_id="r", timer_id=retry_timer_id).to_proto(),
        timer_kind=temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY,
        code_version="test",
        duration=_make_duration(timedelta(minutes=10)),
        status=temporaless_pb2.TIMER_STATUS_FIRED,
        retry_activity_id=activity_id,
    )
    timer.fire_at.FromDatetime(now + timedelta(hours=1))
    timer.created_at.FromDatetime(now)
    timer.fired_at.FromDatetime(now)
    await store.put_timer(timer)
    executions = 0

    async def should_not_run() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="unexpected")

    with pytest.raises(TimerPendingError):
        await workflow.run_activity(
            activity_id,
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            should_not_run,
            retry_policy=policy,
            retry_timer_id=retry_timer_id,
        )
    assert executions == 0
    rearmed = await store.get_timer(TimerKey(workflow_id="wf", run_id="r", timer_id=retry_timer_id))
    assert rearmed is not None
    assert rearmed.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    assert not rearmed.HasField("fired_at")


async def test_lagging_retry_record_honors_newer_prepared_timer(
    store: OpenDALStore,
) -> None:
    workflow = _workflow(store)
    activity_id = "act:prepared-after-short-retry"
    retry_timer_id = _retry_timer_id(activity_id)
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(seconds=1)),
        backoff_coefficient=120,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(minutes=1)),
    )
    now = datetime.now(UTC)
    failure = temporaless_pb2.ActivityFailure(code="transient", message="retry")
    attempt = temporaless_pb2.ActivityAttempt(attempt=1, failure=failure)
    attempt.started_at.FromDatetime(now)
    attempt.completed_at.FromDatetime(now)
    activity = temporaless_pb2.ActivityRecord(
        schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
        key=ActivityKey(workflow_id="wf", run_id="r", activity_id=activity_id).to_proto(),
        activity_type="activity:google.protobuf.StringValue->google.protobuf.StringValue",
        code_version="test",
        status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
        failure=failure,
        attempts=[attempt],
        retry_policy=policy,
        retry_timer_id=retry_timer_id,
    )
    activity.created_at.FromDatetime(now)
    await store.put_activity(activity)

    timer = temporaless_pb2.TimerRecord(
        schema_version=TIMER_RECORD_SCHEMA_VERSION,
        key=TimerKey(workflow_id="wf", run_id="r", timer_id=retry_timer_id).to_proto(),
        timer_kind=temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY,
        code_version="test",
        duration=_make_duration(timedelta(minutes=2)),
        status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
        retry_activity_id=activity_id,
    )
    timer.fire_at.FromDatetime(now + timedelta(minutes=2))
    timer.created_at.FromDatetime(now)
    await store.put_timer(timer)
    executions = 0

    async def should_not_run() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="unexpected")

    with pytest.raises(TimerPendingError) as pending:
        await workflow.run_activity(
            activity_id,
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            should_not_run,
            retry_policy=policy,
            retry_timer_id=retry_timer_id,
        )
    assert pending.value.wake_at == timer.fire_at.ToDatetime(tzinfo=UTC)
    assert executions == 0


async def test_retrying_replay_advances_stale_timer_after_overwrite_failure(
    tmp_path,
) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    workflow = _workflow(store)
    activity_id = "act:repair-stale"
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=2,
        maximum_attempts=4,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    calls = 0

    async def fail() -> StringValue:
        nonlocal calls
        calls += 1
        raise ActivityError("transient", "retry")

    with pytest.raises(TimerPendingError):
        await workflow.run_activity(
            activity_id,
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id(activity_id),
        )
    # Simulate a record-first/legacy partial overwrite: attempt 2 and its next
    # wake committed, while the stable timer still points at attempt 1's wait.
    activity_key = ActivityKey(workflow_id="wf", run_id="r", activity_id=activity_id)
    activity = await store.get_activity(activity_key)
    assert activity is not None
    now = datetime.now(UTC)
    attempt = temporaless_pb2.ActivityAttempt(
        attempt=2,
        failure=temporaless_pb2.ActivityFailure(code="transient", message="retry"),
    )
    attempt.started_at.FromDatetime(now)
    attempt.completed_at.FromDatetime(now)
    activity.attempts.append(attempt)
    activity.failure.CopyFrom(attempt.failure)
    activity.next_attempt_at.FromDatetime(now + timedelta(minutes=20))
    await store.put_activity(activity)

    with pytest.raises(TimerPendingError):
        await workflow.run_activity(
            activity_id,
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id(activity_id),
        )
    assert calls == 1
    timer = await store.get_timer(
        TimerKey(
            workflow_id="wf",
            run_id="r",
            timer_id=_retry_timer_id(activity_id),
        )
    )
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    assert timer.duration.ToTimedelta() == timedelta(minutes=20)


@pytest.mark.parametrize(
    "corruption",
    ["wrong_kind", "wrong_activity", "wrong_code", "canceled", "newer", "newer_short"],
)
async def test_retrying_replay_rejects_incompatible_retry_timer(
    store: OpenDALStore,
    corruption: str,
) -> None:
    workflow = _workflow(store)
    activity_id = f"act:timer-conflict:{corruption}"
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )

    async def fail() -> StringValue:
        raise ActivityError("transient", "retry")

    with pytest.raises(TimerPendingError):
        await workflow.run_activity(
            activity_id,
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id(activity_id),
        )

    timer_key = TimerKey(
        workflow_id="wf",
        run_id="r",
        timer_id=_retry_timer_id(activity_id),
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    if corruption == "wrong_kind":
        timer.timer_kind = temporaless_pb2.TIMER_KIND_SLEEP
    elif corruption == "wrong_activity":
        timer.retry_activity_id = "other:activity"
    elif corruption == "wrong_code":
        timer.code_version = "other"
    elif corruption == "canceled":
        timer.status = temporaless_pb2.TIMER_STATUS_CANCELED
    elif corruption in ("newer", "newer_short"):
        timer.fire_at.FromDatetime(
            timer.fire_at.ToDatetime().replace(tzinfo=UTC) + timedelta(hours=1)
        )
        if corruption == "newer_short":
            timer.duration.FromTimedelta(timedelta(minutes=1))
    await store.put_timer(timer)

    executions = 0

    async def should_not_run() -> StringValue:
        nonlocal executions
        executions += 1
        return StringValue(value="unexpected")

    if corruption == "newer":
        with pytest.raises(TimerPendingError) as pending:
            await workflow.run_activity(
                activity_id,
                "activity:google.protobuf.StringValue->google.protobuf.StringValue",
                StringValue(value="x"),
                StringValue,
                should_not_run,
                retry_policy=policy,
                retry_timer_id=_retry_timer_id(activity_id),
            )
        assert pending.value.wake_at == timer.fire_at.ToDatetime().replace(tzinfo=UTC)
    else:
        with pytest.raises(ActivityConflictError, match="retry timer"):
            await workflow.run_activity(
                activity_id,
                "activity:google.protobuf.StringValue->google.protobuf.StringValue",
                StringValue(value="x"),
                StringValue,
                should_not_run,
                retry_policy=policy,
                retry_timer_id=_retry_timer_id(activity_id),
            )
    assert executions == 0


async def test_due_retry_timer_stays_scheduled_until_resumed_attempt_is_durable(store):
    """Cancellation inside a resumed body must not consume its only wakeup.

    The old due timer remains scheduled while the body is ambiguous, so the
    scanner can dispatch the IN_PROGRESS workflow again. A later successful
    attempt persists its terminal ActivityRecord before firing the timer.
    """
    entered = asyncio.Event()
    never = asyncio.Event()
    activity_calls = 0
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1.0,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )

    async def activity(request: StringValue) -> StringValue:
        nonlocal activity_calls
        activity_calls += 1
        if activity_calls == 1:
            raise ActivityError("transient", "retry")
        if activity_calls == 2:
            entered.set()
            await never.wait()
        return StringValue(value=f"done:{request.value}")

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        return await workflow.execute_activity(
            ActivityOptions(
                activity_id="act:lost-wakeup",
                retry_policy=policy,
                retry_timer_id=_retry_timer_id("act:lost-wakeup"),
            ),
            request,
            StringValue,
            activity,
        )

    options = Options(workflow_id="wf", run_id="r", code_version="test")
    with pytest.raises(TimerPendingError):
        await run(store, options, StringValue(value="x"), StringValue, execute)
    await _rewind_retry(store, "act:lost-wakeup")

    resumed = asyncio.create_task(run(store, options, StringValue(value="x"), StringValue, execute))
    await asyncio.wait_for(entered.wait(), timeout=1)

    timer_key = TimerKey(workflow_id="wf", run_id="r", timer_id=_retry_timer_id("act:lost-wakeup"))
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    assert [due.key.timer_id for due in await due_timers(store, datetime.now(UTC))] == [
        _retry_timer_id("act:lost-wakeup")
    ]

    resumed.cancel()
    with pytest.raises(asyncio.CancelledError):
        await resumed

    workflow_record = await store.get_workflow(WorkflowKey(workflow_id="wf", run_id="r"))
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED

    result = await run(store, options, StringValue(value="x"), StringValue, execute)
    assert result.value == "done:x"
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_FIRED
    assert await due_timers(store, datetime.now(UTC)) == []


async def test_terminal_record_survives_timer_cleanup_error_and_replay_heals(
    tmp_path,
) -> None:
    class FailFirstFiredTimerStore(OpenDALStore):
        fail_fired_write = True

        async def put_timer(self, record: temporaless_pb2.TimerRecord) -> None:
            if self.fail_fired_write and record.status == temporaless_pb2.TIMER_STATUS_FIRED:
                self.fail_fired_write = False
                raise RuntimeError("injected retry-timer cleanup failure")
            await super().put_timer(record)

    store = FailFirstFiredTimerStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    workflow = Workflow(
        store,
        Options(
            workflow_id="wf",
            run_id="r",
            code_version="test",
            claim_owner_id="worker:one",
        ),
    )
    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    calls = 0

    async def fail_then_succeed() -> StringValue:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ActivityError("transient", "retry")
        return StringValue(value="ok")

    with pytest.raises(TimerPendingError):
        await workflow.run_activity(
            "act:cleanup",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            fail_then_succeed,
            retry_policy=policy,
            retry_timer_id=_retry_timer_id("act:cleanup"),
        )
    await _rewind_retry(store, "act:cleanup")

    # The terminal ActivityRecord wins even though its paired timer could not
    # be marked FIRED. Cleanup is diagnostic, not a replacement failure.
    result = await workflow.run_activity(
        "act:cleanup",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value="x"),
        StringValue,
        fail_then_succeed,
        retry_policy=policy,
        retry_timer_id=_retry_timer_id("act:cleanup"),
    )
    assert result.value == "ok"
    assert calls == 2
    assert (
        await store.get_claim(
            ClaimKey(
                workflow_id="wf",
                run_id="r",
                claim_id=f"{ACTIVITY_CLAIM_ID_PREFIX}act:cleanup",
            )
        )
        is None
    )

    timer_key = TimerKey(workflow_id="wf", run_id="r", timer_id=_retry_timer_id("act:cleanup"))
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED

    replay_calls = 0

    async def should_not_run() -> StringValue:
        nonlocal replay_calls
        replay_calls += 1
        return StringValue(value="unexpected")

    replayed = await workflow.run_activity(
        "act:cleanup",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value="x"),
        StringValue,
        should_not_run,
        retry_policy=RetryPolicy(maximum_attempts=1),
    )
    assert replayed.value == "ok"
    assert replay_calls == 0
    timer = await store.get_timer(timer_key)
    assert timer is not None
    assert timer.status == temporaless_pb2.TIMER_STATUS_FIRED


async def test_sleep_accepts_any_caller_owned_timer_id(store):
    """There is no framework-owned retry prefix; kind checks catch actual
    collisions at the point record."""
    wf = _workflow(store)
    with pytest.raises(TimerPendingError):
        await wf.sleep("activity-retry:foo", timedelta(minutes=1))
