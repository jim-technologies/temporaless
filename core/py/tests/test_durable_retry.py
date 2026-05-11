"""Tests for D2: durable retry backoffs.

Mirrors core/go/workflow/durable_retry_test.go.

When RetryPolicy.durable_backoff_threshold > 0 and the next retry interval
crosses it, the runtime persists the wait as a TIMER_KIND_ACTIVITY_RETRY timer
plus an ActivityRecord with next_attempt_at, then raises TimerPendingError so
the workflow stays IN_PROGRESS. A downstream scanner re-invokes after the
timer's fire_at and the retry loop resumes.
"""

from __future__ import annotations

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
    OpenDALStore,
    TimerKey,
)
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ACTIVITY_RETRY_TIMER_ID_PREFIX,
    ActivityError,
    Options,
    RetryPolicy,
    TimerPendingError,
    Workflow,
    _activity_retry_timer_id,
    activity_digest,
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
    )
    assert result.value == "ok"
    assert attempts[0] == 3

    timer_key = TimerKey(
        workflow_id="wf", run_id="r", timer_id=_activity_retry_timer_id("act:short")
    )
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
        )
    assert attempts[0] == 1, "should bail after the first failure"
    assert info.value.timer_id.startswith(ACTIVITY_RETRY_TIMER_ID_PREFIX)
    assert info.value.wake_at >= start + timedelta(minutes=29)

    activity_key = ActivityKey(workflow_id="wf", run_id="r", activity_id="act:long")
    record = await store.get_activity(activity_key)
    assert record is not None
    assert record.status == temporaless_pb2.ACTIVITY_STATUS_RETRYING
    assert record.HasField("next_attempt_at")
    assert len(record.attempts) == 1

    timer_key = TimerKey(
        workflow_id="wf", run_id="r", timer_id=_activity_retry_timer_id("act:long")
    )
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
    digest = activity_digest(
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        "test",
        StringValue(value="x"),
    )
    future = datetime.now(UTC) + timedelta(minutes=10)
    next_at = Timestamp()
    next_at.FromDatetime(future)
    started_at = Timestamp()
    started_at.GetCurrentTime()
    completed_at = Timestamp()
    completed_at.GetCurrentTime()
    created_at = Timestamp()
    created_at.GetCurrentTime()
    key = ActivityKey(workflow_id="wf", run_id="r", activity_id="act:wait")
    await store.put_activity(
        temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            activity_type="activity:google.protobuf.StringValue->google.protobuf.StringValue",
            code_version="test",
            input_digest=digest,
            status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
            next_attempt_at=next_at,
            created_at=created_at,
            attempts=[
                temporaless_pb2.ActivityAttempt(
                    attempt=1, started_at=started_at, completed_at=completed_at
                )
            ],
        )
    )

    executions = [0]

    async def execute() -> StringValue:
        executions[0] += 1
        return StringValue(value="ok")

    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=10)),
        backoff_coefficient=1.0,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    with pytest.raises(TimerPendingError):
        await wf.run_activity(
            "act:wait",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            execute,
            retry_policy=policy,
        )
    assert executions[0] == 0


async def test_replay_after_fire_at_resumes(store):
    """A re-invocation past next_attempt_at runs the next attempt, the
    activity completes, attempt history is preserved, paired timer becomes
    FIRED."""
    wf = _workflow(store)
    digest = activity_digest(
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        "test",
        StringValue(value="x"),
    )
    past = datetime.now(UTC) - timedelta(minutes=1)
    next_at = Timestamp()
    next_at.FromDatetime(past)
    started_at = Timestamp()
    started_at.GetCurrentTime()
    completed_at = Timestamp()
    completed_at.GetCurrentTime()
    created_at = Timestamp()
    created_at.GetCurrentTime()
    activity_key = ActivityKey(workflow_id="wf", run_id="r", activity_id="act:resume")
    await store.put_activity(
        temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=activity_key.to_proto(),
            activity_type="activity:google.protobuf.StringValue->google.protobuf.StringValue",
            code_version="test",
            input_digest=digest,
            status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
            next_attempt_at=next_at,
            created_at=created_at,
            attempts=[
                temporaless_pb2.ActivityAttempt(
                    attempt=1, started_at=started_at, completed_at=completed_at
                )
            ],
        )
    )
    timer_key = TimerKey(
        workflow_id="wf", run_id="r", timer_id=_activity_retry_timer_id("act:resume")
    )
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
            input_digest="ignored",
            duration=duration_ts,
            status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
            fire_at=fire_at_ts,
            created_at=created_at,
        )
    )

    executions = [0]

    async def execute() -> StringValue:
        executions[0] += 1
        return StringValue(value="ok")

    policy = RetryPolicy(
        initial_interval=_make_duration(timedelta(minutes=1)),
        backoff_coefficient=1.0,
        maximum_attempts=3,
        durable_backoff_threshold=_make_duration(timedelta(seconds=30)),
    )
    result = await wf.run_activity(
        "act:resume",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value="x"),
        StringValue,
        execute,
        retry_policy=policy,
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
        )

    timer_key = TimerKey(
        workflow_id="wf", run_id="r", timer_id=_activity_retry_timer_id("act:multi")
    )
    timer1 = await store.get_timer(timer_key)
    assert timer1 is not None
    first_fire_at = timer1.fire_at.ToDatetime()

    # Rewind the stored next_attempt_at into the past so the next invocation
    # resumes immediately into another failure → second durable wait.
    activity_key = ActivityKey(workflow_id="wf", run_id="r", activity_id="act:multi")
    record = await store.get_activity(activity_key)
    assert record is not None
    rewound = Timestamp()
    rewound.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    record.next_attempt_at.CopyFrom(rewound)
    await store.put_activity(record)

    with pytest.raises(TimerPendingError):
        await wf.run_activity(
            "act:multi",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            always_fail,
            retry_policy=policy,
        )

    timer2 = await store.get_timer(timer_key)
    assert timer2 is not None
    assert timer2.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
    assert timer2.fire_at.ToDatetime() > first_fire_at


async def test_sleep_rejects_reserved_prefix(store):
    """workflow.sleep with a timer_id that uses the framework-reserved prefix
    fails fast — prevents collisions with framework-managed retry timers."""
    wf = _workflow(store)
    with pytest.raises(ValueError, match=ACTIVITY_RETRY_TIMER_ID_PREFIX):
        await wf.sleep("activity-retry:foo", timedelta(minutes=1))
