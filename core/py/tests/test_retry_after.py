"""Tests for ActivityError.retry_after — vendor-supplied minimum wait.

Mirrors core/go/workflow/retry_after_test.go. The retry planner uses
``max(computed_interval, retry_after)`` so vendor pacing wins over the
configured exponential schedule. Combined with
``RetryPolicy.durable_backoff_threshold``, a long Retry-After value
automatically becomes a durable timer rather than burning serverless compute.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.storage import ActivityKey, OpenDALStore, TimerKey
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ActivityError,
    Options,
    RetryPolicy,
    TimerPendingError,
    Workflow,
    _activity_retry_timer_id,
)


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


def _workflow(store) -> Workflow:
    return Workflow(store, Options(workflow_id="wf", run_id="r", code_version="test"))


def _duration(td: timedelta) -> Duration:
    d = Duration()
    d.FromTimedelta(td)
    return d


async def test_retry_after_longer_than_computed_wins(store):
    """Computed interval is 100ms but Retry-After is 30s; durable threshold is
    5s; so the wait should become a 30s durable timer."""
    wf = _workflow(store)
    attempts = [0]

    async def execute() -> StringValue:
        attempts[0] += 1
        raise ActivityError("rate_limited", "vendor 429", retry_after=timedelta(seconds=30))

    policy = RetryPolicy(
        initial_interval=_duration(timedelta(milliseconds=100)),
        backoff_coefficient=1.0,
        maximum_interval=_duration(timedelta(milliseconds=100)),
        maximum_attempts=3,
        durable_backoff_threshold=_duration(timedelta(seconds=5)),
    )
    start = datetime.now(UTC)
    with pytest.raises(TimerPendingError) as info:
        await wf.run_activity(
            "act:ra",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            execute,
            retry_policy=policy,
        )
    assert attempts[0] == 1, "should bail after the first failure"
    assert info.value.wake_at >= start + timedelta(seconds=29)

    # Persisted ActivityAttempt.failure must carry the retry_after duration.
    record = await store.get_activity(
        ActivityKey(workflow_id="wf", run_id="r", activity_id="act:ra")
    )
    assert record is not None
    assert len(record.attempts) == 1
    persisted = record.attempts[0].failure.retry_after.ToTimedelta()
    assert persisted == timedelta(seconds=30)


async def test_retry_after_shorter_than_computed_is_ignored(store):
    """Retry-After of 1ms must not undercut a 10ms computed floor."""
    wf = _workflow(store)
    attempts = [0]

    async def execute() -> StringValue:
        attempts[0] += 1
        if attempts[0] < 3:
            raise ActivityError("flaky", "", retry_after=timedelta(milliseconds=1))
        return StringValue(value="ok")

    policy = RetryPolicy(
        initial_interval=_duration(timedelta(milliseconds=10)),
        backoff_coefficient=1.0,
        maximum_interval=_duration(timedelta(milliseconds=10)),
        maximum_attempts=3,
    )
    started = time.monotonic()
    result = await wf.run_activity(
        "act:short",
        "activity:google.protobuf.StringValue->google.protobuf.StringValue",
        StringValue(value="x"),
        StringValue,
        execute,
        retry_policy=policy,
    )
    assert result.value == "ok"
    elapsed = time.monotonic() - started
    # 2 retries × 10ms minimum (plan floor) — short retry_after must not
    # undercut.
    assert elapsed >= 0.018, f"elapsed={elapsed}s; Retry-After undercut floor"


async def test_retry_after_promotes_short_policy_to_durable(store):
    """Policy says 1s interval, threshold is 30s. With Retry-After: 10min the
    effective interval crosses the threshold → durable timer."""
    wf = _workflow(store)
    attempts = [0]

    async def execute() -> StringValue:
        attempts[0] += 1
        raise ActivityError("rate_limited", "vendor 429", retry_after=timedelta(minutes=10))

    policy = RetryPolicy(
        initial_interval=_duration(timedelta(seconds=1)),
        backoff_coefficient=1.0,
        maximum_interval=_duration(timedelta(seconds=1)),
        maximum_attempts=3,
        durable_backoff_threshold=_duration(timedelta(seconds=30)),
    )
    with pytest.raises(TimerPendingError):
        await wf.run_activity(
            "act:promote",
            "activity:google.protobuf.StringValue->google.protobuf.StringValue",
            StringValue(value="x"),
            StringValue,
            execute,
            retry_policy=policy,
        )
    timer = await store.get_timer(
        TimerKey(
            workflow_id="wf",
            run_id="r",
            timer_id=_activity_retry_timer_id("act:promote"),
        )
    )
    assert timer is not None
    assert timer.timer_kind == temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY
    delay = timer.fire_at.ToDatetime().replace(tzinfo=UTC) - datetime.now(UTC)
    assert delay >= timedelta(minutes=9), f"timer fire_at delay {delay}, want ~10min"
