"""Tests for Workflow.activity() — the ergonomic shortcut over execute_activity.

Defaults under test:
- caller supplies activity_id and durable retry_timer_id explicitly
- retry_policy filled in via default_retry_policy() when not given
- result_type inferred from func's return annotation when not given
"""

from __future__ import annotations

from datetime import timedelta

import opendal
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.wrappers_pb2 import Int32Value, StringValue

from temporaless.storage import ActivityKey, OpenDALStore
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    ActivityError,
    Options,
    RetryPolicy,
    Workflow,
    default_retry_policy,
)


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


def _wf(store) -> Workflow:
    return Workflow(store, Options(workflow_id="wf", run_id="r", code_version="test"))


async def _double(req: Int32Value) -> Int32Value:
    return Int32Value(value=req.value * 2)


async def test_activity_requires_explicit_id(store):
    wf = _wf(store)
    with pytest.raises(TypeError, match="activity_id"):
        await wf.activity(_double, Int32Value(value=7))  # type: ignore[call-arg]


async def test_activity_uses_explicit_id(store):
    wf = _wf(store)
    await wf.activity(
        _double,
        Int32Value(value=3),
        activity_id="custom:1",
        retry_timer_id="retry:custom:1",
    )

    custom_record = await store.get_activity(
        ActivityKey(workflow_id="wf", run_id="r", activity_id="custom:1")
    )
    assert custom_record is not None
    assert custom_record.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED


async def test_activity_applies_default_retry_policy(store):
    """When retry_policy is not given, the default (3 attempts) is applied —
    a flaky activity that succeeds on the second try should still succeed
    where execute_activity (no default policy) would fail after 1 attempt."""
    wf = _wf(store)
    attempts = [0]

    async def flaky_then_ok(req: StringValue) -> StringValue:
        attempts[0] += 1
        if attempts[0] < 2:
            raise ActivityError("transient", "first call fails")
        return StringValue(value="ok")

    result = await wf.activity(
        flaky_then_ok,
        StringValue(value="x"),
        activity_id="flaky",
        retry_timer_id="retry:flaky",
    )
    assert result.value == "ok"
    assert attempts[0] == 2, "default retry policy should give a second attempt"


async def test_activity_explicit_retry_policy_overrides_default(store):
    """An explicit RetryPolicy(maximum_attempts=1) disables retries even
    when the default would have retried."""
    wf = _wf(store)
    attempts = [0]

    async def always_fail(req: StringValue) -> StringValue:
        attempts[0] += 1
        raise ActivityError("nope", "fail")

    with pytest.raises(ActivityError):
        await wf.activity(
            always_fail,
            StringValue(value="x"),
            activity_id="always-fail",
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
    assert attempts[0] == 1, "explicit single-attempt policy should override default retry policy"


async def test_activity_infers_result_type_from_annotation(store):
    """result_type defaults to the function's return annotation."""
    wf = _wf(store)
    # _double is annotated as `-> Int32Value`. The helper should use Int32Value
    # as the result_factory without us passing it.
    result = await wf.activity(
        _double,
        Int32Value(value=5),
        activity_id="double",
        retry_timer_id="retry:double",
    )
    assert isinstance(result, Int32Value)
    assert result.value == 10


async def test_activity_result_type_override(store):
    """Explicit result_type wins over inference (useful when the annotation
    is missing or the function is dynamically constructed)."""
    wf = _wf(store)

    async def no_annotation(req):  # type: ignore[no-untyped-def]
        return Int32Value(value=req.value + 1)

    result = await wf.activity(
        no_annotation,
        Int32Value(value=4),
        activity_id="increment",
        retry_timer_id="retry:increment",
        result_type=Int32Value,
    )
    assert result.value == 5


def test_default_retry_policy_shape():
    """Sanity-check the default policy values stay consistent."""
    policy = default_retry_policy()
    assert policy.maximum_attempts == 3
    assert policy.backoff_coefficient == 2.0
    assert policy.initial_interval.ToTimedelta() == timedelta(seconds=1)
    assert policy.maximum_interval.ToTimedelta() == timedelta(seconds=30)
    assert policy.durable_backoff_threshold.ToTimedelta() == timedelta(seconds=30)


def test_default_retry_policy_returns_fresh_instance():
    """Mutation on one returned policy must not leak into the next call."""
    a = default_retry_policy()
    b = default_retry_policy()
    a.maximum_attempts = 99
    assert b.maximum_attempts == 3


async def test_activity_raises_when_no_annotation_and_no_override(store):
    """Inference fails loudly when neither annotation nor override is
    available — better than silently using a wrong type."""
    wf = _wf(store)

    async def no_annotation(req):  # type: ignore[no-untyped-def]
        return Int32Value(value=1)

    with pytest.raises(ValueError, match="result_type"):
        await wf.activity(
            no_annotation,
            Int32Value(value=1),
            activity_id="missing-result-type",
            retry_timer_id="retry:missing-result-type",
        )


# Silence unused-import warnings for Duration / typing helpers used elsewhere.
_ = Duration
