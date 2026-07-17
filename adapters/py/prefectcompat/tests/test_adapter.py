"""Tests for the Prefect compatibility adapter."""

from __future__ import annotations

import base64
import json
import subprocess
import sys
from typing import Any, cast

import pytest
from google.protobuf.message import Message
from google.protobuf.wrappers_pb2 import Int32Value, StringValue
from prefect import task as prefect_task
from prefect.events.schemas.deployment_triggers import DeploymentEventTrigger
from prefect.flows import Flow, load_flow_from_entrypoint
from prefect.schedules import Cron
from prefect.states import Completed
from prefect.tasks import Task

from temporaless_prefectcompat import (
    ActivityWrapOptions,
    PrefectActivity,
    ProtobufFlow,
    WorkflowWrapOptions,
    wrap_activity,
    wrap_workflow,
)


async def echo_activity(req: StringValue) -> StringValue:
    return StringValue(value=req.value)


async def nil_input_activity(_req: StringValue) -> StringValue:
    return cast(Any, None)


async def echo_workflow(req: StringValue) -> StringValue:
    return StringValue(value=f"flow:{req.value}")


async def nil_input_workflow(_req: StringValue) -> StringValue:
    return cast(Any, None)


async def wrong_type_workflow(_req: StringValue) -> StringValue:
    return cast(Any, Int32Value(value=7))


async def wrong_type_activity(_req: StringValue) -> StringValue:
    return cast(Any, Int32Value(value=7))


def test_wrap_helpers_expose_typed_prefect_surfaces() -> None:
    activity: PrefectActivity[StringValue, StringValue] = wrap_activity(echo_activity)
    flow: ProtobufFlow[StringValue, StringValue] = wrap_workflow(echo_workflow)

    assert isinstance(activity, Task)
    assert isinstance(flow, Flow)
    assert callable(activity.submit)
    assert callable(activity.delay)
    assert callable(flow.serialize_parameters)
    assert callable(flow.to_deployment)


async def test_wrap_activity_runs_directly_and_preserves_protobuf_contract() -> None:
    """A wrapped activity is callable directly (outside a flow) and round-trips
    protobuf messages."""
    wrapped = wrap_activity(echo_activity, ActivityWrapOptions(name="echo"))
    result = await wrapped(StringValue(value="AAPL"))
    assert isinstance(result, StringValue)
    assert result.value == "AAPL"


async def test_wrap_workflow_runs_via_prefect_and_returns_protobuf() -> None:
    """A wrapped workflow runs as a Prefect flow — the call goes through
    Prefect's orchestration (run tracking, logger), but the handler's
    contract stays protobuf."""
    wrapped = wrap_workflow(echo_workflow, WorkflowWrapOptions(name="EchoFlow"))
    assert isinstance(wrapped, Flow)
    result = await wrapped(StringValue(value="AAPL"))
    assert isinstance(result, StringValue)
    assert result.value == "flow:AAPL"


async def test_wrap_workflow_serializes_deterministic_protobuf_envelope() -> None:
    wrapped = wrap_workflow(echo_workflow, WorkflowWrapOptions(name="SerializedEchoFlow"))
    flow = cast(Any, wrapped)
    request = StringValue(value="AAPL")

    serialized = flow.serialize_parameters({"input_message": request})

    expected_binary = request.SerializeToString(deterministic=True)
    envelope = serialized["input_message"]
    assert envelope == {
        "__temporaless_protobuf_binary__": 1,
        "type_name": "google.protobuf.StringValue",
        "data_base64": base64.b64encode(expected_binary).decode("ascii"),
    }
    assert "<StringValue>" not in json.dumps(serialized)
    assert json.loads(json.dumps(serialized)) == serialized

    result = await wrapped(envelope)
    assert isinstance(result, StringValue)
    assert result.value == "flow:AAPL"


async def test_wrap_workflow_serializes_deployment_default_parameters() -> None:
    from deployment_flow import DeployedEchoFlow

    flow = cast(Any, DeployedEchoFlow)

    deployment = await flow.to_deployment(
        "deployment-echo",
        parameters={"input_message": StringValue(value="scheduled")},
        triggers=[
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed"},
                parameters={"input_message": StringValue(value="triggered")},
            )
        ],
    )

    envelope = deployment.parameters["input_message"]
    assert envelope["__temporaless_protobuf_binary__"] == 1
    assert envelope["type_name"] == "google.protobuf.StringValue"
    assert StringValue.FromString(base64.b64decode(envelope["data_base64"])).value == "scheduled"
    json.loads(deployment.model_dump_json())
    trigger_envelope = deployment.triggers[0].parameters["input_message"]
    assert (
        StringValue.FromString(base64.b64decode(trigger_envelope["data_base64"])).value
        == "triggered"
    )

    loaded = load_flow_from_entrypoint(deployment.entrypoint, use_placeholder_flow=False)
    loaded_parameters = loaded.serialize_parameters(
        {"input_message": StringValue(value="reloaded")}
    )
    assert loaded_parameters["input_message"]["type_name"] == "google.protobuf.StringValue"
    result = await loaded(loaded_parameters["input_message"])
    assert result.value == "deployed:reloaded"

    probe = """
import asyncio
import json
import sys
from prefect.flows import load_flow_from_entrypoint

flow = load_flow_from_entrypoint(sys.argv[1], use_placeholder_flow=False)
parameters = json.loads(sys.argv[2])
result = asyncio.run(flow.fn(parameters["input_message"]))
print(json.dumps({"name": flow.name, "retries": flow.retries, "value": result.value}))
"""
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            probe,
            deployment.entrypoint,
            json.dumps(deployment.parameters),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    reloaded = json.loads(completed.stdout.strip().splitlines()[-1])
    assert reloaded == {
        "name": "DeployedEchoFlow",
        "retries": 0,
        "value": "deployed:scheduled",
    }


def test_wrap_workflow_rejects_with_options() -> None:
    wrapped = wrap_workflow(echo_workflow, WorkflowWrapOptions(name="OriginalEchoFlow"))
    with pytest.raises(ValueError, match="with_options is unsupported"):
        wrapped.with_options(name="UpdatedEchoFlow")


async def test_wrap_workflow_preserves_prefect_async_dispatch_api() -> None:
    wrapped = cast(
        Any,
        wrap_workflow(echo_workflow, WorkflowWrapOptions(name="AsyncDispatchEchoFlow")),
    )

    assert callable(wrapped.to_deployment.aio)
    assert callable(wrapped.deploy.aio)


async def test_wrap_workflow_honors_prefect_async_dispatch_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deployment_flow import DeployedEchoFlow

    async def capture_async(
        _flow: Flow,
        name: str,
        *,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {"mode": "async", "name": name, "parameters": parameters}

    def capture_sync(
        _flow: Flow,
        name: str,
        *,
        parameters: dict[str, Any] | None = None,
        _sync: bool | None = None,
    ) -> dict[str, Any]:
        return {
            "mode": "sync",
            "name": name,
            "parameters": parameters,
            "_sync": _sync,
        }

    monkeypatch.setattr(Flow, "ato_deployment", capture_async)
    monkeypatch.setattr(Flow, "to_deployment", capture_sync)
    flow = cast(Any, DeployedEchoFlow)
    request = StringValue(value="dispatch")

    asynchronous = await flow.to_deployment.aio(
        flow,
        "async-deployment",
        parameters={"input_message": request},
    )
    synchronous = flow.to_deployment(
        "sync-deployment",
        parameters={"input_message": request},
        _sync=True,
    )

    assert asynchronous["mode"] == "async"
    assert asynchronous["parameters"]["input_message"]["type_name"] == (
        "google.protobuf.StringValue"
    )
    assert synchronous["mode"] == "sync"
    assert synchronous["_sync"] is True
    assert synchronous["parameters"]["input_message"]["type_name"] == (
        "google.protobuf.StringValue"
    )


@pytest.mark.parametrize("method_name", ["ato_deployment", "adeploy"])
async def test_wrap_workflow_serializes_explicit_async_deployment_and_schedule_parameters(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    from deployment_flow import DeployedEchoFlow

    async def capture(
        _flow: Flow,
        name: str,
        *,
        parameters: dict[str, Any] | None = None,
        schedule: Any = None,
        schedules: Any = None,
        triggers: Any = None,
    ) -> dict[str, Any]:
        return {
            "name": name,
            "parameters": parameters,
            "schedule": schedule,
            "schedules": schedules,
            "triggers": triggers,
        }

    monkeypatch.setattr(Flow, method_name, capture)
    flow = cast(Any, DeployedEchoFlow)
    request = StringValue(value=method_name)
    schedule = Cron(
        "0 0 * * *",
        parameters={"input_message": request},
    )
    trigger: Any
    if method_name == "ato_deployment":
        trigger = DeploymentEventTrigger(
            expect={"prefect.flow-run.Completed"},
            parameters={"input_message": request},
        )
    else:
        trigger = {
            "type": "event",
            "expect": ["prefect.flow-run.Completed"],
            "parameters": {"input_message": request},
        }
    kwargs: dict[str, Any] = {
        "parameters": {"input_message": request},
        "schedule": schedule if method_name == "ato_deployment" else None,
        "schedules": [schedule] if method_name == "adeploy" else None,
        "triggers": [trigger],
    }

    captured = await getattr(flow, method_name)("deployment-echo", **kwargs)

    assert captured["parameters"]["input_message"]["__temporaless_protobuf_binary__"] == 1
    captured_schedules = (
        [captured["schedule"]] if captured["schedule"] is not None else captured["schedules"]
    )
    envelope = captured_schedules[0].parameters["input_message"]
    assert envelope["type_name"] == "google.protobuf.StringValue"
    assert StringValue.FromString(base64.b64decode(envelope["data_base64"])).value == method_name
    captured_trigger = captured["triggers"][0]
    if isinstance(captured_trigger, DeploymentEventTrigger):
        json.loads(captured_trigger.model_dump_json())
        trigger_parameters = captured_trigger.parameters
    else:
        json.loads(json.dumps(captured_trigger))
        trigger_parameters = captured_trigger["parameters"]
    trigger_envelope = trigger_parameters["input_message"]
    assert trigger_envelope["type_name"] == "google.protobuf.StringValue"
    assert (
        StringValue.FromString(base64.b64decode(trigger_envelope["data_base64"])).value
        == method_name
    )


async def test_wrap_workflow_accepts_resolved_prefect_future_messages() -> None:
    @prefect_task
    async def produce() -> StringValue:
        return StringValue(value="future")

    inner = wrap_workflow(
        echo_workflow,
        WorkflowWrapOptions(name="FutureInputInnerFlow"),
    )

    async def outer_body(req: StringValue) -> StringValue:
        future = produce.submit()
        result = await cast(Any, inner)(future)
        return StringValue(value=f"{req.value}:{result.value}")

    outer = wrap_workflow(
        outer_body,
        WorkflowWrapOptions(name="FutureInputOuterFlow"),
    )
    result = await outer(StringValue(value="outer"))
    state_result = await cast(Any, inner)(
        Completed(data=StringValue(value="state")),
    )

    assert result.value == "outer:flow:future"
    assert state_result.value == "flow:state"


async def test_local_workflow_is_direct_only_and_rejects_deployment() -> None:
    async def local(req: StringValue) -> StringValue:
        return StringValue(value=f"local:{req.value}")

    flow = wrap_workflow(
        local,
        WorkflowWrapOptions(name="LocalDirectFlow"),
    )
    result = await flow(StringValue(value="ok"))
    assert result.value == "local:ok"

    with pytest.raises(ValueError, match="module-level workflow executor"):
        await flow.to_deployment("local-deployment")


async def test_wrap_activity_inside_workflow_records_a_prefect_task_run() -> None:
    """Calling a wrap_activity-decorated callable inside a wrap_workflow-decorated
    flow registers a real Prefect task run — exercising the integration end-to-end."""
    inner = wrap_activity(echo_activity, ActivityWrapOptions(name="echo_inside_flow"))

    async def composed(req: StringValue) -> StringValue:
        intermediate = await inner(StringValue(value=f"step1:{req.value}"))
        return StringValue(value=f"step2:{intermediate.value}")

    outer = wrap_workflow(composed, WorkflowWrapOptions(name="ComposedFlow"))
    result = await outer(StringValue(value="AAPL"))
    assert result.value == "step2:step1:AAPL"


async def test_wrap_activity_validates_protobuf_contract() -> None:
    """Non-protobuf inputs and non-protobuf return values fail loud."""
    wrapped_nil_input = wrap_activity(echo_activity, ActivityWrapOptions(name="echo_nil"))
    with pytest.raises(ValueError, match="activity input is required"):
        await wrapped_nil_input(cast(StringValue, None))

    wrapped_nil_result = wrap_activity(
        cast(Any, nil_input_activity), ActivityWrapOptions(name="nil_result")
    )
    with pytest.raises(ValueError, match="non-protobuf result"):
        await wrapped_nil_result(StringValue(value="AAPL"))

    wrapped_wrong_result = wrap_activity(
        wrong_type_activity,
        ActivityWrapOptions(name="wrong_type_activity"),
    )
    with pytest.raises(ValueError, match="wrong protobuf result type"):
        await wrapped_wrong_result(StringValue(value="AAPL"))

    with pytest.raises(ValueError, match="wrong protobuf type"):
        await cast(Any, wrapped_wrong_result)(Int32Value(value=7))


async def test_wrap_workflow_validates_protobuf_contract() -> None:
    wrapped_nil_input = wrap_workflow(echo_workflow, WorkflowWrapOptions(name="EchoNil"))
    with pytest.raises(ValueError, match="protobuf binary envelope"):
        await wrapped_nil_input(cast(StringValue, None))

    wrapped_nil_result = wrap_workflow(
        cast(Any, nil_input_workflow), WorkflowWrapOptions(name="NilResult")
    )
    with pytest.raises(ValueError, match="non-protobuf result"):
        await wrapped_nil_result(StringValue(value="AAPL"))

    wrapped_wrong_result = wrap_workflow(
        wrong_type_workflow,
        WorkflowWrapOptions(name="WrongResultType"),
    )
    with pytest.raises(ValueError, match="wrong protobuf result type"):
        await wrapped_wrong_result(StringValue(value="AAPL"))


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda envelope: {
                key: value for key, value in envelope.items() if key != "data_base64"
            },
            "invalid fields",
        ),
        (
            lambda envelope: {**envelope, "unexpected": True},
            "invalid fields",
        ),
        (
            lambda envelope: {
                **envelope,
                "__temporaless_protobuf_binary__": 2,
            },
            "unsupported version",
        ),
        (
            lambda envelope: {
                **envelope,
                "type_name": "google.protobuf.Int32Value",
            },
            "wrong message type",
        ),
        (
            lambda envelope: {**envelope, "data_base64": "***"},
            "invalid base64",
        ),
        (
            lambda envelope: {
                **envelope,
                "data_base64": base64.b64encode(b"\xff").decode("ascii"),
            },
            "invalid protobuf binary",
        ),
    ],
)
async def test_wrap_workflow_rejects_malformed_protobuf_envelopes(
    mutate: Any,
    message: str,
) -> None:
    wrapped = wrap_workflow(echo_workflow, WorkflowWrapOptions(name=f"Malformed{message}"))
    flow = cast(Any, wrapped)
    serialized = flow.serialize_parameters({"input_message": StringValue(value="AAPL")})

    with pytest.raises(ValueError, match=message):
        await wrapped(mutate(serialized["input_message"]))


async def test_wrap_workflow_rejects_wrong_protobuf_request_type() -> None:
    wrapped = wrap_workflow(echo_workflow, WorkflowWrapOptions(name="WrongRequestType"))
    flow = cast(Any, wrapped)

    with pytest.raises(ValueError, match="wrong protobuf type"):
        await cast(Any, wrapped)(Int32Value(value=7))
    with pytest.raises(ValueError, match="wrong protobuf type"):
        flow.serialize_parameters({"input_message": Int32Value(value=7)})


@pytest.mark.parametrize("shape", ["missing", "ambiguous", "base"])
def test_wrap_workflow_requires_one_concrete_request_message_annotation(shape: str) -> None:
    if shape == "missing":

        async def invalid(request) -> StringValue:
            return StringValue(value=str(request))

    elif shape == "ambiguous":

        async def invalid(request: StringValue | Int32Value) -> StringValue:
            return StringValue(value=str(request))

    else:

        async def invalid(request: Message) -> StringValue:
            return StringValue(value=str(request))

    with pytest.raises(ValueError, match="concrete protobuf message class"):
        wrap_workflow(cast(Any, invalid), WorkflowWrapOptions(name=f"Invalid{shape}"))


@pytest.mark.parametrize("boundary", ["activity", "workflow"])
def test_wrap_helpers_require_concrete_response_message_annotation(boundary: str) -> None:
    async def invalid(_request: StringValue):
        return StringValue()

    with pytest.raises(ValueError, match=f"prefect {boundary} response annotation"):
        if boundary == "activity":
            wrap_activity(cast(Any, invalid), ActivityWrapOptions(name="InvalidResponse"))
        else:
            wrap_workflow(cast(Any, invalid), WorkflowWrapOptions(name="InvalidResponse"))


def test_wrap_workflow_requires_exactly_one_request_argument() -> None:
    async def invalid(_left: StringValue, _right: StringValue) -> StringValue:
        return StringValue()

    with pytest.raises(ValueError, match="exactly one protobuf request"):
        wrap_workflow(cast(Any, invalid), WorkflowWrapOptions(name="InvalidArity"))


def test_wrap_helpers_reject_sync_executors() -> None:
    """Async-only stance — sync functions fail at wrap time, not at runtime."""

    def sync_activity(_req: StringValue) -> StringValue:
        return StringValue(value="should-not-reach")

    def sync_workflow(_req: StringValue) -> StringValue:
        return StringValue(value="should-not-reach")

    with pytest.raises(ValueError, match="must be async"):
        wrap_activity(cast(Any, sync_activity), ActivityWrapOptions(name="sync_activity"))
    with pytest.raises(ValueError, match="must be async"):
        wrap_workflow(cast(Any, sync_workflow), WorkflowWrapOptions(name="SyncWorkflow"))


def test_wrap_helpers_validate_required_fields() -> None:
    with pytest.raises(ValueError, match="activity executor"):
        wrap_activity(cast(Any, None), ActivityWrapOptions(name="x"))
    with pytest.raises(ValueError, match="workflow executor"):
        wrap_workflow(cast(Any, None), WorkflowWrapOptions(name="x"))

    async def anon(_req: StringValue) -> None:
        return None

    anon.__name__ = ""
    with pytest.raises(ValueError, match="activity name"):
        wrap_activity(cast(Any, anon), ActivityWrapOptions())
    with pytest.raises(ValueError, match="workflow name"):
        wrap_workflow(cast(Any, anon), WorkflowWrapOptions())


def test_wrap_helpers_reject_wrong_options_type() -> None:
    with pytest.raises(ValueError, match="activity wrap options"):
        wrap_activity(echo_activity, cast(Any, WorkflowWrapOptions()))
    with pytest.raises(ValueError, match="workflow wrap options"):
        wrap_workflow(echo_workflow, cast(Any, ActivityWrapOptions()))


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (ActivityWrapOptions(name="  "), "name must not be blank"),
        (ActivityWrapOptions(name=cast(Any, 123)), "name must be a string"),
        (ActivityWrapOptions(retries=-1), "retries must be a non-negative integer"),
        (
            ActivityWrapOptions(retries=cast(Any, True)),
            "retries must be a non-negative integer",
        ),
        (
            ActivityWrapOptions(retry_delay_seconds=-0.1),
            "retry_delay_seconds must be a finite non-negative number",
        ),
        (
            ActivityWrapOptions(retry_delay_seconds=cast(Any, "soon")),
            "retry_delay_seconds must be a finite non-negative number",
        ),
    ],
)
def test_wrap_activity_rejects_invalid_options(options: ActivityWrapOptions, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        wrap_activity(echo_activity, options)


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (WorkflowWrapOptions(name="\t"), "name must not be blank"),
        (WorkflowWrapOptions(retries=-1), "retries must be a non-negative integer"),
        (
            WorkflowWrapOptions(retry_delay_seconds=float("inf")),
            "retry_delay_seconds must be a finite non-negative number",
        ),
    ],
)
def test_wrap_workflow_rejects_invalid_options(options: WorkflowWrapOptions, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        wrap_workflow(echo_workflow, options)


async def test_wrap_workflow_applies_explicit_prefect_options() -> None:
    """The typed options reach the underlying Prefect flow."""
    wrapped = wrap_workflow(
        echo_workflow,
        WorkflowWrapOptions(name="RetryFlow", retries=2, retry_delay_seconds=0),
    )
    # prefect.flow attaches a Flow object with `.retries` available.
    assert getattr(wrapped, "retries", None) == 2


async def test_wrap_activity_applies_explicit_prefect_options() -> None:
    wrapped = wrap_activity(
        echo_activity,
        ActivityWrapOptions(name="retry_task", retries=3, retry_delay_seconds=0),
    )
    assert getattr(wrapped, "retries", None) == 3
