"""Strict compatibility adapter: expose Temporaless-shaped unary protobuf
handlers as Prefect flows and tasks.

The adapter does not emulate Prefect's runtime. It wraps a Temporaless
handler in ``prefect.flow`` / ``prefect.task`` so the same handler runs
inside Prefect's orchestration (run tracking, UI visibility, scheduling)
*and* against a Temporaless ``Store`` if the body uses ``current_workflow``.

**Async-only.** Like the rest of the framework, only ``async def`` handlers
are accepted. Sync callables fail loud at wrap time.

Compatibility scope:

- one protobuf workflow request and one protobuf workflow response
- one protobuf activity request and one protobuf activity response
- Prefect's flow/task instrumentation: run id, logger, names, and retries
- one small, typed options object for each wrapper boundary

The handler's *protobuf* contract is preserved: input must be a
``Message``, output must be a ``Message``. Type drift fails loudly.
"""

from __future__ import annotations

import base64
import binascii
import inspect
import math
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any, Protocol, TypeVar, cast, get_type_hints

from google.protobuf.message import DecodeError, Message
from prefect import task as prefect_task
from prefect._internal.compatibility.async_dispatch import async_dispatch
from prefect.client.schemas.actions import DeploymentScheduleCreate, DeploymentScheduleUpdate
from prefect.events.schemas.deployment_triggers import BaseDeploymentTrigger
from prefect.flows import Flow
from prefect.futures import PrefectFuture
from prefect.schedules import Schedule
from prefect.states import State

Req = TypeVar("Req", bound=Message)
Resp = TypeVar("Resp", bound=Message)

ActivityFunc = Callable[[Req], Awaitable[Resp]]
WorkflowFunc = Callable[[Req], Awaitable[Resp]]


class PrefectActivity(Protocol[Req, Resp]):
    """Typed public surface of a Temporaless-shaped Prefect task."""

    def __call__(self, input_message: Req) -> Awaitable[Resp]: ...

    def submit(self, input_message: Req) -> PrefectFuture[Resp]: ...


_PROTOBUF_ENVELOPE_VERSION_KEY = "__temporaless_protobuf_binary__"
_PROTOBUF_ENVELOPE_TYPE_KEY = "type_name"
_PROTOBUF_ENVELOPE_DATA_KEY = "data_base64"
_PROTOBUF_ENVELOPE_VERSION = 1
_PROTOBUF_ENVELOPE_KEYS = frozenset(
    {
        _PROTOBUF_ENVELOPE_VERSION_KEY,
        _PROTOBUF_ENVELOPE_TYPE_KEY,
        _PROTOBUF_ENVELOPE_DATA_KEY,
    }
)
_FLOW_INPUT_PARAMETER = "input_message"


@dataclass(frozen=True, slots=True)
class ActivityWrapOptions:
    """Explicit Prefect task-definition options."""

    name: str | None = None
    retries: int | None = None
    retry_delay_seconds: int | float | None = None


@dataclass(frozen=True, slots=True)
class WorkflowWrapOptions:
    """Explicit Prefect flow-definition options."""

    name: str | None = None
    retries: int | None = None
    retry_delay_seconds: int | float | None = None


def _validate_wrap_options(
    boundary: str,
    options: ActivityWrapOptions | WorkflowWrapOptions,
) -> None:
    if options.name is not None:
        if not isinstance(options.name, str):
            raise ValueError(f"prefect {boundary} name must be a string")
        if not options.name.strip():
            raise ValueError(f"prefect {boundary} name must not be blank")
    if options.retries is not None and (type(options.retries) is not int or options.retries < 0):
        raise ValueError(f"prefect {boundary} retries must be a non-negative integer")
    if options.retry_delay_seconds is not None:
        delay = options.retry_delay_seconds
        if type(delay) not in (int, float) or delay < 0:
            raise ValueError(
                f"prefect {boundary} retry_delay_seconds must be a finite non-negative number"
            )
        if type(delay) is float and not math.isfinite(delay):
            raise ValueError(
                f"prefect {boundary} retry_delay_seconds must be a finite non-negative number"
            )


def _message_types(
    boundary: str,
    execute: Callable[..., Awaitable[Message]],
) -> tuple[type[Message], type[Message]]:
    """Resolve the one concrete protobuf request and response type."""
    signature = inspect.signature(execute)
    parameters = list(signature.parameters.values())
    if len(parameters) != 1 or parameters[0].kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise ValueError(
            f"prefect {boundary} executor must accept exactly one protobuf request argument"
        )

    try:
        hints = get_type_hints(execute)
    except (NameError, TypeError) as exc:
        raise ValueError(
            f"prefect {boundary} request annotation must resolve to a concrete "
            "protobuf message class"
        ) from exc

    request_type = hints.get(parameters[0].name, parameters[0].annotation)
    if not _is_concrete_message_type(request_type):
        raise ValueError(
            f"prefect {boundary} request annotation must be a concrete protobuf message class"
        )

    response_annotation = hints.get("return", signature.return_annotation)
    if not _is_concrete_message_type(response_annotation):
        raise ValueError(
            f"prefect {boundary} response annotation must be a concrete protobuf message class"
        )
    return request_type, response_annotation


def _is_concrete_message_type(value: object) -> bool:
    return (
        isinstance(value, type)
        and value is not Message
        and issubclass(value, Message)
        and getattr(value, "DESCRIPTOR", None) is not None
    )


def _encode_protobuf_request(request: Message, request_type: type[Message]) -> dict[str, object]:
    if type(request) is not request_type:
        raise ValueError(
            "prefect workflow input has wrong protobuf type: "
            f"expected {request_type.DESCRIPTOR.full_name}, "
            f"got {_message_type_name(request)}"
        )
    payload = request.SerializeToString(deterministic=True)
    return {
        _PROTOBUF_ENVELOPE_VERSION_KEY: _PROTOBUF_ENVELOPE_VERSION,
        _PROTOBUF_ENVELOPE_TYPE_KEY: request_type.DESCRIPTOR.full_name,
        _PROTOBUF_ENVELOPE_DATA_KEY: base64.b64encode(payload).decode("ascii"),
    }


def _decode_protobuf_request(
    value: object,
    request_type: type[Message],
) -> Message:
    if isinstance(value, Message):
        if type(value) is not request_type:
            raise ValueError(
                "prefect workflow input has wrong protobuf type: "
                f"expected {request_type.DESCRIPTOR.full_name}, "
                f"got {_message_type_name(value)}"
            )
        return value
    if not isinstance(value, dict):
        raise ValueError(
            "prefect workflow input must be "
            f"{request_type.DESCRIPTOR.full_name} or its protobuf binary envelope"
        )
    if set(value) != _PROTOBUF_ENVELOPE_KEYS:
        raise ValueError("prefect workflow protobuf envelope has invalid fields")

    version = value.get(_PROTOBUF_ENVELOPE_VERSION_KEY)
    type_name = value.get(_PROTOBUF_ENVELOPE_TYPE_KEY)
    encoded = value.get(_PROTOBUF_ENVELOPE_DATA_KEY)
    if type(version) is not int or version != _PROTOBUF_ENVELOPE_VERSION:
        raise ValueError("prefect workflow protobuf envelope has unsupported version")
    if type(type_name) is not str or type_name != request_type.DESCRIPTOR.full_name:
        raise ValueError(
            "prefect workflow protobuf envelope has wrong message type: "
            f"expected {request_type.DESCRIPTOR.full_name}, got {type_name!r}"
        )
    if type(encoded) is not str:
        raise ValueError("prefect workflow protobuf envelope data_base64 must be a string")

    try:
        payload = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("prefect workflow protobuf envelope contains invalid base64") from exc

    request = request_type()
    try:
        request.ParseFromString(payload)
    except DecodeError as exc:
        raise ValueError(
            "prefect workflow protobuf envelope contains invalid protobuf binary"
        ) from exc
    if not request.IsInitialized():
        raise ValueError("prefect workflow protobuf envelope contains an uninitialized message")
    return request


def _message_type_name(value: Message) -> str:
    descriptor = getattr(value, "DESCRIPTOR", None)
    return descriptor.full_name if descriptor is not None else type(value).__qualname__


class ProtobufFlow[FlowReq: Message, FlowResp: Message](Flow[..., Awaitable[FlowResp]]):
    """Prefect Flow whose external parameter is deterministic protobuf binary."""

    _temporaless_request_type: type[Message]
    _temporaless_entrypoint_globals: dict[str, Any] | None
    _temporaless_entrypoint_module: str | None
    _temporaless_executor_is_local: bool

    def serialize_parameters(self, parameters: dict[str, Any]) -> dict[str, Any]:
        serialized = dict(parameters)
        if _FLOW_INPUT_PARAMETER in serialized:
            value = serialized[_FLOW_INPUT_PARAMETER]
            if not isinstance(value, (PrefectFuture, State)):
                request = _decode_protobuf_request(
                    value,
                    self._temporaless_request_type,
                )
                serialized[_FLOW_INPUT_PARAMETER] = _encode_protobuf_request(
                    request,
                    self._temporaless_request_type,
                )
        return super().serialize_parameters(serialized)

    def _serialize_schedule(self, schedule: object) -> object:
        if isinstance(schedule, Schedule):
            return replace(
                schedule,
                parameters=self.serialize_parameters(schedule.parameters),
            )
        if isinstance(schedule, (DeploymentScheduleCreate, DeploymentScheduleUpdate)):
            if schedule.parameters is None:
                return schedule
            return schedule.model_copy(
                update={"parameters": self.serialize_parameters(schedule.parameters)}
            )
        if isinstance(schedule, dict):
            serialized = dict(schedule)
            if "parameters" in serialized and serialized["parameters"] is not None:
                parameters = serialized["parameters"]
                if not isinstance(parameters, dict):
                    raise ValueError("Prefect schedule parameters must be a dictionary")
                serialized["parameters"] = self.serialize_parameters(parameters)
            return serialized
        return schedule

    def _serialize_trigger(self, trigger: object) -> object:
        if isinstance(trigger, BaseDeploymentTrigger):
            if trigger.parameters is None:
                return trigger
            return trigger.model_copy(
                update={"parameters": self.serialize_parameters(trigger.parameters)}
            )
        if isinstance(trigger, dict):
            serialized = dict(trigger)
            if "parameters" in serialized and serialized["parameters"] is not None:
                parameters = serialized["parameters"]
                if not isinstance(parameters, dict):
                    raise ValueError("Prefect trigger parameters must be a dictionary")
                serialized["parameters"] = self.serialize_parameters(parameters)
            return serialized
        return trigger

    def _prepare_deployment_entrypoint(self) -> None:
        if self._temporaless_executor_is_local:
            raise ValueError(
                "Prefect deployment and serve require a module-level workflow executor; "
                "local workflow handlers are direct/subflow-only"
            )
        function_globals = self._temporaless_entrypoint_globals
        module_name = self._temporaless_entrypoint_module
        if function_globals is None or module_name is None:
            raise ValueError(
                "Prefect deployment and serve require a module-backed workflow executor"
            )
        names = sorted(
            (
                name
                for name, value in function_globals.items()
                if value is self and name.isidentifier()
            ),
            key=lambda name: (name.startswith("_"), name),
        )
        if not names:
            raise ValueError(
                "Prefect deployment and serve require the wrapped flow to have a "
                "module-global binding that is recreated when its module is imported"
            )

        entrypoint_name = names[0]
        flow_metadata = cast(Any, self)
        flow_metadata.__globals__ = function_globals
        flow_metadata.__name__ = entrypoint_name
        flow_metadata.__qualname__ = entrypoint_name
        self.__module__ = module_name
        self.fn.__module__ = module_name
        function_metadata = cast(Any, self.fn)
        function_metadata.__name__ = entrypoint_name
        function_metadata.__qualname__ = entrypoint_name
        self._entrypoint = f"{module_name}:{entrypoint_name}"

    def _call_with_serialized_parameters(
        self,
        method: Callable[..., Any],
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        dispatch_sync: bool = False,
    ) -> Any:
        """Encode deployment defaults before Prefect builds its API model."""
        self._prepare_deployment_entrypoint()
        bound = inspect.signature(method).bind_partial(*args, **kwargs)
        parameters = bound.arguments.get("parameters")
        if parameters is not None:
            if not isinstance(parameters, dict):
                raise ValueError("Prefect deployment parameters must be a dictionary")
            bound.arguments["parameters"] = self.serialize_parameters(parameters)
        schedule = bound.arguments.get("schedule")
        if schedule is not None:
            bound.arguments["schedule"] = self._serialize_schedule(schedule)
        schedules = bound.arguments.get("schedules")
        if schedules is not None:
            if not isinstance(schedules, Sequence) or isinstance(
                schedules, (str, bytes, bytearray)
            ):
                raise ValueError("Prefect schedules must be a sequence")
            bound.arguments["schedules"] = [
                self._serialize_schedule(schedule) for schedule in schedules
            ]
        triggers = bound.arguments.get("triggers")
        if triggers is not None:
            if not isinstance(triggers, Sequence) or isinstance(triggers, (str, bytes, bytearray)):
                raise ValueError("Prefect triggers must be a sequence")
            bound.arguments["triggers"] = [self._serialize_trigger(trigger) for trigger in triggers]
        call_kwargs = dict(bound.kwargs)
        if dispatch_sync:
            call_kwargs["_sync"] = True
        return method(*bound.args, **call_kwargs)

    def with_options(self, **kwargs: Any) -> ProtobufFlow[FlowReq, FlowResp]:
        del kwargs
        raise ValueError(
            "Prefect Flow.with_options is unsupported for wrapped workflows; "
            "set name and retry policy in WorkflowWrapOptions"
        )

    async def ato_deployment(self, *args: object, **kwargs: object) -> Any:
        return await self._call_with_serialized_parameters(
            super().ato_deployment,
            args,
            kwargs,
        )

    @async_dispatch(ato_deployment)
    def to_deployment(self, *args: object, **kwargs: object) -> Any:
        return self._call_with_serialized_parameters(
            super().to_deployment,
            args,
            kwargs,
            dispatch_sync=True,
        )

    async def adeploy(self, *args: object, **kwargs: object) -> Any:
        return await self._call_with_serialized_parameters(
            super().adeploy,
            args,
            kwargs,
        )

    @async_dispatch(adeploy)
    def deploy(self, *args: object, **kwargs: object) -> Any:
        return self._call_with_serialized_parameters(
            super().deploy,
            args,
            kwargs,
            dispatch_sync=True,
        )

    def serve(self, *args: object, **kwargs: object) -> Any:
        return self._call_with_serialized_parameters(
            super().serve,
            args,
            kwargs,
        )


def wrap_activity(
    execute: ActivityFunc[Req, Resp],
    options: ActivityWrapOptions | None = None,
) -> PrefectActivity[Req, Resp]:
    """Wrap a Temporaless-shaped async activity as a Prefect task.

    The wrapped callable is ``await``-able just like the original. Calling
    it from inside a Prefect flow registers a Prefect task run; calling it
    standalone runs it directly. Either way, the protobuf-shape contract is
    enforced.

    Args:
        execute: ``async def(req: ProtoMessage) -> ProtoMessage``.
        options: Explicit Prefect task name and retry policy. The name
            defaults to ``execute.__name__``.
    """
    if execute is None:
        raise ValueError("prefect activity executor is required")
    if not inspect.iscoroutinefunction(execute):
        raise ValueError("prefect activity executor must be async (define it with `async def`)")
    if options is None:
        options = ActivityWrapOptions()
    elif not isinstance(options, ActivityWrapOptions):
        raise ValueError("prefect activity wrap options are invalid")
    _validate_wrap_options("activity", options)
    task_name = options.name or getattr(execute, "__name__", "")
    if not task_name:
        raise ValueError("prefect activity name is required")
    request_type, response_type = _message_types("activity", execute)

    async def _runner(input_message: Req) -> Resp:
        if type(input_message) is not request_type:
            if not isinstance(input_message, Message):
                raise ValueError("prefect activity input is required")
            raise ValueError(
                "prefect activity input has wrong protobuf type: "
                f"expected {request_type.DESCRIPTOR.full_name}, "
                f"got {_message_type_name(input_message)}"
            )
        result = await execute(input_message)
        if not isinstance(result, Message):
            raise ValueError("prefect activity returned a non-protobuf result")
        if type(result) is not response_type:
            raise ValueError(
                "prefect activity returned the wrong protobuf result type: "
                f"expected {response_type.DESCRIPTOR.full_name}, "
                f"got {_message_type_name(result)}"
            )
        return result

    _runner.__name__ = task_name
    if options.retries is None:
        task = prefect_task(
            name=task_name,
            retry_delay_seconds=options.retry_delay_seconds,
        )(_runner)
    else:
        task = prefect_task(
            name=task_name,
            retries=options.retries,
            retry_delay_seconds=options.retry_delay_seconds,
        )(_runner)
    return cast(PrefectActivity[Req, Resp], task)


def wrap_workflow(
    execute: WorkflowFunc[Req, Resp],
    options: WorkflowWrapOptions | None = None,
) -> ProtobufFlow[Req, Resp]:
    """Wrap a Temporaless-shaped async workflow as a Prefect flow.

    The wrapped callable is ``await``-able just like the original. Calling
    it triggers a Prefect flow run (visible in the Prefect UI / API);
    internally the body runs as written, including any
    ``current_workflow().execute_activity`` / ``sleep`` / ``wait_event``
    calls against your Temporaless ``Store``.

    Args:
        execute: ``async def(req: ProtoMessage) -> ProtoMessage``.
        options: Explicit Prefect flow name and retry policy. The name
            defaults to ``execute.__name__``.
    """
    if execute is None:
        raise ValueError("prefect workflow executor is required")
    if not inspect.iscoroutinefunction(execute):
        raise ValueError("prefect workflow executor must be async (define it with `async def`)")
    if options is None:
        options = WorkflowWrapOptions()
    elif not isinstance(options, WorkflowWrapOptions):
        raise ValueError("prefect workflow wrap options are invalid")
    _validate_wrap_options("workflow", options)
    flow_name = options.name or getattr(execute, "__name__", "")
    if not flow_name:
        raise ValueError("prefect workflow name is required")
    request_type, response_type = _message_types("workflow", execute)

    async def _runner(input_message: object) -> Resp:
        request = _decode_protobuf_request(input_message, request_type)
        result = await execute(cast(Req, request))
        if not isinstance(result, Message):
            raise ValueError("prefect workflow returned a non-protobuf result")
        if type(result) is not response_type:
            raise ValueError(
                "prefect workflow returned the wrong protobuf result type: "
                f"expected {response_type.DESCRIPTOR.full_name}, "
                f"got {_message_type_name(result)}"
            )
        return result

    _runner.__name__ = flow_name
    wrapped = ProtobufFlow(
        fn=_runner,
        name=flow_name,
        retries=options.retries,
        retry_delay_seconds=options.retry_delay_seconds,
        validate_parameters=False,
    )
    wrapped._temporaless_request_type = request_type
    function_globals = getattr(execute, "__globals__", None)
    module_name = getattr(execute, "__module__", None)
    function_qualname = getattr(execute, "__qualname__", "")
    wrapped._temporaless_entrypoint_globals = (
        function_globals if isinstance(function_globals, dict) else None
    )
    wrapped._temporaless_entrypoint_module = module_name if isinstance(module_name, str) else None
    wrapped._temporaless_executor_is_local = "<locals>" in function_qualname
    return wrapped
