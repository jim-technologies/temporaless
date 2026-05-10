"""Strict compatibility adapter: run Temporaless-shaped unary protobuf
handlers on the real Temporal Python SDK.

The adapter does not emulate the Temporal server. It delegates activities,
retries, timeouts, and durable timers to ``temporalio``.

**Async-only.** Workflow and activity bodies must be ``async def``. Modern
Python I/O is async-first; the Temporal Python SDK is async-first; the
framework picks the same direction. Synchronous bodies are rejected at
``wrap_*`` time with a clear error rather than tolerated via runtime hedge.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from types import FunctionType
from typing import Any, TypeVar, cast

from google.protobuf.message import Message
from temporalio import activity, workflow
from temporalio.common import RetryPolicy

Req = TypeVar("Req", bound=Message)
ActivityFunc = Callable[[Req], Awaitable[Message]]
WorkflowFunc = Callable[[Req], Awaitable[Message]]


@dataclass(frozen=True)
class ActivityCall:
    """Options describing how to execute one activity from a workflow body.

    All fields except ``activity`` and ``result_type`` are forwarded to
    ``temporalio.workflow.execute_activity`` unchanged.
    """

    activity: Callable
    result_type: type[Message]
    task_queue: str | None = None
    schedule_to_close_timeout: timedelta | None = None
    schedule_to_start_timeout: timedelta | None = None
    start_to_close_timeout: timedelta | None = None
    heartbeat_timeout: timedelta | None = None
    retry_policy: RetryPolicy | None = None
    activity_id: str | None = None


def wrap_activity(
    execute: ActivityFunc[Req],
    *,
    name: str | None = None,
) -> Callable[[Req], Awaitable[Message]]:
    """Decorate an async unary protobuf function as a Temporal activity.

    ``execute`` must be ``async def``. Sync callables are rejected at wrap
    time; this is intentional — modern Python I/O is async, and the
    framework reflects that.
    """
    if execute is None:
        raise ValueError("temporal activity executor is required")
    if not inspect.iscoroutinefunction(execute):
        raise ValueError("temporal activity executor must be async (define it with `async def`)")
    activity_name = name or getattr(execute, "__name__", "")
    if not activity_name:
        raise ValueError("temporal activity name is required")

    async def temporal_activity(input_message: Req) -> Message:
        if not isinstance(input_message, Message):
            raise ValueError("temporal activity input is required")
        result = await execute(input_message)
        if not isinstance(result, Message):
            raise ValueError("temporal activity returned a non-protobuf result")
        return result

    temporal_activity.__name__ = activity_name
    return activity.defn(name=activity_name)(temporal_activity)


def wrap_workflow(
    execute: WorkflowFunc[Req],
    *,
    name: str | None = None,
) -> type:
    """Decorate an async unary protobuf function as a Temporal workflow class.

    ``execute`` must be ``async def``. Sync callables are rejected at wrap
    time.

    Implementation note: the class is built via the ``type()`` builtin (not
    a local ``class`` statement) and ``@workflow.run`` is applied to a free
    function via a clone of ``_workflow_run_impl``. This is required because
    Temporal's ``@workflow.run`` decorator rejects locally-scoped classes,
    and we want each call to ``wrap_workflow`` to produce a distinct class
    so multiple workflows can be registered side-by-side.

    The generated class runs with Temporal's Python workflow sandbox
    disabled because dynamically generated classes are not globally
    importable in the way the sandbox expects. If you need full sandbox
    behavior, define a native ``@workflow.defn`` class directly and use
    ``execute_activity``, ``sleep``, and wrapped activities inside it.
    """
    if execute is None:
        raise ValueError("temporal workflow executor is required")
    if not inspect.iscoroutinefunction(execute):
        raise ValueError("temporal workflow executor must be async (define it with `async def`)")
    workflow_name = name or getattr(execute, "__name__", "")
    if not workflow_name:
        raise ValueError("temporal workflow name is required")

    workflow_type = type(
        workflow_name,
        (),
        {
            "__module__": __name__,
            "_temporaless_execute": staticmethod(execute),
            "run": _make_workflow_run(workflow_name),
        },
    )
    return workflow.defn(name=workflow_name, sandboxed=False)(workflow_type)


async def execute_activity(call: ActivityCall, input_message: Message) -> Message:
    """Schedule a wrapped activity from inside a wrapped workflow body."""
    if call.activity is None:
        raise ValueError("temporal activity is required")
    if not isinstance(input_message, Message):
        raise ValueError("temporal activity input is required")
    if call.result_type is None:
        raise ValueError("temporal activity result type is required")
    result = await workflow.execute_activity(
        cast(Any, call.activity),
        input_message,
        task_queue=call.task_queue,
        result_type=call.result_type,
        schedule_to_close_timeout=call.schedule_to_close_timeout,
        schedule_to_start_timeout=call.schedule_to_start_timeout,
        start_to_close_timeout=call.start_to_close_timeout,
        heartbeat_timeout=call.heartbeat_timeout,
        retry_policy=call.retry_policy,
        activity_id=call.activity_id,
    )
    if not isinstance(result, call.result_type):
        raise ValueError("temporal activity returned a result with the wrong protobuf type")
    return result


async def sleep(duration: float | timedelta) -> None:
    """Durable sleep — delegates to Temporal SDK's ``workflow.sleep``."""
    await workflow.sleep(duration)


def _make_workflow_run(workflow_name: str) -> Callable:
    """Clone ``_workflow_run_impl`` into a fresh function object and apply
    ``@workflow.run`` to it. Each wrapped workflow needs its own function
    object because Temporal registers handlers by identity.
    """
    run = FunctionType(
        _workflow_run_impl.__code__,
        _workflow_run_impl.__globals__,
        "run",
        _workflow_run_impl.__defaults__,
        _workflow_run_impl.__closure__,
    )
    run.__annotations__ = dict(_workflow_run_impl.__annotations__)
    run.__module__ = __name__
    run.__qualname__ = f"{workflow_name}.run"
    return workflow.run(run)


async def _workflow_run_impl(self, input_message: Message) -> Message:
    if not isinstance(input_message, Message):
        raise ValueError("temporal workflow input is required")
    result = await self._temporaless_execute(input_message)
    if not isinstance(result, Message):
        raise ValueError("temporal workflow returned a non-protobuf result")
    return result
