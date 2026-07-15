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

import inspect
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from google.protobuf.message import Message
from prefect import flow as prefect_flow
from prefect import task as prefect_task

Req = TypeVar("Req", bound=Message)
Resp = TypeVar("Resp", bound=Message)

ActivityFunc = Callable[[Req], Awaitable[Resp]]
WorkflowFunc = Callable[[Req], Awaitable[Resp]]


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


def wrap_activity(
    execute: ActivityFunc[Req, Resp],
    options: ActivityWrapOptions | None = None,
) -> Callable[[Req], Awaitable[Resp]]:
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

    async def _runner(input_message: Req) -> Resp:
        if not isinstance(input_message, Message):
            raise ValueError("prefect activity input is required")
        result = await execute(input_message)
        if not isinstance(result, Message):
            raise ValueError("prefect activity returned a non-protobuf result")
        return result

    _runner.__name__ = task_name
    if options.retries is None:
        return prefect_task(
            name=task_name,
            retry_delay_seconds=options.retry_delay_seconds,
        )(_runner)
    return prefect_task(
        name=task_name,
        retries=options.retries,
        retry_delay_seconds=options.retry_delay_seconds,
    )(_runner)


def wrap_workflow(
    execute: WorkflowFunc[Req, Resp],
    options: WorkflowWrapOptions | None = None,
) -> Callable[[Req], Awaitable[Resp]]:
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

    async def _runner(input_message: Req) -> Resp:
        if not isinstance(input_message, Message):
            raise ValueError("prefect workflow input is required")
        result = await execute(input_message)
        if not isinstance(result, Message):
            raise ValueError("prefect workflow returned a non-protobuf result")
        return result

    _runner.__name__ = flow_name
    return prefect_flow(
        name=flow_name,
        retries=options.retries,
        retry_delay_seconds=options.retry_delay_seconds,
    )(_runner)
