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
- Prefect's flow/task instrumentation: run id, logger, tags, retries
- forwards arbitrary ``flow_kwargs`` / ``task_kwargs`` to the underlying
  ``prefect.flow`` / ``prefect.task`` decorator so users keep full Prefect
  feature access (retries, persist_result, log_prints, etc.) without us
  shadowing them.

The handler's *protobuf* contract is preserved: input must be a
``Message``, output must be a ``Message``. Type drift fails loudly.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from google.protobuf.message import Message
from prefect import flow as prefect_flow
from prefect import task as prefect_task

Req = TypeVar("Req", bound=Message)
Resp = TypeVar("Resp", bound=Message)

ActivityFunc = Callable[[Req], Awaitable[Resp]]
WorkflowFunc = Callable[[Req], Awaitable[Resp]]


def wrap_activity(
    execute: ActivityFunc[Req, Resp],
    *,
    name: str | None = None,
    **task_kwargs: Any,
) -> Callable[[Req], Awaitable[Resp]]:
    """Wrap a Temporaless-shaped async activity as a Prefect task.

    The wrapped callable is ``await``-able just like the original. Calling
    it from inside a Prefect flow registers a Prefect task run; calling it
    standalone runs it directly. Either way, the protobuf-shape contract is
    enforced.

    Args:
        execute: ``async def(req: ProtoMessage) -> ProtoMessage``.
        name: Prefect task name. Defaults to ``execute.__name__``.
        **task_kwargs: forwarded to ``prefect.task`` (``retries``,
            ``cache_policy``, ``persist_result``, ``tags``, etc.).
    """
    if execute is None:
        raise ValueError("prefect activity executor is required")
    if not inspect.iscoroutinefunction(execute):
        raise ValueError("prefect activity executor must be async (define it with `async def`)")
    task_name = name or getattr(execute, "__name__", "")
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
    return prefect_task(name=task_name, **task_kwargs)(_runner)


def wrap_workflow(
    execute: WorkflowFunc[Req, Resp],
    *,
    name: str | None = None,
    **flow_kwargs: Any,
) -> Callable[[Req], Awaitable[Resp]]:
    """Wrap a Temporaless-shaped async workflow as a Prefect flow.

    The wrapped callable is ``await``-able just like the original. Calling
    it triggers a Prefect flow run (visible in the Prefect UI / API);
    internally the body runs as written, including any
    ``current_workflow().execute_activity`` / ``sleep`` / ``wait_event``
    calls against your Temporaless ``Store``.

    Args:
        execute: ``async def(req: ProtoMessage) -> ProtoMessage``.
        name: Prefect flow name. Defaults to ``execute.__name__``.
        **flow_kwargs: forwarded to ``prefect.flow`` (``retries``,
            ``timeout_seconds``, ``log_prints``, ``persist_result``, etc.).
    """
    if execute is None:
        raise ValueError("prefect workflow executor is required")
    if not inspect.iscoroutinefunction(execute):
        raise ValueError("prefect workflow executor must be async (define it with `async def`)")
    flow_name = name or getattr(execute, "__name__", "")
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
    return prefect_flow(name=flow_name, **flow_kwargs)(_runner)
