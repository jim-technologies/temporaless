from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import wraps

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from google.protobuf.message import Message
from temporaless.storage import Store
from temporaless.workflow import (
    ActivityConflictError,
    ActivityError,
    ClaimBusyError,
    ClaimCapabilityError,
    ClaimReleaseError,
    ConcurrencyBusyError,
    EventPendingError,
    Options,
    TimerConflictError,
    TimerPendingError,
    Workflow,
    WorkflowConflictError,
    WorkflowDependencyFailedError,
    WorkflowDependencyPendingError,
    WorkflowInfrastructureError,
    run,
)


@dataclass(frozen=True, slots=True)
class WorkflowMethodWrapOptions[RequestT: Message, ResultT: Message]:
    """Configuration for one ConnectRPC unary workflow method boundary."""

    store: Callable[[object], Store]
    result_type: type[ResultT]
    options_for: Callable[[object, RequestT], Options]


def error_to_connect_code(exc: BaseException) -> tuple[Code, str] | None:
    """Return the stable ConnectRPC code and message for a framework error."""
    if isinstance(exc, ClaimReleaseError):
        return (Code.INTERNAL, str(exc))
    if isinstance(
        exc,
        (
            TimerPendingError,
            EventPendingError,
            WorkflowDependencyPendingError,
            WorkflowInfrastructureError,
        ),
    ):
        return (Code.UNAVAILABLE, str(exc))
    if isinstance(exc, ClaimBusyError):
        return (Code.ALREADY_EXISTS, str(exc))
    if isinstance(exc, ConcurrencyBusyError):
        return (Code.RESOURCE_EXHAUSTED, str(exc))
    if isinstance(exc, ClaimCapabilityError):
        return (Code.FAILED_PRECONDITION, str(exc))
    if isinstance(exc, (WorkflowConflictError, ActivityConflictError, TimerConflictError)):
        return (Code.FAILED_PRECONDITION, str(exc))
    if isinstance(exc, (ActivityError, WorkflowDependencyFailedError)):
        return (Code.INTERNAL, str(exc))
    return None


def is_pending_error(exc: BaseException) -> bool:
    """Classify retryable ConnectRPC status errors for ``backfill``.

    Pass this explicitly as ``backfill(..., pending_error=is_pending_error)``
    when ``invoke`` calls a remote ConnectRPC service. Local adapter calls keep
    their typed ``__cause__`` and are classified by core without this hook.
    """
    pending_codes = {
        Code.UNAVAILABLE,
        Code.ALREADY_EXISTS,
        Code.RESOURCE_EXHAUSTED,
    }
    stack = [exc]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(current, ConnectError) and current.code in pending_codes:
            return True
        if current.__cause__ is not None:
            stack.append(current.__cause__)
        if current.__context__ is not None:
            stack.append(current.__context__)
    return False


def wrap_workflow_method[RequestT: Message, ResultT: Message](
    options: WorkflowMethodWrapOptions[RequestT, ResultT],
) -> Callable[
    [Callable[..., Awaitable[ResultT]]],
    Callable[..., Awaitable[ResultT]],
]:
    """Decorate an async unary ConnectRPC method as a Temporaless workflow."""
    if not isinstance(options, WorkflowMethodWrapOptions):
        raise TypeError("options must be WorkflowMethodWrapOptions")
    if not callable(options.store):
        raise TypeError("options.store must be callable")
    if not isinstance(options.result_type, type) or not issubclass(options.result_type, Message):
        raise TypeError("options.result_type must be a protobuf message class")
    if not callable(options.options_for):
        raise TypeError("options.options_for must be callable")

    def decorator(
        method: Callable[..., Awaitable[ResultT]],
    ) -> Callable[..., Awaitable[ResultT]]:
        if not inspect.iscoroutinefunction(method):
            raise ValueError("workflow method must be async (define it with `async def`)")

        @wraps(method)
        async def wrapped(self_: object, request: RequestT, ctx: object = None) -> ResultT:
            store = options.store(self_)
            workflow_options = options.options_for(self_, request)

            async def body(_workflow: Workflow, req: RequestT) -> ResultT:
                return await method(self_, req, ctx)

            try:
                return await run(store, workflow_options, request, options.result_type, body)
            except Exception as exc:
                mapping = error_to_connect_code(exc)
                if mapping is None:
                    raise
                code, message = mapping
                raise ConnectError(code, message) from exc

        return wrapped

    return decorator
