"""Bounded fire-and-forget asyncio task pool for gRPC-shaped handlers.

Complements :func:`temporaless.workflow.run` (synchronous + durable) with
an asynchronous, in-process path for side effects whose result the caller
doesn't need to wait on -- webhook notifications, telemetry pushes,
best-effort vendor pings, fan-out where the caller wants its own request
to return quickly.

Mirrors ``adapters/go/dispatch`` exactly:

- :class:`Dispatcher` -- the pool. Construct once; ``register``,
  ``do_async``, ``shutdown``.
- :meth:`Dispatcher.register` -- wire a handler under its gRPC
  fully-qualified method name (``"/package.Service/Method"``) so the
  same identity used at the wire layer routes here too.
- :meth:`Dispatcher.do_async` -- look up the handler and schedule it as
  an :class:`asyncio.Task`. Returns immediately.
- :meth:`Dispatcher.shutdown` -- stop accepting new submissions, wait up
  to ``drain_timeout`` (default 15s) for in-flight tasks to finish, then
  cancel the per-handler scope. Always awaits every task -- orphaning a
  handler mid-vendor-call is worse than waiting a few extra seconds for
  it to notice cancellation.

# Scope (intentional)

In-process only. A handler invocation lives inside an asyncio task of
the event loop that called ``do_async``. If that process dies before the
handler finishes, the work is lost. This is the deliberate tradeoff vs.
:func:`temporaless.workflow.run` -- when you need durability across
crashes, write a workflow instead; this module is for things where
at-most-once + best-effort is the right semantics.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TypeVar

from google.protobuf.message import Message

RequestT = TypeVar("RequestT", bound=Message)
ResponseT = TypeVar("ResponseT", bound=Message)

DEFAULT_DRAIN_TIMEOUT = timedelta(seconds=15)
"""How long :meth:`Dispatcher.shutdown` waits for in-flight tasks to finish
before cancelling them. Chosen to match common SIGTERM grace periods
(Kubernetes preStop / terminationGracePeriodSeconds)."""

_log = logging.getLogger("temporaless.dispatch")


class DispatcherShuttingDownError(RuntimeError):
    """:meth:`Dispatcher.do_async` raised this after ``shutdown`` began.

    Treat as a final "do not retry" signal -- the process is going away.
    """


class UnknownMethodError(KeyError):
    """No handler was registered for the requested method."""


class TypeMismatchError(TypeError):
    """The supplied request value is not the type the registered handler expects."""


class _HandlerEntry:
    """Type-erased handler + the request class it expects."""

    __slots__ = ("request_type", "invoke")

    def __init__(
        self,
        request_type: type[Message],
        invoke: Callable[[Message], Awaitable[None]],
    ) -> None:
        self.request_type = request_type
        self.invoke = invoke


class Dispatcher:
    """Bounded fire-and-forget asyncio task pool.

    Usage::

        disp = Dispatcher(drain_timeout=timedelta(seconds=15))
        disp.register(
            "/payments.Charges/Charge",
            ChargeRequest,
            server.charge,
        )
        disp.do_async("/payments.Charges/Charge", ChargeRequest(amount=100))
        # ... process keeps running, handler executes in the background ...
        await disp.shutdown()  # SIGTERM handler
    """

    def __init__(
        self,
        *,
        drain_timeout: timedelta = DEFAULT_DRAIN_TIMEOUT,
        on_error: Callable[[str, BaseException], None] | None = None,
    ) -> None:
        if drain_timeout.total_seconds() <= 0:
            drain_timeout = DEFAULT_DRAIN_TIMEOUT
        self._drain_timeout = drain_timeout
        self._on_error = on_error or _default_on_error
        self._handlers: dict[str, _HandlerEntry] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._closed = False

    def register(
        self,
        method: str,
        request_type: type[RequestT],
        handle: Callable[[RequestT], Awaitable[ResponseT]],
    ) -> None:
        """Register an async handler under ``method``.

        ``method`` should be the gRPC fully-qualified method
        (``"/package.Service/Method"``). ``request_type`` is the protobuf
        message class the handler expects; ``do_async`` rejects mismatched
        types at the call site rather than letting them fail inside the
        task. ``handle`` MUST be a coroutine function (``async def``).

        Re-registering the same method overwrites silently -- last writer
        wins.
        """
        if not method:
            raise ValueError("dispatch.register: method is required")
        if request_type is None:
            raise ValueError("dispatch.register: request_type is required")
        if handle is None:
            raise ValueError("dispatch.register: handle is required")
        if not asyncio.iscoroutinefunction(handle):
            raise TypeError(
                "dispatch.register: handle must be a coroutine function "
                "(define it with `async def`)"
            )

        async def _invoke(req: Message) -> None:
            # `request_type` is captured; the type-check at do_async time
            # already validated, so the cast is safe.
            await handle(req)  # type: ignore[arg-type]

        self._handlers[method] = _HandlerEntry(request_type=request_type, invoke=_invoke)

    def do_async(self, method: str, req: Message) -> None:
        """Schedule ``method``'s handler with ``req`` as an asyncio task.

        Returns immediately after the task is scheduled (the body runs on
        the next event-loop tick). Raises before scheduling if the method
        is unregistered, the request type doesn't match the registered
        handler, or the dispatcher is shutting down -- handler errors flow
        through ``on_error``.

        Must be called from within a running event loop.
        """
        if self._closed:
            raise DispatcherShuttingDownError(
                "dispatcher is shutting down; new submissions are rejected"
            )
        entry = self._handlers.get(method)
        if entry is None:
            raise UnknownMethodError(f"no handler registered for method {method!r}")
        if req is None:
            raise ValueError(f"dispatch.do_async: req is required for method {method!r}")
        if not isinstance(req, entry.request_type):
            raise TypeMismatchError(
                f"handler {method!r} expects {entry.request_type.__name__}, "
                f"got {type(req).__name__}"
            )

        loop = asyncio.get_running_loop()
        task = loop.create_task(self._run(method, entry, req), name=f"dispatch:{method}")
        self._tasks.add(task)
        # Drop the strong reference once the task finishes so the set
        # doesn't pin completed tasks forever. The reference is held only
        # for the lifetime of the running task; shutdown's snapshot picks
        # up everything that's still in-flight.
        task.add_done_callback(self._tasks.discard)

    async def _run(self, method: str, entry: _HandlerEntry, req: Message) -> None:
        try:
            await entry.invoke(req)
        except asyncio.CancelledError:
            # Re-raise so the task reports as cancelled, but ALSO surface
            # via on_error so operators see the abandoned work in logs.
            self._on_error(method, asyncio.CancelledError())
            raise
        except BaseException as err:  # noqa: BLE001 - surfacing user errors
            self._on_error(method, err)

    async def shutdown(self) -> None:
        """Stop accepting new submissions; drain in-flight tasks.

        Phase 1: wait up to ``drain_timeout`` for tasks to finish on their
        own. Phase 2: if any are still running, ``cancel()`` them and
        await their completion (a well-behaved handler observes
        :class:`asyncio.CancelledError` and bails). Always awaits every
        task -- orphaning a handler mid-vendor-call is the failure mode
        we're avoiding.

        Safe to call twice; the second call observes the already-closed
        state and returns once any remaining tasks finish.
        """
        self._closed = True
        if not self._tasks:
            return

        # Snapshot the current set so callbacks discarding from it during
        # iteration don't surprise us.
        inflight = list(self._tasks)
        try:
            await asyncio.wait_for(
                asyncio.shield(asyncio.gather(*inflight, return_exceptions=True)),
                timeout=self._drain_timeout.total_seconds(),
            )
            return
        except asyncio.TimeoutError:
            pass

        # Drain window expired -- signal cancellation so cooperative
        # handlers can bail, then wait for them to actually return.
        for task in inflight:
            if not task.done():
                task.cancel()
        # Unbounded wait: never abandon a goroutine. asyncio.gather with
        # return_exceptions=True swallows CancelledError per task.
        await asyncio.gather(*inflight, return_exceptions=True)


def _default_on_error(method: str, err: BaseException) -> None:
    """Default :class:`Dispatcher` ``on_error`` -- log at WARN."""
    if isinstance(err, asyncio.CancelledError):
        _log.warning("dispatch: handler %r cancelled during shutdown drain", method)
        return
    _log.warning(
        "dispatch: handler %r returned error: %s",
        method,
        err,
        exc_info=err,
    )
