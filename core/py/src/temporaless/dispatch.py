"""Fire-and-forget dispatcher for gRPC-shaped handlers.

Complements :func:`temporaless.workflow.run` (synchronous + durable) with
an asynchronous path for side effects whose result the caller doesn't
need to wait on -- webhook notifications, telemetry pushes, best-effort
vendor pings, fan-out where the caller wants its own request to return
quickly.

Two backends, selected via :class:`Queue`:

- Default in-process: each :meth:`Dispatcher.do_async` spawns an
  :class:`asyncio.Task`, with managed graceful shutdown and an optional
  ``max_inflight`` cap. Not durable across crashes -- if the process
  dies before the handler finishes, the work is lost.
- External queue (Kafka, RabbitMQ, NATS, SQS, Redis Streams, ...):
  implement :class:`Queue` once; the dispatcher proto-marshals each
  request deterministically and hands ``(method, payload bytes)`` to
  the queue. Consumers pull bytes off the bus and call
  :meth:`Dispatcher.invoke` to run the registered handler. Durability
  comes from the queue's native ack / nack semantics.

The surface mirrors ``adapters/go/dispatch`` and
``temporaless::dispatch`` (Rust): same ``register`` /
``do_async`` / ``invoke`` / ``shutdown`` shape, same 15-second default
drain, same "always wait for every spawned task" guarantee.

Use :func:`temporaless.workflow.run` when you need at-least-once
delivery across crashes; this module is for at-most-once + best-effort.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TypeVar, cast

from google.protobuf.message import Message

from temporaless.v1 import temporaless_pb2

RequestT = TypeVar("RequestT", bound=Message)
ResponseT = TypeVar("ResponseT", bound=Message)

DEFAULT_DRAIN_TIMEOUT = timedelta(seconds=15)
"""Per-phase bound for draining admitted producer sends and in-flight tasks.
Chosen to fit common SIGTERM grace periods."""

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


class Queue(ABC):
    """Producer-side adapter point for external message buses.

    A :class:`Queue` receives a method name + the proto-marshaled request
    payload; what it does with them is up to the implementation: write to
    Kafka, publish to RabbitMQ / NATS, SQS SendMessage, Redis Streams
    XADD, etc.

    The consumer side is the implementation's concern. Consumers pull
    messages off their queue and feed ``(method, payload)`` into
    :meth:`Dispatcher.invoke` to look up the registered handler and run
    it; the queue's native ack / nack drives delivery semantics.

    In-process (the default): the handler runs on an :class:`asyncio.Task`
    spawned by the dispatcher, with :class:`Dispatcher`'s ``max_inflight``
    / ``drain_timeout`` applied. See :class:`Dispatcher` for swap-in.
    """

    @abstractmethod
    async def submit(self, method: str, payload: bytes) -> None:
        """Push (method, payload) onto the backing queue."""

    @abstractmethod
    async def close(self) -> None:
        """Release any resources held by the queue.

        :meth:`Dispatcher.shutdown` calls this exactly once after a bounded
        wait for admitted :meth:`submit` calls. If a submit is still blocked,
        ``close`` must safely release or fail it. External queues should flush
        pending sends and close producer connections; the in-process queue is
        a no-op because the dispatcher drains its tasks separately.
        """


class Dispatcher:
    """Fire-and-forget dispatcher.

    Usage::

        from google.protobuf.duration_pb2 import Duration
        from temporaless.v1 import temporaless_pb2

        opts = temporaless_pb2.DispatchOptions(max_inflight=100)
        opts.drain_timeout.FromTimedelta(timedelta(seconds=15))
        disp = Dispatcher(options=opts)

        disp.register(
            "/payments.Charges/Charge",
            ChargeRequest,
            server.charge,
        )

        # In-process default: schedules an asyncio.Task and returns once
        # any concurrency permit has been acquired.
        await disp.do_async("/payments.Charges/Charge", ChargeRequest(amount=100))

        # SIGTERM handler:
        await disp.shutdown()
    """

    def __init__(
        self,
        *,
        options: temporaless_pb2.DispatchOptions | None = None,
        queue: Queue | None = None,
        on_error: Callable[[str, BaseException], None] | None = None,
    ) -> None:
        """Create a Dispatcher.

        ``options`` carries the serializable config (``drain_timeout``,
        ``max_inflight``) as a :class:`~temporaless.v1.temporaless_pb2.DispatchOptions`
        proto so a single config file / env var / CLI flag drives the
        same knobs across Go, Python, and Rust. When ``None``, defaults
        apply (15s drain, unbounded concurrency).

        ``queue`` plugs in an external message bus (Kafka, RabbitMQ,
        NATS, SQS, ...). Default: in-process asyncio-task pool. When a
        custom queue is supplied, ``do_async`` submits to it instead of
        spawning locally; the consumer side calls :meth:`invoke` to run
        the registered handler.

        ``on_error`` receives any handler exception that the in-process
        queue surfaces. External queues report their own failures via
        the queue's native ack / nack semantics; ``on_error`` is unused
        in that path.
        """
        proto = options or temporaless_pb2.DispatchOptions()
        drain_timeout = proto.drain_timeout.ToTimedelta()
        if drain_timeout.total_seconds() <= 0:
            drain_timeout = DEFAULT_DRAIN_TIMEOUT
        max_inflight = int(proto.max_inflight)
        self._drain_timeout = drain_timeout
        self._on_error = on_error or _default_on_error
        self._handlers: dict[str, _HandlerEntry] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._closed = False
        self._shutdown_event = asyncio.Event()
        self._submissions_drained = asyncio.Event()
        self._submissions_drained.set()
        self._active_submissions = 0
        self._shutdown_task: asyncio.Task[None] | None = None
        # When max_inflight > 0, the in-process queue awaits this semaphore
        # before scheduling. When 0, the semaphore is None and the queue
        # never blocks on the slot dimension.
        self._sem: asyncio.Semaphore | None = (
            asyncio.Semaphore(max_inflight) if max_inflight > 0 else None
        )
        # Default queue is in-process; bound after self is otherwise
        # initialised so InProcessQueue can capture a reference to us.
        self._queue: Queue = queue if queue is not None else _InProcessQueue(self)

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
        if not inspect.iscoroutinefunction(handle):
            raise TypeError(
                "dispatch.register: handle must be a coroutine function "
                "(define it with `async def`)"
            )

        async def _invoke(req: Message) -> None:
            # `request_type` is captured; the type-check at do_async time
            # already validated, so the cast is safe.
            await handle(cast("RequestT", req))

        self._handlers[method] = _HandlerEntry(request_type=request_type, invoke=_invoke)

    async def do_async(self, method: str, req: Message) -> None:
        """Route ``method``'s handler through the configured :class:`Queue`.

        The producer-side type check runs synchronously; mismatched types
        raise :class:`TypeMismatchError` BEFORE the bytes are marshaled
        or queued -- catching a typo before it durably hits an external
        bus and gets dead-lettered later.

        For the in-process queue: schedules an :class:`asyncio.Task`,
        awaiting a permit when ``max_inflight`` is set (raises
        :class:`DispatcherShuttingDownError` if shutdown wins the race).

        For an external queue: serializes ``req`` with deterministic
        protobuf encoding and hands ``(method, payload)`` to
        :meth:`Queue.submit`. The queue's send-side errors propagate
        unchanged.

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

        payload = req.SerializeToString(deterministic=True)
        # No await occurs between the closed check above and this increment,
        # so shutdown can atomically stop new submissions and wait for every
        # producer acknowledgement already in progress before closing the
        # external queue.
        self._active_submissions += 1
        self._submissions_drained.clear()
        try:
            await self._queue.submit(method, payload)
        finally:
            self._active_submissions -= 1
            if self._active_submissions == 0:
                self._submissions_drained.set()

    async def _submit_in_process(self, method: str, payload: bytes) -> None:
        """In-process submission path used by the default :class:`Queue`.

        Lives on :class:`Dispatcher` so the queue stays a thin shim with
        no private-state reach-arounds. Awaits a concurrency permit when
        ``max_inflight`` is set, racing the acquire against the shutdown
        signal so a SIGTERM-during-burst wakes parked submitters with
        :class:`DispatcherShuttingDownError`.
        """
        if self._closed:
            raise DispatcherShuttingDownError(
                "dispatcher is shutting down; new submissions are rejected"
            )
        if self._sem is not None:
            acquire = asyncio.create_task(self._sem.acquire())
            shutdown_wait = asyncio.create_task(self._shutdown_event.wait())
            permit_transferred = False
            try:
                done, _ = await asyncio.wait(
                    {acquire, shutdown_wait},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if shutdown_wait in done:
                    raise DispatcherShuttingDownError(
                        "dispatcher is shutting down; new submissions are rejected"
                    )
                acquire.result()
                # No await may occur between acquiring the permit and handing
                # it to a handler. That keeps caller cancellation from
                # stranding a slot during the ownership transfer.
                self._spawn_local(method, payload)
                permit_transferred = True
            finally:
                # A cancelled do_async must not strand either child task. In
                # particular, an orphan semaphore acquire could later consume
                # a permit without ever transferring it to a handler.
                for task in (acquire, shutdown_wait):
                    if not task.done():
                        task.cancel()
                if not permit_transferred and acquire.done() and not acquire.cancelled():
                    try:
                        acquire.result()
                    except BaseException:  # noqa: BLE001 - cleanup only
                        pass
                    else:
                        self._sem.release()
                await asyncio.gather(acquire, shutdown_wait, return_exceptions=True)
            return

        self._spawn_local(method, payload)

    async def invoke(self, method: str, payload: bytes) -> None:
        """Decode ``payload`` as the request type registered for ``method``
        and run the registered handler on the caller's task.

        Intended for queue-backed consumers: pull a message off Kafka /
        Rabbit / NATS / SQS, hand its method-name + payload here, use
        the raised exception (or its absence) to drive ack / nack.

        Unlike :meth:`do_async`, ``invoke`` runs the handler
        synchronously on the caller's task and respects the caller's
        own asyncio cancellation. The producer-side concurrency cap
        and drain semantics don't apply here -- bound your consumer's
        concurrency at the queue's prefetch / consumer-pool layer
        instead.
        """
        entry = self._handlers.get(method)
        if entry is None:
            raise UnknownMethodError(f"no handler registered for method {method!r}")
        req = entry.request_type()
        req.ParseFromString(payload)
        await entry.invoke(req)

    # Internal hook called by :class:`_InProcessQueue.submit` after it has
    # acquired any concurrency permit. Spawns the handler as an asyncio
    # task and returns; the caller (the in-process queue) returns once
    # the task is scheduled.
    def _spawn_local(self, method: str, payload: bytes) -> None:
        entry = self._handlers[method]
        req = entry.request_type()
        req.ParseFromString(payload)
        loop = asyncio.get_running_loop()
        task = loop.create_task(self._run(method, entry, req), name=f"dispatch:{method}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run(self, method: str, entry: _HandlerEntry, req: Message) -> None:
        try:
            try:
                await entry.invoke(req)
            except asyncio.CancelledError:
                # Re-raise so the task reports as cancelled, but ALSO surface
                # via on_error so operators see the abandoned work in logs.
                self._on_error(method, asyncio.CancelledError())
                raise
            except BaseException as err:  # noqa: BLE001 - surfacing user errors
                self._on_error(method, err)
        finally:
            # Release the slot regardless of how the handler exited
            # (success / error / cancellation). Mirrors the deferred
            # release in the Go SDK.
            if self._sem is not None:
                self._sem.release()

    async def shutdown(self) -> None:
        """Stop accepting new submissions; drain in-flight tasks.

        Stop admission, give producer submissions one bounded window to
        finish, then close the queue exactly once so an external producer can
        flush and release blocked sends. In-process tasks get their own
        bounded drain window before cancellation. Queue-close failures are
        re-raised only after handler draining finishes; a producer submission
        still blocked after close raises :class:`TimeoutError`.

        Repeated and concurrent calls await the same finalization task and
        receive the same queue-close exception, if any. Cancelling one caller
        does not cancel finalization.
        """
        # Close admission before the first await. Creating the shared
        # finalization task must not leave an event-loop turn where a new
        # submission can slip in.
        self._closed = True
        self._shutdown_event.set()
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(
                self._shutdown(),
                name="temporaless-dispatch-shutdown",
            )
        await asyncio.shield(self._shutdown_task)

    async def _shutdown(self) -> None:
        timeout = self._drain_timeout.total_seconds()

        submissions_drained = True
        try:
            await asyncio.wait_for(self._submissions_drained.wait(), timeout=timeout)
        except TimeoutError:
            submissions_drained = False

        close_error: BaseException | None = None
        close_task = asyncio.create_task(
            self._queue.close(),
            name="temporaless-dispatch-queue-close",
        )
        close_done, _ = await asyncio.wait({close_task}, timeout=timeout)
        if close_task not in close_done:
            close_task.cancel()
            await asyncio.gather(close_task, return_exceptions=True)
            close_error = TimeoutError("dispatch queue close exceeded drain_timeout")
        else:
            try:
                close_task.result()
            except BaseException as err:  # noqa: BLE001 - re-raise after drain
                close_error = err

        submission_error: TimeoutError | None = None
        if not submissions_drained:
            # Queue.close is the producer-side cancellation/flush boundary and
            # may be what releases a submit awaiting network acknowledgement.
            try:
                await asyncio.wait_for(
                    self._submissions_drained.wait(),
                    timeout=timeout,
                )
            except TimeoutError:
                submission_error = TimeoutError(
                    "dispatch submissions remained active after queue close"
                )

        if self._tasks:
            # Snapshot the current set so callbacks discarding from it during
            # iteration don't surprise us.
            inflight = list(self._tasks)
            try:
                await asyncio.wait_for(
                    asyncio.shield(asyncio.gather(*inflight, return_exceptions=True)),
                    timeout=timeout,
                )
            except TimeoutError:
                # Drain window expired -- signal cancellation so cooperative
                # handlers can bail, then wait for them to actually return.
                for task in inflight:
                    if not task.done():
                        task.cancel()
                # Unbounded wait: never abandon a task. asyncio.gather with
                # return_exceptions=True swallows CancelledError per task.
                await asyncio.gather(*inflight, return_exceptions=True)

        if submission_error is not None:
            if close_error is not None:
                raise submission_error from close_error
            raise submission_error
        if close_error is not None:
            raise close_error


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


class _InProcessQueue(Queue):
    """Default :class:`Queue` -- thin shim that delegates to
    :meth:`Dispatcher._submit_in_process`. All shared state lives on the
    dispatcher; this exists only so the in-process path satisfies the
    same :class:`Queue` interface external adapters implement.
    """

    def __init__(self, dispatcher: Dispatcher) -> None:
        self._dispatcher = dispatcher

    async def submit(self, method: str, payload: bytes) -> None:
        await self._dispatcher._submit_in_process(method, payload)  # noqa: SLF001

    async def close(self) -> None:
        # Dispatcher.shutdown owns the in-process task drain.
        return None
