"""Tests for the dispatch fire-and-forget pool.

Mirrors adapters/go/dispatch/dispatch_test.go.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from google.protobuf.wrappers_pb2 import Int32Value, StringValue

from temporaless.dispatch import (
    DEFAULT_DRAIN_TIMEOUT,
    Dispatcher,
    DispatcherShuttingDownError,
    Queue,
    TypeMismatchError,
    UnknownMethodError,
)
from temporaless.v1 import temporaless_pb2


def _opts(
    *, drain_timeout: timedelta | None = None, max_inflight: int = 0
) -> temporaless_pb2.DispatchOptions:
    proto = temporaless_pb2.DispatchOptions(max_inflight=max_inflight)
    if drain_timeout is not None:
        proto.drain_timeout.FromTimedelta(drain_timeout)
    return proto


def _disp(
    *,
    drain_timeout: timedelta | None = None,
    max_inflight: int = 0,
    on_error=None,
    queue: Queue | None = None,
) -> Dispatcher:
    return Dispatcher(
        options=_opts(drain_timeout=drain_timeout, max_inflight=max_inflight),
        on_error=on_error,
        queue=queue,
    )


async def test_do_async_runs_handler_in_background():
    """``do_async`` returns immediately; the body runs on a later tick."""
    disp = _disp()
    started = asyncio.Event()
    can_finish = asyncio.Event()
    done = asyncio.Event()

    async def slow(_: StringValue) -> StringValue:
        started.set()
        await can_finish.wait()
        done.set()
        return StringValue(value="ok")

    disp.register("/x/Slow", StringValue, slow)
    await disp.do_async("/x/Slow", StringValue(value="hi"))

    # do_async returned without awaiting the handler — body hasn't run yet.
    await asyncio.sleep(0)  # one event-loop tick
    assert started.is_set(), "handler should have started after one tick"
    assert not done.is_set(), "handler is still blocked on can_finish"

    can_finish.set()
    await disp.shutdown()
    assert done.is_set()


async def test_do_async_rejects_unknown_method():
    disp = _disp()

    async def h(_: StringValue) -> StringValue:
        return StringValue()

    disp.register("/x/Known", StringValue, h)
    with pytest.raises(UnknownMethodError):
        await disp.do_async("/x/Missing", StringValue(value="hi"))
    await disp.shutdown()


async def test_do_async_rejects_type_mismatch():
    disp = _disp()

    async def h(_: StringValue) -> StringValue:
        return StringValue()

    disp.register("/x/Strict", StringValue, h)
    with pytest.raises(TypeMismatchError):
        await disp.do_async("/x/Strict", Int32Value(value=7))
    await disp.shutdown()


async def test_do_async_rejects_after_shutdown():
    disp = _disp()

    async def h(_: StringValue) -> StringValue:
        return StringValue()

    disp.register("/x/Want", StringValue, h)
    await disp.shutdown()
    with pytest.raises(DispatcherShuttingDownError):
        await disp.do_async("/x/Want", StringValue(value="hi"))


async def test_register_rejects_sync_handler():
    """Sync handlers would silently never run; reject at register time."""
    disp = _disp()

    def sync_handler(_: StringValue) -> StringValue:
        return StringValue()

    with pytest.raises(TypeError):
        disp.register("/x/Sync", StringValue, sync_handler)  # type: ignore[arg-type]
    await disp.shutdown()


async def test_shutdown_drains_running_tasks():
    """Shutdown waits for in-flight handlers to finish their work."""
    disp = _disp(drain_timeout=timedelta(seconds=2))

    completed = asyncio.Event()

    async def work(_: StringValue) -> StringValue:
        await asyncio.sleep(0.1)  # 100ms vendor-call simulation
        completed.set()
        return StringValue(value="done")

    disp.register("/x/Work", StringValue, work)
    await disp.do_async("/x/Work", StringValue(value="hi"))

    await disp.shutdown()
    assert completed.is_set(), "shutdown returned before handler completed"


async def test_shutdown_cancels_after_drain_timeout():
    """A handler running past drain_timeout receives CancelledError."""
    disp = _disp(drain_timeout=timedelta(milliseconds=50))

    bailed_on_cancel = asyncio.Event()
    handler_returned = asyncio.Event()

    async def long(_: StringValue) -> StringValue:
        try:
            await asyncio.sleep(5)  # would block for 5s
            return StringValue(value="never")
        except asyncio.CancelledError:
            bailed_on_cancel.set()
            raise
        finally:
            handler_returned.set()

    disp.register("/x/Long", StringValue, long)
    await disp.do_async("/x/Long", StringValue(value="hi"))

    await disp.shutdown()
    assert handler_returned.is_set(), "shutdown returned but handler hadn't returned"
    assert bailed_on_cancel.is_set(), "handler did not observe CancelledError"


async def test_handler_errors_flow_through_on_error():
    seen: list[tuple[str, BaseException]] = []

    def hook(method: str, err: BaseException) -> None:
        seen.append((method, err))

    disp = _disp(on_error=hook)

    async def boom(_: StringValue) -> StringValue:
        raise RuntimeError("kaboom")

    disp.register("/x/Boom", StringValue, boom)
    await disp.do_async("/x/Boom", StringValue(value="hi"))
    await disp.shutdown()

    assert len(seen) == 1
    method, err = seen[0]
    assert method == "/x/Boom"
    assert isinstance(err, RuntimeError)
    assert str(err) == "kaboom"


async def test_shutdown_is_idempotent():
    disp = _disp()

    async def h(_: StringValue) -> StringValue:
        return StringValue()

    disp.register("/x/Anything", StringValue, h)
    await disp.shutdown()
    await disp.shutdown()  # must not raise


async def test_default_drain_timeout_is_15s():
    """Documented default; covered so a refactor doesn't silently change it."""
    assert timedelta(seconds=15) == DEFAULT_DRAIN_TIMEOUT


async def test_max_inflight_caps_concurrent_handlers():
    """With max_inflight=N, no more than N handlers run at the same time."""
    cap = 3
    disp = _disp(max_inflight=cap)

    inflight = 0
    max_observed = 0
    release = asyncio.Event()
    lock = asyncio.Lock()

    async def bounded(_: StringValue) -> StringValue:
        nonlocal inflight, max_observed
        async with lock:
            inflight += 1
            max_observed = max(max_observed, inflight)
        await release.wait()
        async with lock:
            inflight -= 1
        return StringValue(value="ok")

    disp.register("/x/Bounded", StringValue, bounded)

    # Submit 10; first 3 enter the handler, the rest await semaphore.
    total = 10
    submitters = [
        asyncio.create_task(disp.do_async("/x/Bounded", StringValue(value=f"{i}")))
        for i in range(total)
    ]
    # Let the first batch reach the handler body.
    await asyncio.sleep(0.05)
    assert inflight == cap, f"inflight={inflight}, want {cap} (rest should be blocked)"

    release.set()
    await asyncio.gather(*submitters)
    await disp.shutdown()
    assert max_observed <= cap, f"max concurrent inflight={max_observed}, want <= {cap}"


async def test_max_inflight_unblocks_on_shutdown():
    """A submitter awaiting a permit raises DispatcherShuttingDownError
    when shutdown begins, instead of waiting for a permit that never comes."""
    disp = _disp(max_inflight=1, drain_timeout=timedelta(milliseconds=100))

    hold = asyncio.Event()

    async def hog(_: StringValue) -> StringValue:
        await hold.wait()
        return StringValue(value="ok")

    disp.register("/x/Hog", StringValue, hog)
    await disp.do_async("/x/Hog", StringValue(value="first"))

    # Second submission awaits the permit.
    second = asyncio.create_task(disp.do_async("/x/Hog", StringValue(value="second")))
    await asyncio.sleep(0.05)  # ensure it's blocked

    # Begin shutdown in parallel, then release the holder so drain completes.
    async def shutdown_then_release() -> None:
        await disp.shutdown()

    shutdown_task = asyncio.create_task(shutdown_then_release())
    await asyncio.sleep(0.01)
    hold.set()

    with pytest.raises(DispatcherShuttingDownError):
        await second
    await shutdown_task


async def test_cancelling_parked_submitter_does_not_leak_permit():
    """Cancelling a semaphore waiter must not consume a later handler's slot."""
    disp = _disp(max_inflight=1)
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def handler(req: StringValue) -> StringValue:
        if req.value == "first":
            first_started.set()
            await release_first.wait()
        return req

    disp.register("/x/Work", StringValue, handler)
    await disp.do_async("/x/Work", StringValue(value="first"))
    await first_started.wait()

    parked = asyncio.create_task(disp.do_async("/x/Work", StringValue(value="cancelled")))
    await asyncio.sleep(0)
    parked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await parked

    release_first.set()
    await asyncio.sleep(0)
    await asyncio.wait_for(
        disp.do_async("/x/Work", StringValue(value="after-cancel")),
        timeout=1,
    )
    await disp.shutdown()


async def test_invoke_runs_registered_handler_from_bytes():
    """External-queue consumer path: bytes → method lookup → handler."""
    disp = _disp()
    got = asyncio.Queue()

    async def echo(req: StringValue) -> StringValue:
        await got.put(req.value)
        return StringValue(value="ack:" + req.value)

    disp.register("/x/Echo", StringValue, echo)
    payload = StringValue(value="hello").SerializeToString(deterministic=True)
    await disp.invoke("/x/Echo", payload)
    assert await got.get() == "hello"
    await disp.shutdown()


async def test_invoke_unknown_method():
    disp = _disp()
    with pytest.raises(UnknownMethodError):
        await disp.invoke("/x/Missing", b"")
    await disp.shutdown()


async def test_custom_queue_receives_submission():
    """A user-supplied Queue captures (method, payload) and bypasses the
    in-process task pool entirely — the Kafka/Rabbit adapter contract."""
    captured: list[tuple[str, bytes]] = []

    class CapturingQueue(Queue):
        async def submit(self, method: str, payload: bytes) -> None:
            captured.append((method, payload))

        async def close(self) -> None:
            return None

    disp = _disp(queue=CapturingQueue())

    async def handler(req: StringValue) -> StringValue:  # noqa: ARG001
        pytest.fail("handler should NOT run when a custom queue is configured")

    disp.register("/x/Submit", StringValue, handler)
    await disp.do_async("/x/Submit", StringValue(value="payload"))
    await disp.shutdown()

    assert len(captured) == 1
    method, payload = captured[0]
    assert method == "/x/Submit"
    # Round-trip the payload to prove the producer+consumer share a
    # wire format (proto.SerializeToString deterministic).
    rt = StringValue()
    rt.ParseFromString(payload)
    assert rt.value == "payload"


@pytest.mark.parametrize("close_fails", [False, True])
async def test_concurrent_shutdown_closes_external_queue_exactly_once(
    close_fails: bool,
):
    class ClosingQueue(Queue):
        def __init__(self) -> None:
            self.close_calls = 0

        async def submit(self, method: str, payload: bytes) -> None:
            return None

        async def close(self) -> None:
            self.close_calls += 1
            await asyncio.sleep(0)
            if close_fails:
                raise RuntimeError("flush failed")

    queue = ClosingQueue()
    disp = _disp(queue=queue)
    results = await asyncio.gather(
        disp.shutdown(),
        disp.shutdown(),
        return_exceptions=True,
    )

    assert queue.close_calls == 1
    if close_fails:
        assert all(isinstance(result, RuntimeError) for result in results)
        assert [str(result) for result in results] == ["flush failed", "flush failed"]
        with pytest.raises(RuntimeError, match="flush failed"):
            await disp.shutdown()
    else:
        assert results == [None, None]
        await disp.shutdown()
        assert queue.close_calls == 1


async def test_cancelling_one_shutdown_waiter_does_not_cancel_finalization():
    close_started = asyncio.Event()
    close_release = asyncio.Event()

    class ClosingQueue(Queue):
        close_calls = 0

        async def submit(self, method: str, payload: bytes) -> None:
            return None

        async def close(self) -> None:
            self.close_calls += 1
            close_started.set()
            await close_release.wait()

    queue = ClosingQueue()
    disp = _disp(queue=queue)
    first = asyncio.create_task(disp.shutdown())
    await close_started.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    close_release.set()
    await disp.shutdown()
    assert queue.close_calls == 1


async def test_queue_timeout_error_is_not_misclassified_as_close_timeout():
    """A queue's own TimeoutError must propagate with its original context."""

    class TimeoutQueue(Queue):
        async def submit(self, method: str, payload: bytes) -> None:
            return None

        async def close(self) -> None:
            raise TimeoutError("broker acknowledgement timed out")

    disp = _disp(queue=TimeoutQueue())
    with pytest.raises(TimeoutError, match="broker acknowledgement timed out"):
        await disp.shutdown()


async def test_shutdown_waits_for_external_submission_before_closing_queue():
    submit_started = asyncio.Event()
    release_submit = asyncio.Event()
    submit_finished = False
    close_calls = 0

    class BlockingQueue(Queue):
        async def submit(self, method: str, payload: bytes) -> None:
            nonlocal submit_finished
            submit_started.set()
            await release_submit.wait()
            submit_finished = True

        async def close(self) -> None:
            nonlocal close_calls
            assert submit_finished
            close_calls += 1

    disp = _disp(queue=BlockingQueue())

    async def handler(req: StringValue) -> StringValue:
        return req

    disp.register("/x/Submit", StringValue, handler)
    submit = asyncio.create_task(disp.do_async("/x/Submit", StringValue(value="payload")))
    await submit_started.wait()
    shutdown = asyncio.create_task(disp.shutdown())
    await asyncio.sleep(0)
    assert close_calls == 0

    release_submit.set()
    await submit
    await shutdown
    assert close_calls == 1


async def test_queue_close_releases_blocked_external_submission():
    submit_started = asyncio.Event()
    release_submit = asyncio.Event()
    close_calls = 0

    class BlockingQueue(Queue):
        async def submit(self, method: str, payload: bytes) -> None:
            submit_started.set()
            await release_submit.wait()

        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1
            release_submit.set()

    disp = _disp(
        queue=BlockingQueue(),
        drain_timeout=timedelta(milliseconds=10),
    )

    async def handler(req: StringValue) -> StringValue:
        return req

    disp.register("/x/Submit", StringValue, handler)
    submit = asyncio.create_task(disp.do_async("/x/Submit", StringValue(value="payload")))
    await submit_started.wait()

    await disp.shutdown()
    await submit
    assert close_calls == 1


async def test_shutdown_reports_submission_still_blocked_after_close():
    submit_started = asyncio.Event()
    release_submit = asyncio.Event()
    close_calls = 0

    class StubbornQueue(Queue):
        async def submit(self, method: str, payload: bytes) -> None:
            submit_started.set()
            await release_submit.wait()

        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1

    disp = _disp(
        queue=StubbornQueue(),
        drain_timeout=timedelta(milliseconds=10),
    )

    async def handler(req: StringValue) -> StringValue:
        return req

    disp.register("/x/Submit", StringValue, handler)
    submit = asyncio.create_task(disp.do_async("/x/Submit", StringValue(value="payload")))
    await submit_started.wait()

    with pytest.raises(
        TimeoutError,
        match="submissions remained active after queue close",
    ):
        await disp.shutdown()
    assert close_calls == 1

    release_submit.set()
    await submit


async def test_queue_close_failure_does_not_skip_handler_drain():
    close_failure = RuntimeError("flush failed")

    class FailingInProcessQueue(Queue):
        dispatcher: Dispatcher
        close_calls = 0

        async def submit(self, method: str, payload: bytes) -> None:
            await self.dispatcher._submit_in_process(method, payload)  # noqa: SLF001

        async def close(self) -> None:
            self.close_calls += 1
            raise close_failure

    queue = FailingInProcessQueue()
    disp = _disp(queue=queue, drain_timeout=timedelta(seconds=1))
    queue.dispatcher = disp
    started = asyncio.Event()
    release = asyncio.Event()
    completed = asyncio.Event()

    async def handler(req: StringValue) -> StringValue:
        started.set()
        await release.wait()
        completed.set()
        return req

    disp.register("/x/Work", StringValue, handler)
    await disp.do_async("/x/Work", StringValue(value="payload"))
    await started.wait()
    asyncio.get_running_loop().call_later(0.02, release.set)

    with pytest.raises(RuntimeError, match="flush failed"):
        await disp.shutdown()
    assert completed.is_set()
    assert queue.close_calls == 1
    with pytest.raises(RuntimeError, match="flush failed"):
        await disp.shutdown()
    assert queue.close_calls == 1


async def test_options_default_when_none():
    """Constructing Dispatcher with no options applies defaults."""
    disp = Dispatcher()  # no options, no queue, no on_error
    assert disp._drain_timeout == DEFAULT_DRAIN_TIMEOUT  # noqa: SLF001
    assert disp._sem is None  # noqa: SLF001 - unbounded by default
    await disp.shutdown()


async def test_options_from_proto_round_trip():
    """The proto-declared options are picked up unchanged at construction."""
    proto = temporaless_pb2.DispatchOptions(max_inflight=42)
    proto.drain_timeout.FromTimedelta(timedelta(seconds=7))
    disp = Dispatcher(options=proto)
    assert disp._drain_timeout == timedelta(seconds=7)  # noqa: SLF001
    assert disp._sem._value == 42  # noqa: SLF001 - asyncio.Semaphore internal
    await disp.shutdown()


async def test_unbounded_by_default():
    """max_inflight=0 (the default) means no cap."""
    disp = _disp()  # max_inflight unset

    inflight = 0
    release = asyncio.Event()
    lock = asyncio.Lock()

    async def burst(_: StringValue) -> StringValue:
        nonlocal inflight
        async with lock:
            inflight += 1
        await release.wait()
        return StringValue(value="ok")

    disp.register("/x/Burst", StringValue, burst)
    total = 50
    await asyncio.gather(
        *(disp.do_async("/x/Burst", StringValue(value=f"{i}")) for i in range(total))
    )
    # Yield to let all tasks reach the handler body.
    for _ in range(10):
        if inflight == total:
            break
        await asyncio.sleep(0.01)
    assert inflight == total, f"inflight={inflight}, want {total} (unbounded should run all)"
    release.set()
    await disp.shutdown()
