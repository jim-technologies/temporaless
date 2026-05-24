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
    TypeMismatchError,
    UnknownMethodError,
)


def _disp(**kw) -> Dispatcher:
    return Dispatcher(**kw)


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
    disp.do_async("/x/Slow", StringValue(value="hi"))

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
        disp.do_async("/x/Missing", StringValue(value="hi"))
    await disp.shutdown()


async def test_do_async_rejects_type_mismatch():
    disp = _disp()

    async def h(_: StringValue) -> StringValue:
        return StringValue()

    disp.register("/x/Strict", StringValue, h)
    with pytest.raises(TypeMismatchError):
        disp.do_async("/x/Strict", Int32Value(value=7))
    await disp.shutdown()


async def test_do_async_rejects_after_shutdown():
    disp = _disp()

    async def h(_: StringValue) -> StringValue:
        return StringValue()

    disp.register("/x/Want", StringValue, h)
    await disp.shutdown()
    with pytest.raises(DispatcherShuttingDownError):
        disp.do_async("/x/Want", StringValue(value="hi"))


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
    disp.do_async("/x/Work", StringValue(value="hi"))

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
    disp.do_async("/x/Long", StringValue(value="hi"))

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
    disp.do_async("/x/Boom", StringValue(value="hi"))
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
    assert DEFAULT_DRAIN_TIMEOUT == timedelta(seconds=15)
