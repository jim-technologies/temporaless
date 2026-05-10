"""Tests for ``temporaless.backfill``."""

from __future__ import annotations

import asyncio

import opendal
import pytest
from google.protobuf.wrappers_pb2 import StringValue

from temporaless import Options, wrap_workflow_method
from temporaless.backfill import BackfillStatus, backfill
from temporaless.storage import OpenDALStore


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


def _service_invoker(service: object):
    """Return an `invoke(run_id)` for a wrap_workflow_method-decorated service.

    The convention: the service's wrapped method is keyed on a single string
    request whose value IS the run_id. The example services in this test
    file follow that convention.
    """

    async def invoke(run_id: str) -> StringValue:
        return await service.fetch_prices(StringValue(value=run_id))  # type: ignore[attr-defined]

    return invoke


class _FetchService:
    """Canonical wrap_workflow_method service used as the backfill target."""

    def __init__(self, store: OpenDALStore) -> None:
        self._store = store
        self.invocations: dict[str, int] = {}

    @wrap_workflow_method(
        store=lambda self: self._store,  # type: ignore[attr-defined]
        result_type=StringValue,
        options_for=lambda _self, request: Options(
            workflow_id="prices",
            run_id=request.value,
            code_version="v1",
        ),
    )
    async def fetch_prices(self, request: StringValue, _ctx: object = None) -> StringValue:
        self.invocations[request.value] = self.invocations.get(request.value, 0) + 1
        return StringValue(value=f"price:{request.value}")


async def test_backfill_runs_all_run_ids_serially(store: OpenDALStore) -> None:
    service = _FetchService(store)
    run_ids = ["2026-05-01", "2026-05-02", "2026-05-03"]

    report = await backfill(_service_invoker(service), run_ids, concurrency=1)

    assert len(report.entries) == 3
    assert len(report.succeeded()) == 3
    assert len(report.failed()) == 0
    assert len(report.pending()) == 0
    for entry in report.entries:
        assert entry.status == BackfillStatus.SUCCEEDED
        assert entry.result is not None
        assert entry.result.value == f"price:{entry.run_id}"


async def test_backfill_replays_already_completed_runs(store: OpenDALStore) -> None:
    """Re-running backfill is free for already-COMPLETED runs: replay
    short-circuits without re-invoking the body."""
    service = _FetchService(store)
    run_ids = ["2026-05-01", "2026-05-02"]

    first = await backfill(_service_invoker(service), run_ids, concurrency=1)
    assert len(first.succeeded()) == 2
    assert sum(service.invocations.values()) == 2  # one invocation per run_id

    # Re-run: storage replay short-circuits the body.
    second = await backfill(_service_invoker(service), run_ids, concurrency=1)
    assert len(second.succeeded()) == 2
    assert sum(service.invocations.values()) == 2  # no new invocations


async def test_backfill_respects_concurrency_limit(store: OpenDALStore) -> None:
    """A semaphore of size N caps simultaneous in-flight invocations to N."""
    in_flight = 0
    peak = 0

    class _SlowService:
        def __init__(self, s: OpenDALStore) -> None:
            self._store = s

        @wrap_workflow_method(
            store=lambda self: self._store,  # type: ignore[attr-defined]
            result_type=StringValue,
            options_for=lambda _self, request: Options(
                workflow_id="slow",
                run_id=request.value,
                code_version="v1",
            ),
        )
        async def fetch_prices(self, request: StringValue, _ctx: object = None) -> StringValue:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            try:
                await asyncio.sleep(0.02)
                return StringValue(value=request.value)
            finally:
                in_flight -= 1

    service = _SlowService(store)
    run_ids = [f"2026-05-{d:02d}" for d in range(1, 11)]  # 10 run_ids
    report = await backfill(_service_invoker(service), run_ids, concurrency=3)

    assert len(report.succeeded()) == 10
    assert peak <= 3  # never exceeded the semaphore


async def test_backfill_continues_past_failures_by_default(store: OpenDALStore) -> None:
    class _FlakyService:
        def __init__(self, s: OpenDALStore) -> None:
            self._store = s

        @wrap_workflow_method(
            store=lambda self: self._store,  # type: ignore[attr-defined]
            result_type=StringValue,
            options_for=lambda _self, request: Options(
                workflow_id="flaky",
                run_id=request.value,
                code_version="v1",
            ),
        )
        async def fetch_prices(self, request: StringValue, _ctx: object = None) -> StringValue:
            if request.value == "BAD":
                raise RuntimeError("upstream broke")
            return StringValue(value=f"price:{request.value}")

    service = _FlakyService(store)
    report = await backfill(_service_invoker(service), ["GOOD-1", "BAD", "GOOD-2"], concurrency=1)

    assert len(report.succeeded()) == 2
    assert len(report.failed()) == 1
    failed = report.failed()[0]
    assert failed.run_id == "BAD"
    assert "upstream broke" in str(failed.error)


async def test_backfill_halt_on_error_stops_after_first_failure(
    store: OpenDALStore,
) -> None:
    """halt_on_error=True signals remaining un-dispatched work to skip and
    report as PENDING — already-running invocations finish naturally."""
    invoked: list[str] = []

    class _HaltService:
        def __init__(self, s: OpenDALStore) -> None:
            self._store = s

        @wrap_workflow_method(
            store=lambda self: self._store,  # type: ignore[attr-defined]
            result_type=StringValue,
            options_for=lambda _self, request: Options(
                workflow_id="halt",
                run_id=request.value,
                code_version="v1",
            ),
        )
        async def fetch_prices(self, request: StringValue, _ctx: object = None) -> StringValue:
            invoked.append(request.value)
            if request.value == "BAD":
                raise RuntimeError("upstream broke")
            return StringValue(value=f"price:{request.value}")

    service = _HaltService(store)
    report = await backfill(
        _service_invoker(service),
        ["BAD", "after-1", "after-2", "after-3"],
        concurrency=1,
        halt_on_error=True,
    )

    # First failure halts the rest. Some may have been invoked before the
    # halt event takes effect (race-tolerant assertion).
    assert len(report.failed()) == 1
    assert report.failed()[0].run_id == "BAD"
    assert len(report.pending()) >= 1  # at least one was halted before invocation
    # Total still equals input count — every input has an entry.
    assert len(report.entries) == 4


async def test_backfill_reports_pending_for_workflows_that_stay_in_progress(
    store: OpenDALStore,
) -> None:
    """A workflow body that returns ConnectError(UNAVAILABLE) — typically from
    TimerPendingError or EventPendingError via wrap_workflow_method's
    auto-mapping — surfaces as PENDING, not FAILED."""
    from temporaless.workflow import current_workflow

    class _PendingService:
        def __init__(self, s: OpenDALStore) -> None:
            self._store = s

        @wrap_workflow_method(
            store=lambda self: self._store,  # type: ignore[attr-defined]
            result_type=StringValue,
            options_for=lambda _self, request: Options(
                workflow_id="pending",
                run_id=request.value,
                code_version="v1",
            ),
        )
        async def fetch_prices(self, request: StringValue, _ctx: object = None) -> StringValue:
            from datetime import timedelta

            await current_workflow().sleep("wait", timedelta(hours=1))
            return StringValue(value=request.value)

    service = _PendingService(store)
    report = await backfill(_service_invoker(service), ["2026-05-01", "2026-05-02"], concurrency=1)

    # Both runs are pending — they raised ConnectError(UNAVAILABLE) which the
    # backfill helper recognizes as a pending sentinel.
    assert len(report.pending()) == 2
    assert len(report.failed()) == 0
    assert len(report.succeeded()) == 0


async def test_backfill_rejects_zero_concurrency(store: OpenDALStore) -> None:
    service = _FetchService(store)
    with pytest.raises(ValueError, match="concurrency"):
        await backfill(_service_invoker(service), ["x"], concurrency=0)


async def test_backfill_with_empty_run_ids_returns_empty_report(
    store: OpenDALStore,
) -> None:
    """Edge case: empty input → empty report, not an error."""

    async def never(_run_id: str) -> StringValue:
        raise AssertionError("invoke should not be called")

    report = await backfill(never, [], concurrency=1)
    assert len(report.entries) == 0
    assert "total=0" in str(report)
