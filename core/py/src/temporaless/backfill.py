"""Backfill helper for running a workflow over many run_ids.

The Dagster/Prefect/Airflow backfill primitive: given a workflow and a set of
``run_ids`` (typically dates / partitions), dispatch each one with bounded
concurrency and report aggregate status.

Backfill is idempotent: already-COMPLETED runs replay from storage in
microseconds; already-FAILED runs re-execute (call ``inspector.reset_workflow``
first to clear them); IN_PROGRESS runs are reported as PENDING and need a
scanner / re-invoke to resume. Re-running ``backfill`` over the same set is
free for COMPLETED runs.

Example::

    from temporaless.backfill import backfill

    service = QuantService(store)

    async def invoke(run_id: str) -> StringValue:
        return await service.fetch_prices(StringValue(value=run_id))

    report = await backfill(
        invoke,
        run_ids=[f"2026-{m:02d}-{d:02d}" for m in (4, 5) for d in range(1, 31)],
        concurrency=5,
    )
    print(f"backfill: {report}")
    for entry in report.failed():
        print(f"  failed: {entry.run_id} → {entry.error}")
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum

from temporaless.workflow import (
    ClaimBusyError,
    ConcurrencyBusyError,
    EventPendingError,
    TimerPendingError,
    WorkflowDependencyPendingError,
)


class BackfillStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PENDING = "pending"


@dataclass
class BackfillEntry[T]:
    run_id: str
    status: BackfillStatus
    result: T | None = None
    error: BaseException | None = None


@dataclass
class BackfillReport[T]:
    entries: list[BackfillEntry[T]] = field(default_factory=list)

    def succeeded(self) -> list[BackfillEntry[T]]:
        return [e for e in self.entries if e.status == BackfillStatus.SUCCEEDED]

    def failed(self) -> list[BackfillEntry[T]]:
        return [e for e in self.entries if e.status == BackfillStatus.FAILED]

    def pending(self) -> list[BackfillEntry[T]]:
        return [e for e in self.entries if e.status == BackfillStatus.PENDING]

    def __str__(self) -> str:
        return (
            f"BackfillReport(succeeded={len(self.succeeded())}, "
            f"failed={len(self.failed())}, "
            f"pending={len(self.pending())}, "
            f"total={len(self.entries)})"
        )


async def backfill[T](
    invoke: Callable[[str], Awaitable[T]],
    run_ids: Iterable[str],
    *,
    concurrency: int = 1,
    halt_on_error: bool = False,
) -> BackfillReport[T]:
    """Run a workflow over many run_ids with bounded concurrency.

    ``invoke`` is a partial-applied workflow caller: given a run_id, it returns
    the workflow result (typically by calling a ``@wrap_workflow_method``
    method that derives ``Options.run_id`` from its request). Per-run results
    are independent: a failure in one run_id doesn't affect others, unless
    ``halt_on_error=True``.

    Workflow runs that stay IN_PROGRESS (timer/event/dependency pending or a
    live claim holder), plus concurrency-cap contention, are PENDING. Their
    ConnectRPC forms (UNAVAILABLE, ALREADY_EXISTS, RESOURCE_EXHAUSTED) are
    classified the same way.

    Args:
        invoke: callable mapping a run_id to a workflow result.
        run_ids: iterable of run_ids to dispatch.
        concurrency: maximum simultaneous in-flight invocations (default 1).
        halt_on_error: stop dispatching new run_ids after the first failure;
            already-running invocations finish; un-dispatched ones are reported
            as PENDING.

    Returns:
        ``BackfillReport`` with one ``BackfillEntry`` per input run_id.
    """
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")

    semaphore = asyncio.Semaphore(concurrency)
    halt = asyncio.Event()

    async def run_one(run_id: str) -> BackfillEntry[T]:
        if halt.is_set():
            return BackfillEntry(run_id=run_id, status=BackfillStatus.PENDING)
        async with semaphore:
            if halt.is_set():
                return BackfillEntry(run_id=run_id, status=BackfillStatus.PENDING)
            try:
                result = await invoke(run_id)
                return BackfillEntry(run_id=run_id, status=BackfillStatus.SUCCEEDED, result=result)
            except Exception as exc:
                if _is_pending_error(exc):
                    return BackfillEntry(run_id=run_id, status=BackfillStatus.PENDING, error=exc)
                if halt_on_error:
                    halt.set()
                return BackfillEntry(run_id=run_id, status=BackfillStatus.FAILED, error=exc)

    entries = await asyncio.gather(*(run_one(rid) for rid in run_ids))
    return BackfillReport(entries=list(entries))


def _is_pending_error(exc: BaseException) -> bool:
    if isinstance(
        exc,
        (
            TimerPendingError,
            EventPendingError,
            WorkflowDependencyPendingError,
            ClaimBusyError,
            ConcurrencyBusyError,
        ),
    ):
        return True
    try:
        from connectrpc.code import Code
        from connectrpc.errors import ConnectError

        if isinstance(exc, ConnectError) and exc.code in {
            Code.UNAVAILABLE,
            Code.ALREADY_EXISTS,
            Code.RESOURCE_EXHAUSTED,
        }:
            return True
    except ImportError:
        pass
    return False
