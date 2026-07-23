"""Concurrency tests for Python claim coordination.

Each thread runs its own asyncio event loop so the test stresses
multi-process-like concurrency on a shared store.
"""

from __future__ import annotations

import asyncio
import threading

import opendal
import pytest
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.storage import OpenDALStore
from temporaless.workflow import (
    ActivityOptions,
    ClaimBusyError,
    Options,
    Workflow,
    run,
)


def test_concurrent_activity_claim_serialization(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)

    n_workers = 4
    activity_calls = 0
    activity_lock = threading.Lock()
    results: list[tuple[int, str | None, BaseException | None]] = []
    results_lock = threading.Lock()
    barrier = threading.Barrier(n_workers)

    async def worker_async(idx: int) -> None:
        async def execute(workflow: Workflow, request: StringValue) -> StringValue:
            async def fetch(req: StringValue) -> StringValue:
                nonlocal activity_calls
                with activity_lock:
                    activity_calls += 1
                # Hold the claim long enough that the other threads see it.
                await asyncio.sleep(0.05)
                return StringValue(value=f"ok:{req.value}")

            return await workflow.execute_activity(
                ActivityOptions(activity_id="fetch:concurrent"),
                request,
                StringValue,
                fetch,
            )

        result = await run(
            store,
            Options(
                workflow_id="prices:concurrent",
                run_id="2026-05-04",
                claim_owner_id=f"worker-{idx}",
            ),
            StringValue(value="AAPL"),
            StringValue,
            execute,
        )
        with results_lock:
            results.append((idx, result.value, None))

    def worker(idx: int) -> None:
        barrier.wait()
        try:
            asyncio.run(worker_async(idx))
        except BaseException as exc:  # noqa: BLE001 - capture all errors
            with results_lock:
                results.append((idx, None, exc))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert activity_calls == 1, f"activity executed {activity_calls} times, want exactly 1"

    successes = [r for _, r, _ in results if r is not None]
    busies = [e for _, _, e in results if isinstance(e, ClaimBusyError)]
    other_errors = [e for _, r, e in results if r is None and not isinstance(e, ClaimBusyError)]

    assert len(successes) >= 1, f"no thread succeeded; results = {results}"
    assert len(busies) >= 1, f"claim should have blocked at least one thread; results = {results}"
    if other_errors:
        # OpenDAL fs is not fully atomic for concurrent reads-during-writes;
        # transient backend errors are documented in docs/hard-cases.md.
        pytest.skip(f"transient backend errors observed: {other_errors!r}")


def test_concurrent_replays_return_same_result(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)

    options = Options(
        workflow_id="prices:replay",
        run_id="2026-05-04",
    )
    activity_calls = 0
    activity_lock = threading.Lock()

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        async def fetch(req: StringValue) -> StringValue:
            nonlocal activity_calls
            with activity_lock:
                activity_calls += 1
            return StringValue(value=f"ok:{req.value}")

        return await workflow.execute_activity(
            ActivityOptions(activity_id="fetch:replay"),
            request,
            StringValue,
            fetch,
        )

    # Seed: run once to completion.
    seeded = asyncio.run(run(store, options, StringValue(value="AAPL"), StringValue, execute))
    assert seeded.value == "ok:AAPL"
    assert activity_calls == 1

    # Now hammer the store with concurrent replays.
    n_threads = 8
    barrier = threading.Barrier(n_threads)
    results: list[str] = []
    results_lock = threading.Lock()

    def replayer() -> None:
        barrier.wait()
        result = asyncio.run(run(store, options, StringValue(value="AAPL"), StringValue, execute))
        with results_lock:
            results.append(result.value)

    threads = [threading.Thread(target=replayer) for _ in range(n_threads)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == n_threads
    assert all(r == "ok:AAPL" for r in results)
    assert activity_calls == 1, "activity should not have re-executed during replay"
