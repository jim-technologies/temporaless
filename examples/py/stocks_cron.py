"""Cron-driven stocks pipeline example.

Demonstrates the full end-to-end production pattern:

1. A cron schedule fires every minute on weekdays for two symbols.
2. Each fire dispatches a workflow with ``run_id = fire_time.isoformat()``.
3. The workflow body fetches a price (an activity) and computes a signal
   (another activity). Both records are persisted, so the workflow replays
   for free if the same (workflow_id, run_id) is re-invoked.
4. Stateless seeding: on startup the scheduler reads each workflow's
   latest-run pointer via ``last_fires_from_runs`` — so the scheduler has no
   separate persistence.

In production:

- Replace the ``while True: tick(); sleep(60)`` driver with a Kubernetes
  CronJob, EventBridge schedule, or another minute-cadence trigger.
- Replace the simulated ``_fetch_price`` with the real vendor call.
- Point the OpenDAL operator at S3/GCS instead of fs.

Run:

    uv run --project core/py python examples/py/stocks_cron.py
"""

from __future__ import annotations

import asyncio
import random
import tempfile
from datetime import UTC, datetime

import opendal
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue

from temporaless import (
    ActivityOptions,
    OpenDALStore,
    Options,
    Store,
    Workflow,
    annotate,
    run,
)
from temporaless.cronscheduler import (
    Schedule,
    Scheduler,
    last_fires_from_runs,
)

RUN_ID_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


async def _fetch_price(request: StringValue) -> StringValue:
    annotate("vendor", "alpha")
    annotate("symbol", request.value)
    await asyncio.sleep(0.05)
    return StringValue(value=f"{request.value}:100.0+{random.uniform(-5, 5):.2f}")


async def _signal(request: StringValue) -> StringValue:
    annotate("kind", "signal")
    return StringValue(value=f"signal({request.value})")


async def _stocks_workflow(workflow: Workflow, symbol: StringValue) -> StringValue:
    """One symbol per workflow: fetch the price, compose a signal."""
    price = await workflow.execute_activity(
        ActivityOptions(activity_id="fetch:price"),
        symbol,
        StringValue,
        _fetch_price,
    )
    return await workflow.execute_activity(
        ActivityOptions(activity_id="compose:signal"),
        price,
        StringValue,
        _signal,
    )


def _make_dispatcher(store: Store):
    """Build the cron dispatcher that turns each fire into a workflow run.

    Two cron-scheduler processes can race here. The shared fire-time run ID
    gives terminal replay, while ``claim_owner_id`` plus OpenDAL's atomic
    create-if-absent claims prevents overlapping first execution.
    """

    async def dispatch(schedule_id: str, fire_time: datetime) -> None:
        symbol = schedule_id.removeprefix("prices:").upper()
        run_order_time = Timestamp()
        run_order_time.FromDatetime(fire_time)
        await run(
            store,
            Options(
                workflow_id=schedule_id,
                run_id=fire_time.strftime(RUN_ID_FORMAT),
                run_order_time=run_order_time,
                # Two scheduler replicas may dispatch this fire together.
                # OpenDAL supplies atomic create-if-absent claims; the caller
                # supplies only a diagnostic owner identity.
                claim_owner_id=f"scheduler:{schedule_id}",
            ),
            StringValue(value=symbol),
            StringValue,
            _stocks_workflow,
        )

    return dispatch


async def main() -> None:
    operator = opendal.AsyncOperator(
        "fs", root=tempfile.mkdtemp(prefix="temporaless-stocks-cron-")
    )
    store = OpenDALStore(operator)

    schedules = [
        Schedule(id="prices:aapl", expression="* * * * 1-5"),
        Schedule(id="prices:tsla", expression="*/5 * * * 1-5"),
    ]
    scheduler = Scheduler(schedules, _make_dispatcher(store))

    # Stateless seeding — read existing fire times from the run records.
    snapshot = await last_fires_from_runs(store, "", [s.id for s in schedules])
    scheduler.restore(snapshot)
    if snapshot:
        print(f"resumed scheduler from storage: {snapshot}")
    else:
        print("first run; anchoring scheduler to current time")

    # Demo: simulate three minutes of clock advancement so the workflow fires.
    # In production, drive .tick() from a real clock (Kubernetes CronJob etc.).
    seed = datetime(2026, 5, 4, 9, 30, tzinfo=UTC)
    for minute in range(3):
        now = seed.replace(minute=30 + minute)
        fired = await scheduler.tick(now)
        print(f"  tick at {now.isoformat()} fired {fired} workflows")

    print("\nfinal storage snapshot:")
    for schedule in schedules:
        pointer = await store.get_latest_workflow_run("", schedule.id)
        latest = pointer.key.run_id if pointer is not None else "none"
        print(f"  {schedule.id}: latest run {latest}")


if __name__ == "__main__":
    asyncio.run(main())
