"""Long-running approval workflow — proof that durable multi-day workflows work.

The shape:

1. **Validate** the request (activity, runs once).
2. **Sleep** until the next processing window (durable timer; survives process
   death).
3. **Wait** for a manager approval signal (durable event; can take days).
4. **Process** the export (activity).
5. **Notify** completion (activity).

Each call to ``run()`` is a separate process. Between calls the workflow record
is ``IN_PROGRESS``, the timer is ``SCHEDULED``, the event is awaited. The
workflow body re-runs on each invocation; replay short-circuits everything
that's already been resolved.

This is the canonical Temporal-style "long-running workflow" pattern, but with
no engine to operate. State lives in the OpenDAL store; pods are interchangeable.

Run:

    uv run --project core/py python examples/py/approval_workflow.py
"""

from __future__ import annotations

import asyncio
import tempfile
from datetime import UTC, datetime, timedelta

import opendal
from google.protobuf.wrappers_pb2 import StringValue

from temporaless import (
    ActivityOptions,
    EventKey,
    EventPendingError,
    OpenDALStore,
    Options,
    TimerKey,
    TimerPendingError,
    Workflow,
    annotate,
    run,
    send_event,
)


async def _validate(request: StringValue) -> StringValue:
    """First step — runs once, persisted in an activity record."""
    annotate("step", "validate")
    return StringValue(value=f"validated:{request.value}")


async def _process(request: StringValue) -> StringValue:
    annotate("step", "process")
    return StringValue(value=f"processed:{request.value}")


async def _notify(request: StringValue) -> StringValue:
    annotate("step", "notify")
    return StringValue(value=f"notified:{request.value}")


async def approval_workflow(workflow: Workflow, request: StringValue) -> StringValue:
    validated = await workflow.execute_activity(
        ActivityOptions(activity_id="validate"),
        request,
        StringValue,
        _validate,
    )

    # Durable timer — workflow stays IN_PROGRESS until the timer fires.
    # In production, a timer-scanner cron picks up the SCHEDULED timer and
    # re-invokes this handler when fire_at <= now.
    await workflow.sleep("processing-window", timedelta(hours=4))

    # Durable event — workflow stays IN_PROGRESS until external code calls
    # send_event. Could be hours, days, weeks; replay cost is constant.
    approval = await workflow.wait_event("manager-approval", StringValue)

    processed = await workflow.execute_activity(
        ActivityOptions(activity_id="process"),
        StringValue(value=f"{validated.value}|approved-by:{approval.value}"),
        StringValue,
        _process,
    )

    return await workflow.execute_activity(
        ActivityOptions(activity_id="notify"),
        processed,
        StringValue,
        _notify,
    )


async def main() -> None:
    operator = opendal.AsyncOperator(
        "fs", root=tempfile.mkdtemp(prefix="temporaless-approval-")
    )
    store = OpenDALStore(operator)
    options = Options(
        workflow_id="export:user42", run_id="2026-05-04", code_version="example"
    )
    request = StringValue(value="data-export-request")

    # Process 1: validate runs, sleep raises TimerPendingError. Workflow
    # stays IN_PROGRESS — caller logs and moves on.
    print("=== process 1: validate, then hit durable sleep ===")
    try:
        await run(store, options, request, StringValue, approval_workflow)
    except TimerPendingError as exc:
        print(f"  timer pending: {exc.timer_id} until {exc.wake_at.isoformat()}")

    # Process 2: simulate the timer scanner firing the timer (in production,
    # a CronJob runs ``store.due_timers(now)`` on a 1-minute cadence).
    print("\n=== process 2: timer scanner advances the timer + re-invokes ===")
    timer_record = await store.get_timer(
        TimerKey(
            workflow_id="export:user42",
            run_id="2026-05-04",
            timer_id="processing-window",
        )
    )
    assert timer_record is not None
    timer_record.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(timer_record)

    try:
        await run(store, options, request, StringValue, approval_workflow)
    except EventPendingError as exc:
        print(f"  event pending: {exc.event_id}")

    # Process 3: external service (Slack bot, approval UI, webhook handler)
    # delivers the approval signal.
    print("\n=== process 3: manager approves; workflow resumes to completion ===")
    await send_event(
        store,
        EventKey(
            workflow_id="export:user42",
            run_id="2026-05-04",
            event_id="manager-approval",
        ),
        StringValue(value="alice@example.com"),
    )
    result = await run(store, options, request, StringValue, approval_workflow)
    print(f"  final: {result.value!r}")

    # Process 4: replay returns the cached result without re-running anything.
    print("\n=== process 4: replay short-circuits to cached result ===")
    replayed = await run(store, options, request, StringValue, approval_workflow)
    print(f"  replayed: {replayed.value!r}")
    assert replayed.value == result.value


if __name__ == "__main__":
    asyncio.run(main())
