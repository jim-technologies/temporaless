"""Scheduler and queue replacement proof using only public Temporaless seams."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.dispatch import Dispatcher, Queue
from temporaless.storage import ActivityKey, OpenDALStore, TimerKey, WorkflowKey
from temporaless.timerscanner import due_timers
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import ActivityOptions, Options, TimerPendingError, Workflow, run


class _BufferedQueue(Queue):
    """Minimal stand-in for an external at-least-once message bus."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes]] = []
        self.acknowledged: list[tuple[str, bytes]] = []
        self.closed = False

    async def submit(self, method: str, payload: bytes) -> None:
        self.messages.append((method, payload))

    async def close(self) -> None:
        self.closed = True

    def ack(self, message: tuple[str, bytes]) -> None:
        self.acknowledged.append(message)


async def test_external_scheduler_queue_reinvokes_due_workflow_idempotently(
    tmp_path,
) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    queue = _BufferedQueue()
    producer = Dispatcher(queue=queue)
    consumer = Dispatcher()
    method = "/prices.v1.PriceWorkflow/Run"
    request = temporaless_pb2.WorkflowKey(
        namespace="default",
        workflow_id="prices:scheduled",
        run_id="2026-08-12T12:00:00Z",
    )
    handler_ids: list[tuple[str, str]] = []
    body_ids: list[tuple[str, str]] = []
    activity_calls = 0

    async def handle(input_message: temporaless_pb2.WorkflowKey) -> StringValue:
        handler_ids.append((input_message.workflow_id, input_message.run_id))

        async def execute(
            workflow: Workflow,
            workflow_input: temporaless_pb2.WorkflowKey,
        ) -> StringValue:
            body_ids.append((workflow_input.workflow_id, workflow_input.run_id))

            async def publish_side_effect(
                activity_input: temporaless_pb2.WorkflowKey,
            ) -> StringValue:
                nonlocal activity_calls
                activity_calls += 1
                return StringValue(
                    value=f"published:{activity_input.workflow_id}:{activity_input.run_id}"
                )

            published = await workflow.execute_activity(
                ActivityOptions(activity_id="publish:price"),
                workflow_input,
                StringValue,
                publish_side_effect,
            )
            await workflow.sleep("wait:market-window", timedelta(hours=1))
            return published

        return await run(
            store,
            Options(
                workflow_id=input_message.workflow_id,
                run_id=input_message.run_id,
            ),
            input_message,
            StringValue,
            execute,
        )

    producer.register(method, temporaless_pb2.WorkflowKey, handle)
    consumer.register(method, temporaless_pb2.WorkflowKey, handle)

    # Any starter (Airflow, NATS, a webhook, or a local cron adapter) submits
    # the same concrete unary protobuf request through the swappable queue.
    await producer.do_async(method, request)
    expected_payload = request.SerializeToString(deterministic=True)
    assert queue.messages == [(method, expected_payload)]

    initial_delivery = queue.messages.pop(0)
    with pytest.raises(TimerPendingError):
        await consumer.invoke(*initial_delivery)
    # A real consumer ACKs this delivery: the typed result means the workflow
    # persisted its timer boundary. NACKing it would hot-redeliver before due;
    # the due-timer path below owns the continuation message instead.
    queue.ack(initial_delivery)
    assert activity_calls == 1

    # Advance the durable timer without relying on a resident process clock.
    timer_key = TimerKey(
        workflow_id=request.workflow_id,
        run_id=request.run_id,
        timer_id="wait:market-window",
    )
    timer = await store.get_timer(timer_key)
    assert timer is not None
    timer.fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    await store.put_timer(timer)

    due = await due_timers(store, datetime.now(UTC))
    assert len(due) == 1
    wake = due[0]
    assert wake.key == timer_key
    assert wake.workflow is not None
    wake_request = temporaless_pb2.WorkflowKey()
    assert wake.workflow.input.Unpack(wake_request)
    assert wake_request == request

    # Two scheduler replicas can enqueue the same wake. Deterministic protobuf
    # bytes and unchanged caller IDs make both deliveries converge on one run.
    await producer.do_async(method, wake_request)
    await producer.do_async(method, wake_request)
    assert queue.messages == [
        (method, expected_payload),
        (method, expected_payload),
    ]

    for _ in range(2):
        delivery = queue.messages.pop(0)
        await consumer.invoke(*delivery)
        queue.ack(delivery)

    expected_ids = (request.workflow_id, request.run_id)
    assert handler_ids == [expected_ids, expected_ids, expected_ids]
    assert body_ids == [expected_ids, expected_ids]
    assert activity_calls == 1
    assert queue.acknowledged == [(method, expected_payload)] * 3

    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id=request.workflow_id, run_id=request.run_id)
    )
    activity_record = await store.get_activity(
        ActivityKey(
            workflow_id=request.workflow_id,
            run_id=request.run_id,
            activity_id="publish:price",
        )
    )
    timer_record = await store.get_timer(timer_key)
    assert workflow_record is not None
    assert workflow_record.status == temporaless_pb2.WORKFLOW_STATUS_COMPLETED
    assert activity_record is not None
    assert activity_record.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED
    assert timer_record is not None
    assert timer_record.status == temporaless_pb2.TIMER_STATUS_FIRED
    assert await due_timers(store, datetime.now(UTC)) == []

    await producer.shutdown()
    await consumer.shutdown()
    assert queue.closed
