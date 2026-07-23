"""End-to-end remote ConnectStore integration test.

Spins up a real ASGI HTTP server in front of an OpenDAL-backed RecordStoreService,
connects via ConnectStore.from_address, and drives workflow.run against it.
Proves the Store abstraction is genuinely transport-neutral in Python (parity
with adapters/go/connectstore/integration_test.go).
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import threading
from datetime import timedelta

import opendal
import pytest
import uvicorn
from google.protobuf.duration_pb2 import Duration
from google.protobuf.wrappers_pb2 import StringValue
from temporaless_indexstore import IndexedStore

from temporaless.connectstore import (
    ConnectQueryStore,
    ConnectStore,
    asgi_application,
    query_asgi_application,
)
from temporaless.inspector import (
    list_workflows_by_status,
    reset_activity,
    reset_workflow,
)
from temporaless.storage import ActivityKey, ClaimKey, WorkflowKey
from temporaless.v1 import temporaless_connect, temporaless_pb2
from temporaless.workflow import (
    WORKFLOW_EXECUTION_CLAIM_ID,
    ActivityError,
    ActivityOptions,
    ClaimBusyError,
    Options,
    RetryPolicy,
    Workflow,
    run,
)


def _free_port() -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def remote_store(tmp_path):
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    backend = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    store_app = asgi_application(backend)
    query_app = query_asgi_application(backend)

    async def app(scope, receive, send):
        path = scope.get("path", "")
        if path.startswith("/temporaless.v1.RecordQueryService/"):
            await query_app(scope, receive, send)
            return
        await store_app(scope, receive, send)

    port = _free_port()
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        loop="asyncio",
        lifespan="off",
    )
    server = uvicorn.Server(config)

    def serve() -> None:
        asyncio.run(server.serve())

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    # Wait for the server to come up rather than busy-looping.
    deadline = 5.0
    while deadline > 0 and not server.started:
        thread.join(0.05)
        deadline -= 0.05
    if not server.started:
        raise RuntimeError("uvicorn server failed to start")
    try:
        address = f"http://127.0.0.1:{port}"
        yield (
            ConnectStore.from_address(address),
            ConnectQueryStore.from_address(address),
            address,
        )
    finally:
        server.should_exit = True
        thread.join(timeout=5)


async def test_generated_client_defaults_to_google_protobuf_codec(remote_store) -> None:
    """The generated client must work without a Temporaless wrapper.

    connectrpc 0.11 defaults to protobuf-py, while Temporaless deliberately
    generates Google protobuf messages. The matching generator option owns
    this compatibility at the generated boundary.
    """
    _remote_store, _remote_query, address = remote_store
    async with temporaless_connect.RecordStoreServiceClient(address) as client:
        response = await client.get_store_capabilities(
            temporaless_pb2.GetStoreCapabilitiesRequest()
        )
    assert response.claim_capability == temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS


async def test_remote_workflow_run_end_to_end(remote_store) -> None:
    remote_store, remote_query, _address = remote_store
    options = Options(
        workflow_id="remote:retry",
        run_id="2026-05-04",
        claim_owner_id="remote-worker",
    )
    duration = Duration()
    duration.FromTimedelta(timedelta(milliseconds=1))
    policy = RetryPolicy(maximum_attempts=3, initial_interval=duration)

    calls = 0

    async def execute(workflow: Workflow, request: StringValue) -> StringValue:
        async def fetch(req: StringValue) -> StringValue:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise ActivityError("rate_limited", "transient")
            return StringValue(value=f"ok:{req.value}")

        return await workflow.execute_activity(
            ActivityOptions(activity_id="fetch:remote", retry_policy=policy),
            request,
            StringValue,
            fetch,
        )

    first = await run(remote_store, options, StringValue(value="AAPL"), StringValue, execute)
    assert first.value == "ok:AAPL"
    assert calls == 3

    # Replay through the remote store — no activity executions.
    async def replay_execute(_w: Workflow, _r: StringValue) -> StringValue:
        raise AssertionError("workflow body should not re-execute on replay")

    second = await run(
        remote_store, options, StringValue(value="AAPL"), StringValue, replay_execute
    )
    assert second.value == "ok:AAPL"

    # Inspector via remote store.
    completed = await list_workflows_by_status(
        remote_query, temporaless_pb2.WORKFLOW_STATUS_COMPLETED
    )
    assert [r.key.workflow_id for r in completed] == ["remote:retry"]

    # List activities — full attempt history persisted via remote PutActivity calls.
    activities = await remote_store.list_activities(
        WorkflowKey(workflow_id="remote:retry", run_id="2026-05-04")
    )
    assert len(activities) == 1
    assert len(activities[0].attempts) == 3

    # Reset via remote store; re-run drives a fresh execution.
    await reset_workflow(remote_store, WorkflowKey(workflow_id="remote:retry", run_id="2026-05-04"))
    await reset_activity(
        remote_store,
        ActivityKey(
            workflow_id="remote:retry",
            run_id="2026-05-04",
            activity_id="fetch:remote",
        ),
    )

    fresh_calls = 0

    async def fresh_execute(workflow: Workflow, request: StringValue) -> StringValue:
        async def fetch(req: StringValue) -> StringValue:
            nonlocal fresh_calls
            fresh_calls += 1
            return StringValue(value=f"fresh:{req.value}")

        return await workflow.execute_activity(
            ActivityOptions(activity_id="fetch:remote", retry_policy=policy),
            request,
            StringValue,
            fetch,
        )

    final = await run(remote_store, options, StringValue(value="AAPL"), StringValue, fresh_execute)
    assert final.value == "fresh:AAPL"
    assert fresh_calls == 1


async def test_remote_workflow_singleflight_busy_release_and_replay(remote_store) -> None:
    remote_store, _remote_query, _address = remote_store
    options = Options(
        workflow_id="remote:singleflight",
        run_id="run:1",
        claim_owner_id="worker:shared",
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def execute(_workflow: Workflow, request: StringValue) -> StringValue:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return StringValue(value=f"ok:{request.value}")

    first = asyncio.create_task(
        run(remote_store, options, StringValue(value="AAPL"), StringValue, execute)
    )
    await entered.wait()

    async def duplicate_body(_workflow: Workflow, _request: StringValue) -> StringValue:
        raise AssertionError("duplicate workflow body executed")

    with pytest.raises(ClaimBusyError):
        await run(
            remote_store,
            options,
            StringValue(value="AAPL"),
            StringValue,
            duplicate_body,
        )

    release.set()
    assert (await first).value == "ok:AAPL"
    assert calls == 1
    assert (
        await remote_store.get_claim(
            ClaimKey(
                workflow_id=options.workflow_id,
                run_id=options.run_id,
                claim_id=WORKFLOW_EXECUTION_CLAIM_ID,
            )
        )
        is None
    )

    async def replay_body(_workflow: Workflow, _request: StringValue) -> StringValue:
        raise AssertionError("terminal replay executed workflow body")

    replayed = await run(
        remote_store,
        options,
        StringValue(value="AAPL"),
        StringValue,
        replay_body,
    )
    assert replayed.value == "ok:AAPL"


async def test_remote_sweep_and_due_timers_round_trip(remote_store) -> None:
    remote_store, remote_query, _address = remote_store
    """The compound RPCs (Sweep + DueTimers) work over the real ASGI wire.
    In-process tests cover the service handlers; this proves the proto
    serialization round-trip too. One round-trip per call rather than the
    N round-trips a client-side composition would need.
    """
    from datetime import UTC, datetime, timedelta

    from google.protobuf.timestamp_pb2 import Timestamp

    from temporaless.storage import (
        TIMER_RECORD_SCHEMA_VERSION,
        WORKFLOW_RECORD_SCHEMA_VERSION,
        TimerKey,
    )

    # Seed: one COMPLETED workflow backdated 48h, one fresh.
    backdated = Timestamp()
    backdated.FromDatetime(datetime.now(UTC) - timedelta(hours=48))
    fresh = Timestamp()
    fresh.GetCurrentTime()
    for run_id, completed_at in (("old", backdated), ("fresh", fresh)):
        await remote_store.put_workflow(
            temporaless_pb2.WorkflowRecord(
                schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
                key=WorkflowKey(workflow_id="remote:sweep", run_id=run_id).to_proto(),
                workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
                status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
                completed_at=completed_at,
            )
        )

    deleted = await remote_query.sweep("", datetime.now(UTC), timedelta(hours=24))
    assert deleted == 1
    assert (
        await remote_store.get_workflow(WorkflowKey(workflow_id="remote:sweep", run_id="old"))
    ) is None

    # DueTimers: seed an IN_PROGRESS workflow + a SCHEDULED timer with fire_at in the past.
    wf_key = WorkflowKey(workflow_id="remote:timer", run_id="2026-05-04")
    timer_key = TimerKey(workflow_id="remote:timer", run_id="2026-05-04", timer_id="cooldown")
    fire_at = Timestamp()
    fire_at.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))

    await remote_store.put_workflow(
        temporaless_pb2.WorkflowRecord(
            schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
            key=wf_key.to_proto(),
            workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
            status=temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        )
    )
    await remote_store.put_timer(
        temporaless_pb2.TimerRecord(
            schema_version=TIMER_RECORD_SCHEMA_VERSION,
            key=timer_key.to_proto(),
            timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
            status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
            fire_at=fire_at,
        )
    )

    due = await remote_store.due_timers("", datetime.now(UTC))
    assert len(due) == 1
    assert due[0].key.timer_id == "cooldown"
    assert due[0].workflow.key.workflow_id == "remote:timer"
