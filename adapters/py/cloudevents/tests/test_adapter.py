from __future__ import annotations

import asyncio
import base64
import json
from itertools import count
from typing import Any, cast

import opendal
import pytest
from cloudevents.core.formats.json import JSONFormat
from cloudevents.core.v1.event import CloudEvent
from connectrpc.method import IdempotencyLevel, MethodInfo
from connectrpc.request import Headers, RequestContext
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue
from temporaless.connectstore import asgi_application
from temporaless.storage import (
    OpenDALStore,
    WorkflowKey,
    activity_key_from_proto,
    claim_key_from_proto,
    event_key_from_proto,
    timer_key_from_proto,
    workflow_key_from_proto,
)
from temporaless.v1 import temporaless_pb2 as pb

from temporaless_cloudevents import Options, RecordStoreInterceptor


@pytest.fixture
def store(tmp_path) -> OpenDALStore:
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


def records() -> dict[str, Any]:
    identity = {"namespace": "tenant", "workflow_id": "prices", "run_id": "run:1"}
    workflow = pb.WorkflowRecord(
        schema_version=pb.RECORD_SCHEMA_VERSION_WORKFLOW,
        key=pb.WorkflowKey(**identity),
        workflow_type="workflow:test",
        status=pb.WORKFLOW_STATUS_COMPLETED,
        annotations={"private": "private-annotation"},
    )
    workflow.input.Pack(StringValue(value="private-input"))
    activity = pb.ActivityRecord(
        schema_version=pb.RECORD_SCHEMA_VERSION_ACTIVITY,
        key=pb.ActivityKey(**identity, activity_id="fetch"),
        activity_type="activity:test",
        status=pb.ACTIVITY_STATUS_COMPLETED,
    )
    activity.result.Pack(StringValue(value="private-result"))
    timer = pb.TimerRecord(
        schema_version=pb.RECORD_SCHEMA_VERSION_TIMER,
        key=pb.TimerKey(**identity, timer_id="wait"),
        timer_kind=pb.TIMER_KIND_SLEEP,
        status=pb.TIMER_STATUS_FIRED,
    )
    event = pb.EventRecord(
        schema_version=pb.RECORD_SCHEMA_VERSION_EVENT,
        key=pb.EventKey(**identity, event_id="approval"),
        received_at=Timestamp(seconds=1_700_000_000),
    )
    event.payload.Pack(StringValue(value="private-event"))
    claim = pb.ClaimRecord(
        schema_version=pb.RECORD_SCHEMA_VERSION_CLAIM,
        key=pb.ClaimKey(**identity, claim_id="workflow"),
        owner_id="private-owner",
        resource_type=pb.CLAIM_RESOURCE_TYPE_WORKFLOW,
        resource_id="prices",
    )
    return {
        "Workflow": workflow,
        "Activity": activity,
        "Timer": timer,
        "Event": event,
        "Claim": claim,
    }


_KEY_READERS = {
    "Workflow": workflow_key_from_proto,
    "Activity": activity_key_from_proto,
    "Timer": timer_key_from_proto,
    "Event": event_key_from_proto,
    "Claim": claim_key_from_proto,
}


@pytest.mark.parametrize(
    ("method", "kind"),
    [
        ("PutWorkflow", "Workflow"),
        ("PutActivity", "Activity"),
        ("PutTimer", "Timer"),
        ("PutEvent", "Event"),
        ("DeliverEvent", "Event"),
        ("TryCreateClaim", "Claim"),
    ],
)
async def test_rpc_publishes_committed_key_and_duplicate_observations(store, method, kind) -> None:
    record = records()[kind]
    key = record.key
    published: list[CloudEvent] = []
    sequence = count(1)

    async def publish(event: CloudEvent) -> None:
        # This read takes the real OpenDAL path while the RPC is awaiting its
        # publisher, proving that publication happens after persistence.
        stored = await getattr(store, f"get_{kind.lower()}")(_KEY_READERS[kind](key))
        assert stored == record
        published.append(event)

    app = asgi_application(
        store,
        interceptors=[
            RecordStoreInterceptor(Options("urn:test:store", lambda: str(next(sequence)), publish))
        ],
    )
    request = getattr(pb, f"{method}Request")(record=record)
    responses = [await call_asgi(app, method, request) for _ in range(2)]
    assert all(status == 200 for status, _ in responses), responses
    assert len(published) == 2
    assert [event.get_id() for event in published] == ["1", "2"]
    for event in published:
        assert event.get_specversion() == "1.0"
        assert event.get_source() == "urn:test:store"
        assert event.get_type() == f"temporaless.v1.RecordStoreService.{method}"
        assert event.get_datacontenttype() == "application/protobuf"
        assert event.get_dataschema() == f"https://type.googleapis.com/{key.DESCRIPTOR.full_name}"
        assert event.get_data() == key.SerializeToString(deterministic=True)
        encoded = json.loads(JSONFormat().write(event))
        assert "data" not in encoded
        assert base64.b64decode(encoded["data_base64"]) == event.get_data()
        assert "private-" not in str(encoded)

    if method == "TryCreateClaim":
        assert pb.TryCreateClaimResponse.FromString(responses[0][1]).created
        assert not pb.TryCreateClaimResponse.FromString(responses[1][1]).created
    if method == "DeliverEvent":
        assert (
            pb.DeliverEventResponse.FromString(responses[1][1]).disposition
            == pb.EVENT_DELIVERY_DISPOSITION_IDEMPOTENT
        )


@pytest.mark.parametrize("kind", ["Workflow", "Activity", "Timer", "Event", "Claim", "Run"])
async def test_delete_observations_include_absent_and_whole_run(store, kind) -> None:
    record = records()["Workflow" if kind == "Run" else kind]
    key = record.key
    if kind == "Claim":
        await store.try_create_claim(record)
    else:
        await getattr(store, f"put_{'workflow' if kind == 'Run' else kind.lower()}")(record)
    if kind == "Run":
        await store.put_activity(records()["Activity"])

    published: list[CloudEvent] = []

    async def publish(event: CloudEvent) -> None:
        selected_kind = "Workflow" if kind == "Run" else kind
        assert (
            await getattr(store, f"get_{selected_kind.lower()}")(_KEY_READERS[selected_kind](key))
            is None
        )
        if kind == "Run":
            assert await store.list_activities(workflow_key_from_proto(key)) == []
        published.append(event)

    sequence = count(1)
    app = asgi_application(
        store,
        interceptors=[
            RecordStoreInterceptor(Options("urn:test:store", lambda: str(next(sequence)), publish))
        ],
    )
    method = f"Delete{kind}"
    request = getattr(pb, f"{method}Request")(key=key)
    for _ in range(2):
        status, body = await call_asgi(app, method, request)
        assert status == 200, body
    assert len(published) == 2
    assert published[0].get_data() == key.SerializeToString(deterministic=True)


@pytest.mark.parametrize(
    "failure", ["publisher", "id-empty", "id-control", "id-c1", "id-space", "id-exception"]
)
async def test_observer_failures_preserve_storage_success(store, caplog, failure) -> None:
    def new_id() -> str:
        if failure == "id-exception":
            raise RuntimeError("id allocation unavailable")
        return {
            "id-empty": "",
            "id-control": "bad\nvalue",
            "id-c1": "bad\x80value",
            "id-space": " event ",
        }.get(failure, "event:1")

    async def publish(_event: CloudEvent) -> None:
        raise OSError("receiver unavailable")

    app = asgi_application(
        store,
        interceptors=[RecordStoreInterceptor(Options("urn:test:store", new_id, publish))],
    )
    record = records()["Workflow"]
    status, body = await call_asgi(app, "PutWorkflow", pb.PutWorkflowRequest(record=record))
    assert status == 200, body
    assert await store.get_workflow(WorkflowKey("prices", "run:1", "tenant")) == record
    assert "storage RPC succeeded" in caplog.text


async def test_read_and_invalid_write_publish_nothing(store) -> None:
    published: list[CloudEvent] = []

    async def publish(event: CloudEvent) -> None:
        published.append(event)

    app = asgi_application(
        store,
        interceptors=[RecordStoreInterceptor(Options("urn:test:store", lambda: "event", publish))],
    )
    key = pb.WorkflowKey(namespace="tenant", workflow_id="prices", run_id="run:1")
    status, _ = await call_asgi(app, "GetWorkflow", pb.GetWorkflowRequest(key=key))
    assert status == 200
    status, _ = await call_asgi(app, "PutWorkflow", pb.PutWorkflowRequest())
    assert status == 400
    assert published == []


async def test_key_snapshot_discards_unknown_fields_and_survives_handler_mutation(store) -> None:
    published: list[CloudEvent] = []

    async def publish(event: CloudEvent) -> None:
        published.append(event)

    interceptor = RecordStoreInterceptor(Options("urn:test:store", lambda: "event", publish))
    record = records()["Workflow"]
    request = pb.PutWorkflowRequest(record=record)
    secret = b"private-unknown-field"
    request.record.key.MergeFromString(b"\x9a\x06" + bytes([len(secret)]) + secret)

    async def mutate_key(call_next, message, ctx):
        response = await call_next(message, ctx)
        message.record.key.run_id = "mutated"
        return response

    class MutatingInterceptor:
        intercept_unary = staticmethod(mutate_key)

    app = asgi_application(store, interceptors=[interceptor, MutatingInterceptor()])
    status, body = await call_asgi(app, "PutWorkflow", request)
    assert status == 200, body
    assert len(published) == 1
    assert published[0].get_data() == record.key.SerializeToString(deterministic=True)
    assert secret not in cast(bytes, published[0].get_data())


@pytest.mark.parametrize(
    ("service", "method", "message"),
    [
        ("example.OtherService", "PutWorkflow", pb.PutWorkflowRequest()),
        ("temporaless.v1.RecordStoreService", "GetWorkflow", pb.PutWorkflowRequest()),
        ("temporaless.v1.RecordStoreService", "PutWorkflow", pb.PutActivityRequest()),
    ],
)
async def test_service_method_and_request_identity_gate(service, method, message) -> None:
    async def publish(_event: CloudEvent) -> None:
        pytest.fail("unrelated RPC must not publish")

    interceptor = RecordStoreInterceptor(Options("urn:test:store", lambda: "event", publish))
    ctx = RequestContext(
        method=MethodInfo(
            name=method,
            service_name=service,
            input=type(message),
            output=pb.PutWorkflowResponse,
            idempotency_level=IdempotencyLevel.UNKNOWN,
        ),
        http_method="POST",
        request_headers=Headers(),
    )
    expected = pb.PutWorkflowResponse()

    async def next_handler(_request, _ctx):
        return expected

    assert await interceptor.intercept_unary(next_handler, message, ctx) is expected


@pytest.mark.parametrize(
    "invalid",
    [
        "source",
        "source-relative",
        "source-escape",
        "source-space",
        "source-control",
        "source-c1",
        "publisher",
        "id",
    ],
)
def test_invalid_configuration_is_rejected(invalid) -> None:
    async def publish(_event: CloudEvent) -> None:
        pass

    async def async_id() -> str:
        return "event"

    source = {
        "source": "",
        "source-relative": "relative/path",
        "source-escape": "urn:records:%zz",
        "source-space": "urn:bad source",
        "source-control": "urn:bad\x00",
        "source-c1": "urn:bad\x80",
    }.get(invalid, "urn:test:store")
    options = Options(
        source,
        async_id if invalid == "id" else lambda: "event",  # ty: ignore[invalid-argument-type]
        (lambda _event: None) if invalid == "publisher" else publish,  # ty: ignore[invalid-argument-type]
    )
    with pytest.raises((TypeError, ValueError)):
        RecordStoreInterceptor(options)


async def test_async_callable_publisher_is_awaited(store) -> None:
    class Publisher:
        completed = False

        async def __call__(self, event: CloudEvent) -> None:
            assert event.get_id() == "event with internal spaces"
            await asyncio.sleep(0)
            self.completed = True

    publisher = Publisher()
    app = asgi_application(
        store,
        interceptors=[
            RecordStoreInterceptor(
                Options("urn:test:store", lambda: "event with internal spaces", publisher)
            )
        ],
    )
    status, body = await call_asgi(
        app, "PutWorkflow", pb.PutWorkflowRequest(record=records()["Workflow"])
    )
    assert status == 200, body
    assert publisher.completed


async def test_cancellation_after_commit_remains_task_cancellation(store) -> None:
    async def publish(_event: CloudEvent) -> None:
        raise asyncio.CancelledError

    interceptor = RecordStoreInterceptor(Options("urn:test:store", lambda: "event", publish))
    request = pb.PutWorkflowRequest(record=records()["Workflow"])
    ctx = RequestContext(
        method=MethodInfo(
            name="PutWorkflow",
            service_name="temporaless.v1.RecordStoreService",
            input=pb.PutWorkflowRequest,
            output=pb.PutWorkflowResponse,
            idempotency_level=IdempotencyLevel.UNKNOWN,
        ),
        http_method="POST",
        request_headers=Headers(),
    )

    async def handler(message, _ctx):
        await store.put_workflow(message.record)
        return pb.PutWorkflowResponse()

    with pytest.raises(asyncio.CancelledError):
        await interceptor.intercept_unary(handler, request, ctx)
    assert await store.get_workflow(workflow_key_from_proto(request.record.key)) == request.record


async def call_asgi(app, method: str, request: Message) -> tuple[int, bytes]:
    body = request.SerializeToString(deterministic=True)
    path = f"/temporaless.v1.RecordStoreService/{method}"
    request_pending = True
    sent: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        nonlocal request_pending
        if request_pending:
            request_pending = False
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    await app(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [
                (b"content-type", b"application/proto"),
                (b"connect-protocol-version", b"1"),
            ],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        },
        receive,
        send,
    )
    start = next(message for message in sent if message["type"] == "http.response.start")
    response_body = b"".join(
        cast(bytes, message.get("body", b""))
        for message in sent
        if message["type"] == "http.response.body"
    )
    return cast(int, start["status"]), response_body
