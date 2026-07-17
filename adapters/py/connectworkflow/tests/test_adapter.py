from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

import opendal
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from google.protobuf.wrappers_pb2 import StringValue
from temporaless.backfill import backfill
from temporaless.storage import OpenDALStore
from temporaless.v1 import temporaless_connect, temporaless_pb2
from temporaless.workflow import (
    ActivityConflictError,
    ActivityError,
    ActivityOptions,
    ClaimBusyError,
    ClaimCapabilityError,
    ClaimReleaseError,
    ConcurrencyBusyError,
    EventPendingError,
    Options,
    TimerConflictError,
    TimerPendingError,
    WorkflowConflictError,
    WorkflowDependencyFailedError,
    WorkflowDependencyPendingError,
    WorkflowInfrastructureError,
    current_workflow,
)

from temporaless_connectworkflow import (
    WorkflowMethodWrapOptions,
    error_to_connect_code,
    is_pending_error,
    wrap_workflow_method,
)


@pytest.fixture
def store(tmp_path) -> OpenDALStore:
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


async def test_wrap_workflow_method_replays_connect_handler(store: OpenDALStore) -> None:
    class PriceService:
        def __init__(self, workflow_store: OpenDALStore) -> None:
            self._store = workflow_store
            self.vendor_calls = 0

        @wrap_workflow_method(
            WorkflowMethodWrapOptions(
                store=lambda self: self._store,  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                result_type=StringValue,
                options_for=lambda _self, request: Options(
                    workflow_id=f"prices:{request.value}",
                    run_id="2026-05-04",
                    code_version="v1",
                ),
            )
        )
        async def fetch_prices(self, request: StringValue, ctx: object = None) -> StringValue:
            async def vendor(req: StringValue) -> StringValue:
                self.vendor_calls += 1
                return StringValue(value=f"vendor:{req.value}")

            return await current_workflow().execute_activity(
                ActivityOptions(activity_id=f"fetch:{request.value}"),
                request,
                StringValue,
                vendor,
            )

    service = PriceService(store)
    first = await service.fetch_prices(StringValue(value="AAPL"))
    second = await service.fetch_prices(StringValue(value="AAPL"))

    assert first.value == "vendor:AAPL"
    assert second.value == "vendor:AAPL"
    assert service.vendor_calls == 1


async def test_generated_asgi_service_executes_and_replays_workflow(
    store: OpenDALStore,
) -> None:
    """Prove the decorator on a generated service method across real ASGI
    Connect framing, not only through a direct Python method call.
    """

    class WorkflowQueryService(temporaless_connect.RecordQueryService):
        def __init__(self, workflow_store: OpenDALStore) -> None:
            self._store = workflow_store
            self.body_calls = 0

        @wrap_workflow_method(
            WorkflowMethodWrapOptions(
                store=lambda self: self._store,  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                result_type=temporaless_pb2.ListWorkflowsResponse,
                options_for=lambda _self, request: Options(
                    workflow_id=f"query:{request.workflow_id}",
                    run_id=request.page_token,
                    code_version="v1",
                    claim_owner_id="connect-asgi-test",
                ),
            )
        )
        async def list_workflows(
            self,
            request: temporaless_pb2.ListWorkflowsRequest,
            _ctx: object = None,
        ) -> temporaless_pb2.ListWorkflowsResponse:
            self.body_calls += 1
            return temporaless_pb2.ListWorkflowsResponse(
                next_page_token=f"seen:{request.namespace}"
            )

    service = WorkflowQueryService(store)
    app = temporaless_connect.RecordQueryServiceASGIApplication(service)
    request = temporaless_pb2.ListWorkflowsRequest(
        namespace="tenant",
        workflow_id="prices",
        page_token="run-1",
    )

    responses = [
        await _call_connect_asgi(
            app,
            "/temporaless.v1.RecordQueryService/ListWorkflows",
            request.SerializeToString(deterministic=True),
        )
        for _ in range(2)
    ]

    for status, body in responses:
        assert status == 200
        response = temporaless_pb2.ListWorkflowsResponse()
        response.ParseFromString(body)
        assert response.next_page_token == "seen:tenant"
    assert service.body_calls == 1


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (TimerPendingError("timer", datetime.now(UTC)), Code.UNAVAILABLE),
        (EventPendingError("event"), Code.UNAVAILABLE),
        (WorkflowDependencyPendingError("workflow", "run"), Code.UNAVAILABLE),
        (WorkflowInfrastructureError("read timer", OSError("backend")), Code.UNAVAILABLE),
        (ClaimBusyError("activity:fetch"), Code.ALREADY_EXISTS),
        (ConcurrencyBusyError("vendor", 2), Code.RESOURCE_EXHAUSTED),
        (ClaimReleaseError("workflow claim", OSError("backend")), Code.INTERNAL),
        (
            ClaimCapabilityError(
                temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS,
                "claim_owner_id",
            ),
            Code.FAILED_PRECONDITION,
        ),
        (WorkflowConflictError("workflow changed"), Code.FAILED_PRECONDITION),
        (ActivityConflictError("activity changed"), Code.FAILED_PRECONDITION),
        (TimerConflictError("timer changed"), Code.FAILED_PRECONDITION),
        (ActivityError("vendor", "failed"), Code.INTERNAL),
        (
            WorkflowDependencyFailedError(
                "workflow",
                "run",
                temporaless_pb2.WORKFLOW_STATUS_FAILED,
            ),
            Code.INTERNAL,
        ),
    ],
)
def test_error_to_connect_code(error: BaseException, expected: Code) -> None:
    mapping = error_to_connect_code(error)
    assert mapping is not None
    code, message = mapping
    assert code is expected
    assert message


def test_error_to_connect_code_leaves_application_error_unmapped() -> None:
    assert error_to_connect_code(ValueError("application")) is None


async def test_wrapper_maps_pending_error_and_preserves_cause(store: OpenDALStore) -> None:
    class SleepingService:
        def __init__(self, workflow_store: OpenDALStore) -> None:
            self._store = workflow_store

        @wrap_workflow_method(
            WorkflowMethodWrapOptions(
                store=lambda self: self._store,  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                result_type=StringValue,
                options_for=lambda _self, request: Options(
                    workflow_id=f"sleep:{request.value}",
                    run_id="2026-05-04",
                    code_version="v1",
                ),
            )
        )
        async def sleep(self, _request: StringValue, _ctx: object = None) -> StringValue:
            await current_workflow().sleep("wait", timedelta(hours=1))
            return StringValue(value="unreachable")

    with pytest.raises(ConnectError) as exc_info:
        await SleepingService(store).sleep(StringValue(value="AAPL"))

    assert exc_info.value.code is Code.UNAVAILABLE
    assert isinstance(exc_info.value.__cause__, TimerPendingError)


async def test_wrapper_preserves_unknown_application_error(store: OpenDALStore) -> None:
    class BrokenService:
        def __init__(self, workflow_store: OpenDALStore) -> None:
            self._store = workflow_store

        @wrap_workflow_method(
            WorkflowMethodWrapOptions(
                store=lambda self: self._store,  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                result_type=StringValue,
                options_for=lambda _self, request: Options(
                    workflow_id=f"broken:{request.value}",
                    run_id="2026-05-04",
                    code_version="v1",
                ),
            )
        )
        async def fail(self, _request: StringValue, _ctx: object = None) -> StringValue:
            raise RuntimeError("application error")

    with pytest.raises(RuntimeError, match="application error"):
        await BrokenService(store).fail(StringValue(value="AAPL"))


def test_wrapper_rejects_sync_method() -> None:
    options = WorkflowMethodWrapOptions(
        store=lambda _service: pytest.fail("not called"),
        result_type=StringValue,
        options_for=lambda _service, _request: Options(),
    )

    with pytest.raises(ValueError, match="must be async"):

        @wrap_workflow_method(options)  # ty: ignore[invalid-argument-type]
        def invalid(_self: object, _request: StringValue) -> StringValue:
            return StringValue()


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (Code.UNAVAILABLE, True),
        (Code.ALREADY_EXISTS, True),
        (Code.RESOURCE_EXHAUSTED, True),
        (Code.INTERNAL, False),
        (Code.FAILED_PRECONDITION, False),
    ],
)
def test_is_pending_error_classifies_connect_status(code: Code, expected: bool) -> None:
    assert is_pending_error(ConnectError(code, "remote")) is expected


async def test_remote_backfill_opts_into_connect_pending_classification() -> None:
    async def invoke(_run_id: str) -> StringValue:
        raise ConnectError(Code.UNAVAILABLE, "remote workflow is sleeping")

    default_report = await backfill(invoke, ["run:default"])
    opted_in_report = await backfill(
        invoke,
        ["run:connect"],
        pending_error=is_pending_error,
    )

    assert len(default_report.failed()) == 1
    assert len(opted_in_report.pending()) == 1


async def _call_connect_asgi(
    app: object,
    path: str,
    body: bytes,
) -> tuple[int, bytes]:
    """Send one Connect unary protobuf request through an ASGI callable."""
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

    await app(  # type: ignore[operator]  # ty: ignore[call-non-callable]
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
