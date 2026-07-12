from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Protocol

from connectrpc.code import Code
from connectrpc.codec import Codec
from connectrpc.compression import Compression
from connectrpc.errors import ConnectError
from connectrpc.interceptor import Interceptor
from connectrpc.request import RequestContext
from google.protobuf.duration_pb2 import Duration
from google.protobuf.message import DecodeError
from google.protobuf.timestamp_pb2 import Timestamp
from protovalidate import ValidationError

from temporaless.storage import (
    NO_CLAIMS,
    ActivityKey,
    ClaimKey,
    ClaimRunListingUnsupportedError,
    ClaimRunStore,
    ClaimStore,
    DueTimer,
    EventKey,
    QueryStore,
    RunRecordValidationError,
    Store,
    TimerKey,
    WorkflowKey,
    _activity_keys_for_run,
    _claim_keys_for_run,
    _event_keys_for_run,
    _timer_keys_for_run,
    activity_key_from_proto,
    claim_key_from_proto,
    event_key_from_proto,
    timer_key_from_proto,
    workflow_key_from_proto,
)
from temporaless.v1 import temporaless_connect, temporaless_pb2


class RecordStoreClient(Protocol):
    async def get_store_capabilities(
        self, request: temporaless_pb2.GetStoreCapabilitiesRequest
    ) -> temporaless_pb2.GetStoreCapabilitiesResponse: ...

    async def get_activity(
        self, request: temporaless_pb2.GetActivityRequest
    ) -> temporaless_pb2.GetActivityResponse: ...

    async def put_activity(
        self, request: temporaless_pb2.PutActivityRequest
    ) -> temporaless_pb2.PutActivityResponse: ...

    async def get_workflow(
        self, request: temporaless_pb2.GetWorkflowRequest
    ) -> temporaless_pb2.GetWorkflowResponse: ...

    async def put_workflow(
        self, request: temporaless_pb2.PutWorkflowRequest
    ) -> temporaless_pb2.PutWorkflowResponse: ...

    async def get_latest_workflow_run(
        self, request: temporaless_pb2.GetLatestWorkflowRunRequest
    ) -> temporaless_pb2.GetLatestWorkflowRunResponse: ...

    async def get_timer(
        self, request: temporaless_pb2.GetTimerRequest
    ) -> temporaless_pb2.GetTimerResponse: ...

    async def put_timer(
        self, request: temporaless_pb2.PutTimerRequest
    ) -> temporaless_pb2.PutTimerResponse: ...

    async def get_claim(
        self, request: temporaless_pb2.GetClaimRequest
    ) -> temporaless_pb2.GetClaimResponse: ...

    async def try_create_claim(
        self, request: temporaless_pb2.TryCreateClaimRequest
    ) -> temporaless_pb2.TryCreateClaimResponse: ...

    async def delete_claim(
        self, request: temporaless_pb2.DeleteClaimRequest
    ) -> temporaless_pb2.DeleteClaimResponse: ...

    async def get_event(
        self, request: temporaless_pb2.GetEventRequest
    ) -> temporaless_pb2.GetEventResponse: ...

    async def put_event(
        self, request: temporaless_pb2.PutEventRequest
    ) -> temporaless_pb2.PutEventResponse: ...

    async def list_activities(
        self, request: temporaless_pb2.ListActivitiesRequest
    ) -> temporaless_pb2.ListActivitiesResponse: ...

    async def list_timers(
        self, request: temporaless_pb2.ListTimersRequest
    ) -> temporaless_pb2.ListTimersResponse: ...

    async def list_events(
        self, request: temporaless_pb2.ListEventsRequest
    ) -> temporaless_pb2.ListEventsResponse: ...

    async def list_claims(
        self, request: temporaless_pb2.ListClaimsRequest
    ) -> temporaless_pb2.ListClaimsResponse: ...

    async def delete_workflow(
        self, request: temporaless_pb2.DeleteWorkflowRequest
    ) -> temporaless_pb2.DeleteWorkflowResponse: ...

    async def delete_activity(
        self, request: temporaless_pb2.DeleteActivityRequest
    ) -> temporaless_pb2.DeleteActivityResponse: ...

    async def delete_timer(
        self, request: temporaless_pb2.DeleteTimerRequest
    ) -> temporaless_pb2.DeleteTimerResponse: ...

    async def delete_event(
        self, request: temporaless_pb2.DeleteEventRequest
    ) -> temporaless_pb2.DeleteEventResponse: ...

    async def delete_run(
        self, request: temporaless_pb2.DeleteRunRequest
    ) -> temporaless_pb2.DeleteRunResponse: ...

    async def due_timers(
        self, request: temporaless_pb2.DueTimersRequest
    ) -> temporaless_pb2.DueTimersResponse: ...


class RecordQueryClient(Protocol):
    async def list_workflows(
        self, request: temporaless_pb2.ListWorkflowsRequest
    ) -> temporaless_pb2.ListWorkflowsResponse: ...

    async def list_activities(
        self, request: temporaless_pb2.RecordQueryServiceListActivitiesRequest
    ) -> temporaless_pb2.RecordQueryServiceListActivitiesResponse: ...

    async def sweep(
        self, request: temporaless_pb2.SweepRequest
    ) -> temporaless_pb2.SweepResponse: ...

    async def due_timers(
        self, request: temporaless_pb2.RecordQueryServiceDueTimersRequest
    ) -> temporaless_pb2.RecordQueryServiceDueTimersResponse: ...


class ConnectStore:
    def __init__(self, client: RecordStoreClient) -> None:
        self._client = client

    @classmethod
    def local(cls, store: Store, claim_store: ClaimStore | None = None) -> ConnectStore:
        """Construct a store client that calls RecordStoreService in-process.

        This preserves the generated protobuf request/response contract without
        requiring an HTTP/ConnectRPC hop for local deployments.
        """
        return cls(LocalRecordStoreClient(RecordStoreService(store, claim_store)))

    @classmethod
    def from_address(
        cls,
        address: str,
        *,
        interceptors: Iterable[Interceptor] = (),
        timeout_ms: int | None = None,
        read_max_bytes: int | None = None,
    ) -> ConnectStore:
        """Construct a ConnectStore that talks to ``address`` via the async
        ``RecordStoreServiceClient``. Forwards the standard ConnectRPC client
        knobs — pass ``interceptors=[auth, retry, logging]`` to plug into
        existing gRPC/ConnectRPC infrastructure. For knobs not surfaced here
        (codec, compression, custom HTTP client), construct
        ``RecordStoreServiceClient`` directly and pass the result to
        ``ConnectStore(...)``.
        """
        return cls(
            temporaless_connect.RecordStoreServiceClient(
                address,
                interceptors=tuple(interceptors),
                timeout_ms=timeout_ms,
                read_max_bytes=read_max_bytes,
            )
        )

    async def claim_capability(self) -> temporaless_pb2.ClaimCapability:
        response = await self._client.get_store_capabilities(
            temporaless_pb2.GetStoreCapabilitiesRequest()
        )
        return response.claim_capability or NO_CLAIMS

    async def get_activity(self, key: ActivityKey) -> temporaless_pb2.ActivityRecord | None:
        response = await self._client.get_activity(
            temporaless_pb2.GetActivityRequest(key=key.to_proto())
        )
        if not response.found:
            return None
        return response.record

    async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None:
        await self._client.put_activity(temporaless_pb2.PutActivityRequest(record=record))

    async def get_workflow(self, key: WorkflowKey) -> temporaless_pb2.WorkflowRecord | None:
        response = await self._client.get_workflow(
            temporaless_pb2.GetWorkflowRequest(key=key.to_proto())
        )
        if not response.found:
            return None
        return response.record

    async def put_workflow(self, record: temporaless_pb2.WorkflowRecord) -> None:
        await self._client.put_workflow(temporaless_pb2.PutWorkflowRequest(record=record))

    async def get_latest_workflow_run(
        self, namespace: str, workflow_id: str
    ) -> temporaless_pb2.LatestWorkflowRunPointer | None:
        response = await self._client.get_latest_workflow_run(
            temporaless_pb2.GetLatestWorkflowRunRequest(
                namespace=namespace, workflow_id=workflow_id
            )
        )
        if not response.found:
            return None
        return response.pointer

    async def get_timer(self, key: TimerKey) -> temporaless_pb2.TimerRecord | None:
        response = await self._client.get_timer(temporaless_pb2.GetTimerRequest(key=key.to_proto()))
        if not response.found:
            return None
        return response.record

    async def put_timer(self, record: temporaless_pb2.TimerRecord) -> None:
        await self._client.put_timer(temporaless_pb2.PutTimerRequest(record=record))

    async def get_claim(self, key: ClaimKey) -> temporaless_pb2.ClaimRecord | None:
        response = await self._client.get_claim(temporaless_pb2.GetClaimRequest(key=key.to_proto()))
        if not response.found:
            return None
        return response.record

    async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
        response = await self._client.try_create_claim(
            temporaless_pb2.TryCreateClaimRequest(record=record)
        )
        return response.created

    async def delete_claim(self, key: ClaimKey) -> bool:
        response = await self._client.delete_claim(
            temporaless_pb2.DeleteClaimRequest(key=key.to_proto())
        )
        return response.deleted

    async def get_event(self, key: EventKey) -> temporaless_pb2.EventRecord | None:
        response = await self._client.get_event(temporaless_pb2.GetEventRequest(key=key.to_proto()))
        if not response.found:
            return None
        return response.record

    async def put_event(self, record: temporaless_pb2.EventRecord) -> None:
        await self._client.put_event(temporaless_pb2.PutEventRequest(record=record))

    async def list_activities(self, key: WorkflowKey) -> list[temporaless_pb2.ActivityRecord]:
        response = await self._client.list_activities(
            temporaless_pb2.ListActivitiesRequest(key=key.to_proto())
        )
        return list(response.records)

    async def list_timers(
        self,
        key: WorkflowKey,
        status: temporaless_pb2.TimerStatus,
    ) -> list[temporaless_pb2.TimerRecord]:
        response = await self._client.list_timers(
            temporaless_pb2.ListTimersRequest(key=key.to_proto(), status=status)
        )
        return list(response.records)

    async def list_events(self, key: WorkflowKey) -> list[temporaless_pb2.EventRecord]:
        response = await self._client.list_events(
            temporaless_pb2.ListEventsRequest(key=key.to_proto())
        )
        return list(response.records)

    async def list_claims(self, key: WorkflowKey) -> list[temporaless_pb2.ClaimRecord]:
        response = await self._client.list_claims(
            temporaless_pb2.ListClaimsRequest(key=key.to_proto())
        )
        return list(response.records)

    async def delete_workflow(self, key: WorkflowKey) -> bool:
        response = await self._client.delete_workflow(
            temporaless_pb2.DeleteWorkflowRequest(key=key.to_proto())
        )
        return response.deleted

    async def delete_activity(self, key: ActivityKey) -> bool:
        response = await self._client.delete_activity(
            temporaless_pb2.DeleteActivityRequest(key=key.to_proto())
        )
        return response.deleted

    async def delete_timer(self, key: TimerKey) -> bool:
        response = await self._client.delete_timer(
            temporaless_pb2.DeleteTimerRequest(key=key.to_proto())
        )
        return response.deleted

    async def delete_event(self, key: EventKey) -> bool:
        response = await self._client.delete_event(
            temporaless_pb2.DeleteEventRequest(key=key.to_proto())
        )
        return response.deleted

    async def delete_run(self, key: WorkflowKey) -> int:
        response = await self._client.delete_run(
            temporaless_pb2.DeleteRunRequest(key=key.to_proto())
        )
        return response.deleted

    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        now_ts = Timestamp()
        now_ts.FromDatetime(now)
        response = await self._client.due_timers(
            temporaless_pb2.DueTimersRequest(namespace=namespace, now=now_ts)
        )
        return [
            DueTimer(
                key=timer_key_from_proto(entry.key),
                record=entry.record,
                workflow=entry.workflow,
            )
            for entry in response.due
        ]


class ConnectQueryStore:
    def __init__(self, client: RecordQueryClient) -> None:
        self._client = client

    @classmethod
    def local(cls, query: QueryStore) -> ConnectQueryStore:
        """Construct a query client that calls RecordQueryService in-process."""
        return cls(LocalRecordQueryClient(RecordQueryService(query)))

    @classmethod
    def from_address(
        cls,
        address: str,
        *,
        interceptors: Iterable[Interceptor] = (),
        timeout_ms: int | None = None,
        read_max_bytes: int | None = None,
    ) -> ConnectQueryStore:
        return cls(
            temporaless_connect.RecordQueryServiceClient(
                address,
                interceptors=tuple(interceptors),
                timeout_ms=timeout_ms,
                read_max_bytes=read_max_bytes,
            )
        )

    async def list_workflows(
        self,
        namespace: str,
        workflow_id: str,
        status: temporaless_pb2.WorkflowStatus,
        *,
        order_by: str = "",
        page_size: int = 0,
        page_token: str = "",
    ) -> tuple[list[temporaless_pb2.WorkflowRecord], str]:
        response = await self._client.list_workflows(
            temporaless_pb2.ListWorkflowsRequest(
                namespace=namespace,
                workflow_id=workflow_id,
                status=status,
                order_by=order_by,
                page_size=page_size,
                page_token=page_token,
            )
        )
        return list(response.records), response.next_page_token

    async def list_activities_query(
        self,
        namespace: str,
        workflow_id: str,
        run_id: str,
        status: temporaless_pb2.ActivityStatus,
        *,
        order_by: str = "",
        page_size: int = 0,
        page_token: str = "",
    ) -> tuple[list[temporaless_pb2.ActivityRecord], str]:
        response = await self._client.list_activities(
            temporaless_pb2.RecordQueryServiceListActivitiesRequest(
                namespace=namespace,
                workflow_id=workflow_id,
                run_id=run_id,
                status=status,
                order_by=order_by,
                page_size=page_size,
                page_token=page_token,
            )
        )
        return list(response.records), response.next_page_token

    async def sweep(self, namespace: str, now: datetime, max_age: timedelta) -> int:
        now_ts = Timestamp()
        now_ts.FromDatetime(now)
        max_age_pb = Duration()
        max_age_pb.FromTimedelta(max_age)
        response = await self._client.sweep(
            temporaless_pb2.SweepRequest(namespace=namespace, now=now_ts, max_age=max_age_pb)
        )
        return response.deleted

    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        now_ts = Timestamp()
        now_ts.FromDatetime(now)
        response = await self._client.due_timers(
            temporaless_pb2.RecordQueryServiceDueTimersRequest(namespace=namespace, now=now_ts)
        )
        return [
            DueTimer(
                key=timer_key_from_proto(entry.key),
                record=entry.record,
                workflow=entry.workflow,
            )
            for entry in response.due
        ]


class RecordStoreService:
    _claim_store: ClaimStore | None

    def __init__(self, store: Store, claim_store: ClaimStore | None = None) -> None:
        self._store = store
        if claim_store is not None:
            self._claim_store = claim_store
        elif isinstance(store, ClaimStore):
            self._claim_store = store
        else:
            self._claim_store = None

    async def get_store_capabilities(
        self,
        request: temporaless_pb2.GetStoreCapabilitiesRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.GetStoreCapabilitiesResponse:
        capability = NO_CLAIMS
        if self._claim_store is not None:
            capability = await self._claim_store.claim_capability()
        return temporaless_pb2.GetStoreCapabilitiesResponse(claim_capability=capability)

    async def get_activity(
        self,
        request: temporaless_pb2.GetActivityRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.GetActivityResponse:
        record = await self._store.get_activity(activity_key_from_proto(request.key))
        if record is None:
            return temporaless_pb2.GetActivityResponse(found=False)
        return temporaless_pb2.GetActivityResponse(found=True, record=record)

    async def put_activity(
        self,
        request: temporaless_pb2.PutActivityRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.PutActivityResponse:
        await self._store.put_activity(request.record)
        return temporaless_pb2.PutActivityResponse()

    async def get_workflow(
        self,
        request: temporaless_pb2.GetWorkflowRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.GetWorkflowResponse:
        record = await self._store.get_workflow(workflow_key_from_proto(request.key))
        if record is None:
            return temporaless_pb2.GetWorkflowResponse(found=False)
        return temporaless_pb2.GetWorkflowResponse(found=True, record=record)

    async def put_workflow(
        self,
        request: temporaless_pb2.PutWorkflowRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.PutWorkflowResponse:
        await self._store.put_workflow(request.record)
        return temporaless_pb2.PutWorkflowResponse()

    async def get_latest_workflow_run(
        self,
        request: temporaless_pb2.GetLatestWorkflowRunRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.GetLatestWorkflowRunResponse:
        pointer = await self._store.get_latest_workflow_run(request.namespace, request.workflow_id)
        if pointer is None:
            return temporaless_pb2.GetLatestWorkflowRunResponse(found=False)
        return temporaless_pb2.GetLatestWorkflowRunResponse(found=True, pointer=pointer)

    async def get_timer(
        self,
        request: temporaless_pb2.GetTimerRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.GetTimerResponse:
        record = await self._store.get_timer(timer_key_from_proto(request.key))
        if record is None:
            return temporaless_pb2.GetTimerResponse(found=False)
        return temporaless_pb2.GetTimerResponse(found=True, record=record)

    async def put_timer(
        self,
        request: temporaless_pb2.PutTimerRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.PutTimerResponse:
        await self._store.put_timer(request.record)
        return temporaless_pb2.PutTimerResponse()

    async def get_claim(
        self,
        request: temporaless_pb2.GetClaimRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.GetClaimResponse:
        if self._claim_store is None:
            raise ConnectError(Code.FAILED_PRECONDITION, "claim store is required")
        record = await self._claim_store.get_claim(claim_key_from_proto(request.key))
        if record is None:
            return temporaless_pb2.GetClaimResponse(found=False)
        return temporaless_pb2.GetClaimResponse(found=True, record=record)

    async def try_create_claim(
        self,
        request: temporaless_pb2.TryCreateClaimRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.TryCreateClaimResponse:
        if self._claim_store is None:
            raise ConnectError(Code.FAILED_PRECONDITION, "claim store is required")
        return temporaless_pb2.TryCreateClaimResponse(
            created=await self._claim_store.try_create_claim(request.record)
        )

    async def delete_claim(
        self,
        request: temporaless_pb2.DeleteClaimRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.DeleteClaimResponse:
        if self._claim_store is None:
            raise ConnectError(Code.FAILED_PRECONDITION, "claim store is required")
        return temporaless_pb2.DeleteClaimResponse(
            deleted=await self._claim_store.delete_claim(claim_key_from_proto(request.key))
        )

    async def get_event(
        self,
        request: temporaless_pb2.GetEventRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.GetEventResponse:
        record = await self._store.get_event(event_key_from_proto(request.key))
        if record is None:
            return temporaless_pb2.GetEventResponse(found=False)
        return temporaless_pb2.GetEventResponse(found=True, record=record)

    async def put_event(
        self,
        request: temporaless_pb2.PutEventRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.PutEventResponse:
        await self._store.put_event(request.record)
        return temporaless_pb2.PutEventResponse()

    async def list_activities(
        self,
        request: temporaless_pb2.ListActivitiesRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.ListActivitiesResponse:
        records = await self._store.list_activities(workflow_key_from_proto(request.key))
        return temporaless_pb2.ListActivitiesResponse(records=records)

    async def list_timers(
        self,
        request: temporaless_pb2.ListTimersRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.ListTimersResponse:
        records = await self._store.list_timers(
            workflow_key_from_proto(request.key), request.status
        )
        return temporaless_pb2.ListTimersResponse(records=records)

    async def list_events(
        self,
        request: temporaless_pb2.ListEventsRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.ListEventsResponse:
        records = await self._store.list_events(workflow_key_from_proto(request.key))
        return temporaless_pb2.ListEventsResponse(records=records)

    async def list_claims(
        self,
        request: temporaless_pb2.ListClaimsRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.ListClaimsResponse:
        records = await self._claims_for_run(workflow_key_from_proto(request.key))
        return temporaless_pb2.ListClaimsResponse(records=records)

    async def delete_workflow(
        self,
        request: temporaless_pb2.DeleteWorkflowRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.DeleteWorkflowResponse:
        deleted = await self._store.delete_workflow(workflow_key_from_proto(request.key))
        return temporaless_pb2.DeleteWorkflowResponse(deleted=deleted)

    async def delete_activity(
        self,
        request: temporaless_pb2.DeleteActivityRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.DeleteActivityResponse:
        deleted = await self._store.delete_activity(activity_key_from_proto(request.key))
        return temporaless_pb2.DeleteActivityResponse(deleted=deleted)

    async def delete_timer(
        self,
        request: temporaless_pb2.DeleteTimerRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.DeleteTimerResponse:
        deleted = await self._store.delete_timer(timer_key_from_proto(request.key))
        return temporaless_pb2.DeleteTimerResponse(deleted=deleted)

    async def delete_event(
        self,
        request: temporaless_pb2.DeleteEventRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.DeleteEventResponse:
        deleted = await self._store.delete_event(event_key_from_proto(request.key))
        return temporaless_pb2.DeleteEventResponse(deleted=deleted)

    async def delete_run(
        self,
        request: temporaless_pb2.DeleteRunRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.DeleteRunResponse:
        key = workflow_key_from_proto(request.key)
        deleted = 0
        # A separately configured claim store is authoritative for claims.
        # Preflight every bounded listing before mutating anything, then
        # remove claims first so later record deletion can be retried.
        await self._validate_record_listings_for_run(key)
        claims = await self._claims_for_run(key)
        if self._claim_store is not None:
            for claim in claims:
                if await self._claim_store.delete_claim(claim_key_from_proto(claim.key)):
                    deleted += 1
        try:
            deleted += await self._store.delete_run(key)
        except (DecodeError, ValidationError, ValueError) as exc:
            raise ConnectError(Code.DATA_LOSS, f"invalid record in run deletion: {exc}") from exc
        return temporaless_pb2.DeleteRunResponse(deleted=deleted)

    async def _validate_record_listings_for_run(self, key: WorkflowKey) -> None:
        try:
            activities = await self._store.list_activities(key)
            timers = await self._store.list_timers(key, temporaless_pb2.TIMER_STATUS_UNSPECIFIED)
            events = await self._store.list_events(key)
            _activity_keys_for_run(key, activities)
            _timer_keys_for_run(key, timers)
            _event_keys_for_run(key, events)
        except (DecodeError, ValidationError, ValueError) as exc:
            raise ConnectError(Code.DATA_LOSS, f"invalid record key in run listing: {exc}") from exc

    async def _claims_for_run(self, key: WorkflowKey) -> list[temporaless_pb2.ClaimRecord]:
        if self._claim_store is None:
            return []
        capability = await self._claim_store.claim_capability()
        if capability not in (
            temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS,
            temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS,
        ):
            return []
        if not isinstance(self._claim_store, ClaimRunStore):
            raise ConnectError(
                Code.FAILED_PRECONDITION,
                "claim store does not support run-scoped claim listing",
            )
        try:
            records = await self._claim_store.list_claims(key)
            _claim_keys_for_run(key, records)
        except TypeError as exc:
            # Structural pass-through adapters expose list_claims even when
            # their wrapped create-only claim store lacks ClaimRunStore.
            raise ConnectError(
                Code.FAILED_PRECONDITION,
                "claim store does not support run-scoped claim listing",
            ) from exc
        except (DecodeError, ValidationError, ValueError) as exc:
            raise ConnectError(Code.DATA_LOSS, f"invalid claim key in run listing: {exc}") from exc
        return records

    async def due_timers(
        self,
        request: temporaless_pb2.DueTimersRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.DueTimersResponse:
        now = request.now.ToDatetime()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        due = await self._store.due_timers(request.namespace, now)
        return temporaless_pb2.DueTimersResponse(
            due=[
                temporaless_pb2.DueTimer(
                    key=entry.key.to_proto(),
                    record=entry.record,
                    workflow=entry.workflow,
                )
                for entry in due
            ]
        )


class RecordQueryService:
    def __init__(self, query: QueryStore) -> None:
        self._query = query

    async def list_workflows(
        self,
        request: temporaless_pb2.ListWorkflowsRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.ListWorkflowsResponse:
        records, next_page_token = await self._query.list_workflows(
            request.namespace,
            request.workflow_id,
            request.status,
            order_by=request.order_by,
            page_size=request.page_size,
            page_token=request.page_token,
        )
        return temporaless_pb2.ListWorkflowsResponse(
            records=records, next_page_token=next_page_token
        )

    async def list_activities(
        self,
        request: temporaless_pb2.RecordQueryServiceListActivitiesRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.RecordQueryServiceListActivitiesResponse:
        records, next_page_token = await self._query.list_activities_query(
            request.namespace,
            request.workflow_id,
            request.run_id,
            request.status,
            order_by=request.order_by,
            page_size=request.page_size,
            page_token=request.page_token,
        )
        return temporaless_pb2.RecordQueryServiceListActivitiesResponse(
            records=records, next_page_token=next_page_token
        )

    async def sweep(
        self,
        request: temporaless_pb2.SweepRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.SweepResponse:
        now = request.now.ToDatetime()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        max_age = request.max_age.ToTimedelta()
        try:
            deleted = await self._query.sweep(request.namespace, now, max_age)
        except ClaimRunListingUnsupportedError as exc:
            raise ConnectError(
                Code.FAILED_PRECONDITION,
                "claim store does not support run-scoped claim listing",
            ) from exc
        except (DecodeError, ValidationError, RunRecordValidationError) as exc:
            raise ConnectError(Code.DATA_LOSS, f"invalid record in retention sweep: {exc}") from exc
        return temporaless_pb2.SweepResponse(deleted=deleted)

    async def due_timers(
        self,
        request: temporaless_pb2.RecordQueryServiceDueTimersRequest,
        ctx: RequestContext | None,
    ) -> temporaless_pb2.RecordQueryServiceDueTimersResponse:
        now = request.now.ToDatetime()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        due = await self._query.due_timers(request.namespace, now)
        return temporaless_pb2.RecordQueryServiceDueTimersResponse(
            due=[
                temporaless_pb2.DueTimer(
                    key=entry.key.to_proto(),
                    record=entry.record,
                    workflow=entry.workflow,
                )
                for entry in due
            ]
        )


class LocalRecordStoreClient:
    """In-process RecordStoreService client.

    Local deployments use the same generated request/response messages as a
    remote ConnectRPC deployment, but dispatch directly to the service object.
    """

    def __init__(self, service: RecordStoreService) -> None:
        self.service = service

    async def get_store_capabilities(
        self, request: temporaless_pb2.GetStoreCapabilitiesRequest
    ) -> temporaless_pb2.GetStoreCapabilitiesResponse:
        return await self.service.get_store_capabilities(request, None)

    async def get_workflow(
        self, request: temporaless_pb2.GetWorkflowRequest
    ) -> temporaless_pb2.GetWorkflowResponse:
        return await self.service.get_workflow(request, None)

    async def put_workflow(
        self, request: temporaless_pb2.PutWorkflowRequest
    ) -> temporaless_pb2.PutWorkflowResponse:
        return await self.service.put_workflow(request, None)

    async def get_latest_workflow_run(
        self, request: temporaless_pb2.GetLatestWorkflowRunRequest
    ) -> temporaless_pb2.GetLatestWorkflowRunResponse:
        return await self.service.get_latest_workflow_run(request, None)

    async def get_activity(
        self, request: temporaless_pb2.GetActivityRequest
    ) -> temporaless_pb2.GetActivityResponse:
        return await self.service.get_activity(request, None)

    async def put_activity(
        self, request: temporaless_pb2.PutActivityRequest
    ) -> temporaless_pb2.PutActivityResponse:
        return await self.service.put_activity(request, None)

    async def get_timer(
        self, request: temporaless_pb2.GetTimerRequest
    ) -> temporaless_pb2.GetTimerResponse:
        return await self.service.get_timer(request, None)

    async def put_timer(
        self, request: temporaless_pb2.PutTimerRequest
    ) -> temporaless_pb2.PutTimerResponse:
        return await self.service.put_timer(request, None)

    async def get_claim(
        self, request: temporaless_pb2.GetClaimRequest
    ) -> temporaless_pb2.GetClaimResponse:
        return await self.service.get_claim(request, None)

    async def try_create_claim(
        self, request: temporaless_pb2.TryCreateClaimRequest
    ) -> temporaless_pb2.TryCreateClaimResponse:
        return await self.service.try_create_claim(request, None)

    async def delete_claim(
        self, request: temporaless_pb2.DeleteClaimRequest
    ) -> temporaless_pb2.DeleteClaimResponse:
        return await self.service.delete_claim(request, None)

    async def get_event(
        self, request: temporaless_pb2.GetEventRequest
    ) -> temporaless_pb2.GetEventResponse:
        return await self.service.get_event(request, None)

    async def put_event(
        self, request: temporaless_pb2.PutEventRequest
    ) -> temporaless_pb2.PutEventResponse:
        return await self.service.put_event(request, None)

    async def list_activities(
        self, request: temporaless_pb2.ListActivitiesRequest
    ) -> temporaless_pb2.ListActivitiesResponse:
        return await self.service.list_activities(request, None)

    async def list_timers(
        self, request: temporaless_pb2.ListTimersRequest
    ) -> temporaless_pb2.ListTimersResponse:
        return await self.service.list_timers(request, None)

    async def list_events(
        self, request: temporaless_pb2.ListEventsRequest
    ) -> temporaless_pb2.ListEventsResponse:
        return await self.service.list_events(request, None)

    async def list_claims(
        self, request: temporaless_pb2.ListClaimsRequest
    ) -> temporaless_pb2.ListClaimsResponse:
        return await self.service.list_claims(request, None)

    async def delete_workflow(
        self, request: temporaless_pb2.DeleteWorkflowRequest
    ) -> temporaless_pb2.DeleteWorkflowResponse:
        return await self.service.delete_workflow(request, None)

    async def delete_activity(
        self, request: temporaless_pb2.DeleteActivityRequest
    ) -> temporaless_pb2.DeleteActivityResponse:
        return await self.service.delete_activity(request, None)

    async def delete_timer(
        self, request: temporaless_pb2.DeleteTimerRequest
    ) -> temporaless_pb2.DeleteTimerResponse:
        return await self.service.delete_timer(request, None)

    async def delete_event(
        self, request: temporaless_pb2.DeleteEventRequest
    ) -> temporaless_pb2.DeleteEventResponse:
        return await self.service.delete_event(request, None)

    async def delete_run(
        self, request: temporaless_pb2.DeleteRunRequest
    ) -> temporaless_pb2.DeleteRunResponse:
        return await self.service.delete_run(request, None)

    async def due_timers(
        self, request: temporaless_pb2.DueTimersRequest
    ) -> temporaless_pb2.DueTimersResponse:
        return await self.service.due_timers(request, None)


class LocalRecordQueryClient:
    """In-process RecordQueryService client."""

    def __init__(self, service: RecordQueryService) -> None:
        self.service = service

    async def list_workflows(
        self, request: temporaless_pb2.ListWorkflowsRequest
    ) -> temporaless_pb2.ListWorkflowsResponse:
        return await self.service.list_workflows(request, None)

    async def list_activities(
        self, request: temporaless_pb2.RecordQueryServiceListActivitiesRequest
    ) -> temporaless_pb2.RecordQueryServiceListActivitiesResponse:
        return await self.service.list_activities(request, None)

    async def sweep(self, request: temporaless_pb2.SweepRequest) -> temporaless_pb2.SweepResponse:
        return await self.service.sweep(request, None)

    async def due_timers(
        self, request: temporaless_pb2.RecordQueryServiceDueTimersRequest
    ) -> temporaless_pb2.RecordQueryServiceDueTimersResponse:
        return await self.service.due_timers(request, None)


def asgi_application(
    store: Store,
    claim_store: ClaimStore | None = None,
    *,
    interceptors: Iterable[Interceptor] = (),
    read_max_bytes: int | None = None,
    compressions: Iterable[Compression] | None = None,
    codecs: Iterable[Codec] | None = None,
) -> temporaless_connect.RecordStoreServiceASGIApplication:
    """Mountable ASGI app that exposes ``RecordStoreService`` over ConnectRPC.

    Forwards the standard ConnectRPC server knobs. Pass ``interceptors=[auth,
    rate_limit, logging]`` to plug into existing gRPC/ConnectRPC middleware —
    every storage RPC (Get/Put/List/Delete) flows through them.
    """
    return temporaless_connect.RecordStoreServiceASGIApplication(
        RecordStoreService(store, claim_store),
        interceptors=tuple(interceptors),
        read_max_bytes=read_max_bytes,
        compressions=compressions,
        codecs=codecs,
    )


def query_asgi_application(
    query: QueryStore,
    *,
    interceptors: Iterable[Interceptor] = (),
    read_max_bytes: int | None = None,
    compressions: Iterable[Compression] | None = None,
    codecs: Iterable[Codec] | None = None,
) -> temporaless_connect.RecordQueryServiceASGIApplication:
    """Mountable ASGI app that exposes optional RecordQueryService over ConnectRPC."""
    return temporaless_connect.RecordQueryServiceASGIApplication(
        RecordQueryService(query),
        interceptors=tuple(interceptors),
        read_max_bytes=read_max_bytes,
        compressions=compressions,
        codecs=codecs,
    )
