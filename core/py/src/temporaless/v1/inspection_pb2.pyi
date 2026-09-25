import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from temporaless.v1 import temporaless_pb2 as _temporaless_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WakeLedgerState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WAKE_LEDGER_STATE_UNSPECIFIED: _ClassVar[WakeLedgerState]
    WAKE_LEDGER_STATE_AGREES: _ClassVar[WakeLedgerState]
    WAKE_LEDGER_STATE_CANONICAL_MISSING: _ClassVar[WakeLedgerState]
    WAKE_LEDGER_STATE_CANONICAL_DIFFERS: _ClassVar[WakeLedgerState]
    WAKE_LEDGER_STATE_CANONICAL_UNREADABLE: _ClassVar[WakeLedgerState]

class PayloadVisibility(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PAYLOAD_VISIBILITY_UNSPECIFIED: _ClassVar[PayloadVisibility]
    PAYLOAD_VISIBILITY_VISIBLE: _ClassVar[PayloadVisibility]
    PAYLOAD_VISIBILITY_REDACTED: _ClassVar[PayloadVisibility]

class RunHistoryEventKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_HISTORY_EVENT_KIND_UNSPECIFIED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_WORKFLOW_STARTED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_WORKFLOW_COMPLETED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_WORKFLOW_FAILED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_SUCCEEDED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_FAILED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_OPEN: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_ACTIVITY_RETRY_BACKOFF: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_ACTIVITY_COMPLETED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_ACTIVITY_FAILED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_TIMER_SCHEDULED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_TIMER_FIRED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_EVENT_RECEIVED: _ClassVar[RunHistoryEventKind]
    RUN_HISTORY_EVENT_KIND_CLAIM_HELD: _ClassVar[RunHistoryEventKind]

class RunPendingReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_PENDING_REASON_UNSPECIFIED: _ClassVar[RunPendingReason]
    RUN_PENDING_REASON_TERMINAL: _ClassVar[RunPendingReason]
    RUN_PENDING_REASON_OVERDUE_WAKE: _ClassVar[RunPendingReason]
    RUN_PENDING_REASON_RETRYING: _ClassVar[RunPendingReason]
    RUN_PENDING_REASON_STALE_CLAIM: _ClassVar[RunPendingReason]
    RUN_PENDING_REASON_EXECUTING: _ClassVar[RunPendingReason]
    RUN_PENDING_REASON_SLEEPING: _ClassVar[RunPendingReason]
    RUN_PENDING_REASON_POLLING: _ClassVar[RunPendingReason]
    RUN_PENDING_REASON_WAITING_NO_WAKE: _ClassVar[RunPendingReason]
WAKE_LEDGER_STATE_UNSPECIFIED: WakeLedgerState
WAKE_LEDGER_STATE_AGREES: WakeLedgerState
WAKE_LEDGER_STATE_CANONICAL_MISSING: WakeLedgerState
WAKE_LEDGER_STATE_CANONICAL_DIFFERS: WakeLedgerState
WAKE_LEDGER_STATE_CANONICAL_UNREADABLE: WakeLedgerState
PAYLOAD_VISIBILITY_UNSPECIFIED: PayloadVisibility
PAYLOAD_VISIBILITY_VISIBLE: PayloadVisibility
PAYLOAD_VISIBILITY_REDACTED: PayloadVisibility
RUN_HISTORY_EVENT_KIND_UNSPECIFIED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_WORKFLOW_STARTED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_WORKFLOW_COMPLETED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_WORKFLOW_FAILED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_SUCCEEDED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_FAILED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_OPEN: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_ACTIVITY_RETRY_BACKOFF: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_ACTIVITY_COMPLETED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_ACTIVITY_FAILED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_TIMER_SCHEDULED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_TIMER_FIRED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_EVENT_RECEIVED: RunHistoryEventKind
RUN_HISTORY_EVENT_KIND_CLAIM_HELD: RunHistoryEventKind
RUN_PENDING_REASON_UNSPECIFIED: RunPendingReason
RUN_PENDING_REASON_TERMINAL: RunPendingReason
RUN_PENDING_REASON_OVERDUE_WAKE: RunPendingReason
RUN_PENDING_REASON_RETRYING: RunPendingReason
RUN_PENDING_REASON_STALE_CLAIM: RunPendingReason
RUN_PENDING_REASON_EXECUTING: RunPendingReason
RUN_PENDING_REASON_SLEEPING: RunPendingReason
RUN_PENDING_REASON_POLLING: RunPendingReason
RUN_PENDING_REASON_WAITING_NO_WAKE: RunPendingReason

class GetInspectionCapabilitiesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetInspectionCapabilitiesResponse(_message.Message):
    __slots__ = ("stores",)
    STORES_FIELD_NUMBER: _ClassVar[int]
    stores: _containers.RepeatedCompositeFieldContainer[InspectionStore]
    def __init__(self, stores: _Optional[_Iterable[_Union[InspectionStore, _Mapping]]] = ...) -> None: ...

class InspectionStore(_message.Message):
    __slots__ = ("store", "display_name", "namespaces", "claims_listed", "payloads_visible", "overdue_grace", "indexed_search", "max_page_size", "max_listed_runs", "max_run_records")
    STORE_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    NAMESPACES_FIELD_NUMBER: _ClassVar[int]
    CLAIMS_LISTED_FIELD_NUMBER: _ClassVar[int]
    PAYLOADS_VISIBLE_FIELD_NUMBER: _ClassVar[int]
    OVERDUE_GRACE_FIELD_NUMBER: _ClassVar[int]
    INDEXED_SEARCH_FIELD_NUMBER: _ClassVar[int]
    MAX_PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAX_LISTED_RUNS_FIELD_NUMBER: _ClassVar[int]
    MAX_RUN_RECORDS_FIELD_NUMBER: _ClassVar[int]
    store: str
    display_name: str
    namespaces: _containers.RepeatedScalarFieldContainer[str]
    claims_listed: bool
    payloads_visible: bool
    overdue_grace: _duration_pb2.Duration
    indexed_search: bool
    max_page_size: int
    max_listed_runs: int
    max_run_records: int
    def __init__(self, store: _Optional[str] = ..., display_name: _Optional[str] = ..., namespaces: _Optional[_Iterable[str]] = ..., claims_listed: _Optional[bool] = ..., payloads_visible: _Optional[bool] = ..., overdue_grace: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., indexed_search: _Optional[bool] = ..., max_page_size: _Optional[int] = ..., max_listed_runs: _Optional[int] = ..., max_run_records: _Optional[int] = ...) -> None: ...

class ListNamespacesRequest(_message.Message):
    __slots__ = ("store",)
    STORE_FIELD_NUMBER: _ClassVar[int]
    store: str
    def __init__(self, store: _Optional[str] = ...) -> None: ...

class ListNamespacesResponse(_message.Message):
    __slots__ = ("namespaces",)
    NAMESPACES_FIELD_NUMBER: _ClassVar[int]
    namespaces: _containers.RepeatedCompositeFieldContainer[InspectionNamespace]
    def __init__(self, namespaces: _Optional[_Iterable[_Union[InspectionNamespace, _Mapping]]] = ...) -> None: ...

class InspectionNamespace(_message.Message):
    __slots__ = ("namespace", "present")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    PRESENT_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    present: bool
    def __init__(self, namespace: _Optional[str] = ..., present: _Optional[bool] = ...) -> None: ...

class ListWorkflowDirectoryRequest(_message.Message):
    __slots__ = ("store", "namespace", "page_size", "page_token", "workflow_id_prefix", "status")
    STORE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_PREFIX_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    store: str
    namespace: str
    page_size: int
    page_token: str
    workflow_id_prefix: str
    status: _temporaless_pb2.WorkflowStatus
    def __init__(self, store: _Optional[str] = ..., namespace: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ..., workflow_id_prefix: _Optional[str] = ..., status: _Optional[_Union[_temporaless_pb2.WorkflowStatus, str]] = ...) -> None: ...

class ListWorkflowDirectoryResponse(_message.Message):
    __slots__ = ("entries", "next_page_token", "scanned")
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    SCANNED_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[WorkflowDirectoryEntry]
    next_page_token: str
    scanned: int
    def __init__(self, entries: _Optional[_Iterable[_Union[WorkflowDirectoryEntry, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., scanned: _Optional[int] = ...) -> None: ...

class WorkflowDirectoryEntry(_message.Message):
    __slots__ = ("pointer", "run", "pending", "stale_pointer")
    POINTER_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    PENDING_FIELD_NUMBER: _ClassVar[int]
    STALE_POINTER_FIELD_NUMBER: _ClassVar[int]
    pointer: _temporaless_pb2.LatestWorkflowRunPointer
    run: WorkflowRunSummary
    pending: RunPendingState
    stale_pointer: bool
    def __init__(self, pointer: _Optional[_Union[_temporaless_pb2.LatestWorkflowRunPointer, _Mapping]] = ..., run: _Optional[_Union[WorkflowRunSummary, _Mapping]] = ..., pending: _Optional[_Union[RunPendingState, _Mapping]] = ..., stale_pointer: _Optional[bool] = ...) -> None: ...

class WorkflowRunSummary(_message.Message):
    __slots__ = ("key", "workflow_type", "status", "failure", "created_at", "completed_at", "run_order_time", "annotations")
    class AnnotationsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    KEY_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    RUN_ORDER_TIME_FIELD_NUMBER: _ClassVar[int]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    key: _temporaless_pb2.WorkflowKey
    workflow_type: str
    status: _temporaless_pb2.WorkflowStatus
    failure: _temporaless_pb2.ActivityFailure
    created_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    run_order_time: _timestamp_pb2.Timestamp
    annotations: _containers.ScalarMap[str, str]
    def __init__(self, key: _Optional[_Union[_temporaless_pb2.WorkflowKey, _Mapping]] = ..., workflow_type: _Optional[str] = ..., status: _Optional[_Union[_temporaless_pb2.WorkflowStatus, str]] = ..., failure: _Optional[_Union[_temporaless_pb2.ActivityFailure, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., run_order_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., annotations: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ListWorkflowRunsRequest(_message.Message):
    __slots__ = ("store", "namespace", "workflow_id", "page_size", "page_token", "ascending")
    STORE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ASCENDING_FIELD_NUMBER: _ClassVar[int]
    store: str
    namespace: str
    workflow_id: str
    page_size: int
    page_token: str
    ascending: bool
    def __init__(self, store: _Optional[str] = ..., namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ..., ascending: _Optional[bool] = ...) -> None: ...

class ListWorkflowRunsResponse(_message.Message):
    __slots__ = ("runs", "next_page_token", "total_listed", "runs_without_workflow_record")
    RUNS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_LISTED_FIELD_NUMBER: _ClassVar[int]
    RUNS_WITHOUT_WORKFLOW_RECORD_FIELD_NUMBER: _ClassVar[int]
    runs: _containers.RepeatedCompositeFieldContainer[WorkflowRunSummary]
    next_page_token: str
    total_listed: int
    runs_without_workflow_record: int
    def __init__(self, runs: _Optional[_Iterable[_Union[WorkflowRunSummary, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., total_listed: _Optional[int] = ..., runs_without_workflow_record: _Optional[int] = ...) -> None: ...

class ListScheduledWakesRequest(_message.Message):
    __slots__ = ("store", "namespace", "page_size", "page_token", "overdue_only")
    STORE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    OVERDUE_ONLY_FIELD_NUMBER: _ClassVar[int]
    store: str
    namespace: str
    page_size: int
    page_token: str
    overdue_only: bool
    def __init__(self, store: _Optional[str] = ..., namespace: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ..., overdue_only: _Optional[bool] = ...) -> None: ...

class ListScheduledWakesResponse(_message.Message):
    __slots__ = ("wakes", "next_page_token", "scanned", "quarantined_entries", "invalid_entries")
    WAKES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    SCANNED_FIELD_NUMBER: _ClassVar[int]
    QUARANTINED_ENTRIES_FIELD_NUMBER: _ClassVar[int]
    INVALID_ENTRIES_FIELD_NUMBER: _ClassVar[int]
    wakes: _containers.RepeatedCompositeFieldContainer[ScheduledWake]
    next_page_token: str
    scanned: int
    quarantined_entries: int
    invalid_entries: int
    def __init__(self, wakes: _Optional[_Iterable[_Union[ScheduledWake, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., scanned: _Optional[int] = ..., quarantined_entries: _Optional[int] = ..., invalid_entries: _Optional[int] = ...) -> None: ...

class ScheduledWake(_message.Message):
    __slots__ = ("timer", "workflow_key", "overdue", "ledger_state", "workflow_status")
    TIMER_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_KEY_FIELD_NUMBER: _ClassVar[int]
    OVERDUE_FIELD_NUMBER: _ClassVar[int]
    LEDGER_STATE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_STATUS_FIELD_NUMBER: _ClassVar[int]
    timer: _temporaless_pb2.TimerRecord
    workflow_key: _temporaless_pb2.WorkflowKey
    overdue: bool
    ledger_state: WakeLedgerState
    workflow_status: _temporaless_pb2.WorkflowStatus
    def __init__(self, timer: _Optional[_Union[_temporaless_pb2.TimerRecord, _Mapping]] = ..., workflow_key: _Optional[_Union[_temporaless_pb2.WorkflowKey, _Mapping]] = ..., overdue: _Optional[bool] = ..., ledger_state: _Optional[_Union[WakeLedgerState, str]] = ..., workflow_status: _Optional[_Union[_temporaless_pb2.WorkflowStatus, str]] = ...) -> None: ...

class DescribeRunRequest(_message.Message):
    __slots__ = ("store", "key")
    STORE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    store: str
    key: _temporaless_pb2.WorkflowKey
    def __init__(self, store: _Optional[str] = ..., key: _Optional[_Union[_temporaless_pb2.WorkflowKey, _Mapping]] = ...) -> None: ...

class DescribeRunResponse(_message.Message):
    __slots__ = ("workflow", "activities", "timers", "events", "claims", "claims_inspected", "history", "pending", "payloads", "observed_at", "payload_visibility", "truncated")
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    ACTIVITIES_FIELD_NUMBER: _ClassVar[int]
    TIMERS_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    CLAIMS_FIELD_NUMBER: _ClassVar[int]
    CLAIMS_INSPECTED_FIELD_NUMBER: _ClassVar[int]
    HISTORY_FIELD_NUMBER: _ClassVar[int]
    PENDING_FIELD_NUMBER: _ClassVar[int]
    PAYLOADS_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_VISIBILITY_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    workflow: _temporaless_pb2.WorkflowRecord
    activities: _containers.RepeatedCompositeFieldContainer[_temporaless_pb2.ActivityRecord]
    timers: _containers.RepeatedCompositeFieldContainer[_temporaless_pb2.TimerRecord]
    events: _containers.RepeatedCompositeFieldContainer[_temporaless_pb2.EventRecord]
    claims: _containers.RepeatedCompositeFieldContainer[_temporaless_pb2.ClaimRecord]
    claims_inspected: bool
    history: _containers.RepeatedCompositeFieldContainer[RunHistoryEvent]
    pending: RunPendingState
    payloads: _containers.RepeatedCompositeFieldContainer[RenderedPayload]
    observed_at: _timestamp_pb2.Timestamp
    payload_visibility: PayloadVisibility
    truncated: bool
    def __init__(self, workflow: _Optional[_Union[_temporaless_pb2.WorkflowRecord, _Mapping]] = ..., activities: _Optional[_Iterable[_Union[_temporaless_pb2.ActivityRecord, _Mapping]]] = ..., timers: _Optional[_Iterable[_Union[_temporaless_pb2.TimerRecord, _Mapping]]] = ..., events: _Optional[_Iterable[_Union[_temporaless_pb2.EventRecord, _Mapping]]] = ..., claims: _Optional[_Iterable[_Union[_temporaless_pb2.ClaimRecord, _Mapping]]] = ..., claims_inspected: _Optional[bool] = ..., history: _Optional[_Iterable[_Union[RunHistoryEvent, _Mapping]]] = ..., pending: _Optional[_Union[RunPendingState, _Mapping]] = ..., payloads: _Optional[_Iterable[_Union[RenderedPayload, _Mapping]]] = ..., observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., payload_visibility: _Optional[_Union[PayloadVisibility, str]] = ..., truncated: _Optional[bool] = ...) -> None: ...

class RunHistoryEvent(_message.Message):
    __slots__ = ("event_id", "kind", "time", "until", "resource_id", "attempt", "failure", "timer_kind")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    UNTIL_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    TIMER_KIND_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    kind: RunHistoryEventKind
    time: _timestamp_pb2.Timestamp
    until: _timestamp_pb2.Timestamp
    resource_id: str
    attempt: int
    failure: _temporaless_pb2.ActivityFailure
    timer_kind: _temporaless_pb2.TimerKind
    def __init__(self, event_id: _Optional[str] = ..., kind: _Optional[_Union[RunHistoryEventKind, str]] = ..., time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., until: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., resource_id: _Optional[str] = ..., attempt: _Optional[int] = ..., failure: _Optional[_Union[_temporaless_pb2.ActivityFailure, _Mapping]] = ..., timer_kind: _Optional[_Union[_temporaless_pb2.TimerKind, str]] = ...) -> None: ...

class RunPendingState(_message.Message):
    __slots__ = ("reason", "resource_id", "at", "attempt", "maximum_attempts", "last_failure")
    REASON_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    LAST_FAILURE_FIELD_NUMBER: _ClassVar[int]
    reason: RunPendingReason
    resource_id: str
    at: _timestamp_pb2.Timestamp
    attempt: int
    maximum_attempts: int
    last_failure: _temporaless_pb2.ActivityFailure
    def __init__(self, reason: _Optional[_Union[RunPendingReason, str]] = ..., resource_id: _Optional[str] = ..., at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., attempt: _Optional[int] = ..., maximum_attempts: _Optional[int] = ..., last_failure: _Optional[_Union[_temporaless_pb2.ActivityFailure, _Mapping]] = ...) -> None: ...

class RenderedPayload(_message.Message):
    __slots__ = ("path", "type_url", "size_bytes", "json", "opaque", "redacted")
    PATH_FIELD_NUMBER: _ClassVar[int]
    TYPE_URL_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    JSON_FIELD_NUMBER: _ClassVar[int]
    OPAQUE_FIELD_NUMBER: _ClassVar[int]
    REDACTED_FIELD_NUMBER: _ClassVar[int]
    path: str
    type_url: str
    size_bytes: int
    json: _struct_pb2.Value
    opaque: bytes
    redacted: bool
    def __init__(self, path: _Optional[str] = ..., type_url: _Optional[str] = ..., size_bytes: _Optional[int] = ..., json: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ..., opaque: _Optional[bytes] = ..., redacted: _Optional[bool] = ...) -> None: ...
