import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ActivityStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACTIVITY_STATUS_UNSPECIFIED: _ClassVar[ActivityStatus]
    ACTIVITY_STATUS_COMPLETED: _ClassVar[ActivityStatus]
    ACTIVITY_STATUS_FAILED: _ClassVar[ActivityStatus]
    ACTIVITY_STATUS_RETRYING: _ClassVar[ActivityStatus]

class WorkflowStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WORKFLOW_STATUS_UNSPECIFIED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_COMPLETED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_FAILED: _ClassVar[WorkflowStatus]
    WORKFLOW_STATUS_IN_PROGRESS: _ClassVar[WorkflowStatus]

class TimerStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TIMER_STATUS_UNSPECIFIED: _ClassVar[TimerStatus]
    TIMER_STATUS_SCHEDULED: _ClassVar[TimerStatus]
    TIMER_STATUS_FIRED: _ClassVar[TimerStatus]
    TIMER_STATUS_CANCELED: _ClassVar[TimerStatus]

class TimerKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TIMER_KIND_UNSPECIFIED: _ClassVar[TimerKind]
    TIMER_KIND_SLEEP: _ClassVar[TimerKind]
    TIMER_KIND_ACTIVITY_RETRY: _ClassVar[TimerKind]

class RecordSchemaVersion(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RECORD_SCHEMA_VERSION_UNSPECIFIED: _ClassVar[RecordSchemaVersion]
    RECORD_SCHEMA_VERSION_ACTIVITY: _ClassVar[RecordSchemaVersion]
    RECORD_SCHEMA_VERSION_WORKFLOW: _ClassVar[RecordSchemaVersion]
    RECORD_SCHEMA_VERSION_TIMER: _ClassVar[RecordSchemaVersion]
    RECORD_SCHEMA_VERSION_CLAIM: _ClassVar[RecordSchemaVersion]
    RECORD_SCHEMA_VERSION_EVENT: _ClassVar[RecordSchemaVersion]

class ClaimResourceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLAIM_RESOURCE_TYPE_UNSPECIFIED: _ClassVar[ClaimResourceType]
    CLAIM_RESOURCE_TYPE_WORKFLOW: _ClassVar[ClaimResourceType]
    CLAIM_RESOURCE_TYPE_ACTIVITY: _ClassVar[ClaimResourceType]
    CLAIM_RESOURCE_TYPE_TIMER: _ClassVar[ClaimResourceType]

class ClaimCapability(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLAIM_CAPABILITY_UNSPECIFIED: _ClassVar[ClaimCapability]
    CLAIM_CAPABILITY_NO_CLAIMS: _ClassVar[ClaimCapability]
    CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS: _ClassVar[ClaimCapability]
    CLAIM_CAPABILITY_CAS_CLAIMS: _ClassVar[ClaimCapability]
ACTIVITY_STATUS_UNSPECIFIED: ActivityStatus
ACTIVITY_STATUS_COMPLETED: ActivityStatus
ACTIVITY_STATUS_FAILED: ActivityStatus
ACTIVITY_STATUS_RETRYING: ActivityStatus
WORKFLOW_STATUS_UNSPECIFIED: WorkflowStatus
WORKFLOW_STATUS_COMPLETED: WorkflowStatus
WORKFLOW_STATUS_FAILED: WorkflowStatus
WORKFLOW_STATUS_IN_PROGRESS: WorkflowStatus
TIMER_STATUS_UNSPECIFIED: TimerStatus
TIMER_STATUS_SCHEDULED: TimerStatus
TIMER_STATUS_FIRED: TimerStatus
TIMER_STATUS_CANCELED: TimerStatus
TIMER_KIND_UNSPECIFIED: TimerKind
TIMER_KIND_SLEEP: TimerKind
TIMER_KIND_ACTIVITY_RETRY: TimerKind
RECORD_SCHEMA_VERSION_UNSPECIFIED: RecordSchemaVersion
RECORD_SCHEMA_VERSION_ACTIVITY: RecordSchemaVersion
RECORD_SCHEMA_VERSION_WORKFLOW: RecordSchemaVersion
RECORD_SCHEMA_VERSION_TIMER: RecordSchemaVersion
RECORD_SCHEMA_VERSION_CLAIM: RecordSchemaVersion
RECORD_SCHEMA_VERSION_EVENT: RecordSchemaVersion
CLAIM_RESOURCE_TYPE_UNSPECIFIED: ClaimResourceType
CLAIM_RESOURCE_TYPE_WORKFLOW: ClaimResourceType
CLAIM_RESOURCE_TYPE_ACTIVITY: ClaimResourceType
CLAIM_RESOURCE_TYPE_TIMER: ClaimResourceType
CLAIM_CAPABILITY_UNSPECIFIED: ClaimCapability
CLAIM_CAPABILITY_NO_CLAIMS: ClaimCapability
CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS: ClaimCapability
CLAIM_CAPABILITY_CAS_CLAIMS: ClaimCapability

class WorkflowOptions(_message.Message):
    __slots__ = ("workflow_id", "run_id", "code_version", "claim_owner_id")
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CODE_VERSION_FIELD_NUMBER: _ClassVar[int]
    CLAIM_OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    run_id: str
    code_version: str
    claim_owner_id: str
    def __init__(self, workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., code_version: _Optional[str] = ..., claim_owner_id: _Optional[str] = ...) -> None: ...

class ActivityOptions(_message.Message):
    __slots__ = ("activity_id", "retry_policy")
    ACTIVITY_ID_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    activity_id: str
    retry_policy: RetryPolicy
    def __init__(self, activity_id: _Optional[str] = ..., retry_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ...) -> None: ...

class RetryPolicy(_message.Message):
    __slots__ = ("initial_interval", "backoff_coefficient", "maximum_interval", "maximum_attempts", "non_retryable_error_codes", "durable_backoff_threshold")
    INITIAL_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_COEFFICIENT_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    NON_RETRYABLE_ERROR_CODES_FIELD_NUMBER: _ClassVar[int]
    DURABLE_BACKOFF_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    initial_interval: _duration_pb2.Duration
    backoff_coefficient: float
    maximum_interval: _duration_pb2.Duration
    maximum_attempts: int
    non_retryable_error_codes: _containers.RepeatedScalarFieldContainer[str]
    durable_backoff_threshold: _duration_pb2.Duration
    def __init__(self, initial_interval: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., backoff_coefficient: _Optional[float] = ..., maximum_interval: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., maximum_attempts: _Optional[int] = ..., non_retryable_error_codes: _Optional[_Iterable[str]] = ..., durable_backoff_threshold: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class WorkflowKey(_message.Message):
    __slots__ = ("namespace", "workflow_id", "run_id")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    workflow_id: str
    run_id: str
    def __init__(self, namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ...) -> None: ...

class ActivityKey(_message.Message):
    __slots__ = ("namespace", "workflow_id", "run_id", "activity_id")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_ID_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    workflow_id: str
    run_id: str
    activity_id: str
    def __init__(self, namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., activity_id: _Optional[str] = ...) -> None: ...

class TimerKey(_message.Message):
    __slots__ = ("namespace", "workflow_id", "run_id", "timer_id")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    TIMER_ID_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    workflow_id: str
    run_id: str
    timer_id: str
    def __init__(self, namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., timer_id: _Optional[str] = ...) -> None: ...

class EventKey(_message.Message):
    __slots__ = ("namespace", "workflow_id", "run_id", "event_id")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    workflow_id: str
    run_id: str
    event_id: str
    def __init__(self, namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., event_id: _Optional[str] = ...) -> None: ...

class ClaimKey(_message.Message):
    __slots__ = ("namespace", "workflow_id", "run_id", "claim_id")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CLAIM_ID_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    workflow_id: str
    run_id: str
    claim_id: str
    def __init__(self, namespace: _Optional[str] = ..., workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., claim_id: _Optional[str] = ...) -> None: ...

class ActivityFailure(_message.Message):
    __slots__ = ("code", "message")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class ActivityAttempt(_message.Message):
    __slots__ = ("attempt", "started_at", "completed_at", "failure")
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    attempt: int
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    failure: ActivityFailure
    def __init__(self, attempt: _Optional[int] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., failure: _Optional[_Union[ActivityFailure, _Mapping]] = ...) -> None: ...

class ActivityRecord(_message.Message):
    __slots__ = ("schema_version", "key", "activity_type", "code_version", "input_digest", "input", "status", "result", "failure", "created_at", "completed_at", "attempts", "annotations", "next_attempt_at")
    class AnnotationsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    CODE_VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_AT_FIELD_NUMBER: _ClassVar[int]
    schema_version: RecordSchemaVersion
    key: ActivityKey
    activity_type: str
    code_version: str
    input_digest: str
    input: _any_pb2.Any
    status: ActivityStatus
    result: _any_pb2.Any
    failure: ActivityFailure
    created_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    attempts: _containers.RepeatedCompositeFieldContainer[ActivityAttempt]
    annotations: _containers.ScalarMap[str, str]
    next_attempt_at: _timestamp_pb2.Timestamp
    def __init__(self, schema_version: _Optional[_Union[RecordSchemaVersion, str]] = ..., key: _Optional[_Union[ActivityKey, _Mapping]] = ..., activity_type: _Optional[str] = ..., code_version: _Optional[str] = ..., input_digest: _Optional[str] = ..., input: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., status: _Optional[_Union[ActivityStatus, str]] = ..., result: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., failure: _Optional[_Union[ActivityFailure, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., attempts: _Optional[_Iterable[_Union[ActivityAttempt, _Mapping]]] = ..., annotations: _Optional[_Mapping[str, str]] = ..., next_attempt_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class WorkflowRecord(_message.Message):
    __slots__ = ("schema_version", "key", "workflow_type", "code_version", "input_digest", "input", "status", "result", "failure", "created_at", "completed_at", "annotations")
    class AnnotationsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    CODE_VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    schema_version: RecordSchemaVersion
    key: WorkflowKey
    workflow_type: str
    code_version: str
    input_digest: str
    input: _any_pb2.Any
    status: WorkflowStatus
    result: _any_pb2.Any
    failure: ActivityFailure
    created_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    annotations: _containers.ScalarMap[str, str]
    def __init__(self, schema_version: _Optional[_Union[RecordSchemaVersion, str]] = ..., key: _Optional[_Union[WorkflowKey, _Mapping]] = ..., workflow_type: _Optional[str] = ..., code_version: _Optional[str] = ..., input_digest: _Optional[str] = ..., input: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., status: _Optional[_Union[WorkflowStatus, str]] = ..., result: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., failure: _Optional[_Union[ActivityFailure, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., annotations: _Optional[_Mapping[str, str]] = ...) -> None: ...

class TimerRecord(_message.Message):
    __slots__ = ("schema_version", "key", "timer_kind", "code_version", "input_digest", "duration", "status", "fire_at", "created_at", "fired_at")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    TIMER_KIND_FIELD_NUMBER: _ClassVar[int]
    CODE_VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FIRE_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    FIRED_AT_FIELD_NUMBER: _ClassVar[int]
    schema_version: RecordSchemaVersion
    key: TimerKey
    timer_kind: TimerKind
    code_version: str
    input_digest: str
    duration: _duration_pb2.Duration
    status: TimerStatus
    fire_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    fired_at: _timestamp_pb2.Timestamp
    def __init__(self, schema_version: _Optional[_Union[RecordSchemaVersion, str]] = ..., key: _Optional[_Union[TimerKey, _Mapping]] = ..., timer_kind: _Optional[_Union[TimerKind, str]] = ..., code_version: _Optional[str] = ..., input_digest: _Optional[str] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., status: _Optional[_Union[TimerStatus, str]] = ..., fire_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., fired_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class EventRecord(_message.Message):
    __slots__ = ("schema_version", "key", "payload", "received_at")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    RECEIVED_AT_FIELD_NUMBER: _ClassVar[int]
    schema_version: RecordSchemaVersion
    key: EventKey
    payload: _any_pb2.Any
    received_at: _timestamp_pb2.Timestamp
    def __init__(self, schema_version: _Optional[_Union[RecordSchemaVersion, str]] = ..., key: _Optional[_Union[EventKey, _Mapping]] = ..., payload: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., received_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ClaimRecord(_message.Message):
    __slots__ = ("schema_version", "key", "owner_id", "resource_type", "resource_id", "code_version", "input_digest", "lease_expires_at", "created_at", "heartbeat_at")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    CODE_VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_AT_FIELD_NUMBER: _ClassVar[int]
    schema_version: RecordSchemaVersion
    key: ClaimKey
    owner_id: str
    resource_type: ClaimResourceType
    resource_id: str
    code_version: str
    input_digest: str
    lease_expires_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    heartbeat_at: _timestamp_pb2.Timestamp
    def __init__(self, schema_version: _Optional[_Union[RecordSchemaVersion, str]] = ..., key: _Optional[_Union[ClaimKey, _Mapping]] = ..., owner_id: _Optional[str] = ..., resource_type: _Optional[_Union[ClaimResourceType, str]] = ..., resource_id: _Optional[str] = ..., code_version: _Optional[str] = ..., input_digest: _Optional[str] = ..., lease_expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., heartbeat_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class GetWorkflowRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: WorkflowKey
    def __init__(self, key: _Optional[_Union[WorkflowKey, _Mapping]] = ...) -> None: ...

class GetWorkflowResponse(_message.Message):
    __slots__ = ("found", "record")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    found: bool
    record: WorkflowRecord
    def __init__(self, found: _Optional[bool] = ..., record: _Optional[_Union[WorkflowRecord, _Mapping]] = ...) -> None: ...

class PutWorkflowRequest(_message.Message):
    __slots__ = ("record",)
    RECORD_FIELD_NUMBER: _ClassVar[int]
    record: WorkflowRecord
    def __init__(self, record: _Optional[_Union[WorkflowRecord, _Mapping]] = ...) -> None: ...

class PutWorkflowResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetTimerRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: TimerKey
    def __init__(self, key: _Optional[_Union[TimerKey, _Mapping]] = ...) -> None: ...

class GetTimerResponse(_message.Message):
    __slots__ = ("found", "record")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    found: bool
    record: TimerRecord
    def __init__(self, found: _Optional[bool] = ..., record: _Optional[_Union[TimerRecord, _Mapping]] = ...) -> None: ...

class PutTimerRequest(_message.Message):
    __slots__ = ("record",)
    RECORD_FIELD_NUMBER: _ClassVar[int]
    record: TimerRecord
    def __init__(self, record: _Optional[_Union[TimerRecord, _Mapping]] = ...) -> None: ...

class PutTimerResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetActivityRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: ActivityKey
    def __init__(self, key: _Optional[_Union[ActivityKey, _Mapping]] = ...) -> None: ...

class GetActivityResponse(_message.Message):
    __slots__ = ("found", "record")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    found: bool
    record: ActivityRecord
    def __init__(self, found: _Optional[bool] = ..., record: _Optional[_Union[ActivityRecord, _Mapping]] = ...) -> None: ...

class PutActivityRequest(_message.Message):
    __slots__ = ("record",)
    RECORD_FIELD_NUMBER: _ClassVar[int]
    record: ActivityRecord
    def __init__(self, record: _Optional[_Union[ActivityRecord, _Mapping]] = ...) -> None: ...

class PutActivityResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetEventRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: EventKey
    def __init__(self, key: _Optional[_Union[EventKey, _Mapping]] = ...) -> None: ...

class GetEventResponse(_message.Message):
    __slots__ = ("found", "record")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    found: bool
    record: EventRecord
    def __init__(self, found: _Optional[bool] = ..., record: _Optional[_Union[EventRecord, _Mapping]] = ...) -> None: ...

class PutEventRequest(_message.Message):
    __slots__ = ("record",)
    RECORD_FIELD_NUMBER: _ClassVar[int]
    record: EventRecord
    def __init__(self, record: _Optional[_Union[EventRecord, _Mapping]] = ...) -> None: ...

class PutEventResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListWorkflowsRequest(_message.Message):
    __slots__ = ("namespace", "status", "workflow_id")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    status: WorkflowStatus
    workflow_id: str
    def __init__(self, namespace: _Optional[str] = ..., status: _Optional[_Union[WorkflowStatus, str]] = ..., workflow_id: _Optional[str] = ...) -> None: ...

class ListWorkflowsResponse(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[WorkflowRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[WorkflowRecord, _Mapping]]] = ...) -> None: ...

class ListActivitiesRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: WorkflowKey
    def __init__(self, key: _Optional[_Union[WorkflowKey, _Mapping]] = ...) -> None: ...

class ListActivitiesResponse(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[ActivityRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[ActivityRecord, _Mapping]]] = ...) -> None: ...

class ListTimersRequest(_message.Message):
    __slots__ = ("key", "status")
    KEY_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    key: WorkflowKey
    status: TimerStatus
    def __init__(self, key: _Optional[_Union[WorkflowKey, _Mapping]] = ..., status: _Optional[_Union[TimerStatus, str]] = ...) -> None: ...

class ListTimersResponse(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[TimerRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[TimerRecord, _Mapping]]] = ...) -> None: ...

class ListEventsRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: WorkflowKey
    def __init__(self, key: _Optional[_Union[WorkflowKey, _Mapping]] = ...) -> None: ...

class ListEventsResponse(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[EventRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[EventRecord, _Mapping]]] = ...) -> None: ...

class DeleteWorkflowRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: WorkflowKey
    def __init__(self, key: _Optional[_Union[WorkflowKey, _Mapping]] = ...) -> None: ...

class DeleteWorkflowResponse(_message.Message):
    __slots__ = ("deleted",)
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    def __init__(self, deleted: _Optional[bool] = ...) -> None: ...

class DeleteActivityRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: ActivityKey
    def __init__(self, key: _Optional[_Union[ActivityKey, _Mapping]] = ...) -> None: ...

class DeleteActivityResponse(_message.Message):
    __slots__ = ("deleted",)
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    def __init__(self, deleted: _Optional[bool] = ...) -> None: ...

class DeleteTimerRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: TimerKey
    def __init__(self, key: _Optional[_Union[TimerKey, _Mapping]] = ...) -> None: ...

class DeleteTimerResponse(_message.Message):
    __slots__ = ("deleted",)
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    def __init__(self, deleted: _Optional[bool] = ...) -> None: ...

class DeleteEventRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: EventKey
    def __init__(self, key: _Optional[_Union[EventKey, _Mapping]] = ...) -> None: ...

class DeleteEventResponse(_message.Message):
    __slots__ = ("deleted",)
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    def __init__(self, deleted: _Optional[bool] = ...) -> None: ...

class GetClaimRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: ClaimKey
    def __init__(self, key: _Optional[_Union[ClaimKey, _Mapping]] = ...) -> None: ...

class GetClaimResponse(_message.Message):
    __slots__ = ("found", "record")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    found: bool
    record: ClaimRecord
    def __init__(self, found: _Optional[bool] = ..., record: _Optional[_Union[ClaimRecord, _Mapping]] = ...) -> None: ...

class TryCreateClaimRequest(_message.Message):
    __slots__ = ("record",)
    RECORD_FIELD_NUMBER: _ClassVar[int]
    record: ClaimRecord
    def __init__(self, record: _Optional[_Union[ClaimRecord, _Mapping]] = ...) -> None: ...

class TryCreateClaimResponse(_message.Message):
    __slots__ = ("created",)
    CREATED_FIELD_NUMBER: _ClassVar[int]
    created: bool
    def __init__(self, created: _Optional[bool] = ...) -> None: ...

class GetStoreCapabilitiesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetStoreCapabilitiesResponse(_message.Message):
    __slots__ = ("claim_capability",)
    CLAIM_CAPABILITY_FIELD_NUMBER: _ClassVar[int]
    claim_capability: ClaimCapability
    def __init__(self, claim_capability: _Optional[_Union[ClaimCapability, str]] = ...) -> None: ...

class SweepRequest(_message.Message):
    __slots__ = ("namespace", "now", "max_age")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    MAX_AGE_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    now: _timestamp_pb2.Timestamp
    max_age: _duration_pb2.Duration
    def __init__(self, namespace: _Optional[str] = ..., now: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., max_age: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class SweepResponse(_message.Message):
    __slots__ = ("deleted",)
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: int
    def __init__(self, deleted: _Optional[int] = ...) -> None: ...

class DueTimer(_message.Message):
    __slots__ = ("key", "record", "workflow")
    KEY_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    key: TimerKey
    record: TimerRecord
    workflow: WorkflowRecord
    def __init__(self, key: _Optional[_Union[TimerKey, _Mapping]] = ..., record: _Optional[_Union[TimerRecord, _Mapping]] = ..., workflow: _Optional[_Union[WorkflowRecord, _Mapping]] = ...) -> None: ...

class DueTimersRequest(_message.Message):
    __slots__ = ("namespace", "now")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    NOW_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    now: _timestamp_pb2.Timestamp
    def __init__(self, namespace: _Optional[str] = ..., now: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DueTimersResponse(_message.Message):
    __slots__ = ("due",)
    DUE_FIELD_NUMBER: _ClassVar[int]
    due: _containers.RepeatedCompositeFieldContainer[DueTimer]
    def __init__(self, due: _Optional[_Iterable[_Union[DueTimer, _Mapping]]] = ...) -> None: ...
