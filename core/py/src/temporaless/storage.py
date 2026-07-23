from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from collections.abc import AsyncIterable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, TypeVar, runtime_checkable

import opendal
from google.protobuf.message import DecodeError, Message
from protovalidate import ValidationError, validate

from temporaless.v1 import temporaless_pb2

DEFAULT_NAMESPACE = "default"
ACTIVITY_RECORD_SCHEMA_VERSION = temporaless_pb2.RECORD_SCHEMA_VERSION_ACTIVITY
WORKFLOW_RECORD_SCHEMA_VERSION = temporaless_pb2.RECORD_SCHEMA_VERSION_WORKFLOW
TIMER_RECORD_SCHEMA_VERSION = temporaless_pb2.RECORD_SCHEMA_VERSION_TIMER
CLAIM_RECORD_SCHEMA_VERSION = temporaless_pb2.RECORD_SCHEMA_VERSION_CLAIM
EVENT_RECORD_SCHEMA_VERSION = temporaless_pb2.RECORD_SCHEMA_VERSION_EVENT
SLEEP_TIMER_KIND = temporaless_pb2.TIMER_KIND_SLEEP
POLL_TIMER_KIND = temporaless_pb2.TIMER_KIND_POLL
NO_CLAIMS = temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS
CREATE_ONLY_CLAIMS = temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS
# Reserved for future wire compatibility. The current ClaimStore Protocol
# cannot implement or advertise CAS semantics.
CAS_CLAIMS = temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS
ClaimCapability = temporaless_pb2.ClaimCapability
NO_ATOMIC_EVENT_DELIVERY = temporaless_pb2.EVENT_DELIVERY_CAPABILITY_NO_ATOMIC_CREATE
CREATE_ONLY_EVENT_DELIVERY = temporaless_pb2.EVENT_DELIVERY_CAPABILITY_CREATE_ONLY
EventDeliveryCapability = temporaless_pb2.EventDeliveryCapability


def _current_claim_capability(
    capability: temporaless_pb2.ClaimCapability,
) -> temporaless_pb2.ClaimCapability:
    """Normalize capabilities implemented by the current ClaimStore surface.

    UNSPECIFIED is fail-closed NO_CLAIMS. CAS is deliberately rejected: the
    enum value is reserved for a future interface with fencing and conditional
    refresh/release/takeover operations.
    """
    if capability in (
        temporaless_pb2.CLAIM_CAPABILITY_UNSPECIFIED,
        NO_CLAIMS,
    ):
        return NO_CLAIMS
    if capability == CREATE_ONLY_CLAIMS:
        return CREATE_ONLY_CLAIMS
    try:
        name = temporaless_pb2.ClaimCapability.Name(capability)
    except ValueError:
        name = str(capability)
    raise ValueError(
        f"claim capability {name} is unsupported by the current create-only claim interface"
    )


class ClaimRunListingUnsupportedError(TypeError):
    """A claim backend can coordinate claims but cannot list one run's claims."""


class RunRecordValidationError(ValueError):
    """A stored or remote record violates its schema or requested location."""


class EventDeliveryUnsupportedError(RuntimeError):
    """The configured store cannot atomically establish an event payload."""


class EventDeliveryConflictError(RuntimeError):
    """An EventKey already contains a different protobuf payload."""

    def __init__(self, key: EventKey) -> None:
        super().__init__(
            f"event {key.workflow_id!r}/{key.run_id!r}/{key.event_id!r} "
            "already contains a different payload"
        )
        self.key = key


# V2 storage is flat and constructed-only. Keys are never inverted back into
# record identity; routines that need identity read the protobuf payload.
STORAGE_ROOT_PREFIX = "temporaless/v2"
_SYSTEM_WORKFLOW_IDS = frozenset({temporaless_pb2.ReservedNames().concurrency_workflow_id})
_LOGGER = logging.getLogger(__name__)

_MessageT = TypeVar("_MessageT", bound=Message)


def _run_prefix(namespace: str, workflow_id: str, run_id: str) -> str:
    return f"{STORAGE_ROOT_PREFIX}/{namespace}/{workflow_id}/{run_id}"


def _latest_pointer_path(namespace: str, workflow_id: str) -> str:
    return f"{STORAGE_ROOT_PREFIX}/{namespace}/_latest/{workflow_id}.binpb"


def _due_root(namespace: str) -> str:
    return f"{STORAGE_ROOT_PREFIX}/{namespace}/_due/"


def _due_invalid_root(namespace: str) -> str:
    return f"{STORAGE_ROOT_PREFIX}/{namespace}/_due_invalid/"


def _timestamp_from_proto(value) -> datetime:
    return value.ToDatetime().replace(tzinfo=UTC)


def _due_entry_path(key: TimerKey) -> str:
    """Return the constructed-only shadow path for one logical timer.

    The deadline is payload data, never object identity. Rewriting this one
    path before every canonical timer write makes the ledger an exact prepared
    value for crash recovery instead of an append-only wake hint.
    """
    key.validate()
    return f"{_due_root(key.namespace)}{key.workflow_id}/{key.run_id}/{key.timer_id}.binpb"


@dataclass(frozen=True)
class WorkflowKey:
    workflow_id: str
    run_id: str
    namespace: str = DEFAULT_NAMESPACE

    def to_proto(self) -> temporaless_pb2.WorkflowKey:
        self.validate()
        return temporaless_pb2.WorkflowKey(
            namespace=self.namespace,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
        )

    def path(self) -> str:
        self.validate()
        return f"{_run_prefix(self.namespace, self.workflow_id, self.run_id)}/workflow.binpb"

    def dir_path(self) -> str:
        """Run-level prefix. Everything authoritative for this run lives below it."""
        self.validate()
        return f"{_run_prefix(self.namespace, self.workflow_id, self.run_id)}/"

    def validate(self) -> None:
        validate(
            temporaless_pb2.WorkflowKey(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
            )
        )
        _validate_user_scope(self.namespace, self.workflow_id)


@dataclass(frozen=True)
class ActivityKey:
    workflow_id: str
    run_id: str
    activity_id: str
    namespace: str = DEFAULT_NAMESPACE

    def to_proto(self) -> temporaless_pb2.ActivityKey:
        self.validate()
        return temporaless_pb2.ActivityKey(
            namespace=self.namespace,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
            activity_id=self.activity_id,
        )

    def path(self) -> str:
        self.validate()
        return (
            f"{_run_prefix(self.namespace, self.workflow_id, self.run_id)}"
            f"/activity/{self.activity_id}.binpb"
        )

    def dir_path(self) -> str:
        self.validate()
        return f"{_run_prefix(self.namespace, self.workflow_id, self.run_id)}/activity/"

    def validate(self) -> None:
        validate(
            temporaless_pb2.ActivityKey(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
                activity_id=self.activity_id,
            )
        )
        _validate_user_scope(self.namespace, self.workflow_id)


@dataclass(frozen=True)
class TimerKey:
    workflow_id: str
    run_id: str
    timer_id: str
    namespace: str = DEFAULT_NAMESPACE

    def to_proto(self) -> temporaless_pb2.TimerKey:
        self.validate()
        return temporaless_pb2.TimerKey(
            namespace=self.namespace,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
            timer_id=self.timer_id,
        )

    def path(self) -> str:
        self.validate()
        return (
            f"{_run_prefix(self.namespace, self.workflow_id, self.run_id)}"
            f"/timer/{self.timer_id}.binpb"
        )

    def dir_path(self) -> str:
        self.validate()
        return f"{_run_prefix(self.namespace, self.workflow_id, self.run_id)}/timer/"

    def validate(self) -> None:
        validate(
            temporaless_pb2.TimerKey(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
                timer_id=self.timer_id,
            )
        )
        _validate_user_scope(self.namespace, self.workflow_id)


@dataclass(frozen=True)
class EventKey:
    workflow_id: str
    run_id: str
    event_id: str
    namespace: str = DEFAULT_NAMESPACE

    def to_proto(self) -> temporaless_pb2.EventKey:
        self.validate()
        return temporaless_pb2.EventKey(
            namespace=self.namespace,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
            event_id=self.event_id,
        )

    def path(self) -> str:
        self.validate()
        return (
            f"{_run_prefix(self.namespace, self.workflow_id, self.run_id)}"
            f"/event/{self.event_id}.binpb"
        )

    def dir_path(self) -> str:
        self.validate()
        return f"{_run_prefix(self.namespace, self.workflow_id, self.run_id)}/event/"

    def validate(self) -> None:
        validate(
            temporaless_pb2.EventKey(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
                event_id=self.event_id,
            )
        )
        _validate_user_scope(self.namespace, self.workflow_id)


@dataclass(frozen=True)
class ClaimKey:
    workflow_id: str
    run_id: str
    claim_id: str
    namespace: str = DEFAULT_NAMESPACE

    def to_proto(self) -> temporaless_pb2.ClaimKey:
        self.validate()
        return temporaless_pb2.ClaimKey(
            namespace=self.namespace,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
            claim_id=self.claim_id,
        )

    def path(self) -> str:
        self.validate()
        return (
            f"{_run_prefix(self.namespace, self.workflow_id, self.run_id)}"
            f"/claim/{self.claim_id}.binpb"
        )

    def dir_path(self) -> str:
        self.validate()
        return f"{_run_prefix(self.namespace, self.workflow_id, self.run_id)}/claim/"

    def validate(self) -> None:
        validate(
            temporaless_pb2.ClaimKey(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
                claim_id=self.claim_id,
            )
        )
        _validate_user_scope(
            self.namespace,
            self.workflow_id,
            allowed_system_workflow_ids=_SYSTEM_WORKFLOW_IDS,
        )


class ActivityStore(Protocol):
    async def get_activity(self, key: ActivityKey) -> temporaless_pb2.ActivityRecord | None: ...

    async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None: ...

    async def list_activities(self, key: WorkflowKey) -> list[temporaless_pb2.ActivityRecord]: ...

    async def delete_activity(self, key: ActivityKey) -> bool: ...


class WorkflowStore(Protocol):
    async def get_workflow(self, key: WorkflowKey) -> temporaless_pb2.WorkflowRecord | None: ...

    async def put_workflow(self, record: temporaless_pb2.WorkflowRecord) -> None: ...

    async def get_latest_workflow_run(
        self, namespace: str, workflow_id: str
    ) -> temporaless_pb2.LatestWorkflowRunPointer | None: ...

    async def delete_workflow(self, key: WorkflowKey) -> bool: ...

    async def delete_run(self, key: WorkflowKey) -> int: ...


class TimerStore(Protocol):
    async def get_timer(self, key: TimerKey) -> temporaless_pb2.TimerRecord | None: ...

    async def put_timer(self, record: temporaless_pb2.TimerRecord) -> None: ...

    async def list_timers(
        self,
        key: WorkflowKey,
        status: temporaless_pb2.TimerStatus,
    ) -> list[temporaless_pb2.TimerRecord]: ...

    async def delete_timer(self, key: TimerKey) -> bool: ...


class EventStore(Protocol):
    async def get_event(self, key: EventKey) -> temporaless_pb2.EventRecord | None: ...

    async def put_event(self, record: temporaless_pb2.EventRecord) -> None: ...

    async def list_events(self, key: WorkflowKey) -> list[temporaless_pb2.EventRecord]: ...

    async def delete_event(self, key: EventKey) -> bool: ...


@runtime_checkable
class EventDeliveryStore(Protocol):
    """Native atomic create-only event delivery.

    Existing records must be strictly validated before canonical payload
    comparison. Corrupt records are errors, never idempotent/conflict results.
    """

    async def event_delivery_capability(
        self,
    ) -> temporaless_pb2.EventDeliveryCapability: ...

    async def deliver_event(
        self,
        record: temporaless_pb2.EventRecord,
    ) -> temporaless_pb2.EventDeliveryDisposition: ...


@runtime_checkable
class ClaimStore(Protocol):
    async def claim_capability(self) -> temporaless_pb2.ClaimCapability: ...

    async def get_claim(self, key: ClaimKey) -> temporaless_pb2.ClaimRecord | None: ...

    async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool: ...

    async def delete_claim(self, key: ClaimKey) -> bool:
        """Idempotently release a held claim. Returns True when the claim
        existed and was removed, False when it was already absent. Used by
        the runtime to release workflow-execution, activity, and concurrency-key
        claims at their durable/orderly boundaries."""
        ...


@runtime_checkable
class ClaimRunStore(ClaimStore, Protocol):
    """Optional run-scoped claim listing used only for bounded run deletion."""

    async def list_claims(self, key: WorkflowKey) -> list[temporaless_pb2.ClaimRecord]: ...


@dataclass(frozen=True)
class DueTimer:
    """A due wake paired with the workflow that owns it.

    If a writer died after publishing the durable ledger entry but before its
    canonical TimerRecord, ``record`` is the exact prepared record embedded in
    that ledger entry.
    """

    key: TimerKey
    record: temporaless_pb2.TimerRecord
    workflow: temporaless_pb2.WorkflowRecord


class Store(ActivityStore, EventStore, TimerStore, WorkflowStore, Protocol):
    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        """Return due wakes whose ``fire_at <= now`` and whose parent
        workflow is still IN_PROGRESS.

        Bucket implementations serve this from the compact due-timer ledger,
        not by walking workflow records. After a ledger-first crash, one scan
        materializes the exact embedded record and a later exact-pair scan
        returns the :class:`DueTimer`.
        """
        ...


class QueryStore(Protocol):
    async def list_workflows(
        self,
        namespace: str,
        workflow_id: str,
        status: temporaless_pb2.WorkflowStatus,
        *,
        order_by: str = "",
        page_size: int = 0,
        page_token: str = "",
    ) -> tuple[list[temporaless_pb2.WorkflowRecord], str]: ...

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
    ) -> tuple[list[temporaless_pb2.ActivityRecord], str]: ...

    async def sweep(self, namespace: str, now: datetime, max_age: timedelta) -> int: ...

    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]: ...


class OpenDALStore:
    def __init__(self, operator: opendal.AsyncOperator) -> None:
        capability = operator.capability()
        required = ("stat", "read", "write", "delete", "list", "create_dir")
        missing = [name for name in required if not getattr(capability, name)]
        if missing:
            raise ValueError(
                "OpenDAL operator does not support required point-store operations: "
                + ", ".join(missing)
            )
        self._operator = operator
        self._claim_capability = (
            CREATE_ONLY_CLAIMS if capability.write_with_if_not_exists else NO_CLAIMS
        )
        self._event_delivery_capability = (
            CREATE_ONLY_EVENT_DELIVERY
            if capability.write_with_if_not_exists
            else NO_ATOMIC_EVENT_DELIVERY
        )
        self._latest_pointer_lock = asyncio.Lock()

    async def claim_capability(self) -> temporaless_pb2.ClaimCapability:
        return self._claim_capability

    async def get_activity(self, key: ActivityKey) -> temporaless_pb2.ActivityRecord | None:
        path = key.path()
        record = await _read_message(self._operator, path, temporaless_pb2.ActivityRecord)
        if record is not None:
            _validate_activity_record(record, expected_key=key, storage_path=path)
        return record

    async def get_workflow(self, key: WorkflowKey) -> temporaless_pb2.WorkflowRecord | None:
        path = key.path()
        record = await _read_message(self._operator, path, temporaless_pb2.WorkflowRecord)
        if record is not None:
            _validate_workflow_record(record, expected_key=key, storage_path=path)
        return record

    async def get_latest_workflow_run(
        self, namespace: str, workflow_id: str
    ) -> temporaless_pb2.LatestWorkflowRunPointer | None:
        pointer = await self._read_latest_workflow_run_pointer(namespace, workflow_id)
        if pointer is None:
            return None
        pointer_key = workflow_key_from_proto(pointer.key)
        workflow = await self.get_workflow(pointer_key)
        if not _validate_latest_workflow_run_reference(pointer, workflow):
            return None
        return pointer

    async def _read_latest_workflow_run_pointer(
        self, namespace: str, workflow_id: str
    ) -> temporaless_pb2.LatestWorkflowRunPointer | None:
        """Read and shape-check a pointer without dereferencing its run.

        Writers need this internal form after the authoritative WorkflowRecord
        has already changed status but before the derived pointer catches up.
        Public reads always use ``get_latest_workflow_run`` and verify the
        referenced workflow as a second point GET.
        """
        namespace = namespace or DEFAULT_NAMESPACE
        _validate_pointer_key(namespace, workflow_id)
        pointer = await _read_message(
            self._operator,
            _latest_pointer_path(namespace, workflow_id),
            temporaless_pb2.LatestWorkflowRunPointer,
        )
        if pointer is None:
            return None
        _validate_latest_workflow_run_pointer(pointer, namespace, workflow_id)
        return pointer

    async def get_timer(self, key: TimerKey) -> temporaless_pb2.TimerRecord | None:
        # Validate the write-ahead shadow before touching the point. A corrupt
        # or stale canonical object must not hide exact prepared recovery data.
        ledger = await self._read_due_entry(key)
        path = key.path()
        try:
            record = await _read_message(self._operator, path, temporaless_pb2.TimerRecord)
            if record is not None:
                _validate_timer_record(record, expected_key=key, storage_path=path)
        except DecodeError, ValidationError, ValueError, OverflowError:
            if ledger is None:
                raise
            if ledger.record.status == temporaless_pb2.TIMER_STATUS_CANCELED:
                return None
            return ledger.record
        if ledger is None:
            return record
        if record is not None and _same_timer_record(record, ledger.record):
            return record
        # The shadow is a write-ahead prepared value. On a mismatch it is the
        # only state that lets a retry observe an interrupted overwrite (for
        # example FIRED -> SCHEDULED activity-retry rearming) instead of
        # replaying the stale point forever.
        if ledger.record.status == temporaless_pb2.TIMER_STATUS_CANCELED:
            return None
        return ledger.record

    async def get_event(self, key: EventKey) -> temporaless_pb2.EventRecord | None:
        path = key.path()
        record = await _read_message(self._operator, path, temporaless_pb2.EventRecord)
        if record is not None:
            _validate_event_record(record, expected_key=key, storage_path=path)
        return record

    async def get_claim(self, key: ClaimKey) -> temporaless_pb2.ClaimRecord | None:
        path = key.path()
        record = await _read_message(self._operator, path, temporaless_pb2.ClaimRecord)
        if record is not None:
            _validate_claim_record(record, expected_key=key, storage_path=path)
        return record

    async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None:
        key = _validate_activity_record(record)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))

    async def put_workflow(self, record: temporaless_pb2.WorkflowRecord) -> None:
        key = _validate_workflow_record(record)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))
        if record.status in (
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            temporaless_pb2.WORKFLOW_STATUS_FAILED,
        ):
            await self._put_latest_pointer(record)

    async def put_timer(self, record: temporaless_pb2.TimerRecord) -> None:
        key = _validate_timer_record(record)
        # Publish the exact prepared value first. If this process dies before
        # the point write, get/list/replay recover every timer field from the
        # deterministic ledger entry without calculating a new deadline.
        await self._put_due_entry(record)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))

    async def put_event(self, record: temporaless_pb2.EventRecord) -> None:
        key = _validate_event_record(record)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))

    async def event_delivery_capability(
        self,
    ) -> temporaless_pb2.EventDeliveryCapability:
        return self._event_delivery_capability

    async def deliver_event(
        self,
        record: temporaless_pb2.EventRecord,
    ) -> temporaless_pb2.EventDeliveryDisposition:
        key = _validate_event_delivery_record(record)
        if self._event_delivery_capability != CREATE_ONLY_EVENT_DELIVERY:
            raise EventDeliveryUnsupportedError(
                "OpenDAL backend does not support atomic event creation"
            )
        await self._operator.create_dir(key.dir_path())
        try:
            await self._operator.write(
                key.path(),
                record.SerializeToString(deterministic=True),
                if_not_exists=True,
            )
        except (
            opendal.exceptions.AlreadyExists,
            opendal.exceptions.ConditionNotMatch,
        ):
            existing = await self.get_event(key)
            if existing is None:
                raise RuntimeError(
                    "conditional event create reported an existing object that could not be read"
                ) from None
            _validate_event_delivery_record(existing, expected_key=key)
            if _same_event_payload(existing, record):
                return temporaless_pb2.EVENT_DELIVERY_DISPOSITION_IDEMPOTENT
            raise EventDeliveryConflictError(key) from None
        return temporaless_pb2.EVENT_DELIVERY_DISPOSITION_CREATED

    async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
        if self._claim_capability == NO_CLAIMS:
            raise RuntimeError("OpenDAL backend does not support atomic create-if-absent claims")
        key = _validate_claim_record(record)
        await self._operator.create_dir(key.dir_path())
        try:
            await self._operator.write(
                key.path(),
                record.SerializeToString(deterministic=True),
                if_not_exists=True,
            )
        except (
            opendal.exceptions.AlreadyExists,
            opendal.exceptions.ConditionNotMatch,
        ):
            return False
        return True

    async def delete_claim(self, key: ClaimKey) -> bool:
        """Lock-free idempotent delete. The underlying object store's Delete
        is atomic on every backend (no fileblob race like create-only writes),
        so cross-process distributed safety is the bucket's contract."""
        return await _delete_if_exists(self._operator, key.path())

    async def delete_workflow(self, key: WorkflowKey) -> bool:
        # The pointer is derived and cross-process writers are lock-free. An
        # unconditional pointer delete can race a newer run's pointer write,
        # so deletion is exact-authoritative only. Public pointer reads turn a
        # dangling reference into not-found.
        return await _delete_if_exists(self._operator, key.path())

    async def delete_run(self, key: WorkflowKey) -> int:
        deleted = 0
        claim_records = await self.list_claims(key)
        activity_records = await self.list_activities(key)
        timer_records = await self.list_timers(key, temporaless_pb2.TIMER_STATUS_UNSPECIFIED)
        event_records = await self.list_events(key)

        # Build and validate the complete deletion plan before removing its
        # first object. Run-scoped listing reads identity from each protobuf
        # payload, so a misplaced/corrupt record must never redirect deletion
        # into another workflow run or leave this run partially deleted.
        claim_keys = _claim_keys_for_run(key, claim_records)
        activity_keys = _activity_keys_for_run(key, activity_records)
        timer_keys = _timer_keys_for_run(key, timer_records)
        event_keys = _event_keys_for_run(key, event_records)

        # Claims are coordination state for the records below. Remove them
        # first so a later record-deletion failure leaves a retryable run and
        # never strands claims after their identifying records are gone.
        for claim_key in claim_keys:
            if await self.delete_claim(claim_key):
                deleted += 1
        for activity_key in activity_keys:
            if await self.delete_activity(activity_key):
                deleted += 1
        for timer_key in timer_keys:
            if await self.delete_timer(timer_key):
                deleted += 1
        for event_key in event_keys:
            if await self.delete_event(event_key):
                deleted += 1
        if await self.delete_workflow(key):
            deleted += 1
        return deleted

    async def list_activities(self, key: WorkflowKey) -> list[temporaless_pb2.ActivityRecord]:
        dir_path = ActivityKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            activity_id="placeholder",
            namespace=key.namespace,
        ).dir_path()
        return await _list_messages(
            self._operator,
            dir_path,
            temporaless_pb2.ActivityRecord,
            lambda record, path: _validate_activity_record(
                record, expected_run=key, storage_path=path
            ),
        )

    async def delete_activity(self, key: ActivityKey) -> bool:
        return await _delete_if_exists(self._operator, key.path())

    async def list_timers(
        self,
        key: WorkflowKey,
        status: temporaless_pb2.TimerStatus,
    ) -> list[temporaless_pb2.TimerRecord]:
        key.validate()
        shadow_by_key: dict[TimerKey, temporaless_pb2.TimerRecord] = {}
        ledger_root = f"{_due_root(key.namespace)}{key.workflow_id}/{key.run_id}/"
        async for ledger_path in _walk_binpb(self._operator, ledger_root):
            try:
                ledger = await _read_message(
                    self._operator,
                    ledger_path,
                    temporaless_pb2.DueTimerEntry,
                )
                if ledger is None:
                    continue
                timer_key = _validate_due_entry(
                    ledger,
                    expected_run=key,
                    storage_path=ledger_path,
                )
            except (DecodeError, ValidationError, ValueError, OverflowError) as exc:
                await _quarantine_invalid_due_entry(
                    self._operator,
                    key.namespace,
                    ledger_path,
                    str(exc),
                )
                continue
            shadow_by_key[timer_key] = ledger.record

        # Read points after their recovery shadows. A corrupt point covered by
        # a valid constructed shadow is recoverable; a corrupt point with no
        # shadow remains a hard storage error instead of disappearing.
        records_by_key: dict[TimerKey, temporaless_pb2.TimerRecord] = {}
        canonical_paths = {timer_key.path(): timer_key for timer_key in shadow_by_key}
        dir_path = TimerKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            timer_id="placeholder",
            namespace=key.namespace,
        ).dir_path()
        try:
            entries = sorted(
                [entry async for entry in await self._operator.list(dir_path)],
                key=lambda entry: entry.path,
            )
        except opendal.exceptions.NotFound:
            entries = []
        for entry in entries:
            if not entry.path.endswith(".binpb"):
                continue
            try:
                record = await _read_message(
                    self._operator,
                    entry.path,
                    temporaless_pb2.TimerRecord,
                )
                if record is None:
                    continue
                timer_key = _validate_timer_record(
                    record,
                    expected_run=key,
                    storage_path=entry.path,
                )
            except DecodeError, ValidationError, ValueError, OverflowError:
                if entry.path not in canonical_paths:
                    raise
                _LOGGER.warning(
                    "recovering invalid canonical timer point %s from its due shadow",
                    entry.path,
                )
                continue
            records_by_key[timer_key] = record

        # Overlay every interrupted shadow-first transition. This covers both
        # missing initial points and stale points left by timer rearming.
        for timer_key, shadow_record in shadow_by_key.items():
            canonical_record = records_by_key.get(timer_key)
            if canonical_record is not None and _same_timer_record(canonical_record, shadow_record):
                continue
            if shadow_record.status == temporaless_pb2.TIMER_STATUS_CANCELED:
                records_by_key.pop(timer_key, None)
            else:
                records_by_key[timer_key] = shadow_record

        records = [
            records_by_key[timer_key]
            for timer_key in sorted(
                records_by_key,
                key=lambda item: (
                    item.namespace,
                    item.workflow_id,
                    item.run_id,
                    item.timer_id,
                ),
            )
        ]
        if status == temporaless_pb2.TIMER_STATUS_UNSPECIFIED:
            return records
        return [record for record in records if record.status == status]

    async def delete_timer(self, key: TimerKey) -> bool:
        key.validate()
        canonical: temporaless_pb2.TimerRecord | None = None
        canonical_exists = await self._operator.exists(key.path())
        if canonical_exists:
            try:
                candidate = await _read_message(
                    self._operator,
                    key.path(),
                    temporaless_pb2.TimerRecord,
                )
                if candidate is not None:
                    _validate_timer_record(
                        candidate,
                        expected_key=key,
                        storage_path=key.path(),
                    )
                    canonical = candidate
            except DecodeError, ValidationError, ValueError, OverflowError:
                # Deletion is exact by the caller-provided key. A corrupt or
                # misplaced point payload must not redirect its tombstone to
                # the identity embedded in that payload.
                canonical = None

        ledger = await self._read_due_entry(key)
        ledger_exists = ledger is not None
        ledger_live = (
            ledger is not None and ledger.record.status != temporaless_pb2.TIMER_STATUS_CANCELED
        )
        if not canonical_exists and not ledger_live:
            return False

        if canonical is not None:
            tombstone = temporaless_pb2.TimerRecord()
            tombstone.CopyFrom(canonical)
        elif ledger_live:
            tombstone = temporaless_pb2.TimerRecord()
            tombstone.CopyFrom(ledger.record)
        else:
            # The point object exists but cannot safely supply identity or
            # metadata. Construct the smallest exact-key tombstone and retain
            # the corrupt point only until the tombstone write succeeds.
            tombstone = temporaless_pb2.TimerRecord(
                schema_version=TIMER_RECORD_SCHEMA_VERSION,
                key=key.to_proto(),
            )
        tombstone.status = temporaless_pb2.TIMER_STATUS_CANCELED
        tombstone.ClearField("fired_at")
        await self._put_due_entry(tombstone)
        await _delete_if_exists(self._operator, key.path())
        return canonical_exists or ledger_exists

    async def list_events(self, key: WorkflowKey) -> list[temporaless_pb2.EventRecord]:
        dir_path = EventKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            event_id="placeholder",
            namespace=key.namespace,
        ).dir_path()
        return await _list_messages(
            self._operator,
            dir_path,
            temporaless_pb2.EventRecord,
            lambda record, path: _validate_event_record(
                record, expected_run=key, storage_path=path
            ),
        )

    async def delete_event(self, key: EventKey) -> bool:
        return await _delete_if_exists(self._operator, key.path())

    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")

        namespaces = await _due_scan_namespaces(self._operator, namespace)
        due: list[DueTimer] = []
        for scan_namespace in namespaces:
            due.extend(await self._due_timers_in_namespace(scan_namespace, now))
        return sorted(
            due,
            key=lambda item: (
                _timestamp_from_proto(item.record.fire_at),
                item.key.namespace,
                item.key.workflow_id,
                item.key.run_id,
                item.key.timer_id,
            ),
        )

    async def _due_timers_in_namespace(
        self,
        namespace: str,
        now: datetime,
    ) -> list[DueTimer]:
        root = _due_root(namespace)
        due: list[DueTimer] = []
        async for ledger_path in _walk_binpb(self._operator, root):
            try:
                ledger = await _read_message(
                    self._operator,
                    ledger_path,
                    temporaless_pb2.DueTimerEntry,
                )
                if ledger is None:
                    continue
                timer_key = _validate_due_entry(
                    ledger,
                    expected_namespace=namespace,
                    storage_path=ledger_path,
                )
                workflow_key = workflow_key_from_proto(ledger.workflow_key)
            except (DecodeError, ValidationError, ValueError, OverflowError) as exc:
                await _quarantine_invalid_due_entry(
                    self._operator,
                    namespace,
                    ledger_path,
                    str(exc),
                )
                # This ledger is the only cross-run discovery record for its
                # timer. Silently skipping it would turn corruption into a
                # successful empty scheduler tick and could strand the owning
                # workflow forever. Keep the non-destructive quarantine copy,
                # then fail the tick with the storage layer's typed corruption
                # error so operators can alert and repair the source object.
                raise RunRecordValidationError(
                    f"invalid due-timer ledger entry {ledger_path}: {exc}"
                ) from exc

            try:
                timer = await self._get_canonical_timer(timer_key)
            except (DecodeError, ValidationError, ValueError, OverflowError) as exc:
                _LOGGER.warning(
                    "repairing due timer with invalid canonical point %s: %s",
                    ledger_path,
                    exc,
                )
                if ledger.record.status == temporaless_pb2.TIMER_STATUS_CANCELED:
                    await _delete_if_exists(self._operator, timer_key.path())
                else:
                    await self._put_canonical_timer(ledger.record)
                continue
            if timer is None:
                if ledger.record.status == temporaless_pb2.TIMER_STATUS_SCHEDULED:
                    await self._put_canonical_timer(ledger.record)
                # A wake is emitted only after a later scan observes the exact
                # prepared shadow and canonical point pair.
                continue
            if not _same_timer_record(timer, ledger.record):
                # Complete an interrupted shadow-first transition, but do not
                # dispatch in the same scan. The next tick must observe an
                # exact pair, which keeps mixed SCHEDULED/FIRED states from
                # producing an early or duplicate wake.
                if ledger.record.status == temporaless_pb2.TIMER_STATUS_CANCELED:
                    await _delete_if_exists(self._operator, timer_key.path())
                else:
                    await self._put_canonical_timer(ledger.record)
                continue
            if timer.status != temporaless_pb2.TIMER_STATUS_SCHEDULED:
                continue

            if ledger.record.status != temporaless_pb2.TIMER_STATUS_SCHEDULED:
                continue
            ledger_fire_at = _timestamp_from_proto(ledger.record.fire_at)
            if ledger_fire_at > now:
                continue

            try:
                workflow = await self.get_workflow(workflow_key)
            except (DecodeError, ValidationError, ValueError, OverflowError) as exc:
                raise RunRecordValidationError(
                    f"due timer {ledger_path} has an invalid parent workflow: {exc}"
                ) from exc
            if workflow is None or workflow.status != temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS:
                continue
            # Dispatch only from an exact prepared shadow/canonical pair.
            due.append(DueTimer(key=timer_key, record=ledger.record, workflow=workflow))
        return due

    async def list_claims(self, key: WorkflowKey) -> list[temporaless_pb2.ClaimRecord]:
        dir_path = ClaimKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            claim_id="placeholder",
            namespace=key.namespace,
        ).dir_path()
        return await _list_messages(
            self._operator,
            dir_path,
            temporaless_pb2.ClaimRecord,
            lambda record, path: _validate_claim_record(
                record, expected_run=key, storage_path=path
            ),
        )

    async def _put_latest_pointer(self, record: temporaless_pb2.WorkflowRecord) -> None:
        async with self._latest_pointer_lock:
            key = workflow_key_from_proto(record.key)
            record_time = _workflow_record_time(record)
            run_order_time = _workflow_run_order_time(record, record_time)
            existing = await self._read_latest_workflow_run_pointer(key.namespace, key.workflow_id)
            if existing is not None and not _should_replace_latest_pointer(
                existing, run_order_time, record_time
            ):
                return

            pointer = temporaless_pb2.LatestWorkflowRunPointer(
                key=record.key,
                status=record.status,
            )
            pointer.record_time.FromDatetime(record_time)
            pointer.run_order_time.FromDatetime(run_order_time)
            pointer.updated_at.GetCurrentTime()
            path = _latest_pointer_path(key.namespace, key.workflow_id)
            await self._operator.create_dir(path.rsplit("/", 1)[0] + "/")
            await self._operator.write(path, pointer.SerializeToString(deterministic=True))

    async def _put_due_entry(self, record: temporaless_pb2.TimerRecord) -> None:
        key = timer_key_from_proto(record.key)
        workflow_key = WorkflowKey(
            namespace=key.namespace,
            workflow_id=key.workflow_id,
            run_id=key.run_id,
        )
        entry = temporaless_pb2.DueTimerEntry(
            key=record.key,
            workflow_key=workflow_key.to_proto(),
            fire_at=record.fire_at,
            record=record,
        )
        _validate_due_entry(entry, storage_path=_due_entry_path(key))
        path = _due_entry_path(key)
        await self._operator.create_dir(path.rsplit("/", 1)[0] + "/")
        await self._operator.write(path, entry.SerializeToString(deterministic=True))

    async def _read_due_entry(
        self,
        key: TimerKey,
    ) -> temporaless_pb2.DueTimerEntry | None:
        path = _due_entry_path(key)
        entry = await _read_message(
            self._operator,
            path,
            temporaless_pb2.DueTimerEntry,
        )
        if entry is not None:
            _validate_due_entry(entry, expected_key=key, storage_path=path)
        return entry

    async def _get_canonical_timer(
        self,
        key: TimerKey,
    ) -> temporaless_pb2.TimerRecord | None:
        path = key.path()
        record = await _read_message(self._operator, path, temporaless_pb2.TimerRecord)
        if record is not None:
            _validate_timer_record(record, expected_key=key, storage_path=path)
        return record

    async def _put_canonical_timer(self, record: temporaless_pb2.TimerRecord) -> None:
        key = _validate_timer_record(record)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(
            key.path(),
            record.SerializeToString(deterministic=True),
        )


def workflow_key_from_proto(key: temporaless_pb2.WorkflowKey) -> WorkflowKey:
    return WorkflowKey(
        namespace=key.namespace or DEFAULT_NAMESPACE,
        workflow_id=key.workflow_id,
        run_id=key.run_id,
    )


def activity_key_from_proto(key: temporaless_pb2.ActivityKey) -> ActivityKey:
    return ActivityKey(
        namespace=key.namespace or DEFAULT_NAMESPACE,
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        activity_id=key.activity_id,
    )


def timer_key_from_proto(key: temporaless_pb2.TimerKey) -> TimerKey:
    return TimerKey(
        namespace=key.namespace or DEFAULT_NAMESPACE,
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        timer_id=key.timer_id,
    )


def claim_key_from_proto(key: temporaless_pb2.ClaimKey) -> ClaimKey:
    return ClaimKey(
        namespace=key.namespace or DEFAULT_NAMESPACE,
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id=key.claim_id,
    )


def event_key_from_proto(key: temporaless_pb2.EventKey) -> EventKey:
    return EventKey(
        namespace=key.namespace or DEFAULT_NAMESPACE,
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        event_id=key.event_id,
    )


def _validate_record_schema(
    actual: temporaless_pb2.RecordSchemaVersion,
    expected: temporaless_pb2.RecordSchemaVersion,
    record_kind: str,
) -> None:
    if actual != expected:
        raise RunRecordValidationError(
            f"{record_kind} record has schema_version {actual}, expected {expected}"
        )


def _validate_record_location(
    record_key: ActivityKey | ClaimKey | EventKey | TimerKey | WorkflowKey,
    record_kind: str,
    *,
    expected_key: ActivityKey | ClaimKey | EventKey | TimerKey | WorkflowKey | None,
    expected_run: WorkflowKey | None,
    storage_path: str | None,
) -> None:
    if expected_key is not None and record_key != expected_key:
        raise RunRecordValidationError(f"{record_kind} payload key does not match requested key")
    if expected_run is not None:
        _validate_run_identity(
            expected_run,
            record_key.namespace,
            record_key.workflow_id,
            record_key.run_id,
            record_kind,
        )
    if storage_path is not None and record_key.path() != storage_path:
        raise RunRecordValidationError(
            f"{record_kind} payload key does not match its object location"
        )


def _validate_activity_record(
    record: temporaless_pb2.ActivityRecord,
    *,
    expected_key: ActivityKey | None = None,
    expected_run: WorkflowKey | None = None,
    storage_path: str | None = None,
) -> ActivityKey:
    _validate_record_schema(record.schema_version, ACTIVITY_RECORD_SCHEMA_VERSION, "activity")
    validate(record.key)
    key = activity_key_from_proto(record.key)
    key.validate()
    _validate_record_location(
        key,
        "activity",
        expected_key=expected_key,
        expected_run=expected_run,
        storage_path=storage_path,
    )
    return key


def _validate_workflow_record(
    record: temporaless_pb2.WorkflowRecord,
    *,
    expected_key: WorkflowKey | None = None,
    expected_run: WorkflowKey | None = None,
    storage_path: str | None = None,
) -> WorkflowKey:
    _validate_record_schema(record.schema_version, WORKFLOW_RECORD_SCHEMA_VERSION, "workflow")
    validate(record.key)
    key = workflow_key_from_proto(record.key)
    key.validate()
    _validate_record_location(
        key,
        "workflow",
        expected_key=expected_key,
        expected_run=expected_run,
        storage_path=storage_path,
    )
    if record.HasField("run_order_time"):
        try:
            _timestamp_from_proto(record.run_order_time)
        except (ValueError, OverflowError) as exc:
            raise RunRecordValidationError(
                f"workflow payload has invalid run_order_time: {exc}"
            ) from exc
    return key


def _validate_timer_record(
    record: temporaless_pb2.TimerRecord,
    *,
    expected_key: TimerKey | None = None,
    expected_run: WorkflowKey | None = None,
    storage_path: str | None = None,
) -> TimerKey:
    _validate_record_schema(record.schema_version, TIMER_RECORD_SCHEMA_VERSION, "timer")
    validate(record.key)
    key = timer_key_from_proto(record.key)
    key.validate()
    _validate_record_location(
        key,
        "timer",
        expected_key=expected_key,
        expected_run=expected_run,
        storage_path=storage_path,
    )
    return key


def _same_timer_record(
    left: temporaless_pb2.TimerRecord,
    right: temporaless_pb2.TimerRecord,
) -> bool:
    return left.SerializeToString(deterministic=True) == right.SerializeToString(deterministic=True)


def _validate_due_entry(
    entry: temporaless_pb2.DueTimerEntry,
    *,
    expected_key: TimerKey | None = None,
    expected_run: WorkflowKey | None = None,
    expected_namespace: str | None = None,
    storage_path: str | None = None,
) -> TimerKey:
    if entry.record.status not in (
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        temporaless_pb2.TIMER_STATUS_FIRED,
        temporaless_pb2.TIMER_STATUS_CANCELED,
    ):
        raise RunRecordValidationError(
            "due-timer ledger record has an invalid persisted timer status"
        )
    if entry.record.status == temporaless_pb2.TIMER_STATUS_SCHEDULED:
        if not entry.record.HasField("fire_at"):
            raise RunRecordValidationError("scheduled due-timer ledger record has no fire_at")
        try:
            _timestamp_from_proto(entry.record.fire_at)
        except (ValueError, OverflowError) as exc:
            raise RunRecordValidationError(
                f"scheduled due-timer ledger record has invalid fire_at: {exc}"
            ) from exc
    validate(entry)
    validate(entry.key)
    validate(entry.workflow_key)
    timer_key = _validate_timer_record(
        entry.record,
        expected_key=expected_key,
        expected_run=expected_run,
    )
    workflow_key = workflow_key_from_proto(entry.workflow_key)
    workflow_key.validate()
    if entry.key != entry.record.key:
        raise RunRecordValidationError(
            "due-timer ledger key does not match its embedded timer record"
        )
    if (
        workflow_key.namespace != timer_key.namespace
        or workflow_key.workflow_id != timer_key.workflow_id
        or workflow_key.run_id != timer_key.run_id
    ):
        raise RunRecordValidationError(
            "due-timer ledger timer and workflow keys do not own the same run"
        )
    if expected_namespace is not None and timer_key.namespace != expected_namespace:
        raise RunRecordValidationError(
            "due-timer ledger key does not belong to the scanned namespace"
        )
    if entry.fire_at != entry.record.fire_at:
        raise RunRecordValidationError(
            "due-timer ledger fire_at does not match its embedded timer record"
        )
    if storage_path is not None and storage_path != _due_entry_path(timer_key):
        raise RunRecordValidationError(
            "due-timer ledger payload identity does not match its object location"
        )
    return timer_key


def _validate_event_record(
    record: temporaless_pb2.EventRecord,
    *,
    expected_key: EventKey | None = None,
    expected_run: WorkflowKey | None = None,
    storage_path: str | None = None,
) -> EventKey:
    _validate_record_schema(record.schema_version, EVENT_RECORD_SCHEMA_VERSION, "event")
    validate(record.key)
    key = event_key_from_proto(record.key)
    key.validate()
    _validate_record_location(
        key,
        "event",
        expected_key=expected_key,
        expected_run=expected_run,
        storage_path=storage_path,
    )
    return key


def _validate_event_delivery_record(
    record: temporaless_pb2.EventRecord,
    *,
    expected_key: EventKey | None = None,
) -> EventKey:
    key = _validate_event_record(record, expected_key=expected_key)
    if not record.HasField("payload"):
        raise RunRecordValidationError("event delivery payload is required")
    if not record.HasField("received_at"):
        raise RunRecordValidationError("event delivery received_at is required")
    try:
        _timestamp_from_proto(record.received_at)
    except (ValueError, OverflowError) as exc:
        raise RunRecordValidationError(f"event delivery has invalid received_at: {exc}") from exc
    return key


def _same_event_payload(
    left: temporaless_pb2.EventRecord,
    right: temporaless_pb2.EventRecord,
) -> bool:
    return left.key.SerializeToString(deterministic=True) == right.key.SerializeToString(
        deterministic=True
    ) and left.payload.SerializeToString(deterministic=True) == right.payload.SerializeToString(
        deterministic=True
    )


def _validate_event_delivery_disposition(
    disposition: temporaless_pb2.EventDeliveryDisposition,
) -> temporaless_pb2.EventDeliveryDisposition:
    if disposition not in (
        temporaless_pb2.EVENT_DELIVERY_DISPOSITION_CREATED,
        temporaless_pb2.EVENT_DELIVERY_DISPOSITION_IDEMPOTENT,
    ):
        raise RunRecordValidationError(
            f"event delivery store returned invalid disposition {disposition}"
        )
    return disposition


def _validate_claim_record(
    record: temporaless_pb2.ClaimRecord,
    *,
    expected_key: ClaimKey | None = None,
    expected_run: WorkflowKey | None = None,
    storage_path: str | None = None,
) -> ClaimKey:
    _validate_record_schema(record.schema_version, CLAIM_RECORD_SCHEMA_VERSION, "claim")
    validate(record.key)
    key = claim_key_from_proto(record.key)
    key.validate()
    _validate_record_location(
        key,
        "claim",
        expected_key=expected_key,
        expected_run=expected_run,
        storage_path=storage_path,
    )
    return key


def _validate_latest_workflow_run_pointer(
    pointer: temporaless_pb2.LatestWorkflowRunPointer,
    namespace: str,
    workflow_id: str,
) -> WorkflowKey:
    namespace = namespace or DEFAULT_NAMESPACE
    _validate_pointer_key(namespace, workflow_id)
    validate(pointer.key)
    key = workflow_key_from_proto(pointer.key)
    key.validate()
    if key.namespace != namespace or key.workflow_id != workflow_id:
        raise RunRecordValidationError(
            "latest workflow pointer key does not match the requested workflow"
        )
    if pointer.status not in (
        temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        temporaless_pb2.WORKFLOW_STATUS_FAILED,
    ):
        raise RunRecordValidationError("latest workflow pointer has an invalid status")
    if (
        not pointer.HasField("record_time")
        or not pointer.HasField("updated_at")
        or not pointer.HasField("run_order_time")
    ):
        raise RunRecordValidationError("latest workflow pointer is missing required timestamps")
    try:
        _timestamp_from_proto(pointer.record_time)
        _timestamp_from_proto(pointer.updated_at)
        _timestamp_from_proto(pointer.run_order_time)
    except (ValueError, OverflowError) as exc:
        raise RunRecordValidationError(
            f"latest workflow pointer has an invalid timestamp: {exc}"
        ) from exc
    return key


def _validate_latest_workflow_run_reference(
    pointer: temporaless_pb2.LatestWorkflowRunPointer,
    workflow: temporaless_pb2.WorkflowRecord | None,
) -> bool:
    if workflow is None:
        return False
    pointer_key = workflow_key_from_proto(pointer.key)
    _validate_workflow_record(workflow, expected_key=pointer_key)
    if workflow.status != pointer.status:
        # WorkflowRecord is authoritative and is written before this derived
        # pointer. A reader may legitimately land inside that transition.
        return False
    expected_record_time = _persisted_workflow_record_time(workflow)
    if expected_record_time is not None:
        if _timestamp_from_proto(pointer.record_time) != expected_record_time:
            return False
        expected_run_order_time = _workflow_run_order_time(workflow, expected_record_time)
        if _timestamp_from_proto(pointer.run_order_time) != expected_run_order_time:
            return False
    return True


def _validate_due_timer(
    due: DueTimer,
    *,
    namespace: str = "",
    now: datetime | None = None,
) -> None:
    due.key.validate()
    _validate_timer_record(due.record, expected_key=due.key)
    workflow_key = WorkflowKey(
        namespace=due.key.namespace,
        workflow_id=due.key.workflow_id,
        run_id=due.key.run_id,
    )
    _validate_workflow_record(due.workflow, expected_key=workflow_key)
    if namespace and due.key.namespace != namespace:
        raise RunRecordValidationError(
            "due timer payload namespace does not match the requested namespace"
        )
    if due.record.status != temporaless_pb2.TIMER_STATUS_SCHEDULED:
        raise RunRecordValidationError("due timer payload is not scheduled")
    if due.workflow.status != temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS:
        raise RunRecordValidationError("due timer workflow is not in progress")
    try:
        fire_at = _timestamp_from_proto(due.record.fire_at)
    except (ValueError, OverflowError) as exc:
        raise RunRecordValidationError(f"due timer has an invalid fire_at: {exc}") from exc
    if now is not None:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if fire_at > now:
            raise RunRecordValidationError("due timer fire_at is later than the requested time")


def _claim_keys_for_run(
    key: WorkflowKey,
    records: list[temporaless_pb2.ClaimRecord],
) -> list[ClaimKey]:
    claim_keys: list[ClaimKey] = []
    for record in records:
        claim_key = _validate_claim_record(record, expected_run=key)
        claim_keys.append(claim_key)
    return claim_keys


def _activity_keys_for_run(
    key: WorkflowKey,
    records: list[temporaless_pb2.ActivityRecord],
) -> list[ActivityKey]:
    activity_keys: list[ActivityKey] = []
    for record in records:
        activity_key = _validate_activity_record(record, expected_run=key)
        activity_keys.append(activity_key)
    return activity_keys


def _timer_keys_for_run(
    key: WorkflowKey,
    records: list[temporaless_pb2.TimerRecord],
    status: temporaless_pb2.TimerStatus = temporaless_pb2.TIMER_STATUS_UNSPECIFIED,
) -> list[TimerKey]:
    timer_keys: list[TimerKey] = []
    for record in records:
        timer_key = _validate_timer_record(record, expected_run=key)
        if status != temporaless_pb2.TIMER_STATUS_UNSPECIFIED and record.status != status:
            raise RunRecordValidationError("timer list payload does not match the requested status")
        timer_keys.append(timer_key)
    return timer_keys


def _event_keys_for_run(
    key: WorkflowKey,
    records: list[temporaless_pb2.EventRecord],
) -> list[EventKey]:
    event_keys: list[EventKey] = []
    for record in records:
        event_key = _validate_event_record(record, expected_run=key)
        event_keys.append(event_key)
    return event_keys


def _validate_run_identity(
    expected: WorkflowKey,
    namespace: str,
    workflow_id: str,
    run_id: str,
    record_kind: str,
) -> None:
    if (
        namespace != expected.namespace
        or workflow_id != expected.workflow_id
        or run_id != expected.run_id
    ):
        raise RunRecordValidationError(
            f"{record_kind} payload key does not match requested workflow run"
        )


async def _read_message(
    operator: opendal.AsyncOperator,
    path: str,
    factory: Callable[[], _MessageT],
) -> _MessageT | None:
    try:
        data = bytes(await operator.read(path))
    except opendal.exceptions.NotFound:
        return None
    record = factory()
    record.ParseFromString(data)
    return record


async def _list_messages(
    operator: opendal.AsyncOperator,
    root: str,
    factory: Callable[[], _MessageT],
    validate_record: Callable[[_MessageT, str], object] | None = None,
) -> list[_MessageT]:
    try:
        entries = sorted([entry async for entry in await operator.list(root)], key=lambda e: e.path)
    except opendal.exceptions.NotFound:
        return []

    records: list[_MessageT] = []
    for entry in entries:
        if entry.path.endswith(".binpb"):
            record = await _read_message(operator, entry.path, factory)
            if record is not None:
                if validate_record is not None:
                    validate_record(record, entry.path)
                records.append(record)
    return records


async def _walk_binpb(operator: opendal.AsyncOperator, root: str) -> AsyncIterable[str]:
    queue = [root]
    while queue:
        current = queue.pop(0)
        try:
            entries = sorted(
                [entry async for entry in await operator.list(current)], key=lambda e: e.path
            )
        except opendal.exceptions.NotFound:
            continue
        for entry in entries:
            path = entry.path
            if path == current:
                continue
            if path.endswith("/"):
                queue.append(path)
            elif path.endswith(".binpb"):
                yield path


async def _due_scan_namespaces(
    operator: opendal.AsyncOperator,
    namespace: str,
) -> list[str]:
    if namespace:
        _validate_pointer_key(namespace, "placeholder")
        return [namespace]

    # Namespace directories are routing partitions only. Record identity is
    # still read exclusively from DueTimerEntry payloads and checked against
    # the partition before any authoritative point read is attempted.
    root = f"{STORAGE_ROOT_PREFIX}/"
    try:
        entries = sorted(
            [entry async for entry in await operator.list(root)], key=lambda entry: entry.path
        )
    except opendal.exceptions.NotFound:
        return []

    namespaces: set[str] = set()
    for entry in entries:
        path = entry.path
        if path == root or not path.endswith("/") or not path.startswith(root):
            continue
        relative = path[len(root) :].strip("/")
        if not relative or "/" in relative:
            continue
        try:
            _validate_pointer_key(relative, "placeholder")
        except ValidationError, ValueError:
            _LOGGER.warning("skipping invalid timer-ledger namespace partition %s", path)
            continue
        namespaces.add(relative)
    return sorted(namespaces)


async def _quarantine_invalid_due_entry(
    operator: opendal.AsyncOperator,
    namespace: str,
    ledger_path: str,
    reason: str,
) -> None:
    _LOGGER.warning(
        "quarantining invalid due-timer ledger entry %s: %s",
        ledger_path,
        reason,
    )
    await _quarantine_due_entry(operator, namespace, ledger_path)


async def _quarantine_due_entry(
    operator: opendal.AsyncOperator, namespace: str, ledger_path: str
) -> None:
    try:
        data = bytes(await operator.read(ledger_path))
    except opendal.exceptions.NotFound:
        return
    except Exception:
        _LOGGER.exception("failed to read invalid due-timer ledger entry %s", ledger_path)
        return
    # A quarantine object is diagnostic only. Keep both it and the source
    # deterministic: repeated scans overwrite one copy instead of growing an
    # unbounded series, and—critically—never delete a path that a concurrent
    # timer writer may still be committing.
    digest = hashlib.sha256(ledger_path.encode("utf-8")).hexdigest()[:16]
    path = f"{_due_invalid_root(namespace)}{digest}.binpb"
    try:
        await operator.create_dir(path.rsplit("/", 1)[0] + "/")
        await operator.write(path, data)
    except Exception:
        # Quarantine must never make discovery of unrelated valid timers fail.
        # asyncio cancellation derives from BaseException and still propagates.
        _LOGGER.exception("failed to copy invalid due-timer ledger entry %s", ledger_path)


def _workflow_record_time(record: temporaless_pb2.WorkflowRecord) -> datetime:
    persisted = _persisted_workflow_record_time(record)
    if persisted is not None:
        return persisted
    return datetime.now(UTC)


def _persisted_workflow_record_time(
    record: temporaless_pb2.WorkflowRecord,
) -> datetime | None:
    if record.status in (
        temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        temporaless_pb2.WORKFLOW_STATUS_FAILED,
    ) and record.HasField("completed_at"):
        return _timestamp_from_proto(record.completed_at)
    if record.HasField("created_at"):
        return _timestamp_from_proto(record.created_at)
    return None


def _workflow_run_order_time(
    record: temporaless_pb2.WorkflowRecord,
    record_time: datetime,
) -> datetime:
    if record.HasField("run_order_time"):
        return _timestamp_from_proto(record.run_order_time)
    return record_time


def _should_replace_latest_pointer(
    existing: temporaless_pb2.LatestWorkflowRunPointer,
    incoming_run_order_time: datetime,
    incoming_record_time: datetime,
) -> bool:
    existing_run_order_time = _timestamp_from_proto(existing.run_order_time)
    existing_record_time = _timestamp_from_proto(existing.record_time)
    if existing_run_order_time != incoming_run_order_time:
        return incoming_run_order_time > existing_run_order_time
    return incoming_record_time >= existing_record_time


def _validate_user_scope(
    namespace: str,
    workflow_id: str,
    *,
    allowed_system_workflow_ids: frozenset[str] = frozenset(),
) -> None:
    if namespace.startswith("_"):
        raise ValueError("namespace values starting with '_' are reserved for Temporaless")
    if workflow_id.startswith("_") and workflow_id not in allowed_system_workflow_ids:
        raise ValueError("workflow_id values starting with '_' are reserved for Temporaless")


def _validate_pointer_key(namespace: str, workflow_id: str) -> None:
    validate(
        temporaless_pb2.WorkflowKey(
            namespace=namespace or DEFAULT_NAMESPACE,
            workflow_id=workflow_id,
            run_id="placeholder",
        )
    )
    _validate_user_scope(namespace or DEFAULT_NAMESPACE, workflow_id)


async def _delete_if_exists(operator: opendal.AsyncOperator, path: str) -> bool:
    try:
        if not await operator.exists(path):
            return False
    except opendal.exceptions.NotFound:
        return False
    with contextlib.suppress(opendal.exceptions.NotFound):
        await operator.delete(path)
        return True
    return False


async def send_event(store: EventDeliveryStore, key: EventKey, payload) -> None:
    """Application-facing create-once event delivery.

    Stores without native conditional creation are rejected. ``put_event`` is
    intentionally left as the low-level replace primitive for storage services
    and migration tooling.
    """
    await deliver_event(store, key, payload)


async def deliver_event(
    store: EventDeliveryStore,
    key: EventKey,
    payload,
) -> temporaless_pb2.EventDeliveryDisposition:
    """Atomically establish the first payload for ``key``.

    Identical duplicate deliveries are idempotent and retain the first
    record's ``received_at``. A different payload raises
    :class:`EventDeliveryConflictError`. Stores without native conditional
    creation reject this operation rather than using check-then-write.
    """
    from google.protobuf.any_pb2 import Any
    from google.protobuf.message import Message
    from google.protobuf.timestamp_pb2 import Timestamp

    if not isinstance(payload, Message):
        raise TypeError("event payload must be a protobuf message")
    key.validate()
    capability = await store.event_delivery_capability()
    if capability in (
        temporaless_pb2.EVENT_DELIVERY_CAPABILITY_UNSPECIFIED,
        NO_ATOMIC_EVENT_DELIVERY,
    ):
        raise EventDeliveryUnsupportedError(
            "configured store does not support atomic event creation"
        )
    if capability != CREATE_ONLY_EVENT_DELIVERY:
        raise RunRecordValidationError(
            f"event delivery store returned invalid capability {capability}"
        )

    packed = Any()
    # Canonical bytes make retries of map-bearing messages compare
    # idempotently even when their insertion order differs.
    packed.Pack(payload, deterministic=True)
    received_at = Timestamp()
    received_at.GetCurrentTime()
    record = temporaless_pb2.EventRecord(
        schema_version=EVENT_RECORD_SCHEMA_VERSION,
        key=key.to_proto(),
        payload=packed,
        received_at=received_at,
    )
    _validate_event_delivery_record(record, expected_key=key)
    return _validate_event_delivery_disposition(await store.deliver_event(record))
