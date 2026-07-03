from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import threading
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
NO_CLAIMS = temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS
CREATE_ONLY_CLAIMS = temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS
CAS_CLAIMS = temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS
ClaimCapability = temporaless_pb2.ClaimCapability

# V2 storage is flat and constructed-only. Keys are never inverted back into
# record identity; routines that need identity read the protobuf payload.
STORAGE_ROOT_PREFIX = "temporaless/v2"
_DEFAULT_LATEST_RUN_ID_FORMATS = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
    "%Y%m%dT%H%M%SZ",
    "%Y%m%dT%H%M%S",
    "%Y%m%d",
)
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


def _timestamp_sort_key(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).strftime("%Y%m%dT%H%M%S.%fZ")


def _timestamp_from_proto(value) -> datetime:
    return value.ToDatetime().replace(tzinfo=UTC)


def _due_entry_path(key: TimerKey, fire_at: datetime) -> str:
    return (
        f"{_due_root(key.namespace)}{_timestamp_sort_key(fire_at)}/"
        f"{key.workflow_id}/{key.run_id}/{key.timer_id}.binpb"
    )


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
class ClaimStore(Protocol):
    async def claim_capability(self) -> temporaless_pb2.ClaimCapability: ...

    async def get_claim(self, key: ClaimKey) -> temporaless_pb2.ClaimRecord | None: ...

    async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool: ...

    async def delete_claim(self, key: ClaimKey) -> bool:
        """Idempotently release a held claim. Returns True when the claim
        existed and was removed, False when it was already absent. Used by
        the runtime to release concurrency-key slots when a workflow reaches
        a terminal status or returns a pending error."""
        ...


@dataclass(frozen=True)
class DueTimer:
    """A SCHEDULED timer that's due, paired with the workflow that owns it."""

    key: TimerKey
    record: temporaless_pb2.TimerRecord
    workflow: temporaless_pb2.WorkflowRecord


class Store(ActivityStore, EventStore, TimerStore, WorkflowStore, Protocol):
    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        """Return SCHEDULED timer records whose ``fire_at <= now`` and whose
        parent workflow is still IN_PROGRESS.

        Bucket implementations serve this from the compact due-timer ledger,
        not by walking workflow records.
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
    def __init__(
        self,
        operator: opendal.AsyncOperator,
        *,
        latest_run_id_formats: tuple[str, ...] | None = None,
    ) -> None:
        self._operator = operator
        self._latest_pointer_lock = threading.Lock()
        self._latest_run_id_formats = latest_run_id_formats or _DEFAULT_LATEST_RUN_ID_FORMATS

    async def claim_capability(self) -> temporaless_pb2.ClaimCapability:
        return CREATE_ONLY_CLAIMS

    async def get_activity(self, key: ActivityKey) -> temporaless_pb2.ActivityRecord | None:
        return await _read_message(self._operator, key.path(), temporaless_pb2.ActivityRecord)

    async def get_workflow(self, key: WorkflowKey) -> temporaless_pb2.WorkflowRecord | None:
        return await _read_message(self._operator, key.path(), temporaless_pb2.WorkflowRecord)

    async def get_latest_workflow_run(
        self, namespace: str, workflow_id: str
    ) -> temporaless_pb2.LatestWorkflowRunPointer | None:
        _validate_pointer_key(namespace or DEFAULT_NAMESPACE, workflow_id)
        return await _read_message(
            self._operator,
            _latest_pointer_path(namespace or DEFAULT_NAMESPACE, workflow_id),
            temporaless_pb2.LatestWorkflowRunPointer,
        )

    async def get_timer(self, key: TimerKey) -> temporaless_pb2.TimerRecord | None:
        return await _read_message(self._operator, key.path(), temporaless_pb2.TimerRecord)

    async def get_event(self, key: EventKey) -> temporaless_pb2.EventRecord | None:
        return await _read_message(self._operator, key.path(), temporaless_pb2.EventRecord)

    async def get_claim(self, key: ClaimKey) -> temporaless_pb2.ClaimRecord | None:
        return await _read_message(self._operator, key.path(), temporaless_pb2.ClaimRecord)

    async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None:
        key = activity_key_from_proto(record.key)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))

    async def put_workflow(self, record: temporaless_pb2.WorkflowRecord) -> None:
        key = workflow_key_from_proto(record.key)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))
        if record.status in (
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            temporaless_pb2.WORKFLOW_STATUS_FAILED,
        ):
            await self._put_latest_pointer(record)

    async def put_timer(self, record: temporaless_pb2.TimerRecord) -> None:
        key = timer_key_from_proto(record.key)
        previous = await self.get_timer(key)
        if record.status == temporaless_pb2.TIMER_STATUS_SCHEDULED:
            await self._put_due_entry(record)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))
        if (
            previous is not None
            and previous.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
            and (
                record.status != temporaless_pb2.TIMER_STATUS_SCHEDULED
                or _due_entry_path(key, _timestamp_from_proto(previous.fire_at))
                != _due_entry_path(key, _timestamp_from_proto(record.fire_at))
            )
        ):
            await self._delete_due_entry(previous)

    async def put_event(self, record: temporaless_pb2.EventRecord) -> None:
        key = event_key_from_proto(record.key)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))

    async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
        key = claim_key_from_proto(record.key)
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
        deleted = await _delete_if_exists(self._operator, key.path())
        if deleted:
            pointer = await self.get_latest_workflow_run(key.namespace, key.workflow_id)
            if pointer is not None and workflow_key_from_proto(pointer.key) == key:
                await _delete_if_exists(
                    self._operator, _latest_pointer_path(key.namespace, key.workflow_id)
                )
        return deleted

    async def delete_run(self, key: WorkflowKey) -> int:
        deleted = 0
        for activity in await self.list_activities(key):
            if await self.delete_activity(activity_key_from_proto(activity.key)):
                deleted += 1
        for timer in await self.list_timers(key, temporaless_pb2.TIMER_STATUS_UNSPECIFIED):
            if await self.delete_timer(timer_key_from_proto(timer.key)):
                deleted += 1
        for event in await self.list_events(key):
            if await self.delete_event(event_key_from_proto(event.key)):
                deleted += 1
        for claim in await self._list_claims(key):
            if await self.delete_claim(claim_key_from_proto(claim.key)):
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
        return await _list_messages(self._operator, dir_path, temporaless_pb2.ActivityRecord)

    async def delete_activity(self, key: ActivityKey) -> bool:
        return await _delete_if_exists(self._operator, key.path())

    async def list_timers(
        self,
        key: WorkflowKey,
        status: temporaless_pb2.TimerStatus,
    ) -> list[temporaless_pb2.TimerRecord]:
        dir_path = TimerKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            timer_id="placeholder",
            namespace=key.namespace,
        ).dir_path()
        records = await _list_messages(self._operator, dir_path, temporaless_pb2.TimerRecord)
        if status == temporaless_pb2.TIMER_STATUS_UNSPECIFIED:
            return records
        return [record for record in records if record.status == status]

    async def delete_timer(self, key: TimerKey) -> bool:
        existing = await self.get_timer(key)
        deleted = await _delete_if_exists(self._operator, key.path())
        if (
            deleted
            and existing is not None
            and existing.status == temporaless_pb2.TIMER_STATUS_SCHEDULED
        ):
            await self._delete_due_entry(existing)
        return deleted

    async def list_events(self, key: WorkflowKey) -> list[temporaless_pb2.EventRecord]:
        dir_path = EventKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            event_id="placeholder",
            namespace=key.namespace,
        ).dir_path()
        return await _list_messages(self._operator, dir_path, temporaless_pb2.EventRecord)

    async def delete_event(self, key: EventKey) -> bool:
        return await _delete_if_exists(self._operator, key.path())

    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")

        namespace = namespace or DEFAULT_NAMESPACE
        _validate_pointer_key(namespace, "placeholder")
        root = _due_root(namespace)
        now_sort = _timestamp_sort_key(now)
        due: list[DueTimer] = []
        try:
            entries = sorted(
                [entry async for entry in await self._operator.list(root)], key=lambda e: e.path
            )
        except opendal.exceptions.NotFound:
            return due

        for entry in entries:
            path = entry.path
            if path == root or not path.endswith("/"):
                continue
            sort_key = _due_sort_key_from_path(root, path)
            if sort_key is None:
                continue
            if sort_key > now_sort:
                break
            async for ledger_path in _walk_binpb(self._operator, path):
                try:
                    ledger = await _read_message(
                        self._operator, ledger_path, temporaless_pb2.DueTimerEntry
                    )
                    if ledger is None:
                        continue
                    ledger_fire_at = _timestamp_from_proto(ledger.fire_at)
                    timer_key = timer_key_from_proto(ledger.key)
                    workflow_key = workflow_key_from_proto(ledger.workflow_key)
                    timer_key.validate()
                    workflow_key.validate()
                except (DecodeError, ValidationError, ValueError) as exc:
                    _LOGGER.warning(
                        "skipping invalid due-timer ledger entry %s: %s",
                        ledger_path,
                        exc,
                    )
                    await _quarantine_due_entry(self._operator, namespace, ledger_path)
                    continue
                if ledger_fire_at > now:
                    continue
                timer = await self.get_timer(timer_key)
                workflow = await self.get_workflow(workflow_key)
                if timer is None or timer.status != temporaless_pb2.TIMER_STATUS_SCHEDULED:
                    await _delete_if_exists(self._operator, ledger_path)
                    continue
                timer_fire_at = _timestamp_from_proto(timer.fire_at)
                if timer_fire_at != ledger_fire_at:
                    await _delete_if_exists(self._operator, ledger_path)
                    continue
                if (
                    workflow is None
                    or workflow.status != temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
                ):
                    await _delete_if_exists(self._operator, ledger_path)
                    continue
                due.append(DueTimer(key=timer_key, record=timer, workflow=workflow))
        return due

    async def _list_claims(self, key: WorkflowKey) -> list[temporaless_pb2.ClaimRecord]:
        dir_path = ClaimKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            claim_id="placeholder",
            namespace=key.namespace,
        ).dir_path()
        return await _list_messages(self._operator, dir_path, temporaless_pb2.ClaimRecord)

    async def _put_latest_pointer(self, record: temporaless_pb2.WorkflowRecord) -> None:
        await asyncio.to_thread(self._latest_pointer_lock.acquire)
        try:
            key = workflow_key_from_proto(record.key)
            record_time = _workflow_record_time(record)
            existing = await self.get_latest_workflow_run(key.namespace, key.workflow_id)
            if existing is not None and not _should_replace_latest_pointer(
                existing, key.run_id, record_time, self._latest_run_id_formats
            ):
                return

            pointer = temporaless_pb2.LatestWorkflowRunPointer(
                key=record.key,
                status=record.status,
            )
            pointer.record_time.FromDatetime(record_time)
            pointer.updated_at.GetCurrentTime()
            path = _latest_pointer_path(key.namespace, key.workflow_id)
            await self._operator.create_dir(path.rsplit("/", 1)[0] + "/")
            await self._operator.write(path, pointer.SerializeToString(deterministic=True))
        finally:
            self._latest_pointer_lock.release()

    async def _put_due_entry(self, record: temporaless_pb2.TimerRecord) -> None:
        key = timer_key_from_proto(record.key)
        fire_at = _timestamp_from_proto(record.fire_at)
        workflow_key = WorkflowKey(
            namespace=key.namespace,
            workflow_id=key.workflow_id,
            run_id=key.run_id,
        )
        entry = temporaless_pb2.DueTimerEntry(
            key=record.key,
            workflow_key=workflow_key.to_proto(),
            fire_at=record.fire_at,
        )
        path = _due_entry_path(key, fire_at)
        await self._operator.create_dir(path.rsplit("/", 1)[0] + "/")
        await self._operator.write(path, entry.SerializeToString(deterministic=True))

    async def _delete_due_entry(self, record: temporaless_pb2.TimerRecord) -> None:
        key = timer_key_from_proto(record.key)
        await _delete_if_exists(
            self._operator, _due_entry_path(key, _timestamp_from_proto(record.fire_at))
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


async def _quarantine_due_entry(
    operator: opendal.AsyncOperator, namespace: str, ledger_path: str
) -> None:
    try:
        data = bytes(await operator.read(ledger_path))
    except opendal.exceptions.NotFound:
        return
    digest = hashlib.sha256(ledger_path.encode("utf-8")).hexdigest()[:16]
    path = f"{_due_invalid_root(namespace)}{_timestamp_sort_key(datetime.now(UTC))}/{digest}.binpb"
    await operator.create_dir(path.rsplit("/", 1)[0] + "/")
    await operator.write(path, data)
    await _delete_if_exists(operator, ledger_path)


def _due_sort_key_from_path(root: str, path: str) -> str | None:
    if not path.startswith(root):
        return None
    rest = path[len(root) :].strip("/")
    if not rest:
        return None
    return rest.split("/", 1)[0]


def _workflow_record_time(record: temporaless_pb2.WorkflowRecord) -> datetime:
    if record.status in (
        temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        temporaless_pb2.WORKFLOW_STATUS_FAILED,
    ) and record.HasField("completed_at"):
        return _timestamp_from_proto(record.completed_at)
    if record.HasField("created_at"):
        return _timestamp_from_proto(record.created_at)
    return datetime.now(UTC)


def _parse_run_id_fire_time(run_id: str, run_id_formats: tuple[str, ...]) -> datetime | None:
    for run_id_format in run_id_formats:
        if run_id_format == "%Y%m%d" and (len(run_id) != 8 or not run_id.isdigit()):
            continue
        try:
            parsed = datetime.strptime(run_id, run_id_format)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def _should_replace_latest_pointer(
    existing: temporaless_pb2.LatestWorkflowRunPointer,
    incoming_run_id: str,
    incoming_record_time: datetime,
    run_id_formats: tuple[str, ...],
) -> bool:
    existing_record_time = _timestamp_from_proto(existing.record_time)
    existing_fire_time = _parse_run_id_fire_time(existing.key.run_id, run_id_formats)
    incoming_fire_time = _parse_run_id_fire_time(incoming_run_id, run_id_formats)
    if existing_fire_time is not None and incoming_fire_time is not None:
        if existing_fire_time != incoming_fire_time:
            return incoming_fire_time > existing_fire_time
        return incoming_record_time >= existing_record_time
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


async def send_event(store: EventStore, key: EventKey, payload) -> None:
    """Pack payload as Any, build EventRecord with current time, write via store.

    Use from external services (webhooks, approval handlers) to deliver a signal
    to a workflow waiting via Workflow.wait_event.
    """
    from google.protobuf.any_pb2 import Any
    from google.protobuf.message import Message
    from google.protobuf.timestamp_pb2 import Timestamp

    if not isinstance(payload, Message):
        raise TypeError("event payload must be a protobuf message")

    packed = Any()
    packed.Pack(payload)
    received_at = Timestamp()
    received_at.GetCurrentTime()

    await store.put_event(
        temporaless_pb2.EventRecord(
            schema_version=EVENT_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            payload=packed,
            received_at=received_at,
        )
    )
