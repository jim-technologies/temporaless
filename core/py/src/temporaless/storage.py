from __future__ import annotations

import contextlib
from collections.abc import AsyncIterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable

import opendal
from protovalidate import validate

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
        return (
            f"temporaless/v1/namespaces/{self.namespace}/workflows/{self.workflow_id}"
            f"/runs/{self.run_id}/workflow.binpb"
        )

    def dir_path(self) -> str:
        self.validate()
        return (
            f"temporaless/v1/namespaces/{self.namespace}/workflows/{self.workflow_id}"
            f"/runs/{self.run_id}/"
        )

    def validate(self) -> None:
        validate(
            temporaless_pb2.WorkflowKey(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
            )
        )


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
            f"temporaless/v1/namespaces/{self.namespace}/workflows/{self.workflow_id}"
            f"/runs/{self.run_id}/activities/{self.activity_id}.binpb"
        )

    def dir_path(self) -> str:
        self.validate()
        return (
            f"temporaless/v1/namespaces/{self.namespace}/workflows/{self.workflow_id}"
            f"/runs/{self.run_id}/activities/"
        )

    def validate(self) -> None:
        validate(
            temporaless_pb2.ActivityKey(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
                activity_id=self.activity_id,
            )
        )


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
            f"temporaless/v1/namespaces/{self.namespace}/workflows/{self.workflow_id}"
            f"/runs/{self.run_id}/timers/{self.timer_id}.binpb"
        )

    def dir_path(self) -> str:
        self.validate()
        return (
            f"temporaless/v1/namespaces/{self.namespace}/workflows/{self.workflow_id}"
            f"/runs/{self.run_id}/timers/"
        )

    def validate(self) -> None:
        validate(
            temporaless_pb2.TimerKey(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
                timer_id=self.timer_id,
            )
        )


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
            f"temporaless/v1/namespaces/{self.namespace}/workflows/{self.workflow_id}"
            f"/runs/{self.run_id}/events/{self.event_id}.binpb"
        )

    def dir_path(self) -> str:
        self.validate()
        return (
            f"temporaless/v1/namespaces/{self.namespace}/workflows/{self.workflow_id}"
            f"/runs/{self.run_id}/events/"
        )

    def validate(self) -> None:
        validate(
            temporaless_pb2.EventKey(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
                event_id=self.event_id,
            )
        )


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
            f"temporaless/v1/namespaces/{self.namespace}/workflows/{self.workflow_id}"
            f"/runs/{self.run_id}/claims/{self.claim_id}.binpb"
        )

    def dir_path(self) -> str:
        self.validate()
        return (
            f"temporaless/v1/namespaces/{self.namespace}/workflows/{self.workflow_id}"
            f"/runs/{self.run_id}/claims/"
        )

    def validate(self) -> None:
        validate(
            temporaless_pb2.ClaimKey(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
                claim_id=self.claim_id,
            )
        )


class ActivityStore(Protocol):
    async def get_activity(self, key: ActivityKey) -> temporaless_pb2.ActivityRecord | None: ...

    async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None: ...

    async def list_activities(self, key: WorkflowKey) -> list[temporaless_pb2.ActivityRecord]: ...

    async def delete_activity(self, key: ActivityKey) -> bool: ...


class WorkflowStore(Protocol):
    async def get_workflow(self, key: WorkflowKey) -> temporaless_pb2.WorkflowRecord | None: ...

    async def put_workflow(self, record: temporaless_pb2.WorkflowRecord) -> None: ...

    async def list_workflows(
        self,
        namespace: str,
        workflow_id: str,
        status: temporaless_pb2.WorkflowStatus,
    ) -> list[temporaless_pb2.WorkflowRecord]: ...

    async def delete_workflow(self, key: WorkflowKey) -> bool: ...


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


@dataclass(frozen=True)
class DueTimer:
    """A SCHEDULED timer that's due, paired with the workflow that owns it."""

    key: TimerKey
    record: temporaless_pb2.TimerRecord
    workflow: temporaless_pb2.WorkflowRecord


class Store(ActivityStore, EventStore, TimerStore, WorkflowStore, Protocol):
    async def sweep(self, namespace: str, now: datetime, max_age: timedelta) -> int:
        """Delete every COMPLETED workflow run whose ``completed_at`` is older
        than ``now - max_age``. Activities, timers, and events under each run
        are deleted before the workflow record itself.

        Returns the number of runs deleted. Idempotent — calling twice is a
        no-op for the runs already removed.
        """
        ...

    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        """Return SCHEDULED timer records whose ``fire_at <= now`` and whose
        parent workflow is still IN_PROGRESS.

        Operators run this on a minute cron and re-invoke the workflow handler
        for each entry to resume a durable sleep.
        """
        ...


class OpenDALStore:
    def __init__(self, operator: opendal.AsyncOperator) -> None:
        self._operator = operator

    async def claim_capability(self) -> temporaless_pb2.ClaimCapability:
        return CREATE_ONLY_CLAIMS

    async def get_activity(self, key: ActivityKey) -> temporaless_pb2.ActivityRecord | None:
        path = key.path()
        if not await self._operator.exists(path):
            return None
        record = temporaless_pb2.ActivityRecord()
        record.ParseFromString(bytes(await self._operator.read(path)))
        return record

    async def get_workflow(self, key: WorkflowKey) -> temporaless_pb2.WorkflowRecord | None:
        path = key.path()
        if not await self._operator.exists(path):
            return None
        record = temporaless_pb2.WorkflowRecord()
        record.ParseFromString(bytes(await self._operator.read(path)))
        return record

    async def get_timer(self, key: TimerKey) -> temporaless_pb2.TimerRecord | None:
        path = key.path()
        if not await self._operator.exists(path):
            return None
        record = temporaless_pb2.TimerRecord()
        record.ParseFromString(bytes(await self._operator.read(path)))
        return record

    async def get_event(self, key: EventKey) -> temporaless_pb2.EventRecord | None:
        path = key.path()
        if not await self._operator.exists(path):
            return None
        record = temporaless_pb2.EventRecord()
        record.ParseFromString(bytes(await self._operator.read(path)))
        return record

    async def get_claim(self, key: ClaimKey) -> temporaless_pb2.ClaimRecord | None:
        path = key.path()
        if not await self._operator.exists(path):
            return None
        record = temporaless_pb2.ClaimRecord()
        record.ParseFromString(bytes(await self._operator.read(path)))
        return record

    async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None:
        key = activity_key_from_proto(record.key)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))

    async def put_workflow(self, record: temporaless_pb2.WorkflowRecord) -> None:
        key = workflow_key_from_proto(record.key)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))

    async def put_timer(self, record: temporaless_pb2.TimerRecord) -> None:
        key = timer_key_from_proto(record.key)
        await self._operator.create_dir(key.dir_path())
        await self._operator.write(key.path(), record.SerializeToString(deterministic=True))

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

    async def list_workflows(
        self,
        namespace: str,
        workflow_id: str,
        status: temporaless_pb2.WorkflowStatus,
    ) -> list[temporaless_pb2.WorkflowRecord]:
        root = "temporaless/v1/namespaces/"
        if namespace:
            root = f"{root}{namespace}/"
            if workflow_id:
                root = f"{root}workflows/{workflow_id}/runs/"
        # Defense-in-depth: when the path can't fully encode the filter
        # (empty namespace + non-empty workflow_id), filter in code too.
        match_workflow_id = workflow_id if not namespace and workflow_id else ""
        records: list[temporaless_pb2.WorkflowRecord] = []
        async for path in _walk_binpb(self._operator, root):
            if not path.endswith("/workflow.binpb"):
                continue
            key = _parse_workflow_path(path)
            if key is None:
                continue
            if match_workflow_id and key.workflow_id != match_workflow_id:
                continue
            record = await self.get_workflow(key)
            if record is None:
                continue
            if status != temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED and record.status != status:
                continue
            records.append(record)
        return records

    async def delete_workflow(self, key: WorkflowKey) -> bool:
        return await _delete_if_exists(self._operator, key.path())

    async def list_activities(self, key: WorkflowKey) -> list[temporaless_pb2.ActivityRecord]:
        dir_path = ActivityKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            activity_id="placeholder",
            namespace=key.namespace,
        ).dir_path()
        records: list[temporaless_pb2.ActivityRecord] = []
        try:
            entries = [entry async for entry in await self._operator.list(dir_path)]
        except opendal.exceptions.NotFound:
            return records
        for entry in entries:
            path = entry.path
            if path == dir_path or not path.endswith(".binpb"):
                continue
            activity_id = path.removeprefix(dir_path).removesuffix(".binpb")
            record = await self.get_activity(
                ActivityKey(
                    workflow_id=key.workflow_id,
                    run_id=key.run_id,
                    activity_id=activity_id,
                    namespace=key.namespace,
                )
            )
            if record is not None:
                records.append(record)
        return records

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
        records: list[temporaless_pb2.TimerRecord] = []
        try:
            entries = [entry async for entry in await self._operator.list(dir_path)]
        except opendal.exceptions.NotFound:
            return records
        for entry in entries:
            path = entry.path
            if path == dir_path or not path.endswith(".binpb"):
                continue
            timer_id = path.removeprefix(dir_path).removesuffix(".binpb")
            record = await self.get_timer(
                TimerKey(
                    workflow_id=key.workflow_id,
                    run_id=key.run_id,
                    timer_id=timer_id,
                    namespace=key.namespace,
                )
            )
            if record is None:
                continue
            if status != temporaless_pb2.TIMER_STATUS_UNSPECIFIED and record.status != status:
                continue
            records.append(record)
        return records

    async def delete_timer(self, key: TimerKey) -> bool:
        return await _delete_if_exists(self._operator, key.path())

    async def list_events(self, key: WorkflowKey) -> list[temporaless_pb2.EventRecord]:
        dir_path = EventKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            event_id="placeholder",
            namespace=key.namespace,
        ).dir_path()
        records: list[temporaless_pb2.EventRecord] = []
        try:
            entries = [entry async for entry in await self._operator.list(dir_path)]
        except opendal.exceptions.NotFound:
            return records
        for entry in entries:
            path = entry.path
            if path == dir_path or not path.endswith(".binpb"):
                continue
            event_id = path.removeprefix(dir_path).removesuffix(".binpb")
            record = await self.get_event(
                EventKey(
                    workflow_id=key.workflow_id,
                    run_id=key.run_id,
                    event_id=event_id,
                    namespace=key.namespace,
                )
            )
            if record is not None:
                records.append(record)
        return records

    async def delete_event(self, key: EventKey) -> bool:
        return await _delete_if_exists(self._operator, key.path())

    async def sweep(self, namespace: str, now: datetime, max_age: timedelta) -> int:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if max_age <= timedelta(0):
            raise ValueError("max_age must be > 0")

        cutoff = now - max_age
        completed = await self.list_workflows(
            namespace, "", temporaless_pb2.WORKFLOW_STATUS_COMPLETED
        )
        deleted = 0
        for record in completed:
            completed_at = record.completed_at.ToDatetime().replace(tzinfo=UTC)
            if completed_at > cutoff:
                continue
            workflow_key = workflow_key_from_proto(record.key)
            for activity in await self.list_activities(workflow_key):
                await self.delete_activity(activity_key_from_proto(activity.key))
            for timer in await self.list_timers(
                workflow_key, temporaless_pb2.TIMER_STATUS_UNSPECIFIED
            ):
                await self.delete_timer(timer_key_from_proto(timer.key))
            for event in await self.list_events(workflow_key):
                await self.delete_event(event_key_from_proto(event.key))
            await self.delete_workflow(workflow_key)
            deleted += 1
        return deleted

    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")

        in_flight = await self.list_workflows(
            namespace, "", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
        )
        due: list[DueTimer] = []
        for workflow in in_flight:
            key = workflow_key_from_proto(workflow.key)
            timers = await self.list_timers(key, temporaless_pb2.TIMER_STATUS_SCHEDULED)
            for timer in timers:
                fire_at = timer.fire_at.ToDatetime().replace(tzinfo=UTC)
                if fire_at > now:
                    continue
                due.append(
                    DueTimer(
                        key=timer_key_from_proto(timer.key),
                        record=timer,
                        workflow=workflow,
                    )
                )
        return due


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


async def _walk_binpb(operator: opendal.AsyncOperator, root: str) -> AsyncIterable[str]:
    queue = [root]
    while queue:
        current = queue.pop(0)
        try:
            entries = [entry async for entry in await operator.list(current)]
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


def _parse_workflow_path(path: str) -> WorkflowKey | None:
    parts = path.split("/")
    if len(parts) != 9:
        return None
    if parts[0] != "temporaless" or parts[1] != "v1" or parts[2] != "namespaces":
        return None
    if parts[4] != "workflows" or parts[6] != "runs" or parts[8] != "workflow.binpb":
        return None
    return WorkflowKey(namespace=parts[3], workflow_id=parts[5], run_id=parts[7])


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
