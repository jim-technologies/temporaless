import logging
from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from protovalidate import ValidationError, validate

from temporaless.cronscheduler import last_fire_from_runs
from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    CLAIM_RECORD_SCHEMA_VERSION,
    EVENT_RECORD_SCHEMA_VERSION,
    TIMER_RECORD_SCHEMA_VERSION,
    WORKFLOW_RECORD_SCHEMA_VERSION,
    ActivityKey,
    ClaimKey,
    EventKey,
    OpenDALStore,
    TimerKey,
    WorkflowKey,
    _due_entry_path,
    _parse_run_id_fire_time,
)
from temporaless.timerscanner import due_timers
from temporaless.v1 import temporaless_pb2


async def test_latest_pointer_uses_fire_time_not_backfill_write_time(tmp_path) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    await store.put_workflow(
        _workflow(
            "prices:aapl",
            "2026-07-01",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime(2026, 7, 1, tzinfo=UTC),
        )
    )
    await store.put_workflow(
        _workflow(
            "prices:aapl",
            "2020-01-01",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime(2026, 7, 3, tzinfo=UTC),
        )
    )

    last = await last_fire_from_runs(store, "", "prices:aapl", "%Y-%m-%d")

    assert last == datetime(2026, 7, 1, tzinfo=UTC)


async def test_delete_run_validates_all_claim_payloads_before_deleting_any(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    key = WorkflowKey(workflow_id="prices:aapl", run_id="run:one")
    valid_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="a-valid",
    )
    valid = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=valid_key.to_proto(),
        owner_id="owner",
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
        resource_id=key.workflow_id,
        code_version="v1",
    )
    assert await store.try_create_claim(valid)
    misplaced_path = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="z-misplaced",
    )
    misplaced = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=ClaimKey(
            workflow_id=key.workflow_id,
            run_id="run:other",
            claim_id="z-misplaced",
        ).to_proto(),
        owner_id="owner",
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
        resource_id=key.workflow_id,
        code_version="v1",
    )
    await operator.create_dir(misplaced_path.dir_path())
    await operator.write(
        misplaced_path.path(),
        misplaced.SerializeToString(deterministic=True),
    )

    with pytest.raises(ValueError, match="does not match requested workflow run"):
        await store.delete_run(key)

    assert await store.get_claim(valid_key) is not None


@pytest.mark.parametrize("record_kind", ["activity", "timer", "event"])
async def test_delete_run_validates_all_record_payloads_before_deleting_any(
    tmp_path,
    record_kind: str,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    key = WorkflowKey(workflow_id="prices:aapl", run_id="run:one")
    await store.put_workflow(
        _workflow(
            key.workflow_id,
            key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        )
    )
    valid_claim_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="valid",
    )
    assert await store.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=valid_claim_key.to_proto(),
            owner_id="owner",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
            resource_id=key.workflow_id,
            code_version="v1",
        )
    )

    if record_kind == "activity":
        path_key = ActivityKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            activity_id="misplaced",
        )
        record = temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=ActivityKey(
                workflow_id=key.workflow_id,
                run_id="run:other",
                activity_id="misplaced",
            ).to_proto(),
            activity_type="activity:test",
            code_version="v1",
            status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
    elif record_kind == "timer":
        path_key = TimerKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            timer_id="misplaced",
        )
        record = temporaless_pb2.TimerRecord(
            schema_version=TIMER_RECORD_SCHEMA_VERSION,
            key=TimerKey(
                workflow_id=key.workflow_id,
                run_id="run:other",
                timer_id="misplaced",
            ).to_proto(),
            timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
            code_version="v1",
            status=temporaless_pb2.TIMER_STATUS_FIRED,
        )
    else:
        path_key = EventKey(
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            event_id="misplaced",
        )
        record = temporaless_pb2.EventRecord(
            schema_version=EVENT_RECORD_SCHEMA_VERSION,
            key=EventKey(
                workflow_id=key.workflow_id,
                run_id="run:other",
                event_id="misplaced",
            ).to_proto(),
        )

    await operator.create_dir(path_key.dir_path())
    await operator.write(path_key.path(), record.SerializeToString(deterministic=True))

    with pytest.raises(
        ValueError,
        match=rf"{record_kind} payload key does not match requested workflow run",
    ):
        await store.delete_run(key)

    assert await store.get_workflow(key) is not None
    assert await store.get_claim(valid_claim_key) is not None
    assert await operator.exists(path_key.path())


async def test_latest_pointer_defaults_parse_compact_run_ids(tmp_path) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    await store.put_workflow(
        _workflow(
            "prices:aapl",
            "20260703T090000Z",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime(2026, 7, 3, 9, 0, tzinfo=UTC),
        )
    )
    await store.put_workflow(
        _workflow(
            "prices:aapl",
            "2026-06-01",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime(2026, 7, 3, 10, 0, tzinfo=UTC),
        )
    )

    pointer = await store.get_latest_workflow_run("", "prices:aapl")
    last = await last_fire_from_runs(store, "", "prices:aapl", "%Y%m%dT%H%M%SZ")

    assert pointer is not None
    assert pointer.key.run_id == "20260703T090000Z"
    assert last == datetime(2026, 7, 3, 9, 0, tzinfo=UTC)


def test_compact_date_run_ids_require_exact_eight_digits() -> None:
    assert _parse_run_id_fire_time("100056", ("%Y%m%d",)) is None
    assert _parse_run_id_fire_time("202661", ("%Y%m%d",)) is None
    assert _parse_run_id_fire_time("20260601", ("%Y%m%d",)) == datetime(2026, 6, 1, tzinfo=UTC)


async def test_underscore_prefixed_workflow_id_is_reserved(tmp_path) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    record = _workflow("_due", "r1", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)

    with pytest.raises((ValueError, ValidationError)):
        await store.put_workflow(record)


def test_proto_validation_reserves_underscore_workflow_keys() -> None:
    with pytest.raises(ValidationError):
        validate(
            temporaless_pb2.WorkflowKey(
                namespace="default",
                workflow_id="_due",
                run_id="r1",
            )
        )


async def test_due_timers_quarantines_invalid_ledger_blob(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    path = "temporaless/v2/default/_due/20000101T000000.000000Z/_due/run/timer.binpb"
    await operator.create_dir(path.rsplit("/", 1)[0] + "/")
    await operator.write(path, b"not a due-timer entry")

    assert await due_timers(store, datetime.now(UTC)) == []
    assert not await operator.exists(path)
    quarantined = [
        entry async for entry in await operator.list("temporaless/v2/default/_due_invalid/")
    ]
    assert quarantined


async def test_due_timers_skips_invalid_entries_and_still_returns_valid_due_timer(
    tmp_path, caplog
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    await store.put_workflow(
        _workflow("prices:timer", "2026-07-01", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    await store.put_timer(
        _timer(
            "prices:timer",
            "2026-07-01",
            "wait",
            temporaless_pb2.TIMER_STATUS_SCHEDULED,
            datetime.now(UTC) - timedelta(seconds=1),
        )
    )
    empty_path = "temporaless/v2/default/_due/20000101T000000.000000Z/empty/run/timer.binpb"
    reserved_path = "temporaless/v2/default/_due/20000101T000001.000000Z/_bad/run/timer.binpb"
    await operator.create_dir(empty_path.rsplit("/", 1)[0] + "/")
    await operator.write(empty_path, temporaless_pb2.DueTimerEntry().SerializeToString())
    fire_at = Timestamp()
    fire_at.FromDatetime(datetime(2000, 1, 1, tzinfo=UTC))
    await operator.create_dir(reserved_path.rsplit("/", 1)[0] + "/")
    await operator.write(
        reserved_path,
        temporaless_pb2.DueTimerEntry(
            key=temporaless_pb2.TimerKey(
                namespace="default",
                workflow_id="_bad",
                run_id="run",
                timer_id="timer",
            ),
            workflow_key=temporaless_pb2.WorkflowKey(
                namespace="default",
                workflow_id="_bad",
                run_id="run",
            ),
            fire_at=fire_at,
        ).SerializeToString(),
    )

    with caplog.at_level(logging.WARNING, logger="temporaless.storage"):
        due = await due_timers(store, datetime.now(UTC))

    assert [timer.key.timer_id for timer in due] == ["wait"]
    assert "skipping invalid due-timer ledger entry" in caplog.text
    assert not await operator.exists(empty_path)
    assert not await operator.exists(reserved_path)


async def test_put_timer_writes_due_ledger_before_timer_record(tmp_path) -> None:
    operator = _RecordingOperator(opendal.AsyncOperator("fs", root=str(tmp_path)))
    store = OpenDALStore(operator)
    await store.put_workflow(
        _workflow("prices:timer", "2026-07-01", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    operator.writes.clear()

    await store.put_timer(
        _timer(
            "prices:timer",
            "2026-07-01",
            "wait",
            temporaless_pb2.TIMER_STATUS_SCHEDULED,
            datetime.now(UTC) + timedelta(minutes=5),
        )
    )

    due_write = next(index for index, path in enumerate(operator.writes) if "/_due/" in path)
    timer_write = next(index for index, path in enumerate(operator.writes) if "/timer/" in path)
    assert due_write < timer_write


async def test_orphan_due_ledger_entry_is_pruned_harmlessly(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    await store.put_workflow(
        _workflow("prices:timer", "2026-07-01", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    timer = _timer(
        "prices:timer",
        "2026-07-01",
        "wait",
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        datetime.now(UTC) - timedelta(seconds=1),
    )
    await store.put_timer(timer)
    timer_key = TimerKey(workflow_id="prices:timer", run_id="2026-07-01", timer_id="wait")
    ledger_path = _due_entry_path(timer_key, timer.fire_at.ToDatetime().replace(tzinfo=UTC))
    await operator.delete(timer_key.path())

    assert await due_timers(store, datetime.now(UTC)) == []
    assert not await operator.exists(ledger_path)


class _RecordingOperator:
    def __init__(self, inner: opendal.AsyncOperator) -> None:
        self._inner = inner
        self.writes: list[str] = []

    async def create_dir(self, path: str):
        return await self._inner.create_dir(path)

    async def write(self, path: str, *args, **kwargs):
        self.writes.append(path)
        return await self._inner.write(path, *args, **kwargs)

    async def read(self, path: str):
        return await self._inner.read(path)

    async def list(self, path: str):
        return await self._inner.list(path)

    async def exists(self, path: str):
        return await self._inner.exists(path)

    async def delete(self, path: str):
        return await self._inner.delete(path)


def _workflow(
    workflow_id: str,
    run_id: str,
    status: temporaless_pb2.WorkflowStatus,
    *,
    completed_at: datetime | None = None,
) -> temporaless_pb2.WorkflowRecord:
    created = Timestamp()
    created.FromDatetime(datetime(2026, 7, 1, tzinfo=UTC))
    record = temporaless_pb2.WorkflowRecord(
        schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
        key=temporaless_pb2.WorkflowKey(
            namespace="default",
            workflow_id=workflow_id,
            run_id=run_id,
        ),
        workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
        code_version="test",
        status=status,
        created_at=created,
    )
    if completed_at is not None:
        completed = Timestamp()
        completed.FromDatetime(completed_at)
        record.completed_at.CopyFrom(completed)
    return record


def _timer(
    workflow_id: str,
    run_id: str,
    timer_id: str,
    status: temporaless_pb2.TimerStatus,
    fire_at: datetime,
) -> temporaless_pb2.TimerRecord:
    fire = Timestamp()
    fire.FromDatetime(fire_at)
    created = Timestamp()
    created.FromDatetime(datetime(2026, 7, 1, tzinfo=UTC))
    return temporaless_pb2.TimerRecord(
        schema_version=TIMER_RECORD_SCHEMA_VERSION,
        key=TimerKey(workflow_id=workflow_id, run_id=run_id, timer_id=timer_id).to_proto(),
        timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
        code_version="test",
        status=status,
        fire_at=fire,
        created_at=created,
    )
