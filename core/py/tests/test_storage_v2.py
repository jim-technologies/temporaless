import asyncio
import logging
from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from protovalidate import ValidationError, validate

import temporaless.storage as storage_module
from temporaless.cronscheduler import last_fire_from_runs
from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    CLAIM_RECORD_SCHEMA_VERSION,
    EVENT_RECORD_SCHEMA_VERSION,
    NO_CLAIMS,
    TIMER_RECORD_SCHEMA_VERSION,
    WORKFLOW_RECORD_SCHEMA_VERSION,
    ActivityKey,
    ClaimKey,
    EventKey,
    OpenDALStore,
    RunRecordValidationError,
    TimerKey,
    WorkflowKey,
    _due_entry_path,
    _latest_pointer_path,
)
from temporaless.timerscanner import due_timers
from temporaless.v1 import temporaless_pb2


def test_opendal_store_rejects_incomplete_point_backend() -> None:
    operator = opendal.AsyncOperator("http", endpoint="https://example.com")

    with pytest.raises(
        ValueError,
        match="required point-store operations: write, delete, list, create_dir",
    ):
        OpenDALStore(operator)


async def test_opendal_store_reports_no_claims_without_conditional_writes() -> None:
    store = OpenDALStore(opendal.AsyncOperator("webdav", endpoint="https://example.com"))

    assert await store.claim_capability() == NO_CLAIMS
    with pytest.raises(RuntimeError, match="atomic create-if-absent"):
        await store.try_create_claim(temporaless_pb2.ClaimRecord())


async def test_latest_pointer_uses_fire_time_not_backfill_write_time(tmp_path) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    await store.put_workflow(
        _workflow(
            "prices:aapl",
            "2026-07-01",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime(2026, 7, 1, tzinfo=UTC),
            run_order_time=datetime(2026, 7, 1, tzinfo=UTC),
        )
    )
    await store.put_workflow(
        _workflow(
            "prices:aapl",
            "2020-01-01",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime(2026, 7, 3, tzinfo=UTC),
            run_order_time=datetime(2020, 1, 1, tzinfo=UTC),
        )
    )

    last = await last_fire_from_runs(store, "", "prices:aapl")

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


@pytest.mark.parametrize("record_kind", ["workflow", "activity", "timer", "event", "claim"])
@pytest.mark.parametrize("corruption", ["key", "schema"])
async def test_point_reads_reject_misplaced_or_wrong_schema_records(
    tmp_path,
    record_kind: str,
    corruption: str,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    path_key = _record_key(record_kind, "run:path", "path")
    payload_key = (
        _record_key(record_kind, "run:payload", "payload") if corruption == "key" else path_key
    )
    record = _record_for_kind(record_kind, payload_key)
    if corruption == "schema":
        record.schema_version = temporaless_pb2.RECORD_SCHEMA_VERSION_UNSPECIFIED
    await operator.create_dir(path_key.dir_path())
    await operator.write(path_key.path(), record.SerializeToString(deterministic=True))

    with pytest.raises(
        RunRecordValidationError,
        match="requested key" if corruption == "key" else "schema_version",
    ):
        await _get_record(store, record_kind, path_key)

    assert await operator.exists(path_key.path())


@pytest.mark.parametrize("record_kind", ["activity", "timer", "event", "claim"])
async def test_run_listings_reject_payload_identity_that_disagrees_with_object_location(
    tmp_path,
    record_kind: str,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    run = WorkflowKey(workflow_id="prices:location", run_id="run:one")
    path_key = _record_key(record_kind, run.run_id, "path", workflow_id=run.workflow_id)
    payload_key = _record_key(
        record_kind,
        run.run_id,
        "payload",
        workflow_id=run.workflow_id,
    )
    record = _record_for_kind(record_kind, payload_key)
    await operator.create_dir(path_key.dir_path())
    await operator.write(path_key.path(), record.SerializeToString(deterministic=True))

    with pytest.raises(RunRecordValidationError, match="object location"):
        await _list_records(store, record_kind, run)


async def test_misplaced_timer_cannot_redirect_due_ledger_deletion(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    await store.put_workflow(
        _workflow("prices:redirect", "run:two", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    target_key = TimerKey(
        workflow_id="prices:redirect",
        run_id="run:two",
        timer_id="target",
    )
    target = _timer(
        target_key.workflow_id,
        target_key.run_id,
        target_key.timer_id,
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        datetime.now(UTC) - timedelta(seconds=1),
    )
    await store.put_timer(target)
    target_ledger = _due_entry_path(target_key)
    path_key = TimerKey(
        workflow_id="prices:redirect",
        run_id="run:one",
        timer_id="path",
    )
    await operator.create_dir(path_key.dir_path())
    await operator.write(path_key.path(), target.SerializeToString(deterministic=True))

    assert await store.delete_timer(path_key)

    assert not await operator.exists(path_key.path())
    assert await operator.exists(target_key.path())
    assert await operator.exists(target_ledger)


async def test_latest_pointer_rejects_wrong_workflow_and_hides_missing_reference(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    await store.put_workflow(
        _workflow("prices:pointer-a", "run:a", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    await store.put_workflow(
        _workflow("prices:pointer-b", "run:b", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    pointer = await store.get_latest_workflow_run("", "prices:pointer-b")
    assert pointer is not None
    pointer_path = _latest_pointer_path("default", "prices:pointer-a")
    await operator.write(pointer_path, pointer.SerializeToString(deterministic=True))

    with pytest.raises(RunRecordValidationError, match="requested workflow"):
        await store.get_latest_workflow_run("", "prices:pointer-a")

    await operator.delete(pointer_path)
    missing = _workflow(
        "prices:pointer-a", "run:missing", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    )
    await store.put_workflow(missing)
    await operator.delete(WorkflowKey(workflow_id="prices:pointer-a", run_id="run:missing").path())

    assert await store.get_latest_workflow_run("", "prices:pointer-a") is None


@pytest.mark.parametrize("mismatch", ["status", "record_time", "run_order_time"])
async def test_latest_pointer_hides_metadata_lag_as_not_found(tmp_path, mismatch: str) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    record = _workflow(
        "prices:pointer-stale",
        "run:one",
        temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        completed_at=datetime(2026, 7, 3, tzinfo=UTC),
        run_order_time=datetime(2026, 7, 2, tzinfo=UTC),
    )
    await store.put_workflow(record)
    pointer = await store.get_latest_workflow_run("", record.key.workflow_id)
    assert pointer is not None

    if mismatch == "status":
        pointer.status = temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    else:
        timestamp = getattr(pointer, mismatch)
        timestamp.FromDatetime(timestamp.ToDatetime(tzinfo=UTC) + timedelta(seconds=1))
    await operator.write(
        _latest_pointer_path("default", record.key.workflow_id),
        pointer.SerializeToString(deterministic=True),
    )

    assert await store.get_latest_workflow_run("", record.key.workflow_id) is None


async def test_latest_pointer_transition_window_is_not_corruption(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    record = _workflow(
        "prices:pointer-transition",
        "run:one",
        temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        run_order_time=datetime(2026, 7, 2, tzinfo=UTC),
    )
    await store.put_workflow(record)

    record.status = temporaless_pb2.WORKFLOW_STATUS_COMPLETED
    record.completed_at.FromDatetime(datetime(2026, 7, 3, tzinfo=UTC))
    key = WorkflowKey(workflow_id=record.key.workflow_id, run_id=record.key.run_id)
    await operator.write(key.path(), record.SerializeToString(deterministic=True))

    assert await store.get_latest_workflow_run("", record.key.workflow_id) is None

    await store.put_workflow(record)
    pointer = await store.get_latest_workflow_run("", record.key.workflow_id)
    assert pointer is not None
    assert pointer.status == temporaless_pb2.WORKFLOW_STATUS_COMPLETED


async def test_latest_pointer_writer_advances_after_authoritative_status_change(tmp_path) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    record = _workflow(
        "prices:pointer-status", "run:one", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    )
    await store.put_workflow(record)
    record.status = temporaless_pb2.WORKFLOW_STATUS_COMPLETED
    record.completed_at.GetCurrentTime()

    await store.put_workflow(record)

    pointer = await store.get_latest_workflow_run("", record.key.workflow_id)
    assert pointer is not None
    assert pointer.status == temporaless_pb2.WORKFLOW_STATUS_COMPLETED


async def test_delete_workflow_retains_derived_pointer_without_inventing_run(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    record = _workflow(
        "prices:pointer-delete", "run:one", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    )
    await store.put_workflow(record)
    pointer_path = _latest_pointer_path("default", record.key.workflow_id)

    assert await store.delete_workflow(
        WorkflowKey(workflow_id=record.key.workflow_id, run_id=record.key.run_id)
    )

    assert await operator.exists(pointer_path)
    assert await store.get_latest_workflow_run("", record.key.workflow_id) is None


async def test_latest_pointer_read_is_two_point_gets(tmp_path) -> None:
    operator = _RecordingOperator(opendal.AsyncOperator("fs", root=str(tmp_path)))
    store = OpenDALStore(operator)
    record = _workflow(
        "prices:pointer-reads", "run:one", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    )
    await store.put_workflow(record)
    operator.reads.clear()

    pointer = await store.get_latest_workflow_run("", record.key.workflow_id)

    assert pointer is not None
    assert operator.reads == [
        _latest_pointer_path("default", record.key.workflow_id),
        WorkflowKey(
            workflow_id=record.key.workflow_id,
            run_id=record.key.run_id,
        ).path(),
    ]


async def test_latest_pointer_lock_cancellation_does_not_leak_lock(tmp_path) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    record = _workflow(
        "prices:pointer-cancel", "run:one", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    )
    await store._latest_pointer_lock.acquire()
    blocked = asyncio.create_task(store.put_workflow(record))
    await asyncio.sleep(0)
    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked
    store._latest_pointer_lock.release()

    await asyncio.wait_for(store.put_workflow(record), timeout=1)
    assert await store.get_latest_workflow_run("", record.key.workflow_id) is not None


async def test_latest_pointer_uses_explicit_run_order_time_for_opaque_run_ids(tmp_path) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    await store.put_workflow(
        _workflow(
            "prices:aapl",
            "20260703T090000Z",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime(2026, 7, 3, 9, 0, tzinfo=UTC),
            run_order_time=datetime(2026, 7, 3, 9, 0, tzinfo=UTC),
        )
    )
    await store.put_workflow(
        _workflow(
            "prices:aapl",
            "2026-06-01",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime(2026, 7, 3, 10, 0, tzinfo=UTC),
            run_order_time=datetime(2026, 6, 1, tzinfo=UTC),
        )
    )

    pointer = await store.get_latest_workflow_run("", "prices:aapl")
    last = await last_fire_from_runs(store, "", "prices:aapl")

    assert pointer is not None
    assert pointer.key.run_id == "20260703T090000Z"
    assert last == datetime(2026, 7, 3, 9, 0, tzinfo=UTC)


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


async def test_due_timers_copies_but_retains_invalid_ledger_blob(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    path = "temporaless/v2/default/_due/_due/run/timer.binpb"
    await operator.create_dir(path.rsplit("/", 1)[0] + "/")
    await operator.write(path, b"not a due-timer entry")

    for _ in range(2):
        with pytest.raises(RunRecordValidationError, match="invalid due-timer ledger entry"):
            await due_timers(store, datetime.now(UTC))
    assert await operator.exists(path)
    quarantined = [
        entry
        async for entry in await operator.list("temporaless/v2/default/_due_invalid/")
        if entry.path.endswith(".binpb")
    ]
    assert len(quarantined) == 1


async def test_due_timers_quarantines_invalid_entry_and_fails_tick_loudly(tmp_path, caplog) -> None:
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
    empty_path = "temporaless/v2/default/_due/empty/run/timer.binpb"
    reserved_path = "temporaless/v2/default/_due/_bad/run/timer.binpb"
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
            record=temporaless_pb2.TimerRecord(
                schema_version=TIMER_RECORD_SCHEMA_VERSION,
                key=temporaless_pb2.TimerKey(
                    namespace="default",
                    workflow_id="_bad",
                    run_id="run",
                    timer_id="timer",
                ),
                status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
                fire_at=fire_at,
            ),
        ).SerializeToString(),
    )

    with (
        caplog.at_level(logging.WARNING, logger="temporaless.storage"),
        pytest.raises(RunRecordValidationError, match="invalid due-timer ledger entry"),
    ):
        await due_timers(store, datetime.now(UTC))

    assert "quarantining invalid due-timer ledger entry" in caplog.text
    assert await operator.exists(empty_path)
    assert await operator.exists(reserved_path)


@pytest.mark.parametrize(
    "status",
    [
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        temporaless_pb2.TIMER_STATUS_FIRED,
        temporaless_pb2.TIMER_STATUS_CANCELED,
    ],
)
async def test_put_timer_writes_exact_due_ledger_before_every_timer_state(
    tmp_path,
    status: temporaless_pb2.TimerStatus,
) -> None:
    operator = _RecordingOperator(opendal.AsyncOperator("fs", root=str(tmp_path)))
    store = OpenDALStore(operator)
    await store.put_workflow(
        _workflow("prices:timer", "2026-07-01", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    operator.writes.clear()

    timer = _timer(
        "prices:timer",
        "2026-07-01",
        "wait",
        status,
        datetime.now(UTC) + timedelta(minutes=5),
    )
    await store.put_timer(timer)

    due_write = next(index for index, path in enumerate(operator.writes) if "/_due/" in path)
    timer_write = next(index for index, path in enumerate(operator.writes) if "/timer/" in path)
    assert due_write < timer_write
    ledger = temporaless_pb2.DueTimerEntry()
    ledger.ParseFromString(
        bytes(
            await operator.read(
                _due_entry_path(
                    TimerKey(
                        workflow_id=timer.key.workflow_id,
                        run_id=timer.key.run_id,
                        timer_id=timer.key.timer_id,
                    )
                )
            )
        )
    )
    assert ledger.record == timer


@pytest.mark.parametrize(
    "status",
    [temporaless_pb2.TIMER_STATUS_UNSPECIFIED, 99],
)
async def test_put_timer_rejects_invalid_persisted_status_before_writing(
    tmp_path,
    status: int,
) -> None:
    operator = _RecordingOperator(opendal.AsyncOperator("fs", root=str(tmp_path)))
    store = OpenDALStore(operator)
    timer = _timer(
        "prices:invalid-status",
        "run",
        "wait",
        status,
        datetime.now(UTC) + timedelta(minutes=5),
    )

    with pytest.raises(RunRecordValidationError, match="persisted timer status"):
        await store.put_timer(timer)

    assert operator.writes == []


@pytest.mark.parametrize("corruption", ["missing", "invalid"])
async def test_put_scheduled_timer_rejects_invalid_fire_at_before_writing(
    tmp_path,
    corruption: str,
) -> None:
    operator = _RecordingOperator(opendal.AsyncOperator("fs", root=str(tmp_path)))
    store = OpenDALStore(operator)
    timer = _timer(
        "prices:invalid-fire-at",
        "run",
        "wait",
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        datetime.now(UTC) + timedelta(minutes=5),
    )
    if corruption == "missing":
        timer.ClearField("fire_at")
    else:
        timer.fire_at.seconds = 253_402_300_800

    with pytest.raises(RunRecordValidationError, match="fire_at"):
        await store.put_timer(timer)

    assert operator.writes == []


async def test_timer_transitions_replace_one_exact_due_ledger_entry(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    workflow_id = "prices:retry-ledger"
    run_id = "2026-07-01"
    timer_id = "activity-retry:fetch"
    await store.put_workflow(
        _workflow(workflow_id, run_id, temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )

    first_fire = datetime.now(UTC) - timedelta(minutes=2)
    first = _timer(
        workflow_id,
        run_id,
        timer_id,
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        first_fire,
    )
    await store.put_timer(first)
    key = TimerKey(workflow_id=workflow_id, run_id=run_id, timer_id=timer_id)
    ledger_path = _due_entry_path(key)
    assert await operator.exists(ledger_path)

    second_fire = datetime.now(UTC) - timedelta(minutes=1)
    second = _timer(
        workflow_id,
        run_id,
        timer_id,
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        second_fire,
    )
    await store.put_timer(second)
    assert await operator.exists(ledger_path)
    ledger = temporaless_pb2.DueTimerEntry()
    ledger.ParseFromString(bytes(await operator.read(ledger_path)))
    assert ledger.record == second
    due = await due_timers(store, datetime.now(UTC))
    assert [item.key.timer_id for item in due] == [timer_id]

    second.status = temporaless_pb2.TIMER_STATUS_FIRED
    second.fired_at.GetCurrentTime()
    await store.put_timer(second)
    ledger.ParseFromString(bytes(await operator.read(ledger_path)))
    assert ledger.record == second
    assert await due_timers(store, datetime.now(UTC)) == []


async def test_ledger_first_crash_keeps_missing_timer_wake_dispatchable(tmp_path) -> None:
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
    timer_key = TimerKey(workflow_id="prices:timer", run_id="2026-07-01", timer_id="wait")
    await store._put_due_entry(timer)
    ledger_path = _due_entry_path(timer_key)
    recovered = await store.get_timer(timer_key)
    assert recovered == timer

    # The first scanner tick materializes the exact canonical point but does
    # not dispatch from a shadow-only state.
    assert await due_timers(store, datetime.now(UTC)) == []
    assert await store._get_canonical_timer(timer_key) == timer
    due = await due_timers(store, datetime.now(UTC))
    assert len(due) == 1
    assert due[0].key == timer_key
    assert due[0].record == timer
    assert await store.list_timers(
        WorkflowKey(workflow_id=timer_key.workflow_id, run_id=timer_key.run_id),
        temporaless_pb2.TIMER_STATUS_UNSPECIFIED,
    ) == [timer]
    assert await operator.exists(ledger_path)


async def test_delete_timer_tombstones_live_ledger_orphan_before_point_delete(
    tmp_path,
) -> None:
    operator = _RecordingOperator(opendal.AsyncOperator("fs", root=str(tmp_path)))
    store = OpenDALStore(operator)
    timer = _timer(
        "prices:timer-delete",
        "run",
        "wait",
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        datetime.now(UTC) + timedelta(days=1),
    )
    key = TimerKey(workflow_id="prices:timer-delete", run_id="run", timer_id="wait")
    await store._put_due_entry(timer)
    operator.writes.clear()

    assert await store.delete_timer(key)
    assert operator.writes[0] == _due_entry_path(key)
    assert await store.get_timer(key) is None
    assert (
        await store.list_timers(
            WorkflowKey(workflow_id=key.workflow_id, run_id=key.run_id),
            temporaless_pb2.TIMER_STATUS_UNSPECIFIED,
        )
        == []
    )

    ledger = temporaless_pb2.DueTimerEntry()
    ledger.ParseFromString(bytes(await operator.read(_due_entry_path(key))))
    assert ledger.record.status == temporaless_pb2.TIMER_STATUS_CANCELED
    assert ledger.record.code_version == timer.code_version
    assert ledger.record.fire_at == timer.fire_at
    assert not await store.delete_timer(key)


async def test_delete_timer_tombstones_and_removes_corrupt_point(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    key = TimerKey(workflow_id="prices:corrupt-delete", run_id="run", timer_id="wait")
    await operator.create_dir(key.dir_path())
    await operator.write(key.path(), b"not a timer record")

    assert await store.delete_timer(key)
    assert not await operator.exists(key.path())
    assert await store.get_timer(key) is None
    ledger = temporaless_pb2.DueTimerEntry()
    ledger.ParseFromString(bytes(await operator.read(_due_entry_path(key))))
    assert ledger.key == key.to_proto()
    assert ledger.record.key == key.to_proto()
    assert ledger.record.status == temporaless_pb2.TIMER_STATUS_CANCELED


async def test_scanner_finishes_interrupted_timer_delete_without_dispatch(
    tmp_path,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    timer = _timer(
        "prices:delete-crash",
        "run",
        "wait",
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        datetime.now(UTC) - timedelta(minutes=1),
    )
    key = TimerKey(workflow_id="prices:delete-crash", run_id="run", timer_id="wait")
    await store.put_timer(timer)
    tombstone = temporaless_pb2.TimerRecord()
    tombstone.CopyFrom(timer)
    tombstone.status = temporaless_pb2.TIMER_STATUS_CANCELED

    # Simulate death after DeleteTimer's shadow write but before its point
    # delete. Logical reads already hide the stale point; the scanner finishes
    # physical cleanup and never emits the old wake.
    await store._put_due_entry(tombstone)
    assert await store.get_timer(key) is None
    assert await store.due_timers("default", datetime.now(UTC)) == []
    assert not await operator.exists(key.path())


async def test_scanner_repairs_interrupted_timer_overwrite_before_dispatch(
    tmp_path,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    workflow_key = WorkflowKey(workflow_id="prices:retry-rearm", run_id="run")
    timer_key = TimerKey(
        workflow_id=workflow_key.workflow_id,
        run_id=workflow_key.run_id,
        timer_id="activity-retry:fetch",
    )
    await store.put_workflow(
        _workflow(
            workflow_key.workflow_id,
            workflow_key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        )
    )
    old = _timer(
        timer_key.workflow_id,
        timer_key.run_id,
        timer_key.timer_id,
        temporaless_pb2.TIMER_STATUS_FIRED,
        datetime.now(UTC) - timedelta(minutes=2),
    )
    old.fired_at.GetCurrentTime()
    await store.put_timer(old)
    rearmed = _timer(
        timer_key.workflow_id,
        timer_key.run_id,
        timer_key.timer_id,
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        datetime.now(UTC) - timedelta(minutes=1),
    )
    rearmed.timer_kind = temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY
    rearmed.code_version = "release:rearmed"
    rearmed.retry_activity_id = "fetch"

    # Simulate death after overwriting the deterministic shadow but before
    # replacing the old FIRED canonical point.
    await store._put_due_entry(rearmed)
    assert await store.get_timer(timer_key) == rearmed
    assert await store.list_timers(
        workflow_key,
        temporaless_pb2.TIMER_STATUS_UNSPECIFIED,
    ) == [rearmed]

    # A mixed point/shadow pair never dispatches. The scan repairs the point;
    # the next tick observes an exact scheduled pair and emits the wake.
    assert await store.due_timers("default", datetime.now(UTC)) == []
    assert await store._get_canonical_timer(timer_key) == rearmed
    due = await store.due_timers("default", datetime.now(UTC))
    assert [item.record for item in due] == [rearmed]


async def test_valid_shadow_recovers_corrupt_canonical_point(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    workflow_key = WorkflowKey(workflow_id="prices:shadow-recovery", run_id="run")
    timer_key = TimerKey(
        workflow_id=workflow_key.workflow_id,
        run_id=workflow_key.run_id,
        timer_id="wait",
    )
    timer = _timer(
        timer_key.workflow_id,
        timer_key.run_id,
        timer_key.timer_id,
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        datetime.now(UTC) - timedelta(minutes=1),
    )
    await store.put_workflow(
        _workflow(
            workflow_key.workflow_id,
            workflow_key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        )
    )
    await store.put_timer(timer)
    await operator.write(timer_key.path(), b"corrupt canonical timer")

    assert await store.get_timer(timer_key) == timer
    assert await store.list_timers(
        workflow_key,
        temporaless_pb2.TIMER_STATUS_UNSPECIFIED,
    ) == [timer]
    assert await store.due_timers("default", datetime.now(UTC)) == []
    assert await store._get_canonical_timer(timer_key) == timer
    due = await store.due_timers("default", datetime.now(UTC))
    assert [item.record for item in due] == [timer]


async def test_due_timers_surfaces_missing_canonical_repair_failure(tmp_path) -> None:
    class FailingCanonicalRepairStore(OpenDALStore):
        async def _put_canonical_timer(self, record: temporaless_pb2.TimerRecord) -> None:
            raise OSError("canonical timer repair unavailable")

    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = FailingCanonicalRepairStore(operator)
    workflow_key = WorkflowKey(workflow_id="prices:repair-failure", run_id="run")
    timer = _timer(
        workflow_key.workflow_id,
        workflow_key.run_id,
        "wait",
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        datetime.now(UTC) - timedelta(seconds=1),
    )
    await store.put_workflow(
        _workflow(
            workflow_key.workflow_id,
            workflow_key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        )
    )
    await store._put_due_entry(timer)

    with pytest.raises(OSError, match="canonical timer repair unavailable"):
        await store.due_timers("default", datetime.now(UTC))


async def test_due_timers_surfaces_tombstone_delete_failure(tmp_path, monkeypatch) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    workflow_key = WorkflowKey(workflow_id="prices:delete-failure", run_id="run")
    timer_key = TimerKey(
        workflow_id=workflow_key.workflow_id,
        run_id=workflow_key.run_id,
        timer_id="wait",
    )
    timer = _timer(
        timer_key.workflow_id,
        timer_key.run_id,
        timer_key.timer_id,
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        datetime.now(UTC) - timedelta(seconds=1),
    )
    await store.put_workflow(
        _workflow(
            workflow_key.workflow_id,
            workflow_key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        )
    )
    await store.put_timer(timer)
    tombstone = temporaless_pb2.TimerRecord()
    tombstone.CopyFrom(timer)
    tombstone.status = temporaless_pb2.TIMER_STATUS_CANCELED
    await store._put_due_entry(tombstone)

    original_delete = storage_module._delete_if_exists

    async def fail_timer_delete(operator_arg, path: str) -> bool:
        if path == timer_key.path():
            raise OSError("canonical timer delete unavailable")
        return await original_delete(operator_arg, path)

    monkeypatch.setattr(storage_module, "_delete_if_exists", fail_timer_delete)

    with pytest.raises(OSError, match="canonical timer delete unavailable"):
        await store.due_timers("default", datetime.now(UTC))
    assert await operator.exists(timer_key.path())


async def test_due_timers_empty_namespace_scans_every_namespace(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    now = datetime.now(UTC)
    for namespace in ("default", "tenant-a"):
        await store.put_workflow(
            _workflow(
                "prices:timer",
                f"run:{namespace}",
                temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
                namespace=namespace,
            )
        )
        await store.put_timer(
            _timer(
                "prices:timer",
                f"run:{namespace}",
                "wait",
                temporaless_pb2.TIMER_STATUS_SCHEDULED,
                now - timedelta(seconds=1),
                namespace=namespace,
            )
        )

    all_due = await store.due_timers("", now)
    tenant_due = await store.due_timers("tenant-a", now)

    assert [(item.key.namespace, item.key.run_id) for item in all_due] == [
        ("default", "run:default"),
        ("tenant-a", "run:tenant-a"),
    ]
    assert [(item.key.namespace, item.key.run_id) for item in tenant_due] == [
        ("tenant-a", "run:tenant-a")
    ]


async def test_due_timers_quarantines_ledger_keys_from_different_runs(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    fire_at = datetime.now(UTC) - timedelta(seconds=1)
    timer_key = TimerKey(workflow_id="prices:timer", run_id="run:one", timer_id="wait")
    ledger_path = _due_entry_path(timer_key)
    fire_at_message = Timestamp()
    fire_at_message.FromDatetime(fire_at)
    await operator.create_dir(ledger_path.rsplit("/", 1)[0] + "/")
    await operator.write(
        ledger_path,
        temporaless_pb2.DueTimerEntry(
            key=timer_key.to_proto(),
            workflow_key=WorkflowKey(
                workflow_id=timer_key.workflow_id,
                run_id="run:other",
            ).to_proto(),
            fire_at=fire_at_message,
            record=_timer(
                timer_key.workflow_id,
                timer_key.run_id,
                timer_key.timer_id,
                temporaless_pb2.TIMER_STATUS_SCHEDULED,
                fire_at,
            ),
        ).SerializeToString(deterministic=True),
    )

    with pytest.raises(RunRecordValidationError, match="invalid due-timer ledger entry"):
        await store.due_timers("", datetime.now(UTC))
    assert await operator.exists(ledger_path)
    assert [entry async for entry in await operator.list("temporaless/v2/default/_due_invalid/")]


@pytest.mark.parametrize(
    "record_kind",
    ["timer_key_mismatch", "workflow_key_mismatch", "workflow_undecodable"],
)
async def test_due_timers_handles_corrupt_authoritative_payload(
    tmp_path,
    caplog,
    record_kind: str,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path))
    store = OpenDALStore(operator)
    fire_at = datetime.now(UTC) - timedelta(seconds=1)
    workflow_key = WorkflowKey(workflow_id="prices:timer", run_id="run:one")
    timer_key = TimerKey(
        workflow_id=workflow_key.workflow_id,
        run_id=workflow_key.run_id,
        timer_id="wait",
    )
    workflow = _workflow(
        workflow_key.workflow_id,
        workflow_key.run_id,
        temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
    )
    timer = _timer(
        timer_key.workflow_id,
        timer_key.run_id,
        timer_key.timer_id,
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        fire_at,
    )
    await store.put_workflow(workflow)
    await store.put_timer(timer)
    ledger_path = _due_entry_path(timer_key)

    if record_kind == "timer_key_mismatch":
        timer.key.CopyFrom(
            TimerKey(
                workflow_id=timer_key.workflow_id,
                run_id="run:other",
                timer_id=timer_key.timer_id,
            ).to_proto()
        )
        await operator.write(timer_key.path(), timer.SerializeToString(deterministic=True))
    elif record_kind == "workflow_key_mismatch":
        workflow.key.CopyFrom(
            WorkflowKey(
                workflow_id=workflow_key.workflow_id,
                run_id="run:other",
            ).to_proto()
        )
        await operator.write(workflow_key.path(), workflow.SerializeToString(deterministic=True))
    else:
        await operator.write(workflow_key.path(), b"not a workflow protobuf")

    if record_kind.startswith("workflow_"):
        with pytest.raises(RunRecordValidationError, match="invalid parent workflow"):
            await store.due_timers("", datetime.now(UTC))
    else:
        with caplog.at_level(logging.WARNING, logger="temporaless.storage"):
            assert await store.due_timers("", datetime.now(UTC)) == []
        assert "repairing due timer with invalid canonical point" in caplog.text
    assert await operator.exists(ledger_path)
    assert not [
        entry async for entry in await operator.list("temporaless/v2/default/_due_invalid/")
    ]


class _RecordingOperator:
    def __init__(self, inner: opendal.AsyncOperator) -> None:
        self._inner = inner
        self.writes: list[str] = []
        self.reads: list[str] = []

    def capability(self):
        return self._inner.capability()

    async def create_dir(self, path: str):
        return await self._inner.create_dir(path)

    async def write(self, path: str, *args, **kwargs):
        self.writes.append(path)
        return await self._inner.write(path, *args, **kwargs)

    async def read(self, path: str):
        self.reads.append(path)
        return await self._inner.read(path)

    async def list(self, path: str):
        return await self._inner.list(path)

    async def exists(self, path: str):
        return await self._inner.exists(path)

    async def delete(self, path: str):
        return await self._inner.delete(path)


def _record_key(
    record_kind: str,
    run_id: str,
    record_id: str,
    *,
    workflow_id: str = "prices:point",
):
    if record_kind == "workflow":
        return WorkflowKey(workflow_id=workflow_id, run_id=run_id)
    if record_kind == "activity":
        return ActivityKey(
            workflow_id=workflow_id,
            run_id=run_id,
            activity_id=record_id,
        )
    if record_kind == "timer":
        return TimerKey(workflow_id=workflow_id, run_id=run_id, timer_id=record_id)
    if record_kind == "event":
        return EventKey(workflow_id=workflow_id, run_id=run_id, event_id=record_id)
    if record_kind == "claim":
        return ClaimKey(workflow_id=workflow_id, run_id=run_id, claim_id=record_id)
    raise AssertionError(f"unsupported record kind: {record_kind}")


def _record_for_kind(record_kind: str, key):
    if record_kind == "workflow":
        return _workflow(
            key.workflow_id,
            key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        )
    if record_kind == "activity":
        return temporaless_pb2.ActivityRecord(
            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            activity_type="activity:test",
            code_version="v1",
            status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
    if record_kind == "timer":
        return _timer(
            key.workflow_id,
            key.run_id,
            key.timer_id,
            temporaless_pb2.TIMER_STATUS_SCHEDULED,
            datetime.now(UTC) - timedelta(seconds=1),
        )
    if record_kind == "event":
        return temporaless_pb2.EventRecord(
            schema_version=EVENT_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
        )
    if record_kind == "claim":
        return temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            owner_id="owner",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
            resource_id=key.workflow_id,
            code_version="v1",
        )
    raise AssertionError(f"unsupported record kind: {record_kind}")


async def _get_record(store: OpenDALStore, record_kind: str, key):
    if record_kind == "workflow":
        return await store.get_workflow(key)
    if record_kind == "activity":
        return await store.get_activity(key)
    if record_kind == "timer":
        return await store.get_timer(key)
    if record_kind == "event":
        return await store.get_event(key)
    if record_kind == "claim":
        return await store.get_claim(key)
    raise AssertionError(f"unsupported record kind: {record_kind}")


async def _list_records(store: OpenDALStore, record_kind: str, key: WorkflowKey):
    if record_kind == "activity":
        return await store.list_activities(key)
    if record_kind == "timer":
        return await store.list_timers(key, temporaless_pb2.TIMER_STATUS_UNSPECIFIED)
    if record_kind == "event":
        return await store.list_events(key)
    if record_kind == "claim":
        return await store.list_claims(key)
    raise AssertionError(f"unsupported record kind: {record_kind}")


def _workflow(
    workflow_id: str,
    run_id: str,
    status: temporaless_pb2.WorkflowStatus,
    *,
    completed_at: datetime | None = None,
    run_order_time: datetime | None = None,
    namespace: str = "default",
) -> temporaless_pb2.WorkflowRecord:
    created = Timestamp()
    created.FromDatetime(datetime(2026, 7, 1, tzinfo=UTC))
    record = temporaless_pb2.WorkflowRecord(
        schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
        key=temporaless_pb2.WorkflowKey(
            namespace=namespace,
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
    if run_order_time is not None:
        record.run_order_time.FromDatetime(run_order_time)
    return record


def _timer(
    workflow_id: str,
    run_id: str,
    timer_id: str,
    status: temporaless_pb2.TimerStatus,
    fire_at: datetime,
    *,
    namespace: str = "default",
) -> temporaless_pb2.TimerRecord:
    fire = Timestamp()
    fire.FromDatetime(fire_at)
    created = Timestamp()
    created.FromDatetime(datetime(2026, 7, 1, tzinfo=UTC))
    return temporaless_pb2.TimerRecord(
        schema_version=TIMER_RECORD_SCHEMA_VERSION,
        key=TimerKey(
            namespace=namespace,
            workflow_id=workflow_id,
            run_id=run_id,
            timer_id=timer_id,
        ).to_proto(),
        timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
        code_version="test",
        status=status,
        fire_at=fire,
        created_at=created,
    )
