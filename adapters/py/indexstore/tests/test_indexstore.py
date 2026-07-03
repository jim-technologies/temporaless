from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    TIMER_RECORD_SCHEMA_VERSION,
    WORKFLOW_RECORD_SCHEMA_VERSION,
    ActivityKey,
    OpenDALStore,
    TimerKey,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2

import temporaless_indexstore.adapter as index_adapter
from temporaless_indexstore import IndexedStore


async def test_write_through_lists_workflows(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")

    await store.put_workflow(_workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_FAILED))
    await store.put_workflow(
        _workflow("prices:msft", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )

    records, token = await store.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_FAILED)
    assert token == ""
    assert [record.key.workflow_id for record in records] == ["prices:aapl"]


async def test_rebuild_from_populated_bucket(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    bucket = OpenDALStore(operator)
    await bucket.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )

    indexed = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    await indexed.rebuild()

    records, _ = await indexed.list_workflows(
        "", "prices:aapl", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED
    )
    assert [record.key.run_id for record in records] == ["r1"]


async def test_rebuild_dispatches_by_key_structure_not_substrings(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    bucket = OpenDALStore(operator)
    await bucket.put_workflow(
        _workflow("prices:aapl", "activity", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    await bucket.put_timer(
        _timer(
            "prices:aapl",
            "activity",
            "wait",
            temporaless_pb2.TIMER_STATUS_SCHEDULED,
            datetime.now(UTC) - timedelta(seconds=1),
        )
    )

    indexed = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    await indexed.rebuild()

    due = await indexed.due_timers("", datetime.now(UTC))
    assert [timer.key.timer_id for timer in due] == ["wait"]


async def test_rebuild_skips_corrupt_records_without_poisoning_index(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    bucket = OpenDALStore(operator)
    await bucket.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )
    garbage_path = "temporaless/v2/default/garbage/r1/workflow.binpb"
    empty_path = "temporaless/v2/default/empty/r1/workflow.binpb"
    await operator.create_dir(garbage_path.rsplit("/", 1)[0] + "/")
    await operator.write(garbage_path, b"not a workflow record")
    await operator.create_dir(empty_path.rsplit("/", 1)[0] + "/")
    await operator.write(empty_path, temporaless_pb2.WorkflowRecord().SerializeToString())

    indexed = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    skipped = await indexed.rebuild()

    records, _ = await indexed.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED)
    assert skipped == 2
    assert [(record.key.workflow_id, record.key.run_id) for record in records] == [
        ("prices:aapl", "r1")
    ]


async def test_failed_rebuild_leaves_previous_index_intact(tmp_path, monkeypatch) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    await store.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )
    boom_path = "temporaless/v2/default/boom/r1/workflow.binpb"
    await operator.create_dir(boom_path.rsplit("/", 1)[0] + "/")
    await operator.write(
        boom_path,
        _workflow("boom", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED).SerializeToString(),
    )
    original_read_pb = index_adapter._read_pb

    async def fail_on_boom(operator, path, factory):
        if "/boom/" in path:
            raise RuntimeError("forced rebuild interruption")
        return await original_read_pb(operator, path, factory)

    monkeypatch.setattr(index_adapter, "_read_pb", fail_on_boom)

    with pytest.raises(RuntimeError, match="forced rebuild interruption"):
        await store.rebuild()

    records, _ = await store.list_workflows(
        "", "prices:aapl", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED
    )
    assert [record.key.run_id for record in records] == ["r1"]


async def test_rebuild_preserves_puts_written_during_walk(tmp_path, monkeypatch) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    bucket = OpenDALStore(operator)
    await bucket.put_workflow(_workflow("seed", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    now = datetime.now(UTC)
    original_read_rebuild_record = index_adapter._read_rebuild_record
    injected = False

    async def inject_put_during_rebuild(operator, path, factory, key_factory):
        nonlocal injected
        result = await original_read_rebuild_record(operator, path, factory, key_factory)
        if not injected:
            injected = True
            await store.put_workflow(
                _workflow("prices:aapl", "r2", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
            )
            await store.put_timer(
                _timer(
                    "prices:aapl",
                    "r2",
                    "wait",
                    temporaless_pb2.TIMER_STATUS_SCHEDULED,
                    now - timedelta(seconds=1),
                )
            )
        return result

    monkeypatch.setattr(index_adapter, "_read_rebuild_record", inject_put_during_rebuild)

    await store.rebuild()

    due = await store.due_timers("", now)
    assert [timer.key.timer_id for timer in due] == ["wait"]


async def test_rebuild_skips_not_found_race_without_counting_corrupt(tmp_path, monkeypatch) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    bucket = OpenDALStore(operator)
    await bucket.put_workflow(_workflow("keeper", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED))
    await bucket.put_workflow(_workflow("vanish", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    original_read_pb = index_adapter._read_pb

    async def delete_before_read(operator, path, factory):
        if "/vanish/" in path:
            await operator.delete(path)
        return await original_read_pb(operator, path, factory)

    monkeypatch.setattr(index_adapter, "_read_pb", delete_before_read)

    skipped = await store.rebuild()

    records, _ = await store.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED)
    assert skipped == 0
    assert [(record.key.workflow_id, record.key.run_id) for record in records] == [("keeper", "r1")]


async def test_list_workflows_pages_stably_with_order_by(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    for idx in range(5):
        await store.put_workflow(
            _workflow(
                "prices:aapl",
                f"r{idx}",
                temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
                created_at=datetime(2026, 7, idx + 1, tzinfo=UTC),
            )
        )

    first, token = await store.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
        order_by="created_at desc",
        page_size=2,
    )
    second, next_token = await store.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
        order_by="created_at desc",
        page_size=2,
        page_token=token,
    )
    repeated_second, _ = await store.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
        order_by="created_at desc",
        page_size=2,
        page_token=token,
    )
    third, final_token = await store.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
        order_by="created_at desc",
        page_size=2,
        page_token=next_token,
    )

    assert [record.key.run_id for record in first] == ["r4", "r3"]
    assert [record.key.run_id for record in second] == ["r2", "r1"]
    assert [record.key.run_id for record in repeated_second] == ["r2", "r1"]
    assert [record.key.run_id for record in third] == ["r0"]
    assert final_token == ""


async def test_list_activities_query_honors_order_by(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    for activity_id in ("a", "c", "b"):
        await store.put_activity(
            _activity(
                "prices:aapl",
                "r1",
                activity_id,
                temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
            )
        )

    records, token = await store.list_activities_query(
        "",
        "prices:aapl",
        "r1",
        temporaless_pb2.ACTIVITY_STATUS_UNSPECIFIED,
        order_by="activity_id desc",
    )

    assert token == ""
    assert [record.key.activity_id for record in records] == ["c", "b", "a"]


async def test_sweep_deletes_bucket_and_index_rows(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    old = _workflow(
        "prices:aapl",
        "old",
        temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        completed_at=datetime.now(UTC) - timedelta(days=2),
    )
    fresh = _workflow(
        "prices:aapl",
        "fresh",
        temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        completed_at=datetime.now(UTC),
    )
    await store.put_workflow(old)
    await store.put_workflow(fresh)

    deleted = await store.sweep("", datetime.now(UTC), timedelta(days=1))

    assert deleted == 1
    assert await store.get_workflow(WorkflowKey(workflow_id="prices:aapl", run_id="old")) is None
    records, _ = await store.list_workflows(
        "", "prices:aapl", temporaless_pb2.WORKFLOW_STATUS_COMPLETED
    )
    assert [record.key.run_id for record in records] == ["fresh"]


async def test_indexed_due_timers(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    await store.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    await store.put_timer(
        _timer(
            "prices:aapl",
            "r1",
            "wait",
            temporaless_pb2.TIMER_STATUS_SCHEDULED,
            datetime.now(UTC) - timedelta(seconds=1),
        )
    )

    due = await store.due_timers("", datetime.now(UTC))

    assert len(due) == 1
    assert due[0].key.timer_id == "wait"


async def test_indexed_due_timers_prunes_terminal_workflow_rows(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    await store.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    await store.put_timer(
        _timer(
            "prices:aapl",
            "r1",
            "wait",
            temporaless_pb2.TIMER_STATUS_SCHEDULED,
            datetime.now(UTC) - timedelta(seconds=1),
        )
    )
    await store.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )

    assert await store.due_timers("", datetime.now(UTC)) == []

    await store.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    assert await store.due_timers("", datetime.now(UTC)) == []


async def test_indexed_due_timers_self_heals_future_record_stale_due_row(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    now = datetime.now(UTC)
    future_fire = now + timedelta(hours=1)
    stale_due = now - timedelta(minutes=5)
    await store.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    timer = _timer(
        "prices:aapl",
        "r1",
        "wait",
        temporaless_pb2.TIMER_STATUS_SCHEDULED,
        future_fire,
    )
    await store.put_timer(timer)
    await store._run_db(
        lambda conn: conn.execute(
            """
            UPDATE timers SET fire_at=?
            WHERE namespace=? AND workflow_id=? AND run_id=? AND timer_id=?
            """,
            (_iso(stale_due), "default", "prices:aapl", "r1", "wait"),
        )
    )

    assert await store.due_timers("", now) == []
    rows = await store._run_db(lambda conn: list(conn.execute("SELECT fire_at FROM timers")))
    assert [row["fire_at"] for row in rows] == [_iso(future_fire)]


async def test_indexed_due_timers_fires_past_record_with_stale_future_row(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    now = datetime.now(UTC)
    past_fire = now - timedelta(minutes=5)
    stale_future = now + timedelta(hours=1)
    await store.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    await store.put_timer(
        _timer(
            "prices:aapl",
            "r1",
            "wait",
            temporaless_pb2.TIMER_STATUS_SCHEDULED,
            past_fire,
        )
    )
    await store._run_db(
        lambda conn: conn.execute(
            """
            UPDATE timers SET fire_at=?
            WHERE namespace=? AND workflow_id=? AND run_id=? AND timer_id=?
            """,
            (_iso(stale_future), "default", "prices:aapl", "r1", "wait"),
        )
    )

    due = await store.due_timers("", now)
    rows = await store._run_db(lambda conn: list(conn.execute("SELECT fire_at FROM timers")))

    assert [timer.key.timer_id for timer in due] == ["wait"]
    assert [row["fire_at"] for row in rows] == [_iso(past_fire)]


def _workflow(
    workflow_id: str,
    run_id: str,
    status: temporaless_pb2.WorkflowStatus,
    *,
    completed_at: datetime | None = None,
    created_at: datetime | None = None,
) -> temporaless_pb2.WorkflowRecord:
    now = Timestamp()
    if created_at is None:
        now.GetCurrentTime()
    else:
        now.FromDatetime(created_at)
    record = temporaless_pb2.WorkflowRecord(
        schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
        key=WorkflowKey(workflow_id=workflow_id, run_id=run_id).to_proto(),
        workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
        code_version="test",
        status=status,
        created_at=now,
    )
    if completed_at is not None:
        completed = Timestamp()
        completed.FromDatetime(completed_at)
        record.completed_at.CopyFrom(completed)
    elif status in (
        temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        temporaless_pb2.WORKFLOW_STATUS_FAILED,
    ):
        record.completed_at.CopyFrom(now)
    return record


def _activity(
    workflow_id: str,
    run_id: str,
    activity_id: str,
    status: temporaless_pb2.ActivityStatus,
) -> temporaless_pb2.ActivityRecord:
    created = Timestamp()
    created.GetCurrentTime()
    completed = Timestamp()
    completed.GetCurrentTime()
    return temporaless_pb2.ActivityRecord(
        schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
        key=ActivityKey(
            workflow_id=workflow_id,
            run_id=run_id,
            activity_id=activity_id,
        ).to_proto(),
        activity_type="activity:google.protobuf.StringValue->google.protobuf.StringValue",
        code_version="test",
        status=status,
        created_at=created,
        completed_at=completed,
    )


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
    created.GetCurrentTime()
    return temporaless_pb2.TimerRecord(
        schema_version=TIMER_RECORD_SCHEMA_VERSION,
        key=TimerKey(workflow_id=workflow_id, run_id=run_id, timer_id=timer_id).to_proto(),
        timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
        code_version="test",
        status=status,
        fire_at=fire,
        created_at=created,
    )


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()
