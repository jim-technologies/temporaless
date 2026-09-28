import asyncio
import logging
import sqlite3
from datetime import UTC, datetime, timedelta

import opendal
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from google.protobuf.timestamp_pb2 import Timestamp
from temporaless.connectstore import ConnectQueryStore
from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    CLAIM_RECORD_SCHEMA_VERSION,
    TIMER_RECORD_SCHEMA_VERSION,
    WORKFLOW_RECORD_SCHEMA_VERSION,
    ActivityKey,
    ClaimKey,
    OpenDALStore,
    RunRecordValidationError,
    TimerKey,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2

import temporaless_indexstore.adapter as index_adapter
from temporaless_indexstore import IndexedStore, SQLiteExecutor


@pytest.fixture(params=["constructor", "injected"])
async def index_factory(request):
    opened = []

    async def create(operator, db_path=":memory:", *, claim_store=None, inner=None):
        if request.param == "constructor":
            if inner is None:
                store = IndexedStore.from_opendal(operator, db_path, claim_store=claim_store)
            else:
                store = IndexedStore(inner, db_path, operator=operator, claim_store=claim_store)
        else:
            store = await IndexedStore.from_executor(
                inner if inner is not None else OpenDALStore(operator),
                SQLiteExecutor(db_path),
                owns_executor=True,
                operator=operator,
                claim_store=claim_store,
            )
        opened.append(store)
        return store

    yield create
    for store in reversed(opened):
        await store.close()


async def test_write_through_lists_workflows(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")

    await store.put_workflow(_workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_FAILED))
    await store.put_workflow(
        _workflow("prices:msft", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )

    records, token = await store.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_FAILED)
    assert token == ""
    assert [record.key.workflow_id for record in records] == ["prices:aapl"]


async def test_default_index_is_an_in_memory_working_copy(index_factory, tmp_path) -> None:
    # Without a db_path the index is an in-memory SQLite database: no file,
    # and each instance sees only its own writes until it rebuilds from the
    # bucket.
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    writer = await index_factory(operator)
    sweep_copy = await index_factory(operator)
    try:
        await writer.put_workflow(
            _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_FAILED)
        )

        before, _ = await sweep_copy.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_FAILED)
        assert await sweep_copy.rebuild() == 0  # no corrupt records skipped
        after, _ = await sweep_copy.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_FAILED)
        database_rows = await sweep_copy._run_db(lambda conn: conn.execute("PRAGMA database_list"))
        database_files = [row["file"] for row in database_rows]
    finally:
        await writer.close()
        await sweep_copy.close()

    assert before == []
    assert [record.key.workflow_id for record in after] == ["prices:aapl"]
    assert database_files == [""]


async def test_claim_run_listing_passes_through(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    key = WorkflowKey(workflow_id="prices:aapl", run_id="r1")
    claim_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="arbitrary",
    )
    assert await store.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=claim_key.to_proto(),
            owner_id="owner",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
            resource_id=key.workflow_id,
        )
    )

    claims = await store.list_claims(key)

    assert [claim.key.claim_id for claim in claims] == ["arbitrary"]


async def test_rebuild_from_populated_bucket(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    bucket = OpenDALStore(operator)
    await bucket.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )

    indexed = await index_factory(operator, tmp_path / "index.sqlite")
    await indexed.rebuild()

    records, _ = await indexed.list_workflows(
        "", "prices:aapl", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED
    )
    assert [record.key.run_id for record in records] == ["r1"]


async def test_rebuild_dispatches_by_key_structure_not_substrings(index_factory, tmp_path) -> None:
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

    indexed = await index_factory(operator, tmp_path / "index.sqlite")
    await indexed.rebuild()

    due = await indexed.due_timers("", datetime.now(UTC))
    assert [timer.key.timer_id for timer in due] == ["wait"]


async def test_rebuild_skips_corrupt_records_without_poisoning_index(
    index_factory, tmp_path
) -> None:
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

    indexed = await index_factory(operator, tmp_path / "index.sqlite")
    skipped = await indexed.rebuild()

    records, _ = await indexed.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED)
    assert skipped == 2
    assert [(record.key.workflow_id, record.key.run_id) for record in records] == [
        ("prices:aapl", "r1")
    ]


async def test_rebuild_rejects_wrong_schema_and_payload_path_identity(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    wrong_path = WorkflowKey(workflow_id="wrong-path", run_id="r1").path()
    wrong_path_record = _workflow("payload-id", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    wrong_schema_key = WorkflowKey(workflow_id="wrong-schema", run_id="r1")
    wrong_schema_record = _workflow(
        wrong_schema_key.workflow_id,
        wrong_schema_key.run_id,
        temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
    )
    wrong_schema_record.schema_version = temporaless_pb2.RECORD_SCHEMA_VERSION_UNSPECIFIED
    for path, record in (
        (wrong_path, wrong_path_record),
        (wrong_schema_key.path(), wrong_schema_record),
    ):
        await operator.create_dir(path.rsplit("/", 1)[0] + "/")
        await operator.write(path, record.SerializeToString(deterministic=True))

    indexed = await index_factory(operator, tmp_path / "index.sqlite")

    assert await indexed.rebuild() == 2
    records, token = await indexed.list_workflows(
        "", "", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED
    )
    assert records == []
    assert token == ""


async def test_failed_rebuild_leaves_previous_index_intact(
    index_factory, tmp_path, monkeypatch
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
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


async def test_successful_rebuild_reports_cleanup_failure(
    index_factory, tmp_path, monkeypatch
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    await store.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )
    original_drop_rebuild_index = index_adapter._drop_rebuild_index

    async def fail_after_cleanup(conn, *, owner):
        await original_drop_rebuild_index(conn, owner=owner)
        raise sqlite3.OperationalError("forced rebuild cleanup failure")

    monkeypatch.setattr(index_adapter, "_drop_rebuild_index", fail_after_cleanup)

    with pytest.raises(sqlite3.OperationalError, match="forced rebuild cleanup failure"):
        await store.rebuild()

    monkeypatch.setattr(index_adapter, "_drop_rebuild_index", original_drop_rebuild_index)
    owners = await store._run_db(lambda conn: conn.execute("SELECT owner FROM _rebuild_owner"))
    assert len(owners) == 1  # Failed DDL cleanup rolls back, retaining recoverable ownership.
    await store._run_db(
        lambda conn: original_drop_rebuild_index(conn, owner=owners[0]["owner"]), immediate=True
    )
    assert await store.rebuild() == 0


async def test_rebuild_preserves_puts_written_during_walk(
    index_factory, tmp_path, monkeypatch
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    bucket = OpenDALStore(operator)
    await bucket.put_workflow(_workflow("seed", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED))
    store = await index_factory(operator, tmp_path / "index.sqlite")
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


async def test_rebuild_does_not_overwrite_or_resurrect_concurrent_mutations(
    index_factory, tmp_path, monkeypatch
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    db_path = tmp_path / "index.sqlite"
    store = await index_factory(operator, db_path)
    writer = await index_factory(operator, db_path)
    workflow_id = "prices:aapl"
    run_id = "r1"
    activity_key = ActivityKey(workflow_id=workflow_id, run_id=run_id, activity_id="fetch")
    await store.put_workflow(
        _workflow(workflow_id, run_id, temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    await store.put_activity(
        _activity(
            workflow_id,
            run_id,
            activity_key.activity_id,
            temporaless_pb2.ACTIVITY_STATUS_FAILED,
        )
    )
    now = datetime.now(UTC)
    await store.put_timer(
        _timer(
            workflow_id,
            run_id,
            "wait",
            temporaless_pb2.TIMER_STATUS_SCHEDULED,
            now + timedelta(hours=1),
        )
    )
    original_read_rebuild_record = index_adapter._read_rebuild_record
    mutated_kinds: set[str] = set()

    async def mutate_after_rebuild_read(operator, path, factory, key_factory):
        result = await original_read_rebuild_record(operator, path, factory, key_factory)
        record, _ = result
        if isinstance(record, temporaless_pb2.WorkflowRecord) and "workflow" not in mutated_kinds:
            mutated_kinds.add("workflow")
            await writer.put_workflow(
                _workflow(workflow_id, run_id, temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
            )
        elif isinstance(record, temporaless_pb2.ActivityRecord) and "activity" not in mutated_kinds:
            mutated_kinds.add("activity")
            assert await writer.delete_activity(activity_key)
        elif isinstance(record, temporaless_pb2.TimerRecord) and "timer" not in mutated_kinds:
            mutated_kinds.add("timer")
            await writer.put_timer(
                _timer(
                    workflow_id,
                    run_id,
                    "wait",
                    temporaless_pb2.TIMER_STATUS_CANCELED,
                    now + timedelta(hours=1),
                )
            )
        return result

    monkeypatch.setattr(index_adapter, "_read_rebuild_record", mutate_after_rebuild_read)

    await store.rebuild()

    async def select_statuses(conn):
        return (
            await conn.execute("SELECT status FROM workflows"),
            await conn.execute("SELECT status FROM activities"),
            await conn.execute("SELECT status FROM timers"),
        )

    rows = await store._run_db(select_statuses)
    workflow_rows, activity_rows, timer_rows = rows
    assert mutated_kinds == {"workflow", "activity", "timer"}
    assert [row["status"] for row in workflow_rows] == [
        int(temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    ]
    assert activity_rows == []
    assert [row["status"] for row in timer_rows] == [int(temporaless_pb2.TIMER_STATUS_CANCELED)]


async def test_rebuild_does_not_resurrect_deleted_unindexed_record(
    index_factory, tmp_path, monkeypatch
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    db_path = tmp_path / "index.sqlite"
    bucket = OpenDALStore(operator)
    key = WorkflowKey(workflow_id="prices:aapl", run_id="r1")
    await bucket.put_workflow(
        _workflow(key.workflow_id, key.run_id, temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )
    rebuilder = await index_factory(operator, db_path)
    writer = await index_factory(operator, db_path)
    original_read_rebuild_record = index_adapter._read_rebuild_record
    deleted = False

    async def delete_staged_unindexed_record(operator, path, factory, key_factory):
        nonlocal deleted
        result = await original_read_rebuild_record(operator, path, factory, key_factory)
        if isinstance(result[0], temporaless_pb2.WorkflowRecord) and not deleted:
            deleted = True
            assert await writer.delete_workflow(key)
        return result

    monkeypatch.setattr(
        index_adapter,
        "_read_rebuild_record",
        delete_staged_unindexed_record,
    )

    await rebuilder.rebuild()

    assert deleted
    assert await bucket.get_workflow(key) is None
    rows = await rebuilder._run_db(lambda conn: conn.execute("SELECT * FROM workflows"))
    assert rows == []


async def test_rebuild_does_not_resurrect_run_children_deleted_after_read(
    index_factory, tmp_path, monkeypatch
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    db_path = tmp_path / "index.sqlite"
    bucket = OpenDALStore(operator)
    key = WorkflowKey(workflow_id="prices:aapl", run_id="r1")
    await bucket.put_workflow(
        _workflow(key.workflow_id, key.run_id, temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )
    await bucket.put_activity(
        _activity(
            key.workflow_id,
            key.run_id,
            "fetch",
            temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
    )
    rebuilder = await index_factory(operator, db_path)
    writer = await index_factory(operator, db_path)
    original_read_rebuild_record = index_adapter._read_rebuild_record
    deleted = False

    async def delete_run_after_activity_read(operator, path, factory, key_factory):
        nonlocal deleted
        result = await original_read_rebuild_record(operator, path, factory, key_factory)
        if isinstance(result[0], temporaless_pb2.ActivityRecord) and not deleted:
            deleted = True
            assert await writer.delete_run(key) == 2
        return result

    monkeypatch.setattr(
        index_adapter,
        "_read_rebuild_record",
        delete_run_after_activity_read,
    )

    await rebuilder.rebuild()

    async def select_remaining(conn):
        return (
            await conn.execute("SELECT * FROM workflows"),
            await conn.execute("SELECT * FROM activities"),
        )

    rows = await rebuilder._run_db(select_remaining)
    assert rows == ([], [])


async def test_second_rebuild_coordinator_is_rejected(index_factory, tmp_path, monkeypatch) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    db_path = tmp_path / "index.sqlite"
    first = await index_factory(operator, db_path)
    second = await index_factory(operator, db_path)
    await first.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )
    original_read_rebuild_record = index_adapter._read_rebuild_record
    first_is_walking = asyncio.Event()
    release_first = asyncio.Event()

    async def pause_first_rebuild(operator, path, factory, key_factory):
        result = await original_read_rebuild_record(operator, path, factory, key_factory)
        if not first_is_walking.is_set():
            first_is_walking.set()
            await release_first.wait()
        return result

    monkeypatch.setattr(index_adapter, "_read_rebuild_record", pause_first_rebuild)
    first_task = asyncio.create_task(first.rebuild())
    await asyncio.wait_for(first_is_walking.wait(), timeout=2)
    try:
        with pytest.raises(RuntimeError, match="another index rebuild is in progress"):
            await second.rebuild()
    finally:
        release_first.set()
        await first_task


async def test_canceled_rebuild_cleans_staging_state(index_factory, tmp_path, monkeypatch) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    await store.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED)
    )
    original_run_db = store._run_db
    reset_completed = asyncio.Event()
    release_reset = asyncio.Event()
    first_call = True

    async def pause_after_reset(fn, **kwargs):
        nonlocal first_call
        result = await original_run_db(fn, **kwargs)
        if first_call:
            first_call = False
            reset_completed.set()
            await release_reset.wait()
        return result

    monkeypatch.setattr(store, "_run_db", pause_after_reset)
    task = asyncio.create_task(store.rebuild())
    await asyncio.wait_for(reset_completed.wait(), timeout=2)
    task.cancel()
    release_reset.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    monkeypatch.setattr(store, "_run_db", original_run_db)
    assert await store.rebuild() == 0


async def test_rebuild_skips_not_found_race_without_counting_corrupt(
    index_factory, tmp_path, monkeypatch
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    bucket = OpenDALStore(operator)
    await bucket.put_workflow(_workflow("keeper", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED))
    await bucket.put_workflow(_workflow("vanish", "r1", temporaless_pb2.WORKFLOW_STATUS_COMPLETED))
    store = await index_factory(operator, tmp_path / "index.sqlite")
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


async def test_list_workflows_pages_stably_with_order_by(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
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


async def test_list_workflows_repairs_stale_rows_and_fills_pages(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    bucket = OpenDALStore(operator)
    for idx in range(4):
        await store.put_workflow(
            _workflow(
                "prices:aapl",
                f"r{idx}",
                temporaless_pb2.WORKFLOW_STATUS_FAILED,
                created_at=datetime(2026, 7, idx + 1, tzinfo=UTC),
            )
        )

    # Bypass the write-through wrapper to model a missed index update and a
    # stale row whose authoritative object has already disappeared.
    await bucket.put_workflow(
        _workflow(
            "prices:aapl",
            "r0",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            created_at=datetime(2026, 7, 1, tzinfo=UTC),
        )
    )
    await bucket.delete_workflow(WorkflowKey(workflow_id="prices:aapl", run_id="r1"))

    first, token = await store.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_FAILED,
        order_by="created_at asc",
        page_size=1,
    )
    second, final_token = await store.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_FAILED,
        order_by="created_at asc",
        page_size=1,
        page_token=token,
    )

    assert [record.key.run_id for record in first] == ["r2"]
    assert token
    assert [record.key.run_id for record in second] == ["r3"]
    assert final_token == ""
    rows = await store._run_db(
        lambda conn: conn.execute("SELECT run_id, status FROM workflows ORDER BY run_id ASC")
    )
    assert [(row["run_id"], row["status"]) for row in rows] == [
        ("r0", int(temporaless_pb2.WORKFLOW_STATUS_COMPLETED)),
        ("r2", int(temporaless_pb2.WORKFLOW_STATUS_FAILED)),
        ("r3", int(temporaless_pb2.WORKFLOW_STATUS_FAILED)),
    ]


async def test_list_workflows_reselects_after_stale_sort_field_moves(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    bucket = OpenDALStore(operator)
    for run_id, day in (("a", 1), ("b", 2), ("c", 3)):
        await store.put_workflow(
            _workflow(
                "prices:aapl",
                run_id,
                temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
                created_at=datetime(2026, 7, day, tzinfo=UTC),
            )
        )
    await bucket.put_workflow(
        _workflow(
            "prices:aapl",
            "a",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            created_at=datetime(2026, 7, 4, tzinfo=UTC),
        )
    )

    run_ids: list[str] = []
    page_token = ""
    while True:
        records, page_token = await store.list_workflows(
            "",
            "prices:aapl",
            temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
            order_by="created_at asc",
            page_size=1,
            page_token=page_token,
        )
        run_ids.extend(record.key.run_id for record in records)
        if not page_token:
            break

    assert run_ids == ["b", "c", "a"]


async def test_list_workflows_restarts_page_when_later_candidate_moves(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    bucket = OpenDALStore(operator)
    for run_id, day in (("a", 1), ("b", 2), ("c", 3)):
        await store.put_workflow(
            _workflow(
                "prices:aapl",
                run_id,
                temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
                created_at=datetime(2026, 7, day, tzinfo=UTC),
            )
        )
    await bucket.put_workflow(
        _workflow(
            "prices:aapl",
            "b",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            created_at=datetime(2026, 6, 30, tzinfo=UTC),
        )
    )

    first, page_token = await store.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
        order_by="created_at asc",
        page_size=2,
    )
    second, final_token = await store.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
        order_by="created_at asc",
        page_size=2,
        page_token=page_token,
    )

    assert [record.key.run_id for record in first] == ["b", "a"]
    assert [record.key.run_id for record in second] == ["c"]
    assert final_token == ""


async def test_list_workflows_unlimited_restarts_after_only_row_is_repaired(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    bucket = OpenDALStore(operator)
    await store.put_workflow(
        _workflow(
            "prices:aapl",
            "r1",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            created_at=datetime(2026, 7, 1, tzinfo=UTC),
        )
    )
    await bucket.put_workflow(
        _workflow(
            "prices:aapl",
            "r1",
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            created_at=datetime(2026, 7, 2, tzinfo=UTC),
        )
    )

    records, page_token = await store.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
    )

    assert [record.key.run_id for record in records] == ["r1"]
    assert page_token == ""


async def test_page_token_is_bound_to_query(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    for run_id in ("a", "b"):
        await store.put_workflow(
            _workflow(
                "prices:aapl",
                run_id,
                temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            )
        )
    _, page_token = await store.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
        order_by="created_at asc",
        page_size=1,
    )

    with pytest.raises(ValueError, match="page_token is invalid"):
        await store.list_workflows(
            "",
            "prices:aapl",
            temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
            order_by="created_at desc",
            page_size=1,
            page_token=page_token,
        )


async def test_duplicate_order_by_field_is_rejected(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")

    with pytest.raises(ValueError, match="duplicate order_by field"):
        await store.list_workflows(
            "",
            "",
            temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED,
            order_by="workflow_id desc, workflow_id asc",
        )


async def test_list_activities_query_honors_order_by(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
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


async def test_list_activities_pages_equal_sort_values_by_full_key(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    created_at = Timestamp()
    created_at.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
    for activity_id in ("c", "a", "b"):
        record = _activity(
            "prices:aapl",
            "r1",
            activity_id,
            temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
        record.created_at.CopyFrom(created_at)
        await store.put_activity(record)

    activity_ids: list[str] = []
    page_token = ""
    while True:
        records, page_token = await store.list_activities_query(
            "",
            "prices:aapl",
            "r1",
            temporaless_pb2.ACTIVITY_STATUS_UNSPECIFIED,
            page_size=1,
            page_token=page_token,
        )
        activity_ids.extend(record.key.activity_id for record in records)
        if not page_token:
            break

    assert activity_ids == ["a", "b", "c"]


async def test_list_activities_reselects_after_stale_sort_field_moves(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    bucket = OpenDALStore(operator)
    for activity_id, day in (("a", 1), ("b", 2), ("c", 3)):
        record = _activity(
            "prices:aapl",
            "r1",
            activity_id,
            temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
        record.created_at.FromDatetime(datetime(2026, 7, day, tzinfo=UTC))
        await store.put_activity(record)
    moved = _activity(
        "prices:aapl",
        "r1",
        "a",
        temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
    )
    moved.created_at.FromDatetime(datetime(2026, 7, 4, tzinfo=UTC))
    await bucket.put_activity(moved)

    activity_ids: list[str] = []
    page_token = ""
    while True:
        records, page_token = await store.list_activities_query(
            "",
            "prices:aapl",
            "r1",
            temporaless_pb2.ACTIVITY_STATUS_UNSPECIFIED,
            order_by="created_at asc",
            page_size=1,
            page_token=page_token,
        )
        activity_ids.extend(record.key.activity_id for record in records)
        if not page_token:
            break

    assert activity_ids == ["b", "c", "a"]


async def test_list_activities_restarts_page_when_later_candidate_moves(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    bucket = OpenDALStore(operator)
    for activity_id, day in (("a", 1), ("b", 2), ("c", 3)):
        record = _activity(
            "prices:aapl",
            "r1",
            activity_id,
            temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
        record.created_at.FromDatetime(datetime(2026, 7, day, tzinfo=UTC))
        await store.put_activity(record)
    moved = _activity(
        "prices:aapl",
        "r1",
        "b",
        temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
    )
    moved.created_at.FromDatetime(datetime(2026, 6, 30, tzinfo=UTC))
    await bucket.put_activity(moved)

    first, page_token = await store.list_activities_query(
        "",
        "prices:aapl",
        "r1",
        temporaless_pb2.ACTIVITY_STATUS_UNSPECIFIED,
        order_by="created_at asc",
        page_size=2,
    )
    second, final_token = await store.list_activities_query(
        "",
        "prices:aapl",
        "r1",
        temporaless_pb2.ACTIVITY_STATUS_UNSPECIFIED,
        order_by="created_at asc",
        page_size=2,
        page_token=page_token,
    )

    assert [record.key.activity_id for record in first] == ["b", "a"]
    assert [record.key.activity_id for record in second] == ["c"]
    assert final_token == ""


async def test_list_activities_unlimited_restarts_after_only_row_is_repaired(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    bucket = OpenDALStore(operator)
    original = _activity(
        "prices:aapl",
        "r1",
        "fetch",
        temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
    )
    original.created_at.FromDatetime(datetime(2026, 7, 1, tzinfo=UTC))
    await store.put_activity(original)
    updated = _activity(
        "prices:aapl",
        "r1",
        "fetch",
        temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
    )
    updated.created_at.FromDatetime(datetime(2026, 7, 2, tzinfo=UTC))
    await bucket.put_activity(updated)

    records, page_token = await store.list_activities_query(
        "",
        "prices:aapl",
        "r1",
        temporaless_pb2.ACTIVITY_STATUS_UNSPECIFIED,
    )

    assert [record.key.activity_id for record in records] == ["fetch"]
    assert page_token == ""


async def test_list_activities_query_rechecks_status_and_prunes_missing_rows(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    bucket = OpenDALStore(operator)
    for activity_id in ("a", "b", "c"):
        await store.put_activity(
            _activity(
                "prices:aapl",
                "r1",
                activity_id,
                temporaless_pb2.ACTIVITY_STATUS_FAILED,
            )
        )
    await bucket.put_activity(
        _activity(
            "prices:aapl",
            "r1",
            "a",
            temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
        )
    )
    await bucket.delete_activity(
        ActivityKey(workflow_id="prices:aapl", run_id="r1", activity_id="b")
    )

    records, token = await store.list_activities_query(
        "",
        "prices:aapl",
        "r1",
        temporaless_pb2.ACTIVITY_STATUS_FAILED,
        order_by="activity_id asc",
        page_size=1,
    )

    assert [record.key.activity_id for record in records] == ["c"]
    assert token == ""
    rows = await store._run_db(
        lambda conn: conn.execute(
            "SELECT activity_id, status FROM activities ORDER BY activity_id ASC"
        )
    )
    assert [(row["activity_id"], row["status"]) for row in rows] == [
        ("a", int(temporaless_pb2.ACTIVITY_STATUS_COMPLETED)),
        ("c", int(temporaless_pb2.ACTIVITY_STATUS_FAILED)),
    ]


async def test_sweep_deletes_bucket_and_index_rows(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
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


async def test_sweep_rechecks_authoritative_workflow_before_deletion(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    bucket = OpenDALStore(operator)
    key = WorkflowKey(workflow_id="prices:aapl", run_id="reopened")
    await store.put_workflow(
        _workflow(
            key.workflow_id,
            key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime.now(UTC) - timedelta(days=2),
        )
    )
    await bucket.put_workflow(
        _workflow(
            key.workflow_id,
            key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
        )
    )

    deleted = await store.sweep("", datetime.now(UTC), timedelta(days=1))

    assert deleted == 0
    record = await bucket.get_workflow(key)
    assert record is not None
    assert record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    rows = await store._run_db(
        lambda conn: conn.execute("SELECT status, completed_at FROM workflows")
    )
    assert [(row["status"], row["completed_at"]) for row in rows] == [
        (int(temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS), "")
    ]


async def test_sweep_deletes_claims_from_separate_claim_store(index_factory, tmp_path) -> None:
    records = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "records")))
    claims = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "claims")))
    store = await index_factory(None, tmp_path / "index.sqlite", inner=records, claim_store=claims)
    key = WorkflowKey(workflow_id="prices:aapl", run_id="old")
    await store.put_workflow(
        _workflow(
            key.workflow_id,
            key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime.now(UTC) - timedelta(days=2),
        )
    )
    claim_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="activity:fetch",
    )
    assert await claims.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=claim_key.to_proto(),
            owner_id="worker",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
            resource_id="fetch",
        )
    )

    deleted = await store.sweep("", datetime.now(UTC), timedelta(days=1))

    assert deleted == 1
    assert await records.get_workflow(key) is None
    assert await claims.get_claim(claim_key) is None


async def test_sweep_rejects_list_incapable_claim_store_before_mutation(
    index_factory, tmp_path
) -> None:
    class PointOnlyClaimStore:
        def __init__(self, inner: OpenDALStore) -> None:
            self._inner = inner

        async def claim_capability(self):
            return await self._inner.claim_capability()

        async def get_claim(self, key):
            return await self._inner.get_claim(key)

        async def try_create_claim(self, record):
            return await self._inner.try_create_claim(record)

        async def delete_claim(self, key):
            return await self._inner.delete_claim(key)

    records = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "records")))
    claims = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "claims")))
    point_only = PointOnlyClaimStore(claims)
    store = await index_factory(
        None, tmp_path / "index.sqlite", inner=records, claim_store=point_only
    )
    query = ConnectQueryStore.local(store)
    key = WorkflowKey(workflow_id="prices:aapl", run_id="old")
    await store.put_workflow(
        _workflow(
            key.workflow_id,
            key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime.now(UTC) - timedelta(days=2),
        )
    )
    claim_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="workflow:execution",
    )
    assert await point_only.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=claim_key.to_proto(),
            owner_id="worker",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
            resource_id=key.workflow_id,
        )
    )

    with pytest.raises(ConnectError) as captured:
        await query.sweep("", datetime.now(UTC), timedelta(days=1))

    assert captured.value.code is Code.FAILED_PRECONDITION
    assert await records.get_workflow(key) is not None
    assert await claims.get_claim(claim_key) is not None
    indexed, _ = await store.list_workflows(
        "", key.workflow_id, temporaless_pb2.WORKFLOW_STATUS_COMPLETED
    )
    assert [record.key.run_id for record in indexed] == [key.run_id]


async def test_sweep_respects_no_claims_capability(index_factory, tmp_path) -> None:
    class NoClaimsStore:
        async def claim_capability(self):
            return temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS

        async def get_claim(self, key):
            raise AssertionError(f"get_claim must not be called: {key}")

        async def try_create_claim(self, record):
            raise AssertionError(f"try_create_claim must not be called: {record}")

        async def delete_claim(self, key):
            raise AssertionError(f"delete_claim must not be called: {key}")

    records = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "records")))
    store = await index_factory(
        None, tmp_path / "index.sqlite", inner=records, claim_store=NoClaimsStore()
    )
    key = WorkflowKey(workflow_id="prices:aapl", run_id="old")
    await store.put_workflow(
        _workflow(
            key.workflow_id,
            key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime.now(UTC) - timedelta(days=2),
        )
    )

    assert await store.sweep("", datetime.now(UTC), timedelta(days=1)) == 1
    assert await records.get_workflow(key) is None


async def test_sweep_rejects_reserved_cas_capability_before_mutation(
    index_factory, tmp_path
) -> None:
    class ReservedCASStore:
        async def claim_capability(self):
            return temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS

        async def get_claim(self, key):
            raise AssertionError(f"get_claim must not be called: {key}")

        async def try_create_claim(self, record):
            raise AssertionError(f"try_create_claim must not be called: {record}")

        async def delete_claim(self, key):
            raise AssertionError(f"delete_claim must not be called: {key}")

        async def list_claims(self, key):
            raise AssertionError(f"list_claims must not be called: {key}")

    records = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "records")))
    store = await index_factory(
        None, tmp_path / "index.sqlite", inner=records, claim_store=ReservedCASStore()
    )
    key = WorkflowKey(workflow_id="prices:cas", run_id="old")
    await store.put_workflow(
        _workflow(
            key.workflow_id,
            key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime.now(UTC) - timedelta(days=2),
        )
    )

    with pytest.raises(ValueError, match="unsupported by the current create-only"):
        await store.sweep("", datetime.now(UTC), timedelta(days=1))

    claim_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="workflow:execution",
    )
    claim = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=claim_key.to_proto(),
        owner_id="worker",
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
        resource_id=key.workflow_id,
    )
    with pytest.raises(ValueError, match="unsupported by the current create-only"):
        await store.try_create_claim(claim)
    with pytest.raises(ValueError, match="unsupported by the current create-only"):
        await store.delete_claim(claim_key)

    assert await records.get_workflow(key) is not None


async def test_sweep_prevalidates_separate_claim_listing_before_mutation(
    index_factory, tmp_path
) -> None:
    class CorruptClaimRunStore:
        def __init__(
            self,
            inner: OpenDALStore,
            records: list[temporaless_pb2.ClaimRecord],
        ) -> None:
            self._inner = inner
            self._records = records
            self.delete_calls = 0

        async def claim_capability(self):
            return await self._inner.claim_capability()

        async def get_claim(self, key):
            return await self._inner.get_claim(key)

        async def try_create_claim(self, record):
            return await self._inner.try_create_claim(record)

        async def delete_claim(self, key):
            self.delete_calls += 1
            return await self._inner.delete_claim(key)

        async def list_claims(self, _key):
            return self._records

    records = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "records")))
    claims = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "claims")))
    key = WorkflowKey(workflow_id="prices:aapl", run_id="old")
    valid_claim_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="valid",
    )
    valid = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=valid_claim_key.to_proto(),
        owner_id="worker",
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
        resource_id=key.workflow_id,
    )
    assert await claims.try_create_claim(valid)
    misplaced = temporaless_pb2.ClaimRecord(
        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
        key=ClaimKey(
            workflow_id=key.workflow_id,
            run_id="other",
            claim_id="misplaced",
        ).to_proto(),
        owner_id="worker",
        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
        resource_id=key.workflow_id,
    )
    corrupt_claims = CorruptClaimRunStore(claims, [valid, misplaced])
    store = await index_factory(
        None, tmp_path / "index.sqlite", inner=records, claim_store=corrupt_claims
    )
    query = ConnectQueryStore.local(store)
    await store.put_workflow(
        _workflow(
            key.workflow_id,
            key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime.now(UTC) - timedelta(days=2),
        )
    )

    with pytest.raises(RunRecordValidationError, match="claim payload key"):
        await query.sweep("", datetime.now(UTC), timedelta(days=1))

    assert corrupt_claims.delete_calls == 0
    assert await claims.get_claim(valid_claim_key) is not None
    assert await records.get_workflow(key) is not None


async def test_sweep_prevalidates_record_listing_before_separate_claim_deletion(
    index_factory,
    tmp_path,
) -> None:
    records_operator = opendal.AsyncOperator("fs", root=str(tmp_path / "records"))
    records = OpenDALStore(records_operator)
    claims = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "claims")))
    store = await index_factory(None, tmp_path / "index.sqlite", inner=records, claim_store=claims)
    query = ConnectQueryStore.local(store)
    key = WorkflowKey(workflow_id="prices:aapl", run_id="old")
    await store.put_workflow(
        _workflow(
            key.workflow_id,
            key.run_id,
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            completed_at=datetime.now(UTC) - timedelta(days=2),
        )
    )
    claim_key = ClaimKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        claim_id="valid",
    )
    assert await claims.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=claim_key.to_proto(),
            owner_id="worker",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
            resource_id=key.workflow_id,
        )
    )
    path_key = ActivityKey(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
        activity_id="misplaced",
    )
    misplaced = temporaless_pb2.ActivityRecord(
        schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
        key=ActivityKey(
            workflow_id=key.workflow_id,
            run_id="other",
            activity_id="misplaced",
        ).to_proto(),
        activity_type="activity:test",
        status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
    )
    await records_operator.create_dir(path_key.dir_path())
    await records_operator.write(path_key.path(), misplaced.SerializeToString(deterministic=True))

    with pytest.raises(RunRecordValidationError, match="activity payload key"):
        await query.sweep("", datetime.now(UTC), timedelta(days=1))

    assert await claims.get_claim(claim_key) is not None
    assert await records.get_workflow(key) is not None
    assert await records_operator.exists(path_key.path())


async def test_indexed_due_timers(index_factory, tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
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


async def test_indexed_due_timers_recovers_authoritative_timer_after_workflow_reopens(
    index_factory,
    tmp_path,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
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
    due = await store.due_timers("", datetime.now(UTC))
    assert len(due) == 1
    assert due[0].key.timer_id == "wait"


async def test_indexed_due_timers_self_heals_future_record_stale_due_row(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
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
    rows = await store._run_db(lambda conn: conn.execute("SELECT fire_at FROM timers"))
    assert [row["fire_at"] for row in rows] == [_iso(future_fire)]


async def test_indexed_due_timers_fires_past_record_with_stale_future_row(
    index_factory, tmp_path
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
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
    rows = await store._run_db(lambda conn: conn.execute("SELECT fire_at FROM timers"))

    assert [timer.key.timer_id for timer in due] == ["wait"]
    assert [row["fire_at"] for row in rows] == [_iso(past_fire)]


async def test_timer_remains_discoverable_after_index_upsert_failure(
    index_factory,
    tmp_path,
    monkeypatch,
    caplog,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    now = datetime.now(UTC)
    await store.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    original_run_db = store._run_db
    failed = False

    async def fail_first_index_write(fn):
        nonlocal failed
        if not failed:
            failed = True
            raise sqlite3.OperationalError("forced timer-index outage")
        return await original_run_db(fn)

    monkeypatch.setattr(store, "_run_db", fail_first_index_write)
    with caplog.at_level(logging.ERROR, logger="temporaless_indexstore.adapter"):
        await store.put_timer(
            _timer(
                "prices:aapl",
                "r1",
                "wait",
                temporaless_pb2.TIMER_STATUS_SCHEDULED,
                now - timedelta(seconds=1),
            )
        )

    rows = await original_run_db(lambda conn: conn.execute("SELECT * FROM timers"))
    assert rows == []
    assert "durable timer remains discoverable" in caplog.text

    due = await store.due_timers("", now)
    repaired = await original_run_db(lambda conn: conn.execute("SELECT * FROM timers"))

    assert [item.key.timer_id for item in due] == ["wait"]
    assert [row["timer_id"] for row in repaired] == ["wait"]


async def test_execution_records_remain_durable_during_index_outage(
    index_factory,
    tmp_path,
    monkeypatch,
    caplog,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = await index_factory(operator, tmp_path / "index.sqlite")
    workflow = _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    activity = _activity("prices:aapl", "r1", "fetch", temporaless_pb2.ACTIVITY_STATUS_COMPLETED)

    async def fail_index(_fn):
        raise sqlite3.OperationalError("forced execution-index outage")

    monkeypatch.setattr(store, "_run_db", fail_index)
    with caplog.at_level(logging.ERROR, logger="temporaless_indexstore.adapter"):
        await store.put_workflow(workflow)
        await store.put_activity(activity)

    bucket = OpenDALStore(operator)
    assert await bucket.get_workflow(WorkflowKey(workflow_id="prices:aapl", run_id="r1"))
    assert await bucket.get_activity(
        ActivityKey(workflow_id="prices:aapl", run_id="r1", activity_id="fetch")
    )
    assert "workflow index update failed" in caplog.text
    assert "activity index update failed" in caplog.text


async def test_due_timers_uses_bucket_ledger_during_index_outage(
    index_factory,
    tmp_path,
    monkeypatch,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    bucket = OpenDALStore(operator)
    store = await index_factory(operator, tmp_path / "index.sqlite", inner=bucket)
    now = datetime.now(UTC)
    await bucket.put_workflow(
        _workflow("prices:aapl", "r1", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    )
    await bucket.put_timer(
        _timer(
            "prices:aapl",
            "r1",
            "wait",
            temporaless_pb2.TIMER_STATUS_SCHEDULED,
            now - timedelta(seconds=1),
        )
    )

    async def fail_index(_fn):
        raise sqlite3.OperationalError("forced timer-index outage")

    monkeypatch.setattr(store, "_run_db", fail_index)

    due = await store.due_timers("", now)

    assert [item.key.timer_id for item in due] == ["wait"]


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
        status=status,
        fire_at=fire,
        created_at=created,
    )


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


@pytest.mark.parametrize("file_index", [False, True])
async def test_previous_sqlite_query_token_remains_compatible(index_factory, tmp_path, file_index):
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    index = await index_factory(operator, tmp_path / "index.sqlite" if file_index else ":memory:")
    for day in (1, 2):
        await index.put_workflow(
            _workflow(
                "prices:aapl",
                f"r{day}",
                temporaless_pb2.WORKFLOW_STATUS_FAILED,
                created_at=datetime(2026, 7, day, tzinfo=UTC),
            )
        )
    first, token = await index.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_FAILED,
        order_by="created_at asc",
        page_size=1,
    )
    # Generated by the pre-executor implementation at 719e795, not by this test.
    assert token == "v1.553bdf75da7546fb01d8a8fce2965ba2241badb96b290c6007f8375d78c9b6a1.1"
    second, final = await index.list_workflows(
        "",
        "prices:aapl",
        temporaless_pb2.WORKFLOW_STATUS_FAILED,
        order_by="created_at asc",
        page_size=1,
        page_token=token,
    )
    assert [record.key.run_id for record in first + second] == ["r1", "r2"]
    assert final == ""
