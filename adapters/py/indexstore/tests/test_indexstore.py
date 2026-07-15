import asyncio
import logging
import sqlite3
import threading
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
from temporaless_indexstore import IndexedStore


async def test_blocked_database_operation_does_not_block_event_loop(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
    operation_started = threading.Event()
    release_operation = threading.Event()
    loop_progressed = asyncio.Event()
    loop = asyncio.get_running_loop()

    def blocked_operation(_conn: sqlite3.Connection) -> None:
        operation_started.set()
        if not release_operation.wait(timeout=5):
            raise TimeoutError("test did not release blocked database operation")

    def signal_loop_after_operation_starts() -> None:
        if operation_started.wait(timeout=5):
            loop.call_soon_threadsafe(loop_progressed.set)

    sentinel = threading.Thread(target=signal_loop_after_operation_starts, daemon=True)
    # The timer prevents the regression case from hanging pytest: when SQLite
    # work runs on the event-loop thread, the sentinel cannot run until this
    # fallback releases the blocked operation.
    fallback_release = threading.Timer(1, release_operation.set)
    sentinel.start()
    fallback_release.start()
    db_task = asyncio.create_task(store._run_db(blocked_operation))
    try:
        await asyncio.wait_for(loop_progressed.wait(), timeout=2)
        assert not db_task.done()
    finally:
        release_operation.set()
        await db_task
        fallback_release.cancel()
        sentinel.join(timeout=1)
        await store.close()


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


async def test_claim_run_listing_passes_through(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
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
            code_version="v1",
        )
    )

    claims = await store.list_claims(key)

    assert [claim.key.claim_id for claim in claims] == ["arbitrary"]


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


async def test_rebuild_rejects_wrong_schema_and_payload_path_identity(tmp_path) -> None:
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

    indexed = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")

    assert await indexed.rebuild() == 2
    records, token = await indexed.list_workflows(
        "", "", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED
    )
    assert records == []
    assert token == ""


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


async def test_list_workflows_repairs_stale_rows_and_fills_pages(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
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
        lambda conn: list(conn.execute("SELECT run_id, status FROM workflows ORDER BY run_id ASC"))
    )
    assert [(row["run_id"], row["status"]) for row in rows] == [
        ("r0", int(temporaless_pb2.WORKFLOW_STATUS_COMPLETED)),
        ("r2", int(temporaless_pb2.WORKFLOW_STATUS_FAILED)),
        ("r3", int(temporaless_pb2.WORKFLOW_STATUS_FAILED)),
    ]


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


async def test_list_activities_query_rechecks_status_and_prunes_missing_rows(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
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
        lambda conn: list(
            conn.execute("SELECT activity_id, status FROM activities ORDER BY activity_id ASC")
        )
    )
    assert [(row["activity_id"], row["status"]) for row in rows] == [
        ("a", int(temporaless_pb2.ACTIVITY_STATUS_COMPLETED)),
        ("c", int(temporaless_pb2.ACTIVITY_STATUS_FAILED)),
    ]


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


async def test_sweep_rechecks_authoritative_workflow_before_deletion(tmp_path) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
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
        lambda conn: list(conn.execute("SELECT status, completed_at FROM workflows"))
    )
    assert [(row["status"], row["completed_at"]) for row in rows] == [
        (int(temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS), "")
    ]


async def test_sweep_deletes_claims_from_separate_claim_store(tmp_path) -> None:
    records = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "records")))
    claims = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "claims")))
    store = IndexedStore(
        records,
        tmp_path / "index.sqlite",
        claim_store=claims,
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
            code_version="v1",
        )
    )

    deleted = await store.sweep("", datetime.now(UTC), timedelta(days=1))

    assert deleted == 1
    assert await records.get_workflow(key) is None
    assert await claims.get_claim(claim_key) is None


async def test_sweep_rejects_list_incapable_claim_store_before_mutation(tmp_path) -> None:
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
    store = IndexedStore(
        records,
        tmp_path / "index.sqlite",
        claim_store=point_only,
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
            code_version="v1",
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


async def test_sweep_respects_no_claims_capability(tmp_path) -> None:
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
    store = IndexedStore(
        records,
        tmp_path / "index.sqlite",
        claim_store=NoClaimsStore(),
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


async def test_sweep_prevalidates_separate_claim_listing_before_mutation(tmp_path) -> None:
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
        code_version="v1",
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
        code_version="v1",
    )
    corrupt_claims = CorruptClaimRunStore(claims, [valid, misplaced])
    store = IndexedStore(
        records,
        tmp_path / "index.sqlite",
        claim_store=corrupt_claims,
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
    tmp_path,
) -> None:
    records_operator = opendal.AsyncOperator("fs", root=str(tmp_path / "records"))
    records = OpenDALStore(records_operator)
    claims = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "claims")))
    store = IndexedStore(
        records,
        tmp_path / "index.sqlite",
        claim_store=claims,
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
        claim_id="valid",
    )
    assert await claims.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=claim_key.to_proto(),
            owner_id="worker",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
            resource_id=key.workflow_id,
            code_version="v1",
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
        code_version="v1",
        status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
    )
    await records_operator.create_dir(path_key.dir_path())
    await records_operator.write(path_key.path(), misplaced.SerializeToString(deterministic=True))

    with pytest.raises(RunRecordValidationError, match="activity payload key"):
        await query.sweep("", datetime.now(UTC), timedelta(days=1))

    assert await claims.get_claim(claim_key) is not None
    assert await records.get_workflow(key) is not None
    assert await records_operator.exists(path_key.path())


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


async def test_indexed_due_timers_recovers_authoritative_timer_after_workflow_reopens(
    tmp_path,
) -> None:
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
    due = await store.due_timers("", datetime.now(UTC))
    assert len(due) == 1
    assert due[0].key.timer_id == "wait"


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


async def test_timer_remains_discoverable_after_index_upsert_failure(
    tmp_path,
    monkeypatch,
    caplog,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
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

    rows = await original_run_db(lambda conn: list(conn.execute("SELECT * FROM timers")))
    assert rows == []
    assert "durable timer remains discoverable" in caplog.text

    due = await store.due_timers("", now)
    repaired = await original_run_db(lambda conn: list(conn.execute("SELECT * FROM timers")))

    assert [item.key.timer_id for item in due] == ["wait"]
    assert [row["timer_id"] for row in repaired] == ["wait"]


async def test_execution_records_remain_durable_during_index_outage(
    tmp_path,
    monkeypatch,
    caplog,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    store = IndexedStore.from_opendal(operator, tmp_path / "index.sqlite")
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
    tmp_path,
    monkeypatch,
) -> None:
    operator = opendal.AsyncOperator("fs", root=str(tmp_path / "bucket"))
    bucket = OpenDALStore(operator)
    store = IndexedStore(bucket, tmp_path / "index.sqlite", operator=operator)
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
