import asyncio
import sqlite3
import threading
from contextlib import asynccontextmanager

import opendal
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from temporaless.connectstore import RecordStoreService
from temporaless.storage import OpenDALStore
from temporaless.v1 import temporaless_pb2 as pb

from temporaless_indexstore import IndexedStore, SQLiteExecutor


@pytest.fixture(params=[":memory:", "file"])
def executor(request, tmp_path):
    return SQLiteExecutor(
        ":memory:" if request.param == ":memory:" else tmp_path / "index.sqlite",
        schema=("CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT)",),
    )


async def test_atomic_mutations_rollback_error_and_cancel(executor):
    async with executor.transaction() as session:
        await session.execute("INSERT INTO records VALUES(1, 'committed')")
    with pytest.raises(sqlite3.IntegrityError):
        async with executor.transaction() as session:
            await session.execute("UPDATE records SET value='partial'")
            await session.execute("INSERT INTO records VALUES(1, 'duplicate')")

    written = asyncio.Event()

    async def interrupted():
        async with executor.transaction() as session:
            await session.execute("UPDATE records SET value='cancelled'")
            written.set()
            await asyncio.Future()

    task = asyncio.create_task(interrupted())
    await asyncio.wait_for(written.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with executor.transaction() as session:
        rows = await session.execute("SELECT * FROM records")
    assert rows == [{"id": 1, "value": "committed"}]
    await executor.close()
    assert rows[0]["value"] == "committed"  # Materialized beyond connection lifetime.


async def test_session_cannot_escape_or_break_its_transaction(executor):
    async with executor.transaction() as session:
        for statement in ("COMMIT", "ROLLBACK", "BEGIN", "SAVEPOINT nested_savepoint"):
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                await session.execute(statement)
        with pytest.raises(RuntimeError, match="nested"):
            async with executor.transaction():
                pytest.fail("nested transaction entered")
        with pytest.raises(RuntimeError, match="inside its transaction"):
            await executor.close()
        with pytest.raises(RuntimeError, match="another task"):
            await asyncio.create_task(session.execute("SELECT * FROM records"))
        await session.execute("INSERT INTO records VALUES(1, 'valid')")
    with pytest.raises(RuntimeError, match="inactive"):
        await session.execute("SELECT * FROM records")
    await executor.close()
    await executor.close()
    with pytest.raises(RuntimeError, match="closed"):
        async with executor.transaction():
            pytest.fail("closed executor entered")


@pytest.mark.parametrize("immediate", [False, True])
async def test_blocked_sql_cancellation_drains_before_rollback_and_close(tmp_path, immediate):
    path = tmp_path / "index.sqlite"
    executor = SQLiteExecutor(path, schema=("CREATE TABLE records(id INTEGER)",))
    blocker = sqlite3.connect(path, check_same_thread=False)
    blocker.execute("BEGIN IMMEDIATE")
    started = asyncio.Event()
    loop = asyncio.get_running_loop()
    executor._connection.set_trace_callback(
        lambda sql: (
            loop.call_soon_threadsafe(started.set)
            if sql == ("BEGIN IMMEDIATE" if immediate else "INSERT INTO records VALUES(1)")
            else None
        )
    )
    # A real independent SQLite writer blocks the worker, not a fake callback.
    # The fallback prevents an event-loop regression from hanging the test.
    fallback = threading.Timer(3, blocker.rollback)
    fallback.start()

    async def write():
        async with executor.transaction(immediate=immediate) as session:
            await session.execute("INSERT INTO records VALUES(1)")

    task = asyncio.create_task(write())
    try:
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        await asyncio.sleep(0.02)
        task.cancel()  # Repeated cancellation must not release the connection early.
        closing = asyncio.create_task(executor.close())
        await asyncio.sleep(0.02)
        assert not task.done()
        assert not closing.done()
        blocker.rollback()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(closing, 2)
        assert blocker.execute("SELECT * FROM records").fetchall() == []
    finally:
        blocker.rollback()
        fallback.cancel()
        blocker.close()
        await executor.close()


async def test_cancellation_after_commit_starts_has_documented_committed_outcome(tmp_path):
    path = tmp_path / "index.sqlite"
    executor = SQLiteExecutor(path, schema=("CREATE TABLE records(id INTEGER)",))
    committing = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()

    def trace(sql):
        if sql == "COMMIT":
            loop.call_soon_threadsafe(committing.set)
            assert release.wait(3)

    executor._connection.set_trace_callback(trace)

    async def write():
        async with executor.transaction() as session:
            await session.execute("INSERT INTO records VALUES(1)")

    task = asyncio.create_task(write())
    try:
        await asyncio.wait_for(committing.wait(), 2)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await executor.close()
    with sqlite3.connect(path) as observer:
        assert observer.execute("SELECT * FROM records").fetchall() == [(1,)]


@pytest.mark.parametrize("owned", [False, True])
async def test_injected_setup_and_close_ownership(tmp_path, owned):
    inner = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "bucket")))
    executor = SQLiteExecutor()
    index = await IndexedStore.from_executor(inner, executor, owns_executor=owned)
    await index.close()
    await index.close()
    with pytest.raises(RuntimeError, match="index is closed"):
        await index.list_workflows("", "", pb.WORKFLOW_STATUS_UNSPECIFIED)
    if owned:
        with pytest.raises(RuntimeError, match="closed"):
            async with executor.transaction():
                pytest.fail("owned executor still open")
    else:
        async with executor.transaction() as session:
            assert await session.execute("SELECT * FROM workflows") == []
        await executor.close()


@pytest.mark.parametrize("owned", [False, True])
async def test_failed_injected_setup_rolls_back_without_closing_borrowed_executor(tmp_path, owned):
    inner = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "bucket")))
    executor = SQLiteExecutor()

    class FailSetup:
        closed = False

        @asynccontextmanager
        async def transaction(self, *, immediate=False):
            async with executor.transaction(immediate=immediate) as session:
                yield session
                raise RuntimeError("setup failed before commit")

        async def close(self):
            self.closed = True
            await executor.close()

    injected = FailSetup()
    with pytest.raises(RuntimeError, match="setup failed"):
        await IndexedStore.from_executor(inner, injected, owns_executor=owned)
    assert injected.closed is owned
    if not owned:
        async with executor.transaction() as session:
            assert await session.execute("SELECT name FROM sqlite_schema") == []
        await executor.close()


async def test_index_executor_adds_no_fencing_capability(tmp_path):
    inner = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "bucket")))
    index = await IndexedStore.from_executor(inner, SQLiteExecutor(), owns_executor=True)
    service = RecordStoreService(index)
    capabilities = await service.get_store_capabilities(pb.GetStoreCapabilitiesRequest(), None)
    assert capabilities.fenced_execution_capability == pb.FENCED_EXECUTION_CAPABILITY_UNSUPPORTED
    for operation in (
        service.acquire_execution(pb.AcquireExecutionRequest(), None),
        service.renew_execution(pb.RenewExecutionRequest(), None),
        service.release_execution(pb.ReleaseExecutionRequest(), None),
        service.apply_execution_mutations(pb.ApplyExecutionMutationsRequest(), None),
        service.get_execution_operation(pb.GetExecutionOperationRequest(), None),
    ):
        with pytest.raises(ConnectError) as raised:
            await operation
        assert raised.value.code == Code.UNIMPLEMENTED
    await index.close()


@pytest.mark.parametrize("owned", [False, True])
async def test_index_close_drains_accepted_operation_and_respects_ownership(tmp_path, owned):
    inner = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "bucket")))
    executor = SQLiteExecutor(tmp_path / "index.sqlite")
    index = await IndexedStore.from_executor(inner, executor, owns_executor=owned)
    active = asyncio.Event()
    release = asyncio.Event()

    async def operation(session):
        await session.execute("CREATE TABLE close_proof(value INTEGER)")
        await session.execute("INSERT INTO close_proof VALUES(1)")
        active.set()
        await release.wait()

    writing = asyncio.create_task(index._run_db(operation))
    await asyncio.wait_for(active.wait(), 2)
    closing = asyncio.create_task(index.close())
    await asyncio.sleep(0.02)
    assert not closing.done()
    release.set()
    await writing
    await asyncio.wait_for(closing, 2)
    with sqlite3.connect(tmp_path / "index.sqlite") as observer:
        assert observer.execute("SELECT * FROM close_proof").fetchall() == [(1,)]
    if not owned:
        async with executor.transaction() as session:
            assert await session.execute("SELECT * FROM close_proof") == [{"value": 1}]
        await executor.close()


@pytest.mark.parametrize("owned", [False, True])
async def test_cancelled_async_factory_rolls_back_setup_and_closes_only_owned(tmp_path, owned):
    inner = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path / "bucket")))
    executor = SQLiteExecutor()
    staged = asyncio.Event()

    class PausedSetup:
        closed = False

        @asynccontextmanager
        async def transaction(self, *, immediate=False):
            async with executor.transaction(immediate=immediate) as session:
                yield session
                staged.set()
                await asyncio.Future()

        async def close(self):
            self.closed = True
            await executor.close()

    injected = PausedSetup()
    task = asyncio.create_task(IndexedStore.from_executor(inner, injected, owns_executor=owned))
    await asyncio.wait_for(staged.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert injected.closed is owned
    if not owned:
        async with executor.transaction() as session:
            assert await session.execute("SELECT name FROM sqlite_schema") == []
        await executor.close()
