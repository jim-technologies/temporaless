from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import AsyncIterator, Awaitable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from pathlib import Path
from typing import Any, Protocol

IndexRow = Mapping[str, Any]


class IndexSession(Protocol):
    """One atomic, connection-pinned SQLite unit; rows never retain a cursor.

    Execute complete statements, including complete trigger definitions. Do not
    issue transaction control, scripts with implicit commits, or share a session
    between tasks. SQL dialect, schema and query policy belong to the index.
    """

    async def execute(self, sql: str, parameters: Sequence[object] = ()) -> list[IndexRow]: ...


class IndexExecutor(Protocol):
    """Async SQLite transaction boundary, not an arbitrary SQL backend contract.

    A transaction serializes this connection until commit/rollback completes.
    `immediate` acquires SQLite's writer reservation before any statement: rebuild
    tracking must not be removed while another connection can commit a write.
    Exceptions and cancellation before commit roll back the whole unit. Once
    commit starts its outcome can be committed even if the caller is cancelled.
    Close drains active work and rejects new transactions. No buffering/flush or
    distributed execution fencing is implied by this local derived-index seam.
    """

    def transaction(
        self, *, immediate: bool = False
    ) -> AbstractAsyncContextManager[IndexSession]: ...

    async def close(self) -> None: ...


async def _finish[T](operation: Awaitable[T]) -> T:
    # Cancelling to_thread leaves the worker running. Keep the connection owned
    # until it finishes, even after repeated cancellation, before rollback/close.
    task = asyncio.ensure_future(operation)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        # Retrieve worker failures so cancellation cannot leave an unobserved task.
        with suppress(BaseException):
            task.result()
        raise asyncio.CancelledError
    return task.result()


class _SQLiteSession:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self._owner = asyncio.current_task()
        self._active = True

    async def execute(self, sql: str, parameters: Sequence[object] = ()) -> list[IndexRow]:
        if not self._active or asyncio.current_task() is not self._owner:
            raise RuntimeError("index session is inactive or belongs to another task")

        def run() -> list[IndexRow]:
            def authorize(action, _arg1, _arg2, _database, _source):
                if action in (sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            self._connection.set_authorizer(authorize)
            try:
                cursor = self._connection.execute(sql, parameters)
            finally:
                self._connection.set_authorizer(None)
            try:
                return [dict(row) for row in cursor.fetchall()]
            finally:
                cursor.close()

        return await _finish(asyncio.to_thread(run))


class SQLiteExecutor:
    """Owned stdlib SQLite connection with async sessions and unchanged pragmas.

    Construction is synchronous, like IndexedStore's original constructor.
    Optional schema statements run atomically before the connection is exposed;
    injected IndexedStore.from_executor instead performs its setup asynchronously.
    All session I/O and close run off the event loop. Use on one event loop.
    """

    def __init__(self, db_path: str | Path = ":memory:", *, schema: Sequence[str] = ()) -> None:
        self._connection = sqlite3.connect(
            str(db_path), check_same_thread=False, isolation_level=None
        )
        self._connection.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task | None = None
        self._closed = False
        try:
            self._connection.execute("BEGIN")
            for statement in schema:
                self._connection.execute(statement)
            self._connection.execute("COMMIT")
        except BaseException:
            self._connection.close()
            raise

    @asynccontextmanager
    async def transaction(self, *, immediate: bool = False) -> AsyncIterator[IndexSession]:
        if self._owner is asyncio.current_task():
            raise RuntimeError("nested index transactions are unsupported")
        async with self._lock:
            if self._closed:
                raise RuntimeError("SQLite executor is closed")
            self._owner = asyncio.current_task()
            session = _SQLiteSession(self._connection)
            try:
                await _finish(
                    asyncio.to_thread(
                        self._connection.execute, "BEGIN IMMEDIATE" if immediate else "BEGIN"
                    )
                )
                yield session
                await _finish(asyncio.to_thread(self._connection.execute, "COMMIT"))
            except BaseException:
                # BEGIN can itself be cancelled or fail while waiting on another
                # connection. Roll back only if a transaction actually started.
                await _finish(asyncio.to_thread(self._connection.rollback))
                raise
            finally:
                session._active = False
                self._owner = None

    async def close(self) -> None:
        if self._owner is asyncio.current_task():
            raise RuntimeError("cannot close an executor inside its transaction")
        async with self._lock:
            if not self._closed:
                self._closed = True
                await _finish(asyncio.to_thread(self._connection.close))
