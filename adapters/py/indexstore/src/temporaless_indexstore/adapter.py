from __future__ import annotations

import logging
import sqlite3
import threading
from collections.abc import AsyncIterable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import opendal
from google.protobuf.message import DecodeError
from protovalidate import ValidationError
from temporaless.storage import (
    NO_CLAIMS,
    ActivityKey,
    ClaimKey,
    ClaimStore,
    DueTimer,
    EventKey,
    OpenDALStore,
    Store,
    TimerKey,
    WorkflowKey,
    activity_key_from_proto,
    timer_key_from_proto,
    workflow_key_from_proto,
)
from temporaless.v1 import temporaless_pb2

_LOGGER = logging.getLogger(__name__)
_WORKFLOWS_TABLE = "workflows"
_ACTIVITIES_TABLE = "activities"
_TIMERS_TABLE = "timers"
_REBUILD_WORKFLOWS_TABLE = "_rebuild_workflows"
_REBUILD_ACTIVITIES_TABLE = "_rebuild_activities"
_REBUILD_TIMERS_TABLE = "_rebuild_timers"
_REBUILD_EXISTING_WORKFLOWS_TABLE = "_rebuild_existing_workflows"
_REBUILD_EXISTING_ACTIVITIES_TABLE = "_rebuild_existing_activities"
_REBUILD_EXISTING_TIMERS_TABLE = "_rebuild_existing_timers"


class IndexedStore:
    """Write-through SQLite query index for a Temporaless Store.

    SQLite stores record keys, statuses, and timestamps only. Protobuf record
    payloads stay in the wrapped bucket store and are reloaded for every query
    response.
    """

    def __init__(
        self,
        inner: Store,
        db_path: str | Path,
        *,
        operator: opendal.AsyncOperator | None = None,
    ) -> None:
        self._inner = inner
        self._operator = operator
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._init_schema()

    @classmethod
    def from_opendal(cls, operator: opendal.AsyncOperator, db_path: str | Path) -> IndexedStore:
        return cls(OpenDALStore(operator), db_path, operator=operator)

    async def claim_capability(self) -> temporaless_pb2.ClaimCapability:
        if not isinstance(self._inner, ClaimStore):
            return NO_CLAIMS
        return await self._inner.claim_capability()

    async def get_workflow(self, key: WorkflowKey) -> temporaless_pb2.WorkflowRecord | None:
        return await self._inner.get_workflow(key)

    async def put_workflow(self, record: temporaless_pb2.WorkflowRecord) -> None:
        await self._inner.put_workflow(record)
        await self._run_db(lambda conn: _upsert_workflow(conn, record))

    async def get_latest_workflow_run(
        self, namespace: str, workflow_id: str
    ) -> temporaless_pb2.LatestWorkflowRunPointer | None:
        return await self._inner.get_latest_workflow_run(namespace, workflow_id)

    async def delete_workflow(self, key: WorkflowKey) -> bool:
        deleted = await self._inner.delete_workflow(key)
        if deleted:
            await self._run_db(lambda conn: _delete_workflow_row(conn, key))
        return deleted

    async def delete_run(self, key: WorkflowKey) -> int:
        deleted = await self._inner.delete_run(key)
        await self._run_db(lambda conn: _delete_run_rows(conn, key))
        return deleted

    async def get_activity(self, key: ActivityKey) -> temporaless_pb2.ActivityRecord | None:
        return await self._inner.get_activity(key)

    async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None:
        await self._inner.put_activity(record)
        await self._run_db(lambda conn: _upsert_activity(conn, record))

    async def list_activities(self, key: WorkflowKey) -> list[temporaless_pb2.ActivityRecord]:
        return await self._inner.list_activities(key)

    async def delete_activity(self, key: ActivityKey) -> bool:
        deleted = await self._inner.delete_activity(key)
        if deleted:
            await self._run_db(lambda conn: _delete_activity_row(conn, key))
        return deleted

    async def get_timer(self, key: TimerKey) -> temporaless_pb2.TimerRecord | None:
        return await self._inner.get_timer(key)

    async def put_timer(self, record: temporaless_pb2.TimerRecord) -> None:
        await self._inner.put_timer(record)
        await self._run_db(lambda conn: _upsert_timer(conn, record))

    async def list_timers(
        self, key: WorkflowKey, status: temporaless_pb2.TimerStatus
    ) -> list[temporaless_pb2.TimerRecord]:
        return await self._inner.list_timers(key, status)

    async def delete_timer(self, key: TimerKey) -> bool:
        deleted = await self._inner.delete_timer(key)
        if deleted:
            await self._run_db(lambda conn: _delete_timer_row(conn, key))
        return deleted

    async def get_event(self, key: EventKey) -> temporaless_pb2.EventRecord | None:
        return await self._inner.get_event(key)

    async def put_event(self, record: temporaless_pb2.EventRecord) -> None:
        await self._inner.put_event(record)

    async def list_events(self, key: WorkflowKey) -> list[temporaless_pb2.EventRecord]:
        return await self._inner.list_events(key)

    async def delete_event(self, key: EventKey) -> bool:
        return await self._inner.delete_event(key)

    async def get_claim(self, key: ClaimKey) -> temporaless_pb2.ClaimRecord | None:
        return await self._require_claim_store().get_claim(key)

    async def try_create_claim(self, record: temporaless_pb2.ClaimRecord) -> bool:
        return await self._require_claim_store().try_create_claim(record)

    async def delete_claim(self, key: ClaimKey) -> bool:
        return await self._require_claim_store().delete_claim(key)

    async def list_workflows(
        self,
        namespace: str,
        workflow_id: str,
        status: temporaless_pb2.WorkflowStatus,
        *,
        order_by: str = "",
        page_size: int = 0,
        page_token: str = "",
    ) -> tuple[list[temporaless_pb2.WorkflowRecord], str]:
        rows, next_page_token = await self._run_db(
            lambda conn: _select_workflows(
                conn, namespace, workflow_id, status, order_by, page_size, page_token
            )
        )
        records: list[temporaless_pb2.WorkflowRecord] = []
        for row in rows:
            key = WorkflowKey(
                namespace=row["namespace"],
                workflow_id=row["workflow_id"],
                run_id=row["run_id"],
            )
            record = await self._inner.get_workflow(key)
            if record is None:
                await self._run_db(lambda conn, key=key: _delete_workflow_row(conn, key))
                continue
            records.append(record)
        return records, next_page_token

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
    ) -> tuple[list[temporaless_pb2.ActivityRecord], str]:
        rows, next_page_token = await self._run_db(
            lambda conn: _select_activities(
                conn, namespace, workflow_id, run_id, status, order_by, page_size, page_token
            )
        )
        records: list[temporaless_pb2.ActivityRecord] = []
        for row in rows:
            key = ActivityKey(
                namespace=row["namespace"],
                workflow_id=row["workflow_id"],
                run_id=row["run_id"],
                activity_id=row["activity_id"],
            )
            record = await self._inner.get_activity(key)
            if record is None:
                await self._run_db(lambda conn, key=key: _delete_activity_row(conn, key))
                continue
            records.append(record)
        return records, next_page_token

    async def sweep(self, namespace: str, now: datetime, max_age: timedelta) -> int:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if max_age <= timedelta(0):
            raise ValueError("max_age must be > 0")

        cutoff = _dt(now - max_age)
        rows = await self._run_db(lambda conn: _select_sweep(conn, namespace, cutoff))
        deleted = 0
        for row in rows:
            key = WorkflowKey(
                namespace=row["namespace"],
                workflow_id=row["workflow_id"],
                run_id=row["run_id"],
            )
            await self._inner.delete_run(key)
            await self._run_db(lambda conn, key=key: _delete_run_rows(conn, key))
            deleted += 1
        return deleted

    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        rows = await self._run_db(lambda conn: _select_due_timers(conn, namespace, _dt(now)))
        due: list[DueTimer] = []
        for row in rows:
            timer_key = TimerKey(
                namespace=row["namespace"],
                workflow_id=row["workflow_id"],
                run_id=row["run_id"],
                timer_id=row["timer_id"],
            )
            workflow_key = WorkflowKey(
                namespace=row["namespace"],
                workflow_id=row["workflow_id"],
                run_id=row["run_id"],
            )
            timer = await self._inner.get_timer(timer_key)
            workflow = await self._inner.get_workflow(workflow_key)
            if timer is None or timer.status != temporaless_pb2.TIMER_STATUS_SCHEDULED:
                await self._run_db(lambda conn, key=timer_key: _delete_timer_row(conn, key))
                continue
            timer_fire_at = timer.fire_at.ToDatetime().replace(tzinfo=UTC)
            timer_fire_at_index = _dt(timer_fire_at)
            if timer_fire_at > now:
                if timer_fire_at_index != row["fire_at"]:
                    await self._run_db(lambda conn, record=timer: _upsert_timer(conn, record))
                continue
            if timer_fire_at_index != row["fire_at"]:
                await self._run_db(lambda conn, record=timer: _upsert_timer(conn, record))
            if workflow is None or workflow.status != temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS:
                await self._run_db(lambda conn, key=timer_key: _delete_timer_row(conn, key))
                continue
            due.append(DueTimer(key=timer_key, record=timer, workflow=workflow))
        return due

    async def rebuild(self) -> int:
        """Rebuild the whole index from a populated v2 bucket.

        This is the one sanctioned full bucket walk; it lives in the optional
        query adapter, not in the core store protocol.
        """
        if self._operator is None:
            raise ValueError("rebuild requires an OpenDAL operator")
        skipped = 0
        await self._run_db(_reset_rebuild_index)
        try:
            async for path in _walk_binpb(self._operator, "temporaless/v2/"):
                kind = _v2_record_kind(path)
                if kind == "workflow":
                    record, count_skipped = await _read_rebuild_record(
                        self._operator,
                        path,
                        temporaless_pb2.WorkflowRecord,
                        workflow_key_from_proto,
                    )
                    if record is None:
                        if count_skipped:
                            skipped += 1
                        continue
                    await self._run_db(
                        lambda conn, record=record: _upsert_workflow(
                            conn, record, table=_REBUILD_WORKFLOWS_TABLE
                        )
                    )
                elif kind == "activity":
                    record, count_skipped = await _read_rebuild_record(
                        self._operator,
                        path,
                        temporaless_pb2.ActivityRecord,
                        activity_key_from_proto,
                    )
                    if record is None:
                        if count_skipped:
                            skipped += 1
                        continue
                    await self._run_db(
                        lambda conn, record=record: _upsert_activity(
                            conn, record, table=_REBUILD_ACTIVITIES_TABLE
                        )
                    )
                elif kind == "timer":
                    record, count_skipped = await _read_rebuild_record(
                        self._operator, path, temporaless_pb2.TimerRecord, timer_key_from_proto
                    )
                    if record is None:
                        if count_skipped:
                            skipped += 1
                        continue
                    await self._run_db(
                        lambda conn, record=record: _upsert_timer(
                            conn, record, table=_REBUILD_TIMERS_TABLE
                        )
                    )
            await self._run_db(_swap_rebuild_index)
        finally:
            await self._run_db(_drop_rebuild_index)
        return skipped

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _require_claim_store(self) -> ClaimStore:
        if not isinstance(self._inner, ClaimStore):
            raise TypeError("inner store does not support claims")
        return cast("ClaimStore", self._inner)

    async def _run_db(self, fn: Callable[[sqlite3.Connection], object]):
        with self._lock:
            try:
                result = fn(self._conn)
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()
                return result

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS workflows (
                    namespace TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    status INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    PRIMARY KEY (namespace, workflow_id, run_id)
                );
                CREATE INDEX IF NOT EXISTS workflows_status_completed
                    ON workflows(namespace, status, completed_at);
                CREATE INDEX IF NOT EXISTS workflows_by_id_created
                    ON workflows(namespace, workflow_id, created_at);

                CREATE TABLE IF NOT EXISTS activities (
                    namespace TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    activity_id TEXT NOT NULL,
                    status INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    PRIMARY KEY (namespace, workflow_id, run_id, activity_id)
                );
                CREATE INDEX IF NOT EXISTS activities_status
                    ON activities(namespace, status, created_at);

                CREATE TABLE IF NOT EXISTS timers (
                    namespace TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    timer_id TEXT NOT NULL,
                    status INTEGER NOT NULL,
                    fire_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    fired_at TEXT NOT NULL,
                    PRIMARY KEY (namespace, workflow_id, run_id, timer_id)
                );
                CREATE INDEX IF NOT EXISTS timers_due
                    ON timers(namespace, status, fire_at);
                """
            )
            self._conn.commit()


def _upsert_workflow(
    conn: sqlite3.Connection,
    record: temporaless_pb2.WorkflowRecord,
    *,
    table: str = _WORKFLOWS_TABLE,
) -> None:
    _validate_table(table, {_WORKFLOWS_TABLE, _REBUILD_WORKFLOWS_TABLE})
    key = workflow_key_from_proto(record.key)
    conn.execute(
        f"""
        INSERT INTO {table}(namespace, workflow_id, run_id, status, created_at, completed_at)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(namespace, workflow_id, run_id) DO UPDATE SET
            status=excluded.status,
            created_at=excluded.created_at,
            completed_at=excluded.completed_at
        """,
        (
            key.namespace,
            key.workflow_id,
            key.run_id,
            int(record.status),
            _ts(record, "created_at"),
            _ts(record, "completed_at"),
        ),
    )


def _upsert_activity(
    conn: sqlite3.Connection,
    record: temporaless_pb2.ActivityRecord,
    *,
    table: str = _ACTIVITIES_TABLE,
) -> None:
    _validate_table(table, {_ACTIVITIES_TABLE, _REBUILD_ACTIVITIES_TABLE})
    key = activity_key_from_proto(record.key)
    conn.execute(
        f"""
        INSERT INTO {table}(
            namespace, workflow_id, run_id, activity_id, status, created_at, completed_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(namespace, workflow_id, run_id, activity_id) DO UPDATE SET
            status=excluded.status,
            created_at=excluded.created_at,
            completed_at=excluded.completed_at
        """,
        (
            key.namespace,
            key.workflow_id,
            key.run_id,
            key.activity_id,
            int(record.status),
            _ts(record, "created_at"),
            _ts(record, "completed_at"),
        ),
    )


def _upsert_timer(
    conn: sqlite3.Connection,
    record: temporaless_pb2.TimerRecord,
    *,
    table: str = _TIMERS_TABLE,
) -> None:
    _validate_table(table, {_TIMERS_TABLE, _REBUILD_TIMERS_TABLE})
    key = timer_key_from_proto(record.key)
    conn.execute(
        f"""
        INSERT INTO {table}(
            namespace, workflow_id, run_id, timer_id, status, fire_at, created_at, fired_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(namespace, workflow_id, run_id, timer_id) DO UPDATE SET
            status=excluded.status,
            fire_at=excluded.fire_at,
            created_at=excluded.created_at,
            fired_at=excluded.fired_at
        """,
        (
            key.namespace,
            key.workflow_id,
            key.run_id,
            key.timer_id,
            int(record.status),
            _ts(record, "fire_at"),
            _ts(record, "created_at"),
            _ts(record, "fired_at"),
        ),
    )


def _validate_table(table: str, allowed: set[str]) -> None:
    if table not in allowed:
        raise ValueError(f"unsupported index table {table!r}")


def _delete_workflow_row(conn: sqlite3.Connection, key: WorkflowKey) -> None:
    conn.execute(
        "DELETE FROM workflows WHERE namespace=? AND workflow_id=? AND run_id=?",
        (key.namespace, key.workflow_id, key.run_id),
    )


def _delete_activity_row(conn: sqlite3.Connection, key: ActivityKey) -> None:
    conn.execute(
        """
        DELETE FROM activities
        WHERE namespace=? AND workflow_id=? AND run_id=? AND activity_id=?
        """,
        (key.namespace, key.workflow_id, key.run_id, key.activity_id),
    )


def _delete_timer_row(conn: sqlite3.Connection, key: TimerKey) -> None:
    conn.execute(
        """
        DELETE FROM timers
        WHERE namespace=? AND workflow_id=? AND run_id=? AND timer_id=?
        """,
        (key.namespace, key.workflow_id, key.run_id, key.timer_id),
    )


def _delete_run_rows(conn: sqlite3.Connection, key: WorkflowKey) -> None:
    params = (key.namespace, key.workflow_id, key.run_id)
    conn.execute("DELETE FROM activities WHERE namespace=? AND workflow_id=? AND run_id=?", params)
    conn.execute("DELETE FROM timers WHERE namespace=? AND workflow_id=? AND run_id=?", params)
    conn.execute("DELETE FROM workflows WHERE namespace=? AND workflow_id=? AND run_id=?", params)


def _select_workflows(
    conn: sqlite3.Connection,
    namespace: str,
    workflow_id: str,
    status: temporaless_pb2.WorkflowStatus,
    order_by: str,
    page_size: int,
    page_token: str,
) -> tuple[list[sqlite3.Row], str]:
    where: list[str] = []
    params: list[object] = []
    if namespace:
        where.append("namespace=?")
        params.append(namespace)
    if workflow_id:
        where.append("workflow_id=?")
        params.append(workflow_id)
    if status != temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED:
        where.append("status=?")
        params.append(int(status))
    return _select_rows(
        conn,
        "workflows",
        where,
        params,
        order_by,
        page_size,
        page_token,
        allowed_order={"created_at", "completed_at", "workflow_id", "run_id", "status"},
    )


def _select_activities(
    conn: sqlite3.Connection,
    namespace: str,
    workflow_id: str,
    run_id: str,
    status: temporaless_pb2.ActivityStatus,
    order_by: str,
    page_size: int,
    page_token: str,
) -> tuple[list[sqlite3.Row], str]:
    where: list[str] = []
    params: list[object] = []
    if namespace:
        where.append("namespace=?")
        params.append(namespace)
    if workflow_id:
        where.append("workflow_id=?")
        params.append(workflow_id)
    if run_id:
        where.append("run_id=?")
        params.append(run_id)
    if status != temporaless_pb2.ACTIVITY_STATUS_UNSPECIFIED:
        where.append("status=?")
        params.append(int(status))
    return _select_rows(
        conn,
        "activities",
        where,
        params,
        order_by,
        page_size,
        page_token,
        allowed_order={"created_at", "completed_at", "activity_id", "status"},
    )


def _select_rows(
    conn: sqlite3.Connection,
    table: str,
    where: list[str],
    params: list[object],
    order_by: str,
    page_size: int,
    page_token: str,
    *,
    allowed_order: set[str],
) -> tuple[list[sqlite3.Row], str]:
    offset = _decode_offset(page_token)
    limit_sql = ""
    limit_params: list[object] = []
    if page_size < 0:
        raise ValueError("page_size must be >= 0")
    if page_size > 0:
        limit_sql = " LIMIT ? OFFSET ?"
        limit_params.extend([page_size + 1, offset])

    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    order_sql = _order_by_sql(order_by, allowed_order)
    rows = list(
        conn.execute(
            f"SELECT * FROM {table}{where_sql} ORDER BY {order_sql}{limit_sql}",
            (*params, *limit_params),
        )
    )
    if page_size <= 0 or len(rows) <= page_size:
        return rows, ""
    return rows[:page_size], str(offset + page_size)


def _select_sweep(conn: sqlite3.Connection, namespace: str, cutoff: str) -> list[sqlite3.Row]:
    params: list[object] = [int(temporaless_pb2.WORKFLOW_STATUS_COMPLETED), cutoff]
    where = "status=? AND completed_at != '' AND completed_at <= ?"
    if namespace:
        where = f"namespace=? AND {where}"
        params.insert(0, namespace)
    return list(conn.execute(f"SELECT * FROM workflows WHERE {where}", params))


def _select_due_timers(conn: sqlite3.Connection, namespace: str, now: str) -> list[sqlite3.Row]:
    del now
    params: list[object] = [int(temporaless_pb2.TIMER_STATUS_SCHEDULED)]
    where = "status=? AND fire_at != ''"
    if namespace:
        where = f"namespace=? AND {where}"
        params.insert(0, namespace)
    return list(conn.execute(f"SELECT * FROM timers WHERE {where} ORDER BY fire_at ASC", params))


def _reset_rebuild_index(conn: sqlite3.Connection) -> None:
    _drop_rebuild_index(conn)
    conn.executescript(
        f"""
        CREATE TABLE {_REBUILD_WORKFLOWS_TABLE} AS SELECT * FROM workflows WHERE 0;
        CREATE TABLE {_REBUILD_ACTIVITIES_TABLE} AS SELECT * FROM activities WHERE 0;
        CREATE TABLE {_REBUILD_TIMERS_TABLE} AS SELECT * FROM timers WHERE 0;
        CREATE TABLE {_REBUILD_EXISTING_WORKFLOWS_TABLE} AS
            SELECT namespace, workflow_id, run_id FROM workflows;
        CREATE TABLE {_REBUILD_EXISTING_ACTIVITIES_TABLE} AS
            SELECT namespace, workflow_id, run_id, activity_id FROM activities;
        CREATE TABLE {_REBUILD_EXISTING_TIMERS_TABLE} AS
            SELECT namespace, workflow_id, run_id, timer_id FROM timers;
        CREATE UNIQUE INDEX {_REBUILD_WORKFLOWS_TABLE}_pk
            ON {_REBUILD_WORKFLOWS_TABLE}(namespace, workflow_id, run_id);
        CREATE UNIQUE INDEX {_REBUILD_ACTIVITIES_TABLE}_pk
            ON {_REBUILD_ACTIVITIES_TABLE}(namespace, workflow_id, run_id, activity_id);
        CREATE UNIQUE INDEX {_REBUILD_TIMERS_TABLE}_pk
            ON {_REBUILD_TIMERS_TABLE}(namespace, workflow_id, run_id, timer_id);
        """
    )


def _swap_rebuild_index(conn: sqlite3.Connection) -> None:
    _delete_rows_absent_from_rebuild(
        conn,
        live_table=_TIMERS_TABLE,
        existing_table=_REBUILD_EXISTING_TIMERS_TABLE,
        rebuild_table=_REBUILD_TIMERS_TABLE,
        key_columns=("namespace", "workflow_id", "run_id", "timer_id"),
    )
    _delete_rows_absent_from_rebuild(
        conn,
        live_table=_ACTIVITIES_TABLE,
        existing_table=_REBUILD_EXISTING_ACTIVITIES_TABLE,
        rebuild_table=_REBUILD_ACTIVITIES_TABLE,
        key_columns=("namespace", "workflow_id", "run_id", "activity_id"),
    )
    _delete_rows_absent_from_rebuild(
        conn,
        live_table=_WORKFLOWS_TABLE,
        existing_table=_REBUILD_EXISTING_WORKFLOWS_TABLE,
        rebuild_table=_REBUILD_WORKFLOWS_TABLE,
        key_columns=("namespace", "workflow_id", "run_id"),
    )
    conn.execute(f"INSERT OR REPLACE INTO workflows SELECT * FROM {_REBUILD_WORKFLOWS_TABLE}")
    conn.execute(f"INSERT OR REPLACE INTO activities SELECT * FROM {_REBUILD_ACTIVITIES_TABLE}")
    conn.execute(f"INSERT OR REPLACE INTO timers SELECT * FROM {_REBUILD_TIMERS_TABLE}")


def _delete_rows_absent_from_rebuild(
    conn: sqlite3.Connection,
    *,
    live_table: str,
    existing_table: str,
    rebuild_table: str,
    key_columns: tuple[str, ...],
) -> None:
    live_match_existing = " AND ".join(
        f"{existing_table}.{column}={live_table}.{column}" for column in key_columns
    )
    live_match_rebuild = " AND ".join(
        f"{rebuild_table}.{column}={live_table}.{column}" for column in key_columns
    )
    conn.execute(
        f"""
        DELETE FROM {live_table}
        WHERE EXISTS (
            SELECT 1 FROM {existing_table}
            WHERE {live_match_existing}
        )
        AND NOT EXISTS (
            SELECT 1 FROM {rebuild_table}
            WHERE {live_match_rebuild}
        )
        """
    )


def _drop_rebuild_index(conn: sqlite3.Connection) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {_REBUILD_EXISTING_TIMERS_TABLE}")
    conn.execute(f"DROP TABLE IF EXISTS {_REBUILD_EXISTING_ACTIVITIES_TABLE}")
    conn.execute(f"DROP TABLE IF EXISTS {_REBUILD_EXISTING_WORKFLOWS_TABLE}")
    conn.execute(f"DROP TABLE IF EXISTS {_REBUILD_TIMERS_TABLE}")
    conn.execute(f"DROP TABLE IF EXISTS {_REBUILD_ACTIVITIES_TABLE}")
    conn.execute(f"DROP TABLE IF EXISTS {_REBUILD_WORKFLOWS_TABLE}")


def _order_by_sql(order_by: str, allowed: set[str]) -> str:
    if not order_by:
        return "created_at ASC, namespace ASC, workflow_id ASC, run_id ASC"
    parts: list[str] = []
    for item in order_by.split(","):
        raw = item.strip()
        if not raw:
            continue
        tokens = raw.split()
        field = tokens[0]
        direction = tokens[1].upper() if len(tokens) > 1 else "ASC"
        if field not in allowed:
            raise ValueError(f"unsupported order_by field {field!r}")
        if direction not in {"ASC", "DESC"} or len(tokens) > 2:
            raise ValueError(f"unsupported order_by direction in {raw!r}")
        parts.append(f"{field} {direction}")
    if not parts:
        return "created_at ASC, namespace ASC, workflow_id ASC, run_id ASC"
    parts.extend(["namespace ASC", "workflow_id ASC", "run_id ASC"])
    return ", ".join(parts)


def _decode_offset(page_token: str) -> int:
    if not page_token:
        return 0
    try:
        offset = int(page_token)
    except ValueError as exc:
        raise ValueError("page_token is invalid") from exc
    if offset < 0:
        raise ValueError("page_token is invalid")
    return offset


def _ts(record, field: str) -> str:
    if not record.HasField(field):
        return ""
    return _dt(getattr(record, field).ToDatetime().replace(tzinfo=UTC))


def _dt(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _v2_record_kind(path: str) -> str | None:
    parts = path.split("/")
    if len(parts) == 6 and parts[:2] == ["temporaless", "v2"] and parts[5] == "workflow.binpb":
        return "workflow"
    if (
        len(parts) == 7
        and parts[:2] == ["temporaless", "v2"]
        and parts[5] in {"activity", "timer", "event", "claim"}
        and parts[6].endswith(".binpb")
    ):
        return parts[5]
    return None


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
            if entry.path == current:
                continue
            if entry.path.endswith("/"):
                queue.append(entry.path)
            elif entry.path.endswith(".binpb"):
                yield entry.path


async def _read_rebuild_record(operator: opendal.AsyncOperator, path: str, factory, key_factory):
    try:
        record = await _read_pb(operator, path, factory)
        key_factory(record.key).validate()
    except opendal.exceptions.NotFound:
        return None, False
    except (DecodeError, ValidationError, ValueError) as exc:
        _LOGGER.warning("skipping invalid index rebuild record %s: %s", path, exc)
        return None, True
    return record, False


async def _read_pb(operator: opendal.AsyncOperator, path: str, factory):
    record = factory()
    record.ParseFromString(bytes(await operator.read(path)))
    return record
