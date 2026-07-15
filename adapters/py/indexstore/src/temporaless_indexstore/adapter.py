from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
from collections.abc import AsyncIterable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import opendal
from google.protobuf.message import DecodeError
from protovalidate import ValidationError
from temporaless.storage import (
    NO_CLAIMS,
    ActivityKey,
    ClaimKey,
    ClaimRunListingUnsupportedError,
    ClaimRunStore,
    ClaimStore,
    DueTimer,
    EventKey,
    OpenDALStore,
    Store,
    TimerKey,
    WorkflowKey,
    _activity_keys_for_run,
    _claim_keys_for_run,
    _event_keys_for_run,
    _timer_keys_for_run,
    _validate_activity_record,
    _validate_timer_record,
    _validate_workflow_record,
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
    response. When claims use a separate backend, pass it as ``claim_store``
    so both point ``DeleteRun`` and retention ``Sweep`` clean coordination
    records before deleting the run.
    """

    def __init__(
        self,
        inner: Store,
        db_path: str | Path,
        *,
        operator: opendal.AsyncOperator | None = None,
        claim_store: ClaimStore | None = None,
    ) -> None:
        self._inner = inner
        self._operator = operator
        if claim_store is not None:
            self._claim_store = claim_store
        elif isinstance(inner, ClaimStore):
            self._claim_store = inner
        else:
            self._claim_store = None
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._init_schema()

    @classmethod
    def from_opendal(
        cls,
        operator: opendal.AsyncOperator,
        db_path: str | Path,
        *,
        claim_store: ClaimStore | None = None,
    ) -> IndexedStore:
        return cls(
            OpenDALStore(operator),
            db_path,
            operator=operator,
            claim_store=claim_store,
        )

    async def claim_capability(self) -> temporaless_pb2.ClaimCapability:
        if self._claim_store is None:
            return NO_CLAIMS
        return await self._claim_store.claim_capability()

    async def get_workflow(self, key: WorkflowKey) -> temporaless_pb2.WorkflowRecord | None:
        return await self._inner.get_workflow(key)

    async def put_workflow(self, record: temporaless_pb2.WorkflowRecord) -> None:
        await self._inner.put_workflow(record)
        try:
            await self._run_db(lambda conn: _upsert_workflow(conn, record))
        except Exception:
            _LOGGER.exception("workflow index update failed; authoritative record remains durable")

    async def get_latest_workflow_run(
        self, namespace: str, workflow_id: str
    ) -> temporaless_pb2.LatestWorkflowRunPointer | None:
        return await self._inner.get_latest_workflow_run(namespace, workflow_id)

    async def delete_workflow(self, key: WorkflowKey) -> bool:
        deleted = await self._inner.delete_workflow(key)
        if deleted:
            try:
                await self._run_db(lambda conn: _delete_workflow_row(conn, key))
            except Exception:
                _LOGGER.exception("workflow index delete failed; rebuild will remove the stale row")
        return deleted

    async def delete_run(self, key: WorkflowKey) -> int:
        claim_store, claim_keys = await self._claim_deletion_plan(key)
        await self._validate_inner_deletion_plan(key)

        deleted = 0
        if claim_store is not None:
            for claim_key in claim_keys:
                if await claim_store.delete_claim(claim_key):
                    deleted += 1
        deleted += await self._inner.delete_run(key)
        try:
            await self._run_db(lambda conn: _delete_run_rows(conn, key))
        except Exception:
            _LOGGER.exception("run index delete failed; rebuild will remove stale rows")
        return deleted

    async def get_activity(self, key: ActivityKey) -> temporaless_pb2.ActivityRecord | None:
        return await self._inner.get_activity(key)

    async def put_activity(self, record: temporaless_pb2.ActivityRecord) -> None:
        await self._inner.put_activity(record)
        try:
            await self._run_db(lambda conn: _upsert_activity(conn, record))
        except Exception:
            _LOGGER.exception("activity index update failed; authoritative record remains durable")

    async def list_activities(self, key: WorkflowKey) -> list[temporaless_pb2.ActivityRecord]:
        return await self._inner.list_activities(key)

    async def delete_activity(self, key: ActivityKey) -> bool:
        deleted = await self._inner.delete_activity(key)
        if deleted:
            try:
                await self._run_db(lambda conn: _delete_activity_row(conn, key))
            except Exception:
                _LOGGER.exception("activity index delete failed; rebuild will remove the stale row")
        return deleted

    async def get_timer(self, key: TimerKey) -> temporaless_pb2.TimerRecord | None:
        return await self._inner.get_timer(key)

    async def put_timer(self, record: temporaless_pb2.TimerRecord) -> None:
        await self._inner.put_timer(record)
        try:
            await self._run_db(lambda conn: _upsert_timer(conn, record))
        except Exception:
            # The SQLite database is a derived query accelerator. Once the
            # authoritative timer (and its bucket due ledger) is durable, an
            # index outage must not turn a successful scheduling boundary into
            # a lost wakeup. due_timers repairs the missing row from the inner
            # store on every scan.
            _LOGGER.exception("timer index update failed; durable timer remains discoverable")

    async def list_timers(
        self, key: WorkflowKey, status: temporaless_pb2.TimerStatus
    ) -> list[temporaless_pb2.TimerRecord]:
        return await self._inner.list_timers(key, status)

    async def delete_timer(self, key: TimerKey) -> bool:
        deleted = await self._inner.delete_timer(key)
        if deleted:
            try:
                await self._run_db(lambda conn: _delete_timer_row(conn, key))
            except Exception:
                _LOGGER.exception("timer index delete failed; rebuild will remove the stale row")
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

    async def list_claims(self, key: WorkflowKey) -> list[temporaless_pb2.ClaimRecord]:
        return await self._require_claim_run_store().list_claims(key)

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
        if page_size < 0:
            raise ValueError("page_size must be >= 0")

        # An index update is deliberately best-effort, so every returned row
        # must be checked against the authoritative protobuf. For paginated
        # queries, consume one raw row at a time and fetch one extra valid row.
        # This keeps pages full even when stale rows are repaired or pruned and
        # makes the returned offset point at the first unreturned valid row.
        select_size = 0 if page_size == 0 else 1
        cursor = page_token
        records: list[temporaless_pb2.WorkflowRecord] = []
        while True:
            rows, next_cursor = await self._run_db(
                lambda conn, cursor=cursor: _select_workflows(
                    conn,
                    namespace,
                    workflow_id,
                    status,
                    order_by,
                    select_size,
                    cursor,
                )
            )
            if not rows:
                return records, ""

            rows_to_check = rows if page_size == 0 else rows[:1]

            removed_current_row = False
            next_record_cursor = cursor
            for row in rows_to_check:
                row_cursor = next_record_cursor
                if page_size == 0:
                    # Unlimited queries have no follow-up token, so this value
                    # is only used for a uniform append path below.
                    row_cursor = ""
                elif next_cursor:
                    next_record_cursor = next_cursor

                key = WorkflowKey(
                    namespace=row["namespace"],
                    workflow_id=row["workflow_id"],
                    run_id=row["run_id"],
                )
                record = await self._inner.get_workflow(key)
                if record is None:
                    await self._run_db(lambda conn, key=key: _delete_workflow_row(conn, key))
                    removed_current_row = True
                    continue

                # Refresh every selected projection before applying the
                # caller's filter. A stale FAILED/COMPLETED row must never be
                # returned merely because SQLite missed the latest write.
                await self._run_db(lambda conn, record=record: _upsert_workflow(conn, record))
                if not _workflow_matches_query(record, namespace, workflow_id, status):
                    removed_current_row = True
                    continue

                if page_size > 0 and len(records) == page_size:
                    return records, row_cursor
                records.append(record)

            if page_size == 0:
                return records, ""
            if removed_current_row:
                # Repair/delete removed the selected row from this result set;
                # retry the same offset so the row that shifted into its place
                # is not skipped.
                continue
            if not next_cursor:
                return records, ""
            cursor = next_cursor

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
        if page_size < 0:
            raise ValueError("page_size must be >= 0")

        select_size = 0 if page_size == 0 else 1
        cursor = page_token
        records: list[temporaless_pb2.ActivityRecord] = []
        while True:
            rows, next_cursor = await self._run_db(
                lambda conn, cursor=cursor: _select_activities(
                    conn,
                    namespace,
                    workflow_id,
                    run_id,
                    status,
                    order_by,
                    select_size,
                    cursor,
                )
            )
            if not rows:
                return records, ""

            rows_to_check = rows if page_size == 0 else rows[:1]
            removed_current_row = False
            next_record_cursor = cursor
            for row in rows_to_check:
                row_cursor = next_record_cursor if page_size > 0 else ""
                if page_size > 0 and next_cursor:
                    next_record_cursor = next_cursor

                key = ActivityKey(
                    namespace=row["namespace"],
                    workflow_id=row["workflow_id"],
                    run_id=row["run_id"],
                    activity_id=row["activity_id"],
                )
                record = await self._inner.get_activity(key)
                if record is None:
                    await self._run_db(lambda conn, key=key: _delete_activity_row(conn, key))
                    removed_current_row = True
                    continue

                await self._run_db(lambda conn, record=record: _upsert_activity(conn, record))
                if not _activity_matches_query(record, namespace, workflow_id, run_id, status):
                    removed_current_row = True
                    continue

                if page_size > 0 and len(records) == page_size:
                    return records, row_cursor
                records.append(record)

            if page_size == 0:
                return records, ""
            if removed_current_row:
                continue
            if not next_cursor:
                return records, ""
            cursor = next_cursor

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
            record = await self._inner.get_workflow(key)
            if record is None:
                await self._run_db(lambda conn, key=key: _delete_workflow_row(conn, key))
                continue

            # SQLite is derived state. Re-evaluate retention against the
            # authoritative record so a missed reopen/fresh-completion update
            # can never delete a live or too-young run.
            await self._run_db(lambda conn, record=record: _upsert_workflow(conn, record))
            if record.status != temporaless_pb2.WORKFLOW_STATUS_COMPLETED:
                continue
            if not record.HasField("completed_at"):
                continue
            try:
                completed_at = record.completed_at.ToDatetime().replace(tzinfo=UTC)
            except (ValueError, OverflowError) as exc:
                raise ValueError("workflow completed_at is invalid") from exc
            if completed_at > now - max_age:
                continue
            await self.delete_run(key)
            deleted += 1
        return deleted

    async def due_timers(self, namespace: str, now: datetime) -> list[DueTimer]:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")

        # The bucket due ledger is authoritative for discovery. Unioning it
        # with indexed candidates makes a failed/missed SQLite upsert
        # self-healing without a full bucket walk: OpenDAL and Connect stores
        # both serve this as one compact DueTimers operation.
        authoritative_due = await self._inner.due_timers(namespace, now)
        due_by_key = {item.key: item for item in authoritative_due}
        for item in authoritative_due:
            try:
                await self._run_db(lambda conn, record=item.record: _upsert_timer(conn, record))
            except Exception:
                _LOGGER.exception(
                    "failed to repair timer index row for %s/%s/%s/%s",
                    item.key.namespace,
                    item.key.workflow_id,
                    item.key.run_id,
                    item.key.timer_id,
                )

        try:
            rows = await self._run_db(lambda conn: _select_due_timers(conn, namespace, _dt(now)))
        except Exception:
            _LOGGER.exception("timer index query failed; using authoritative due ledger")
            return sorted(authoritative_due, key=_due_timer_sort_key)

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
            if timer_key in due_by_key:
                continue
            try:
                timer_key.validate()
                workflow_key.validate()
                timer = await self._inner.get_timer(timer_key)
                workflow = await self._inner.get_workflow(workflow_key)
                if timer is not None:
                    authoritative_timer_key = timer_key_from_proto(timer.key)
                    authoritative_timer_key.validate()
                    if authoritative_timer_key != timer_key:
                        raise ValueError(
                            "authoritative timer payload key does not match its index row"
                        )
                if workflow is not None:
                    authoritative_workflow_key = workflow_key_from_proto(workflow.key)
                    authoritative_workflow_key.validate()
                    if authoritative_workflow_key != workflow_key:
                        raise ValueError(
                            "authoritative workflow payload key does not match the timer index row"
                        )
            except (DecodeError, ValidationError, ValueError, OverflowError) as exc:
                _LOGGER.warning(
                    "pruning invalid timer index row %s/%s/%s/%s: %s",
                    timer_key.namespace,
                    timer_key.workflow_id,
                    timer_key.run_id,
                    timer_key.timer_id,
                    exc,
                )
                try:
                    await self._run_db(lambda conn, key=timer_key: _delete_timer_row(conn, key))
                except Exception:
                    _LOGGER.exception("failed to prune invalid timer index row")
                continue
            if timer is None or timer.status != temporaless_pb2.TIMER_STATUS_SCHEDULED:
                try:
                    await self._run_db(lambda conn, key=timer_key: _delete_timer_row(conn, key))
                except Exception:
                    _LOGGER.exception("failed to prune stale timer index row")
                continue
            try:
                timer_fire_at = timer.fire_at.ToDatetime().replace(tzinfo=UTC)
            except (ValueError, OverflowError) as exc:
                _LOGGER.warning("pruning timer index row with invalid fire_at: %s", exc)
                try:
                    await self._run_db(lambda conn, key=timer_key: _delete_timer_row(conn, key))
                except Exception:
                    _LOGGER.exception("failed to prune invalid timer index row")
                continue
            timer_fire_at_index = _dt(timer_fire_at)
            if timer_fire_at > now:
                if timer_fire_at_index != row["fire_at"]:
                    try:
                        await self._run_db(lambda conn, record=timer: _upsert_timer(conn, record))
                    except Exception:
                        _LOGGER.exception("failed to repair future timer index row")
                continue
            if timer_fire_at_index != row["fire_at"]:
                try:
                    await self._run_db(lambda conn, record=timer: _upsert_timer(conn, record))
                except Exception:
                    _LOGGER.exception("failed to repair due timer index row")
            if workflow is None or workflow.status != temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS:
                try:
                    await self._run_db(lambda conn, key=timer_key: _delete_timer_row(conn, key))
                except Exception:
                    _LOGGER.exception("failed to prune terminal timer index row")
                continue
            due_by_key[timer_key] = DueTimer(key=timer_key, record=timer, workflow=workflow)
        return sorted(due_by_key.values(), key=_due_timer_sort_key)

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
                        _validate_workflow_record,
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
                        _validate_activity_record,
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
                        self._operator, path, temporaless_pb2.TimerRecord, _validate_timer_record
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

    async def close(self) -> None:
        """Close the SQLite connection without blocking the event loop."""

        await asyncio.to_thread(self._close_db)

    def _close_db(self) -> None:
        with self._lock:
            self._conn.close()

    def _require_claim_store(self) -> ClaimStore:
        if self._claim_store is None:
            raise TypeError("configured store does not support claims")
        return self._claim_store

    def _require_claim_run_store(self) -> ClaimRunStore:
        if not isinstance(self._claim_store, ClaimRunStore):
            raise ClaimRunListingUnsupportedError(
                "claim store does not support run-scoped claim listing"
            )
        return self._claim_store

    async def _claim_deletion_plan(
        self, key: WorkflowKey
    ) -> tuple[ClaimStore | None, list[ClaimKey]]:
        claim_store = self._claim_store
        if claim_store is None:
            return None, []
        capability = await claim_store.claim_capability()
        if capability not in (
            temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS,
            temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS,
        ):
            return None, []
        claim_run_store = self._require_claim_run_store()
        try:
            records = await claim_run_store.list_claims(key)
        except TypeError as exc:
            raise ClaimRunListingUnsupportedError(
                "claim store does not support run-scoped claim listing"
            ) from exc
        return claim_store, _claim_keys_for_run(key, records)

    async def _validate_inner_deletion_plan(self, key: WorkflowKey) -> None:
        activities = await self._inner.list_activities(key)
        timers = await self._inner.list_timers(key, temporaless_pb2.TIMER_STATUS_UNSPECIFIED)
        events = await self._inner.list_events(key)
        _activity_keys_for_run(key, activities)
        _timer_keys_for_run(key, timers)
        _event_keys_for_run(key, events)

        # An explicitly separate claim store is authoritative, but the inner
        # record store may still have its own claim objects that its
        # delete_run implementation will inspect. Validate those too before
        # deleting the authoritative claims.
        if self._claim_store is self._inner or not isinstance(self._inner, ClaimRunStore):
            return
        capability = await self._inner.claim_capability()
        if capability not in (
            temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS,
            temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS,
        ):
            return
        try:
            records = await self._inner.list_claims(key)
        except TypeError as exc:
            raise ClaimRunListingUnsupportedError(
                "inner claim store does not support run-scoped claim listing"
            ) from exc
        _claim_keys_for_run(key, records)

    async def _run_db[T](self, fn: Callable[[sqlite3.Connection], T]) -> T:
        def run() -> T:
            with self._lock:
                try:
                    result = fn(self._conn)
                except Exception:
                    self._conn.rollback()
                    raise
                else:
                    self._conn.commit()
                    return result

        return await asyncio.to_thread(run)

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


def _due_timer_sort_key(item: DueTimer) -> tuple[datetime, str, str, str, str]:
    return (
        item.record.fire_at.ToDatetime().replace(tzinfo=UTC),
        item.key.namespace,
        item.key.workflow_id,
        item.key.run_id,
        item.key.timer_id,
    )


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


def _workflow_matches_query(
    record: temporaless_pb2.WorkflowRecord,
    namespace: str,
    workflow_id: str,
    status: temporaless_pb2.WorkflowStatus,
) -> bool:
    key = workflow_key_from_proto(record.key)
    return (
        (not namespace or key.namespace == namespace)
        and (not workflow_id or key.workflow_id == workflow_id)
        and (status == temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED or record.status == status)
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


def _activity_matches_query(
    record: temporaless_pb2.ActivityRecord,
    namespace: str,
    workflow_id: str,
    run_id: str,
    status: temporaless_pb2.ActivityStatus,
) -> bool:
    key = activity_key_from_proto(record.key)
    return (
        (not namespace or key.namespace == namespace)
        and (not workflow_id or key.workflow_id == workflow_id)
        and (not run_id or key.run_id == run_id)
        and (status == temporaless_pb2.ACTIVITY_STATUS_UNSPECIFIED or record.status == status)
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
    params: list[object] = [int(temporaless_pb2.TIMER_STATUS_SCHEDULED), now]
    where = "status=? AND fire_at != '' AND fire_at <= ?"
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


async def _read_rebuild_record(operator: opendal.AsyncOperator, path: str, factory, validator):
    try:
        record = await _read_pb(operator, path, factory)
        validator(record, storage_path=path)
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
