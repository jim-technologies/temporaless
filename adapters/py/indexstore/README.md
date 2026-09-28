# temporaless-indexstore

This is an optional local SQLite reference adapter. The production convention
is [CloudEvents emission](../../../docs/cloudevents.md) with downstream logging
and query/lake projectors; this adapter's direct write-through path does not
consume that feed and is not required for workflow execution.

Optional SQLite reference query index for Temporaless Python stores.

`IndexedStore` wraps a bucket/file `Store` and mirrors record keys plus query
metadata into SQLite. The bucket remains the source of truth; query results are
loaded back from the wrapped store before being returned. The index can be
rebuilt from a populated v2 bucket.

## Where the index lives

- **In memory, rebuilt when needed: the default, and the supported posture on
  pods and scale-to-zero jobs.** Without a `db_path`, `IndexedStore` keeps
  the index in process memory (`":memory:"`). Workflow invocations and ticks
  write records through the plain bucket store and never touch an index. A
  job that needs cross-run queries, such as the janitor's retention sweep or
  an operator listing in-flight runs, builds one, rebuilds it from the
  bucket, queries, and closes it, so nothing is written to local disk:

  ```python
  from datetime import UTC, datetime, timedelta
  from temporaless.janitor import sweep

  index = IndexedStore.from_opendal(operator)  # db_path=":memory:"
  try:
      await index.rebuild()
      deleted = await sweep(index, datetime.now(UTC), timedelta(days=30))
  finally:
      await index.close()
  ```

  An in-memory index sees only the writes made through that instance after
  its rebuild. The rebuild reads every record under `temporaless/v2/` (one
  listing per directory and one read per record), so size the job's timeout
  to the retention window it walks.
- **A file, maintained write-through.** Pass `db_path` for a long-lived
  process on a host that owns its disk, such as a workstation or a single VM.
  Every indexed record write adds one journaled SQLite commit after the
  bucket write. The file is a rebuildable cache, never the source of truth;
  do not give a pod an index file on node disk or an `emptyDir`.

Operational notes:

- Write-through is best-effort after the bucket write. If SQLite upsert/delete
  fails, the authoritative record may exist without a matching row until
  `rebuild()` repairs the index.
- `rebuild()` is idempotent and stages rows before an atomic merge. If rebuild
  is interrupted, the previous index stays visible. Rows written through the
  same SQLite database while rebuild walks the bucket are tracked in a
  temporary mutation journal; successful index updates and deletes win over
  scanned rows. A failed best-effort SQLite update and an external bucket
  writer bypass that journal, so quiesce external writers or reconcile again.
  Exactly one rebuild coordinator may use a SQLite database at a time; a
  second is rejected. After a process crash leaves rebuild staging state,
  verify no rebuild is active and recreate the derived SQLite database.
  Corrupt bucket records are skipped and counted; records that disappear
  between LIST and GET are treated as ordinary delete races.
- Page tokens are opaque and bound to their filters and ordering. The SQLite
  reference uses offsets, so concurrent inserts/deletes between pages provide
  weak cross-page consistency; production indexes should use an epoch-bound
  keyset cursor when stable multi-page snapshots are required.
- Indexed `due_timers()` scans all SQLite rows with `TIMER_STATUS_SCHEDULED`
  and reloads each timer/workflow pair from the bucket so stale index rows can
  self-heal. Runtime-created scheduled timers always set `fire_at`; malformed
  scheduled timer records with unset `fire_at` are outside the supported record
  contract and may be ignored by the index.
- SQLite operations run on worker threads, while connection ownership uses an
  async lock, so index I/O does not block the async runtime. Call `await store.close()` during graceful
  shutdown; close waits for any in-flight index operation without blocking the
  event loop.
- This package intentionally opens SQLite only, in memory or as a file. It is
  a convenience implementation, not Temporaless's database contract. Other
  databases, search engines, warehouses, or remote index services implement
  the generated `RecordQueryService` (or the matching language-local
  `QueryStore` seam) in a separate adapter without changing core workflow
  code.

## Injecting the transaction executor

The default synchronous constructor and `from_opendal()` still prepare an owned
stdlib SQLite connection, with `":memory:"` as the default. To supply a connection
boundary explicitly, use the async factory:

```python
from temporaless.storage import OpenDALStore
from temporaless_indexstore import IndexedStore, SQLiteExecutor

executor = SQLiteExecutor("index.sqlite")
index = await IndexedStore.from_executor(
    OpenDALStore(operator), executor, operator=operator, owns_executor=False
)
try:
    await index.rebuild()
finally:
    await index.close()  # drains accepted SQL transactions; keeps borrowed executor open
    await executor.close()  # caller owns its lifetime
```

`IndexExecutor.transaction(immediate=False)` yields one `IndexSession`; its
async `execute(sql, parameters)` returns materialized row mappings. Success
commits before returning. Errors or cancellation before commit roll back the
whole unit, including schema changes. A blocked SQLite worker must finish before
rollback or close can release its connection. Once commit has started, a cancelled
caller can observe a committed outcome; the adapter never automatically retries
an uncertain operation. Session use from another task, nested transactions and
explicit transaction-control SQL are rejected by `SQLiteExecutor`. Use each
executor on one event loop.

The factory prepares the schema asynchronously. Executors are borrowed unless
`owns_executor=True`; ownership then transfers on factory entry, including setup
failure. Index close is idempotent and never closes the authoritative bucket. Stop callers
and rebuilds before closing: close drains accepted SQL transactions, not bucket
I/O or the remaining steps of a query/rebuild coroutine.
Constructor setup remains synchronous, as before; all subsequent SQLite I/O and
close run off the event loop. Failed rebuild cleanup now rolls its DDL back as a
unit and retains its owner/staging state for the existing explicit recovery path.

This seam requires SQLite SQL and connection-pinned transactions. Rebuild setup,
merge and cleanup use `BEGIN IMMEDIATE`; trigger definitions remain complete SQL
statements and concurrent-writer dirty tracking is preserved. No executor for
PostgreSQL, remote SQL batches or snapshot publication is provided or qualified.
A process-local SQLite lock is not a distributed execution fence: the bundled
record service still reports fenced execution unsupported and refuses its RPCs.

There is no buffering or `flush()` API, and no journal/synchronous setting change.
The protobufs, record keys, derived SQLite schema, ordering, query-bound `v1`
offset tokens, read repair and per-operation commit boundary stay unchanged.
The complete index behavior suite runs through both constructor and injected
paths; executor tests exercise real SQLite lock contention, rollback and close.
Rolling back the package requires no record or derived-schema migration.

For the production ClickHouse query-index and Iceberg analytical-projection
contract, see [`docs/clickhouse-iceberg.md`](../../../docs/clickhouse-iceberg.md).
