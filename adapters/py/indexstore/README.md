# temporaless-indexstore

Optional SQLite reference query index for Temporaless Python stores.

`IndexedStore` wraps a bucket/file `Store` and mirrors record keys plus query
metadata into SQLite. The bucket remains the source of truth; query results are
loaded back from the wrapped store before being returned. The index can be
rebuilt from a populated v2 bucket.

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
- SQLite operations and lock acquisition run on worker threads so index I/O
  does not block the async runtime. Call `await store.close()` during graceful
  shutdown; close waits for any in-flight index operation without blocking the
  event loop.
- This package intentionally opens SQLite files only. It is a convenience
  implementation, not Temporaless's database contract. Other databases,
  search engines, warehouses, or remote index services implement the generated
  `RecordQueryService` (or the matching language-local `QueryStore` seam) in a
  separate adapter without changing core workflow code.

For the production ClickHouse query-index and Iceberg analytical-projection
contract, see [`docs/clickhouse-iceberg.md`](../../../docs/clickhouse-iceberg.md).
