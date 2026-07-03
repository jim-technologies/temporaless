# temporaless-indexstore

Optional SQLite query index for Temporaless Python stores.

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
  same `IndexedStore` while rebuild walks the bucket survive the merge. Corrupt
  bucket records are skipped and counted; records that disappear between LIST
  and GET are treated as ordinary delete races.
- Indexed `due_timers()` scans all SQLite rows with `TIMER_STATUS_SCHEDULED`
  and reloads each timer/workflow pair from the bucket so stale index rows can
  self-heal. Runtime-created scheduled timers always set `fire_at`; malformed
  scheduled timer records with unset `fire_at` are outside the supported record
  contract and may be ignored by the index.
- Postgres is future work. This package currently opens SQLite files only.
