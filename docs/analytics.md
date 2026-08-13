# Analytics And Search

Temporaless stores protobuf records in your bucket. The bucket is the
authoritative archive; it is not the interactive query engine.

The v2 runtime writes flat object keys under `temporaless/v2/` and never
parses paths back into record identity:

```text
temporaless/v2/{namespace}/{workflow_id}/{run_id}/workflow.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/activity/{activity_id}.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/timer/{timer_id}.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/event/{event_id}.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/claim/{claim_id}.binpb
```

That layout keeps durability cheap: point reads, point writes, run-scoped
prefetch, run-prefix deletion, and bucket lifecycle rules. It deliberately does
not optimize cross-run search.

## Online Search

Use a query index when you need interactive operations:

- list workflows with status filters
- order by `created_at` / `completed_at`
- paginate inspector views
- sweep completed runs older than a retention threshold
- query due timers with a database `SELECT`

The backend-neutral contract is `temporaless.v1.RecordQueryService`. A
deployment may implement it with any database, search engine, warehouse,
hosted service, or application-owned metadata index. Temporaless defines the
protobuf request/response semantics; it does not require SQL or prescribe a
physical schema.

Python also ships `temporaless-indexstore`, an optional write-through SQLite
reference adapter for small deployments and local operation. It stores only
keys, statuses, and timestamps; protobuf payloads stay in the bucket and remain
the source of truth. The index can be rebuilt by scanning the bucket:

```python
import opendal
from temporaless_indexstore import IndexedStore

operator = opendal.AsyncOperator("fs", root="/var/temporaless")
store = IndexedStore.from_opendal(operator, "/var/temporaless/index.sqlite")

await store.rebuild()
records, token = await store.list_workflows(
    "default",
    "prices:aapl",
    WORKFLOW_STATUS_COMPLETED,
    order_by="completed_at desc",
    page_size=100,
)
```

The same query surface is exposed locally or over ConnectRPC. Core workflow
replay never imports an index database and never needs this service.

A common production arrangement is ClickHouse for this low-latency metadata
query surface and Iceberg for batched analytics or archival tables. They are
independent, optional projections; neither replaces the authoritative point
store or due-timer ledger. See
[`clickhouse-iceberg.md`](clickhouse-iceberg.md) for ingestion ordering,
tombstones, hydration, pagination, reconciliation, and per-run archive-gate
requirements.

The SQLite index is derived infrastructure. A bucket write can commit and the
SQLite upsert can still fail, leaving a missing or stale row until `rebuild()`.
`rebuild()` is idempotent and stages rows before an atomic merge, so an
interrupted rebuild leaves the previous index intact. Rows written through the
same SQLite database while rebuild is walking the bucket are tracked through a
temporary mutation journal; successful index mutations win over scanned rows.
A failed best-effort index update or an external bucket writer is not in that
journal, so quiesce external writers or run another reconciliation. Only one
rebuild coordinator may use a SQLite database at a time. Corrupt bucket records
are skipped and logged with a skipped count; records that disappear between
LIST and GET are treated as ordinary delete races.

## Bucket-Only Analytics

For offline analytics, scan `temporaless/v2/`, read `.binpb` files, decode the
protobuf payloads, and materialize the fields you need into a warehouse table.
This is a batch job, not the runtime path.

Iceberg is a natural target for these batched tables. Keep the lossless
`record_binpb` and its deterministic digest alongside typed scalar columns,
projection batch/source metadata, and the full record identity. Iceberg table
snapshots describe projection commits; they are not workflow event history and
cannot recover point states overwritten between scans.

Common table shapes:

```sql
workflows(namespace, workflow_id, run_id, status, created_at, completed_at)
activities(namespace, workflow_id, run_id, activity_id, status, activity_type,
           created_at, completed_at, attempts)
timers(namespace, workflow_id, run_id, timer_id, timer_kind, status, fire_at)
events(namespace, workflow_id, run_id, event_id, received_at)
```

Once decoded, normal SQL answers the usual questions:

```sql
SELECT workflow_id,
       count(*) FILTER (WHERE status = 'WORKFLOW_STATUS_FAILED') * 1.0 / count(*) AS failure_rate,
       count(*) AS runs
FROM workflows
WHERE completed_at >= now() - INTERVAL 24 HOURS
GROUP BY workflow_id
ORDER BY failure_rate DESC;
```

```sql
SELECT activity_type,
       median(json_array_length(attempts)) AS p50_attempts,
       approx_quantile(json_array_length(attempts), 0.95) AS p95_attempts
FROM activities
WHERE status IN ('ACTIVITY_STATUS_COMPLETED', 'ACTIVITY_STATUS_FAILED')
GROUP BY activity_type
ORDER BY p95_attempts DESC;
```

## Retention

Bucket-only deployments can use conservative lifecycle rules for age-based
archive retention. Do not expire active run timer records or `_due` prepared records
before the maximum timer horizon plus scheduler-outage/recovery grace; exempt
them if timer durations are unbounded. If you need exact application-level
deletion by workflow status, use a query index: it selects matching terminal
runs by indexed metadata, deletes the run prefixes from the bucket, and removes
the index rows. `_due` tombstone cleanup is a separate offline/quiescent
maintenance operation; the generic ledger has no online compaction mode.

## Why This Boundary Exists

The runtime must run on only object/file storage. Recursive bucket walks for
every listing or scheduler decision turn the bucket into a database and force
the key layout to serve query parsing. Temporaless v2 keeps those concerns
separate:

- **Core:** point operations on your bucket.
- **Search:** optional derived index.
- **Analytics:** offline scans over the protobuf archive.
