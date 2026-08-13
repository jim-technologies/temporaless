# ClickHouse And Iceberg

ClickHouse and Iceberg fit Temporaless without changing workflow code, but they
serve different roles:

- ClickHouse is a low-latency, rebuildable metadata index behind
  `temporaless.v1.RecordQueryService`.
- Iceberg is a batched analytical or archival projection.
- The point store remains authoritative for replay, claims, event delivery,
  timers, and deletion decisions.

Neither backend belongs in the core runtime, and neither requires a new core
RPC. A deployment can use ClickHouse, Iceberg, both, or neither.

This document is the integration and conformance contract. The repository does
not currently ship a ClickHouse client, Iceberg projector, catalog setup, or
live-backend integration tests; do not claim first-party backend support until
an adapter passes the checks below against the deployed versions.

```text
workflow runtime
      |
      v
RecordStoreService ------> protobuf .binpb objects (authoritative)
                                |
                 notifications / inventory / reconciliation
                                |
                                v
                         projection worker
                          /             \
                         v               v
              ClickHouse metadata    Iceberg tables
                         |               |
                         v               v
              RecordQueryService    batch SQL / BI / archive
                         |
                         v
              authoritative point GET before returning or deleting
```

The Iceberg warehouse and Temporaless records may use the same object-storage
account, but keep their prefixes and lifecycle policies separate. An Iceberg
table is not a Temporaless point store.

## Responsibility Matrix

| Operation | Authoritative point store | ClickHouse | Iceberg |
|---|---:|---:|---:|
| Workflow/activity replay | Yes | No | No |
| Atomic claims and event delivery | Yes | No | No |
| Durable timer discovery | Yes, or an external scheduler | Acceleration only | No |
| Interactive workflow/activity search | Hydration | Yes | Cold/eventual only |
| Retention sweep | Final recheck and deletion | Candidate selection | Optional archive gate |
| Historical analytics | Raw `.binpb` archive | Optional | Yes |

This split preserves Temporaless's serverless/stateless core runtime and
correctness path even when ClickHouse is an operated service. A ClickHouse or
Iceberg outage can make optional search or analytics unavailable, but it does
not prevent a workflow from replaying against its point records.

## Existing RPC Boundary

`RecordQueryService` already defines the portable query contract:

- `ListWorkflows`
- `ListActivities`
- `Sweep`
- `DueTimers`

A ClickHouse-backed service implements those generated RPCs directly, or
implements the language-local `QueryStore` seam and mounts the existing
ConnectRPC query handler. Query responses contain full protobuf records even
though the index stores only identities and query metadata. The adapter must
load each selected record from `RecordStoreService`, reapply the filter, and
return that authoritative value.

An analytics-only Iceberg service may implement just `ListWorkflows` and
`ListActivities` through the generated service and return `UNIMPLEMENTED` for
operations it cannot make safe. It must not advertise the complete
language-local `QueryStore` interface unless all four methods satisfy the
contract.

## Projection Ingestion

Temporaless records are mutable point snapshots. They intentionally do not
carry a universal monotonic mutation revision, and the core does not expose a
generic CDC log. A production projector therefore needs an adapter-owned
ordering source:

1. Commit the canonical `.binpb` record first.
2. Treat an object notification as an invalidation and re-read the canonical
   object instead of trusting notification payload ordering.
3. Attach a source revision that is comparable and monotonic for the same full
   record identity. An object generation or partitioned queue sequence can
   qualify; an opaque S3 VersionId or a sequence from an unrelated partition
   does not automatically qualify.
4. Write a versioned observation or tombstone keyed by the full Temporaless
   identity.
5. Periodically reconcile against object inventory or a bounded authoritative
   scan to repair missed notifications.

A projector derives typed identity from the protobuf payload, never by parsing
the v2 object path. A delete notification has no remaining payload, so retain a
`source_object_key -> typed identity` mapping from the last upsert, carry a
typed mutation envelope from the storage gateway, or let full reconciliation
discover the deletion. Keep `source_object_key` in projection metadata when
using object events.

A deterministic record digest is useful for idempotent ingestion, but it is
not an ordering primitive: a value can change and later return to the same
bytes. Wall-clock timestamps alone are also unsafe when multiple writers can
arrive out of order. A single projector helps only when its input is already
ordered per identity. If the source cannot provide that order, use scheduled
snapshot reconciliation rather than claiming real-time CDC correctness.

The bundled SQLite `IndexedStore` is a small-deployment write-through
reference. Its best-effort update followed by `rebuild()` is not a durable
cloud change feed.

## ClickHouse Query Adapter

Keep native ClickHouse tables narrow. Useful columns are:

```text
workflows(namespace, workflow_id, run_id, status, created_at, completed_at,
          source_object_key, source_revision, deleted, record_digest)
activities(namespace, workflow_id, run_id, activity_id, status, activity_type,
           created_at, completed_at, source_object_key, source_revision,
           deleted, record_digest)
timers(namespace, workflow_id, run_id, timer_id, status, timer_kind, fire_at,
       source_object_key, source_revision, deleted, record_digest)
```

Use the full identity as the `ReplacingMergeTree` deduplication / `ORDER BY`
key. If the adapter uses `ReplacingMergeTree(version, deleted)`, it must
resolve the latest version with `FINAL`, `argMax`, or an equivalent
current-state view before applying a status filter. Background merges do not
make duplicate versions disappear immediately. The `version` must come from
the projector's monotonic source; Temporaless does not manufacture it. Choose
an immutable partition expression that keeps every version of one identity in
the same partition; never partition current-state rows by a mutable status or
completion timestamp.

For production behavior:

- Batch observations or use ClickHouse asynchronous inserts. Do not issue one
  synchronous insert for every activity checkpoint.
- Append the full record identity as deterministic ordering tie-breakers.
- Use opaque keyset page tokens containing a token version, a fingerprint of
  the filters and `order_by`, the last complete sort tuple, and a projector
  snapshot/epoch. This requires a globally ordered `projection_epoch` or
  committed-batch sequence on every observation, retention of observation
  history for at least the token lifetime, and latest-per-identity queries
  constrained to `projection_epoch <= token_epoch`. If the adapter cannot pin
  that view, document weak cross-page consistency under concurrent writes
  instead of promising stable pagination. Reject a token reused with a
  different query.
- Fetch candidate point records with bounded concurrency, then revalidate the
  identity, projected digest/order tuple, and filters. Repair a changed
  projection and reselect before returning; prune missing objects and refill
  the page after rejected candidates. A missed projection can cause a
  temporary false negative until reconciliation. Never return the projected
  payload as canonical: return the authoritative value observed during point
  hydration. A later concurrent point write can still make that observation
  stale, so ordering and multi-page consistency follow the adapter's declared
  epoch or weak-consistency model.
- Delegate `DueTimers` to the authoritative point-store ledger, or union that
  ledger with ClickHouse candidates. A projection miss must never lose a wake.
- Use ClickHouse only to select `Sweep` candidates. Re-read the workflow,
  recheck status and age, run the existing claim-aware preflight, and delete
  the point-store run before tombstoning the projection.
- Retain deletion rows until every older source mutation is outside a
  source-specific safe horizon and reconciliation has passed it. ClickHouse
  delete markers are stored rows, so high retention-delete volume needs an
  explicit cleanup/compaction policy rather than indefinite tombstone growth.

ClickHouse documents that `ReplacingMergeTree` deduplication is eventually
performed by background merges, and that query-time latest-row resolution is
needed for correct current-state answers. It also recommends batching small
inserts or using asynchronous inserts. See the official
[ReplacingMergeTree guide](https://clickhouse.com/docs/en/guides/replacing-merge-tree)
and [bulk insert guidance](https://clickhouse.com/docs/en/optimize/bulk-inserts).

## Iceberg Projection

Use Iceberg for decoded, batched data rather than hot point lookups. Two useful
models are:

- an append-only, potentially incomplete projection-observation table for
  trend analysis;
- optional current-state workflow/activity tables maintained with `MERGE` by
  the full Temporaless identity and source revision.

Separate typed tables are usually easier to query than one sparse union.
Alongside useful scalar fields, retain:

```text
record_kind, full identity, schema_version, status/kind, record_digest,
record_binpb, source_object_key, source_revision, projection_operation,
projected_at, batch_id
```

`record_binpb` preserves the lossless protobuf archive. Keep `Any` payload
bytes opaque unless an application-owned projection has the matching
descriptors; do not introduce generic JSON serialization into the framework.
Use time transforms for append-only observations and stable, bounded identity
buckets for current-state tables, not raw high-cardinality workflow or run IDs.

Batch commits and schedule snapshot/file maintenance. An Iceberg snapshot is a
version of the projected table, not a Temporaless workflow event or audit
history:
point states overwritten between projection scans cannot be reconstructed.
Current-state `MERGE` support depends on the chosen Iceberg compute engine and
catalog; its match must update only when the incoming source revision is newer.
Apache Iceberg documents atomic table snapshots and optimistic concurrency,
Spark [`MERGE INTO`](https://iceberg.apache.org/docs/latest/spark-writes/), and
the need for regular
[snapshot and file maintenance](https://iceberg.apache.org/docs/latest/maintenance/).

ClickHouse may query Iceberg for cold analytics. Prefer native ClickHouse
projection tables for low-latency operator screens and keep timer correctness
off the lake path.

## Retention And Archival Safety

If Iceberg is merely analytics, projection lag is an observability concern. If
it is the required long-term archive, lag becomes a deletion safety concern.
Do not let bucket lifecycle rules or `Sweep` delete a run merely because a
projection job was scheduled.

Quiesce the run before its authoritative inventory/digest scan and keep it
quiesced through the Iceberg commit, manifest verification, and `DeleteRun`.
The deployment must publish a durable per-run archive manifest only after the
Iceberg commit succeeds. It should enumerate the workflow, activity, timer,
event, and claim identities that `DeleteRun` will remove, with each source
revision/digest; a global wall-clock watermark cannot prove that one run has no
projection gap. Alternatively, after quiescence, verify a previously committed
manifest against a fresh complete run scan. Retention may delete only a
quiesced run covered by that verified manifest.

The stock `Sweep` RPC has no archive-manifest input. Do not use it for an
archive-gated policy unless that `QueryStore` implementation enforces the gate.
Otherwise, use an application retention operator that selects candidates,
verifies the per-run manifest, then invokes authoritative `DeleteRun`, or keep
conservative bucket lifecycle rules. Temporaless deliberately does not add a
database-specific archive transaction to core.

## Adapter Conformance Checklist

Before claiming a ClickHouse- or Iceberg-backed query adapter is compatible,
test all behavior it exposes:

- exact namespace, workflow, run, and enum-status filters;
- allowed AIP-132 fields and rejection of unsupported ordering;
- a total order using the full identity, including `activity_id`;
- total, snapshot/epoch-bound pagination and tokens bound to the original
  query, or an explicit weak-consistency declaration;
- authoritative hydration, filter recheck, stale-row repair, and missing-row
  pruning;
- corrupt canonical record failure rather than returning projected data;
- idempotent duplicate observations, out-of-order updates, and tombstones;
- recovery from a missed notification through reconciliation;
- authoritative due-ledger fallback for `DueTimers`;
- authoritative status/age recheck and claim-aware deletion for `Sweep`;
- interruption and retry of projection batches without advancing the archive
  manifest/gate early.

Temporaless intentionally does not vendor a ClickHouse or Iceberg client in
core. The generated protobuf service is the stable integration surface; the
database lifecycle, credentials, catalog, projection cadence, and physical
schema stay deployment-owned.
