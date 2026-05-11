# Analytics Cookbook

The differentiator from Temporal / Inngest / Trigger.dev / Step Functions:
**every workflow boundary is a protobuf record at a deterministic path in
object storage**. You don't need to run our service, query our database, or
learn our API to analyze what your pipelines are doing. Point your favorite
analytical engine at `temporaless/v1/` and the Hive partition columns —
`namespace`, `workflow_id`, `run_id`, `kind`, plus the per-kind id — are
auto-discovered.

This page shows the common queries quant / ML / ETL teams actually run.

## The layout, recap

```text
temporaless/v1/
  namespace={ns}/
    workflow_id={wf}/
      run_id={rid}/
        kind=workflow/record.binpb
        kind=activity/activity_id={aid}/record.binpb
        kind=timer/timer_id={tid}/record.binpb
        kind=event/event_id={eid}/record.binpb
        kind=claim/claim_id={cid}/record.binpb
```

Predicate pushdown works because the path columns are real partitions —
`WHERE namespace='default' AND kind='activity'` only fetches activity records.

## Option 1: DuckDB reading the bucket directly

The fastest path. No framework code; DuckDB reads `.binpb` files via its
binary reader and you decode protobuf inline via a small UDF. For local
development with the OpenDAL `fs` backend, this is one shell command away.

```sql
-- Treat every binpb as an opaque BLOB with Hive partition columns surfaced.
CREATE VIEW records AS
SELECT
  namespace,
  workflow_id,
  run_id,
  kind,
  -- DuckDB's Hive scanner exposes any `key=value` directory level as a column.
  filename,
  contents
FROM read_blob('s3://my-bucket/temporaless/v1/**/*.binpb', hive_partitioning=true);

-- Count workflows by status (decode the WorkflowRecord protobuf in-place).
-- The Python alternative below is easier if your DuckDB doesn't ship a proto UDF.
SELECT status, count(*) AS runs
FROM records
WHERE kind = 'workflow'
GROUP BY status
ORDER BY runs DESC;
```

If you're on local files, swap `s3://my-bucket/` for the OpenDAL root path.

## Option 2: Export-then-load (any warehouse)

For warehouses that don't speak protobuf natively (BigQuery, Snowflake,
Redshift, Athena), use the bundled `export` CLI to convert binpb to JSONL,
then load:

```sh
# Export every workflow record under a namespace.
temporaless --store-scheme fs --store-root /var/temporaless export \
  --kind workflow > workflows.jsonl

# 2-line DuckDB load.
duckdb -c "
  CREATE TABLE workflows AS
  SELECT * FROM read_json_auto('workflows.jsonl');
"
```

For BigQuery, drop the `.jsonl` in a GCS bucket and `bq load --source_format=NEWLINE_DELIMITED_JSON`.

> **Note:** the `export` CLI is a transitional surface. Once the framework's
> protobuf service is migrated to `invariantprotocol`, the CLI (and an MCP
> server) come for free from the generated bindings, and the Go binary
> retires. The bucket-direct path (Option 1) is forever.

## Useful queries

These assume the records have already been decoded into typed columns
(either via DuckDB's protobuf UDF or via the export-then-load path).

### Failure rate by workflow_id, last 24h

```sql
WITH recent AS (
  SELECT
    workflow_id,
    status,
    completed_at
  FROM workflows
  WHERE completed_at >= now() - INTERVAL 24 HOURS
)
SELECT
  workflow_id,
  count(*) FILTER (WHERE status = 'WORKFLOW_STATUS_FAILED')
    * 1.0 / count(*) AS failure_rate,
  count(*) AS total_runs
FROM recent
GROUP BY workflow_id
ORDER BY failure_rate DESC;
```

### p50 / p95 / p99 activity attempt count

Heavy retry counts are usually the first signal that a vendor is degrading
or a backoff policy is wrong.

```sql
SELECT
  activity_type,
  median(json_array_length(attempts))                        AS p50,
  approx_quantile(json_array_length(attempts), 0.95)         AS p95,
  approx_quantile(json_array_length(attempts), 0.99)         AS p99,
  max(json_array_length(attempts))                           AS p100
FROM activities
WHERE status IN ('ACTIVITY_STATUS_COMPLETED', 'ACTIVITY_STATUS_FAILED')
  AND completed_at >= now() - INTERVAL 7 DAYS
GROUP BY activity_type
ORDER BY p99 DESC;
```

### Vendor latency from annotations

If activity bodies call `workflow.Annotate(ctx, "vendor", ...)` and
`workflow.Annotate(ctx, "latency_ms", ...)`, the per-attempt history is
visible in storage.

```sql
SELECT
  annotations->>'vendor' AS vendor,
  median(cast(annotations->>'latency_ms' AS DOUBLE))             AS p50_ms,
  approx_quantile(cast(annotations->>'latency_ms' AS DOUBLE), 0.95) AS p95_ms,
  count(*) AS samples
FROM activities
WHERE annotations->>'vendor' IS NOT NULL
  AND completed_at >= now() - INTERVAL 1 DAY
GROUP BY vendor
ORDER BY p95_ms DESC;
```

### Stale workflows (operator alert query)

The framework returns typed pending errors when a workflow waits on a timer
or event. The workflow stays `IN_PROGRESS` until a scanner re-invokes it. If
something's broken in the scanner / event-delivery pipeline, workflows pile
up in IN_PROGRESS forever — alert on these:

```sql
SELECT
  namespace,
  workflow_id,
  run_id,
  now() - created_at AS age
FROM workflows
WHERE status = 'WORKFLOW_STATUS_IN_PROGRESS'
  AND created_at < now() - INTERVAL 1 HOUR
ORDER BY age DESC;
```

### Durable retry pressure

How much of our retry budget is going to durable timers (long backoffs vs
in-process)? When this jumps, a vendor is rate-limiting more than usual.

```sql
SELECT
  date_trunc('hour', created_at) AS hour,
  count(*) AS durable_retries_scheduled
FROM timers
WHERE timer_kind = 'TIMER_KIND_ACTIVITY_RETRY'
GROUP BY hour
ORDER BY hour DESC;
```

## Why this matters

Every other workflow framework expects you to query *their* database / API
to learn what happened. With storage-first, the records are the source of
truth and they live at predictable, partitioned paths under YOUR bucket.
That means:

- **Data warehouse owns the audit trail.** dbt / Looker / Snowflake reports
  can stand on this without orchestration-team coordination.
- **No special runtime is required for analysis.** A laptop with DuckDB and
  `aws s3 cp` is enough.
- **Forensic reconstruction is free.** Records are immutable per-key writes;
  S3 versioning preserves history.

The runtime is one of many consumers of the bucket. Analytics is another.
That's the point.
