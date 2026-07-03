# Scheduling

Temporaless does not keep a worker process alive for durable sleeps. A long sleep is a protobuf timer record.

When workflow code calls sleep:

1. Temporaless writes a `TimerRecord` with a stable timer ID and `fire_at`.
2. The workflow returns a typed pending error.
3. A scheduler invokes the same workflow again after `fire_at`.
4. The timer record is marked fired and workflow code continues.

This means `sleep(10 years)` is valid as a durable record, but no process blocks for ten years.

## Core vs Scheduler

The core owns:

- timer protobuf records
- timer conflict detection
- pending/fired state transitions during workflow replay

Schedulers are adapters. A scheduler can be:

- a cloud cron that invokes due workflows
- an object-storage due-ledger scanner for small deployments (`adapters/go/timerscanner` and `temporaless.timerscanner` ship one)
- an in-process cron loop for fixed schedule lists (`adapters/go/cronscheduler` and `temporaless.cronscheduler` ship one)
- a SQL-backed due-work index for larger deployments
- a queue worker fed by object-storage notifications

SQL is useful for interactive queries and very large operational views, but it should stay optional. The core should remain storage-first and stateless. In v2 storage, workflow IDs and namespaces beginning with `_` are reserved for system prefixes such as `_latest` and `_due`.

The bundled scanner lists the compact due-timer ledger under `temporaless/v2/{namespace}/_due/`, returns timers with `TIMER_STATUS_SCHEDULED` and `fire_at <= now`, and attaches each timer's workflow record so callers have enough context to dispatch a re-invocation. Re-invocation itself is left to the caller because the right transport (HTTP, queue, in-process) varies by deployment.

The same operation is exposed as the `DueTimers` RPC on `RecordStoreService`. When the storage backend lives behind ConnectRPC (`ConnectStore`), the client makes one round-trip and the server reads the due ledger locally.

Timer writes create the `_due` ledger entry before writing the timer record. A crash in that window can leave an orphaned ledger entry, but the scanner re-reads the authoritative timer/workflow records and prunes stale entries. The dangerous inverse window — timer record present but no ledger entry — is avoided for bucket-only deployments.

If a due-ledger object is corrupt or contains invalid keys, the scanner logs it
and moves the bytes under `temporaless/v2/{namespace}/_due_invalid/`. That keeps
the scanner from re-reading the same bad entry forever while preserving the
object for forensics or manual cleanup.

## Distributing the cron scheduler

The bundled cron scheduler keeps last-fire times in memory. To run it in a distributed or restartable deployment, externalize that state in one of two ways:

1. **Snapshot/Restore.** Call `Snapshot()` after each `Tick` and persist the returned map (e.g. as a `WorkflowRecord` annotation, in storage, or in your own database). On boot, call `Restore(map)` before the first `Tick`. This works for any run_id format.

2. **Storage-derived (recommended when run_ids embed fire times).** Call `LastFiresFromRuns(ctx, store, namespace, scheduleIDs, runIDLayout)` (Go) or `last_fires_from_runs(...)` (Python) on boot. This reads one latest-run pointer under `temporaless/v2/{namespace}/_latest/{workflow_id}.binpb` per schedule, parses the pointed-to run ID as a timestamp, and returns a snapshot suitable for `Restore`. No separate persistence needed — the workflow records plus pointer objects are the scheduler's memory.

Latest-run pointers are derived bucket objects, not authoritative records. Python bucket stores update them with a best-effort read/compare/write: if both the existing and incoming run IDs parse as schedule fire times, the parsed fire time is the monotonic guard; otherwise the workflow record timestamp is used. `OpenDALStore(..., latest_run_id_formats=(...))` configures deployment-specific run ID layouts; common ISO-like formats are tried by default. A lost race can leave the pointer stale, and deleting the run that currently owns the pointer deletes the pointer without searching for the previous run. If you need stronger delete-time schedule memory, keep all schedule runs until lifecycle expiry or seed from a query index/offline scan after deletes.

Either way, the scheduler is fully stateless across restarts: the in-memory map is just a cache of derivable state.

## Cron

Cron should create workflow runs; it should not be mixed into workflow execution.

The intended convention is:

```text
schedule id -> workflow id + run id + protobuf input
```

For market-data workflows, a run ID should usually include the schedule fire time, for example:

```text
prices:aapl / 2026-05-02T09:30:00Z
```

This keeps reruns and backfills explicit.
