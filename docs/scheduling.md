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
- an object-storage scanner for small deployments (`adapters/go/timerscanner` and `temporaless.timerscanner` ship one)
- an in-process cron loop for fixed schedule lists (`adapters/go/cronscheduler` and `temporaless.cronscheduler` ship one)
- a SQL-backed due-work index for larger deployments
- a queue worker fed by object-storage notifications

SQL is useful for efficient due-time queries, but it should stay optional. The core should remain storage-first and stateless.

The bundled scanner walks the Hive partition `temporaless/v1/.../kind=timer/timer_id=*/record.binpb`, returns timers with `TIMER_STATUS_SCHEDULED` and `fire_at <= now`, and attaches each timer's workflow record so callers have enough context to dispatch a re-invocation. Re-invocation itself is left to the caller because the right transport (HTTP, queue, in-process) varies by deployment.

The same operation is exposed as the `DueTimers` RPC on `RecordStoreService`. When the storage backend lives behind ConnectRPC (`ConnectStore`), the client makes one round-trip and the server runs the list-and-filter loop locally. For remote stores this is a substantial latency win — the Python `timerscanner.due_timers` helper transparently routes through that single round-trip.

## Distributing the cron scheduler

The bundled cron scheduler keeps last-fire times in memory. To run it in a distributed or restartable deployment, externalize that state in one of two ways:

1. **Snapshot/Restore.** Call `Snapshot()` after each `Tick` and persist the returned map (e.g. as a `WorkflowRecord` annotation, in storage, or in your own database). On boot, call `Restore(map)` before the first `Tick`. This works for any run_id format.

2. **Storage-derived (recommended when run_ids embed fire times).** Call `LastFiresFromRuns(ctx, store, namespace, scheduleIDs, runIDLayout)` (Go) or `last_fires_from_runs(...)` (Python) on boot. This scans existing workflow records, parses run_ids as timestamps, and returns a snapshot suitable for `Restore`. No separate persistence needed — the workflow records themselves are the scheduler's memory.

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
