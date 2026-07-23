# Scheduling

Temporaless does not keep a worker process alive for durable waits. Sleeps,
activity-retry backoffs, and optional event/dependency polls are protobuf timer
records.

When workflow code calls sleep:

1. Temporaless writes a `TimerRecord` with a stable timer ID and `fire_at`.
2. The workflow returns a typed pending error.
3. A scheduler invokes the same workflow again after `fire_at`.
4. Workflow code continues while the due timer remains `SCHEDULED`, so a crash
   still has a wakeup to redeliver.
5. After a later wake-bearing timer or terminal workflow record is durable,
   Temporaless marks the consumed timer `FIRED`.

This means `sleep(10 years)` is valid as a durable record, but no process blocks for ten years.

## Core vs Scheduler

The core owns:

- timer protobuf records
- timer conflict detection
- pending/fired state transitions during workflow replay
- caller-identified `TIMER_KIND_POLL` rearming for opted-in waits

Schedulers are adapters. A scheduler can be:

- a cloud cron that invokes due workflows
- an object-storage due-ledger scanner for small deployments (`adapters/go/timerscanner` and `temporaless.timerscanner` ship one)
- an in-process cron loop for fixed schedule lists (`adapters/go/cronscheduler` and `temporaless.cronscheduler` ship one)
- a SQL-backed due-work index for larger deployments
- a queue worker fed by object-storage notifications

SQL is useful for interactive queries and very large operational views, but it should stay optional. The core should remain storage-first and stateless. In v2 storage, workflow IDs and namespaces beginning with `_` are reserved for system prefixes such as `_latest` and `_due`.

The bundled scanner lists the compact due-timer ledger under `temporaless/v2/{namespace}/_due/`, returns sleep, activity-retry, and poll timers with `TIMER_STATUS_SCHEDULED` and `fire_at <= now`, and attaches each timer's workflow record so callers have enough context to dispatch a re-invocation. Re-invocation itself is left to the caller because the right transport (HTTP, queue, in-process) varies by deployment.

The scanner is an at-least-once wake source, not a queue acknowledgement
protocol. A due timer remains visible across ticks until workflow replay
persists a later wake-bearing timer or a terminal workflow record. Concurrent
scanners may therefore dispatch the same workflow. Execution claims can
single-flight cooperating workflow invocations, but activities still need
domain idempotency for external side effects.

The same operation is exposed as the `DueTimers` RPC on `RecordStoreService`. When the storage backend lives behind ConnectRPC (`ConnectStore`), the client makes one round-trip and the server reads the due ledger locally.

## Manual Waits And Optional Durable Polling

Event and workflow-dependency waits do not create a timer by default. With
`poll_options=None` in Python or a final `nil` in Go, the typed pending result
leaves the workflow `IN_PROGRESS`, and the application must invoke it again.
An event-delivery webhook will usually call `send_event` / `SendEvent` and then
dispatch that same workflow run. A dependency wait can be retried by its
upstream completion path, a backfill, or another application trigger.

Callers that prefer scanner-driven rechecks can pass `PollOptions` with an
explicit, stable timer ID and positive interval:

```python
await workflow.wait_event(
    "approval",
    Approval,
    PollOptions(timer_id="poll:approval", interval=interval),
)
```

```go
workflow.WaitEvent(ctx, "approval", newApproval, &workflow.PollOptions{
    TimerId:  "poll:approval",
    Interval: durationpb.New(time.Minute),
})
```

While the condition is unresolved, the wait writes or rearms that
`TIMER_KIND_POLL` timer and returns a pending error containing the next wake
time. `DueTimers` exposes it through the same at-least-once scanner as every
other timer. Re-invocation rereads the authoritative event or upstream
workflow; if it is still unresolved, Temporaless rearms the same caller-owned
timer from the current time. Once resolved, the poll timer is consumed only
after a later durable boundary, so a crash cannot remove its only wakeup.

The timer ID and interval are replay contracts. Reusing the ID for another
timer kind or changing the interval under the same run fails as a timer
conflict. Polling controls re-invocation latency, not event delivery:
`SendEvent` remains the atomic create-once write, and an application may still
invoke immediately after delivery instead of waiting for the next poll tick.

Every timer transition first overwrites one deterministic `_due` object for its
`TimerKey`, including the full prepared `TimerRecord`, and then writes the
canonical run-scoped timer point. The prepared object is a write-ahead overlay:
`GetTimer` and `ListTimers` prefer it if a process dies between those writes, so
replay retains the exact timer kind, duration, status, and original deadline.
Deletion writes a `CANCELED` tombstone before removing the point.

The scanner reconciles a missing, stale, or corrupt canonical point from that
exact prepared record and does not dispatch it in the same scan. A later scan
must observe both copies in agreement before it emits a due wake. This adds at
most one scanner interval after an interrupted write and prevents a half-written
re-arm or status transition from producing an early or invented wake. Terminal
or missing parent workflows and non-scheduled timer states remain filtered.

If a due-ledger object is corrupt or contains invalid keys, the scanner copies
the bytes under `temporaless/v2/{namespace}/_due_invalid/` and fails the tick
loudly; it never silently discards the only cross-run wake index. The source
remains in place so the failure stays visible until an operator restores or
republishes the exact timer. The deterministic quarantine copy avoids unbounded
duplicate forensic objects. Remove stale/corrupt source entries only with an
offline or otherwise quiescent cleanup that cannot race timer writers.

## Retention And Ledger Compaction

The timer record under the workflow run and its `_due` prepared record must both
survive until the timer can be delivered. Size object-lifecycle rules to at
least:

```text
maximum durable-timer horizon + maximum scheduler outage + recovery/redelivery grace
```

For example, supporting a ten-year sleep means preserving the active run's
timer records and the corresponding `_due` entries for more than ten years.
If the application accepts unbounded durations, exempt active run prefixes and
`_due` from age-based deletion. A lifecycle rule that expires a prepared record based
only on object age can silently strand a still-`SCHEDULED` timer.

The generic bucket ledger keeps one deterministic object per logical timer and
overwrites it as the timer changes state. Canceled tombstones are retained
online because ordinary object-storage delete cannot prove that a concurrent
writer did not just rearm the same logical timer. Remove terminal tombstones
only during a maintenance window after quiescing timer writers and scanners.
Re-read the `TimerRecord`, prepared entry, and `WorkflowRecord` for every
candidate before deleting it; the generic ledger defines no online compaction
mode.

These retention rules apply independently of the ConnectStore process. The
bundled production-server examples serve storage RPCs only; an application
workflow service, `BackgroundWorkers`, or an external scheduler must perform
the scan and route each wake to the correct workflow handler.

## Distributing the cron scheduler

The bundled cron scheduler keeps last-fire times in memory. To run it in a distributed or restartable deployment, externalize that state in one of two ways:

1. **Snapshot/Restore.** Call `Snapshot()` after each `Tick` and persist the returned map (e.g. as a `WorkflowRecord` annotation, in storage, or in your own database). On boot, call `Restore(map)` before the first `Tick`. This works for any run_id format.

2. **Storage-derived.** Set `WorkflowOptions.run_order_time` to every scheduled
   fire time, then call `LastFiresFromRuns(ctx, store, namespace, scheduleIDs)`
   (Go) or `last_fires_from_runs(...)` (Python) on boot. This reads one
   latest-run pointer under
   `temporaless/v2/{namespace}/_latest/{workflow_id}.binpb` per schedule and
   returns a snapshot suitable for `Restore`. Run IDs remain opaque; neither
   SDK parses them as dates.

Latest-run pointers are derived bucket objects, not authoritative records. Go
and Python bucket stores compare the protobuf `run_order_time`; if the caller
omits it they fall back to the workflow lifecycle timestamp.
A lost race can leave the pointer stale. A reader can also land after the new
authoritative workflow record is written but before its pointer metadata is
updated. Public reads return not-found for that transient metadata mismatch so
a scheduler never consumes stale order/status; its next tick can retry.
Deleting the referenced run likewise leaves the derived pointer object in
place, with public reads returning not-found after the workflow point GET
misses. Generic core never deletes the pointer: an unconditional delete can
race a newer writer. If you need stronger delete-time schedule memory, keep
schedule runs until lifecycle expiry or seed from a query index/offline scan
after deletes.

Storage-derived recovery is stateless only while the pointed workflow run is
retained. Keep schedule runs longer than the maximum scheduler outage/cadence,
or persist `Snapshot()` externally. Deleting the pointed run does not scan
history or select an earlier run in the core point store.

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
