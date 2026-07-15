# Timer Scanner Adapter

This is a decision adapter that finds due timer records so a serverless or worker process can re-invoke the workflow handler.

## Purpose

Temporaless workflows return `ErrTimerPending` when they reach a future durable
sleep. Some caller has to re-invoke them after `fire_at`. This adapter reads the
core due-timer ledger, returns due wakes whose `fire_at` has passed, and
includes the associated workflow record so callers can dispatch it. A
ledger-first process crash leaves the full prepared `TimerRecord`; the first
scan repairs its canonical point and a later scan dispatches only after both
copies match exactly.

## Position

This is the simplest "no-engine" scheduler: a stateless point-ledger read.
Larger deployments may index timers in SQL or push them onto a queue when they
are written.

## Supported Behavior

- calls `Store.DueTimers` once; bucket stores serve it from the compact ledger
- filter to `fire_at <= now`
- attach the `WorkflowRecord` for each due timer
- backend-agnostic: works against any `storage.Store`, including a remote `connectstore.ClientStore`
- at-least-once delivery: a wake may be returned on consecutive ticks until
  replay persists a later wake-bearing or terminal boundary

## Rejected Behavior

- no re-invocation built in: callers decide how to dispatch (HTTP, queue, in-process)
- no claim coordination between concurrent scanners: set `claim_owner_id` on
  the dispatched workflow and use an atomic claim store to single-flight
  cooperating invocations; external side effects still require idempotency
- no SQL required; an optional query adapter may provide an alternate indexed scan
- stale timers under COMPLETED or FAILED workflows are skipped: the workflow has already moved past them

## Retention

The generic `_due` ledger keeps one deterministic prepared object per timer.
Preserve both active canonical timer records and prepared objects for at least
the maximum timer horizon plus scheduler outage/recovery grace; exempt them
from lifecycle deletion when durations are unbounded. Remove terminal/canceled
tombstones only while timer writers and scanners are quiesced, after checking
the prepared, canonical, and workflow records. The generic ledger has no
online compaction mode.
