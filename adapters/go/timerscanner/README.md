# Timer Scanner Adapter

This is a decision adapter that finds due timer records so a serverless or worker process can re-invoke the workflow handler.

## Purpose

Temporaless workflows return `ErrTimerPending` when they reach a future durable sleep. Some caller has to re-invoke them after `fire_at`. This adapter walks the OpenDAL-backed record tree, returns scheduled timers whose `fire_at` has passed, and includes the associated workflow record so callers have the context to dispatch.

## Position

This is the simplest "no-engine" scheduler: a stateless walk over storage. Larger deployments should index timers in SQL or push them onto a queue when they are written. Either of those is also fine as a Temporaless adapter, but they are out of scope here.

## Supported Behavior

- composes `Store.ListWorkflows(IN_PROGRESS)` with `Store.ListTimers(SCHEDULED)` per run
- filter to `fire_at <= now`
- attach the `WorkflowRecord` for each due timer
- backend-agnostic: works against any `storage.Store`, including a remote `connectstore.ClientStore`

## Rejected Behavior

- no re-invocation built in: callers decide how to dispatch (HTTP, queue, in-process)
- no claim coordination between concurrent scanners: pair with `gocdkclaims` if duplicate dispatch is unsafe
- no SQL or external index: large deployments should add their own
- stale timers under COMPLETED or FAILED workflows are skipped: the workflow has already moved past them
