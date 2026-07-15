# Cron Scheduler Adapter

This is a decision adapter that turns a list of cron expressions into a stream of fire times that callers map to workflow runs.

## Purpose

Temporaless workflows expect explicit caller-provided IDs. Schedule-driven workflows (stocks fetches every minute during market hours, daily summaries, etc.) need *something* to compute fire times and trigger workflow runs at those instants. This adapter is that something.

## Position

In-process and stateful but in-memory. For durability across restarts, callers
can rebuild last-fire times with one latest-run pointer GET per schedule. Set
`WorkflowOptions.run_order_time` to the scheduled fire time; run IDs remain
opaque and application-owned.

## Supported Behavior

- standard 5-field cron expressions via `robfig/cron/v3`
- catch-up: missed fires while the process was down are dispatched in order
- per-schedule last-fire tracking
- point-operation seeding from the core store's latest-run pointer
- caller-owned dispatcher, so the workflow trigger transport (in-process, ConnectRPC, queue) is up to the caller
- at-least-once dispatch when ticks overlap; workflow execution claims can
  single-flight cooperating recipients, but the dispatcher itself does not
  claim a fire

## Rejected Behavior

- no built-in timezone handling: callers pass timezone-aware times to `Tick`
- no claim coordination across multiple scheduler processes: set a caller-owned
  `claim_owner_id` on each dispatched workflow and provide an atomic claim
  store when overlapping live execution must be suppressed
- no persistence: the user owns where last-fire times live
- storage-derived recovery requires retaining the pointed workflow run; use
  `Snapshot`/`Restore` when retention may be shorter than an outage
- not a replacement for cloud cron services or Dagster/Prefect schedules — those remain valid adapters that produce the same workflow trigger semantics
