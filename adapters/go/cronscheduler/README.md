# Cron Scheduler Adapter

This is a decision adapter that turns a list of cron expressions into a stream of fire times that callers map to workflow runs.

## Purpose

Temporaless workflows expect explicit caller-provided IDs. Schedule-driven workflows (stocks fetches every minute during market hours, daily summaries, etc.) need *something* to compute fire times and trigger workflow runs at those instants. This adapter is that something.

## Position

In-process and stateful but in-memory. For durability across restarts, callers should rebuild last-fire times from existing workflow records — the convention is to embed the fire time in the workflow's `run_id` (e.g. `prices:aapl/2026-05-02T09:30:00Z`).

## Supported Behavior

- standard 5-field cron expressions via `robfig/cron/v3`
- catch-up: missed fires while the process was down are dispatched in order
- per-schedule last-fire tracking
- caller-owned dispatcher, so the workflow trigger transport (in-process, ConnectRPC, queue) is up to the caller

## Rejected Behavior

- no built-in timezone handling: callers pass timezone-aware times to `Tick`
- no claim coordination across multiple scheduler processes: pair with `gocdkclaims` if you run more than one
- no persistence: the user owns where last-fire times live
- not a replacement for cloud cron services or Dagster/Prefect schedules — those remain valid adapters that produce the same workflow trigger semantics
