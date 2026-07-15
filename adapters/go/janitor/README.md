# Janitor Adapter

This is a decision adapter that recursively deletes completed workflow runs older than a configured max-age threshold.

## Purpose

Records persist forever by default. The janitor is the simplest workable retention story: completed runs stay around as long as the operator wants them, and a periodic sweep removes the rest.

## Position

The janitor takes a `WorkflowQueryStore` for cross-run COMPLETED candidates, an
authoritative point `Store` for run-scoped snapshots/deletes, and an optional
explicit `ClaimStore`. Core bucket stores never provide the candidate query.
Use an indexed adapter in production or `scanquery` only for offline/small
development buckets.

## Supported Behavior

- list `WORKFLOW_STATUS_COMPLETED` whose `completed_at` is older than `now - max_age`
- preflight claim capability and require run-scoped claim listing from a claim-capable backend
- snapshot and validate every eligible run before the first mutation
- delete run-scoped claims before activities, timers, events, and the workflow record
- idempotent: re-sweeping after a partial failure is safe

Sweep is not an execution fence or transaction. Externally quiesce eligible runs while it executes; a concurrent record or claim created after the snapshot is outside the cleanup guarantee.

Bucket lifecycle rules must not expire active run timer records or `_due`
prepared objects before the maximum timer horizon plus scheduler
outage/recovery grace. The janitor intentionally selects terminal runs;
due-ledger tombstone cleanup is a separate offline/quiescent operation.

## Rejected Behavior

- no FAILED-record retention (operator concern: failed runs may need to live longer for forensics)
- no IN_PROGRESS-record retention (those are sweepable separately, but the default is to leave them alone — they may be slow workflows, not stuck ones)
- no archival to cold storage: deletion is destructive; if you need archives, run a copy job first
