# Janitor Adapter

This is a decision adapter that recursively deletes completed workflow runs older than a configured max-age threshold.

## Purpose

Records persist forever by default. The janitor is the simplest workable retention story: completed runs stay around as long as the operator wants them, and a periodic sweep removes the rest.

## Position

Thin wrapper over `Store.ListWorkflows(COMPLETED)` plus bounded run deletion. It accepts the generated `SweepRequest` and an optional explicit `ClaimStore`; pass the deployment's separate claim backend when claims do not live on the record store. Remote `connectstore.ClientStore` instances auto-detect their service-backed claim surface.

## Supported Behavior

- list `WORKFLOW_STATUS_COMPLETED` whose `completed_at` is older than `now - max_age`
- preflight claim capability and require run-scoped claim listing from a claim-capable backend
- snapshot and validate every eligible run before the first mutation
- delete run-scoped claims before activities, timers, events, and the workflow record
- idempotent: re-sweeping after a partial failure is safe

Sweep is not an execution fence or transaction. Externally quiesce eligible runs while it executes; a concurrent record or claim created after the snapshot is outside the cleanup guarantee.

## Rejected Behavior

- no FAILED-record retention (operator concern: failed runs may need to live longer for forensics)
- no IN_PROGRESS-record retention (those are sweepable separately, but the default is to leave them alone — they may be slow workflows, not stuck ones)
- no archival to cold storage: deletion is destructive; if you need archives, run a copy job first
