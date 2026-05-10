# Janitor Adapter

This is a decision adapter that recursively deletes completed workflow runs older than a configured max-age threshold.

## Purpose

Records persist forever by default. The janitor is the simplest workable retention story: completed runs stay around as long as the operator wants them, and a periodic sweep removes the rest.

## Position

Thin wrapper over `Store.ListWorkflows(COMPLETED)` plus `Store.Delete*`. Backend-agnostic: works against any `storage.Store`, including a remote `connectstore.ClientStore`.

## Supported Behavior

- list `WORKFLOW_STATUS_COMPLETED` whose `completed_at` is older than `now - max_age`
- delete the workflow's activities, timers, and events before the workflow record itself
- idempotent: re-sweeping after a partial failure is safe

## Rejected Behavior

- no FAILED-record retention (operator concern: failed runs may need to live longer for forensics)
- no IN_PROGRESS-record retention (those are sweepable separately, but the default is to leave them alone — they may be slow workflows, not stuck ones)
- no archival to cold storage: deletion is destructive; if you need archives, run a copy job first
