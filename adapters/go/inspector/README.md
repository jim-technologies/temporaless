# Inspector Adapter

This is a decision adapter for operator visibility plus explicit point-reset
helpers over the authoritative record store.

## Purpose

Core Temporaless ships no UI or control plane; the `*.binpb` records are the source of truth. This adapter turns "list workflows currently in flight" or "list failed workflows" into a single function call. For a read-only view in a browser, the optional [console](../../../docs/console.md) serves `RunInspectionService` over the same records.

## Position

Cross-run workflow views use `WorkflowQueryStore`. Run-scoped activity listing
and reset helpers use the authoritative point `Store`. A remote
`connectstore.ClientStore` can implement both when configured with both RPC
services.

## Supported Behavior

- list workflows by `WorkflowStatus` (IN_PROGRESS, FAILED, COMPLETED, ...)
- list activities under a workflow run
- reset (delete) a workflow record, activity record, or event record so the next invocation re-executes
- returns the full proto records so callers can read failure codes, attempts, annotations

Reset helpers are point deletes, not an execution fence. Quiesce the run. For
a partial retry, validate and delete failed activities' paired retry timers,
delete only the failed activity records, and delete the parent workflow record
last; completed activity records remain checkpoints. See `docs/runbook.md`.

## Rejected Behavior

- no per-activity listing (yet — easy to add when needed)
- no claim listing (compose with the claim store directly)
- no streaming: results are buffered in memory; use pagination directly on the
  query adapter for very large result sets
- not a control plane: reset helpers are explicit point deletes and provide no
  workflow dispatch, transaction, or execution fence
