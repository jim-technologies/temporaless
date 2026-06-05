# Inspector Adapter

This is a decision adapter for read-only operator visibility into the OpenDAL-backed record tree.

## Purpose

Temporaless does not ship a UI or control plane. The bundled "view" of system state is whatever you can decode from `*.binpb` records. This adapter turns "list workflows currently in flight" or "list failed workflows" into a single function call.

## Position

Thin wrappers over `Store.ListWorkflows`, `Store.ListActivities`, and `Store.Delete*`. Backend-agnostic: works against any `storage.Store`, including a remote `connectstore.ClientStore`.

## Supported Behavior

- list workflows by `WorkflowStatus` (IN_PROGRESS, FAILED, COMPLETED, ...)
- list activities under a workflow run
- reset (delete) a workflow record, activity record, or event record so the next invocation re-executes
- returns the full proto records so callers can read failure codes, attempts, annotations

## Rejected Behavior

- no per-activity listing (yet — easy to add when needed)
- no claim listing (compose with the claim store directly)
- no streaming: results are buffered in memory; not suited to multi-million-record trees without a SQL index
- not a control plane: this adapter is read-only
