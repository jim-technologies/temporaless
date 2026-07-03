"""Operator-visibility helpers.

Cross-run listing is served by an optional query index. Reset helpers remain
point deletes against the core Store.
"""

from __future__ import annotations

from temporaless.storage import ActivityKey, EventKey, QueryStore, Store, WorkflowKey
from temporaless.v1 import temporaless_pb2


async def list_in_flight_workflows(query: QueryStore) -> list[temporaless_pb2.WorkflowRecord]:
    """Return every workflow record whose status is IN_PROGRESS."""
    records, _ = await query.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS)
    return records


async def list_failed_workflows(query: QueryStore) -> list[temporaless_pb2.WorkflowRecord]:
    """Return every workflow record whose status is FAILED."""
    records, _ = await query.list_workflows("", "", temporaless_pb2.WORKFLOW_STATUS_FAILED)
    return records


async def list_workflows_by_status(
    query: QueryStore,
    status: temporaless_pb2.WorkflowStatus,
) -> list[temporaless_pb2.WorkflowRecord]:
    """Generic form, exposed for callers wanting COMPLETED audits and similar."""
    records, _ = await query.list_workflows("", "", status)
    return records


async def list_activities(store: Store, key: WorkflowKey) -> list[temporaless_pb2.ActivityRecord]:
    """Return every activity record under the given workflow run."""
    return await store.list_activities(key)


async def reset_workflow(store: Store, key: WorkflowKey) -> None:
    """Delete the workflow record so the next invocation re-executes from scratch."""
    await store.delete_workflow(key)


async def reset_activity(store: Store, key: ActivityKey) -> None:
    """Delete the activity record so the next ExecuteActivity re-executes."""
    await store.delete_activity(key)


async def reset_event(store: Store, key: EventKey) -> None:
    """Delete the event record so WaitEvent returns pending again."""
    await store.delete_event(key)
