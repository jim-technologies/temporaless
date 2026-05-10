"""Cross-workflow dependency helper.

When pipeline B depends on pipeline A's run for the same date / partition,
``wait_for_workflow`` reads A's record and either:

- returns A's typed result if A is COMPLETED;
- raises ``WorkflowDependencyPendingError`` if A is still IN_PROGRESS or
  hasn't started — B stays IN_PROGRESS until a scanner / re-invoke retries it;
- raises ``WorkflowDependencyFailedError`` if A is in a terminal-failed state
  — B fails too, since the upstream is unrecoverable without operator action.

Replay-friendly: a single ``store.get_workflow`` call, no record writes from
this side, idempotent on workflow re-execution.

The ``current_workflow().store`` accessor is the canonical way to reach the
store from inside a workflow body — adapter helpers like this one don't need
to be threaded through.

Example::

    from temporaless import current_workflow
    from temporaless.dependencies import wait_for_workflow

    async def compose_signal(input: SignalRequest, ctx) -> SignalResponse:
        wf = current_workflow()
        upstream = await wait_for_workflow(
            wf.store,
            workflow_id=f"prices:{input.symbol}",
            run_id=input.date,
            result_factory=FetchResponse,
        )
        # … compute signal from upstream.prices …
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from google.protobuf.message import Message

from temporaless.storage import Store, WorkflowKey
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    WorkflowConflictError,
    WorkflowDependencyFailedError,
    WorkflowDependencyPendingError,
)

ResultT = TypeVar("ResultT", bound=Message)


async def wait_for_workflow(
    store: Store,
    *,
    workflow_id: str,
    run_id: str,
    result_factory: Callable[[], ResultT],
) -> ResultT:
    """Read an upstream workflow's record and return its result.

    Args:
        store: the same Store the upstream workflow ran against.
        workflow_id: upstream workflow id.
        run_id: upstream run id (typically the same partition/date as the
            calling workflow's run_id).
        result_factory: callable returning a fresh instance of the upstream's
            result message type. Used by ``Any.Unpack``.

    Raises:
        WorkflowDependencyPendingError: upstream not COMPLETED yet.
        WorkflowDependencyFailedError: upstream ended in a non-COMPLETED
            terminal state.
        WorkflowConflictError: upstream's stored result type doesn't match
            ``result_factory()``.
    """
    key = WorkflowKey(workflow_id=workflow_id, run_id=run_id)
    record = await store.get_workflow(key)
    if record is None or record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS:
        raise WorkflowDependencyPendingError(workflow_id, run_id)
    if record.status != temporaless_pb2.WORKFLOW_STATUS_COMPLETED:
        raise WorkflowDependencyFailedError(workflow_id, run_id, record.status)
    result = result_factory()
    if not record.result.Unpack(result):
        raise WorkflowConflictError(
            f"workflow {workflow_id!r}/{run_id!r} stored result type does not match requested type"
        )
    return result
