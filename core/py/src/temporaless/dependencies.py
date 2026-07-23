"""Cross-workflow dependency helper.

When pipeline B depends on pipeline A's run for the same date / partition,
``wait_for_workflow`` reads A's record and either:

- returns A's typed result if A is COMPLETED;
- raises ``WorkflowDependencyPendingError`` if A is still IN_PROGRESS or
  hasn't started — B stays IN_PROGRESS; the application must re-invoke it
  unless ``poll_options`` arms a scanner-visible timer;
- raises ``WorkflowDependencyFailedError`` if A is in a terminal-failed state
  — B fails too, since the upstream is unrecoverable without operator action.

Replay-friendly: each call reads the upstream point record once. Manual waits
write nothing; ``poll_options`` may write or rearm one caller-identified timer
in B's run. Both modes are idempotent on workflow re-execution.

The ``current_workflow().store`` accessor is the canonical way to reach the
store from inside a workflow body — adapter helpers like this one don't need
to be threaded through.

Example::

    from temporaless import current_workflow
    from temporaless.dependencies import wait_for_workflow

    async def compose_signal(request: SignalRequest) -> SignalResponse:
        wf = current_workflow()
        upstream = await wait_for_workflow(
            wf.store,
            workflow_id=f"prices:{request.symbol}",
            run_id=request.date,
            result_factory=FetchResponse,
        )
        # … compute signal from upstream.prices …
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from google.protobuf.message import DecodeError, Message

from temporaless.storage import RunRecordValidationError, Store, WorkflowKey
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    PollOptions,
    WorkflowConflictError,
    WorkflowDependencyFailedError,
    WorkflowDependencyPendingError,
    _await_workflow_infrastructure,
    _validated_poll_interval,
    current_workflow,
)

ResultT = TypeVar("ResultT", bound=Message)


async def wait_for_workflow(
    store: Store,
    *,
    workflow_id: str,
    run_id: str,
    result_factory: Callable[[], ResultT],
    poll_options: PollOptions | None = None,
) -> ResultT:
    """Read an upstream workflow's record and return its result.

    Args:
        store: the same Store the upstream workflow ran against.
        workflow_id: upstream workflow id.
        run_id: upstream run id (typically the same partition/date as the
            calling workflow's run_id).
        result_factory: callable returning a fresh instance of the upstream's
            result message type. Used by ``Any.Unpack``.
        poll_options: optional caller-identified durable polling timer. When
            omitted, the wait remains manually re-invoked as before.

    Raises:
        WorkflowDependencyPendingError: upstream not COMPLETED yet.
        WorkflowDependencyFailedError: upstream ended in a non-COMPLETED
            terminal state.
        WorkflowConflictError: upstream's stored result type doesn't match
            ``result_factory()``.
        RunRecordValidationError: a COMPLETED upstream has no decodable result.
    """
    key = WorkflowKey(workflow_id=workflow_id, run_id=run_id)
    key.validate()
    if poll_options is not None:
        _validated_poll_interval(poll_options)
    result = result_factory()
    if not isinstance(result, Message):
        raise TypeError("result_factory must return a protobuf message")
    record = await _await_workflow_infrastructure(
        "read workflow dependency",
        store.get_workflow(key),
    )
    if record is None or record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS:
        wake_at = (
            await current_workflow().arm_poll(poll_options) if poll_options is not None else None
        )
        raise WorkflowDependencyPendingError(workflow_id, run_id, wake_at)
    if poll_options is not None:
        await current_workflow().resolve_poll(poll_options)
    if record.status != temporaless_pb2.WORKFLOW_STATUS_COMPLETED:
        raise WorkflowDependencyFailedError(workflow_id, run_id, record.status)
    if not record.HasField("result"):
        raise RunRecordValidationError(
            f"workflow {workflow_id!r}/{run_id!r} completed without a stored result"
        )
    try:
        matched = record.result.Unpack(result)
    except DecodeError as exc:
        raise RunRecordValidationError(
            f"workflow {workflow_id!r}/{run_id!r} has an invalid stored result"
        ) from exc
    if not matched:
        raise WorkflowConflictError(
            f"workflow {workflow_id!r}/{run_id!r} stored result type does not match requested type"
        )
    return result
