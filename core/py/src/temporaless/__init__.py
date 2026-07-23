"""Temporaless public API — convention over configuration.

The top-level surface here is the **application-author** API: the things
you import to write and run a workflow and catch the errors users typically
catch. Transport-specific handler wrappers live in adapters.

Adapter authors and operators reach into submodules for finer-grained
types (record schema versions, sub-Protocols, claim capabilities, custom
RPC client/server wiring, conflict-error variants):

- ``from temporaless.storage import …`` — schema version constants,
  ``ClaimKey``, ``DueTimer``, ``ClaimCapability`` enum, sub-Protocols
  (``ActivityStore``, ``EventStore``, ``TimerStore``, ``WorkflowStore``).
- ``from temporaless.workflow import …`` — conflict errors
  (``WorkflowConflictError``, ``ActivityConflictError``,
  ``TimerConflictError``) and lower-level activity primitives.
- ``from temporaless.connectstore import …`` — ``RecordStoreService``,
  ``RecordQueryService``, and client Protocols for custom RPC clients.
- ``from temporaless.inspector import …`` — query-index-backed visibility helpers.
- ``from temporaless.visualization import …`` — optional plan validation,
  approval digests, and plan-versus-run evidence projection.
- ``from temporaless.janitor import …`` / ``temporaless.timerscanner`` /
  ``temporaless.cronscheduler`` — periodic operations adapters.

If you find yourself reaching into submodules a lot, that's a signal you
might be writing an adapter rather than an application — that's fine; the
submodule API is stable and documented.
"""

from temporaless.connectstore import ConnectStore, asgi_application
from temporaless.storage import (
    ActivityKey,
    EventDeliveryConflictError,
    EventDeliveryUnsupportedError,
    EventKey,
    OpenDALStore,
    Store,
    TimerKey,
    WorkflowKey,
    deliver_event,
    send_event,
)
from temporaless.visualization import (
    ClaimLister,
    NodeProjection,
    RunInspection,
    RunProjection,
    inspect_run,
    plan_digest,
    project_workflow_run,
    validate_plan,
)
from temporaless.workflow import (
    ActivityError,
    ActivityOptions,
    ActivityWrapOptions,
    ClaimBusyError,
    ClaimCapabilityError,
    ClaimReleaseError,
    EventPendingError,
    Options,
    PollOptions,
    RetryPolicy,
    TimerPendingError,
    Workflow,
    WorkflowDependencyFailedError,
    WorkflowDependencyPendingError,
    WorkflowInfrastructureError,
    WorkflowWrapOptions,
    annotate,
    current_workflow,
    gather_activities,
    run,
    wrap_activity,
    wrap_workflow,
)

__all__ = [
    # Errors users catch
    "ActivityError",
    "ActivityKey",
    "ActivityOptions",
    "ActivityWrapOptions",
    "ClaimBusyError",
    "ClaimCapabilityError",
    "ClaimLister",
    "ClaimReleaseError",
    "ConnectStore",
    "EventDeliveryConflictError",
    "EventDeliveryUnsupportedError",
    "EventKey",
    "EventPendingError",
    "OpenDALStore",
    "NodeProjection",
    "Options",
    "PollOptions",
    "RetryPolicy",
    "RunInspection",
    "RunProjection",
    "Store",
    "TimerKey",
    "TimerPendingError",
    "Workflow",
    "WorkflowDependencyFailedError",
    "WorkflowDependencyPendingError",
    "WorkflowInfrastructureError",
    "WorkflowKey",
    "WorkflowWrapOptions",
    "annotate",
    "asgi_application",
    "current_workflow",
    "deliver_event",
    "gather_activities",
    "inspect_run",
    "plan_digest",
    "project_workflow_run",
    "run",
    "send_event",
    "validate_plan",
    "wrap_activity",
    "wrap_workflow",
]
