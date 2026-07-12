from __future__ import annotations

import asyncio
import contextvars
import inspect
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import TypeVar, cast

from google.protobuf.any_pb2 import Any
from google.protobuf.duration_pb2 import Duration
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp
from protovalidate import validate

from temporaless._cache import RunScopedCache
from temporaless.storage import (
    ACTIVITY_RECORD_SCHEMA_VERSION,
    CLAIM_RECORD_SCHEMA_VERSION,
    DEFAULT_NAMESPACE,
    SLEEP_TIMER_KIND,
    TIMER_RECORD_SCHEMA_VERSION,
    WORKFLOW_RECORD_SCHEMA_VERSION,
    ActivityKey,
    ClaimKey,
    ClaimStore,
    EventKey,
    Store,
    TimerKey,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2

RequestT = TypeVar("RequestT", bound=Message)
ResultT = TypeVar("ResultT", bound=Message)
TaskResultT = TypeVar("TaskResultT")
Options = temporaless_pb2.WorkflowOptions
ActivityOptions = temporaless_pb2.ActivityOptions
RetryPolicy = temporaless_pb2.RetryPolicy

# Framework-reserved string literals sourced from the proto-declared defaults
# on ``temporaless.v1.ReservedNames``. Reading from a zero-value instance
# (rather than declaring parallel constants here) guarantees the proto
# contract is the single source of truth — renaming any reserved string is a
# one-line proto change plus regenerate, no SDK constant drifts.
_RESERVED_NAMES = temporaless_pb2.ReservedNames()
_RUNTIME_DEFAULTS = temporaless_pb2.RuntimeDefaults()
DEFAULT_CLAIM_LEASE_DURATION = timedelta(seconds=_RUNTIME_DEFAULTS.claim_lease_duration_seconds)

# Marks timer records owned by the runtime's durable retry path. User code
# passing this prefix to ``Workflow.sleep`` is rejected so framework-managed
# retry timers don't collide with user timers.
ACTIVITY_RETRY_TIMER_ID_PREFIX = _RESERVED_NAMES.activity_retry_timer_id_prefix
ACTIVITY_CLAIM_ID_PREFIX = _RESERVED_NAMES.activity_claim_id_prefix

# Deterministic claim_id used to serialize live invocations of one workflow
# run. The workflow_id and run_id live in the surrounding ClaimKey.
WORKFLOW_EXECUTION_CLAIM_ID = _RESERVED_NAMES.workflow_execution_claim_id


async def _await_task_to_completion(
    task: asyncio.Task[TaskResultT],
) -> tuple[TaskResultT, asyncio.CancelledError | None]:
    """Wait for a state-changing child task without letting repeated parent
    cancellation abandon it mid-operation.

    Returns the first cancellation request after the child has resolved. The
    caller records the acquired/released state, then re-raises cancellation.
    ``uncancel`` removes only the request consumed here; any earlier
    cancellation already propagating through a surrounding finally remains.
    """
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
            current = asyncio.current_task()
            if current is not None:
                current.uncancel()
    return task.result(), cancellation


def _activity_retry_timer_id(activity_id: str) -> str:
    """Deterministic timer_id that pairs an ActivityRecord with its durable
    retry timer. Stable per activity_id; later retries overwrite the record
    with a new fire_at."""
    return f"{ACTIVITY_RETRY_TIMER_ID_PREFIX}{activity_id}"


async def _acquire_concurrency_slot(
    claim_store: ClaimStore,
    namespace: str,
    concurrency_key: str,
    limit: int,
    owner_id: str,
    code_version: str,
    lease_duration: timedelta,
) -> str | None:
    """Try slots 0..limit-1 in order; return the slot_id of the acquired slot
    or None when all slots are taken by other owners.

    Existing slots are always occupied, even when owner_id matches. Treating a
    matching owner as re-acquired would let two live duplicate invocations
    share and prematurely release the same slot.

    Distributed-safe: uses only :meth:`ClaimStore.try_create_claim` (the
    storage backend arbitrates the create race via S3 If-None-Match / GCS
    ifGenerationMatch=0 / OpenDAL if_not_exists). No app-level locks.
    """
    slot_id_prefix = _RESERVED_NAMES.concurrency_slot_id_prefix
    for i in range(limit):
        slot_id = f"{slot_id_prefix}{i}"
        slot_key = ClaimKey(
            namespace=namespace,
            workflow_id=CONCURRENCY_WORKFLOW_ID,
            run_id=concurrency_key,
            claim_id=slot_id,
        )
        now = datetime.now(UTC)
        created_at = Timestamp()
        created_at.FromDatetime(now)
        expires_at = Timestamp()
        expires_at.FromDatetime(now + lease_duration)
        claim = temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=slot_key.to_proto(),
            owner_id=owner_id,
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_CONCURRENCY_KEY,
            resource_id=concurrency_key,
            code_version=code_version,
            lease_expires_at=expires_at,
            created_at=created_at,
            heartbeat_at=created_at,
        )
        if await claim_store.try_create_claim(claim):
            return slot_id
    return None


async def _release_concurrency_slot(
    claim_store: ClaimStore,
    namespace: str,
    concurrency_key: str,
    slot_id: str,
) -> None:
    """Delete the named slot claim. Idempotent: a missing claim is not an
    error. Always called in a finally block so every exit path (success,
    failure, pending) releases the slot."""
    slot_key = ClaimKey(
        namespace=namespace,
        workflow_id=CONCURRENCY_WORKFLOW_ID,
        run_id=concurrency_key,
        claim_id=slot_id,
    )
    await claim_store.delete_claim(slot_key)


async def _release_activity_claim(claim_store: ClaimStore, claim_key: ClaimKey) -> None:
    try:
        await claim_store.delete_claim(claim_key)
    except BaseException as exc:
        raise ClaimReleaseError("activity claim", exc) from exc


async def _release_invocation_claims(
    claim_store: ClaimStore,
    concurrency_key: str,
    acquired_slot_id: str | None,
    workflow_claim_key: ClaimKey | None,
) -> None:
    """Release every claim held by one workflow invocation.

    Both deletes are attempted. Cleanup failures are surfaced to the caller;
    silently returning a pending workflow with a leaked create-only claim
    would make every later resume fail with ClaimBusyError.
    """
    failures: list[ClaimReleaseError] = []
    if acquired_slot_id is not None and concurrency_key:
        try:
            await _release_concurrency_slot(
                claim_store,
                DEFAULT_NAMESPACE,
                concurrency_key,
                acquired_slot_id,
            )
        except BaseException as exc:  # cleanup must still attempt the run claim
            failures.append(ClaimReleaseError("concurrency slot", exc))
    if workflow_claim_key is not None:
        try:
            await claim_store.delete_claim(workflow_claim_key)
        except BaseException as exc:  # surfaced after both deletes are attempted
            failures.append(ClaimReleaseError("workflow execution claim", exc))
    if len(failures) == 1:
        raise failures[0]
    if failures:
        raise ClaimReleaseError(
            "workflow invocation claims",
            ExceptionGroup("multiple claim releases failed", failures),
        )


def default_retry_policy() -> RetryPolicy:
    """Sensible default for :meth:`Workflow.activity` callers who don't pass
    an explicit ``retry_policy``.

    The shape (3 attempts, 1s initial, 2x backoff, 30s max, 30s durable
    threshold) is tuned for the framework's stated workloads: LLM completions
    (rate-limit windows of 30s–10min become durable timers automatically),
    vendor APIs returning transient 5xx, and quant-pipeline activities
    hitting short-lived market-data hiccups.

    Returns a fresh proto on each call so callers can mutate without
    sharing state.
    """
    policy = RetryPolicy(
        maximum_attempts=3,
        backoff_coefficient=2.0,
    )
    policy.initial_interval.FromTimedelta(timedelta(seconds=1))
    policy.maximum_interval.FromTimedelta(timedelta(seconds=30))
    policy.durable_backoff_threshold.FromTimedelta(timedelta(seconds=30))
    return policy


def _infer_activity_id(func: Callable[..., object]) -> str:
    """Use ``func.__qualname__`` as the activity_id default. Stable for
    module-level functions and class methods; varies for closures (rare in
    workflow bodies)."""
    import re

    qualname = getattr(func, "__qualname__", None)
    if not qualname:
        raise ValueError(
            "cannot infer activity_id: function has no __qualname__; pass activity_id= explicitly"
        )
    # Python's qualname inserts ``<locals>`` for nested functions; that's a
    # marker, not part of any meaningful identity, and contains characters
    # the framework's ID regex rejects.
    qualname = qualname.replace("<locals>.", "")
    if not re.match(r"^[A-Za-z0-9._:-]+$", qualname):
        raise ValueError(
            f"cannot infer activity_id from __qualname__ {qualname!r}: contains "
            "characters disallowed in framework IDs (allowed: [A-Za-z0-9._:-]). "
            "Pass activity_id= explicitly."
        )
    return qualname


def _infer_result_type(func: Callable[..., object]) -> type:
    """Pull the result type out of ``func``'s return annotation. Expects
    ``async def fn(req) -> ResultMessage`` shape; the return annotation is
    ``ResultMessage`` (NOT ``Awaitable[ResultMessage]`` thanks to PEP 3107
    semantics for coroutine functions).
    """
    import typing

    hints = typing.get_type_hints(func)
    if "return" not in hints:
        raise ValueError(
            "cannot infer result_type: function has no return annotation; "
            "pass result_type= explicitly"
        )
    return_type = hints["return"]
    # If the user annotated `Awaitable[X]` explicitly, unwrap.
    origin = typing.get_origin(return_type)
    if origin is not None and origin in {
        getattr(typing, "Awaitable", None),
        getattr(typing, "Coroutine", None),
    }:
        args = typing.get_args(return_type)
        if args:
            return_type = args[-1]
    return return_type


class ActivityConflictError(RuntimeError):
    pass


class ActivityError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        cause: BaseException | None = None,
        retry_after: timedelta | None = None,
    ) -> None:
        super().__init__(
            f"activity error [{code}]: {message}" if code else f"activity error: {message}"
        )
        self.code = code
        self.message = message
        self.cause = cause
        # When set (> 0), overrides the retry policy's computed interval for
        # the next attempt: planner uses max(computed, retry_after). Set this
        # from a vendor's HTTP ``Retry-After`` header, a Slack
        # ``Retry-After-In`` field, or an OpenAI ``x-ratelimit-reset`` header.
        self.retry_after = retry_after


class WorkflowConflictError(RuntimeError):
    pass


class ConcurrencyBusyError(RuntimeError):
    """Raised when a workflow's ``concurrency_key`` slot pool is full.

    The workflow body did NOT execute and no IN_PROGRESS record was written —
    callers retry the same ``workflow.run`` when capacity is available. Maps
    to gRPC code RESOURCE_EXHAUSTED via :func:`workflow_error_to_connect_code`.
    """

    def __init__(self, key: str, limit: int) -> None:
        super().__init__(f"concurrency cap {key!r} at limit {limit}")
        self.key = key
        self.limit = limit


# Synthetic workflow_id under which concurrency slot claims are stored.
# Sourced from the proto-declared default on
# ``ReservedNames.concurrency_workflow_id`` — see the _RESERVED_NAMES block
# at the top of this module for the single-source-of-truth pattern.
CONCURRENCY_WORKFLOW_ID = _RESERVED_NAMES.concurrency_workflow_id


class ClaimBusyError(RuntimeError):
    def __init__(
        self,
        claim_id: str,
        owner_id: str | None = None,
        lease_expires_at: datetime | None = None,
        capability: temporaless_pb2.ClaimCapability | None = None,
    ) -> None:
        message = f"claim {claim_id!r} is busy"
        if lease_expires_at is not None:
            message = f"{message} (recorded lease expiry {lease_expires_at.isoformat()})"
        super().__init__(message)
        self.claim_id = claim_id
        self.owner_id = owner_id
        self.lease_expires_at = lease_expires_at
        self.capability = capability


class ClaimReleaseError(RuntimeError):
    """Raised when an acquired workflow or concurrency claim cannot be
    released. Retrying may remain busy until operator cleanup."""

    def __init__(self, resource: str, cause: BaseException) -> None:
        super().__init__(f"failed to release {resource}: {cause}")
        self.resource = resource
        self.__cause__ = cause


class ClaimCapabilityError(RuntimeError):
    """Requested claim coordination is unavailable from the configured store.

    The runtime fails closed rather than silently degrading ``claim_owner_id``
    or ``concurrency_key`` to at-least-once execution.
    """

    def __init__(self, capability: temporaless_pb2.ClaimCapability, option: str) -> None:
        try:
            capability_name = temporaless_pb2.ClaimCapability.Name(capability)
        except ValueError:
            capability_name = str(capability)
        super().__init__(f"claim capability {capability_name} does not support {option}")
        self.capability = capability
        self.option = option


class TimerConflictError(RuntimeError):
    pass


class TimerPendingError(RuntimeError):
    def __init__(self, timer_id: str, wake_at: datetime) -> None:
        super().__init__(f"timer {timer_id!r} is pending until {wake_at.isoformat()}")
        self.timer_id = timer_id
        self.wake_at = wake_at


class EventPendingError(RuntimeError):
    def __init__(self, event_id: str) -> None:
        super().__init__(f"event {event_id!r} is pending")
        self.event_id = event_id


class WorkflowDependencyPendingError(RuntimeError):
    """Raised when a workflow body waits on another workflow that hasn't
    completed yet. Like ``EventPendingError``, this leaves the calling
    workflow IN_PROGRESS so a scanner / re-invoke can resume it later."""

    def __init__(self, workflow_id: str, run_id: str) -> None:
        super().__init__(f"workflow {workflow_id!r}/{run_id!r} has not completed")
        self.workflow_id = workflow_id
        self.run_id = run_id


class WorkflowDependencyFailedError(RuntimeError):
    """Raised when a workflow body waits on another workflow that ended in
    a non-COMPLETED terminal status (FAILED). The dependency is unrecoverable
    without operator action — propagating as a typed error means downstream
    workflows fail loudly rather than waiting forever."""

    def __init__(self, workflow_id: str, run_id: str, status: int) -> None:
        super().__init__(f"workflow {workflow_id!r}/{run_id!r} dependency failed (status={status})")
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.status = status


def workflow_error_to_connect_code(exc: BaseException) -> tuple[object, str] | None:
    """Map a workflow exception to a ``(connectrpc.code.Code, message)`` pair.

    Useful in ConnectRPC handlers that wrap workflows: the body raises one of
    our typed errors, the handler catches and re-raises as ``ConnectError``.
    Returns ``None`` for unknown exception types so callers can decide between
    re-raising as ``Internal`` or letting the exception propagate.

    Standard mapping (mirrors ``docs/deployment.md``):

    - ``TimerPendingError``, ``EventPendingError``,
      ``WorkflowDependencyPendingError`` → ``UNAVAILABLE``
      (caller should retry later — workflow stays IN_PROGRESS).
    - ``ClaimBusyError`` → ``ALREADY_EXISTS`` (another worker holds the claim).
    - ``ClaimReleaseError`` → ``INTERNAL`` (cleanup failed and retry may stay busy).
    - ``ClaimCapabilityError`` → ``FAILED_PRECONDITION`` (the store cannot
      provide coordination requested by the workflow options).
    - ``WorkflowConflictError``, ``ActivityConflictError``, ``TimerConflictError``
      → ``FAILED_PRECONDITION`` (stored record's workflow_type / activity_type
      / timer kind / code_version is incompatible with the current call).
    - ``ActivityError``, ``WorkflowDependencyFailedError`` → ``INTERNAL`` (the
      upstream pipeline produced a terminal failure that this workflow can't
      recover from).

    Lazy import of ``connectrpc.code.Code`` — the helper is in core/workflow.py
    so workflow.py stays usable without a connectrpc dependency for non-RPC
    callers.
    """
    from connectrpc.code import Code

    if isinstance(exc, ClaimReleaseError):
        return (Code.INTERNAL, str(exc))
    if isinstance(exc, (TimerPendingError, EventPendingError, WorkflowDependencyPendingError)):
        return (Code.UNAVAILABLE, str(exc))
    if isinstance(exc, ClaimBusyError):
        return (Code.ALREADY_EXISTS, str(exc))
    if isinstance(exc, ConcurrencyBusyError):
        return (Code.RESOURCE_EXHAUSTED, str(exc))
    if isinstance(exc, ClaimCapabilityError):
        return (Code.FAILED_PRECONDITION, str(exc))
    if isinstance(exc, (WorkflowConflictError, ActivityConflictError, TimerConflictError)):
        return (Code.FAILED_PRECONDITION, str(exc))
    if isinstance(exc, (ActivityError, WorkflowDependencyFailedError)):
        return (Code.INTERNAL, str(exc))
    return None


@dataclass
class _AnnotationsBag:
    data: dict[str, str] = field(default_factory=dict)

    def set(self, key: str, value: str) -> None:
        self.data[key] = value

    def snapshot(self) -> dict[str, str]:
        return dict(self.data)


_annotations_var: contextvars.ContextVar[_AnnotationsBag | None] = contextvars.ContextVar(
    "temporaless_annotations", default=None
)
_workflow_var: contextvars.ContextVar[Workflow | None] = contextvars.ContextVar(  # noqa: F821
    "temporaless_workflow", default=None
)


def annotate(key: str, value: str) -> None:
    """Attach a key/value pair to the running activity or workflow record.

    Annotations survive replay because they are persisted on the stored record.
    """
    bag = _annotations_var.get()
    if bag is not None:
        bag.set(key, value)


def current_workflow() -> Workflow:
    """Return the ``Workflow`` of the in-flight ``run`` invocation.

    Use inside a ConnectRPC handler that's been decorated with
    ``wrap_workflow_method``: the handler doesn't see a ``Workflow`` argument,
    but can reach it through this context-local accessor to call
    ``execute_activity``, ``sleep``, ``wait_event``, etc.

    Raises ``RuntimeError`` if called outside a workflow body — that's a
    programming error and should fail fast.
    """
    workflow = _workflow_var.get()
    if workflow is None:
        raise RuntimeError(
            "current_workflow() called outside a workflow body — wrap your handler "
            "with wrap_workflow / wrap_workflow_method, or use temporaless.workflow.run."
        )
    return workflow


@dataclass(frozen=True)
class WorkflowWrapOptions[RequestT: Message]:
    store: Store
    options: Options | None = None
    options_for: Callable[[RequestT], Options] | None = None


class Workflow:
    def __init__(
        self,
        store: Store,
        options: Options,
        *,
        claim_capability: temporaless_pb2.ClaimCapability | None = None,
    ) -> None:
        options = normalized_workflow_options(options)
        claim_store: ClaimStore | None = None
        if options.claim_owner_id:
            if not isinstance(store, ClaimStore):
                raise ValueError("claim store is required when claim owner is provided")
            claim_store = store
        self._store = store
        self._claim_store = claim_store
        self._claim_capability = claim_capability
        self._workflow_id = options.workflow_id
        self._run_id = options.run_id
        self._code_version = options.code_version
        self._claim_owner = options.claim_owner_id or None

    @property
    def workflow_id(self) -> str:
        return self._workflow_id

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def code_version(self) -> str:
        return self._code_version

    async def run_activity(
        self,
        activity_id: str,
        activity_type: str,
        input_message: Message,
        result_factory: Callable[[], ResultT],
        execute: Callable[[], Awaitable[ResultT]],
        retry_policy: RetryPolicy | None = None,
    ) -> ResultT:
        validate(ActivityOptions(activity_id=activity_id))
        if not activity_type:
            raise ValueError("activity type is required")
        plan = _plan_retries(retry_policy)

        key = ActivityKey(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            activity_id=activity_id,
        )

        def inspect_record(
            stored: temporaless_pb2.ActivityRecord | None,
        ) -> tuple[
            ResultT | None,
            list[temporaless_pb2.ActivityAttempt],
            dict[str, str],
            datetime | None,
        ]:
            if stored is None:
                return None, [], {}, None
            if stored.status in (
                temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
                temporaless_pb2.ACTIVITY_STATUS_FAILED,
            ):
                return (
                    _replay_record(stored, activity_type, self._code_version, result_factory),
                    [],
                    {},
                    None,
                )
            if stored.status != temporaless_pb2.ACTIVITY_STATUS_RETRYING:
                raise ActivityConflictError("stored activity has unknown status")
            _assert_activity_identity(stored, activity_type, self._code_version)
            wake_at = None
            if stored.HasField("next_attempt_at"):
                wake_at = stored.next_attempt_at.ToDatetime().replace(tzinfo=UTC)
            return None, list(stored.attempts), dict(stored.annotations), wake_at

        record = await self._store.get_activity(key)
        replayed, attempts, seeded_annotations, retry_wake_at = inspect_record(record)
        if replayed is not None:
            return replayed
        if retry_wake_at is not None and datetime.now(UTC) < retry_wake_at:
            raise TimerPendingError(_activity_retry_timer_id(activity_id), retry_wake_at)

        activity_claim_key: ClaimKey | None = None
        activity_claim_acquired = False
        release_activity_claim = False
        try:
            if self._claim_owner is not None:
                if self._claim_store is None:
                    raise ValueError("claim store is required when claim owner is provided")
                if self._claim_capability is None:
                    self._claim_capability = await self._claim_store.claim_capability()
                if self._claim_capability not in (
                    temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS,
                    temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS,
                ):
                    raise ClaimCapabilityError(self._claim_capability, "claim_owner_id")
                claim_id = f"{ACTIVITY_CLAIM_ID_PREFIX}{activity_id}"
                activity_claim_key = ClaimKey(
                    workflow_id=self._workflow_id,
                    run_id=self._run_id,
                    claim_id=claim_id,
                )
                for claim_attempt in range(2):
                    now = datetime.now(UTC)
                    created_at = Timestamp()
                    created_at.FromDatetime(now)
                    expires_at = Timestamp()
                    expires_at.FromDatetime(now + DEFAULT_CLAIM_LEASE_DURATION)
                    claim = temporaless_pb2.ClaimRecord(
                        schema_version=CLAIM_RECORD_SCHEMA_VERSION,
                        key=activity_claim_key.to_proto(),
                        owner_id=self._claim_owner,
                        resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
                        resource_id=activity_id,
                        code_version=self._code_version,
                        lease_expires_at=expires_at,
                        created_at=created_at,
                        heartbeat_at=created_at,
                    )
                    create_task = asyncio.create_task(self._claim_store.try_create_claim(claim))
                    created, acquisition_cancelled = await _await_task_to_completion(create_task)
                    if acquisition_cancelled is not None:
                        activity_claim_acquired = created
                        raise acquisition_cancelled
                    if created:
                        activity_claim_acquired = True
                        break

                    # A terminal activity may have appeared after our cached
                    # negative read and failed conditional create. Bypass the
                    # normal replay cache and update it with authoritative state.
                    if isinstance(self._store, RunScopedCache):
                        fresh = await self._store.refresh_activity(key)
                    else:
                        fresh = await self._store.get_activity(key)
                    if (
                        fresh is not None
                        and fresh.status != temporaless_pb2.ACTIVITY_STATUS_RETRYING
                    ):
                        return _replay_record(
                            fresh,
                            activity_type,
                            self._code_version,
                            result_factory,
                        )

                    existing = await self._claim_store.get_claim(activity_claim_key)
                    if existing is not None or claim_attempt == 1:
                        owner_id = None
                        lease_expires_at = None
                        if existing is not None:
                            owner_id = existing.owner_id
                            if existing.HasField("lease_expires_at"):
                                lease_expires_at = existing.lease_expires_at.ToDatetime().replace(
                                    tzinfo=UTC
                                )
                        raise ClaimBusyError(
                            claim_id,
                            owner_id,
                            lease_expires_at,
                            self._claim_capability,
                        )

                if not activity_claim_acquired:
                    raise RuntimeError("failed to acquire activity claim")

                # A prior holder may have completed or advanced the retry
                # record immediately before releasing. Refresh again after we
                # own the claim and derive all retry state from that record.
                release_activity_claim = True
                if isinstance(self._store, RunScopedCache):
                    record = await self._store.refresh_activity(key)
                else:
                    record = await self._store.get_activity(key)
                replayed, attempts, seeded_annotations, retry_wake_at = inspect_record(record)
                if replayed is not None:
                    return replayed
                if retry_wake_at is not None and datetime.now(UTC) < retry_wake_at:
                    raise TimerPendingError(_activity_retry_timer_id(activity_id), retry_wake_at)
                # From this point through the activity body, an exception may
                # mean a side effect or storage write had an ambiguous outcome.
                release_activity_claim = False

            # Only consume a due retry timer after claim arbitration. A busy
            # activity leaves the timer scheduled so the scanner can try again.
            if retry_wake_at is not None:
                await self._mark_activity_retry_timer_fired(activity_id)

            input_any = Any()
            input_any.Pack(input_message)

            interval = plan.initial_interval
            attempt_idx = len(attempts)
            activity_annotations = _AnnotationsBag(data=dict(seeded_annotations))
            annotations_token = _annotations_var.set(activity_annotations)
            try:
                while attempt_idx < plan.maximum_attempts:
                    attempt_idx += 1
                    started_at = datetime.now(UTC)
                    try:
                        # A persisted in-process backoff is a safe release
                        # boundary, but the next body attempt is ambiguous
                        # again as soon as it starts.
                        release_activity_claim = False
                        result = await execute()
                    except asyncio.CancelledError:
                        # Cancellation can arrive after an external side effect
                        # but before its result record. Retain the create-only
                        # claim for verified operator recovery.
                        raise
                    except BaseException as run_err:  # noqa: BLE001
                        completed_at = datetime.now(UTC)
                        failure = _failure_from_exception(run_err)
                        attempt_record = temporaless_pb2.ActivityAttempt(
                            attempt=attempt_idx,
                            failure=failure,
                        )
                        attempt_record.started_at.FromDatetime(started_at)
                        attempt_record.completed_at.FromDatetime(completed_at)
                        attempts.append(attempt_record)

                        if failure.HasField("retry_after"):
                            retry_after = failure.retry_after.ToTimedelta()
                            if retry_after > interval:
                                interval = retry_after

                        non_retryable = failure.code in plan.non_retryable_codes
                        if attempt_idx >= plan.maximum_attempts or non_retryable:
                            failed_record = temporaless_pb2.ActivityRecord(
                                schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
                                key=key.to_proto(),
                                activity_type=activity_type,
                                code_version=self._code_version,
                                input=input_any,
                                status=temporaless_pb2.ACTIVITY_STATUS_FAILED,
                                failure=failure,
                                attempts=attempts,
                                annotations=activity_annotations.snapshot(),
                            )
                            failed_record.created_at.FromDatetime(
                                attempts[0].started_at.ToDatetime()
                            )
                            failed_record.completed_at.FromDatetime(completed_at)
                            await self._store.put_activity(failed_record)
                            release_activity_claim = True
                            raise ActivityError(failure.code, failure.message, run_err) from run_err

                        retrying_record = temporaless_pb2.ActivityRecord(
                            schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
                            key=key.to_proto(),
                            activity_type=activity_type,
                            code_version=self._code_version,
                            input=input_any,
                            status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
                            failure=failure,
                            attempts=attempts,
                            annotations=activity_annotations.snapshot(),
                        )
                        retrying_record.created_at.FromDatetime(attempts[0].started_at.ToDatetime())

                        if (
                            plan.durable_threshold > timedelta(0)
                            and interval >= plan.durable_threshold
                        ):
                            next_attempt_at = datetime.now(UTC) + interval
                            next_at_ts = Timestamp()
                            next_at_ts.FromDatetime(next_attempt_at)
                            retrying_record.next_attempt_at.CopyFrom(next_at_ts)
                            await self._store.put_activity(retrying_record)
                            await self._put_activity_retry_timer(
                                activity_id, interval, next_attempt_at
                            )
                            release_activity_claim = True
                            raise TimerPendingError(
                                _activity_retry_timer_id(activity_id), next_attempt_at
                            ) from None

                        await self._store.put_activity(retrying_record)
                        release_activity_claim = True
                        await asyncio.sleep(interval.total_seconds())
                        interval = _next_interval(interval, plan)
                        continue

                    completed_at = datetime.now(UTC)
                    attempt_record = temporaless_pb2.ActivityAttempt(attempt=attempt_idx)
                    attempt_record.started_at.FromDatetime(started_at)
                    attempt_record.completed_at.FromDatetime(completed_at)
                    attempts.append(attempt_record)

                    result_any = Any()
                    result_any.Pack(result)
                    completed_record = temporaless_pb2.ActivityRecord(
                        schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
                        key=key.to_proto(),
                        activity_type=activity_type,
                        code_version=self._code_version,
                        input=input_any,
                        status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
                        result=result_any,
                        attempts=attempts,
                        annotations=activity_annotations.snapshot(),
                    )
                    completed_record.created_at.FromDatetime(attempts[0].started_at.ToDatetime())
                    completed_record.completed_at.FromDatetime(completed_at)
                    await self._store.put_activity(completed_record)
                    release_activity_claim = True
                    return result

                raise RuntimeError(f"activity {activity_id!r} exhausted retry plan")
            finally:
                _annotations_var.reset(annotations_token)
        finally:
            if (
                release_activity_claim
                and activity_claim_acquired
                and activity_claim_key is not None
                and self._claim_store is not None
            ):
                cleanup_task = asyncio.create_task(
                    _release_activity_claim(self._claim_store, activity_claim_key)
                )
                _, cleanup_cancelled = await _await_task_to_completion(cleanup_task)
                if cleanup_cancelled is not None:
                    raise cleanup_cancelled

    async def execute_activity(
        self,
        options: ActivityOptions,
        input_message: RequestT,
        result_factory: Callable[[], ResultT],
        execute: Callable[[RequestT], Awaitable[ResultT]],
    ) -> ResultT:
        validate(options)
        if not inspect.iscoroutinefunction(execute):
            raise ValueError("activity executor must be async (define it with `async def`)")
        result_template = result_factory()
        activity_type = message_pair_type("activity", input_message, result_template)

        async def adapter() -> ResultT:
            return await execute(input_message)

        return await self.run_activity(
            options.activity_id,
            activity_type,
            input_message,
            result_factory,
            adapter,
            options.retry_policy if options.HasField("retry_policy") else None,
        )

    async def activity(
        self,
        func: Callable[[RequestT], Awaitable[ResultT]],
        input_message: RequestT,
        *,
        activity_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
        result_type: type[ResultT] | None = None,
    ) -> ResultT:
        """Ergonomic shortcut over :meth:`execute_activity`. Defaults reduce
        the per-call boilerplate to the bare minimum a normal Python function
        call already requires (the function and its argument).

        Defaults applied unless overridden:

        - ``activity_id`` ← ``func.__qualname__`` (e.g. ``"Service.fetch_quote"``).
          Override when two activity callsites share the same function but
          should have distinct records (e.g. ``"fetch:aapl"`` vs ``"fetch:msft"``).
        - ``retry_policy`` ← :func:`default_retry_policy` (3 attempts, 1s
          initial, 2x backoff, 30s max interval, 30s durable threshold) —
          sensible for the framework's stated workloads (LLM / vendor /
          quant). Pass ``RetryPolicy()`` explicitly for single-attempt.
        - ``result_type`` ← inferred from ``func``'s return annotation
          (``Awaitable[X]`` ⇒ ``X``). Required only when the annotation is
          missing or not introspectable.

        Caveat: auto-inferred ``activity_id`` is stable across runs only if
        ``func.__qualname__`` is stable. Renaming the function invalidates
        stored activity records (treated as a new activity_id). Use the
        explicit ``activity_id`` override when refactor-stability matters
        more than terseness.
        """
        if activity_id is None:
            activity_id = _infer_activity_id(func)
        if retry_policy is None:
            retry_policy = default_retry_policy()
        if result_type is None:
            # _infer_result_type returns plain `type` (dynamic discovery via
            # typing.get_type_hints); cast back to `type[ResultT]` so static
            # checkers can see the parameterized result type.
            result_type = cast("type[ResultT]", _infer_result_type(func))
        options = ActivityOptions(activity_id=activity_id, retry_policy=retry_policy)
        return await self.execute_activity(options, input_message, result_type, func)

    async def wait_event(
        self,
        event_id: str,
        payload_factory: Callable[[], ResultT],
    ) -> ResultT:
        key = EventKey(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            event_id=event_id,
        )
        record = await self._store.get_event(key)
        if record is None:
            raise EventPendingError(event_id)
        payload = payload_factory()
        if not record.payload.Unpack(payload):
            raise WorkflowConflictError(
                "stored event payload type does not match requested payload"
            )
        return payload

    @property
    def store(self) -> Store:
        """The Store this workflow is replaying against. Exposed so adapter
        helpers (e.g. ``temporaless.dependencies.wait_for_workflow``) can read
        records without reaching into private state."""
        return self._store

    async def sleep(self, timer_id: str, duration: timedelta) -> None:
        if timer_id.startswith(ACTIVITY_RETRY_TIMER_ID_PREFIX):
            raise ValueError(
                f"timer_id {timer_id!r} uses the framework-reserved "
                f"{ACTIVITY_RETRY_TIMER_ID_PREFIX!r} prefix; choose another"
            )
        timer_kind = SLEEP_TIMER_KIND
        key = TimerKey(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            timer_id=timer_id,
        )
        now = datetime.now(UTC)

        record = await self._store.get_timer(key)
        if record is not None:
            if record.timer_kind != timer_kind:
                raise TimerConflictError(
                    f"timer kind changed from {record.timer_kind!r} to {timer_kind!r}"
                )
            if record.code_version != self._code_version:
                raise TimerConflictError(
                    f"code version changed from {record.code_version!r} to {self._code_version!r}"
                )
            stored_duration = record.duration.ToTimedelta()
            if stored_duration != duration:
                raise TimerConflictError(
                    f"timer duration changed from {stored_duration} to {duration}"
                )
            if record.status == temporaless_pb2.TIMER_STATUS_FIRED:
                return
            if record.status == temporaless_pb2.TIMER_STATUS_CANCELED:
                raise TimerConflictError("timer was canceled")
            fire_at = record.fire_at.ToDatetime().replace(tzinfo=UTC)
            if now < fire_at:
                raise TimerPendingError(timer_id, fire_at)
            record.status = temporaless_pb2.TIMER_STATUS_FIRED
            record.fired_at.GetCurrentTime()
            await self._store.put_timer(record)
            return

        fire_at = now + duration
        status = temporaless_pb2.TIMER_STATUS_SCHEDULED
        if now >= fire_at:
            status = temporaless_pb2.TIMER_STATUS_FIRED
        duration_message = Duration()
        duration_message.FromTimedelta(duration)
        fire_at_message = Timestamp()
        fire_at_message.FromDatetime(fire_at)
        created_at = Timestamp()
        created_at.FromDatetime(now)

        record = temporaless_pb2.TimerRecord(
            schema_version=TIMER_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            timer_kind=timer_kind,
            code_version=self._code_version,
            duration=duration_message,
            status=status,
            fire_at=fire_at_message,
            created_at=created_at,
        )
        if status == temporaless_pb2.TIMER_STATUS_FIRED:
            record.fired_at.GetCurrentTime()
        await self._store.put_timer(record)
        if status == temporaless_pb2.TIMER_STATUS_SCHEDULED:
            raise TimerPendingError(timer_id, fire_at)

    async def _put_activity_retry_timer(
        self,
        activity_id: str,
        duration: timedelta,
        fire_at: datetime,
    ) -> None:
        """Write (or overwrite) the TIMER_KIND_ACTIVITY_RETRY timer paired
        with an activity's durable retry. Stable per activity_id so later
        retries naturally overwrite earlier scheduled state."""
        key = TimerKey(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            timer_id=_activity_retry_timer_id(activity_id),
        )
        timer_kind = temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY
        duration_message = Duration()
        duration_message.FromTimedelta(duration)
        fire_at_message = Timestamp()
        fire_at_message.FromDatetime(fire_at)
        created_at = Timestamp()
        created_at.GetCurrentTime()
        record = temporaless_pb2.TimerRecord(
            schema_version=TIMER_RECORD_SCHEMA_VERSION,
            key=key.to_proto(),
            timer_kind=timer_kind,
            code_version=self._code_version,
            duration=duration_message,
            status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
            fire_at=fire_at_message,
            created_at=created_at,
        )
        await self._store.put_timer(record)

    async def _mark_activity_retry_timer_fired(self, activity_id: str) -> None:
        """Transition the paired retry timer to FIRED so the timer scanner
        stops returning it while the activity body executes the resumed
        attempt. No-op when the timer is absent (legacy path) or already
        FIRED."""
        key = TimerKey(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            timer_id=_activity_retry_timer_id(activity_id),
        )
        record = await self._store.get_timer(key)
        if record is None or record.status == temporaless_pb2.TIMER_STATUS_FIRED:
            return
        record.status = temporaless_pb2.TIMER_STATUS_FIRED
        record.fired_at.GetCurrentTime()
        await self._store.put_timer(record)


@dataclass(frozen=True)
class ActivityWrapOptions[RequestT: Message]:
    workflow: Workflow
    options: ActivityOptions | None = None
    options_for: Callable[[RequestT], ActivityOptions] | None = None


async def run(
    store: Store,
    options: Options,
    input_message: RequestT,
    result_factory: Callable[[], ResultT],
    execute: Callable[[Workflow, RequestT], Awaitable[ResultT]],
) -> ResultT:
    if not inspect.iscoroutinefunction(execute):
        raise ValueError("workflow executor must be async (define it with `async def`)")
    options = normalized_workflow_options(options)
    if options.claim_owner_id and not isinstance(store, ClaimStore):
        raise ValueError("claim store is required when claim owner is provided")
    if options.concurrency_key and not isinstance(store, ClaimStore):
        raise ValueError("claim store is required when concurrency_key is set")
    result_template = result_factory()
    workflow_type = message_pair_type("workflow", input_message, result_template)
    key = WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)

    raw_store = store
    claim_store = cast("ClaimStore", raw_store) if isinstance(raw_store, ClaimStore) else None

    def inspect_record(
        record: temporaless_pb2.WorkflowRecord | None,
    ) -> tuple[ResultT | None, Timestamp | None]:
        if record is None:
            return None, None
        if record.status in (
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            temporaless_pb2.WORKFLOW_STATUS_FAILED,
        ):
            return (
                _replay_workflow_record(
                    record, workflow_type, options.code_version, result_factory
                ),
                None,
            )
        if record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS:
            _assert_workflow_identity(record, workflow_type, options.code_version)
            created_at = Timestamp()
            created_at.CopyFrom(record.created_at)
            return None, created_at
        raise WorkflowConflictError("stored workflow has unknown status")

    # Read terminal state before acquiring a claim so completed/failed runs
    # remain pure replay, even if an old create-only claim was leaked.
    record = await raw_store.get_workflow(key)
    replayed, created_at = inspect_record(record)
    if replayed is not None:
        return replayed

    claim_capability: temporaless_pb2.ClaimCapability | None = None
    claim_option = ""
    if options.concurrency_key:
        claim_option = "concurrency_key"
    elif options.claim_owner_id:
        claim_option = "claim_owner_id"
    if claim_option:
        assert claim_store is not None
        claim_capability = await claim_store.claim_capability()
        if claim_capability not in (
            temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS,
            temporaless_pb2.CLAIM_CAPABILITY_CAS_CLAIMS,
        ):
            raise ClaimCapabilityError(claim_capability, claim_option)

    workflow_claim_key: ClaimKey | None = None
    workflow_claim_acquired = False
    acquired_slot_id: str | None = None
    try:
        # A caller-provided claim_owner_id opts this run into storage-backed
        # single-flight execution. Any existing workflow claim is busy,
        # including one with the same owner: concurrent requests often reuse a
        # worker identity and still must not enter the body together.
        if options.claim_owner_id:
            if claim_store is None:
                raise ValueError("claim store is required when claim owner is provided")
            workflow_claim_key = ClaimKey(
                workflow_id=options.workflow_id,
                run_id=options.run_id,
                claim_id=WORKFLOW_EXECUTION_CLAIM_ID,
            )
            for attempt in range(2):
                now = datetime.now(UTC)
                claim_created_at = Timestamp()
                claim_created_at.FromDatetime(now)
                claim_expires_at = Timestamp()
                claim_expires_at.FromDatetime(now + DEFAULT_CLAIM_LEASE_DURATION)
                claim_heartbeat_at = Timestamp()
                claim_heartbeat_at.FromDatetime(now)
                workflow_claim = temporaless_pb2.ClaimRecord(
                    schema_version=CLAIM_RECORD_SCHEMA_VERSION,
                    key=workflow_claim_key.to_proto(),
                    owner_id=options.claim_owner_id,
                    resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
                    resource_id=options.workflow_id,
                    code_version=options.code_version,
                    lease_expires_at=claim_expires_at,
                    created_at=claim_created_at,
                    heartbeat_at=claim_heartbeat_at,
                )
                create_task = asyncio.create_task(claim_store.try_create_claim(workflow_claim))
                created, acquisition_cancelled = await _await_task_to_completion(create_task)
                if acquisition_cancelled is not None:
                    # Record ownership before cancellation reaches the outer
                    # finally so a successful conditional create is released.
                    workflow_claim_acquired = created
                    raise acquisition_cancelled
                if created:
                    workflow_claim_acquired = True
                    break

                # The winner may have completed between our initial read and
                # failed claim create. Always re-read the raw store here; a
                # run-scoped negative cache would be stale by construction.
                fresh = await raw_store.get_workflow(key)
                fresh_replay, _ = inspect_record(fresh)
                if fresh_replay is not None:
                    return fresh_replay

                existing = await claim_store.get_claim(workflow_claim_key)
                # A normal release can race our failed create. Retry once when
                # the claim disappeared; otherwise return the current holder.
                if existing is not None or attempt == 1:
                    lease_expires_at = None
                    owner_id = None
                    if existing is not None:
                        owner_id = existing.owner_id
                        if existing.HasField("lease_expires_at"):
                            lease_expires_at = existing.lease_expires_at.ToDatetime().replace(
                                tzinfo=UTC
                            )
                    raise ClaimBusyError(
                        workflow_claim_key.claim_id,
                        owner_id,
                        lease_expires_at,
                        claim_capability,
                    )

            if not workflow_claim_acquired:
                raise RuntimeError("failed to acquire workflow execution claim")

            # State can change between the initial read and acquisition (for
            # example a prior holder completed and released). Refresh before
            # constructing a cache or entering the workflow body.
            record = await raw_store.get_workflow(key)
            replayed, created_at = inspect_record(record)
            if replayed is not None:
                return replayed

        # Substitute the user-provided store with a fresh run-scoped cache
        # only after claim arbitration. Writes remain write-through.
        cache = RunScopedCache(raw_store, key)
        store = cast("Store", cache)
        if created_at is not None:
            # Replay: prefetch activities / timers / events in parallel so the
            # body's subsequent get-by-key calls hit memory.
            await cache.prefetch()

        # The per-run claim is acquired before the cluster-wide concurrency
        # slot so a rejected duplicate does not consume global capacity.
        if options.concurrency_key and options.concurrency_limit > 0:
            if claim_store is None:
                raise ValueError("claim store is required when concurrency_key is set")
            slot_task = asyncio.create_task(
                _acquire_concurrency_slot(
                    claim_store,
                    DEFAULT_NAMESPACE,
                    options.concurrency_key,
                    options.concurrency_limit,
                    options.claim_owner_id,
                    options.code_version,
                    DEFAULT_CLAIM_LEASE_DURATION,
                )
            )
            acquired_slot_id, acquisition_cancelled = await _await_task_to_completion(slot_task)
            if acquisition_cancelled is not None:
                raise acquisition_cancelled
            if acquired_slot_id is None:
                raise ConcurrencyBusyError(options.concurrency_key, options.concurrency_limit)

        input_any = Any()
        input_any.Pack(input_message)

        if created_at is None:
            created_at = Timestamp()
            created_at.GetCurrentTime()
            in_progress = temporaless_pb2.WorkflowRecord(
                schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
                key=key.to_proto(),
                workflow_type=workflow_type,
                code_version=options.code_version,
                input=input_any,
                status=temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
                created_at=created_at,
            )
            await store.put_workflow(in_progress)

        workflow = Workflow(
            store=store,
            options=options,
            claim_capability=claim_capability,
        )
        workflow_annotations = _AnnotationsBag()
        annotations_token = _annotations_var.set(workflow_annotations)
        workflow_token = _workflow_var.set(workflow)
        try:
            try:
                result = await execute(workflow, input_message)
            except (
                TimerPendingError,
                ClaimBusyError,
                ClaimReleaseError,
                EventPendingError,
                WorkflowDependencyPendingError,
            ):
                raise
            except asyncio.CancelledError:
                # Cancellation is shutdown, not workflow failure. Leave the
                # record IN_PROGRESS so another invocation can resume.
                raise
            except BaseException as exc:
                completed_at = Timestamp()
                completed_at.GetCurrentTime()
                failed = temporaless_pb2.WorkflowRecord(
                    schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
                    key=key.to_proto(),
                    workflow_type=workflow_type,
                    code_version=options.code_version,
                    input=input_any,
                    status=temporaless_pb2.WORKFLOW_STATUS_FAILED,
                    failure=_failure_from_exception(exc),
                    created_at=created_at,
                    completed_at=completed_at,
                    annotations=workflow_annotations.snapshot(),
                )
                await store.put_workflow(failed)
                raise

            result_any = Any()
            result_any.Pack(result)
            completed_at = Timestamp()
            completed_at.GetCurrentTime()
            completed = temporaless_pb2.WorkflowRecord(
                schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
                key=key.to_proto(),
                workflow_type=workflow_type,
                code_version=options.code_version,
                input=input_any,
                status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
                result=result_any,
                created_at=created_at,
                completed_at=completed_at,
                annotations=workflow_annotations.snapshot(),
            )
            await store.put_workflow(completed)
            return result
        finally:
            _workflow_var.reset(workflow_token)
            _annotations_var.reset(annotations_token)
    finally:
        held_workflow_claim = workflow_claim_key if workflow_claim_acquired else None
        if claim_store is not None and (
            acquired_slot_id is not None or held_workflow_claim is not None
        ):
            # One explicit child performs slot-first/run-claim-second cleanup.
            # Repeated parent cancellation is recorded but cannot abandon it;
            # cleanup errors surface because create-only leaks block resumption.
            cleanup_task = asyncio.create_task(
                _release_invocation_claims(
                    claim_store,
                    options.concurrency_key,
                    acquired_slot_id,
                    held_workflow_claim,
                )
            )
            _, cleanup_cancelled = await _await_task_to_completion(cleanup_task)
            if cleanup_cancelled is not None:
                raise cleanup_cancelled


def wrap_workflow(
    options: WorkflowWrapOptions[RequestT],
    result_factory: Callable[[], ResultT],
) -> Callable[[Callable[[RequestT], Awaitable[ResultT]]], Callable[[RequestT], Awaitable[ResultT]]]:
    def decorate(
        execute: Callable[[RequestT], Awaitable[ResultT]],
    ) -> Callable[[RequestT], Awaitable[ResultT]]:
        if not inspect.iscoroutinefunction(execute):
            raise ValueError("workflow executor must be async (define it with `async def`)")

        @wraps(execute)
        async def wrapped(input_message: RequestT) -> ResultT:
            if options.options is not None and options.options_for is not None:
                raise ValueError(
                    "workflow wrap options are ambiguous: set options OR options_for, not both"
                )
            if options.options_for is not None:
                run_options = options.options_for(input_message)
            elif options.options is not None:
                run_options = options.options
            else:
                raise ValueError("workflow run options are required")

            async def adapter(_workflow: Workflow, request: RequestT) -> ResultT:
                return await execute(request)

            return await run(
                options.store,
                run_options,
                input_message,
                result_factory,
                adapter,
            )

        return wrapped

    return decorate


def wrap_activity(
    options: ActivityWrapOptions[RequestT],
    result_factory: Callable[[], ResultT],
) -> Callable[[Callable[[RequestT], Awaitable[ResultT]]], Callable[[RequestT], Awaitable[ResultT]]]:
    def decorate(
        execute: Callable[[RequestT], Awaitable[ResultT]],
    ) -> Callable[[RequestT], Awaitable[ResultT]]:
        if not inspect.iscoroutinefunction(execute):
            raise ValueError("activity executor must be async (define it with `async def`)")

        @wraps(execute)
        async def wrapped(input_message: RequestT) -> ResultT:
            if options.options is not None and options.options_for is not None:
                raise ValueError(
                    "activity wrap options are ambiguous: set options OR options_for, not both"
                )
            if options.options_for is not None:
                activity_options = options.options_for(input_message)
            elif options.options is not None:
                activity_options = options.options
            else:
                raise ValueError("activity options are required")
            if activity_options is None:
                raise ValueError("activity options are required")
            return await options.workflow.execute_activity(
                activity_options,
                input_message,
                result_factory,
                execute,
            )

        return wrapped

    return decorate


def normalized_workflow_options(options: Options) -> Options:
    if options is None:
        raise ValueError("workflow options are required")
    normalized = Options()
    normalized.CopyFrom(options)
    if not normalized.code_version:
        normalized.code_version = os.environ.get("TEMPORALESS_CODE_VERSION") or "dev"
    validate(normalized)
    return normalized


def message_pair_type(kind: str, input_message: Message, output_message: Message) -> str:
    return f"{kind}:{input_message.DESCRIPTOR.full_name}->{output_message.DESCRIPTOR.full_name}"


def _assert_activity_identity(
    record: temporaless_pb2.ActivityRecord,
    activity_type: str,
    code_version: str,
) -> None:
    """Guard against shape changes that would make the stored record
    incompatible with the current code path: a swapped request/response
    message type (which changes ``activity_type``) or a bumped
    ``code_version``. The ``activity_id`` itself is the de-duplication key;
    same id + same shape + same code_version is treated as the same logical
    activity regardless of the input bytes — the caller chose the id and
    owns its semantics.
    """
    if record.activity_type != activity_type:
        raise ActivityConflictError(
            f"activity type changed from {record.activity_type!r} to {activity_type!r}"
        )
    if record.code_version != code_version:
        raise ActivityConflictError(
            f"code version changed from {record.code_version!r} to {code_version!r}"
        )


def _replay_record(
    record: temporaless_pb2.ActivityRecord,
    activity_type: str,
    code_version: str,
    result_factory: Callable[[], ResultT],
) -> ResultT:
    _assert_activity_identity(record, activity_type, code_version)

    if record.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED:
        result = result_factory()
        if not record.result.Unpack(result):
            raise ActivityConflictError(
                "stored activity result type does not match requested result"
            )
        return result
    if record.status == temporaless_pb2.ACTIVITY_STATUS_FAILED:
        raise ActivityError(record.failure.code, record.failure.message)
    raise ActivityConflictError("stored activity has unknown status")


@dataclass(frozen=True)
class _RetryPlan:
    maximum_attempts: int
    initial_interval: timedelta
    backoff_coefficient: float
    maximum_interval: timedelta
    durable_threshold: timedelta
    non_retryable_codes: frozenset[str]


def _plan_retries(policy: RetryPolicy | None) -> _RetryPlan:
    if policy is None:
        return _RetryPlan(
            maximum_attempts=1,
            initial_interval=timedelta(0),
            backoff_coefficient=1.0,
            maximum_interval=timedelta(0),
            durable_threshold=timedelta(0),
            non_retryable_codes=frozenset(),
        )
    maximum_attempts = policy.maximum_attempts
    if maximum_attempts == 0:
        raise ValueError("retry policy maximum_attempts must be > 0")
    initial_interval = policy.initial_interval.ToTimedelta()
    if maximum_attempts > 1 and initial_interval <= timedelta(0):
        raise ValueError("retry policy initial_interval must be > 0 when maximum_attempts > 1")
    backoff_coefficient = policy.backoff_coefficient or 1.0
    durable_threshold = policy.durable_backoff_threshold.ToTimedelta()
    if durable_threshold < timedelta(0):
        raise ValueError("retry policy durable_backoff_threshold must be >= 0")
    return _RetryPlan(
        maximum_attempts=maximum_attempts,
        initial_interval=initial_interval,
        backoff_coefficient=backoff_coefficient,
        maximum_interval=policy.maximum_interval.ToTimedelta(),
        durable_threshold=durable_threshold,
        non_retryable_codes=frozenset(policy.non_retryable_error_codes),
    )


def _next_interval(prev: timedelta, plan: _RetryPlan) -> timedelta:
    next_interval = timedelta(seconds=prev.total_seconds() * plan.backoff_coefficient)
    if plan.maximum_interval > timedelta(0) and next_interval > plan.maximum_interval:
        return plan.maximum_interval
    return next_interval


def _failure_from_exception(exc: BaseException) -> temporaless_pb2.ActivityFailure:
    if isinstance(exc, ActivityError):
        failure = temporaless_pb2.ActivityFailure(code=exc.code, message=exc.message)
        if exc.retry_after is not None and exc.retry_after > timedelta(0):
            failure.retry_after.FromTimedelta(exc.retry_after)
        return failure
    return temporaless_pb2.ActivityFailure(message=str(exc))


def _replay_workflow_record(
    record: temporaless_pb2.WorkflowRecord,
    workflow_type: str,
    code_version: str,
    result_factory: Callable[[], ResultT],
) -> ResultT:
    _assert_workflow_identity(record, workflow_type, code_version)
    if record.status == temporaless_pb2.WORKFLOW_STATUS_COMPLETED:
        result = result_factory()
        if not record.result.Unpack(result):
            raise WorkflowConflictError(
                "stored workflow result type does not match requested result"
            )
        return result
    if record.status == temporaless_pb2.WORKFLOW_STATUS_FAILED:
        raise ActivityError(record.failure.code, record.failure.message)
    raise WorkflowConflictError("stored workflow has unknown status")


def _assert_workflow_identity(
    record: temporaless_pb2.WorkflowRecord,
    workflow_type: str,
    code_version: str,
) -> None:
    """See :func:`_assert_activity_identity` for the de-duplication contract."""
    if record.workflow_type != workflow_type:
        raise WorkflowConflictError(
            f"workflow type changed from {record.workflow_type!r} to {workflow_type!r}"
        )
    if record.code_version != code_version:
        raise WorkflowConflictError(
            f"code version changed from {record.code_version!r} to {code_version!r}"
        )


def wrap_workflow_method[RequestT: Message, ResultT: Message](
    *,
    store: Callable[[object], Store],
    result_type: type[ResultT],
    options_for: Callable[[object, RequestT], Options],
) -> Callable[
    [Callable[..., Awaitable[ResultT]]],
    Callable[..., Awaitable[ResultT]],
]:
    """Decorate a ConnectRPC unary method as a Temporaless workflow.

    The wrapped method has the standard ConnectRPC handler shape
    ``async def m(self, request, ctx) -> Response``. Each invocation routes
    through ``workflow.run``: the body becomes the workflow body, replays
    short-circuit via stored records, and the in-flight ``Workflow`` is
    available inside the body via :func:`current_workflow`.

    Args:
        store: extracts the ``Store`` from ``self`` (e.g. ``lambda s: s._store``).
        result_type: the protobuf response class (callable as a no-arg factory).
        options_for: builds ``Options`` from ``(self, request)``.

    Example:

        class PriceService:
            def __init__(self, store: Store) -> None:
                self._store = store

            @wrap_workflow_method(
                store=lambda s: s._store,
                result_type=FetchResponse,
                options_for=lambda s, r: Options(
                    workflow_id=f"prices:{r.symbol}",
                    run_id=r.run_id,
                    code_version="v1",
                ),
            )
            async def fetch_prices(self, request: FetchRequest, ctx) -> FetchResponse:
                # Normal gRPC body. Calls below replay from storage on retry.
                price = await current_workflow().execute_activity(
                    ActivityOptions(activity_id=f"vendor:{request.symbol}"),
                    request,
                    FetchResponse,
                    _vendor_fetch,
                )
                return price

    Mount the service on a ConnectRPC ASGI app as usual; every invocation now
    has workflow replay semantics without changing the gRPC interface.

    Error mapping is applied automatically: framework typed errors (timer
    pending, event pending, claim busy, conflicts, activity errors) are
    re-raised as ``ConnectError`` with the appropriate code so Connect clients
    see correct gRPC status codes. Unknown exceptions propagate unchanged.
    """

    def decorator(
        method: Callable[..., Awaitable[ResultT]],
    ) -> Callable[..., Awaitable[ResultT]]:
        if not inspect.iscoroutinefunction(method):
            raise ValueError("workflow method must be async (define it with `async def`)")

        @wraps(method)
        async def wrapped(self_: object, request: RequestT, ctx: object = None) -> ResultT:
            store_instance = store(self_)
            opts = options_for(self_, request)

            async def body(_workflow: Workflow, req: RequestT) -> ResultT:
                return await method(self_, req, ctx)

            try:
                return await run(store_instance, opts, request, result_type, body)
            except Exception as exc:
                mapping = workflow_error_to_connect_code(exc)
                if mapping is None:
                    raise
                from connectrpc.errors import ConnectError

                code, message = mapping
                raise ConnectError(code, message) from exc  # ty: ignore[invalid-argument-type]

        return wrapped

    return decorator
