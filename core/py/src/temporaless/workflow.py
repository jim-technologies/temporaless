from __future__ import annotations

import asyncio
import contextvars
import inspect
import logging
import math
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import TypeVar, cast

from google.protobuf.any_pb2 import Any
from google.protobuf.duration_pb2 import Duration
from google.protobuf.message import DecodeError, Message
from google.protobuf.timestamp_pb2 import Timestamp
from protovalidate import ValidationError, validate

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
    RunRecordValidationError,
    Store,
    TimerKey,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2

_LOGGER = logging.getLogger(__name__)

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


class _ResultTypeError(TypeError):
    """A workflow/activity body violated its declared protobuf response type."""


class WorkflowConflictError(RuntimeError):
    pass


class ConcurrencyBusyError(RuntimeError):
    """Raised when a workflow's ``concurrency_key`` slot pool is full.

    The workflow body did NOT execute and no IN_PROGRESS record was written —
    callers retry the same ``workflow.run`` when capacity is available. Maps
    to a transport-specific retry signal by an adapter.
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


class WorkflowInfrastructureError(RuntimeError):
    """A retryable framework storage/coordination failure inside a workflow.

    ``run`` leaves the parent record ``IN_PROGRESS`` when this escapes a
    Temporaless primitive. Activity bodies remain application boundaries: an
    activity that returns this exception is recorded and retried like any
    other business failure.
    """

    def __init__(self, operation: str, cause: BaseException) -> None:
        super().__init__(f"workflow infrastructure operation {operation!r} failed: {cause}")
        self.operation = operation
        self.__cause__ = cause


async def _await_workflow_infrastructure(
    operation: str,
    awaitable: Awaitable[TaskResultT],
) -> TaskResultT:
    try:
        return await awaitable
    except DecodeError, RunRecordValidationError, ValidationError:
        # Corrupt records and invalid framework writes are durable invariant
        # violations, not transient outages. The caller's conflict/terminal
        # path must expose them rather than retrying forever.
        raise
    except WorkflowInfrastructureError:
        raise
    except Exception as exc:
        raise WorkflowInfrastructureError(operation, exc) from exc


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

    Use inside a workflow body that does not receive a ``Workflow`` argument:
    the body can reach it through this context-local accessor to call
    ``execute_activity``, ``sleep``, ``wait_event``, and related primitives.

    Raises ``RuntimeError`` if called outside a workflow body — that's a
    programming error and should fail fast.
    """
    workflow = _workflow_var.get()
    if workflow is None:
        raise RuntimeError(
            "current_workflow() called outside a workflow body — wrap your handler "
            "with a workflow adapter, or use temporaless.workflow.run."
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
        # A due sleep has been observed by this invocation, but its wakeup is
        # not consumed until the workflow reaches another durable wakeup or a
        # terminal workflow record. Keeping the timer scheduled across the
        # ambiguous body window makes a crash redeliverable.
        self._due_sleep_timer_ids: set[str] = set()

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
        retry_timer_id: str = "",
    ) -> ResultT:
        # Normalize and validate policy semantics first so malformed policies
        # are reported directly instead of being masked by the conditional
        # durable retry_timer_id requirement.
        plan = _plan_retries(retry_policy)
        activity_options = ActivityOptions(
            activity_id=activity_id,
            retry_timer_id=retry_timer_id,
        )
        if retry_policy is not None:
            activity_options.retry_policy.CopyFrom(retry_policy)
        validate(activity_options)
        if not activity_type:
            raise ValueError("activity type is required")
        result_template = result_factory()
        _validate_result_template("activity", result_template)

        key = ActivityKey(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            activity_id=activity_id,
        )

        def inspect_record(
            stored: temporaless_pb2.ActivityRecord | None,
        ) -> tuple[
            temporaless_pb2.ActivityRecord | None,
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
                return stored, [], {}, None
            if stored.status != temporaless_pb2.ACTIVITY_STATUS_RETRYING:
                raise ActivityConflictError("stored activity has unknown status")
            _assert_activity_identity(stored, activity_type, self._code_version)
            _assert_activity_retry_policy(stored, plan.record_policy)
            if stored.retry_timer_id != retry_timer_id:
                raise ActivityConflictError(
                    "activity retry timer ID changed while activity is RETRYING"
                )
            attempts = _validate_retrying_activity(stored, plan)
            wake_at = None
            if stored.HasField("next_attempt_at"):
                try:
                    wake_at = stored.next_attempt_at.ToDatetime().replace(tzinfo=UTC)
                except (OverflowError, ValueError) as exc:
                    raise ActivityConflictError(
                        "stored RETRYING activity has invalid next_attempt_at"
                    ) from exc
            return None, attempts, dict(stored.annotations), wake_at

        async def reconcile_terminal_retry_timer(
            stored: temporaless_pb2.ActivityRecord,
        ) -> None:
            # A process can die after persisting the terminal activity record
            # but before consuming the durable retry timer. Heal that partial
            # boundary before returning the authoritative outcome.
            # One-attempt activities cannot have an outstanding retry timer,
            # so avoid an unnecessary point read on their hot replay path.
            if stored.retry_timer_id:
                try:
                    await self._mark_activity_retry_timer_fired(
                        stored.key.activity_id,
                        stored.retry_timer_id,
                    )
                except Exception:
                    # The ActivityRecord is already authoritative. Timer cleanup
                    # must not replace a completed result or terminal failure;
                    # its scheduled ledger remains a safe retry path, terminal
                    # replay tries again, and DueTimers prunes it after the
                    # parent workflow becomes terminal.
                    _LOGGER.warning(
                        "failed to reconcile retry timer for terminal activity %s/%s/%s",
                        self._workflow_id,
                        self._run_id,
                        activity_id,
                        exc_info=True,
                    )

        async def ensure_retry_timer(
            stored: temporaless_pb2.ActivityRecord | None,
            stored_attempts: list[temporaless_pb2.ActivityAttempt],
            wake_at: datetime | None,
            *,
            authoritative: bool = False,
        ) -> datetime | None:
            """Repair or honor the non-transactional timer/retry boundary.

            Durable retries publish their timer first. A missing/older timer
            means an older record-first writer or failed overwrite and is
            repaired. A later scheduled timer means a timer-first writer died
            before advancing ActivityRecord; it is honored without regression.
            """
            if plan.durable_threshold <= timedelta(0) or not retry_timer_id:
                return wake_at

            timer_key = TimerKey(
                workflow_id=self._workflow_id,
                run_id=self._run_id,
                timer_id=retry_timer_id,
            )
            if authoritative and isinstance(self._store, RunScopedCache):
                timer = await _await_workflow_infrastructure(
                    "refresh activity retry timer",
                    self._store.refresh_timer(timer_key),
                )
            else:
                timer = await _await_workflow_infrastructure(
                    "read activity retry timer",
                    self._store.get_timer(timer_key),
                )
            timer_fire_at: datetime | None = None
            timer_duration: timedelta | None = None
            if timer is not None:
                if timer.key != timer_key.to_proto():
                    raise ActivityConflictError(
                        "stored activity retry timer key does not match its storage key"
                    )
                if timer.timer_kind != temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY:
                    raise ActivityConflictError(
                        "stored activity retry timer collides with another timer kind"
                    )
                if timer.retry_activity_id != activity_id:
                    raise ActivityConflictError(
                        "stored activity retry timer belongs to another activity"
                    )
                if timer.code_version != self._code_version:
                    raise ActivityConflictError("stored activity retry timer code version changed")
                if timer.schema_version != TIMER_RECORD_SCHEMA_VERSION:
                    raise ActivityConflictError(
                        "stored activity retry timer schema version changed"
                    )
                if not timer.HasField("fire_at"):
                    raise ActivityConflictError("stored activity retry timer has no fire_at")
                try:
                    timer_fire_at = _validated_proto_timestamp(
                        timer.fire_at, "persisted activity retry timer fire_at"
                    )
                except (OverflowError, ValueError) as exc:
                    raise ActivityConflictError(
                        "stored activity retry timer has invalid fire_at"
                    ) from exc
                if not timer.HasField("duration"):
                    raise ActivityConflictError("stored activity retry timer has no duration")
                try:
                    timer_duration = _validated_proto_duration(
                        timer.duration, "persisted activity retry timer duration"
                    )
                except (OverflowError, ValueError) as exc:
                    raise ActivityConflictError(
                        "stored activity retry timer has invalid duration"
                    ) from exc
                if timer_duration < timedelta(0):
                    raise ActivityConflictError("stored activity retry timer has negative duration")
                if not timer.HasField("created_at"):
                    raise ActivityConflictError("stored activity retry timer has no created_at")
                try:
                    _validated_proto_timestamp(
                        timer.created_at, "persisted activity retry timer created_at"
                    )
                except (OverflowError, ValueError) as exc:
                    raise ActivityConflictError(
                        "stored activity retry timer has invalid created_at"
                    ) from exc
                if timer.status == temporaless_pb2.TIMER_STATUS_CANCELED:
                    raise ActivityConflictError("stored activity retry timer was canceled")
                if timer.status not in (
                    temporaless_pb2.TIMER_STATUS_SCHEDULED,
                    temporaless_pb2.TIMER_STATUS_FIRED,
                ):
                    raise ActivityConflictError("stored activity retry timer has unknown status")
                if timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED:
                    if timer.HasField("fired_at"):
                        raise ActivityConflictError(
                            "stored SCHEDULED activity retry timer has fired_at"
                        )
                else:
                    if not timer.HasField("fired_at"):
                        raise ActivityConflictError(
                            "stored FIRED activity retry timer has no fired_at"
                        )
                    try:
                        _validated_proto_timestamp(
                            timer.fired_at, "persisted activity retry timer fired_at"
                        )
                    except (OverflowError, ValueError) as exc:
                        raise ActivityConflictError(
                            "stored FIRED activity retry timer has invalid fired_at"
                        ) from exc

            if stored is None or wake_at is None:
                # Timer-first publication can survive while its ActivityRecord
                # write is missing or still reflects an earlier in-process
                # retry. Respect the caller-owned timer's future wake before
                # repeating the ambiguous attempt. Once due, at-least-once
                # execution resumes and the scheduled timer remains a wake
                # until a later durable boundary.
                if timer is None:
                    return wake_at
                assert timer_fire_at is not None
                assert timer_duration is not None
                prepared_after_attempt = 1
                if stored is not None:
                    prepared_after_attempt = len(stored_attempts) + 1
                if prepared_after_attempt >= plan.maximum_attempts:
                    raise ActivityConflictError(
                        "stored prepared retry timer would follow the terminal attempt"
                    )
                minimum_interval = _retry_interval_after_failed_attempt(
                    plan, prepared_after_attempt
                )
                if timer_duration < minimum_interval or timer_duration < plan.durable_threshold:
                    raise ActivityConflictError(
                        "stored prepared retry timer duration is below its required retry interval"
                    )
                if timer.status == temporaless_pb2.TIMER_STATUS_FIRED:
                    await self._put_activity_retry_timer(
                        activity_id,
                        retry_timer_id,
                        timer_duration,
                        timer_fire_at,
                    )
                await self._reconcile_due_sleep_timers()
                return timer_fire_at

            retry_interval = _retry_interval_after_failed_attempt(plan, len(stored_attempts))
            retry_after = stored_attempts[-1].failure.retry_after
            if stored_attempts[-1].failure.HasField("retry_after"):
                persisted_retry_after = _validated_proto_duration(
                    retry_after, "persisted retry_after"
                )
                if persisted_retry_after < timedelta(0):
                    raise ActivityConflictError("stored RETRYING activity has negative retry_after")
                retry_interval = max(retry_interval, persisted_retry_after)

            if timer is None:
                await self._put_activity_retry_timer(
                    activity_id, retry_timer_id, retry_interval, wake_at
                )
                await self._reconcile_due_sleep_timers()
                return wake_at

            assert timer_fire_at is not None
            assert timer_duration is not None

            if timer_fire_at == wake_at and timer_duration != retry_interval:
                raise ActivityConflictError(
                    "stored activity retry timer duration does not match its retry policy"
                )
            if timer.status == temporaless_pb2.TIMER_STATUS_SCHEDULED and timer_fire_at == wake_at:
                # The verified retry timer is a durable successor for any
                # ordinary sleep crossed earlier in this invocation.
                await self._reconcile_due_sleep_timers()
                return wake_at
            if timer_fire_at > wake_at:
                # A timer-first writer published a later retry but died before
                # advancing ActivityRecord. Wait for that wake, then retry from
                # the older authoritative attempt history at-least-once.
                prepared_after_attempt = len(stored_attempts) + 1
                if prepared_after_attempt >= plan.maximum_attempts:
                    raise ActivityConflictError(
                        "stored newer retry timer would follow the terminal attempt"
                    )
                minimum_interval = _retry_interval_after_failed_attempt(
                    plan, prepared_after_attempt
                )
                if timer_duration < minimum_interval or timer_duration < plan.durable_threshold:
                    raise ActivityConflictError(
                        "stored newer retry timer duration is below its required retry interval"
                    )
                if timer.status == temporaless_pb2.TIMER_STATUS_FIRED:
                    await self._put_activity_retry_timer(
                        activity_id,
                        retry_timer_id,
                        timer_duration,
                        timer_fire_at,
                    )
                await self._reconcile_due_sleep_timers()
                return timer_fire_at

            # Missing the latest overwrite is the expected partial-write case;
            # a same/older FIRED timer is likewise safe to restore because the
            # ActivityRecord is still explicitly RETRYING.
            await self._put_activity_retry_timer(
                activity_id, retry_timer_id, retry_interval, wake_at
            )
            await self._reconcile_due_sleep_timers()
            return wake_at

        async def replay_terminal(stored: temporaless_pb2.ActivityRecord) -> ResultT:
            await reconcile_terminal_retry_timer(stored)
            return _replay_record(stored, activity_type, self._code_version, result_template)

        record = await _await_workflow_infrastructure(
            "read activity",
            self._store.get_activity(key),
        )
        terminal, attempts, seeded_annotations, retry_wake_at = inspect_record(record)
        if terminal is not None:
            return await replay_terminal(terminal)
        retry_wake_at = await ensure_retry_timer(record, attempts, retry_wake_at)
        if retry_wake_at is not None and datetime.now(UTC) < retry_wake_at:
            raise TimerPendingError(retry_timer_id, retry_wake_at)

        activity_claim_key: ClaimKey | None = None
        activity_claim_acquired = False
        release_activity_claim = False
        try:
            if self._claim_owner is not None:
                if self._claim_store is None:
                    raise ValueError("claim store is required when claim owner is provided")
                if self._claim_capability is None:
                    self._claim_capability = await _await_workflow_infrastructure(
                        "read activity claim capability",
                        self._claim_store.claim_capability(),
                    )
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
                    try:
                        created, acquisition_cancelled = await _await_task_to_completion(
                            create_task
                        )
                    except RunRecordValidationError, ValidationError:
                        raise
                    except Exception as exc:
                        raise WorkflowInfrastructureError("create activity claim", exc) from exc
                    if acquisition_cancelled is not None:
                        activity_claim_acquired = created
                        # The conditional create has completed and told us
                        # exactly whether this invocation owns the claim. The
                        # activity body has not started, so a successfully
                        # acquired claim is safe—and necessary—to release. A
                        # cancellation here must not permanently strand a
                        # create-only claim whose lease cannot be taken over.
                        release_activity_claim = created
                        raise acquisition_cancelled
                    if created:
                        activity_claim_acquired = True
                        break

                    # A terminal activity may have appeared after our cached
                    # negative read and failed conditional create. Bypass the
                    # normal replay cache and update it with authoritative state.
                    if isinstance(self._store, RunScopedCache):
                        fresh = await _await_workflow_infrastructure(
                            "refresh activity after claim race",
                            self._store.refresh_activity(key),
                        )
                    else:
                        fresh = await _await_workflow_infrastructure(
                            "refresh activity after claim race",
                            self._store.get_activity(key),
                        )
                    if fresh is not None and fresh.status in (
                        temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
                        temporaless_pb2.ACTIVITY_STATUS_FAILED,
                    ):
                        return await replay_terminal(fresh)
                    if (
                        fresh is not None
                        and fresh.status != temporaless_pb2.ACTIVITY_STATUS_RETRYING
                    ):
                        raise ActivityConflictError("stored activity has unknown status")

                    existing = await _await_workflow_infrastructure(
                        "read competing activity claim",
                        self._claim_store.get_claim(activity_claim_key),
                    )
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
                    record = await _await_workflow_infrastructure(
                        "refresh activity after claim acquisition",
                        self._store.refresh_activity(key),
                    )
                else:
                    record = await _await_workflow_infrastructure(
                        "refresh activity after claim acquisition",
                        self._store.get_activity(key),
                    )
                terminal, attempts, seeded_annotations, retry_wake_at = inspect_record(record)
                if terminal is not None:
                    return await replay_terminal(terminal)
                retry_wake_at = await ensure_retry_timer(
                    record,
                    attempts,
                    retry_wake_at,
                    authoritative=True,
                )
                if retry_wake_at is not None and datetime.now(UTC) < retry_wake_at:
                    raise TimerPendingError(retry_timer_id, retry_wake_at)

            input_any = Any()
            input_any.Pack(input_message)

            policy_interval = _retry_interval_after_attempts(plan, attempts)
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
                        _validate_result_message("activity", result, result_template)
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

                        retry_interval = policy_interval
                        if failure.HasField("retry_after"):
                            retry_after = failure.retry_after.ToTimedelta()
                            if retry_after > retry_interval:
                                retry_interval = retry_after

                        non_retryable = (
                            isinstance(run_err, _ResultTypeError)
                            or failure.code in plan.non_retryable_codes
                        )
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
                                retry_timer_id=retry_timer_id,
                            )
                            failed_record.created_at.FromDatetime(
                                attempts[0].started_at.ToDatetime()
                            )
                            failed_record.completed_at.FromDatetime(completed_at)
                            if plan.record_policy is not None:
                                failed_record.retry_policy.CopyFrom(plan.record_policy)
                            await _await_workflow_infrastructure(
                                "persist failed activity",
                                self._store.put_activity(failed_record),
                            )
                            release_activity_claim = True
                            await reconcile_terminal_retry_timer(failed_record)
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
                            retry_timer_id=retry_timer_id,
                        )
                        retrying_record.created_at.FromDatetime(attempts[0].started_at.ToDatetime())
                        if plan.record_policy is not None:
                            retrying_record.retry_policy.CopyFrom(plan.record_policy)

                        if (
                            plan.durable_threshold > timedelta(0)
                            and retry_interval >= plan.durable_threshold
                        ):
                            next_attempt_at = datetime.now(UTC) + retry_interval
                            next_at_ts = Timestamp()
                            next_at_ts.FromDatetime(next_attempt_at)
                            retrying_record.next_attempt_at.CopyFrom(next_at_ts)
                            pending = TimerPendingError(retry_timer_id, next_attempt_at)
                            try:
                                # Publish the wake before replay state. A crash
                                # between these writes may repeat a known-failed
                                # attempt, but cannot strand the workflow.
                                timer_task = asyncio.create_task(
                                    self._put_activity_retry_timer(
                                        activity_id,
                                        retry_timer_id,
                                        retry_interval,
                                        next_attempt_at,
                                    )
                                )
                                _, timer_cancelled = await _await_task_to_completion(timer_task)
                                # From this point the failed activity has a
                                # durable/redeliverable successor. Release its
                                # claim even if cancellation or a later write
                                # interrupts this boundary.
                                release_activity_claim = True
                                if timer_cancelled is not None:
                                    raise timer_cancelled
                                await self._reconcile_due_sleep_timers()
                                await _await_workflow_infrastructure(
                                    "persist durable retrying activity",
                                    self._store.put_activity(retrying_record),
                                )
                            except Exception as persistence_error:
                                release_activity_claim = True
                                # Infrastructure failures must not become a
                                # terminal workflow failure. Preserve the typed
                                # pending outcome and chain the storage cause.
                                raise pending from persistence_error
                            raise pending from None

                        await _await_workflow_infrastructure(
                            "persist retrying activity",
                            self._store.put_activity(retrying_record),
                        )
                        release_activity_claim = True
                        await asyncio.sleep(retry_interval.total_seconds())
                        policy_interval = _next_interval(policy_interval, plan)
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
                        retry_timer_id=retry_timer_id,
                    )
                    completed_record.created_at.FromDatetime(attempts[0].started_at.ToDatetime())
                    completed_record.completed_at.FromDatetime(completed_at)
                    if plan.record_policy is not None:
                        completed_record.retry_policy.CopyFrom(plan.record_policy)
                    await _await_workflow_infrastructure(
                        "persist completed activity",
                        self._store.put_activity(completed_record),
                    )
                    release_activity_claim = True
                    await reconcile_terminal_retry_timer(completed_record)
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
            options.retry_timer_id,
        )

    async def activity(
        self,
        func: Callable[[RequestT], Awaitable[ResultT]],
        input_message: RequestT,
        *,
        activity_id: str,
        retry_timer_id: str = "",
        retry_policy: RetryPolicy | None = None,
        result_type: type[ResultT] | None = None,
    ) -> ResultT:
        """Ergonomic shortcut over :meth:`execute_activity`.

        Defaults applied unless overridden:

        - ``activity_id`` is always caller-supplied and stable across replay.
        - ``retry_timer_id`` is caller-supplied and required when the retry
          policy enables durable backoff.
        - ``retry_policy`` ← :func:`default_retry_policy` (3 attempts, 1s
          initial, 2x backoff, 30s max interval, 30s durable threshold) —
          sensible for the framework's stated workloads (LLM / vendor /
          quant). Pass ``RetryPolicy(maximum_attempts=1)`` explicitly for a
          single attempt.
        - ``result_type`` ← inferred from ``func``'s return annotation
          (``Awaitable[X]`` ⇒ ``X``). Required only when the annotation is
          missing or not introspectable.

        Temporaless never derives either ID from a callable name.
        """
        if retry_policy is None:
            retry_policy = default_retry_policy()
        if result_type is None:
            # _infer_result_type returns plain `type` (dynamic discovery via
            # typing.get_type_hints); cast back to `type[ResultT]` so static
            # checkers can see the parameterized result type.
            result_type = cast("type[ResultT]", _infer_result_type(func))
        options = ActivityOptions(
            activity_id=activity_id,
            retry_policy=retry_policy,
            retry_timer_id=retry_timer_id,
        )
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
        record = await _await_workflow_infrastructure(
            "read workflow event",
            self._store.get_event(key),
        )
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
        if duration < timedelta(0):
            raise ValueError("sleep duration must not be negative")

        timer_kind = SLEEP_TIMER_KIND
        key = TimerKey(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            timer_id=timer_id,
        )
        now = datetime.now(UTC)

        def inspect_sleep_timer(
            stored: temporaless_pb2.TimerRecord,
        ) -> tuple[datetime, temporaless_pb2.TimerStatus]:
            if stored.timer_kind != timer_kind:
                raise TimerConflictError(
                    f"timer kind changed from {stored.timer_kind!r} to {timer_kind!r}"
                )
            if stored.code_version != self._code_version:
                raise TimerConflictError(
                    f"code version changed from {stored.code_version!r} to {self._code_version!r}"
                )
            if stored.retry_activity_id:
                raise TimerConflictError("stored sleep timer belongs to an activity retry")
            if not stored.HasField("duration"):
                raise TimerConflictError("stored sleep timer has no duration")
            try:
                stored_duration = _validated_proto_duration(
                    stored.duration, "persisted sleep timer duration"
                )
            except (OverflowError, ValueError) as exc:
                raise TimerConflictError("stored sleep timer has invalid duration") from exc
            if stored_duration < timedelta(0):
                raise TimerConflictError("stored sleep timer has negative duration")
            if stored_duration != duration:
                raise TimerConflictError(
                    f"timer duration changed from {stored_duration} to {duration}"
                )
            if not stored.HasField("fire_at"):
                raise TimerConflictError("stored sleep timer has no fire_at")
            if not stored.HasField("created_at"):
                raise TimerConflictError("stored sleep timer has no created_at")
            try:
                fire_at = _validated_proto_timestamp(
                    stored.fire_at, "persisted sleep timer fire_at"
                )
                _validated_proto_timestamp(stored.created_at, "persisted sleep timer created_at")
            except (OverflowError, ValueError) as exc:
                raise TimerConflictError("stored sleep timer has an invalid timestamp") from exc

            if stored.status == temporaless_pb2.TIMER_STATUS_SCHEDULED:
                if stored.HasField("fired_at"):
                    raise TimerConflictError("stored SCHEDULED sleep timer has fired_at")
                return fire_at, stored.status
            if stored.status == temporaless_pb2.TIMER_STATUS_FIRED:
                if not stored.HasField("fired_at"):
                    raise TimerConflictError("stored FIRED sleep timer has no fired_at")
                try:
                    _validated_proto_timestamp(stored.fired_at, "persisted sleep timer fired_at")
                except (OverflowError, ValueError) as exc:
                    raise TimerConflictError(
                        "stored FIRED sleep timer has invalid fired_at"
                    ) from exc
                return fire_at, stored.status
            if stored.status == temporaless_pb2.TIMER_STATUS_CANCELED:
                if stored.HasField("fired_at"):
                    raise TimerConflictError("stored CANCELED sleep timer has fired_at")
                raise TimerConflictError("timer was canceled")
            raise TimerConflictError("stored sleep timer has unknown status")

        record = await _await_workflow_infrastructure(
            "read sleep timer",
            self._store.get_timer(key),
        )
        if record is not None:
            fire_at, status = inspect_sleep_timer(record)
            if status == temporaless_pb2.TIMER_STATUS_SCHEDULED:
                if now < fire_at:
                    # This future timer is the durable successor for any earlier
                    # due sleeps traversed by the replay.
                    await self._reconcile_due_sleep_timers()
                    raise TimerPendingError(timer_id, fire_at)
                self._due_sleep_timer_ids.add(timer_id)
            return

        try:
            fire_at = now + duration
        except OverflowError as exc:
            raise ValueError("sleep duration is outside the supported range") from exc
        duration_message = Duration()
        try:
            duration_message.FromTimedelta(duration)
            _validated_proto_duration(duration_message, "sleep duration")
        except (OverflowError, ValueError) as exc:
            raise ValueError("sleep duration is outside the protobuf range") from exc
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
            # Even an immediately due timer stays scheduled until the body
            # crosses a later durable boundary. Otherwise a process death
            # immediately after this write would consume its only wakeup.
            status=temporaless_pb2.TIMER_STATUS_SCHEDULED,
            fire_at=fire_at_message,
            created_at=created_at,
        )
        try:
            await _await_workflow_infrastructure(
                "persist sleep timer",
                self._store.put_timer(record),
            )
        except WorkflowInfrastructureError as write_error:
            # A remote point write may commit and then lose its response. Read
            # through the cache to the authoritative store before deciding
            # whether the caller alone must retry or a scanner wake is durable.
            try:
                if isinstance(self._store, RunScopedCache):
                    verified = await _await_workflow_infrastructure(
                        "verify ambiguous sleep timer write",
                        self._store.refresh_timer(key),
                    )
                else:
                    verified = await _await_workflow_infrastructure(
                        "verify ambiguous sleep timer write",
                        self._store.get_timer(key),
                    )
            except WorkflowInfrastructureError as verify_error:
                causes = ExceptionGroup(
                    "sleep timer write and verification failed",
                    [write_error, verify_error],
                )
                raise WorkflowInfrastructureError(
                    "persist and verify sleep timer",
                    causes,
                ) from verify_error
            if verified is None:
                # Definitive before-commit failure: no scheduler wake exists,
                # so UNAVAILABLE tells the requester to invoke this run again.
                raise
            verified_fire_at, verified_status = inspect_sleep_timer(verified)
            if verified_status == temporaless_pb2.TIMER_STATUS_FIRED:
                return
            # Stop even when the verified wake is already due. The scheduled
            # timer makes this invocation independently redeliverable and the
            # parent must remain IN_PROGRESS after the ambiguous response.
            raise TimerPendingError(timer_id, verified_fire_at) from write_error
        if now >= fire_at:
            self._due_sleep_timer_ids.add(timer_id)
            return
        # Persist the successor before consuming any prior due wakeups. A
        # cleanup failure leaves duplicate delivery, not a lost workflow.
        await self._reconcile_due_sleep_timers()
        raise TimerPendingError(timer_id, fire_at)

    async def _reconcile_due_sleep_timers(self) -> None:
        """Best-effort acknowledgement after a durable successor boundary.

        The caller must persist that boundary before invoking this method.
        Failed acknowledgements intentionally remain scheduled so a scanner
        can safely redeliver the workflow.
        """
        for timer_id in tuple(self._due_sleep_timer_ids):
            key = TimerKey(
                workflow_id=self._workflow_id,
                run_id=self._run_id,
                timer_id=timer_id,
            )
            try:
                record = await self._store.get_timer(key)
                if record is None or record.status != temporaless_pb2.TIMER_STATUS_SCHEDULED:
                    self._due_sleep_timer_ids.discard(timer_id)
                    continue
                if (
                    record.timer_kind != SLEEP_TIMER_KIND
                    or record.code_version != self._code_version
                ):
                    _LOGGER.warning(
                        "refusing to reconcile changed sleep timer %s/%s/%s",
                        self._workflow_id,
                        self._run_id,
                        timer_id,
                    )
                    continue
                fired = temporaless_pb2.TimerRecord()
                fired.CopyFrom(record)
                fired.status = temporaless_pb2.TIMER_STATUS_FIRED
                fired.fired_at.GetCurrentTime()
                await self._store.put_timer(fired)
                self._due_sleep_timer_ids.discard(timer_id)
            except Exception:
                _LOGGER.warning(
                    "failed to reconcile due sleep timer %s/%s/%s",
                    self._workflow_id,
                    self._run_id,
                    timer_id,
                    exc_info=True,
                )

    async def _put_activity_retry_timer(
        self,
        activity_id: str,
        retry_timer_id: str,
        duration: timedelta,
        fire_at: datetime,
    ) -> None:
        """Write (or overwrite) the TIMER_KIND_ACTIVITY_RETRY timer paired
        with an activity's durable retry. The caller-supplied ID is stable so
        later retries naturally overwrite earlier scheduled state."""
        key = TimerKey(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            timer_id=retry_timer_id,
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
            retry_activity_id=activity_id,
        )
        await _await_workflow_infrastructure(
            "persist activity retry timer",
            self._store.put_timer(record),
        )

    async def _mark_activity_retry_timer_fired(
        self,
        activity_id: str,
        retry_timer_id: str,
    ) -> None:
        """Transition a paired retry timer after its activity is terminal."""
        key = TimerKey(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            timer_id=retry_timer_id,
        )
        record = await _await_workflow_infrastructure(
            "read terminal activity retry timer",
            self._store.get_timer(key),
        )
        if record is None:
            return
        if record.key != key.to_proto():
            raise ActivityConflictError(
                "stored activity retry timer key does not match its storage key"
            )
        if record.timer_kind != temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY:
            raise ActivityConflictError(
                "stored activity retry timer collides with another timer kind"
            )
        if record.retry_activity_id != activity_id:
            raise ActivityConflictError("stored activity retry timer belongs to another activity")
        if record.code_version != self._code_version:
            raise ActivityConflictError("stored activity retry timer code version changed")
        if record.schema_version != TIMER_RECORD_SCHEMA_VERSION:
            raise ActivityConflictError("stored activity retry timer schema version changed")
        if record.status == temporaless_pb2.TIMER_STATUS_FIRED:
            return
        if record.status == temporaless_pb2.TIMER_STATUS_CANCELED:
            raise ActivityConflictError("stored activity retry timer was canceled")
        if record.status != temporaless_pb2.TIMER_STATUS_SCHEDULED:
            raise ActivityConflictError("stored activity retry timer has unknown status")
        if not record.HasField("fire_at"):
            raise ActivityConflictError("stored activity retry timer has no fire_at")
        try:
            record.fire_at.ToJsonString()
        except (OverflowError, ValueError) as exc:
            raise ActivityConflictError("stored activity retry timer has invalid fire_at") from exc
        fired = temporaless_pb2.TimerRecord()
        fired.CopyFrom(record)
        fired.status = temporaless_pb2.TIMER_STATUS_FIRED
        fired.fired_at.GetCurrentTime()
        await _await_workflow_infrastructure(
            "acknowledge terminal activity retry timer",
            self._store.put_timer(fired),
        )


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
    _validate_result_template("workflow", result_template)
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
                    record,
                    workflow_type,
                    options.code_version,
                    options.run_order_time if options.HasField("run_order_time") else None,
                    result_template,
                ),
                None,
            )
        if record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS:
            _assert_workflow_identity(
                record,
                workflow_type,
                options.code_version,
                options.run_order_time if options.HasField("run_order_time") else None,
            )
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
            if options.HasField("run_order_time"):
                in_progress.run_order_time.CopyFrom(options.run_order_time)
            await store.put_workflow(in_progress)
        else:
            assert record is not None
            in_progress = temporaless_pb2.WorkflowRecord()
            in_progress.CopyFrom(record)

        workflow = Workflow(
            store=store,
            options=options,
            claim_capability=claim_capability,
        )
        workflow_annotations = _AnnotationsBag(data=dict(in_progress.annotations))
        annotations_token = _annotations_var.set(workflow_annotations)
        workflow_token = _workflow_var.set(workflow)
        try:
            try:
                result = await execute(workflow, input_message)
                _validate_result_message("workflow", result, result_template)
            except (
                TimerPendingError,
                ClaimBusyError,
                ClaimReleaseError,
                EventPendingError,
                WorkflowDependencyPendingError,
                WorkflowInfrastructureError,
            ):
                annotations = workflow_annotations.snapshot()
                if dict(in_progress.annotations) != annotations:
                    updated = temporaless_pb2.WorkflowRecord()
                    updated.CopyFrom(in_progress)
                    updated.annotations.clear()
                    updated.annotations.update(annotations)
                    await store.put_workflow(updated)
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
                if options.HasField("run_order_time"):
                    failed.run_order_time.CopyFrom(options.run_order_time)
                await store.put_workflow(failed)
                # The terminal record is authoritative. Only now is it safe
                # to consume sleep wakeups traversed by this invocation.
                await workflow._reconcile_due_sleep_timers()
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
            if options.HasField("run_order_time"):
                completed.run_order_time.CopyFrom(options.run_order_time)
            await store.put_workflow(completed)
            await workflow._reconcile_due_sleep_timers()
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
    if normalized.HasField("run_order_time"):
        try:
            normalized.run_order_time.ToDatetime(tzinfo=UTC)
        except (OverflowError, ValueError) as exc:
            raise ValueError("run_order_time must be a valid protobuf timestamp") from exc
    return normalized


def message_pair_type(kind: str, input_message: Message, output_message: Message) -> str:
    return f"{kind}:{input_message.DESCRIPTOR.full_name}->{output_message.DESCRIPTOR.full_name}"


def _validate_result_template(kind: str, result_template: object) -> None:
    if not isinstance(result_template, Message):
        raise _ResultTypeError(
            f"{kind} result factory returned non-protobuf type "
            f"{type(result_template).__module__}.{type(result_template).__qualname__}"
        )


def _validate_result_message(kind: str, result: object, expected: Message) -> None:
    expected_type = expected.DESCRIPTOR.full_name
    if not isinstance(result, Message):
        raise _ResultTypeError(
            f"{kind} executor returned non-protobuf type "
            f"{type(result).__module__}.{type(result).__qualname__}; "
            f"expected {expected_type}"
        )
    actual_type = result.DESCRIPTOR.full_name
    if actual_type != expected_type:
        raise _ResultTypeError(
            f"{kind} executor returned protobuf type {actual_type}; expected {expected_type}"
        )


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


def _assert_activity_retry_policy(
    record: temporaless_pb2.ActivityRecord,
    current: RetryPolicy | None,
) -> None:
    """Reject retry-plan drift while an activity is resumable.

    An absent stored policy means single-attempt execution and therefore can
    never validly describe a RETRYING record. Terminal records deliberately do
    not use this check: their persisted outcome remains authoritative.
    """
    if not record.HasField("retry_policy"):
        if current is None:
            raise ActivityConflictError("stored RETRYING activity has no retry policy")
        raise ActivityConflictError("activity retry policy changed from single-attempt execution")
    if current is None:
        raise ActivityConflictError("activity retry policy changed to single-attempt execution")
    if record.retry_policy != current:
        raise ActivityConflictError("activity retry policy changed while activity is RETRYING")


def _replay_record(
    record: temporaless_pb2.ActivityRecord,
    activity_type: str,
    code_version: str,
    result: ResultT,
) -> ResultT:
    _assert_activity_identity(record, activity_type, code_version)

    if record.status == temporaless_pb2.ACTIVITY_STATUS_COMPLETED:
        if not record.result.Unpack(result):
            raise ActivityConflictError(
                "stored activity result type does not match requested result"
            )
        return result
    if record.status == temporaless_pb2.ACTIVITY_STATUS_FAILED:
        if not record.HasField("failure"):
            raise ActivityConflictError("stored FAILED activity has no failure")
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
    record_policy: RetryPolicy | None


def _validate_retrying_activity(
    record: temporaless_pb2.ActivityRecord,
    plan: _RetryPlan,
) -> list[temporaless_pb2.ActivityAttempt]:
    attempts = list(record.attempts)
    if not attempts:
        raise ActivityConflictError("stored RETRYING activity has no attempts")
    if len(attempts) >= plan.maximum_attempts:
        raise ActivityConflictError(
            "stored RETRYING activity has exhausted its retry policy "
            f"({len(attempts)} attempts under a {plan.maximum_attempts}-attempt policy)"
        )
    for ordinal, attempt in enumerate(attempts, start=1):
        if attempt.attempt != ordinal:
            raise ActivityConflictError(
                f"stored RETRYING activity attempt {ordinal} is out of sequence"
            )
        if not attempt.HasField("failure"):
            raise ActivityConflictError(
                f"stored RETRYING activity attempt {ordinal} has no failure"
            )
        if attempt.failure.HasField("retry_after"):
            try:
                # ToTimedelta accepts values outside the protobuf Duration
                # range; the public JSON conversion performs the canonical
                # protobuf validity check first.
                retry_after = _validated_proto_duration(
                    attempt.failure.retry_after, "persisted retry_after"
                )
            except (OverflowError, ValueError) as exc:
                raise ActivityConflictError(
                    f"stored RETRYING activity attempt {ordinal} has invalid retry_after"
                ) from exc
            if retry_after < timedelta(0):
                raise ActivityConflictError(
                    f"stored RETRYING activity attempt {ordinal} has negative retry_after"
                )
    last_failure = attempts[-1].failure
    if last_failure.code in plan.non_retryable_codes:
        raise ActivityConflictError("stored RETRYING activity ends with a non-retryable failure")
    if not record.HasField("failure") or record.failure != last_failure:
        raise ActivityConflictError(
            "stored RETRYING activity failure does not match its latest attempt"
        )

    retry_interval = _retry_interval_after_failed_attempt(plan, len(attempts))
    if last_failure.HasField("retry_after"):
        retry_interval = max(
            retry_interval,
            _validated_proto_duration(last_failure.retry_after, "persisted retry_after"),
        )
    requires_durable_wait = (
        plan.durable_threshold > timedelta(0) and retry_interval >= plan.durable_threshold
    )
    if record.HasField("next_attempt_at") != requires_durable_wait:
        expected = "present" if requires_durable_wait else "absent"
        raise ActivityConflictError(
            f"stored RETRYING activity next_attempt_at must be {expected} for its retry policy"
        )
    return attempts


def _validated_proto_duration(value: Duration, name: str) -> timedelta:
    try:
        # ToJsonString enforces the protobuf Duration range and canonical
        # seconds/nanos sign rules that ToTimedelta alone accepts loosely.
        value.ToJsonString()
        return value.ToTimedelta()
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"retry policy {name} is invalid") from exc


def _validated_proto_timestamp(value: Timestamp, name: str) -> datetime:
    try:
        # ToJsonString enforces the protobuf Timestamp range. ToDatetime then
        # gives a timezone-aware UTC value for direct wall-clock comparison.
        value.ToJsonString()
        return value.ToDatetime(tzinfo=UTC)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{name} is invalid") from exc


def _plan_retries(policy: RetryPolicy | None) -> _RetryPlan:
    if policy is None:
        return _RetryPlan(
            maximum_attempts=1,
            initial_interval=timedelta(0),
            backoff_coefficient=1.0,
            maximum_interval=timedelta(0),
            durable_threshold=timedelta(0),
            non_retryable_codes=frozenset(),
            record_policy=None,
        )
    maximum_attempts = policy.maximum_attempts
    if maximum_attempts == 0:
        raise ValueError("retry policy maximum_attempts must be > 0")
    initial_interval = _validated_proto_duration(policy.initial_interval, "initial_interval")
    if initial_interval < timedelta(0):
        raise ValueError("retry policy initial_interval must be >= 0")
    if maximum_attempts > 1 and initial_interval <= timedelta(0):
        raise ValueError("retry policy initial_interval must be > 0 when maximum_attempts > 1")
    backoff_coefficient = policy.backoff_coefficient or 1.0
    if not math.isfinite(backoff_coefficient) or backoff_coefficient <= 0:
        raise ValueError("retry policy backoff_coefficient must be finite and > 0")
    maximum_interval = _validated_proto_duration(policy.maximum_interval, "maximum_interval")
    if maximum_interval < timedelta(0):
        raise ValueError("retry policy maximum_interval must be >= 0")
    if maximum_interval > timedelta(0) and initial_interval > maximum_interval:
        raise ValueError("retry policy maximum_interval must be >= initial_interval")
    durable_threshold = _validated_proto_duration(
        policy.durable_backoff_threshold, "durable_backoff_threshold"
    )
    if durable_threshold < timedelta(0):
        raise ValueError("retry policy durable_backoff_threshold must be >= 0")
    non_retryable_codes = frozenset(policy.non_retryable_error_codes)
    record_policy = RetryPolicy(
        maximum_attempts=maximum_attempts,
        backoff_coefficient=backoff_coefficient,
        non_retryable_error_codes=sorted(non_retryable_codes),
    )
    if initial_interval != timedelta(0):
        record_policy.initial_interval.FromTimedelta(initial_interval)
    if maximum_interval != timedelta(0):
        record_policy.maximum_interval.FromTimedelta(maximum_interval)
    if durable_threshold != timedelta(0):
        record_policy.durable_backoff_threshold.FromTimedelta(durable_threshold)
    return _RetryPlan(
        maximum_attempts=maximum_attempts,
        initial_interval=initial_interval,
        backoff_coefficient=backoff_coefficient,
        maximum_interval=maximum_interval,
        durable_threshold=durable_threshold,
        non_retryable_codes=non_retryable_codes,
        record_policy=record_policy,
    )


def _next_interval(prev: timedelta, plan: _RetryPlan) -> timedelta:
    next_seconds = prev.total_seconds() * plan.backoff_coefficient
    if (
        plan.maximum_interval > timedelta(0)
        and next_seconds >= plan.maximum_interval.total_seconds()
    ):
        return plan.maximum_interval
    if not math.isfinite(next_seconds):
        raise ValueError("retry policy produced a non-finite backoff interval")
    try:
        result = timedelta(seconds=next_seconds)
    except OverflowError as exc:
        raise ValueError("retry policy produced an out-of-range backoff interval") from exc
    if prev > timedelta(0) and result <= timedelta(0):
        raise ValueError("retry policy produced a non-positive backoff interval")
    return result


def _retry_interval_after_failed_attempt(
    plan: _RetryPlan,
    attempt: int,
) -> timedelta:
    if attempt <= 0:
        raise ActivityConflictError("stored RETRYING activity has no failed attempt")
    interval = plan.initial_interval
    for _ in range(1, attempt):
        interval = _next_interval(interval, plan)
    return interval


def _retry_interval_after_attempts(
    plan: _RetryPlan,
    attempts: list[temporaless_pb2.ActivityAttempt],
) -> timedelta:
    """Rebuild the exponential schedule at a RETRYING replay boundary.

    The normalized policy is stored on ActivityRecord and checked before this
    reconstruction. Advancing its exponential schedule once per persisted
    failed attempt produces the same interval the uninterrupted loop would use
    after the next attempt. A prior attempt's Retry-After affects only that
    attempt's wait; it must not become the base for later exponential growth.
    """
    if not attempts:
        return plan.initial_interval
    if not attempts[-1].HasField("failure"):
        raise ActivityConflictError("stored RETRYING activity attempt has no failure")
    return _next_interval(
        _retry_interval_after_failed_attempt(plan, len(attempts)),
        plan,
    )


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
    run_order_time: Timestamp | None,
    result: ResultT,
) -> ResultT:
    _assert_workflow_identity(record, workflow_type, code_version, run_order_time)
    if record.status == temporaless_pb2.WORKFLOW_STATUS_COMPLETED:
        if not record.result.Unpack(result):
            raise WorkflowConflictError(
                "stored workflow result type does not match requested result"
            )
        return result
    if record.status == temporaless_pb2.WORKFLOW_STATUS_FAILED:
        if not record.HasField("failure"):
            raise WorkflowConflictError("stored FAILED workflow has no failure")
        raise ActivityError(record.failure.code, record.failure.message)
    raise WorkflowConflictError("stored workflow has unknown status")


def _assert_workflow_identity(
    record: temporaless_pb2.WorkflowRecord,
    workflow_type: str,
    code_version: str,
    run_order_time: Timestamp | None,
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
    if record.HasField("run_order_time") != (run_order_time is not None):
        raise WorkflowConflictError("run_order_time changed for an existing workflow run")
    if run_order_time is not None and record.run_order_time != run_order_time:
        raise WorkflowConflictError("run_order_time changed for an existing workflow run")
