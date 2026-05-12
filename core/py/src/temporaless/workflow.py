from __future__ import annotations

import asyncio
import contextvars
import hashlib
import inspect
import logging
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
DEFAULT_CLAIM_LEASE_DURATION = timedelta(minutes=15)
Options = temporaless_pb2.WorkflowOptions
ActivityOptions = temporaless_pb2.ActivityOptions
RetryPolicy = temporaless_pb2.RetryPolicy

# Framework-reserved string literals sourced from the proto-declared defaults
# on ``temporaless.v1.ReservedNames``. Reading from a zero-value instance
# (rather than declaring parallel constants here) guarantees the proto
# contract is the single source of truth — renaming any reserved string is a
# one-line proto change plus regenerate, no SDK constant drifts.
_RESERVED_NAMES = temporaless_pb2.ReservedNames()

# Marks timer records owned by the runtime's durable retry path. User code
# passing this prefix to ``Workflow.sleep`` is rejected so framework-managed
# retry timers don't collide with user timers.
ACTIVITY_RETRY_TIMER_ID_PREFIX = _RESERVED_NAMES.activity_retry_timer_id_prefix


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

    A slot held by the same owner is treated as re-acquired (crash recovery),
    so one workflow can't consume multiple slots across crash boundaries.

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
            input_digest=slot_id,
            lease_expires_at=expires_at,
            created_at=created_at,
            heartbeat_at=created_at,
        )
        if await claim_store.try_create_claim(claim):
            return slot_id
        # Slot taken — see whether it's our own stale claim from a prior
        # invocation that crashed before releasing.
        existing = await claim_store.get_claim(slot_key)
        if existing is not None and existing.owner_id == owner_id:
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


def _concurrency_owner_id(workflow_id: str, run_id: str) -> str:
    """Stable owner identity for the workflow holding the slot. Using
    ``workflow_id:run_id`` lets a crashed invocation's next invocation
    re-acquire its previously-held slot (idempotent recovery)."""
    return f"{workflow_id}:{run_id}"


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
            message = f"{message} until {lease_expires_at.isoformat()}"
        super().__init__(message)
        self.claim_id = claim_id
        self.owner_id = owner_id
        self.lease_expires_at = lease_expires_at
        self.capability = capability


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
    - ``WorkflowConflictError``, ``ActivityConflictError``, ``TimerConflictError``
      → ``FAILED_PRECONDITION`` (stored fingerprint disagrees with input).
    - ``ActivityError``, ``WorkflowDependencyFailedError`` → ``INTERNAL`` (the
      upstream pipeline produced a terminal failure that this workflow can't
      recover from).

    Lazy import of ``connectrpc.code.Code`` — the helper is in core/workflow.py
    so workflow.py stays usable without a connectrpc dependency for non-RPC
    callers.
    """
    from connectrpc.code import Code

    if isinstance(exc, (TimerPendingError, EventPendingError, WorkflowDependencyPendingError)):
        return (Code.UNAVAILABLE, str(exc))
    if isinstance(exc, ClaimBusyError):
        return (Code.ALREADY_EXISTS, str(exc))
    if isinstance(exc, ConcurrencyBusyError):
        return (Code.RESOURCE_EXHAUSTED, str(exc))
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
    ) -> None:
        options = normalized_workflow_options(options)
        claim_store: ClaimStore | None = None
        if options.claim_owner_id:
            if not isinstance(store, ClaimStore):
                raise ValueError("claim store is required when claim owner is provided")
            claim_store = store
        self._store = store
        self._claim_store = claim_store
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

        digest = activity_digest(activity_type, self._code_version, input_message)
        key = ActivityKey(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            activity_id=activity_id,
        )

        record = await self._store.get_activity(key)
        attempts: list[temporaless_pb2.ActivityAttempt] = []
        seeded_annotations: dict[str, str] = {}
        if record is not None:
            if record.status in (
                temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
                temporaless_pb2.ACTIVITY_STATUS_FAILED,
            ):
                return _replay_record(
                    record, activity_type, self._code_version, digest, result_factory
                )
            if record.status == temporaless_pb2.ACTIVITY_STATUS_RETRYING:
                _assert_activity_fingerprint(record, activity_type, self._code_version, digest)
                # Durable retry resume: if the record carries next_attempt_at
                # and the wake instant hasn't arrived yet, bail back to the
                # workflow as pending. The paired TIMER_KIND_ACTIVITY_RETRY
                # timer keeps the scanner waking the workflow until fire_at.
                if record.HasField("next_attempt_at"):
                    wake_at = record.next_attempt_at.ToDatetime().replace(tzinfo=UTC)
                    if datetime.now(UTC) < wake_at:
                        raise TimerPendingError(_activity_retry_timer_id(activity_id), wake_at)
                    # Past the wake instant — mark the paired timer FIRED so
                    # the scanner stops returning it while we run the resume.
                    await self._mark_activity_retry_timer_fired(activity_id)
                attempts = list(record.attempts)
                # Restore prior annotations so per-attempt metadata survives
                # cross-invocation resumes.
                seeded_annotations = dict(record.annotations)
            else:
                raise ActivityConflictError("stored activity has unknown status")

        if self._claim_owner is not None:
            claim_id = f"activity:{activity_id}"
            claim_key = ClaimKey(
                workflow_id=self._workflow_id,
                run_id=self._run_id,
                claim_id=claim_id,
            )
            now = datetime.now(UTC)
            created_at = Timestamp()
            created_at.FromDatetime(now)
            expires_at = Timestamp()
            expires_at.FromDatetime(now + DEFAULT_CLAIM_LEASE_DURATION)
            claim = temporaless_pb2.ClaimRecord(
                schema_version=CLAIM_RECORD_SCHEMA_VERSION,
                key=claim_key.to_proto(),
                owner_id=self._claim_owner,
                resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
                resource_id=activity_id,
                code_version=self._code_version,
                input_digest=digest,
                lease_expires_at=expires_at,
                created_at=created_at,
                heartbeat_at=created_at,
            )
            if self._claim_store is None:
                raise ValueError("claim store is required when claim owner is provided")
            if not await self._claim_store.try_create_claim(claim):
                fresh = await self._store.get_activity(key)
                if fresh is not None and fresh.status != temporaless_pb2.ACTIVITY_STATUS_RETRYING:
                    return _replay_record(
                        fresh,
                        activity_type,
                        self._code_version,
                        digest,
                        result_factory,
                    )

                existing = await self._claim_store.get_claim(claim_key)
                if existing is not None and existing.owner_id == self._claim_owner:
                    # We already own the claim — resuming a prior attempt by
                    # the same owner. Safe to proceed.
                    pass
                elif existing is None:
                    raise ClaimBusyError(
                        claim_id, capability=await self._claim_store.claim_capability()
                    )
                else:
                    lease_expires_at = existing.lease_expires_at.ToDatetime().replace(tzinfo=UTC)
                    raise ClaimBusyError(
                        claim_id,
                        existing.owner_id,
                        lease_expires_at,
                        await self._claim_store.claim_capability(),
                    )

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
                    result = await execute()
                except asyncio.CancelledError:
                    # Cancellation is shutdown signal, not a vendor failure —
                    # propagate without persisting an attempt or retrying.
                    raise
                except BaseException as run_err:  # noqa: BLE001 - we want to capture user errors
                    completed_at = datetime.now(UTC)
                    failure = _failure_from_exception(run_err)
                    attempt_record = temporaless_pb2.ActivityAttempt(
                        attempt=attempt_idx,
                        failure=failure,
                    )
                    attempt_record.started_at.FromDatetime(started_at)
                    attempt_record.completed_at.FromDatetime(completed_at)
                    attempts.append(attempt_record)

                    # Vendor-supplied Retry-After overrides the computed
                    # interval when it's longer. The configured exponential
                    # schedule still applies as a floor — so an aggressive
                    # policy doesn't undershoot a vendor's stated window.
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
                            input_digest=digest,
                            input=input_any,
                            status=temporaless_pb2.ACTIVITY_STATUS_FAILED,
                            failure=failure,
                            attempts=attempts,
                            annotations=activity_annotations.snapshot(),
                        )
                        failed_record.created_at.FromDatetime(attempts[0].started_at.ToDatetime())
                        failed_record.completed_at.FromDatetime(completed_at)
                        await self._store.put_activity(failed_record)
                        raise ActivityError(failure.code, failure.message, run_err) from run_err

                    retrying_record = temporaless_pb2.ActivityRecord(
                        schema_version=ACTIVITY_RECORD_SCHEMA_VERSION,
                        key=key.to_proto(),
                        activity_type=activity_type,
                        code_version=self._code_version,
                        input_digest=digest,
                        input=input_any,
                        status=temporaless_pb2.ACTIVITY_STATUS_RETRYING,
                        failure=failure,
                        attempts=attempts,
                        annotations=activity_annotations.snapshot(),
                    )
                    retrying_record.created_at.FromDatetime(attempts[0].started_at.ToDatetime())

                    # Durable retry branch: when the next backoff interval
                    # crosses the configured threshold, persist the wait as a
                    # TIMER_KIND_ACTIVITY_RETRY timer and surface
                    # TimerPendingError. The timer scanner re-invokes the
                    # workflow after fire_at; run_activity then enters the
                    # RETRYING-resume branch above and continues the loop.
                    if plan.durable_threshold > timedelta(0) and interval >= plan.durable_threshold:
                        next_attempt_at = datetime.now(UTC) + interval
                        next_at_ts = Timestamp()
                        next_at_ts.FromDatetime(next_attempt_at)
                        retrying_record.next_attempt_at.CopyFrom(next_at_ts)
                        await self._store.put_activity(retrying_record)
                        await self._put_activity_retry_timer(activity_id, interval, next_attempt_at)
                        # `from None` because the workflow itself isn't
                        # failing — the durable retry is a normal pending
                        # signal, not an exception caused by run_err.
                        raise TimerPendingError(
                            _activity_retry_timer_id(activity_id), next_attempt_at
                        ) from None

                    await self._store.put_activity(retrying_record)
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
                    input_digest=digest,
                    input=input_any,
                    status=temporaless_pb2.ACTIVITY_STATUS_COMPLETED,
                    result=result_any,
                    attempts=attempts,
                    annotations=activity_annotations.snapshot(),
                )
                completed_record.created_at.FromDatetime(attempts[0].started_at.ToDatetime())
                completed_record.completed_at.FromDatetime(completed_at)
                await self._store.put_activity(completed_record)
                return result

            raise RuntimeError(f"activity {activity_id!r} exhausted retry plan")
        finally:
            _annotations_var.reset(annotations_token)

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
        digest = timer_digest(timer_kind, self._code_version, duration)
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
            if record.input_digest != digest:
                raise TimerConflictError("timer duration changed")
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
            input_digest=digest,
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
        digest = timer_digest(timer_kind, self._code_version, duration)
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
            input_digest=digest,
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
    digest = execution_digest("workflow", workflow_type, options.code_version, input_message)
    key = WorkflowKey(workflow_id=options.workflow_id, run_id=options.run_id)

    # Substitute the user-provided store with a run-scoped cache. The cache
    # is write-through for the underlying store and serves get-by-key reads
    # from memory after prefetch — turning N round-trips per replay into
    # one list per record kind. Out-of-scope reads (e.g. cross-pipeline
    # dependencies) pass straight through. See _cache.py for the contract.
    cache = RunScopedCache(store, key)
    store = cache  # type: ignore[assignment]

    record = await store.get_workflow(key)
    created_at: Timestamp | None = None
    if record is not None:
        if record.status in (
            temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
            temporaless_pb2.WORKFLOW_STATUS_FAILED,
        ):
            return _replay_workflow_record(
                record, workflow_type, options.code_version, digest, result_factory
            )
        if record.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS:
            _assert_workflow_fingerprint(record, workflow_type, options.code_version, digest)
            created_at = Timestamp()
            created_at.CopyFrom(record.created_at)
            # Replay: prefetch activities / timers / events in parallel so
            # the body's subsequent get-by-key calls hit memory.
            await cache.prefetch()
        else:
            raise WorkflowConflictError("stored workflow has unknown status")

    # Pre-emptive cluster-wide concurrency cap. Acquire BEFORE writing the
    # IN_PROGRESS record so a "busy" condition leaves no observable side
    # effect — the caller simply retries the same workflow.run when capacity
    # is available. Released in the finally block so every exit path
    # (success, failure, pending) frees the slot.
    acquired_slot_id: str | None = None
    if options.concurrency_key and options.concurrency_limit > 0:
        owner_id = _concurrency_owner_id(options.workflow_id, options.run_id)
        # `store` is now the RunScopedCache, which forwards claim methods to
        # the wrapped underlying claim store. cast for ty.
        acquired_slot_id = await _acquire_concurrency_slot(
            cast("ClaimStore", store),
            DEFAULT_NAMESPACE,
            options.concurrency_key,
            options.concurrency_limit,
            owner_id,
            options.code_version,
            DEFAULT_CLAIM_LEASE_DURATION,
        )
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
            input_digest=digest,
            input=input_any,
            status=temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS,
            created_at=created_at,
        )
        await store.put_workflow(in_progress)

    workflow = Workflow(
        store=store,
        options=options,
    )
    workflow_annotations = _AnnotationsBag()
    annotations_token = _annotations_var.set(workflow_annotations)
    workflow_token = _workflow_var.set(workflow)
    try:
        try:
            result = await execute(workflow, input_message)
        except (TimerPendingError, ClaimBusyError, EventPendingError):
            raise
        except asyncio.CancelledError:
            # Same rationale as inside run_activity: cancellation isn't a
            # workflow failure. Leave the IN_PROGRESS record so the next
            # invocation can resume.
            raise
        except BaseException as exc:
            completed_at = Timestamp()
            completed_at.GetCurrentTime()
            failed = temporaless_pb2.WorkflowRecord(
                schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
                key=key.to_proto(),
                workflow_type=workflow_type,
                code_version=options.code_version,
                input_digest=digest,
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
            input_digest=digest,
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
        if acquired_slot_id is not None and options.concurrency_key:
            # Use a shielded release so a cancelled parent task still frees
            # the slot. Worst case: slow release races with lease expiry, the
            # stored claim's lease eventually expires and the slot frees itself.
            try:
                await asyncio.shield(
                    _release_concurrency_slot(
                        cast("ClaimStore", store),
                        DEFAULT_NAMESPACE,
                        options.concurrency_key,
                        acquired_slot_id,
                    )
                )
            except Exception:
                logging.getLogger(__name__).exception(
                    "failed to release concurrency slot %s for %s",
                    acquired_slot_id,
                    options.concurrency_key,
                )


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


def activity_digest(activity_type: str, code_version: str, input_message: Message) -> str:
    return execution_digest("activity", activity_type, code_version, input_message)


def timer_digest(
    timer_kind: temporaless_pb2.TimerKind,
    code_version: str,
    duration: timedelta,
) -> str:
    duration_message = Duration()
    duration_message.FromTimedelta(duration)
    return execution_digest(
        "timer",
        temporaless_pb2.TimerKind.Name(timer_kind),
        code_version,
        duration_message,
    )


def execution_digest(
    kind: str,
    execution_type: str,
    code_version: str,
    input_message: Message,
) -> str:
    digest = hashlib.sha256()
    digest.update(f"temporaless.{kind}.v1".encode())
    digest.update(b"\x00")
    digest.update(execution_type.encode())
    digest.update(b"\x00")
    digest.update(code_version.encode())
    digest.update(b"\x00")
    digest.update(input_message.DESCRIPTOR.full_name.encode())
    digest.update(b"\x00")
    digest.update(input_message.SerializeToString(deterministic=True))
    return digest.hexdigest()


def message_pair_type(kind: str, input_message: Message, output_message: Message) -> str:
    return f"{kind}:{input_message.DESCRIPTOR.full_name}->{output_message.DESCRIPTOR.full_name}"


def _assert_activity_fingerprint(
    record: temporaless_pb2.ActivityRecord,
    activity_type: str,
    code_version: str,
    input_digest: str,
) -> None:
    if record.activity_type != activity_type:
        raise ActivityConflictError(
            f"activity type changed from {record.activity_type!r} to {activity_type!r}"
        )
    if record.code_version != code_version:
        raise ActivityConflictError(
            f"code version changed from {record.code_version!r} to {code_version!r}"
        )
    if record.input_digest != input_digest:
        raise ActivityConflictError(
            "input digest changed (the activity's request differs from the stored attempt; "
            "either pass the original request, delete the activity record to re-execute, "
            "or bump code_version if this change is intentional)"
        )


def _replay_record(
    record: temporaless_pb2.ActivityRecord,
    activity_type: str,
    code_version: str,
    input_digest: str,
    result_factory: Callable[[], ResultT],
) -> ResultT:
    _assert_activity_fingerprint(record, activity_type, code_version, input_digest)

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
    input_digest: str,
    result_factory: Callable[[], ResultT],
) -> ResultT:
    _assert_workflow_fingerprint(record, workflow_type, code_version, input_digest)
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


def _assert_workflow_fingerprint(
    record: temporaless_pb2.WorkflowRecord,
    workflow_type: str,
    code_version: str,
    input_digest: str,
) -> None:
    if record.workflow_type != workflow_type:
        raise WorkflowConflictError(
            f"workflow type changed from {record.workflow_type!r} to {workflow_type!r}"
        )
    if record.code_version != code_version:
        raise WorkflowConflictError(
            f"code version changed from {record.code_version!r} to {code_version!r}"
        )
    if record.input_digest != input_digest:
        raise WorkflowConflictError(
            "input digest changed (the workflow's request differs from the stored run; "
            "either pass the original request, delete the workflow record to re-execute, "
            "or bump code_version if this change is intentional)"
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
