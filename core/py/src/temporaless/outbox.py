"""Derive a stable idempotency key from a workflow + activity identity.

Activity bodies pass the returned string to external systems (HTTP
``Idempotency-Key`` header, DB upsert key, S3 object name) so retries against
the same vendor side-effect are deduplicated.

The key is deterministic over ``(namespace, workflow_id, run_id, activity_id)``.
Every retry of the same activity — in-process or after a durable wake — produces
the same key, so a vendor that supports idempotency keys (Stripe, Slack, OpenAI,
…) treats a retry-after-mid-flight-failure as a duplicate and returns the
original response.

This closes the gap called out in ``docs/hard-cases.md`` — "activity result
storage is for replay; external side effects need their own idempotency key" —
by deriving the key the framework already has the information to produce.

Caveats:

- The current handler implementation does not change the key. If the previous
  result or side effect is no longer the same logical activity, rotate the
  ``activity_id`` (or the ``run_id`` for the whole pipeline).
- The key is per-activity, not per-attempt. Vendors that don't support
  idempotency at all should rely on their natural keys (DB upsert, S3 object
  name) instead.

Usage::

    @wrap_activity(...)
    async def charge(req: ChargeRequest) -> ChargeResponse:
        key = outbox.idempotency_key(current_workflow(), "charge:invoice-42")
        return await stripe.charges.new(req, idempotency_key=key)
"""

from __future__ import annotations

import hashlib

from temporaless.storage import DEFAULT_NAMESPACE
from temporaless.workflow import Workflow

# Marks keys produced by this helper. Lets operators recognize a key as
# framework-derived when grepping vendor dashboards / DB rows.
PREFIX = "temporaless-"


def idempotency_key(workflow: Workflow, activity_id: str) -> str:
    """Return a stable idempotency key for the given activity within the
    workflow. Same ``activity_id`` + same workflow run = same key across all
    retries (including durable resumes after a TIMER_KIND_ACTIVITY_RETRY).

    ``activity_id`` must be the same value the caller passes to
    ``ActivityOptions.activity_id``. Passing a different value yields a
    different key and breaks vendor-side dedup.
    """
    return derive(DEFAULT_NAMESPACE, workflow.workflow_id, workflow.run_id, activity_id)


def derive(namespace: str, workflow_id: str, run_id: str, activity_id: str) -> str:
    """Pure form for callers that already have the identity tuple (operator
    scripts, tests, cross-language probes). Prefer :func:`idempotency_key`
    inside an activity body."""
    # `|` is not a permitted character in any framework ID (the validation
    # regex is [A-Za-z0-9._:=-]), so it's an unambiguous separator.
    identity = f"{namespace}|{workflow_id}|{run_id}|{activity_id}"
    digest = hashlib.sha256(identity.encode()).hexdigest()
    # 16 hex bytes (64 bits) is plenty for collision-freeness across any
    # realistic workflow population and fits comfortably in vendor key
    # length limits.
    return f"{PREFIX}{digest[:32]}"
