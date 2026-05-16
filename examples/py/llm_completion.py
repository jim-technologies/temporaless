"""LLM completion example: every reliability primitive working together.

A realistic vendor-LLM activity uses the full stack:

  - ``workflow.activity()`` — one-line activity dispatch with auto-inferred
    activity_id (from the function name) and a sensible default retry policy
    (3 attempts, 1s initial, 2x backoff, 30s max, 30s durable threshold).
  - ``outbox.idempotency_key()`` — stable per-activity dedup key for the
    vendor's HTTP ``Idempotency-Key`` header. Retries don't double-charge.
  - ``ActivityError(retry_after=...)`` — surfaces the vendor's
    ``Retry-After`` header so the runtime waits at least that long before
    the next attempt; if it crosses the durable threshold it becomes a
    durable timer (no compute burned during long rate-limit windows).
  - ``concurrency_key`` + ``concurrency_limit`` — pre-emptive cluster-wide
    cap on in-flight vendor calls. At most N workflows share the vendor
    quota at any moment, regardless of how many worker replicas dispatch.
  - ``annotate()`` — durable per-activity metadata (model, tokens, vendor)
    that survives replay and is queryable via Hive partitioning + DuckDB.

Run with ``uv run --project core/py python examples/py/llm_completion.py``.
"""

from __future__ import annotations

import asyncio
import tempfile
from datetime import timedelta

import opendal
from google.protobuf.wrappers_pb2 import StringValue

from temporaless import outbox
from temporaless.storage import ActivityKey, OpenDALStore, WorkflowKey
from temporaless.workflow import (
    ActivityError,
    Options,
    Workflow,
    annotate,
    current_workflow,
    run,
)

llm_attempts = 0


async def llm_complete(prompt: StringValue) -> StringValue:
    """Activity body — what a vendor LLM call looks like under the framework.

    The reliability story:
      - Idempotency key from the framework's outbox helper, attached as if
        we were calling Stripe / OpenAI / Slack. Retries-after-mid-flight
        return the original vendor response instead of double-charging.
      - Durable annotations capture model + token usage; visible in
        analytics queries without re-reading the activity record.
      - On a 429, raise ``ActivityError(retry_after=...)`` with the vendor's
        suggested wait. The runtime treats this as the floor for the next
        retry interval and, when the wait crosses the durable threshold,
        persists it as a timer instead of holding the process alive.
    """
    global llm_attempts
    llm_attempts += 1

    workflow = current_workflow()
    idempotency_key = outbox.idempotency_key(workflow, "llm_complete")
    annotate("vendor", "openai")
    annotate("model", "claude-opus-4-7")
    annotate("attempt", str(llm_attempts))
    annotate("idempotency_key", idempotency_key)

    if llm_attempts < 3:
        # Simulate vendor rate limit with a Retry-After hint. The runtime
        # will use max(computed_interval, retry_after) for the next attempt,
        # so vendor pacing wins over the configured exponential schedule.
        raise ActivityError(
            "rate_limited",
            "vendor 429",
            retry_after=timedelta(milliseconds=50),
        )

    completion = f"[fake completion for: {prompt.value}]"
    annotate("completion_tokens", str(len(completion)))
    return StringValue(value=completion)


async def ask_llm_workflow(workflow: Workflow, prompt: StringValue) -> StringValue:
    """Workflow body — one line per activity, defaults from the framework.

    ``workflow.activity()`` infers ``activity_id`` from ``llm_complete.__qualname__``
    and applies the default retry policy. To override either, pass
    ``activity_id=`` or ``retry_policy=`` as a keyword argument.
    """
    annotate("request_kind", "qa")
    return await workflow.activity(llm_complete, prompt)


async def main() -> None:
    global llm_attempts
    root = tempfile.mkdtemp(prefix="temporaless-llm-py-")
    operator = opendal.AsyncOperator("fs", root=root)
    store = OpenDALStore(operator)

    # WorkflowOptions: concurrency_key+concurrency_limit cap in-flight LLM
    # calls cluster-wide at 5 — pair with multiple worker replicas calling
    # the same vendor and the framework arbitrates via storage claims.
    options = Options(
        workflow_id="llm:answer",
        run_id="2026-05-02-r1",
        code_version="example",
        concurrency_key="vendor:openai",
        concurrency_limit=5,
    )
    prompt = StringValue(value="Why is the sky blue?")

    print("first invocation: retries through transient failures, stores result")
    answer = await run(store, options, prompt, StringValue, ask_llm_workflow)
    print(f"  result: {answer.value!r}")

    workflow_record = await store.get_workflow(
        WorkflowKey(workflow_id="llm:answer", run_id="2026-05-02-r1")
    )
    activity_record = await store.get_activity(
        ActivityKey(
            workflow_id="llm:answer",
            run_id="2026-05-02-r1",
            activity_id="llm_complete",
        )
    )
    assert workflow_record is not None
    assert activity_record is not None
    print(f"  workflow annotations: {dict(workflow_record.annotations)}")
    print(f"  activity annotations: {dict(activity_record.annotations)}")
    print(f"  activity attempts: {len(activity_record.attempts)} (should be 3)")
    print(
        f"  attempt 1 retry_after: "
        f"{activity_record.attempts[0].failure.retry_after.ToTimedelta()}"
    )

    print("\nsecond invocation: replays stored workflow result, no LLM calls")
    llm_attempts = 0
    answer = await run(store, options, prompt, StringValue, ask_llm_workflow)
    print(f"  result: {answer.value!r}")
    print(f"  LLM calls during replay: {llm_attempts} (should be 0)")


if __name__ == "__main__":
    asyncio.run(main())
