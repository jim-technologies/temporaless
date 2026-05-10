"""LLM completion example: retry policy, durable annotations, replay.

Run with ``uv run --project core/py python examples/py/llm_completion.py``.
"""

from __future__ import annotations

import asyncio
import tempfile
from datetime import timedelta

import opendal
from google.protobuf.duration_pb2 import Duration
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.storage import ActivityKey, OpenDALStore, WorkflowKey
from temporaless.workflow import (
    ActivityError,
    ActivityOptions,
    Options,
    RetryPolicy,
    Workflow,
    annotate,
    run,
)

llm_attempts = 0


async def fake_llm_complete(prompt: StringValue) -> StringValue:
    global llm_attempts
    llm_attempts += 1
    annotate("model", "claude-opus-4-7")
    annotate("attempt", str(llm_attempts))
    if llm_attempts < 3:
        raise ActivityError("rate_limited", "vendor 429")
    completion = f"[fake completion for: {prompt.value}]"
    annotate("completion_tokens", str(len(completion)))
    return StringValue(value=completion)


async def ask_llm_workflow(workflow: Workflow, prompt: StringValue) -> StringValue:
    annotate("request_kind", "qa")
    initial = Duration()
    initial.FromTimedelta(timedelta(milliseconds=10))
    return await workflow.execute_activity(
        ActivityOptions(
            activity_id="llm:complete",
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=initial,
                backoff_coefficient=2.0,
                non_retryable_error_codes=["invalid_argument"],
            ),
        ),
        prompt,
        StringValue,
        fake_llm_complete,
    )


async def main() -> None:
    global llm_attempts
    root = tempfile.mkdtemp(prefix="temporaless-llm-py-")
    operator = opendal.AsyncOperator("fs", root=root)
    store = OpenDALStore(operator)

    options = Options(
        workflow_id="llm:answer",
        run_id="2026-05-02-r1",
        code_version="example",
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
            activity_id="llm:complete",
        )
    )
    assert workflow_record is not None
    assert activity_record is not None
    print(f"  workflow annotations: {dict(workflow_record.annotations)}")
    print(f"  activity annotations: {dict(activity_record.annotations)}")
    print(f"  activity attempts: {len(activity_record.attempts)} (should be 3)")

    print("\nsecond invocation: replays stored workflow result, no LLM calls")
    llm_attempts = 0
    answer = await run(store, options, prompt, StringValue, ask_llm_workflow)
    print(f"  result: {answer.value!r}")
    print(f"  LLM calls during replay: {llm_attempts} (should be 0)")


if __name__ == "__main__":
    asyncio.run(main())
