"""Tests for concurrency keys — pre-emptive cluster-wide cap on in-flight
workflow.run invocations sharing the same key.

Mirrors core/go/workflow/concurrency_test.go.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import opendal
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.storage import (
    CLAIM_RECORD_SCHEMA_VERSION,
    DEFAULT_NAMESPACE,
    ClaimKey,
    OpenDALStore,
    WorkflowKey,
)
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import (
    CONCURRENCY_WORKFLOW_ID,
    ConcurrencyBusyError,
    Options,
    Workflow,
    _concurrency_owner_id,
    run,
)


@pytest.fixture
def store(tmp_path):
    return OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))


async def _identity(workflow: Workflow, request: StringValue) -> StringValue:
    return StringValue(value="ok:" + request.value)


async def test_concurrency_acquire_when_free(store):
    """Free slot pool → workflow runs to completion; slot released."""
    options = Options(
        workflow_id="wf",
        run_id="r-1",
        code_version="test",
        concurrency_key="vendor:test",
        concurrency_limit=3,
    )
    result = await run(store, options, StringValue(value="x"), StringValue, _identity)
    assert result.value == "ok:x"

    # Slot must be released on completion.
    slot_key = ClaimKey(
        namespace=DEFAULT_NAMESPACE,
        workflow_id=CONCURRENCY_WORKFLOW_ID,
        run_id="vendor:test",
        claim_id="slot:0",
    )
    record = await store.get_claim(slot_key)
    assert record is None, "slot:0 must be released after workflow completion"


async def test_concurrency_busy_when_full(store):
    """Pre-fill all slots with other-owner claims; new workflow gets busy
    and no IN_PROGRESS record is written (no observable side effect)."""
    for i in range(2):
        now_ts = Timestamp()
        now_ts.GetCurrentTime()
        expires = Timestamp()
        expires.FromDatetime(datetime.now(UTC) + timedelta(minutes=15))
        slot_key = ClaimKey(
            namespace=DEFAULT_NAMESPACE,
            workflow_id=CONCURRENCY_WORKFLOW_ID,
            run_id="vendor:test",
            claim_id=f"slot:{i}",
        )
        claim = temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=slot_key.to_proto(),
            owner_id="other-workflow:other-run",
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_CONCURRENCY_KEY,
            resource_id="vendor:test",
            lease_expires_at=expires,
            created_at=now_ts,
            heartbeat_at=now_ts,
        )
        assert await store.try_create_claim(claim) is True

    options = Options(
        workflow_id="wf",
        run_id="r-1",
        code_version="test",
        concurrency_key="vendor:test",
        concurrency_limit=2,
    )
    executed = []

    async def body(workflow: Workflow, request: StringValue) -> StringValue:
        executed.append(True)
        return StringValue(value="ok")

    with pytest.raises(ConcurrencyBusyError):
        await run(store, options, StringValue(value="x"), StringValue, body)
    assert executed == [], "body must not execute when busy"

    # No IN_PROGRESS record should exist — busy is a no-side-effect condition.
    workflow_record = await store.get_workflow(WorkflowKey(workflow_id="wf", run_id="r-1"))
    assert workflow_record is None


async def test_concurrency_released_on_failure(store):
    """Slot released even when workflow body raises."""
    options = Options(
        workflow_id="wf",
        run_id="r-failed",
        code_version="test",
        concurrency_key="vendor:test",
        concurrency_limit=2,
    )

    async def explody(workflow: Workflow, request: StringValue) -> StringValue:
        raise RuntimeError("body failed")

    with pytest.raises(RuntimeError, match="body failed"):
        await run(store, options, StringValue(value="x"), StringValue, explody)

    slot_key = ClaimKey(
        namespace=DEFAULT_NAMESPACE,
        workflow_id=CONCURRENCY_WORKFLOW_ID,
        run_id="vendor:test",
        claim_id="slot:0",
    )
    record = await store.get_claim(slot_key)
    assert record is None, "slot must be released after workflow failure"


async def test_concurrency_owner_reacquires_stale_slot(store):
    """Crashed-and-restarted workflow re-acquires its own stale slot rather
    than consuming a second one."""
    owner_id = _concurrency_owner_id("wf", "r-1")
    slot_key = ClaimKey(
        namespace=DEFAULT_NAMESPACE,
        workflow_id=CONCURRENCY_WORKFLOW_ID,
        run_id="vendor:test",
        claim_id="slot:0",
    )
    now_ts = Timestamp()
    now_ts.GetCurrentTime()
    expires = Timestamp()
    expires.FromDatetime(datetime.now(UTC) + timedelta(minutes=15))
    await store.try_create_claim(
        temporaless_pb2.ClaimRecord(
            schema_version=CLAIM_RECORD_SCHEMA_VERSION,
            key=slot_key.to_proto(),
            owner_id=owner_id,
            resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_CONCURRENCY_KEY,
            lease_expires_at=expires,
            created_at=now_ts,
            heartbeat_at=now_ts,
        )
    )

    options = Options(
        workflow_id="wf",
        run_id="r-1",
        code_version="test",
        concurrency_key="vendor:test",
        concurrency_limit=2,
    )

    async def body(workflow: Workflow, request: StringValue) -> StringValue:
        # Inside body, slot:1 must NOT be held — we reused slot:0.
        slot1 = ClaimKey(
            namespace=DEFAULT_NAMESPACE,
            workflow_id=CONCURRENCY_WORKFLOW_ID,
            run_id="vendor:test",
            claim_id="slot:1",
        )
        existing = await store.get_claim(slot1)
        assert existing is None, "slot:1 should not be held"
        return StringValue(value="ok")

    await run(store, options, StringValue(value="x"), StringValue, body)


async def test_concurrency_validation_paired_rejects_unbalanced():
    """protovalidate's paired CEL rejects key-without-limit and vice versa."""
    from protovalidate import ValidationError

    from temporaless.workflow import normalized_workflow_options

    for opts in (
        Options(workflow_id="w", run_id="r", concurrency_key="x", concurrency_limit=0),
        Options(workflow_id="w", run_id="r", concurrency_key="", concurrency_limit=5),
    ):
        with pytest.raises(ValidationError):
            normalized_workflow_options(opts)


async def test_concurrency_multiple_workflows_obey_limit(store):
    """N concurrent workflows with limit=K observe max-in-flight <= K."""
    limit = 2
    total = 5
    gate = asyncio.Event()
    inflight = [0]
    max_inflight = [0]

    async def body(workflow: Workflow, request: StringValue) -> StringValue:
        inflight[0] += 1
        max_inflight[0] = max(max_inflight[0], inflight[0])
        await gate.wait()
        inflight[0] -= 1
        return StringValue(value="ok")

    async def run_one(i: int) -> str:
        try:
            await run(
                store,
                Options(
                    workflow_id="wf",
                    run_id=f"r-{i}",
                    code_version="test",
                    concurrency_key="vendor:test",
                    concurrency_limit=limit,
                ),
                StringValue(value="x"),
                StringValue,
                body,
            )
            return "ok"
        except ConcurrencyBusyError:
            return "busy"

    tasks = [asyncio.create_task(run_one(i)) for i in range(total)]
    await asyncio.sleep(0.1)
    gate.set()
    results = await asyncio.gather(*tasks)
    succeeded = sum(1 for r in results if r == "ok")
    busy = sum(1 for r in results if r == "busy")

    assert max_inflight[0] <= limit, f"max_inflight = {max_inflight[0]}, want <= {limit}"
    assert succeeded + busy == total
    assert succeeded > 0
