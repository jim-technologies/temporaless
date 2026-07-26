from __future__ import annotations

from datetime import timedelta
from typing import cast

import opendal
import pytest
from google.protobuf import descriptor_pb2, descriptor_pool
from google.protobuf.wrappers_pb2 import StringValue
from protovalidate import ValidationError

import temporaless.visualization as visualization_module
from temporaless.storage import (
    CLAIM_RECORD_SCHEMA_VERSION,
    ClaimKey,
    EventKey,
    OpenDALStore,
    Store,
    WorkflowKey,
    send_event,
)
from temporaless.v1 import temporaless_pb2
from temporaless.visualization import (
    RunInspection,
    inspect_run,
    plan_digest,
    project_workflow_run,
    validate_plan,
    validate_plan_with_descriptors,
    verify_approved_plan,
)
from temporaless.workflow import ActivityOptions, Options, TimerPendingError, Workflow, run


def _node(
    node_id: str,
    kind: temporaless_pb2.WorkflowPlanNodeKind,
) -> temporaless_pb2.WorkflowPlanNode:
    node = temporaless_pb2.WorkflowPlanNode(
        node_id=node_id,
        display_name=node_id,
        kind=kind,
    )
    if kind in (
        temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
        temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_BRANCH,
    ):
        node.operation = f"example.v1.Service.{node_id}"
        node.request_type = "google.protobuf.StringValue"
        node.response_type = "google.protobuf.StringValue"
    return node


def _base_plan() -> temporaless_pb2.WorkflowPlan:
    return temporaless_pb2.WorkflowPlan(
        plan_id="approval",
        revision=1,
        nodes=[
            _node("validate", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY),
            _node("approve", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT),
        ],
        edges=[
            temporaless_pb2.WorkflowPlanEdge(
                source_node_id="validate",
                target_node_id="approve",
                kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONTROL,
            )
        ],
    )


def _invalid_plan(case: str) -> temporaless_pb2.WorkflowPlan:
    plan = _base_plan()
    if case == "duplicate-node":
        plan.nodes.append(_node("validate", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_SLEEP))
    elif case == "unknown-source":
        plan.edges[0].source_node_id = "missing"
    elif case == "unknown-target":
        plan.edges[0].target_node_id = "missing"
    elif case == "duplicate-edge":
        plan.edges.add().CopyFrom(plan.edges[0])
    elif case == "missing-callable-field":
        plan.nodes[0].operation = ""
    elif case == "conditional-label":
        plan.edges[0].kind = temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL
    elif case == "conditional-source":
        plan.edges[0].kind = temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL
        plan.edges[0].label = "approved"
    elif case == "duplicate-conditional-label":
        plan.nodes[0].kind = temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_BRANCH
        plan.nodes.append(_node("reject", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_SLEEP))
        plan.edges[0].kind = temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL
        plan.edges[0].label = "approved"
        plan.edges.add(
            source_node_id="validate",
            target_node_id="reject",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL,
            label="approved",
        )
    elif case == "data-structural-endpoint":
        plan.edges[0].kind = temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_DATA
    elif case == "data-type-mismatch":
        plan.nodes[1].CopyFrom(_node("approve", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY))
        plan.nodes[1].request_type = "google.protobuf.Int32Value"
        plan.edges[0].kind = temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_DATA
    elif case == "data-fan-in":
        plan.nodes[1].CopyFrom(_node("approve", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY))
        plan.nodes.append(_node("review", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY))
        plan.edges[0].kind = temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_DATA
        plan.edges.add(
            source_node_id="review",
            target_node_id="approve",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_DATA,
        )
    elif case == "data-cycle":
        plan.nodes[1].CopyFrom(_node("approve", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY))
        plan.edges[0].kind = temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_DATA
        plan.edges.add(
            source_node_id="approve",
            target_node_id="validate",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_DATA,
        )
    elif case == "loop-back":
        plan.edges[0].kind = temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_LOOP_BACK
    elif case == "forward-cycle":
        plan.edges.add(
            source_node_id="approve",
            target_node_id="validate",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONTROL,
        )
    else:
        raise AssertionError(f"unknown case {case}")
    return plan


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("duplicate-node", "duplicate node_id"),
        ("unknown-source", "unknown source"),
        ("unknown-target", "unknown target"),
        ("duplicate-edge", "duplicate edge"),
        ("missing-callable-field", "requires operation"),
        ("conditional-label", "requires a label"),
        ("conditional-source", "must start at a branch node"),
        ("duplicate-conditional-label", "duplicate conditional label"),
        ("data-structural-endpoint", "must connect callable nodes"),
        ("data-type-mismatch", "incompatible protobuf types"),
        ("data-fan-in", "more than one incoming data edge"),
        ("data-cycle", "contain a cycle"),
        ("loop-back", "must touch a loop node"),
        ("forward-cycle", "contain a cycle"),
    ],
)
def test_validate_plan_rejects_ambiguous_graphs(case: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_plan(_invalid_plan(case))


def test_validate_plan_uses_protovalidate_for_field_rules() -> None:
    with pytest.raises(ValidationError):
        validate_plan(temporaless_pb2.WorkflowPlan())


@pytest.mark.parametrize(
    "case",
    [
        "nodes",
        "edges",
        "annotations",
        "annotation_key",
        "node_annotation_key",
        "description_bytes",
    ],
)
def test_validate_plan_rejects_resource_limit_overflow(case: str) -> None:
    plan = _base_plan()
    if case == "nodes":
        plan.nodes.extend(
            _node(
                f"wait:{index}",
                temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT,
            )
            for index in range(63)
        )
    elif case == "edges":
        for _ in range(128):
            plan.edges.add().CopyFrom(plan.edges[0])
    elif case == "annotations":
        plan.annotations.update({f"key:{index}": "value" for index in range(65)})
    elif case == "annotation_key":
        plan.annotations["__proto__"] = "hidden"
    elif case == "node_annotation_key":
        plan.nodes[0].annotations["__proto__"] = "hidden"
    elif case == "description_bytes":
        plan.nodes[0].description = "😀" * 1025
    else:
        raise AssertionError(f"unknown resource-limit case {case!r}")

    with pytest.raises(ValueError):
        validate_plan(plan)


def test_validate_plan_rejects_collection_overflow_before_protovalidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _base_plan()
    plan.nodes.extend(
        _node(
            f"wait:{index}",
            temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT,
        )
        for index in range(63)
    )

    def unexpected_validation(*args: object, **kwargs: object) -> None:
        raise AssertionError(f"Protovalidate called with {args!r} {kwargs!r}")

    monkeypatch.setattr(visualization_module, "validate", unexpected_validation)
    with pytest.raises(ValueError, match="at most 64 nodes"):
        visualization_module.validate_plan(plan)


def test_validate_plan_accepts_exact_collection_limits() -> None:
    plan = temporaless_pb2.WorkflowPlan(
        plan_id="exact-limits",
        revision=1,
        nodes=[
            _node("loop", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_LOOP),
            *[
                _node(
                    f"wait:{index}",
                    temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT,
                )
                for index in range(1, 64)
            ],
        ],
        annotations={f"plan:{index}": "value" for index in range(64)},
    )
    plan.nodes[0].annotations.update({f"node:{index}": "value" for index in range(32)})
    for index in range(1, 64):
        plan.edges.add(
            source_node_id="loop",
            target_node_id=f"wait:{index}",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_LOOP_BACK,
        )
        plan.edges.add(
            source_node_id=f"wait:{index}",
            target_node_id="loop",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_LOOP_BACK,
        )
    for label in ("first", "second"):
        plan.edges.add(
            source_node_id="loop",
            target_node_id="loop",
            kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_LOOP_BACK,
            label=label,
        )

    validate_plan(plan)


def test_validate_plan_allows_explicit_loop_back_without_forward_cycle() -> None:
    plan = temporaless_pb2.WorkflowPlan(
        plan_id="bounded-loop",
        revision=1,
        nodes=[
            _node("loop", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_LOOP),
            _node("work", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY),
        ],
        edges=[
            temporaless_pb2.WorkflowPlanEdge(
                source_node_id="loop",
                target_node_id="work",
                kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONTROL,
            ),
            temporaless_pb2.WorkflowPlanEdge(
                source_node_id="work",
                target_node_id="loop",
                kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_LOOP_BACK,
            ),
        ],
    )

    validate_plan(plan)


def test_validate_plan_allows_compatible_data_edge() -> None:
    plan = temporaless_pb2.WorkflowPlan(
        plan_id="typed-pipeline",
        revision=1,
        nodes=[
            _node("produce", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY),
            _node("consume", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY),
        ],
        edges=[
            temporaless_pb2.WorkflowPlanEdge(
                source_node_id="produce",
                target_node_id="consume",
                kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_DATA,
            )
        ],
    )

    validate_plan(plan)


def test_plan_digest_matches_cross_sdk_fixture() -> None:
    plan = temporaless_pb2.WorkflowPlan(
        plan_id="approval:export",
        revision=1,
        nodes=[
            temporaless_pb2.WorkflowPlanNode(
                node_id="validate",
                display_name="Validate",
                kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
                operation="exports.v1.ExportService.Validate",
                request_type="exports.v1.ValidateRequest",
                response_type="exports.v1.ValidateResponse",
            ),
            temporaless_pb2.WorkflowPlanNode(
                node_id="approve",
                display_name="Approve",
                kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT,
            ),
        ],
        edges=[
            temporaless_pb2.WorkflowPlanEdge(
                source_node_id="validate",
                target_node_id="approve",
                kind=temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONTROL,
            )
        ],
    )

    assert plan_digest(plan) == "f3ed8cdf8a4aa2fe3d323661dfff0a50c7097aeac1d307784ed2a726810797f0"


def test_plan_digest_matches_unicode_and_numeric_map_key_fixture() -> None:
    annotations = {
        "2": "two",
        "10": "ten",
        "é": "café",
        "😀": "rocket",
    }
    plan = temporaless_pb2.WorkflowPlan(
        plan_id="approval:unicode",
        revision=2,
        annotations=annotations,
        nodes=[
            temporaless_pb2.WorkflowPlanNode(
                node_id="validate",
                display_name="Vérifier 😀",
                kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
                operation="exports.v1.ExportService.Validate",
                request_type="exports.v1.ValidateRequest",
                response_type="exports.v1.ValidateResponse",
                annotations=annotations,
            )
        ],
    )

    assert plan_digest(plan) == "c6f9214a9f270eabf45fc518eeb48faa3ca08628fc105264acdd825ca56f9662"


def test_plan_digest_canonicalizes_maps_but_retains_repeated_order() -> None:
    left = _base_plan()
    left.annotations["z"] = "last"
    left.annotations["a"] = "first"
    left.nodes[0].annotations["z"] = "last"
    left.nodes[0].annotations["a"] = "first"

    right = _base_plan()
    right.annotations["a"] = "first"
    right.annotations["z"] = "last"
    right.nodes[0].annotations["a"] = "first"
    right.nodes[0].annotations["z"] = "last"

    assert plan_digest(left) == plan_digest(right)

    reordered = temporaless_pb2.WorkflowPlan()
    reordered.CopyFrom(right)
    reordered.nodes.reverse()
    assert plan_digest(left) != plan_digest(reordered)


_GET_WORKFLOW_OPERATION = "temporaless.v1.RecordStoreService.GetWorkflow"


def _descriptor_plan() -> temporaless_pb2.WorkflowPlan:
    plan = _base_plan()
    plan.nodes[0].operation = _GET_WORKFLOW_OPERATION
    plan.nodes[0].request_type = "temporaless.v1.GetWorkflowRequest"
    plan.nodes[0].response_type = "temporaless.v1.GetWorkflowResponse"
    return plan


def test_validate_plan_with_descriptors_accepts_allowlisted_unary_rpc() -> None:
    validate_plan_with_descriptors(
        _descriptor_plan(),
        pool=descriptor_pool.Default(),
        allowed_operations={_GET_WORKFLOW_OPERATION},
    )


def test_validate_plan_with_descriptors_accepts_structural_plan_with_empty_allowlist() -> None:
    validate_plan_with_descriptors(
        temporaless_pb2.WorkflowPlan(
            plan_id="structural",
            revision=1,
            nodes=[
                _node(
                    "approval",
                    temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT,
                )
            ],
        ),
        pool=descriptor_pool.Default(),
        allowed_operations=set(),
    )


@pytest.mark.parametrize(
    ("pool", "allowed_operations", "message"),
    [
        (
            cast(descriptor_pool.DescriptorPool, None),
            {_GET_WORKFLOW_OPERATION},
            "descriptor pool is required",
        ),
        (
            descriptor_pool.Default(),
            cast(set[str], None),
            "operation allowlist is required",
        ),
    ],
)
def test_validate_plan_with_descriptors_rejects_missing_policy_after_type_erasure(
    pool: descriptor_pool.DescriptorPool,
    allowed_operations: set[str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validate_plan_with_descriptors(
            _descriptor_plan(),
            pool=pool,
            allowed_operations=allowed_operations,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("operation", "temporaless.v1.RecordStoreService.PutWorkflow", "not allowlisted"),
        ("request_type", "temporaless.v1.PutWorkflowRequest", "does not match protobuf RPC input"),
        (
            "response_type",
            "temporaless.v1.PutWorkflowResponse",
            "does not match protobuf RPC output",
        ),
    ],
)
def test_validate_plan_with_descriptors_rejects_callable_mismatch(
    field: str,
    value: str,
    message: str,
) -> None:
    plan = _descriptor_plan()
    setattr(plan.nodes[0], field, value)

    with pytest.raises(ValueError, match=message):
        validate_plan_with_descriptors(
            plan,
            pool=descriptor_pool.Default(),
            allowed_operations={_GET_WORKFLOW_OPERATION},
        )


@pytest.mark.parametrize(
    ("operation", "message"),
    [
        ("temporaless.v1.RecordStoreService", "does not resolve to a protobuf RPC"),
        ("/temporaless.v1.RecordStoreService/GetWorkflow", "does not resolve to a protobuf RPC"),
        ("RecordStoreService.GetWorkflow", "canonical package.Service.Method"),
    ],
)
def test_validate_plan_with_descriptors_rejects_noncanonical_operation(
    operation: str,
    message: str,
) -> None:
    plan = _descriptor_plan()
    plan.nodes[0].operation = operation

    with pytest.raises(ValueError, match=message):
        validate_plan_with_descriptors(
            plan,
            pool=descriptor_pool.Default(),
            allowed_operations={operation},
        )


def test_validate_plan_with_descriptors_ignores_unused_allowlist_entries() -> None:
    validate_plan_with_descriptors(
        _descriptor_plan(),
        pool=descriptor_pool.Default(),
        allowed_operations={_GET_WORKFLOW_OPERATION, "stale.invalid.Entry"},
    )


@pytest.mark.parametrize("location", ["plan", "node", "edge"])
def test_validate_plan_with_descriptors_rejects_unknown_protobuf_fields(
    location: str,
) -> None:
    plan = _descriptor_plan()
    target = {
        "plan": plan,
        "node": plan.nodes[0],
        "edge": plan.edges[0],
    }[location]
    target.MergeFromString(b"\xa0\x06\x01")

    with pytest.raises(ValueError, match="unknown protobuf fields"):
        validate_plan_with_descriptors(
            plan,
            pool=descriptor_pool.Default(),
            allowed_operations={_GET_WORKFLOW_OPERATION},
        )


@pytest.mark.parametrize(
    ("client_streaming", "server_streaming"),
    [(True, False), (False, True)],
)
def test_validate_plan_with_descriptors_rejects_streaming_rpc(
    client_streaming: bool,
    server_streaming: bool,
) -> None:
    file_descriptor = descriptor_pb2.FileDescriptorProto(
        name="streaming/v1/streaming.proto",
        package="streaming.v1",
        syntax="proto3",
    )
    file_descriptor.message_type.add(name="UploadRequest")
    file_descriptor.message_type.add(name="UploadResponse")
    service = file_descriptor.service.add(name="StreamService")
    service.method.add(
        name="Upload",
        input_type=".streaming.v1.UploadRequest",
        output_type=".streaming.v1.UploadResponse",
        client_streaming=client_streaming,
        server_streaming=server_streaming,
    )
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_descriptor)
    operation = "streaming.v1.StreamService.Upload"
    plan = temporaless_pb2.WorkflowPlan(
        plan_id="streaming",
        revision=1,
        nodes=[
            temporaless_pb2.WorkflowPlanNode(
                node_id="upload",
                display_name="Upload",
                kind=temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
                operation=operation,
                request_type="streaming.v1.UploadRequest",
                response_type="streaming.v1.UploadResponse",
            )
        ],
    )

    with pytest.raises(ValueError, match="must be a unary protobuf RPC"):
        validate_plan_with_descriptors(
            plan,
            pool=pool,
            allowed_operations={operation},
        )


@pytest.mark.parametrize("field", ["operation", "request_type", "response_type"])
def test_validate_plan_with_descriptors_rejects_rpc_metadata_on_non_callable_node(
    field: str,
) -> None:
    plan = _descriptor_plan()
    setattr(plan.nodes[1], field, "unexpected")

    with pytest.raises(ValueError, match="non-callable workflow plan node"):
        validate_plan_with_descriptors(
            plan,
            pool=descriptor_pool.Default(),
            allowed_operations={_GET_WORKFLOW_OPERATION},
        )


@pytest.mark.parametrize(
    "approved_digest",
    [
        "",
        "0" * 63,
        "0" * 65,
        "G" * 64,
        "A" * 64,
        f"sha256:{'0' * 64}",
    ],
)
def test_verify_approved_plan_rejects_noncanonical_digest(approved_digest: str) -> None:
    with pytest.raises(ValueError, match="exactly 64 lowercase hexadecimal"):
        verify_approved_plan(
            _descriptor_plan(),
            approved_digest,
            pool=descriptor_pool.Default(),
            allowed_operations={_GET_WORKFLOW_OPERATION},
        )


def test_verify_approved_plan_returns_matching_digest_and_rejects_mismatch() -> None:
    plan = _descriptor_plan()
    digest = plan_digest(plan)

    assert (
        verify_approved_plan(
            plan,
            digest,
            pool=descriptor_pool.Default(),
            allowed_operations={_GET_WORKFLOW_OPERATION},
        )
        == digest
    )
    with pytest.raises(ValueError, match="does not match the approved digest"):
        verify_approved_plan(
            plan,
            "0" * 64,
            pool=descriptor_pool.Default(),
            allowed_operations={_GET_WORKFLOW_OPERATION},
        )


def _claim(
    key: WorkflowKey,
    claim_id: str,
    resource_type: temporaless_pb2.ClaimResourceType,
    resource_id: str,
) -> temporaless_pb2.ClaimRecord:
    return temporaless_pb2.ClaimRecord(
        key=temporaless_pb2.ClaimKey(
            namespace=key.namespace,
            workflow_id=key.workflow_id,
            run_id=key.run_id,
            claim_id=claim_id,
        ),
        resource_type=resource_type,
        resource_id=resource_id,
    )


class _OneRecordStore:
    def __init__(
        self,
        record: (
            temporaless_pb2.WorkflowRecord
            | temporaless_pb2.ActivityRecord
            | temporaless_pb2.TimerRecord
            | temporaless_pb2.EventRecord
            | temporaless_pb2.ClaimRecord
        ),
    ) -> None:
        self.record = record

    async def get_workflow(
        self,
        _key: WorkflowKey,
    ) -> temporaless_pb2.WorkflowRecord | None:
        if isinstance(self.record, temporaless_pb2.WorkflowRecord):
            return self.record
        return None

    async def list_activities(
        self,
        _key: WorkflowKey,
    ) -> list[temporaless_pb2.ActivityRecord]:
        if isinstance(self.record, temporaless_pb2.ActivityRecord):
            return [self.record]
        return []

    async def list_timers(
        self,
        _key: WorkflowKey,
        _status: temporaless_pb2.TimerStatus,
    ) -> list[temporaless_pb2.TimerRecord]:
        if isinstance(self.record, temporaless_pb2.TimerRecord):
            return [self.record]
        return []

    async def list_events(
        self,
        _key: WorkflowKey,
    ) -> list[temporaless_pb2.EventRecord]:
        if isinstance(self.record, temporaless_pb2.EventRecord):
            return [self.record]
        return []

    async def list_claims(
        self,
        _key: WorkflowKey,
    ) -> list[temporaless_pb2.ClaimRecord]:
        if isinstance(self.record, temporaless_pb2.ClaimRecord):
            return [self.record]
        return []


def _record_for_run(
    record_kind: str,
    key: WorkflowKey,
    *,
    include_key: bool,
    run_id: str | None = None,
) -> (
    temporaless_pb2.WorkflowRecord
    | temporaless_pb2.ActivityRecord
    | temporaless_pb2.TimerRecord
    | temporaless_pb2.EventRecord
    | temporaless_pb2.ClaimRecord
):
    actual_run_id = key.run_id if run_id is None else run_id
    if record_kind == "workflow":
        if not include_key:
            return temporaless_pb2.WorkflowRecord()
        return temporaless_pb2.WorkflowRecord(
            key=temporaless_pb2.WorkflowKey(
                namespace=key.namespace,
                workflow_id=key.workflow_id,
                run_id=actual_run_id,
            )
        )
    if record_kind == "activity":
        if not include_key:
            return temporaless_pb2.ActivityRecord()
        return temporaless_pb2.ActivityRecord(
            key=temporaless_pb2.ActivityKey(
                namespace=key.namespace,
                workflow_id=key.workflow_id,
                run_id=actual_run_id,
                activity_id="validate",
            )
        )
    if record_kind == "timer":
        if not include_key:
            return temporaless_pb2.TimerRecord()
        return temporaless_pb2.TimerRecord(
            key=temporaless_pb2.TimerKey(
                namespace=key.namespace,
                workflow_id=key.workflow_id,
                run_id=actual_run_id,
                timer_id="delay",
            )
        )
    if record_kind == "event":
        if not include_key:
            return temporaless_pb2.EventRecord()
        return temporaless_pb2.EventRecord(
            key=temporaless_pb2.EventKey(
                namespace=key.namespace,
                workflow_id=key.workflow_id,
                run_id=actual_run_id,
                event_id="approve",
            )
        )
    if record_kind == "claim":
        if not include_key:
            return temporaless_pb2.ClaimRecord()
        return temporaless_pb2.ClaimRecord(
            key=temporaless_pb2.ClaimKey(
                namespace=key.namespace,
                workflow_id=key.workflow_id,
                run_id=actual_run_id,
                claim_id="activity:validate",
            )
        )
    raise AssertionError(f"unknown record kind {record_kind}")


@pytest.mark.parametrize(
    "record_kind",
    ["workflow", "activity", "timer", "event", "claim"],
)
@pytest.mark.parametrize(
    ("include_key", "run_id", "message"),
    [
        (False, None, "key is required"),
        (True, "other-run", "key does not match inspection run"),
    ],
)
async def test_inspect_run_rejects_unscoped_or_cross_run_records(
    record_kind: str,
    include_key: bool,
    run_id: str | None,
    message: str,
) -> None:
    key = WorkflowKey(workflow_id="workflow", run_id="run")
    record = _record_for_run(record_kind, key, include_key=include_key, run_id=run_id)
    store = cast(Store, _OneRecordStore(record))

    with pytest.raises(ValueError, match=rf"{record_kind} record {message}"):
        await inspect_run(store, key)


def test_project_uses_kind_aware_exact_evidence_and_retains_every_other_record() -> None:
    inspection_key = WorkflowKey(workflow_id="workflow", run_id="run")
    plan = temporaless_pb2.WorkflowPlan(
        plan_id="projection",
        revision=1,
        nodes=[
            _node("sleep", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_SLEEP),
            _node("activity", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY),
            _node("event", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT),
            _node("fan", temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_FAN_OUT),
        ],
    )
    inspection = RunInspection(
        key=inspection_key,
        workflow=temporaless_pb2.WorkflowRecord(key=inspection_key.to_proto()),
        activities=(
            temporaless_pb2.ActivityRecord(
                key=temporaless_pb2.ActivityKey(
                    namespace=inspection_key.namespace,
                    workflow_id=inspection_key.workflow_id,
                    run_id=inspection_key.run_id,
                    activity_id="sleep",
                )
            ),
            temporaless_pb2.ActivityRecord(
                key=temporaless_pb2.ActivityKey(
                    namespace=inspection_key.namespace,
                    workflow_id=inspection_key.workflow_id,
                    run_id=inspection_key.run_id,
                    activity_id="activity",
                )
            ),
            temporaless_pb2.ActivityRecord(
                key=temporaless_pb2.ActivityKey(
                    namespace=inspection_key.namespace,
                    workflow_id=inspection_key.workflow_id,
                    run_id=inspection_key.run_id,
                    activity_id="z",
                )
            ),
        ),
        timers=(
            temporaless_pb2.TimerRecord(
                key=temporaless_pb2.TimerKey(
                    namespace=inspection_key.namespace,
                    workflow_id=inspection_key.workflow_id,
                    run_id=inspection_key.run_id,
                    timer_id="activity",
                )
            ),
            temporaless_pb2.TimerRecord(
                key=temporaless_pb2.TimerKey(
                    namespace=inspection_key.namespace,
                    workflow_id=inspection_key.workflow_id,
                    run_id=inspection_key.run_id,
                    timer_id="retry:activity",
                ),
                timer_kind=temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY,
                retry_activity_id="activity",
            ),
            temporaless_pb2.TimerRecord(
                key=temporaless_pb2.TimerKey(
                    namespace=inspection_key.namespace,
                    workflow_id=inspection_key.workflow_id,
                    run_id=inspection_key.run_id,
                    timer_id="event",
                ),
                timer_kind=temporaless_pb2.TIMER_KIND_POLL,
            ),
            temporaless_pb2.TimerRecord(
                key=temporaless_pb2.TimerKey(
                    namespace=inspection_key.namespace,
                    workflow_id=inspection_key.workflow_id,
                    run_id=inspection_key.run_id,
                    timer_id="sleep",
                ),
                timer_kind=temporaless_pb2.TIMER_KIND_SLEEP,
            ),
        ),
        events=(
            temporaless_pb2.EventRecord(
                key=temporaless_pb2.EventKey(
                    namespace=inspection_key.namespace,
                    workflow_id=inspection_key.workflow_id,
                    run_id=inspection_key.run_id,
                    event_id="activity",
                )
            ),
            temporaless_pb2.EventRecord(
                key=temporaless_pb2.EventKey(
                    namespace=inspection_key.namespace,
                    workflow_id=inspection_key.workflow_id,
                    run_id=inspection_key.run_id,
                    event_id="event",
                )
            ),
        ),
        claims=(
            _claim(
                inspection_key,
                "activity-claim",
                temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
                "activity",
            ),
            _claim(
                inspection_key,
                "event-timer-claim",
                temporaless_pb2.CLAIM_RESOURCE_TYPE_TIMER,
                "event",
            ),
            _claim(
                inspection_key,
                "timer-claim",
                temporaless_pb2.CLAIM_RESOURCE_TYPE_TIMER,
                "sleep",
            ),
            _claim(
                inspection_key,
                "structural-claim",
                temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
                "fan",
            ),
            _claim(
                inspection_key,
                "workflow-claim",
                temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW,
                "activity",
            ),
        ),
        claims_inspected=True,
    )

    projected = project_workflow_run(plan, inspection)

    assert [node.node.node_id for node in projected.nodes] == [
        "activity",
        "event",
        "fan",
        "sleep",
    ]
    by_id = {node.node.node_id: node for node in projected.nodes}
    assert by_id["activity"].activity is inspection.activities[1]
    assert [record.key.timer_id for record in by_id["activity"].timers] == ["retry:activity"]
    assert by_id["activity"].event is None
    assert [record.key.claim_id for record in by_id["activity"].claims] == ["activity-claim"]
    assert [record.key.timer_id for record in by_id["sleep"].timers] == ["sleep"]
    assert [record.key.claim_id for record in by_id["sleep"].claims] == ["timer-claim"]
    assert by_id["event"].event is inspection.events[1]
    assert [record.key.timer_id for record in by_id["event"].timers] == ["event"]
    assert [record.key.claim_id for record in by_id["event"].claims] == ["event-timer-claim"]
    assert by_id["fan"].activity is None
    assert by_id["fan"].timers == ()
    assert by_id["fan"].event is None
    assert not hasattr(by_id["fan"], "status")
    assert [record.key.activity_id for record in projected.unplanned_activities] == ["sleep", "z"]
    assert [record.key.timer_id for record in projected.unplanned_timers] == ["activity"]
    assert [record.key.event_id for record in projected.unplanned_events] == ["activity"]
    assert [record.key.claim_id for record in projected.run_claims] == ["workflow-claim"]
    assert [record.key.claim_id for record in projected.unplanned_claims] == ["structural-claim"]
    assert projected.claims_inspected is True


@pytest.mark.parametrize(
    ("include_key", "run_id", "message"),
    [
        (False, None, "activity record key is required"),
        (True, "other-run", "activity record key does not match inspection run"),
    ],
)
def test_project_workflow_run_revalidates_manual_inspection_keys(
    include_key: bool,
    run_id: str | None,
    message: str,
) -> None:
    key = WorkflowKey(workflow_id="workflow", run_id="run")
    activity = _record_for_run("activity", key, include_key=include_key, run_id=run_id)
    assert isinstance(activity, temporaless_pb2.ActivityRecord)
    inspection = RunInspection(
        key=key,
        workflow=None,
        activities=(activity,),
        timers=(),
        events=(),
        claims=(),
        claims_inspected=False,
    )

    with pytest.raises(ValueError, match=message):
        project_workflow_run(_base_plan(), inspection)


async def test_inspect_run_reads_and_sorts_opendal_run_evidence(tmp_path) -> None:
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(tmp_path)))
    key = WorkflowKey(workflow_id="visual", run_id="run")
    options = Options(
        workflow_id=key.workflow_id,
        run_id=key.run_id,
    )

    async def activity(request: StringValue) -> StringValue:
        return request

    async def workflow(workflow: Workflow, request: StringValue) -> StringValue:
        for activity_id in ("z-activity", "a-activity"):
            await workflow.execute_activity(
                ActivityOptions(activity_id=activity_id),
                request,
                StringValue,
                activity,
            )
        await workflow.sleep("sleep", timedelta(hours=1))
        return request

    with pytest.raises(TimerPendingError):
        await run(store, options, StringValue(value="request"), StringValue, workflow)
    for event_id in ("z-event", "a-event"):
        await send_event(
            store,
            EventKey(
                workflow_id=key.workflow_id,
                run_id=key.run_id,
                event_id=event_id,
            ),
            StringValue(value=event_id),
        )
    for claim_id in ("z-claim", "a-claim"):
        assert await store.try_create_claim(
            temporaless_pb2.ClaimRecord(
                schema_version=CLAIM_RECORD_SCHEMA_VERSION,
                key=ClaimKey(
                    workflow_id=key.workflow_id,
                    run_id=key.run_id,
                    claim_id=claim_id,
                ).to_proto(),
                owner_id="inspector",
                resource_type=temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY,
                resource_id="a-activity",
            )
        )

    inspected = await inspect_run(store, key)

    assert inspected.key == key
    assert inspected.workflow is not None
    assert inspected.workflow.status == temporaless_pb2.WORKFLOW_STATUS_IN_PROGRESS
    assert [record.key.activity_id for record in inspected.activities] == [
        "a-activity",
        "z-activity",
    ]
    assert [record.key.timer_id for record in inspected.timers] == ["sleep"]
    assert [record.key.event_id for record in inspected.events] == ["a-event", "z-event"]
    assert [record.key.claim_id for record in inspected.claims] == ["a-claim", "z-claim"]
    assert inspected.claims_inspected is True
