"""Conservative workflow-plan validation and record projection.

``WorkflowPlan`` is display metadata, not an execution language.  This module
joins its caller-owned node IDs to authoritative run-scoped protobuf records;
it deliberately does not infer topology from code or invent running/skipped
states when no durable evidence exists.
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from protovalidate import validate

from temporaless.storage import Store, WorkflowKey
from temporaless.v1 import temporaless_pb2


@runtime_checkable
class ClaimLister(Protocol):
    """Optional run-scoped claim evidence source."""

    async def list_claims(
        self,
        key: WorkflowKey,
    ) -> list[temporaless_pb2.ClaimRecord]: ...


@dataclass(frozen=True, slots=True)
class RunInspection:
    """Authoritative records currently stored for one workflow run."""

    key: WorkflowKey
    workflow: temporaless_pb2.WorkflowRecord | None
    activities: tuple[temporaless_pb2.ActivityRecord, ...]
    timers: tuple[temporaless_pb2.TimerRecord, ...]
    events: tuple[temporaless_pb2.EventRecord, ...]
    claims: tuple[temporaless_pb2.ClaimRecord, ...]
    claims_inspected: bool


@dataclass(frozen=True, slots=True)
class NodeProjection:
    """Durable records whose application-owned ID exactly matches one node."""

    node: temporaless_pb2.WorkflowPlanNode
    activity: temporaless_pb2.ActivityRecord | None = None
    timers: tuple[temporaless_pb2.TimerRecord, ...] = ()
    event: temporaless_pb2.EventRecord | None = None
    claims: tuple[temporaless_pb2.ClaimRecord, ...] = ()


@dataclass(frozen=True, slots=True)
class RunProjection:
    """A plan overlaid with evidence, retaining every unplanned record."""

    plan: temporaless_pb2.WorkflowPlan
    workflow: temporaless_pb2.WorkflowRecord | None
    key: WorkflowKey
    nodes: tuple[NodeProjection, ...]
    run_claims: tuple[temporaless_pb2.ClaimRecord, ...]
    unplanned_activities: tuple[temporaless_pb2.ActivityRecord, ...]
    unplanned_timers: tuple[temporaless_pb2.TimerRecord, ...]
    unplanned_events: tuple[temporaless_pb2.EventRecord, ...]
    unplanned_claims: tuple[temporaless_pb2.ClaimRecord, ...]
    claims_inspected: bool


async def inspect_run(store: Store, key: WorkflowKey) -> RunInspection:
    """Read one run's workflow, activity, timer, event, and optional claim records."""

    key.validate()
    claims_inspected = isinstance(store, ClaimLister)

    async def no_claims() -> list[temporaless_pb2.ClaimRecord]:
        return []

    claim_read = store.list_claims(key) if claims_inspected else no_claims()
    workflow_result, activities_result, timers_result, events_result, claims = await asyncio.gather(
        store.get_workflow(key),
        store.list_activities(key),
        store.list_timers(key, temporaless_pb2.TIMER_STATUS_UNSPECIFIED),
        store.list_events(key),
        claim_read,
    )
    inspection = RunInspection(
        key=key,
        workflow=workflow_result,
        activities=tuple(sorted(activities_result, key=lambda record: record.key.activity_id)),
        timers=tuple(sorted(timers_result, key=lambda record: record.key.timer_id)),
        events=tuple(sorted(events_result, key=lambda record: record.key.event_id)),
        claims=tuple(sorted(claims, key=lambda record: record.key.claim_id)),
        claims_inspected=claims_inspected,
    )
    _validate_inspection_keys(inspection)
    return inspection


def validate_plan(plan: temporaless_pb2.WorkflowPlan) -> None:
    """Validate protobuf constraints and unambiguous visual-graph semantics."""

    validate(plan)

    nodes: dict[str, temporaless_pb2.WorkflowPlanNode] = {}
    for node in plan.nodes:
        if node.node_id in nodes:
            raise ValueError(f"workflow plan has duplicate node_id {node.node_id!r}")
        nodes[node.node_id] = node

        callable_node = node.kind in (
            temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
            temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_BRANCH,
        )
        callable_fields = (node.operation, node.request_type, node.response_type)
        if callable_node and any(not value for value in callable_fields):
            raise ValueError(
                f"callable workflow plan node {node.node_id!r} requires operation, "
                "request_type, and response_type"
            )

    seen_edges: set[tuple[str, str, int, str]] = set()
    conditional_labels: set[tuple[str, str]] = set()
    data_targets: set[str] = set()
    forward_adjacency = {node_id: set[str]() for node_id in nodes}
    forward_indegree = {node_id: 0 for node_id in nodes}

    for edge in plan.edges:
        source = nodes.get(edge.source_node_id)
        if source is None:
            raise ValueError(
                f"workflow plan edge references unknown source node {edge.source_node_id!r}"
            )
        target = nodes.get(edge.target_node_id)
        if target is None:
            raise ValueError(
                f"workflow plan edge references unknown target node {edge.target_node_id!r}"
            )

        edge_identity = (
            edge.source_node_id,
            edge.target_node_id,
            edge.kind,
            edge.label,
        )
        if edge_identity in seen_edges:
            raise ValueError(
                f"workflow plan has duplicate edge {edge.source_node_id!r}->{edge.target_node_id!r}"
            )
        seen_edges.add(edge_identity)

        if edge.kind == temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL and not edge.label:
            raise ValueError("conditional workflow plan edge requires a label")
        if edge.kind == temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL:
            if source.kind != temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_BRANCH:
                raise ValueError("conditional workflow plan edge must start at a branch node")
            label_identity = (edge.source_node_id, edge.label)
            if label_identity in conditional_labels:
                raise ValueError(
                    f"branch node {edge.source_node_id!r} has duplicate conditional "
                    f"label {edge.label!r}"
                )
            conditional_labels.add(label_identity)

        if edge.kind == temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_LOOP_BACK:
            if (
                source.kind != temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_LOOP
                and target.kind != temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_LOOP
            ):
                raise ValueError("loop-back workflow plan edge must touch a loop node")
            continue

        if edge.kind == temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_DATA:
            callable_kinds = (
                temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
                temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_BRANCH,
            )
            if source.kind not in callable_kinds or target.kind not in callable_kinds:
                raise ValueError("data workflow plan edge must connect callable nodes")
            if (
                not source.response_type
                or not target.request_type
                or source.response_type != target.request_type
            ):
                raise ValueError(
                    f"data workflow plan edge {edge.source_node_id!r}->"
                    f"{edge.target_node_id!r} has incompatible protobuf types"
                )
            if edge.target_node_id in data_targets:
                raise ValueError(
                    f"workflow plan node {edge.target_node_id!r} has more than one "
                    "incoming data edge"
                )
            data_targets.add(edge.target_node_id)

        if (
            edge.kind
            in (
                temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONTROL,
                temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_DATA,
                temporaless_pb2.WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL,
            )
            and edge.target_node_id not in forward_adjacency[edge.source_node_id]
        ):
            forward_adjacency[edge.source_node_id].add(edge.target_node_id)
            forward_indegree[edge.target_node_id] += 1

    ready = [node_id for node_id, degree in forward_indegree.items() if degree == 0]
    visited = 0
    while ready:
        node_id = ready.pop()
        visited += 1
        for target_node_id in forward_adjacency[node_id]:
            forward_indegree[target_node_id] -= 1
            if forward_indegree[target_node_id] == 0:
                ready.append(target_node_id)
    if visited != len(nodes):
        raise ValueError("workflow plan forward edges contain a cycle")


def plan_digest(plan: temporaless_pb2.WorkflowPlan) -> str:
    """Return the hex SHA-256 of deterministic protobuf binary."""

    validate_plan(plan)
    payload = plan.SerializeToString(deterministic=True)
    return hashlib.sha256(payload).hexdigest()


def project_workflow_run(
    plan: temporaless_pb2.WorkflowPlan,
    inspection: RunInspection,
) -> RunProjection:
    """Join exact node IDs to durable evidence without deriving node states."""

    validate_plan(plan)
    _validate_inspection_keys(inspection)
    activity_by_id = {record.key.activity_id: record for record in inspection.activities}
    timer_by_id = {record.key.timer_id: record for record in inspection.timers}
    event_by_id = {record.key.event_id: record for record in inspection.events}
    retry_timers_by_activity: dict[str, list[temporaless_pb2.TimerRecord]] = {}
    for record in inspection.timers:
        if (
            record.timer_kind == temporaless_pb2.TIMER_KIND_ACTIVITY_RETRY
            and record.retry_activity_id
        ):
            retry_timers_by_activity.setdefault(record.retry_activity_id, []).append(record)
    activity_claims_by_resource: dict[str, list[temporaless_pb2.ClaimRecord]] = {}
    timer_claims_by_resource: dict[str, list[temporaless_pb2.ClaimRecord]] = {}
    for record in inspection.claims:
        if record.resource_type == temporaless_pb2.CLAIM_RESOURCE_TYPE_ACTIVITY:
            activity_claims_by_resource.setdefault(record.resource_id, []).append(record)
        elif record.resource_type == temporaless_pb2.CLAIM_RESOURCE_TYPE_TIMER:
            timer_claims_by_resource.setdefault(record.resource_id, []).append(record)

    matched_activity_ids: set[str] = set()
    matched_timer_ids: set[str] = set()
    matched_event_ids: set[str] = set()
    run_claims = tuple(
        sorted(
            (
                record
                for record in inspection.claims
                if record.resource_type == temporaless_pb2.CLAIM_RESOURCE_TYPE_WORKFLOW
            ),
            key=lambda record: record.key.claim_id,
        )
    )
    matched_claim_ids = {record.key.claim_id for record in run_claims}
    projected_nodes: list[NodeProjection] = []
    for node in sorted(plan.nodes, key=lambda item: item.node_id):
        activity = None
        timers: tuple[temporaless_pb2.TimerRecord, ...] = ()
        event = None
        claims: tuple[temporaless_pb2.ClaimRecord, ...] = ()
        if node.kind in (
            temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
            temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_BRANCH,
        ):
            activity = activity_by_id.get(node.node_id)
            timers = tuple(
                sorted(
                    retry_timers_by_activity.get(node.node_id, ()),
                    key=lambda record: record.key.timer_id,
                )
            )
            claims = tuple(
                sorted(
                    activity_claims_by_resource.get(node.node_id, ()),
                    key=lambda record: record.key.claim_id,
                )
            )
            if activity is not None:
                matched_activity_ids.add(node.node_id)
            matched_timer_ids.update(record.key.timer_id for record in timers)
            matched_claim_ids.update(record.key.claim_id for record in claims)
        elif node.kind in (
            temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_SLEEP,
            temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT,
            temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_WAIT_WORKFLOW,
        ):
            exact_timer = timer_by_id.get(node.node_id)
            expected_timer_kind = (
                temporaless_pb2.TIMER_KIND_SLEEP
                if node.kind == temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_SLEEP
                else temporaless_pb2.TIMER_KIND_POLL
            )
            if exact_timer is not None and exact_timer.timer_kind != expected_timer_kind:
                exact_timer = None
            timers = (exact_timer,) if exact_timer is not None else ()
            claims = tuple(
                sorted(
                    timer_claims_by_resource.get(node.node_id, ()),
                    key=lambda record: record.key.claim_id,
                )
            )
            matched_claim_ids.update(record.key.claim_id for record in claims)
            if exact_timer is not None:
                matched_timer_ids.add(node.node_id)
        if node.kind == temporaless_pb2.WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT:
            event = event_by_id.get(node.node_id)
            if event is not None:
                matched_event_ids.add(node.node_id)
        projected_nodes.append(
            NodeProjection(
                node=node,
                activity=activity,
                timers=timers,
                event=event,
                claims=claims,
            )
        )

    return RunProjection(
        plan=plan,
        key=inspection.key,
        workflow=inspection.workflow,
        nodes=tuple(projected_nodes),
        run_claims=run_claims,
        unplanned_activities=tuple(
            sorted(
                (
                    record
                    for record in inspection.activities
                    if record.key.activity_id not in matched_activity_ids
                ),
                key=lambda record: record.key.activity_id,
            )
        ),
        unplanned_timers=tuple(
            sorted(
                (
                    record
                    for record in inspection.timers
                    if record.key.timer_id not in matched_timer_ids
                ),
                key=lambda record: record.key.timer_id,
            )
        ),
        unplanned_events=tuple(
            sorted(
                (
                    record
                    for record in inspection.events
                    if record.key.event_id not in matched_event_ids
                ),
                key=lambda record: record.key.event_id,
            )
        ),
        unplanned_claims=tuple(
            sorted(
                (
                    record
                    for record in inspection.claims
                    if record.key.claim_id not in matched_claim_ids
                ),
                key=lambda record: record.key.claim_id,
            )
        ),
        claims_inspected=inspection.claims_inspected,
    )


def _validate_inspection_keys(inspection: RunInspection) -> None:
    inspection.key.validate()
    if inspection.workflow is not None:
        _assert_run_key("workflow", inspection.workflow, inspection.key)
    for record in inspection.activities:
        _assert_run_key("activity", record, inspection.key)
    for record in inspection.timers:
        _assert_run_key("timer", record, inspection.key)
    for record in inspection.events:
        _assert_run_key("event", record, inspection.key)
    for record in inspection.claims:
        _assert_run_key("claim", record, inspection.key)


def _assert_run_key(
    record_kind: str,
    record: (
        temporaless_pb2.WorkflowRecord
        | temporaless_pb2.ActivityRecord
        | temporaless_pb2.TimerRecord
        | temporaless_pb2.EventRecord
        | temporaless_pb2.ClaimRecord
    ),
    expected: WorkflowKey,
) -> None:
    if not record.HasField("key"):
        raise ValueError(f"{record_kind} record key is required")
    actual = record.key
    if (
        actual.namespace != expected.namespace
        or actual.workflow_id != expected.workflow_id
        or actual.run_id != expected.run_id
    ):
        raise ValueError(
            f"{record_kind} record key does not match inspection run "
            f"{expected.namespace!r}/{expected.workflow_id!r}/{expected.run_id!r}"
        )


__all__ = [
    "ClaimLister",
    "NodeProjection",
    "RunInspection",
    "RunProjection",
    "inspect_run",
    "plan_digest",
    "project_workflow_run",
    "validate_plan",
]
