from __future__ import annotations

from datetime import timedelta
from typing import cast

import opendal
import pytest
from google.protobuf.wrappers_pb2 import StringValue
from protovalidate import ValidationError

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


def test_validate_plan_runs_protovalidate_first() -> None:
    with pytest.raises(ValidationError):
        validate_plan(temporaless_pb2.WorkflowPlan())


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
