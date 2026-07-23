// Package visualization validates display-only workflow plans and joins them
// to authoritative durable run records for user interfaces.
//
// A WorkflowPlan describes intended topology. It is not an execution language:
// applications still execute ordinary Temporaless workflow code, and the
// projection never invents lifecycle state that is absent from stored records.
package visualization

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"buf.build/go/protovalidate"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
)

// ErrInvalidPlan marks protobuf or cross-field plan validation failures.
var ErrInvalidPlan = errors.New("invalid workflow plan")

// ClaimLister is the optional run-scoped claim surface used by InspectRun.
// The default OpenDAL point store deliberately does not provide claim listing.
type ClaimLister interface {
	ListClaims(context.Context, storage.WorkflowKey) ([]*temporalessv1.ClaimRecord, error)
}

// RunInspection is a deterministic, read-only snapshot of one workflow run.
// ClaimsInspected distinguishes "no claims exist" from "no claim lister was
// supplied".
type RunInspection struct {
	Key             storage.WorkflowKey
	Workflow        *temporalessv1.WorkflowRecord
	Activities      []*temporalessv1.ActivityRecord
	Timers          []*temporalessv1.TimerRecord
	Events          []*temporalessv1.EventRecord
	Claims          []*temporalessv1.ClaimRecord
	ClaimsInspected bool
}

// NodeProjection contains only kind-compatible durable records whose boundary
// ID exactly matches Node.node_id, or whose explicit retry_activity_id links
// back to that node. A nil record means there is no authoritative evidence for
// that boundary; it does not mean running, pending, or skipped.
type NodeProjection struct {
	Node     *temporalessv1.WorkflowPlanNode
	Activity *temporalessv1.ActivityRecord
	Timers   []*temporalessv1.TimerRecord
	Event    *temporalessv1.EventRecord
	Claims   []*temporalessv1.ClaimRecord
}

// RunProjection joins intended topology to actual durable records. Records
// that do not have one unambiguous, kind-compatible node remain visible in an
// Unplanned slice instead of being discarded or forced onto a node.
type RunProjection struct {
	Plan                *temporalessv1.WorkflowPlan
	Key                 storage.WorkflowKey
	Workflow            *temporalessv1.WorkflowRecord
	Nodes               []*NodeProjection
	UnplannedActivities []*temporalessv1.ActivityRecord
	UnplannedTimers     []*temporalessv1.TimerRecord
	UnplannedEvents     []*temporalessv1.EventRecord
	RunClaims           []*temporalessv1.ClaimRecord
	UnplannedClaims     []*temporalessv1.ClaimRecord
	ClaimsInspected     bool
}

// ValidatePlan applies protobuf validation plus the graph rules that cannot be
// expressed as field constraints.
func ValidatePlan(plan *temporalessv1.WorkflowPlan) error {
	if plan == nil {
		return fmt.Errorf("%w: plan is required", ErrInvalidPlan)
	}
	if err := protovalidate.Validate(plan); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPlan, err)
	}

	nodes := make(map[string]*temporalessv1.WorkflowPlanNode, len(plan.GetNodes()))
	indegree := make(map[string]int, len(plan.GetNodes()))
	adjacency := make(map[string][]string, len(plan.GetNodes()))
	for index, node := range plan.GetNodes() {
		if node == nil {
			return fmt.Errorf("%w: node at index %d is required", ErrInvalidPlan, index)
		}
		nodeID := node.GetNodeId()
		if _, exists := nodes[nodeID]; exists {
			return fmt.Errorf("%w: duplicate node_id %q", ErrInvalidPlan, nodeID)
		}
		nodes[nodeID] = node
		indegree[nodeID] = 0

		switch node.GetKind() {
		case temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
			temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_BRANCH:
			if node.GetOperation() == "" {
				return fmt.Errorf("%w: callable node %q requires operation", ErrInvalidPlan, nodeID)
			}
			if node.GetRequestType() == "" {
				return fmt.Errorf("%w: callable node %q requires request_type", ErrInvalidPlan, nodeID)
			}
			if node.GetResponseType() == "" {
				return fmt.Errorf("%w: callable node %q requires response_type", ErrInvalidPlan, nodeID)
			}
		}
	}

	type edgeIdentity struct {
		source string
		target string
		kind   temporalessv1.WorkflowPlanEdgeKind
		label  string
	}
	edges := make(map[edgeIdentity]struct{}, len(plan.GetEdges()))
	conditionalLabels := make(map[struct {
		source string
		label  string
	}]struct{})
	dataTargets := make(map[string]struct{})
	for index, edge := range plan.GetEdges() {
		if edge == nil {
			return fmt.Errorf("%w: edge at index %d is required", ErrInvalidPlan, index)
		}

		identity := edgeIdentity{
			source: edge.GetSourceNodeId(),
			target: edge.GetTargetNodeId(),
			kind:   edge.GetKind(),
			label:  edge.GetLabel(),
		}
		if _, exists := edges[identity]; exists {
			return fmt.Errorf(
				"%w: duplicate edge %q -> %q (%s, label %q)",
				ErrInvalidPlan,
				identity.source,
				identity.target,
				identity.kind,
				identity.label,
			)
		}
		edges[identity] = struct{}{}

		source, sourceExists := nodes[identity.source]
		if !sourceExists {
			return fmt.Errorf("%w: edge source node %q does not exist", ErrInvalidPlan, identity.source)
		}
		target, targetExists := nodes[identity.target]
		if !targetExists {
			return fmt.Errorf("%w: edge target node %q does not exist", ErrInvalidPlan, identity.target)
		}
		if identity.kind == temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL &&
			identity.label == "" {
			return fmt.Errorf(
				"%w: conditional edge %q -> %q requires label",
				ErrInvalidPlan,
				identity.source,
				identity.target,
			)
		}
		if identity.kind == temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL {
			if source.GetKind() != temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_BRANCH {
				return fmt.Errorf(
					"%w: conditional edge %q -> %q must originate from a BRANCH node",
					ErrInvalidPlan,
					identity.source,
					identity.target,
				)
			}
			labelKey := struct {
				source string
				label  string
			}{
				source: identity.source,
				label:  identity.label,
			}
			if _, exists := conditionalLabels[labelKey]; exists {
				return fmt.Errorf(
					"%w: branch node %q has duplicate conditional label %q",
					ErrInvalidPlan,
					identity.source,
					identity.label,
				)
			}
			conditionalLabels[labelKey] = struct{}{}
		}
		if identity.kind == temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_DATA {
			if !isCallableNode(source) || !isCallableNode(target) {
				return fmt.Errorf(
					"%w: data edge %q -> %q requires callable endpoints",
					ErrInvalidPlan,
					identity.source,
					identity.target,
				)
			}
			if source.GetResponseType() == "" ||
				target.GetRequestType() == "" ||
				source.GetResponseType() != target.GetRequestType() {
				return fmt.Errorf(
					"%w: data edge %q -> %q has incompatible types %q -> %q",
					ErrInvalidPlan,
					identity.source,
					identity.target,
					source.GetResponseType(),
					target.GetRequestType(),
				)
			}
			if _, exists := dataTargets[identity.target]; exists {
				return fmt.Errorf(
					"%w: node %q has more than one incoming data edge",
					ErrInvalidPlan,
					identity.target,
				)
			}
			dataTargets[identity.target] = struct{}{}
		}
		if identity.kind == temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_LOOP_BACK {
			if source.GetKind() != temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_LOOP &&
				target.GetKind() != temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_LOOP {
				return fmt.Errorf(
					"%w: loop-back edge %q -> %q must touch a LOOP node",
					ErrInvalidPlan,
					identity.source,
					identity.target,
				)
			}
			continue
		}

		adjacency[identity.source] = append(adjacency[identity.source], identity.target)
		indegree[identity.target]++
	}

	for nodeID := range adjacency {
		sort.Strings(adjacency[nodeID])
	}
	ready := make([]string, 0, len(nodes))
	for nodeID, degree := range indegree {
		if degree == 0 {
			ready = append(ready, nodeID)
		}
	}
	sort.Strings(ready)

	visited := 0
	for len(ready) > 0 {
		nodeID := ready[0]
		ready = ready[1:]
		visited++
		for _, targetID := range adjacency[nodeID] {
			indegree[targetID]--
			if indegree[targetID] == 0 {
				ready = append(ready, targetID)
				sort.Strings(ready)
			}
		}
	}
	if visited != len(nodes) {
		return fmt.Errorf("%w: non-loop-back edges must form a DAG", ErrInvalidPlan)
	}

	return nil
}

// Digest returns the lowercase SHA-256 digest of the plan's deterministic
// protobuf binary. Repeated node and edge order remains significant; protobuf
// map insertion order does not.
func Digest(plan *temporalessv1.WorkflowPlan) (string, error) {
	if err := ValidatePlan(plan); err != nil {
		return "", err
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(plan)
	if err != nil {
		return "", fmt.Errorf("marshal workflow plan: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// InspectRun reads every authoritative point-store record for one run. It
// performs no writes and returns no partial snapshot when a read fails.
func InspectRun(
	ctx context.Context,
	store storage.Store,
	claimLister ClaimLister,
	key storage.WorkflowKey,
) (*RunInspection, error) {
	if store == nil {
		return nil, errors.New("visualization: store is required")
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("visualization: invalid workflow key: %w", err)
	}
	key = storage.WorkflowKeyFromProto(key.Proto())

	workflow, found, err := store.GetWorkflow(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("inspect workflow: %w", err)
	}
	if found {
		if err := storage.ValidateWorkflowRecord(workflow, key); err != nil {
			return nil, fmt.Errorf("inspect workflow record: %w", err)
		}
	} else {
		workflow = nil
	}

	activities, err := store.ListActivities(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("inspect activities: %w", err)
	}
	timers, err := store.ListTimers(
		ctx,
		key,
		temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED,
	)
	if err != nil {
		return nil, fmt.Errorf("inspect timers: %w", err)
	}
	events, err := store.ListEvents(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("inspect events: %w", err)
	}

	claims := make([]*temporalessv1.ClaimRecord, 0)
	claimsInspected := claimLister != nil
	if claimLister != nil {
		claims, err = claimLister.ListClaims(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("inspect claims: %w", err)
		}
	}

	activities = sortedActivities(activities)
	timers = sortedTimers(timers)
	events = sortedEvents(events)
	claims = sortedClaims(claims)

	inspection := &RunInspection{
		Key:             key,
		Workflow:        workflow,
		Activities:      activities,
		Timers:          timers,
		Events:          events,
		Claims:          claims,
		ClaimsInspected: claimsInspected,
	}
	if err := validateInspectionRecords(inspection); err != nil {
		return nil, fmt.Errorf("inspect run records: %w", err)
	}
	return inspection, nil
}

// Project joins a validated plan to one inspected run using exact boundary
// IDs. The result exposes stored records as-is and intentionally has no
// synthesized node status.
func Project(
	plan *temporalessv1.WorkflowPlan,
	inspection *RunInspection,
) (*RunProjection, error) {
	if err := ValidatePlan(plan); err != nil {
		return nil, err
	}
	if inspection == nil {
		return nil, errors.New("visualization: run inspection is required")
	}
	if err := validateInspectionRecords(inspection); err != nil {
		return nil, fmt.Errorf("visualization: invalid run inspection: %w", err)
	}

	nodes := make([]*NodeProjection, 0, len(plan.GetNodes()))
	nodesByID := make(map[string]*NodeProjection, len(plan.GetNodes()))
	for _, node := range plan.GetNodes() {
		projection := &NodeProjection{
			Node:   node,
			Timers: make([]*temporalessv1.TimerRecord, 0),
			Claims: make([]*temporalessv1.ClaimRecord, 0),
		}
		nodes = append(nodes, projection)
		nodesByID[node.GetNodeId()] = projection
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Node.GetNodeId() < nodes[j].Node.GetNodeId()
	})

	unplannedActivities := make([]*temporalessv1.ActivityRecord, 0)
	for _, record := range sortedActivities(inspection.Activities) {
		node := nodesByID[record.GetKey().GetActivityId()]
		if node != nil &&
			(node.Node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY ||
				node.Node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_BRANCH) &&
			node.Activity == nil {
			node.Activity = record
			continue
		}
		unplannedActivities = append(unplannedActivities, record)
	}

	unplannedTimers := make([]*temporalessv1.TimerRecord, 0)
	for _, record := range sortedTimers(inspection.Timers) {
		if record.GetTimerKind() == temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY {
			node := nodesByID[record.GetRetryActivityId()]
			if node != nil &&
				(node.Node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY ||
					node.Node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_BRANCH) {
				node.Timers = append(node.Timers, record)
				continue
			}
			unplannedTimers = append(unplannedTimers, record)
			continue
		}

		node := nodesByID[record.GetKey().GetTimerId()]
		if node != nil && timerMatchesNode(record, node.Node) {
			node.Timers = append(node.Timers, record)
			continue
		}
		unplannedTimers = append(unplannedTimers, record)
	}

	unplannedEvents := make([]*temporalessv1.EventRecord, 0)
	for _, record := range sortedEvents(inspection.Events) {
		node := nodesByID[record.GetKey().GetEventId()]
		if node != nil &&
			node.Node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT &&
			node.Event == nil {
			node.Event = record
			continue
		}
		unplannedEvents = append(unplannedEvents, record)
	}

	runClaims := make([]*temporalessv1.ClaimRecord, 0)
	unplannedClaims := make([]*temporalessv1.ClaimRecord, 0)
	for _, record := range sortedClaims(inspection.Claims) {
		if record.GetResourceType() == temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW {
			runClaims = append(runClaims, record)
			continue
		}
		node := nodesByID[record.GetResourceId()]
		if node != nil && claimMatchesNode(record, node.Node) {
			node.Claims = append(node.Claims, record)
			continue
		}
		unplannedClaims = append(unplannedClaims, record)
	}
	for _, node := range nodes {
		node.Timers = sortedTimers(node.Timers)
		node.Claims = sortedClaims(node.Claims)
	}

	return &RunProjection{
		Plan:                plan,
		Key:                 storage.WorkflowKeyFromProto(inspection.Key.Proto()),
		Workflow:            inspection.Workflow,
		Nodes:               nodes,
		UnplannedActivities: unplannedActivities,
		UnplannedTimers:     unplannedTimers,
		UnplannedEvents:     unplannedEvents,
		RunClaims:           runClaims,
		UnplannedClaims:     unplannedClaims,
		ClaimsInspected:     inspection.ClaimsInspected,
	}, nil
}

func validateInspectionRecords(inspection *RunInspection) error {
	if inspection == nil {
		return errors.New("run inspection is required")
	}
	if err := inspection.Key.Validate(); err != nil {
		return fmt.Errorf("invalid inspected workflow key: %w", err)
	}
	key := storage.WorkflowKeyFromProto(inspection.Key.Proto())

	if inspection.Workflow != nil {
		if err := storage.ValidateWorkflowRecord(inspection.Workflow, key); err != nil {
			return fmt.Errorf("workflow record: %w", err)
		}
	}
	for index, record := range inspection.Activities {
		expected := storage.ActivityKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			ActivityID: record.GetKey().GetActivityId(),
		}
		if err := storage.ValidateActivityRecord(record, expected); err != nil {
			return fmt.Errorf("activity record at index %d: %w", index, err)
		}
	}
	for index, record := range inspection.Timers {
		expected := storage.TimerKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			TimerID:    record.GetKey().GetTimerId(),
		}
		if err := storage.ValidateTimerRecord(record, expected); err != nil {
			return fmt.Errorf("timer record at index %d: %w", index, err)
		}
	}
	for index, record := range inspection.Events {
		expected := storage.EventKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			EventID:    record.GetKey().GetEventId(),
		}
		if err := storage.ValidateEventRecord(record, expected); err != nil {
			return fmt.Errorf("event record at index %d: %w", index, err)
		}
	}
	for index, record := range inspection.Claims {
		expected := storage.ClaimKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			ClaimID:    record.GetKey().GetClaimId(),
		}
		if err := storage.ValidateClaimRecord(record, expected); err != nil {
			return fmt.Errorf("claim record at index %d: %w", index, err)
		}
	}

	return nil
}

func isCallableNode(node *temporalessv1.WorkflowPlanNode) bool {
	return node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY ||
		node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_BRANCH
}

func claimMatchesNode(
	claim *temporalessv1.ClaimRecord,
	node *temporalessv1.WorkflowPlanNode,
) bool {
	switch claim.GetResourceType() {
	case temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY:
		return node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY ||
			node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_BRANCH
	case temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_TIMER:
		return node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_SLEEP ||
			node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT ||
			node.GetKind() == temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_WORKFLOW
	default:
		return false
	}
}

func timerMatchesNode(
	timer *temporalessv1.TimerRecord,
	node *temporalessv1.WorkflowPlanNode,
) bool {
	switch node.GetKind() {
	case temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_SLEEP:
		return timer.GetTimerKind() == temporalessv1.TimerKind_TIMER_KIND_SLEEP
	case temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT,
		temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_WORKFLOW:
		return timer.GetTimerKind() == temporalessv1.TimerKind_TIMER_KIND_POLL
	default:
		return false
	}
}

func sortedActivities(records []*temporalessv1.ActivityRecord) []*temporalessv1.ActivityRecord {
	sorted := append(make([]*temporalessv1.ActivityRecord, 0, len(records)), records...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetKey().GetActivityId() < sorted[j].GetKey().GetActivityId()
	})
	return sorted
}

func sortedTimers(records []*temporalessv1.TimerRecord) []*temporalessv1.TimerRecord {
	sorted := append(make([]*temporalessv1.TimerRecord, 0, len(records)), records...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetKey().GetTimerId() < sorted[j].GetKey().GetTimerId()
	})
	return sorted
}

func sortedEvents(records []*temporalessv1.EventRecord) []*temporalessv1.EventRecord {
	sorted := append(make([]*temporalessv1.EventRecord, 0, len(records)), records...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetKey().GetEventId() < sorted[j].GetKey().GetEventId()
	})
	return sorted
}

func sortedClaims(records []*temporalessv1.ClaimRecord) []*temporalessv1.ClaimRecord {
	sorted := append(make([]*temporalessv1.ClaimRecord, 0, len(records)), records...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetKey().GetClaimId() < sorted[j].GetKey().GetClaimId()
	})
	return sorted
}
