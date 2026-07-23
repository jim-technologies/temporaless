package visualization_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/visualization"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
)

func TestValidatePlan(t *testing.T) {
	tests := []struct {
		name         string
		plan         func() *temporalessv1.WorkflowPlan
		wantContains string
	}{
		{
			name: "linear",
			plan: digestFixture,
		},
		{
			name: "explicit loop",
			plan: validLoopPlan,
		},
		{
			name: "typed data edge",
			plan: validDataPlan,
		},
		{
			name: "conditional branches",
			plan: validBranchPlan,
		},
		{
			name: "nil plan",
			plan: func() *temporalessv1.WorkflowPlan {
				return nil
			},
			wantContains: "plan is required",
		},
		{
			name: "protobuf revision constraint",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Revision = 0
				return plan
			},
			wantContains: "revision",
		},
		{
			name: "duplicate node",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Nodes = append(plan.Nodes, proto.Clone(plan.Nodes[0]).(*temporalessv1.WorkflowPlanNode))
				return plan
			},
			wantContains: "duplicate node_id",
		},
		{
			name: "duplicate edge",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Edges = append(plan.Edges, proto.Clone(plan.Edges[0]).(*temporalessv1.WorkflowPlanEdge))
				return plan
			},
			wantContains: "duplicate edge",
		},
		{
			name: "activity operation required",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Nodes[0].Operation = ""
				return plan
			},
			wantContains: "requires operation",
		},
		{
			name: "activity request type required",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Nodes[0].RequestType = ""
				return plan
			},
			wantContains: "requires request_type",
		},
		{
			name: "activity response type required",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Nodes[0].ResponseType = ""
				return plan
			},
			wantContains: "requires response_type",
		},
		{
			name: "conditional label required",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := validBranchPlan()
				plan.Edges[0].Label = ""
				return plan
			},
			wantContains: "requires label",
		},
		{
			name: "conditional source must be branch",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := validBranchPlan()
				plan.Nodes[0].Kind = temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY
				return plan
			},
			wantContains: "must originate from a BRANCH",
		},
		{
			name: "conditional labels unique per branch",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := validBranchPlan()
				plan.Edges[1].Label = plan.Edges[0].Label
				return plan
			},
			wantContains: "duplicate conditional label",
		},
		{
			name: "missing source",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Edges[0].SourceNodeId = "missing"
				return plan
			},
			wantContains: "source node",
		},
		{
			name: "missing target",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Edges[0].TargetNodeId = "missing"
				return plan
			},
			wantContains: "target node",
		},
		{
			name: "forward cycle",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Edges = append(plan.Edges, &temporalessv1.WorkflowPlanEdge{
					SourceNodeId: "approve",
					TargetNodeId: "validate",
					Kind:         temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_CONTROL,
				})
				return plan
			},
			wantContains: "must form a DAG",
		},
		{
			name: "loop back must touch loop",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Edges[0].Kind = temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_LOOP_BACK
				return plan
			},
			wantContains: "must touch a LOOP",
		},
		{
			name: "data edge type mismatch",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := validDataPlan()
				plan.Nodes[1].RequestType = "types.v1.Other"
				return plan
			},
			wantContains: "incompatible types",
		},
		{
			name: "data edge structural endpoint",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := validDataPlan()
				plan.Nodes[1].Kind = temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT
				return plan
			},
			wantContains: "requires callable endpoints",
		},
		{
			name: "multiple incoming data edges",
			plan: func() *temporalessv1.WorkflowPlan {
				plan := validDataPlan()
				plan.Nodes = append(plan.Nodes, callableNode("source-two", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY, "types.v1.Input", "types.v1.Shared"))
				plan.Edges = append(plan.Edges, &temporalessv1.WorkflowPlanEdge{
					SourceNodeId: "source-two",
					TargetNodeId: "target",
					Kind:         temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_DATA,
				})
				return plan
			},
			wantContains: "more than one incoming data edge",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := visualization.ValidatePlan(test.plan())
			if test.wantContains == "" {
				if err != nil {
					t.Fatalf("ValidatePlan() error = %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidatePlan() error = nil, want substring %q", test.wantContains)
			}
			if !errors.Is(err, visualization.ErrInvalidPlan) {
				t.Fatalf("ValidatePlan() error = %v, want ErrInvalidPlan", err)
			}
			if !strings.Contains(err.Error(), test.wantContains) {
				t.Fatalf("ValidatePlan() error = %q, want substring %q", err, test.wantContains)
			}
		})
	}
}

func TestDigest(t *testing.T) {
	const want = "f3ed8cdf8a4aa2fe3d323661dfff0a50c7097aeac1d307784ed2a726810797f0"

	got, err := visualization.Digest(digestFixture())
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Digest() = %q, want %q", got, want)
	}
}

func TestDigestDeterminism(t *testing.T) {
	tests := []struct {
		name      string
		left      func() *temporalessv1.WorkflowPlan
		right     func() *temporalessv1.WorkflowPlan
		wantEqual bool
	}{
		{
			name: "map insertion order ignored",
			left: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Annotations = make(map[string]string)
				plan.Annotations["z"] = "last"
				plan.Annotations["a"] = "first"
				return plan
			},
			right: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Annotations = make(map[string]string)
				plan.Annotations["a"] = "first"
				plan.Annotations["z"] = "last"
				return plan
			},
			wantEqual: true,
		},
		{
			name: "repeated field order retained",
			left: digestFixture,
			right: func() *temporalessv1.WorkflowPlan {
				plan := digestFixture()
				plan.Nodes[0], plan.Nodes[1] = plan.Nodes[1], plan.Nodes[0]
				return plan
			},
			wantEqual: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left, err := visualization.Digest(test.left())
			if err != nil {
				t.Fatal(err)
			}
			right, err := visualization.Digest(test.right())
			if err != nil {
				t.Fatal(err)
			}
			if got := left == right; got != test.wantEqual {
				t.Fatalf("digest equality = %t, want %t (%q, %q)", got, test.wantEqual, left, right)
			}
		})
	}
}

func TestDigestRejectsInvalidPlan(t *testing.T) {
	plan := digestFixture()
	plan.Edges[0].TargetNodeId = "missing"

	digest, err := visualization.Digest(plan)
	if digest != "" {
		t.Fatalf("Digest() value = %q, want empty", digest)
	}
	if !errors.Is(err, visualization.ErrInvalidPlan) {
		t.Fatalf("Digest() error = %v, want ErrInvalidPlan", err)
	}
}

func TestInspectRun(t *testing.T) {
	ctx := context.Background()
	store := newOpenDALStore(t)
	inputKey := storage.WorkflowKey{WorkflowID: "approval:export", RunID: "run:1"}
	key := storage.WorkflowKeyFromProto(inputKey.Proto())

	if err := store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "exports.v1.ExportWorkflow.Run",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
	}); err != nil {
		t.Fatal(err)
	}
	for _, activityID := range []string{"z-activity", "a-activity"} {
		if err := store.PutActivity(ctx, activityRecord(key, activityID, activityID)); err != nil {
			t.Fatal(err)
		}
	}
	for _, timerID := range []string{"z-timer", "a-timer"} {
		if err := store.PutTimer(ctx, timerRecord(
			key,
			timerID,
			temporalessv1.TimerKind_TIMER_KIND_SLEEP,
			"",
		)); err != nil {
			t.Fatal(err)
		}
	}
	for _, eventID := range []string{"z-event", "a-event"} {
		if err := store.PutEvent(ctx, eventRecord(key, eventID)); err != nil {
			t.Fatal(err)
		}
	}

	claims := &stubClaimLister{
		records: []*temporalessv1.ClaimRecord{
			claimRecord(key, "z-claim", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW, "approval:export"),
			claimRecord(key, "a-claim", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY, "a-activity"),
		},
	}
	inspection, err := visualization.InspectRun(ctx, store, claims, inputKey)
	if err != nil {
		t.Fatal(err)
	}

	if inspection.Key != key {
		t.Fatalf("inspection key = %#v, want normalized %#v", inspection.Key, key)
	}
	if inspection.Workflow == nil || inspection.Workflow.GetWorkflowType() != "exports.v1.ExportWorkflow.Run" {
		t.Fatalf("workflow = %#v, want stored workflow", inspection.Workflow)
	}
	assertStrings(t, activityIDs(inspection.Activities), []string{"a-activity", "z-activity"})
	assertStrings(t, timerIDs(inspection.Timers), []string{"a-timer", "z-timer"})
	assertStrings(t, eventIDs(inspection.Events), []string{"a-event", "z-event"})
	assertStrings(t, claimIDs(inspection.Claims), []string{"a-claim", "z-claim"})
	if !inspection.ClaimsInspected {
		t.Fatal("ClaimsInspected = false, want true")
	}
	if claims.calls != 1 || claims.gotKey != key {
		t.Fatalf("claim lister calls/key = %d/%#v, want 1/%#v", claims.calls, claims.gotKey, key)
	}

	withoutClaims, err := visualization.InspectRun(ctx, store, nil, inputKey)
	if err != nil {
		t.Fatal(err)
	}
	if withoutClaims.ClaimsInspected {
		t.Fatal("ClaimsInspected = true without a claim lister")
	}
	if withoutClaims.Claims == nil || len(withoutClaims.Claims) != 0 {
		t.Fatalf("Claims = %#v, want a deterministic empty slice", withoutClaims.Claims)
	}
}

func TestInspectRunErrors(t *testing.T) {
	store := newOpenDALStore(t)
	claimErr := errors.New("claim backend unavailable")
	tests := []struct {
		name         string
		store        storage.Store
		claims       visualization.ClaimLister
		key          storage.WorkflowKey
		wantContains string
	}{
		{
			name:         "nil store",
			key:          storage.NewWorkflowKey("workflow", "run"),
			wantContains: "store is required",
		},
		{
			name:         "invalid key",
			store:        store,
			key:          storage.NewWorkflowKey("bad/workflow", "run"),
			wantContains: "invalid workflow key",
		},
		{
			name:  "claim read",
			store: store,
			claims: &stubClaimLister{
				err: claimErr,
			},
			key:          storage.NewWorkflowKey("workflow", "run"),
			wantContains: "inspect claims",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inspection, err := visualization.InspectRun(
				context.Background(),
				test.store,
				test.claims,
				test.key,
			)
			if inspection != nil {
				t.Fatalf("InspectRun() inspection = %#v, want nil on error", inspection)
			}
			if err == nil || !strings.Contains(err.Error(), test.wantContains) {
				t.Fatalf("InspectRun() error = %v, want substring %q", err, test.wantContains)
			}
		})
	}
}

func TestInspectRunRejectsInvalidRecordIdentity(t *testing.T) {
	base := newOpenDALStore(t)
	key := storage.NewWorkflowKey("workflow", "run")
	otherKey := storage.NewWorkflowKey("other-workflow", "other-run")
	tests := []struct {
		name         string
		configure    func(*readOverrideStore) visualization.ClaimLister
		wantContains string
	}{
		{
			name: "nil workflow returned as found",
			configure: func(store *readOverrideStore) visualization.ClaimLister {
				store.workflowOverride = true
				store.workflowFound = true
				return nil
			},
			wantContains: "workflow record",
		},
		{
			name: "workflow missing embedded key",
			configure: func(store *readOverrideStore) visualization.ClaimLister {
				store.workflowOverride = true
				store.workflowFound = true
				store.workflow = &temporalessv1.WorkflowRecord{
					SchemaVersion: storage.WorkflowRecordSchemaVersion,
				}
				return nil
			},
			wantContains: "workflow record",
		},
		{
			name: "workflow belongs to another run",
			configure: func(store *readOverrideStore) visualization.ClaimLister {
				store.workflowOverride = true
				store.workflowFound = true
				store.workflow = &temporalessv1.WorkflowRecord{
					SchemaVersion: storage.WorkflowRecordSchemaVersion,
					Key:           otherKey.Proto(),
				}
				return nil
			},
			wantContains: "workflow record",
		},
		{
			name: "nil activity",
			configure: func(store *readOverrideStore) visualization.ClaimLister {
				store.activitiesOverride = true
				store.activities = []*temporalessv1.ActivityRecord{nil}
				return nil
			},
			wantContains: "activity record",
		},
		{
			name: "activity missing embedded key",
			configure: func(store *readOverrideStore) visualization.ClaimLister {
				store.activitiesOverride = true
				store.activities = []*temporalessv1.ActivityRecord{{
					SchemaVersion: storage.ActivityRecordSchemaVersion,
				}}
				return nil
			},
			wantContains: "activity record",
		},
		{
			name: "activity belongs to another run",
			configure: func(store *readOverrideStore) visualization.ClaimLister {
				store.activitiesOverride = true
				store.activities = []*temporalessv1.ActivityRecord{
					activityRecord(otherKey, "activity", "activity"),
				}
				return nil
			},
			wantContains: "activity record",
		},
		{
			name: "timer missing embedded key",
			configure: func(store *readOverrideStore) visualization.ClaimLister {
				store.timersOverride = true
				store.timers = []*temporalessv1.TimerRecord{{
					SchemaVersion: storage.TimerRecordSchemaVersion,
				}}
				return nil
			},
			wantContains: "timer record",
		},
		{
			name: "timer belongs to another run",
			configure: func(store *readOverrideStore) visualization.ClaimLister {
				store.timersOverride = true
				store.timers = []*temporalessv1.TimerRecord{
					timerRecord(otherKey, "timer", temporalessv1.TimerKind_TIMER_KIND_SLEEP, ""),
				}
				return nil
			},
			wantContains: "timer record",
		},
		{
			name: "event missing embedded key",
			configure: func(store *readOverrideStore) visualization.ClaimLister {
				store.eventsOverride = true
				store.events = []*temporalessv1.EventRecord{{
					SchemaVersion: storage.EventRecordSchemaVersion,
				}}
				return nil
			},
			wantContains: "event record",
		},
		{
			name: "event belongs to another run",
			configure: func(store *readOverrideStore) visualization.ClaimLister {
				store.eventsOverride = true
				store.events = []*temporalessv1.EventRecord{
					eventRecord(otherKey, "event"),
				}
				return nil
			},
			wantContains: "event record",
		},
		{
			name: "claim missing embedded key",
			configure: func(_ *readOverrideStore) visualization.ClaimLister {
				return &stubClaimLister{
					records: []*temporalessv1.ClaimRecord{{
						SchemaVersion: storage.ClaimRecordSchemaVersion,
					}},
				}
			},
			wantContains: "claim record",
		},
		{
			name: "claim belongs to another run",
			configure: func(_ *readOverrideStore) visualization.ClaimLister {
				return &stubClaimLister{
					records: []*temporalessv1.ClaimRecord{
						claimRecord(
							otherKey,
							"claim",
							temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
							"other-workflow",
						),
					},
				}
			},
			wantContains: "claim record",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &readOverrideStore{Store: base}
			claims := test.configure(store)
			inspection, err := visualization.InspectRun(
				context.Background(),
				store,
				claims,
				key,
			)
			if inspection != nil {
				t.Fatalf("InspectRun() inspection = %#v, want nil", inspection)
			}
			if err == nil || !strings.Contains(err.Error(), test.wantContains) {
				t.Fatalf("InspectRun() error = %v, want substring %q", err, test.wantContains)
			}
		})
	}
}

func TestProject(t *testing.T) {
	key := storage.NewWorkflowKey("approval:export", "run:1")
	plan := projectionPlan()
	workflow := &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
	}
	firstActivity := activityRecord(key, "activity", "first")
	duplicateActivity := activityRecord(key, "activity", "duplicate")
	inspection := &visualization.RunInspection{
		Key:      key,
		Workflow: workflow,
		Activities: []*temporalessv1.ActivityRecord{
			activityRecord(key, "z-activity", "unplanned"),
			firstActivity,
			activityRecord(key, "sleep", "wrong-kind-node"),
			duplicateActivity,
			activityRecord(key, "branch", "branch"),
		},
		Timers: []*temporalessv1.TimerRecord{
			timerRecord(key, "z-timer", temporalessv1.TimerKind_TIMER_KIND_POLL, ""),
			timerRecord(key, "retry-activity", temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY, "activity"),
			timerRecord(key, "sleep", temporalessv1.TimerKind_TIMER_KIND_SLEEP, ""),
			timerRecord(key, "approval", temporalessv1.TimerKind_TIMER_KIND_POLL, ""),
			timerRecord(key, "dependency", temporalessv1.TimerKind_TIMER_KIND_POLL, ""),
			timerRecord(key, "retry-branch", temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY, "branch"),
			timerRecord(key, "m-retry", temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY, "missing"),
			timerRecord(key, "sleep", temporalessv1.TimerKind_TIMER_KIND_POLL, ""),
			timerRecord(key, "approval", temporalessv1.TimerKind_TIMER_KIND_SLEEP, ""),
		},
		Events: []*temporalessv1.EventRecord{
			eventRecord(key, "z-event"),
			eventRecord(key, "approval"),
			eventRecord(key, "sleep"),
		},
		Claims: []*temporalessv1.ClaimRecord{
			claimRecord(key, "z-run", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW, "approval:export"),
			claimRecord(key, "a-run", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW, "approval:export"),
			claimRecord(key, "activity-claim", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY, "activity"),
			claimRecord(key, "branch-claim", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY, "branch"),
			claimRecord(key, "sleep-claim", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_TIMER, "sleep"),
			claimRecord(key, "approval-claim", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_TIMER, "approval"),
			claimRecord(key, "dependency-claim", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_TIMER, "dependency"),
			claimRecord(key, "mismatch-claim", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY, "sleep"),
			claimRecord(key, "concurrency-claim", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_CONCURRENCY_KEY, "activity"),
			claimRecord(key, "fan-timer-claim", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_TIMER, "fan"),
		},
		ClaimsInspected: true,
	}

	projection, err := visualization.Project(plan, inspection)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Plan != plan || projection.Workflow != workflow {
		t.Fatal("projection did not retain plan/workflow evidence")
	}
	if !projection.ClaimsInspected {
		t.Fatal("ClaimsInspected = false, want true")
	}
	assertStrings(
		t,
		projectedNodeIDs(projection.Nodes),
		[]string{"activity", "approval", "branch", "dependency", "fan", "loop", "sleep"},
	)
	nodes := projectionsByID(projection.Nodes)

	if nodes["activity"].Activity != firstActivity {
		t.Fatalf("activity evidence = %#v, want first duplicate deterministically", nodes["activity"].Activity)
	}
	assertStrings(t, timerIDs(nodes["activity"].Timers), []string{"retry-activity"})
	assertStrings(t, claimIDs(nodes["activity"].Claims), []string{"activity-claim"})

	if nodes["branch"].Activity.GetKey().GetActivityId() != "branch" {
		t.Fatalf("branch activity = %#v", nodes["branch"].Activity)
	}
	assertStrings(t, timerIDs(nodes["branch"].Timers), []string{"retry-branch"})
	assertStrings(t, claimIDs(nodes["branch"].Claims), []string{"branch-claim"})

	if nodes["approval"].Event.GetKey().GetEventId() != "approval" {
		t.Fatalf("approval event = %#v", nodes["approval"].Event)
	}
	assertStrings(t, timerIDs(nodes["approval"].Timers), []string{"approval"})
	assertStrings(t, claimIDs(nodes["approval"].Claims), []string{"approval-claim"})

	assertStrings(t, timerIDs(nodes["dependency"].Timers), []string{"dependency"})
	assertStrings(t, claimIDs(nodes["dependency"].Claims), []string{"dependency-claim"})
	assertStrings(t, timerIDs(nodes["sleep"].Timers), []string{"sleep"})
	assertStrings(t, claimIDs(nodes["sleep"].Claims), []string{"sleep-claim"})

	for _, nodeID := range []string{"fan", "loop"} {
		node := nodes[nodeID]
		if node.Activity != nil || node.Event != nil || len(node.Timers) != 0 || len(node.Claims) != 0 {
			t.Fatalf("structural node %q has invented evidence: %#v", nodeID, node)
		}
	}

	assertStrings(t, claimIDs(projection.RunClaims), []string{"a-run", "z-run"})
	assertStrings(
		t,
		activityIDs(projection.UnplannedActivities),
		[]string{"activity", "sleep", "z-activity"},
	)
	assertStrings(
		t,
		timerIDs(projection.UnplannedTimers),
		[]string{"approval", "m-retry", "sleep", "z-timer"},
	)
	assertStrings(t, eventIDs(projection.UnplannedEvents), []string{"sleep", "z-event"})
	assertStrings(
		t,
		claimIDs(projection.UnplannedClaims),
		[]string{"concurrency-claim", "fan-timer-claim", "mismatch-claim"},
	)

	if got, want := projectedActivityCount(projection), len(inspection.Activities); got != want {
		t.Fatalf("projected activity count = %d, want conservation of %d", got, want)
	}
	if got, want := projectedTimerCount(projection), len(inspection.Timers); got != want {
		t.Fatalf("projected timer count = %d, want conservation of %d", got, want)
	}
	if got, want := projectedEventCount(projection), len(inspection.Events); got != want {
		t.Fatalf("projected event count = %d, want conservation of %d", got, want)
	}
	if got, want := projectedClaimCount(projection), len(inspection.Claims); got != want {
		t.Fatalf("projected claim count = %d, want conservation of %d", got, want)
	}

	if _, exists := reflect.TypeOf(visualization.NodeProjection{}).FieldByName("Status"); exists {
		t.Fatal("NodeProjection must not synthesize lifecycle status")
	}
}

func TestProjectErrors(t *testing.T) {
	validInspection := &visualization.RunInspection{
		Key: storage.NewWorkflowKey("workflow", "run"),
	}
	tests := []struct {
		name         string
		plan         *temporalessv1.WorkflowPlan
		inspection   *visualization.RunInspection
		wantContains string
	}{
		{
			name:         "invalid plan",
			plan:         &temporalessv1.WorkflowPlan{},
			inspection:   validInspection,
			wantContains: "invalid workflow plan",
		},
		{
			name:         "nil inspection",
			plan:         digestFixture(),
			wantContains: "run inspection is required",
		},
		{
			name: "invalid inspected key",
			plan: digestFixture(),
			inspection: &visualization.RunInspection{
				Key: storage.NewWorkflowKey("bad/workflow", "run"),
			},
			wantContains: "invalid inspected workflow key",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projection, err := visualization.Project(test.plan, test.inspection)
			if projection != nil {
				t.Fatalf("Project() projection = %#v, want nil", projection)
			}
			if err == nil || !strings.Contains(err.Error(), test.wantContains) {
				t.Fatalf("Project() error = %v, want substring %q", err, test.wantContains)
			}
		})
	}
}

func TestProjectRejectsInvalidRecordIdentity(t *testing.T) {
	key := storage.NewWorkflowKey("workflow", "run")
	otherKey := storage.NewWorkflowKey("other-workflow", "other-run")
	tests := []struct {
		name         string
		mutate       func(*visualization.RunInspection)
		wantContains string
	}{
		{
			name: "workflow missing embedded key",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Workflow = &temporalessv1.WorkflowRecord{
					SchemaVersion: storage.WorkflowRecordSchemaVersion,
				}
			},
			wantContains: "workflow record",
		},
		{
			name: "workflow belongs to another run",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Workflow = &temporalessv1.WorkflowRecord{
					SchemaVersion: storage.WorkflowRecordSchemaVersion,
					Key:           otherKey.Proto(),
				}
			},
			wantContains: "workflow record",
		},
		{
			name: "nil activity",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Activities = []*temporalessv1.ActivityRecord{nil}
			},
			wantContains: "activity record",
		},
		{
			name: "activity missing embedded key",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Activities = []*temporalessv1.ActivityRecord{{
					SchemaVersion: storage.ActivityRecordSchemaVersion,
				}}
			},
			wantContains: "activity record",
		},
		{
			name: "activity belongs to another run",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Activities = []*temporalessv1.ActivityRecord{
					activityRecord(otherKey, "activity", "activity"),
				}
			},
			wantContains: "activity record",
		},
		{
			name: "timer missing embedded key",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Timers = []*temporalessv1.TimerRecord{{
					SchemaVersion: storage.TimerRecordSchemaVersion,
				}}
			},
			wantContains: "timer record",
		},
		{
			name: "timer belongs to another run",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Timers = []*temporalessv1.TimerRecord{
					timerRecord(otherKey, "timer", temporalessv1.TimerKind_TIMER_KIND_SLEEP, ""),
				}
			},
			wantContains: "timer record",
		},
		{
			name: "event missing embedded key",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Events = []*temporalessv1.EventRecord{{
					SchemaVersion: storage.EventRecordSchemaVersion,
				}}
			},
			wantContains: "event record",
		},
		{
			name: "event belongs to another run",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Events = []*temporalessv1.EventRecord{
					eventRecord(otherKey, "event"),
				}
			},
			wantContains: "event record",
		},
		{
			name: "claim missing embedded key",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Claims = []*temporalessv1.ClaimRecord{{
					SchemaVersion: storage.ClaimRecordSchemaVersion,
				}}
			},
			wantContains: "claim record",
		},
		{
			name: "claim belongs to another run",
			mutate: func(inspection *visualization.RunInspection) {
				inspection.Claims = []*temporalessv1.ClaimRecord{
					claimRecord(
						otherKey,
						"claim",
						temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
						"other-workflow",
					),
				}
			},
			wantContains: "claim record",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inspection := &visualization.RunInspection{Key: key}
			test.mutate(inspection)
			projection, err := visualization.Project(digestFixture(), inspection)
			if projection != nil {
				t.Fatalf("Project() projection = %#v, want nil", projection)
			}
			if err == nil || !strings.Contains(err.Error(), test.wantContains) {
				t.Fatalf("Project() error = %v, want substring %q", err, test.wantContains)
			}
		})
	}
}

func digestFixture() *temporalessv1.WorkflowPlan {
	return &temporalessv1.WorkflowPlan{
		PlanId:   "approval:export",
		Revision: 1,
		Nodes: []*temporalessv1.WorkflowPlanNode{
			{
				NodeId:       "validate",
				DisplayName:  "Validate",
				Kind:         temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
				Operation:    "exports.v1.ExportService.Validate",
				RequestType:  "exports.v1.ValidateRequest",
				ResponseType: "exports.v1.ValidateResponse",
			},
			{
				NodeId:      "approve",
				DisplayName: "Approve",
				Kind:        temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT,
			},
		},
		Edges: []*temporalessv1.WorkflowPlanEdge{
			{
				SourceNodeId: "validate",
				TargetNodeId: "approve",
				Kind:         temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_CONTROL,
			},
		},
	}
}

func validLoopPlan() *temporalessv1.WorkflowPlan {
	return &temporalessv1.WorkflowPlan{
		PlanId:   "bounded:loop",
		Revision: 1,
		Nodes: []*temporalessv1.WorkflowPlanNode{
			structuralNode("loop", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_LOOP),
			callableNode("step", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY, "types.v1.Input", "types.v1.Output"),
		},
		Edges: []*temporalessv1.WorkflowPlanEdge{
			{
				SourceNodeId: "loop",
				TargetNodeId: "step",
				Kind:         temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_CONTROL,
			},
			{
				SourceNodeId: "step",
				TargetNodeId: "loop",
				Kind:         temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_LOOP_BACK,
			},
		},
	}
}

func validDataPlan() *temporalessv1.WorkflowPlan {
	return &temporalessv1.WorkflowPlan{
		PlanId:   "typed:data",
		Revision: 1,
		Nodes: []*temporalessv1.WorkflowPlanNode{
			callableNode("source", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY, "types.v1.Input", "types.v1.Shared"),
			callableNode("target", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY, "types.v1.Shared", "types.v1.Output"),
		},
		Edges: []*temporalessv1.WorkflowPlanEdge{
			{
				SourceNodeId: "source",
				TargetNodeId: "target",
				Kind:         temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_DATA,
			},
		},
	}
}

func validBranchPlan() *temporalessv1.WorkflowPlan {
	return &temporalessv1.WorkflowPlan{
		PlanId:   "conditional:branch",
		Revision: 1,
		Nodes: []*temporalessv1.WorkflowPlanNode{
			callableNode("branch", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_BRANCH, "types.v1.Input", "types.v1.Decision"),
			structuralNode("approve", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT),
			structuralNode("reject", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT),
		},
		Edges: []*temporalessv1.WorkflowPlanEdge{
			{
				SourceNodeId: "branch",
				TargetNodeId: "approve",
				Kind:         temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL,
				Label:        "approved",
			},
			{
				SourceNodeId: "branch",
				TargetNodeId: "reject",
				Kind:         temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_CONDITIONAL,
				Label:        "rejected",
			},
		},
	}
}

func projectionPlan() *temporalessv1.WorkflowPlan {
	return &temporalessv1.WorkflowPlan{
		PlanId:   "projection:all-kinds",
		Revision: 1,
		Nodes: []*temporalessv1.WorkflowPlanNode{
			structuralNode("sleep", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_SLEEP),
			callableNode("branch", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_BRANCH, "types.v1.Input", "types.v1.Decision"),
			structuralNode("fan", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_FAN_OUT),
			callableNode("activity", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY, "types.v1.Input", "types.v1.Output"),
			structuralNode("approval", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT),
			structuralNode("loop", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_LOOP),
			structuralNode("dependency", temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_WORKFLOW),
		},
	}
}

func callableNode(
	nodeID string,
	kind temporalessv1.WorkflowPlanNodeKind,
	requestType string,
	responseType string,
) *temporalessv1.WorkflowPlanNode {
	return &temporalessv1.WorkflowPlanNode{
		NodeId:       nodeID,
		DisplayName:  nodeID,
		Kind:         kind,
		Operation:    "example.v1.Service." + nodeID,
		RequestType:  requestType,
		ResponseType: responseType,
	}
}

func structuralNode(
	nodeID string,
	kind temporalessv1.WorkflowPlanNodeKind,
) *temporalessv1.WorkflowPlanNode {
	return &temporalessv1.WorkflowPlanNode{
		NodeId:      nodeID,
		DisplayName: nodeID,
		Kind:        kind,
	}
}

func newOpenDALStore(t *testing.T) *storage.OpenDALStore {
	t.Helper()
	operator, err := opendal.NewOperator(
		fs.Scheme,
		opendal.OperatorOptions{"root": t.TempDir()},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	return storage.NewOpenDALStore(operator)
}

type readOverrideStore struct {
	storage.Store
	workflowOverride   bool
	workflow           *temporalessv1.WorkflowRecord
	workflowFound      bool
	activitiesOverride bool
	activities         []*temporalessv1.ActivityRecord
	timersOverride     bool
	timers             []*temporalessv1.TimerRecord
	eventsOverride     bool
	events             []*temporalessv1.EventRecord
}

func (store *readOverrideStore) GetWorkflow(
	ctx context.Context,
	key storage.WorkflowKey,
) (*temporalessv1.WorkflowRecord, bool, error) {
	if store.workflowOverride {
		return store.workflow, store.workflowFound, nil
	}
	return store.Store.GetWorkflow(ctx, key)
}

func (store *readOverrideStore) ListActivities(
	ctx context.Context,
	key storage.WorkflowKey,
) ([]*temporalessv1.ActivityRecord, error) {
	if store.activitiesOverride {
		return store.activities, nil
	}
	return store.Store.ListActivities(ctx, key)
}

func (store *readOverrideStore) ListTimers(
	ctx context.Context,
	key storage.WorkflowKey,
	status temporalessv1.TimerStatus,
) ([]*temporalessv1.TimerRecord, error) {
	if store.timersOverride {
		return store.timers, nil
	}
	return store.Store.ListTimers(ctx, key, status)
}

func (store *readOverrideStore) ListEvents(
	ctx context.Context,
	key storage.WorkflowKey,
) ([]*temporalessv1.EventRecord, error) {
	if store.eventsOverride {
		return store.events, nil
	}
	return store.Store.ListEvents(ctx, key)
}

type stubClaimLister struct {
	records []*temporalessv1.ClaimRecord
	err     error
	calls   int
	gotKey  storage.WorkflowKey
}

func (stub *stubClaimLister) ListClaims(
	_ context.Context,
	key storage.WorkflowKey,
) ([]*temporalessv1.ClaimRecord, error) {
	stub.calls++
	stub.gotKey = key
	return stub.records, stub.err
}

func activityRecord(
	key storage.WorkflowKey,
	activityID string,
	activityType string,
) *temporalessv1.ActivityRecord {
	return &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key: (&storage.ActivityKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			ActivityID: activityID,
		}).Proto(),
		ActivityType: activityType,
	}
}

func timerRecord(
	key storage.WorkflowKey,
	timerID string,
	kind temporalessv1.TimerKind,
	retryActivityID string,
) *temporalessv1.TimerRecord {
	return &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key: (&storage.TimerKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			TimerID:    timerID,
		}).Proto(),
		TimerKind:       kind,
		Status:          temporalessv1.TimerStatus_TIMER_STATUS_FIRED,
		RetryActivityId: retryActivityID,
	}
}

func eventRecord(key storage.WorkflowKey, eventID string) *temporalessv1.EventRecord {
	return &temporalessv1.EventRecord{
		SchemaVersion: storage.EventRecordSchemaVersion,
		Key: (&storage.EventKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			EventID:    eventID,
		}).Proto(),
	}
}

func claimRecord(
	key storage.WorkflowKey,
	claimID string,
	resourceType temporalessv1.ClaimResourceType,
	resourceID string,
) *temporalessv1.ClaimRecord {
	return &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key: (&storage.ClaimKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			ClaimID:    claimID,
		}).Proto(),
		ResourceType: resourceType,
		ResourceId:   resourceID,
	}
}

func activityIDs(records []*temporalessv1.ActivityRecord) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.GetKey().GetActivityId())
	}
	return ids
}

func timerIDs(records []*temporalessv1.TimerRecord) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.GetKey().GetTimerId())
	}
	return ids
}

func eventIDs(records []*temporalessv1.EventRecord) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.GetKey().GetEventId())
	}
	return ids
}

func claimIDs(records []*temporalessv1.ClaimRecord) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.GetKey().GetClaimId())
	}
	return ids
}

func projectedNodeIDs(nodes []*visualization.NodeProjection) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.Node.GetNodeId())
	}
	return ids
}

func projectionsByID(nodes []*visualization.NodeProjection) map[string]*visualization.NodeProjection {
	byID := make(map[string]*visualization.NodeProjection, len(nodes))
	for _, node := range nodes {
		byID[node.Node.GetNodeId()] = node
	}
	return byID
}

func projectedActivityCount(projection *visualization.RunProjection) int {
	count := len(projection.UnplannedActivities)
	for _, node := range projection.Nodes {
		if node.Activity != nil {
			count++
		}
	}
	return count
}

func projectedTimerCount(projection *visualization.RunProjection) int {
	count := len(projection.UnplannedTimers)
	for _, node := range projection.Nodes {
		count += len(node.Timers)
	}
	return count
}

func projectedEventCount(projection *visualization.RunProjection) int {
	count := len(projection.UnplannedEvents)
	for _, node := range projection.Nodes {
		if node.Event != nil {
			count++
		}
	}
	return count
}

func projectedClaimCount(projection *visualization.RunProjection) int {
	count := len(projection.RunClaims) + len(projection.UnplannedClaims)
	for _, node := range projection.Nodes {
		count += len(node.Claims)
	}
	return count
}

func assertStrings(t *testing.T, got []string, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("values = %#v, want %#v", got, want)
	}
}
