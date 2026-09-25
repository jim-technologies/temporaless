package inspection_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func directoryFixture(t *testing.T) *fixture {
	f := newFixture(t)
	completed := temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED
	f.workflow(key("pull:crypto", "20260925T074500"), completed, -29*time.Minute, duration(-27*time.Minute))
	f.workflow(key("pull:odds", "20260925T075000"), completed, -24*time.Minute, duration(-23*time.Minute))
	f.workflow(key("pull:polymarket", "20260925T075500"), temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED, -19*time.Minute, duration(-18*time.Minute))
	weather := key("pull:weather", "20260925T080000")
	f.workflow(weather, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, -14*time.Minute, nil)
	f.activity(weather, "fetch:page-3", temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
		[]*temporalessv1.ActivityAttempt{attempt(1, -3*time.Minute, -2*time.Minute, "rate_limited")}, at(30*time.Second))
	f.workflow(key("backfill:equities", "2026-09-01..2026-09-24"), completed, -2*time.Hour, duration(-time.Hour))
	return f
}

func TestListWorkflowDirectory(t *testing.T) {
	tests := []struct {
		name    string
		request *inspectionv1.ListWorkflowDirectoryRequest
		want    []string
	}{
		{
			name:    "whole namespace in workflow_id order",
			request: &inspectionv1.ListWorkflowDirectoryRequest{},
			want:    []string{"backfill:equities", "pull:crypto", "pull:odds", "pull:polymarket", "pull:weather"},
		},
		{
			name:    "prefix",
			request: &inspectionv1.ListWorkflowDirectoryRequest{WorkflowIdPrefix: "pull:"},
			want:    []string{"pull:crypto", "pull:odds", "pull:polymarket", "pull:weather"},
		},
		{
			name:    "status",
			request: &inspectionv1.ListWorkflowDirectoryRequest{Status: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED},
			want:    []string{"pull:polymarket"},
		},
		{
			name:    "prefix and status",
			request: &inspectionv1.ListWorkflowDirectoryRequest{WorkflowIdPrefix: "backfill", Status: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS},
			want:    nil,
		},
	}
	f := directoryFixture(t)
	service := f.service(inspection.Options{})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := proto.Clone(test.request).(*inspectionv1.ListWorkflowDirectoryRequest)
			request.Store, request.Namespace = "engine", "default"
			response, err := service.ListWorkflowDirectory(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, entry := range response.GetEntries() {
				got = append(got, entry.GetRun().GetKey().GetWorkflowId())
			}
			if fmt.Sprint(got) != fmt.Sprint(test.want) {
				t.Fatalf("workflows = %v, want %v", got, test.want)
			}
			if response.GetNextPageToken() != "" {
				t.Fatalf("unexpected next page token")
			}
		})
	}
}

func TestListWorkflowDirectoryPendingAndStaleness(t *testing.T) {
	f := directoryFixture(t)
	// Retention removed the run the pointer still names.
	f.remove("temporaless/v2/default/pull:odds/20260925T075000/workflow.binpb")
	before := f.snapshot()
	response, err := f.service(inspection.Options{}).ListWorkflowDirectory(context.Background(), &inspectionv1.ListWorkflowDirectoryRequest{Store: "engine", Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]*inspectionv1.WorkflowDirectoryEntry{}
	for _, entry := range response.GetEntries() {
		byID[entry.GetPointer().GetKey().GetWorkflowId()] = entry
	}
	weather := byID["pull:weather"]
	if weather.GetPending().GetReason() != inspectionv1.RunPendingReason_RUN_PENDING_REASON_RETRYING || weather.GetPending().GetResourceId() != "fetch:page-3" {
		t.Fatalf("weather pending = %v", weather.GetPending())
	}
	if byID["pull:crypto"].GetPending().GetReason() != inspectionv1.RunPendingReason_RUN_PENDING_REASON_TERMINAL {
		t.Fatalf("crypto pending = %v", byID["pull:crypto"].GetPending())
	}
	odds := byID["pull:odds"]
	if !odds.GetStalePointer() || odds.GetRun() != nil {
		t.Fatalf("odds stale=%t run=%v, want stale with no run", odds.GetStalePointer(), odds.GetRun())
	}
	if byID["pull:crypto"].GetRun().GetAnnotations()["feed"] != "pull:crypto" {
		t.Fatalf("run summary lost annotations")
	}
	assertUnchanged(t, before, f.snapshot())
}

func TestListWorkflowDirectoryPaging(t *testing.T) {
	f := directoryFixture(t)
	limits := inspection.DefaultLimits()
	limits.ReadBudget = 2
	service := f.service(inspection.Options{Limits: limits})
	request := &inspectionv1.ListWorkflowDirectoryRequest{Store: "engine", Namespace: "default", PageSize: 1, Status: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED}
	var got []string
	pages := 0
	for {
		response, err := service.ListWorkflowDirectory(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if response.GetScanned() > 2 {
			t.Fatalf("scanned %d pointers, budget is 2", response.GetScanned())
		}
		for _, entry := range response.GetEntries() {
			got = append(got, entry.GetRun().GetKey().GetWorkflowId())
		}
		if response.GetNextPageToken() == "" {
			break
		}
		request.PageToken = response.GetNextPageToken()
	}
	if fmt.Sprint(got) != "[backfill:equities pull:crypto pull:odds]" {
		t.Fatalf("paged workflows = %v", got)
	}
	if pages < 3 {
		t.Fatalf("pages = %d, want the budget to force several", pages)
	}

	// A token only opens under the filters that produced it.
	first, err := service.ListWorkflowDirectory(context.Background(), &inspectionv1.ListWorkflowDirectoryRequest{Store: "engine", Namespace: "default", PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ListWorkflowDirectory(context.Background(), &inspectionv1.ListWorkflowDirectoryRequest{
		Store: "engine", Namespace: "default", PageSize: 1, WorkflowIdPrefix: "pull:", PageToken: first.GetNextPageToken(),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("replayed token code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestListWorkflowRuns(t *testing.T) {
	f := newFixture(t)
	completed := temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED
	for index, runID := range []string{"20260925T070000", "20260925T074500", "20260925T080000"} {
		f.workflow(key("pull:weather", runID), completed, time.Duration(index-3)*time.Hour, duration(time.Duration(index-3)*time.Hour+time.Minute))
	}
	// A run directory with records but no workflow record has no trusted
	// identity: it is counted, not shown.
	f.activity(key("pull:weather", "20260925T060000"), "fetch", completed_(), []*temporalessv1.ActivityAttempt{attempt(1, -5*time.Hour, -5*time.Hour+time.Second, "")}, nil)
	service := f.service(inspection.Options{})

	tests := []struct {
		name      string
		ascending bool
		pageSize  uint32
		want      []string
		missing   uint32
	}{
		{"newest first by default", false, 0, []string{"20260925T080000", "20260925T074500", "20260925T070000"}, 1},
		{"ascending", true, 0, []string{"20260925T070000", "20260925T074500", "20260925T080000"}, 1},
		{"paged newest first", false, 2, []string{"20260925T080000", "20260925T074500", "20260925T070000"}, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := &inspectionv1.ListWorkflowRunsRequest{Store: "engine", Namespace: "default", WorkflowId: "pull:weather", Ascending: test.ascending, PageSize: test.pageSize}
			var got []string
			var missing uint32
			for {
				response, err := service.ListWorkflowRuns(context.Background(), request)
				if err != nil {
					t.Fatal(err)
				}
				if response.GetTotalListed() != 4 {
					t.Fatalf("total listed = %d, want 4", response.GetTotalListed())
				}
				missing += response.GetRunsWithoutWorkflowRecord()
				for _, run := range response.GetRuns() {
					got = append(got, run.GetKey().GetRunId())
				}
				if response.GetNextPageToken() == "" {
					break
				}
				request.PageToken = response.GetNextPageToken()
			}
			if fmt.Sprint(got) != fmt.Sprint(test.want) || missing != test.missing {
				t.Fatalf("runs = %v (missing %d), want %v (missing %d)", got, missing, test.want, test.missing)
			}
		})
	}

	limits := inspection.DefaultLimits()
	limits.MaxListedRuns = 3
	_, err := f.service(inspection.Options{Limits: limits}).ListWorkflowRuns(context.Background(), &inspectionv1.ListWorkflowRunsRequest{Store: "engine", Namespace: "default", WorkflowId: "pull:weather"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("over-bound listing code = %s, want FailedPrecondition", status.Code(err))
	}
	_, err = service.ListWorkflowRuns(context.Background(), &inspectionv1.ListWorkflowRunsRequest{Store: "engine", Namespace: "default", WorkflowId: "_latest"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reserved workflow id code = %s, want InvalidArgument", status.Code(err))
	}
}

func completed_() temporalessv1.ActivityStatus {
	return temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED
}

func TestListScheduledWakes(t *testing.T) {
	f := newFixture(t)
	scheduled := temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED
	sleep := temporalessv1.TimerKind_TIMER_KIND_SLEEP
	running := temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS

	onTime := key("pull:a", "r1")
	f.workflow(onTime, running, -time.Hour, nil)
	f.timer(onTime, "sleep:1", sleep, scheduled, 5*time.Minute)

	late := key("pull:b", "r1")
	f.workflow(late, running, -time.Hour, nil)
	f.timer(late, "sleep:1", sleep, scheduled, -44*time.Minute)

	fired := key("pull:c", "r1")
	f.workflow(fired, running, -time.Hour, nil)
	f.timer(fired, "sleep:1", sleep, temporalessv1.TimerStatus_TIMER_STATUS_FIRED, -30*time.Minute)

	interrupted := key("pull:d", "r1")
	f.workflow(interrupted, running, -time.Hour, nil)
	timer := f.timer(interrupted, "sleep:1", sleep, scheduled, time.Hour)
	f.remove(mustPath(t, storage.TimerKeyFromProto(timer.GetKey()).Path))

	differs := key("pull:e", "r1")
	f.workflow(differs, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, -time.Hour, duration(-time.Minute))
	timer = f.timer(differs, "sleep:1", sleep, scheduled, 2*time.Hour)
	changed := proto.Clone(timer).(*temporalessv1.TimerRecord)
	changed.FireAt = at(3 * time.Hour)
	f.write(mustPath(t, storage.TimerKeyFromProto(timer.GetKey()).Path), mustMarshal(t, changed))

	f.write("temporaless/v2/default/_due/pull:f/r1/sleep:1.binpb", []byte("torn write"))
	f.write("temporaless/v2/default/_due_invalid/0f1e.binpb", []byte("quarantined"))
	before := f.snapshot()

	service := f.service(inspection.Options{})
	response, err := service.ListScheduledWakes(context.Background(), &inspectionv1.ListScheduledWakesRequest{Store: "engine", Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		workflow string
		overdue  bool
		state    inspectionv1.WakeLedgerState
		parent   temporalessv1.WorkflowStatus
	}
	var got []row
	for _, wake := range response.GetWakes() {
		got = append(got, row{wake.GetWorkflowKey().GetWorkflowId(), wake.GetOverdue(), wake.GetLedgerState(), wake.GetWorkflowStatus()})
	}
	want := []row{
		{"pull:a", false, inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_AGREES, running},
		{"pull:b", true, inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_AGREES, running},
		{"pull:d", false, inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_CANONICAL_MISSING, running},
		{"pull:e", false, inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_CANONICAL_DIFFERS, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("wakes = %v\nwant    %v", got, want)
	}
	if response.GetInvalidEntries() != 1 || response.GetQuarantinedEntries() != 1 || response.GetScanned() != 6 {
		t.Fatalf("invalid=%d quarantined=%d scanned=%d", response.GetInvalidEntries(), response.GetQuarantinedEntries(), response.GetScanned())
	}

	overdue, err := service.ListScheduledWakes(context.Background(), &inspectionv1.ListScheduledWakesRequest{Store: "engine", Namespace: "default", OverdueOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(overdue.GetWakes()) != 1 || overdue.GetWakes()[0].GetWorkflowKey().GetWorkflowId() != "pull:b" {
		t.Fatalf("overdue wakes = %v", overdue.GetWakes())
	}

	// Reporting never repairs: the missing canonical timer stays missing and
	// the torn ledger entry stays where it is.
	assertUnchanged(t, before, f.snapshot())
}

func TestCapabilitiesAndNamespacesFollowAccess(t *testing.T) {
	f := directoryFixture(t)
	hidden := &inspection.Store{ID: "tenant-b", Namespaces: []string{"default"}, Records: f.records, Bucket: inspection.NewOpenDALBucket(f.operator)}
	access := func(_ context.Context, store *inspection.Store) (inspection.Grant, bool, error) {
		if store.ID == "tenant-b" {
			return inspection.Grant{}, false, nil
		}
		return inspection.Grant{Namespaces: []string{"default"}}, true, nil
	}
	service, err := inspection.NewService([]*inspection.Store{hidden, f.store}, inspection.Options{Access: access, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := service.GetInspectionCapabilities(context.Background(), &inspectionv1.GetInspectionCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(capabilities.GetStores()) != 1 {
		t.Fatalf("stores = %v, want only engine", capabilities.GetStores())
	}
	engine := capabilities.GetStores()[0]
	if engine.GetStore() != "engine" || fmt.Sprint(engine.GetNamespaces()) != "[default]" || engine.GetPayloadsVisible() || engine.GetIndexedSearch() {
		t.Fatalf("engine capabilities = %v", engine)
	}
	if engine.GetOverdueGrace().AsDuration() != inspection.DefaultOverdueGrace {
		t.Fatalf("overdue grace = %s", engine.GetOverdueGrace().AsDuration())
	}

	namespaces, err := service.ListNamespaces(context.Background(), &inspectionv1.ListNamespacesRequest{Store: "engine"})
	if err != nil {
		t.Fatal(err)
	}
	if len(namespaces.GetNamespaces()) != 1 || !namespaces.GetNamespaces()[0].GetPresent() {
		t.Fatalf("namespaces = %v", namespaces.GetNamespaces())
	}
	for _, request := range []*inspectionv1.ListNamespacesRequest{{Store: "tenant-b"}, {Store: "absent"}} {
		if _, err := service.ListNamespaces(context.Background(), request); status.Code(err) != codes.NotFound {
			t.Fatalf("store %q code = %s, want NotFound", request.GetStore(), status.Code(err))
		}
	}
	_, err = service.ListWorkflowDirectory(context.Background(), &inspectionv1.ListWorkflowDirectoryRequest{Store: "engine", Namespace: "backfill"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ungranted namespace code = %s, want PermissionDenied", status.Code(err))
	}
}

func TestNewServiceRequiresAccess(t *testing.T) {
	f := newFixture(t)
	if _, err := inspection.NewService([]*inspection.Store{f.store}, inspection.Options{}); err == nil {
		t.Fatal("NewService accepted a nil Access")
	}
}
