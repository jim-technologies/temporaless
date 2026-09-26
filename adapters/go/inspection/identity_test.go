package inspection_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Identity comes from payloads, never from paths: every decoded record must
// construct the path it was read from. These tests plant records that are
// valid in themselves but stored under another record's name.

func (f *fixture) read(path string) []byte {
	f.t.Helper()
	data, err := os.ReadFile(filepath.Join(f.root, filepath.FromSlash(path)))
	if err != nil {
		f.t.Fatal(err)
	}
	return data
}

func TestDescribeRunRejectsMisplacedRecords(t *testing.T) {
	run := key("pull:weather", "20260925T080000")
	runDir := "temporaless/v2/default/pull:weather/20260925T080000/"
	tests := []struct {
		name  string
		plant func(f *fixture)
	}{
		{
			name: "activity stored under another activity's name",
			plant: func(f *fixture) {
				f.activity(run, "fetch:page-1", temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
					[]*temporalessv1.ActivityAttempt{attempt(1, -3*time.Minute, -2*time.Minute, "")}, nil)
				f.write(runDir+"activity/fetch:page-2.binpb", f.read(runDir+"activity/fetch:page-1.binpb"))
			},
		},
		{
			name: "event stored under another event's name",
			plant: func(f *fixture) {
				f.event(run, "approval", -time.Minute)
				f.write(runDir+"event/rejection.binpb", f.read(runDir+"event/approval.binpb"))
			},
		},
		{
			name: "claim stored under another claim's name",
			plant: func(f *fixture) {
				f.claim(run, "workflow", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW, 4*time.Minute)
				f.write(runDir+"claim/other.binpb", f.read(runDir+"claim/workflow.binpb"))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			f.workflow(run, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, -10*time.Minute, nil)
			test.plant(f)
			_, err := f.service(inspection.Options{}).DescribeRun(context.Background(), &inspectionv1.DescribeRunRequest{Store: "engine", Key: run.Proto()})
			if status.Code(err) != codes.DataLoss {
				t.Fatalf("code = %s (%v), want DataLoss for a record stored at another record's key", status.Code(err), err)
			}
		})
	}
}

func TestListingsIgnoreMisplacedRecords(t *testing.T) {
	f := directoryFixture(t)
	// A valid latest-run pointer for pull:odds, stored as if it were the
	// pointer of workflow pull:ghost.
	f.write("temporaless/v2/default/_latest/pull:ghost.binpb", f.read("temporaless/v2/default/_latest/pull:odds.binpb"))
	// A valid workflow record of pull:crypto's run, copied into another run
	// directory of the same workflow.
	f.write("temporaless/v2/default/pull:crypto/20260925T090000/workflow.binpb",
		f.read("temporaless/v2/default/pull:crypto/20260925T074500/workflow.binpb"))
	service := f.service(inspection.Options{})

	directory, err := service.ListWorkflowDirectory(context.Background(), &inspectionv1.ListWorkflowDirectoryRequest{Store: "engine", Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	var workflows []string
	for _, entry := range directory.GetEntries() {
		workflows = append(workflows, entry.GetPointer().GetKey().GetWorkflowId())
	}
	want := []string{"backfill:equities", "pull:crypto", "pull:odds", "pull:polymarket", "pull:weather"}
	if fmt.Sprint(workflows) != fmt.Sprint(want) || directory.GetScanned() != 6 {
		t.Fatalf("directory = %v (scanned %d), want %v with the misplaced pointer scanned and skipped", workflows, directory.GetScanned(), want)
	}

	runs, err := service.ListWorkflowRuns(context.Background(), &inspectionv1.ListWorkflowRunsRequest{Store: "engine", Namespace: "default", WorkflowId: "pull:crypto"})
	if err != nil {
		t.Fatal(err)
	}
	var runIDs []string
	for _, summary := range runs.GetRuns() {
		runIDs = append(runIDs, summary.GetKey().GetRunId())
	}
	if fmt.Sprint(runIDs) != "[20260925T074500]" || runs.GetRunsWithoutWorkflowRecord() != 1 || runs.GetTotalListed() != 2 {
		t.Fatalf("runs = %v (without record %d, listed %d), want the copy counted, not shown", runIDs, runs.GetRunsWithoutWorkflowRecord(), runs.GetTotalListed())
	}
}
