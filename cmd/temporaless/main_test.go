package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CLI tests exercise the `run` entrypoint directly with in-process args. The
// CLI is intentionally a thin wrapper around inspector / janitor adapters, so
// the test surface is: does the right adapter get called, with the right
// inputs, and does the output format match expectations.

func newTestRoot(t *testing.T) (string, *storage.OpenDALStore) {
	t.Helper()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{
		"root": root,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	return root, storage.NewOpenDALStore(operator)
}

func seedWorkflow(t *testing.T, store *storage.OpenDALStore, workflowID, runID string, status temporalessv1.WorkflowStatus) {
	t.Helper()
	key := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflowID,
		RunID:      runID,
	}
	now := timestamppb.New(time.Now().UTC())
	record := &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
		CodeVersion:   "test",
		InputDigest:   "deadbeef",
		Status:        status,
		CreatedAt:     now,
	}
	if status == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		record.CompletedAt = now
	}
	if err := store.PutWorkflow(context.Background(), record); err != nil {
		t.Fatal(err)
	}
}

func seedActivity(t *testing.T, store *storage.OpenDALStore, workflowID, runID, activityID string) {
	t.Helper()
	key := storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflowID,
		RunID:      runID,
		ActivityID: activityID,
	}
	now := timestamppb.New(time.Now().UTC())
	record := &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key:           key.Proto(),
		ActivityType:  "activity:google.protobuf.StringValue->google.protobuf.StringValue",
		CodeVersion:   "test",
		InputDigest:   "abc",
		Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
		CreatedAt:     now,
		CompletedAt:   now,
	}
	if err := store.PutActivity(context.Background(), record); err != nil {
		t.Fatal(err)
	}
}

func TestCLIListWorkflowsTextOutput(t *testing.T) {
	root, store := newTestRoot(t)
	seedWorkflow(t, store, "wf-a", "run-1", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	seedWorkflow(t, store, "wf-b", "run-2", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED)

	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--store-scheme", "fs",
		"--store-root", root,
		"list-workflows",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("err=%v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "wf-a/run-1") {
		t.Errorf("expected wf-a/run-1 in output: %s", out)
	}
	if !strings.Contains(out, "wf-b/run-2") {
		t.Errorf("expected wf-b/run-2 in output: %s", out)
	}
	if !strings.Contains(out, "WORKFLOW_STATUS_COMPLETED") {
		t.Errorf("expected completed status in output: %s", out)
	}
}

func TestCLIListWorkflowsStatusFilter(t *testing.T) {
	root, store := newTestRoot(t)
	seedWorkflow(t, store, "wf-a", "run-1", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	seedWorkflow(t, store, "wf-b", "run-2", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED)

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"--store-scheme", "fs",
		"--store-root", root,
		"list-workflows", "--status", "failed",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("err=%v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	if strings.Contains(out, "wf-a/run-1") {
		t.Errorf("expected wf-a NOT in failed-only output: %s", out)
	}
	if !strings.Contains(out, "wf-b/run-2") {
		t.Errorf("expected wf-b in failed output: %s", out)
	}
}

func TestCLIListWorkflowsJSON(t *testing.T) {
	root, store := newTestRoot(t)
	seedWorkflow(t, store, "wf-a", "run-1", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"--store-scheme", "fs",
		"--store-root", root,
		"--json",
		"list-workflows",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("err=%v stderr=%s", err, stderr.String())
	}
	var records []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &records); err != nil {
		t.Fatalf("json unmarshal: %v\noutput: %s", err, stdout.String())
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0]["codeVersion"] != "test" {
		t.Errorf("unexpected codeVersion: %v", records[0]["codeVersion"])
	}
}

func TestCLIListActivities(t *testing.T) {
	root, store := newTestRoot(t)
	seedWorkflow(t, store, "wf-a", "run-1", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	seedActivity(t, store, "wf-a", "run-1", "act:1")
	seedActivity(t, store, "wf-a", "run-1", "act:2")

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"--store-scheme", "fs",
		"--store-root", root,
		"list-activities", "--workflow-id", "wf-a", "--run-id", "run-1",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("err=%v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "act:1") || !strings.Contains(out, "act:2") {
		t.Errorf("expected both activities in output: %s", out)
	}
}

func TestCLIGetWorkflow(t *testing.T) {
	root, store := newTestRoot(t)
	seedWorkflow(t, store, "wf-a", "run-1", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"--store-scheme", "fs",
		"--store-root", root,
		"get-workflow", "--workflow-id", "wf-a", "--run-id", "run-1",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("err=%v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "status=WORKFLOW_STATUS_COMPLETED") {
		t.Errorf("expected status line in output: %s", out)
	}
	if !strings.Contains(out, "input_digest=deadbeef") {
		t.Errorf("expected input_digest line in output: %s", out)
	}
}

func TestCLIGetWorkflowNotFound(t *testing.T) {
	root, _ := newTestRoot(t)
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--store-scheme", "fs",
		"--store-root", root,
		"get-workflow", "--workflow-id", "nope", "--run-id", "x",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for missing workflow")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error: %v", err)
	}
}

func TestCLIResetWorkflow(t *testing.T) {
	root, store := newTestRoot(t)
	seedWorkflow(t, store, "wf-a", "run-1", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED)

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"--store-scheme", "fs",
		"--store-root", root,
		"reset-workflow", "--workflow-id", "wf-a", "--run-id", "run-1",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("err=%v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "reset workflow wf-a/run-1") {
		t.Errorf("unexpected output: %s", stdout.String())
	}
	// Confirm the record actually went away.
	_, found, err := store.GetWorkflow(context.Background(), storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "wf-a",
		RunID:      "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("expected workflow to be deleted")
	}
}

func TestCLIResetActivity(t *testing.T) {
	root, store := newTestRoot(t)
	seedWorkflow(t, store, "wf-a", "run-1", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED)
	seedActivity(t, store, "wf-a", "run-1", "act:1")

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"--store-scheme", "fs",
		"--store-root", root,
		"reset-activity", "--workflow-id", "wf-a", "--run-id", "run-1", "--activity-id", "act:1",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("err=%v stderr=%s", err, stderr.String())
	}
	_, found, err := store.GetActivity(context.Background(), storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "wf-a",
		RunID:      "run-1",
		ActivityID: "act:1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("expected activity to be deleted")
	}
}

func TestCLISweep(t *testing.T) {
	root, store := newTestRoot(t)
	// Old completed record (created_at and completed_at backdated).
	key := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "wf-old",
		RunID:      "run-1",
	}
	old := timestamppb.New(time.Now().UTC().Add(-48 * time.Hour))
	if err := store.PutWorkflow(context.Background(), &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
		CodeVersion:   "test",
		InputDigest:   "x",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		CreatedAt:     old,
		CompletedAt:   old,
	}); err != nil {
		t.Fatal(err)
	}
	seedWorkflow(t, store, "wf-new", "run-2", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"--store-scheme", "fs",
		"--store-root", root,
		"sweep", "--max-age", "24h",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("err=%v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "deleted 1 runs") {
		t.Errorf("expected 'deleted 1 runs' in output: %s", stdout.String())
	}
	// Old should be gone, new should remain.
	_, found, err := store.GetWorkflow(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("expected old workflow swept")
	}
	_, foundNew, err := store.GetWorkflow(context.Background(), storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "wf-new",
		RunID:      "run-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !foundNew {
		t.Error("expected new workflow to remain")
	}
}

func TestCLIRejectsMissingStoreRoot(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"list-workflows"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error when --store-root is missing")
	}
	if !strings.Contains(err.Error(), "store-root") {
		t.Errorf("expected store-root in error: %v", err)
	}
}

func TestCLIRejectsUnknownSubcommand(t *testing.T) {
	root, _ := newTestRoot(t)
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--store-scheme", "fs",
		"--store-root", root,
		"frobnicate",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
	if !strings.Contains(err.Error(), "frobnicate") {
		t.Errorf("expected subcommand name in error: %v", err)
	}
}

func TestCLIHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"help"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "SUBCOMMANDS") {
		t.Errorf("expected usage in stdout: %s", stdout.String())
	}
}
