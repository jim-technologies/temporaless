package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDescribeRunRequiresRunIdentity(t *testing.T) {
	_, store := newTestRoot(t)
	tests := []struct {
		name string
		args []string
	}{
		{name: "both missing"},
		{name: "workflow missing", args: []string{"--run-id", "run-1"}},
		{name: "run missing", args: []string{"--workflow-id", "wf-1"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			err := cmdDescribeRun(
				context.Background(),
				store,
				nil,
				globalOpts{},
				test.args,
				&stdout,
			)
			if err == nil || !strings.Contains(err.Error(), "--workflow-id and --run-id are required") {
				t.Fatalf("error = %v, want required workflow and run IDs", err)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

func TestDescribeRunReportsNoInspectableRecordsForEmptyRun(t *testing.T) {
	_, store := newTestRoot(t)
	var stdout bytes.Buffer
	err := cmdDescribeRun(
		context.Background(),
		store,
		nil,
		globalOpts{},
		[]string{"--workflow-id", "missing", "--run-id", "run-1"},
		&stdout,
	)
	if err == nil || !strings.Contains(err.Error(), "no inspectable records") {
		t.Fatalf("error = %v, want no inspectable records", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestDescribeRunAllowsOrphanChildRecord(t *testing.T) {
	_, store := newTestRoot(t)
	seedActivity(t, store, "orphan", "run-1", "activity-1")

	var stdout bytes.Buffer
	if err := cmdDescribeRun(
		context.Background(),
		store,
		nil,
		globalOpts{},
		[]string{"--workflow-id", "orphan", "--run-id", "run-1"},
		&stdout,
	); err != nil {
		t.Fatal(err)
	}
	out := stdout.String()
	if !strings.Contains(out, "workflow=missing") {
		t.Fatalf("output does not preserve missing workflow state:\n%s", out)
	}
	if !strings.Contains(out, "activity-1") {
		t.Fatalf("output does not include orphan activity:\n%s", out)
	}
}

func TestDescribeRunTextIsDeterministicAndComplete(t *testing.T) {
	_, store := newTestRoot(t)
	seedDescribeRun(t, store, "wf-1", "run-1")
	args := []string{"--workflow-id", "wf-1", "--run-id", "run-1"}

	var first, second bytes.Buffer
	for _, output := range []*bytes.Buffer{&first, &second} {
		if err := cmdDescribeRun(
			context.Background(),
			store,
			nil,
			globalOpts{},
			args,
			output,
		); err != nil {
			t.Fatal(err)
		}
	}
	if first.String() != second.String() {
		t.Fatalf("describe output changed between identical reads:\nfirst:\n%s\nsecond:\n%s", first.String(), second.String())
	}

	out := first.String()
	for _, want := range []string{
		"run=default/wf-1/run-1",
		"snapshot_consistency=non-atomic",
		"WORKFLOW_STATUS_IN_PROGRESS",
		"activity-a",
		"activity-z",
		"attempts=1",
		"timer-a",
		"timer-z",
		"event-a",
		"event-z",
		"claims=not-inspected",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output does not contain %q:\n%s", want, out)
		}
	}
	assertOrdered(t, out, "activity-a", "activity-z")
	assertOrdered(t, out, "timer-a", "timer-z")
	assertOrdered(t, out, "event-a", "event-z")
}

func TestDescribeRunJSONIsDeterministicAndPreservesUnknownAny(t *testing.T) {
	root, store := newTestRoot(t)
	typeURL, payload := seedDescribeRun(t, store, "wf-1", "run-1")
	args := []string{
		"--store-root", root,
		"--json",
		"describe-run",
		"--workflow-id", "wf-1",
		"--run-id", "run-1",
	}

	var first, second bytes.Buffer
	for _, output := range []*bytes.Buffer{&first, &second} {
		var stderr bytes.Buffer
		if err := run(context.Background(), args, output, &stderr); err != nil {
			t.Fatalf("describe-run: %v\nstderr: %s", err, stderr.String())
		}
	}
	if first.String() != second.String() {
		t.Fatalf("JSON output changed between identical reads:\nfirst:\n%s\nsecond:\n%s", first.String(), second.String())
	}

	var got struct {
		FormatVersion       uint32 `json:"formatVersion"`
		SnapshotConsistency string `json:"snapshotConsistency"`
		Key                 struct {
			Namespace  string `json:"namespace"`
			WorkflowID string `json:"workflowId"`
			RunID      string `json:"runId"`
		} `json:"key"`
		Workflow struct {
			Input struct {
				TypeURL     string `json:"typeUrl"`
				ValueBase64 string `json:"valueBase64"`
			} `json:"input"`
		} `json:"workflow"`
		Activities []struct {
			Key struct {
				ActivityID string `json:"activityId"`
			} `json:"key"`
			Input  cliAny `json:"input"`
			Result cliAny `json:"result"`
		} `json:"activities"`
		Timers []struct {
			Key struct {
				TimerID string `json:"timerId"`
			} `json:"key"`
		} `json:"timers"`
		Events []struct {
			Key struct {
				EventID string `json:"eventId"`
			} `json:"key"`
			Payload cliAny `json:"payload"`
		} `json:"events"`
		Claims          []json.RawMessage `json:"claims"`
		ClaimsInspected bool              `json:"claimsInspected"`
	}
	if err := json.Unmarshal(first.Bytes(), &got); err != nil {
		t.Fatalf("decode describe JSON: %v\n%s", err, first.String())
	}
	if got.FormatVersion != runDescriptionFormatVersion {
		t.Errorf("formatVersion = %d, want %d", got.FormatVersion, runDescriptionFormatVersion)
	}
	if got.SnapshotConsistency != "non-atomic" {
		t.Errorf("snapshotConsistency = %q, want non-atomic", got.SnapshotConsistency)
	}
	if got.Key.Namespace != storage.DefaultNamespace || got.Key.WorkflowID != "wf-1" || got.Key.RunID != "run-1" {
		t.Errorf("key = %+v, want default/wf-1/run-1", got.Key)
	}
	if got.Workflow.Input.TypeURL != typeURL {
		t.Errorf("input.typeUrl = %q, want %q", got.Workflow.Input.TypeURL, typeURL)
	}
	if got.Workflow.Input.ValueBase64 != base64.StdEncoding.EncodeToString(payload) {
		t.Errorf("input.valueBase64 = %q, want %q", got.Workflow.Input.ValueBase64, base64.StdEncoding.EncodeToString(payload))
	}
	if len(got.Activities) != 2 ||
		got.Activities[0].Key.ActivityID != "activity-a" ||
		got.Activities[1].Key.ActivityID != "activity-z" {
		t.Errorf("activity order = %+v, want activity-a then activity-z", got.Activities)
	}
	for _, activity := range got.Activities {
		activityID := activity.Key.ActivityID
		if activity.Input.TypeURL != "type.googleapis.com/example.workflow.v1.ActivityRequest" ||
			activity.Input.ValueBase64 != base64.StdEncoding.EncodeToString([]byte("input:"+activityID)) {
			t.Errorf("activity %s input = %+v", activityID, activity.Input)
		}
		if activity.Result.TypeURL != "type.googleapis.com/example.workflow.v1.ActivityResponse" ||
			activity.Result.ValueBase64 != base64.StdEncoding.EncodeToString([]byte("result:"+activityID)) {
			t.Errorf("activity %s result = %+v", activityID, activity.Result)
		}
	}
	if len(got.Timers) != 2 ||
		got.Timers[0].Key.TimerID != "timer-a" ||
		got.Timers[1].Key.TimerID != "timer-z" {
		t.Errorf("timer order = %+v, want timer-a then timer-z", got.Timers)
	}
	if len(got.Events) != 2 ||
		got.Events[0].Key.EventID != "event-a" ||
		got.Events[1].Key.EventID != "event-z" {
		t.Errorf("event order = %+v, want event-a then event-z", got.Events)
	}
	for _, event := range got.Events {
		if event.Payload.TypeURL != "type.googleapis.com/example.workflow.v1.Approval" ||
			event.Payload.ValueBase64 != base64.StdEncoding.EncodeToString([]byte(event.Key.EventID)) {
			t.Errorf("event %s payload = %+v", event.Key.EventID, event.Payload)
		}
	}
	if got.Claims == nil || len(got.Claims) != 0 {
		t.Errorf("claims = %#v, want an explicit empty array", got.Claims)
	}
	if got.ClaimsInspected {
		t.Error("claimsInspected = true for the local OpenDAL store, want false")
	}
}

func TestDescribeRunRejectsCorruptPointRecordsWithoutPartialOutput(t *testing.T) {
	tests := []struct {
		name       string
		corruptKey func(storage.WorkflowKey) (string, error)
	}{
		{
			name: "workflow",
			corruptKey: func(key storage.WorkflowKey) (string, error) {
				return key.Path()
			},
		},
		{
			name: "activity",
			corruptKey: func(key storage.WorkflowKey) (string, error) {
				return (storage.ActivityKey{
					Namespace: key.Namespace, WorkflowID: key.WorkflowID,
					RunID: key.RunID, ActivityID: "activity-a",
				}).Path()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, store := newTestRoot(t)
			seedDescribeRun(t, store, "wf-1", "run-1")
			key := storage.WorkflowKey{
				Namespace:  storage.DefaultNamespace,
				WorkflowID: "wf-1",
				RunID:      "run-1",
			}
			relative, err := test.corruptKey(key)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, relative), []byte{0xff}, 0o600); err != nil {
				t.Fatal(err)
			}

			var stdout bytes.Buffer
			err = cmdDescribeRun(
				context.Background(),
				store,
				nil,
				globalOpts{json: true},
				[]string{"--workflow-id", "wf-1", "--run-id", "run-1"},
				&stdout,
			)
			if !errors.Is(err, storage.ErrCorruptRecord) {
				t.Fatalf("error = %v, want ErrCorruptRecord", err)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

func TestDescribeRunClaimsAreCapabilityDriven(t *testing.T) {
	_, store := newTestRoot(t)
	seedWorkflow(t, store, "wf-1", "run-1", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS)
	key := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "wf-1",
		RunID:      "run-1",
	}
	claim := func(claimID string) *temporalessv1.ClaimRecord {
		return &temporalessv1.ClaimRecord{
			SchemaVersion: storage.ClaimRecordSchemaVersion,
			Key: (&storage.ClaimKey{
				Namespace:  key.Namespace,
				WorkflowID: key.WorkflowID,
				RunID:      key.RunID,
				ClaimID:    claimID,
			}).Proto(),
			ResourceType: temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
			ResourceId:   "wf-1",
		}
	}
	lister := &describeClaimLister{records: []*temporalessv1.ClaimRecord{
		claim("claim-z"),
		claim("claim-a"),
	}}

	var stdout bytes.Buffer
	if err := cmdDescribeRun(
		context.Background(),
		store,
		lister,
		globalOpts{json: true},
		[]string{"--workflow-id", "wf-1", "--run-id", "run-1"},
		&stdout,
	); err != nil {
		t.Fatal(err)
	}
	var output struct {
		Claims []struct {
			Key struct {
				ClaimID string `json:"claimId"`
			} `json:"key"`
		} `json:"claims"`
		ClaimsInspected bool `json:"claimsInspected"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if !output.ClaimsInspected {
		t.Fatal("claimsInspected = false, want true")
	}
	if len(output.Claims) != 2 ||
		output.Claims[0].Key.ClaimID != "claim-a" ||
		output.Claims[1].Key.ClaimID != "claim-z" {
		t.Fatalf("claims = %+v, want claim-a then claim-z", output.Claims)
	}

	claimErr := errors.New("claim backend unavailable")
	stdout.Reset()
	err := cmdDescribeRun(
		context.Background(),
		store,
		&describeClaimLister{err: claimErr},
		globalOpts{json: true},
		[]string{"--workflow-id", "wf-1", "--run-id", "run-1"},
		&stdout,
	)
	if !errors.Is(err, claimErr) {
		t.Fatalf("error = %v, want wrapped claim backend error", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q after failed snapshot, want empty", stdout.String())
	}
}

type describeClaimLister struct {
	records []*temporalessv1.ClaimRecord
	err     error
}

func (lister *describeClaimLister) ListClaims(
	_ context.Context,
	_ storage.WorkflowKey,
) ([]*temporalessv1.ClaimRecord, error) {
	return lister.records, lister.err
}

func seedDescribeRun(t *testing.T, store *storage.OpenDALStore, workflowID, runID string) (string, []byte) {
	t.Helper()
	ctx := context.Background()
	now := timestamppb.New(time.Date(2026, time.August, 12, 9, 30, 0, 0, time.UTC))
	workflowKey := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflowID,
		RunID:      runID,
	}
	typeURL := "type.googleapis.com/example.workflow.v1.StartRequest"
	payload := []byte{0x00, 0x01, 0x02, 0xff}
	if err := store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           workflowKey.Proto(),
		WorkflowType:  "workflow:example.workflow.v1.StartRequest->example.workflow.v1.StartResponse",
		Input:         &anypb.Any{TypeUrl: typeURL, Value: payload},
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		CreatedAt:     now,
		Annotations:   map[string]string{"environment": "test"},
	}); err != nil {
		t.Fatal(err)
	}

	for _, activityID := range []string{"activity-z", "activity-a"} {
		key := storage.ActivityKey{
			Namespace:  workflowKey.Namespace,
			WorkflowID: workflowID,
			RunID:      runID,
			ActivityID: activityID,
		}
		if err := store.PutActivity(ctx, &temporalessv1.ActivityRecord{
			SchemaVersion: storage.ActivityRecordSchemaVersion,
			Key:           key.Proto(),
			ActivityType:  "activity:google.protobuf.StringValue->google.protobuf.StringValue",
			Input: &anypb.Any{
				TypeUrl: "type.googleapis.com/example.workflow.v1.ActivityRequest",
				Value:   []byte("input:" + activityID),
			},
			Status: temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
			Result: &anypb.Any{
				TypeUrl: "type.googleapis.com/example.workflow.v1.ActivityResponse",
				Value:   []byte("result:" + activityID),
			},
			CreatedAt:   now,
			CompletedAt: now,
			Attempts: []*temporalessv1.ActivityAttempt{{
				Attempt:     1,
				StartedAt:   now,
				CompletedAt: now,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, timerID := range []string{"timer-z", "timer-a"} {
		key := storage.TimerKey{
			Namespace:  workflowKey.Namespace,
			WorkflowID: workflowID,
			RunID:      runID,
			TimerID:    timerID,
		}
		if err := store.PutTimer(ctx, &temporalessv1.TimerRecord{
			SchemaVersion: storage.TimerRecordSchemaVersion,
			Key:           key.Proto(),
			TimerKind:     storage.SleepTimerKind,
			Duration:      durationpb.New(2 * time.Hour),
			Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
			FireAt:        timestamppb.New(now.AsTime().Add(2 * time.Hour)),
			CreatedAt:     now,
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, eventID := range []string{"event-z", "event-a"} {
		key := storage.EventKey{
			Namespace:  workflowKey.Namespace,
			WorkflowID: workflowID,
			RunID:      runID,
			EventID:    eventID,
		}
		if err := store.PutEvent(ctx, &temporalessv1.EventRecord{
			SchemaVersion: storage.EventRecordSchemaVersion,
			Key:           key.Proto(),
			Payload: &anypb.Any{
				TypeUrl: "type.googleapis.com/example.workflow.v1.Approval",
				Value:   []byte(eventID),
			},
			ReceivedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}

	return typeURL, payload
}

func assertOrdered(t *testing.T, text, first, second string) {
	t.Helper()
	firstIndex := strings.Index(text, first)
	secondIndex := strings.Index(text, second)
	if firstIndex < 0 || secondIndex < 0 || firstIndex >= secondIndex {
		t.Errorf("expected %q before %q in:\n%s", first, second, text)
	}
}
