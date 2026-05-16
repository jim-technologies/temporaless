package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Activity() is the ergonomic shortcut: auto-id from the function reference,
// default retry policy when not specified, result type inferred from the
// generic. Tests verify both the defaults and the override path.

func doubleInt32(_ context.Context, req *wrapperspb.Int32Value) (*wrapperspb.Int32Value, error) {
	return wrapperspb.Int32(req.GetValue() * 2), nil
}

func TestActivityInfersIDFromFunctionName(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{store: store, workflowID: "wf", runID: "r", codeVersion: "test"}
	ctx = context.WithValue(ctx, workflowContextKey{}, wf)

	result, err := Activity(ctx, doubleInt32, wrapperspb.Int32(7))
	if err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != 14 {
		t.Fatalf("result = %d, want 14", result.GetValue())
	}

	// Stored record's activity_id must equal what InferActivityID derived.
	inferredID, err := InferActivityID(doubleInt32)
	if err != nil {
		t.Fatal(err)
	}
	key := storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		ActivityID: inferredID,
	}
	record, found, err := store.GetActivity(ctx, key)
	if err != nil || !found {
		t.Fatalf("expected record under %q: err=%v found=%v", inferredID, err, found)
	}
	if record.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED {
		t.Fatalf("status = %s", record.GetStatus())
	}
}

func TestActivityWithExplicitID(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{store: store, workflowID: "wf", runID: "r", codeVersion: "test"}
	ctx = context.WithValue(ctx, workflowContextKey{}, wf)

	_, err := Activity(ctx, doubleInt32, wrapperspb.Int32(3), WithActivityID("custom:1"))
	if err != nil {
		t.Fatal(err)
	}

	// Inferred-id record must NOT exist.
	inferred, _ := InferActivityID(doubleInt32)
	if inferred != "" {
		_, found, _ := store.GetActivity(ctx, storage.ActivityKey{
			Namespace:  storage.DefaultNamespace,
			WorkflowID: wf.workflowID,
			RunID:      wf.runID,
			ActivityID: inferred,
		})
		if found {
			t.Fatalf("explicit id should suppress the inferred-id write under %q", inferred)
		}
	}
	// Custom-id record must exist.
	_, found, err := store.GetActivity(ctx, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		ActivityID: "custom:1",
	})
	if err != nil || !found {
		t.Fatalf("expected record under custom:1: err=%v found=%v", err, found)
	}
}

func TestActivityDefaultRetryPolicyRetries(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{store: store, workflowID: "wf", runID: "r", codeVersion: "test"}
	ctx = context.WithValue(ctx, workflowContextKey{}, wf)

	attempts := 0
	flaky := func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		attempts++
		if attempts < 2 {
			return nil, NewActivityError("transient", "first try fails", nil)
		}
		return wrapperspb.String("ok"), nil
	}

	result, err := Activity(ctx, flaky, wrapperspb.String("x"))
	if err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != "ok" {
		t.Fatalf("result = %q", result.GetValue())
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (default retry should give a second attempt)", attempts)
	}
}

func TestActivityExplicitRetryPolicyOverridesDefault(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{store: store, workflowID: "wf", runID: "r", codeVersion: "test"}
	ctx = context.WithValue(ctx, workflowContextKey{}, wf)

	attempts := 0
	alwaysFail := func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		attempts++
		return nil, NewActivityError("nope", "fail", nil)
	}

	_, err := Activity(ctx, alwaysFail, wrapperspb.String("x"),
		WithRetryPolicy(&temporalessv1.RetryPolicy{MaximumAttempts: 1}),
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (single-attempt override should disable retries)", attempts)
	}
}

func TestDefaultRetryPolicyShape(t *testing.T) {
	policy := DefaultRetryPolicy()
	if policy.GetMaximumAttempts() != 3 {
		t.Errorf("MaximumAttempts = %d, want 3", policy.GetMaximumAttempts())
	}
	if policy.GetBackoffCoefficient() != 2.0 {
		t.Errorf("BackoffCoefficient = %f, want 2.0", policy.GetBackoffCoefficient())
	}
	if got := policy.GetInitialInterval().AsDuration(); got != time.Second {
		t.Errorf("InitialInterval = %s, want 1s", got)
	}
	if got := policy.GetMaximumInterval().AsDuration(); got != 30*time.Second {
		t.Errorf("MaximumInterval = %s, want 30s", got)
	}
	if got := policy.GetDurableBackoffThreshold().AsDuration(); got != 30*time.Second {
		t.Errorf("DurableBackoffThreshold = %s, want 30s", got)
	}
}

func TestDefaultRetryPolicyReturnsFreshInstance(t *testing.T) {
	a := DefaultRetryPolicy()
	b := DefaultRetryPolicy()
	a.MaximumAttempts = 99
	if b.GetMaximumAttempts() != 3 {
		t.Fatalf("mutation on one returned policy leaked into another: b.MaximumAttempts = %d", b.GetMaximumAttempts())
	}
}

func TestInferActivityIDSanitizesPathSegments(t *testing.T) {
	// Reuse doubleInt32 — full name is "github.com/.../workflow.doubleInt32".
	// Inference must drop the path prefix and leave a path-safe id.
	id, err := InferActivityID(doubleInt32)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(id, "/") {
		t.Fatalf("inferred id %q still contains a path separator", id)
	}
	if !strings.Contains(id, "doubleInt32") {
		t.Fatalf("inferred id %q doesn't contain function name", id)
	}
	if !activityIDRegex.MatchString(id) {
		t.Fatalf("inferred id %q is not path-safe", id)
	}
}

func TestInferActivityIDRejectsNonFunction(t *testing.T) {
	_, err := InferActivityID(42)
	if err == nil || !errors.Is(err, err) {
		t.Fatal("expected error for non-function argument")
	}
	if !strings.Contains(err.Error(), "not a function") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Pin durationpb (used in DefaultRetryPolicy) so an unused import doesn't
// show up if a future refactor moves things around.
var _ = durationpb.New
