package workflow

import (
	"context"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Activity() is the ergonomic shortcut with explicit caller-owned IDs, a
// default retry policy, and a result type inferred from the generic.

func doubleInt32(_ context.Context, req *wrapperspb.Int32Value) (*wrapperspb.Int32Value, error) {
	return wrapperspb.Int32(req.GetValue() * 2), nil
}

func TestActivityRequiresExplicitID(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{store: store, workflowID: "wf", runID: "r", codeVersion: "test"}
	ctx = context.WithValue(ctx, workflowContextKey{}, wf)

	_, err := Activity(ctx, doubleInt32, wrapperspb.Int32(7))
	if err == nil {
		t.Fatal("expected missing activity_id error")
	}
}

func TestActivityWithExplicitID(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{store: store, workflowID: "wf", runID: "r", codeVersion: "test"}
	ctx = context.WithValue(ctx, workflowContextKey{}, wf)

	_, err := Activity(
		ctx,
		doubleInt32,
		wrapperspb.Int32(3),
		WithActivityID("custom:1"),
		WithRetryTimerID("retry:custom:1"),
	)
	if err != nil {
		t.Fatal(err)
	}

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

	result, err := Activity(
		ctx,
		flaky,
		wrapperspb.String("x"),
		WithActivityID("flaky"),
		WithRetryTimerID("retry:flaky"),
	)
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
		WithActivityID("always-fail"),
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
