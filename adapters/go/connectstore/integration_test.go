package connectstore_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/connectstore"
	"github.com/jim-technologies/temporaless/adapters/go/inspector"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// End-to-end test: workflow.Run drives an OpenDAL backend via a remote
// ConnectStore client. Proves the Store abstraction is transport-neutral by
// exercising replay, retries (RETRYING persistence), IN_PROGRESS marking, and
// inspector listing — all over HTTP.
func TestRemoteWorkflowRunEndToEnd(t *testing.T) {
	ctx := context.Background()

	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	backend := storage.NewOpenDALStore(operator)

	_, handler := connectstore.NewHTTPHandler(backend)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	remoteStore := connectstore.NewHTTPClientStore(server.Client(), server.URL)

	// First run: an activity that fails twice before succeeding. RETRYING
	// records get persisted between attempts via remote PutActivity calls.
	options := &workflow.Options{
		WorkflowId:  "remote:retry",
		RunId:       "2026-05-04",
		CodeVersion: "test-version",
	}
	policy := &temporalessv1.RetryPolicy{
		MaximumAttempts: 3,
		InitialInterval: durationpb.New(time.Millisecond),
	}

	calls := 0
	first, err := workflow.Run(
		ctx,
		remoteStore,
		options,
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return workflow.ExecuteActivity(
				ctx,
				&workflow.ActivityOptions{ActivityId: "fetch:remote", RetryPolicy: policy},
				input,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					calls++
					if calls < 3 {
						return nil, workflow.NewActivityError("rate_limited", "transient", nil)
					}
					return wrapperspb.String("ok:" + request.GetValue()), nil
				},
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.GetValue() != "ok:AAPL" {
		t.Fatalf("first result = %q", first.GetValue())
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}

	// Second run: replay through the remote store, no activity executions.
	calls = 0
	second, err := workflow.Run(
		ctx,
		remoteStore,
		options,
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			t.Fatal("workflow body should not re-execute on replay")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if second.GetValue() != "ok:AAPL" {
		t.Fatalf("replay result = %q", second.GetValue())
	}

	// Inspector via remote store: list completed workflows.
	completed, err := inspector.ListWorkflowsByStatus(
		ctx,
		remoteStore,
		temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(completed); got != 1 {
		t.Fatalf("completed via remote = %d, want 1", got)
	}
	if completed[0].GetKey().GetWorkflowId() != "remote:retry" {
		t.Fatalf("completed workflow = %q", completed[0].GetKey().GetWorkflowId())
	}

	// List activities via remote store and confirm full attempt history persisted.
	activities, err := remoteStore.ListActivities(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "remote:retry",
		RunID:      "2026-05-04",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(activities); got != 1 {
		t.Fatalf("activities = %d, want 1", got)
	}
	if got := len(activities[0].GetAttempts()); got != 3 {
		t.Fatalf("attempts persisted via remote = %d, want 3", got)
	}

	// Reset via remote store, then re-run drives a fresh execution.
	if err := inspector.ResetWorkflow(ctx, remoteStore, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "remote:retry",
		RunID:      "2026-05-04",
	}); err != nil {
		t.Fatal(err)
	}
	if err := inspector.ResetActivity(ctx, remoteStore, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "remote:retry",
		RunID:      "2026-05-04",
		ActivityID: "fetch:remote",
	}); err != nil {
		t.Fatal(err)
	}

	calls = 0
	final, err := workflow.Run(
		ctx,
		remoteStore,
		options,
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return workflow.ExecuteActivity(
				ctx,
				&workflow.ActivityOptions{ActivityId: "fetch:remote", RetryPolicy: policy},
				input,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					calls++
					return wrapperspb.String("fresh:" + request.GetValue()), nil
				},
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if final.GetValue() != "fresh:AAPL" {
		t.Fatalf("post-reset result = %q", final.GetValue())
	}
	if calls != 1 {
		t.Fatalf("post-reset calls = %d, want 1", calls)
	}
}
