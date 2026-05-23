package workflow_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// BenchmarkWorkflowRunFreshExecution measures the cost of a fresh
// workflow.Run with one activity from a clean store. This is the typical
// "first invocation" path.
func BenchmarkWorkflowRunFreshExecution(b *testing.B) {
	store := newBenchStore(b)
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		_, err := workflow.Run(
			ctx,
			store,
			&workflow.Options{
				WorkflowId:  "bench:fresh",
				RunId:       fmt.Sprintf("run-%05d", i),
				CodeVersion: "v1",
			},
			nil,
			wrapperspb.String("input"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return workflow.ExecuteActivity(
					ctx,
					&workflow.ActivityOptions{ActivityId: "fetch"},
					input,
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
						return wrapperspb.String("ok:" + request.GetValue()), nil
					},
				)
			},
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWorkflowRunReplay measures the cost of replaying a completed
// workflow — Get from store, identity check, return result. This is what
// happens on every duplicate invocation.
func BenchmarkWorkflowRunReplay(b *testing.B) {
	store := newBenchStore(b)
	ctx := context.Background()

	options := &workflow.Options{
		WorkflowId:  "bench:replay",
		RunId:       "shared",
		CodeVersion: "v1",
	}
	body := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return workflow.ExecuteActivity(
			ctx,
			&workflow.ActivityOptions{ActivityId: "fetch"},
			input,
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return wrapperspb.String("ok:" + request.GetValue()), nil
			},
		)
	}

	if _, err := workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("input"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		body,
	); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := workflow.Run(
			ctx,
			store,
			options,
			nil,
			wrapperspb.String("input"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			body,
		); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRetryLoopInProcess measures the per-attempt overhead of an
// in-process retry loop with a 1ms backoff. Three attempts means three
// activity record writes (RETRYING, RETRYING, COMPLETED).
func BenchmarkRetryLoopInProcess(b *testing.B) {
	store := newBenchStore(b)
	ctx := context.Background()
	policy := &temporalessv1.RetryPolicy{
		MaximumAttempts: 3,
		InitialInterval: durationpb.New(time.Millisecond),
	}

	for i := 0; i < b.N; i++ {
		calls := 0
		_, err := workflow.Run(
			ctx,
			store,
			&workflow.Options{
				WorkflowId:  "bench:retry",
				RunId:       fmt.Sprintf("run-%05d", i),
				CodeVersion: "v1",
			},
			nil,
			wrapperspb.String("input"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return workflow.ExecuteActivity(
					ctx,
					&workflow.ActivityOptions{ActivityId: "fetch", RetryPolicy: policy},
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
			b.Fatal(err)
		}
	}
}

func newBenchStore(b *testing.B) *storage.OpenDALStore {
	b.Helper()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(operator.Close)
	return storage.NewOpenDALStore(operator)
}
