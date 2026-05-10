package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// llmAttempts tracks the global call count so the fake "LLM" can fail the
// first two attempts and succeed on the third — demonstrating retry policy.
var llmAttempts atomic.Int64

func main() {
	root, err := os.MkdirTemp("", "temporaless-llm-")
	if err != nil {
		panic(err)
	}
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		panic(err)
	}
	defer operator.Close()
	store := storage.NewOpenDALStore(operator)

	options := &workflow.Options{
		WorkflowId:  "llm:answer",
		RunId:       "2026-05-02-r1",
		CodeVersion: "example",
	}

	ctx := context.Background()
	prompt := "Why is the sky blue?"

	fmt.Println("first invocation: retries through transient failures, stores result")
	answer, err := workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String(prompt),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		askLLMWorkflow,
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("  result: %q\n", answer.GetValue())
	printAnnotations(ctx, store)

	fmt.Println("\nsecond invocation: replays stored workflow result, no LLM calls")
	llmAttempts.Store(0)
	answer, err = workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String(prompt),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		askLLMWorkflow,
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("  result: %q\n", answer.GetValue())
	fmt.Printf("  LLM calls during replay: %d (should be 0)\n", llmAttempts.Load())
}

func askLLMWorkflow(ctx context.Context, prompt *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	workflow.Annotate(ctx, "request_kind", "qa")
	return workflow.ExecuteActivity(
		ctx,
		&workflow.ActivityOptions{
			ActivityId: "llm:complete",
			RetryPolicy: &temporalessv1.RetryPolicy{
				MaximumAttempts:        3,
				InitialInterval:        durationpb.New(10 * time.Millisecond),
				BackoffCoefficient:     2.0,
				NonRetryableErrorCodes: []string{"invalid_argument"},
			},
		},
		prompt,
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		fakeLLMComplete,
	)
}

func fakeLLMComplete(ctx context.Context, prompt *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	attempt := llmAttempts.Add(1)
	workflow.Annotate(ctx, "model", "claude-opus-4-7")
	workflow.Annotate(ctx, "attempt", strconv.FormatInt(attempt, 10))
	if attempt < 3 {
		return nil, workflow.NewActivityError("rate_limited", "vendor 429", nil)
	}
	completion := "[fake completion for: " + prompt.GetValue() + "]"
	workflow.Annotate(ctx, "completion_tokens", strconv.Itoa(len(completion)))
	return wrapperspb.String(completion), nil
}

func printAnnotations(ctx context.Context, store *storage.OpenDALStore) {
	wf, _, err := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "llm:answer",
		RunID:      "2026-05-02-r1",
	})
	if err != nil {
		fmt.Printf("  workflow record error: %v\n", err)
		return
	}
	fmt.Printf("  workflow annotations: %v\n", wf.GetAnnotations())

	act, _, err := store.GetActivity(ctx, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "llm:answer",
		RunID:      "2026-05-02-r1",
		ActivityID: "llm:complete",
	})
	if err != nil {
		fmt.Printf("  activity record error: %v\n", err)
		return
	}
	fmt.Printf("  activity annotations: %v\n", act.GetAnnotations())
	fmt.Printf("  activity attempts: %d (should be 3)\n", len(act.GetAttempts()))
}
