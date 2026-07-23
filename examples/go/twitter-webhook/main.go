package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Demonstrates the storage-first webhook/event flow:
//   - workflow processes a tweet: classifies it, then waits for a moderation
//     decision delivered out-of-band
//   - first invocation returns ErrEventPending and leaves the workflow IN_PROGRESS
//   - this local, single-process demo simulates delivery with the low-level
//     PutEvent operator primitive
//   - second invocation replays the workflow body, finds the event, completes
//
// Production Go delivery must call storage.SendEvent through a ConnectStore
// whose RecordStoreService advertises atomic create-only delivery. Direct Go
// OpenDAL intentionally reports delivery unsupported because its binding does
// not expose a conditional write.
//
// Run with `go run ./examples/go/twitter-webhook/`.
func main() {
	root, err := os.MkdirTemp("", "temporaless-twitter-")
	if err != nil {
		panic(err)
	}
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		panic(err)
	}
	defer operator.Close()
	store := storage.NewOpenDALStore(operator)

	ctx := context.Background()
	options := &workflow.Options{
		WorkflowId: "twitter:moderate",
		RunId:      "tweet-12345",
	}
	tweet := wrapperspb.String("Markets up 2% today! /s")

	fmt.Println("first invocation: classifies, then waits for moderation event")
	_, err = workflow.Run(ctx, store, options, nil, tweet, newReply, moderateWorkflow)
	if !errors.Is(err, workflow.ErrEventPending) {
		panic(fmt.Sprintf("expected ErrEventPending, got %v", err))
	}
	fmt.Printf("  classify activity calls: %d (should be 1, classified result is now stored)\n", classifyCalls.Load())

	wf, _, _ := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "twitter:moderate",
		RunID:      "tweet-12345",
	})
	fmt.Printf("  workflow status mid-flight: %s\n\n", wf.GetStatus())

	fmt.Println("external moderator approves (local demo fixture via PutEvent)")
	approval := wrapperspb.String("approved:moderator-jane")
	eventKey := storage.EventKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "twitter:moderate",
		RunID:      "tweet-12345",
		EventID:    "moderation-decision",
	}
	packed := &anypb.Any{}
	if err := anypb.MarshalFrom(
		packed,
		approval,
		proto.MarshalOptions{Deterministic: true},
	); err != nil {
		panic(err)
	}
	if err := store.PutEvent(ctx, &temporalessv1.EventRecord{
		SchemaVersion: storage.EventRecordSchemaVersion,
		Key:           eventKey.Proto(),
		Payload:       packed,
		ReceivedAt:    timestamppb.Now(),
	}); err != nil {
		panic(err)
	}

	fmt.Println("\nsecond invocation: replay classify (cached), pick up event, post reply")
	result, err := workflow.Run(ctx, store, options, nil, tweet, newReply, moderateWorkflow)
	if err != nil {
		panic(err)
	}
	fmt.Printf("  classify activity calls (should still be 1, replayed): %d\n", classifyCalls.Load())
	fmt.Printf("  post-reply activity calls: %d\n", postReplyCalls.Load())
	fmt.Printf("  result: %q\n", result.GetValue())
}

func newReply() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }

func moderateWorkflow(ctx context.Context, tweet *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	workflow.Annotate(ctx, "tweet_id", "12345")

	classification, err := workflow.ExecuteActivity(
		ctx,
		&workflow.ActivityOptions{ActivityId: "classify"},
		tweet,
		newReply,
		fakeClassify,
	)
	if err != nil {
		return nil, err
	}

	decision, err := workflow.WaitEvent(ctx, "moderation-decision", newReply, nil)
	if err != nil {
		return nil, err
	}
	workflow.Annotate(ctx, "decision", decision.GetValue())

	if !startsWith(decision.GetValue(), "approved:") {
		return wrapperspb.String("rejected"), nil
	}

	return workflow.ExecuteActivity(
		ctx,
		&workflow.ActivityOptions{ActivityId: "post-reply"},
		classification,
		newReply,
		fakePostReply,
	)
}

var (
	classifyCalls  atomic.Int64
	postReplyCalls atomic.Int64
)

func fakeClassify(ctx context.Context, tweet *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	classifyCalls.Add(1)
	workflow.Annotate(ctx, "model", "claude-haiku-4-5")
	return wrapperspb.String("class:sarcasm"), nil
}

func fakePostReply(ctx context.Context, classification *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	postReplyCalls.Add(1)
	workflow.Annotate(ctx, "channel", "twitter")
	return wrapperspb.String("posted:" + classification.GetValue()), nil
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
