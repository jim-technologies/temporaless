// LLM completion example: every reliability primitive working together.
//
// A realistic vendor-LLM activity uses the full stack:
//
//   - workflow.Activity[...] — one-line activity dispatch with auto-inferred
//     activity_id (from the function name) and a sensible default retry
//     policy (3 attempts, 1s initial, 2x backoff, 30s max, 30s durable
//     threshold).
//   - outbox.IdempotencyKey — stable per-activity dedup key for the vendor's
//     HTTP `Idempotency-Key` header. Retries don't double-charge.
//   - workflow.NewRetryableActivityError(...) — surfaces the vendor's
//     `Retry-After` header so the runtime waits at least that long before
//     the next attempt; if it crosses the durable threshold it becomes a
//     durable timer (no compute burned during long rate-limit windows).
//   - WorkflowOptions.ConcurrencyKey + ConcurrencyLimit — pre-emptive
//     cluster-wide cap on in-flight vendor calls. At most N workflows
//     share the vendor quota at any moment, regardless of how many worker
//     replicas dispatch.
//   - workflow.Annotate — durable per-activity metadata (model, tokens,
//     vendor) that survives replay and is queryable via Hive partitioning
//   - DuckDB.
//
// Run with `go run ./examples/go/llm-completion`.
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
	"github.com/jim-technologies/temporaless/adapters/go/gocdkclaims"
	"github.com/jim-technologies/temporaless/adapters/go/outbox"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"gocloud.dev/blob/fileblob"
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

	// Concurrency-key slots use claim records; the gocdkclaims adapter
	// provides storage-arbitrated create-if-absent via gocloud.dev/blob.
	//
	// MetadataDontWrite avoids fileblob's `.attrs` JSON sidecar — a separate
	// per-record file that gets truncated-then-written, with the truncation
	// NOT gated by IfNotExist. A racing GetClaim that lands during the
	// sidecar's brief empty window reads `io.EOF` out of the JSON decoder.
	// Production deployments use S3/GCS drivers with native preconditions
	// and don't go through this path.
	claimsBucket, err := fileblob.OpenBucket(root, &fileblob.Options{
		Metadata: fileblob.MetadataDontWrite,
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = claimsBucket.Close() }()
	claimStore := gocdkclaims.NewStore(claimsBucket)

	options := &workflow.Options{
		WorkflowId:  "llm:answer",
		RunId:       "2026-05-02-r1",
		CodeVersion: "example",
		// Pre-emptive cluster-wide cap: at most 5 LLM workflows in flight
		// sharing the "vendor:openai" quota. Pair with multiple worker
		// replicas and the framework arbitrates via storage claims.
		ConcurrencyKey:   "vendor:openai",
		ConcurrencyLimit: 5,
	}

	ctx := context.Background()
	prompt := "Why is the sky blue?"

	fmt.Println("first invocation: retries through transient failures, stores result")
	answer, err := workflow.Run(
		ctx,
		store,
		options,
		claimStore,
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
		claimStore,
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

// askLLMWorkflow — one line per activity, defaults from the framework.
// workflow.Activity infers the activity_id from fakeLLMComplete's function
// name and applies workflow.DefaultRetryPolicy(). To override, pass
// workflow.WithActivityID(...) or workflow.WithRetryPolicy(...).
func askLLMWorkflow(ctx context.Context, prompt *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	workflow.Annotate(ctx, "request_kind", "qa")
	return workflow.Activity(ctx, fakeLLMComplete, prompt)
}

// fakeLLMComplete — what a vendor LLM call looks like under the framework.
//
// The reliability story:
//   - Idempotency key from outbox.IdempotencyKey, attached as if we were
//     calling Stripe / OpenAI / Slack. Retries-after-mid-flight return the
//     original vendor response instead of double-charging.
//   - Durable annotations capture model + token usage; visible in analytics
//     queries without re-reading the activity record.
//   - On a 429, NewRetryableActivityError(..., retryAfter, ...) surfaces the
//     vendor's suggested wait. The runtime takes max(computed, retryAfter)
//     for the next interval; if it crosses the durable threshold, the wait
//     becomes a timer record instead of holding the process alive.
func fakeLLMComplete(ctx context.Context, prompt *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	wf, _ := workflow.Current(ctx)
	idempotencyKey := outbox.IdempotencyKey(wf, "fakeLLMComplete")
	workflow.Annotate(ctx, "vendor", "openai")
	workflow.Annotate(ctx, "model", "claude-opus-4-7")
	workflow.Annotate(ctx, "idempotency_key", idempotencyKey)

	attempt := llmAttempts.Add(1)
	workflow.Annotate(ctx, "attempt", strconv.FormatInt(attempt, 10))
	if attempt < 3 {
		return nil, workflow.NewRetryableActivityError(
			"rate_limited",
			"vendor 429",
			50*time.Millisecond,
			nil,
		)
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

	// Activity ID is the inferred function name (main.fakeLLMComplete after
	// sanitization). Use the helper to look it up the same way.
	activityID, err := workflow.InferActivityID(fakeLLMComplete)
	if err != nil {
		fmt.Printf("  infer activity id: %v\n", err)
		return
	}
	act, _, err := store.GetActivity(ctx, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "llm:answer",
		RunID:      "2026-05-02-r1",
		ActivityID: activityID,
	})
	if err != nil {
		fmt.Printf("  activity record error: %v\n", err)
		return
	}
	fmt.Printf("  activity annotations: %v\n", act.GetAnnotations())
	fmt.Printf("  activity attempts: %d (should be 3)\n", len(act.GetAttempts()))
	if len(act.GetAttempts()) > 0 {
		fmt.Printf(
			"  attempt 1 retry_after: %s\n",
			act.GetAttempts()[0].GetFailure().GetRetryAfter().AsDuration(),
		)
	}
}
