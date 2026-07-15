package workflow

import (
	"context"
	"fmt"
	"reflect"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// DefaultRetryPolicy returns the policy applied by Activity() when the
// caller doesn't pass one explicitly.
//
// Shape (3 attempts, 1s initial, 2x backoff, 30s max, 30s durable threshold)
// is tuned for the framework's stated workloads: LLM completions (rate-limit
// windows of 30s–10min become durable timers automatically), vendor APIs
// returning transient 5xx, and quant-pipeline activities hitting short-lived
// market-data hiccups.
//
// Returns a fresh proto on each call so callers can mutate without sharing
// state with other Activity() invocations.
func DefaultRetryPolicy() *temporalessv1.RetryPolicy {
	return &temporalessv1.RetryPolicy{
		MaximumAttempts:         3,
		InitialInterval:         durationpb.New(1 * time.Second),
		BackoffCoefficient:      2.0,
		MaximumInterval:         durationpb.New(30 * time.Second),
		DurableBackoffThreshold: durationpb.New(30 * time.Second),
	}
}

type activityConfig struct {
	activityID   string
	retryTimerID string
	retryPolicy  *temporalessv1.RetryPolicy
}

// ActivityOption overrides one of Activity()'s defaults. Use sparingly —
// the default behavior is intentionally sufficient for most callsites.
type ActivityOption func(*activityConfig)

// WithActivityID supplies the stable application-owned activity_id.
func WithActivityID(id string) ActivityOption {
	return func(c *activityConfig) { c.activityID = id }
}

// WithRetryTimerID supplies the stable application-owned timer_id used when
// the retry policy crosses its durable backoff threshold.
func WithRetryTimerID(id string) ActivityOption {
	return func(c *activityConfig) { c.retryTimerID = id }
}

// WithRetryPolicy overrides DefaultRetryPolicy(). Pass `&temporalessv1.RetryPolicy{MaximumAttempts: 1}`
// for single-attempt semantics.
func WithRetryPolicy(policy *temporalessv1.RetryPolicy) ActivityOption {
	return func(c *activityConfig) { c.retryPolicy = policy }
}

// Activity is the ergonomic shortcut over ExecuteActivity. IDs remain
// explicit and application-owned; the framework never derives them from a
// function name or generates them.
//
//	resp, err := workflow.Activity(
//	    ctx,
//	    fetchPrices,
//	    &FetchRequest{Symbol: "AAPL"},
//	    workflow.WithActivityID("fetch:aapl"),
//	    workflow.WithRetryTimerID("retry:fetch:aapl"),
//	)
//
// Defaults applied unless overridden via opts:
//
//   - retry_policy ← DefaultRetryPolicy() (3 attempts, 1s, 2x, 30s max, 30s
//     durable threshold). Override via WithRetryPolicy.
func Activity[Req proto.Message, Resp proto.Message](
	ctx context.Context,
	fn func(context.Context, Req) (Resp, error),
	input Req,
	opts ...ActivityOption,
) (Resp, error) {
	var zero Resp
	cfg := activityConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.activityID == "" {
		return zero, fmt.Errorf("activity_id is required; pass WithActivityID")
	}
	if cfg.retryPolicy == nil {
		cfg.retryPolicy = DefaultRetryPolicy()
	}
	return ExecuteActivity(ctx,
		&temporalessv1.ActivityOptions{
			ActivityId:   cfg.activityID,
			RetryPolicy:  cfg.retryPolicy,
			RetryTimerId: cfg.retryTimerID,
		},
		input,
		func() Resp { return newProtoMessage[Resp]() },
		fn,
	)
}

// newProtoMessage constructs a fresh instance of a proto.Message type
// parameterized via generics. Proto messages are typically `*Foo` pointers
// to a generated struct; reflect.New on the element type returns the
// correctly-typed pointer.
func newProtoMessage[T proto.Message]() T {
	var zero T
	rt := reflect.TypeOf(zero)
	if rt.Kind() == reflect.Pointer {
		return reflect.New(rt.Elem()).Interface().(T)
	}
	return zero
}
