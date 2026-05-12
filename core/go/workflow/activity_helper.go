package workflow

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"runtime"
	"strings"
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
	activityID  string
	retryPolicy *temporalessv1.RetryPolicy
}

// ActivityOption overrides one of Activity()'s defaults. Use sparingly —
// the default behavior is intentionally sufficient for most callsites.
type ActivityOption func(*activityConfig)

// WithActivityID overrides the auto-inferred activity_id. Use when two
// callsites share the same function but should produce distinct activity
// records (e.g. `fetch:aapl` vs `fetch:msft` over the same handler).
func WithActivityID(id string) ActivityOption {
	return func(c *activityConfig) { c.activityID = id }
}

// WithRetryPolicy overrides DefaultRetryPolicy(). Pass `&temporalessv1.RetryPolicy{MaximumAttempts: 1}`
// for single-attempt semantics.
func WithRetryPolicy(policy *temporalessv1.RetryPolicy) ActivityOption {
	return func(c *activityConfig) { c.retryPolicy = policy }
}

// Activity is the ergonomic shortcut over ExecuteActivity. Defaults reduce
// the per-call boilerplate to roughly what a plain function call already
// requires: pass the function and its argument.
//
//	resp, err := workflow.Activity(ctx, fetchPrices, &FetchRequest{Symbol: "AAPL"})
//
// Defaults applied unless overridden via opts:
//
//   - activity_id ← qualified function name (e.g. "examples/go/fetch-prices.FetchPrices").
//     Override via WithActivityID when two callsites share the same function
//     but should produce distinct activity records.
//   - retry_policy ← DefaultRetryPolicy() (3 attempts, 1s, 2x, 30s max, 30s
//     durable threshold). Override via WithRetryPolicy.
//
// Caveat: auto-inferred activity_id is stable across builds for package-level
// functions and methods. Closures (function literals) may produce names like
// "pkg.func1" that aren't stable across edits — pass WithActivityID() for
// long-lived closure-based activities.
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
		id, err := inferActivityID(fn)
		if err != nil {
			return zero, err
		}
		cfg.activityID = id
	}
	if cfg.retryPolicy == nil {
		cfg.retryPolicy = DefaultRetryPolicy()
	}
	return ExecuteActivity(ctx,
		&temporalessv1.ActivityOptions{
			ActivityId:  cfg.activityID,
			RetryPolicy: cfg.retryPolicy,
		},
		input,
		func() Resp { return newProtoMessage[Resp]() },
		fn,
	)
}

var activityIDRegex = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

// inferActivityID extracts a path-safe identifier from a function reference.
// Strategy:
//
//  1. Get the fully-qualified Go function name via runtime.FuncForPC.
//  2. Drop everything up to and including the last `/` (the import path).
//  3. Strip method-receiver markers (`(*` and `)`) — Go reports methods as
//     `pkg.(*Type).Method`; the parens aren't in the framework's ID regex.
//  4. Validate against the framework's ID regex; reject closures whose
//     generated names contain characters we can't represent in storage paths.
func inferActivityID(fn any) (string, error) {
	rv := reflect.ValueOf(fn)
	if rv.Kind() != reflect.Func {
		return "", fmt.Errorf("inferActivityID: argument is not a function")
	}
	pc := rv.Pointer()
	rf := runtime.FuncForPC(pc)
	if rf == nil {
		return "", fmt.Errorf("inferActivityID: no runtime function for PC")
	}
	name := rf.Name()
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.NewReplacer("(*", "", ")", "").Replace(name)
	if !activityIDRegex.MatchString(name) {
		return "", fmt.Errorf(
			"cannot infer activity_id from function name %q (contains characters "+
				"disallowed in framework IDs); use WithActivityID() to set one explicitly",
			name,
		)
	}
	return name, nil
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
