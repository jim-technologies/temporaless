package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
)

// ActivityCall is one independent protobuf activity branch passed to
// AllActivities. The closure keeps each activity's caller-owned options and
// input explicit:
//
//	func(ctx context.Context) (*FetchResponse, error) {
//	    return workflow.Activity(
//	        ctx,
//	        fetch,
//	        request,
//	        workflow.WithActivityID("fetch:aapl"),
//	    )
//	}
type ActivityCall[Resp proto.Message] func(context.Context) (Resp, error)

// AllActivities runs independent activity branches concurrently, waits for
// every branch to settle, and returns results in call order.
//
// It deliberately does not stop at the first error. Every started activity
// reaches its own durable boundary before this function returns, so the parent
// workflow cannot commit a terminal record while a slower sibling is still
// mutating activity state.
//
// If any branch returns a workflow-continuation error (timer/event/claim/
// infrastructure pending, or caller cancellation), that error wins for this
// invocation and keeps the parent workflow IN_PROGRESS. Terminal activity
// failures are already durable and replay on the next invocation. Otherwise a
// single error is returned unchanged and multiple errors are joined.
func AllActivities[Resp proto.Message](
	ctx context.Context,
	calls ...ActivityCall[Resp],
) ([]Resp, error) {
	for index, call := range calls {
		if call == nil {
			return nil, fmt.Errorf("activity call %d is required", index)
		}
	}
	if len(calls) == 0 {
		return []Resp{}, nil
	}

	results := make([]Resp, len(calls))
	failures := make([]error, len(calls))
	var group sync.WaitGroup
	group.Add(len(calls))
	for index, call := range calls {
		go func() {
			defer group.Done()
			results[index], failures[index] = call(ctx)
		}()
	}
	group.Wait()

	for _, failure := range failures {
		if failure != nil && isWorkflowContinuationError(failure) {
			return nil, failure
		}
	}

	terminal := make([]error, 0, len(failures))
	for _, failure := range failures {
		if failure != nil {
			terminal = append(terminal, failure)
		}
	}
	switch len(terminal) {
	case 0:
		return results, nil
	case 1:
		return nil, terminal[0]
	default:
		return nil, errors.Join(terminal...)
	}
}
