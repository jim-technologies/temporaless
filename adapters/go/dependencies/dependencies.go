// Package dependencies provides the cross-workflow dependency primitive.
//
// When pipeline B depends on pipeline A's run for the same date / partition,
// WaitForWorkflow reads A's record and either:
//
//   - returns A's typed result if A is COMPLETED;
//   - returns *workflow.WorkflowDependencyPendingError if A is still
//     IN_PROGRESS or hasn't started — B stays IN_PROGRESS until a scanner /
//     re-invoke retries it;
//   - returns *workflow.WorkflowDependencyFailedError if A is in a
//     terminal-failed state — B fails too, since the upstream is
//     unrecoverable without operator action.
//
// Replay-friendly: a single store.GetWorkflow call, no record writes from
// this side, idempotent on workflow re-execution.
//
// Usage from inside a workflow body:
//
//	upstream, err := dependencies.WaitForWorkflow(
//	    ctx,
//	    workflow.Current(ctx).Store(),
//	    storage.NewWorkflowKey("prices:AAPL", "2026-05-04"),
//	    func() *pricesv1.FetchResponse { return &pricesv1.FetchResponse{} },
//	)
//	if err != nil { return nil, err }
//	// … compute signal from upstream.GetPrice() …
package dependencies

import (
	"context"
	"errors"
	"fmt"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// WaitForWorkflow reads an upstream workflow's record and returns its result.
//
// store: the same Store the upstream workflow ran against.
// key: identifies the upstream workflow run.
// newResult: returns a fresh instance of the upstream's result message type;
// used by anypb.UnmarshalTo to decode the stored result.
func WaitForWorkflow[Resp proto.Message](
	ctx context.Context,
	store storage.Store,
	key storage.WorkflowKey,
	newResult func() Resp,
) (Resp, error) {
	var zero Resp
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if store == nil {
		return zero, errors.New("store is required")
	}
	if newResult == nil {
		return zero, errors.New("newResult is required")
	}
	record, found, err := store.GetWorkflow(ctx, key)
	if err != nil {
		return zero, err
	}
	if !found || record.GetStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		return zero, &workflow.WorkflowDependencyPendingError{
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
		}
	}
	if record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		return zero, &workflow.WorkflowDependencyFailedError{
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			Status:     int32(record.GetStatus()),
		}
	}
	result := newResult()
	if err := unpackAny(record.GetResult(), result); err != nil {
		return zero, fmt.Errorf(
			"workflow %q/%q stored result type does not match requested type: %w",
			key.WorkflowID,
			key.RunID,
			err,
		)
	}
	return result, nil
}

func unpackAny(any *anypb.Any, into proto.Message) error {
	if any == nil {
		return errors.New("upstream record has no result")
	}
	return any.UnmarshalTo(into)
}
