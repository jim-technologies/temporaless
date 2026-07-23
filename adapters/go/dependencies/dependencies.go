// Package dependencies provides the cross-workflow dependency primitive.
//
// When pipeline B depends on pipeline A's run for the same date / partition,
// WaitForWorkflow reads A's record and either:
//
//   - returns A's typed result if A is COMPLETED;
//   - returns *workflow.WorkflowDependencyPendingError if A is still
//     IN_PROGRESS or hasn't started — B stays IN_PROGRESS; the caller must
//     re-invoke it unless pollOptions arms a scanner-visible timer;
//   - returns *workflow.WorkflowDependencyFailedError if A is in a
//     terminal-failed state — B fails too, since the upstream is
//     unrecoverable without operator action.
//
// Replay-friendly: each call reads the upstream point record once. Manual
// waits write nothing; pollOptions may write or rearm one caller-identified
// poll timer in B's run. Both modes are idempotent on workflow re-execution.
//
// Usage from inside a workflow body:
//
//	current, _ := workflow.Current(ctx) // guaranteed inside the workflow body
//	upstream, err := dependencies.WaitForWorkflow(
//	    ctx,
//	    current.Store(),
//	    storage.NewWorkflowKey("prices:AAPL", "2026-05-04"),
//	    func() *pricesv1.FetchResponse { return &pricesv1.FetchResponse{} },
//	    nil, // or caller-supplied PollOptions for automatic durable polling
//	)
//	if err != nil { return nil, err }
//	// … compute signal from upstream.GetPrice() …
package dependencies

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/proto"
)

// WaitForWorkflow reads an upstream workflow's record and returns its result.
//
// store: the same Store the upstream workflow ran against.
// key: identifies the upstream workflow run.
// newResult: returns a fresh instance of the upstream's result message type;
// used by Any.UnmarshalTo to decode the stored result.
// pollOptions: optional caller-identified durable polling timer; nil leaves
// re-invocation to the application.
func WaitForWorkflow[Resp proto.Message](
	ctx context.Context,
	store storage.Store,
	key storage.WorkflowKey,
	newResult func() Resp,
	pollOptions *workflow.PollOptions,
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
	if err := key.Validate(); err != nil {
		return zero, err
	}
	if err := workflow.ValidatePollOptions(pollOptions); err != nil {
		return zero, err
	}
	result := newResult()
	if isNilProtoMessage(result) {
		return zero, errors.New("newResult returned nil")
	}
	record, found, err := store.GetWorkflow(ctx, key)
	if err != nil {
		if errors.Is(err, storage.ErrCorruptRecord) {
			return zero, err
		}
		var infrastructureErr *workflow.WorkflowInfrastructureError
		if errors.As(err, &infrastructureErr) {
			return zero, err
		}
		return zero, &workflow.WorkflowInfrastructureError{
			Operation: "read workflow dependency",
			Cause:     err,
		}
	}
	if !found || record.GetStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		var wakeAt time.Time
		if pollOptions != nil {
			wakeAt, err = workflow.ArmPoll(ctx, pollOptions)
			if err != nil {
				return zero, err
			}
		}
		return zero, &workflow.WorkflowDependencyPendingError{
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			WakeAt:     wakeAt,
		}
	}
	if pollOptions != nil {
		if err := workflow.ResolvePoll(ctx, pollOptions); err != nil {
			return zero, err
		}
	}
	if record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		return zero, &workflow.WorkflowDependencyFailedError{
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			Status:     int32(record.GetStatus()),
		}
	}
	if record.GetResult() == nil {
		return zero, fmt.Errorf(
			"%w: workflow %q/%q completed without a stored result",
			storage.ErrCorruptRecord,
			key.WorkflowID,
			key.RunID,
		)
	}
	if !record.GetResult().MessageIs(result) {
		return zero, fmt.Errorf(
			"%w: workflow %q/%q stored result type does not match requested type",
			workflow.ErrWorkflowConflict,
			key.WorkflowID,
			key.RunID,
		)
	}
	if err := record.GetResult().UnmarshalTo(result); err != nil {
		return zero, fmt.Errorf(
			"%w: decode workflow %q/%q stored result: %w",
			storage.ErrCorruptRecord,
			key.WorkflowID,
			key.RunID,
			err,
		)
	}
	return result, nil
}

func isNilProtoMessage(message proto.Message) bool {
	if message == nil {
		return true
	}
	value := reflect.ValueOf(message)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
