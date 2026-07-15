// Package inspector provides operator-visibility helpers over the explicit
// query and point-store interfaces.
//
// Cross-run listing requires a QueryStore. Reset helpers and run-scoped lists
// use the authoritative point Store.
package inspector

import (
	"context"
	"fmt"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

// ListInFlightWorkflows returns every workflow record whose status is
// WORKFLOW_STATUS_IN_PROGRESS.
func ListInFlightWorkflows(ctx context.Context, query storage.WorkflowQueryStore) ([]*temporalessv1.WorkflowRecord, error) {
	return listWorkflowsByStatus(ctx, query, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS)
}

// ListFailedWorkflows returns every workflow record whose status is
// WORKFLOW_STATUS_FAILED.
func ListFailedWorkflows(ctx context.Context, query storage.WorkflowQueryStore) ([]*temporalessv1.WorkflowRecord, error) {
	return listWorkflowsByStatus(ctx, query, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED)
}

// ListWorkflowsByStatus is the generic form, exposed for callers that want a
// status the helpers above don't cover (e.g. COMPLETED for audits).
func ListWorkflowsByStatus(
	ctx context.Context,
	query storage.WorkflowQueryStore,
	status temporalessv1.WorkflowStatus,
) ([]*temporalessv1.WorkflowRecord, error) {
	return listWorkflowsByStatus(ctx, query, status)
}

func listWorkflowsByStatus(ctx context.Context, query storage.WorkflowQueryStore, status temporalessv1.WorkflowStatus) ([]*temporalessv1.WorkflowRecord, error) {
	if query == nil {
		return nil, fmt.Errorf("query store is required")
	}
	var records []*temporalessv1.WorkflowRecord
	pageToken := ""
	seenPageTokens := map[string]struct{}{}
	for {
		response, err := query.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{
			Status:    status,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		records = append(records, response.GetRecords()...)
		next := response.GetNextPageToken()
		if next == "" {
			return records, nil
		}
		if _, repeated := seenPageTokens[next]; repeated {
			return nil, fmt.Errorf("query store repeated page token %q", next)
		}
		seenPageTokens[next] = struct{}{}
		pageToken = next
	}
}

// ListActivities returns every activity record under the given workflow run.
func ListActivities(
	ctx context.Context,
	store storage.Store,
	key storage.WorkflowKey,
) ([]*temporalessv1.ActivityRecord, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	return store.ListActivities(ctx, key)
}

// ResetWorkflow deletes the workflow record so the next invocation re-executes
// from scratch. Activity, timer, event, and claim records under the same run
// are left untouched — call ResetActivity or use a new run_id if a full reset
// is intended.
func ResetWorkflow(ctx context.Context, store storage.Store, key storage.WorkflowKey) error {
	if store == nil {
		return fmt.Errorf("store is required")
	}
	_, err := store.DeleteWorkflow(ctx, key)
	return err
}

// ResetActivity deletes a stored activity record so the next ExecuteActivity
// call re-executes the activity body.
func ResetActivity(ctx context.Context, store storage.Store, key storage.ActivityKey) error {
	if store == nil {
		return fmt.Errorf("store is required")
	}
	_, err := store.DeleteActivity(ctx, key)
	return err
}

// ResetEvent deletes a stored event record so the workflow's WaitEvent call
// returns ErrEventPending again on the next invocation.
func ResetEvent(ctx context.Context, store storage.Store, key storage.EventKey) error {
	if store == nil {
		return fmt.Errorf("store is required")
	}
	_, err := store.DeleteEvent(ctx, key)
	return err
}
