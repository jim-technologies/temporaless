package connectstore

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

// errorPathCases exercise the genuinely conditional branches: optional claim
// store, missing-record reporting, idempotent deletes. Per-RPC nil-store
// guards were removed — Handler is constructed via NewHandler(store) which
// enforces non-nil; bypassing the constructor is a programming error and
// will surface as a nil-pointer panic.

func TestHandlerClaimRPCsRequireClaimStore(t *testing.T) {
	store := newTestStore(t)
	handler := NewHandler(store)
	ctx := context.Background()

	// nil claim store => GetClaim and TryCreateClaim should error.
	if _, err := handler.GetClaim(ctx, connect.NewRequest(&temporalessv1.GetClaimRequest{})); err == nil {
		t.Fatal("GetClaim should error without claim store")
	}
	if _, err := handler.TryCreateClaim(ctx, connect.NewRequest(&temporalessv1.TryCreateClaimRequest{})); err == nil {
		t.Fatal("TryCreateClaim should error without claim store")
	}
	listed, err := handler.ListClaims(ctx, connect.NewRequest(&temporalessv1.ListClaimsRequest{
		Key: storage.NewWorkflowKey("missing", "run").Proto(),
	}))
	if err != nil {
		t.Fatalf("ListClaims without claim store: %v", err)
	}
	if len(listed.Msg.GetRecords()) != 0 {
		t.Fatalf("ListClaims count = %d, want 0", len(listed.Msg.GetRecords()))
	}
}

func TestHandlerGetWorkflowReportsNotFound(t *testing.T) {
	store := newTestStore(t)
	handler := NewHandler(store)
	ctx := context.Background()

	resp, err := handler.GetWorkflow(ctx, connect.NewRequest(&temporalessv1.GetWorkflowRequest{
		Key: storage.WorkflowKey{
			Namespace:  storage.DefaultNamespace,
			WorkflowID: "missing",
			RunID:      "missing",
		}.Proto(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetFound() {
		t.Fatal("found = true for missing record")
	}
}

func TestHandlerDeleteRPCsAreIdempotent(t *testing.T) {
	store := newTestStore(t)
	handler := NewHandler(store)
	ctx := context.Background()

	resp, err := handler.DeleteWorkflow(ctx, connect.NewRequest(&temporalessv1.DeleteWorkflowRequest{
		Key: storage.WorkflowKey{
			Namespace:  storage.DefaultNamespace,
			WorkflowID: "missing",
			RunID:      "missing",
		}.Proto(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetDeleted() {
		t.Fatal("deleted = true for non-existent record")
	}
}
