package connectstore_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/connectstore"
	"github.com/jim-technologies/temporaless/adapters/go/gocdkclaims"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"gocloud.dev/blob/fileblob"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRemoteWorkflowSingleFlightBusyReleaseAndReplay(t *testing.T) {
	recordOperator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{
		"root": t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(recordOperator.Close)
	recordStore := storage.NewOpenDALStore(recordOperator)

	claimBucket, err := fileblob.OpenBucket(t.TempDir(), &fileblob.Options{
		Metadata: fileblob.MetadataDontWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = claimBucket.Close() })
	claimStore := gocdkclaims.NewStore(claimBucket)

	_, handler := connectstore.NewHTTPHandlerWithClaims(recordStore, claimStore)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	remote := connectstore.NewHTTPClientStore(server.Client(), server.URL)

	options := &workflow.Options{
		WorkflowId:   "remote:singleflight",
		RunId:        "run:1",
		ClaimOwnerId: "worker:shared",
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	firstResult := make(chan *wrapperspb.StringValue, 1)
	firstErr := make(chan error, 1)
	var calls atomic.Int64

	execute := func(_ context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		calls.Add(1)
		close(entered)
		<-release
		return wrapperspb.String("ok:" + input.GetValue()), nil
	}
	go func() {
		result, runErr := workflow.Run(
			context.Background(), remote, options, remote,
			wrapperspb.String("AAPL"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			execute,
		)
		firstResult <- result
		firstErr <- runErr
	}()
	<-entered

	_, err = workflow.Run(
		context.Background(), remote, options, remote,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			t.Fatal("duplicate workflow body executed")
			return nil, nil
		},
	)
	if !errors.Is(err, workflow.ErrClaimBusy) {
		t.Fatalf("duplicate error = %v, want ErrClaimBusy", err)
	}

	close(release)
	if err := <-firstErr; err != nil {
		t.Fatalf("first invocation: %v", err)
	}
	if got := (<-firstResult).GetValue(); got != "ok:AAPL" {
		t.Fatalf("first result = %q, want %q", got, "ok:AAPL")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("workflow calls = %d, want 1", got)
	}

	claimKey := storage.NewClaimKey(
		options.GetWorkflowId(), options.GetRunId(), workflow.WorkflowExecutionClaimID,
	)
	if _, found, err := remote.GetClaim(context.Background(), claimKey); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("workflow execution claim remained after orderly completion")
	}

	replayed, err := workflow.Run(
		context.Background(), remote, options, remote,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			t.Fatal("terminal replay executed workflow body")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf("terminal replay: %v", err)
	}
	if got := replayed.GetValue(); got != "ok:AAPL" {
		t.Fatalf("replay result = %q, want %q", got, "ok:AAPL")
	}
}

func TestRemoteWorkflowRejectsMissingClaimBackendBeforeWriting(t *testing.T) {
	recordOperator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{
		"root": t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(recordOperator.Close)
	recordStore := storage.NewOpenDALStore(recordOperator)

	_, handler := connectstore.NewHTTPHandler(recordStore)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	remote := connectstore.NewHTTPClientStore(server.Client(), server.URL)
	options := &workflow.Options{
		WorkflowId:   "remote:no-claims",
		RunId:        "run:1",
		ClaimOwnerId: "worker",
	}
	bodyCalled := false

	_, err = workflow.Run(
		context.Background(), remote, options, remote,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			bodyCalled = true
			return wrapperspb.String("unexpected"), nil
		},
	)
	if !errors.Is(err, workflow.ErrClaimCapabilityUnsupported) {
		t.Fatalf("error = %v, want ErrClaimCapabilityUnsupported", err)
	}
	if bodyCalled {
		t.Fatal("workflow body ran without remote claim capability")
	}
	if _, found, getErr := remote.GetWorkflow(
		context.Background(),
		storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
	); getErr != nil {
		t.Fatal(getErr)
	} else if found {
		t.Fatal("unsupported remote claim option wrote a workflow record")
	}
}
