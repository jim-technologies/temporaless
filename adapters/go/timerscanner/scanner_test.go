package timerscanner_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/timerscanner"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestDueTimersFindsScheduledTimersInflight(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	options := &workflow.Options{
		WorkflowId:  "prices:scanner",
		RunId:       "2026-05-02",
		CodeVersion: "test-version",
	}

	_, runErr := workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			if err := workflow.Sleep(ctx, "wait:vendor-window", time.Hour); err != nil {
				return nil, err
			}
			return wrapperspb.String("done"), nil
		},
	)
	if !errors.Is(runErr, workflow.ErrTimerPending) {
		t.Fatalf("first run err = %v, want ErrTimerPending", runErr)
	}

	beforeFire := time.Now().Add(time.Minute)
	due, err := timerscanner.DueTimers(ctx, store, beforeFire)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(due); got != 0 {
		t.Fatalf("due timers before fire_at = %d, want 0", got)
	}

	afterFire := time.Now().Add(2 * time.Hour)
	due, err = timerscanner.DueTimers(ctx, store, afterFire)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(due); got != 1 {
		t.Fatalf("due timers after fire_at = %d, want 1", got)
	}
	if due[0].Key.TimerID != "wait:vendor-window" {
		t.Fatalf("timer id = %q", due[0].Key.TimerID)
	}
	if due[0].Workflow == nil || due[0].Workflow.GetKey().GetWorkflowId() != "prices:scanner" {
		t.Fatalf("workflow record = %+v", due[0].Workflow)
	}
}

func TestDueTimersSkipsFiredTimers(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	options := &workflow.Options{
		WorkflowId:  "prices:scanner-fired",
		RunId:       "2026-05-02",
		CodeVersion: "test-version",
	}
	_, _ = workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			if err := workflow.Sleep(ctx, "wait:zero", 0); err != nil {
				return nil, err
			}
			return wrapperspb.String("done"), nil
		},
	)

	due, err := timerscanner.DueTimers(ctx, store, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(due); got != 0 {
		t.Fatalf("due timers = %d, want 0 (timer already fired)", got)
	}
}
