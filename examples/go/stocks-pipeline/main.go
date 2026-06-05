package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/cronscheduler"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Demonstrates the full schedule-driven shape:
//   - cronscheduler fires every minute during the simulated market window
//   - each fire dispatches a workflow with run_id = fire time
//   - the workflow runs a 3-activity pipeline (fetch / normalize / persist)
//   - re-invoking the same fire time replays without re-executing activities
//
// Run with `go run ./examples/go/stocks-pipeline/`.
func main() {
	root, err := os.MkdirTemp("", "temporaless-stocks-")
	if err != nil {
		panic(err)
	}
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		panic(err)
	}
	defer operator.Close()
	store := storage.NewOpenDALStore(operator)

	ctx := context.Background()
	scheduler, err := cronscheduler.New(
		[]cronscheduler.Schedule{{ID: "prices:aapl", Expression: "* * * * *"}},
		dispatchPrices(ctx, store),
	)
	if err != nil {
		panic(err)
	}
	if err := scheduler.Seed("prices:aapl", time.Date(2026, 5, 4, 9, 30, 0, 0, time.UTC)); err != nil {
		panic(err)
	}

	fmt.Println("first scan: fires three minute ticks (9:31, 9:32, 9:33)")
	if _, err := scheduler.Tick(ctx, time.Date(2026, 5, 4, 9, 33, 5, 0, time.UTC)); err != nil {
		panic(err)
	}
	fmt.Printf("activity executions so far: %d (3 fires × 3 activities = 9)\n\n", activityCalls.Load())

	fmt.Println("re-running the same scan: catches up zero new fires (last_fire advanced to 9:33)")
	if _, err := scheduler.Tick(ctx, time.Date(2026, 5, 4, 9, 33, 5, 0, time.UTC)); err != nil {
		panic(err)
	}
	fmt.Printf("activity executions: %d (still 9)\n\n", activityCalls.Load())

	fmt.Println("explicit replay of the 9:31 run via workflow.Run: stored result returned, no new activity calls")
	calls := activityCalls.Load()
	fireTime := time.Date(2026, 5, 4, 9, 31, 0, 0, time.UTC)
	result, err := runPricesWorkflow(ctx, store, fireTime)
	if err != nil {
		panic(err)
	}
	fmt.Printf("  result: %q\n", result.GetValue())
	fmt.Printf("  new activity calls during replay: %d\n", activityCalls.Load()-calls)

	fmt.Printf("\nstorage root: %s\n", root)
}

func dispatchPrices(ctx context.Context, store *storage.OpenDALStore) cronscheduler.Dispatcher {
	return func(_ context.Context, _ string, fireTime time.Time) error {
		_, err := runPricesWorkflow(ctx, store, fireTime)
		// Idempotent: if the workflow already completed for this fire_time, the
		// scheduler still considers the dispatch successful.
		if err != nil && !errors.Is(err, workflow.ErrWorkflowConflict) {
			return err
		}
		return nil
	}
}

func runPricesWorkflow(ctx context.Context, store *storage.OpenDALStore, fireTime time.Time) (*wrapperspb.StringValue, error) {
	options := &workflow.Options{
		WorkflowId:  "prices:aapl",
		RunId:       fireTime.UTC().Format("2006-01-02T15-04-05Z"),
		CodeVersion: "example",
	}
	return workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, symbol *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			workflow.Annotate(ctx, "fire_time", fireTime.UTC().Format(time.RFC3339))

			raw, err := workflow.ExecuteActivity(
				ctx,
				&workflow.ActivityOptions{ActivityId: "fetch:" + symbol.GetValue()},
				symbol,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				fakeFetch,
			)
			if err != nil {
				return nil, err
			}
			normalized, err := workflow.ExecuteActivity(
				ctx,
				&workflow.ActivityOptions{ActivityId: "normalize:" + symbol.GetValue()},
				raw,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				fakeNormalize,
			)
			if err != nil {
				return nil, err
			}
			return workflow.ExecuteActivity(
				ctx,
				&workflow.ActivityOptions{ActivityId: "persist:" + symbol.GetValue()},
				normalized,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				fakePersist,
			)
		},
	)
}

var activityCalls atomic.Int64

func fakeFetch(ctx context.Context, symbol *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	activityCalls.Add(1)
	workflow.Annotate(ctx, "vendor", "fake-stocks")
	return wrapperspb.String("raw:" + symbol.GetValue() + ":100.00"), nil
}

func fakeNormalize(ctx context.Context, raw *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	activityCalls.Add(1)
	workflow.Annotate(ctx, "rows", "1")
	return wrapperspb.String("normalized:" + raw.GetValue()), nil
}

func fakePersist(ctx context.Context, normalized *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	activityCalls.Add(1)
	workflow.Annotate(ctx, "table", "prices")
	workflow.Annotate(ctx, "rows_written", strconv.Itoa(1))
	return wrapperspb.String("persisted:" + normalized.GetValue()), nil
}
