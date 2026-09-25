package cronscheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/cronscheduler"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestLastFireFromRunsDerivesStateFromStorage(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	// Three runs of the schedule, with run_ids = ISO timestamps.
	fireTimes := []time.Time{
		time.Date(2026, 5, 4, 9, 30, 0, 0, time.UTC),
		time.Date(2026, 5, 4, 9, 31, 0, 0, time.UTC),
		time.Date(2026, 5, 4, 9, 32, 0, 0, time.UTC),
	}
	for _, fireTime := range fireTimes {
		if _, err := workflow.Run(
			ctx,
			store,
			&workflow.Options{
				WorkflowId:   "prices:aapl",
				RunId:        fireTime.UTC().Format(time.RFC3339),
				RunOrderTime: timestamppb.New(fireTime),
			},
			nil,
			wrapperspb.String("AAPL"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return wrapperspb.String("ok"), nil
			},
		); err != nil {
			t.Fatal(err)
		}
	}

	last, ok, err := cronscheduler.LastFireFromRuns(ctx, store, "", "prices:aapl")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected last fire to be derived from existing runs")
	}
	want := time.Date(2026, 5, 4, 9, 32, 0, 0, time.UTC)
	if !last.Equal(want) {
		t.Fatalf("last fire = %s, want %s", last, want)
	}
}

func TestLastFireFromRunsReturnsFalseWhenNoRuns(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	_, ok, err := cronscheduler.LastFireFromRuns(ctx, store, "", "prices:aapl")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false when no runs exist")
	}
}

func TestOneShotSchedulerRestoresBootstrapThenStoredProgress(t *testing.T) {
	tests := []struct {
		name        string
		existingRun bool
	}{
		{name: "first boot"},
		{name: "stored progress overrides bootstrap", existingRun: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			anchor := time.Date(2026, 5, 4, 9, 30, 0, 0, time.UTC)
			firstFire := anchor.Add(time.Minute)
			secondFire := anchor.Add(2 * time.Minute)
			var fired []time.Time
			bootAndTick := func(now time.Time) int {
				// Every invocation creates its own store and scheduler over fs.
				operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
				if err != nil {
					t.Fatal(err)
				}
				defer operator.Close()
				store := storage.NewOpenDALStore(operator)
				scheduler, err := cronscheduler.New(
					[]cronscheduler.Schedule{{ID: "prices:aapl", Expression: "* * * * *"}},
					func(ctx context.Context, scheduleID string, fireTime time.Time) error {
						fired = append(fired, fireTime)
						_, err := workflow.Run(ctx, store, &workflow.Options{
							WorkflowId:   scheduleID,
							RunId:        fireTime.UTC().Format(time.RFC3339),
							RunOrderTime: timestamppb.New(fireTime),
						}, nil, wrapperspb.String("AAPL"),
							func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
							func(_ context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
								return input, nil
							},
						)
						return err
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				scheduler.Restore(map[string]time.Time{"prices:aapl": anchor})
				snapshot, err := cronscheduler.LastFiresFromRuns(ctx, store, "", []string{"prices:aapl"})
				if err != nil {
					t.Fatal(err)
				}
				scheduler.Restore(snapshot)
				count, err := scheduler.Tick(ctx, now)
				if err != nil {
					t.Fatal(err)
				}
				return count
			}
			if test.existingRun {
				if got := bootAndTick(firstFire); got != 1 {
					t.Fatalf("seed tick = %d, want 1", got)
				}
				fired = nil
			}
			want := []time.Time{firstFire, secondFire}
			if test.existingRun {
				want = []time.Time{secondFire}
			}
			if got := bootAndTick(secondFire); got != len(want) {
				t.Fatalf("first tick after boot = %d, want %d", got, len(want))
			}
			for i, fireTime := range fired {
				if !fireTime.Equal(want[i]) {
					t.Fatalf("fire[%d] = %s, want %s", i, fireTime, want[i])
				}
			}
			if got := bootAndTick(secondFire); got != 0 {
				t.Fatalf("repeat boot tick = %d, want 0", got)
			}
		})
	}
}

func TestLastFiresFromRunsBuildsRestorableSnapshot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	for _, schedule := range []struct {
		workflowID string
		runID      string
		fireTime   time.Time
	}{
		{"prices:aapl", "run:aapl:32", time.Date(2026, 5, 4, 9, 32, 0, 0, time.UTC)},
		{"prices:msft", "run:msft:33", time.Date(2026, 5, 4, 9, 33, 0, 0, time.UTC)},
	} {
		if _, err := workflow.Run(
			ctx,
			store,
			&workflow.Options{
				WorkflowId:   schedule.workflowID,
				RunId:        schedule.runID,
				RunOrderTime: timestamppb.New(schedule.fireTime),
			},
			nil,
			wrapperspb.String("input"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return wrapperspb.String("ok"), nil
			},
		); err != nil {
			t.Fatal(err)
		}
	}

	snapshot, err := cronscheduler.LastFiresFromRuns(
		ctx,
		store,
		"",
		[]string{"prices:aapl", "prices:msft", "prices:never-ran"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(snapshot); got != 2 {
		t.Fatalf("snapshot entries = %d, want 2 (the run-less schedule should be omitted)", got)
	}
	if got := snapshot["prices:aapl"]; !got.Equal(time.Date(2026, 5, 4, 9, 32, 0, 0, time.UTC)) {
		t.Fatalf("aapl last fire = %s", got)
	}
	if got := snapshot["prices:msft"]; !got.Equal(time.Date(2026, 5, 4, 9, 33, 0, 0, time.UTC)) {
		t.Fatalf("msft last fire = %s", got)
	}
}
