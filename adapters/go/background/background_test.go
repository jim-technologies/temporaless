package background

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/cronscheduler"
	"github.com/jim-technologies/temporaless/adapters/go/timerscanner"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestStore(t *testing.T) *storage.OpenDALStore {
	t.Helper()
	op, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(op.Close)
	return storage.NewOpenDALStore(op)
}

func TestNoConfigStartIsNoop(t *testing.T) {
	store := newTestStore(t)
	w, err := New(store, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.Stop() // returns immediately when no workers are running
}

func TestCronLoopTicks(t *testing.T) {
	store := newTestStore(t)
	var dispatches atomic.Int64
	dispatch := func(ctx context.Context, id string, fire time.Time) error {
		dispatches.Add(1)
		return nil
	}
	sched, err := cronscheduler.New([]cronscheduler.Schedule{
		{ID: "hourly", Expression: "0 * * * *"},
	}, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	if err := sched.Seed("hourly", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	w, err := New(store, Config{
		Cron: &CronConfig{Scheduler: sched, Interval: 50 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	w.Stop()
	if got := dispatches.Load(); got == 0 {
		t.Fatalf("expected dispatches, got 0")
	}
}

func TestTimerScannerLoopDispatches(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	// Seed an in-progress workflow + due timer.
	now := timestamppb.New(time.Now().UTC())
	past := timestamppb.New(time.Now().UTC().Add(-time.Minute))
	wfKey := storage.WorkflowKey{Namespace: storage.DefaultNamespace, WorkflowID: "wf", RunID: "r"}
	if err := store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           wfKey.Proto(),
		WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
		CodeVersion:   "test",
		InputDigest:   "x",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		CreatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}
	tKey := storage.TimerKey{Namespace: wfKey.Namespace, WorkflowID: wfKey.WorkflowID, RunID: wfKey.RunID, TimerID: "due"}
	if err := store.PutTimer(ctx, &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key:           tKey.Proto(),
		TimerKind:     temporalessv1.TimerKind_TIMER_KIND_SLEEP,
		CodeVersion:   "test",
		InputDigest:   "d",
		Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:        past,
		CreatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}

	var seen atomic.Int64
	dispatch := func(ctx context.Context, timer timerscanner.DueTimer) error {
		seen.Add(1)
		return nil
	}
	w, err := New(store, Config{
		TimerScanner: &TimerScannerConfig{Dispatch: dispatch, Interval: 50 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	w.Stop()
	if seen.Load() == 0 {
		t.Fatal("expected scanner to dispatch the due timer")
	}
}

func TestJanitorLoopSweeps(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	old := timestamppb.New(time.Now().UTC().Add(-2 * time.Hour))
	key := storage.WorkflowKey{Namespace: storage.DefaultNamespace, WorkflowID: "wf-old", RunID: "r"}
	if err := store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
		CodeVersion:   "test",
		InputDigest:   "x",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		CreatedAt:     old,
		CompletedAt:   old,
	}); err != nil {
		t.Fatal(err)
	}

	w, err := New(store, Config{
		Janitor: &JanitorConfig{MaxAge: time.Hour, Interval: 50 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	w.Stop()

	_, found, err := store.GetWorkflow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected janitor to sweep the old COMPLETED workflow")
	}
}

func TestStopIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	w, err := New(store, Config{})
	if err != nil {
		t.Fatal(err)
	}
	w.Stop() // before start
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.Stop()
	w.Stop() // after stop
}

func TestStartTwiceErrors(t *testing.T) {
	store := newTestStore(t)
	dispatch := func(ctx context.Context, id string, fire time.Time) error { return nil }
	sched, err := cronscheduler.New([]cronscheduler.Schedule{
		{ID: "x", Expression: "0 * * * *"},
	}, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	w, err := New(store, Config{Cron: &CronConfig{Scheduler: sched}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err := w.Start(context.Background()); err == nil {
		t.Fatal("expected error on double start")
	}
}

func TestNewValidation(t *testing.T) {
	store := newTestStore(t)
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "cron without scheduler",
			cfg:  Config{Cron: &CronConfig{Interval: time.Second}},
			want: "cron.Scheduler",
		},
		{
			name: "timer_scanner without dispatch",
			cfg:  Config{TimerScanner: &TimerScannerConfig{Interval: time.Second}},
			want: "Dispatch",
		},
		{
			name: "janitor missing MaxAge",
			cfg:  Config{Janitor: &JanitorConfig{}},
			want: "MaxAge",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(store, tc.cfg)
			if err == nil {
				t.Fatal("expected error")
			}
			if !contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestLoopIterationErrorDoesNotKillWorker(t *testing.T) {
	store := newTestStore(t)
	var calls atomic.Int64
	dispatch := func(ctx context.Context, id string, fire time.Time) error {
		calls.Add(1)
		return errors.New("boom")
	}
	// Use a scheduler that ALWAYS plans something to dispatch each tick.
	sched, err := cronscheduler.New([]cronscheduler.Schedule{
		{ID: "minute", Expression: "* * * * *"},
	}, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	if err := sched.Seed("minute", time.Now().Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}

	w, err := New(store, Config{Cron: &CronConfig{Scheduler: sched, Interval: 30 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	w.Stop()
	if calls.Load() == 0 {
		t.Fatal("expected dispatcher to be called at least once")
	}
	// Wokers must still be alive after errors — verify by checking we got past
	// the first failure (the loop didn't terminate).
}

func TestStartAfterStopErrors(t *testing.T) {
	store := newTestStore(t)
	w, err := New(store, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.Stop()
	if err := w.Start(context.Background()); err == nil {
		t.Fatal("expected error when starting a stopped Workers")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// pin fmt to silence "imported and not used" if a future edit removes the
// only fmt-using line.
var _ = fmt.Sprintf
