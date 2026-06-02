package cronscheduler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/cronscheduler"
)

func TestTickFiresEverySchedulePastDue(t *testing.T) {
	tests := []struct {
		name     string
		schedule cronscheduler.Schedule
		seedAt   time.Time
		tickAt   time.Time
		wantHits []time.Time
	}{
		{
			name:     "minute schedule fires once for one-minute step",
			schedule: cronscheduler.Schedule{ID: "every-minute", Expression: "* * * * *"},
			seedAt:   time.Date(2026, 5, 2, 9, 30, 0, 0, time.UTC),
			tickAt:   time.Date(2026, 5, 2, 9, 31, 30, 0, time.UTC),
			wantHits: []time.Time{time.Date(2026, 5, 2, 9, 31, 0, 0, time.UTC)},
		},
		{
			name:     "minute schedule fires three times after a three-minute gap",
			schedule: cronscheduler.Schedule{ID: "every-minute", Expression: "* * * * *"},
			seedAt:   time.Date(2026, 5, 2, 9, 30, 0, 0, time.UTC),
			tickAt:   time.Date(2026, 5, 2, 9, 33, 5, 0, time.UTC),
			wantHits: []time.Time{
				time.Date(2026, 5, 2, 9, 31, 0, 0, time.UTC),
				time.Date(2026, 5, 2, 9, 32, 0, 0, time.UTC),
				time.Date(2026, 5, 2, 9, 33, 0, 0, time.UTC),
			},
		},
		{
			name:     "weekday-only schedule does not fire on weekends",
			schedule: cronscheduler.Schedule{ID: "weekday-open", Expression: "30 9 * * 1-5"},
			seedAt:   time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC),  // Saturday
			tickAt:   time.Date(2026, 5, 4, 9, 35, 0, 0, time.UTC), // Monday after 9:30
			wantHits: []time.Time{time.Date(2026, 5, 4, 9, 30, 0, 0, time.UTC)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var fired []time.Time
			scheduler, err := cronscheduler.New(
				[]cronscheduler.Schedule{test.schedule},
				func(_ context.Context, _ string, fireTime time.Time) error {
					fired = append(fired, fireTime)
					return nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := scheduler.Seed(test.schedule.ID, test.seedAt); err != nil {
				t.Fatal(err)
			}
			count, err := scheduler.Tick(context.Background(), test.tickAt)
			if err != nil {
				t.Fatal(err)
			}
			if count != len(test.wantHits) {
				t.Fatalf("fired = %d, want %d", count, len(test.wantHits))
			}
			if len(fired) != len(test.wantHits) {
				t.Fatalf("dispatcher invocations = %v, want %v", fired, test.wantHits)
			}
			for i := range fired {
				if !fired[i].Equal(test.wantHits[i]) {
					t.Fatalf("hit[%d] = %s, want %s", i, fired[i], test.wantHits[i])
				}
			}
		})
	}
}

func TestTickWithoutSeedAnchorsToFirstTick(t *testing.T) {
	dispatched := 0
	scheduler, err := cronscheduler.New(
		[]cronscheduler.Schedule{{ID: "every-minute", Expression: "* * * * *"}},
		func(context.Context, string, time.Time) error {
			dispatched++
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	first, err := scheduler.Tick(context.Background(), time.Date(2026, 5, 2, 9, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if first != 0 || dispatched != 0 {
		t.Fatalf("first tick fired = %d, dispatched = %d, want 0/0", first, dispatched)
	}

	second, err := scheduler.Tick(context.Background(), time.Date(2026, 5, 2, 9, 31, 30, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if second != 1 || dispatched != 1 {
		t.Fatalf("second tick fired = %d, dispatched = %d, want 1/1", second, dispatched)
	}
}

func TestDispatcherErrorStopsTick(t *testing.T) {
	stopErr := errors.New("dispatcher boom")
	dispatched := 0
	scheduler, err := cronscheduler.New(
		[]cronscheduler.Schedule{{ID: "every-minute", Expression: "* * * * *"}},
		func(context.Context, string, time.Time) error {
			dispatched++
			return stopErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Seed("every-minute", time.Date(2026, 5, 2, 9, 30, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	count, err := scheduler.Tick(context.Background(), time.Date(2026, 5, 2, 9, 33, 0, 0, time.UTC))
	if !errors.Is(err, stopErr) {
		t.Fatalf("err = %v, want %v", err, stopErr)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (error before any successful dispatch)", count)
	}
	if dispatched != 1 {
		t.Fatalf("dispatched = %d, want 1 (only first attempt before error)", dispatched)
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	dispatched := []time.Time{}
	scheduler, err := cronscheduler.New(
		[]cronscheduler.Schedule{{ID: "every-minute", Expression: "* * * * *"}},
		func(_ context.Context, _ string, fireTime time.Time) error {
			dispatched = append(dispatched, fireTime)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Seed("every-minute", time.Date(2026, 5, 4, 9, 30, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Tick(context.Background(), time.Date(2026, 5, 4, 9, 32, 30, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if got := len(dispatched); got != 2 {
		t.Fatalf("first scheduler dispatched = %d, want 2", got)
	}

	// Persist the snapshot — this is what a caller would write to storage.
	snapshot := scheduler.Snapshot()
	want := time.Date(2026, 5, 4, 9, 32, 0, 0, time.UTC)
	if got := snapshot["every-minute"]; !got.Equal(want) {
		t.Fatalf("snapshot last fire = %s, want %s", got, want)
	}

	// New process boots with a fresh scheduler. Restore the snapshot.
	dispatched = nil
	revived, err := cronscheduler.New(
		[]cronscheduler.Schedule{{ID: "every-minute", Expression: "* * * * *"}},
		func(_ context.Context, _ string, fireTime time.Time) error {
			dispatched = append(dispatched, fireTime)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	revived.Restore(snapshot)

	if _, err := revived.Tick(context.Background(), time.Date(2026, 5, 4, 9, 33, 30, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if got := len(dispatched); got != 1 {
		t.Fatalf("revived scheduler dispatched = %d, want 1 (only 9:33 since restore set last_fire to 9:32)", got)
	}
	if !dispatched[0].Equal(time.Date(2026, 5, 4, 9, 33, 0, 0, time.UTC)) {
		t.Fatalf("revived dispatch = %s, want 9:33:00", dispatched[0])
	}
}

func TestNewRejectsBadInput(t *testing.T) {
	tests := []struct {
		name      string
		schedules []cronscheduler.Schedule
		dispatch  cronscheduler.Dispatcher
	}{
		{
			name:      "nil dispatcher",
			schedules: []cronscheduler.Schedule{{ID: "x", Expression: "* * * * *"}},
			dispatch:  nil,
		},
		{
			name: "duplicate id",
			schedules: []cronscheduler.Schedule{
				{ID: "x", Expression: "* * * * *"},
				{ID: "x", Expression: "0 9 * * *"},
			},
			dispatch: func(context.Context, string, time.Time) error { return nil },
		},
		{
			name:      "bad expression",
			schedules: []cronscheduler.Schedule{{ID: "x", Expression: "not a cron"}},
			dispatch:  func(context.Context, string, time.Time) error { return nil },
		},
		{
			name:      "missing id",
			schedules: []cronscheduler.Schedule{{Expression: "* * * * *"}},
			dispatch:  func(context.Context, string, time.Time) error { return nil },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := cronscheduler.New(test.schedules, test.dispatch)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
