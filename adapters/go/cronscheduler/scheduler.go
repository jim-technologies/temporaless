// Package cronscheduler is a small cron-style scheduler that dispatches
// workflow runs at scheduled fire times.
//
// Callers hand in a list of cron schedules and a dispatcher callback; Tick(now)
// computes which schedules are due since the last fire and invokes the
// dispatcher with the schedule ID and fire time.
//
// The scheduler is stateful but the state is fully serializable. For
// distributed or restartable use:
//   - Call Snapshot() to extract the current last-fires map.
//   - Persist it externally (storage, SQL, KV).
//   - On next boot, call Restore() with the persisted map.
//
// For fully storage-derived state (no separate persistence), pair the scheduler
// with LastFireFromRuns: it scans existing workflow records for the schedule
// and parses run_ids as timestamps, returning the most recent fire time. This
// is the recommended pattern when run_ids follow the
// `prices:aapl/2026-05-04T09:30:00Z` convention.
package cronscheduler

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// Schedule is a stable schedule definition.
type Schedule struct {
	ID         string
	Expression string
}

// Dispatcher is invoked for each fired schedule. The fireTime is the cron's
// scheduled instant, not wall-clock now — so callers can build deterministic
// run_ids like `2026-05-02T09:30:00Z`.
type Dispatcher func(ctx context.Context, scheduleID string, fireTime time.Time) error

// Scheduler tracks last-fire times and dispatches due schedules on Tick.
type Scheduler struct {
	mu        sync.Mutex
	parser    cron.Parser
	parsed    map[string]cron.Schedule
	lastFires map[string]time.Time
	schedules []Schedule
	dispatch  Dispatcher
}

// New constructs a scheduler. ParseStandard semantics — five fields, minute
// resolution.
func New(schedules []Schedule, dispatch Dispatcher) (*Scheduler, error) {
	if dispatch == nil {
		return nil, fmt.Errorf("dispatcher is required")
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	parsed := make(map[string]cron.Schedule, len(schedules))
	for _, schedule := range schedules {
		if schedule.ID == "" {
			return nil, fmt.Errorf("schedule id is required")
		}
		if _, exists := parsed[schedule.ID]; exists {
			return nil, fmt.Errorf("duplicate schedule id %q", schedule.ID)
		}
		spec, err := parser.Parse(schedule.Expression)
		if err != nil {
			return nil, fmt.Errorf("schedule %q: %w", schedule.ID, err)
		}
		parsed[schedule.ID] = spec
	}
	return &Scheduler{
		parser:    parser,
		parsed:    parsed,
		lastFires: make(map[string]time.Time, len(schedules)),
		schedules: schedules,
		dispatch:  dispatch,
	}, nil
}

// Seed sets the last-fire time for a schedule. Use this on boot to skip
// schedules whose fire times are already represented by stored workflow runs.
func (s *Scheduler) Seed(scheduleID string, lastFire time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.parsed[scheduleID]; !ok {
		return fmt.Errorf("unknown schedule %q", scheduleID)
	}
	s.lastFires[scheduleID] = lastFire
	return nil
}

// Tick fires every schedule whose next fire time after its last seen fire is
// at or before `now`. The dispatcher is called once per fired schedule. If a
// schedule has multiple due fires (e.g. the process slept through several
// cron intervals), all of them are dispatched in chronological order.
func (s *Scheduler) Tick(ctx context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fired := 0
	for _, schedule := range s.schedules {
		if err := ctx.Err(); err != nil {
			return fired, err
		}
		spec := s.parsed[schedule.ID]
		anchor, ok := s.lastFires[schedule.ID]
		if !ok {
			anchor = now
			s.lastFires[schedule.ID] = anchor
			continue
		}
		next := spec.Next(anchor)
		for !next.After(now) {
			if err := ctx.Err(); err != nil {
				return fired, err
			}
			if err := s.dispatch(ctx, schedule.ID, next); err != nil {
				return fired, fmt.Errorf("schedule %q dispatch at %s: %w",
					schedule.ID, next.UTC().Format(time.RFC3339Nano), err)
			}
			fired++
			s.lastFires[schedule.ID] = next
			next = spec.Next(next)
		}
	}
	return fired, nil
}

// LastFire returns the last fire time the scheduler has dispatched (or seeded)
// for the schedule.
func (s *Scheduler) LastFire(scheduleID string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.lastFires[scheduleID]
	return t, ok
}

// Snapshot returns a copy of the current last-fire map. Persist it externally
// to make the scheduler stateless across restarts.
func (s *Scheduler) Snapshot() map[string]time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]time.Time, len(s.lastFires))
	maps.Copy(out, s.lastFires)
	return out
}

// Restore replaces the in-memory last-fire map with the given snapshot.
// Schedules in the snapshot but not in the scheduler are silently ignored.
// Schedules in the scheduler but not in the snapshot keep whatever state they
// had (typically: nothing — first Tick will anchor them to `now`).
func (s *Scheduler) Restore(snapshot map[string]time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range snapshot {
		if _, ok := s.parsed[id]; !ok {
			continue
		}
		s.lastFires[id] = t
	}
}
