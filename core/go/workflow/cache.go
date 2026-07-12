package workflow

import (
	"context"
	"sync"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

// runScopedCache wraps a storage.Store with an in-memory record cache scoped
// to a single workflow run. Reads under the scoped (namespace, workflow_id,
// run_id) serve from cache when available; reads outside the scope and writes
// pass through to the underlying store (write-through).
//
// Replay is the motivating case: a workflow that previously executed N
// activities had to issue N individual GetActivity round-trips on every
// re-invocation. With prefetch enabled, a single ListActivities call up front
// populates every record and subsequent GetActivity calls hit memory. The
// same applies to timers and events.
//
// The cache is safe under concurrent use: asyncio-style fan-out activities
// inside one workflow body may issue Get/Put calls in parallel.
type runScopedCache struct {
	inner storage.Store
	scope storage.WorkflowKey

	mu sync.Mutex

	workflowKnown bool
	// nil records the negative-cache case (workflow record absent from store)
	workflow *temporalessv1.WorkflowRecord

	activitiesListed bool
	activities       map[string]*temporalessv1.ActivityRecord

	timersListed bool
	timers       map[string]*temporalessv1.TimerRecord

	eventsListed bool
	events       map[string]*temporalessv1.EventRecord
}

func newRunScopedCache(inner storage.Store, scope storage.WorkflowKey) *runScopedCache {
	return &runScopedCache{
		inner:      inner,
		scope:      scope,
		activities: map[string]*temporalessv1.ActivityRecord{},
		timers:     map[string]*temporalessv1.TimerRecord{},
		events:     map[string]*temporalessv1.EventRecord{},
	}
}

// prefetch issues ListActivities, ListTimers, and ListEvents in parallel and
// populates the cache. After prefetch returns, Get calls for keys not in the
// result short-circuit to (nil, false) without an underlying round-trip.
//
// Worth calling only when the workflow record exists in IN_PROGRESS state — a
// fresh run has nothing to prefetch.
func (c *runScopedCache) prefetch(ctx context.Context) error {
	var (
		activities []*temporalessv1.ActivityRecord
		timers     []*temporalessv1.TimerRecord
		events     []*temporalessv1.EventRecord
		actErr     error
		timErr     error
		evtErr     error
	)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		activities, actErr = c.inner.ListActivities(ctx, c.scope)
	}()
	go func() {
		defer wg.Done()
		timers, timErr = c.inner.ListTimers(ctx, c.scope, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
	}()
	go func() {
		defer wg.Done()
		events, evtErr = c.inner.ListEvents(ctx, c.scope)
	}()
	wg.Wait()
	if actErr != nil {
		return actErr
	}
	if timErr != nil {
		return timErr
	}
	if evtErr != nil {
		return evtErr
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range activities {
		c.activities[storage.ActivityKeyFromProto(r.GetKey()).ActivityID] = r
	}
	c.activitiesListed = true
	for _, r := range timers {
		c.timers[storage.TimerKeyFromProto(r.GetKey()).TimerID] = r
	}
	c.timersListed = true
	for _, r := range events {
		c.events[storage.EventKeyFromProto(r.GetKey()).EventID] = r
	}
	c.eventsListed = true
	return nil
}

func (c *runScopedCache) inScope(namespace, workflowID, runID string) bool {
	return namespace == c.scope.Namespace &&
		workflowID == c.scope.WorkflowID &&
		runID == c.scope.RunID
}

// WorkflowStore -------------------------------------------------------------

func (c *runScopedCache) GetWorkflow(ctx context.Context, key storage.WorkflowKey) (*temporalessv1.WorkflowRecord, bool, error) {
	if !c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		return c.inner.GetWorkflow(ctx, key)
	}
	c.mu.Lock()
	if c.workflowKnown {
		rec := c.workflow
		c.mu.Unlock()
		if rec == nil {
			return nil, false, nil
		}
		return rec, true, nil
	}
	c.mu.Unlock()
	rec, found, err := c.inner.GetWorkflow(ctx, key)
	if err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	c.workflowKnown = true
	if found {
		c.workflow = rec
	}
	c.mu.Unlock()
	if !found {
		return nil, false, nil
	}
	return rec, true, nil
}

func (c *runScopedCache) PutWorkflow(ctx context.Context, record *temporalessv1.WorkflowRecord) error {
	if err := c.inner.PutWorkflow(ctx, record); err != nil {
		return err
	}
	key := storage.WorkflowKeyFromProto(record.GetKey())
	if c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		c.mu.Lock()
		c.workflowKnown = true
		c.workflow = record
		c.mu.Unlock()
	}
	return nil
}

func (c *runScopedCache) ListWorkflows(
	ctx context.Context,
	namespace string,
	workflowID string,
	status temporalessv1.WorkflowStatus,
) ([]*temporalessv1.WorkflowRecord, error) {
	return c.inner.ListWorkflows(ctx, namespace, workflowID, status)
}

func (c *runScopedCache) DeleteWorkflow(ctx context.Context, key storage.WorkflowKey) (bool, error) {
	deleted, err := c.inner.DeleteWorkflow(ctx, key)
	if err == nil && c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		c.mu.Lock()
		c.workflowKnown = true
		c.workflow = nil
		c.mu.Unlock()
	}
	return deleted, err
}

// ActivityStore -------------------------------------------------------------

func (c *runScopedCache) GetActivity(ctx context.Context, key storage.ActivityKey) (*temporalessv1.ActivityRecord, bool, error) {
	if !c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		return c.inner.GetActivity(ctx, key)
	}
	c.mu.Lock()
	if rec, ok := c.activities[key.ActivityID]; ok {
		c.mu.Unlock()
		if rec == nil {
			return nil, false, nil
		}
		return rec, true, nil
	}
	listed := c.activitiesListed
	c.mu.Unlock()
	if listed {
		return nil, false, nil
	}
	rec, found, err := c.inner.GetActivity(ctx, key)
	if err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	if found {
		c.activities[key.ActivityID] = rec
	} else {
		c.activities[key.ActivityID] = nil
	}
	c.mu.Unlock()
	if !found {
		return nil, false, nil
	}
	return rec, true, nil
}

// refreshActivity bypasses any cached hit or miss and reloads the activity
// from the authoritative store. Claim arbitration uses this after a failed or
// successful create: another invocation may have committed a terminal record
// after this run cached an earlier miss. The refreshed value is written back
// into the run cache so subsequent replay reads observe the same state.
func (c *runScopedCache) refreshActivity(
	ctx context.Context,
	key storage.ActivityKey,
) (*temporalessv1.ActivityRecord, bool, error) {
	if !c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		return c.inner.GetActivity(ctx, key)
	}
	rec, found, err := c.inner.GetActivity(ctx, key)
	if err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	if found {
		c.activities[key.ActivityID] = rec
	} else {
		c.activities[key.ActivityID] = nil
	}
	c.mu.Unlock()
	if !found {
		return nil, false, nil
	}
	return rec, true, nil
}

func (c *runScopedCache) PutActivity(ctx context.Context, record *temporalessv1.ActivityRecord) error {
	if err := c.inner.PutActivity(ctx, record); err != nil {
		return err
	}
	key := storage.ActivityKeyFromProto(record.GetKey())
	if c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		c.mu.Lock()
		c.activities[key.ActivityID] = record
		c.mu.Unlock()
	}
	return nil
}

func (c *runScopedCache) ListActivities(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.ActivityRecord, error) {
	if !c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		return c.inner.ListActivities(ctx, key)
	}
	c.mu.Lock()
	if c.activitiesListed {
		records := make([]*temporalessv1.ActivityRecord, 0, len(c.activities))
		for _, r := range c.activities {
			if r != nil {
				records = append(records, r)
			}
		}
		c.mu.Unlock()
		return records, nil
	}
	c.mu.Unlock()
	records, err := c.inner.ListActivities(ctx, key)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	for _, r := range records {
		c.activities[storage.ActivityKeyFromProto(r.GetKey()).ActivityID] = r
	}
	c.activitiesListed = true
	c.mu.Unlock()
	return records, nil
}

func (c *runScopedCache) DeleteActivity(ctx context.Context, key storage.ActivityKey) (bool, error) {
	deleted, err := c.inner.DeleteActivity(ctx, key)
	if err == nil && c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		c.mu.Lock()
		c.activities[key.ActivityID] = nil
		c.mu.Unlock()
	}
	return deleted, err
}

// TimerStore ----------------------------------------------------------------

func (c *runScopedCache) GetTimer(ctx context.Context, key storage.TimerKey) (*temporalessv1.TimerRecord, bool, error) {
	if !c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		return c.inner.GetTimer(ctx, key)
	}
	c.mu.Lock()
	if rec, ok := c.timers[key.TimerID]; ok {
		c.mu.Unlock()
		if rec == nil {
			return nil, false, nil
		}
		return rec, true, nil
	}
	listed := c.timersListed
	c.mu.Unlock()
	if listed {
		return nil, false, nil
	}
	rec, found, err := c.inner.GetTimer(ctx, key)
	if err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	if found {
		c.timers[key.TimerID] = rec
	} else {
		c.timers[key.TimerID] = nil
	}
	c.mu.Unlock()
	if !found {
		return nil, false, nil
	}
	return rec, true, nil
}

func (c *runScopedCache) PutTimer(ctx context.Context, record *temporalessv1.TimerRecord) error {
	if err := c.inner.PutTimer(ctx, record); err != nil {
		return err
	}
	key := storage.TimerKeyFromProto(record.GetKey())
	if c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		c.mu.Lock()
		c.timers[key.TimerID] = record
		c.mu.Unlock()
	}
	return nil
}

func (c *runScopedCache) ListTimers(
	ctx context.Context,
	key storage.WorkflowKey,
	status temporalessv1.TimerStatus,
) ([]*temporalessv1.TimerRecord, error) {
	if !c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		return c.inner.ListTimers(ctx, key, status)
	}
	c.mu.Lock()
	if c.timersListed {
		records := make([]*temporalessv1.TimerRecord, 0, len(c.timers))
		for _, r := range c.timers {
			if r == nil {
				continue
			}
			if status != temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED && r.GetStatus() != status {
				continue
			}
			records = append(records, r)
		}
		c.mu.Unlock()
		return records, nil
	}
	c.mu.Unlock()
	// Only the unfiltered list call populates the cache — a filtered call could
	// hide records the body later wants.
	if status != temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED {
		return c.inner.ListTimers(ctx, key, status)
	}
	records, err := c.inner.ListTimers(ctx, key, status)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	for _, r := range records {
		c.timers[storage.TimerKeyFromProto(r.GetKey()).TimerID] = r
	}
	c.timersListed = true
	c.mu.Unlock()
	return records, nil
}

func (c *runScopedCache) DeleteTimer(ctx context.Context, key storage.TimerKey) (bool, error) {
	deleted, err := c.inner.DeleteTimer(ctx, key)
	if err == nil && c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		c.mu.Lock()
		c.timers[key.TimerID] = nil
		c.mu.Unlock()
	}
	return deleted, err
}

// EventStore ----------------------------------------------------------------

func (c *runScopedCache) GetEvent(ctx context.Context, key storage.EventKey) (*temporalessv1.EventRecord, bool, error) {
	if !c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		return c.inner.GetEvent(ctx, key)
	}
	c.mu.Lock()
	if rec, ok := c.events[key.EventID]; ok {
		c.mu.Unlock()
		if rec == nil {
			return nil, false, nil
		}
		return rec, true, nil
	}
	listed := c.eventsListed
	c.mu.Unlock()
	if listed {
		return nil, false, nil
	}
	rec, found, err := c.inner.GetEvent(ctx, key)
	if err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	if found {
		c.events[key.EventID] = rec
	} else {
		c.events[key.EventID] = nil
	}
	c.mu.Unlock()
	if !found {
		return nil, false, nil
	}
	return rec, true, nil
}

func (c *runScopedCache) PutEvent(ctx context.Context, record *temporalessv1.EventRecord) error {
	if err := c.inner.PutEvent(ctx, record); err != nil {
		return err
	}
	key := storage.EventKeyFromProto(record.GetKey())
	if c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		c.mu.Lock()
		c.events[key.EventID] = record
		c.mu.Unlock()
	}
	return nil
}

func (c *runScopedCache) ListEvents(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.EventRecord, error) {
	if !c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		return c.inner.ListEvents(ctx, key)
	}
	c.mu.Lock()
	if c.eventsListed {
		records := make([]*temporalessv1.EventRecord, 0, len(c.events))
		for _, r := range c.events {
			if r != nil {
				records = append(records, r)
			}
		}
		c.mu.Unlock()
		return records, nil
	}
	c.mu.Unlock()
	records, err := c.inner.ListEvents(ctx, key)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	for _, r := range records {
		c.events[storage.EventKeyFromProto(r.GetKey()).EventID] = r
	}
	c.eventsListed = true
	c.mu.Unlock()
	return records, nil
}

func (c *runScopedCache) DeleteEvent(ctx context.Context, key storage.EventKey) (bool, error) {
	deleted, err := c.inner.DeleteEvent(ctx, key)
	if err == nil && c.inScope(key.Namespace, key.WorkflowID, key.RunID) {
		c.mu.Lock()
		c.events[key.EventID] = nil
		c.mu.Unlock()
	}
	return deleted, err
}

// Operator-only methods pass straight through ------------------------------

func (c *runScopedCache) Sweep(ctx context.Context, namespace string, now time.Time, maxAge time.Duration) (uint32, error) {
	return c.inner.Sweep(ctx, namespace, now, maxAge)
}

func (c *runScopedCache) DueTimers(ctx context.Context, namespace string, now time.Time) ([]storage.DueTimer, error) {
	return c.inner.DueTimers(ctx, namespace, now)
}
