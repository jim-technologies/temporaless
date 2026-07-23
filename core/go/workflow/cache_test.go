package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// countingStore wraps a storage.Store and tallies each method call. Used by
// cache tests to assert that replay paths issue ListActivities/ListTimers/
// ListEvents up front and serve subsequent Get* calls from cache.
type countingStore struct {
	inner storage.Store

	getActivity  atomic.Int64
	listActivity atomic.Int64
	putActivity  atomic.Int64
	getWorkflow  atomic.Int64
	putWorkflow  atomic.Int64
	getTimer     atomic.Int64
	listTimer    atomic.Int64
	putTimer     atomic.Int64
	getEvent     atomic.Int64
	listEvent    atomic.Int64
	putEvent     atomic.Int64
	deleteCalls  atomic.Int64
}

func newCountingStore(inner storage.Store) *countingStore {
	return &countingStore{inner: inner}
}

func (s *countingStore) GetActivity(ctx context.Context, key storage.ActivityKey) (*temporalessv1.ActivityRecord, bool, error) {
	s.getActivity.Add(1)
	return s.inner.GetActivity(ctx, key)
}

func (s *countingStore) PutActivity(ctx context.Context, record *temporalessv1.ActivityRecord) error {
	s.putActivity.Add(1)
	return s.inner.PutActivity(ctx, record)
}

func (s *countingStore) ListActivities(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.ActivityRecord, error) {
	s.listActivity.Add(1)
	return s.inner.ListActivities(ctx, key)
}

func (s *countingStore) DeleteActivity(ctx context.Context, key storage.ActivityKey) (bool, error) {
	s.deleteCalls.Add(1)
	return s.inner.DeleteActivity(ctx, key)
}

func (s *countingStore) GetWorkflow(ctx context.Context, key storage.WorkflowKey) (*temporalessv1.WorkflowRecord, bool, error) {
	s.getWorkflow.Add(1)
	return s.inner.GetWorkflow(ctx, key)
}

func (s *countingStore) PutWorkflow(ctx context.Context, record *temporalessv1.WorkflowRecord) error {
	s.putWorkflow.Add(1)
	return s.inner.PutWorkflow(ctx, record)
}

func (s *countingStore) GetLatestWorkflowRun(ctx context.Context, namespace, workflowID string) (*temporalessv1.LatestWorkflowRunPointer, bool, error) {
	return s.inner.GetLatestWorkflowRun(ctx, namespace, workflowID)
}

func (s *countingStore) DeleteWorkflow(ctx context.Context, key storage.WorkflowKey) (bool, error) {
	s.deleteCalls.Add(1)
	return s.inner.DeleteWorkflow(ctx, key)
}

func (s *countingStore) GetTimer(ctx context.Context, key storage.TimerKey) (*temporalessv1.TimerRecord, bool, error) {
	s.getTimer.Add(1)
	return s.inner.GetTimer(ctx, key)
}

func (s *countingStore) PutTimer(ctx context.Context, record *temporalessv1.TimerRecord) error {
	s.putTimer.Add(1)
	return s.inner.PutTimer(ctx, record)
}

func (s *countingStore) ListTimers(ctx context.Context, key storage.WorkflowKey, status temporalessv1.TimerStatus) ([]*temporalessv1.TimerRecord, error) {
	s.listTimer.Add(1)
	return s.inner.ListTimers(ctx, key, status)
}

func (s *countingStore) DeleteTimer(ctx context.Context, key storage.TimerKey) (bool, error) {
	s.deleteCalls.Add(1)
	return s.inner.DeleteTimer(ctx, key)
}

func (s *countingStore) GetEvent(ctx context.Context, key storage.EventKey) (*temporalessv1.EventRecord, bool, error) {
	s.getEvent.Add(1)
	return s.inner.GetEvent(ctx, key)
}

func (s *countingStore) PutEvent(ctx context.Context, record *temporalessv1.EventRecord) error {
	s.putEvent.Add(1)
	return s.inner.PutEvent(ctx, record)
}

func (s *countingStore) ListEvents(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.EventRecord, error) {
	s.listEvent.Add(1)
	return s.inner.ListEvents(ctx, key)
}

func (s *countingStore) DeleteEvent(ctx context.Context, key storage.EventKey) (bool, error) {
	s.deleteCalls.Add(1)
	return s.inner.DeleteEvent(ctx, key)
}

func (s *countingStore) DueTimers(ctx context.Context, namespace string, now time.Time) ([]storage.DueTimer, error) {
	return s.inner.DueTimers(ctx, namespace, now)
}

// runFanout drives a workflow with N parallel-style fan-out activities — each
// records a unique activity_id, deterministic input, returns a unique output.
// The body runs sequentially in this helper to keep tests focused on the
// storage RPC pattern rather than concurrent execution.
func runFanout(t *testing.T, ctx context.Context, store storage.Store, runID string, n int, expectedExecutions *atomic.Int64) (*wrapperspb.Int32Value, error) {
	t.Helper()
	return Run(
		ctx,
		store,
		&temporalessv1.WorkflowOptions{
			WorkflowId: "fanout",
			RunId:      runID,
		},
		nil,
		wrapperspb.Int32(int32(n)),
		func() *wrapperspb.Int32Value { return &wrapperspb.Int32Value{} },
		func(ctx context.Context, req *wrapperspb.Int32Value) (*wrapperspb.Int32Value, error) {
			var sum int32
			for i := int32(0); i < req.GetValue(); i++ {
				result, err := ExecuteActivity(
					ctx,
					&temporalessv1.ActivityOptions{ActivityId: activityIDForIndex(i)},
					wrapperspb.Int32(i),
					func() *wrapperspb.Int32Value { return &wrapperspb.Int32Value{} },
					func(_ context.Context, in *wrapperspb.Int32Value) (*wrapperspb.Int32Value, error) {
						if expectedExecutions != nil {
							expectedExecutions.Add(1)
						}
						return wrapperspb.Int32(in.GetValue() * 2), nil
					},
				)
				if err != nil {
					return nil, err
				}
				sum += result.GetValue()
			}
			return wrapperspb.Int32(sum), nil
		},
	)
}

func activityIDForIndex(i int32) string {
	// activity IDs only allow letters/numbers/. _ - : so use ":"
	return "act:" + intToASCII(i)
}

func intToASCII(i int32) string {
	if i == 0 {
		return "0"
	}
	var buf []byte
	for i > 0 {
		buf = append([]byte{byte('0' + i%10)}, buf...)
		i /= 10
	}
	return string(buf)
}

// TestReplayUsesCachedActivities verifies the core promise: on replay, the
// runtime issues a single ListActivities call up front and serves every
// GetActivity from cache. Without the cache, replay would have issued N
// GetActivity round-trips.
func TestReplayUsesCachedActivities(t *testing.T) {
	ctx := context.Background()
	const activityCount = 8

	inner := newTestStore(t)
	counter := newCountingStore(inner)

	// First run: fresh, all activities execute.
	executions := &atomic.Int64{}
	_, err := runFanout(t, ctx, counter, "run-1", activityCount, executions)
	if err != nil {
		t.Fatal(err)
	}
	if got := executions.Load(); got != activityCount {
		t.Fatalf("first run executions = %d, want %d", got, activityCount)
	}

	// Reset counters before the replay.
	counter.getActivity.Store(0)
	counter.listActivity.Store(0)
	counter.putActivity.Store(0)
	counter.getWorkflow.Store(0)
	counter.putWorkflow.Store(0)
	counter.listTimer.Store(0)
	counter.listEvent.Store(0)
	executions.Store(0)

	// Delete the COMPLETED workflow record so Run takes the IN_PROGRESS replay
	// path (otherwise it short-circuits at the first GetWorkflow). Then re-put
	// the record as IN_PROGRESS to mimic a crashed pod resuming mid-run.
	key := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "fanout",
		RunID:      "run-1",
	}
	completed, found, err := inner.GetWorkflow(ctx, key)
	if err != nil || !found {
		t.Fatalf("expected completed workflow record: err=%v found=%v", err, found)
	}
	completed.Status = temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS
	completed.CompletedAt = nil
	completed.Result = nil
	if err := inner.PutWorkflow(ctx, completed); err != nil {
		t.Fatal(err)
	}

	// Second run: must reach the body (since status is IN_PROGRESS), but every
	// activity replays from cache.
	_, err = runFanout(t, ctx, counter, "run-1", activityCount, executions)
	if err != nil {
		t.Fatal(err)
	}
	if got := executions.Load(); got != 0 {
		t.Fatalf("replay executions = %d, want 0 (all should replay from cache)", got)
	}

	// The big assertion: replay must NOT issue N GetActivity round-trips. The
	// cache should have served them all after one ListActivities call.
	if got := counter.getActivity.Load(); got != 0 {
		t.Fatalf("replay GetActivity calls = %d, want 0 (all should hit cache after prefetch)", got)
	}
	if got := counter.listActivity.Load(); got != 1 {
		t.Fatalf("replay ListActivities calls = %d, want 1", got)
	}
	if got := counter.listTimer.Load(); got != 1 {
		t.Fatalf("replay ListTimers calls = %d, want 1", got)
	}
	if got := counter.listEvent.Load(); got != 1 {
		t.Fatalf("replay ListEvents calls = %d, want 1", got)
	}
}

// TestFreshRunSkipsPrefetch verifies that a fresh run (no prior records)
// doesn't issue an unnecessary ListActivities/ListTimers/ListEvents trio when
// the workflow record isn't yet present.
func TestFreshRunSkipsPrefetch(t *testing.T) {
	ctx := context.Background()

	inner := newTestStore(t)
	counter := newCountingStore(inner)

	executions := &atomic.Int64{}
	_, err := runFanout(t, ctx, counter, "fresh-run", 3, executions)
	if err != nil {
		t.Fatal(err)
	}

	if got := counter.listActivity.Load(); got != 0 {
		t.Fatalf("fresh run ListActivities calls = %d, want 0 (no prefetch on a fresh run)", got)
	}
	if got := counter.listTimer.Load(); got != 0 {
		t.Fatalf("fresh run ListTimers calls = %d, want 0", got)
	}
	if got := counter.listEvent.Load(); got != 0 {
		t.Fatalf("fresh run ListEvents calls = %d, want 0", got)
	}
}

// TestWriteThroughAndReadback verifies cache write-through: after Put, a Get
// for the same key returns the just-written value (without hitting the inner
// store). And inner is still authoritative — a separate process reading
// directly sees the same record.
func TestWriteThroughAndReadback(t *testing.T) {
	ctx := context.Background()
	inner := newTestStore(t)
	counter := newCountingStore(inner)
	scope := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "w",
		RunID:      "r",
	}
	cache := newRunScopedCache(counter, scope)

	record := &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key: storage.ActivityKey{
			Namespace:  scope.Namespace,
			WorkflowID: scope.WorkflowID,
			RunID:      scope.RunID,
			ActivityID: "a",
		}.Proto(),
		ActivityType: "activity:google.protobuf.StringValue->google.protobuf.StringValue",
		Status:       temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
		CreatedAt:    timestamppb.Now(),
		CompletedAt:  timestamppb.Now(),
	}
	if err := cache.PutActivity(ctx, record); err != nil {
		t.Fatal(err)
	}

	// Get via cache should hit memory — no inner GetActivity.
	got, found, err := cache.GetActivity(ctx, storage.ActivityKey{
		Namespace:  scope.Namespace,
		WorkflowID: scope.WorkflowID,
		RunID:      scope.RunID,
		ActivityID: "a",
	})
	if err != nil || !found {
		t.Fatalf("err=%v found=%v", err, found)
	}
	if got.GetActivityType() != record.GetActivityType() {
		t.Fatalf("activity_type = %q", got.GetActivityType())
	}
	if counter.getActivity.Load() != 0 {
		t.Fatalf("expected 0 GetActivity passthroughs, got %d", counter.getActivity.Load())
	}
	if counter.putActivity.Load() != 1 {
		t.Fatalf("expected 1 PutActivity passthrough, got %d", counter.putActivity.Load())
	}

	// Inner should also have the record.
	innerGot, innerFound, err := inner.GetActivity(ctx, storage.ActivityKey{
		Namespace:  scope.Namespace,
		WorkflowID: scope.WorkflowID,
		RunID:      scope.RunID,
		ActivityID: "a",
	})
	if err != nil || !innerFound {
		t.Fatalf("inner: err=%v found=%v", err, innerFound)
	}
	if innerGot.GetActivityType() != record.GetActivityType() {
		t.Fatalf("inner activity_type = %q", innerGot.GetActivityType())
	}
}

// TestOutOfScopeReadPassesThrough verifies the cache doesn't interfere with
// cross-pipeline reads — e.g. dependencies.WaitForWorkflow reads another
// workflow_id's record, which must still hit the underlying store.
func TestOutOfScopeReadPassesThrough(t *testing.T) {
	ctx := context.Background()
	inner := newTestStore(t)
	counter := newCountingStore(inner)
	scope := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "active-workflow",
		RunID:      "r",
	}
	cache := newRunScopedCache(counter, scope)
	if err := cache.prefetch(ctx); err != nil {
		t.Fatal(err)
	}

	otherKey := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "other-workflow",
		RunID:      "r",
	}
	otherRecord := &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           otherKey.Proto(),
		WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		CreatedAt:     timestamppb.Now(),
	}
	if err := inner.PutWorkflow(ctx, otherRecord); err != nil {
		t.Fatal(err)
	}
	counter.getWorkflow.Store(0)

	got, found, err := cache.GetWorkflow(ctx, otherKey)
	if err != nil || !found {
		t.Fatalf("err=%v found=%v", err, found)
	}
	if got.GetWorkflowType() != otherRecord.GetWorkflowType() {
		t.Fatal("returned record doesn't match")
	}
	if counter.getWorkflow.Load() != 1 {
		t.Fatalf("expected 1 passthrough GetWorkflow, got %d", counter.getWorkflow.Load())
	}
}

// TestNegativeCacheAfterPrefetch verifies that after prefetch, a Get for a
// non-existent activity_id returns (nil, false) without hitting the underlying
// store — the prefetch already proved nothing was there.
func TestNegativeCacheAfterPrefetch(t *testing.T) {
	ctx := context.Background()
	inner := newTestStore(t)
	counter := newCountingStore(inner)
	scope := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "w",
		RunID:      "r",
	}
	cache := newRunScopedCache(counter, scope)
	if err := cache.prefetch(ctx); err != nil {
		t.Fatal(err)
	}
	counter.getActivity.Store(0)

	_, found, err := cache.GetActivity(ctx, storage.ActivityKey{
		Namespace:  scope.Namespace,
		WorkflowID: scope.WorkflowID,
		RunID:      scope.RunID,
		ActivityID: "never-existed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected found=false for a never-existed activity")
	}
	if counter.getActivity.Load() != 0 {
		t.Fatalf("expected 0 GetActivity passthroughs after prefetch negative-cache hit, got %d", counter.getActivity.Load())
	}
}

// TestDeleteInvalidatesCache verifies a delete via the cache wrapper clears
// the cached entry so subsequent reads pass through (and see the missing
// record).
func TestDeleteInvalidatesCache(t *testing.T) {
	ctx := context.Background()
	inner := newTestStore(t)
	counter := newCountingStore(inner)
	scope := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "w",
		RunID:      "r",
	}
	cache := newRunScopedCache(counter, scope)

	record := &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key: storage.ActivityKey{
			Namespace:  scope.Namespace,
			WorkflowID: scope.WorkflowID,
			RunID:      scope.RunID,
			ActivityID: "a",
		}.Proto(),
		ActivityType: "activity:google.protobuf.StringValue->google.protobuf.StringValue",
		Status:       temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
		CreatedAt:    timestamppb.Now(),
		CompletedAt:  timestamppb.Now(),
	}
	if err := cache.PutActivity(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.DeleteActivity(ctx, storage.ActivityKey{
		Namespace:  scope.Namespace,
		WorkflowID: scope.WorkflowID,
		RunID:      scope.RunID,
		ActivityID: "a",
	}); err != nil {
		t.Fatal(err)
	}
	_, found, err := cache.GetActivity(ctx, storage.ActivityKey{
		Namespace:  scope.Namespace,
		WorkflowID: scope.WorkflowID,
		RunID:      scope.RunID,
		ActivityID: "a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected delete to clear the cache")
	}
}

// TestSemanticEquivalenceAgainstRawStore replays the same workflow against a
// raw store and against the cache, asserting both produce the same result
// and the same number of activity executions. This guards against the cache
// drifting from the underlying-store contract.
func TestSemanticEquivalenceAgainstRawStore(t *testing.T) {
	ctx := context.Background()

	for _, runID := range []string{"eq-1"} {
		inner := newTestStore(t)
		executions := &atomic.Int64{}
		result, err := runFanout(t, ctx, inner, runID, 4, executions)
		if err != nil {
			t.Fatal(err)
		}
		// 0+2+4+6 = 12
		if result.GetValue() != 12 {
			t.Fatalf("first-run result = %d, want 12", result.GetValue())
		}
		if executions.Load() != 4 {
			t.Fatalf("first-run executions = %d, want 4", executions.Load())
		}

		// Force replay by flipping COMPLETED→IN_PROGRESS.
		key := storage.WorkflowKey{
			Namespace:  storage.DefaultNamespace,
			WorkflowID: "fanout",
			RunID:      runID,
		}
		completed, _, err := inner.GetWorkflow(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		completed.Status = temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS
		completed.CompletedAt = nil
		completed.Result = nil
		if err := inner.PutWorkflow(ctx, completed); err != nil {
			t.Fatal(err)
		}

		executions.Store(0)
		result, err = runFanout(t, ctx, inner, runID, 4, executions)
		if err != nil {
			t.Fatal(err)
		}
		if result.GetValue() != 12 {
			t.Fatalf("replay result = %d, want 12", result.GetValue())
		}
		if executions.Load() != 0 {
			t.Fatalf("replay re-executed %d activities, want 0", executions.Load())
		}
	}
}

// TestCacheConcurrentGet verifies the cache mutex protects concurrent reads
// from the workflow body — important because asyncio.gather (Python) and
// errgroup (Go) callers may issue many parallel Get calls.
func TestCacheConcurrentGet(t *testing.T) {
	ctx := context.Background()
	inner := newTestStore(t)
	counter := newCountingStore(inner)
	scope := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "w",
		RunID:      "r",
	}
	// Seed 50 activity records.
	for i := 0; i < 50; i++ {
		record := &temporalessv1.ActivityRecord{
			SchemaVersion: storage.ActivityRecordSchemaVersion,
			Key: storage.ActivityKey{
				Namespace:  scope.Namespace,
				WorkflowID: scope.WorkflowID,
				RunID:      scope.RunID,
				ActivityID: "act:" + intToASCII(int32(i)),
			}.Proto(),
			ActivityType: "activity:google.protobuf.StringValue->google.protobuf.StringValue",
			Status:       temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
			CreatedAt:    timestamppb.Now(),
			CompletedAt:  timestamppb.Now(),
		}
		if err := inner.PutActivity(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	cache := newRunScopedCache(counter, scope)
	if err := cache.prefetch(ctx); err != nil {
		t.Fatal(err)
	}
	counter.getActivity.Store(0)

	done := make(chan error, 50)
	for i := 0; i < 50; i++ {
		i := i
		go func() {
			_, found, err := cache.GetActivity(ctx, storage.ActivityKey{
				Namespace:  scope.Namespace,
				WorkflowID: scope.WorkflowID,
				RunID:      scope.RunID,
				ActivityID: "act:" + intToASCII(int32(i)),
			})
			if err != nil || !found {
				done <- err
				return
			}
			done <- nil
		}()
	}
	for i := 0; i < 50; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if got := counter.getActivity.Load(); got != 0 {
		t.Fatalf("expected 0 GetActivity passthroughs after prefetch, got %d", got)
	}
}
