package inspection

import (
	"context"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

// requestView returns store with every read it serves (point reads through
// Records and listings and raw reads through Bucket) passing one semaphore of
// Limits.Concurrency slots. Every RPC resolves its store once through
// storeScope, so all the reads of one request share that budget, including
// nested fan-out such as a directory page reading, for each run still in
// progress, that run's records.
func (service *Service) requestView(store *Store) *Store {
	gate := make(readGate, max(service.limits.Concurrency, 1))
	view := *store
	view.Records = gatedRecords{Store: store.Records, gate: gate}
	view.Bucket = gatedBucket{bucket: store.Bucket, gate: gate}
	return &view
}

// readGate is a counting semaphore. A slot is held only for the duration of
// one read, never while waiting on other reads, so nested fan-out cannot
// deadlock on it.
type readGate chan struct{}

func (gate readGate) acquire(ctx context.Context) error {
	select {
	case gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (gate readGate) release() { <-gate }

// gatedRecords gates the point-store reads inspection makes. Inspection
// calls only GetWorkflow and ListTimers; the embedded Store's writes are
// never called.
type gatedRecords struct {
	storage.Store
	gate readGate
}

func (records gatedRecords) GetWorkflow(ctx context.Context, key storage.WorkflowKey) (*temporalessv1.WorkflowRecord, bool, error) {
	if err := records.gate.acquire(ctx); err != nil {
		return nil, false, err
	}
	defer records.gate.release()
	return records.Store.GetWorkflow(ctx, key)
}

func (records gatedRecords) ListTimers(ctx context.Context, key storage.WorkflowKey, status temporalessv1.TimerStatus) ([]*temporalessv1.TimerRecord, error) {
	if err := records.gate.acquire(ctx); err != nil {
		return nil, err
	}
	defer records.gate.release()
	return records.Store.ListTimers(ctx, key, status)
}

type gatedBucket struct {
	bucket Bucket
	gate   readGate
}

func (bucket gatedBucket) List(ctx context.Context, dir string) ([]string, error) {
	if err := bucket.gate.acquire(ctx); err != nil {
		return nil, err
	}
	defer bucket.gate.release()
	return bucket.bucket.List(ctx, dir)
}

func (bucket gatedBucket) Read(ctx context.Context, path string) ([]byte, bool, error) {
	if err := bucket.gate.acquire(ctx); err != nil {
		return nil, false, err
	}
	defer bucket.gate.release()
	return bucket.bucket.Read(ctx, path)
}
