// Package backfill runs a workflow over many run_ids with bounded concurrency
// and aggregates per-run status — the Dagster/Prefect/Airflow backfill
// primitive.
//
// Backfill is idempotent: already-COMPLETED runs replay from storage in
// microseconds; already-FAILED runs re-execute (call inspector.ResetWorkflow
// first to clear them); IN_PROGRESS runs are reported as Pending and need a
// scanner / re-invoke to resume. Re-running Backfill over the same set is
// free for COMPLETED runs.
//
// Usage:
//
//	report, err := backfill.Backfill(ctx, runIDs, backfill.Options{
//	    Concurrency: 5,
//	}, func(ctx context.Context, runID string) (*pricesv1.FetchResponse, error) {
//	    return service.FetchPrices(ctx, connect.NewRequest(&pricesv1.FetchRequest{
//	        Symbol: "AAPL", RunId: runID,
//	    })).Msg, nil
//	})
package backfill

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"connectrpc.com/connect"
	"github.com/jim-technologies/temporaless/core/go/workflow"
)

// Status of a single backfill entry.
type Status int

const (
	StatusUnspecified Status = iota
	StatusSucceeded
	StatusFailed
	// StatusPending means the workflow stayed IN_PROGRESS — a typed
	// pending error was returned, mapped to connect.CodeUnavailable, or
	// halt-on-error skipped this run_id.
	StatusPending
)

func (s Status) String() string {
	switch s {
	case StatusSucceeded:
		return "succeeded"
	case StatusFailed:
		return "failed"
	case StatusPending:
		return "pending"
	default:
		return "unspecified"
	}
}

// Entry is the result for one run_id.
type Entry[Resp any] struct {
	RunID  string
	Status Status
	Result Resp
	Err    error
}

// Report is the aggregate result of a Backfill call.
type Report[Resp any] struct {
	Entries []Entry[Resp]
}

// Succeeded returns the entries that completed cleanly.
func (r *Report[Resp]) Succeeded() []Entry[Resp] {
	return r.filter(StatusSucceeded)
}

// Failed returns the entries that ended with a non-pending error.
func (r *Report[Resp]) Failed() []Entry[Resp] {
	return r.filter(StatusFailed)
}

// Pending returns the entries that need to be re-driven (IN_PROGRESS).
func (r *Report[Resp]) Pending() []Entry[Resp] {
	return r.filter(StatusPending)
}

func (r *Report[Resp]) filter(status Status) []Entry[Resp] {
	out := make([]Entry[Resp], 0, len(r.Entries))
	for _, e := range r.Entries {
		if e.Status == status {
			out = append(out, e)
		}
	}
	return out
}

func (r *Report[Resp]) String() string {
	return fmt.Sprintf(
		"Report(succeeded=%d, failed=%d, pending=%d, total=%d)",
		len(r.Succeeded()),
		len(r.Failed()),
		len(r.Pending()),
		len(r.Entries),
	)
}

// Options configures a Backfill call.
type Options struct {
	// Concurrency is the maximum simultaneous in-flight invocations.
	// Zero means 1.
	Concurrency int
	// HaltOnError stops dispatching new run_ids after the first failure.
	// Already-running invocations finish; un-dispatched ones are reported
	// as Pending.
	HaltOnError bool
}

// Invoke is the user-supplied function the backfill loop calls per run_id.
type Invoke[Resp any] func(ctx context.Context, runID string) (Resp, error)

// Backfill runs invoke once per run_id with bounded concurrency and aggregates
// the results into a Report. Per-run errors are independent unless
// opts.HaltOnError is set.
//
// Workflow runs that stay IN_PROGRESS (TimerPendingError, EventPendingError,
// WorkflowDependencyPendingError, or their connect.CodeUnavailable mapped
// form) are reported as Pending — they aren't failures, they need a scanner
// to re-invoke them.
func Backfill[Resp any](
	ctx context.Context,
	runIDs []string,
	opts Options,
	invoke Invoke[Resp],
) (*Report[Resp], error) {
	if invoke == nil {
		return nil, errors.New("invoke is required")
	}
	concurrency := opts.Concurrency
	if concurrency == 0 {
		concurrency = 1
	}
	if concurrency < 1 {
		return nil, errors.New("concurrency must be >= 1")
	}

	entries := make([]Entry[Resp], len(runIDs))
	sem := make(chan struct{}, concurrency)
	halt := make(chan struct{})
	var haltOnce sync.Once
	closeHalt := func() { haltOnce.Do(func() { close(halt) }) }

	var wg sync.WaitGroup
	for i, runID := range runIDs {
		wg.Add(1)
		go func(idx int, rid string) {
			defer wg.Done()
			select {
			case <-halt:
				entries[idx] = Entry[Resp]{RunID: rid, Status: StatusPending}
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()

			select {
			case <-halt:
				entries[idx] = Entry[Resp]{RunID: rid, Status: StatusPending}
				return
			default:
			}

			result, err := invoke(ctx, rid)
			if err == nil {
				entries[idx] = Entry[Resp]{RunID: rid, Status: StatusSucceeded, Result: result}
				return
			}
			if isPendingError(err) {
				entries[idx] = Entry[Resp]{RunID: rid, Status: StatusPending, Err: err}
				return
			}
			if opts.HaltOnError {
				closeHalt()
			}
			entries[idx] = Entry[Resp]{RunID: rid, Status: StatusFailed, Err: err}
		}(i, runID)
	}
	wg.Wait()
	return &Report[Resp]{Entries: entries}, nil
}

func isPendingError(err error) bool {
	var timerPending *workflow.TimerPendingError
	if errors.As(err, &timerPending) {
		return true
	}
	var eventPending *workflow.EventPendingError
	if errors.As(err, &eventPending) {
		return true
	}
	var depPending *workflow.WorkflowDependencyPendingError
	if errors.As(err, &depPending) {
		return true
	}
	var connectErr *connect.Error
	if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeUnavailable {
		return true
	}
	return false
}
