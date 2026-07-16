// Package background wires the periodic adapters (cron scheduler, timer
// scanner, janitor) into the workflow service process as toggleable goroutine
// loops.
//
// # Why this exists
//
// Every replica polling the bucket for cron / timer / janitor work is
// wasteful. Deployers typically want one "operator" replica running these
// loops while N "handler" replicas just serve workflow RPCs. This package
// makes that wiring tidy without adding new concepts — each loop is opt-in
// via its config struct; absence means disabled.
//
// # Why not leader election
//
// Coordination dances (lease + heartbeat) add complexity the framework
// explicitly rejects. The simpler answer: deployers configure only the
// replicas they want to run background work.
//
// # Safety net if you mis-configure
//
// If two replicas accidentally both run the same loop, the framework's replay
// model still produces correct results — the second workflow.Run short-circuits
// via stored records; query-adapter sweeps and point deletes are idempotent. The
// opt-in is purely an efficiency optimization, not a correctness one.
//
// # Typical wiring (operator replica)
//
//	workers := background.New(store, background.Config{
//	    QueryStore: queryStore,
//	    Cron: &background.CronConfig{Scheduler: myScheduler},
//	    TimerScanner: &background.TimerScannerConfig{Dispatch: dispatchDueTimer},
//	    Janitor: &background.JanitorConfig{MaxAge: 7 * 24 * time.Hour},
//	})
//	if err := workers.Start(ctx); err != nil { /* ... */ }
//	defer workers.Stop()
//
// Handler-only replica: skip this package entirely. Or construct Workers with
// an empty Config — Start becomes a no-op.
//
// For platforms with their own scheduler (Lambda + EventBridge, Cloud Run +
// Cloud Scheduler, Kubernetes CronJob), use those instead — they already
// provide the "one-fire-per-tick" semantics this package gives you in-process.
package background

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/cronscheduler"
	"github.com/jim-technologies/temporaless/adapters/go/janitor"
	"github.com/jim-technologies/temporaless/adapters/go/timerscanner"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CronConfig runs Scheduler.Tick on a loop. The scheduler is responsible for
// invoking its dispatcher per fired schedule — this config just drives the
// tick cadence.
type CronConfig struct {
	Scheduler *cronscheduler.Scheduler
	// Interval defaults to 60s when zero.
	Interval time.Duration
}

// DueTimerDispatcher is invoked once per due timer the scanner finds. Typically
// re-invokes the workflow handler so a durable workflow.Sleep resumes. Return
// cleanly; the loop logs and continues on per-timer errors so one bad workflow
// doesn't stall the whole scanner.
type DueTimerDispatcher func(ctx context.Context, timer timerscanner.DueTimer) error

// TimerScannerConfig polls timerscanner.DueTimers and invokes Dispatch for
// each.
type TimerScannerConfig struct {
	Dispatch DueTimerDispatcher
	// Interval defaults to 60s when zero.
	Interval time.Duration
	// Namespace limits timer discovery. Empty means all namespaces.
	Namespace string
}

// JanitorConfig periodically sweeps COMPLETED runs older than MaxAge.
type JanitorConfig struct {
	MaxAge time.Duration
	// Interval defaults to 24h when zero.
	Interval time.Duration
	// Namespace limits retention candidates. Empty means all namespaces.
	Namespace string
	// ClaimStore is the optional separately configured claim backend. Nil lets
	// janitor.Sweep auto-detect claim support from the record store.
	ClaimStore storage.ClaimStore
}

// Config selects which background loops this replica runs. Nil entries are
// disabled.
type Config struct {
	Cron         *CronConfig
	TimerScanner *TimerScannerConfig
	Janitor      *JanitorConfig
	// QueryStore is required when Janitor is configured. Core bucket stores do
	// not expose cross-run retention candidates.
	QueryStore storage.WorkflowQueryStore

	// Logger is used for transient per-iteration errors that shouldn't kill
	// the worker. Defaults to slog.Default() when nil.
	Logger *slog.Logger
}

// Workers is the container for opt-in background loops. Construct with New
// and call Start/Stop on the service lifecycle.
type Workers struct {
	store  storage.Store
	query  storage.WorkflowQueryStore
	cfg    Config
	logger *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   bool
}

// New validates the config and returns a ready-to-Start Workers.
func New(store storage.Store, cfg Config) (*Workers, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if cfg.Cron != nil {
		if cfg.Cron.Scheduler == nil {
			return nil, fmt.Errorf("cron.Scheduler is required when cron is configured")
		}
		if cfg.Cron.Interval < 0 {
			return nil, fmt.Errorf("cron.Interval must be >= 0")
		}
	}
	if cfg.TimerScanner != nil {
		if cfg.TimerScanner.Dispatch == nil {
			return nil, fmt.Errorf("timer_scanner.Dispatch is required when timer_scanner is configured")
		}
		if cfg.TimerScanner.Interval < 0 {
			return nil, fmt.Errorf("timer_scanner.Interval must be >= 0")
		}
	}
	if cfg.Janitor != nil {
		if cfg.Janitor.MaxAge <= 0 {
			return nil, fmt.Errorf("janitor.MaxAge must be > 0")
		}
		if cfg.Janitor.Interval < 0 {
			return nil, fmt.Errorf("janitor.Interval must be >= 0")
		}
		if cfg.QueryStore == nil {
			return nil, fmt.Errorf("QueryStore is required when janitor is configured")
		}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Workers{store: store, query: cfg.QueryStore, cfg: cfg, logger: logger}, nil
}

// Start spawns goroutines for each enabled loop. Returns immediately; the
// loops run until Stop is called or the supplied ctx is cancelled. Calling
// Start more than once returns an error so accidental double-start is loud.
func (w *Workers) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		return errors.New("background workers already started")
	}
	if w.done {
		return errors.New("background workers already stopped; create a new Workers to restart")
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	if w.cfg.Cron != nil {
		w.wg.Add(1)
		go w.runCron(runCtx, w.cfg.Cron)
	}
	if w.cfg.TimerScanner != nil {
		w.wg.Add(1)
		go w.runTimerScanner(runCtx, w.cfg.TimerScanner)
	}
	if w.cfg.Janitor != nil {
		w.wg.Add(1)
		go w.runJanitor(runCtx, w.cfg.Janitor)
	}
	return nil
}

// Stop signals cancellation and waits for all loops to exit. Safe to call
// when never started.
func (w *Workers) Stop() {
	w.mu.Lock()
	if w.cancel == nil {
		w.mu.Unlock()
		return
	}
	cancel := w.cancel
	w.cancel = nil
	w.done = true
	w.mu.Unlock()
	cancel()
	w.wg.Wait()
}

func (w *Workers) runCron(ctx context.Context, cfg *CronConfig) {
	defer w.wg.Done()
	interval := cfg.Interval
	if interval == 0 {
		interval = 60 * time.Second
	}
	w.loop(ctx, "cron", interval, func(ctx context.Context) error {
		_, err := cfg.Scheduler.Tick(ctx, time.Now().UTC())
		return err
	})
}

func (w *Workers) runTimerScanner(ctx context.Context, cfg *TimerScannerConfig) {
	defer w.wg.Done()
	interval := cfg.Interval
	if interval == 0 {
		interval = 60 * time.Second
	}
	w.loop(ctx, "timer_scanner", interval, func(ctx context.Context) error {
		due, err := timerscanner.DueTimers(ctx, w.store, time.Now().UTC(), cfg.Namespace)
		if err != nil {
			return err
		}
		for _, timer := range due {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := cfg.Dispatch(ctx, timer); err != nil {
				// One bad workflow shouldn't stall the scanner.
				w.logger.Error("timer_scanner dispatch failed",
					"namespace", timer.Key.Namespace,
					"workflow_id", timer.Key.WorkflowID,
					"run_id", timer.Key.RunID,
					"timer_id", timer.Key.TimerID,
					"error", err,
				)
			}
		}
		return nil
	})
}

func (w *Workers) runJanitor(ctx context.Context, cfg *JanitorConfig) {
	defer w.wg.Done()
	interval := cfg.Interval
	if interval == 0 {
		interval = 24 * time.Hour
	}
	w.loop(ctx, "janitor", interval, func(ctx context.Context) error {
		_, err := janitor.Sweep(ctx, w.query, w.store, cfg.ClaimStore, &temporalessv1.SweepRequest{
			Namespace: cfg.Namespace,
			Now:       timestamppb.New(time.Now().UTC()),
			MaxAge:    durationpb.New(cfg.MaxAge),
		})
		return err
	})
}

func (w *Workers) loop(
	ctx context.Context,
	name string,
	interval time.Duration,
	body func(context.Context) error,
) {
	timer := time.NewTimer(0) // fire immediately on start
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := body(ctx); err != nil && !errors.Is(err, context.Canceled) {
				// Log and keep looping — a transient store error shouldn't
				// kill the worker for the rest of the deployment.
				w.logger.Error("background loop iteration failed", "loop", name, "error", err)
			}
			timer.Reset(interval)
		}
	}
}
