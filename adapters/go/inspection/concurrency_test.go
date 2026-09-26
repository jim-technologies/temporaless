package inspection_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

// gauge records the most store reads in flight at once. Each read holds its
// slot briefly so reads that may overlap do.
type gauge struct {
	mu       sync.Mutex
	inFlight int
	peak     int
}

func (g *gauge) enter() {
	g.mu.Lock()
	g.inFlight++
	g.peak = max(g.peak, g.inFlight)
	g.mu.Unlock()
	time.Sleep(2 * time.Millisecond)
}

func (g *gauge) leave() {
	g.mu.Lock()
	g.inFlight--
	g.mu.Unlock()
}

type gaugedBucket struct {
	inspection.Bucket
	gauge *gauge
}

func (bucket gaugedBucket) List(ctx context.Context, dir string) ([]string, error) {
	bucket.gauge.enter()
	defer bucket.gauge.leave()
	return bucket.Bucket.List(ctx, dir)
}

func (bucket gaugedBucket) Read(ctx context.Context, path string) ([]byte, bool, error) {
	bucket.gauge.enter()
	defer bucket.gauge.leave()
	return bucket.Bucket.Read(ctx, path)
}

type gaugedRecords struct {
	storage.Store
	gauge *gauge
}

func (records gaugedRecords) GetWorkflow(ctx context.Context, key storage.WorkflowKey) (*temporalessv1.WorkflowRecord, bool, error) {
	records.gauge.enter()
	defer records.gauge.leave()
	return records.Store.GetWorkflow(ctx, key)
}

func (records gaugedRecords) ListTimers(ctx context.Context, key storage.WorkflowKey, status temporalessv1.TimerStatus) ([]*temporalessv1.TimerRecord, error) {
	records.gauge.enter()
	defer records.gauge.leave()
	return records.Store.ListTimers(ctx, key, status)
}

// TestConcurrencyIsOneBudgetPerRequest proves Limits.Concurrency caps the
// store reads of one request, including nested ones: a directory page reads
// its pointers in parallel and, for each run still in progress, that run's
// records in parallel too.
func TestConcurrencyIsOneBudgetPerRequest(t *testing.T) {
	f := newFixture(t)
	for index := range 6 {
		run := key(fmt.Sprintf("pull:feed-%d", index), "20260925T080000")
		f.workflow(run, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, -10*time.Minute, nil)
		for page := range 4 {
			f.activity(run, fmt.Sprintf("fetch:page-%d", page), temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
				[]*temporalessv1.ActivityAttempt{attempt(1, -9*time.Minute, -8*time.Minute, "")}, nil)
		}
	}
	reads := &gauge{}
	f.store.Bucket = gaugedBucket{Bucket: f.store.Bucket, gauge: reads}
	f.store.Records = gaugedRecords{Store: f.records, gauge: reads}
	limits := inspection.DefaultLimits()
	limits.Concurrency = 2
	service := f.service(inspection.Options{Limits: limits})

	calls := []struct {
		name string
		call func() error
	}{
		{"directory with in-progress runs", func() error {
			response, err := service.ListWorkflowDirectory(context.Background(), &inspectionv1.ListWorkflowDirectoryRequest{Store: "engine", Namespace: "default"})
			if err == nil && len(response.GetEntries()) != 6 {
				err = fmt.Errorf("entries = %d, want 6", len(response.GetEntries()))
			}
			return err
		}},
		{"describe run", func() error {
			_, err := service.DescribeRun(context.Background(), &inspectionv1.DescribeRunRequest{Store: "engine", Key: key("pull:feed-0", "20260925T080000").Proto()})
			return err
		}},
	}
	for _, test := range calls {
		t.Run(test.name, func(t *testing.T) {
			*reads = gauge{}
			if err := test.call(); err != nil {
				t.Fatal(err)
			}
			if reads.peak > limits.Concurrency {
				t.Fatalf("%d store reads ran at once, want at most Limits.Concurrency = %d", reads.peak, limits.Concurrency)
			}
			if reads.peak < 2 {
				t.Fatalf("peak = %d: the reads never overlapped, so the bound was not exercised", reads.peak)
			}
		})
	}
}
