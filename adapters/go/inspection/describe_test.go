package inspection_test

import (
	"context"
	"testing"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDescribeRunPendingReasons(t *testing.T) {
	running := temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS
	scheduled := temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED
	tests := []struct {
		name         string
		setup        func(f *fixture, run storage.WorkflowKey)
		wantReason   inspectionv1.RunPendingReason
		wantResource string
		wantAt       time.Duration
	}{
		{
			name: "completed run is terminal",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, -10*time.Minute, duration(-5*time.Minute))
			},
			wantReason: inspectionv1.RunPendingReason_RUN_PENDING_REASON_TERMINAL,
		},
		{
			name: "failed run is terminal",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED, -10*time.Minute, duration(-5*time.Minute))
			},
			wantReason: inspectionv1.RunPendingReason_RUN_PENDING_REASON_TERMINAL,
		},
		{
			name: "timer past the overdue grace wins over a retry",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, running, -30*time.Minute, nil)
				f.timer(run, "sleep:backoff", temporalessv1.TimerKind_TIMER_KIND_SLEEP, scheduled, -44*time.Minute)
				f.activity(run, "fetch:page-3", temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
					[]*temporalessv1.ActivityAttempt{attempt(1, -3*time.Minute, -2*time.Minute, "rate_limited")}, at(30*time.Second))
			},
			wantReason:   inspectionv1.RunPendingReason_RUN_PENDING_REASON_OVERDUE_WAKE,
			wantResource: "sleep:backoff",
			wantAt:       -44 * time.Minute,
		},
		{
			name: "timer inside the grace is not overdue",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, running, -30*time.Minute, nil)
				f.timer(run, "sleep:backoff", temporalessv1.TimerKind_TIMER_KIND_SLEEP, scheduled, -30*time.Second)
			},
			wantReason:   inspectionv1.RunPendingReason_RUN_PENDING_REASON_SLEEPING,
			wantResource: "sleep:backoff",
			wantAt:       -30 * time.Second,
		},
		{
			name: "retrying activity",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, running, -14*time.Minute, nil)
				f.activity(run, "fetch:page-3", temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING, []*temporalessv1.ActivityAttempt{
					attempt(1, -10*time.Minute, -9*time.Minute, "rate_limited"),
					attempt(2, -5*time.Minute, -4*time.Minute, "rate_limited"),
				}, at(30*time.Second))
				f.timer(run, "retry:fetch:page-3", temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY, scheduled, 30*time.Second)
			},
			wantReason:   inspectionv1.RunPendingReason_RUN_PENDING_REASON_RETRYING,
			wantResource: "fetch:page-3",
			wantAt:       30 * time.Second,
		},
		{
			name: "retry timer without its activity record still evidences a retry",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, running, -14*time.Minute, nil)
				f.timer(run, "retry:fetch:page-3", temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY, scheduled, 30*time.Second)
			},
			wantReason:   inspectionv1.RunPendingReason_RUN_PENDING_REASON_RETRYING,
			wantResource: "fetch:page-3",
			wantAt:       30 * time.Second,
		},
		{
			name: "claim past its lease is stale",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, running, -30*time.Minute, nil)
				f.claim(run, "workflow", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW, -time.Minute)
			},
			wantReason:   inspectionv1.RunPendingReason_RUN_PENDING_REASON_STALE_CLAIM,
			wantResource: "workflow",
			wantAt:       -time.Minute,
		},
		{
			name: "unexpired workflow claim is executing",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, running, -time.Minute, nil)
				f.claim(run, "workflow", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW, 4*time.Minute)
				f.timer(run, "sleep:later", temporalessv1.TimerKind_TIMER_KIND_SLEEP, scheduled, time.Hour)
			},
			wantReason:   inspectionv1.RunPendingReason_RUN_PENDING_REASON_EXECUTING,
			wantResource: "workflow",
			wantAt:       4 * time.Minute,
		},
		{
			name: "future sleep",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, running, -time.Minute, nil)
				f.timer(run, "sleep:until-open", temporalessv1.TimerKind_TIMER_KIND_SLEEP, scheduled, 86*time.Minute)
			},
			wantReason:   inspectionv1.RunPendingReason_RUN_PENDING_REASON_SLEEPING,
			wantResource: "sleep:until-open",
			wantAt:       86 * time.Minute,
		},
		{
			name: "poll timer",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, running, -time.Minute, nil)
				f.timer(run, "poll:approval", temporalessv1.TimerKind_TIMER_KIND_POLL, scheduled, 5*time.Minute)
			},
			wantReason:   inspectionv1.RunPendingReason_RUN_PENDING_REASON_POLLING,
			wantResource: "poll:approval",
			wantAt:       5 * time.Minute,
		},
		{
			name: "fired timers leave the run waiting with no wake",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.workflow(run, running, -time.Hour, nil)
				f.timer(run, "sleep:done", temporalessv1.TimerKind_TIMER_KIND_SLEEP, temporalessv1.TimerStatus_TIMER_STATUS_FIRED, -30*time.Minute)
			},
			wantReason: inspectionv1.RunPendingReason_RUN_PENDING_REASON_WAITING_NO_WAKE,
		},
		{
			name: "run without a workflow record",
			setup: func(f *fixture, run storage.WorkflowKey) {
				f.activity(run, "orphan", temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
					[]*temporalessv1.ActivityAttempt{attempt(1, -2*time.Minute, -time.Minute, "")}, nil)
			},
			wantReason: inspectionv1.RunPendingReason_RUN_PENDING_REASON_UNSPECIFIED,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			run := key("pull:weather", "20260925T080000")
			test.setup(f, run)
			before := f.snapshot()

			response, err := f.service(inspection.Options{}).DescribeRun(context.Background(), &inspectionv1.DescribeRunRequest{
				Store: "engine",
				Key:   run.Proto(),
			})
			if err != nil {
				t.Fatal(err)
			}
			pending := response.GetPending()
			if pending.GetReason() != test.wantReason {
				t.Fatalf("reason = %s, want %s", pending.GetReason(), test.wantReason)
			}
			if pending.GetResourceId() != test.wantResource {
				t.Fatalf("resource = %q, want %q", pending.GetResourceId(), test.wantResource)
			}
			if test.wantResource != "" && !pending.GetAt().AsTime().Equal(now.Add(test.wantAt)) {
				t.Fatalf("at = %s, want %s", pending.GetAt().AsTime(), now.Add(test.wantAt))
			}
			assertUnchanged(t, before, f.snapshot())
		})
	}
}

func TestDescribeRunRetryDetail(t *testing.T) {
	f := newFixture(t)
	run := key("pull:weather", "20260925T080000")
	f.workflow(run, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, -14*time.Minute, nil)
	f.activity(run, "fetch:page-3", temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING, []*temporalessv1.ActivityAttempt{
		attempt(1, -10*time.Minute, -9*time.Minute, "upstream_5xx"),
		attempt(2, -5*time.Minute, -4*time.Minute, "rate_limited"),
	}, at(30*time.Second))

	response, err := f.service(inspection.Options{}).DescribeRun(context.Background(), &inspectionv1.DescribeRunRequest{Store: "engine", Key: run.Proto()})
	if err != nil {
		t.Fatal(err)
	}
	pending := response.GetPending()
	if pending.GetAttempt() != 2 || pending.GetMaximumAttempts() != 6 || pending.GetLastFailure().GetCode() != "rate_limited" {
		t.Fatalf("retry detail = attempt %d of %d, last failure %q", pending.GetAttempt(), pending.GetMaximumAttempts(), pending.GetLastFailure().GetCode())
	}
}

func TestDescribeRunHistory(t *testing.T) {
	f := newFixture(t)
	run := key("pull:weather", "20260925T080000")
	f.workflow(run, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, -14*time.Minute, nil)
	f.activity(run, "resolve:series", temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
		[]*temporalessv1.ActivityAttempt{attempt(1, -13*time.Minute, -12*time.Minute, "")}, nil)
	f.activity(run, "fetch:page-3", temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
		[]*temporalessv1.ActivityAttempt{attempt(1, -10*time.Minute, -9*time.Minute, "rate_limited")}, at(30*time.Second))
	f.timer(run, "sleep:warmup", temporalessv1.TimerKind_TIMER_KIND_SLEEP, temporalessv1.TimerStatus_TIMER_STATUS_FIRED, -11*time.Minute)
	f.event(run, "approval", -8*time.Minute)
	f.claim(run, "workflow", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW, 4*time.Minute)

	response, err := f.service(inspection.Options{}).DescribeRun(context.Background(), &inspectionv1.DescribeRunRequest{Store: "engine", Key: run.Proto()})
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		id   string
		kind inspectionv1.RunHistoryEventKind
	}
	var got []row
	for _, event := range response.GetHistory() {
		got = append(got, row{event.GetEventId(), event.GetKind()})
	}
	want := []row{
		{"workflow/started", inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_WORKFLOW_STARTED},
		{"activity/resolve:series/attempt/1", inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_SUCCEEDED},
		{"activity/resolve:series/completed", inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_COMPLETED},
		{"timer/sleep:warmup/fired", inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_TIMER_FIRED},
		{"activity/fetch:page-3/attempt/1", inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_FAILED},
		{"activity/fetch:page-3/retry-backoff/1", inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_RETRY_BACKOFF},
		{"event/approval/received", inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_EVENT_RECEIVED},
		{"claim/workflow/held", inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_CLAIM_HELD},
	}
	// The fired timer's scheduled span starts at created_at (-15m), before the
	// workflow started in this fixture, so it leads the history.
	want = append([]row{{"timer/sleep:warmup/scheduled", inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_TIMER_SCHEDULED}}, want...)
	if len(got) != len(want) {
		t.Fatalf("history = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("history[%d] = %v, want %v (all: %v)", index, got[index], want[index], got)
		}
	}
	backoff := response.GetHistory()[6]
	if !backoff.GetTime().AsTime().Equal(now.Add(-9*time.Minute)) || !backoff.GetUntil().AsTime().Equal(now.Add(30*time.Second)) {
		t.Fatalf("backoff span = %s..%s", backoff.GetTime().AsTime(), backoff.GetUntil().AsTime())
	}
	if !response.GetClaimsInspected() || len(response.GetClaims()) != 1 {
		t.Fatalf("claims inspected=%t count=%d", response.GetClaimsInspected(), len(response.GetClaims()))
	}
}

func TestDescribeRunWithoutClaimListing(t *testing.T) {
	f := newFixture(t)
	f.store.ListClaims = false
	run := key("pull:weather", "20260925T080000")
	f.workflow(run, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, -time.Minute, nil)
	f.claim(run, "workflow", temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW, -time.Minute)

	response, err := f.service(inspection.Options{}).DescribeRun(context.Background(), &inspectionv1.DescribeRunRequest{Store: "engine", Key: run.Proto()})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetClaimsInspected() || len(response.GetClaims()) != 0 {
		t.Fatalf("claims inspected=%t count=%d, want not inspected", response.GetClaimsInspected(), len(response.GetClaims()))
	}
	// A stale claim that was never read cannot be the pending reason.
	if response.GetPending().GetReason() != inspectionv1.RunPendingReason_RUN_PENDING_REASON_WAITING_NO_WAKE {
		t.Fatalf("reason = %s", response.GetPending().GetReason())
	}
}

func TestDescribeRunTruncatesLargeRuns(t *testing.T) {
	f := newFixture(t)
	run := key("backfill:equities", "2026-09-01..2026-09-24")
	f.workflow(run, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, -time.Hour, duration(-time.Minute))
	for _, id := range []string{"day:01", "day:02", "day:03"} {
		f.activity(run, id, temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
			[]*temporalessv1.ActivityAttempt{attempt(1, -50*time.Minute, -49*time.Minute, "")}, nil)
	}
	limits := inspection.DefaultLimits()
	limits.MaxRunRecords = 2
	response, err := f.service(inspection.Options{Limits: limits}).DescribeRun(context.Background(), &inspectionv1.DescribeRunRequest{Store: "engine", Key: run.Proto()})
	if err != nil {
		t.Fatal(err)
	}
	if !response.GetTruncated() || len(response.GetActivities()) != 2 {
		t.Fatalf("truncated=%t activities=%d, want true and 2", response.GetTruncated(), len(response.GetActivities()))
	}
}

func TestDescribeRunErrors(t *testing.T) {
	f := newFixture(t)
	f.workflow(key("pull:weather", "r1"), temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, -time.Hour, duration(-time.Minute))
	f.write("temporaless/v2/default/pull:weather/r2/activity/fetch.binpb", []byte("not a protobuf record"))
	service := f.service(inspection.Options{})
	tests := []struct {
		name    string
		request *inspectionv1.DescribeRunRequest
		want    codes.Code
	}{
		{"missing run", &inspectionv1.DescribeRunRequest{Store: "engine", Key: key("pull:weather", "absent").Proto()}, codes.NotFound},
		{"unknown store", &inspectionv1.DescribeRunRequest{Store: "other", Key: key("pull:weather", "r1").Proto()}, codes.NotFound},
		{"namespace outside the allowlist", &inspectionv1.DescribeRunRequest{Store: "engine", Key: &temporalessv1.WorkflowKey{Namespace: "tenant-b", WorkflowId: "pull:weather", RunId: "r1"}}, codes.PermissionDenied},
		{"empty namespace", &inspectionv1.DescribeRunRequest{Store: "engine", Key: &temporalessv1.WorkflowKey{WorkflowId: "pull:weather", RunId: "r1"}}, codes.InvalidArgument},
		{"path-shaped run id", &inspectionv1.DescribeRunRequest{Store: "engine", Key: &temporalessv1.WorkflowKey{Namespace: "default", WorkflowId: "pull:weather", RunId: "../r1"}}, codes.InvalidArgument},
		{"corrupt child record", &inspectionv1.DescribeRunRequest{Store: "engine", Key: key("pull:weather", "r2").Proto()}, codes.DataLoss},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := service.DescribeRun(context.Background(), test.request)
			if got := status.Code(err); got != test.want {
				t.Fatalf("code = %s (%v), want %s", got, err, test.want)
			}
		})
	}
}

func assertUnchanged(t *testing.T, before, after map[string][32]byte) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("inspection changed the store: %d files before, %d after", len(before), len(after))
	}
	for path, digest := range before {
		if after[path] != digest {
			t.Fatalf("inspection changed %s", path)
		}
	}
}
