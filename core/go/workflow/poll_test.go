package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func pollOptions(timerID string, interval time.Duration) *PollOptions {
	return &PollOptions{
		TimerId:  timerID,
		Interval: durationpb.New(interval),
	}
}

func waitForApproval(
	options *PollOptions,
) WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue] {
	return func(
		ctx context.Context,
		_ *wrapperspb.StringValue,
	) (*wrapperspb.StringValue, error) {
		return WaitEvent(
			ctx,
			"approval",
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			options,
		)
	}
}

func TestWaitEventWithoutPollRemainsManual(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{
		WorkflowId: "event-manual",
		RunId:      "run",
	}

	_, err := Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		waitForApproval(nil),
	)
	var pending *EventPendingError
	if !errors.As(err, &pending) || !pending.WakeAt.IsZero() {
		t.Fatalf("error = %#v, want manual EventPendingError without wake_at", err)
	}
	timers, err := store.ListTimers(
		ctx,
		storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
		temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(timers) != 0 {
		t.Fatalf("manual wait wrote %d timers, want none", len(timers))
	}
}

func TestWaitEventPollSchedulesReusesAndRearmsDueTimer(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	runOptions := &Options{
		WorkflowId: "event-poll",
		RunId:      "run",
	}
	poll := pollOptions("poll:approval", time.Hour)
	runOnce := func() *EventPendingError {
		t.Helper()
		_, err := Run(
			ctx,
			store,
			runOptions,
			nil,
			wrapperspb.String("request"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			waitForApproval(poll),
		)
		var pending *EventPendingError
		if !errors.As(err, &pending) {
			t.Fatalf("error = %v, want EventPendingError", err)
		}
		return pending
	}

	first := runOnce()
	if first.WakeAt.IsZero() {
		t.Fatal("polling pending error has no wake_at")
	}
	key := storage.NewTimerKey(
		runOptions.GetWorkflowId(),
		runOptions.GetRunId(),
		poll.GetTimerId(),
	)
	record, found, err := store.GetTimer(ctx, key)
	if err != nil || !found {
		t.Fatalf("timer found=%v err=%v", found, err)
	}
	if record.GetTimerKind() != storage.PollTimerKind ||
		record.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		t.Fatalf("timer kind/status = %s/%s", record.GetTimerKind(), record.GetStatus())
	}
	firstFireAt := record.GetFireAt().AsTime()
	dueTimers, err := store.DueTimers(ctx, "", firstFireAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(dueTimers) != 1 || dueTimers[0].Key != key ||
		dueTimers[0].Workflow.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		t.Fatalf("due poll timers = %+v, want poll timer with IN_PROGRESS parent", dueTimers)
	}

	second := runOnce()
	if !second.WakeAt.Equal(first.WakeAt) {
		t.Fatalf("future poll changed wake_at from %s to %s", first.WakeAt, second.WakeAt)
	}
	record, found, err = store.GetTimer(ctx, key)
	if err != nil || !found || !record.GetFireAt().AsTime().Equal(firstFireAt) {
		t.Fatalf("future poll timer was not reused: found=%v err=%v record=%v", found, err, record)
	}

	due := record
	due.FireAt = timestamppb.New(time.Now().Add(-time.Minute))
	if err := store.PutTimer(ctx, due); err != nil {
		t.Fatal(err)
	}
	rearmStarted := time.Now()
	third := runOnce()
	if !third.WakeAt.After(rearmStarted) {
		t.Fatalf("rearmed wake_at = %s, want after %s", third.WakeAt, rearmStarted)
	}
	record, found, err = store.GetTimer(ctx, key)
	if err != nil || !found {
		t.Fatalf("rearmed timer found=%v err=%v", found, err)
	}
	if record.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED ||
		record.GetFiredAt() != nil ||
		!record.GetFireAt().AsTime().Equal(third.WakeAt) {
		t.Fatalf("rearmed timer = %v", record)
	}
}

func TestResolvedPollRetainsCrashWakeThenTerminalAcknowledges(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	runOptions := &Options{
		WorkflowId: "event-poll-resolve",
		RunId:      "run",
	}
	poll := pollOptions("poll:approval", time.Hour)

	_, err := Run(
		ctx,
		store,
		runOptions,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		waitForApproval(poll),
	)
	if !errors.Is(err, ErrEventPending) {
		t.Fatalf("first run error = %v, want ErrEventPending", err)
	}
	eventPayload, err := anypb.New(wrapperspb.String("approved"))
	if err != nil {
		t.Fatal(err)
	}
	eventKey := storage.NewEventKey(
		runOptions.GetWorkflowId(),
		runOptions.GetRunId(),
		"approval",
	)
	if err := store.PutEvent(ctx, &temporalessv1.EventRecord{
		SchemaVersion: storage.EventRecordSchemaVersion,
		Key:           eventKey.Proto(),
		Payload:       eventPayload,
		ReceivedAt:    timestamppb.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	crashAfterResolve := func(
		ctx context.Context,
		_ *wrapperspb.StringValue,
	) (*wrapperspb.StringValue, error) {
		if _, err := WaitEvent(
			ctx,
			"approval",
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			poll,
		); err != nil {
			return nil, err
		}
		return nil, context.Canceled
	}
	_, err = Run(
		ctx,
		store,
		runOptions,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		crashAfterResolve,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("crash run error = %v, want context.Canceled", err)
	}
	timerKey := storage.NewTimerKey(
		runOptions.GetWorkflowId(),
		runOptions.GetRunId(),
		poll.GetTimerId(),
	)
	timer, found, err := store.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("crash wake found=%v err=%v", found, err)
	}
	if timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		t.Fatalf("crash consumed timer status = %s, want SCHEDULED", timer.GetStatus())
	}

	result, err := Run(
		ctx,
		store,
		runOptions,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		waitForApproval(poll),
	)
	if err != nil || result.GetValue() != "approved" {
		t.Fatalf("terminal run result=%v err=%v", result, err)
	}
	timer, found, err = store.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("acknowledged timer found=%v err=%v", found, err)
	}
	if timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_FIRED ||
		timer.GetFiredAt() == nil {
		t.Fatalf("terminal timer = %v, want FIRED with fired_at", timer)
	}
}

func TestWaitEventPollRejectsTimerCollisionAndDrift(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		runID   string
		prepare func(*temporalessv1.TimerRecord)
		poll    *PollOptions
	}{
		{
			name:  "kind collision",
			runID: "kind-collision",
			prepare: func(record *temporalessv1.TimerRecord) {
				record.TimerKind = storage.SleepTimerKind
			},
			poll: pollOptions("poll:approval", time.Hour),
		},
		{
			name:    "interval drift",
			runID:   "interval-drift",
			prepare: func(*temporalessv1.TimerRecord) {},
			poll:    pollOptions("poll:approval", 2*time.Hour),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			runOptions := &Options{
				WorkflowId: "event-poll-conflict",
				RunId:      test.runID,
			}
			originalPoll := pollOptions("poll:approval", time.Hour)
			_, err := Run(
				ctx,
				store,
				runOptions,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				waitForApproval(originalPoll),
			)
			if !errors.Is(err, ErrEventPending) {
				t.Fatalf("seed error = %v", err)
			}
			timerKey := storage.NewTimerKey(
				runOptions.GetWorkflowId(),
				runOptions.GetRunId(),
				originalPoll.GetTimerId(),
			)
			record, found, err := store.GetTimer(ctx, timerKey)
			if err != nil || !found {
				t.Fatalf("seed timer found=%v err=%v", found, err)
			}
			test.prepare(record)
			if err := store.PutTimer(ctx, record); err != nil {
				t.Fatal(err)
			}
			_, err = Run(
				ctx,
				store,
				runOptions,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				waitForApproval(test.poll),
			)
			if !errors.Is(err, ErrTimerConflict) {
				t.Fatalf("error = %v, want ErrTimerConflict", err)
			}
		})
	}
}

func TestWaitEventPollRejectsOutOfRangeDurationAndNilFactoryBeforeMutation(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		body WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue]
	}{
		{
			name: "out-of-range duration",
			body: waitForApproval(&PollOptions{
				TimerId: "poll:approval",
				Interval: &durationpb.Duration{
					Seconds: 315_576_000_000,
				},
			}),
		},
		{
			name: "typed nil payload factory",
			body: func(
				ctx context.Context,
				_ *wrapperspb.StringValue,
			) (*wrapperspb.StringValue, error) {
				var payload *wrapperspb.StringValue
				return WaitEvent(
					ctx,
					"approval",
					func() *wrapperspb.StringValue { return payload },
					pollOptions("poll:approval", time.Hour),
				)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			runOptions := &Options{
				WorkflowId: "event-poll-validation",
				RunId:      "run-" + string(rune('a'+len(test.name)%20)),
			}
			_, err := Run(
				ctx,
				store,
				runOptions,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				test.body,
			)
			if err == nil {
				t.Fatal("expected validation error")
			}
			timers, listErr := store.ListTimers(
				ctx,
				storage.NewWorkflowKey(
					runOptions.GetWorkflowId(),
					runOptions.GetRunId(),
				),
				temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED,
			)
			if listErr != nil {
				t.Fatal(listErr)
			}
			if len(timers) != 0 {
				t.Fatalf("invalid wait wrote %d timers", len(timers))
			}
		})
	}
}

func TestResolvedPollRejectsCorruptTimerWithoutAcknowledging(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	runOptions := &Options{
		WorkflowId: "event-poll-corrupt-resolve",
		RunId:      "run",
	}
	poll := pollOptions("poll:approval", time.Hour)
	_, err := Run(
		ctx,
		store,
		runOptions,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		waitForApproval(poll),
	)
	if !errors.Is(err, ErrEventPending) {
		t.Fatalf("seed error=%v", err)
	}
	timerKey := storage.NewTimerKey(
		runOptions.GetWorkflowId(),
		runOptions.GetRunId(),
		poll.GetTimerId(),
	)
	timer, found, err := store.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("timer found=%v err=%v", found, err)
	}
	timer.CreatedAt = nil
	if err := store.PutTimer(ctx, timer); err != nil {
		t.Fatal(err)
	}
	payload, err := anypb.New(wrapperspb.String("approved"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutEvent(ctx, &temporalessv1.EventRecord{
		SchemaVersion: storage.EventRecordSchemaVersion,
		Key: storage.NewEventKey(
			runOptions.GetWorkflowId(),
			runOptions.GetRunId(),
			"approval",
		).Proto(),
		Payload:    payload,
		ReceivedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	_, err = Run(
		ctx,
		store,
		runOptions,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		waitForApproval(poll),
	)
	if !errors.Is(err, ErrTimerConflict) {
		t.Fatalf("resolved corrupt timer error=%v, want ErrTimerConflict", err)
	}
	timer, found, err = store.GetTimer(ctx, timerKey)
	if err != nil || !found ||
		timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		t.Fatalf("corrupt timer was acknowledged: found=%v err=%v timer=%v", found, err, timer)
	}
}

func TestWaitEventRejectsCorruptDeliveredEventBeforeResolvingPoll(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	runOptions := &Options{
		WorkflowId: "event-poll-corrupt-event",
		RunId:      "run",
	}
	poll := pollOptions("poll:approval", time.Hour)
	_, err := Run(
		ctx,
		store,
		runOptions,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		waitForApproval(poll),
	)
	if !errors.Is(err, ErrEventPending) {
		t.Fatalf("seed error=%v", err)
	}
	payload, err := anypb.New(wrapperspb.String("approved"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutEvent(ctx, &temporalessv1.EventRecord{
		SchemaVersion: storage.EventRecordSchemaVersion,
		Key: storage.NewEventKey(
			runOptions.GetWorkflowId(),
			runOptions.GetRunId(),
			"approval",
		).Proto(),
		Payload: payload,
		// Missing received_at is legal only for low-level operator writes.
	}); err != nil {
		t.Fatal(err)
	}
	_, err = Run(
		ctx,
		store,
		runOptions,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		waitForApproval(poll),
	)
	if !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("error=%v, want ErrCorruptRecord", err)
	}
	timerKey := storage.NewTimerKey(
		runOptions.GetWorkflowId(),
		runOptions.GetRunId(),
		poll.GetTimerId(),
	)
	timer, found, getErr := store.GetTimer(ctx, timerKey)
	if getErr != nil || !found ||
		timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		t.Fatalf("poll was resolved despite corrupt event: found=%v err=%v timer=%v", found, getErr, timer)
	}
}

func TestWaitEventClassifiesPayloadTypeMismatchAndMalformedWire(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		payload *anypb.Any
		want    error
	}{
		{
			name: "type-mismatch",
			payload: func() *anypb.Any {
				value, err := anypb.New(wrapperspb.Int32(7))
				if err != nil {
					t.Fatal(err)
				}
				return value
			}(),
			want: ErrWorkflowConflict,
		},
		{
			name: "malformed-wire",
			payload: &anypb.Any{
				TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
				Value:   []byte{0xff},
			},
			want: storage.ErrCorruptRecord,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			runOptions := &Options{
				WorkflowId: "event-payload-classification",
				RunId:      test.name,
			}
			key := storage.NewEventKey(
				runOptions.GetWorkflowId(),
				runOptions.GetRunId(),
				"approval",
			)
			if err := store.PutEvent(ctx, &temporalessv1.EventRecord{
				SchemaVersion: storage.EventRecordSchemaVersion,
				Key:           key.Proto(),
				Payload:       test.payload,
				ReceivedAt:    timestamppb.Now(),
			}); err != nil {
				t.Fatal(err)
			}
			_, err := Run(
				ctx,
				store,
				runOptions,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				waitForApproval(nil),
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
}

type ambiguousPollWriteStore struct {
	storage.Store
	err          error
	commitBefore bool
	failed       bool
}

func (store *ambiguousPollWriteStore) PutTimer(
	ctx context.Context,
	record *temporalessv1.TimerRecord,
) error {
	if !store.failed &&
		record.GetTimerKind() == storage.PollTimerKind &&
		record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		store.failed = true
		if store.commitBefore {
			if err := store.Store.PutTimer(ctx, record); err != nil {
				return err
			}
		}
		return store.err
	}
	return store.Store.PutTimer(ctx, record)
}

func TestPollAmbiguousWriteIsVerifiedAndRemainsResumable(t *testing.T) {
	tests := []struct {
		name         string
		commitBefore bool
		wantTimer    bool
	}{
		{name: "before-commit", commitBefore: false, wantTimer: false},
		{name: "after-commit", commitBefore: true, wantTimer: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			base := newTestStore(t)
			writeErr := errors.New("ambiguous poll timer write")
			store := &ambiguousPollWriteStore{
				Store:        base,
				err:          writeErr,
				commitBefore: test.commitBefore,
			}
			runOptions := &Options{
				WorkflowId: "event-poll-ambiguous",
				RunId:      test.name,
			}
			poll := pollOptions("poll:approval", time.Hour)
			_, err := Run(
				ctx,
				store,
				runOptions,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				waitForApproval(poll),
			)
			if !errors.Is(err, ErrWorkflowInfrastructure) ||
				!errors.Is(err, writeErr) {
				t.Fatalf("first error=%v, want infrastructure + backend", err)
			}
			timerKey := storage.NewTimerKey(
				runOptions.GetWorkflowId(),
				runOptions.GetRunId(),
				poll.GetTimerId(),
			)
			_, found, getErr := base.GetTimer(ctx, timerKey)
			if getErr != nil || found != test.wantTimer {
				t.Fatalf("first timer found=%v err=%v, want %v", found, getErr, test.wantTimer)
			}

			_, err = Run(
				ctx,
				store,
				runOptions,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				waitForApproval(poll),
			)
			if !errors.Is(err, ErrEventPending) {
				t.Fatalf("retry error=%v, want ErrEventPending", err)
			}
			timer, found, getErr := base.GetTimer(ctx, timerKey)
			if getErr != nil || !found ||
				timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
				t.Fatalf("retry timer found=%v err=%v timer=%v", found, getErr, timer)
			}
		})
	}
}
