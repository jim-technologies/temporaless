package inspection

import (
	"fmt"
	"sort"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RunRecords is the durable evidence for one run that the derivations read.
// Claims are only meaningful when ClaimsInspected is true.
type RunRecords struct {
	Workflow        *temporalessv1.WorkflowRecord
	Activities      []*temporalessv1.ActivityRecord
	Timers          []*temporalessv1.TimerRecord
	Events          []*temporalessv1.EventRecord
	Claims          []*temporalessv1.ClaimRecord
	ClaimsInspected bool
}

// DeriveHistory turns record timestamps into history events. It never
// invents a time: a record without the timestamp an event needs yields no
// event. The result is ordered by time, then by event ID.
func DeriveHistory(records RunRecords) []*inspectionv1.RunHistoryEvent {
	var history []*inspectionv1.RunHistoryEvent
	add := func(event *inspectionv1.RunHistoryEvent) {
		if event.GetTime() != nil {
			history = append(history, event)
		}
	}

	if workflow := records.Workflow; workflow != nil {
		add(&inspectionv1.RunHistoryEvent{
			EventId: "workflow/started",
			Kind:    inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_WORKFLOW_STARTED,
			Time:    workflow.GetCreatedAt(),
		})
		switch workflow.GetStatus() {
		case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:
			add(&inspectionv1.RunHistoryEvent{
				EventId: "workflow/completed",
				Kind:    inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_WORKFLOW_COMPLETED,
				Time:    workflow.GetCompletedAt(),
			})
		case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED:
			add(&inspectionv1.RunHistoryEvent{
				EventId: "workflow/failed",
				Kind:    inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_WORKFLOW_FAILED,
				Time:    workflow.GetCompletedAt(),
				Failure: workflow.GetFailure(),
			})
		}
	}

	for _, activity := range records.Activities {
		id := activity.GetKey().GetActivityId()
		var lastAttemptEnd *timestamppb.Timestamp
		for _, attempt := range activity.GetAttempts() {
			event := &inspectionv1.RunHistoryEvent{
				EventId:    fmt.Sprintf("activity/%s/attempt/%d", id, attempt.GetAttempt()),
				Time:       attempt.GetStartedAt(),
				Until:      attempt.GetCompletedAt(),
				ResourceId: id,
				Attempt:    attempt.GetAttempt(),
				Failure:    attempt.GetFailure(),
			}
			switch {
			case attempt.GetCompletedAt() == nil:
				event.Kind = inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_OPEN
			case attempt.GetFailure() != nil:
				event.Kind = inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_FAILED
			default:
				event.Kind = inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_SUCCEEDED
			}
			add(event)
			if attempt.GetCompletedAt() != nil {
				lastAttemptEnd = attempt.GetCompletedAt()
			}
		}
		switch activity.GetStatus() {
		case temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING:
			if activity.GetNextAttemptAt() != nil && lastAttemptEnd != nil {
				add(&inspectionv1.RunHistoryEvent{
					EventId:    fmt.Sprintf("activity/%s/retry-backoff/%d", id, len(activity.GetAttempts())),
					Kind:       inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_RETRY_BACKOFF,
					Time:       lastAttemptEnd,
					Until:      activity.GetNextAttemptAt(),
					ResourceId: id,
					Attempt:    uint32(len(activity.GetAttempts())),
				})
			}
		case temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED:
			add(&inspectionv1.RunHistoryEvent{
				EventId:    "activity/" + id + "/completed",
				Kind:       inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_COMPLETED,
				Time:       activity.GetCompletedAt(),
				ResourceId: id,
			})
		case temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED:
			add(&inspectionv1.RunHistoryEvent{
				EventId:    "activity/" + id + "/failed",
				Kind:       inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_FAILED,
				Time:       activity.GetCompletedAt(),
				ResourceId: id,
				Failure:    activity.GetFailure(),
			})
		}
	}

	for _, timer := range records.Timers {
		id := timer.GetKey().GetTimerId()
		if timer.GetFireAt() != nil {
			add(&inspectionv1.RunHistoryEvent{
				EventId:    "timer/" + id + "/scheduled",
				Kind:       inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_TIMER_SCHEDULED,
				Time:       timer.GetCreatedAt(),
				Until:      timer.GetFireAt(),
				ResourceId: id,
				TimerKind:  timer.GetTimerKind(),
			})
		}
		if timer.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
			add(&inspectionv1.RunHistoryEvent{
				EventId:    "timer/" + id + "/fired",
				Kind:       inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_TIMER_FIRED,
				Time:       timer.GetFiredAt(),
				ResourceId: id,
				TimerKind:  timer.GetTimerKind(),
			})
		}
	}

	for _, event := range records.Events {
		id := event.GetKey().GetEventId()
		add(&inspectionv1.RunHistoryEvent{
			EventId:    "event/" + id + "/received",
			Kind:       inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_EVENT_RECEIVED,
			Time:       event.GetReceivedAt(),
			ResourceId: id,
		})
	}

	if records.ClaimsInspected {
		for _, claim := range records.Claims {
			id := claim.GetKey().GetClaimId()
			add(&inspectionv1.RunHistoryEvent{
				EventId:    "claim/" + id + "/held",
				Kind:       inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_CLAIM_HELD,
				Time:       claim.GetCreatedAt(),
				Until:      claim.GetLeaseExpiresAt(),
				ResourceId: id,
			})
		}
	}

	sort.SliceStable(history, func(left, right int) bool {
		leftTime := history[left].GetTime().AsTime()
		rightTime := history[right].GetTime().AsTime()
		if !leftTime.Equal(rightTime) {
			return leftTime.Before(rightTime)
		}
		return history[left].GetEventId() < history[right].GetEventId()
	})
	return history
}

// DerivePending names the first matching pending reason for a run and the
// record that evidences it. The precedence is the RunPendingReason order.
func DerivePending(records RunRecords, now time.Time, overdueGrace time.Duration) *inspectionv1.RunPendingState {
	workflow := records.Workflow
	if workflow == nil {
		return &inspectionv1.RunPendingState{}
	}
	switch workflow.GetStatus() {
	case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED:
		return &inspectionv1.RunPendingState{Reason: inspectionv1.RunPendingReason_RUN_PENDING_REASON_TERMINAL}
	case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS:
	default:
		return &inspectionv1.RunPendingState{}
	}

	scheduled := make([]*temporalessv1.TimerRecord, 0, len(records.Timers))
	for _, timer := range records.Timers {
		if timer.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED && timer.GetFireAt() != nil {
			scheduled = append(scheduled, timer)
		}
	}
	sort.SliceStable(scheduled, func(left, right int) bool {
		return scheduled[left].GetFireAt().AsTime().Before(scheduled[right].GetFireAt().AsTime())
	})

	overdueBefore := now.Add(-overdueGrace)
	for _, timer := range scheduled {
		if timer.GetFireAt().AsTime().Before(overdueBefore) {
			return &inspectionv1.RunPendingState{
				Reason:     inspectionv1.RunPendingReason_RUN_PENDING_REASON_OVERDUE_WAKE,
				ResourceId: timer.GetKey().GetTimerId(),
				At:         timer.GetFireAt(),
			}
		}
	}

	for _, activity := range records.Activities {
		if activity.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING {
			continue
		}
		state := &inspectionv1.RunPendingState{
			Reason:          inspectionv1.RunPendingReason_RUN_PENDING_REASON_RETRYING,
			ResourceId:      activity.GetKey().GetActivityId(),
			At:              activity.GetNextAttemptAt(),
			Attempt:         uint32(len(activity.GetAttempts())),
			MaximumAttempts: activity.GetRetryPolicy().GetMaximumAttempts(),
		}
		if attempts := activity.GetAttempts(); len(attempts) > 0 {
			state.LastFailure = attempts[len(attempts)-1].GetFailure()
		}
		return state
	}
	// A scheduled retry timer is retry evidence even when its activity record
	// was not read (for example, a truncated description).
	for _, timer := range scheduled {
		if timer.GetTimerKind() == temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY {
			return &inspectionv1.RunPendingState{
				Reason:     inspectionv1.RunPendingReason_RUN_PENDING_REASON_RETRYING,
				ResourceId: timer.GetRetryActivityId(),
				At:         timer.GetFireAt(),
			}
		}
	}

	if records.ClaimsInspected {
		for _, claim := range records.Claims {
			if claim.GetLeaseExpiresAt() != nil && claim.GetLeaseExpiresAt().AsTime().Before(now) {
				return &inspectionv1.RunPendingState{
					Reason:     inspectionv1.RunPendingReason_RUN_PENDING_REASON_STALE_CLAIM,
					ResourceId: claim.GetKey().GetClaimId(),
					At:         claim.GetLeaseExpiresAt(),
				}
			}
		}
		for _, claim := range records.Claims {
			if claim.GetResourceType() == temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW {
				return &inspectionv1.RunPendingState{
					Reason:     inspectionv1.RunPendingReason_RUN_PENDING_REASON_EXECUTING,
					ResourceId: claim.GetKey().GetClaimId(),
					At:         claim.GetLeaseExpiresAt(),
				}
			}
		}
	}

	for _, kind := range []struct {
		timer  temporalessv1.TimerKind
		reason inspectionv1.RunPendingReason
	}{
		{temporalessv1.TimerKind_TIMER_KIND_SLEEP, inspectionv1.RunPendingReason_RUN_PENDING_REASON_SLEEPING},
		{temporalessv1.TimerKind_TIMER_KIND_POLL, inspectionv1.RunPendingReason_RUN_PENDING_REASON_POLLING},
	} {
		for _, timer := range scheduled {
			if timer.GetTimerKind() == kind.timer {
				return &inspectionv1.RunPendingState{
					Reason:     kind.reason,
					ResourceId: timer.GetKey().GetTimerId(),
					At:         timer.GetFireAt(),
				}
			}
		}
	}

	return &inspectionv1.RunPendingState{Reason: inspectionv1.RunPendingReason_RUN_PENDING_REASON_WAITING_NO_WAKE}
}
