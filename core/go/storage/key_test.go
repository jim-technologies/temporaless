package storage

import "testing"

func TestRecordKeyPaths(t *testing.T) {
	tests := []struct {
		name string
		path func() (string, error)
		want string
	}{
		{
			name: "workflow",
			path: func() (string, error) {
				return WorkflowKey{
					WorkflowID: "prices:aapl",
					RunID:      "2026-05-02",
				}.Path()
			},
			want: "temporaless/v1/namespace=default/workflow_id=prices:aapl/run_id=2026-05-02/kind=workflow/record.binpb",
		},
		{
			name: "activity",
			path: func() (string, error) {
				return ActivityKey{
					WorkflowID: "prices:aapl",
					RunID:      "2026-05-02",
					ActivityID: "fetch:price",
				}.Path()
			},
			want: "temporaless/v1/namespace=default/workflow_id=prices:aapl/run_id=2026-05-02/kind=activity/activity_id=fetch:price/record.binpb",
		},
		{
			name: "timer",
			path: func() (string, error) {
				return TimerKey{
					WorkflowID: "prices:aapl",
					RunID:      "2026-05-02",
					TimerID:    "wait:vendor-window",
				}.Path()
			},
			want: "temporaless/v1/namespace=default/workflow_id=prices:aapl/run_id=2026-05-02/kind=timer/timer_id=wait:vendor-window/record.binpb",
		},
		{
			name: "claim",
			path: func() (string, error) {
				return ClaimKey{
					WorkflowID: "prices:aapl",
					RunID:      "2026-05-02",
					ClaimID:    "activity:fetch:price",
				}.Path()
			},
			want: "temporaless/v1/namespace=default/workflow_id=prices:aapl/run_id=2026-05-02/kind=claim/claim_id=activity:fetch:price/record.binpb",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.path()
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("path = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRecordKeysRejectInvalidIDs(t *testing.T) {
	tests := []struct {
		name string
		path func() (string, error)
	}{
		{
			name: "workflow slash",
			path: func() (string, error) {
				return WorkflowKey{WorkflowID: "prices/aapl"}.Path()
			},
		},
		{
			name: "activity slash",
			path: func() (string, error) {
				return ActivityKey{WorkflowID: "prices:aapl", ActivityID: "fetch/price"}.Path()
			},
		},
		{
			name: "timer slash",
			path: func() (string, error) {
				return TimerKey{WorkflowID: "prices:aapl", TimerID: "wait/vendor"}.Path()
			},
		},
		{
			name: "claim slash",
			path: func() (string, error) {
				return ClaimKey{WorkflowID: "prices:aapl", ClaimID: "activity/fetch"}.Path()
			},
		},
		{
			name: "empty workflow",
			path: func() (string, error) {
				return WorkflowKey{WorkflowID: ""}.Path()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.path(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
