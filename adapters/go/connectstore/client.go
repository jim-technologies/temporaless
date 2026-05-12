package connectstore

import (
	"context"
	"time"

	"connectrpc.com/connect"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/temporalessv1connect"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ storage.Store = (*ClientStore)(nil)
var _ storage.ClaimStore = (*ClientStore)(nil)

type ClientStore struct {
	client temporalessv1connect.RecordStoreServiceClient
}

func NewClientStore(client temporalessv1connect.RecordStoreServiceClient) *ClientStore {
	return &ClientStore{client: client}
}

func NewHTTPClientStore(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) *ClientStore {
	return NewClientStore(temporalessv1connect.NewRecordStoreServiceClient(httpClient, baseURL, opts...))
}

func (store *ClientStore) GetActivity(ctx context.Context, key storage.ActivityKey) (*temporalessv1.ActivityRecord, bool, error) {
	resp, err := store.client.GetActivity(ctx, connect.NewRequest(&temporalessv1.GetActivityRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, false, err
	}
	return resp.Msg.GetRecord(), resp.Msg.GetFound(), nil
}

func (store *ClientStore) PutActivity(ctx context.Context, record *temporalessv1.ActivityRecord) error {
	_, err := store.client.PutActivity(ctx, connect.NewRequest(&temporalessv1.PutActivityRequest{
		Record: record,
	}))
	return err
}

func (store *ClientStore) GetWorkflow(ctx context.Context, key storage.WorkflowKey) (*temporalessv1.WorkflowRecord, bool, error) {
	resp, err := store.client.GetWorkflow(ctx, connect.NewRequest(&temporalessv1.GetWorkflowRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, false, err
	}
	return resp.Msg.GetRecord(), resp.Msg.GetFound(), nil
}

func (store *ClientStore) PutWorkflow(ctx context.Context, record *temporalessv1.WorkflowRecord) error {
	_, err := store.client.PutWorkflow(ctx, connect.NewRequest(&temporalessv1.PutWorkflowRequest{
		Record: record,
	}))
	return err
}

func (store *ClientStore) GetTimer(ctx context.Context, key storage.TimerKey) (*temporalessv1.TimerRecord, bool, error) {
	resp, err := store.client.GetTimer(ctx, connect.NewRequest(&temporalessv1.GetTimerRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, false, err
	}
	return resp.Msg.GetRecord(), resp.Msg.GetFound(), nil
}

func (store *ClientStore) PutTimer(ctx context.Context, record *temporalessv1.TimerRecord) error {
	_, err := store.client.PutTimer(ctx, connect.NewRequest(&temporalessv1.PutTimerRequest{
		Record: record,
	}))
	return err
}

func (store *ClientStore) ListWorkflows(ctx context.Context, namespace string, workflowID string, status temporalessv1.WorkflowStatus) ([]*temporalessv1.WorkflowRecord, error) {
	resp, err := store.client.ListWorkflows(ctx, connect.NewRequest(&temporalessv1.ListWorkflowsRequest{
		Namespace:  namespace,
		Status:     status,
		WorkflowId: workflowID,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRecords(), nil
}

func (store *ClientStore) DeleteWorkflow(ctx context.Context, key storage.WorkflowKey) (bool, error) {
	resp, err := store.client.DeleteWorkflow(ctx, connect.NewRequest(&temporalessv1.DeleteWorkflowRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return false, err
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) ListActivities(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.ActivityRecord, error) {
	resp, err := store.client.ListActivities(ctx, connect.NewRequest(&temporalessv1.ListActivitiesRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRecords(), nil
}

func (store *ClientStore) DeleteActivity(ctx context.Context, key storage.ActivityKey) (bool, error) {
	resp, err := store.client.DeleteActivity(ctx, connect.NewRequest(&temporalessv1.DeleteActivityRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return false, err
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) ListTimers(ctx context.Context, key storage.WorkflowKey, status temporalessv1.TimerStatus) ([]*temporalessv1.TimerRecord, error) {
	resp, err := store.client.ListTimers(ctx, connect.NewRequest(&temporalessv1.ListTimersRequest{
		Key:    key.Proto(),
		Status: status,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRecords(), nil
}

func (store *ClientStore) DeleteTimer(ctx context.Context, key storage.TimerKey) (bool, error) {
	resp, err := store.client.DeleteTimer(ctx, connect.NewRequest(&temporalessv1.DeleteTimerRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return false, err
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) ListEvents(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.EventRecord, error) {
	resp, err := store.client.ListEvents(ctx, connect.NewRequest(&temporalessv1.ListEventsRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRecords(), nil
}

func (store *ClientStore) DeleteEvent(ctx context.Context, key storage.EventKey) (bool, error) {
	resp, err := store.client.DeleteEvent(ctx, connect.NewRequest(&temporalessv1.DeleteEventRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return false, err
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) GetEvent(ctx context.Context, key storage.EventKey) (*temporalessv1.EventRecord, bool, error) {
	resp, err := store.client.GetEvent(ctx, connect.NewRequest(&temporalessv1.GetEventRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, false, err
	}
	return resp.Msg.GetRecord(), resp.Msg.GetFound(), nil
}

func (store *ClientStore) PutEvent(ctx context.Context, record *temporalessv1.EventRecord) error {
	_, err := store.client.PutEvent(ctx, connect.NewRequest(&temporalessv1.PutEventRequest{
		Record: record,
	}))
	return err
}

func (store *ClientStore) ClaimCapability(ctx context.Context) (storage.ClaimCapability, error) {
	resp, err := store.client.GetStoreCapabilities(ctx, connect.NewRequest(&temporalessv1.GetStoreCapabilitiesRequest{}))
	if err != nil {
		return storage.NoClaims, err
	}
	return resp.Msg.GetClaimCapability(), nil
}

func (store *ClientStore) GetClaim(ctx context.Context, key storage.ClaimKey) (*temporalessv1.ClaimRecord, bool, error) {
	resp, err := store.client.GetClaim(ctx, connect.NewRequest(&temporalessv1.GetClaimRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, false, err
	}
	return resp.Msg.GetRecord(), resp.Msg.GetFound(), nil
}

func (store *ClientStore) TryCreateClaim(ctx context.Context, record *temporalessv1.ClaimRecord) (bool, error) {
	resp, err := store.client.TryCreateClaim(ctx, connect.NewRequest(&temporalessv1.TryCreateClaimRequest{
		Record: record,
	}))
	if err != nil {
		return false, err
	}
	return resp.Msg.GetCreated(), nil
}

func (store *ClientStore) DeleteClaim(ctx context.Context, key storage.ClaimKey) (bool, error) {
	resp, err := store.client.DeleteClaim(ctx, connect.NewRequest(&temporalessv1.DeleteClaimRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return false, err
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) Sweep(ctx context.Context, namespace string, now time.Time, maxAge time.Duration) (uint32, error) {
	resp, err := store.client.Sweep(ctx, connect.NewRequest(&temporalessv1.SweepRequest{
		Namespace: namespace,
		Now:       timestamppb.New(now),
		MaxAge:    durationpb.New(maxAge),
	}))
	if err != nil {
		return 0, err
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) DueTimers(ctx context.Context, namespace string, now time.Time) ([]storage.DueTimer, error) {
	resp, err := store.client.DueTimers(ctx, connect.NewRequest(&temporalessv1.DueTimersRequest{
		Namespace: namespace,
		Now:       timestamppb.New(now),
	}))
	if err != nil {
		return nil, err
	}
	due := make([]storage.DueTimer, 0, len(resp.Msg.GetDue()))
	for _, entry := range resp.Msg.GetDue() {
		due = append(due, storage.DueTimer{
			Key:      storage.TimerKeyFromProto(entry.GetKey()),
			Record:   entry.GetRecord(),
			Workflow: entry.GetWorkflow(),
		})
	}
	return due, nil
}
