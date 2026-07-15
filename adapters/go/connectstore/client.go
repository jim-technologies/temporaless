package connectstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/temporalessv1connect"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ storage.Store = (*ClientStore)(nil)
var _ storage.QueryStore = (*ClientStore)(nil)
var _ storage.ClaimStore = (*ClientStore)(nil)
var _ storage.ClaimRunStore = (*ClientStore)(nil)

type ClientStore struct {
	client      temporalessv1connect.RecordStoreServiceClient
	queryClient temporalessv1connect.RecordQueryServiceClient
}

func NewClientStore(client temporalessv1connect.RecordStoreServiceClient) *ClientStore {
	return &ClientStore{client: client}
}

func NewClientStoreWithQuery(client temporalessv1connect.RecordStoreServiceClient, queryClient temporalessv1connect.RecordQueryServiceClient) *ClientStore {
	return &ClientStore{client: client, queryClient: queryClient}
}

// NewLocalClientStore dispatches point operations through the protobuf
// service-shaped handler in-process.
func NewLocalClientStore(store storage.Store) *ClientStore {
	return NewClientStore(NewHandler(store))
}

// NewLocalClientStoreWithClaims is NewLocalClientStore with an explicit claim
// store for the RecordStoreService side.
func NewLocalClientStoreWithClaims(store storage.Store, claimStore storage.ClaimStore) *ClientStore {
	return NewClientStore(NewHandlerWithClaims(store, claimStore))
}

// NewLocalClientStoreWithQuery adds an explicit in-process QueryStore.
func NewLocalClientStoreWithQuery(store storage.Store, query storage.QueryStore) *ClientStore {
	return NewClientStoreWithQuery(NewHandler(store), NewQueryHandler(query))
}

// NewLocalClientStoreWithClaimsAndQuery adds explicit point claims and an
// explicit in-process QueryStore.
func NewLocalClientStoreWithClaimsAndQuery(store storage.Store, claimStore storage.ClaimStore, query storage.QueryStore) *ClientStore {
	return NewClientStoreWithQuery(NewHandlerWithClaims(store, claimStore), NewQueryHandler(query))
}

// NewHTTPClientStore wraps remote RecordStoreService and RecordQueryService
// endpoints mounted at the same base URL. Point operations need only
// RecordStoreService; QueryStore methods require RecordQueryService.
func NewHTTPClientStore(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) *ClientStore {
	return NewClientStoreWithQuery(
		temporalessv1connect.NewRecordStoreServiceClient(httpClient, baseURL, opts...),
		temporalessv1connect.NewRecordQueryServiceClient(httpClient, baseURL, opts...),
	)
}

func (store *ClientStore) GetActivity(ctx context.Context, key storage.ActivityKey) (*temporalessv1.ActivityRecord, bool, error) {
	resp, err := store.client.GetActivity(ctx, connect.NewRequest(&temporalessv1.GetActivityRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, false, clientStoreError(err)
	}
	record := resp.Msg.GetRecord()
	if err := validateFoundPayload("activity", resp.Msg.GetFound(), record != nil); err != nil {
		return nil, false, err
	}
	if !resp.Msg.GetFound() {
		return nil, false, nil
	}
	if err := storage.ValidateActivityRecord(record, key); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

func (store *ClientStore) PutActivity(ctx context.Context, record *temporalessv1.ActivityRecord) error {
	_, err := store.client.PutActivity(ctx, connect.NewRequest(&temporalessv1.PutActivityRequest{
		Record: record,
	}))
	return clientStoreError(err)
}

func (store *ClientStore) GetWorkflow(ctx context.Context, key storage.WorkflowKey) (*temporalessv1.WorkflowRecord, bool, error) {
	resp, err := store.client.GetWorkflow(ctx, connect.NewRequest(&temporalessv1.GetWorkflowRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, false, clientStoreError(err)
	}
	record := resp.Msg.GetRecord()
	if err := validateFoundPayload("workflow", resp.Msg.GetFound(), record != nil); err != nil {
		return nil, false, err
	}
	if !resp.Msg.GetFound() {
		return nil, false, nil
	}
	if err := storage.ValidateWorkflowRecord(record, key); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

func (store *ClientStore) PutWorkflow(ctx context.Context, record *temporalessv1.WorkflowRecord) error {
	_, err := store.client.PutWorkflow(ctx, connect.NewRequest(&temporalessv1.PutWorkflowRequest{
		Record: record,
	}))
	return clientStoreError(err)
}

func (store *ClientStore) GetLatestWorkflowRun(
	ctx context.Context,
	namespace string,
	workflowID string,
) (*temporalessv1.LatestWorkflowRunPointer, bool, error) {
	resp, err := store.client.GetLatestWorkflowRun(ctx, connect.NewRequest(&temporalessv1.GetLatestWorkflowRunRequest{
		Namespace:  namespace,
		WorkflowId: workflowID,
	}))
	if err != nil {
		return nil, false, clientStoreError(err)
	}
	pointer := resp.Msg.GetPointer()
	if err := validateFoundPayload("latest workflow run pointer", resp.Msg.GetFound(), pointer != nil); err != nil {
		return nil, false, err
	}
	if !resp.Msg.GetFound() {
		return nil, false, nil
	}
	if err := storage.ValidateLatestWorkflowRunPointer(pointer, namespace, workflowID); err != nil {
		return nil, false, err
	}
	referenceKey := storage.WorkflowKeyFromProto(pointer.GetKey())
	reference, referenceFound, err := store.GetWorkflow(ctx, referenceKey)
	if err != nil {
		return nil, false, clientStoreError(err)
	}
	if !referenceFound {
		return nil, false, nil
	}
	if err := storage.ValidateLatestWorkflowRunReference(pointer, reference); err != nil {
		if errors.Is(err, storage.ErrStaleLatestPointer) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return pointer, true, nil
}

func (store *ClientStore) GetTimer(ctx context.Context, key storage.TimerKey) (*temporalessv1.TimerRecord, bool, error) {
	resp, err := store.client.GetTimer(ctx, connect.NewRequest(&temporalessv1.GetTimerRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, false, clientStoreError(err)
	}
	record := resp.Msg.GetRecord()
	if err := validateFoundPayload("timer", resp.Msg.GetFound(), record != nil); err != nil {
		return nil, false, err
	}
	if !resp.Msg.GetFound() {
		return nil, false, nil
	}
	if err := storage.ValidateTimerRecord(record, key); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

func (store *ClientStore) PutTimer(ctx context.Context, record *temporalessv1.TimerRecord) error {
	_, err := store.client.PutTimer(ctx, connect.NewRequest(&temporalessv1.PutTimerRequest{
		Record: record,
	}))
	return clientStoreError(err)
}

func (store *ClientStore) ListWorkflows(ctx context.Context, request *temporalessv1.ListWorkflowsRequest) (*temporalessv1.ListWorkflowsResponse, error) {
	if store.queryClient == nil {
		return nil, errors.New("record query service client is required for ListWorkflows")
	}
	resp, err := store.queryClient.ListWorkflows(ctx, connect.NewRequest(request))
	if err != nil {
		return nil, clientStoreError(err)
	}
	for _, record := range resp.Msg.GetRecords() {
		key := storage.WorkflowKeyFromProto(record.GetKey())
		if err := storage.ValidateWorkflowRecord(record, key); err != nil {
			return nil, err
		}
		if request.GetNamespace() != "" && namespaceOrDefault(key.Namespace) != request.GetNamespace() {
			return nil, corruptClientResponsef("workflow query payload crosses the requested namespace")
		}
		if request.GetWorkflowId() != "" && key.WorkflowID != request.GetWorkflowId() {
			return nil, corruptClientResponsef("workflow query payload crosses the requested workflow_id")
		}
		if request.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED && record.GetStatus() != request.GetStatus() {
			return nil, corruptClientResponsef("workflow query payload does not match the requested status")
		}
	}
	return resp.Msg, nil
}

func (store *ClientStore) DeleteWorkflow(ctx context.Context, key storage.WorkflowKey) (bool, error) {
	resp, err := store.client.DeleteWorkflow(ctx, connect.NewRequest(&temporalessv1.DeleteWorkflowRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return false, clientStoreError(err)
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) ListActivities(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.ActivityRecord, error) {
	resp, err := store.client.ListActivities(ctx, connect.NewRequest(&temporalessv1.ListActivitiesRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, clientStoreError(err)
	}
	records := resp.Msg.GetRecords()
	for _, record := range records {
		recordKey := storage.ActivityKeyFromProto(record.GetKey())
		if err := storage.ValidateActivityRecord(record, recordKey); err != nil {
			return nil, err
		}
		if !sameWorkflowRun(key, recordKey.Namespace, recordKey.WorkflowID, recordKey.RunID) {
			return nil, corruptClientResponsef("activity list payload crosses the requested workflow run")
		}
	}
	return records, nil
}

func (store *ClientStore) ListActivitiesQuery(
	ctx context.Context,
	request *temporalessv1.RecordQueryServiceListActivitiesRequest,
) (*temporalessv1.RecordQueryServiceListActivitiesResponse, error) {
	if store.queryClient == nil {
		return nil, errors.New("record query service client is required for ListActivitiesQuery")
	}
	resp, err := store.queryClient.ListActivities(ctx, connect.NewRequest(request))
	if err != nil {
		return nil, clientStoreError(err)
	}
	for _, record := range resp.Msg.GetRecords() {
		key := storage.ActivityKeyFromProto(record.GetKey())
		if err := storage.ValidateActivityRecord(record, key); err != nil {
			return nil, err
		}
		if request.GetNamespace() != "" && namespaceOrDefault(key.Namespace) != request.GetNamespace() {
			return nil, corruptClientResponsef("activity query payload crosses the requested namespace")
		}
		if request.GetWorkflowId() != "" && key.WorkflowID != request.GetWorkflowId() {
			return nil, corruptClientResponsef("activity query payload crosses the requested workflow_id")
		}
		if request.GetRunId() != "" && key.RunID != request.GetRunId() {
			return nil, corruptClientResponsef("activity query payload crosses the requested run_id")
		}
		if request.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_UNSPECIFIED && record.GetStatus() != request.GetStatus() {
			return nil, corruptClientResponsef("activity query payload does not match the requested status")
		}
	}
	return resp.Msg, nil
}

func (store *ClientStore) DeleteActivity(ctx context.Context, key storage.ActivityKey) (bool, error) {
	resp, err := store.client.DeleteActivity(ctx, connect.NewRequest(&temporalessv1.DeleteActivityRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return false, clientStoreError(err)
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) ListTimers(ctx context.Context, key storage.WorkflowKey, status temporalessv1.TimerStatus) ([]*temporalessv1.TimerRecord, error) {
	resp, err := store.client.ListTimers(ctx, connect.NewRequest(&temporalessv1.ListTimersRequest{
		Key:    key.Proto(),
		Status: status,
	}))
	if err != nil {
		return nil, clientStoreError(err)
	}
	records := resp.Msg.GetRecords()
	for _, record := range records {
		recordKey := storage.TimerKeyFromProto(record.GetKey())
		if err := storage.ValidateTimerRecord(record, recordKey); err != nil {
			return nil, err
		}
		if !sameWorkflowRun(key, recordKey.Namespace, recordKey.WorkflowID, recordKey.RunID) {
			return nil, corruptClientResponsef("timer list payload crosses the requested workflow run")
		}
		if status != temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED && record.GetStatus() != status {
			return nil, corruptClientResponsef("timer list payload does not match the requested status")
		}
	}
	return records, nil
}

func (store *ClientStore) DeleteTimer(ctx context.Context, key storage.TimerKey) (bool, error) {
	resp, err := store.client.DeleteTimer(ctx, connect.NewRequest(&temporalessv1.DeleteTimerRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return false, clientStoreError(err)
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) ListEvents(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.EventRecord, error) {
	resp, err := store.client.ListEvents(ctx, connect.NewRequest(&temporalessv1.ListEventsRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, clientStoreError(err)
	}
	records := resp.Msg.GetRecords()
	for _, record := range records {
		recordKey := storage.EventKeyFromProto(record.GetKey())
		if err := storage.ValidateEventRecord(record, recordKey); err != nil {
			return nil, err
		}
		if !sameWorkflowRun(key, recordKey.Namespace, recordKey.WorkflowID, recordKey.RunID) {
			return nil, corruptClientResponsef("event list payload crosses the requested workflow run")
		}
	}
	return records, nil
}

func (store *ClientStore) ListClaims(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.ClaimRecord, error) {
	resp, err := store.client.ListClaims(ctx, connect.NewRequest(&temporalessv1.ListClaimsRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, clientStoreError(err)
	}
	records := resp.Msg.GetRecords()
	for _, record := range records {
		recordKey := storage.ClaimKeyFromProto(record.GetKey())
		if err := storage.ValidateClaimRecord(record, recordKey); err != nil {
			return nil, err
		}
		if !sameWorkflowRun(key, recordKey.Namespace, recordKey.WorkflowID, recordKey.RunID) {
			return nil, corruptClientResponsef("claim list payload crosses the requested workflow run")
		}
	}
	return records, nil
}

func (store *ClientStore) DeleteEvent(ctx context.Context, key storage.EventKey) (bool, error) {
	resp, err := store.client.DeleteEvent(ctx, connect.NewRequest(&temporalessv1.DeleteEventRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return false, clientStoreError(err)
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) GetEvent(ctx context.Context, key storage.EventKey) (*temporalessv1.EventRecord, bool, error) {
	resp, err := store.client.GetEvent(ctx, connect.NewRequest(&temporalessv1.GetEventRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, false, clientStoreError(err)
	}
	record := resp.Msg.GetRecord()
	if err := validateFoundPayload("event", resp.Msg.GetFound(), record != nil); err != nil {
		return nil, false, err
	}
	if !resp.Msg.GetFound() {
		return nil, false, nil
	}
	if err := storage.ValidateEventRecord(record, key); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

func (store *ClientStore) PutEvent(ctx context.Context, record *temporalessv1.EventRecord) error {
	_, err := store.client.PutEvent(ctx, connect.NewRequest(&temporalessv1.PutEventRequest{
		Record: record,
	}))
	return clientStoreError(err)
}

func (store *ClientStore) ClaimCapability(ctx context.Context) (storage.ClaimCapability, error) {
	resp, err := store.client.GetStoreCapabilities(ctx, connect.NewRequest(&temporalessv1.GetStoreCapabilitiesRequest{}))
	if err != nil {
		return storage.NoClaims, clientStoreError(err)
	}
	return resp.Msg.GetClaimCapability(), nil
}

func (store *ClientStore) GetClaim(ctx context.Context, key storage.ClaimKey) (*temporalessv1.ClaimRecord, bool, error) {
	resp, err := store.client.GetClaim(ctx, connect.NewRequest(&temporalessv1.GetClaimRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return nil, false, clientStoreError(err)
	}
	record := resp.Msg.GetRecord()
	if err := validateFoundPayload("claim", resp.Msg.GetFound(), record != nil); err != nil {
		return nil, false, err
	}
	if !resp.Msg.GetFound() {
		return nil, false, nil
	}
	if err := storage.ValidateClaimRecord(record, key); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

func (store *ClientStore) TryCreateClaim(ctx context.Context, record *temporalessv1.ClaimRecord) (bool, error) {
	resp, err := store.client.TryCreateClaim(ctx, connect.NewRequest(&temporalessv1.TryCreateClaimRequest{
		Record: record,
	}))
	if err != nil {
		return false, clientStoreError(err)
	}
	return resp.Msg.GetCreated(), nil
}

func (store *ClientStore) DeleteClaim(ctx context.Context, key storage.ClaimKey) (bool, error) {
	resp, err := store.client.DeleteClaim(ctx, connect.NewRequest(&temporalessv1.DeleteClaimRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return false, clientStoreError(err)
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) DeleteRun(ctx context.Context, key storage.WorkflowKey) (uint32, error) {
	resp, err := store.client.DeleteRun(ctx, connect.NewRequest(&temporalessv1.DeleteRunRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		return 0, clientStoreError(err)
	}
	return resp.Msg.GetDeleted(), nil
}

func (store *ClientStore) Sweep(ctx context.Context, request *temporalessv1.SweepRequest) (*temporalessv1.SweepResponse, error) {
	if store.queryClient == nil {
		return nil, errors.New("record query service client is required for Sweep")
	}
	resp, err := store.queryClient.Sweep(ctx, connect.NewRequest(request))
	if err != nil {
		return nil, clientStoreError(err)
	}
	return resp.Msg, nil
}

func (store *ClientStore) DueTimers(ctx context.Context, namespace string, now time.Time) ([]storage.DueTimer, error) {
	resp, err := store.client.DueTimers(ctx, connect.NewRequest(&temporalessv1.DueTimersRequest{
		Namespace: namespace,
		Now:       timestamppb.New(now),
	}))
	if err != nil {
		return nil, clientStoreError(err)
	}
	due := make([]storage.DueTimer, 0, len(resp.Msg.GetDue()))
	for _, entry := range resp.Msg.GetDue() {
		item, err := validateDueTimerEntry(entry, namespace, now)
		if err != nil {
			return nil, err
		}
		due = append(due, item)
	}
	return due, nil
}

func (store *ClientStore) DueTimersQuery(
	ctx context.Context,
	request *temporalessv1.RecordQueryServiceDueTimersRequest,
) (*temporalessv1.RecordQueryServiceDueTimersResponse, error) {
	if store.queryClient == nil {
		return nil, errors.New("record query service client is required for DueTimersQuery")
	}
	resp, err := store.queryClient.DueTimers(ctx, connect.NewRequest(request))
	if err != nil {
		return nil, clientStoreError(err)
	}
	for _, entry := range resp.Msg.GetDue() {
		if _, err := validateDueTimerEntry(entry, request.GetNamespace(), request.GetNow().AsTime()); err != nil {
			return nil, err
		}
	}
	return resp.Msg, nil
}

func validateFoundPayload(kind string, found bool, present bool) error {
	if found == present {
		return nil
	}
	return fmt.Errorf(
		"%w: %s response has found=%t with payload present=%t",
		storage.ErrCorruptRecord,
		kind,
		found,
		present,
	)
}

func sameWorkflowRun(key storage.WorkflowKey, namespace string, workflowID string, runID string) bool {
	return namespaceOrDefault(key.Namespace) == namespaceOrDefault(namespace) &&
		key.WorkflowID == workflowID &&
		key.RunID == runID
}

func namespaceOrDefault(namespace string) string {
	if namespace == "" {
		return storage.DefaultNamespace
	}
	return namespace
}

func validateDueTimerEntry(
	entry *temporalessv1.DueTimer,
	namespace string,
	now time.Time,
) (storage.DueTimer, error) {
	if entry == nil {
		return storage.DueTimer{}, corruptClientResponsef("due timer response contains a nil entry")
	}
	due := storage.DueTimer{
		Key:      storage.TimerKeyFromProto(entry.GetKey()),
		Record:   entry.GetRecord(),
		Workflow: entry.GetWorkflow(),
	}
	if err := storage.ValidateDueTimer(due, namespace, now); err != nil {
		return storage.DueTimer{}, err
	}
	return due, nil
}

func corruptClientResponsef(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", storage.ErrCorruptRecord, fmt.Sprintf(format, arguments...))
}

func clientStoreError(err error) error {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeDataLoss {
		return fmt.Errorf("%w: %w", storage.ErrCorruptRecord, err)
	}
	return err
}
