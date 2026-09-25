// Package inspection implements temporaless.v1.RunInspectionService directly
// over bucket record stores: bounded listings and point reads, plus history
// and pending state derived from record timestamps.
//
// Nothing here writes, repairs, claims, or deletes. Run it with read-only
// bucket credentials. The due ledger is read as it is, including entries a
// timer scanner would repair or quarantine; inspection reports those states
// and leaves the repair to the scanner.
package inspection

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Store is one registered record store.
type Store struct {
	// ID is the stable identifier requests use.
	ID string
	// DisplayName labels the store in pickers. Empty uses ID.
	DisplayName string
	// Namespaces is the allowlist of namespaces this store serves.
	Namespaces []string
	// ListClaims reads run-scoped claim records from the bucket. Enable it
	// only when the store's claim adapter writes claims to the same bucket
	// layout; otherwise DescribeRun reports claims as not inspected.
	ListClaims bool
	// OverdueGrace is how late past fire_at a SCHEDULED timer may be before
	// it is reported as an overdue wake. Zero selects DefaultOverdueGrace.
	OverdueGrace time.Duration
	// Records serves authoritative point reads. Only its read methods are
	// called.
	Records storage.Store
	// Bucket serves listings and raw reads under the same root as Records.
	Bucket Bucket
	// Payloads renders Any payloads. Nil renders well-known and linked types
	// only.
	Payloads *PayloadRenderer
}

// DefaultOverdueGrace is two one-minute timer-scanner periods.
const DefaultOverdueGrace = 2 * time.Minute

// Grant is what one caller may inspect in one store.
type Grant struct {
	// Namespaces narrows the store's allowlist. Empty grants all of it.
	Namespaces []string
	// Payloads allows payload values; false redacts them.
	Payloads bool
}

// Access decides whether the caller in ctx may see a store and what it may
// inspect there. visible=false hides the store entirely.
type Access func(ctx context.Context, store *Store) (grant Grant, visible bool, err error)

// AllowAll grants every store, namespace, and payload. It suits a loopback
// development server and tests; hosted servers pass an authorizing Access.
func AllowAll(context.Context, *Store) (Grant, bool, error) {
	return Grant{Payloads: true}, true, nil
}

// PageTokens seals listing cursors into opaque page tokens bound to the
// request that produced them, and opens them again. binding names the method,
// store, namespace, and filters; a token must not open under another binding.
type PageTokens interface {
	Seal(ctx context.Context, binding string, cursor string) (string, error)
	Open(ctx context.Context, binding string, token string) (string, error)
}

// PlainPageTokens encodes the binding beside the cursor without a MAC. It
// catches a token replayed with other filters by mistake but authenticates
// nothing; hosted servers use a MAC-bound implementation.
type PlainPageTokens struct{}

// Seal implements PageTokens.
func (PlainPageTokens) Seal(_ context.Context, binding string, cursor string) (string, error) {
	return base64.RawURLEncoding.EncodeToString([]byte(binding + "\x00" + cursor)), nil
}

// Open implements PageTokens.
func (PlainPageTokens) Open(_ context.Context, binding string, token string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", errors.New("malformed page token")
	}
	gotBinding, cursor, ok := strings.Cut(string(data), "\x00")
	if !ok || gotBinding != binding {
		return "", errors.New("page token does not belong to this request")
	}
	return cursor, nil
}

// Limits bounds the work one request may do.
type Limits struct {
	// DefaultPageSize applies when a request leaves page_size at zero.
	DefaultPageSize int
	// MaxPageSize caps page_size; larger requests are coerced down.
	MaxPageSize int
	// ReadBudget caps the objects a filtered listing reads per request.
	ReadBudget int
	// MaxListedRuns caps the run directories listed for one workflow_id.
	MaxListedRuns int
	// MaxListedObjects caps pointer and ledger listings per namespace.
	MaxListedObjects int
	// MaxRunRecords caps records read per record kind for one run.
	MaxRunRecords int
	// Concurrency caps parallel point reads per request.
	Concurrency int
}

// DefaultLimits are conservative for object storage behind a scale-to-zero
// server.
func DefaultLimits() Limits {
	return Limits{
		DefaultPageSize:  50,
		MaxPageSize:      100,
		ReadBudget:       500,
		MaxListedRuns:    20000,
		MaxListedObjects: 50000,
		MaxRunRecords:    2000,
		Concurrency:      8,
	}
}

// Options configures a Service. Zero values select safe defaults, except
// Access, which is required so an unauthorized server cannot be built by
// omission.
type Options struct {
	Access     Access
	PageTokens PageTokens
	Limits     Limits
	Now        func() time.Time
	Logger     *slog.Logger
}

// Service implements temporaless.v1.RunInspectionService.
type Service struct {
	inspectionv1.UnimplementedRunInspectionServiceServer

	stores   map[string]*Store
	order    []string
	access   Access
	tokens   PageTokens
	limits   Limits
	now      func() time.Time
	logger   *slog.Logger
	renderer *PayloadRenderer
}

var _ inspectionv1.RunInspectionServiceServer = (*Service)(nil)

// NewService validates the store registrations and returns the service.
func NewService(stores []*Store, options Options) (*Service, error) {
	if options.Access == nil {
		return nil, errors.New("inspection: Options.Access is required (use AllowAll for a loopback server)")
	}
	if len(stores) == 0 {
		return nil, errors.New("inspection: at least one store is required")
	}
	service := &Service{
		stores: make(map[string]*Store, len(stores)),
		access: options.Access,
		tokens: options.PageTokens,
		limits: options.Limits,
		now:    options.Now,
		logger: options.Logger,
	}
	if service.tokens == nil {
		service.tokens = PlainPageTokens{}
	}
	if service.limits == (Limits{}) {
		service.limits = DefaultLimits()
	}
	if service.now == nil {
		service.now = time.Now
	}
	if service.logger == nil {
		service.logger = slog.Default()
	}
	renderer, err := NewPayloadRenderer(nil, 0)
	if err != nil {
		return nil, err
	}
	service.renderer = renderer
	for _, store := range stores {
		if store == nil || store.ID == "" {
			return nil, errors.New("inspection: every store needs an ID")
		}
		if _, exists := service.stores[store.ID]; exists {
			return nil, fmt.Errorf("inspection: duplicate store %q", store.ID)
		}
		if store.Records == nil || store.Bucket == nil {
			return nil, fmt.Errorf("inspection: store %q needs Records and Bucket", store.ID)
		}
		if len(store.Namespaces) == 0 {
			return nil, fmt.Errorf("inspection: store %q needs at least one namespace", store.ID)
		}
		for _, namespace := range store.Namespaces {
			if err := validateScopeSegments(namespace, ""); err != nil {
				return nil, fmt.Errorf("inspection: store %q namespace %q: %w", store.ID, namespace, err)
			}
		}
		service.stores[store.ID] = store
		service.order = append(service.order, store.ID)
	}
	sort.Strings(service.order)
	return service, nil
}

// GetInspectionCapabilities implements RunInspectionService.
func (service *Service) GetInspectionCapabilities(
	ctx context.Context,
	_ *inspectionv1.GetInspectionCapabilitiesRequest,
) (*inspectionv1.GetInspectionCapabilitiesResponse, error) {
	response := &inspectionv1.GetInspectionCapabilitiesResponse{}
	for _, id := range service.order {
		store := service.stores[id]
		grant, visible, err := service.access(ctx, store)
		if err != nil {
			return nil, err
		}
		if !visible {
			continue
		}
		displayName := store.DisplayName
		if displayName == "" {
			displayName = store.ID
		}
		response.Stores = append(response.Stores, &inspectionv1.InspectionStore{
			Store:           store.ID,
			DisplayName:     displayName,
			Namespaces:      grantedNamespaces(store, grant),
			ClaimsListed:    store.ListClaims,
			PayloadsVisible: grant.Payloads,
			OverdueGrace:    durationpb.New(overdueGrace(store)),
			IndexedSearch:   false,
			MaxPageSize:     uint32(service.limits.MaxPageSize),
			MaxListedRuns:   uint32(service.limits.MaxListedRuns),
			MaxRunRecords:   uint32(service.limits.MaxRunRecords),
		})
	}
	return response, nil
}

// ListNamespaces implements RunInspectionService.
func (service *Service) ListNamespaces(
	ctx context.Context,
	request *inspectionv1.ListNamespacesRequest,
) (*inspectionv1.ListNamespacesResponse, error) {
	store, grant, err := service.storeScope(ctx, request.GetStore())
	if err != nil {
		return nil, err
	}
	response := &inspectionv1.ListNamespacesResponse{}
	for _, namespace := range grantedNamespaces(store, grant) {
		children, err := store.Bucket.List(ctx, latestDir(namespace))
		if err != nil {
			return nil, service.storeError(err)
		}
		response.Namespaces = append(response.Namespaces, &inspectionv1.InspectionNamespace{
			Namespace: namespace,
			Present:   len(children) > 0,
		})
	}
	return response, nil
}

// storeScope resolves a store the caller may see. An unknown or invisible
// store is reported the same way so the error is not an existence oracle.
func (service *Service) storeScope(ctx context.Context, storeID string) (*Store, Grant, error) {
	store, ok := service.stores[storeID]
	if !ok {
		return nil, Grant{}, status.Errorf(codes.NotFound, "store %q is not available", storeID)
	}
	grant, visible, err := service.access(ctx, store)
	if err != nil {
		return nil, Grant{}, err
	}
	if !visible {
		return nil, Grant{}, status.Errorf(codes.NotFound, "store %q is not available", storeID)
	}
	return store, grant, nil
}

// namespaceScope additionally requires a granted namespace. An empty
// namespace is never accepted: there is no all-namespaces listing.
func (service *Service) namespaceScope(ctx context.Context, storeID, namespace string) (*Store, Grant, error) {
	store, grant, err := service.storeScope(ctx, storeID)
	if err != nil {
		return nil, Grant{}, err
	}
	if namespace == "" {
		return nil, Grant{}, status.Error(codes.InvalidArgument, "namespace is required")
	}
	if !slices.Contains(grantedNamespaces(store, grant), namespace) {
		return nil, Grant{}, status.Errorf(codes.PermissionDenied, "namespace %q is not inspectable in store %q", namespace, store.ID)
	}
	return store, grant, nil
}

func grantedNamespaces(store *Store, grant Grant) []string {
	namespaces := make([]string, 0, len(store.Namespaces))
	for _, namespace := range store.Namespaces {
		if len(grant.Namespaces) == 0 || slices.Contains(grant.Namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}
	sort.Strings(namespaces)
	return namespaces
}

func overdueGrace(store *Store) time.Duration {
	if store.OverdueGrace > 0 {
		return store.OverdueGrace
	}
	return DefaultOverdueGrace
}

func (service *Service) pageSize(requested uint32) int {
	size := int(requested)
	if size <= 0 {
		size = service.limits.DefaultPageSize
	}
	return min(size, service.limits.MaxPageSize)
}

func (service *Service) openCursor(ctx context.Context, binding, token string) (string, error) {
	if token == "" {
		return "", nil
	}
	cursor, err := service.tokens.Open(ctx, binding, token)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
	}
	return cursor, nil
}

func (service *Service) sealCursor(ctx context.Context, binding, cursor string) (string, error) {
	if cursor == "" {
		return "", nil
	}
	return service.tokens.Seal(ctx, binding, cursor)
}

// storeError maps a storage failure onto a status. Corruption is data loss;
// a listing past its bound asks for an index; anything else is the store
// being unavailable. Context errors pass through unchanged.
func (service *Service) storeError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case status.Code(err) != codes.Unknown:
		return err
	case errors.Is(err, storage.ErrCorruptRecord):
		return status.Error(codes.DataLoss, err.Error())
	case errors.Is(err, errTooManyObjects):
		return status.Errorf(codes.FailedPrecondition, "%v; use an indexed query store for this scope", err)
	default:
		service.logger.Warn("inspection: store read failed", "error", err)
		return status.Error(codes.Unavailable, "the record store could not be read")
	}
}

// forEach runs fn over items with bounded concurrency and returns the first
// error. Results are written by index, so their order is the input order.
func forEach[T any](ctx context.Context, limit int, items []T, fn func(ctx context.Context, index int, item T) error) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(max(limit, 1))
	for index, item := range items {
		group.Go(func() error { return fn(groupCtx, index, item) })
	}
	return group.Wait()
}
