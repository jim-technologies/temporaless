package gocdkclaims

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"google.golang.org/protobuf/proto"
)

const protobufContentType = "application/protobuf"

var _ storage.ClaimStore = (*Store)(nil)
var _ storage.ClaimRunStore = (*Store)(nil)

// Store wraps a GoCDK blob.Bucket and implements create-only claim semantics
// via WriterOptions.IfNotExist.
//
// A process-level mutex serializes TryCreateClaim calls within a single Store
// instance. This is necessary because some GoCDK drivers (notably fileblob)
// implement IfNotExist as Stat-then-Rename rather than a truly atomic
// primitive — so concurrent goroutines in the same process can otherwise both
// see "no file" and both succeed at writing.
//
// For multi-process or distributed atomicity, rely on the driver's native
// preconditions: S3 If-None-Match, GCS ifGenerationMatch=0, etc.
//
// # fileblob backend — pass MetadataDontWrite
//
// When wrapping `gocloud.dev/blob/fileblob` (dev/test backend), open the
// bucket with `&fileblob.Options{Metadata: fileblob.MetadataDontWrite}`.
// fileblob's default mode writes a JSON metadata sidecar (`<path>.attrs`)
// via `os.Create` BEFORE the IfNotExist precondition is checked — meaning
// every losing writer briefly truncates an existing sidecar to zero bytes,
// and a racing GetClaim that reads during that window gets `io.EOF` out of
// the JSON decoder. Claim records carry no GoCDK metadata, so the sidecar
// is pure overhead; MetadataDontWrite skips it and the entire failure mode
// disappears. Native cloud drivers (S3, GCS) use real preconditions and
// don't have this race.
type Store struct {
	bucket *blob.Bucket
	mu     sync.Mutex
}

func NewStore(bucket *blob.Bucket) *Store {
	return &Store{bucket: bucket}
}

func (store *Store) ClaimCapability(context.Context) (storage.ClaimCapability, error) {
	return storage.CreateOnlyClaims, nil
}

func (store *Store) GetClaim(ctx context.Context, key storage.ClaimKey) (*temporalessv1.ClaimRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if store.bucket == nil {
		return nil, false, fmt.Errorf("gocdk bucket is required")
	}

	path, err := key.Path()
	if err != nil {
		return nil, false, err
	}

	data, err := store.bucket.ReadAll(ctx, path)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, false, nil
		}
		return nil, false, err
	}

	record := &temporalessv1.ClaimRecord{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, false, fmt.Errorf("%w: decode claim payload at %s: %w", storage.ErrCorruptRecord, path, err)
	}
	if err := storage.ValidateClaimRecord(record, key); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

// ListClaims returns every protobuf claim record under one workflow run. The
// object names are used only to enumerate blobs; record identity always comes
// from the protobuf payload.
func (store *Store) ListClaims(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.ClaimRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store.bucket == nil {
		return nil, fmt.Errorf("gocdk bucket is required")
	}

	prefix, err := (storage.ClaimKey{
		Namespace:  key.Namespace,
		WorkflowID: key.WorkflowID,
		RunID:      key.RunID,
		ClaimID:    "placeholder",
	}).DirPath()
	if err != nil {
		return nil, err
	}

	records := make([]*temporalessv1.ClaimRecord, 0)
	iterator := store.bucket.List(&blob.ListOptions{Prefix: prefix})
	for {
		object, err := iterator.Next(ctx)
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		if object.IsDir {
			continue
		}

		data, err := store.bucket.ReadAll(ctx, object.Key)
		if err != nil {
			return nil, err
		}
		record := &temporalessv1.ClaimRecord{}
		if err := proto.Unmarshal(data, record); err != nil {
			return nil, fmt.Errorf("%w: decode claim payload at %s: %w", storage.ErrCorruptRecord, object.Key, err)
		}
		recordKey := storage.ClaimKeyFromProto(record.GetKey())
		if err := storage.ValidateClaimRecord(record, recordKey); err != nil {
			return nil, err
		}
		expectedPath, err := recordKey.Path()
		if err != nil {
			return nil, err
		}
		requested := key.Proto()
		actual := recordKey.Proto()
		if expectedPath != object.Key ||
			requested.GetNamespace() != actual.GetNamespace() ||
			requested.GetWorkflowId() != actual.GetWorkflowId() ||
			requested.GetRunId() != actual.GetRunId() {
			return nil, fmt.Errorf(
				"%w: claim payload key does not match its listed workflow run and location",
				storage.ErrCorruptRecord,
			)
		}
		records = append(records, record)
	}
}

func (store *Store) TryCreateClaim(ctx context.Context, record *temporalessv1.ClaimRecord) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if store.bucket == nil {
		return false, fmt.Errorf("gocdk bucket is required")
	}
	key := storage.ClaimKeyFromProto(record.GetKey())
	if err := storage.ValidateClaimRecord(record, key); err != nil {
		return false, err
	}
	path, err := key.Path()
	if err != nil {
		return false, err
	}

	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
	if err != nil {
		return false, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	err = store.bucket.WriteAll(ctx, path, data, &blob.WriterOptions{
		ContentType:                 protobufContentType,
		DisableContentTypeDetection: true,
		IfNotExist:                  true,
	})
	if err != nil {
		if gcerrors.Code(err) == gcerrors.FailedPrecondition || gcerrors.Code(err) == gcerrors.AlreadyExists {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DeleteClaim removes a held claim. Idempotent: returns false when the claim
// was already absent. Lock-free: the underlying bucket.Delete is atomic on
// every GoCDK driver (no fileblob race like TryCreateClaim's IfNotExist), so
// the cross-process distributed safety guarantee comes from the bucket
// directly.
func (store *Store) DeleteClaim(ctx context.Context, key storage.ClaimKey) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if store.bucket == nil {
		return false, fmt.Errorf("gocdk bucket is required")
	}
	path, err := key.Path()
	if err != nil {
		return false, err
	}
	exists, err := store.bucket.Exists(ctx, path)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	if err := store.bucket.Delete(ctx, path); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
