package gocdkclaims

import (
	"context"
	"fmt"
	"sync"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"google.golang.org/protobuf/proto"
)

const protobufContentType = "application/protobuf"

var _ storage.ClaimStore = (*Store)(nil)

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
		return nil, false, err
	}
	return record, true, nil
}

func (store *Store) TryCreateClaim(ctx context.Context, record *temporalessv1.ClaimRecord) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if store.bucket == nil {
		return false, fmt.Errorf("gocdk bucket is required")
	}
	if record == nil {
		return false, fmt.Errorf("claim record is required")
	}

	key := storage.ClaimKeyFromProto(record.GetKey())
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
