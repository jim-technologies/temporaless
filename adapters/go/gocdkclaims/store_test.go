package gocdkclaims

import (
	"context"
	"errors"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStoreTryCreateClaim(t *testing.T) {
	tests := []struct {
		name        string
		firstOwner  string
		secondOwner string
		wantFirst   bool
		wantSecond  bool
		wantOwner   string
	}{
		{
			name:        "first writer owns claim",
			firstOwner:  "owner-one",
			secondOwner: "owner-two",
			wantFirst:   true,
			wantSecond:  false,
			wantOwner:   "owner-one",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			bucket := newFileBucket(t)
			store := NewStore(bucket)
			key := storage.ClaimKey{
				WorkflowID: "prices:aapl",
				RunID:      "2026-05-02",
				ClaimID:    "activity:fetch:price",
			}

			first, err := store.TryCreateClaim(ctx, newClaimRecord(key, test.firstOwner))
			if err != nil {
				t.Fatal(err)
			}
			if first != test.wantFirst {
				t.Fatalf("first create = %t, want %t", first, test.wantFirst)
			}

			second, err := store.TryCreateClaim(ctx, newClaimRecord(key, test.secondOwner))
			if err != nil {
				t.Fatal(err)
			}
			if second != test.wantSecond {
				t.Fatalf("second create = %t, want %t", second, test.wantSecond)
			}

			got, found, err := store.GetClaim(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("expected stored claim")
			}
			if got.GetOwnerId() != test.wantOwner {
				t.Fatalf("owner = %q, want %q", got.GetOwnerId(), test.wantOwner)
			}
		})
	}
}

func TestStoreGetClaim(t *testing.T) {
	tests := []struct {
		name   string
		create bool
		found  bool
	}{
		{
			name:   "found",
			create: true,
			found:  true,
		},
		{
			name:   "missing",
			create: false,
			found:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			bucket := newFileBucket(t)
			store := NewStore(bucket)
			key := storage.ClaimKey{
				WorkflowID: "prices:aapl",
				RunID:      "2026-05-02",
				ClaimID:    "activity:fetch:price",
			}

			if test.create {
				created, err := store.TryCreateClaim(ctx, newClaimRecord(key, "owner-one"))
				if err != nil {
					t.Fatal(err)
				}
				if !created {
					t.Fatal("expected claim create")
				}
			}

			_, found, err := store.GetClaim(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if found != test.found {
				t.Fatalf("found = %t, want %t", found, test.found)
			}
		})
	}
}

func TestStoreTryCreateClaimRejectsWrongSchemaBeforeWrite(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	store := NewStore(bucket)
	key := storage.NewClaimKey("prices:aapl", "run", "claim")
	record := newClaimRecord(key, "owner")
	record.SchemaVersion = storage.ActivityRecordSchemaVersion

	created, err := store.TryCreateClaim(ctx, record)
	if created || !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("created=%v err=%v, want false/ErrCorruptRecord", created, err)
	}
	path, err := key.Path()
	if err != nil {
		t.Fatal(err)
	}
	exists, err := bucket.Exists(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("invalid claim was written")
	}
}

func TestStoreListClaimsScopesToOneRun(t *testing.T) {
	ctx := context.Background()
	store := NewStore(newFileBucket(t))
	target := storage.NewWorkflowKey("prices:aapl", "run:one")
	keys := []storage.ClaimKey{
		storage.NewClaimKey(target.WorkflowID, target.RunID, "arbitrary:one"),
		storage.NewClaimKey(target.WorkflowID, target.RunID, "arbitrary:two"),
		storage.NewClaimKey(target.WorkflowID, "run:other", "arbitrary:other"),
	}
	for _, key := range keys {
		created, err := store.TryCreateClaim(ctx, newClaimRecord(key, "owner"))
		if err != nil {
			t.Fatal(err)
		}
		if !created {
			t.Fatalf("claim %q was not created", key.ClaimID)
		}
	}

	records, err := store.ListClaims(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool, len(records))
	for _, record := range records {
		got[record.GetKey().GetClaimId()] = true
	}
	if len(got) != 2 || !got["arbitrary:one"] || !got["arbitrary:two"] {
		t.Fatalf("claim ids = %v, want arbitrary:one and arbitrary:two", got)
	}
}

func newFileBucket(t *testing.T) *blob.Bucket {
	t.Helper()

	// MetadataDontWrite suppresses fileblob's `.attrs` JSON sidecar — that
	// sidecar's write is not gated by IfNotExist, so a racing GetClaim mid-
	// write reads a truncated file and gets io.EOF from the JSON decoder.
	// Claim records don't carry GoCDK metadata, so dropping the sidecar is
	// free. Production deployments use real cloud drivers with native
	// preconditions and don't hit this path.
	bucket, err := fileblob.OpenBucket(t.TempDir(), &fileblob.Options{
		Metadata: fileblob.MetadataDontWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return bucket
}

func newClaimRecord(key storage.ClaimKey, ownerID string) *temporalessv1.ClaimRecord {
	now := timestamppb.Now()
	return &temporalessv1.ClaimRecord{
		SchemaVersion:  storage.ClaimRecordSchemaVersion,
		Key:            key.Proto(),
		OwnerId:        ownerID,
		ResourceType:   temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY,
		ResourceId:     "fetch:price",
		CodeVersion:    "test",
		LeaseExpiresAt: timestamppb.New(time.Now().Add(time.Minute)),
		CreatedAt:      now,
		HeartbeatAt:    now,
	}
}
