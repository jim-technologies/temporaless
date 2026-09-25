package inspection

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

// Bucket is the read-only object access inspection needs beyond the point
// store: bounded, non-recursive listings and raw reads. Paths are storage keys
// relative to the store root; directory paths end with "/".
type Bucket interface {
	// List returns the direct children of dir, sorted, with directory
	// children ending in "/". A missing directory lists as empty.
	List(ctx context.Context, dir string) ([]string, error)
	// Read returns an object's bytes. found is false when it does not exist.
	Read(ctx context.Context, path string) (data []byte, found bool, err error)
}

// OpenDALBucket adapts an OpenDAL operator to Bucket. It never writes.
type OpenDALBucket struct {
	operator *opendal.Operator
}

// NewOpenDALBucket wraps an operator. Give it read-only credentials: the
// inspector never needs more.
func NewOpenDALBucket(operator *opendal.Operator) *OpenDALBucket {
	return &OpenDALBucket{operator: operator}
}

// List implements Bucket.
func (bucket *OpenDALBucket) List(ctx context.Context, dir string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lister, err := bucket.operator.List(dir)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	var children []string
	for lister.Next() {
		if err := ctx.Err(); err != nil {
			_ = lister.Close()
			return nil, err
		}
		path := lister.Entry().Path()
		if path == dir {
			continue
		}
		children = append(children, path)
	}
	closeErr := lister.Close()
	if err := lister.Error(); err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	sort.Strings(children)
	return children, nil
}

// Read implements Bucket.
func (bucket *OpenDALBucket) Read(ctx context.Context, path string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	data, err := bucket.operator.Read(path)
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return data, true, nil
}

func isNotFound(err error) bool {
	var openDALErr *opendal.Error
	return errors.As(err, &openDALErr) && openDALErr.Code() == opendal.CodeNotFound
}

// walkFiles lists every ".binpb" object under root, depth first, sorted. It
// stops with errTooManyObjects once more than limit objects are seen, so an
// unexpectedly large prefix fails loudly instead of listing without bound.
func walkFiles(ctx context.Context, bucket Bucket, root string, limit int) ([]string, error) {
	var files []string
	queue := []string{root}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		children, err := bucket.List(ctx, current)
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			switch {
			case strings.HasSuffix(child, "/"):
				queue = append(queue, child)
			case strings.HasSuffix(child, ".binpb"):
				files = append(files, child)
				if len(files) > limit {
					return nil, fmt.Errorf("%w: more than %d objects under %s", errTooManyObjects, limit, root)
				}
			}
		}
	}
	sort.Strings(files)
	return files, nil
}

var errTooManyObjects = errors.New("listing exceeds the inspection bound")

// The flat v2 key layout, constructed from validated key fields exactly as
// the core store writes it (AGENTS.md "Storage"). Inspection never parses
// these paths back into identity: it reads each payload and checks that the
// payload's own key constructs the path it was read from.

func namespaceRoot(namespace string) string {
	return fmt.Sprintf("%s/%s/", storage.StorageRootPrefix, namespace)
}

func latestDir(namespace string) string {
	return namespaceRoot(namespace) + "_latest/"
}

func latestPath(namespace, workflowID string) string {
	return fmt.Sprintf("%s%s.binpb", latestDir(namespace), workflowID)
}

func workflowDir(namespace, workflowID string) string {
	return fmt.Sprintf("%s%s/", namespaceRoot(namespace), workflowID)
}

func dueDir(namespace string) string {
	return namespaceRoot(namespace) + "_due/"
}

func dueInvalidDir(namespace string) string {
	return namespaceRoot(namespace) + "_due_invalid/"
}

func duePath(key storage.TimerKey) string {
	return fmt.Sprintf("%s%s/%s/%s.binpb", dueDir(key.Namespace), key.WorkflowID, key.RunID, key.TimerID)
}

// validateScopeSegments rejects a namespace or workflow ID that could not
// appear in a valid WorkflowKey, before either reaches a listing path.
func validateScopeSegments(namespace, workflowID string) error {
	probe := storage.WorkflowKey{Namespace: namespace, WorkflowID: workflowID, RunID: "placeholder"}
	if probe.WorkflowID == "" {
		probe.WorkflowID = "placeholder"
	}
	return probe.Validate()
}
