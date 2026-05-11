// Package outbox derives a stable idempotency key from a workflow + activity
// identity. Activity bodies pass the returned string to external systems
// (HTTP `Idempotency-Key` header, DB upsert key, S3 object name) so retries
// against the same vendor side-effect are deduplicated.
//
// The key is deterministic over `(namespace, workflow_id, run_id, activity_id)`.
// Every retry of the same activity — in-process or after a durable wake —
// produces the same key, so a vendor that supports idempotency keys (Stripe,
// Slack, OpenAI, …) treats a retry-after-mid-flight-failure as a duplicate
// and returns the original response.
//
// This is the "strict idempotency" shape: same key across all attempts. It
// closes the gap called out in `docs/hard-cases.md` — "activity result storage
// is for replay; external side effects need their own idempotency key" — by
// deriving the key the framework already has the information to produce.
//
// Caveats:
//
//   - Bumping `code_version` does NOT change the key. If the rationale for
//     a code bump is "the previous result is invalid", also rotate the
//     activity_id (or the run_id for the whole pipeline).
//   - The key is per-activity, not per-attempt. Vendors that don't support
//     idempotency at all should rely on their natural keys (DB upsert,
//     S3 object name) instead.
//
// Usage:
//
//	func charge(ctx context.Context, req *ChargeRequest) (*ChargeResponse, error) {
//	    workflow, _ := workflow.Current(ctx)
//	    key := outbox.IdempotencyKey(workflow, "charge:invoice-42")
//	    return stripe.Charges.New(req, stripe.WithHeader("Idempotency-Key", key))
//	}
package outbox

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
)

// Prefix marks keys produced by this helper. Lets operators recognize a key
// as framework-derived when grepping vendor dashboards / DB rows.
const Prefix = "temporaless-"

// IdempotencyKey returns a stable idempotency key for the given activity
// within the workflow. Same `activity_id` + same workflow run = same key
// across all retries (including durable resumes after a TIMER_KIND_ACTIVITY_RETRY).
//
// `activity_id` must be the same value the caller passes to
// `ActivityOptions.ActivityId`. Passing a different value yields a different
// key and breaks vendor-side dedup.
func IdempotencyKey(wf *workflow.Workflow, activityID string) string {
	return Derive(storage.DefaultNamespace, wf.WorkflowID(), wf.RunID(), activityID)
}

// Derive is the pure form for callers that already have the identity tuple
// (e.g. operator scripts, tests, cross-language probes). Prefer
// IdempotencyKey from inside an activity body.
func Derive(namespace, workflowID, runID, activityID string) string {
	// `|` is not a permitted character in any framework ID (the validation
	// regex is [A-Za-z0-9._:-]), so it's an unambiguous separator.
	identity := fmt.Sprintf("%s|%s|%s|%s", namespace, workflowID, runID, activityID)
	sum := sha256.Sum256([]byte(identity))
	// 16 hex bytes (64 bits) is plenty for collision-freeness across any
	// realistic workflow population and fits comfortably in vendor key
	// length limits.
	return Prefix + hex.EncodeToString(sum[:16])
}
