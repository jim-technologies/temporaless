# outbox

`outbox.IdempotencyKey(workflow, activity_id)` derives a stable string from
`(namespace, workflow_id, run_id, activity_id)` that activity bodies pass to
external systems as a vendor-side dedup key.

The framework guarantees activity replay against storage: a stored
`COMPLETED` activity returns its result without re-executing. But a process
death **between** the side-effect and the result-write means the activity
runs again on the next invocation. Storage replay can't deduplicate against
vendor state. The outbox key is how the activity body delegates that to the
vendor.

## Usage patterns

### HTTP — `Idempotency-Key` header

Stripe / Slack / OpenAI / GitHub all accept an `Idempotency-Key` request
header. Same key → vendor returns the original response, no double-charge:

```go
func charge(ctx context.Context, req *ChargeRequest) (*ChargeResponse, error) {
    wf, _ := workflow.Current(ctx)
    key := outbox.IdempotencyKey(wf, "charge:invoice-42")
    return stripe.Charges.New(req.ToStripe(), stripe.WithHeader("Idempotency-Key", key))
}
```

### Database upsert

Make the row's natural key the framework-derived idempotency key (or include
it as a unique column):

```sql
INSERT INTO inference_results (idempotency_key, model, output, ...)
VALUES ($1, $2, $3, ...)
ON CONFLICT (idempotency_key) DO NOTHING;
```

```go
key := outbox.IdempotencyKey(wf, "infer:gpt-4:prompt-7")
_, err := db.Exec(ctx, query, key, "gpt-4", output, ...)
```

### Object storage — deterministic object name

```go
key := outbox.IdempotencyKey(wf, "snapshot:2026-05-11")
_, err := s3.PutObject(ctx, &s3.PutObjectInput{
    Bucket: aws.String("snapshots"),
    Key:    aws.String(key + ".parquet"),
    Body:   reader,
})
```

S3 PutObject overwrites the same key with the same content idempotently.

## Caveats

- **Deploying new handler code doesn't rotate the key.** Completed activity
  records remain authoritative across deployments. If a previous result is
  invalid, rotate the `activity_id` or `run_id` so both replay identity and
  the vendor-side key change.
- **Per-activity, not per-attempt.** All retry attempts (including durable
  resumes after a `TIMER_KIND_ACTIVITY_RETRY`) share the same key. This is
  the textbook strict-idempotency pattern: a vendor that charged the card
  on the first attempt returns the same response on every retry instead of
  charging again. If you want fresh execution on retry, this helper isn't
  the right primitive.
- **Vendor must support idempotency.** This helper provides a key; the
  vendor either honors it or doesn't. For vendors that don't, fall back to
  natural keys (DB upsert, S3 object name).

## Format

`temporaless-{32-hex-chars}` — fixed-width regardless of input ID length.
The 32 hex chars come from the first 16 bytes of `SHA-256("{ns}|{wf}|{rid}|{aid}")`.

Python equivalent: `temporaless.outbox.idempotency_key(workflow, activity_id)`
(same algorithm, same output for the same inputs).
