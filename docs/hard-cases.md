# Hard Cases

## Concurrent Invocations

Two serverless invocations can reach the same missing or `IN_PROGRESS` workflow at the same time. Without claim coordination, both may enter the workflow body and reach the same missing activity.

A non-empty `WorkflowOptions.claim_owner_id` opts the run into single-flight execution. The runtime atomically creates the deterministic per-run `workflow:execution` claim before entering the body. A loser re-reads storage: it replays a terminal record if the winner already finished, otherwise it receives `ClaimBusyError` (`ALREADY_EXISTS`). Every existing execution claim is busy, including one with the same owner ID.

When `claim_owner_id` is empty, overlapping execution remains at-least-once and `concurrency_key` is rejected because the framework will not invent a slot owner. Completed and failed records still replay, but an `IN_PROGRESS` record is observable state, not by itself a lock.

For production, one of these must be added:

- storage backend with atomic create-if-absent claims
- CAS-capable leases with renewal and fenced takeover
- queue-based single-flight execution
- external idempotency keys for every side effect

Even with claims, activities must tolerate a crash after an external side effect but before its result record is stored. Do not use check-then-write as a lock; it is not atomic.

The runtime releases the workflow claim after every orderly workflow return and releases activity claims only after a persisted terminal/retry boundary. It retains an activity claim when cancellation or storage failure leaves the activity outcome ambiguous. A process crash or failed delete can therefore leave a claim behind. Its lease timestamp does not authorize takeover: an expired create-only claim remains busy until an operator verifies no worker is live and deletes it. CAS refresh/takeover is not implemented by the current core.

### Backend atomicity expectations

The framework relies on the underlying storage providing **atomic record writes** — a reader either sees the previous version of a record or the next, never a partial blend. Production backends meet this:

- S3: `PutObject` is atomic; the new object becomes visible only after the upload completes. `If-None-Match: *` provides truly atomic create-if-absent.
- GCS: same guarantees via `ifGenerationMatch=0`.

The OpenDAL `fs` scheme used in tests and small-scale local deployments **does not** provide truly atomic concurrent writes — readers can observe partial files during a write. This is acceptable for development but not safe for multi-writer production use. Use S3, GCS, or another backend with native object-store semantics in production.

Python OpenDAL exposes `if_not_exists=True`, so the Python core can provide
create-only claims on backends that honor that precondition. On `fs`, treat
that as local-development coordination, not a distributed lock. Filesystem
deployments that need multiple writers should use a backend-specific claim
adapter or keep activities idempotent.

The bundled `gocdkclaims` adapter wraps GoCDK's `WriterOptions.IfNotExist`. The fileblob driver implements this as Stat-then-Rename, which is racy across goroutines, so the adapter additionally serializes `TryCreateClaim` through a process-level mutex. For multi-process atomicity, again rely on S3/GCS native preconditions.

## Side Effects

Fetching market data is usually safe to repeat. Writing to a database, placing orders, sending notifications, or charging money is not.

The convention is:

- activity result storage is for replay
- external side effects need their own idempotency key
- order placement should use broker/exchange idempotency where available
- database writes should be upserts keyed by workflow and activity identity

## Activity ID Reuse

The activity_id is the de-duplication key. Reusing the same activity_id replays the stored result — including when the new input bytes differ. This is deliberate: the caller chose the id and owns its meaning. If you want the activity body to run again with different input, pick a different activity_id.

If the shape changes — request or response message types swapped — the stored `activity_type` no longer matches and the runtime raises `ErrActivityConflict` / `ActivityConflictError` so you can't silently replay against incompatible code. Bumping `code_version` is the explicit lever for "treat all old records as stale."

## Code Changes

Changing activity code can make old results invalid even when input is the same.

Production code should set `TEMPORALESS_CODE_VERSION` to an immutable build identity. Reusing the same activity ID and input under a new code version causes a mismatch. Use a new run ID or explicit reset tooling when reprocessing is intended.

## Partial Execution

The first scaffold stores completed activity results after the function returns. If the process dies after doing an external side effect but before storing the protobuf result, the activity may run again.

Production side-effect activities need an outbox or domain-specific idempotency record outside Temporaless.

## Retries

Activities accept an optional `temporaless.v1.RetryPolicy` on `ActivityOptions`. When set, the runtime retries in-process with exponential backoff (`initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`). Errors carrying a `code` listed in `non_retryable_error_codes` skip remaining retries and fail immediately.

Activities surface coded failures by returning `*workflow.ActivityError` (Go) or raising `workflow.ActivityError` (Python) with a stable string code. Errors without a code are still retried until exhaustion.

After each failed attempt with retries remaining, the runtime persists `ActivityRecord{status: RETRYING, attempts: [...so far]}` before sleeping the backoff. If the process dies during the sleep, the next invocation reads the RETRYING record and resumes from `len(attempts) + 1` rather than restarting from attempt 1 — the full attempt history is preserved across process boundaries.

With claims enabled, a fully persisted durable retry releases its activity claim before returning pending. Any later invocation may acquire a fresh claim and resume; owner equality is never used as proof that the old attempt is gone. A due retry timer is marked fired only after claim acquisition, so a busy claimant does not consume the wakeup.

On exhaustion, the runtime writes `ActivityRecord{status: FAILED, failure: ..., attempts: [...]}` and surfaces the failure to the workflow. On a later workflow re-invocation the stored failure is replayed rather than re-executed; the inspector adapter's `ResetActivity` clears it for re-execution.

## Long Running Activities

Object storage is not a worker scheduler. Long activities need leases, heartbeats, or a queue-backed execution adapter.

For market-data ingestion, prefer small activities:

- fetch one vendor and symbol batch
- normalize one batch
- write one batch

## Timers And Cron

Durable sleeps are timer records, not blocked processes. A workflow that reaches a future timer returns a pending error and must be invoked again after the timer is due.

Pending timers also write compact due-ledger entries under the bucket. Timer scanners list that ledger by sortable `fire_at`, stop when they pass `now`, and delete stale entries when timers fire or are cancelled. They do not walk every workflow run.

Cron should be implemented as a scheduler adapter that creates workflow runs from a schedule. The bundled scheduler seeds from latest-run pointer objects written by the bucket store. SQL can be introduced as an optional query index for search and large operational views, but the core must not require it.

## Determinism

Workflow code may re-run from the beginning. Activity calls must be ordered and identified consistently.

Do not generate activity IDs from wall-clock time, random values, map iteration order, or vendor response order.

## Schema Evolution

Activity inputs and outputs are protobuf messages. Backward-compatible schema evolution is expected. Breaking changes should use a new message type, new activity type, new activity ID, or new run ID.

Buf lint is part of the local quality gate. The repository also defines a Buf breaking-change policy so CI can compare future schema changes against a chosen baseline.
