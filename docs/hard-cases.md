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

The runtime releases the workflow claim after every orderly workflow return. An activity claim is also released when cancellation is observed after a successful conditional create but before the activity body starts: no application work is ambiguous at that point. After body entry, the claim is released only after a persisted terminal/retry boundary; cancellation or storage failure before that boundary retains it because the activity outcome may be ambiguous. A process crash or failed delete can therefore leave a claim behind. Its lease timestamp does not authorize takeover: an expired create-only claim remains busy until an operator verifies no worker is live and deletes it. CAS refresh/takeover is not implemented by the current core.

### Backend atomicity expectations

The framework relies on the underlying storage providing **atomic record writes** — a reader either sees the previous version of a record or the next, never a partial blend. Production backends meet this:

- S3: `PutObject` is atomic; the new object becomes visible only after the upload completes. `If-None-Match: *` provides truly atomic create-if-absent.
- GCS: same guarantees via `ifGenerationMatch=0`.

The OpenDAL `fs` scheme used in tests and small-scale local deployments **does not** provide truly atomic concurrent writes — readers can observe partial files during a write. This is acceptable for development but not safe for multi-writer production use. Use S3, GCS, or another backend with native object-store semantics in production.

Python OpenDAL exposes `if_not_exists=True`, so the Python core reports
create-only claims only when the selected operator advertises
`write_with_if_not_exists`. Backends without that capability report
`NO_CLAIMS`. On `fs`, treat create-only behavior as local-development
coordination, not a distributed lock. Filesystem deployments that need
multiple writers should use a backend-specific claim adapter or keep
activities idempotent.

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

Activities accept an optional `temporaless.v1.RetryPolicy` on `ActivityOptions`. When set, the runtime retries with exponential backoff (`initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`). Short backoffs remain in-process; waits at or above `durable_backoff_threshold` use durable timers. Errors carrying a `code` listed in `non_retryable_error_codes` skip remaining retries and fail immediately.

When durable backoff is enabled, `ActivityOptions.retry_timer_id` is required
and caller-supplied. Keep it stable and unique within the workflow run, just
like `activity_id`; Temporaless never generates it.

The policy delay is derived from the failed attempt number, so replay computes the same schedule as uninterrupted execution. `ActivityFailure.retry_after` is a floor for that failure's wait only; it does not become the base of later exponential delays. The normalized effective policy is stored on every activity record, and a `RETRYING` replay rejects policy drift under the same activity identity.

Activities surface coded failures by returning `*workflow.ActivityError` (Go) or raising `workflow.ActivityError` (Python) with a stable string code. Errors without a code are still retried until exhaustion.

After each failed attempt with retries remaining, short backoffs persist
`ActivityRecord{status: RETRYING, attempts: [...so far]}` before sleeping. For
durable backoffs, the runtime publishes the caller-supplied retry timer first,
then the RETRYING record. A crash between those writes preserves a wake and may
repeat the ambiguous failed attempt at-least-once; it cannot silently strand
the workflow or bypass a future timer on immediate redelivery.

With claims enabled, a fully persisted durable retry releases its activity claim before returning pending. Any later invocation may acquire a fresh claim and resume; owner equality is never used as proof that the old attempt is gone. A busy claimant leaves the due retry timer scheduled so another invocation can consume the wakeup.

A due retry timer remains scheduled while its resumed activity attempt is
ambiguous. A new durable retry overwrites the same caller-supplied timer key;
the reciprocal `retry_activity_id` prevents accidental cross-activity reuse. A
terminal activity record is persisted before the timer is marked fired. If
timer cleanup fails, the terminal activity outcome remains authoritative and a
later terminal replay retries cleanup.

`RETRYING` replay also repairs the nontransactional timer/activity boundary.
A missing or compatible older retry timer is recreated from the normalized
policy and persisted `next_attempt_at`; a compatible newer timer published
before a lagging activity write is honored without moving the deadline
backward. Operators should therefore re-invoke a valid `RETRYING` run before
resetting it. A scanner's ledger-only wake is dispatch metadata, not a timer to
store as repair.

On exhaustion, the runtime writes `ActivityRecord{status: FAILED, failure: ..., attempts: [...]}` and surfaces the failure to the workflow. On a later workflow re-invocation the stored failure is replayed rather than re-executed; the inspector adapter's `ResetActivity` clears it for re-execution.

### Batch checkpoints

Use one stable `activity_id` per partition (for example `batch:000` through
`batch:099`). Each completed `ActivityRecord` is the checkpoint: if 90
partitions complete while 10 are still retrying, replay returns the 90 stored
results and runs only the remaining attempts. No separate checkpoint service is
required.

If those 10 exhaust their policies, the activity and workflow outcomes become
terminal. Core deliberately has no workflow-level retry policy. An operator can
retry only the failed partitions, but the reset must be quiesced and
child-first: validate and delete each failed activity's paired
`retry_timer_id` timer, delete those 10 failed activity records, then delete
the parent workflow record **last** with `inspector.ResetWorkflow` /
`reset_workflow`. Invoke once before restoring ordinary dispatch. The 90
successful activity records remain and replay without re-execution. See the
operator sequence in `docs/runbook.md`. Alternatively, model every partition
as its own workflow run when independent terminal/retry control is more useful
than one aggregate result.

## Long Running Activities

Object storage is not a worker scheduler. Long activities need leases, heartbeats, or a queue-backed execution adapter.

For market-data ingestion, prefer small activities:

- fetch one vendor and symbol batch
- normalize one batch
- write one batch

## Timers And Cron

Durable sleeps are timer records, not blocked processes. A workflow that reaches a future timer returns a pending error and must be invoked again after the timer is due.

When a due sleep resumes, its timer remains scheduled while the rest of that workflow invocation is ambiguous. The runtime marks it fired only after persisting a later wake-bearing timer or a terminal workflow record. Cancellation, claim contention, an event/dependency pending result, or a failed terminal write therefore leaves the original wakeup redeliverable. Without workflow claims this may produce overlapping at-least-once dispatches; with claims, a busy duplicate leaves the timer untouched.

Timer transitions also write one deterministic prepared object under the
bucket's compact due ledger. The object contains the full `TimerRecord` and is
written before the canonical timer point. Scanners list that ledger, repair any
missing, stale, or corrupt canonical point, and wait for a later scan to observe
both exact copies before dispatch. A `CANCELED` prepared tombstone suppresses an
older point after an interrupted delete. Scanners do not walk every workflow
run.

Consequently, scanner dispatch is at-least-once and ledger retention must
cover the maximum durable-timer horizon plus the tolerated scheduler outage
and recovery window. If timers are unbounded, exempt active run timer records
and `_due` prepared records from lifecycle expiry. Remove terminal tombstones
only offline while writers/scanners are quiesced; the generic ledger has no
online compaction mode.

Cron should be implemented as a scheduler adapter that creates workflow runs from a schedule. The bundled scheduler seeds from latest-run pointer objects written by the bucket store. SQL can be introduced as an optional query index for search and large operational views, but the core must not require it.

### Point-store scan size

Core `RecordStoreService.DueTimers` and the run-scoped activity, timer, event,
and claim lists are deliberately unpaginated. Each call materializes the
selected namespace or run in the server and client. This keeps replay and
cleanup semantics explicit, but it is not a large-backlog query engine.

Keep individual runs bounded, partition timer-heavy tenants across namespaces,
and avoid an empty-namespace `DueTimers` scan when the global ledger can grow
without bound. Very large timer backlogs should use an optional indexed
`RecordQueryService` due query or an external scheduler whose index can page
and shard independently of the core point store.

## Authorization Boundaries

A namespace is a storage partition, not an authorization boundary. The bearer
token in the production-server examples represents one trusted internal
principal and grants the example's full mounted RPC surface. It is not suitable
as a shared credential for untrusted workflow callers and operators.

Production deployments should authorize each RPC at the ConnectRPC
interceptor or gateway, issue separate least-privilege identities to workflow
runtimes and operators, and reserve reset, delete, sweep, claim cleanup, and
timer-repair methods for operator identities. External authorization remains
necessary even when the storage key itself contains a namespace.

## Determinism

Workflow code may re-run from the beginning. Activity calls must be ordered and identified consistently.

Do not generate activity IDs from wall-clock time, random values, map iteration order, or vendor response order.

## Schema Evolution

Activity inputs and outputs are protobuf messages. Backward-compatible schema evolution is expected. Breaking changes should use a new message type, new activity type, new activity ID, or new run ID.

Buf lint is part of the local quality gate. The repository also defines a Buf breaking-change policy so CI can compare future schema changes against a chosen baseline.
