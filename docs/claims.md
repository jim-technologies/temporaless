# Claims And Leases

Temporaless should not require Redis, ZooKeeper, or a database lock table. The default coordination primitive should be storage-native conditional writes.

## Position

Claims are an optimization for reducing duplicate work. They are not the source of correctness.

Correctness must come from:

- stable workflow, activity, and timer IDs
- stored completed records keyed by those IDs
- idempotent external side effects

Claims prevent cooperating serverless invocations from performing the same missing work at the same time. They do not make an external side effect exactly-once: a process can still complete the side effect and crash before storing its result. If claims are unavailable or disabled, Temporaless still runs with at-least-once execution, and activities that cause external side effects must use domain idempotency keys.

Temporaless does not generate claim owner IDs. Serverless handlers pass an explicit request, invocation, or logical-worker identity through `WorkflowOptions.claim_owner_id` when they opt into workflow, activity, and concurrency-slot claims. The value is diagnostic; matching an existing owner never grants re-entry because two live calls can reuse the same identity. `concurrency_key` therefore requires a non-empty caller-owned `claim_owner_id`.

## Preferred Primitive

The preferred claim operation is atomic create-if-absent:

```text
create claim object only if the object does not already exist
```

Examples:

- Google Cloud Storage: write with `ifGenerationMatch=0`.
- Amazon S3: write with `If-None-Match: *`.

This fits serverless well because object storage is already part of the runtime, and billing remains pay-as-you-go.

## Claim Record

Claim records should be protobuf binary like all other framework state.

Recommended path:

```text
temporaless/v2/{namespace}/{workflow_id}/{run_id}/claim/{claim_id}.binpb
```

Recommended fields:

- `claim_id`: stable ID, for example `workflow:execution` or `activity:fetch:aapl`
- `owner_id`: caller-provided invocation, request, or worker ID
- `resource_type`: `workflow`, `activity`, or `timer`
- `resource_id`: workflow ID, activity ID, or timer ID
- `code_version`
- `lease_expires_at`
- `created_at`
- `heartbeat_at`

## Workflow Execution Single-Flight

Setting `WorkflowOptions.claim_owner_id` opts a run into storage-backed single-flight execution. A claim store is then required. Before entering a missing or `IN_PROGRESS` run, the runtime atomically creates this deterministic claim:

```text
temporaless/v2/{namespace}/{workflow_id}/{run_id}/claim/workflow:execution.binpb
```

The surrounding key supplies the workflow and run identity; the claim record uses `CLAIM_RESOURCE_TYPE_WORKFLOW`. An existing execution claim is busy even when it has the same `owner_id`, because two live requests may reuse one logical-worker identity.

The runtime follows this order:

1. Read the workflow record. A terminal `COMPLETED` or `FAILED` record replays immediately, even if an old claim remains.
2. Atomically create `workflow:execution` before writing a new `IN_PROGRESS` record or re-entering an existing one.
3. If creation loses, re-read the workflow. Replay it if it became terminal; otherwise return `ClaimBusyError`, mapped to ConnectRPC `ALREADY_EXISTS`.
4. Release the execution claim on every orderly return, including completion, failure, timer/event/dependency pending, claim pending, and cancellation. If cleanup fails, surface that storage error. A terminal record that was already persisted remains replayable despite the stale claim; a nonterminal run requires verified operator cleanup before it can resume.

If `claim_owner_id` is empty, workflow execution claims are disabled. Terminal records still replay, but overlapping calls to a missing or `IN_PROGRESS` run may both enter the workflow body. That is the portable at-least-once mode.

## Activity Claim Lifecycle

The same opt-in creates `activity:{activity_id}` before executing missing or due `RETRYING` activity work. Every existing activity claim is busy, including one with the same owner. Claim arbitration bypasses the replay cache and re-reads the authoritative activity record, so a loser can replay a winner's terminal record instead of returning a stale busy result.

Activity claims are released only after a durable boundary:

- a `COMPLETED` or `FAILED` activity record was stored
- a validated durable retry timer was stored (the following `RETRYING` record
  may lag; replay honors the timer and repeats ambiguous work at-least-once)
- an in-process retry state was stored and the invocation exits during its backoff

They are deliberately retained when a successful side effect may be ambiguous:
cancellation while the activity body is running, terminal-result persistence
failure after the body returns, or failed claim cleanup. This is fail-closed.
An operator must first verify that no old execution remains before deleting
such a claim. Retry-boundary persistence failures instead surface typed pending
and release the claim so request redelivery can repair the timer/record pair;
once a timer is verified, another invocation honors its wake before repeating
ambiguous work.

## Lease Lifecycle

The complete lifecycle needs more than create-if-absent:

1. Try to create the claim object if absent.
2. If it succeeds, this invocation owns the claim.
3. If it fails and a terminal workflow/activity record now exists, replay that record.
4. If it fails and the claim is still active, return a typed busy/pending error.
5. If the claim is expired, takeover would require compare-and-swap against the claim version, generation, or ETag.

The important detail is step 5. A stale claim cannot be safely overwritten with a normal write, because two invocations can both observe expiry and both overwrite. The current runtime does not implement CAS takeover; it always treats an existing create-only claim as busy.

## Backend Behavior

Backends should be classified by capability:

- `CLAIM_CAPABILITY_NO_CLAIMS`: no atomic claim support. Options that require claims (`claim_owner_id` or `concurrency_key`) are rejected; omit them to execute idempotently with at-least-once semantics.
- `CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS`: can create-if-absent but cannot safely take over stale claims. Useful where orderly release is expected and manual cleanup after crashes is acceptable.
- `CLAIM_CAPABILITY_CAS_CLAIMS`: reserved for adapters that can create, refresh, conditionally release, and take over with generation/ETag preconditions.

Claim capability is a protobuf enum in `temporaless.v1`, not a language-local string. The current core claim surface provides create/get/delete semantics only and performs no CAS refresh or takeover, so `CLAIM_CAPABILITY_CAS_CLAIMS` is future-only today.

## OpenDAL Status

The Python OpenDAL binding exposes `if_not_exists`, so Temporaless Python has a create-only claim helper:

```python
store.try_create_claim(record)
```

It writes the protobuf `ClaimRecord` with `if_not_exists=True`, returns `True` when this invocation acquired the claim, and returns `False` when the claim object already exists.

The current Go OpenDAL binding exposes simple `Write` and does not expose conditional write options. Go support should wait for one of these:

- OpenDAL Go exposes `if_not_exists` / conditional write.
- an adjacent adapter uses a storage API that exposes native object-store preconditions.

Until OpenDAL exposes conditional write/CAS consistently in Go and Python, stronger claim implementations should live in backend-specific adapters such as:

- `adapters/gcsstore`
- `adapters/s3store`

These adapters can still use protobuf records and the same storage paths.

## Go Note

Do not implement Go claims with `IsExist` followed by `Write`. That is not atomic.

For Go, Temporaless has `adapters/gocdkclaims`, a narrow GoCDK-backed create-only claim adapter. It uses `blob.WriterOptions.IfNotExist`, which maps to native preconditions in supported GoCDK drivers such as GCS `DoesNotExist` and S3 `If-None-Match: *`.

This adapter is intentionally not the default record store. OpenDAL remains the primary storage layer; GoCDK is present only where Go needs conditional create support today.

Acceptable Go claim implementations are:

- `adapters/gocdkclaims`: create claim with GoCDK `IfNotExist`
- `adapters/gcsstore`: create claim with GCS `ifGenerationMatch=0`
- `adapters/s3store`: create claim with S3 `If-None-Match: *`
- future generic OpenDAL implementation when the Go binding exposes conditional writes

## Core Behavior

The core handles claim availability as follows:

- A non-empty `claim_owner_id` enables workflow-execution and activity claims and supplies the owner for any configured concurrency slot; merely supplying a claim store does not opt in. `concurrency_key` without it is invalid.
- Acquire `workflow:execution` before entering missing or `IN_PROGRESS` workflow work.
- Preflight `ClaimCapability`; reject requested coordination unless the store reports create-only or CAS capability. This prevents a remote store with no claim backend from silently degrading single-flight to at-least-once execution.
- If a terminal workflow record appears after a failed claim create, replay it.
- If the claim exists and the workflow is not terminal, return `ClaimBusyError` / `ALREADY_EXISTS`.
- Delete the workflow execution claim after every orderly invocation exit so timers, events, and retries can resume through a later invocation. A delete failure is returned to the caller and leaves the claim for operator recovery.
- Delete activity claims at persisted terminal/retry boundaries; retain them on ambiguous execution or persistence outcomes.
- Expose bounded `ListClaims(workflow_key)` only for recursive run deletion and retention. `DeleteRun` and claim-aware sweep remove claims before the other run records and reject a claim backend that cannot enumerate its run-scoped claims; cross-run claim search remains outside core. Both cleanup paths require external quiescence because listing plus point deletion is not a transaction or execution fence.
- Quiesce a run before `DeleteRun`. Run-scoped listing plus point deletion is intentionally not transactional with a new claim created after the listing snapshot.
- If a create-only claim survives a process crash or failed cleanup, it remains busy even after `lease_expires_at`. Expiry is not ownership; verify that no worker is live, then delete the exact claim through `ClaimStore.delete_claim` / `DeleteClaim`.
- If `claim_owner_id` is empty, execute with at-least-once semantics and rely on stored terminal records, activity records, and external idempotency.

Do not implement check-then-write locking. It is not atomic and creates false confidence.

## SQL

SQL can be useful for scheduler indexes and due-work queries, but it should not be required for claims. If introduced, SQL should be an adapter:

- object storage remains the durable record source
- SQL indexes claim/timer/schedule metadata for efficient lookup
- SQL can be rebuilt from protobuf records if needed

This keeps Temporaless serverless-first while allowing larger deployments to add operational efficiency.
