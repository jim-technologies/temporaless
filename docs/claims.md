# Claims And Leases

Temporaless should not require Redis, ZooKeeper, or a database lock table. The default coordination primitive should be storage-native conditional writes.

## Position

Claims are an optimization for reducing duplicate work. They are not the source of correctness.

Correctness must come from:

- stable workflow, activity, and timer IDs
- protobuf input digests
- stored completed records
- idempotent external side effects

Claims reduce the chance that two serverless invocations perform the same missing work at the same time. If claims are unavailable, Temporaless should still work, but activities that cause external side effects must use domain idempotency keys.

Temporaless does not generate claim owner IDs. Serverless handlers should pass an explicit invocation, request, or worker identity when they opt into claims.

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
temporaless/v1/namespaces/{namespace}/workflows/{workflow_id}/runs/{run_id}/claims/{claim_id}.binpb
```

Recommended fields:

- `claim_id`: stable ID, for example `activity:fetch:aapl` or `timer:wait:vendor-window`
- `owner_id`: caller-provided invocation, request, or worker ID
- `resource_type`: `workflow`, `activity`, or `timer`
- `resource_id`: workflow ID, activity ID, or timer ID
- `code_version`
- `input_digest`
- `lease_expires_at`
- `created_at`
- `heartbeat_at`

## Lease Lifecycle

The complete lifecycle needs more than create-if-absent:

1. Try to create the claim object if absent.
2. If it succeeds, this invocation owns the claim.
3. If it fails and the completed record now exists, replay the completed record.
4. If it fails and the claim is still active, return a typed busy/pending error.
5. If the claim is expired, takeover requires compare-and-swap against the claim version, generation, or ETag.

The important detail is step 5. A stale claim cannot be safely overwritten with a normal write, because two invocations can both observe expiry and both overwrite. Takeover must be conditional on the version that was read.

## Backend Behavior

Backends should be classified by capability:

- `CLAIM_CAPABILITY_NO_CLAIMS`: no atomic claim support. Execute idempotently.
- `CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS`: can create-if-absent but cannot safely take over stale claims. Useful only for short leases where manual cleanup is acceptable.
- `CLAIM_CAPABILITY_CAS_CLAIMS`: can create, refresh, release, and take over with generation/ETag preconditions. This is the production target.

Temporaless should prefer `CLAIM_CAPABILITY_CAS_CLAIMS` when an adapter can provide it. Claim capability is a protobuf enum in `temporaless.v1`, not a language-local string.

## OpenDAL Status

The current Go OpenDAL binding exposes simple `Write` and does not expose conditional write options. The Python binding exposes `if_not_exists`, but we should not build an asymmetric core around that.

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

The core should handle claim availability gracefully:

- If the store supports claims, acquire before executing missing workflow/activity work.
- If a completed record appears after a failed claim create, replay the completed record.
- If the claim exists and no completed record exists, return a typed pending/busy error so serverless can retry later.
- If a create-only claim is expired, still return busy. Expiry is not ownership. Takeover requires `CASClaims`.
- If the store does not support claims, execute normally and rely on stored records plus idempotency.

Do not implement check-then-write locking. It is not atomic and creates false confidence.

## SQL

SQL can be useful for scheduler indexes and due-work queries, but it should not be required for claims. If introduced, SQL should be an adapter:

- object storage remains the durable record source
- SQL indexes claim/timer/schedule metadata for efficient lookup
- SQL can be rebuilt from protobuf records if needed

This keeps Temporaless serverless-first while allowing larger deployments to add operational efficiency.
