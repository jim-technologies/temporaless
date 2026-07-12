# Adapter Contract

Temporaless core is allowed to be opinionated. Adapters are allowed to be broader, but they must be explicit about any behavior they translate, reject, or approximate.

## Core Rule

Core behavior should be conservative and storage-first:

- one protobuf request
- one protobuf response
- explicit workflow ID
- explicit run ID
- explicit activity and timer IDs
- explicit claim owner ID when claims are enabled
- protobuf binary storage only

The core should reject ambiguous inputs instead of inventing defaults. It should not generate IDs, silently serialize arbitrary objects, or emulate a server feature with an unsafe local approximation.

## Adapter Rule

Every adapter must choose one of two positions:

- **Compatibility adapter**: match the source system behavior as closely as possible and prove it with compatibility tests.
- **Decision adapter**: intentionally diverge from the source system and document each divergence in the adapter package.

An adapter must not look like a compatibility layer while quietly changing semantics.

## Required Adapter Notes

Each adapter should document:

- source system or API it adapts
- supported behavior
- rejected behavior
- semantic gaps
- dependency choices
- storage and concurrency assumptions
- tests proving the declared behavior

For Temporal-shaped adapters, unsupported Temporal behavior should fail loudly. Examples include multiple workflow arguments, custom payload converters, non-protobuf payloads, child workflows, signals, queries, cancellation semantics, retry policies, and workflow task replay details.

## Claims

Claim adapters must declare one capability:

- `CLAIM_CAPABILITY_NO_CLAIMS`
- `CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS`
- `CLAIM_CAPABILITY_CAS_CLAIMS`

When `claim_owner_id` enables them, create-only claims prevent concurrent
workflow starts and missing activity execution, but they cannot safely take
over stale claims. A terminal workflow record replays before claim arbitration;
otherwise an existing `workflow:execution` claim is busy even for the same
owner. Completed activity records likewise replay before activity contention.
Every existing activity claim is also busy, including for the same owner.
Adapters must support run-scoped claim listing when they are used behind
`DeleteRun` or claim-aware retention sweep, so recursive deletion can remove
every claim before deleting the remaining run records.
