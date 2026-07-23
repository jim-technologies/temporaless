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
- claim and event-delivery capabilities
- tests proving the declared behavior

For Temporal-shaped adapters, unsupported Temporal behavior should fail loudly. Examples include multiple workflow arguments, custom payload converters, non-protobuf payloads, child workflows, signals, queries, cancellation semantics, retry policies, and workflow task replay details.

## Claims

Claim adapters must declare one capability:

- `CLAIM_CAPABILITY_NO_CLAIMS`
- `CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS`
- `CLAIM_CAPABILITY_CAS_CLAIMS` (reserved for a future fenced CAS interface)

Current adapters must report only `NO_CLAIMS` or `CREATE_ONLY_CLAIMS`.
Workflow, ConnectStore, deletion, and retention boundaries reject the reserved
CAS value today: the existing create/get/unconditional-delete surface cannot
honestly provide refresh, fenced release, or safe takeover.

When `claim_owner_id` enables them, create-only claims prevent concurrent
workflow starts and missing activity execution, but they cannot safely take
over stale claims. A terminal workflow record replays before claim arbitration;
otherwise an existing `workflow:execution` claim is busy even for the same
owner. Completed activity records likewise replay before activity contention.
Every existing activity claim is also busy, including for the same owner.
Adapters must support run-scoped claim listing when they are used behind
`DeleteRun` or claim-aware retention sweep, so recursive deletion can remove
every claim before deleting the remaining run records.

## Event Delivery

Event-delivery adapters must declare one capability:

- `EVENT_DELIVERY_CAPABILITY_NO_ATOMIC_CREATE`
- `EVENT_DELIVERY_CAPABILITY_CREATE_ONLY`

`UNSPECIFIED` is treated as no support. A create-only implementation must use
one backend-native conditional create; it must not emulate the boundary with a
read followed by an unconditional write. An existing valid record with the
same canonical protobuf payload is idempotent, while a different payload is a
typed conflict. Corrupt existing records are errors, never idempotent or
conflict outcomes.

`PutEvent` is a separate low-level replace primitive for operators, migrations,
and fixtures. Adapters must not silently route application `DeliverEvent`
through it.
