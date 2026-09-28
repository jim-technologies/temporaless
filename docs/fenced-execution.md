# Fenced execution contract foundation

Fenced execution is **unavailable in this release**. The additive protobuf
contract reserves a complete atomic run-ownership boundary; it does not turn
create-only claims into recoverable leases. No bundled store, server or runtime
advertises `FENCED_EXECUTION_CAPABILITY_ATOMIC_RUN_MUTATIONS`. The five new
RecordStore RPCs return `UNIMPLEMENTED`. Go, Python and experimental Rust reject
`WorkflowOptions.fenced_execution` before reading records or executing a body.
Missing capability fields from older servers mean unsupported; the Go, Python
and TypeScript clients also reject unknown and reserved future capabilities.

The canonical semantics are in
[`temporaless.proto`](../api/temporaless/v1/temporaless.proto). This document
records implementation status, the mutation audit and the qualification required
before enabling that contract. Existing unclaimed/create-only v2 stores retain
their current semantics and binary record layout. This phase changes no stored
records and needs no storage migration. Rollback is a code rollback; callers must
remove the new option because older runtimes cannot enforce it.

Wire additions preserve existing request/record encodings, but this is not a
claim of universal source compatibility. Regenerated service interfaces include
five more methods; custom implementations must provide explicit unsupported
handlers or use the generated unimplemented base where available. Experimental
Rust's public `WorkflowOptions` gains a field: constructor users are unaffected,
but existing struct literals must set `fenced_execution: None`. Exhaustive Rust
matches must also handle the new unsupported `RunError` variant.

## Why a conditional claim is insufficient

Current record stores put and delete workflow, activity, timer and event records
without an execution authority token. Create-if-absent claims prevent two live
owners from creating the same claim, but cannot recover an expired holder safely:
an unconditional delete permits a paused owner to delete its successor's claim
and publish stale results. A separate lease read followed by an unconditional
record write has the same race. Per-process locks and scheduler overlap policies
do not serialize different processes or independently submitted jobs.

The Go and Python timer stores also maintain a due-ledger shadow before the
canonical timer. `due_timers`/`DueTimers` repair canonical records and delete
canceled ones outside the runner. These are mutations that require fencing,
even though their callers look like query/scanner code. Latest-run pointers and
optional query indexes are derived views, not a second authority.

Per-object conditional writes cannot validate a lease in one object atomically
with a change to another object. A snapshot SQL executor or an in-memory mutex
cannot supply a distributed transaction boundary either. A future backend must
prove its actual durable transaction and uncertain-outcome behavior.

## Reserved ownership and mutation boundary

`FencedExecutionOptions` names a caller-supplied owner and unique acquisition
identity and requests a positive lease duration. `ExecutionToken` binds those
identities to a complete workflow key, a durable store incarnation and a
monotonically increasing generation. Matching an owner name grants no reentry.
Store time governs expiry. Release/deletion retain generation tombstones;
retirement prevents the same run identity from executing again. Generation
exhaustion fails closed. Transport authorization remains separate from the token.

`AcquireExecution` conditionally acquires an unowned run or takes over an expired
holder. A live holder is busy. `RenewExecution` and `ReleaseExecution` compare the
exact still-live token; they cannot recreate expired authority. Acquisition also
binds the expected store incarnation returned by capabilities, so a recreated
store cannot impersonate the store that may have accepted an uncertain request.

`ApplyExecutionMutations` validates that token and its expiry in the **same atomic
boundary** as all changes and the operation receipt. It carries at most 100 typed
puts/deletes over existing records and keys, with protobuf validation requiring
every key to belong to the token's namespace/workflow/run. Stores must still
perform existing record validation and may enforce an additional byte bound.
Terminal results are immutable. Tokenless legacy mutations must refuse protected
runs; wrapping the new RPC around legacy unconditional puts is insufficient.

Each acquisition, renewal, release and mutation batch has a caller-supplied
operation identity. `ExecutionOperationReceipt` binds the run, incarnation,
validated typed request digest and complete result in the same commit. The server
computes that digest. Deterministic protobuf serialization is not cross-language
canonical encoding; clients treat the returned digest as an opaque receipt binding,
not something they precompute. The store must preserve its request comparison and
digest normalization within an incarnation. Reusing
an identity with different contents is a conflict. `GetExecutionOperation` only
reads a past receipt: it neither reexecutes work nor grants current authority.
Missing receipts do not prove that an interrupted request cannot still commit.
A caller cannot replace an uncertain operation identity and blindly retry. A
recovered old acquisition must establish current authority before starting work.

A stale holder must receive a coordination error for every mutation. It must not
convert lease loss into a FAILED workflow, retry timer, annotation, or unconditional
claim release. Cancellation is useful for stopping new work, but the backend's
atomic check supplies the stale-writer exclusion when a paused process resumes.

External `DeliverEvent` remains a distinct producer operation: immutable create,
idempotent replay, or conflict. A future fenced implementation must serialize
its run-incarnation/retirement check and event insertion against the same fence.
It must not require producers to impersonate the executing owner. Execution-owned
event replacement/deletion does require the token.

Retention needs a separate reviewed authority: atomically retire an eligible
terminal, unowned run, excluding acquire/delivery/mutation, then remove children
idempotently while retaining the generation tombstone. Legacy `DeleteRun` and
`Sweep` must refuse protected runs until that path exists. Removing claims first
would reopen an ownership race.

## Mutation audit and remaining implementation

| Path | Required future implementation |
| --- | --- |
| Go/Python initial, annotation and terminal workflow writes | A run-scoped execution session holds authority through publication; read-only terminal replay stays read-only. |
| Activity completion, failure, retry state and retry timers | The same token covers every write and durable receipt, including child workflow contexts. |
| Retry preparation/inherited records | Acquire target authority before the first seed; use idempotent bounded batches and a completion marker before execution. Source terminal history is immutable. |
| Sleep, polling, wake consumption and event mutations | Carry the session token through storage/cache paths; no direct legacy-store escape. |
| Due-ledger publication, scanner repair and cancellation cleanup | Atomic fenced canonical transitions; derived discovery must never revive stale state. |
| Latest pointers and optional scheduler/query indexes | Consume only committed record revisions and reject stale repair; they cannot grant authority. |
| Connect store handlers and clients | Forward typed requests/results without client-side read/check/write; reject tokenless mutations of protected runs server-side. |
| Janitor, `DeleteRun`, indexed sweeps and direct deletes | Retire under exclusion, retain tombstones, then perform idempotent cleanup. |
| Experimental Rust | Implement the complete session/storage boundary before claiming support; current direct OpenDAL runtime explicitly refuses it. |
| TypeScript | Client only; generated RPC types are available but no runtime or support is implied. |

The new Go `storage.FencedExecutionStore` and Python `execution.FencedExecutionStore`
interfaces reserve that typed seam. **No implementation or execution session is
shipped in this phase.** A later shared-runtime change must thread authority through
all paths above, refresh caches per acquisition, and handle coordination errors
without stale cleanup. Combining fencing with old create-only concurrency slots
is rejected until their interaction has a separately qualified contract.

## Qualification boundaries

The current gate uses one shared Go/Python protobuf fixture corpus to check
valid envelopes, missing identities, lease bounds, receipt result/identity binding,
all eight mutation variants, cross-run and cross-namespace rejection, and bounded
batches. Runtime tests prove refusal before record access/body execution, including
when old claim options are present. Transport tests prove the reserved methods do
not fall through to the legacy store; capability tests cover old, unsupported,
reserved and unknown responses. These prove the **disabled foundation**, not a
distributed locking algorithm.

Before any backend/runtime advertises atomic fencing, qualification must include:

- Two independent engines and storage clients: exactly one live acquire winner.
- Kill an owner, expire its lease, race two takeovers, and resume from committed
  activities/deadlines without resetting checkpoints.
- Pause A, take over with B, then resume A. Every workflow/activity/timer/event
  put/delete, retry seed, scanner repair, terminal write, renew and release from A
  fails while B's records and authority remain unchanged. Reusing the same owner
  name must not bypass generation/acquisition checks.
- Lose responses before and after acquire, renewal, mutation and release commits.
  Recover exact outcomes from receipts without creating new operation identities;
  recovered old leases cannot grant authority.
- Race timer scanning and event ingress against takeover/retirement; test stale
  caches, wrong-run tokens, generation exhaustion, legacy tokenless operations,
  partial retry seeding and missing/unknown capabilities.
- Prove transaction rollback and accepted-history recovery using the actual
  distributed backend, separate processes and fresh local caches. A process-local
  fake can test orchestration but cannot qualify the capability.

Durable fencing excludes stale **publications**, not arbitrary provider effects.
An old owner may finish an already-started provider call after expiry. Applications
remain responsible for side-effect idempotency, reconciliation, provider fencing
where available, and quota ledgers. There is no exactly-once provider guarantee.

## Later migration and rollback

The next phases are a complete shared runtime session, an independently qualified
durable backend adapter, then explicit application wiring and migration. Keep
application-specific clients and relational schemas outside the generic core.
Do not silently reinterpret old claims or dual-write two equal authorities.

Before new-store writes, rollback restores the old config/image and its existing
serialization. After new writes, drain all writers and validate a reverse export
of the newest accepted records, retry lineage and timer deadlines before returning
to v2 storage. An old snapshot would discard progress. Keep both stores through
the reviewed rollback window; quota/accounting state needs its own reconciliation.
No automatic downgrade or data deletion is part of this foundation.
