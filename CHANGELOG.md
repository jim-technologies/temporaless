# Changelog

All notable changes to Temporaless are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project aims to follow [Semantic Versioning](https://semver.org/) once it ships a tagged release.

The framework is **pre-1.0** — wire-format changes will be called out clearly when they happen, and there's no backwards-compatibility commitment until v1.0.0.

Starting with v0.5.0, Go, Python core, every Python adapter, Rust, and
TypeScript share the repository-root `VERSION` and ship from one plain
`vX.Y.Z` repository tag. Earlier tags remain immutable and predate this
lockstep policy.

## [Unreleased]

## [0.8.0] — 2026-07-17

### Added

- Canonical-workflow guidance now shows how an application-generated unary
  protobuf service method becomes a durable Temporaless workflow through the
  existing ConnectRPC wrappers, and how Invariant Protocol projects that same
  application descriptor into tools without introducing a second workflow
  schema or registry.
- Prefect deployment, schedule, event-trigger, async-dispatch, and
  fresh-process entrypoint coverage now proves that deployment models retain
  a concrete request as deterministic protobuf binary and reload the declared
  protobuf response boundary.
- Integration tests now execute a wrapped Python workflow through a generated
  ConnectRPC ASGI service and register both Go wrapper values with Temporal's
  real SDK test environment.

### Changed

- The lockstep repository version is now 0.8.0.
- Invariant Protocol is pinned to the immutable v0.8.1 release commit. The
  generated-service registration, Connect HTTP, MCP, CLI, and tool-catalog
  APIs used by Temporaless remain compatible.
- Orchestrator documentation now defines the shipped Temporal and Prefect
  adapters as outbound adapters for Temporaless-shaped handlers. Arbitrary
  framework-native workflows do not become Temporaless workflows through an
  import substitution.
- Dagster integration is explicitly process-isolated through the canonical
  application ConnectRPC service while Dagster 1.13.14 requires protobuf
  below version 7.

### Fixed

- Prefect deployments no longer persist protobuf inputs as display
  placeholders. The adapter validates a versioned, typed, JSON-safe envelope,
  decodes only the declared request message, preserves Prefect's sync/async
  deployment APIs, and keeps worker entrypoints importable.
- The Python getting-started guide now mounts the generated application
  service rather than incorrectly mounting Temporaless's framework
  `RecordStoreService` ASGI application as the application workflow service.
- Temporal and Prefect compatibility claims now distinguish reusable unary
  protobuf activity/business bodies from orchestration code that still needs
  a small runtime-specific rewrite.

### Upgrade notes

- Every Go, Python, Rust, and TypeScript Temporaless consumer must repin to the
  new repository tag or commit; all SDKs and adapters continue to share one
  release version.
- Prefect workflow and activity handlers must declare exactly one concrete
  protobuf request annotation and one concrete protobuf response annotation.
  Deployment callers that bypass the wrapped flow and call
  `run_deployment` directly must first use the wrapped flow's
  `serialize_parameters` method.
- There is no core protobuf schema, storage-key, or stored-record migration in
  this release.

## [0.7.3] — 2026-07-16

### Changed

- The lockstep repository version is now 0.7.3.
- Invariant Protocol is pinned to the immutable v0.8.0 commit. Its server,
  projection, registration, MCP, CLI, HTTP, and tool-catalog APIs used by
  Temporaless remain compatible; the reported Invariant server version is now
  0.8.0.

### Fixed

- Git-source CI now runs Python, Go, Rust, and npm installation checks on
  isolated runners so their large tool and dependency caches cannot exhaust a
  shared ephemeral disk or prevent later SDK checks from running.
- The Rust Git-source job checks out the exact immutable commit from the local
  bare origin and compiles every target against the repository's committed
  lockfile. This preserves the Git distribution boundary without introducing
  a fresh, externally mutable registry resolution into the release gate.

### Upgrade notes

- There are no Temporaless runtime, protobuf, storage, or SDK API changes from
  v0.7.2. Git consumers only need to repin Temporaless.
- Invariant Protocol's separate data-schema bundle format moved from IR and
  mapping version 1 to version 2. Temporaless neither consumes nor re-exports
  those data-schema APIs, so this does not require a Temporaless migration.

## [0.7.2] — 2026-07-16

### Changed

- The lockstep repository version is now 0.7.2.

### Fixed

- Git-SHA installation CI retries the Rust clean-consumer check from a fresh
  consumer directory after bounded registry or transport failures. The exact
  Temporaless commit remains immutable, and deterministic compile failures
  still fail after three attempts.

### Upgrade notes

- There are no runtime, protobuf, storage, or SDK API changes from v0.7.1.
  Git consumers only need to repin Temporaless.

## [0.7.1] — 2026-07-16

### Changed

- The lockstep repository version is now 0.7.1.
- Invariant Protocol is pinned to the immutable v0.7.1 commit. Temporaless now
  inherits its stricter MCP validation, absolute Connect deadlines and
  disconnect cancellation, bounded responses, preserved rich errors and
  metadata, and corrected remote-service reflection. The server, projection,
  registration, and tool-catalog APIs used by Temporaless remain compatible.

### Upgrade notes

- There are no Temporaless protobuf, storage, Go, Python, or runtime API
  migrations in this patch. Git consumers only need to repin Temporaless.
- TypeScript callers constructing the re-exported `JsonRpcRequest` must omit
  `id` for notifications instead of setting it to `null`, matching strict MCP
  JSON-RPC request semantics.
- `includeOperatorMethods` filters MCP, CLI, HTTP/Connect, and tool-catalog
  projections; it is not native-gRPC authorization. Secure any native gRPC
  server separately with per-RPC authorization.

## [0.7.0] — 2026-07-16

### Added

- CI now runs weekly Go, Python, npm, Rust, secret-history, and production
  container vulnerability checks. The repository also publishes a confidential
  vulnerability-reporting policy in `SECURITY.md`.
- The Go background timer scanner accepts an explicit namespace partition.
  Empty still means the all-namespace operator scope.
- Production container CI now proves the server becomes healthy with a
  read-only root filesystem, no Linux capabilities, `no-new-privileges`, and
  only explicit writable tmpfs mounts.

### Changed

- The lockstep repository version is now 0.7.0.
- The Invariant Protocol projection is read-only by default. Record mutation,
  claim coordination, timer repair, retention sweeps, and deletes require the
  explicit `includeOperatorMethods: true` opt-in and an operator-authorized
  boundary.
- Python `OpenDALStore` validates the backend's required point-operation
  capabilities at construction and advertises create-only claims only when the
  operator reports atomic `write_with_if_not_exists` support.
- Python cron catch-up generates and dispatches one fire at a time instead of
  materializing an unbounded in-memory plan after a long outage.
- The production image keeps application code and its virtual environment
  root-owned while continuing to run as UID 10001.
- Git-only Python installation CI now exercises ordinary `pip` against one
  immutable repository commit for the core and every adapter.
- Python and npm packages now carry complete license, repository, homepage,
  and issue metadata; the Rust lockfile uses Tokio 1.52.4.
- The transitional operator CLI now accurately advertises its bundled
  filesystem-only backend. Cloud operation belongs behind authenticated
  RecordStoreService / RecordQueryService clients.
- Production documentation now makes the unpaginated point-store scan boundary
  explicit: bound runs and namespace timer partitions, and use an indexed or
  external scheduler for very large backlogs.

### Fixed

- Go workflow and activity bodies that return a nil protobuf result now
  persist a terminal failure, release claims, and replay that failure instead
  of leaving an unrecorded ambiguous execution.
- Go and Python due-timer scans now surface a corrupt parent workflow as typed
  storage corruption rather than silently dropping a wake.
- Workflow annotations written before a timer, event, claim, dependency, or
  infrastructure continuation boundary now survive replay in Go and Python.
- Python production RPC logs now include the exact Connect procedure on
  successful, Connect-error, and unhandled-error outcomes.
- Go timer discovery now preserves the caller's namespace instead of always
  scanning every namespace.
- Operator JSONL exports write through a mode-`0600` temporary file and
  atomically replace the destination, avoiding symlink/hardlink clobbering and
  surfacing final sync/close failures.
- The Buf registry token is exposed only to trusted main-branch protobuf
  regeneration and is stripped from local generator subprocesses.

### Upgrade notes

- Go callers of `timerscanner.DueTimers` must pass the namespace as the fourth
  argument. Use `""` only for an intentional all-namespace operator scan.
- Invariant consumers that intentionally need mutation or retention methods
  must set `includeOperatorMethods: true` and protect that facade with
  operator-scoped authentication and per-RPC authorization.
- Python OpenDAL backends without atomic create-if-absent now report
  `CLAIM_CAPABILITY_NO_CLAIMS`; deployments that require single-flight claims
  must select a capable object backend or provide a dedicated claim adapter.

## [0.6.0] — 2026-07-16

### Added

- The TypeScript Invariant Protocol facade can now register generated
  `RecordStoreService` and `RecordQueryService` implementations directly,
  while retaining descriptor-backed Connect HTTP proxy support.

### Changed

- The lockstep development version is now 0.6.0.
- Invariant Protocol is pinned to the immutable v0.7.0 commit. The TypeScript
  facade now exposes the unified Connect `Interceptor`, `serveMcpStdio`, and
  the public MCP types. The removed upstream-internal `mcpDispatch` export and
  the old split unary/stream interceptor aliases are no longer exported;
  Invariant error codes now use the upstream `canceled` spelling.
- Go now declares 1.26.5 and selects that patched release through Go's
  checksum-verified toolchain mechanism while retaining Flox's portable
  1.26.4 bootstrap. gRPC and Genproto were promoted to their current patches.
- Python ConnectRPC was promoted to 0.11.1, every Python distribution is
  explicitly marked private/Git-only, and adapter wheels now carry `py.typed`.
- npm 12 Git installation examples now explicitly opt in to Git dependencies,
  which is required for both Temporaless and its immutable Invariant Protocol
  dependency.

### Fixed

- Go cron dispatch no longer runs while holding the scheduler state mutex, so
  callbacks can safely inspect, snapshot, seed, or restore scheduler state.
- Python treats malformed stored protobuf bytes as permanent record corruption
  rather than a transient workflow infrastructure outage.
- Python validates workflow and activity response messages against their
  declared protobuf result type before persisting completion; invalid activity
  results fail terminally instead of entering retry loops or poisoning replay.
- Python Connect-backed and cached timer listings reject records whose status
  does not match the requested filter.
- Activity-ID documentation now matches runtime semantics: the caller-owned ID
  is the de-duplication contract, even when a later invocation supplies
  different input bytes.
- Release validation now proves that the Invariant dependency, npm lock, and
  install-script allowlist use one full immutable Git SHA, and that Flox
  selects the Go patch declared by `go.mod`.

### Upgrade notes

- A latest-run pointer originally written before v0.5 has no
  `run_order_time` and is rejected by current storage validation. Before
  upgrading such a bucket, quiesce workflow writers and scanners, preserve an
  inventory of the referenced canonical workflow records, remove the affected
  derived `temporaless/v2/{namespace}/_latest/{workflow_id}.binpb` objects, and
  rebuild each pointer by re-putting the intended latest `WorkflowRecord`
  through the upgraded store. If losing scheduler seed memory is acceptable,
  the next new workflow run can recreate the pointer instead.
- `run_order_time` is part of workflow replay identity once supplied. Existing
  records that lack it must continue replaying without it, use a new run ID, or
  be migrated while quiesced from application-owned schedule metadata before
  upgraded callers begin supplying the field. Temporaless does not infer
  ordering from opaque run IDs.

## [0.5.0] — 2026-07-15

### Added

- Durable activity backoff now uses caller-supplied `retry_timer_id` values in
  both SDKs. Timer-first publication, reciprocal activity ownership, replay
  repair, and scanner redelivery keep long retries wakeable without retaining
  a worker process.
- Batch processing needs no separate checkpoint service: each stable
  `activity_id` is an independently replayable protobuf checkpoint, and
  operator reset helpers can re-run failed partitions while completed records
  remain reusable.
- Activity records now persist the normalized effective `RetryPolicy`, letting
  resumed `RETRYING` activities reject policy drift and reconstruct the same
  ordinal backoff schedule in Go and Python.
- Go bucket storage now uses the flat v2 record layout and compact due-timer
  ledger, with non-destructive malformed-entry quarantine, authoritative stale
  filtering, and all-namespace timer discovery.
- Workflow storage/coordination outages now return typed infrastructure errors
  and leave parent workflows `IN_PROGRESS`; ambiguous durable-sleep writes are
  authoritatively reread so a committed timer remains wakeable.

### Changed

- Release versioning is now lockstep across every SDK and adapter. A root
  `VERSION`, exact internal Python requirements, one-command synchronizer, and
  CI drift/tag checks replace independently managed package versions. The
  existing v0.4.0 tag remains historical; v0.5.0 is the first unified line.
- Temporaless-owned packages are now explicitly Git-only distributions. npm
  and Cargo registry publication are disabled, and repository policy excludes
  PyPI, npm, crates.io, and other language registries as release channels.
- Workflow, run, activity, retry-timer, and claim-owner identities are now
  consistently application-supplied. Runtime function-name inference and
  retry-timer ID derivation were removed.
- Latest-run ordering uses the caller's protobuf `run_order_time`, with
  lifecycle timestamps as fallback; runtime code no longer parses opaque run
  IDs.
- Go core bucket storage is now strictly point/run-scoped. Cross-run workflow
  and activity listing plus exact retention use the explicit `QueryStore`
  boundary; `adapters/go/scanquery` provides an offline/development fallback.
- Due activity-retry timers remain `SCHEDULED` while a resumed attempt is
  ambiguous. Due workflow sleeps likewise remain redeliverable until a later
  scheduled wake or terminal workflow record is durable.
- Go and the experimental Rust crate are mandatory repository/CI gates. Rust
  remains non-first-class at the product API level.
- ConnectRPC workflow-trigger wrappers now live in explicit Go/Python boundary
  adapters; core workflow replay no longer imports ConnectRPC. Prefect and
  Temporal Python wrappers likewise accept one typed options object per
  boundary instead of boundary-specific keywords or free-form decorator
  keyword arguments.
- Python now requires 3.14, Node-based development uses the Node 24 LTS line,
  Go uses the newest Flox-catalog 1.26 patch available on every supported
  platform, and the experimental Rust crate uses edition 2024 on Rust 1.97.
- Direct Go, Python, Rust, TypeScript, Buf, and container dependencies were
  promoted to their current stable ceilings. Invariant Protocol is pinned to
  the immutable v0.6.1 commit; CI actions and Python/uv container bases are
  digest/commit pinned.
- Point-store RPCs now require their embedded protobuf key/record messages;
  timer queries require `now`, and retention sweeps require a positive
  `max_age`. Invalid requests fail before storage is touched.

### Fixed

- Python timer-ledger scans now fail loudly after non-destructively quarantining
  a corrupt discovery entry, and canonical timer repair/delete failures reach
  the scheduler instead of being logged as successful empty ticks.
- Cancellation after a successful activity-claim create but before activity
  body entry now releases that unambiguous claim; post-entry cancellation still
  retains the claim until operator recovery when no durable outcome exists.
- Activity claim acquisition now refreshes both activity and retry-timer state
  authoritatively before execution, closing a cache/claim race that could run
  an attempt before its prepared durable backoff elapsed.
- Replay rejects malformed `RETRYING`, terminal-failure, and durable-sleep
  records instead of executing through inconsistent state. Go and Python now
  persist the same retry-timer identity on short and durable backoffs.
- Timer transitions now publish one deterministic due-ledger object containing
  the full prepared `TimerRecord` before the canonical point. Reads overlay an
  interrupted write exactly; scanners repair missing, stale, or corrupt points
  and wait for a later exact scan before dispatch; deletes use durable canceled
  tombstones.
- Clean CI installs now explicitly allow the root package's SHA-pinned Git
  dependency, build it over HTTPS with `npm ci`, and deny unneeded transitive
  install scripts.
- Durable retry resume no longer compounds an earlier vendor `Retry-After`
  into later exponential intervals, and maximum-interval caps are preserved
  across process boundaries.
- Timer-ledger discovery validates payload ownership instead of deriving
  identity from object paths. Corrupt/misplaced entries are copied to a
  deterministic quarantine path while source entries remain intact; stale
  entries are filtered without racing concurrent timer writers.
- OpenDAL and Connect-backed point reads now reject protobufs whose record
  schema or embedded key does not match the requested object location. Run
  prefetch/list paths enforce the same boundary, and latest-run pointers are
  returned only while their referenced workflow still exists and matches.
  Transient pointer-metadata lag between authoritative and derived writes is
  treated as not-found rather than corrupt data.
- The optional Python SQLite timer index now falls back to the authoritative
  bucket ledger after write/query failures and self-repairs missing rows, so
  an index outage cannot make a durable timer permanently invisible.
- SQLite index locks, transactions, and close operations now run off the async
  event loop; a blocked database operation no longer stalls unrelated async
  workflow work.
- ConnectRPC 0.11 now uses async-only stubs generated with its explicit
  Google-protobuf compatibility codecs. Remote workflow/query calls and direct
  generated clients no longer fall through to the new `protobuf-py` default.
- Query results now reread authoritative records, repair or prune stale index
  rows, and continue pagination until the requested page is full. Rebuilds
  reject misplaced records whose protobuf identity disagrees with the object
  path.
- The production ConnectStore examples fail closed without an auth token and
  explicit storage configuration, and no longer advertise an inert
  timer/cron loop as an operator.
- Production HTTP examples authenticate before consuming RPC bodies, bound
  encoded and decoded request sizes, reject implicit ephemeral storage, and
  document ingress requirements for slow uploads and compressed payloads. Go
  also sets finite header/read/write/idle timeouts.
- The Python production example now rejects in-memory storage unconditionally;
  its explicit unsafe acknowledgement applies only to the one-node filesystem
  development backend.

### Upgrade notes

- Quiesce active v0.4 runs before upgrading. A v0.4 `RETRYING` activity does
  not contain the persisted effective retry policy or caller-supplied retry
  timer identity now required to resume safely. Let it finish, or delete/reset
  its activity and paired retry-timer records before resetting the parent
  workflow record; completed activity checkpoints remain reusable.
- Regenerate clients from the updated v1 schema. Callers of `Sweep` and
  `DueTimers` must populate the newly required time/duration messages.
- The v0.4 time-partitioned due-ledger shape is not read by the new scanner.
  Before switching scanners, quiesce timer writers and re-save every live
  canonical `SCHEDULED` timer through the upgraded `PutTimer` / `put_timer`
  API (using application run inventory or a rebuilt optional query index).
  Verify the deterministic
  `temporaless/v2/{namespace}/_due/{workflow_id}/{run_id}/{timer_id}.binpb`
  objects exist, then remove the superseded time-partitioned v0.4 entries while
  writers/scanners remain quiesced and deploy the new scanner. Otherwise those
  legacy shapes are retained and quarantined as invalid on each scan. This
  preserves each original `fire_at`; do not recreate timers from duration at
  upgrade time.

## [0.4.0] — 2026-07-12

### Added

- **Storage-backed workflow single-flight** — a caller-provided
  `WorkflowOptions.claim_owner_id` now serializes live invocations of the same
  workflow run with an atomic `workflow:execution` claim. Terminal records
  still replay immediately, while overlapping live calls receive a typed busy
  error (`ALREADY_EXISTS` over ConnectRPC).
- **Activity execution claims** — missing and due-retry activities use
  deterministic `activity:{activity_id}` claims and release them only after a
  durable terminal or retry boundary. Ambiguous execution/storage outcomes
  retain the claim for verified operator recovery.
- **Claim-capability enforcement and bounded claim listing** — requested
  coordination now fails explicitly when the store lacks create-if-absent
  support. `RecordStoreService.ListClaims` and `ClaimRunStore` let `DeleteRun`
  and retention sweeps clean claims from local, remote, or separately
  configured claim backends.
- **`scheduler_runner.run` (Python)** — the optional *serverful* half of
  scheduling. The `cronscheduler.Scheduler` is serverless on its own (a
  stateless `tick(now)` driven by any external clock); this is a thin resident
  loop that ticks it on a fixed interval until a `stop` event is set, for
  callers who want one always-on process instead of an external scheduler. It
  adds no scheduling logic and imports nothing from the core engine
  (`workflow`/`storage`) — it only owns the clock and the loop. Single-process
  by design; multi-process still uses stateless `tick` + `last_fire_from_runs`.
  A failing tick is logged, not fatal. *(Go parity is a follow-up.)*

### Changed

- `concurrency_key` now requires the caller-owned `claim_owner_id`; the
  framework no longer invents claim owner identities.
- `DeleteRun` and indexed retention sweeps prevalidate complete run snapshots,
  delete run-scoped claims before records, and reject claim-capable backends
  that cannot enumerate one run. These cleanup operations explicitly require
  external quiescence and are not execution fences or transactions.
- Backfill classifies claim/concurrency contention as pending work rather than
  a workflow failure.

### Fixed

- Cron ticks advance each fire only after successful dispatch, so a failed
  dispatch remains due and concurrent ticks cannot skip or double-commit it.
- Activity retry claims no longer consume a due retry timer before claim
  arbitration, and durable retries can resume under a different caller-owned
  claim identity.

## [0.3.0] — 2026-07-03

The framework remains **pre-1.0**. This release makes the storage boundary
match the product identity: core = point operations on your bucket; search =
optional derived index.

### Added

- **v2 flat storage keys** for Python core records:
  `temporaless/v2/{namespace}/{workflow_id}/{run_id}/workflow.binpb`,
  `activity/{activity_id}.binpb`, `timer/{timer_id}.binpb`,
  `event/{event_id}.binpb`, and `claim/{claim_id}.binpb`.
- **Latest-run pointer records** written by the bucket store on
  `IN_PROGRESS`, `COMPLETED`, and `FAILED` workflow puts. Cron seeding via
  `last_fire_from_runs` now does one pointer GET per schedule instead of a
  cross-run bucket walk. The pointer update is best-effort read-compare-write:
  when both run IDs parse as schedule fire times, parsed fire time is the
  monotonic guard; otherwise record timestamp is used. Racing writers can
  momentarily lose a pointer update, but never corrupt the authoritative
  workflow record. The built-in compact date parser accepts bare `%Y%m%d`
  dates only when the run ID is exactly eight digits; shorter numeric IDs fall
  back to record-time comparison.
- **Due-timer ledger** under the bucket. Pending timers write compact ledger
  entries sorted by `fire_at`; firing/cancel/deletion removes them. Durable
  timer scanning no longer walks all workflow records. Ledger entries are
  written before timer records so a crash leaves a self-pruning orphan instead
  of an unfindable pending timer. Corrupt ledger entries are moved to
  `_due_invalid/` instead of being retried forever.
- **`RecordQueryService` protobuf service** for optional cross-run search:
  `ListWorkflows`, query-style `ListActivities`, `Sweep`, and indexed
  `DueTimers`.
- **`adapters/py/indexstore`**: write-through SQLite query index for Python.
  It stores keys, statuses, and timestamps only; protobuf payloads remain in
  the bucket. `rebuild()` repopulates the index from a v2 bucket using staging
  tables and an atomic merge; rows written through the same index while the
  walk is running survive the merge, corrupt bucket records are skipped and
  counted, and records that disappear between LIST and GET are skipped
  silently.
- **`temporaless-migrate-v1-to-v2`** CLI. Reads a v1 hive tree, writes v2
  records, emits a JSONL audit log, supports `--all-runs` and
  `--newest-per-workflow`, and can populate the SQLite index.
- **Storage benchmarks** comparing latest-pointer seeding and index-backed
  listing against legacy full-bucket walks, plus run-scoped prefetch.

### Changed (breaking)

- **Python `Store` protocol shrank.** Core stores no longer expose cross-run
  `list_workflows` or `sweep`. Use `QueryStore` / `RecordQueryService` for
  listing, inspector views, and indexed retention.
- **`RecordStoreService` is point-operation only.** `ListWorkflows` and
  `Sweep` moved to `RecordQueryService`; `GetLatestWorkflowRun`, `DeleteRun`,
  and core `DueTimers` were added.
- **Storage paths are incompatible with v1.** There is no runtime legacy mode
  and no automatic fallback. Run `temporaless-migrate-v1-to-v2` once for
  existing buckets.
- **ID validation now permits `=`.** Slashes remain rejected because object
  keys are path-like. The old `=` restriction existed only for v1 path
  parsing. Namespace and workflow_id values beginning with `_` are now reserved
  for Temporaless system prefixes such as `_latest` and `_due`.
- **`last_fire_from_runs` changed semantics.** v1 derived scheduler memory by
  walking every run for a workflow and taking the max parsed run_id. v2 reads
  the latest-run pointer and relies on the pointer's parsed-fire-time
  monotonicity rule. If the pointer is missing or unparseable, a provided query
  index is used as fallback and paged until a parseable run is found within the
  bounded fallback window.
- **`DeleteRun` caveat.** Deleting the run that owns a latest-run pointer
  deletes that pointer; the bucket store does not scan for the previous run.
  Keep schedule runs until lifecycle expiry or seed from a query index/offline
  scan after destructive deletes when that memory matters.
- **Inspector and janitor require a query store for cross-run work.** Bucket
  only deployments should use bucket lifecycle rules for retention and
  offline scans for ad hoc inspection.
- **Docs and APIs now treat SQL as optional index infrastructure.** Core
  Python packages import no SQL modules; SQLite lives only in
  `temporaless-indexstore`.

### Migration

1. Run `temporaless-migrate-v1-to-v2 --source-fs-root OLD --dest-fs-root NEW
   --audit-log migration.jsonl --all-runs` to preserve replay history.
2. Use `--newest-per-workflow` only when you are seeding schedules and do not
   need old replay history. It compares parsed run_id fire times for common
   ISO-like formats by default; pass `--run-id-format` for deployment-specific
   formats.
3. Add `--index-sqlite index.sqlite` if you want the query index populated
   during migration; otherwise call `IndexedStore.rebuild()` later.
4. Records whose legacy IDs are invalid under v2 validation are skipped with an
   audit-log reason; they do not abort the migration.
5. Corrupt v1 protobuf blobs are skipped with an audit-log reason; they do not
   abort the migration.
6. Update code that called `store.list_workflows(...)` or `store.sweep(...)`
   to depend on a `QueryStore`.

### Parity Notes

- Go and Rust storage parity is a follow-up. Shared protobufs were regenerated,
  but the Python core and Python index adapter are the implementation target
  for this release.
- `adapters/go/connectstore` and `examples/go/production-server` do not compile
  against the regenerated stubs in this release. They are excluded from the
  default `scripts/check` gate; set `TEMPORALESS_CHECK_GO_RUST=1` to run the
  explicitly failing parity gate while the follow-up is in progress.
- The query index adapter is SQLite-only. Postgres-via-DSN is future work.

### Known Follow-ups

- `delete_run` removes files but can leave empty directories on filesystem
  backends.
- Index write-through is best-effort after the bucket write. If SQLite upsert
  or delete fails, rows can diverge until `rebuild()` or query-time pruning
  repairs them.

## [0.2.0] — 2026-05-24

The framework remains **pre-1.0**. This release bakes a per-submission
task tracker into `dispatch` so consumers stop having to roll their own.
Go-only for now; **Python and Rust parity is a
follow-up release**.

### Added

- **`Dispatcher.Status(taskID)` (Go)**. Returns a `*TaskInfo` describing
  the current lifecycle state of one `DoAsync` submission: PENDING →
  RUNNING → DONE (with handler response wrapped in `google.protobuf.Any`)
  or FAILED (with error message). Unknown / TTL-evicted ids return
  `(nil, false)` so callers can distinguish "no such task" from a
  terminal state.
- **`TaskStatus` enum + `TaskInfo` message** in `temporaless.v1` proto.
  Cross-language wire shape; Python and Rust SDKs will mirror in the
  next release.
- **`DispatchOptions.task_ttl`** (`google.protobuf.Duration`). How long
  completed task records stay queryable before the GC sweep evicts
  them. Default 1 hour. In-flight (PENDING/RUNNING) records never
  evict — only terminal ones age out. Tracking itself is always on:
  the framework is opinionated about this, the cost is one map entry
  per submission.

### Changed (breaking)

- **`Dispatcher.DoAsync` signature (Go)**. Returns `(taskID string, err
  error)` instead of `error`. Callers that don't want the id just
  underscore it: `_, err := disp.DoAsync(...)`. The task_id is the
  primary key for `Status()` polling.
- **`Options.OnError` signature (Go)**. Now `func(method, taskID
  string, err error)`. The default slog handler logs the task_id
  alongside the method so failed handlers are grep-able by id.
- **`Queue.Submit` signature (Go)**. Now `Submit(ctx, method, taskID,
  payload []byte) error`. External queue adapters should propagate
  the task_id alongside the payload (message header / attribute) so
  consumers can correlate.

### Internal

- Tracker is a `sync.RWMutex`-guarded map with a GC goroutine sweeping
  at `taskTTL / 2`. Shutdown stops the GC last (after handler drain)
  so terminal-state writes during drain land in the map before it
  closes.
- New tests: `TestStatusTracksLifecycle`, `TestStatusFailedSurfacesError`,
  `TestStatusUnknownIDReturnsFalse`. All under `-race`.

## [0.1.0] — 2026-05-24

First tagged release. The framework is **pre-1.0** — wire-format changes
will continue to be called out clearly when they happen, and there's no
backwards-compatibility commitment until v1.0.0.

### Added

- **`dispatch` bounded concurrency (`MaxInflight`) + pluggable `Queue`**.
  Two related additions to the pool that landed in the previous entry:
  - **Bounded concurrency**: new `max_inflight` knob on the proto
    `DispatchOptions`. When > 0, `DoAsync` blocks until a slot frees up
    — natural producer-side backpressure for bursty callers, respecting
    caller ctx / asyncio cancel / tokio shutdown notification. Zero
    (default) is unbounded — current behavior preserved.
  - **Proto-driven options**: `DrainTimeout` and `MaxInflight` moved into
    `temporaless.v1.DispatchOptions` so a single config file / env / CLI
    flag drives them identically across Go, Python, and Rust. Runtime
    hooks (`OnError`, `Queue`) stay language-local on the constructor.
    Small breaking change to the brand-new dispatcher Options struct in
    all three SDKs — no external users to migrate yet.
  - **`Queue` interface**: new producer-only abstraction `Submit(method,
    payload []byte)` + `Close()`. Default is in-process (current
    goroutine-pool behavior). External users implement `Queue` against
    Kafka / RabbitMQ / NATS / SQS / Redis Streams / etc.; the dispatcher
    proto-marshals the request deterministically and hands `(method,
    payload)` to the queue. Consumer-side helper `Dispatcher.Invoke(ctx,
    method, payload)` looks up the registered handler, decodes the
    bytes, and runs it — so the user's worker loop is just "pull from
    queue, call Invoke, ack on Ok / nack on Err". Producer-side type
    check catches mismatched request types BEFORE the bytes hit the
    queue, so a typo doesn't get durably enqueued and then dead-lettered
    later.
  - In Python and Rust, `do_async` is now `async` (it was sync) because
    the semaphore wait needs `await`. Go's signature is unchanged.

- **`dispatch` adapter — fire-and-forget pool for gRPC-shaped handlers**,
  in all three SDKs (`adapters/go/dispatch/`,
  `core/py/src/temporaless/dispatch.py`,
  `core/rs/temporaless/src/dispatch.rs`). Identical shape: `register(method,
  handler)` wires a typed handler under its gRPC fully-qualified method
  name, `do_async(method, req)` spawns it as a goroutine / asyncio task /
  tokio task and returns immediately, `shutdown()` stops accepting new
  submissions and drains in-flight work for up to `drain_timeout`
  (default 15s — matches common SIGTERM grace windows) before cancelling.
  Always waits for every spawned task to actually return; orphaning a
  handler mid-vendor-call is the failure mode we're avoiding.
  - In-process only; not durable across crashes. Use `workflow.run` when
    you need at-least-once delivery — `dispatch` is at-most-once +
    best-effort for side effects whose result the caller doesn't need to
    wait on (webhook notifications, telemetry pushes, fan-out where the
    caller wants its own request to return quickly).
  - Handler errors flow through `OnError` (default: WARN log). Panicking
    handlers are recovered in Go (`recover()`) and surfaced via the same
    path so a single bad call can't take the process down.

### Changed (breaking)

- **Rust `workflow_type` / `activity_type` now use the proto descriptor
  full name.** Replaces `std::any::type_name::<T>()` (which produced
  Rust-shaped strings like `prost_types::wrappers::StringValue`) with
  `prost::Name::full_name()` (the canonical proto name
  `google.protobuf.StringValue`, matching Go's
  `proto.Message.ProtoReflect().Descriptor().FullName()` and Python's
  `message.DESCRIPTOR.full_name`). With the digest gone, `workflow_type`
  is now the cross-language shape gate — so Rust must agree with the other
  SDKs on what that string looks like, otherwise a Python-authored record
  cannot be replayed from Rust.
  - Generic bounds widened: `workflow::run`, `execute_activity`, and the
    `activity()` helper now require `Req: Message + Name` and
    `Resp: Message + Name`. Generated types from `prost-build` with
    `enable_type_names()` (which the framework's `build.rs` now sets)
    satisfy this automatically. Hand-rolled prost messages in tests /
    examples need a one-line `impl Name`.
  - New test: `tests/interop.rs::rust_replays_python_authored_workflow_record`
    pre-seeds a `WorkflowRecord` with `workflow_type =
    "workflow:google.protobuf.StringValue->..."` (the canonical
    Python/Go form) and asserts the Rust runtime replays it without
    `WorkflowConflict`.
- **`input_digest` removed.** The SHA-256 fingerprint over deterministic
  input bytes is gone from every record kind (`WorkflowRecord`,
  `ActivityRecord`, `TimerRecord`, `ClaimRecord`) and from every SDK.
  Field numbers 5 (workflow/activity/timer) and 7 (claim) are now
  `reserved`. The de-duplication contract is the caller-supplied id:
  same `workflow_id+run_id` (workflow) or `activity_id` under the run
  (activity) replays the stored result regardless of new input bytes.
  Replay still rejects shape changes (request/response type changed →
  stored `workflow_type`/`activity_type` mismatches) and `code_version`
  changes — those are the explicit invalidation levers.
  - Motivation: the digest punished correct usage. With user-supplied
    ids, `workflow.run("prices:aapl", "2026-05-04T09:30:00Z", req)` was
    already the dedup key; the digest just added cross-language fragility
    (Rust's `std::any::type_name` vs Go/Python's proto descriptor name
    produced different bytes for the same wire input) and locked
    integrators into recomputing the hash to author records out-of-band.
  - Migration: nothing required for new records. Existing records keep
    their stored bytes — the field is unknown on decode and silently
    dropped. Code that previously relied on `ErrActivityConflict` /
    `ActivityConflictError` to detect "same id, different input" now
    sees the stored result returned; pick a distinct id if you want
    distinct executions.
  - Old `activity_digest` / `timer_digest` / `execution_digest` helpers
    and `_assert_*_fingerprint` functions are removed from the public
    Python surface. Tests and tooling that imported them need to drop
    the import; the new identity guards are internal.

### Added

- **Rust workflow runtime** — `workflow::run()`, `workflow::execute_activity()`,
  ergonomic `workflow::activity()` helper, `current()` accessor (via
  `tokio::task_local!`), `annotate()` durable metadata. Implements the
  same three replay branches Go/Python do (COMPLETED short-circuit, FAILED
  replay, IN_PROGRESS resume, fresh execution). In-process retries with
  exponential backoff + `ActivityError::with_retry_after(...)` honoring
  vendor pacing. Workflows authored in any SDK produce identical record
  bytes (same protobuf wire format, same Hive paths).
  - Not yet in Rust: claims, concurrency keys, durable timer backoffs,
    `Sleep`/`WaitEvent`, ConnectRPC handler integration. Track per-feature
    parity in `docs/sdks.md`.
- **`docs/sdks.md`** — single-page cross-SDK compatibility audit.
  Surface-by-surface comparison (construct store, workflow body, activity
  dispatch) showing identical-where-possible / idiomatic-where-needed.
  Capability matrix: what each SDK ships today vs the runway.
- **Cross-language interop tests** (`core/rs/temporaless/tests/interop.rs`)
  proving wire-format identity: hand-construct records the way Python's
  runtime would, write via the Rust `OpenDALStore`, read back, assert
  every field round-trips including nested `Any` payloads, retry history,
  and annotations. Also verifies Rust writes to the canonical Hive path.
- **Rust SDK (storage layer)** at `core/rs/temporaless/`. Native `opendal`
  crate, prost-generated proto types, full read/write of every record kind
  (workflow, activity, timer, event, claim) at the same Hive-partitioned
  paths and protobuf wire format the Go/Python SDKs use. Workflows
  authored in Python or Go are fully readable from Rust and vice versa.
  - **Scope is storage-only for now.** No workflow.Run, no claims runtime,
    no cron/timer/janitor adapters, no ConnectStore client. Those layers
    will come as the Rust SDK matures; the storage layer is the
    prerequisite that lets Rust-native tooling (analytics, MCP servers,
    future runtime) interoperate.
  - **Cross-language storage benchmark.** `scripts/bench-rs` mirrors the
    Go/Python benchmark output format (`BenchmarkName N ns/op`). On the
    `fs` backend, Rust runs `PutGetWorkflow` and `PutGetActivity` at
    ~270k ns/op — roughly **1.9× faster than Go** (~510k ns/op) and **59×
    faster than Python** (~16M ns/op). The Rust SDK uses the native
    `opendal` crate directly; Go/Python go through FFI bindings, which
    explains the gap. See `docs/benchmarks.md` for the full table.
  - **Edition 2023 downgrade in build.rs.** `prost-build` + `protox` don't
    yet parse edition 2023 (as of mid-2026), so the Rust build script
    preprocesses the canonical proto into a proto3-equivalent for Rust
    codegen only — same wire bytes, same field numbers, same RPCs. The
    `ReservedNames` defaults are emitted as Rust constants in
    `$OUT_DIR/reserved_names.rs`, so the canonical proto remains the
    single source of truth.
- **Rust toolchain in Flox manifest.** `rustc`, `cargo`, `rustfmt`,
  `clippy` added alongside the existing Go / Python / `uv` / `buf`
  packages — same thin-manifest rationale.
- **`scripts/check` runs Rust tests + `cargo fmt --check` + `cargo
  clippy -D warnings`** alongside the existing Go/Python checks. All
  three SDKs land in one gate.

### Changed

- **Proto migrated from `syntax = "proto3"` to `edition = "2023"`.**
  Enables string-typed field defaults via `[default = "..."]`. The file-level
  `features.field_presence = IMPLICIT` keeps proto3-style scalar codegen
  (value types, not pointers) so existing struct literals and `GetX()` calls
  are unchanged. The new `ReservedNames` message uses explicit-presence
  fields so the defaults actually fire.
- **Framework-reserved string literals now live in proto.** The synthetic
  `__concurrency__` workflow_id, the `activity-retry:` timer-id prefix, and
  the `slot:` concurrency-slot claim_id prefix are declared as default
  values on `temporaless.v1.ReservedNames`. Both SDKs read them from the
  generated code at module load:
  - Go: `temporalessv1.Default_ReservedNames_ConcurrencyWorkflowId` (and the
    matching `_ActivityRetryTimerIdPrefix`, `_ConcurrencySlotIdPrefix`
    package-level constants).
  - Python: `temporaless_pb2.ReservedNames().concurrency_workflow_id`
    (etc.) via a module-level `_RESERVED_NAMES` singleton.
  Renaming any reserved string is now a one-line proto change plus
  regenerate; no SDK constants can drift. When invariantprotocol generates
  the CLI / MCP / HTTP wrappers, the reserved strings are visible to those
  too — fully protobuf-driven.

### Fixed

- TOCTOU race in `OpenDALStore.get_claim` (Python): the exists-then-read
  pattern occasionally raised `NotFound` when a concurrent
  acquire/release deleted the claim mid-call. Replaced with a single
  `read` that treats `NotFound` as "absent" — also one fewer round-trip.

### Added

- **Concurrency keys** (`WorkflowOptions.concurrency_key` + `concurrency_limit`).
  Pre-emptive cluster-wide cap on in-flight `workflow.Run` invocations
  sharing a key. The runtime acquires one of N slot claims before executing
  the workflow body; all slots full → `ConcurrencyBusyError` (mapped to
  RESOURCE_EXHAUSTED) and no IN_PROGRESS record is written. Slots are
  released on terminal status, failure, and pending. Built for vendor
  rate-limit pre-emption: every workflow sharing
  `concurrency_key="vendor:openai"` with `concurrency_limit=5` has at most
  5 active invocations cluster-wide, regardless of how many workers
  dispatch. Pairs naturally with the existing `Retry-After` and
  `durable_backoff_threshold` for the full vendor-friendly story.
  - Proto: `WorkflowOptions.concurrency_key/concurrency_limit` with a
    paired-CEL validation, `ClaimResourceType.CLAIM_RESOURCE_TYPE_CONCURRENCY_KEY`
    enum value, new `DeleteClaim` RPC on `RecordStoreService`.
  - All shared types/options/enums live in proto — when invariantprotocol
    generates the CLI / MCP / HTTP wrappers, the concurrency config rides
    along automatically.
  - Distributed-safe by design: acquire arbitrates via `TryCreateClaim`
    (the storage backend's native atomic precondition — S3 `If-None-Match`,
    GCS `ifGenerationMatch=0`, OpenDAL `if_not_exists`); no app-level locks.
    Crash recovery via owner-id check: a re-invocation re-acquires its own
    stale slot rather than consuming a second one.
- **`background` workers helper** (`adapters/go/background` /
  `temporaless.background`): opt-in toggles for in-process cron / timer
  scanner / janitor loops. Solves "every replica polling the bucket is
  wasteful" — deployers configure background loops on one "operator"
  replica; the rest are handler-only and skip the helper. Each loop is
  independently toggleable; absence in `Config` means disabled.
  Deliberately not leader-elected: the framework's replay-via-storage
  catches duplicate dispatches as a no-op, so opt-in is purely efficiency.
  Skip the helper entirely when the platform provides scheduled
  invocations (EventBridge, Cloud Scheduler, Kubernetes CronJob, …).
  `docs/deployment.md` gains a section walking through the pattern.
- **Ergonomic activity helper**: `Workflow.activity()` (Python) and
  `workflow.Activity[Req, Resp](...)` (Go) collapse the most common
  callsite to roughly what a plain function call already requires —
  pass the function and its argument. Defaults applied unless overridden:
  - `activity_id` ← inferred from the function's qualified name
    (`func.__qualname__` in Python, `runtime.FuncForPC` in Go). Use
    `WithActivityID(...)` / `activity_id=` to override when two callsites
    share a function but should produce distinct records.
  - `retry_policy` ← `DefaultRetryPolicy()` / `default_retry_policy()`
    (3 attempts, 1s initial, 2× backoff, 30s max, 30s durable threshold) —
    tuned for the framework's stated LLM / vendor / quant workloads.
    Override via `WithRetryPolicy(...)` / `retry_policy=`.
  - `result_type` (Python) inferred from the function's return annotation;
    Go infers from the `Resp` generic parameter directly.
  The existing `execute_activity` / `ExecuteActivity` APIs are unchanged
  — the helper is opt-in. Cuts per-activity boilerplate roughly in half
  in the examples.
- **`docs/analytics.md`** — DuckDB / Trino / BigQuery queries against the
  Hive-partitioned bucket. Shows how to read records directly without our
  service (Option 1) or via the `export` CLI (Option 2). Lean-in on the
  storage-first audit differentiator: every other workflow framework
  requires you to query *their* database / API to learn what happened;
  with Temporaless your data warehouse owns the audit trail.
- **CLI `export --kind {workflow,activity,timer,event}`** — bulk-decode
  records to JSON Lines for ingestion into warehouses that don't speak
  protobuf natively (BigQuery, Snowflake, Redshift). Transitional surface:
  once `invariantprotocol` generates the CLI from the proto, this command
  comes for free and the bundled binary retires.
- **Vendor-aware retries via `ActivityError.retry_after` /
  `ActivityFailure.retry_after`.** Activity bodies can surface a vendor-supplied
  minimum wait (HTTP `Retry-After`, OpenAI `x-ratelimit-reset`, etc.); the
  retry planner uses `max(computed_interval, retry_after)` so vendor pacing
  wins over the configured exponential schedule. Combined with the durable
  backoff threshold, a long `Retry-After` value automatically becomes a
  durable timer instead of an in-process sleep.
- **CLI: `stale-workflows --older-than DURATION`** — list IN_PROGRESS
  workflows whose `created_at` is older than the threshold. Wire into
  alerting to catch stuck timer-scanners, missing event deliveries, or
  claim leaks.
- **CLI: `tail`** — stream new workflow records as they are written (poll
  loop; `--poll-interval` configurable, default 2s). Operator surface for
  babysitting a backfill or freshly-deployed pipeline. Honors `--json`,
  `--status`, `--namespace`, `--workflow-id` filters.
- **Durable retry backoffs** (`RetryPolicy.durable_backoff_threshold`).
  When the next retry interval crosses the configured threshold, the runtime
  persists the wait as a `TIMER_KIND_ACTIVITY_RETRY` timer + writes the
  `ActivityRecord` with `next_attempt_at`, then surfaces `TimerPendingError`
  so the workflow stays IN_PROGRESS. The bundled timer scanner re-invokes
  the workflow after `fire_at` and the retry loop resumes from the next
  attempt. Designed for LLM workflows whose rate-limit windows (30s–10min)
  exceed serverless request timeouts. Zero (default) preserves the existing
  in-process retry behavior. Resolves PRD D2.
- **Outbox idempotency-key helper** (`adapters/go/outbox` /
  `core/py/src/temporaless/outbox.py`). Derives a stable
  `temporaless-{32-hex}` key from `(namespace, workflow_id, run_id, activity_id)`
  that activity bodies pass to vendor APIs (HTTP `Idempotency-Key`, DB upsert
  keys, S3 object names) so retries after a mid-flight failure dedupe
  vendor-side instead of double-charging. Closes the side-effect-safety gap
  documented in `docs/hard-cases.md`.
- **Run-scoped replay cache** in both Go and Python. On replay, the runtime
  prefetches all activity, timer, and event records under the run in parallel
  (3 `List` calls) and serves subsequent get-by-key reads from memory. A
  workflow with N fan-out activities that previously issued N individual
  `GetActivity` round-trips per replay now issues 0 — a strict win on
  ConnectStore / S3 / GCS backends and a no-op on fresh runs (no prefetch
  triggered). Negative-cache entries short-circuit get-by-key for records
  that didn't exist at prefetch time. Out-of-scope reads (cross-pipeline
  dependencies, inspector adapters) pass straight through.
- **`temporaless` operator CLI** (`cmd/temporaless`) — thin wrapper over the
  inspector / janitor adapters. Subcommands: `list-workflows`,
  `list-activities`, `get-workflow`, `reset-workflow`, `reset-activity`,
  `reset-event`, `sweep`. Supports text and `--json` (protojson) output.
  Storage backend is selected via `--store-scheme` (`fs` by default; extend
  by importing additional `opendal-go-services/*` schemes). This is a
  transitional surface — once the protobuf service is migrated to
  `invariantprotocol`, the CLI (and MCP) will be generated automatically and
  this binary retires.
- Cross-language parity for backfill + cross-pipeline dependencies:
  - Go: `adapters/go/backfill` (`Backfill[Resp](ctx, runIDs, Options, invoke)`) and `adapters/go/dependencies` (`WaitForWorkflow[Resp](ctx, store, key, newResult)`).
  - Typed errors `WorkflowDependencyPendingError` / `WorkflowDependencyFailedError` in core, mapped through `connectworkflow.ErrorToCode` to `Unavailable` / `Internal`.
  - `(*Workflow).Store()` accessor so adapter helpers can read records without reaching into private state.
- Stress tests covering 100 concurrent workflows, 100-workflow replay, 50 parallel activities in one workflow, and high-concurrency backfill with poison-pill isolation.
- `examples/{go,py}/production-server` — tested production wiring (bearer-token auth, `/healthz` + `/readyz`, structured JSON logs, graceful shutdown).
- `Dockerfile` — multi-stage Python 3.13-slim image, ~187MB, non-root user, health check wired.
- `.github/workflows/ci.yml` — runs the full gate on push/PR.
- `docs/runbook.md` — operator runbook for common production incidents.
- `docs/production-checklist.md` — pre-launch checklist.
- Prefect 3 compatibility adapter (`adapters/py/prefectcompat`).
- Auto error-mapping in `connectworkflow.Handle` (Go) / `wrap_workflow_method` (Python) — framework typed errors now translate to `*connect.Error` / `ConnectError` automatically with the original error preserved via wrapping. The Go trigger adapter lives outside the transport-agnostic core.
- Production-server smoke tests in both languages that spawn the binary as a subprocess and verify auth + health + RPC behavior.

### Changed

- **Storage paths use strict Hive partitioning.** Every directory level is `key=value`. Spark / Trino / DuckDB / Athena pointed at `temporaless/v1/` auto-discovers `namespace`, `workflow_id`, `run_id`, `kind`, and the per-kind id as partition columns.
  - Old: `temporaless/v1/namespaces/{ns}/workflows/{wf}/runs/{rid}/workflow.binpb`
  - New: `temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=workflow/record.binpb`
- Error messages clarified for `wrap options are ambiguous` and `input digest changed` — both now state the fix, not just the failure.
- Type-checker switched from `pyright` to `ty` (Astral). `pyright` and `pyrefly` retired.

### Removed

- `deploy/k8s/` — the framework is platform-agnostic (Lambda / Cloud Run / Modal / Fly Machines / VMs+cron / K8s); shipping K8s manifests advertised the wrong default. Deployment guidance in `docs/deployment.md` and `docs/production-checklist.md` lists all options as equal.

### Known limitations

- Dagster compat adapter blocked upstream — Dagster pins `protobuf<7`, this project uses `>=7.34.1`. Tracked in `PRD.md` D11. Will revisit when Dagster unpins.
- Long retry backoffs (>30s) currently sleep in-process. Durable retry timers are designed but gated; see `PRD.md` D2.
- Temporal SDK test fixtures download an embedded test-server binary on first run. The temporalcompat tests skip cleanly with a clear message when Temporal's CDN is unreachable.
