# Temporaless — Production Readiness

Scope of this assessment: the **Go core + adapters**, the build/lint/test
gate, the Flox dev env, CI, the Docker image, and a Nomad deploy spec for the
shared medallion cluster. It also answers whether temporaless's point-in-time
guarantees are sufficient for medallion-os's look-ahead-bias guard.

Date: 2026-06-01.

## State

Green across the Go gate:

| Check | Result |
|---|---|
| `go build ./...` | pass |
| `go vet ./...` | pass |
| `gofmt -l` (excl. generated) | clean |
| `golangci-lint run ./...` | 0 issues |
| `go test -race ./...` | pass (all packages) |
| `flox activate -- make check` | pass |
| `docker build .` | pass (~140 MB python:3.13-slim image) |
| image `/readyz` + `/healthz` | both return 200 at runtime |

Test coverage on the temporal/as-of core is strong: `core/go/workflow` ~69.5%,
`core/go/storage` ~64.8%, adapters mostly 60–90%. The replay/identity/retry/
timer/event/claim/cache paths are all exercised by table-driven tests.

## Fixes applied (conservative, no rewrites)

1. **Race/correctness bug in `adapters/go/backfill`.**
   `TestBackfillHaltOnErrorStopsAfterFirstFailure` failed under `-race`.
   Root cause: the dispatch loop eagerly spawned one goroutine per run_id, all
   racing for the semaphore, so with `HaltOnError` later run_ids could finish
   *before* an earlier failure closed the halt channel — defeating
   halt-on-error entirely (observed `succeeded=3, failed=1, pending=0`).
   Fix: acquire the semaphore in the dispatch loop **in submission order**
   before spawning each worker, and re-check `halt` after acquiring a slot.
   With concurrency N, at most N runs are in-flight when a failure halts; every
   not-yet-dispatched run_id is reported `Pending` in order. Verified stable
   over `-count=5 -race`. (`adapters/go/backfill/backfill.go`)

2. **gofmt.** Eight source/test files were not gofmt-clean (import ordering +
   struct-field alignment only — purely mechanical). Applied `gofmt -w`.

3. **Dead code.** Removed an unused `correlationIDFromContext` getter in the
   production-server example (flagged by the `unused` linter; the correlation
   ID is captured in a local and logged directly).

4. **Unchecked deferred `Close()`** in an example and a test — made the
   intent explicit with `defer func() { _ = x.Close() }()`. (errcheck)

5. **`go.mod` tidy.** `github.com/oklog/ulid/v2` is a *direct* import in
   `adapters/go/dispatch` but was listed `// indirect`; `go mod tidy` promoted
   it to a direct require. go.sum unchanged.

6. **New leakage-guard test.** `TestRunIsPointInTimeAgainstMutatingSource`
   (`core/go/workflow/workflow_test.go`): an activity reads a *mutating* live
   source; after the as-of snapshot is committed the source moves; a re-run
   must return the **frozen** value and the body must execute exactly once.
   This is the property medallion-os actually depends on, asserted end-to-end.

## Tooling / infra added

- **`Makefile`** with a top-level `make check` = `fmt-check` + `vet` + `lint`
  (golangci-lint) + `test -race`. Also `make fmt`, `make tidy-check`, and
  `make gate` (delegates to the full cross-language `scripts/check`).
- **`.golangci.yml`** (v2 schema): standard linters + `bodyclose`,
  `errorlint`, `misspell`, `unconvert`; generated `core/go/gen` excluded;
  errcheck tuned to ignore idiomatic `fmt.Fprint*` / best-effort `Close`.
- **Flox manifest** now installs `golangci-lint` (a CLI that cannot live in
  go.mod — same rationale already applied to `clippy`/`rustfmt`). Resolved
  version 2.11.4, built with the pinned go1.26.2.
- **`scripts/check`** Go section hardened: now runs gofmt-check + vet +
  golangci-lint + `go test -race` (was just `go test` + `go vet`).
- **CI** (`.github/workflows/ci.yml`): added a `go-check` job running
  `flox activate -- make check`. Existing `full-gate` (`scripts/check`) and
  fast `go-build` jobs retained.
- **Nomad** (`deploy/temporaless.nomad.hcl` + `deploy/vars.hcl.example`): a
  `service` job using the **docker** driver and the repo's own image, modeled
  on the cluster's existing `timescaledb` (docker) and `medallionapi` specs —
  rolling `update` with auto-revert, `restart`/`reschedule`, HTTP health check
  on `/readyz`, `kill_timeout` for graceful drain, `AUTH_TOKEN` /
  `TEMPORALESS_STORAGE_ROOT` / `PORT` env, and a `temporaless_data` host volume
  for the default fs backend.

## Point-in-time sufficiency for medallion-os — VERDICT

**Sufficient as a durable, append-once, point-in-time *record* store for
feature/decision provenance — with one important boundary the caller must
respect.**

How the guarantee works. Every workflow/activity boundary is a protobuf record
at a deterministic, caller-keyed, Hive-partitioned path. On replay, a stored
COMPLETED/FAILED boundary returns the **stored** result and the body does **not
re-execute** (`replayRecord` / `replayWorkflowRecord` in
`core/go/workflow/workflow.go`). So a feature value fetched/computed at as-of
time T is frozen: re-running the same `(workflow_id, run_id, activity_id)` can
never pull in data that only exists after T. Identity guards
(`assertActivityIdentity` / `assertWorkflowIdentity`) reject silent drift — a
changed message shape (`activity_type`) or `code_version` is an error, not a
quiet overwrite. This is exactly the structural defense against the 2026-05-16
look-ahead disaster: the as-of snapshot is the source of truth, not a
recomputation against live data.

Why this is the right tool for the leakage guard:
- **Knowledge-time is explicit and immutable.** The record's `created_at` /
  `completed_at` and the caller-chosen run_id (e.g. a date stamp) pin *when*
  the value was known; the bytes never change after COMPLETED.
- **Re-runs are deterministic.** Backtests/replays read the frozen snapshot,
  so a model can be re-scored without leaking future ticks/fundamentals.
- **Auditable.** Hive partitioning lets DuckDB/Trino query the bucket as-of any
  `(namespace, workflow_id, run_id, kind)` slice for forensic provenance.

Boundaries / caveats the caller owns (these are by design, not gaps):
1. **It stores, it does not bitemporally version a single key.** Temporaless is
   write-once-per-boundary: a COMPLETED record is the answer. It is *not* a
   bitemporal table where the same logical key carries (event-time,
   knowledge-time) revisions. Point-in-time correctness comes from the caller
   encoding the as-of dimension **into the run_id / activity_id** (the
   framework explicitly refuses to generate IDs). If medallion-os needs "value
   of feature F as known on date D," that must be `run_id=D` — the store will
   not infer it.
2. **The framework does not freeze the clock or the data source.** Determinism
   on replay is guaranteed because the body is skipped, not because inputs are
   captured/snapshotted. First-execution correctness is the activity author's
   responsibility: an activity must fetch as-of data, not "latest." Temporaless
   guarantees the *second* read is leak-free; it cannot retroactively fix a
   leaky *first* fetch.
3. **fs backend atomicity.** The default OpenDAL `fs` store is not atomic for
   concurrent create (the tests note this). For production point-in-time
   integrity under concurrency use an object-store backend (S3/GCS) where
   conditional writes are atomic — the Go claim path documents this tier
   explicitly. The Nomad spec defaults to fs-on-host-volume for a single-node
   start; switch the image to S3/GCS before running multi-node.

Bottom line: temporaless gives medallion-os a clean, durable, replay-stable
point-in-time **record/provenance** layer and a structural re-run leakage
guard. It is sufficient for "freeze the as-of snapshot and replay it without
leakage," provided the caller (a) keys the as-of dimension into the IDs,
(b) writes as-of-correct activity bodies, and (c) uses an atomic object-store
backend in production. It is *not* a substitute for a bitemporal SQL store if
you need multi-revision history of a single logical key.

## Remaining gaps / risks (none blocking)

- **Nomad spec is unvalidated by a live `nomad` binary** (no CLI in this env).
  It mirrors the working `timescaledb`/`medallionapi` specs; validate with
  `nomad job validate` and `nomad job plan` before first apply.
- **Image entrypoint is the example `production_server.py`** with the fs
  backend. For production, rebuild with an S3/GCS-backed server (the README and
  `docs/deployment.md` describe this) and drop the host volume + node
  constraint from the Nomad job.
- **golangci-lint config is intentionally conservative.** Tightening (e.g.
  `revive`, `gocritic`) can come later; current set is high-signal and clean.
- **Pre-existing untracked files** in the working tree
  (`core/py/src/temporaless/scheduler_runner.py` + its test, and a modified
  `CHANGELOG.md`) predate this work and were left untouched.
