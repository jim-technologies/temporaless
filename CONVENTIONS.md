# Conventions Conformance

Strict audit of this repo against the agreed conventions (source of truth:
[`AGENTS.md`](AGENTS.md), [`Makefile`](Makefile),
[`MAKEFILE-CONTRACT.md`](MAKEFILE-CONTRACT.md),
[`scripts/validate`](scripts/validate)). Each row is one convention →
**Conforms** / **Fixed** / **Intentional deviation** (+ one-line rationale).

The gate is `flox activate -- make validate` — the one gate verb, delegating
to `scripts/validate` (Buf, TypeScript, Go, Python/uv, and Rust — the whole
toolchain, Rust 1.97.1 included, comes from the Flox manifest). Go-only
changes get fast feedback from individual Makefile sub-targets (`fmt-check`,
`tidy-check`, `vet`, `lint`, `test-go`). CI runs the identical Flox
environment, so both paths are the same path.

| # | Convention | Status | Notes |
|---|------------|--------|-------|
| 1 | Deps via Flox: `.flox/env/manifest.toml` installs every toolchain — `go`, `python314`, `uv`, `buf`, `libffi`, the `gcc-unwrapped` lib output, Node 24, the Rust 1.97.1 toolchain, `cargo-audit`, and `gitleaks`; language libraries live in `go.mod`/uv locks/`Cargo.lock`/`package-lock.json`. | **Conforms** | golangci-lint runs as a pinned Go module. Flox is the only toolchain provider, locally and in CI; `rust-toolchain.toml` is gone. |
| 2 | `flox activate -- make validate` clean. | **Conforms** | Verified green: golangci-lint `0 issues`, `go vet` clean, `go test -race ./...` all `ok`. |
| 3 | ONE gate verb: `make validate` runs gofmt-check + `go vet` + golangci-lint + `go test -race` (plus the cross-language suites). | **Conforms** | `Makefile` `validate:` delegates to `scripts/validate`; Go tests use `-race`; `check`/`gate` no longer exist. |
| 3a | "gofumpt-check" in the brief. | **Intentional deviation** | Repo formats with `gofmt` only (manifest + `.golangci.yml` `formatters: [gofmt]`); gofumpt is not installed and adding it would reformat sources (a change). gofmt + golangci-lint is the agreed Google-Go gate here. |
| 4 | Idiomatic error wrapping with `%w`. | **Conforms** | No `fmt.Errorf` wraps an existing `err` without `%w`. Bare `fmt.Errorf("…")` cases are sentinel/validation messages (no error to wrap) — correct usage. |
| 5 | Doc comments on exported symbols (Google Go style). | **Intentional deviation** | Non-obvious exported symbols (e.g. `storage.DueTimer`, `Store.Sweep`, `Store.DueTimers`, `ClaimStore.DeleteClaim`, `WorkflowStore.ListWorkflows`) carry doc comments; self-evident CRUD interface methods and plain data-holder structs (`WorkflowKey`, `ActivityKey`, …) are intentionally left bare to avoid restating the obvious — consistent with AGENTS.md "keep functions direct / behavior obvious". Not linter-enforced (no `revive`/`godot`), so the gate is unaffected. |
| 6 | golangci-lint config is conservative + excludes generated proto. | **Conforms** | `.golangci.yml` v2: `standard` + `bodyclose`/`errorlint`/`misspell`/`unconvert`; `core/go/gen` excluded from linters and formatters; best-effort `Close`/`Fprint*` excluded from errcheck with rationale. |
| 7 | CI runs `flox activate -- make validate`. | **Conforms** | `.github/workflows/ci.yml` has exactly one job whose only command is `flox activate -- make validate`; `audit.yml` runs `flox activate -- make audit` weekly. No other workflows, jobs, or setup actions exist. |
| 8 | Point-in-time / leakage-guard semantics + documented caller responsibilities are stated clearly. | **Conforms** | Caller-provided workflow/run/activity/timer/claim-owner IDs and protobuf type identities are the replay contract; current handlers resume `IN_PROGRESS` runs while terminal records stay authoritative. Storage-safe-character validation is documented in `AGENTS.md` (Storage, Claims). Caller owns incompatible-ID rollover and retention cadence/threshold (`Store.Sweep`; deferred retention enhancements are recorded in `docs/analytics.md`). |
| 9 | Two-tier tests where relevant (always-run unit + gated live integration). | **Conforms** | Go tests are hermetic (OpenDAL `fs` + `t.TempDir`, no external services), so no env gate is needed; the subprocess smoke test self-gates on `testing.Short()`. The live/integration tier lives in the Python adapters that talk to real Temporal/Prefect SDKs (run via `make validate`). |
| 10 | Tests use OpenDAL `fs` + temp dir, not memory stores. | **Conforms** | Go `*_test.go` use OpenDAL `fs` over `t.TempDir()`; no in-memory framework stores. |
| 11 | Official SDKs only (no community wrappers). | **Conforms** | `go.mod`: `connectrpc.com/connect`, `go.temporal.io/sdk`, `gocloud.dev`, `apache/opendal` bindings, `google.golang.org/protobuf`, protovalidate — all first-party. |
| 12 | One options-driven wrapper per boundary; ConnectRPC at the boundary, not in replay logic; Temporal SDK kept out of core. | **Conforms** | Wrappers in `core/go/workflow`; ConnectRPC adapter in `adapters/go/connectstore`; Temporal SDK confined to `adapters/go/temporalcompat`. |
| 13 | Claims: storage-native conditional writes; declared `ClaimCapability`; no check-then-write; no always-on lock server. | **Conforms** | `gocdkclaims` uses GoCDK `WriterOptions.IfNotExist` (narrow claims-only use, per AGENTS.md); capabilities are generated proto enums (`storage.ClaimStore.ClaimCapability`). |

## Makefile contract conformance

Audit against [`MAKEFILE-CONTRACT.md`](MAKEFILE-CONTRACT.md), the
jim-technologies open-source Makefile contract.

| Verb / rule | Status | Notes |
|---|---|---|
| `make fmt` rewrites every language | **Conforms** | `fmt` fans out to `fmt-go` (gofmt), `fmt-proto` (buf format), `fmt-py` (ruff across core + every adapter), and `fmt-rs` (cargo fmt when installed). TypeScript has no configured formatter; `ts-check` still compiles and tests it. |
| `make test` is the full suite; `test-<lang>` sub-verbs | **Conforms** | `test` = `test-go` + `test-ts` + `test-py` + `test-rs`; hermetic (OpenDAL `fs` + temp dirs, locked uv environments, no external services). |
| `make validate` is the one gate verb, exactly what CI runs | **Conforms** | Delegates to `scripts/validate`; ci.yml's only job runs exactly `flox activate -- make validate`. `make check` and `make gate` no longer exist. |
| CI never checks more or less than `validate` | **Conforms** | ci.yml is a single job running the single gate command with the Flox manifest as its only toolchain source. Network-dependent supply-chain audits are contractually excluded from the gate and run weekly via `audit.yml` → `make audit`, itself runnable locally. |
| `make build` produces the artifacts locally | **Conforms** | Go packages, TypeScript dist, and the Rust workspace; Python ships as source (Git-only distribution). |
| `make generate`; stale committed output fails `validate` | **Conforms** | `generate` delegates to `scripts/generate`; `validate` rejects a stale checked-in descriptor, and CI rejects any regeneration diff on main. |
| `make release` refuses a dirty or unpushed tree; CI never publishes | **Conforms** | `scripts/release` refuses a dirty tree, an unpushed HEAD, and an existing tag, then creates and pushes the one root `vVERSION` tag. Publishing is Git-tag-only for every SDK (AGENTS.md forbids registry publication; npm is `private: true`, Cargo `publish = false`), so the git tag *is* the public-ecosystem publish. |
| Public-surface guard inside `validate` | **Conforms** | `scripts/public-surface-check` scans code, docs, examples, and the Makefile itself using this repository's own deny-list; `validate` runs it everywhere. |
| No `make deploy` | **Conforms** | No deploy target or CI deploy step exists anywhere. |
| No privately resolving dependencies | **Conforms** | Every lock resolves from public registries or pinned public GitHub URLs (`apache/opendal`, `jim-technologies/invariantprotocol`); the Git-SHA install checks in `make audit` prove a stranger can install all four SDKs. |
| No CI secrets | **Conforms** | Workflows hold zero secrets. The former CI-side `BUF_TOKEN` regeneration proof is gone; maintainers prove a trusted BSR regeneration locally with `TEMPORALESS_REQUIRE_BUF_GENERATE=1` before releasing schema changes, and every gate still validates the checked-in descriptor and compiles/tests every generated consumer. |
| No encrypted-secret store | **Conforms** | None exists; `.gitleaks.toml` configures the history secret *scanner*, and the repository holds no secrets. |
| Furniture: `LICENSE` · `CHANGELOG.md` · one `VERSION` · `make help` | **Conforms** | All present; `scripts/check_versions.py` keeps every SDK mirror equal to the root `VERSION`, and `make help` self-documents every verb via the `## comment` convention. |

## Buf / protobuf conventions

Audit against the jim-technologies buf/protobuf conventions block. Where the
block and existing config differed, the stricter of the two was adopted.

| Convention | Status | Notes |
|---|---|---|
| buf comes from the Flox manifest (pinned); never installed in CI steps | **Conforms** | `.flox/env/manifest.toml` pins `buf` 1.71.0; both workflows run only `flox activate -- make <verb>` with no setup or install steps. |
| One `buf.yaml` at module root; lint DEFAULT plus COMMENTS on public RPCs/messages/fields | **Fixed** (stricter adopted) | The root `buf.yaml` is the one module config (module `api`). v2 `STANDARD` (the v2 name of DEFAULT) was already on; `COMMENT_SERVICE`/`COMMENT_RPC`/`COMMENT_MESSAGE`/`COMMENT_FIELD` are now also enforced, and every previously undocumented public message and field gained a doc comment. The `dagstercompat` test-fixture proto lints with Buf defaults — it is a test fixture, not a published surface. |
| Breaking-change detection against main runs inside `make validate` for any proto published or consumed outside the repo | **Intentional deviation** (stricter kept) | `scripts/validate` runs `buf breaking` against the newest reachable release tag — a superset of against-main for the published `temporaless.v1` module, since it also catches breakage already merged since the last release. The one narrow documented exception is the v0.8.2 → v0.9.0 `code_version` field deletions (`scripts/check_buf_breaking.py`). Never weakened. |
| `make generate` regenerates all bindings; generated code is committed; `make validate` fails on drift | **Conforms** | `generate` delegates to `scripts/generate` (all language bindings + the descriptor). Every gate byte-compares the checked-in descriptor; CI additionally requires a clean tree after formatting/generation. Remote-plugin regeneration is skipped in secretless CI (the Makefile contract forbids CI secrets); maintainers prove it with `TEMPORALESS_REQUIRE_BUF_GENERATE=1` before releasing schema changes. |
| Package naming: versioned suffix always; new majors are new packages | **Conforms** | The one published package is `temporaless.v1`; `STANDARD` enforces `PACKAGE_VERSION_SUFFIX`. A future v2 is a new package, not an edit of v1. |
| Multiple language bindings share the single VERSION and release together | **Conforms** | Root `VERSION` is the release version for every SDK; `scripts/check_versions.py` runs first in every gate and `scripts/release` creates the one root `vX.Y.Z` tag. |

## Audit outcome

- **Fixes applied this pass:** exact timer write-ahead recovery, retry/claim
  race hardening, authoritative query/index validation, bounded production
  request handling, latest stable dependency/toolchain pins, immutable CI and
  container inputs, and mandatory cross-language gates.
- **Documented intentional deviations:** rows 3a (gofmt-only, no gofumpt) and 5
  (doc comments on non-obvious exported symbols only) — both deliberate, neither
  linter-enforced, neither a behavior/API/wire change.
- **Skipped (risky) items:** none within the first-class Go/Python scope. CAS
  claim takeover and full Rust parity remain explicitly outside the current
  core contract rather than being implied as complete.
- **Gate:** `flox activate -- make validate` is GREEN, Rust 1.97.1
  format/Clippy/tests included via the Flox toolchain. The container image
  build/scan/smoke check left CI with the Flox-only consolidation (it needs a
  Docker daemon, which Flox cannot supply); `docker build` from the
  repository `Dockerfile` remains the operator path.
