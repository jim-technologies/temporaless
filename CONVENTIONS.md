# Conventions Conformance

Strict audit of this repo against the agreed conventions (source of truth:
[`AGENTS.md`](AGENTS.md), [`PRD.md`](PRD.md), [`Makefile`](Makefile),
[`scripts/check`](scripts/check)). Each row is one convention →
**Conforms** / **Fixed** / **Intentional deviation** (+ one-line rationale).

The gate (`flox activate -- make check`) is the Go-focused fast gate; the full
cross-language superset is `flox activate -- scripts/check` (adds buf + Rust +
Python/uv). Both stay green.

| # | Convention | Status | Notes |
|---|------------|--------|-------|
| 1 | Deps via Flox: `.flox/env/manifest.toml` installs the toolchain (`go`, `python313`, `uv`, `buf`, `golangci-lint`, plus `libffi` + `gcc-unwrapped` lib for cgo/OpenDAL); thin manifest, language libs live in `go.mod`/`uv.lock`/`Cargo.toml`/`buf.gen.yaml`. | **Conforms** | `golangci-lint` is a CLI (not importable) so it correctly lives in Flox, with an inline rationale comment. |
| 2 | `flox activate -- make check` clean. | **Conforms** | Verified green: golangci-lint `0 issues`, `go vet` clean, `go test -race ./...` all `ok`. |
| 3 | ONE `make check` = gofmt-check + `go vet` + golangci-lint + `go test -race`. | **Conforms** | `Makefile` `check: fmt-check vet lint test`; `test` uses `-race`. |
| 3a | "gofumpt-check" in the brief. | **Intentional deviation** | Repo formats with `gofmt` only (manifest + `.golangci.yml` `formatters: [gofmt]`); gofumpt is not installed and adding it would reformat sources (a change). gofmt + golangci-lint is the agreed Google-Go gate here. |
| 4 | Idiomatic error wrapping with `%w`. | **Conforms** | No `fmt.Errorf` wraps an existing `err` without `%w`. Bare `fmt.Errorf("…")` cases are sentinel/validation messages (no error to wrap) — correct usage. |
| 5 | Doc comments on exported symbols (Google Go style). | **Intentional deviation** | Non-obvious exported symbols (e.g. `storage.DueTimer`, `Store.Sweep`, `Store.DueTimers`, `ClaimStore.DeleteClaim`, `WorkflowStore.ListWorkflows`) carry doc comments; self-evident CRUD interface methods and plain data-holder structs (`WorkflowKey`, `ActivityKey`, …) are intentionally left bare to avoid restating the obvious — consistent with AGENTS.md "keep functions direct / behavior obvious". Not linter-enforced (no `revive`/`godot`), so the gate is unaffected. |
| 6 | golangci-lint config is conservative + excludes generated proto. | **Conforms** | `.golangci.yml` v2: `standard` + `bodyclose`/`errorlint`/`misspell`/`unconvert`; `core/go/gen` excluded from linters and formatters; best-effort `Close`/`Fprint*` excluded from errcheck with rationale. |
| 7 | CI runs `flox activate -- make check`. | **Conforms** | `.github/workflows/ci.yml` `go-check` job runs `flox activate -- make check`; `full-gate` runs `flox activate -- scripts/check` (cross-language superset); `go-build` is a fast no-flox build/vet smoke. |
| 8 | Point-in-time / leakage-guard semantics + documented caller responsibilities are stated clearly. | **Conforms** | Replay identity guards stamp `TEMPORALESS_CODE_VERSION` onto every record so replay rejects code drift; caller-provided IDs (workflow/run/activity/timer/claim-owner) and storage-safe-char validation are documented in `AGENTS.md` (Storage, Claims). Caller owns retention cadence/threshold (`Store.Sweep`, `decision.md` D10). |
| 9 | Two-tier tests where relevant (always-run unit + gated live integration). | **Conforms** | Go tests are hermetic (OpenDAL `fs` + `t.TempDir`, no external services), so no env gate is needed; the subprocess smoke test self-gates on `testing.Short()`. The live/integration tier lives in the Python adapters that talk to real Temporal/Prefect SDKs (run via `scripts/check`). |
| 10 | Tests use OpenDAL `fs` + temp dir, not memory stores. | **Conforms** | Go `*_test.go` use OpenDAL `fs` over `t.TempDir()`; no in-memory framework stores. |
| 11 | Official SDKs only (no community wrappers). | **Conforms** | `go.mod`: `connectrpc.com/connect`, `go.temporal.io/sdk`, `gocloud.dev`, `apache/opendal` bindings, `google.golang.org/protobuf`, protovalidate — all first-party. |
| 12 | One options-driven wrapper per boundary; ConnectRPC at the boundary, not in replay logic; Temporal SDK kept out of core. | **Conforms** | Wrappers in `core/go/workflow`; ConnectRPC adapter in `adapters/go/connectstore`; Temporal SDK confined to `adapters/go/temporalcompat`. |
| 13 | Claims: storage-native conditional writes; declared `ClaimCapability`; no check-then-write; no always-on lock server. | **Conforms** | `gocdkclaims` uses GoCDK `WriterOptions.IfNotExist` (narrow claims-only use, per AGENTS.md); capabilities are generated proto enums (`storage.ClaimStore.ClaimCapability`). |

## Audit outcome

- **Fixes applied this pass:** none. The prior "Production hardening" commit
  (`ea66c5c`) already brought the repo into conformance; this audit verified it
  and recorded the result here.
- **Documented intentional deviations:** rows 3a (gofmt-only, no gofumpt) and 5
  (doc comments on non-obvious exported symbols only) — both deliberate, neither
  linter-enforced, neither a behavior/API/wire change.
- **Skipped (risky) items:** none required code changes. Concurrency / durable
  replay paths were read-only audited (replay identity guard, dispatch
  wait-group, claim CAS) and left untouched per the conservative mandate.
- **Gate:** `flox activate -- make check` is GREEN, including `-race`.
