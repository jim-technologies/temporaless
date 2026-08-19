# Dependency Choices

## Release Version

The repository-root `VERSION` is the one release version for every Temporaless
SDK and adapter. Required ecosystem copies in npm, Python, uv, and Cargo are
updated together with `make version-set VERSION=X.Y.Z` and verified by
`scripts/check_versions.py`. Go receives that same version from the single
plain root tag `vX.Y.Z`. Invariant Protocol is a separate project and keeps its
own independent release version.

Before creating the tag, move the current changelog entries into a dated
`[X.Y.Z]` section. Tagged CI requires that first release heading and the tag
name to match `VERSION`, so an unfinished `Unreleased` section cannot be tagged
accidentally.

## Flox

Flox is the only toolchain source, locally and in CI — every language runtime
and every tool the gate or the audits need comes from the manifest:

- `go`: required for the Go core and Go module tooling.
- `python314`: required Python 3.14 runtime for development.
- `uv`: required Python dependency manager.
- `buf`: required protobuf formatter, linter, and generator.
- `libffi`: required by the Apache OpenDAL Go binding.
- `gcc-unwrapped` `lib` output: provides `libstdc++.so.6` for Python Protovalidate's RE2 dependency.
- `nodejs` 24: the TypeScript SDK's package and test runtime.
- `cargo`/`rustc`/`clippy`/`rustfmt` 1.97.1: the one pinned Rust toolchain.
- `cargo-audit` and `gitleaks`: back the scheduled `make audit` verb.

The environment intentionally does not include language-specific linters,
generators, or `protoc`. Third-party Go, Python, Rust, and TypeScript
libraries remain in `go.mod`, uv lockfiles, `Cargo.lock`, and
`package-lock.json`. The Go gate runs the checksum-resolved module version of
golangci-lint.

The bootstrap package pins remain the highest versions that resolve together
on all four default systems (`x86_64-linux`, `aarch64-linux`, `x86_64-darwin`,
and `aarch64-darwin`): Go 1.26.4, Python 3.14.4, and uv 0.11.25. The Node,
Rust, and audit toolchains live in their own package groups and are scoped to
the three systems the catalog builds them for (`x86_64-darwin` is excluded).
`go.mod` requires Go 1.26.5, so `GOTOOLCHAIN=go1.26.5+auto` selects that exact
checksum-verified security patch while the catalog package acts only as the
bootstrap command.
The production image has no Flox catalog constraint and uses upstream Python
3.14.6 and uv 0.11.31.

## Go

Go dependencies live in `go.mod`:

- `github.com/apache/opendal/bindings/go`: default storage implementation.
- `github.com/apache/opendal-go-services`: OpenDAL service schemes used by tests and examples.
- `gocloud.dev`: narrow Go claim adapter that uses blob conditional writes for create-if-absent leases.
- `google.golang.org/protobuf`: protobuf marshaling and generated code runtime.
- `buf.build/go/protovalidate`: validation for protobuf-declared options and IDs.
- `connectrpc.com/connect`: generated ConnectRPC Go stubs.
- `go.temporal.io/sdk`: strict Temporal compatibility adapter only. It is not a core runtime dependency conceptually, and the core must not import Temporal SDK packages.

## Python

Python dependencies live in `core/py/pyproject.toml` and `core/py/uv.lock`:

- `opendal`: Python storage adapter target.
- `protobuf`: generated protobuf runtime.
- `protovalidate`: validation for protobuf-declared options and IDs.
- `connectrpc`: ConnectRPC runtime for Python clients and servers.
- `protoc-gen-connectrpc`: a development-only, commit-pinned generator invoked
  by Buf through `uv run --frozen`. The generated surface is async-only and
  selects ConnectRPC's Google-protobuf codecs explicitly; the BSR copy still
  trails the runtime version.
- `ruff`: formatter and linter (Astral, Rust-based).
- `ty`: type checker (Astral, Rust-based). Replaces pyright; pinned to the
  current pre-1.0 baseline in each uv project.
- `types-protobuf`: type stubs for `google.protobuf.*_pb2` modules so ty resolves `Timestamp`, `Duration`, `Any`, etc. The runtime `protobuf` wheel ships no `.pyi` stubs.
- `pytest`: tests.

## Python Temporal Adapter

The Python Temporal adapter has its own uv project in `adapters/py/temporalcompat`:

- `temporalio`: strict Temporal compatibility adapter only. It is intentionally outside `core/py` so the core runtime does not depend on Temporal.
- `temporaless`: editable path dependency on `core/py` for generated protobuf types and core conventions.
- `ruff`, `ty`, and `pytest`: adapter-local quality gate. (`types-protobuf` is also a dev dep so ty resolves `google.protobuf` modules in tests.)

## Python Connect Workflow Adapter

`adapters/py/connectworkflow` is a separate uv project. It depends only on the
Python core and `connectrpc`, and owns the ConnectRPC method wrapper, typed
wrapper options, error-code mapping, and remote-backfill status predicate.
Keeping those imports here prevents transport policy from leaking into core
workflow replay.

## Python Prefect Adapter

`adapters/py/prefectcompat` is separately locked to Prefect 3.8.2. It wraps
async unary protobuf handlers as real Prefect flows/tasks and keeps Prefect's
deployment, scheduling, retry, and UI state out of Temporaless core.

## Python Dagster Process-Boundary Proof

`adapters/py/dagstercompat` is a separately locked, development-only virtual uv
project, not an installable SDK package or same-process runtime adapter. Its dev
environment contains Dagster 1.13.17, ConnectRPC 0.11.1, and protobuf 6,
deliberately has no Temporaless dependency, and executes the supported
generated-application-RPC integration in CI. The test launches its service in
the separately locked core Python environment with protobuf 7, Temporaless,
OpenDAL, and the Connect workflow adapter. Buf descriptor comparisons ensure
both generated sides still match the single application proto. Dagster's
official `protobuf<7` bound makes that process isolation mandatory.

## Rust

Rust dependencies live in `core/rs/temporaless/Cargo.toml`. The repository
root has a small `Cargo.toml` workspace so Cargo can install the crate directly
from git without a registry. The Flox manifest pins Rust 1.97.1 plus Clippy
and rustfmt, so the same toolchain gates the Rust SDK locally and in CI.

## TypeScript

TypeScript dependencies live in root `package.json` and `package-lock.json`.
Node 24 LTS is the package and CI runtime baseline.
The npm package entry is at the repository root because npm git dependencies
install from the repository package root; TypeScript source remains under
`core/ts`. Generated protobuf and ConnectRPC code is produced by Buf into
`core/ts/src/gen`. The invariantprotocol projection is an explicit subpath
backed by a full-SHA Git pin of `@jim-technologies/invariant-protocol` 0.14.0
rather than a mutable branch. The root TypeScript export stays a lightweight
generated-types + Connect-client surface. The facade uses Invariant's
generated-service registration and unified Connect interceptor API.

## Buf

Buf remote plugins are version-pinned in `buf.gen.yaml`. That keeps protoc
plugins out of Flox and avoids a second tool installation layer. CI is
secretless and skips remote-plugin regeneration (avoiding the anonymous BSR
rate limit) while still validating the checked-in descriptor and compiling
and testing every generated consumer. Maintainers set
`TEMPORALESS_REQUIRE_BUF_GENERATE=1` for a forced local regeneration and run
it before releasing schema changes.
Run `scripts/generate` (or `make generate`) to regenerate all language outputs
and the checked-in TypeScript descriptor together; raw `buf generate` does not
build that descriptor set.
`buf.yaml` also tracks the Protovalidate schema dependency used by validation
annotations.
