# Temporaless top-level developer gate.
#
# `make validate` is the one gate verb (MAKEFILE-CONTRACT.md): it delegates to
# scripts/validate, the full cross-language gate (Buf + TypeScript + Go +
# Python/uv, plus Rust when installed) that CI runs. Go-only changes can get
# fast feedback from the individual sub-targets (fmt-check, tidy-check, vet,
# lint, test-go) without the full cross-language gate.
#
# Run inside the Flox env so pinned Go is on PATH; the lint target runs the
# pinned golangci-lint module through `go run`:
#
#   flox activate -- make validate

GO        ?= go
GOFMT     ?= gofmt
GOFLAGS   ?=
GO_PKGS   ?= ./...
GOLANGCI_LINT ?= $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2

.DEFAULT_GOAL := help

.PHONY: help validate version-check version-set generate public-surface-check fmt fmt-go fmt-proto fmt-py fmt-rs fmt-check vet lint test test-go test-ts test-py test-rs build ts-check tidy-check

## help: show available make targets.
help:
	@awk 'BEGIN {printf "Usage:\n  make <target>\n\nTargets:\n"} /^## / {line=$$0; sub(/^## /, "", line); target=line; sub(/:.*/, "", target); if (target ~ /^[A-Za-z0-9_.-]+$$/) {desc=line; sub(/^[^:]+: */, "", desc); printf "  %-14s %s\n", target, desc}}' $(MAKEFILE_LIST)

## validate: the gate — full cross-language checks, exactly what CI runs.
validate:
	scripts/validate

## version-check: verify every SDK and adapter uses the root VERSION.
version-check:
	python3 scripts/check_versions.py

## version-set: synchronize every SDK and adapter (usage: make version-set VERSION=X.Y.Z).
version-set:
	@test -n "$(VERSION)" || { echo "VERSION is required"; exit 2; }
	python3 scripts/set_version.py "$(VERSION)"

## generate: regenerate protobuf SDK sources and the checked-in descriptor.
generate:
	scripts/generate

## public-surface-check: fail on private/internal wording in public docs/examples.
public-surface-check:
	scripts/public-surface-check

## fmt: rewrite formatting in place for every language in the repo.
fmt: fmt-go fmt-proto fmt-py fmt-rs

## fmt-go: rewrite Go sources in place with gofmt.
fmt-go:
	$(GOFMT) -w .

## fmt-proto: rewrite protobuf sources in place with buf format.
fmt-proto:
	buf format -w api
	buf format -w adapters/py/dagstercompat/tests/proto

## fmt-py: rewrite Python sources in place with ruff format.
fmt-py:
	uv run --project core/py ruff format core/py/src core/py/tests core/py/benchmarks examples/py scripts/check_buf_breaking.py scripts/check_versions.py scripts/set_version.py
	uv run --project adapters/py/connectworkflow ruff format adapters/py/connectworkflow/src adapters/py/connectworkflow/tests
	uv run --project adapters/py/dagstercompat ruff format adapters/py/dagstercompat/tests
	uv run --project adapters/py/temporalcompat ruff format adapters/py/temporalcompat/src adapters/py/temporalcompat/tests
	uv run --project adapters/py/prefectcompat ruff format adapters/py/prefectcompat/src adapters/py/prefectcompat/tests
	uv run --project adapters/py/indexstore ruff format adapters/py/indexstore/src adapters/py/indexstore/tests

## fmt-rs: rewrite Rust sources in place with cargo fmt (when cargo is installed).
fmt-rs:
	@if command -v cargo >/dev/null 2>&1; then \
		cargo fmt --all; \
	else \
		echo "Skipping Rust formatting; cargo is not on PATH." >&2; \
	fi

## fmt-check: fail if any Go source is not gofmt-clean.
fmt-check:
	@unformatted="$$($(GOFMT) -l . | grep -v '^core/go/gen/' || true)"; \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needs to run on:"; echo "$$unformatted"; \
		echo "run 'make fmt'"; \
		exit 1; \
	fi

## vet: go vet across all packages.
vet:
	$(GO) vet $(GO_PKGS)

## lint: golangci-lint (config in .golangci.yml).
lint:
	$(GOLANGCI_LINT) run $(GO_PKGS)

## test: the full test suite — every language, offline and hermetic.
test: test-go test-ts test-py test-rs

## test-go: go test with the race detector.
test-go:
	$(GO) test -race $(GOFLAGS) $(GO_PKGS)

## test-ts: run the TypeScript client tests (when npm is installed).
test-ts:
	@if command -v npm >/dev/null 2>&1; then \
		npm test; \
	else \
		echo "Skipping TypeScript tests; npm is not on PATH." >&2; \
	fi

## test-py: run the Python core and every Python adapter test suite.
test-py:
	uv run --project core/py --locked pytest core/py/tests
	uv run --project adapters/py/connectworkflow --locked pytest adapters/py/connectworkflow/tests
	uv run --project adapters/py/dagstercompat --locked pytest adapters/py/dagstercompat/tests
	uv run --project adapters/py/temporalcompat --locked pytest adapters/py/temporalcompat/tests
	PREFECT_LOGGING_LEVEL=ERROR PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW=ignore \
		uv run --project adapters/py/prefectcompat --locked pytest adapters/py/prefectcompat/tests
	uv run --project adapters/py/indexstore --locked pytest adapters/py/indexstore/tests

## test-rs: run the Rust workspace tests (when cargo is installed).
test-rs:
	@if command -v cargo >/dev/null 2>&1; then \
		cargo test --workspace --locked; \
	else \
		echo "Skipping the experimental Rust SDK tests; CI validates them with the pinned rust-toolchain.toml." >&2; \
	fi

## build: produce the artifacts locally — Go packages, TypeScript dist, Rust workspace.
build:
	$(GO) build $(GO_PKGS)
	@if command -v npm >/dev/null 2>&1; then \
		npm run build; \
	else \
		echo "Skipping the TypeScript build; npm is not on PATH." >&2; \
	fi
	@if command -v cargo >/dev/null 2>&1; then \
		cargo build --workspace --locked; \
	else \
		echo "Skipping the Rust build; CI builds it with the pinned rust-toolchain.toml." >&2; \
	fi

## ts-check: run the TypeScript client build and tests.
ts-check:
	npm run check

## tidy-check: verify go.mod / go.sum are tidy.
tidy-check:
	$(GO) mod tidy -diff
