# Temporaless top-level developer gate.
#
# `make check` is the fast gate: public-surface audit + gofmt-check + go vet +
# golangci-lint + go test -race. It mirrors the Go portion of `scripts/check`
# (which also runs Buf, TypeScript, Rust, and the Python/uv suites) so a
# Go-only change can be validated fast without the full cross-language gate.
#
# Run inside the Flox env so pinned Go is on PATH; the lint target runs the
# pinned golangci-lint module through `go run`:
#
#   flox activate -- make check
#
# `make gate` delegates to scripts/check for the full cross-language gate.

GO        ?= go
GOFMT     ?= gofmt
GOFLAGS   ?=
GO_PKGS   ?= ./...
GOLANGCI_LINT ?= $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2

.DEFAULT_GOAL := help

.PHONY: help check version-check version-set public-surface-check fmt fmt-check vet lint test ts-check gate tidy-check

## help: show available make targets.
help:
	@awk 'BEGIN {printf "Usage:\n  make <target>\n\nTargets:\n"} /^## / {line=$$0; sub(/^## /, "", line); target=line; sub(/:.*/, "", target); if (target ~ /^[A-Za-z0-9_.-]+$$/) {desc=line; sub(/^[^:]+: */, "", desc); printf "  %-14s %s\n", target, desc}}' $(MAKEFILE_LIST)

## check: run the fast developer gate.
check: version-check public-surface-check fmt-check tidy-check vet lint test

## version-check: verify every SDK and adapter uses the root VERSION.
version-check:
	python3 scripts/check_versions.py

## version-set: synchronize every SDK and adapter (usage: make version-set VERSION=X.Y.Z).
version-set:
	@test -n "$(VERSION)" || { echo "VERSION is required"; exit 2; }
	python3 scripts/set_version.py "$(VERSION)"

## public-surface-check: fail on private/internal wording in public docs/examples.
public-surface-check:
	scripts/public-surface-check

## fmt: rewrite Go sources in place with gofmt.
fmt:
	$(GOFMT) -w .

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

## test: go test with the race detector.
test:
	$(GO) test -race $(GOFLAGS) $(GO_PKGS)

## ts-check: run the TypeScript client build and tests.
ts-check:
	npm run check

## tidy-check: verify go.mod / go.sum are tidy.
tidy-check:
	$(GO) mod tidy -diff

## gate: full cross-language gate (Buf + TypeScript + Go + Rust + Python).
gate:
	scripts/check
