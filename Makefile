# Temporaless top-level developer gate.
#
# `make check` is the Go-focused gate: gofmt-check + go vet + golangci-lint +
# go test -race. It mirrors the Go portion of `scripts/check` (which also runs
# buf, Rust, and the Python/uv suites) so a Go-only change can be validated
# fast without the full cross-language gate.
#
# Run inside the Flox env so the pinned go / golangci-lint are on PATH:
#
#   flox activate -- make check
#
# `make gate` delegates to scripts/check for the full cross-language gate.

GO        ?= go
GOFMT     ?= gofmt
GOFLAGS   ?=
GO_PKGS   ?= ./...

.PHONY: check fmt fmt-check vet lint test gate tidy-check

check: fmt-check vet lint test

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
	golangci-lint run $(GO_PKGS)

## test: go test with the race detector.
test:
	$(GO) test -race $(GOFLAGS) $(GO_PKGS)

## tidy-check: verify go.mod / go.sum are tidy.
tidy-check:
	$(GO) mod tidy
	@git diff --exit-code go.mod go.sum

## gate: full cross-language gate (buf + Go + Rust + Python).
gate:
	scripts/check
