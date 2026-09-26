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

.PHONY: help help-all validate audit version-check version-set release generate public-surface fmt fmt-go fmt-proto fmt-py fmt-rs fmt-check vet lint test test-go test-ts test-py test-rs build build-console ts-check tidy-check

help: ## One-screen help (make help-all for every target)
	@echo "Daily:"
	@echo "  make fmt        rewrite formatting in place, every language"
	@echo "  make test       the full test suite, every language"
	@echo "  make validate   the full offline gate, exactly what CI runs"
	@echo "  make generate   regenerate protobuf SDK sources and descriptor"
	@echo "  make release    tag vVERSION and push (refuses a dirty tree)"
	@echo ""
	@echo "Everything else: make help-all"

help-all: ## Every target with its description
	@grep -hE '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/'

validate: ## the gate — full cross-language checks, exactly what CI runs
	scripts/validate

audit: ## network-dependent supply-chain audits (vulns, secrets, Git installability)
	scripts/audit

version-check: ## verify every SDK and adapter uses the root VERSION
	python3 scripts/check_versions.py

version-set: ## synchronize every SDK and adapter (usage: make version-set VERSION=X.Y.Z)
	@test -n "$(VERSION)" || { echo "VERSION is required"; exit 2; }
	python3 scripts/set_version.py "$(VERSION)"

release: ## publish one release — tag vVERSION and push it; refuses a dirty or unpushed tree
	scripts/release

generate: ## regenerate protobuf SDK sources and the checked-in descriptor
	scripts/generate

public-surface: ## guard the public surface — tracked content, paths, and unpushed commit messages
	scripts/public-surface-check
	scripts/public-surface-check-test

fmt: fmt-go fmt-proto fmt-py fmt-rs ## rewrite formatting in place for every language in the repo

fmt-go: ## rewrite Go sources in place with gofmt
	$(GOFMT) -w .

fmt-proto: ## rewrite protobuf sources in place with buf format
	buf format -w api
	buf format -w adapters/py/dagstercompat/tests/proto
	buf format -w adapters/go/console/proto

fmt-py: ## rewrite Python sources in place with ruff format
	uv run --project adapters/py/cloudevents ruff format adapters/py/cloudevents/src adapters/py/cloudevents/tests
	uv run --project core/py ruff format core/py/src core/py/tests core/py/benchmarks examples/py scripts/check_buf_breaking.py scripts/check_versions.py scripts/set_version.py
	uv run --project adapters/py/connectworkflow ruff format adapters/py/connectworkflow/src adapters/py/connectworkflow/tests
	uv run --project adapters/py/dagstercompat ruff format adapters/py/dagstercompat/tests
	uv run --project adapters/py/temporalcompat ruff format adapters/py/temporalcompat/src adapters/py/temporalcompat/tests
	uv run --project adapters/py/prefectcompat ruff format adapters/py/prefectcompat/src adapters/py/prefectcompat/tests
	uv run --project adapters/py/indexstore ruff format adapters/py/indexstore/src adapters/py/indexstore/tests

fmt-rs: ## rewrite Rust sources in place with cargo fmt (when cargo is installed)
	@if command -v cargo >/dev/null 2>&1; then \
		cargo fmt --all; \
	else \
		echo "Skipping Rust formatting; cargo is not on PATH (enter the Flox env)." >&2; \
	fi

fmt-check: ## fail if any Go source is not gofmt-clean
	@unformatted="$$($(GOFMT) -l . | grep -v '^core/go/gen/' || true)"; \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needs to run on:"; echo "$$unformatted"; \
		echo "run 'make fmt'"; \
		exit 1; \
	fi

vet: ## go vet across all packages
	$(GO) vet $(GO_PKGS)

lint: ## golangci-lint (config in .golangci.yml)
	$(GOLANGCI_LINT) run $(GO_PKGS)

test: test-go test-ts test-py test-rs ## the full test suite — every language, offline and hermetic

test-go: ## go test with the race detector
	$(GO) test -race $(GOFLAGS) $(GO_PKGS)

test-ts: ## run the TypeScript client tests and the console UI host tests (when npm is installed)
	@if command -v npm >/dev/null 2>&1; then \
		npm test; \
		if [ ! -d cmd/temporaless-console/ui/node_modules ]; then (cd cmd/temporaless-console/ui && npm ci); fi; \
		(cd cmd/temporaless-console/ui && npm test); \
	else \
		echo "Skipping TypeScript tests; npm is not on PATH." >&2; \
	fi

test-py: ## run the Python core and every Python adapter test suite
	uv run --project adapters/py/cloudevents --locked pytest adapters/py/cloudevents/tests
	uv run --project core/py --locked pytest core/py/tests
	uv run --project adapters/py/connectworkflow --locked pytest adapters/py/connectworkflow/tests
	uv run --project adapters/py/dagstercompat --locked pytest adapters/py/dagstercompat/tests
	uv run --project adapters/py/temporalcompat --locked pytest adapters/py/temporalcompat/tests
	PREFECT_LOGGING_LEVEL=ERROR PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW=ignore \
		uv run --project adapters/py/prefectcompat --locked pytest adapters/py/prefectcompat/tests
	uv run --project adapters/py/indexstore --locked pytest adapters/py/indexstore/tests

test-rs: ## run the Rust workspace tests (when cargo is installed)
	@if command -v cargo >/dev/null 2>&1; then \
		cargo test --workspace --locked; \
	else \
		echo "Skipping the Rust SDK tests; cargo is not on PATH (enter the Flox env)." >&2; \
	fi

build: build-console ## produce the artifacts locally — Go packages, the console, TypeScript dist, Rust workspace
	$(GO) build $(GO_PKGS)
	@if command -v npm >/dev/null 2>&1; then \
		npm run build; \
	else \
		echo "Skipping the TypeScript build; npm is not on PATH." >&2; \
	fi
	@if command -v cargo >/dev/null 2>&1; then \
		cargo build --workspace --locked; \
	else \
		echo "Skipping the Rust build; cargo is not on PATH (enter the Flox env)." >&2; \
	fi

build-console: ## build the optional read-only console UI and the binary that embeds it (build/temporaless-console)
	scripts/build-console

ts-check: ## run the TypeScript client build and tests
	npm run check

tidy-check: ## verify go.mod / go.sum are tidy
	$(GO) mod tidy -diff
