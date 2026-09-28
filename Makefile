# Temporaless top-level developer gate.
#
# `make validate` is the one gate verb (MAKEFILE-CONTRACT.md): it delegates to
# scripts/validate, the full cross-language gate (Buf + TypeScript + Go +
# Python/uv, plus Rust when installed) that CI runs. Go-only changes can get
# fast feedback from the individual sub-targets (fmt-check, tidy-check, vet,
# lint, test-go) without the full cross-language gate.
#
# Every target names one tool or one script; loops and conditionals live in
# scripts/. Run inside the Flox env so pinned Go is on PATH; the lint target
# runs the pinned golangci-lint module through `go run`:
#
#   flox activate -- make validate

GO        ?= go
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

fmt: ## rewrite formatting in place for every language in the repo
	scripts/fmt

fmt-go: ## rewrite Go sources in place with gofmt
	scripts/fmt go

fmt-proto: ## rewrite protobuf sources in place with buf format
	scripts/fmt proto

fmt-py: ## rewrite every Python project in scripts/python-projects with ruff format
	scripts/fmt py

fmt-rs: ## rewrite Rust sources in place with cargo fmt (when cargo is installed)
	scripts/fmt rs

fmt-check: ## fail if any Go source is not gofmt-clean
	scripts/gofmt-check

vet: ## go vet across all packages
	$(GO) vet $(GO_PKGS)

lint: ## golangci-lint (config in .golangci.yml)
	$(GOLANGCI_LINT) run $(GO_PKGS)

test: ## the full test suite — every language, offline and hermetic
	scripts/test

test-go: ## go test with the race detector (GO_PKGS narrows the packages)
	scripts/test go

test-ts: ## run the TypeScript client tests and the console UI host tests (when npm is installed)
	scripts/test ts

test-py: ## run every Python project's tests in scripts/python-projects
	scripts/test py

test-rs: ## run the Rust workspace tests (when cargo is installed)
	scripts/test rs

build: ## produce the artifacts locally — the console, Go packages, TypeScript dist, Rust workspace
	scripts/build

build-console: ## build the optional read-only console UI and the binary that embeds it (build/temporaless-console)
	scripts/build-console

ts-check: ## run the TypeScript client build and tests
	npm run check

tidy-check: ## verify go.mod / go.sum are tidy
	$(GO) mod tidy -diff
