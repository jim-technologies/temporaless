# Dependency Choices

## Flox

Flox contains only tools or runtime libraries needed before language package managers can run:

- `go`: required for the Go core and Go module tooling.
- `python313`: required runtime for Python development.
- `uv`: required Python dependency manager.
- `buf`: required protobuf formatter, linter, and generator.
- `libffi`: required by the Apache OpenDAL Go binding.
- `gcc-unwrapped` `lib` output: provides `libstdc++.so.6` for Python Protovalidate's RE2 dependency.

The environment intentionally does not include `make`, `protoc`, or language-specific linters. Go and uv already handle those needs for this repository.

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
- `ruff`: formatter and linter (Astral, Rust-based).
- `ty`: type checker (Astral, Rust-based). Replaces pyright; pinned `>=0.0.34` until 0.1+ ships.
- `types-protobuf`: type stubs for `google.protobuf.*_pb2` modules so ty resolves `Timestamp`, `Duration`, `Any`, etc. The runtime `protobuf` wheel ships no `.pyi` stubs.
- `pytest`: tests.

## Python Temporal Adapter

The Python Temporal adapter has its own uv project in `adapters/py/temporalcompat`:

- `temporalio`: strict Temporal compatibility adapter only. It is intentionally outside `core/py` so the core runtime does not depend on Temporal.
- `temporaless`: editable path dependency on `core/py` for generated protobuf types and core conventions.
- `ruff`, `ty`, and `pytest`: adapter-local quality gate. (`types-protobuf` is also a dev dep so ty resolves `google.protobuf` modules in tests.)

## Buf

Buf remote plugins are used in `buf.gen.yaml`. That keeps protoc plugins out of Flox and avoids a second tool installation layer. `buf.yaml` also tracks the Protovalidate schema dependency used by validation annotations.
