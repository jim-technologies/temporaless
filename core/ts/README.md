# Temporaless TypeScript

TypeScript support is a client SDK for the Temporaless protobuf and ConnectRPC
boundary. It provides generated `temporaless.v1` types, thin store/query
wrappers for Connect clients, and an optional Node-only invariantprotocol
projection.

It is intentionally not a workflow runtime. Go and Python own local replay and
activity execution. TypeScript callers can inspect, query, or write records
through `RecordStoreService` and `RecordQueryService` when a service exposes
those RPCs.

## Install From Git

The npm package entry lives at the repository root so clients can install it
directly from git without publishing to npm:

```sh
npm install "github:jim-technologies/temporaless#RELEASE_TAG" @connectrpc/connect-web
```

For a private repository, use the SSH form:

```sh
npm install "git+ssh://git@github.com/jim-technologies/temporaless.git#RELEASE_TAG" @connectrpc/connect-web
```

Replace `RELEASE_TAG` with the same root `vX.Y.Z` tag used by the Go, Python,
and Rust packages. Every Temporaless SDK and adapter shares `VERSION`; mutable
branch names are intentionally not installation examples.

Use `@connectrpc/connect-node` instead of `@connectrpc/connect-web` for Node
services.

## Usage

Create a Connect transport with the environment-specific Connect package, then
wrap it with the Temporaless client:

```ts
import { createConnectTransport } from "@connectrpc/connect-web";
import { ConnectStore } from "@jim-technologies/temporaless";

const transport = createConnectTransport({
  baseUrl: "https://temporaless.example.com",
});

const store = ConnectStore.fromTransport(transport);
const capability = await store.claimCapability();
```

Node services can use `@connectrpc/connect-node` to create the transport. The
Temporaless package only depends on the common `@connectrpc/connect` interfaces
so browser and server clients use the same wrapper.

## Invariant Protocol Projection

The `@jim-technologies/temporaless/invariant` subpath uses
`@jim-technologies/invariant-protocol` to project Temporaless RPCs into MCP,
CLI, HTTP/Connect, and descriptor-backed tool catalogs.

```ts
import {
  createTemporalessInvariantHttpProxy,
  runCli,
} from "@jim-technologies/temporaless/invariant";

const server = createTemporalessInvariantHttpProxy(
  "https://temporaless.example.com",
);

console.log(server.toolCatalog());
console.log(await runCli(server, ["RecordQueryService", "ListWorkflows"]));
```

This subpath reads the checked-in Temporaless descriptor set with source
comments. It is intended for Node services, CLIs, MCP hosts, and inspectors.
Browser clients should use the root package and provide a Connect transport.

## Development

Run these commands from the repository root:

```sh
npm install
npm run check
```
