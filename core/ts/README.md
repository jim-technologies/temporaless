# Temporaless TypeScript

TypeScript support is a client SDK for the Temporaless protobuf and ConnectRPC
boundary. It provides generated `temporaless.v1` types, thin store/query
wrappers for Connect clients, browser-compatible visual-plan helpers, and an
optional Node-only invariantprotocol projection.

It is intentionally not a workflow runtime. Go and Python own local replay and
activity execution. TypeScript callers can inspect, query, or write records
through `RecordStoreService` and `RecordQueryService` when a service exposes
those RPCs.

## Install From Git

The npm package entry lives at the repository root so clients can install it
directly from git without publishing to npm:

```sh
npm install --allow-git=all \
  "github:jim-technologies/temporaless#RELEASE_TAG" \
  @connectrpc/connect-web
```

For a private repository, use the SSH form:

```sh
npm install --allow-git=all \
  "git+ssh://git@github.com/jim-technologies/temporaless.git#RELEASE_TAG" \
  @connectrpc/connect-web
```

Replace `RELEASE_TAG` with the same root `vX.Y.Z` tag used by the Go, Python,
and Rust packages. Every Temporaless SDK and adapter shares `VERSION`; mutable
branch names are intentionally not installation examples.

npm 12 disables Git dependencies unless they are explicitly allowed. The
flag is also required for Temporaless's immutable Git-pinned Invariant
Protocol dependency.

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
const eventDelivery = await store.eventDeliveryCapability();
```

Node services can use `@connectrpc/connect-node` to create the transport. The
Temporaless package only depends on the common `@connectrpc/connect` interfaces
so browser and server clients use the same wrapper.

`eventDeliveryCapability()` reports whether the remote store can atomically
establish an event payload. `deliverEvent(record)` preserves the server's
created/idempotent disposition and typed Connect failure; it does not add
atomicity to an incapable backend. `putEvent(record)` is the low-level replace
operation and should stay behind an operator boundary.

## Visual Plans

An AI planner or graph editor can produce a protobuf `WorkflowPlan`, show it to
a user, and bind approval to the deterministic digest. Once the Go or Python
workflow starts, the same client can overlay durable run evidence on the plan:

```ts
import {
  inspectRun,
  projectWorkflowRun,
  validateWorkflowPlan,
  workflowPlanDigest,
} from "@jim-technologies/temporaless";

validateWorkflowPlan(plan);
const approvedSha256 = await workflowPlanDigest(plan);

const snapshot = await inspectRun(store, workflowKey);
const projection = projectWorkflowRun(plan, snapshot);
```

The plan describes intended boxes and arrows; it is not a second execution
language. The projection retains unplanned records and never invents running or
skipped states that are absent from storage.

## Invariant Protocol Projection

The `@jim-technologies/temporaless/invariant` subpath uses
`@jim-technologies/invariant-protocol` to project Temporaless RPCs into MCP,
CLI, HTTP/Connect, and descriptor-backed tool catalogs.

Application workflows are not limited to Temporaless's system descriptor.
Load the application's generated descriptor with Invariant's generic
`Server.fromDescriptor(...)`, then `connectHttp(...)` to the generated
ConnectRPC service whose methods are wrapped as workflows. The application RPC
remains the single canonical request/response contract. The
Temporaless-specific helpers below are conveniences only for
`RecordStoreService` and `RecordQueryService`; they are not a workflow
registry.

```ts
import {
  createTemporalessInvariantHttpProxy,
  runCli,
} from "@jim-technologies/temporaless/invariant";

const server = createTemporalessInvariantHttpProxy(
  "https://temporaless.example.com",
);

console.log(server.toolCatalog());
console.log(
  await runCli(server, ["temporaless.v1.RecordQueryService", "ListWorkflows"]),
);
```

The default catalog is inspection-only. It includes point reads, run-scoped
lists, and query reads, but excludes record writes, claim creation/deletion,
point-store `DueTimers` repair, retention `Sweep`, and all delete RPCs.
The same filter governs the optional MCP, CLI, and HTTP/Connect projections.

`includeOperatorMethods` is not native-gRPC authorization. Invariant's
`server.grpcServer()` exposes the registered native service surface regardless
of projection filters. Do not expose that server expecting the default catalog
to make it read-only; use separate native-gRPC authentication and per-RPC
authorization, or avoid serving native gRPC from this facade.

Operator methods require an explicit opt-in:

```ts
const operatorServer = createTemporalessInvariantHttpProxy(
  "https://temporaless-admin.example.com",
  {
    includeOperatorMethods: true,
    auth: {
      headerProvider: () => ({
        authorization: `Bearer ${process.env.TEMPORALESS_OPERATOR_TOKEN!}`,
      }),
    },
  },
);
```

That projection opt-in is intentionally dangerous. Use a narrowly scoped backend
credential, authenticate and authorize the inbound MCP/HTTP/CLI boundary, and
do not expose the operator catalog to untrusted users or a general-purpose
agent. The outbound `auth` option authenticates the proxy to ConnectRPC; it
does not protect the facade itself.

Generated implementations can be registered on the same descriptor-backed
server without adding a second Temporaless-specific handler shape:

```ts
import {
  createTemporalessInvariantServer,
  registerTemporalessInvariantServices,
} from "@jim-technologies/temporaless/invariant";

const server = createTemporalessInvariantServer({ includeQuery: false });
registerTemporalessInvariantServices(server, {
  recordStore: {
    getWorkflow() {
      return { found: false };
    },
  },
});
```

This subpath reads the checked-in Temporaless descriptor set with source
comments. It is intended for Node services, CLIs, MCP hosts, and inspectors.
Browser clients should use the root package and provide a Connect transport.

## Development

Run these commands from the repository root:

```sh
npm install --allow-git=all
npm run check
```
