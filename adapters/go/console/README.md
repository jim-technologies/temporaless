# Go console adapter

The optional, read-only operator console. Core Temporaless ships no UI; this
adapter and [`cmd/temporaless-console`](../../../cmd/temporaless-console) are
opt-in. Operator documentation: [`docs/console.md`](../../../docs/console.md).

- **`TerminalService`** projects [`RunInspectionService`](../../../docs/inspection.md)
  onto the public terminal-core dashboard contract (vendored under
  `third_party/`), so the bundled `templates/executions.json` renders
  workflows, runs, a run's summary, pending state, derived history, payloads,
  the whole `DescribeRun` response (the contract's `json` case), and
  scheduled wakes with generic widgets. Only `Get` and `ListSources` are
  implemented; streams, AI, and actions answer Unimplemented. Every source
  answers in the payload case its declared shape names. Sources that depend
  on a selection the operator has not made yet answer with one row, event,
  object, or document that says what to pick. Times are formatted server-side in
  the zone the `tz` parameter names (UTC by default), with the zone in every
  time column's label.
- **`NewInvariantServer`** registers both services on one Invariant Protocol
  server. The HTTP, MCP, and CLI projections include only the read methods
  (`ProjectedMethods`); request validation runs on every call.
- **`RegisterPayloadTypes`** registers an application `FileDescriptorSet`
  process-wide. Those projections resolve `Any` payloads through the global
  registry, so this is what makes `DescribeRun` show a store's own types as
  typed ProtoJSON; conflicting definitions are refused, never overwritten.
- **`Handler`** serves the authenticated API (every `POST`), the UI and
  `/ui/config` (no secrets), and `/healthz` / `/readyz`, with a strict CSP.

## Authentication and authorization

One control per boundary, all optional pieces behind small interfaces:

| Mode | Authenticator | Access |
|---|---|---|
| Loopback development | none (`LoopbackPrincipal`) | every store; bind to a loopback address only |
| Private-network operator | `StaticToken` from a mounted file | every store |
| Hosted, multi-tenant | `JWTVerifier` (ES256/RS256 against a JWKS URL or file; issuer, audience, expiry; configurable subject and tenant claims) | `ScopedAccess`: a store is visible only to its own tenant and only when the `Authorizer` (e.g. `OpenFGA`) allows its read relation; payloads need the payload relation (`can_read` by default, `can_write` to limit them to editors) |

Scope is forced server-side from the verified token; a request can never name
another tenant. OpenFGA outages fail closed. `MACPageTokens` seal listing
cursors with HMAC-SHA256 over the method, store, namespace, filters, the
caller's tenant, and an expiry, so a page token cannot be replayed under
another scope.

Run the console with read-only bucket credentials: nothing in it writes.
