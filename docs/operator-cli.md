# Operator CLI

The bundled `cmd/temporaless` binary is a thin local-filesystem operator tool.
It registers only the OpenDAL `fs` service and reads the same protobuf records
used by replay. For cloud stores, use authenticated
`RecordStoreService`/`RecordQueryService` clients or generated remote operator
tooling; do not add cloud credentials to this binary.

## Describe One Run

`describe-run` composes point reads into one operator-friendly view of a run:

```sh
go run ./cmd/temporaless \
  --store-root /var/lib/temporaless \
  describe-run \
  --namespace default \
  --workflow-id prices:aapl \
  --run-id 2026-08-12
```

Put `--json` before the subcommand for machine-readable output:

```sh
go run ./cmd/temporaless \
  --store-root /var/lib/temporaless \
  --json \
  describe-run \
  --workflow-id prices:aapl \
  --run-id 2026-08-12
```

The command gets the workflow record and lists that run's activity, timer,
event, and, when supported, claim records. Records within each kind are sorted
by their protobuf key, so unchanged durable state produces stable output. It
uses only the point-store surface; a query index and `RecordQueryService` are
not required.

The JSON envelope has `formatVersion: 1` and the top-level fields `key`,
`workflow`, `activities`, `timers`, `events`, `claims`, `claimsInspected`, and
`snapshotConsistency`. Record bodies follow the pre-release `temporaless.v1`
protobuf schema and may evolve with that schema; consumers should check the
format version rather than infer a contract from field presence.

This is durable evidence, not an event history or a transactionally consistent
snapshot. Each returned protobuf record is authoritative, but the command
performs several reads, so a workflow writing concurrently can change between
them. The bundled local `fs` configuration does not expose run-scoped claim
listing and therefore always reports `claimsInspected: false`; its empty
`claims` array is not proof that no claim exists. Custom or authenticated
operator tooling may supply a claim lister and report `true`.

Application payloads are `google.protobuf.Any`. The local CLI does not fetch
or guess application descriptors. Its JSON representation therefore keeps an
unknown value in an opaque envelope containing its type URL and base64-encoded
protobuf bytes:

```json
{"typeUrl":"type.googleapis.com/acme.v1.Request","valueBase64":"CAE="}
```

Decode those bytes with the exact application descriptor when a typed view is
needed. This is intentionally Temporaless CLI JSON, not standard ProtoJSON;
objects containing this envelope cannot be passed directly to
`protojson.Unmarshal`. The same descriptor-free representation is used by the
other CLI JSON and JSON Lines commands, so application-defined payloads never
make generic inspection fail merely because their generated code is not
linked.

To render intended flow as well as actual durable evidence, pair this snapshot
with an application-supplied `WorkflowPlan`; see
[`visual workflows`](visual-workflows.md).
