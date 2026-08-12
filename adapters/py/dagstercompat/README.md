# Temporaless Dagster Process-Boundary Proof

This development-only, non-installable uv project is an executable
compatibility proof for the supported Dagster integration direction:

```text
Dagster 1.13.17 process (protobuf 6)
        │ generated application ConnectRPC client
        │ protobuf binary over HTTP
        ▼
application workflow service (Temporaless lives here, protobuf 7)
```

It is **not** a Temporaless SDK package or a same-process adapter. Its uv
project sets `package = false`, has no distributable Python module, and does
not participate in Temporaless release versioning. It deliberately has no
`temporaless` dependency and must never import Temporaless framework modules
or copy the Temporaless framework proto. Dagster 1.13.17 officially requires
`protobuf>=4,<7` on Python 3.11+, which cannot coexist with Temporaless's
protobuf 7 runtime.

The test fixture contains only one tiny application RPC. Buf generates two
copies of that application contract: a protobuf-6 client for Dagster and a
protobuf-7 service for Temporaless. A real Dagster job calls the separate
Temporaless process twice with the same explicit `workflow_id` and `run_id`.
That server uses the real `OpenDALStore` `fs` backend and the real
`wrap_workflow_method` ConnectRPC adapter. Both calls return the same persisted
protobuf response, while a body-side-effect witness proves the application
body ran exactly once.

The server binds its own ephemeral listening socket before publishing an
atomic readiness file, so the test does not reserve a free port and race a
second bind. The test also asks Buf to compile `tests/proto` and compares that
source descriptor with both generated runtimes. Schema edits therefore fail
the gate until both fixtures are regenerated; generated files cannot silently
drift from the application proto.

Application users should generate Dagster-side clients from their own
application `.proto`; there is intentionally no `temporaless_dagstercompat`
module to import.

Run the isolated proof with:

```bash
flox activate -- uv sync --project adapters/py/dagstercompat --locked
flox activate -- uv run --project adapters/py/dagstercompat \
  pytest adapters/py/dagstercompat/tests
```

When the test application schema changes, regenerate both protobuf majors
from the single source (trusted environments need BSR access for the remote
protobuf plugins):

```bash
flox activate -- buf generate \
  --template adapters/py/dagstercompat/buf.gen.yaml
```

Semantic limits remain explicit: Dagster owns assets, jobs, schedules,
sensors, partitions, lineage, retries, and its run state. Temporaless owns
workflow replay only across the remote application RPC. A Dagster retry must
reuse the same application workflow/run IDs to receive replay.
