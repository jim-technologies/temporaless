# Temporaless for Python

Temporaless is an async-only, storage-first workflow runtime for protobuf unary
handlers. Every workflow and activity accepts exactly one generated protobuf
request and returns exactly one generated protobuf response. Replay state is
stored as deterministic protobuf binary records through an OpenDAL-backed
store; no coordinator process or SQL database is required by the core.

The package includes durable workflow/activity replay, retry policies, durable
sleep, optional timer-backed event/dependency polling, atomic create-once
external events on capable stores, claims, the point-operation ConnectRPC
storage service, small timer/cron operator primitives, and optional visual-plan
validation and run projection. Cross-run search and retention use an optional
derived query adapter.

Event/dependency waits are manual unless the call supplies a caller-owned
`PollOptions`. Python `OpenDALStore` advertises create-once event delivery only
when its operator supports `write_with_if_not_exists`; unsupported stores fail
closed rather than emulating conditional creation.

Install from an immutable Git commit:

```sh
pip install "temporaless @ git+https://github.com/jim-technologies/temporaless.git@COMMIT_SHA#subdirectory=core/py"
```

Use the same root `vX.Y.Z` release tag or immutable commit for core and every
adapter. All Temporaless Python distributions share the repository `VERSION`;
there is no adapter-specific version stream.

## Visual Plans

An AI planner or graph editor can produce a protobuf
`temporaless.v1.WorkflowPlan`, show it to a user, and bind approval to its
deterministic digest. Execution remains an ordinary typed workflow; stable plan
node IDs are reused as activity, timer, and event IDs.

```python
from temporaless import (
    WorkflowKey,
    inspect_run,
    plan_digest,
    project_workflow_run,
    validate_plan_with_descriptors,
    verify_approved_plan,
)
from google.protobuf import descriptor_pool

allowed_operations = {"exports.v1.ExportService.ExecuteExport"}
proposal_plan = type(plan).FromString(plan.SerializeToString(deterministic=True))
validate_plan_with_descriptors(
    proposal_plan,
    pool=descriptor_pool.Default(),
    allowed_operations=allowed_operations,
)
candidate_sha256 = plan_digest(proposal_plan)
# After the user approves, the application stores candidate_sha256 under an
# authenticated, opaque approval_id.

# At the trusted service boundary, after importing the application's generated
# protobuf module so its service descriptor is registered:
execution_plan = type(plan).FromString(plan.SerializeToString(deterministic=True))
approved_sha256 = await approvals.approved_digest(approval_id)
verify_approved_plan(
    execution_plan,
    approved_sha256,
    pool=descriptor_pool.Default(),
    allowed_operations=allowed_operations,
)
# Compile and execute execution_plan, not the caller-shared plan.

# After or during execution, overlay durable evidence on the approved plan.
inspection = await inspect_run(store, WorkflowKey("export", "run:plan-r1"))
projection = project_workflow_run(execution_plan, inspection)
```

The strict validator requires canonical unary protobuf RPC names, exact
request/response descriptors, and an explicit operation allowlist. The
projection preserves unplanned records and does not invent running or skipped
states. See
[`examples/py/data_pipeline.py`](../../examples/py/data_pipeline.py) for a
runnable sequence, fan-out, branch, replay, and backfill example.

See the repository
[README](https://github.com/jim-technologies/temporaless/blob/main/README.md)
and [getting-started guide](https://github.com/jim-technologies/temporaless/blob/main/docs/getting-started.md)
for the full API and deployment model.
