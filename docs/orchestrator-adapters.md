# Orchestrator Adapters

Temporaless should not try to become Dagster or Prefect, and it should not add compatibility adapters for them yet.

## Dagster

Dagster is asset-first. Its docs define assets as persistent objects such as tables, files, and models, with asset definitions describing how to produce and update them. Jobs are the execution and monitoring unit, and schedules or sensors launch runs of jobs.

Useful future adapter:

- a Dagster asset or op wrapper that invokes a Temporaless workflow with a protobuf input
- a Dagster schedule or sensor that creates explicit Temporaless workflow IDs and run IDs
- optional asset metadata linking Dagster materializations to Temporaless workflow records

Not useful now:

- pretending Temporaless can replace Dagster's asset catalog, run queue, UI, partition model, sensors, or asset checks

## Prefect

Prefect is flow/deployment-first. Its docs describe deployments as server-side representations of flows that store orchestration metadata, scheduling, event triggers, and infrastructure choices. Prefect schedules support cron, interval, and RRule styles.

Useful future adapter:

- a Prefect task or flow wrapper that invokes a Temporaless workflow with a protobuf input
- a Prefect deployment helper that maps deployment run metadata to explicit Temporaless workflow/run IDs
- a schedule bridge that lets Prefect create Temporaless workflow runs

Not useful now:

- importing Prefect's server/deployment model into Temporaless core
- trying to make Temporaless mimic Prefect workers, work pools, automations, or UI state

## Decision

Build these only as outbound integration adapters after the core market-data workflow path is stable. The first adapter priority remains Temporal compatibility because existing Temporal usage has the closest semantic overlap with this framework.

Sources:

- https://docs.dagster.io/guides/build/assets
- https://docs.dagster.io/guides/build/jobs
- https://docs.dagster.io/guides/automate/schedules
- https://docs.dagster.io/guides/automate/sensors
- https://docs.prefect.io/v3/concepts/deployments
- https://docs.prefect.io/v3/concepts/schedules
