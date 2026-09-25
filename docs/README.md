# Documentation map

Start with [architecture](architecture.md) and [readiness](readiness.md).
Together they describe the intended system, what ships, and the operational
responsibilities required for a serverless deployment.

| Task | Documents |
|---|---|
| Understand design choices | [Philosophy](philosophy.md), [comparisons](comparisons.md) |
| Write workflows | [Getting started](getting-started.md), [canonical protobuf workflows](canonical-workflows.md) |
| Deploy and resume | [Deployment](deployment.md), [scheduling](scheduling.md), [production checklist](production-checklist.md) |
| Recover and operate | [Runbook](runbook.md), [operator CLI](operator-cli.md), [hard cases](hard-cases.md), [claims](claims.md) |
| Emit logs and index records | [CloudEvents](cloudevents.md), [analytics](analytics.md), [ClickHouse and Iceberg](clickhouse-iceberg.md) |
| Implement storage/query boundaries | [Storage RPC](storage-rpc.md), [adapter contract](adapter-contract.md) |
| Adapt another orchestrator | [Portability](adapter-portability.md), [Temporal](temporal-adapter.md), [Dagster/Prefect](orchestrator-adapters.md) |
| Build a visual application | [Visual workflows](visual-workflows.md), [run inspection](inspection.md) |
| Choose SDKs and contribute | [SDK matrix](sdks.md), [dependencies](dependencies.md), [benchmarks](benchmarks.md), [developer gate](../MAKEFILE-CONTRACT.md) |

Backend-specific projection details are contracts for application-owned
adapters; they do not imply that an implementation is bundled. The readiness
page separates these contracts from tested framework capabilities.
