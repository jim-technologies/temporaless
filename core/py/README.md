# Temporaless for Python

Temporaless is an async-only, storage-first workflow runtime for protobuf unary
handlers. Every workflow and activity accepts exactly one generated protobuf
request and returns exactly one generated protobuf response. Replay state is
stored as deterministic protobuf binary records through an OpenDAL-backed
store; no coordinator process or SQL database is required by the core.

The package includes durable workflow/activity replay, retry policies, durable
sleep, external events, claims, the point-operation ConnectRPC storage service,
and small timer/cron operator primitives. Cross-run search and retention use an
optional derived query adapter.

Install from an immutable Git commit:

```sh
pip install "temporaless @ git+https://github.com/jim-technologies/temporaless.git@COMMIT_SHA#subdirectory=core/py"
```

Use the same root `vX.Y.Z` release tag or immutable commit for core and every
adapter. All Temporaless Python distributions share the repository `VERSION`;
there is no adapter-specific version stream.

See the repository
[README](https://github.com/jim-technologies/temporaless/blob/main/README.md)
and [getting-started guide](https://github.com/jim-technologies/temporaless/blob/main/docs/getting-started.md)
for the full API and deployment model.
