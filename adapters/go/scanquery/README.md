# OpenDAL Scan Query Adapter

`scanquery` is an explicit offline/development implementation of
`storage.QueryStore`. It walks an OpenDAL bucket, decodes protobuf payloads,
and validates that every embedded key reconstructs the object location before
returning or deleting anything.

Use it for small local instances, one-shot exports, tests, and index rebuilds.
It does not provide ordering or pagination and should not serve production
operator traffic. Production cross-run search and retention should use a
rebuildable SQL/DuckLake-style index behind `RecordQueryService`.

The core `storage.OpenDALStore` remains point-operation and run-listing only;
installing this adapter is an explicit choice to pay for full bucket scans.
