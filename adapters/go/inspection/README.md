# Go inspection adapter

`inspection` implements the read-only
[`temporaless.v1.RunInspectionService`](../../../docs/inspection.md) directly
over bucket record stores. It needs no index: the workflow directory comes
from latest-run pointers, a workflow's runs from one bounded directory
listing, wakes from the due ledger, and `DescribeRun` from point reads.

```go
operator, _ := opendal.NewOperator(s3.Scheme, opendal.OperatorOptions{
    "bucket": "state", "root": "workflows/", // read-only credentials
})
renderer, _ := inspection.NewPayloadRenderer(appDescriptors, 0) // optional
service, err := inspection.NewService([]*inspection.Store{{
    ID:         "engine",
    Namespaces: []string{"default"},
    Records:    storage.NewOpenDALStore(operator),
    Bucket:     inspection.NewOpenDALBucket(operator),
    Payloads:   renderer,
}}, inspection.Options{Access: inspection.AllowAll})
```

- **Read-only.** No method writes, repairs, claims, or deletes. The due ledger
  is read as it is; entries a timer scanner would repair are reported as
  `CANONICAL_MISSING` or `CANONICAL_DIFFERS`, and torn entries are counted.
  `Store.DueTimers` is never called because it repairs. Tests fingerprint the
  store before and after every call.
- **Identity from payloads.** Listings order and page by the storage names
  Temporaless constructed. Every decoded record must construct the path it
  was read from; a run directory without a readable workflow record is
  counted, never shown with an identity parsed from its path.
- **Bounded.** `Limits` caps page size, reads per request (filtered pages
  return a continuation token when the budget runs out), run directories per
  workflow, objects per namespace listing, records per run kind (then
  `truncated`), and read concurrency. The concurrency cap is one budget per
  request: every point read, listing, and raw read of the request, nested
  fan-out included, waits for a slot of the same semaphore.
- **Scoped.** `Options.Access` decides, per caller, which stores are visible,
  which of each store's namespaces may be read, and whether payload values are
  returned or redacted. An empty namespace is always rejected. `AllowAll` is
  for loopback servers and tests.
- **Page tokens** are sealed by `Options.PageTokens` with a binding of method,
  store, namespace, and filters. `PlainPageTokens` only catches mistakes; a
  hosted server supplies a MAC-bound implementation.
- **Derivations** (`DeriveHistory`, `DerivePending`) are pure functions over
  `RunRecords`, exported for callers that already hold the records.
- **Payloads** render as ProtoJSON for well-known types, types linked into the
  binary, and types in an optional `FileDescriptorSet`; anything else keeps its
  opaque bytes, and payloads over the render limit report only type and size.
  The records in a `DescribeRun` response keep a stored Any only when ProtoJSON
  can marshal it with `protoregistry.GlobalTypes`, which the Connect/HTTP JSON
  and MCP projections use; otherwise, and whenever payloads are redacted, the
  record carries a `temporaless.v1.OpaquePayload` so the response marshals on
  every projection.

Claims are listed only when `Store.ListClaims` is set, because the Go point
store has no claim listing and claims may live elsewhere. Without it,
`DescribeRun` reports `claims_inspected=false` rather than "no claims".
