# Temporaless CloudEvents adapter

`temporaless-cloudevents` emits CNCF CloudEvents 1.0 after successful mutating
`RecordStoreService` calls. Install its server interceptor on the ConnectRPC
storage gateway, and route events to application-owned logs and query indexes.
Iceberg is the conventional downstream analytical target; no sink is required.

```python
from temporaless.connectstore import asgi_application
from temporaless_cloudevents import Options, RecordStoreInterceptor

# store, next_observation_id and publish_event are application dependencies.
# publish_event must be async and accepts the official SDK CloudEvent type.
app = asgi_application(
    store,
    interceptors=[
        RecordStoreInterceptor(
            Options(
                source="urn:example:workflow-storage",
                new_id=next_observation_id,
                publish=publish_event,
            )
        ),
    ],
)
```

`source` must be an absolute URI without whitespace or control characters.
`new_id` is a synchronous application callable returning a nonempty string
without control characters or surrounding whitespace, unique within `source`
for each observation. Retrying publication must preserve
the original event and ID. Both callbacks must support concurrent requests.
The async publisher owns credentials, routing,
request timeouts and any transport retries; the interceptor awaits it and does
not create background tasks. Attach this interceptor only to the server.

The event `type` is the generated full RPC method name, for example
`temporaless.v1.RecordStoreService.PutWorkflow`. `data` contains deterministic
protobuf binary of the existing `WorkflowKey`, `ActivityKey`, `TimerKey`,
`EventKey` or `ClaimKey`. `DeleteRun` carries its `WorkflowKey` and invalidates
all records in that run. `datacontenttype` is `application/protobuf` and
`dataschema` is `https://type.googleapis.com/<fully-qualified-key-message>`.
The SDK supplies an observation `time`; it is not a mutation revision or a
record lifecycle timestamp. Record inputs, results, annotations, failures and
claim owner IDs and unknown protobuf fields are excluded. The official SDK can
encode this byte payload in the CloudEvents HTTP binary binding or structured
JSON `data_base64`; framework records stay protobuf binary.

Observed RPCs are `PutWorkflow`, `PutActivity`, `PutTimer`, `PutEvent`,
`DeliverEvent`, `TryCreateClaim`, `DeleteWorkflow`, `DeleteActivity`,
`DeleteTimer`, `DeleteEvent`, `DeleteClaim` and `DeleteRun`. Every successful
response produces an observation, including an already absent delete,
duplicate delivery or rejected claim acquisition. Consumers must treat them as
invalidations and re-read authoritative state; the method name does not prove
that a state transition happened. Reads, other services and failed RPCs emit
nothing.

This is a best-effort observation boundary. Publisher and event-construction
errors are logged and preserve successful storage responses. Process crashes,
task cancellation, direct store writes, internal repairs and mutations from a
partially failed RPC can leave gaps. Task cancellation propagates normally,
including after storage has committed. This package has no durable outbox and
does not guarantee every transition, ordered delivery or exactly-once
publication. Cross-run reconciliation belongs in downstream adapters, and the
authoritative due-timer ledger must continue to drive wakes. Required audit
history needs a separately durable publication mechanism.

Downstream systems must accept protobuf key data or supply an application-owned
translator. A receiver that accepts only domain-specific JSON audit bodies is
not automatically compatible. See the shared
[CloudEvents contract](../../../docs/cloudevents.md).
