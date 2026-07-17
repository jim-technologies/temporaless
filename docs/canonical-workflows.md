# Canonical Protobuf Workflows

An application workflow starts as an ordinary unary protobuf service method:

```proto
service PriceService {
  rpc FetchPrices(FetchPricesRequest) returns (FetchPricesResponse);
}
```

The generated request and response messages are the only application payload
shape. Temporaless does not add an argument bag, JSON codec, workflow
definition registry, or a second transport contract.

## Registration Means Wrapping the Generated Method

There is deliberately no separate `register_as_workflow` control plane.
Registration is the one options-driven wrapper at the service boundary:

- Python decorates the generated async ConnectRPC implementation method with
  `wrap_workflow_method`.
- Go calls `connectworkflow.Handle` from the generated ConnectRPC
  implementation method.
- A cloud function or queue consumer can call the core `wrap_workflow`/`run`
  boundary directly with the same request and response messages.

The application still mounts the generated service normally. Authentication,
rate limiting, tracing, and routing remain ordinary ConnectRPC middleware.
The wrapper adds durable replay, activities, retries, sleeps, events, and
optional claim-based single-flight execution behind the unchanged RPC.

Python:

```python
class PriceService:
    def __init__(self, store: Store) -> None:
        self.store = store

    @wrap_workflow_method(
        WorkflowMethodWrapOptions(
            store=lambda service: service.store,
            result_type=FetchPricesResponse,
            options_for=lambda _service, request: Options(
                workflow_id=request.workflow_id,
                run_id=request.run_id,
                code_version="prices-v1",
                claim_owner_id="price-service",
            ),
        )
    )
    async def fetch_prices(
        self,
        request: FetchPricesRequest,
        ctx: RequestContext,
    ) -> FetchPricesResponse:
        return await current_workflow().execute_activity(
            ActivityOptions(activity_id="fetch:vendor"),
            request,
            FetchPricesResponse,
            fetch_from_vendor,
        )


app = PriceServiceASGIApplication(PriceService(store))
```

Go keeps the generated handler signature and supplies
`workflow.WorkflowWrapOptions` to `connectworkflow.Handle`. The runnable
`examples/go/quant-service` and `examples/py/quant_service.py` examples show
the wrapper shape with well-known protobuf messages; replace those messages
with application-generated types and mount the generated service class or
handler in a deployment.

Every ID remains application-owned. `workflow_id` and `run_id` should normally
be fields in the application request or values from application routing.
Supplying `claim_owner_id` opts the run into live single-flight execution when
the configured claim store supports atomic create-if-absent. Without it,
terminal calls still replay, while overlapping first calls are at-least-once.

## Invariant Protocol Uses the Same Descriptor

Invariant Protocol is a projection of the protobuf service, not another
workflow runtime. Point it at the application descriptor and the mounted
ConnectRPC service:

```ts
import { Server } from "@jim-technologies/invariant-protocol";

const server = Server.fromDescriptor("./gen/application_descriptor.binpb");
server.connectHttp("https://workflow.example.com");

console.log(server.toolCatalog());
```

The same `PriceService.FetchPrices` method can then be exposed through
Invariant's typed tool catalog, MCP, CLI, or HTTP projections while its actual
execution still crosses the generated ConnectRPC boundary and runs through
Temporaless. No Temporaless-specific tool schema is maintained.

`registerTemporalessInvariantServices` is only a convenience for
Temporaless's own `RecordStoreService` and `RecordQueryService`. Application
workflow services should use their own descriptor with Invariant's generic
`Server.register` or `connectHttp` APIs.

## What Can Be Reused Across Orchestrators

Import-only migration is possible only for code that already obeys the unary
protobuf convention and does not call runtime-specific APIs.

| Existing code | Reuse |
|---|---|
| Async unary protobuf activity/business handler | Body can remain unchanged; wrap it at the target boundary. |
| Generated ConnectRPC service method | Signature and transport stay unchanged; add the Temporaless method wrapper. |
| Temporal or Prefect workflow orchestration | Requires a small mechanical rewrite for activity dispatch, sleep, events, and explicit IDs. |
| Temporal signals, queries, children, converters, or arbitrary arguments | No drop-in mapping; keep native Temporal or redesign the contract explicitly. |
| Dagster assets/jobs | Use a separate Dagster process that invokes the canonical ConnectRPC method; asset and lineage semantics remain Dagster-owned. |

The shipped Temporal and Prefect adapters are the outbound direction:
Temporaless-shaped handlers running on those real runtimes. They do not
pretend to execute arbitrary existing Temporal or Prefect workflow code on
Temporaless storage.

## Showing Users What Will Execute

The protobuf request is also the clean UI contract. If an AI proposes a plan,
model that plan as application protobuf fields—such as repeated typed steps,
dependencies, and an approval token—render that message in the UI, and submit
the exact confirmed message to the workflow RPC. Store the proposed/confirmed
plan in the application's own service schema when it must be queried before
execution.

Temporaless records the request and each executed activity boundary. It does
not infer a static DAG from arbitrary Python or Go control flow, so a UI should
not claim that an inferred graph is authoritative. The confirmed protobuf plan
is authoritative before execution; workflow and activity records are
authoritative after execution.
