# CloudEvents record observations

An optional ConnectRPC server interceptor emits CNCF CloudEvents after
successful `RecordStoreService` mutations. Downstream consumers own logging,
indexing, and Iceberg projection; core replay stays independent of those sinks.

```go
import (
    "context"
    "net/http"

    "connectrpc.com/connect"
    "github.com/cloudevents/sdk-go/v2/event"
    "github.com/jim-technologies/temporaless/adapters/go/cloudevents"
    "github.com/jim-technologies/temporaless/adapters/go/connectstore"
    "github.com/jim-technologies/temporaless/core/go/storage"
)

func recordHandler(
    store storage.Store,
    nextObservationID func() string,
    publish func(context.Context, event.Event) error,
) (http.Handler, error) {
    observer, err := cloudevents.NewInterceptor(cloudevents.Options{
        Source: "urn:example:production:records",
        NewID: nextObservationID,
        Publish: publish,
    })
    if err != nil {
        return nil, err
    }
    _, handler := connectstore.NewHTTPHandler(store, connect.WithInterceptors(observer))
    return handler, nil
}
```

Mount behind your existing RPC authorization. `NewID` and `Publish` must be
safe for concurrent calls. The application allocates observation IDs unique
within `Source`; the publisher bounds its I/O and preserves each event on
transport retries. The official CloudEvents SDK can encode HTTP binary or
structured messages. The adapter does not choose a transport or start a task.

Events contain only deterministic protobuf keys, without unknown fields or
record payloads. The type is the generated method full name. Successful
idempotent repeats also emit: consumers re-read current state, including after
a delete observation. `DeleteRun` invalidates the entire run.

Publisher errors and callback panics are logged without failing an already
successful storage RPC. Process loss can still occur after storage commits and
before publication. Direct store writes, failed/partial RPCs, internal repairs,
and query sweeps are not observed. Periodic reconciliation is required; there
is no durable publication journal or complete audit history.

See the shared [event contract](../../../docs/cloudevents.md) for exact
attributes, method coverage, ordering, and recovery guarantees.
