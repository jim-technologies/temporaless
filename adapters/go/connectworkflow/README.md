# ConnectRPC Workflow Trigger Adapter

`connectworkflow` adapts Temporaless' unary protobuf workflow wrapper to a
standard ConnectRPC unary handler. The core workflow package remains
transport-agnostic.

Use `connectworkflow.Handle` inside a generated service method. It unwraps the
Connect request, calls `workflow.WrapWorkflow`, wraps the protobuf response,
and maps known framework errors to standard ConnectRPC codes. Unknown errors
pass through unchanged.

Use `connectworkflow.ErrorToCode` when invoking the core wrapper directly at a
custom ConnectRPC boundary.

This adapter does not own storage, routing, authentication, scheduling, or
workflow IDs. Configure those explicitly in the application.
