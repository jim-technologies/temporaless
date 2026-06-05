# ConnectRPC Store Adapter

This is a decision adapter for exposing Temporaless record storage over ConnectRPC.

## Purpose

The adapter exposes the protobuf record store service so a process can read and write workflow, activity, timer, and claim records remotely.

## Supported Behavior

- protobuf request and response messages only
- generated ConnectRPC handlers
- generated ConnectRPC clients wrapped as `storage.Store`
- same record keys and protobuf binary records as the core storage package
- generated storage capability response for claim support

## Rejected Behavior

- no non-protobuf payloads
- no custom codecs
- no server-side workflow execution
- no lock service or scheduler behavior

This adapter is a transport boundary for storage records. It is not a Temporal frontend or worker service.
