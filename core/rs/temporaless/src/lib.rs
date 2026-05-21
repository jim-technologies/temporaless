// The public `RunError` enum carries a handful of variants that include
// boxed errors from upstream crates; clippy considers the resulting Result
// "large" (~128B). The error type IS the public surface — reshaping it just
// to satisfy the lint would worsen ergonomics for callers. Allow it.
#![allow(clippy::result_large_err)]

//! Temporaless — Rust SDK (storage layer).
//!
//! # Scope (intentional)
//!
//! This crate currently ships the **storage layer only**: an
//! [`OpenDALStore`](storage::OpenDALStore) that reads/writes the same
//! Hive-partitioned protobuf records the Go and Python SDKs use, so
//! Rust-side tooling (analytics, custom inspectors, MCP servers, future
//! workflow runtime) interoperates with workflows authored in either of
//! the other languages.
//!
//! It does NOT yet ship: workflow.run, activity replay, claims, durable
//! retries, ConnectStore client/server, cron scheduler, timer scanner,
//! janitor. Those are tracked separately; the storage layer is the
//! prerequisite.
//!
//! # OpenDAL native
//!
//! Unlike the Go (`opendal-go-services`) and Python (`opendal` PyO3
//! binding) SDKs which call into the Rust core via FFI, this crate uses
//! the native `opendal` crate directly. Same wire format, same paths —
//! workflows authored in Python or Go are fully readable here.
//!
//! # Conventions
//!
//! - All public types are re-exported from this crate root. Submodule
//!   imports are an internal detail.
//! - Async-only. The Python SDK is async-only end-to-end; Rust matches.
//! - Generated proto types live under [`v1`] and mirror the Go/Python
//!   `temporalessv1` / `temporaless_pb2` namespaces.

pub mod storage;
pub mod workflow;

#[allow(clippy::all)]
#[allow(missing_docs)]
pub mod v1 {
    //! Generated protobuf types — `temporaless.v1.*`. Built via `build.rs`
    //! with `prost-build` + `protox` at compile time.
    include!(concat!(env!("OUT_DIR"), "/temporaless.v1.rs"));
}

pub use storage::{
    ActivityKey, ClaimKey, EventKey, OpenDALStore, Store, StoreError, TimerKey, WorkflowKey,
};
pub use workflow::{
    activity, annotate, current, default_retry_policy, execute_activity, run, ActivityError,
    ActivityOptions, RetryPolicy, RunError, Workflow, WorkflowOptions,
};
