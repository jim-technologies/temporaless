// The public `RunError` enum carries a handful of variants that include
// boxed errors from upstream crates; clippy considers the resulting Result
// "large" (~128B). The error type IS the public surface — reshaping it just
// to satisfy the lint would worsen ergonomics for callers. Allow it.
#![allow(clippy::result_large_err)]

//! Temporaless — experimental Rust SDK.
//!
//! # Scope (intentional)
//!
//! This crate ships an [`OpenDALStore`](storage::OpenDALStore), a partial
//! workflow/activity runtime, and an in-process dispatcher. Storage uses the
//! canonical flat v2 paths and protobuf binary records shared with Go and
//! Python.
//!
//! It does NOT yet ship the full first-class SDK surface: durable sleeps and
//! durable activity backoff, events, production claim integration,
//! ConnectStore, cron, and indexed timer scanning remain Go/Python features.
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

pub mod dispatch;
pub mod storage;
pub mod workflow;

#[allow(clippy::all)]
#[allow(missing_docs)]
pub mod v1 {
    //! Generated protobuf types — `temporaless.v1.*`. Built via `build.rs`
    //! with `prost-build` + `protox` at compile time.
    include!(concat!(env!("OUT_DIR"), "/temporaless.v1.rs"));
}

pub mod reserved_names {
    //! Framework-reserved names generated from protobuf Edition defaults.
    include!(concat!(env!("OUT_DIR"), "/reserved_names.rs"));
}

pub use storage::{
    ActivityKey, ClaimKey, EventKey, OpenDALStore, Store, StoreError, TimerKey, WorkflowKey,
};
pub use workflow::{
    ActivityError, ActivityOptions, RetryPolicy, RunError, Workflow, WorkflowOptions, annotate,
    current, execute_activity, run,
};
