//! Shared Tokio runtime for async TDS operations.
//!
//! pounce is a sync Python extension that wraps async Rust (tabby + tokio).
//! This module provides a lazily-initialised multi-threaded Tokio runtime
//! that all TDS operations share. Two worker threads is enough — we're
//! I/O-bound on a single TDS connection anyway.

use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

/// Global Tokio runtime, created on first use.
pub static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

/// Run an async future to completion on the shared runtime.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    RUNTIME.block_on(future)
}
