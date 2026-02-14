//! Error mapping: tabby TDS errors → Python exceptions.
//!
//! Translates tabby's `Error` enum into appropriate Python exception
//! types so that DB-API consumers get meaningful error classes.

use pyo3::prelude::*;
use tabby::error::Error as TdsError;

/// Convert a tabby TDS error into a Python exception.
///
/// - Server errors (SQL syntax, permission, constraint) → `RuntimeError`
/// - I/O errors (connection lost, timeout) → `ConnectionError`
/// - Everything else → `RuntimeError`
pub fn to_pyerr(e: TdsError) -> PyErr {
    let msg = format!("{}", e);
    match &e {
        TdsError::Server(_) => pyo3::exceptions::PyRuntimeError::new_err(msg),
        TdsError::Io { .. } => pyo3::exceptions::PyConnectionError::new_err(msg),
        _ => pyo3::exceptions::PyRuntimeError::new_err(msg),
    }
}
