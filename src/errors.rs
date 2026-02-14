use pyo3::prelude::*;
use crate::tabby::error::Error as TdsError;

pub fn to_pyerr(e: TdsError) -> PyErr {
    let msg = format!("{}", e);
    match &e {
        TdsError::Server(_) => pyo3::exceptions::PyRuntimeError::new_err(msg),
        TdsError::Io { .. } => pyo3::exceptions::PyConnectionError::new_err(msg),
        _ => pyo3::exceptions::PyRuntimeError::new_err(msg),
    }
}
