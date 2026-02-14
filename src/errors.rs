use pyo3::prelude::*;
use tiberius::error::Error as TibError;

pub fn to_pyerr(e: TibError) -> PyErr {
    let msg = format!("{}", e);
    match &e {
        TibError::Server(_) => pyo3::exceptions::PyRuntimeError::new_err(msg),
        TibError::Io { .. } => pyo3::exceptions::PyConnectionError::new_err(msg),
        _ => pyo3::exceptions::PyRuntimeError::new_err(msg),
    }
}
