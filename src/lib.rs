use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

mod runtime;
mod connection;
mod cursor;
mod arrow_convert;
mod types;
mod errors;
mod ingest;

use connection::TdsConnection;

#[pyclass]
pub struct NativeConnection {
    inner: Arc<Mutex<TdsConnection>>,
}

#[pymethods]
impl NativeConnection {
    #[new]
    fn new(connection_str: &str) -> PyResult<Self> {
        let conn = TdsConnection::new(connection_str)?;
        Ok(NativeConnection { inner: Arc::new(Mutex::new(conn)) })
    }

    fn close(&self) -> PyResult<()> {
        self.inner.lock().unwrap().close()
    }

    fn commit(&self) -> PyResult<()> {
        self.inner.lock().unwrap().commit()
    }

    fn rollback(&self) -> PyResult<()> {
        self.inner.lock().unwrap().rollback()
    }

    fn set_autocommit(&self, value: bool) -> PyResult<()> {
        let mut conn = self.inner.lock().unwrap();
        if value && conn.in_transaction {
            drop(conn);
            self.commit()?;
            self.inner.lock().unwrap().autocommit = value;
        } else {
            conn.autocommit = value;
        }
        Ok(())
    }

    fn get_autocommit(&self) -> bool {
        self.inner.lock().unwrap().autocommit
    }

    /// Execute SQL and return results as Arrow RecordBatches (zero-copy via FFI).
    /// Returns a list of PyArrow RecordBatch objects.
    fn execute_arrow(&self, py: Python<'_>, sql: &str, batch_size: Option<usize>) -> PyResult<PyObject> {
        let bs = batch_size.unwrap_or(65536);
        let mut conn = self.inner.lock().unwrap();
        conn.begin_if_needed()?;
        let client = conn.get_client()?;
        drop(conn);

        let result = cursor::execute_to_arrow(&client, sql, bs)?;

        match result {
            cursor::ExecResult::Query { batches, .. } => {
                cursor::batches_to_pyarrow_table(py, &batches)
            }
            cursor::ExecResult::Dml { rowcount } => {
                Ok(rowcount.into_pyobject(py)?.into_any().unbind())
            }
        }
    }

    /// Execute SQL and return results as list of tuples (DB-API style).
    fn execute_rows(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        let mut conn = self.inner.lock().unwrap();
        conn.begin_if_needed()?;
        let client = conn.get_client()?;
        drop(conn);

        let (columns, rows) = cursor::execute_to_rows(&client, sql)?;

        let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let py_rows: Vec<PyObject> = rows.iter()
            .map(|r| cursor::row_to_py_tuple(py, r))
            .collect::<PyResult<Vec<_>>>()?;

        let result = pyo3::types::PyDict::new(py);
        result.set_item("columns", col_names)?;
        result.set_item("rows", py_rows)?;
        Ok(result.into_any().unbind())
    }

    /// Execute simple SQL (DDL, DML) without results
    fn execute_simple(&self, sql: &str) -> PyResult<i64> {
        let mut conn = self.inner.lock().unwrap();
        conn.begin_if_needed()?;
        let client = conn.get_client()?;
        drop(conn);

        let result = cursor::execute_to_arrow(&client, sql, 1)?;
        match result {
            cursor::ExecResult::Dml { rowcount } => Ok(rowcount),
            cursor::ExecResult::Query { .. } => Ok(-1),
        }
    }

    /// Ingest a PyArrow Table into SQL Server
    fn ingest(&self, table_name: &str, table: &Bound<'_, PyAny>, mode: &str) -> PyResult<i64> {
        let mut conn = self.inner.lock().unwrap();
        conn.begin_if_needed()?;
        let client = conn.get_client()?;
        drop(conn);

        ingest::ingest_arrow_table(&client, table_name, table, mode)
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeConnection>()?;
    Ok(())
}
