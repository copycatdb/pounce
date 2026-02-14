//! # pounce — Arrow-native SQL Server driver
//!
//! PyO3 extension module that bridges Python ↔ tabby (Rust TDS) ↔ Arrow.
//!
//! ```text
//! Python (PyArrow)  →  pounce (this crate)  →  tabby (TDS 7.4+)  →  SQL Server
//!                         ↕ Arrow FFI
//!                      zero-copy
//! ```
//!
//! The main entry point is [`NativeConnection`], exposed to Python as
//! `pounce._native.NativeConnection`. The Python DB-API layer in
//! `pounce/dbapi.py` wraps it with cursor semantics, context managers,
//! and lazy row materialisation.

#![allow(unexpected_cfgs)]

use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

mod arrow_convert;
mod connection;
mod cursor;
mod errors;
mod ingest;
mod runtime;
mod types;

use connection::TdsConnection;

/// Native SQL Server connection, exposed to Python via PyO3.
///
/// Holds a tabby `Client<TcpStream>` behind a `Mutex`. All SQL execution
/// flows through this struct; the Python-side `Connection` and `Cursor`
/// classes in `dbapi.py` delegate here.
///
/// Thread safety: the `Mutex` allows sharing across Python threads, but
/// only one operation runs at a time (no concurrent queries on a single
/// TDS session — that's a protocol constraint, not a limitation).
#[pyclass]
pub struct NativeConnection {
    inner: Arc<Mutex<TdsConnection>>,
}

#[pymethods]
impl NativeConnection {
    /// Open a new TDS connection.
    ///
    /// Parses an ADO-style connection string and establishes a TCP+TLS
    /// session to SQL Server via tabby.
    #[new]
    fn new(connection_str: &str) -> PyResult<Self> {
        let conn = TdsConnection::new(connection_str)?;
        Ok(NativeConnection {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    /// Close the connection and drop the TDS session.
    fn close(&self) -> PyResult<()> {
        self.inner.lock().unwrap().close()
    }

    /// Commit the current transaction (sends `COMMIT TRANSACTION`).
    fn commit(&self) -> PyResult<()> {
        self.inner.lock().unwrap().commit()
    }

    /// Roll back the current transaction (sends `ROLLBACK TRANSACTION`).
    fn rollback(&self) -> PyResult<()> {
        self.inner.lock().unwrap().rollback()
    }

    /// Enable or disable autocommit mode.
    ///
    /// When switching from manual → autocommit while a transaction is
    /// active, the pending transaction is committed first.
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

    /// Returns `True` if autocommit is on.
    fn get_autocommit(&self) -> bool {
        self.inner.lock().unwrap().autocommit
    }

    /// Execute SQL and return results as a PyArrow Table (zero-copy FFI).
    ///
    /// This is the primary query path. TDS row data is decoded directly
    /// into Arrow columnar arrays in Rust, then transferred to Python via
    /// Arrow C Data Interface — no intermediate Python objects.
    ///
    /// Returns:
    /// - `pyarrow.Table` for queries with results (SELECT, OUTPUT, etc.)
    /// - `int` (row count) for DML without results (INSERT, UPDATE, DELETE)
    fn execute_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        batch_size: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
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

    /// Execute SQL and return results as Python dicts (DB-API style).
    ///
    /// Slower than `execute_arrow` — creates Python objects per row.
    /// Used internally by the DB-API `fetchone()`/`fetchall()` path
    /// when Arrow is overkill.
    fn execute_rows(&self, py: Python<'_>, sql: &str) -> PyResult<Py<PyAny>> {
        let mut conn = self.inner.lock().unwrap();
        conn.begin_if_needed()?;
        let client = conn.get_client()?;
        drop(conn);

        let (columns, rows) = cursor::execute_to_rows(&client, sql)?;

        let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let py_rows: Vec<Py<PyAny>> = rows
            .iter()
            .map(|r| cursor::row_to_py_tuple(py, r))
            .collect::<PyResult<Vec<_>>>()?;

        let result = pyo3::types::PyDict::new(py);
        result.set_item("columns", col_names)?;
        result.set_item("rows", py_rows)?;
        Ok(result.into_any().unbind())
    }

    /// Execute DDL/DML without returning a result set.
    ///
    /// Returns the number of rows affected, or -1 if the statement
    /// produced a result set (shouldn't happen for DDL).
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

    /// Bulk-load a PyArrow Table into SQL Server.
    ///
    /// Modes: "create", "append", "replace", "create_append".
    /// Generates INSERT statements from Arrow arrays — not true TDS
    /// bulk insert (BCP), but convenient for moderate data sizes.
    fn ingest(&self, table_name: &str, table: &Bound<'_, PyAny>, mode: &str) -> PyResult<i64> {
        let mut conn = self.inner.lock().unwrap();
        conn.begin_if_needed()?;
        let client = conn.get_client()?;
        drop(conn);

        ingest::ingest_arrow_table(&client, table_name, table, mode)
    }
}

/// Register the native extension module as `pounce._native`.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeConnection>()?;
    Ok(())
}
