//! Arrow table ingestion: PyArrow Table → SQL Server.
//!
//! Reads column data from a PyArrow Table (via PyO3), generates
//! `INSERT` statements, and sends them through tabby. Supports
//! create/append/replace/create_append modes.
//!
//! Note: this is INSERT-based, not TDS bulk insert (BCP). Fine for
//! moderate data sizes; a true bulk path is on the roadmap.

use pyo3::prelude::*;

use crate::connection::SharedClient;
use crate::errors::to_pyerr;
use crate::runtime;

/// Ingest a PyArrow Table into a SQL Server table.
///
/// Modes:
/// - `"create"` — CREATE TABLE + INSERT (fails if table exists)
/// - `"append"` — INSERT into existing table
/// - `"replace"` — DROP + CREATE + INSERT
/// - `"create_append"` — CREATE if not exists, then INSERT
#[allow(clippy::await_holding_lock)]
pub fn ingest_arrow_table(
    client: &SharedClient,
    table_name: &str,
    py_table: &Bound<'_, PyAny>,
    mode: &str, // "create", "append", "replace", "create_append"
) -> PyResult<i64> {
    let py = py_table.py();

    // Extract schema and batches from PyArrow Table
    let schema_obj = py_table.getattr("schema")?;
    let num_columns: usize = schema_obj.getattr("__len__")?.call0()?.extract()?;
    let num_rows: usize = py_table.getattr("num_rows")?.extract()?;

    // Get column names and types
    let mut col_names: Vec<String> = Vec::with_capacity(num_columns);
    let mut col_sql_types: Vec<String> = Vec::with_capacity(num_columns);

    for i in 0..num_columns {
        let field = schema_obj.call_method1("field", (i,))?;
        let name: String = field.getattr("name")?.extract()?;
        let dtype = field.getattr("type")?;
        let sql_type = arrow_type_to_sql(&dtype)?;
        col_names.push(name);
        col_sql_types.push(sql_type);
    }

    // Handle table creation/replacement
    let client_ref = client.clone();
    match mode {
        "replace" => {
            let drop_sql = format!(
                "IF OBJECT_ID(N'{}', 'U') IS NOT NULL DROP TABLE {}",
                table_name, table_name
            );
            exec_simple_internal(&client_ref, &drop_sql)?;
            let create_sql = build_create_table_sql(table_name, &col_names, &col_sql_types);
            exec_simple_internal(&client_ref, &create_sql)?;
        }
        "create" => {
            let create_sql = build_create_table_sql(table_name, &col_names, &col_sql_types);
            exec_simple_internal(&client_ref, &create_sql)?;
        }
        "create_append" => {
            let create_sql = format!(
                "IF OBJECT_ID(N'{}', 'U') IS NULL BEGIN {} END",
                table_name,
                build_create_table_sql(table_name, &col_names, &col_sql_types)
            );
            exec_simple_internal(&client_ref, &create_sql)?;
        }
        "append" => {} // table must exist
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown ingest mode: {}",
                mode
            )));
        }
    }

    if num_rows == 0 {
        return Ok(0);
    }

    // Build INSERT statements in batches
    // For simplicity, use parameterized INSERT via execute_raw with literal values
    // A proper implementation would use TDS BulkLoad, but that requires more complex setup
    let batches_obj = py_table.call_method0("to_batches")?;
    let batches_list: Vec<Bound<'_, PyAny>> =
        batches_obj.try_iter()?.collect::<PyResult<Vec<_>>>()?;

    let mut total_rows: i64 = 0;

    for batch in &batches_list {
        let batch_rows: usize = batch.getattr("num_rows")?.extract()?;
        let columns: Vec<Bound<'_, PyAny>> = (0..num_columns)
            .map(|i| batch.call_method1("column", (i,)))
            .collect::<PyResult<Vec<_>>>()?;

        // Build INSERT in chunks of 1000 rows (SQL Server limit)
        let chunk_size = 1000;
        for chunk_start in (0..batch_rows).step_by(chunk_size) {
            let chunk_end = std::cmp::min(chunk_start + chunk_size, batch_rows);
            let mut sql = format!(
                "INSERT INTO {} ({}) VALUES ",
                table_name,
                col_names
                    .iter()
                    .map(|n| format!("[{}]", n))
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            for row_idx in chunk_start..chunk_end {
                if row_idx > chunk_start {
                    sql.push_str(", ");
                }
                sql.push('(');
                for (col_idx, col_array) in columns.iter().enumerate() {
                    if col_idx > 0 {
                        sql.push_str(", ");
                    }
                    let is_null: bool = col_array
                        .get_item(row_idx)?
                        .call_method0("as_py")?
                        .is_none();
                    if is_null {
                        sql.push_str("NULL");
                    } else {
                        let val = col_array.get_item(row_idx)?;
                        let py_val = val.call_method0("as_py")?;
                        let literal = py_value_to_sql_literal(py, &py_val)?;
                        sql.push_str(&literal);
                    }
                }
                sql.push(')');
            }

            exec_simple_internal(&client_ref, &sql)?;
            total_rows += (chunk_end - chunk_start) as i64;
        }
    }

    Ok(total_rows)
}

#[allow(clippy::await_holding_lock)]
fn exec_simple_internal(client: &SharedClient, sql: &str) -> PyResult<()> {
    let client = client.clone();
    let sql = sql.to_string();
    Python::attach(|py| {
        py.detach(|| {
            runtime::block_on(async {
                let mut c = client.lock().unwrap();
                c.execute_raw(sql)
                    .await
                    .map_err(to_pyerr)?
                    .into_results()
                    .await
                    .map_err(to_pyerr)?;
                Ok(())
            })
        })
    })
}

fn build_create_table_sql(table_name: &str, col_names: &[String], col_types: &[String]) -> String {
    let cols: Vec<String> = col_names
        .iter()
        .zip(col_types.iter())
        .map(|(n, t)| format!("[{}] {}", n, t))
        .collect();
    format!("CREATE TABLE {} ({})", table_name, cols.join(", "))
}

fn arrow_type_to_sql(dtype: &Bound<'_, PyAny>) -> PyResult<String> {
    let type_str: String = dtype.str()?.extract()?;
    let sql = match type_str.as_str() {
        "bool" => "BIT".to_string(),
        "uint8" | "int8" => "TINYINT".to_string(),
        "int16" => "SMALLINT".to_string(),
        "int32" | "uint16" => "INT".to_string(),
        "int64" | "uint32" | "uint64" => "BIGINT".to_string(),
        "float" | "float16" | "float32" => "REAL".to_string(),
        "double" | "float64" => "FLOAT".to_string(),
        "date32[day]" | "date32" => "DATE".to_string(),
        "string" | "utf8" | "large_string" | "large_utf8" => "NVARCHAR(MAX)".to_string(),
        "binary" | "large_binary" => "VARBINARY(MAX)".to_string(),
        s if s.starts_with("timestamp") => "DATETIME2".to_string(),
        s if s.starts_with("time64") || s.starts_with("time32") => "TIME".to_string(),
        s if s.starts_with("decimal128") => {
            let inner = s.trim_start_matches("decimal128");
            format!("DECIMAL{}", inner)
        }
        _ => "NVARCHAR(MAX)".to_string(),
    };
    Ok(sql)
}

fn py_value_to_sql_literal(_py: Python<'_>, val: &Bound<'_, PyAny>) -> PyResult<String> {
    if val.is_none() {
        return Ok("NULL".to_string());
    }
    if let Ok(v) = val.extract::<bool>() {
        return Ok(if v { "1" } else { "0" }.to_string());
    }
    if let Ok(v) = val.extract::<i64>() {
        return Ok(v.to_string());
    }
    if let Ok(v) = val.extract::<f64>() {
        return Ok(v.to_string());
    }
    if let Ok(v) = val.extract::<String>() {
        return Ok(format!("N'{}'", v.replace('\'', "''")));
    }
    if let Ok(v) = val.extract::<Vec<u8>>() {
        let hex: String = v.iter().map(|b| format!("{:02X}", b)).collect();
        return Ok(format!("0x{}", hex));
    }
    // datetime
    let s = val.str()?.to_string();
    Ok(format!("N'{}'", s.replace('\'', "''")))
}
