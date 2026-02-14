use arrow::array::{Array, ArrayBuilder, StructArray};
use arrow::datatypes::Field;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, to_ffi};
use arrow::record_batch::RecordBatch;
use futures_util::TryStreamExt;
use pyo3::prelude::*;
use tabby::{ResultItem, Row as TdsRow, SqlValue};

use crate::arrow_convert::{append_column_data, finish_builders, make_builder};
use crate::connection::SharedClient;
use crate::errors::to_pyerr;
use crate::runtime;
use crate::types::column_to_field;

/// Result of executing a query — either Arrow batches or rowcount for DML
pub enum ExecResult {
    /// Query with results: fields + batches of RecordBatch
    Query {
        #[allow(dead_code)]
        fields: Vec<Field>,
        batches: Vec<RecordBatch>,
    },
    /// DML with no results
    Dml { rowcount: i64 },
}

/// Execute SQL and stream results directly into Arrow RecordBatches.
/// No intermediate Python objects — TDS wire → Arrow columnar buffers.
#[allow(clippy::await_holding_lock)]
pub fn execute_to_arrow(
    client: &SharedClient,
    sql: &str,
    batch_size: usize,
) -> PyResult<ExecResult> {
    let client = client.clone();
    let sql = sql.to_string();

    Python::attach(|py| {
        py.detach(|| {
            runtime::block_on(async {
                let mut c = client.lock().unwrap();
                let empty_params: &[&dyn tabby::IntoSql] = &[];
                let mut stream = c.execute(sql, empty_params).await.map_err(to_pyerr)?;

                let mut fields: Option<Vec<Field>> = None;
                let mut builders: Option<Vec<Box<dyn ArrayBuilder>>> = None;
                let mut batches: Vec<RecordBatch> = Vec::new();
                let mut row_count_in_batch: usize = 0;

                while let Some(item) = stream.try_next().await.map_err(to_pyerr)? {
                    match item {
                        ResultItem::Metadata(meta) => {
                            // Flush any previous batch
                            if let (Some(blds), Some(flds)) = (&mut builders, &fields)
                                && row_count_in_batch > 0
                            {
                                let batch = finish_builders(blds, flds).map_err(|e| {
                                    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
                                })?;
                                batches.push(batch);
                            }
                            let flds: Vec<Field> =
                                meta.columns().iter().map(column_to_field).collect();
                            let blds: Vec<Box<dyn ArrayBuilder>> = flds
                                .iter()
                                .map(|f| make_builder(f.data_type(), batch_size))
                                .collect();
                            fields = Some(flds);
                            builders = Some(blds);
                            row_count_in_batch = 0;
                        }
                        ResultItem::Row(row) => {
                            if let Some(ref mut blds) = builders {
                                // Directly append TDS column data into Arrow builders
                                for (i, (_col, data)) in row.cells().enumerate() {
                                    append_column_data(&mut blds[i], data);
                                }
                                row_count_in_batch += 1;

                                if row_count_in_batch >= batch_size {
                                    let flds = fields.as_ref().unwrap();
                                    let batch = finish_builders(blds, flds).map_err(|e| {
                                        pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
                                    })?;
                                    batches.push(batch);
                                    // Re-create builders
                                    *blds = flds
                                        .iter()
                                        .map(|f| make_builder(f.data_type(), batch_size))
                                        .collect();
                                    row_count_in_batch = 0;
                                }
                            }
                        }
                        ResultItem::Message(_) => {}
                    }
                }

                // Flush final batch
                if let (Some(blds), Some(flds)) = (&mut builders, &fields)
                    && row_count_in_batch > 0
                {
                    let batch = finish_builders(blds, flds)
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    batches.push(batch);
                }

                drop(stream);
                drop(c);

                match fields {
                    Some(flds) => Ok(ExecResult::Query {
                        fields: flds,
                        batches,
                    }),
                    None => {
                        // DML — get rowcount
                        let mut c2 = client.lock().unwrap();
                        let mut s2 = c2
                            .execute("SELECT @@ROWCOUNT", empty_params)
                            .await
                            .map_err(to_pyerr)?;
                        let mut rc: i64 = 0;
                        while let Some(item) = s2.try_next().await.map_err(to_pyerr)? {
                            if let ResultItem::Row(row) = item
                                && let Some(v) = row.try_get::<i32, _>(0).map_err(to_pyerr)?
                            {
                                rc = v as i64;
                            }
                        }
                        drop(s2);
                        drop(c2);
                        Ok(ExecResult::Dml { rowcount: rc })
                    }
                }
            })
        })
    })
}

/// Execute SQL and collect all rows as Python tuples (for DB-API fetchall)
#[allow(clippy::await_holding_lock)]
pub fn execute_to_rows(
    client: &SharedClient,
    sql: &str,
) -> PyResult<(Vec<ColumnInfo>, Vec<TdsRow>)> {
    let client = client.clone();
    let sql = sql.to_string();

    Python::attach(|py| {
        py.detach(|| {
            runtime::block_on(async {
                let mut c = client.lock().unwrap();
                let empty_params: &[&dyn tabby::IntoSql] = &[];
                let mut stream = c.execute(sql, empty_params).await.map_err(to_pyerr)?;

                let mut columns: Vec<ColumnInfo> = Vec::new();
                let mut rows: Vec<TdsRow> = Vec::new();

                while let Some(item) = stream.try_next().await.map_err(to_pyerr)? {
                    match item {
                        ResultItem::Metadata(meta) => {
                            columns = meta
                                .columns()
                                .iter()
                                .map(|c| ColumnInfo {
                                    name: c.name().to_string(),
                                })
                                .collect();
                        }
                        ResultItem::Row(row) => rows.push(row),
                        ResultItem::Message(_) => {}
                    }
                }

                drop(stream);
                drop(c);
                Ok((columns, rows))
            })
        })
    })
}

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
}

/// Convert a RecordBatch to a PyArrow RecordBatch via Arrow C Data Interface (zero-copy)
pub fn record_batch_to_pyarrow(py: Python<'_>, batch: &RecordBatch) -> PyResult<Py<PyAny>> {
    let struct_array = StructArray::from(batch.clone());
    let data = struct_array.into_data();

    let (ffi_array, ffi_schema) =
        to_ffi(&data).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    // Allocate on heap and get raw pointers
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as usize;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as usize;

    let pa = py.import("pyarrow")?;
    let rb_cls = pa.getattr("RecordBatch")?;
    let result = rb_cls.call_method1("_import_from_c", (array_ptr, schema_ptr))?;

    // _import_from_c takes ownership of the data, but we still need to free the Box allocations
    // Actually, PyArrow's _import_from_c reads from the pointers but doesn't free them.
    // We need to reclaim the boxes.
    unsafe {
        let _ = Box::from_raw(array_ptr as *mut FFI_ArrowArray);
        let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
    }

    Ok(result.unbind())
}

/// Convert multiple RecordBatches to a PyArrow Table
pub fn batches_to_pyarrow_table(py: Python<'_>, batches: &[RecordBatch]) -> PyResult<Py<PyAny>> {
    let pa = py.import("pyarrow")?;

    if batches.is_empty() {
        let empty_schema = pa.call_method1("schema", (Vec::<(&str, &str)>::new(),))?;
        let result = pa
            .getattr("Table")?
            .call_method1("from_batches", (Vec::<Py<PyAny>>::new(), empty_schema))?;
        return Ok(result.unbind());
    }
    let py_batches: Vec<Py<PyAny>> = batches
        .iter()
        .map(|b| record_batch_to_pyarrow(py, b))
        .collect::<PyResult<Vec<_>>>()?;

    let table_cls = pa.getattr("Table")?;
    let result = table_cls.call_method1("from_batches", (py_batches,))?;
    Ok(result.unbind())
}

/// Convert a TDS Row + column info to Python objects for DB-API
pub fn row_to_py_tuple(py: Python<'_>, row: &TdsRow) -> PyResult<Py<PyAny>> {
    let vals: Vec<Py<PyAny>> = row
        .cells()
        .map(|(_col, data)| column_data_to_py(py, data))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(pyo3::types::PyTuple::new(py, vals)?.into_any().unbind())
}

fn column_data_to_py(py: Python<'_>, data: &SqlValue<'static>) -> PyResult<Py<PyAny>> {
    use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
    match data {
        SqlValue::Bit(Some(v)) => Ok(pyo3::types::PyBool::new(py, *v)
            .to_owned()
            .into_any()
            .unbind()),
        SqlValue::Bit(None) => Ok(py.None()),
        SqlValue::U8(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SqlValue::U8(None) => Ok(py.None()),
        SqlValue::I16(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SqlValue::I16(None) => Ok(py.None()),
        SqlValue::I32(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SqlValue::I32(None) => Ok(py.None()),
        SqlValue::I64(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SqlValue::I64(None) => Ok(py.None()),
        SqlValue::F32(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SqlValue::F32(None) => Ok(py.None()),
        SqlValue::F64(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SqlValue::F64(None) => Ok(py.None()),
        SqlValue::String(Some(v)) => Ok(v.as_ref().into_pyobject(py)?.into_any().unbind()),
        SqlValue::String(None) => Ok(py.None()),
        SqlValue::Guid(Some(v)) => Ok(v.to_string().into_pyobject(py)?.into_any().unbind()),
        SqlValue::Guid(None) => Ok(py.None()),
        SqlValue::Binary(Some(v)) => Ok(v.as_ref().into_pyobject(py)?.into_any().unbind()),
        SqlValue::Binary(None) => Ok(py.None()),
        SqlValue::Numeric(Some(v)) => {
            let dec_mod = py.import("decimal")?;
            let dec_cls = dec_mod.getattr("Decimal")?;
            Ok(dec_cls
                .call1((v.to_string(),))?
                .into_pyobject(py)?
                .into_any()
                .unbind())
        }
        SqlValue::Numeric(None) => Ok(py.None()),
        SqlValue::Xml(Some(x)) => Ok(format!("{}", x).into_pyobject(py)?.into_any().unbind()),
        SqlValue::Xml(None) => Ok(py.None()),
        SqlValue::DateTime(Some(dt)) => {
            let datetime_mod = py.import("datetime")?;
            let base = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
            let date = base + chrono::Duration::days(dt.days() as i64);
            let ticks = dt.seconds_fragments() as i64;
            let total_ms = ticks * 1000 / 300;
            let secs = (total_ms / 1000) as u32;
            let micros = ((total_ms % 1000) * 1000) as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, 0).unwrap_or_default();
            let ndt = NaiveDateTime::new(date, time);
            let py_dt = datetime_mod.getattr("datetime")?.call1((
                ndt.year(),
                ndt.month(),
                ndt.day(),
                ndt.hour(),
                ndt.minute(),
                ndt.second(),
                micros,
            ))?;
            Ok(py_dt.into_pyobject(py)?.into_any().unbind())
        }
        SqlValue::DateTime(None) => Ok(py.None()),
        SqlValue::SmallDateTime(Some(dt)) => {
            let datetime_mod = py.import("datetime")?;
            let base = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
            let date = base + chrono::Duration::days(dt.days() as i64);
            let mins = dt.seconds_fragments() as u32;
            let time =
                NaiveTime::from_num_seconds_from_midnight_opt(mins * 60, 0).unwrap_or_default();
            let ndt = NaiveDateTime::new(date, time);
            let py_dt = datetime_mod.getattr("datetime")?.call1((
                ndt.year(),
                ndt.month(),
                ndt.day(),
                ndt.hour(),
                ndt.minute(),
                ndt.second(),
                0u32,
            ))?;
            Ok(py_dt.into_pyobject(py)?.into_any().unbind())
        }
        SqlValue::SmallDateTime(None) => Ok(py.None()),
        SqlValue::DateTime2(Some(dt)) => {
            let datetime_mod = py.import("datetime")?;
            let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let date = base + chrono::Duration::days(dt.date().days() as i64);
            let t = dt.time();
            let nanos = t.increments() * 10u64.pow(9 - t.scale() as u32);
            let secs = (nanos / 1_000_000_000) as u32;
            let micros = ((nanos % 1_000_000_000) / 1000) as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, 0).unwrap_or_default();
            let ndt = NaiveDateTime::new(date, time);
            let py_dt = datetime_mod.getattr("datetime")?.call1((
                ndt.year(),
                ndt.month(),
                ndt.day(),
                ndt.hour(),
                ndt.minute(),
                ndt.second(),
                micros,
            ))?;
            Ok(py_dt.into_pyobject(py)?.into_any().unbind())
        }
        SqlValue::DateTime2(None) => Ok(py.None()),
        SqlValue::Date(Some(d)) => {
            let datetime_mod = py.import("datetime")?;
            let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let date = base + chrono::Duration::days(d.days() as i64);
            let py_date =
                datetime_mod
                    .getattr("date")?
                    .call1((date.year(), date.month(), date.day()))?;
            Ok(py_date.into_pyobject(py)?.into_any().unbind())
        }
        SqlValue::Date(None) => Ok(py.None()),
        SqlValue::Time(Some(t)) => {
            let datetime_mod = py.import("datetime")?;
            let nanos = t.increments() * 10u64.pow(9 - t.scale() as u32);
            let secs = (nanos / 1_000_000_000) as u32;
            let micros = ((nanos % 1_000_000_000) / 1000) as u32;
            let hour = secs / 3600;
            let minute = (secs % 3600) / 60;
            let second = secs % 60;
            let py_time = datetime_mod
                .getattr("time")?
                .call1((hour, minute, second, micros))?;
            Ok(py_time.into_pyobject(py)?.into_any().unbind())
        }
        SqlValue::Time(None) => Ok(py.None()),
        SqlValue::DateTimeOffset(Some(dto)) => {
            let datetime_mod = py.import("datetime")?;
            let d = dto.datetime2().date();
            let t = dto.datetime2().time();
            let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let date = base + chrono::Duration::days(d.days() as i64);
            let nanos = t.increments() * 10u64.pow(9 - t.scale() as u32);
            let secs = (nanos / 1_000_000_000) as u32;
            let micros = ((nanos % 1_000_000_000) / 1000) as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, 0).unwrap_or_default();
            let ndt = NaiveDateTime::new(date, time);
            let offset_mins = dto.offset() as i32;
            let local_ndt = ndt + chrono::Duration::minutes(offset_mins as i64);
            let td = datetime_mod
                .getattr("timedelta")?
                .call1((0, offset_mins * 60))?;
            let tz = datetime_mod.getattr("timezone")?.call1((td,))?;
            let py_dt = datetime_mod.getattr("datetime")?.call1((
                local_ndt.year(),
                local_ndt.month(),
                local_ndt.day(),
                local_ndt.hour(),
                local_ndt.minute(),
                local_ndt.second(),
                micros,
                tz,
            ))?;
            Ok(py_dt.into_pyobject(py)?.into_any().unbind())
        }
        SqlValue::DateTimeOffset(None) => Ok(py.None()),
    }
}
