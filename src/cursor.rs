use pyo3::prelude::*;
use pyo3::ffi as pyffi;
use tiberius::{Row as TibRow, ColumnData, QueryItem};
use arrow::array::{Array, ArrayBuilder, StructArray};
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, to_ffi};
use std::sync::Arc;
use futures_util::TryStreamExt;

use crate::connection::SharedClient;
use crate::runtime;
use crate::errors::to_pyerr;
use crate::types::column_to_field;
use crate::arrow_convert::{make_builder, append_column_data, finish_builders};

const DEFAULT_BATCH_SIZE: usize = 65536;

/// Result of executing a query — either Arrow batches or rowcount for DML
pub enum ExecResult {
    /// Query with results: fields + batches of RecordBatch
    Query {
        fields: Vec<Field>,
        batches: Vec<RecordBatch>,
    },
    /// DML with no results
    Dml { rowcount: i64 },
}

/// Execute SQL and stream results directly into Arrow RecordBatches.
/// No intermediate Python objects — TDS wire → Arrow columnar buffers.
pub fn execute_to_arrow(
    client: &SharedClient,
    sql: &str,
    batch_size: usize,
) -> PyResult<ExecResult> {
    let client = client.clone();
    let sql = sql.to_string();

    Python::with_gil(|py| {
        py.allow_threads(|| {
            runtime::block_on(async {
                let mut c = client.lock().unwrap();
                let empty_params: &[&dyn tiberius::ToSql] = &[];
                let mut stream = c.query(sql, empty_params).await.map_err(to_pyerr)?;

                let mut fields: Option<Vec<Field>> = None;
                let mut builders: Option<Vec<Box<dyn ArrayBuilder>>> = None;
                let mut batches: Vec<RecordBatch> = Vec::new();
                let mut row_count_in_batch: usize = 0;

                while let Some(item) = stream.try_next().await.map_err(to_pyerr)? {
                    match item {
                        QueryItem::Metadata(meta) => {
                            // Flush any previous batch
                            if let (Some(ref mut blds), Some(ref flds)) = (&mut builders, &fields) {
                                if row_count_in_batch > 0 {
                                    let batch = finish_builders(blds, flds)
                                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                                    batches.push(batch);
                                }
                            }
                            let flds: Vec<Field> = meta.columns().iter().map(column_to_field).collect();
                            let blds: Vec<Box<dyn ArrayBuilder>> = flds.iter()
                                .map(|f| make_builder(f.data_type(), batch_size))
                                .collect();
                            fields = Some(flds);
                            builders = Some(blds);
                            row_count_in_batch = 0;
                        }
                        QueryItem::Row(row) => {
                            if let Some(ref mut blds) = builders {
                                // Directly append TDS column data into Arrow builders
                                for (i, (_col, data)) in row.cells().enumerate() {
                                    append_column_data(&mut blds[i], data);
                                }
                                row_count_in_batch += 1;

                                if row_count_in_batch >= batch_size {
                                    let flds = fields.as_ref().unwrap();
                                    let batch = finish_builders(blds, flds)
                                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                                    batches.push(batch);
                                    // Re-create builders
                                    *blds = flds.iter()
                                        .map(|f| make_builder(f.data_type(), batch_size))
                                        .collect();
                                    row_count_in_batch = 0;
                                }
                            }
                        }
                        QueryItem::Message(_) => {}
                    }
                }

                // Flush final batch
                if let (Some(ref mut blds), Some(ref flds)) = (&mut builders, &fields) {
                    if row_count_in_batch > 0 {
                        let batch = finish_builders(blds, flds)
                            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                        batches.push(batch);
                    }
                }

                drop(stream);
                drop(c);

                match fields {
                    Some(flds) => Ok(ExecResult::Query { fields: flds, batches }),
                    None => {
                        // DML — get rowcount
                        let mut c2 = client.lock().unwrap();
                        let mut s2 = c2.query("SELECT @@ROWCOUNT", empty_params).await.map_err(to_pyerr)?;
                        let mut rc: i64 = 0;
                        while let Some(item) = s2.try_next().await.map_err(to_pyerr)? {
                            if let QueryItem::Row(row) = item {
                                if let Some(v) = row.try_get::<i32, _>(0).map_err(to_pyerr)? {
                                    rc = v as i64;
                                }
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
pub fn execute_to_rows(
    client: &SharedClient,
    sql: &str,
) -> PyResult<(Vec<ColumnInfo>, Vec<TibRow>)> {
    let client = client.clone();
    let sql = sql.to_string();

    Python::with_gil(|py| {
        py.allow_threads(|| {
            runtime::block_on(async {
                let mut c = client.lock().unwrap();
                let empty_params: &[&dyn tiberius::ToSql] = &[];
                let mut stream = c.query(sql, empty_params).await.map_err(to_pyerr)?;

                let mut columns: Vec<ColumnInfo> = Vec::new();
                let mut rows: Vec<TibRow> = Vec::new();

                while let Some(item) = stream.try_next().await.map_err(to_pyerr)? {
                    match item {
                        QueryItem::Metadata(meta) => {
                            columns = meta.columns().iter().map(|c| ColumnInfo {
                                name: c.name().to_string(),
                            }).collect();
                        }
                        QueryItem::Row(row) => rows.push(row),
                        QueryItem::Message(_) => {}
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
pub fn record_batch_to_pyarrow(py: Python<'_>, batch: &RecordBatch) -> PyResult<PyObject> {
    let struct_array = StructArray::from(batch.clone());
    let data = struct_array.into_data();

    let (ffi_array, ffi_schema) = to_ffi(&data)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

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
pub fn batches_to_pyarrow_table(py: Python<'_>, batches: &[RecordBatch]) -> PyResult<PyObject> {
    let pa = py.import("pyarrow")?;

    if batches.is_empty() {
        let empty_schema = pa.call_method1("schema", (Vec::<(&str, &str)>::new(),))?;
        let result = pa.getattr("Table")?.call_method1("from_batches", (Vec::<PyObject>::new(), empty_schema))?;
        return Ok(result.unbind());
    }
    let py_batches: Vec<PyObject> = batches.iter()
        .map(|b| record_batch_to_pyarrow(py, b))
        .collect::<PyResult<Vec<_>>>()?;

    let table_cls = pa.getattr("Table")?;
    let result = table_cls.call_method1("from_batches", (py_batches,))?;
    Ok(result.unbind())
}

/// Convert a tiberius Row + column info to Python objects for DB-API
pub fn row_to_py_tuple(py: Python<'_>, row: &TibRow) -> PyResult<PyObject> {
    let vals: Vec<PyObject> = row.cells().map(|(_col, data)| {
        column_data_to_py(py, data)
    }).collect::<PyResult<Vec<_>>>()?;
    Ok(pyo3::types::PyTuple::new(py, vals)?.into_any().unbind())
}

fn column_data_to_py(py: Python<'_>, data: &ColumnData<'static>) -> PyResult<PyObject> {
    use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Datelike, Timelike};
    match data {
        ColumnData::Bit(Some(v)) => Ok(pyo3::types::PyBool::new(py, *v).to_owned().into_any().unbind()),
        ColumnData::Bit(None) => Ok(py.None()),
        ColumnData::U8(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        ColumnData::U8(None) => Ok(py.None()),
        ColumnData::I16(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        ColumnData::I16(None) => Ok(py.None()),
        ColumnData::I32(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        ColumnData::I32(None) => Ok(py.None()),
        ColumnData::I64(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        ColumnData::I64(None) => Ok(py.None()),
        ColumnData::F32(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        ColumnData::F32(None) => Ok(py.None()),
        ColumnData::F64(Some(v)) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        ColumnData::F64(None) => Ok(py.None()),
        ColumnData::String(Some(ref v)) => Ok(v.as_ref().into_pyobject(py)?.into_any().unbind()),
        ColumnData::String(None) => Ok(py.None()),
        ColumnData::Guid(Some(ref v)) => Ok(v.to_string().into_pyobject(py)?.into_any().unbind()),
        ColumnData::Guid(None) => Ok(py.None()),
        ColumnData::Binary(Some(ref v)) => Ok(v.as_ref().into_pyobject(py)?.into_any().unbind()),
        ColumnData::Binary(None) => Ok(py.None()),
        ColumnData::Numeric(Some(ref v)) => {
            let dec_mod = py.import("decimal")?;
            let dec_cls = dec_mod.getattr("Decimal")?;
            Ok(dec_cls.call1((v.to_string(),))?.into_pyobject(py)?.into_any().unbind())
        }
        ColumnData::Numeric(None) => Ok(py.None()),
        ColumnData::Xml(Some(ref x)) => Ok(format!("{}", x).into_pyobject(py)?.into_any().unbind()),
        ColumnData::Xml(None) => Ok(py.None()),
        ColumnData::DateTime(Some(ref dt)) => {
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
                ndt.year(), ndt.month(), ndt.day(),
                ndt.hour(), ndt.minute(), ndt.second(), micros
            ))?;
            Ok(py_dt.into_pyobject(py)?.into_any().unbind())
        }
        ColumnData::DateTime(None) => Ok(py.None()),
        ColumnData::SmallDateTime(Some(ref dt)) => {
            let datetime_mod = py.import("datetime")?;
            let base = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
            let date = base + chrono::Duration::days(dt.days() as i64);
            let mins = dt.seconds_fragments() as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(mins * 60, 0).unwrap_or_default();
            let ndt = NaiveDateTime::new(date, time);
            let py_dt = datetime_mod.getattr("datetime")?.call1((
                ndt.year(), ndt.month(), ndt.day(),
                ndt.hour(), ndt.minute(), ndt.second(), 0u32
            ))?;
            Ok(py_dt.into_pyobject(py)?.into_any().unbind())
        }
        ColumnData::SmallDateTime(None) => Ok(py.None()),
        ColumnData::DateTime2(Some(ref dt)) => {
            let datetime_mod = py.import("datetime")?;
            let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let date = base + chrono::Duration::days(dt.date().days() as i64);
            let t = dt.time();
            let nanos = t.increments() as u64 * 10u64.pow(9 - t.scale() as u32);
            let secs = (nanos / 1_000_000_000) as u32;
            let micros = ((nanos % 1_000_000_000) / 1000) as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, 0).unwrap_or_default();
            let ndt = NaiveDateTime::new(date, time);
            let py_dt = datetime_mod.getattr("datetime")?.call1((
                ndt.year(), ndt.month(), ndt.day(),
                ndt.hour(), ndt.minute(), ndt.second(), micros
            ))?;
            Ok(py_dt.into_pyobject(py)?.into_any().unbind())
        }
        ColumnData::DateTime2(None) => Ok(py.None()),
        ColumnData::Date(Some(ref d)) => {
            let datetime_mod = py.import("datetime")?;
            let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let date = base + chrono::Duration::days(d.days() as i64);
            let py_date = datetime_mod.getattr("date")?.call1((date.year(), date.month(), date.day()))?;
            Ok(py_date.into_pyobject(py)?.into_any().unbind())
        }
        ColumnData::Date(None) => Ok(py.None()),
        ColumnData::Time(Some(ref t)) => {
            let datetime_mod = py.import("datetime")?;
            let nanos = t.increments() as u64 * 10u64.pow(9 - t.scale() as u32);
            let secs = (nanos / 1_000_000_000) as u32;
            let micros = ((nanos % 1_000_000_000) / 1000) as u32;
            let hour = secs / 3600;
            let minute = (secs % 3600) / 60;
            let second = secs % 60;
            let py_time = datetime_mod.getattr("time")?.call1((hour, minute, second, micros))?;
            Ok(py_time.into_pyobject(py)?.into_any().unbind())
        }
        ColumnData::Time(None) => Ok(py.None()),
        ColumnData::DateTimeOffset(Some(ref dto)) => {
            let datetime_mod = py.import("datetime")?;
            let d = dto.datetime2().date();
            let t = dto.datetime2().time();
            let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let date = base + chrono::Duration::days(d.days() as i64);
            let nanos = t.increments() as u64 * 10u64.pow(9 - t.scale() as u32);
            let secs = (nanos / 1_000_000_000) as u32;
            let micros = ((nanos % 1_000_000_000) / 1000) as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, 0).unwrap_or_default();
            let ndt = NaiveDateTime::new(date, time);
            let offset_mins = dto.offset() as i32;
            let local_ndt = ndt + chrono::Duration::minutes(offset_mins as i64);
            let td = datetime_mod.getattr("timedelta")?.call1((0, offset_mins * 60))?;
            let tz = datetime_mod.getattr("timezone")?.call1((td,))?;
            let py_dt = datetime_mod.getattr("datetime")?.call1((
                local_ndt.year(), local_ndt.month(), local_ndt.day(),
                local_ndt.hour(), local_ndt.minute(), local_ndt.second(), micros, tz
            ))?;
            Ok(py_dt.into_pyobject(py)?.into_any().unbind())
        }
        ColumnData::DateTimeOffset(None) => Ok(py.None()),
    }
}
