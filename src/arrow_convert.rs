//! Arrow type conversion: TDS wire data → Arrow columnar arrays.
//!
//! Each `SqlValue` variant from tabby maps to an Arrow `ArrayBuilder`.
//! This module handles the per-cell append logic — the hot path that
//! determines how fast we can stream TDS rows into Arrow batches.

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use claw::SqlValue;

/// Append a single TDS value to the matching Arrow array builder.
///
/// The builder type must match the `DataType` returned by
/// [`types::column_to_arrow_type`]. Panics (via `unwrap`) if there's
/// a type mismatch — that's a bug in our type mapping, not user error.
pub fn append_column_data(builder: &mut Box<dyn ArrayBuilder>, data: &SqlValue<'_>) {
    match data {
        // Boolean
        SqlValue::Bit(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        // Integers
        SqlValue::U8(v) => {
            let b = builder.as_any_mut().downcast_mut::<UInt8Builder>().unwrap();
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        SqlValue::I16(v) => {
            let b = builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap();
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        SqlValue::I32(v) => {
            let b = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        SqlValue::I64(v) => {
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        // Floats
        SqlValue::F32(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap();
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        SqlValue::F64(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        // Strings
        SqlValue::String(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            match v {
                Some(val) => b.append_value(val.as_ref()),
                None => b.append_null(),
            }
        }
        // GUID as string
        SqlValue::Guid(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            match v {
                Some(val) => b.append_value(val.to_string()),
                None => b.append_null(),
            }
        }
        // Binary
        SqlValue::Binary(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap();
            match v {
                Some(val) => b.append_value(val.as_ref()),
                None => b.append_null(),
            }
        }
        // Numeric → Decimal128
        SqlValue::Numeric(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .unwrap();
            match v {
                Some(val) => {
                    b.append_value(val.value());
                }
                None => b.append_null(),
            }
        }
        // XML as string
        SqlValue::Xml(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            match v {
                Some(val) => b.append_value(format!("{}", val)),
                None => b.append_null(),
            }
        }
        // DateTime → Timestamp microseconds
        SqlValue::DateTime(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            match v {
                Some(dt) => {
                    let base = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
                    let date = base + chrono::Duration::days(dt.days() as i64);
                    let ticks = dt.seconds_fragments() as i64;
                    let total_ms = ticks * 1000 / 300;
                    let secs = total_ms / 1000;
                    let micros = (total_ms % 1000) * 1000;
                    let time =
                        chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, 0)
                            .unwrap_or_default();
                    let ndt = chrono::NaiveDateTime::new(date, time);
                    let timestamp_micros = ndt.and_utc().timestamp_micros() + micros;
                    b.append_value(timestamp_micros);
                }
                None => b.append_null(),
            }
        }
        SqlValue::SmallDateTime(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            match v {
                Some(dt) => {
                    let base = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
                    let date = base + chrono::Duration::days(dt.days() as i64);
                    let mins = dt.seconds_fragments() as u32;
                    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(mins * 60, 0)
                        .unwrap_or_default();
                    let ndt = chrono::NaiveDateTime::new(date, time);
                    b.append_value(ndt.and_utc().timestamp_micros());
                }
                None => b.append_null(),
            }
        }
        SqlValue::DateTime2(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            match v {
                Some(dt) => {
                    let micros = datetime2_to_micros(dt);
                    b.append_value(micros);
                }
                None => b.append_null(),
            }
        }
        SqlValue::Date(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap();
            match v {
                Some(d) => {
                    let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
                    let date = base + chrono::Duration::days(d.days() as i64);
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let days = (date - epoch).num_days() as i32;
                    b.append_value(days);
                }
                None => b.append_null(),
            }
        }
        SqlValue::Time(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Time64NanosecondBuilder>()
                .unwrap();
            match v {
                Some(t) => {
                    let nanos = t.increments() as i64 * 10i64.pow(9 - t.scale() as u32);
                    b.append_value(nanos);
                }
                None => b.append_null(),
            }
        }
        SqlValue::DateTimeOffset(v) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            match v {
                Some(dto) => {
                    let micros = datetime2_to_micros(&dto.datetime2());
                    // dto stores datetime2 in local time; subtract offset to get UTC
                    let offset_micros = dto.offset() as i64 * 60 * 1_000_000;
                    b.append_value(micros - offset_micros);
                }
                None => b.append_null(),
            }
        }
    }
}

fn datetime2_to_micros(dt: &claw::temporal::DateTime2) -> i64 {
    let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
    let date = base + chrono::Duration::days(dt.date().days() as i64);
    let t = dt.time();
    let nanos = t.increments() * 10u64.pow(9 - t.scale() as u32);
    let secs = (nanos / 1_000_000_000) as u32;
    let remaining_micros = ((nanos % 1_000_000_000) / 1000) as i64;
    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, 0).unwrap_or_default();
    let ndt = chrono::NaiveDateTime::new(date, time);
    ndt.and_utc().timestamp_micros() + remaining_micros
}

/// Create an Arrow ArrayBuilder for a given DataType
pub fn make_builder(dt: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Decimal128(p, s) => Box::new(
            Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(*p, *s)
                .unwrap(),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let b =
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone());
            Box::new(b)
        }
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Time64(TimeUnit::Nanosecond) => {
            Box::new(Time64NanosecondBuilder::with_capacity(capacity))
        }
        DataType::Null => Box::new(NullBuilder::new()),
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)), // fallback
    }
}

/// Finish builders into a RecordBatch
pub fn finish_builders(
    builders: &mut [Box<dyn ArrayBuilder>],
    fields: &[Field],
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
    let schema = Schema::new(fields.to_vec());
    RecordBatch::try_new(std::sync::Arc::new(schema), arrays)
}
