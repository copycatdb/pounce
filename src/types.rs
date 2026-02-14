use arrow::datatypes::{DataType, Field, TimeUnit};
use tabby::{Column, DataType as TdsDataType, FixedLenType, VarLenType};

/// Map a TDS Column to an Arrow DataType + nullable
pub fn column_to_arrow_type(col: &Column) -> (DataType, bool) {
    let nullable = col.nullable().unwrap_or(true);
    let dt = match col.type_info() {
        Some(ti) => type_info_to_arrow(ti),
        None => DataType::Utf8,
    };
    (dt, nullable)
}

fn type_info_to_arrow(ti: &TdsDataType) -> DataType {
    match ti {
        TdsDataType::FixedLen(ft) => match ft {
            FixedLenType::Null => DataType::Null,
            FixedLenType::Bit => DataType::Boolean,
            FixedLenType::Int1 => DataType::UInt8,
            FixedLenType::Int2 => DataType::Int16,
            FixedLenType::Int4 => DataType::Int32,
            FixedLenType::Int8 => DataType::Int64,
            FixedLenType::Float4 => DataType::Float32,
            FixedLenType::Float8 => DataType::Float64,
            FixedLenType::Datetime | FixedLenType::Datetime4 => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            FixedLenType::Money | FixedLenType::Money4 => DataType::Decimal128(19, 4),
        },
        TdsDataType::VarLenSized(ctx) => match ctx.r#type() {
            VarLenType::Bitn => DataType::Boolean,
            VarLenType::Intn => match ctx.len() {
                1 => DataType::UInt8,
                2 => DataType::Int16,
                4 => DataType::Int32,
                _ => DataType::Int64,
            },
            VarLenType::Floatn => match ctx.len() {
                4 => DataType::Float32,
                _ => DataType::Float64,
            },
            VarLenType::Guid => DataType::Utf8, // UUID as string
            VarLenType::NVarchar
            | VarLenType::NChar
            | VarLenType::BigVarChar
            | VarLenType::BigChar
            | VarLenType::Text
            | VarLenType::NText => DataType::Utf8,
            VarLenType::BigVarBin | VarLenType::BigBinary | VarLenType::Image => DataType::Binary,
            VarLenType::Datetimen => DataType::Timestamp(TimeUnit::Microsecond, None),
            VarLenType::Daten => DataType::Date32,
            VarLenType::Timen => DataType::Time64(TimeUnit::Nanosecond),
            VarLenType::Datetime2 => DataType::Timestamp(TimeUnit::Microsecond, None),
            VarLenType::DatetimeOffsetn => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            VarLenType::Money => DataType::Decimal128(19, 4),
            VarLenType::Xml => DataType::Utf8,
            _ => DataType::Utf8,
        },
        TdsDataType::VarLenSizedPrecision {
            precision, scale, ..
        } => DataType::Decimal128(*precision, *scale as i8),
        TdsDataType::Xml { .. } => DataType::Utf8,
    }
}

/// Build an Arrow Field from a TDS Column
pub fn column_to_field(col: &Column) -> Field {
    let (dt, nullable) = column_to_arrow_type(col);
    Field::new(col.name(), dt, nullable)
}
