//! Arrow-native RowWriter: decodes TDS values directly into Arrow ArrayBuilders.
//!
//! This bypasses the `SqlValue` enum entirely — the RowWriter trait methods
//! map 1:1 to Arrow builder append calls.

use arrow::array::*;
use arrow::datatypes::Field;
use claw::row_writer::RowWriter;

use crate::types::column_to_field;

/// Holds Arrow `ArrayBuilder`s and appends TDS values directly via `RowWriter`.
pub struct ArrowRowWriter {
    builders: Vec<Box<dyn ArrayBuilder>>,
    fields: Vec<Field>,
    row_count: usize,
}

impl ArrowRowWriter {
    /// Create a new writer from TDS column metadata.
    pub fn from_columns(columns: &[claw::Column], capacity: usize) -> Self {
        let fields: Vec<Field> = columns.iter().map(column_to_field).collect();
        let builders: Vec<Box<dyn ArrayBuilder>> = fields
            .iter()
            .map(|f| crate::arrow_convert::make_builder(f.data_type(), capacity))
            .collect();
        Self {
            builders,
            fields,
            row_count: 0,
        }
    }

    /// Increment the row counter (call once per row after all columns written).
    pub fn finish_row(&mut self) {
        self.row_count += 1;
    }

    /// Current number of rows written.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// The Arrow fields for this result set.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Finish current builders into a RecordBatch and reset for next batch.
    pub fn flush(
        &mut self,
        capacity: usize,
    ) -> Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError> {
        let batch = crate::arrow_convert::finish_builders(&mut self.builders, &self.fields)?;
        self.builders = self
            .fields
            .iter()
            .map(|f| crate::arrow_convert::make_builder(f.data_type(), capacity))
            .collect();
        self.row_count = 0;
        Ok(batch)
    }

    /// Finish remaining builders into a RecordBatch (final flush).
    pub fn finish_batch(
        &mut self,
    ) -> Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError> {
        crate::arrow_convert::finish_builders(&mut self.builders, &self.fields)
    }
}

impl RowWriter for ArrowRowWriter {
    fn write_null(&mut self, col: usize) {
        let b = &mut self.builders[col];
        // We need to figure out the builder type to append null.
        // Use a helper that tries common types.
        append_null(b);
    }

    fn write_bool(&mut self, col: usize, val: bool) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_value(val);
    }

    fn write_u8(&mut self, col: usize, val: u8) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<UInt8Builder>()
            .unwrap()
            .append_value(val);
    }

    fn write_i16(&mut self, col: usize, val: i16) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<Int16Builder>()
            .unwrap()
            .append_value(val);
    }

    fn write_i32(&mut self, col: usize, val: i32) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_value(val);
    }

    fn write_i64(&mut self, col: usize, val: i64) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_value(val);
    }

    fn write_f32(&mut self, col: usize, val: f32) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .unwrap()
            .append_value(val);
    }

    fn write_f64(&mut self, col: usize, val: f64) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_value(val);
    }

    fn write_str(&mut self, col: usize, val: &str) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_value(val);
    }

    fn write_bytes(&mut self, col: usize, val: &[u8]) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .unwrap()
            .append_value(val);
    }

    fn write_date(&mut self, col: usize, days: i32) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .unwrap()
            .append_value(days);
    }

    fn write_time(&mut self, col: usize, nanos: i64) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<Time64NanosecondBuilder>()
            .unwrap()
            .append_value(nanos);
    }

    fn write_datetime(&mut self, col: usize, micros: i64) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .unwrap()
            .append_value(micros);
    }

    fn write_datetimeoffset(&mut self, col: usize, micros: i64, _offset_minutes: i16) {
        // Arrow stores as UTC timestamp; offset already subtracted by caller
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .unwrap()
            .append_value(micros);
    }

    fn write_decimal(&mut self, col: usize, value: i128, _precision: u8, _scale: u8) {
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .unwrap()
            .append_value(value);
    }

    fn write_guid(&mut self, col: usize, bytes: &[u8; 16]) {
        // GUID stored as string in Arrow (matches existing mapping)
        let uuid = uuid::Uuid::from_bytes(*bytes);
        self.builders[col]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_value(uuid.to_string());
    }
}

/// Append a null to a dynamically-typed ArrayBuilder.
fn append_null(builder: &mut Box<dyn ArrayBuilder>) {
    // Try each builder type in order of frequency
    if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<UInt8Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int16Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float32Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Decimal128Builder>() {
        b.append_null();
    } else if let Some(b) = builder
        .as_any_mut()
        .downcast_mut::<TimestampMicrosecondBuilder>()
    {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
        b.append_null();
    } else if let Some(b) = builder
        .as_any_mut()
        .downcast_mut::<Time64NanosecondBuilder>()
    {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<NullBuilder>() {
        b.append_null();
    } else {
        // Fallback: try StringBuilder (our default fallback type)
        panic!("Unknown builder type for null append");
    }
}
