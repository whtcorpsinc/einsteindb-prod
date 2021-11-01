
use arrow::array::{ArrayRef, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::Result;
use arrow::record_batch::RecordBatch;

use comfy_table::{Cell, Table};

use chrono::prelude::*;


pub fn pretty_format_batches(results: &[RecordBatch]) -> Result<String> {
    Ok(create_table(results)?.to_string())
}

/*1. It first checks if the column is a timestamp array. 
If it is, it then checks if the timezone is UTC. 
If it is, it then converts the timestamp to a string in the preferred format.
*/
fn array_value_to_string(column: &ArrayRef, row: usize) -> Result<String> {
    match column.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, None) if column.is_valid(row) => {
            let ts_column = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();

            let ts_value = ts_column.value(row);
            const NANOS_IN_SEC: i64 = 1_000_000_000;
            let secs = ts_value / NANOS_IN_SEC;
            let nanos = (ts_value - (secs * NANOS_IN_SEC)) as u32;
            let ts = NaiveDateTime::from_timestamp(secs, nanos);
            // treat as UTC
            let ts = DateTime::<Utc>::from_utc(ts, Utc);
            // convert to string in preferred influx format
            let use_z = true;
            /*2. If the column is not a timestamp array, it falls back to the default printing of the array*/
            Ok(ts.to_rfc3339_opts(SecondsFormat::AutoSi, use_z))
        }
        _ => {
            // fallback to arrow's default printing for other types
            arrow::util::display::array_value_to_string(column, row)
        }
    }
}


fn create_table(results: &[RecordBatch]) -> Result<Table> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }
    table.set_header(header);

    for batch in results {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for col in 0..batch.num_columns() {
                let column = batch.column(col);
                cells.push(Cell::new(&array_value_to_string(column, row)?));
            }
            table.add_row(cells);
        }
    }

    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::{
        array::{
            ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray,
            UInt64Array,
        },
        datatypes::Int32Type,
    };

    #[test]
    fn test_formatting() {
        // tests formatting all of the Arrow array types used in EinsteinDB

        // tags use string dictionary
        let dict_array: ArrayRef = Arc::new(
            vec![Some("a"), None, Some("b")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );