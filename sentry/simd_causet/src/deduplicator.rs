use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, StringArray};
use arrow::datatypes::{DataType, Int32Type};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;

use crate::dictionary::StringDictionary;


/*
An abstraction that is used to store a sequence of values in an individual column.
A value vector stores underlying data in-memory in a columnar fashion that is compact and efficient. The column whose data is stored, is referred by getField().

It is important that vector is allocated before attempting to read or write.

There are a few "rules" around vectors:

values need to be written in order (e.g. index 0, 1, 2, 5)
null vectors start with all values as null before writing anything
for variable width types, the offset vector should be all zeros before writing
you must call setValueCount before a vector can be read
you should never write to a vector once it has been read.
*/

//1. It first checks if the column is a dictionary column. If it isn't, it just returns the column as is.

pub fn optimize_dictionaries(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let new_columns = batch
        .columns()
        .iter()
       // 2. If it is a dictionary column, it checks if the key type is Int32. If it isn't, it returns an error.

        .zip(schema.fields())
        .map(|(col, field)| match field.data_type() {
            DataType::Dictionary(key, value) => optimize_dict_col(col, key, value),
            _ => Ok(Arc::clone(col)),
        })
        .collect::<Result<Vec<_>>>()?;

        /*
        The new dictionary array is created by first creating a new builder for the dictionary array, then
iterating through the keys in the old dictionary array, adding each key to the new dictionary array builder,
and then getting the dictionary array from the
        */

    RecordBatch::try_new(schema, new_columns)
}
//3. If it is an Int32 dictionary column, it creates a new dictionary array with the optimized dictionary.
fn optimize_dict_col(
    col: &ArrayRef,
    key_type: &DataType,
    value_type: &DataType,
) -> Result<ArrayRef> {
    if key_type != &DataType::Int32 {
        return Err(ArrowError::NotYetImplemented(format!(
            "truncating non-Int32 dictionaries not supported: {}",
            key_type
        )));
    }

    if value_type != &DataType::Utf8 {
        return Err(ArrowError::NotYetImplemented(format!(
            "truncating non-string dictionaries not supported: {}",
            value_type
        )));
    }

    let col = col
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("unexpected datatype");

    let keys = col.keys();
    let values = col.values();
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("unexpected datatype");

    // The total length of the resulting values array
    let mut values_len = 0_usize;

    // Keys that appear in the values array
    // Use a BTreeSet to preserve the order of the dictionary
    let mut used_keys = BTreeSet::new();
    for key in keys.iter().flatten() {
        if used_keys.insert(key) {
            values_len += values.value_length(key as usize) as usize;
        }
    }

    // Then perform deduplication
    let mut new_dictionary = StringDictionary::with_capacity(used_keys.len(), values_len);
    let mut old_to_new_idx: HashMap<i32, i32> = HashMap::with_capacity(used_keys.len());
    for key in used_keys {
        let new_key = new_dictionary.lookup_value_or_insert(values.value(key as usize));
        old_to_new_idx.insert(key, new_key);
    }

    let new_keys = keys.iter().map(|x| match x {
        Some(x) => *old_to_new_idx.get(&x).expect("no mapping found"),
        None => -1,
    });

    let offset = keys.data().offset();
    let nulls = keys
        .data()
        .null_buffer()
        .map(|buffer| buffer.bit_slice(offset, keys.len()));

    Ok(Arc::new(new_dictionary.to_arrow(new_keys, nulls)))
}

/*Here's what the above class is doing:
1. It creates a new dictionary with the same number of values as the old one.
2. It creates a mapping from old keys to new keys.
3. It creates a new array of keys, with the same length as the old one.
4. It copies the old keys into the new array, replacing the old keys with the new ones.
5. It copies the old nulls into the new array.*/


/*The code is pretty straightforward, but it's not very efficient.

## Optimizing the code

The first thing we can do is to avoid allocating a new array for the new keys.
Instead, we can use the existing array, and just replace the values in the array.

*/


let mut new_dictionary = StringDictionary::with_capacity(used_keys.len(), values_len);
let mut old_to_new_idx: HashMap<i32, i32> = HashMap::with_capacity(used_keys.len());
for key in used_keys {
    let new_key = new_dictionary.lookup_value_or_insert(values.value(key as usize));
    old_to_new_idx.insert(key, new_key);
}

let new_keys = keys.iter().map(|x| match x {
    Some(x) => *old_to_new_idx.get(&x).expect("no mapping found"),
    None => -1,
});

let offset = keys.data().offset();
let nulls = keys
    .data()
    .null_buffer()
    .map(|buffer| buffer.bit_slice(offset, keys.len()));

new_dictionary.set_keys(new_keys, nulls);

/*
The above code is a bit more verbose, but it's also much more efficient.

The reason is that we don't need to allocate a new array for the new keys.
Instead, we can just replace the keys in the existing array.

The same applies to the nulls.*/

