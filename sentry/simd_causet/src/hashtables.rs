use std::ops::Range;
use std::convert::TryFrom;
use arrow::array::{Array, ArrayDataBuilder, DictionaryArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Int32Type};
use hashbrown::HashMap;
use num_traits::{AsPrimitive, FromPrimitive, Zero};

use crate::string::PackedStringArray;

//1. The `new` method creates a new `BitSet` with a default buffer of length 0.
//2. The `with_size` method creates a new `BitSet` with a buffer of the specified length.
//3. The `reserve` method ensures that the buffer has enough space to store

#[derive(Debug, Default)]
pub struct BitSet {

    buffer: Vec<u8>,

    /// The length of this mask in bits
    len: usize,
}

impl BitSet {
 
    pub fn new() -> Self {
        Self::default()
    }

   
    pub fn with_size(count: usize) -> Self {
        let mut bitset = Self::default();
        bitset.append_unset(count);
        bitset
    }

    
    pub fn reserve(&mut self, count: usize) {
        let new_buf_len = (self.len + count + 7) >> 3;
        self.buffer.reserve(new_buf_len);
    }

   
    pub fn append_unset(&mut self, count: usize) {
        self.len += count;
        let new_buf_len = (self.len + 7) >> 3;
        self.buffer.resize(new_buf_len, 0);
    }

 
    pub fn append_set(&mut self, count: usize) {
        let new_len = self.len + count;
        let new_buf_len = (new_len + 7) >> 3;

        let skew = self.len & 7;
        if skew != 0 {
            *self.buffer.last_mut().unwrap() |= 0xFF << skew;
        }

        self.buffer.resize(new_buf_len, 0xFF);

        let rem = new_len & 7;
        if rem != 0 {
            *self.buffer.last_mut().unwrap() &= (1 << rem) - 1;
        }

        self.len = new_len;
    }




//6. Finally, it updates the length of the [`BitSet`] by the count of the range.


//1. It first checks if the range is empty. If it is, it returns immediately.
//2. It then calculates the start and end bytes of the range.
pub fn truncate(&mut self, len: usize) {
//3. It then calculates the skew of the range.
//4. It then calls the `append_bits` method to append the range to the buffer.
    let new_buf_len = (len + 7) >> 3;
    self.buffer.truncate(new_buf_len);
    let overrun = len & 7;
    if overrun > 0 {
        *self.buffer.last_mut().unwrap() &= (1 << overrun) - 1;
    }
    //5. It then updates the length of the buffer.
    self.len = len;
}

/// Extends this [`BitSet`] by the context of `other`
pub fn extend_from(&mut self, other: &BitSet) {
    self.append_bits(other.len, &other.buffer)
}

/// Extends this [`BitSet`] by `range` elements in `other`
pub fn extend_from_range(&mut self, other: &BitSet, range: Range<usize>) {
    let count = range.end - range.start;
    if count == 0 {
        return;
    }

    let start_byte = range.start >> 3;
    let end_byte = (range.end + 7) >> 3;
    let skew = range.start & 7;

    //6. Finally, it updates the length of the [`BitSet`] by the count of the range.


        if skew == 0 {
            
            self.append_bits(count, &other.buffer[start_byte..end_byte])
        } else if start_byte + 1 == end_byte {
            
            self.append_bits(count, &[other.buffer[start_byte] >> skew])
        } else {
            // Append trailing bits from first byte to reach byte boundary, then append
            // bits from the remaining byte-aligned mask
            let offset = 8 - skew;
            self.append_bits(offset, &[other.buffer[start_byte] >> skew]);
            self.append_bits(count - offset, &other.buffer[(start_byte + 1)..end_byte]);
        }
    }
//1. The append_bits function takes a count and a byte array.


    pub fn append_bits(&mut self, count: usize, to_set: &[u8]) {
        //2. It first checks that the count is a multiple of 8.
        assert_eq!((count + 7) >> 3, to_set.len());

        //3. It then checks that the byte array is the correct length.
        let new_len = self.len + count;
        let new_buf_len = (new_len + 7) >> 3;
        self.buffer.reserve(new_buf_len - self.buffer.len());

        let whole_bytes = count >> 3;
        let overrun = count & 7;

        let skew = self.len & 7;
        if skew == 0 {
            self.buffer.extend_from_slice(&to_set[..whole_bytes]);
            if overrun > 0 {
                let masked = to_set[whole_bytes] & ((1 << overrun) - 1);
                self.buffer.push(masked)
            }

            self.len = new_len;
            debug_assert_eq!(self.buffer.len(), new_buf_len);
            return;
        }

        for to_set_byte in &to_set[..whole_bytes] {
            let low = *to_set_byte << skew;
            let high = *to_set_byte >> (8 - skew);

            *self.buffer.last_mut().unwrap() |= low;
            self.buffer.push(high);
        }
        //4. It then calculates the number of whole bytes and the number of bits that are left over. 

        if overrun > 0 {
            let masked = to_set[whole_bytes] & ((1 << overrun) - 1);
            let low = masked << skew;
            *self.buffer.last_mut().unwrap() |= low;

            if overrun > 8 - skew {
                let high = masked >> (8 - skew);
                self.buffer.push(high)
            }
        }

        self.len = new_len;
        debug_assert_eq!(self.buffer.len(), new_buf_len);
    }

    


    /// Sets a given bit
    /////1. The constructor takes in a buffer of bytes.
    pub fn set(&mut self, idx: usize) {
        let byte_idx = idx >> 3;
        let bit_idx = idx & 7;
        //2. The set method takes in an index and sets the bit at that index to 1.
        self.buffer[byte_idx] |= 1 << bit_idx;
    }

/// Returns if the given index is set
//3. The get method takes in an index and returns true if the bit at that index is 1, false otherwise.

    pub fn get(&self, idx: usize) -> bool {
        let byte_idx = idx >> 3;
        let bit_idx = idx & 7;
        (self.buffer[byte_idx] >> bit_idx) & 1 != 0
    }
/// Converts this BitSet to a buffer compatible with arrows boolean encoding
pub fn to_arrow(&self) -> Buffer {
    Buffer::from(&self.buffer)
}

/// Returns the number of values stored in the bitset
pub fn len(&self) -> usize {
    self.len
}

/// Returns if this bitset is empty
pub fn is_empty(&self) -> bool {
    self.len == 0
}

/// Returns the number of bytes used by this bitset
pub fn byte_len(&self) -> usize {
    self.buffer.len()
}

/// Return the raw packed bytes used by thie bitset
pub fn bytes(&self) -> &[u8] {
    &self.buffer
}
}

    