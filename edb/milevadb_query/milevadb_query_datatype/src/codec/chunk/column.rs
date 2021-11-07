//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::convert::TryFrom;

use crate::prelude::*;
use crate::{EvalType, FieldTypeFlag, FieldTypeTp};
use codec::buffer::{BufferReader, BufferWriter};
use codec::number::{NumberDecoder, NumberEncoder};
use violetabftstore::interlock::::buffer_vec::BufferVec;
use fidel_timeshare::FieldType;

use super::{Error, Result};
use crate::codec::data_type::{SolitonRef, VectorValue};
use crate::codec::datum;
use crate::codec::datum_codec::DatumPayloadDecoder;
use crate::codec::mysql::decimal::{
    Decimal, DecimalDatumPayloadSolitonEncoder, DecimalDecoder, DecimalEncoder, DECIMAL_STRUCT_SIZE,
};
use crate::codec::mysql::duration::{
    Duration, DurationDatumPayloadSolitonEncoder, DurationDecoder, DurationEncoder,
};
use crate::codec::mysql::json::{
    Json, JsonDatumPayloadSolitonEncoder, JsonDecoder, JsonEncoder, JsonRef,
};
use crate::codec::mysql::time::{Time, TimeDatumPayloadSolitonEncoder, TimeDecoder, TimeEncoder};
use crate::codec::Datum;
use crate::expr::EvalContext;

/// `PrimaryCauset` stores the same PrimaryCauset data of multi events in one Soliton.
#[derive(Default)]
pub struct PrimaryCauset {
    length: usize,
    null_cnt: usize,
    null_bitmap: Vec<u8>,
    var_offsets: Vec<usize>,
    data: Vec<u8>,
    // if the data's length is fixed, fixed_len should be bigger than 0
    fixed_len: usize,
}

impl PrimaryCauset {
    /// Create the PrimaryCauset with a specified type and capacity.
    pub fn new(field_type_tp: FieldTypeTp, init_cap: usize) -> PrimaryCauset {
        match field_type_tp {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong
            | FieldTypeTp::Year
            | FieldTypeTp::Double
            | FieldTypeTp::Duration => PrimaryCauset::new_fixed_len(8, init_cap),
            FieldTypeTp::Float => PrimaryCauset::new_fixed_len(4, init_cap),
            FieldTypeTp::Date | FieldTypeTp::DateTime | FieldTypeTp::Timestamp => {
                PrimaryCauset::new_fixed_len(8, init_cap)
            }
            FieldTypeTp::NewDecimal => PrimaryCauset::new_fixed_len(DECIMAL_STRUCT_SIZE, init_cap),
            _ => PrimaryCauset::new_var_len_PrimaryCauset(init_cap),
        }
    }

    pub fn from_raw_datums(
        field_type: &FieldType,
        raw_datums: &BufferVec,
        logical_rows: &[usize],
        ctx: &mut EvalContext,
    ) -> Result<Self> {
        let mut col = PrimaryCauset::new(field_type.as_accessor().tp(), logical_rows.len());

        let eval_type = box_try!(EvalType::try_from(field_type.as_accessor().tp()));
        match eval_type {
            EvalType::Int => {
                if field_type.is_unsigned() {
                    for &row_index in logical_rows {
                        col.applightlike_u64_datum(&raw_datums[row_index])?
                    }
                } else {
                    for &row_index in logical_rows {
                        col.applightlike_i64_datum(&raw_datums[row_index])?
                    }
                }
            }
            EvalType::Real => {
                if field_type.as_accessor().tp() == FieldTypeTp::Float {
                    for &row_index in logical_rows {
                        col.applightlike_f32_datum(&raw_datums[row_index])?
                    }
                } else {
                    for &row_index in logical_rows {
                        col.applightlike_f64_datum(&raw_datums[row_index])?
                    }
                }
            }
            EvalType::Decimal => {
                for &row_index in logical_rows {
                    col.applightlike_decimal_datum(&raw_datums[row_index])?
                }
            }
            EvalType::Bytes => {
                for &row_index in logical_rows {
                    col.applightlike_bytes_datum(&raw_datums[row_index])?
                }
            }
            EvalType::DateTime => {
                for &row_index in logical_rows {
                    col.applightlike_time_datum(&raw_datums[row_index], ctx, field_type)?
                }
            }
            EvalType::Duration => {
                for &row_index in logical_rows {
                    col.applightlike_duration_datum(&raw_datums[row_index])?
                }
            }
            EvalType::Json => {
                for &row_index in logical_rows {
                    col.applightlike_json_datum(&raw_datums[row_index])?
                }
            }
        }

        Ok(col)
    }

    pub fn from_vector_value(
        field_type: &FieldType,
        v: &VectorValue,
        logical_rows: &[usize],
    ) -> Result<Self> {
        let mut col = PrimaryCauset::new(field_type.as_accessor().tp(), logical_rows.len());

        match v {
            VectorValue::Int(vec) => {
                if field_type.is_unsigned() {
                    for &row_index in logical_rows {
                        match vec.get_option_ref(row_index) {
                            None => {
                                col.applightlike_null();
                            }
                            Some(val) => {
                                col.applightlike_u64(*val as u64)?;
                            }
                        }
                    }
                } else {
                    for &row_index in logical_rows {
                        match vec.get_option_ref(row_index) {
                            None => {
                                col.applightlike_null();
                            }
                            Some(val) => {
                                col.applightlike_i64(*val)?;
                            }
                        }
                    }
                }
            }
            VectorValue::Real(vec) => {
                if col.get_fixed_len() == 4 {
                    for &row_index in logical_rows {
                        match vec.get_option_ref(row_index) {
                            None => {
                                col.applightlike_null();
                            }
                            Some(val) => {
                                col.applightlike_f32(f64::from(*val) as f32)?;
                            }
                        }
                    }
                } else {
                    for &row_index in logical_rows {
                        match vec.get_option_ref(row_index) {
                            None => {
                                col.applightlike_null();
                            }
                            Some(val) => {
                                col.applightlike_f64(f64::from(*val))?;
                            }
                        }
                    }
                }
            }
            VectorValue::Decimal(vec) => {
                for &row_index in logical_rows {
                    match vec.get_option_ref(row_index) {
                        None => {
                            col.applightlike_null();
                        }
                        Some(val) => {
                            col.applightlike_decimal(&val)?;
                        }
                    }
                }
            }
            VectorValue::Bytes(vec) => {
                for &row_index in logical_rows {
                    match vec.get_option_ref(row_index) {
                        None => {
                            col.applightlike_null();
                        }
                        Some(val) => {
                            col.applightlike_bytes(&val)?;
                        }
                    }
                }
            }
            VectorValue::DateTime(vec) => {
                for &row_index in logical_rows {
                    match vec.get_option_ref(row_index) {
                        None => {
                            col.applightlike_null();
                        }
                        Some(val) => {
                            col.applightlike_time(*val)?;
                        }
                    }
                }
            }
            VectorValue::Duration(vec) => {
                for &row_index in logical_rows {
                    match vec.get_option_ref(row_index) {
                        None => {
                            col.applightlike_null();
                        }
                        Some(val) => {
                            col.applightlike_duration(*val)?;
                        }
                    }
                }
            }
            VectorValue::Json(vec) => {
                for &row_index in logical_rows {
                    match vec.get_option_ref(row_index) {
                        None => {
                            col.applightlike_null();
                        }
                        Some(val) => col.applightlike_json(val)?,
                    }
                }
            }
        }
        Ok(col)
    }

    /// Get the datum of one Evcausetidx with the specified type.
    pub fn get_datum(&self, idx: usize, field_type: &dyn FieldTypeAccessor) -> Result<Datum> {
        if self.is_null(idx) {
            return Ok(Datum::Null);
        }
        let d = match field_type.tp() {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong
            | FieldTypeTp::Year => {
                if field_type.flag().contains(FieldTypeFlag::UNSIGNED) {
                    Datum::U64(self.get_u64(idx)?)
                } else {
                    Datum::I64(self.get_i64(idx)?)
                }
            }
            FieldTypeTp::Double => Datum::F64(self.get_f64(idx)?),
            FieldTypeTp::Float => Datum::F64(f64::from(self.get_f32(idx)?)),
            FieldTypeTp::Date | FieldTypeTp::DateTime | FieldTypeTp::Timestamp => {
                Datum::Time(self.get_time(idx)?)
            }
            FieldTypeTp::Duration => Datum::Dur(self.get_duration(idx, field_type.decimal())?),
            FieldTypeTp::NewDecimal => Datum::Dec(self.get_decimal(idx)?),
            FieldTypeTp::JSON => Datum::Json(self.get_json(idx)?),
            FieldTypeTp::Enum | FieldTypeTp::Bit | FieldTypeTp::Set => {
                return Err(box_err!(
                    "get datum with {} is not supported yet.",
                    field_type.tp()
                ));
            }
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::Blob
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob => Datum::Bytes(self.get_bytes(idx).to_vec()),
            _ => unreachable!(),
        };
        Ok(d)
    }

    /// Applightlike datum to the PrimaryCauset.
    pub fn applightlike_datum(&mut self, data: &Datum) -> Result<()> {
        match data {
            Datum::Null => {
                self.applightlike_null();
                Ok(())
            }
            Datum::I64(v) => self.applightlike_i64(*v),
            Datum::U64(v) => self.applightlike_u64(*v),
            Datum::F64(v) => {
                if self.fixed_len == 4 {
                    self.applightlike_f32(*v as f32)
                } else {
                    self.applightlike_f64(*v)
                }
            }
            Datum::Bytes(ref v) => self.applightlike_bytes(v),
            Datum::Dec(ref v) => self.applightlike_decimal(v),
            Datum::Dur(v) => self.applightlike_duration(*v),
            Datum::Time(v) => self.applightlike_time(*v),
            Datum::Json(ref v) => self.applightlike_json(v.as_ref()),
            _ => Err(box_err!("unsupported datum {:?}", data)),
        }
    }

    /// Create a PrimaryCauset with a fixed element length.
    pub fn new_fixed_len(element_len: usize, init_cap: usize) -> PrimaryCauset {
        PrimaryCauset {
            fixed_len: element_len,
            data: Vec::with_capacity(element_len * init_cap),
            null_bitmap: Vec::with_capacity(init_cap / 8),
            ..Default::default()
        }
    }

    /// Create a PrimaryCauset with variant element length.
    pub fn new_var_len_PrimaryCauset(init_cap: usize) -> PrimaryCauset {
        let mut offsets = Vec::with_capacity(init_cap + 1);
        offsets.push(0);
        PrimaryCauset {
            var_offsets: offsets,
            data: Vec::with_capacity(4 * init_cap),
            null_bitmap: Vec::with_capacity(init_cap / 8),
            ..Default::default()
        }
    }

    /// Return whether the PrimaryCauset has a fixed length or not.
    #[inline]
    fn is_fixed(&self) -> bool {
        self.fixed_len > 0
    }

    /// Return the PrimaryCauset's fixed length.
    #[inline]
    pub fn get_fixed_len(&self) -> usize {
        self.fixed_len
    }

    /// Reset the PrimaryCauset
    pub fn reset(&mut self) {
        self.length = 0;
        self.null_cnt = 0;
        self.null_bitmap.clear();
        if !self.var_offsets.is_empty() {
            // The first offset is always 0, it makes slicing the data easier, we need to keep it.
            self.var_offsets.truncate(1);
        }
        self.data.clear();
    }

    /// Return whether the datum for the Evcausetidx is null or not.
    pub fn is_null(&self, row_idx: usize) -> bool {
        if self.null_cnt == 0 {
            return false;
        }

        if let Some(null_byte) = self.null_bitmap.get(row_idx >> 3) {
            null_byte & (1 << ((row_idx) & 7)) == 0
        } else {
            panic!("index out of cone");
        }
    }

    /// fidelio the null bitmap and count when applightlike a datum.
    /// `on` is false means the datum is null.
    #[inline]
    fn applightlike_null_bitmap(&mut self, on: bool) {
        let idx = self.length >> 3;
        if idx >= self.null_bitmap.len() {
            self.null_bitmap.push(0);
        }
        if on {
            let pos = self.length & 7;
            self.null_bitmap[idx] |= 1 << pos;
        } else {
            self.null_cnt += 1;
        }
    }

    /// Applightlike null to the PrimaryCauset.
    #[inline]
    pub fn applightlike_null(&mut self) {
        self.applightlike_null_bitmap(false);
        if self.is_fixed() {
            let len = self.fixed_len + self.data.len();
            self.data.resize(len, 0);
        } else {
            let offset = self.var_offsets[self.length];
            self.var_offsets.push(offset);
        }
        self.length += 1;
    }

    /// Called when the fixed datum has been applightlikeed.
    #[inline]
    fn finish_applightlike_fixed(&mut self) {
        self.applightlike_null_bitmap(true);
        self.length += 1;
        self.data.resize(self.length * self.fixed_len, 0);
    }

    /// Called when the variant datum has been applightlikeed.
    #[inline]
    fn finished_applightlike_var(&mut self) {
        self.applightlike_null_bitmap(true);
        let offset = self.data.len();
        self.var_offsets.push(offset);
        self.length += 1;
    }

    /// Applightlike i64 datum to the PrimaryCauset.
    #[inline]
    pub fn applightlike_i64(&mut self, v: i64) -> Result<()> {
        self.data.write_i64_le(v)?;
        self.finish_applightlike_fixed();
        Ok(())
    }

    pub fn applightlike_i64_datum(&mut self, src_datum: &[u8]) -> Result<()> {
        if src_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }
        let flag = src_datum[0];
        let mut raw_datum = &src_datum[1..];
        match flag {
            datum::NIL_FLAG => self.applightlike_null(),
            datum::INT_FLAG => self.applightlike_i64(raw_datum.read_datum_payload_i64()?)?,
            datum::UINT_FLAG => self.applightlike_i64(raw_datum.read_datum_payload_u64()? as i64)?,
            datum::VAR_INT_FLAG => self.applightlike_i64(raw_datum.read_datum_payload_var_i64()?)?,
            datum::VAR_UINT_FLAG => {
                self.applightlike_i64(raw_datum.read_datum_payload_var_u64()? as i64)?
            }
            _ => {
                return Err(Error::InvalidDataType(format!(
                    "Unsupported datum flag {} for Int vector",
                    flag
                )))
            }
        }
        Ok(())
    }

    /// Get the i64 datum of the Evcausetidx in the PrimaryCauset.
    #[inline]
    pub fn get_i64(&self, idx: usize) -> Result<i64> {
        let spacelike = idx * self.fixed_len;
        let lightlike = spacelike + self.fixed_len;
        let mut data = &self.data[spacelike..lightlike];
        data.read_i64_le().map_err(Error::from)
    }

    /// Applightlike u64 datum to the PrimaryCauset.
    #[inline]
    pub fn applightlike_u64(&mut self, v: u64) -> Result<()> {
        self.data.write_u64_le(v)?;
        self.finish_applightlike_fixed();
        Ok(())
    }

    pub fn applightlike_u64_datum(&mut self, src_datum: &[u8]) -> Result<()> {
        if src_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }
        let flag = src_datum[0];
        let mut raw_datum = &src_datum[1..];
        match flag {
            datum::NIL_FLAG => self.applightlike_null(),
            datum::INT_FLAG => self.applightlike_u64(raw_datum.read_datum_payload_i64()? as u64)?,
            datum::UINT_FLAG => self.applightlike_u64(raw_datum.read_datum_payload_u64()?)?,
            datum::VAR_INT_FLAG => {
                self.applightlike_u64(raw_datum.read_datum_payload_var_i64()? as u64)?
            }
            datum::VAR_UINT_FLAG => self.applightlike_u64(raw_datum.read_datum_payload_var_u64()?)?,
            _ => {
                return Err(Error::InvalidDataType(format!(
                    "Unsupported datum flag {} for Int vector",
                    flag
                )))
            }
        }
        Ok(())
    }

    /// Get the u64 datum of the Evcausetidx in the PrimaryCauset.
    #[inline]
    pub fn get_u64(&self, idx: usize) -> Result<u64> {
        let spacelike = idx * self.fixed_len;
        let lightlike = spacelike + self.fixed_len;
        let mut data = &self.data[spacelike..lightlike];
        data.read_u64_le().map_err(Error::from)
    }

    /// Applightlike a f64 datum to the PrimaryCauset.
    #[inline]
    pub fn applightlike_f64(&mut self, v: f64) -> Result<()> {
        self.data.write_f64_le(v)?;
        self.finish_applightlike_fixed();
        Ok(())
    }

    #[inline]
    pub fn applightlike_f64_datum(&mut self, src_datum: &[u8]) -> Result<()> {
        if src_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }
        let flag = src_datum[0];
        let mut raw_datum = &src_datum[1..];
        match flag {
            datum::NIL_FLAG => self.applightlike_null(),
            // In both index and record, it's flag is `FLOAT`. See MilevaDB's `encode()`.
            datum::FLOAT_FLAG => {
                let v = raw_datum.read_datum_payload_f64()?;
                self.applightlike_f64(v)?;
            }
            _ => {
                return Err(Error::InvalidDataType(format!(
                    "Unsupported datum flag {} for Real vector",
                    flag
                )))
            }
        }
        Ok(())
    }

    /// Applightlike a f32 datum to the PrimaryCauset.
    #[inline]
    pub fn applightlike_f32(&mut self, v: f32) -> Result<()> {
        self.data.write_f32_le(v)?;
        self.finish_applightlike_fixed();
        Ok(())
    }

    pub fn applightlike_f32_datum(&mut self, src_datum: &[u8]) -> Result<()> {
        if src_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }
        let flag = src_datum[0];
        let mut raw_datum = &src_datum[1..];
        match flag {
            datum::NIL_FLAG => self.applightlike_null(),
            // In both index and record, it's flag is `FLOAT`. See MilevaDB's `encode()`.
            datum::FLOAT_FLAG => {
                let v = raw_datum.read_datum_payload_f64()?;
                self.applightlike_f32(v as f32)?;
            }
            _ => {
                return Err(Error::InvalidDataType(format!(
                    "Unsupported datum flag {} for Real vector",
                    flag
                )))
            }
        }
        Ok(())
    }

    /// Get the f64 datum of the Evcausetidx in the PrimaryCauset.
    #[inline]
    pub fn get_f64(&self, idx: usize) -> Result<f64> {
        let spacelike = idx * self.fixed_len;
        let lightlike = spacelike + self.fixed_len;
        let mut data = &self.data[spacelike..lightlike];
        data.read_f64_le().map_err(Error::from)
    }

    /// Get the f32 datum of the Evcausetidx in the PrimaryCauset.
    #[inline]
    pub fn get_f32(&self, idx: usize) -> Result<f32> {
        let spacelike = idx * self.fixed_len;
        let lightlike = spacelike + self.fixed_len;
        let mut data = &self.data[spacelike..lightlike];
        data.read_f32_le().map_err(Error::from)
    }

    /// Applightlike a bytes datum to the PrimaryCauset.
    #[inline]
    pub fn applightlike_bytes(&mut self, byte: &[u8]) -> Result<()> {
        self.data.extlightlike_from_slice(byte);
        self.finished_applightlike_var();
        Ok(())
    }

    pub fn applightlike_bytes_datum(&mut self, src_datum: &[u8]) -> Result<()> {
        if src_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }
        let flag = src_datum[0];
        let mut raw_datum = &src_datum[1..];
        match flag {
            datum::NIL_FLAG => self.applightlike_null(),
            // In index, it's flag is `BYTES`. See MilevaDB's `encode()`.
            // TODO: this method's performance can be further improved
            datum::BYTES_FLAG => self.applightlike_bytes(&raw_datum.read_datum_payload_bytes()?)?,
            // In record, it's flag is `COMPACT_BYTES`. See MilevaDB's `encode()`.
            datum::COMPACT_BYTES_FLAG => {
                let vn = raw_datum.read_var_i64()? as usize;
                let data = raw_datum.read_bytes(vn)?;
                self.applightlike_bytes(&data[0..vn])?;
            }
            _ => {
                return Err(Error::InvalidDataType(format!(
                    "Unsupported datum flag {} for Bytes vector",
                    flag
                )))
            }
        }
        Ok(())
    }

    /// Get the bytes datum of the Evcausetidx in the PrimaryCauset.
    #[inline]
    pub fn get_bytes(&self, idx: usize) -> &[u8] {
        let spacelike = self.var_offsets[idx];
        let lightlike = self.var_offsets[idx + 1];
        &self.data[spacelike..lightlike]
    }

    /// Applightlike a time datum to the PrimaryCauset.
    #[inline]
    pub fn applightlike_time(&mut self, t: Time) -> Result<()> {
        self.data.write_time(t)?;
        self.finish_applightlike_fixed();
        Ok(())
    }

    pub fn applightlike_time_datum(
        &mut self,
        src_datum: &[u8],
        ctx: &mut EvalContext,
        field_type: &FieldType,
    ) -> Result<()> {
        if src_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }
        let flag = src_datum[0];
        let raw_datum = &src_datum[1..];

        match flag {
            datum::NIL_FLAG => self.applightlike_null(),
            // In index, it's flag is `UINT`. See MilevaDB's `encode()`.
            datum::UINT_FLAG => {
                self.data
                    .write_time_to_Soliton_by_datum_payload_int(raw_datum, ctx, field_type)?;
                self.finish_applightlike_fixed();
            }
            // In record, it's flag is `VAR_UINT`. See MilevaDB's `flatten()` and `encode()`.
            datum::VAR_UINT_FLAG => {
                self.data
                    .write_time_to_Soliton_by_datum_payload_varint(raw_datum, ctx, field_type)?;
                self.finish_applightlike_fixed();
            }
            _ => {
                return Err(Error::InvalidDataType(format!(
                    "Unsupported datum flag {} for DateTime vector.",
                    flag
                )))
            }
        }
        Ok(())
    }

    /// Get the time datum of the Evcausetidx in the PrimaryCauset.
    #[inline]
    pub fn get_time(&self, idx: usize) -> Result<Time> {
        let spacelike = idx * self.fixed_len;
        let lightlike = spacelike + self.fixed_len;
        let mut data = &self.data[spacelike..lightlike];
        data.read_time_from_Soliton()
    }

    /// Applightlike a duration datum to the PrimaryCauset.
    #[inline]
    pub fn applightlike_duration(&mut self, d: Duration) -> Result<()> {
        self.data.write_duration_to_Soliton(d)?;
        self.finish_applightlike_fixed();
        Ok(())
    }

    pub fn applightlike_duration_datum(&mut self, src_datum: &[u8]) -> Result<()> {
        if src_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }
        let flag = src_datum[0];
        let raw_datum = &src_datum[1..];
        match flag {
            datum::NIL_FLAG => self.applightlike_null(),
            // In index, it's flag is `DURATION`. See MilevaDB's `encode()`.
            datum::DURATION_FLAG => {
                self.data
                    .write_duration_to_Soliton_by_datum_payload_int(raw_datum)?;
                self.finish_applightlike_fixed();
            }
            // In record, it's flag is `VAR_INT`. See MilevaDB's `flatten()` and `encode()`.
            datum::VAR_INT_FLAG => {
                self.data
                    .write_duration_to_Soliton_by_datum_payload_varint(raw_datum)?;
                self.finish_applightlike_fixed();
            }
            _ => {
                return Err(Error::InvalidDataType(format!(
                    "Unsupported datum flag {} for Duration vector",
                    flag
                )))
            }
        }
        Ok(())
    }

    /// Get the duration datum of the Evcausetidx in the PrimaryCauset.
    #[inline]
    pub fn get_duration(&self, idx: usize, fsp: isize) -> Result<Duration> {
        let spacelike = idx * self.fixed_len;
        let lightlike = spacelike + self.fixed_len;
        let mut data = &self.data[spacelike..lightlike];
        data.read_duration_from_Soliton(fsp)
    }

    /// Applightlike a decimal datum to the PrimaryCauset.
    #[inline]
    pub fn applightlike_decimal(&mut self, d: &Decimal) -> Result<()> {
        self.data.write_decimal_to_Soliton(d)?;
        self.finish_applightlike_fixed();
        Ok(())
    }

    pub fn applightlike_decimal_datum(&mut self, src_datum: &[u8]) -> Result<()> {
        if src_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }
        let flag = src_datum[0];
        let raw_datum = &src_datum[1..];
        match flag {
            datum::NIL_FLAG => self.applightlike_null(),
            // In both index and record, it's flag is `DECIMAL`. See MilevaDB's `encode()`.
            datum::DECIMAL_FLAG => {
                self.data
                    .write_decimal_to_Soliton_by_datum_payload(raw_datum)?;
                self.finish_applightlike_fixed();
            }
            _ => {
                return Err(Error::InvalidDataType(format!(
                    "Unsupported datum flag {} for Decimal vector",
                    flag
                )))
            }
        }
        Ok(())
    }

    /// Get the decimal datum of the Evcausetidx in the PrimaryCauset.
    #[inline]
    pub fn get_decimal(&self, idx: usize) -> Result<Decimal> {
        let spacelike = idx * self.fixed_len;
        let lightlike = spacelike + self.fixed_len;
        let mut data = &self.data[spacelike..lightlike];
        data.read_decimal_from_Soliton()
    }

    /// Applightlike a json datum to the PrimaryCauset.
    #[inline]
    pub fn applightlike_json(&mut self, j: JsonRef) -> Result<()> {
        self.data.write_json(j)?;
        self.finished_applightlike_var();
        Ok(())
    }

    /// Applightlike a json datum in raw bytes to the PrimaryCauset.
    pub fn applightlike_json_datum(&mut self, src_datum: &[u8]) -> Result<()> {
        if src_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }
        let flag = src_datum[0];
        let raw_datum = &src_datum[1..];
        match flag {
            datum::NIL_FLAG => self.applightlike_null(),
            // In both index and record, it's flag is `JSON`. See MilevaDB's `encode()`.
            datum::JSON_FLAG => {
                self.data.write_json_to_Soliton_by_datum_payload(raw_datum)?;
                self.finished_applightlike_var();
            }
            _ => {
                return Err(Error::InvalidDataType(format!(
                    "Unsupported datum flag {} for Json vector",
                    flag
                )))
            }
        }
        Ok(())
    }

    /// Get the json datum of the Evcausetidx in the PrimaryCauset.
    #[inline]
    pub fn get_json(&self, idx: usize) -> Result<Json> {
        let spacelike = self.var_offsets[idx];
        let lightlike = self.var_offsets[idx + 1];
        let mut data = &self.data[spacelike..lightlike];
        data.read_json()
    }

    /// Return the total events in the PrimaryCauset.
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[causet(test)]
    pub fn decode(buf: &mut violetabftstore::interlock::::codec::BytesSlice<'_>, tp: FieldTypeTp) -> Result<PrimaryCauset> {
        let length = buf.read_u32_le()? as usize;
        let mut col = PrimaryCauset::new(tp, length);
        col.length = length;
        col.null_cnt = buf.read_u32_le()? as usize;
        let null_length = (col.length + 7) / 8 as usize;
        if col.null_cnt > 0 {
            col.null_bitmap = buf.read_bytes(null_length)?.to_vec();
        } else {
            col.null_bitmap = vec![0xFF; null_length];
        }

        let data_length = if col.is_fixed() {
            col.fixed_len * col.length
        } else {
            col.var_offsets.clear();
            for _ in 0..=length {
                col.var_offsets.push(buf.read_i64_le()? as usize);
            }
            col.var_offsets[col.length]
        };
        col.data = buf.read_bytes(data_length)?.to_vec();
        Ok(col)
    }
}

/// `SolitonPrimaryCausetEncoder` encodes the Soliton PrimaryCauset.
pub trait SolitonPrimaryCausetEncoder: NumberEncoder {
    fn write_Soliton_PrimaryCauset(&mut self, col: &PrimaryCauset) -> Result<()> {
        // length
        self.write_u32_le(col.length as u32)?;
        // null_cnt
        self.write_u32_le(col.null_cnt as u32)?;
        // bitmap
        if col.null_cnt > 0 {
            let length = (col.length + 7) / 8;
            self.write_bytes(&col.null_bitmap[0..length])?;
        }
        // offsets
        if !col.is_fixed() {
            //let length = (col.length+1)*4;
            for v in &col.var_offsets {
                self.write_i64_le(*v as i64)?;
            }
        }
        // data
        self.write_bytes(&col.data)?;
        Ok(())
    }
}

impl<T: BufferWriter> SolitonPrimaryCausetEncoder for T {}

#[causet(test)]
mod tests {
    use super::*;
    use crate::codec::datum::Datum;
    use std::{f64, u64};
    use fidel_timeshare::FieldType;

    #[test]
    fn test_PrimaryCauset_i64() {
        let fields: Vec<FieldType> = vec![
            FieldTypeTp::Tiny.into(),
            FieldTypeTp::Short.into(),
            FieldTypeTp::Int24.into(),
            FieldTypeTp::Long.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Year.into(),
        ];
        let data = vec![
            Datum::Null,
            Datum::I64(-1),
            Datum::I64(12),
            Datum::I64(1024),
        ];
        for field in &fields {
            let mut PrimaryCauset = PrimaryCauset::new(field.as_accessor().tp(), data.len());
            for v in &data {
                PrimaryCauset.applightlike_datum(v).unwrap();
            }

            for (id, expect) in data.iter().enumerate() {
                let get = PrimaryCauset.get_datum(id, field).unwrap();
                assert_eq!(&get, expect);
            }
        }
    }

    #[test]
    fn test_PrimaryCauset_u64() {
        let mut fields: Vec<FieldType> = vec![
            FieldTypeTp::Tiny.into(),
            FieldTypeTp::Short.into(),
            FieldTypeTp::Int24.into(),
            FieldTypeTp::Long.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Year.into(),
        ];
        for field in &mut fields {
            field.set_flag(FieldTypeFlag::UNSIGNED.bits());
        }
        let data = vec![
            Datum::Null,
            Datum::U64(1),
            Datum::U64(u64::MIN),
            Datum::U64(u64::MAX),
        ];
        test_colum_datum(fields, data);
    }

    fn test_colum_datum(fields: Vec<FieldType>, data: Vec<Datum>) {
        for field in &fields {
            let mut PrimaryCauset = PrimaryCauset::new(field.as_accessor().tp(), data.len());
            for v in &data {
                PrimaryCauset.applightlike_datum(v).unwrap();
            }
            for (id, expect) in data.iter().enumerate() {
                let get = PrimaryCauset.get_datum(id, field).unwrap();
                assert_eq!(&get, expect);
            }
        }
    }

    #[test]
    fn test_PrimaryCauset_f64() {
        let fields: Vec<FieldType> = vec![FieldTypeTp::Double.into()];
        let data = vec![Datum::Null, Datum::F64(f64::MIN), Datum::F64(f64::MAX)];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_PrimaryCauset_f32() {
        let fields: Vec<FieldType> = vec![FieldTypeTp::Float.into()];
        let data = vec![
            Datum::Null,
            Datum::F64(std::f32::MIN.into()),
            Datum::F64(std::f32::MAX.into()),
        ];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_PrimaryCauset_time() {
        let mut ctx = EvalContext::default();
        let fields: Vec<FieldType> = vec![
            FieldTypeTp::Date.into(),
            FieldTypeTp::DateTime.into(),
            FieldTypeTp::Timestamp.into(),
        ];
        let time = Time::parse_datetime(&mut ctx, "2012-12-31 11:30:45", -1, true).unwrap();
        let data = vec![Datum::Null, Datum::Time(time)];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_PrimaryCauset_duration() {
        let fields: Vec<FieldType> = vec![FieldTypeTp::Duration.into()];
        let duration = Duration::parse(&mut EvalContext::default(), b"10:11:12", 0).unwrap();
        let data = vec![Datum::Null, Datum::Dur(duration)];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_PrimaryCauset_decimal() {
        let fields: Vec<FieldType> = vec![FieldTypeTp::NewDecimal.into()];
        let dec: Decimal = "1234.00".parse().unwrap();
        let data = vec![Datum::Null, Datum::Dec(dec)];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_PrimaryCauset_json() {
        let fields: Vec<FieldType> = vec![FieldTypeTp::JSON.into()];
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();

        let data = vec![Datum::Null, Datum::Json(json)];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_PrimaryCauset_bytes() {
        let fields: Vec<FieldType> = vec![
            FieldTypeTp::VarChar.into(),
            FieldTypeTp::VarString.into(),
            FieldTypeTp::String.into(),
            FieldTypeTp::Blob.into(),
            FieldTypeTp::TinyBlob.into(),
            FieldTypeTp::MediumBlob.into(),
            FieldTypeTp::LongBlob.into(),
        ];
        let data = vec![Datum::Null, Datum::Bytes(b"xxx".to_vec())];
        test_colum_datum(fields, data);
    }
}
