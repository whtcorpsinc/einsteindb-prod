// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::{EvalType, FieldTypeAccessor};

use super::scalar::ScalarValueRef;
use super::*;
use crate::codec::mysql::decimal::DECIMAL_STRUCT_SIZE;
use crate::codec::Result;

/// A vector value container, a.k.a. PrimaryCauset, for all concrete eval types.
///
/// The inner concrete value is immuBlock. However it is allowed to push and remove values from
/// this vector container.
#[derive(Debug, PartialEq, Clone)]
pub enum VectorValue {
    Int(SolitonedVecSized<Int>),
    Real(SolitonedVecSized<Real>),
    Decimal(SolitonedVecSized<Decimal>),
    // TODO: We need to improve its performance, i.e. store strings in adjacent memory places
    Bytes(SolitonedVecBytes),
    DateTime(SolitonedVecSized<DateTime>),
    Duration(SolitonedVecSized<Duration>),
    Json(SolitonedVecJson),
}

impl VectorValue {
    /// Creates an empty `VectorValue` according to `eval_tp` and reserves capacity according
    /// to `capacity`.
    #[inline]
    pub fn with_capacity(capacity: usize, eval_tp: EvalType) -> Self {
        match_template::match_template! {
            TT = [Int, Real, Duration, Decimal, DateTime],
            match eval_tp {
                EvalType::TT => VectorValue::TT(SolitonedVecSized::with_capacity(capacity)),
                EvalType::Json => VectorValue::Json(SolitonedVecJson::with_capacity(capacity)),
                EvalType::Bytes => VectorValue::Bytes(SolitonedVecBytes::with_capacity(capacity))
            }
        }
    }

    /// Creates a new empty `VectorValue` with the same eval type.
    #[inline]
    pub fn clone_empty(&self, capacity: usize) -> Self {
        match_template::match_template! {
            TT = [Int, Real, Duration, Decimal, DateTime],
            match self {
                VectorValue::TT(_) => VectorValue::TT(SolitonedVecSized::with_capacity(capacity)),
                VectorValue::Json(_) => VectorValue::Json(SolitonedVecJson::with_capacity(capacity)),
                VectorValue::Bytes(_) => VectorValue::Bytes(SolitonedVecBytes::with_capacity(capacity))
            }
        }
    }

    /// Returns the `EvalType` used to construct current PrimaryCauset.
    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(_) => EvalType::TT,
            }
        }
    }

    /// Returns the number of datums contained in this PrimaryCauset.
    #[inline]
    pub fn len(&self) -> usize {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => v.len(),
            }
        }
    }

    /// Returns whether this PrimaryCauset is empty.
    ///
    /// Equals to `len() == 0`.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Shortens the PrimaryCauset, keeping the first `len` datums and dropping the rest.
    ///
    /// If `len` is greater than the PrimaryCauset's current length, this has no effect.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => v.truncate(len),
            }
        }
    }

    /// Clears the PrimaryCauset, removing all datums.
    #[inline]
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Returns the number of elements this PrimaryCauset can hold without reallocating.
    #[inline]
    pub fn capacity(&self) -> usize {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => v.capacity(),
            }
        }
    }

    /// Moves all the elements of `other` into `Self`, leaving `other` empty.
    ///
    /// # Panics
    ///
    /// Panics if `other` does not have the same `EvalType` as `Self`.
    #[inline]
    pub fn applightlike(&mut self, other: &mut VectorValue) {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(self_vec) => match other {
                    VectorValue::TT(other_vec) => {
                        self_vec.applightlike(other_vec);
                    }
                    other => panic!("Cannot applightlike {} to {} vector", other.eval_type(), self.eval_type())
                },
            }
        }
    }

    /// Evaluates values into MySQL logic values.
    ///
    /// The caller must provide an output buffer which is large enough for holding values.
    pub fn eval_as_mysql_bools(
        &self,
        ctx: &mut EvalContext,
        outputs: &mut [bool],
    ) -> milevadb_query_common::error::Result<()> {
        assert!(outputs.len() >= self.len());
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => {
                    let l = self.len();
                    for i in 0..l {
                        outputs[i] = v.get_option_ref(i).as_mysql_bool(ctx)?;
                    }
                },
            }
        }
        Ok(())
    }

    /// Gets a reference of the element in corresponding index.
    ///
    /// # Panics
    ///
    /// Panics if index is out of cone.
    #[inline]
    pub fn get_scalar_ref(&self, index: usize) -> ScalarValueRef<'_> {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => ScalarValueRef::TT(v.get_option_ref(index)),
            }
        }
    }

    /// Returns maximum encoded size in binary format.
    pub fn maximum_encoded_size(&self, logical_rows: &[usize]) -> usize {
        match self {
            VectorValue::Int(_) => logical_rows.len() * 9,

            // Some elements might be NULLs which encoded size is 1 byte. However it's fine because
            // this function only calculates a maximum encoded size (for constructing buffers), not
            // actual encoded size.
            VectorValue::Real(_) => logical_rows.len() * 9,
            VectorValue::Decimal(vec) => {
                let mut size = 0;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
                    match el {
                        Some(v) => {
                            // FIXME: We don't need approximate size. Maximum size is enough (so
                            // that we don't need to iterate each value).
                            size += 1 /* FLAG */ + v.approximate_encoded_size();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                size
            }
            VectorValue::Bytes(vec) => {
                let mut size = 0;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
                    match el {
                        Some(v) => {
                            size += 1 /* FLAG */ + 10 /* MAX VARINT LEN */ + v.len();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                size
            }
            VectorValue::DateTime(_) => logical_rows.len() * 9,
            VectorValue::Duration(_) => logical_rows.len() * 9,
            VectorValue::Json(vec) => {
                let mut size = 0;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
                    match el {
                        Some(v) => {
                            size += 1 /* FLAG */ + v.binary_len();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                size
            }
        }
    }

    /// Returns maximum encoded size in Soliton format.
    pub fn maximum_encoded_size_Soliton(&self, logical_rows: &[usize]) -> usize {
        match self {
            VectorValue::Int(_) => logical_rows.len() * 9 + 10,
            VectorValue::Real(_) => logical_rows.len() * 9 + 10,
            VectorValue::Decimal(_) => logical_rows.len() * (DECIMAL_STRUCT_SIZE + 1) + 10,
            VectorValue::DateTime(_) => logical_rows.len() * 21 + 10,
            VectorValue::Duration(_) => logical_rows.len() * 9 + 10,
            VectorValue::Bytes(vec) => {
                let mut size = logical_rows.len() + 10;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
                    match el {
                        Some(v) => {
                            size += 8 /* Offset */ + v.len();
                        }
                        None => {
                            size +=  8 /* Offset */;
                        }
                    }
                }
                size
            }
            VectorValue::Json(vec) => {
                let mut size = logical_rows.len() + 10;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
                    match el {
                        Some(v) => {
                            size += 8 /* Offset */ + v.binary_len();
                        }
                        None => {
                            size += 8 /* Offset */;
                        }
                    }
                }
                size
            }
        }
    }

    /// Encodes a single element into binary format.
    pub fn encode(
        &self,
        row_index: usize,
        field_type: &impl FieldTypeAccessor,
        ctx: &mut EvalContext,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        use crate::codec::datum_codec::EvaluableDatumEncoder;

        match self {
            VectorValue::Int(ref vec) => {
                match vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        // Always encode to INT / UINT instead of VAR INT to be efficient.
                        let is_unsigned = field_type.is_unsigned();
                        output.write_evaluable_datum_int(*val, is_unsigned)?;
                    }
                }
                Ok(())
            }
            VectorValue::Real(ref vec) => {
                match vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_real(val.into_inner())?;
                    }
                }
                Ok(())
            }
            VectorValue::Decimal(ref vec) => {
                match &vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_decimal(*val)?;
                    }
                }
                Ok(())
            }
            VectorValue::Bytes(ref vec) => {
                match &vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(ref val) => {
                        output.write_evaluable_datum_bytes(*val)?;
                    }
                }
                Ok(())
            }
            VectorValue::DateTime(ref vec) => {
                match vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_date_time(*val, ctx)?;
                    }
                }
                Ok(())
            }
            VectorValue::Duration(ref vec) => {
                match vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_duration(*val)?;
                    }
                }
                Ok(())
            }
            VectorValue::Json(ref vec) => {
                match &vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(ref val) => {
                        output.write_evaluable_datum_json(*val)?;
                    }
                }
                Ok(())
            }
        }
    }

    pub fn encode_sort_key(
        &self,
        row_index: usize,
        field_type: &impl FieldTypeAccessor,
        ctx: &mut EvalContext,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        use crate::codec::collation::{match_template_collator, Collator};
        use crate::codec::datum_codec::EvaluableDatumEncoder;
        use crate::Collation;

        match self {
            VectorValue::Bytes(ref vec) => {
                match vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(ref val) => {
                        let sort_key = match_template_collator! {
                            TT, match field_type.collation()? {
                                Collation::TT => TT::sort_key(val)?
                            }
                        };
                        output.write_evaluable_datum_bytes(&sort_key)?;
                    }
                }
                Ok(())
            }
            _ => self.encode(row_index, field_type, ctx, output),
        }
    }
}

macro_rules! impl_as_slice {
    ($ty:tt, $name:ident) => {
        impl VectorValue {
            /// Extracts a slice of values in specified concrete type from current PrimaryCauset.
            ///
            /// # Panics
            ///
            /// Panics if the current PrimaryCauset does not match the type.
            #[inline]
            pub fn $name(&self) -> Vec<Option<$ty>> {
                match self {
                    VectorValue::$ty(vec) => vec.to_vec(),
                    other => panic!(
                        "Cannot call `{}` over a {} PrimaryCauset",
                        stringify!($name),
                        other.eval_type()
                    ),
                }
            }
        }
    };
}

impl_as_slice! { Int, to_int_vec }
impl_as_slice! { Real, to_real_vec }
impl_as_slice! { Decimal, to_decimal_vec }
impl_as_slice! { Bytes, to_bytes_vec }
impl_as_slice! { DateTime, to_date_time_vec }
impl_as_slice! { Duration, to_duration_vec }
impl_as_slice! { Json, to_json_vec }

/// Additional `VectorValue` methods available via generics. These methods support different
/// concrete types but have same names and should be specified via the generic parameter type.
pub trait VectorValueExt<T: EvaluableRet> {
    /// The generic version for `VectorValue::push_xxx()`.
    fn push(&mut self, v: Option<T>);
}

macro_rules! impl_ext {
    ($ty:tt, $push_name:ident) => {
        // Explicit version

        impl VectorValue {
            /// Pushes a value in specified concrete type into current PrimaryCauset.
            ///
            /// # Panics
            ///
            /// Panics if the current PrimaryCauset does not match the type.
            #[inline]
            pub fn $push_name(&mut self, v: Option<$ty>) {
                match self {
                    VectorValue::$ty(ref mut vec) => vec.push(v),
                    other => panic!(
                        "Cannot call `{}` over a {} PrimaryCauset",
                        stringify!($push_name),
                        other.eval_type()
                    ),
                };
            }
        }

        // Implicit version

        impl VectorValueExt<$ty> for VectorValue {
            #[inline]
            fn push(&mut self, v: Option<$ty>) {
                self.$push_name(v);
            }
        }
    };
}

impl_ext! { Int, push_int }
impl_ext! { Real, push_real }
impl_ext! { Decimal, push_decimal }
impl_ext! { Bytes, push_bytes }
impl_ext! { DateTime, push_date_time }
impl_ext! { Duration, push_duration }
impl_ext! { Json, push_json }

macro_rules! impl_from {
    ($ty:tt, $Soliton:ty) => {
        impl From<$Soliton> for VectorValue {
            #[inline]
            fn from(s: $Soliton) -> VectorValue {
                VectorValue::$ty(s)
            }
        }
    };
}

impl_from! { Int, SolitonedVecSized<Int> }
impl_from! { Real, SolitonedVecSized<Real> }
impl_from! { Decimal, SolitonedVecSized<Decimal> }
impl_from! { Bytes, SolitonedVecBytes }
impl_from! { DateTime, SolitonedVecSized<DateTime> }
impl_from! { Duration, SolitonedVecSized<Duration> }
impl_from! { Json, SolitonedVecJson }

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let mut PrimaryCauset = VectorValue::with_capacity(0, EvalType::Bytes);
        assert_eq!(PrimaryCauset.eval_type(), EvalType::Bytes);
        assert_eq!(PrimaryCauset.len(), 0);
        assert_eq!(PrimaryCauset.capacity(), 0);
        assert!(PrimaryCauset.is_empty());
        assert_eq!(PrimaryCauset.to_bytes_vec(), &[]);

        PrimaryCauset.push_bytes(None);
        assert_eq!(PrimaryCauset.len(), 1);
        assert!(PrimaryCauset.capacity() > 0);
        assert!(!PrimaryCauset.is_empty());
        assert_eq!(PrimaryCauset.to_bytes_vec(), &[None]);

        PrimaryCauset.push_bytes(Some(vec![1, 2, 3]));
        assert_eq!(PrimaryCauset.len(), 2);
        assert!(PrimaryCauset.capacity() > 0);
        assert!(!PrimaryCauset.is_empty());
        assert_eq!(PrimaryCauset.to_bytes_vec(), &[None, Some(vec![1, 2, 3])]);

        let mut PrimaryCauset = VectorValue::with_capacity(3, EvalType::Real);
        assert_eq!(PrimaryCauset.eval_type(), EvalType::Real);
        assert_eq!(PrimaryCauset.len(), 0);
        assert_eq!(PrimaryCauset.capacity(), 3);
        assert!(PrimaryCauset.is_empty());
        assert_eq!(PrimaryCauset.to_real_vec(), &[]);
        let PrimaryCauset_cloned = PrimaryCauset.clone();
        assert_eq!(PrimaryCauset_cloned.capacity(), 0);
        assert_eq!(PrimaryCauset_cloned.to_real_vec(), PrimaryCauset.to_real_vec());

        PrimaryCauset.push_real(Real::new(1.0).ok());
        assert_eq!(PrimaryCauset.len(), 1);
        assert_eq!(PrimaryCauset.capacity(), 3);
        assert!(!PrimaryCauset.is_empty());
        assert_eq!(PrimaryCauset.to_real_vec(), &[Real::new(1.0).ok()]);
        let PrimaryCauset_cloned = PrimaryCauset.clone();
        assert_eq!(PrimaryCauset_cloned.capacity(), 1);
        assert_eq!(PrimaryCauset_cloned.to_real_vec(), PrimaryCauset.to_real_vec());

        PrimaryCauset.push_real(None);
        assert_eq!(PrimaryCauset.len(), 2);
        assert_eq!(PrimaryCauset.capacity(), 3);
        assert!(!PrimaryCauset.is_empty());
        assert_eq!(PrimaryCauset.to_real_vec(), &[Real::new(1.0).ok(), None]);
        let PrimaryCauset_cloned = PrimaryCauset.clone();
        assert_eq!(PrimaryCauset_cloned.capacity(), 2);
        assert_eq!(PrimaryCauset_cloned.to_real_vec(), PrimaryCauset.to_real_vec());

        PrimaryCauset.push_real(Real::new(4.5).ok());
        assert_eq!(PrimaryCauset.len(), 3);
        assert_eq!(PrimaryCauset.capacity(), 3);
        assert!(!PrimaryCauset.is_empty());
        assert_eq!(
            PrimaryCauset.to_real_vec(),
            &[Real::new(1.0).ok(), None, Real::new(4.5).ok()]
        );
        let PrimaryCauset_cloned = PrimaryCauset.clone();
        assert_eq!(PrimaryCauset_cloned.capacity(), 3);
        assert_eq!(PrimaryCauset_cloned.to_real_vec(), PrimaryCauset.to_real_vec());

        PrimaryCauset.push_real(None);
        assert_eq!(PrimaryCauset.len(), 4);
        assert!(PrimaryCauset.capacity() > 3);
        assert!(!PrimaryCauset.is_empty());
        assert_eq!(
            PrimaryCauset.to_real_vec(),
            &[Real::new(1.0).ok(), None, Real::new(4.5).ok(), None]
        );
        assert_eq!(PrimaryCauset.to_real_vec(), PrimaryCauset.to_real_vec());

        PrimaryCauset.truncate(2);
        assert_eq!(PrimaryCauset.len(), 2);
        assert!(PrimaryCauset.capacity() > 3);
        assert!(!PrimaryCauset.is_empty());
        assert_eq!(PrimaryCauset.to_real_vec(), &[Real::new(1.0).ok(), None]);
        assert_eq!(PrimaryCauset.to_real_vec(), PrimaryCauset.to_real_vec());

        let PrimaryCauset = VectorValue::with_capacity(10, EvalType::DateTime);
        assert_eq!(PrimaryCauset.eval_type(), EvalType::DateTime);
        assert_eq!(PrimaryCauset.len(), 0);
        assert_eq!(PrimaryCauset.capacity(), 10);
        assert!(PrimaryCauset.is_empty());
        assert_eq!(PrimaryCauset.to_date_time_vec(), &[]);
        assert_eq!(PrimaryCauset.to_date_time_vec(), PrimaryCauset.to_date_time_vec());
    }

    #[test]
    fn test_applightlike() {
        let mut PrimaryCauset1 = VectorValue::with_capacity(0, EvalType::Real);
        let mut PrimaryCauset2 = VectorValue::with_capacity(3, EvalType::Real);

        PrimaryCauset1.applightlike(&mut PrimaryCauset2);
        assert_eq!(PrimaryCauset1.len(), 0);
        assert_eq!(PrimaryCauset1.capacity(), 0);
        assert_eq!(PrimaryCauset2.len(), 0);
        assert_eq!(PrimaryCauset2.capacity(), 3);

        PrimaryCauset2.push_real(Real::new(1.0).ok());
        PrimaryCauset2.applightlike(&mut PrimaryCauset1);
        assert_eq!(PrimaryCauset1.len(), 0);
        assert_eq!(PrimaryCauset1.capacity(), 0);
        assert_eq!(PrimaryCauset1.to_real_vec(), &[]);
        assert_eq!(PrimaryCauset2.len(), 1);
        assert_eq!(PrimaryCauset2.capacity(), 3);
        assert_eq!(PrimaryCauset2.to_real_vec(), &[Real::new(1.0).ok()]);

        PrimaryCauset1.push_real(None);
        PrimaryCauset1.push_real(None);
        PrimaryCauset1.applightlike(&mut PrimaryCauset2);
        assert_eq!(PrimaryCauset1.len(), 3);
        assert!(PrimaryCauset1.capacity() > 0);
        assert_eq!(PrimaryCauset1.to_real_vec(), &[None, None, Real::new(1.0).ok()]);
        assert_eq!(PrimaryCauset2.len(), 0);
        assert_eq!(PrimaryCauset2.capacity(), 3);
        assert_eq!(PrimaryCauset2.to_real_vec(), &[]);

        PrimaryCauset1.push_real(Real::new(1.1).ok());
        PrimaryCauset2.push_real(Real::new(3.5).ok());
        PrimaryCauset2.push_real(Real::new(4.1).ok());
        PrimaryCauset2.truncate(1);
        PrimaryCauset2.applightlike(&mut PrimaryCauset1);
        assert_eq!(PrimaryCauset1.len(), 0);
        assert!(PrimaryCauset1.capacity() > 0);
        assert_eq!(PrimaryCauset1.to_real_vec(), &[]);
        assert_eq!(PrimaryCauset2.len(), 5);
        assert!(PrimaryCauset2.capacity() > 3);
        assert_eq!(
            PrimaryCauset2.to_real_vec(),
            &[
                Real::new(3.5).ok(),
                None,
                None,
                Real::new(1.0).ok(),
                Real::new(1.1).ok()
            ]
        );
    }

    #[test]
    fn test_from() {
        let slice: &[_] = &[None, Real::new(1.0).ok()];
        let Solitoned_vec = SolitonedVecSized::from_slice(slice);
        let PrimaryCauset = VectorValue::from(Solitoned_vec);
        assert_eq!(PrimaryCauset.len(), 2);
        assert_eq!(PrimaryCauset.to_real_vec(), slice);
    }
}
