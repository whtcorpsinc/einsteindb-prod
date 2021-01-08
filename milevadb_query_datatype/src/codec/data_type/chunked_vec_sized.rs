// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use super::bit_vec::BitVec;
use super::{SolitonRef, SolitonedVec, Evaluable, EvaluableRet, UnsafeRefInto};
use crate::impl_Solitoned_vec_common;

/// A vector storing `Option<T>` with a compact layout.
///
/// `T` must be a primitive structure. All data must be stored
/// in that structure itself. This includes `Int`, `Real`, `Decimal`,
/// `DateTime` and `Duration` in copr framework.
///
/// Inside `SolitonedVecSized`, `bitmap` indicates if an element at given index is null,
/// and `data` stores actual data. If the element at given index is null (or `None`),
/// the corresponding `bitmap` bit is false, and `data` stores zero value for
/// that element. Otherwise, `data` stores actual data, and `bitmap` bit is true.
#[derive(Debug, PartialEq, Clone)]
pub struct SolitonedVecSized<T: Sized> {
    data: Vec<T>,
    bitmap: BitVec,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Sized + Clone> SolitonedVecSized<T> {
    impl_Solitoned_vec_common! { T }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: BitVec::with_capacity(capacity),
            phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn push_data(&mut self, value: T) {
        self.bitmap.push(true);
        self.data.push(value);
    }

    #[inline]
    pub fn push_null(&mut self) {
        self.bitmap.push(false);
        self.data.push(unsafe { std::mem::zeroed() });
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len);
        self.bitmap.truncate(len);
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn applightlike(&mut self, other: &mut Self) {
        self.data.applightlike(&mut other.data);
        self.bitmap.applightlike(&mut other.bitmap);
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<&T> {
        assert!(idx < self.data.len());
        if self.bitmap.get(idx) {
            Some(&self.data[idx])
        } else {
            None
        }
    }

    pub fn to_vec(&self) -> Vec<Option<T>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).cloned());
        }
        x
    }
}

impl<T: Clone> SolitonedVec<T> for SolitonedVecSized<T> {
    fn Solitoned_with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    #[inline]
    fn Solitoned_push(&mut self, value: Option<T>) {
        self.push(value)
    }
}

impl<'a, T: Evaluable + EvaluableRet> SolitonRef<'a, &'a T> for &'a SolitonedVecSized<T> {
    #[inline]
    fn get_option_ref(self, idx: usize) -> Option<&'a T> {
        self.get(idx)
    }

    fn get_bit_vec(self) -> &'a BitVec {
        &self.bitmap
    }

    #[inline]
    fn phantom_data(self) -> Option<&'a T> {
        None
    }
}

impl<T: Clone> Into<SolitonedVecSized<T>> for Vec<Option<T>> {
    fn into(self) -> SolitonedVecSized<T> {
        SolitonedVecSized::from_vec(self)
    }
}

impl<'a, T: Evaluable> UnsafeRefInto<&'static SolitonedVecSized<T>> for &'a SolitonedVecSized<T> {
    unsafe fn unsafe_into(self) -> &'static SolitonedVecSized<T> {
        std::mem::transmute(self)
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use crate::codec::data_type::*;

    #[test]
    fn test_slice_vec() {
        let test_decimal: &[Option<Decimal>] = &[
            Decimal::from_f64(1.233).ok(),
            Decimal::from_f64(2.233).ok(),
            Decimal::from_f64(3.233).ok(),
            Decimal::from_f64(4.233).ok(),
            Decimal::from_f64(5.233).ok(),
            None,
        ];
        assert_eq!(
            SolitonedVecSized::<Decimal>::from_slice(test_decimal).to_vec(),
            test_decimal
        );
        assert_eq!(
            SolitonedVecSized::<Decimal>::from_vec(test_decimal.to_vec()).to_vec(),
            test_decimal
        );
        let test_real: &[Option<Real>] = &[
            Real::new(1.01001).ok(),
            Real::new(-0.01).ok(),
            Real::new(1.02001).ok(),
            Real::new(std::f64::MIN).ok(),
            Real::new(std::f64::MAX).ok(),
            None,
        ];
        assert_eq!(
            SolitonedVecSized::<Real>::from_slice(test_real).to_vec(),
            test_real
        );
        assert_eq!(
            SolitonedVecSized::<Real>::from_vec(test_real.to_vec()).to_vec(),
            test_real
        );
        let mut ctx = EvalContext::default();
        let test_duration: &[Option<Duration>] = &[
            Duration::parse(&mut ctx, b"17:51:04.78", 2).ok(),
            Duration::parse(&mut ctx, b"-17:51:04.78", 2).ok(),
            Duration::parse(&mut ctx, b"17:51:04.78", 0).ok(),
            Duration::parse(&mut ctx, b"-17:51:04.78", 0).ok(),
            None,
        ];
        assert_eq!(
            SolitonedVecSized::<Duration>::from_slice(test_duration).to_vec(),
            test_duration
        );
        assert_eq!(
            SolitonedVecSized::<Duration>::from_vec(test_duration.to_vec()).to_vec(),
            test_duration
        );
        let test_datetime: &[Option<DateTime>] = &[
            DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:00", 0, false).ok(),
            DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:01", 0, false).ok(),
            DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:02", 0, false).ok(),
        ];
        assert_eq!(
            SolitonedVecSized::<DateTime>::from_slice(test_datetime).to_vec(),
            test_datetime
        );
        assert_eq!(
            SolitonedVecSized::<DateTime>::from_vec(test_datetime.to_vec()).to_vec(),
            test_datetime
        );
        let test_int: &[Option<Int>] =
            &[Some(1), Some(1), Some(233), Some(2333), Some(23333), None];
        assert_eq!(
            SolitonedVecSized::<Int>::from_slice(test_int).to_vec(),
            test_int
        );
        assert_eq!(
            SolitonedVecSized::<Int>::from_vec(test_int.to_vec()).to_vec(),
            test_int
        );
    }

    #[test]
    fn test_basics() {
        let mut x: SolitonedVecSized<Int> = SolitonedVecSized::with_capacity(0);
        x.push(Some(1));
        x.push(Some(2));
        x.push(Some(3));
        x.push(None);
        assert_eq!(x.get(0), Some(&1));
        assert_eq!(x.get(1), Some(&2));
        assert_eq!(x.get(2), Some(&3));
        assert_eq!(x.get(3), None);
        assert_eq!(x.len(), 4);
        assert!(!x.is_empty());
    }

    #[test]
    fn test_truncate() {
        let test_real: &[Option<Real>] = &[
            None,
            Real::new(1.01001).ok(),
            Real::new(-0.01).ok(),
            Real::new(1.02001).ok(),
            Real::new(std::f64::MIN).ok(),
            Real::new(std::f64::MAX).ok(),
            None,
        ];
        let mut Solitoned_vec = SolitonedVecSized::<Real>::from_slice(test_real);
        Solitoned_vec.truncate(100);
        assert_eq!(Solitoned_vec.len(), 7);
        Solitoned_vec.truncate(3);
        assert_eq!(Solitoned_vec.len(), 3);
        assert_eq!(Solitoned_vec.get(0), None);
        assert_eq!(Solitoned_vec.get(1), Real::new(1.01001).ok().as_ref());
        assert_eq!(Solitoned_vec.get(2), Real::new(-0.01).ok().as_ref());
        Solitoned_vec.truncate(0);
        assert_eq!(Solitoned_vec.len(), 0);
    }

    #[test]
    fn test_applightlike() {
        let test_real_1: &[Option<Real>] = &[None, Real::new(1.01001).ok(), Real::new(-0.01).ok()];
        let test_real_2: &[Option<Real>] = &[
            Real::new(1.02001).ok(),
            Real::new(std::f64::MIN).ok(),
            Real::new(std::f64::MAX).ok(),
            None,
        ];
        let mut Solitoned_vec_1 = SolitonedVecSized::<Real>::from_slice(test_real_1);
        let mut Solitoned_vec_2 = SolitonedVecSized::<Real>::from_slice(test_real_2);
        Solitoned_vec_1.applightlike(&mut Solitoned_vec_2);
        assert_eq!(Solitoned_vec_1.len(), 7);
        assert!(Solitoned_vec_2.is_empty());
        assert_eq!(
            Solitoned_vec_1.to_vec(),
            &[
                None,
                Real::new(1.01001).ok(),
                Real::new(-0.01).ok(),
                Real::new(1.02001).ok(),
                Real::new(std::f64::MIN).ok(),
                Real::new(std::f64::MAX).ok(),
                None,
            ]
        );
    }
}

#[causet(test)]
mod benches {
    use super::*;

    #[bench]
    fn bench_applightlike(b: &mut test::Bencher) {
        b.iter(|| {
            let mut Solitoned_vec_int = SolitonedVecSized::with_capacity(10000);
            for _i in 0..5000 {
                Solitoned_vec_int.push(Some(233));
                Solitoned_vec_int.push(None);
            }
        });
    }

    #[bench]
    fn bench_iterate(b: &mut test::Bencher) {
        let mut Solitoned_vec_int = SolitonedVecSized::with_capacity(10000);
        for _i in 0..5000 {
            Solitoned_vec_int.push(Some(233));
            Solitoned_vec_int.push(None);
        }
        b.iter(|| {
            let mut sum = 0;
            for i in 0..10000 {
                if let Some(x) = Solitoned_vec_int.get(i) {
                    sum += *x
                }
            }
            sum
        });
    }
}
