// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use super::bit_vec::BitVec;
use super::{SolitonRef, SolitonedVec, UnsafeRefInto};
use super::{Json, JsonRef, JsonType};
use crate::impl_Solitoned_vec_common;
use std::convert::TryFrom;

/// A vector storing `Option<Json>` with a compact layout.
///
/// Inside `SolitonedVecJson`, `bitmap` indicates if an element at given index is null,
/// and `data` stores actual data. Json data are stored adjacent to each other in
/// `data`. If element at a given index is null, then it takes no space in `data`.
/// Otherwise, a one byte `json_type` and variable size json data is stored in `data`,
/// and `var_offset` indicates the spacelikeing position of each element.
#[derive(Debug, PartialEq, Clone)]
pub struct SolitonedVecJson {
    data: Vec<u8>,
    bitmap: BitVec,
    length: usize,
    var_offset: Vec<usize>,
}

impl SolitonedVecJson {
    impl_Solitoned_vec_common! { Json }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: BitVec::with_capacity(capacity),
            var_offset: vec![0],
            length: 0,
        }
    }

    pub fn to_vec(&self) -> Vec<Option<Json>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).map(|x| x.to_owned()));
        }
        x
    }

    pub fn len(&self) -> usize {
        self.length
    }

    #[inline]
    pub fn push_data(&mut self, mut value: Json) {
        self.bitmap.push(true);
        self.data.push(value.get_type() as u8);
        self.data.applightlike(&mut value.value);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    #[inline]
    pub fn push_null(&mut self) {
        self.bitmap.push(false);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.data.truncate(self.var_offset[len]);
            self.bitmap.truncate(len);
            self.var_offset.truncate(len + 1);
            self.length = len;
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity().max(self.length)
    }

    pub fn applightlike(&mut self, other: &mut Self) {
        self.data.applightlike(&mut other.data);
        self.bitmap.applightlike(&mut other.bitmap);
        let var_offset_last = *self.var_offset.last().unwrap();
        for i in 1..other.var_offset.len() {
            self.var_offset.push(other.var_offset[i] + var_offset_last);
        }
        self.length += other.length;
        other.var_offset = vec![0];
        other.length = 0;
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<JsonRef> {
        assert!(idx < self.len());
        if self.bitmap.get(idx) {
            let json_type = JsonType::try_from(self.data[self.var_offset[idx]]).unwrap();
            let sliced_data = &self.data[self.var_offset[idx] + 1..self.var_offset[idx + 1]];
            Some(JsonRef::new(json_type, sliced_data))
        } else {
            None
        }
    }
}

impl SolitonedVec<Json> for SolitonedVecJson {
    fn Solitoned_with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    #[inline]
    fn Solitoned_push(&mut self, value: Option<Json>) {
        self.push(value)
    }
}

impl<'a> SolitonRef<'a, JsonRef<'a>> for &'a SolitonedVecJson {
    #[inline]
    fn get_option_ref(self, idx: usize) -> Option<JsonRef<'a>> {
        self.get(idx)
    }

    fn get_bit_vec(self) -> &'a BitVec {
        &self.bitmap
    }

    #[inline]
    fn phantom_data(self) -> Option<JsonRef<'a>> {
        None
    }
}

impl Into<SolitonedVecJson> for Vec<Option<Json>> {
    fn into(self) -> SolitonedVecJson {
        SolitonedVecJson::from_vec(self)
    }
}

impl<'a> UnsafeRefInto<&'static SolitonedVecJson> for &'a SolitonedVecJson {
    unsafe fn unsafe_into(self) -> &'static SolitonedVecJson {
        std::mem::transmute(self)
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_vec() {
        let test_json: &[Option<Json>] = &[
            None,
            Some(Json::from_str_val("我好菜啊").unwrap()),
            None,
            Some(Json::from_str_val("我菜爆了").unwrap()),
            Some(Json::from_str_val("我失败了").unwrap()),
            Some(Json::from_f64(2.333).unwrap()),
            Some(Json::from_str_val("💩").unwrap()),
            None,
        ];
        assert_eq!(SolitonedVecJson::from_slice(test_json).to_vec(), test_json);
        assert_eq!(
            SolitonedVecJson::from_slice(&test_json.to_vec()).to_vec(),
            test_json
        );
    }

    #[test]
    fn test_basics() {
        let mut x: SolitonedVecJson = SolitonedVecJson::with_capacity(0);
        x.push(None);
        x.push(Some(Json::from_str_val("我好菜啊").unwrap()));
        x.push(None);
        x.push(Some(Json::from_str_val("我菜爆了").unwrap()));
        x.push(Some(Json::from_str_val("我失败了").unwrap()));
        assert_eq!(x.get(0), None);
        assert_eq!(
            x.get(1),
            Some(Json::from_str_val("我好菜啊").unwrap().as_ref())
        );
        assert_eq!(x.get(2), None);
        assert_eq!(
            x.get(3),
            Some(Json::from_str_val("我菜爆了").unwrap().as_ref())
        );
        assert_eq!(
            x.get(4),
            Some(Json::from_str_val("我失败了").unwrap().as_ref())
        );
        assert_eq!(x.len(), 5);
        assert!(!x.is_empty());
    }

    #[test]
    fn test_truncate() {
        let test_json: &[Option<Json>] = &[
            None,
            None,
            Some(Json::from_str_val("我好菜啊").unwrap()),
            None,
            Some(Json::from_str_val("我菜爆了").unwrap()),
            Some(Json::from_str_val("我失败了").unwrap()),
            Some(Json::from_f64(2.333).unwrap()),
            Some(Json::from_str_val("💩").unwrap()),
            None,
        ];
        let mut Solitoned_vec = SolitonedVecJson::from_slice(test_json);
        Solitoned_vec.truncate(100);
        assert_eq!(Solitoned_vec.len(), 9);
        Solitoned_vec.truncate(3);
        assert_eq!(Solitoned_vec.len(), 3);
        assert_eq!(Solitoned_vec.get(0), None);
        assert_eq!(Solitoned_vec.get(1), None);
        assert_eq!(
            Solitoned_vec.get(2),
            Some(Json::from_str_val("我好菜啊").unwrap().as_ref())
        );
        Solitoned_vec.truncate(2);
        assert_eq!(Solitoned_vec.len(), 2);
        assert_eq!(Solitoned_vec.get(0), None);
        assert_eq!(Solitoned_vec.get(1), None);
        Solitoned_vec.truncate(1);
        assert_eq!(Solitoned_vec.len(), 1);
        assert_eq!(Solitoned_vec.get(0), None);
        Solitoned_vec.truncate(0);
        assert_eq!(Solitoned_vec.len(), 0);
    }

    #[test]
    fn test_applightlike() {
        let test_json_1: &[Option<Json>] = &[
            None,
            None,
            Some(Json::from_str_val("我好菜啊").unwrap()),
            None,
        ];
        let test_json_2: &[Option<Json>] = &[
            Some(Json::from_str_val("我菜爆了").unwrap()),
            Some(Json::from_str_val("我失败了").unwrap()),
            Some(Json::from_f64(2.333).unwrap()),
            Some(Json::from_str_val("💩").unwrap()),
            None,
        ];
        let mut Solitoned_vec_1 = SolitonedVecJson::from_slice(test_json_1);
        let mut Solitoned_vec_2 = SolitonedVecJson::from_slice(test_json_2);
        Solitoned_vec_1.applightlike(&mut Solitoned_vec_2);
        assert_eq!(Solitoned_vec_1.len(), 9);
        assert!(Solitoned_vec_2.is_empty());
        assert_eq!(
            Solitoned_vec_1.to_vec(),
            &[
                None,
                None,
                Some(Json::from_str_val("我好菜啊").unwrap()),
                None,
                Some(Json::from_str_val("我菜爆了").unwrap()),
                Some(Json::from_str_val("我失败了").unwrap()),
                Some(Json::from_f64(2.333).unwrap()),
                Some(Json::from_str_val("💩").unwrap()),
                None,
            ]
        );
    }
}
