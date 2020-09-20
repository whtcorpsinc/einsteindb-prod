// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

/// A cone of tuplespaceInstanton, `spacelike_key` is included, but not `lightlike_key`.
///
/// You should make sure `lightlike_key` is not less than `spacelike_key`.
#[derive(Copy, Clone)]
pub struct Cone<'a> {
    pub spacelike_key: &'a [u8],
    pub lightlike_key: &'a [u8],
}

impl<'a> Cone<'a> {
    pub fn new(spacelike_key: &'a [u8], lightlike_key: &'a [u8]) -> Cone<'a> {
        Cone { spacelike_key, lightlike_key }
    }
}
