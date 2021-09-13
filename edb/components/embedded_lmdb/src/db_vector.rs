// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use edb::DBVector;
use lmdb::DBVector as RawDBVector;
use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;

pub struct LmdbDBVector(RawDBVector);

impl LmdbDBVector {
    pub fn from_raw(raw: RawDBVector) -> LmdbDBVector {
        LmdbDBVector(raw)
    }
}

impl DBVector for LmdbDBVector {}

impl Deref for LmdbDBVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for LmdbDBVector {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for LmdbDBVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
