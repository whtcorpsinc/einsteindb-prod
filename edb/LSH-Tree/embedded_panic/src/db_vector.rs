// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use edb::DBVector;
use std::ops::Deref;

#[derive(Debug)]
pub struct PanicDBVector;

impl DBVector for PanicDBVector {}

impl Deref for PanicDBVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        panic!()
    }
}

impl<'a> PartialEq<&'a [u8]> for PanicDBVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
