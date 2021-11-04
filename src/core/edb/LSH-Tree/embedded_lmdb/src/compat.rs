// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::edb::LmdbEngine;
use crate::raw::DB;
use std::sync::Arc;

/// A trait to enter the world of engine promises from a raw `Arc<DB>`
/// with as little syntax as possible.
///
/// This will be used during the transition from Lmdb to the
/// `CausetEngine` abstraction and then discarded.
pub trait Compat {
    type Other;

    fn c(&self) -> &Self::Other;
}

impl Compat for Arc<DB> {
    type Other = LmdbEngine;

    #[inline]
    fn c(&self) -> &LmdbEngine {
        LmdbEngine::from_ref(self)
    }
}
