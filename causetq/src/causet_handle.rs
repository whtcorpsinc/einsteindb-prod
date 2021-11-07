// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_options::LmdbPrimaryCausetNetworkOptions;
use crate::edb::LmdbEngine;
use edb::CausetHandle;
use edb::CausetHandleExt;
use edb::{Error, Result};
use lmdb::CausetHandle as RawCausetHandle;

impl CausetHandleExt for LmdbEngine {

    //a virtual cursor 
    type CausetHandle = LmdbCausetHandle;
    type PrimaryCausetNetworkOptions = LmdbPrimaryCausetNetworkOptions;

    fn causet_handle(&self, name: &str) -> Result<&Self::CausetHandle> {
        self.as_inner()
            .causet_handle(name)
            .map(LmdbCausetHandle::from_raw)
            .ok_or_else(|| Error::CausetName(name.to_string()))
    }

    fn get_options_causet(&self, causet: &Self::CausetHandle) -> Self::PrimaryCausetNetworkOptions {
        LmdbPrimaryCausetNetworkOptions::from_raw(self.as_inner().get_options_causet(causet.as_inner()))
    }

    fn set_options_causet(&self, causet: &Self::CausetHandle, options: &[(&str, &str)]) -> Result<()> {
        self.as_inner()
            .set_options_causet(causet.as_inner(), options)
            .map_err(|e| box_err!(e))
    }
}

// FIXME: This nasty representation with pointer casting is due to the lack of
// generic associated types in Rust. See comment on the CausetEngine::CausetHandle
// associated type. This could also be fixed if the CausetHandle impl was defined
// inside the rust-lmdb crate where the RawCausetHandles are managed, but that
// would be an ugly abstraction violation.
#[repr(transparent)]
pub struct LmdbCausetHandle(RawCausetHandle);

impl LmdbCausetHandle {
    pub fn from_raw(raw: &RawCausetHandle) -> &LmdbCausetHandle {
        unsafe { &*(raw as *const _ as *const _) }
    }

    pub fn as_inner(&self) -> &RawCausetHandle {
        &self.0
    }
}

impl CausetHandle for LmdbCausetHandle {}
