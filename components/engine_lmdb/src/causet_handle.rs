// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_options::LmdbPrimaryCausetNetworkOptions;
use crate::engine::LmdbEngine;
use engine_promises::CAUSETHandle;
use engine_promises::CAUSETHandleExt;
use engine_promises::{Error, Result};
use lmdb::CAUSETHandle as RawCAUSETHandle;

impl CAUSETHandleExt for LmdbEngine {
    type CAUSETHandle = LmdbCAUSETHandle;
    type PrimaryCausetNetworkOptions = LmdbPrimaryCausetNetworkOptions;

    fn causet_handle(&self, name: &str) -> Result<&Self::CAUSETHandle> {
        self.as_inner()
            .causet_handle(name)
            .map(LmdbCAUSETHandle::from_raw)
            .ok_or_else(|| Error::CAUSETName(name.to_string()))
    }

    fn get_options_causet(&self, causet: &Self::CAUSETHandle) -> Self::PrimaryCausetNetworkOptions {
        LmdbPrimaryCausetNetworkOptions::from_raw(self.as_inner().get_options_causet(causet.as_inner()))
    }

    fn set_options_causet(&self, causet: &Self::CAUSETHandle, options: &[(&str, &str)]) -> Result<()> {
        self.as_inner()
            .set_options_causet(causet.as_inner(), options)
            .map_err(|e| box_err!(e))
    }
}

#[repr(transparent)]
pub struct LmdbCAUSETHandle(RawCAUSETHandle);

impl LmdbCAUSETHandle {
    pub fn from_raw(raw: &RawCAUSETHandle) -> &LmdbCAUSETHandle {
        unsafe { &*(raw as *const _ as *const _) }
    }

    pub fn as_inner(&self) -> &RawCAUSETHandle {
        &self.0
    }
}

impl CAUSETHandle for LmdbCAUSETHandle {}
