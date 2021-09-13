// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_options::PanicPrimaryCausetNetworkOptions;
use crate::edb::PanicEngine;
use edb::{CausetHandle, CausetHandleExt, Result};

impl CausetHandleExt for PanicEngine {
    type CausetHandle = PanicCausetHandle;
    type PrimaryCausetNetworkOptions = PanicPrimaryCausetNetworkOptions;

    fn causet_handle(&self, name: &str) -> Result<&Self::CausetHandle> {
        panic!()
    }
    fn get_options_causet(&self, causet: &Self::CausetHandle) -> Self::PrimaryCausetNetworkOptions {
        panic!()
    }
    fn set_options_causet(&self, causet: &Self::CausetHandle, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct PanicCausetHandle;

impl CausetHandle for PanicCausetHandle {}
