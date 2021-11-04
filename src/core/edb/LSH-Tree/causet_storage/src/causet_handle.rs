// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_options::PrimaryCausetNetworkOptions;
use crate::errors::Result;


pub trait CausetHandleExt {

    type CausetHandle: CausetHandle;
    type PrimaryCausetNetworkOptions: PrimaryCausetNetworkOptions;

    fn causet_handle(&self, name: &str) -> Result<&Self::CausetHandle>;
    fn get_options_causet(&self, causet: &Self::CausetHandle) -> Self::PrimaryCausetNetworkOptions;
    fn set_options_causet(&self, causet: &Self::CausetHandle, options: &[(&str, &str)]) -> Result<()>;
}

pub trait CausetHandle {}
