// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_options::PrimaryCausetNetworkOptions;
use crate::errors::Result;


pub trait CAUSETHandleExt {

    type CAUSETHandle: CAUSETHandle;
    type PrimaryCausetNetworkOptions: PrimaryCausetNetworkOptions;

    fn causet_handle(&self, name: &str) -> Result<&Self::CAUSETHandle>;
    fn get_options_causet(&self, causet: &Self::CAUSETHandle) -> Self::PrimaryCausetNetworkOptions;
    fn set_options_causet(&self, causet: &Self::CAUSETHandle, options: &[(&str, &str)]) -> Result<()>;
}

pub trait CAUSETHandle {}
