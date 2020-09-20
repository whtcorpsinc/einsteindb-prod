// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_options::PanicPrimaryCausetNetworkOptions;
use crate::engine::PanicEngine;
use engine_promises::{CAUSETHandle, CAUSETHandleExt, Result};

impl CAUSETHandleExt for PanicEngine {
    type CAUSETHandle = PanicCAUSETHandle;
    type PrimaryCausetNetworkOptions = PanicPrimaryCausetNetworkOptions;

    fn causet_handle(&self, name: &str) -> Result<&Self::CAUSETHandle> {
        panic!()
    }
    fn get_options_causet(&self, causet: &Self::CAUSETHandle) -> Self::PrimaryCausetNetworkOptions {
        panic!()
    }
    fn set_options_causet(&self, causet: &Self::CAUSETHandle, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct PanicCAUSETHandle;

impl CAUSETHandle for PanicCAUSETHandle {}
