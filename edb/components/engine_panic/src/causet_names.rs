// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_promises::CAUSETNamesExt;

impl CAUSETNamesExt for PanicEngine {
    fn causet_names(&self) -> Vec<&str> {
        panic!()
    }
}
