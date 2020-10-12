// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::engine::LmdbEngine;
use engine_promises::CAUSETNamesExt;

impl CAUSETNamesExt for LmdbEngine {
    fn causet_names(&self) -> Vec<&str> {
        self.as_inner().causet_names()
    }
}
