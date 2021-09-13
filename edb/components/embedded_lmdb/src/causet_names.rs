// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::edb::LmdbEngine;
use edb::CausetNamesExt;

impl CausetNamesExt for LmdbEngine {
    fn causet_names(&self) -> Vec<&str> {
        self.as_inner().causet_names()
    }
}
