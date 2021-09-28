// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::allegro::Panicallegro;
use allegrosql_promises::CausetNamesExt;

impl CausetNamesExt for Panicallegro {
    fn Causet_names(&self) -> Vec<&str> {
        panic!()
    }
}
