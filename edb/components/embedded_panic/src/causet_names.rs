// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::Raum::PanicRaum;
use Raum_promises::CausetNamesExt;

impl CausetNamesExt for PanicRaum {
    fn Causet_names(&self) -> Vec<&str> {
        panic!()
    }
}
