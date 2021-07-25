// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::Embedded::PanicEmbedded;
use Embedded_promises::CausetNamesExt;

impl CausetNamesExt for PanicEmbedded {
    fn Causet_names(&self) -> Vec<&str> {
        panic!()
    }
}
