// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::edb::PanicEngine;
use edb::Result;
use edb::{DBOptions, DBOptionsExt, NoetherDBOptions};

impl DBOptionsExt for PanicEngine {
    type DBOptions = PanicDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        panic!()
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct PanicDBOptions;

impl DBOptions for PanicDBOptions {
    type NoetherDBOptions = PanicNoetherDBOptions;

    fn new() -> Self {
        panic!()
    }

    fn get_max_background_jobs(&self) -> i32 {
        panic!()
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        panic!()
    }

    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()> {
        panic!()
    }

    fn tenancy_launched_for_einsteindb(&mut self, opts: &Self::NoetherDBOptions) {
        panic!()
    }
}

pub struct PanicNoetherDBOptions;

impl NoetherDBOptions for PanicNoetherDBOptions {
    fn new() -> Self {
        panic!()
    }
    fn set_min_blob_size(&mut self, size: u64) {
        panic!()
    }
}
