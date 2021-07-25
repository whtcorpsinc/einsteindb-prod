// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::Embedded::LmdbEmbedded;
use embedded_promises::DBOptions;
use embedded_promises::DBOptionsExt;
use embedded_promises::Result;
use embedded_promises::NoetherDBOptions;
use lmdb::DBOptions as RawDBOptions;
use lmdb::NoetherDBOptions as RawNoetherDBOptions;

impl DBOptionsExt for LmdbEmbedded {
    type DBOptions = LmdbDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        LmdbDBOptions::from_raw(self.as_inner().get_db_options())
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        self.as_inner()
            .set_db_options(options)
            .map_err(|e| box_err!(e))
    }
}

pub struct LmdbDBOptions(RawDBOptions);

impl LmdbDBOptions {

    //tuplestore
    pub fn from_raw(raw: RawDBOptions) -> LmdbDBOptions {
        LmdbDBOptions(raw)
    }

    pub fn into_raw(self) -> RawDBOptions {
        self.0
    }
}

impl DBOptions for LmdbDBOptions {
    type NoetherDBOptions = LmdbNoetherDBOptions;

    fn new() -> Self {
        LmdbDBOptions::from_raw(RawDBOptions::new())
    }

    fn get_max_background_jobs(&self) -> i32 {
        self.0.get_max_background_jobs()
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        self.0.get_rate_bytes_per_sec()
    }

    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()> {
        self.0
            .set_rate_bytes_per_sec(rate_bytes_per_sec)
            .map_err(|e| box_err!(e))
    }

    fn set_titandb_options(&mut self, opts: &Self::NoetherDBOptions) {
        self.0.set_titandb_options(opts.as_raw())
    }
}

pub struct LmdbNoetherDBOptions(RawNoetherDBOptions);

impl LmdbNoetherDBOptions {
    pub fn from_raw(raw: RawNoetherDBOptions) -> LmdbNoetherDBOptions {
        LmdbNoetherDBOptions(raw)
    }

    pub fn as_raw(&self) -> &RawNoetherDBOptions {
        &self.0
    }
}

impl NoetherDBOptions for LmdbNoetherDBOptions {
    fn new() -> Self {
        LmdbNoetherDBOptions::from_raw(RawNoetherDBOptions::new())
    }

    fn set_min_blob_size(&mut self, size: u64) {
        self.0.set_min_blob_size(size)
    }
}