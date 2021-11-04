// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

/// A trait for engines that support setting global options
pub trait DBOptionsExt {
    type DBOptions: DBOptions;

    fn get_db_options(&self) -> Self::DBOptions;
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()>;
}

/// A handle to a database's options
pub trait DBOptions {
    type NoetherDBOptions: NoetherDBOptions;

    fn new() -> Self;
    fn get_max_background_jobs(&self) -> i32;
    fn get_rate_bytes_per_sec(&self) -> Option<i64>;
    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()>;
    fn tenancy_launched_for_einsteindb(&mut self, opts: &Self::NoetherDBOptions);
}

/// Noether-specefic options
pub trait NoetherDBOptions {
    fn new() -> Self;
    fn set_min_blob_size(&mut self, size: u64);
}
