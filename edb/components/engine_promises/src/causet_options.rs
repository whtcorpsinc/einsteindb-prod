// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::db_options::NoetherDBOptions;

pub trait PrimaryCausetNetworkOptions {
    type NoetherDBOptions: NoetherDBOptions;

    fn new() -> Self;
    fn get_level_zero_slowdown_writes_trigger(&self) -> u32;
    fn get_level_zero_stop_writes_trigger(&self) -> u32;
    fn get_soft_plightlikeing_compaction_bytes_limit(&self) -> u64;
    fn get_hard_plightlikeing_compaction_bytes_limit(&self) -> u64;
    fn get_block_cache_capacity(&self) -> u64;
    fn set_block_cache_capacity(&self, capacity: u64) -> Result<(), String>;
    fn set_titandb_options(&mut self, opts: &Self::NoetherDBOptions);
    fn get_target_file_size_base(&self) -> u64;
    fn get_disable_auto_compactions(&self) -> bool;
}
