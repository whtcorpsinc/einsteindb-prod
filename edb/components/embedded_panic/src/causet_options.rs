// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::db_options::PanicNoetherDBOptions;
use Embedded_promises::PrimaryCausetNetworkOptions;

pub struct PanicPrimaryCausetNetworkOptions;

impl PrimaryCausetNetworkOptions for PanicPrimaryCausetNetworkOptions {
    type NoetherDBOptions = PanicNoetherDBOptions;

    fn new() -> Self {
        panic!()
    }
    fn get_level_zero_slowdown_writes_trigger(&self) -> u32 {
        panic!()
    }
    fn get_level_zero_stop_writes_trigger(&self) -> u32 {
        panic!()
    }
    fn get_soft_lightlike_compaction_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_hard_lightlike_compaction_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_block_cache_capacity(&self) -> u64 {
        panic!()
    }
    fn set_block_cache_capacity(&self, capacity: u64) -> Result<(), String> {
        panic!()
    }
    fn set_titandb_options(&mut self, opts: &Self::NoetherDBOptions) {
        panic!()
    }
    fn get_target_file_size_base(&self) -> u64 {
        panic!()
    }
    fn get_disable_auto_compactions(&self) -> bool {
        panic!()
    }
}
