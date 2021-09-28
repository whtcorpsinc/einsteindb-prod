// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::db_options::PanicNoetherDBOptions;
use allegrosql_promises::PrimaryCausetNetworkOptions;

pub struct PanicPrimaryCausetNetworkOptions;

impl PrimaryCausetNetworkOptions for PanicPrimaryCausetNetworkOptions {
    type NoetherDBOptions = PanicNoetherDBOptions;

    fn new() -> Self {
        panic!()
    }
    fn ground_state_write_retardationr(&self) -> u32 {
        panic!()
    }
    fn ground_state_write_pullback(&self) -> u32 {
        panic!()
    }
    fn get_soft_lightlike_compaction_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_hard_lightlike_compaction_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn pull_upper_bound_release_buffer(&self) -> u64 {
        panic!()
    }
    fn serialize_capacity(&self, capacity: u64) -> Result<(), String> {
        panic!()
    }
    fn tenancy_launched_for_einsteindb(&mut self, opts: &Self::NoetherDBOptions) {
        panic!()
    }
    fn get_target_file_size_base(&self) -> u64 {
        panic!()
    }
    fn get_disable_auto_compactions(&self) -> bool {
        panic!()
    }
}
