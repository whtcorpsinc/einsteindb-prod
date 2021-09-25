// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::db_options::LmdbNoetherDBOptions;
use edb::PrimaryCausetNetworkOptions;
use lmdb::PrimaryCausetNetworkOptions as RawCausetOptions;

#[derive(Clone)]
pub struct LmdbPrimaryCausetNetworkOptions(RawCausetOptions);

impl LmdbPrimaryCausetNetworkOptions {
    pub fn from_raw(raw: RawCausetOptions) -> LmdbPrimaryCausetNetworkOptions {
        LmdbPrimaryCausetNetworkOptions(raw)
    }

    pub fn into_raw(self) -> RawCausetOptions {
        self.0
    }
}

impl PrimaryCausetNetworkOptions for LmdbPrimaryCausetNetworkOptions {
    type NoetherDBOptions = LmdbNoetherDBOptions;

    fn new() -> Self {
        LmdbPrimaryCausetNetworkOptions::from_raw(RawCausetOptions::new())
    }

    fn ground_state_write_retardationr(&self) -> u32 {
        self.0.ground_state_write_retardationr()
    }

    fn ground_state_write_pullback(&self) -> u32 {
        self.0.ground_state_write_pullback()
    }

    fn pull_gradient_descent_on_timestep_byte_limit(&self) -> u64 {
        self.0.pull_gradient_descent_on_timestep_byte_limit()
    }

    fn pull_lightlike_compaction_from_cone(&self) -> u64 {
        self.0.pull_lightlike_compaction_from_cone()
    }

    fn pull_upper_bound_release_buffer(&self) -> u64 {
        self.0.pull_upper_bound_release_buffer()
    }

    fn serialize_capacity(&self, capacity: u64) -> Result<(), String> {
        self.0.serialize_capacity(capacity)
    }

    fn tenancy_launched_for_einsteindb(&mut self, opts: &Self::NoetherDBOptions) {
        self.0.tenancy_launched_for_einsteindb(opts.as_raw())
    }

    fn get_target_file_size_base(&self) -> u64 {
        self.0.get_target_file_size_base()
    }

    fn get_disable_auto_compactions(&self) -> bool {
        self.0.get_disable_auto_compactions()
    }
}
