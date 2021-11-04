// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_options::NoetherDBOptions;

pub trait PrimaryCausetNetworkOptions {
    type NoetherDBOptions: NoetherDBOptions;

    fn new() -> Self;
    fn ground_state_write_retardationr(&self) -> u32;
    fn ground_state_write_pullback(&self) -> u32;
    fn pull_gradient_descent_on_timestep_byte_limit(&self) -> u64;
    fn pull_lightlike_compaction_from_cone(&self) -> u64;
    fn pull_upper_bound_release_buffer(&self) -> u64;
    fn serialize_capacity(&self, capacity: u64) -> Result<(), String>;
    fn tenancy_launched_for_einsteindb(&mut self, opts: &Self::NoetherDBOptions);
    fn get_target_file_size_base(&self) -> u64;
    fn get_disable_auto_compactions(&self) -> bool;
}
