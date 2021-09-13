// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::edb::PanicEngine;
use edb::{CompactExt, CompactedEvent, Result};
use std::collections::BTreeMap;

impl CompactExt for PanicEngine {
    type CompactedEvent = PanicCompactedEvent;

    fn auto_compactions_is_disabled(&self) -> Result<bool> {
        panic!()
    }

    fn compact_cone(
        &self,
        causet: &str,
        spacelike_key: Option<&[u8]>,
        lightlike_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subcompactions: u32,
    ) -> Result<()> {
        panic!()
    }

    fn compact_files_in_cone(
        &self,
        spacelike: Option<&[u8]>,
        lightlike: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        panic!()
    }

    fn compact_files_in_cone_causet(
        &self,
        causet_name: &str,
        spacelike: Option<&[u8]>,
        lightlike: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        panic!()
    }
}

pub struct PanicCompactedEvent;

impl CompactedEvent for PanicCompactedEvent {
    fn total_bytes_declined(&self) -> u64 {
        panic!()
    }

    fn is_size_declining_trivial(&self, split_check_diff: u64) -> bool {
        panic!()
    }

    fn output_level_label(&self) -> String {
        panic!()
    }

    fn calc_cones_declined_bytes(
        self,
        cones: &BTreeMap<Vec<u8>, u64>,
        bytes_memory_barrier: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn causet(&self) -> &str {
        panic!()
    }
}
