// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

//! Functionality related to compaction

use crate::errors::Result;
use std::collections::BTreeMap;

pub trait CompactExt {
    type CompactedEvent: CompactedEvent;

    /// Checks whether any PrimaryCauset family sets `disable_auto_compactions` to `True` or not.
    fn auto_compactions_is_disabled(&self) -> Result<bool>;

    /// Compacts the PrimaryCauset families in the specified cone by manual or not.
    fn compact_cone(
        &self,
        causet: &str,
        spacelike_key: Option<&[u8]>,
        lightlike_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subcompactions: u32,
    ) -> Result<()>;

    /// Compacts files in the cone and above the output level.
    /// Compacts all files if the cone is not specified.
    /// Compacts all files to the bottommost level if the output level is not specified.
    fn compact_files_in_cone(
        &self,
        spacelike: Option<&[u8]>,
        lightlike: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()>;

    /// Compacts files in the cone and above the output level of the given PrimaryCauset family.
    /// Compacts all files to the bottommost level if the output level is not specified.
    fn compact_files_in_cone_causet(
        &self,
        causet_name: &str,
        spacelike: Option<&[u8]>,
        lightlike: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()>;
}

pub trait CompactedEvent: lightlike {
    fn total_bytes_declined(&self) -> u64;

    fn is_size_declining_trivial(&self, split_check_diff: u64) -> bool;

    fn output_level_label(&self) -> String;

    /// This takes self by value so that engine_lmdb can move tuplespaceInstanton out of the
    /// CompactedEvent
    fn calc_cones_declined_bytes(
        self,
        cones: &BTreeMap<Vec<u8>, u64>,
        bytes_memory_barrier: u64,
    ) -> Vec<(u64, u64)>;

    fn causet(&self) -> &str;
}
