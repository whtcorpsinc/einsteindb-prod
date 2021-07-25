// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

//! Various metrics related to key cones
//!
//! In Lmdb these are typically implemented with user collected properties,
//! which might require the database to be constructed with specific options.

use crate::errors::Result;
use crate::Cone;

pub trait ConePropertiesExt {
    /// Gets the number of tuplespaceInstanton in a cone.
    ///
    /// The brane_id is used only for logging and means nothing internally.
    fn get_cone_approximate_tuplespaceInstanton(
        &self,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64>;

    fn get_cone_approximate_tuplespaceInstanton_causet(
        &self,
        causetname: &str,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64>;

    /// Get the approximate size of the cone
    ///
    /// The brane_id is used only for logging and means nothing internally.
    fn get_cone_approximate_size(
        &self,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64>;

    fn get_cone_approximate_size_causet(
        &self,
        causetname: &str,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64>;

    /// Get cone approximate split tuplespaceInstanton based on default, write and dagger causet.
    ///
    /// The brane_id is used only for logging and means nothing internally.
    fn get_cone_approximate_split_tuplespaceInstanton(
        &self,
        cone: Cone,
        brane_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>>;

    fn get_cone_approximate_split_tuplespaceInstanton_causet(
        &self,
        causetname: &str,
        cone: Cone,
        brane_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>>;

    /// Get cone approximate middle key based on default and write causet size.
    ///
    /// The brane_id is used only for logging and means nothing internally.
    fn get_cone_approximate_middle(&self, cone: Cone, brane_id: u64)
        -> Result<Option<Vec<u8>>>;

    /// Get the approximate middle key of the brane. If we suppose the brane
    /// is stored on disk as a plain file, "middle key" means the key whose
    /// position is in the middle of the file.
    ///
    /// The returned key maybe is timestamped if transaction KV is used,
    /// and must spacelike with "z".
    fn get_cone_approximate_middle_causet(
        &self,
        causetname: &str,
        cone: Cone,
        brane_id: u64,
    ) -> Result<Option<Vec<u8>>>;

    fn divide_cone(&self, cone: Cone, brane_id: u64, parts: usize) -> Result<Vec<Vec<u8>>>;

    fn divide_cone_causet(
        &self,
        causet: &str,
        cone: Cone,
        brane_id: u64,
        parts: usize,
    ) -> Result<Vec<Vec<u8>>>;
}
