// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::edb::PanicEngine;
use edb::{Cone, ConePropertiesExt, Result};

impl ConePropertiesExt for PanicEngine {
    fn get_cone_approximate_tuplespaceInstanton(
        &self,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64> {
        panic!()
    }

    fn get_cone_approximate_tuplespaceInstanton_causet(
        &self,
        causetname: &str,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64> {
        panic!()
    }

    fn get_cone_approximate_size(
        &self,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64> {
        panic!()
    }

    fn get_cone_approximate_size_causet(
        &self,
        causetname: &str,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64> {
        panic!()
    }

    fn get_cone_approximate_split_tuplespaceInstanton(
        &self,
        cone: Cone,
        brane_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>> {
        panic!()
    }

    fn get_cone_approximate_split_tuplespaceInstanton_causet(
        &self,
        causetname: &str,
        cone: Cone,
        brane_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>> {
        panic!()
    }

    fn get_cone_approximate_middle(
        &self,
        cone: Cone,
        brane_id: u64,
    ) -> Result<Option<Vec<u8>>> {
        panic!()
    }

    fn get_cone_approximate_middle_causet(
        &self,
        causetname: &str,
        cone: Cone,
        brane_id: u64,
    ) -> Result<Option<Vec<u8>>> {
        panic!()
    }

    fn divide_cone(&self, cone: Cone, brane_id: u64, parts: usize) -> Result<Vec<Vec<u8>>> {
        panic!()
    }

    fn divide_cone_causet(
        &self,
        causet: &str,
        cone: Cone,
        brane_id: u64,
        parts: usize,
    ) -> Result<Vec<Vec<u8>>> {
        panic!()
    }
}
