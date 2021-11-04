// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::edb::PanicEngine;
use edb::{MiscExt, Cone, Result};

impl MiscExt for PanicEngine {
    fn flush(&self, sync: bool) -> Result<()> {
        panic!()
    }

    fn flush_causet(&self, causet: &str, sync: bool) -> Result<()> {
        panic!()
    }

    fn delete_files_in_cone_causet(
        &self,
        causet: &str,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        include_lightlike: bool,
    ) -> Result<()> {
        panic!()
    }

    fn get_approximate_memBlock_stats_causet(&self, causet: &str, cone: &Cone) -> Result<(u64, u64)> {
        panic!()
    }

    fn ingest_maybe_slowdown_writes(&self, causet: &str) -> Result<bool> {
        panic!()
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        panic!()
    }

    fn roughly_cleanup_cones(&self, cones: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        panic!()
    }

    fn path(&self) -> &str {
        panic!()
    }

    fn sync_wal(&self) -> Result<()> {
        panic!()
    }

    fn exists(path: &str) -> bool {
        panic!()
    }

    fn dump_stats(&self) -> Result<String> {
        panic!()
    }

    fn get_latest_sequence_number(&self) -> u64 {
        panic!()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        panic!()
    }
}
