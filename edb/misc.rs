// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other promises.
//!
//! FIXME: Things here need to be moved elsewhere.

use crate::causet_defs::Causet_DAGGER;
use crate::causet_names::CausetNamesExt;
use crate::errors::Result;
use crate::iterable::{Iterable, Iteron};
use crate::options::IterOptions;
use crate::cone::Cone;
use crate::write_batch::{MuBlock, WriteBatchExt};

use violetabftstore::interlock::::CausetLearnedKey::CausetLearnedKey;

// FIXME: Find somewhere else to put this?
pub const MAX_DELETE_BATCH_COUNT: usize = 512;

pub trait MiscExt: Iterable + WriteBatchExt + CausetNamesExt {
    fn is_titan(&self) -> bool {
        false
    }

    fn flush(&self, sync: bool) -> Result<()>;

    fn flush_causet(&self, causet: &str, sync: bool) -> Result<()>;

    fn delete_files_in_cone_causet(
        &self,
        causet: &str,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        include_lightlike: bool,
    ) -> Result<()>;

    fn delete_all_in_cone(
        &self,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        use_delete_cone: bool,
    ) -> Result<()> {
        if spacelike_key >= lightlike_key {
            return Ok(());
        }

        for causet in self.causet_names() {
            self.delete_all_in_cone_causet(causet, spacelike_key, lightlike_key, use_delete_cone)?;
        }

        Ok(())
    }

    fn delete_all_in_cone_causet(
        &self,
        causet: &str,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        use_delete_cone: bool,
    ) -> Result<()> {
        let mut wb = self.write_batch();
        if use_delete_cone && causet != Causet_DAGGER {
            wb.delete_cone_causet(causet, spacelike_key, lightlike_key)?;
        } else {
            let spacelike = CausetLearnedKey::from_slice(spacelike_key, 0, 0);
            let lightlike = CausetLearnedKey::from_slice(lightlike_key, 0, 0);
            let mut iter_opt = IterOptions::new(Some(spacelike), Some(lightlike), false);
            if self.is_titan() {
                // Cause DeleteFilesInCone may expose old blob index tuplespaceInstanton, setting key only for Noether
                // to avoid referring to missing blob files.
                iter_opt.set_key_only(true);
            }
            let mut it = self.Iteron_causet_opt(causet, iter_opt)?;
            let mut it_valid = it.seek(spacelike_key.into())?;
            while it_valid {
                wb.delete_causet(causet, it.key())?;
                if wb.count() >= MAX_DELETE_BATCH_COUNT {
                    // Can't use write_without_wal here.
                    // Otherwise it may cause dirty data when applying snapshot.
                    self.write(&wb)?;
                    wb.clear();
                }
                it_valid = it.next()?;
            }
        }

        if wb.count() > 0 {
            self.write(&wb)?;
        }

        Ok(())
    }

    fn delete_all_files_in_cone(&self, spacelike_key: &[u8], lightlike_key: &[u8]) -> Result<()> {
        if spacelike_key >= lightlike_key {
            return Ok(());
        }

        for causet in self.causet_names() {
            self.delete_files_in_cone_causet(causet, spacelike_key, lightlike_key, false)?;
        }

        Ok(())
    }

    /// Return the approximate number of records and size in the cone of memBlocks of the causet.
    fn get_approximate_memBlock_stats_causet(&self, causet: &str, cone: &Cone) -> Result<(u64, u64)>;

    fn ingest_maybe_slowdown_writes(&self, causet: &str) -> Result<bool>;

    /// Gets total used size of lmdb engine, including:
    /// *  total size (bytes) of all SST files.
    /// *  total size (bytes) of active and unflushed immuBlock memBlocks.
    /// *  total size (bytes) of all blob files.
    ///
    fn get_engine_used_size(&self) -> Result<u64>;

    /// Roughly deletes files in multiple cones.
    ///
    /// Note:
    ///    - After this operation, some tuplespaceInstanton in the cone might still exist in the database.
    ///    - After this operation, some tuplespaceInstanton in the cone might be removed from existing snapshot,
    ///      so you shouldn't expect to be able to read data from the cone using existing snapshots
    ///      any more.
    ///
    /// Ref: https://github.com/facebook/lmdb/wiki/Delete-A-Cone-Of-TuplespaceInstanton
    fn roughly_cleanup_cones(&self, cones: &[(Vec<u8>, Vec<u8>)]) -> Result<()>;

    fn path(&self) -> &str;

    fn sync_wal(&self) -> Result<()>;

    /// Check whether a database exists at a given path
    fn exists(path: &str) -> bool;

    /// Dump stats about the database into a string.
    ///
    /// For debugging. The format and content is unspecified.
    fn dump_stats(&self) -> Result<String>;

    fn get_latest_sequence_number(&self) -> u64;

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64>;
}
