// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::fs;
use std::path::Path;

use edb::{CacheStats, VioletaBftEngine, VioletaBftLogBatch as VioletaBftLogBatchTrait, Result};
use ekvproto::violetabft_server_timeshare::VioletaBftLocalState;
use violetabft::evioletabft_timeshare::Entry;
use violetabft_engine::{EntryExt, Error as VioletaBftEngineError, LogBatch, VioletaBftLogEngine as RawVioletaBftEngine};

pub use violetabft_engine::config::RecoveryMode;
pub use violetabft_engine::Config as VioletaBftEngineConfig;

#[derive(Clone)]
pub struct EntryExtTyped;

impl EntryExt<Entry> for EntryExtTyped {
    fn index(e: &Entry) -> u64 {
        e.index
    }
}

#[derive(Clone)]
pub struct VioletaBftLogEngine(RawVioletaBftEngine<Entry, EntryExtTyped>);

impl VioletaBftLogEngine {
    pub fn new(config: VioletaBftEngineConfig) -> Self {
        VioletaBftLogEngine(RawVioletaBftEngine::new(config))
    }

    /// If path is not an empty directory, we say db exists.
    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }
        fs::read_dir(&path).unwrap().next().is_some()
    }
}

#[derive(Default)]
pub struct VioletaBftLogBatch(LogBatch<Entry, EntryExtTyped>);

const VIOLETABFT_LOG_STATE_KEY: &[u8] = b"R";

impl VioletaBftLogBatchTrait for VioletaBftLogBatch {
    fn applightlike(&mut self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        self.0.add_entries(violetabft_group_id, entries);
        Ok(())
    }

    fn cut_logs(&mut self, _: u64, _: u64, _: u64) {
        // It's unnecessary because overlapped entries can be handled in `applightlike`.
    }

    fn put_violetabft_state(&mut self, violetabft_group_id: u64, state: &VioletaBftLocalState) -> Result<()> {
        box_try!(self
            .0
            .put_msg(violetabft_group_id, VIOLETABFT_LOG_STATE_KEY.to_vec(), state));
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.0.items.is_empty()
    }
}

impl VioletaBftEngine for VioletaBftLogEngine {
    type LogBatch = VioletaBftLogBatch;

    fn log_batch(&self, _capacity: usize) -> Self::LogBatch {
        VioletaBftLogBatch::default()
    }

    fn sync(&self) -> Result<()> {
        box_try!(self.0.sync());
        Ok(())
    }

    fn get_violetabft_state(&self, violetabft_group_id: u64) -> Result<Option<VioletaBftLocalState>> {
        let state = box_try!(self.0.get_msg(violetabft_group_id, VIOLETABFT_LOG_STATE_KEY));
        Ok(state)
    }

    fn get_entry(&self, violetabft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        self.0
            .get_entry(violetabft_group_id, index)
            .map_err(|e| transfer_error(e))
    }

    fn fetch_entries_to(
        &self,
        violetabft_group_id: u64,
        begin: u64,
        lightlike: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize> {
        self.0
            .fetch_entries_to(violetabft_group_id, begin, lightlike, max_size, to)
            .map_err(|e| transfer_error(e))
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<usize> {
        let ret = box_try!(self.0.write(&mut batch.0, sync));
        Ok(ret)
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        _: usize,
        _: usize,
    ) -> Result<usize> {
        let ret = box_try!(self.0.write(&mut batch.0, sync));
        Ok(ret)
    }

    fn clean(
        &self,
        violetabft_group_id: u64,
        _: &VioletaBftLocalState,
        batch: &mut VioletaBftLogBatch,
    ) -> Result<()> {
        batch.0.clean_brane(violetabft_group_id);
        Ok(())
    }

    fn applightlike(&self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        let mut batch = Self::LogBatch::default();
        batch.0.add_entries(violetabft_group_id, entries);
        let ret = box_try!(self.0.write(&mut batch.0, false));
        Ok(ret)
    }

    fn put_violetabft_state(&self, violetabft_group_id: u64, state: &VioletaBftLocalState) -> Result<()> {
        box_try!(self.0.put_msg(violetabft_group_id, VIOLETABFT_LOG_STATE_KEY, state));
        Ok(())
    }

    fn gc(&self, violetabft_group_id: u64, _from: u64, to: u64) -> Result<usize> {
        Ok(self.0.compact_to(violetabft_group_id, to) as usize)
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        let ret = box_try!(self.0.purge_expired_files());
        Ok(ret)
    }

    fn has_builtin_entry_cache(&self) -> bool {
        true
    }

    fn gc_entry_cache(&self, violetabft_group_id: u64, to: u64) {
        self.0.compact_cache_to(violetabft_group_id, to)
    }
    /// Flush current cache stats.
    fn flush_stats(&self) -> Option<CacheStats> {
        let stat = self.0.flush_stats();
        Some(edb::CacheStats {
            hit: stat.hit,
            miss: stat.miss,
            cache_size: stat.cache_size,
        })
    }

    fn stop(&self) {
        self.0.stop();
    }

    fn dump_stats(&self) -> Result<String> {
        // VioletaBft engine won't dump anything.
        Ok("".to_owned())
    }
}

fn transfer_error(e: VioletaBftEngineError) -> edb::Error {
    match e {
        VioletaBftEngineError::StorageCompacted => edb::Error::EntriesCompacted,
        VioletaBftEngineError::StorageUnavailable => edb::Error::EntriesUnavailable,
        e => {
            let e = box_err!(e);
            edb::Error::Other(e)
        }
    }
}
