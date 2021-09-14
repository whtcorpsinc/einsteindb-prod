use crate::*;
use ekvproto::violetabft_server_timeshare::VioletaBftLocalState;
use violetabft::evioletabft_timeshare::Entry;

pub trait VioletaBftEngine: Clone + Sync + lightlike + 'static {
    type LogBatch: VioletaBftLogBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch;

    /// Synchronize the VioletaBft engine.
    fn sync(&self) -> Result<()>;

    fn get_violetabft_state(&self, violetabft_group_id: u64) -> Result<Option<VioletaBftLocalState>>;

    fn get_entry(&self, violetabft_group_id: u64, index: u64) -> Result<Option<Entry>>;

    /// Return count of fetched entries.
    fn fetch_entries_to(
        &self,
        violetabft_group_id: u64,
        begin: u64,
        lightlike: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize>;

    /// Consume the write batch by moving the content into the engine itself
    /// and return written bytes.
    fn consume(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<usize>;

    /// Like `consume` but shrink `batch` if need.
    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<usize>;

    fn clean(
        &self,
        violetabft_group_id: u64,
        state: &VioletaBftLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()>;

    /// Applightlike some log entries and retrun written bytes.
    ///
    /// Note: `VioletaBftLocalState` won't be fideliod in this call.
    fn applightlike(&self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<usize>;

    /// Applightlike some log entries and retrun written bytes.
    ///
    /// Note: `VioletaBftLocalState` won't be fideliod in this call.
    fn applightlike_slice(&self, violetabft_group_id: u64, entries: &[Entry]) -> Result<usize> {
        self.applightlike(violetabft_group_id, entries.to_vec())
    }

    fn put_violetabft_state(&self, violetabft_group_id: u64, state: &VioletaBftLocalState) -> Result<()>;

    /// Like `cut_logs` but the cone could be very large. Return the deleted count.
    /// Generally, `from` can be passed in `0`.
    fn gc(&self, violetabft_group_id: u64, from: u64, to: u64) -> Result<usize>;

    /// Purge expired logs files and return a set of VioletaBft group ids
    /// which needs to be compacted ASAP.
    fn purge_expired_files(&self) -> Result<Vec<u64>>;

    /// The `VioletaBftEngine` has a builtin entry cache or not.
    fn has_builtin_entry_cache(&self) -> bool {
        false
    }

    /// GC the builtin entry cache.
    fn gc_entry_cache(&self, _violetabft_group_id: u64, _to: u64) {}

    fn flush_metrics(&self, _instance: &str) {}
    fn flush_stats(&self) -> Option<CacheStats> {
        None
    }
    fn reset_statistics(&self) {}

    fn stop(&self) {}

    fn dump_stats(&self) -> Result<String>;
}

pub trait VioletaBftLogBatch: lightlike {
    /// Note: `VioletaBftLocalState` won't be fideliod in this call.
    fn applightlike(&mut self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<()>;

    /// Note: `VioletaBftLocalState` won't be fideliod in this call.
    fn applightlike_slice(&mut self, violetabft_group_id: u64, entries: &[Entry]) -> Result<()> {
        self.applightlike(violetabft_group_id, entries.to_vec())
    }

    /// Remove VioletaBft logs in [`from`, `to`) which will be overwritten later.
    fn cut_logs(&mut self, violetabft_group_id: u64, from: u64, to: u64);

    fn put_violetabft_state(&mut self, violetabft_group_id: u64, state: &VioletaBftLocalState) -> Result<()>;

    fn is_empty(&self) -> bool;
}

#[derive(Clone, Copy, Default)]
pub struct CacheStats {
    pub hit: usize,
    pub miss: usize,
    pub cache_size: usize,
}
