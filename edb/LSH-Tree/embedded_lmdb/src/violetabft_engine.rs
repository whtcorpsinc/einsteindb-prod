use crate::{LmdbEngine, LmdbWriteBatch};

use edb::{Error, VioletaBftEngine, VioletaBftLogBatch, Result};
use edb::{
    Iterable, CausetEngine, MiscExt, MuBlock, Peekable, SyncMuBlock, WriteBatchExt, WriteOptions,
    Causet_DEFAULT, MAX_DELETE_BATCH_COUNT,
};
use ekvproto::violetabft_server_timeshare::VioletaBftLocalState;
use protobuf::Message;
use violetabft::evioletabft_timeshare::Entry;

const VIOLETABFT_LOG_MULTI_GET_CNT: u64 = 8;

impl VioletaBftEngine for LmdbEngine {
    type LogBatch = LmdbWriteBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch {
        LmdbWriteBatch::with_capacity(self.as_inner().clone(), capacity)
    }

    fn sync(&self) -> Result<()> {
        self.sync_wal()
    }

    fn get_violetabft_state(&self, violetabft_group_id: u64) -> Result<Option<VioletaBftLocalState>> {
        let key = tuplespaceInstanton::violetabft_state_key(violetabft_group_id);
        self.get_msg_causet(Causet_DEFAULT, &key)
    }

    fn get_entry(&self, violetabft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        let key = tuplespaceInstanton::violetabft_log_key(violetabft_group_id, index);
        self.get_msg_causet(Causet_DEFAULT, &key)
    }

    fn fetch_entries_to(
        &self,
        brane_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>,
        buf: &mut Vec<Entry>,
    ) -> Result<usize> {
        let (max_size, mut total_size, mut count) = (max_size.unwrap_or(usize::MAX), 0, 0);

        if high - low <= VIOLETABFT_LOG_MULTI_GET_CNT {
            // If election happens in inactive branes, they will just try to fetch one empty log.
            for i in low..high {
                if total_size > 0 && total_size >= max_size {
                    break;
                }
                let key = tuplespaceInstanton::violetabft_log_key(brane_id, i);
                match self.get_value(&key) {
                    Ok(None) => return Err(Error::EntriesCompacted),
                    Ok(Some(v)) => {
                        let mut entry = Entry::default();
                        entry.merge_from_bytes(&v)?;
                        assert_eq!(entry.get_index(), i);
                        buf.push(entry);
                        total_size += v.len();
                        count += 1;
                    }
                    Err(e) => return Err(box_err!(e)),
                }
            }
            return Ok(count);
        }

        let (mut check_compacted, mut next_index) = (true, low);
        let spacelike_key = tuplespaceInstanton::violetabft_log_key(brane_id, low);
        let lightlike_key = tuplespaceInstanton::violetabft_log_key(brane_id, high);
        self.scan(
            &spacelike_key,
            &lightlike_key,
            true, // fill_cache
            |_, value| {
                let mut entry = Entry::default();
                entry.merge_from_bytes(value)?;

                if check_compacted {
                    if entry.get_index() != low {
                        // May meet gap or has been compacted.
                        return Ok(false);
                    }
                    check_compacted = false;
                } else {
                    assert_eq!(entry.get_index(), next_index);
                }
                next_index += 1;

                buf.push(entry);
                total_size += value.len();
                count += 1;
                Ok(total_size < max_size)
            },
        )?;

        // If we get the correct number of entries, returns.
        // Or the total size almost exceeds max_size, returns.
        if count == (high - low) as usize || total_size >= max_size {
            return Ok(count);
        }

        // Here means we don't fetch enough entries.
        Err(Error::EntriesUnavailable)
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync_log: bool) -> Result<usize> {
        let bytes = batch.data_size();
        let mut opts = WriteOptions::default();
        opts.set_sync(sync_log);
        self.write_opt(batch, &opts)?;
        batch.clear();
        Ok(bytes)
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync_log: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<usize> {
        let data_size = self.consume(batch, sync_log)?;
        if data_size > max_capacity {
            *batch = self.write_batch_with_cap(shrink_to);
        }
        Ok(data_size)
    }

    fn clean(
        &self,
        violetabft_group_id: u64,
        state: &VioletaBftLocalState,
        batch: &mut LmdbWriteBatch,
    ) -> Result<()> {
        batch.delete(&tuplespaceInstanton::violetabft_state_key(violetabft_group_id))?;
        let seek_key = tuplespaceInstanton::violetabft_log_key(violetabft_group_id, 0);
        let prefix = tuplespaceInstanton::violetabft_log_prefix(violetabft_group_id);
        if let Some((key, _)) = self.seek(&seek_key)? {
            if !key.spacelikes_with(&prefix) {
                // No violetabft logs for the violetabft group.
                return Ok(());
            }
            let first_index = match tuplespaceInstanton::violetabft_log_index(&key) {
                Ok(index) => index,
                Err(_) => return Ok(()),
            };
            for index in first_index..=state.last_index {
                let key = tuplespaceInstanton::violetabft_log_key(violetabft_group_id, index);
                batch.delete(&key)?;
            }
        }
        Ok(())
    }

    fn applightlike(&self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        self.applightlike_slice(violetabft_group_id, &entries)
    }

    fn applightlike_slice(&self, violetabft_group_id: u64, entries: &[Entry]) -> Result<usize> {
        let mut wb = LmdbWriteBatch::new(self.as_inner().clone());
        let buf = Vec::with_capacity(1024);
        wb.applightlike_impl(violetabft_group_id, entries, buf)?;
        self.consume(&mut wb, false)
    }

    fn put_violetabft_state(&self, violetabft_group_id: u64, state: &VioletaBftLocalState) -> Result<()> {
        self.put_msg(&tuplespaceInstanton::violetabft_state_key(violetabft_group_id), state)
    }

    fn gc(&self, violetabft_group_id: u64, mut from: u64, to: u64) -> Result<usize> {
        if from >= to {
            return Ok(0);
        }
        if from == 0 {
            let spacelike_key = tuplespaceInstanton::violetabft_log_key(violetabft_group_id, 0);
            let prefix = tuplespaceInstanton::violetabft_log_prefix(violetabft_group_id);
            match self.seek(&spacelike_key)? {
                Some((k, _)) if k.spacelikes_with(&prefix) => from = box_try!(tuplespaceInstanton::violetabft_log_index(&k)),
                // No need to gc.
                _ => return Ok(0),
            }
        }

        let mut violetabft_wb = self.write_batch_with_cap(4 * 1024);
        for idx in from..to {
            let key = tuplespaceInstanton::violetabft_log_key(violetabft_group_id, idx);
            violetabft_wb.delete(&key)?;
            if violetabft_wb.count() >= MAX_DELETE_BATCH_COUNT {
                self.write(&violetabft_wb)?;
                violetabft_wb.clear();
            }
        }

        // TODO: disable WAL here.
        if !MuBlock::is_empty(&violetabft_wb) {
            self.write(&violetabft_wb)?;
        }
        Ok((to - from) as usize)
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        Ok(vec![])
    }

    fn has_builtin_entry_cache(&self) -> bool {
        false
    }

    fn flush_metrics(&self, instance: &str) {
        CausetEngine::flush_metrics(self, instance)
    }
    fn reset_statistics(&self) {
        CausetEngine::reset_statistics(self)
    }

    fn dump_stats(&self) -> Result<String> {
        MiscExt::dump_stats(self)
    }
}

impl VioletaBftLogBatch for LmdbWriteBatch {
    fn applightlike(&mut self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        self.applightlike_slice(violetabft_group_id, &entries)
    }

    fn applightlike_slice(&mut self, violetabft_group_id: u64, entries: &[Entry]) -> Result<()> {
        if let Some(max_size) = entries.iter().map(|e| e.compute_size()).max() {
            let ser_buf = Vec::with_capacity(max_size as usize);
            return self.applightlike_impl(violetabft_group_id, entries, ser_buf);
        }
        Ok(())
    }

    fn cut_logs(&mut self, violetabft_group_id: u64, from: u64, to: u64) {
        for index in from..to {
            let key = tuplespaceInstanton::violetabft_log_key(violetabft_group_id, index);
            self.delete(&key).unwrap();
        }
    }

    fn put_violetabft_state(&mut self, violetabft_group_id: u64, state: &VioletaBftLocalState) -> Result<()> {
        self.put_msg(&tuplespaceInstanton::violetabft_state_key(violetabft_group_id), state)
    }

    fn is_empty(&self) -> bool {
        MuBlock::is_empty(self)
    }
}

impl LmdbWriteBatch {
    fn applightlike_impl(
        &mut self,
        violetabft_group_id: u64,
        entries: &[Entry],
        mut ser_buf: Vec<u8>,
    ) -> Result<()> {
        for entry in entries {
            let key = tuplespaceInstanton::violetabft_log_key(violetabft_group_id, entry.get_index());
            ser_buf.clear();
            entry.write_to_vec(&mut ser_buf).unwrap();
            self.put(&key, &ser_buf)?;
        }
        Ok(())
    }
}
