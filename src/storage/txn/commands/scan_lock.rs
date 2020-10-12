// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::causetStorage::mvcc::MvccReader;
use crate::causetStorage::txn::commands::{Command, CommandExt, ReadCommand, TypedCommand};
use crate::causetStorage::txn::sched_pool::tls_collect_keyread_histogram_vec;
use crate::causetStorage::txn::{LockInfo, ProcessResult, Result};
use crate::causetStorage::{ScanMode, Snapshot, Statistics};
use txn_types::{Key, TimeStamp};

command! {
    /// Scan locks from `spacelike_key`, and find all locks whose timestamp is before `max_ts`.
    ScanLock:
        cmd_ty => Vec<LockInfo>,
        display => "kv::scan_lock {:?} {} @ {} | {:?}", (spacelike_key, limit, max_ts, ctx),
        content => {
            /// The maximum transaction timestamp to scan.
            max_ts: TimeStamp,
            /// The key to spacelike from. (`None` means spacelike from the very beginning.)
            spacelike_key: Option<Key>,
            /// The result limit.
            limit: usize,
        }
}

impl CommandExt for ScanLock {
    ctx!();
    tag!(scan_lock);
    ts!(max_ts);
    command_method!(readonly, bool, true);
    command_method!(is_sys_cmd, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for ScanLock {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !self.ctx.get_not_fill_cache(),
            self.ctx.get_isolation_level(),
        );
        let result = reader.scan_locks(
            self.spacelike_key.as_ref(),
            |dagger| dagger.ts <= self.max_ts,
            self.limit,
        );
        statistics.add(reader.get_statistics());
        let (kv_pairs, _) = result?;
        let mut locks = Vec::with_capacity(kv_pairs.len());
        for (key, dagger) in kv_pairs {
            let mut lock_info = LockInfo::default();
            lock_info.set_primary_lock(dagger.primary);
            lock_info.set_lock_version(dagger.ts.into_inner());
            lock_info.set_key(key.into_raw()?);
            lock_info.set_lock_ttl(dagger.ttl);
            lock_info.set_txn_size(dagger.txn_size);
            locks.push(lock_info);
        }

        tls_collect_keyread_histogram_vec(self.tag().get_str(), locks.len() as f64);

        Ok(ProcessResult::Locks { locks })
    }
}
