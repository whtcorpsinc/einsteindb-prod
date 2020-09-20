// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::causetStorage::mvcc::MvccReader;
use crate::causetStorage::txn::commands::{
    find_mvcc_infos_by_key, Command, CommandExt, ReadCommand, TypedCommand,
};
use crate::causetStorage::txn::{ProcessResult, Result};
use crate::causetStorage::types::MvccInfo;
use crate::causetStorage::{ScanMode, Snapshot, Statistics};
use txn_types::{Key, TimeStamp};

command! {
    /// Retrieve MVCC info for the first committed key which `spacelike_ts == ts`.
    MvccByStartTs:
        cmd_ty => Option<(Key, MvccInfo)>,
        display => "kv::command::mvccbyspacelikets {:?} | {:?}", (spacelike_ts, ctx),
        content => {
            spacelike_ts: TimeStamp,
        }
}

impl CommandExt for MvccByStartTs {
    ctx!();
    tag!(spacelike_ts_mvcc);
    ts!(spacelike_ts);
    command_method!(readonly, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for MvccByStartTs {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !self.ctx.get_not_fill_cache(),
            self.ctx.get_isolation_level(),
        );
        match reader.seek_ts(self.spacelike_ts)? {
            Some(key) => {
                let result = find_mvcc_infos_by_key(&mut reader, &key, TimeStamp::max());
                statistics.add(reader.get_statistics());
                let (lock, writes, values) = result?;
                Ok(ProcessResult::MvccStartTs {
                    mvcc: Some((
                        key,
                        MvccInfo {
                            lock,
                            writes,
                            values,
                        },
                    )),
                })
            }
            None => Ok(ProcessResult::MvccStartTs { mvcc: None }),
        }
    }
}
