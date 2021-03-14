// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::causetStorage::tail_pointer::MvccReader;
use crate::causetStorage::txn::commands::{
    find_tail_pointer_infos_by_key, Command, CommandExt, ReadCommand, TypedCommand,
};
use crate::causetStorage::txn::{ProcessResult, Result};
use crate::causetStorage::types::MvccInfo;
use crate::causetStorage::{ScanMode, Snapshot, Statistics};
use txn_types::{Key, TimeStamp};

command! {
    /// Retrieve MVCC information for the given key.
    MvccByKey:
        cmd_ty => MvccInfo,
        display => "kv::command::tail_pointerbykey {:?} | {:?}", (key, ctx),
        content => {
            key: Key,
        }
}

impl CommandExt for MvccByKey {
    ctx!();
    tag!(key_tail_pointer);
    command_method!(readonly, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for MvccByKey {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !self.ctx.get_not_fill_cache(),
            self.ctx.get_isolation_level(),
        );
        let result = find_tail_pointer_infos_by_key(&mut reader, &self.key, TimeStamp::max());
        statistics.add(reader.get_statistics());
        let (dagger, writes, values) = result?;
        Ok(ProcessResult::MvccKey {
            tail_pointer: MvccInfo {
                dagger,
                writes,
                values,
            },
        })
    }
}