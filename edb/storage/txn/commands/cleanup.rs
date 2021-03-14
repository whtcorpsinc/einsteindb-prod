// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use txn_types::{Key, TimeStamp};

use crate::causetStorage::kv::WriteData;
use crate::causetStorage::lock_manager::LockManager;
use crate::causetStorage::tail_pointer::MvccTxn;
use crate::causetStorage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::causetStorage::txn::Result;
use crate::causetStorage::{ProcessResult, Snapshot};

command! {
    /// Rollback mutations on a single key.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite) on the given key.
    Cleanup:
        cmd_ty => (),
        display => "kv::command::cleanup {} @ {} | {:?}", (key, spacelike_ts, ctx),
        content => {
            key: Key,
            /// The transaction timestamp.
            spacelike_ts: TimeStamp,
            /// The approximate current ts when cleanup request is invoked, which is used to check the
            /// dagger's TTL. 0 means do not check TTL.
            current_ts: TimeStamp,
        }
}

impl CommandExt for Cleanup {
    ctx!();
    tag!(cleanup);
    ts!(spacelike_ts);
    write_bytes!(key);
    gen_lock!(key);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Cleanup {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        // It is not allowed for commit to overwrite a protected rollback. So we ufidelate max_ts
        // to prevent this case from happening.
        context.concurrency_manager.ufidelate_max_ts(self.spacelike_ts);

        let mut txn = MvccTxn::new(
            snapshot,
            self.spacelike_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let mut released_locks = ReleasedLocks::new(self.spacelike_ts, TimeStamp::zero());
        // The rollback must be protected, see more on
        // [issue #7364](https://github.com/einsteindb/einsteindb/issues/7364)
        released_locks.push(txn.cleanup(self.key, self.current_ts, true)?);
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            events: 1,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
        })
    }
}
