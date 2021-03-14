// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::causetStorage::kv::WriteData;
use crate::causetStorage::lock_manager::LockManager;
use crate::causetStorage::tail_pointer::MvccTxn;
use crate::causetStorage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::causetStorage::txn::Result;
use crate::causetStorage::{ProcessResult, Snapshot};
use txn_types::{Key, TimeStamp};

command! {
    /// Rollback from the transaction that was spacelikeed at `spacelike_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite) on the given key.
    Rollback:
        cmd_ty => (),
        display => "kv::command::rollback tuplespaceInstanton({}) @ {} | {:?}", (tuplespaceInstanton.len, spacelike_ts, ctx),
        content => {
            tuplespaceInstanton: Vec<Key>,
            /// The transaction timestamp.
            spacelike_ts: TimeStamp,
        }
}

impl CommandExt for Rollback {
    ctx!();
    tag!(rollback);
    ts!(spacelike_ts);
    write_bytes!(tuplespaceInstanton: multiple);
    gen_lock!(tuplespaceInstanton: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Rollback {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.spacelike_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let events = self.tuplespaceInstanton.len();
        let mut released_locks = ReleasedLocks::new(self.spacelike_ts, TimeStamp::zero());
        for k in self.tuplespaceInstanton {
            released_locks.push(txn.rollback(k)?);
        }
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            events,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
        })
    }
}
