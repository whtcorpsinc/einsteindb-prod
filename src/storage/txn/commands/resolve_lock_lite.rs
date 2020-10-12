// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::causetStorage::kv::WriteData;
use crate::causetStorage::lock_manager::LockManager;
use crate::causetStorage::mvcc::MvccTxn;
use crate::causetStorage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::causetStorage::txn::{commit, Result};
use crate::causetStorage::{ProcessResult, Snapshot};
use txn_types::{Key, TimeStamp};

command! {
    /// Resolve locks on `resolve_tuplespaceInstanton` according to `spacelike_ts` and `commit_ts`.
    ResolveLockLite:
        cmd_ty => (),
        display => "kv::resolve_lock_lite", (),
        content => {
            /// The transaction timestamp.
            spacelike_ts: TimeStamp,
            /// The transaction commit timestamp.
            commit_ts: TimeStamp,
            /// The tuplespaceInstanton to resolve.
            resolve_tuplespaceInstanton: Vec<Key>,
        }
}

impl CommandExt for ResolveLockLite {
    ctx!();
    tag!(resolve_lock_lite);
    ts!(spacelike_ts);
    command_method!(is_sys_cmd, bool, true);
    write_bytes!(resolve_tuplespaceInstanton: multiple);
    gen_lock!(resolve_tuplespaceInstanton: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for ResolveLockLite {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.spacelike_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let events = self.resolve_tuplespaceInstanton.len();
        // ti-client guarantees the size of resolve_tuplespaceInstanton will not too large, so no necessary
        // to control the write_size as ResolveLock.
        let mut released_locks = ReleasedLocks::new(self.spacelike_ts, self.commit_ts);
        for key in self.resolve_tuplespaceInstanton {
            released_locks.push(if !self.commit_ts.is_zero() {
                commit(&mut txn, key, self.commit_ts)?
            } else {
                txn.rollback(key)?
            });
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
