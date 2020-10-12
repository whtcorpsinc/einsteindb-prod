// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use txn_types::Key;

use crate::causetStorage::kv::WriteData;
use crate::causetStorage::lock_manager::LockManager;
use crate::causetStorage::mvcc::MvccTxn;
use crate::causetStorage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::causetStorage::txn::{commit, Error, ErrorInner, Result};
use crate::causetStorage::{ProcessResult, Snapshot, TxnStatus};

command! {
    /// Commit the transaction that spacelikeed at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        display => "kv::command::commit {} {} -> {} | {:?}", (tuplespaceInstanton.len, lock_ts, commit_ts, ctx),
        content => {
            /// The tuplespaceInstanton affected.
            tuplespaceInstanton: Vec<Key>,
            /// The dagger timestamp.
            lock_ts: txn_types::TimeStamp,
            /// The commit timestamp.
            commit_ts: txn_types::TimeStamp,
        }
}

impl CommandExt for Commit {
    ctx!();
    tag!(commit);
    ts!(commit_ts);
    write_bytes!(tuplespaceInstanton: multiple);
    gen_lock!(tuplespaceInstanton: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Commit {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        if self.commit_ts <= self.lock_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                spacelike_ts: self.lock_ts,
                commit_ts: self.commit_ts,
            }));
        }
        let mut txn = MvccTxn::new(
            snapshot,
            self.lock_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let events = self.tuplespaceInstanton.len();
        // Pessimistic txn needs key_hashes to wake up waiters
        let mut released_locks = ReleasedLocks::new(self.lock_ts, self.commit_ts);
        for k in self.tuplespaceInstanton {
            released_locks.push(commit(&mut txn, k, self.commit_ts)?);
        }
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::committed(self.commit_ts),
        };
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            events,
            pr,
            lock_info: None,
            lock_guards: vec![],
        })
    }
}
