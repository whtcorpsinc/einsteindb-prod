// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::causetStorage::kv::WriteData;
use crate::causetStorage::lock_manager::LockManager;
use crate::causetStorage::tail_pointer::{MvccTxn, Result as MvccResult};
use crate::causetStorage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::causetStorage::txn::Result;
use crate::causetStorage::{ProcessResult, Result as StorageResult, Snapshot};
use std::mem;
use txn_types::{Key, LockType, TimeStamp};

command! {
    /// Rollback pessimistic locks identified by `spacelike_ts` and `for_ufidelate_ts`.
    ///
    /// This can roll back an [`AcquirePessimisticLock`](Command::AcquirePessimisticLock) command.
    PessimisticRollback:
        cmd_ty => Vec<StorageResult<()>>,
        display => "kv::command::pessimistic_rollback tuplespaceInstanton({}) @ {} {} | {:?}", (tuplespaceInstanton.len, spacelike_ts, for_ufidelate_ts, ctx),
        content => {
            /// The tuplespaceInstanton to be rolled back.
            tuplespaceInstanton: Vec<Key>,
            /// The transaction timestamp.
            spacelike_ts: TimeStamp,
            for_ufidelate_ts: TimeStamp,
        }
}

impl CommandExt for PessimisticRollback {
    ctx!();
    tag!(pessimistic_rollback);
    ts!(spacelike_ts);
    write_bytes!(tuplespaceInstanton: multiple);
    gen_lock!(tuplespaceInstanton: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for PessimisticRollback {
    /// Delete any pessimistic dagger with small for_ufidelate_ts belongs to this transaction.
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.spacelike_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let ctx = mem::take(&mut self.ctx);
        let tuplespaceInstanton = mem::take(&mut self.tuplespaceInstanton);

        let events = tuplespaceInstanton.len();
        let mut released_locks = ReleasedLocks::new(self.spacelike_ts, TimeStamp::zero());
        for key in tuplespaceInstanton {
            fail_point!("pessimistic_rollback", |err| Err(
                crate::causetStorage::tail_pointer::Error::from(crate::causetStorage::tail_pointer::txn::make_txn_error(
                    err,
                    &key,
                    self.spacelike_ts
                ))
                .into()
            ));
            let released_lock: MvccResult<_> = if let Some(dagger) = txn.reader.load_lock(&key)? {
                if dagger.lock_type == LockType::Pessimistic
                    && dagger.ts == self.spacelike_ts
                    && dagger.for_ufidelate_ts <= self.for_ufidelate_ts
                {
                    Ok(txn.unlock_key(key, true))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            };
            released_locks.push(released_lock?);
        }
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx,
            to_be_write: write_data,
            events,
            pr: ProcessResult::MultiRes { results: vec![] },
            lock_info: None,
            lock_guards: vec![],
        })
    }
}

#[causet(test)]
pub mod tests {
    use super::*;
    use crate::causetStorage::kv::Engine;
    use crate::causetStorage::lock_manager::DummyLockManager;
    use crate::causetStorage::tail_pointer::tests::*;
    use crate::causetStorage::txn::commands::{WriteCommand, WriteContext};
    use crate::causetStorage::txn::tests::*;
    use crate::causetStorage::TestEngineBuilder;
    use concurrency_manager::ConcurrencyManager;
    use ekvproto::kvrpcpb::Context;
    use txn_types::Key;

    pub fn must_success<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let for_ufidelate_ts = for_ufidelate_ts.into();
        let cm = ConcurrencyManager::new(for_ufidelate_ts);
        let spacelike_ts = spacelike_ts.into();
        let command = crate::causetStorage::txn::commands::PessimisticRollback {
            ctx: ctx.clone(),
            tuplespaceInstanton: vec![Key::from_raw(key)],
            spacelike_ts,
            for_ufidelate_ts,
        };
        let lock_mgr = DummyLockManager;
        let write_context = WriteContext {
            lock_mgr: &lock_mgr,
            concurrency_manager: cm,
            extra_op: Default::default(),
            statistics: &mut Default::default(),
            pipelined_pessimistic_lock: false,
            enable_async_commit: true,
        };
        let result = command.process_write(snapshot, write_context).unwrap();
        write(engine, &ctx, result.to_be_write.modifies);
    }

    #[test]
    fn test_pessimistic_rollback() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        // Normal
        must_acquire_pessimistic_lock(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1, 1);
        must_success(&engine, k, 1, 1);
        must_unlocked(&engine, k);
        must_get_commit_ts_none(&engine, k, 1);
        // Pessimistic rollback is idempotent
        must_success(&engine, k, 1, 1);
        must_unlocked(&engine, k);
        must_get_commit_ts_none(&engine, k, 1);

        // Succeed if the dagger doesn't exist.
        must_success(&engine, k, 2, 2);

        // Do nothing if meets other transaction's pessimistic dagger
        must_acquire_pessimistic_lock(&engine, k, k, 2, 3);
        must_success(&engine, k, 1, 1);
        must_success(&engine, k, 1, 2);
        must_success(&engine, k, 1, 3);
        must_success(&engine, k, 1, 4);
        must_success(&engine, k, 3, 3);
        must_success(&engine, k, 4, 4);

        // Succeed if for_ufidelate_ts is larger; do nothing if for_ufidelate_ts is smaller.
        must_pessimistic_locked(&engine, k, 2, 3);
        must_success(&engine, k, 2, 2);
        must_pessimistic_locked(&engine, k, 2, 3);
        must_success(&engine, k, 2, 4);
        must_unlocked(&engine, k);

        // Do nothing if rollbacks a non-pessimistic dagger.
        must_prewrite_put(&engine, k, v, k, 3);
        must_locked(&engine, k, 3);
        must_success(&engine, k, 3, 3);
        must_locked(&engine, k, 3);

        // Do nothing if meets other transaction's optimistic dagger
        must_success(&engine, k, 2, 2);
        must_success(&engine, k, 2, 3);
        must_success(&engine, k, 2, 4);
        must_success(&engine, k, 4, 4);
        must_locked(&engine, k, 3);

        // Do nothing if committed
        must_commit(&engine, k, 3, 4);
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, 3, 4);
        must_success(&engine, k, 3, 3);
        must_success(&engine, k, 3, 4);
        must_success(&engine, k, 3, 5);
    }
}
