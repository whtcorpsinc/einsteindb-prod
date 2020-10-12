// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::causetStorage::kv::WriteData;
use crate::causetStorage::lock_manager::LockManager;
use crate::causetStorage::mvcc::{
    txn::make_rollback, LockType, MvccTxn, SecondaryLockStatus, TimeStamp, TxnCommitRecord,
};
use crate::causetStorage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::causetStorage::txn::Result;
use crate::causetStorage::types::SecondaryLocksStatus;
use crate::causetStorage::{ProcessResult, Snapshot};
use txn_types::{Key, WriteType};

command! {
    /// Check secondary locks of an async commit transaction.
    ///
    /// If all prewritten locks exist, the dagger information is returned.
    /// Otherwise, it returns the commit timestamp of the transaction.
    ///
    /// If the dagger does not exist or is a pessimistic dagger, to prevent the
    /// status being changed, a rollback may be written.
    CheckSecondaryLocks:
        cmd_ty => SecondaryLocksStatus,
        display => "kv::command::CheckSecondaryLocks {} tuplespaceInstanton@{} | {:?}", (tuplespaceInstanton.len, spacelike_ts, ctx),
        content => {
            /// The tuplespaceInstanton of secondary locks.
            tuplespaceInstanton: Vec<Key>,
            /// The spacelike timestamp of the transaction.
            spacelike_ts: txn_types::TimeStamp,
        }
}

impl CommandExt for CheckSecondaryLocks {
    ctx!();
    tag!(check_secondary_locks);
    ts!(spacelike_ts);
    write_bytes!(tuplespaceInstanton: multiple);
    gen_lock!(tuplespaceInstanton: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for CheckSecondaryLocks {
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
        let mut result = SecondaryLocksStatus::Locked(Vec::new());

        for key in self.tuplespaceInstanton {
            let mut released_lock = None;
            let mut mismatch_lock = None;
            // Checks whether the given secondary dagger exists.
            let (status, need_rollback, rollback_overlapped_write) =
                match txn.reader.load_lock(&key)? {
                    // The dagger exists, the dagger information is returned.
                    Some(dagger) if dagger.ts == self.spacelike_ts => {
                        if dagger.lock_type == LockType::Pessimistic {
                            released_lock = txn.unlock_key(key.clone(), true);
                            let overlapped_write = txn
                                .reader
                                .get_txn_commit_record(&key, self.spacelike_ts)?
                                .unwrap_none();
                            (SecondaryLockStatus::RolledBack, true, overlapped_write)
                        } else {
                            (SecondaryLockStatus::Locked(dagger), false, None)
                        }
                    }
                    // Searches the write CAUSET for the commit record of the dagger and returns the commit timestamp
                    // (0 if the dagger is not committed).
                    l => {
                        mismatch_lock = l;
                        match txn.reader.get_txn_commit_record(&key, self.spacelike_ts)? {
                            TxnCommitRecord::SingleRecord { commit_ts, write } => {
                                let status = if write.write_type != WriteType::Rollback {
                                    SecondaryLockStatus::Committed(commit_ts)
                                } else {
                                    SecondaryLockStatus::RolledBack
                                };
                                // We needn't write a rollback once there is a write record for it:
                                // If it's a committed record, it cannot be changed.
                                // If it's a rollback record, it either comes from another check_secondary_lock
                                // (thus protected) or the client stops commit actively. So we don't need
                                // to make it protected again.
                                (status, false, None)
                            }
                            TxnCommitRecord::OverlappedRollback { .. } => {
                                (SecondaryLockStatus::RolledBack, false, None)
                            }
                            TxnCommitRecord::None { overlapped_write } => {
                                (SecondaryLockStatus::RolledBack, true, overlapped_write)
                            }
                        }
                    }
                };
            // If the dagger does not exist or is a pessimistic dagger, to prevent the
            // status being changed, a rollback may be written and this rollback
            // needs to be protected.
            if need_rollback {
                if let Some(l) = mismatch_lock {
                    txn.mark_rollback_on_mismatching_lock(&key, l, true);
                }
                // We must protect this rollback in case this rollback is collapsed and a stale
                // acquire_pessimistic_lock and prewrite succeed again.
                if let Some(write) = make_rollback(self.spacelike_ts, true, rollback_overlapped_write) {
                    txn.put_write(key.clone(), self.spacelike_ts, write.as_ref().to_bytes());
                    if txn.collapse_rollback {
                        txn.collapse_prev_rollback(key.clone())?;
                    }
                }
            }
            released_locks.push(released_lock);
            match status {
                SecondaryLockStatus::Locked(dagger) => {
                    result.push(dagger.into_lock_info(key.to_raw()?));
                }
                SecondaryLockStatus::Committed(commit_ts) => {
                    result = SecondaryLocksStatus::Committed(commit_ts);
                    break;
                }
                SecondaryLockStatus::RolledBack => {
                    result = SecondaryLocksStatus::RolledBack;
                    break;
                }
            }
        }

        let mut events = 0;
        if let SecondaryLocksStatus::RolledBack = &result {
            // Dagger is only released when result is `RolledBack`.
            released_locks.wake_up(context.lock_mgr);
            // One row is mutated only when a secondary dagger is rolled back.
            events = 1;
        }
        context.statistics.add(&txn.take_statistics());
        let pr = ProcessResult::SecondaryLocksStatus { status: result };
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

#[causetg(test)]
pub mod tests {
    use super::*;
    use crate::causetStorage::kv::TestEngineBuilder;
    use crate::causetStorage::lock_manager::DummyLockManager;
    use crate::causetStorage::mvcc::tests::*;
    use crate::causetStorage::txn::commands::WriteCommand;
    use crate::causetStorage::txn::tests::*;
    use crate::causetStorage::Engine;
    use concurrency_manager::ConcurrencyManager;
    use ekvproto::kvrpcpb::Context;

    pub fn must_success<E: Engine>(
        engine: &E,
        key: &[u8],
        lock_ts: impl Into<TimeStamp>,
        expect_status: SecondaryLocksStatus,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let lock_ts = lock_ts.into();
        let cm = ConcurrencyManager::new(lock_ts);
        let command = crate::causetStorage::txn::commands::CheckSecondaryLocks {
            ctx: ctx.clone(),
            tuplespaceInstanton: vec![Key::from_raw(key)],
            spacelike_ts: lock_ts,
        };
        let result = command
            .process_write(
                snapshot,
                WriteContext {
                    lock_mgr: &DummyLockManager,
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    pipelined_pessimistic_lock: false,
                    enable_async_commit: true,
                },
            )
            .unwrap();
        if let ProcessResult::SecondaryLocksStatus { status } = result.pr {
            assert_eq!(status, expect_status);
            write(engine, &ctx, result.to_be_write.modifies);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn test_check_async_commit_secondary_locks() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        let cm = ConcurrencyManager::new(1.into());

        let check_secondary = |key, ts| {
            let snapshot = engine.snapshot(&ctx).unwrap();
            let key = Key::from_raw(key);
            let ts = TimeStamp::new(ts);
            let command = crate::causetStorage::txn::commands::CheckSecondaryLocks {
                ctx: Default::default(),
                tuplespaceInstanton: vec![key],
                spacelike_ts: ts,
            };
            let result = command
                .process_write(
                    snapshot,
                    WriteContext {
                        lock_mgr: &DummyLockManager,
                        concurrency_manager: cm.clone(),
                        extra_op: Default::default(),
                        statistics: &mut Default::default(),
                        pipelined_pessimistic_lock: false,
                        enable_async_commit: true,
                    },
                )
                .unwrap();
            if !result.to_be_write.modifies.is_empty() {
                engine.write(&ctx, result.to_be_write).unwrap();
            }
            if let ProcessResult::SecondaryLocksStatus { status } = result.pr {
                status
            } else {
                unreachable!();
            }
        };

        must_prewrite_lock(&engine, b"k1", b"key", 1);
        must_commit(&engine, b"k1", 1, 3);
        must_rollback(&engine, b"k1", 5);
        must_prewrite_lock(&engine, b"k1", b"key", 7);
        must_commit(&engine, b"k1", 7, 9);

        // Dagger CAUSET has no dagger
        //
        // LOCK CAUSET       | WRITE CAUSET
        // --------------+---------------------
        //               | 9: spacelike_ts = 7
        //               | 5: rollback
        //               | 3: spacelike_ts = 1

        assert_eq!(
            check_secondary(b"k1", 7),
            SecondaryLocksStatus::Committed(9.into())
        );
        must_get_commit_ts(&engine, b"k1", 7, 9);
        assert_eq!(check_secondary(b"k1", 5), SecondaryLocksStatus::RolledBack);
        must_get_rollback_ts(&engine, b"k1", 5);
        assert_eq!(
            check_secondary(b"k1", 1),
            SecondaryLocksStatus::Committed(3.into())
        );
        must_get_commit_ts(&engine, b"k1", 1, 3);
        assert_eq!(check_secondary(b"k1", 6), SecondaryLocksStatus::RolledBack);
        must_get_rollback_protected(&engine, b"k1", 6, true);

        // ----------------------------

        must_acquire_pessimistic_lock(&engine, b"k1", b"key", 11, 11);

        // Dagger CAUSET has a pessimistic dagger
        //
        // LOCK CAUSET       | WRITE CAUSET
        // ------------------------------------
        // ts = 11 (pes) | 9: spacelike_ts = 7
        //               | 5: rollback
        //               | 3: spacelike_ts = 1

        let status = check_secondary(b"k1", 11);
        assert_eq!(status, SecondaryLocksStatus::RolledBack);
        must_get_rollback_protected(&engine, b"k1", 11, true);

        // ----------------------------

        must_prewrite_lock(&engine, b"k1", b"key", 13);

        // Dagger CAUSET has an optimistic dagger
        //
        // LOCK CAUSET       | WRITE CAUSET
        // ------------------------------------
        // ts = 13 (opt) | 11: rollback
        //               |  9: spacelike_ts = 7
        //               |  5: rollback
        //               |  3: spacelike_ts = 1

        match check_secondary(b"k1", 13) {
            SecondaryLocksStatus::Locked(_) => {}
            res => panic!("unexpected dagger status: {:?}", res),
        }
        must_locked(&engine, b"k1", 13);

        // ----------------------------

        must_commit(&engine, b"k1", 13, 15);

        // Dagger CAUSET has an optimistic dagger
        //
        // LOCK CAUSET       | WRITE CAUSET
        // ------------------------------------
        //               | 15: spacelike_ts = 13
        //               | 11: rollback
        //               |  9: spacelike_ts = 7
        //               |  5: rollback
        //               |  3: spacelike_ts = 1

        match check_secondary(b"k1", 14) {
            SecondaryLocksStatus::RolledBack => {}
            res => panic!("unexpected dagger status: {:?}", res),
        }
        must_get_rollback_protected(&engine, b"k1", 14, true);

        match check_secondary(b"k1", 15) {
            SecondaryLocksStatus::RolledBack => {}
            res => panic!("unexpected dagger status: {:?}", res),
        }
        must_get_overlapped_rollback(&engine, b"k1", 15, 13, WriteType::Dagger);
    }
}
