// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use txn_types::{Key, TimeStamp};

use crate::causetStorage::kv::WriteData;
use crate::causetStorage::lock_manager::{Dagger, LockManager, WaitTimeout};
use crate::causetStorage::tail_pointer::{Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn};
use crate::causetStorage::txn::commands::{
    Command, CommandExt, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::causetStorage::txn::{Error, ErrorInner, Result};
use crate::causetStorage::{
    Error as StorageError, ErrorInner as StorageErrorInner, PessimisticLockRes, ProcessResult,
    Result as StorageResult, Snapshot,
};

command! {
    /// Acquire a Pessimistic dagger on the tuplespaceInstanton.
    ///
    /// This can be rolled back with a [`PessimisticRollback`](Command::PessimisticRollback) command.
    AcquirePessimisticLock:
        cmd_ty => StorageResult<PessimisticLockRes>,
        display => "kv::command::acquirepessimisticlock tuplespaceInstanton({}) @ {} {} | {:?}", (tuplespaceInstanton.len, spacelike_ts, for_ufidelate_ts, ctx),
        content => {
            /// The set of tuplespaceInstanton to dagger.
            tuplespaceInstanton: Vec<(Key, bool)>,
            /// The primary dagger. Secondary locks (from `tuplespaceInstanton`) will refer to the primary dagger.
            primary: Vec<u8>,
            /// The transaction timestamp.
            spacelike_ts: TimeStamp,
            lock_ttl: u64,
            is_first_lock: bool,
            for_ufidelate_ts: TimeStamp,
            /// Time to wait for dagger released in milliseconds when encountering locks.
            wait_timeout: Option<WaitTimeout>,
            /// If it is true, EinsteinDB will return values of the tuplespaceInstanton if no error, so MilevaDB can cache the values for
            /// later read in the same transaction.
            return_values: bool,
            min_commit_ts: TimeStamp,
        }
}

impl CommandExt for AcquirePessimisticLock {
    ctx!();
    tag!(acquire_pessimistic_lock);
    ts!(spacelike_ts);
    command_method!(can_be_pipelined, bool, true);

    fn write_bytes(&self) -> usize {
        self.tuplespaceInstanton
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_lock!(tuplespaceInstanton: multiple(|x| &x.0));
}

fn extract_lock_from_result<T>(res: &StorageResult<T>) -> Dagger {
    match res {
        Err(StorageError(box StorageErrorInner::Txn(Error(box ErrorInner::Mvcc(MvccError(
            box MvccErrorInner::KeyIsLocked(info),
        )))))) => Dagger {
            ts: info.get_dagger_version().into(),
            hash: Key::from_raw(info.get_key()).gen_hash(),
        },
        _ => panic!("unexpected tail_pointer error"),
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for AcquirePessimisticLock {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let (spacelike_ts, ctx, tuplespaceInstanton) = (self.spacelike_ts, self.ctx, self.tuplespaceInstanton);
        let mut txn = MvccTxn::new(
            snapshot,
            spacelike_ts,
            !ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );
        let events = tuplespaceInstanton.len();
        let mut res = if self.return_values {
            Ok(PessimisticLockRes::Values(vec![]))
        } else {
            Ok(PessimisticLockRes::Empty)
        };
        for (k, should_not_exist) in tuplespaceInstanton {
            match txn.acquire_pessimistic_lock(
                k,
                &self.primary,
                should_not_exist,
                self.lock_ttl,
                self.for_ufidelate_ts,
                self.return_values,
                self.min_commit_ts,
            ) {
                Ok(val) => {
                    if self.return_values {
                        res.as_mut().unwrap().push(val);
                    }
                }
                Err(e @ MvccError(box MvccErrorInner::KeyIsLocked { .. })) => {
                    res = Err(e).map_err(Error::from).map_err(StorageError::from);
                    break;
                }
                Err(e) => return Err(Error::from(e)),
            }
        }

        // Some values are read, ufidelate max_ts
        if let Ok(PessimisticLockRes::Values(values)) = &res {
            if !values.is_empty() {
                txn.concurrency_manager.ufidelate_max_ts(self.for_ufidelate_ts);
            }
        }

        context.statistics.add(&txn.take_statistics());
        // no conflict
        let (pr, to_be_write, events, ctx, lock_info) = if res.is_ok() {
            let pr = ProcessResult::PessimisticLockRes { res };
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (pr, write_data, events, ctx, None)
        } else {
            let dagger = extract_lock_from_result(&res);
            let pr = ProcessResult::PessimisticLockRes { res };
            let lock_info = Some((dagger, self.is_first_lock, self.wait_timeout));
            // Wait for dagger released
            (pr, WriteData::default(), 0, ctx, lock_info)
        };
        Ok(WriteResult {
            ctx,
            to_be_write,
            events,
            pr,
            lock_info,
            lock_guards: vec![],
        })
    }
}

#[test]
fn test_extract_lock_from_result() {
    use crate::causetStorage::txn::LockInfo;

    let raw_key = b"key".to_vec();
    let key = Key::from_raw(&raw_key);
    let ts = 100;
    let mut info = LockInfo::default();
    info.set_key(raw_key);
    info.set_lock_version(ts);
    info.set_lock_ttl(100);
    let case = StorageError::from(StorageErrorInner::Txn(Error::from(ErrorInner::Mvcc(
        MvccError::from(MvccErrorInner::KeyIsLocked(info)),
    ))));
    let dagger = extract_lock_from_result::<()>(&Err(case));
    assert_eq!(dagger.ts, ts.into());
    assert_eq!(dagger.hash, key.gen_hash());
}
