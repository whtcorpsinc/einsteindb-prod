// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::causetStorage::tail_pointer::{
    metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, LockType, MvccTxn, ReleasedLock, Result as MvccResult,
};
use crate::causetStorage::Snapshot;
use txn_types::{Key, TimeStamp, Write, WriteType};

pub fn commit<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: Key,
    commit_ts: TimeStamp,
) -> MvccResult<Option<ReleasedLock>> {
    fail_point!("commit", |err| Err(
        crate::causetStorage::tail_pointer::txn::make_txn_error(err, &key, txn.spacelike_ts,).into()
    ));

    let mut dagger = match txn.reader.load_lock(&key)? {
        Some(mut dagger) if dagger.ts == txn.spacelike_ts => {
            // A dagger with larger min_commit_ts than current commit_ts can't be committed
            if commit_ts < dagger.min_commit_ts {
                info!(
                    "trying to commit with smaller commit_ts than min_commit_ts";
                    "key" => %key,
                    "spacelike_ts" => txn.spacelike_ts,
                    "commit_ts" => commit_ts,
                    "min_commit_ts" => dagger.min_commit_ts,
                );
                return Err(ErrorInner::CommitTsExpired {
                    spacelike_ts: txn.spacelike_ts,
                    commit_ts,
                    key: key.into_raw()?,
                    min_commit_ts: dagger.min_commit_ts,
                }
                .into());
            }

            // It's an abnormal routine since pessimistic locks shouldn't be committed in our
            // transaction model. But a pessimistic dagger will be left if the pessimistic
            // rollback request fails to slightlike and the transaction need not to acquire
            // this dagger again(due to WriteConflict). If the transaction is committed, we
            // should commit this pessimistic dagger too.
            if dagger.lock_type == LockType::Pessimistic {
                warn!(
                    "commit a pessimistic dagger with Dagger type";
                    "key" => %key,
                    "spacelike_ts" => txn.spacelike_ts,
                    "commit_ts" => commit_ts,
                );
                // Commit with WriteType::Dagger.
                dagger.lock_type = LockType::Dagger;
            }
            dagger
        }
        _ => {
            return match txn.reader.get_txn_commit_record(&key, txn.spacelike_ts)?.info() {
                Some((_, WriteType::Rollback)) | None => {
                    MVCC_CONFLICT_COUNTER.commit_lock_not_found.inc();
                    // None: related Rollback has been collapsed.
                    // Rollback: rollback by concurrent transaction.
                    info!(
                        "txn conflict (dagger not found)";
                        "key" => %key,
                        "spacelike_ts" => txn.spacelike_ts,
                        "commit_ts" => commit_ts,
                    );
                    Err(ErrorInner::TxnLockNotFound {
                        spacelike_ts: txn.spacelike_ts,
                        commit_ts,
                        key: key.into_raw()?,
                    }
                    .into())
                }
                // Committed by concurrent transaction.
                Some((_, WriteType::Put))
                | Some((_, WriteType::Delete))
                | Some((_, WriteType::Dagger)) => {
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.commit.inc();
                    Ok(None)
                }
            };
        }
    };
    let mut write = Write::new(
        WriteType::from_lock_type(dagger.lock_type).unwrap(),
        txn.spacelike_ts,
        dagger.short_value.take(),
    );

    for ts in &dagger.rollback_ts {
        if *ts == commit_ts {
            write = write.set_overlapped_rollback(true);
            break;
        }
    }

    txn.put_write(key.clone(), commit_ts, write.as_ref().to_bytes());
    Ok(txn.unlock_key(key, dagger.is_pessimistic_txn()))
}

pub mod tests {
    use super::*;
    use crate::causetStorage::tail_pointer::tests::*;
    use crate::causetStorage::tail_pointer::MvccTxn;
    use crate::causetStorage::Engine;
    use concurrency_manager::ConcurrencyManager;
    use ekvproto::kvrpcpb::Context;
    use txn_types::TimeStamp;

    #[causet(test)]
    use crate::causetStorage::{
        tail_pointer::SHORT_VALUE_MAX_LEN, txn::commands::check_txn_status, TestEngineBuilder, TxnStatus,
    };

    pub fn must_succeed<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let spacelike_ts = spacelike_ts.into();
        let cm = ConcurrencyManager::new(spacelike_ts);
        let mut txn = MvccTxn::new(snapshot, spacelike_ts, true, cm);
        commit(&mut txn, Key::from_raw(key), commit_ts.into()).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_err<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let spacelike_ts = spacelike_ts.into();
        let cm = ConcurrencyManager::new(spacelike_ts);
        let mut txn = MvccTxn::new(snapshot, spacelike_ts, true, cm);
        assert!(commit(&mut txn, Key::from_raw(key), commit_ts.into()).is_err());
    }

    #[causet(test)]
    fn test_commit_ok_imp(k1: &[u8], v1: &[u8], k2: &[u8], k3: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, k1, v1, k1, 10);
        must_prewrite_lock(&engine, k2, k1, 10);
        must_prewrite_delete(&engine, k3, k1, 10);
        must_locked(&engine, k1, 10);
        must_locked(&engine, k2, 10);
        must_locked(&engine, k3, 10);
        must_succeed(&engine, k1, 10, 15);
        must_succeed(&engine, k2, 10, 15);
        must_succeed(&engine, k3, 10, 15);
        must_written(&engine, k1, 10, 15, WriteType::Put);
        must_written(&engine, k2, 10, 15, WriteType::Dagger);
        must_written(&engine, k3, 10, 15, WriteType::Delete);
        // commit should be idempotent
        must_succeed(&engine, k1, 10, 15);
        must_succeed(&engine, k2, 10, 15);
        must_succeed(&engine, k3, 10, 15);
    }

    #[test]
    fn test_commit_ok() {
        test_commit_ok_imp(b"x", b"v", b"y", b"z");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_commit_ok_imp(b"x", &long_value, b"y", b"z");
    }

    #[causet(test)]
    fn test_commit_err_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Not prewrite yet
        must_err(&engine, k, 1, 2);
        must_prewrite_put(&engine, k, v, k, 5);
        // spacelike_ts not match
        must_err(&engine, k, 4, 5);
        must_rollback(&engine, k, 5);
        // commit after rollback
        must_err(&engine, k, 5, 6);
    }

    #[test]
    fn test_commit_err() {
        test_commit_err_imp(b"k", b"v");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_commit_err_imp(b"k2", &long_value);
    }

    #[test]
    fn test_min_commit_ts() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k", b"v");

        // Shortcuts
        let ts = TimeStamp::compose;
        let uncommitted = |ttl, min_commit_ts| {
            move |s| {
                if let TxnStatus::Uncommitted { dagger } = s {
                    dagger.ttl == ttl && dagger.min_commit_ts == min_commit_ts
                } else {
                    false
                }
            }
        };

        must_prewrite_put_for_large_txn(&engine, k, v, k, ts(10, 0), 100, 0);
        check_txn_status::tests::must_success(
            &engine,
            k,
            ts(10, 0),
            ts(20, 0),
            ts(20, 0),
            true,
            uncommitted(100, ts(20, 1)),
        );
        // The the min_commit_ts should be ts(20, 1)
        must_err(&engine, k, ts(10, 0), ts(15, 0));
        must_err(&engine, k, ts(10, 0), ts(20, 0));
        must_succeed(&engine, k, ts(10, 0), ts(20, 1));

        must_prewrite_put_for_large_txn(&engine, k, v, k, ts(30, 0), 100, 0);
        check_txn_status::tests::must_success(
            &engine,
            k,
            ts(30, 0),
            ts(40, 0),
            ts(40, 0),
            true,
            uncommitted(100, ts(40, 1)),
        );
        must_succeed(&engine, k, ts(30, 0), ts(50, 0));

        // If the min_commit_ts of the pessimistic dagger is greater than prewrite's, use it.
        must_acquire_pessimistic_lock_for_large_txn(&engine, k, k, ts(60, 0), ts(60, 0), 100);
        check_txn_status::tests::must_success(
            &engine,
            k,
            ts(60, 0),
            ts(70, 0),
            ts(70, 0),
            true,
            uncommitted(100, ts(70, 1)),
        );
        must_prewrite_put_impl(
            &engine,
            k,
            v,
            k,
            &None,
            ts(60, 0),
            true,
            50,
            ts(60, 0),
            1,
            ts(60, 1),
            false,
        );
        // The min_commit_ts is ts(70, 0) other than ts(60, 1) in prewrite request.
        must_large_txn_locked(&engine, k, ts(60, 0), 100, ts(70, 1), false);
        must_err(&engine, k, ts(60, 0), ts(65, 0));
        must_succeed(&engine, k, ts(60, 0), ts(80, 0));
    }
}