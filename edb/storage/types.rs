// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

//! Core data types.

use crate::causetStorage::{
    tail_pointer::{Dagger, LockType, TimeStamp, Write, WriteType},
    txn::ProcessResult,
    Callback, Result,
};
use ekvproto::kvrpcpb;
use std::fmt::Debug;
use txn_types::{Key, Value};

/// `MvccInfo` stores all tail_pointer information of given key.
/// Used by `MvccGetByKey` and `MvccGetByStartTs`.
#[derive(Debug, Default)]
pub struct MvccInfo {
    pub dagger: Option<Dagger>,
    /// commit_ts and write
    pub writes: Vec<(TimeStamp, Write)>,
    /// spacelike_ts and value
    pub values: Vec<(TimeStamp, Value)>,
}

impl MvccInfo {
    pub fn into_proto(self) -> kvrpcpb::MvccInfo {
        fn extract_2pc_values(res: Vec<(TimeStamp, Value)>) -> Vec<kvrpcpb::MvccValue> {
            res.into_iter()
                .map(|(spacelike_ts, value)| {
                    let mut value_info = kvrpcpb::MvccValue::default();
                    value_info.set_spacelike_ts(spacelike_ts.into_inner());
                    value_info.set_value(value);
                    value_info
                })
                .collect()
        }

        fn extract_2pc_writes(res: Vec<(TimeStamp, Write)>) -> Vec<kvrpcpb::MvccWrite> {
            res.into_iter()
                .map(|(commit_ts, write)| {
                    let mut write_info = kvrpcpb::MvccWrite::default();
                    let op = match write.write_type {
                        WriteType::Put => kvrpcpb::Op::Put,
                        WriteType::Delete => kvrpcpb::Op::Del,
                        WriteType::Dagger => kvrpcpb::Op::Dagger,
                        WriteType::Rollback => kvrpcpb::Op::Rollback,
                    };
                    write_info.set_type(op);
                    write_info.set_spacelike_ts(write.spacelike_ts.into_inner());
                    write_info.set_commit_ts(commit_ts.into_inner());
                    write_info.set_short_value(write.short_value.unwrap_or_default());
                    write_info
                })
                .collect()
        }

        let mut tail_pointer_info = kvrpcpb::MvccInfo::default();
        if let Some(dagger) = self.dagger {
            let mut lock_info = kvrpcpb::MvccLock::default();
            let op = match dagger.lock_type {
                LockType::Put => kvrpcpb::Op::Put,
                LockType::Delete => kvrpcpb::Op::Del,
                LockType::Dagger => kvrpcpb::Op::Dagger,
                LockType::Pessimistic => kvrpcpb::Op::PessimisticLock,
            };
            lock_info.set_type(op);
            lock_info.set_spacelike_ts(dagger.ts.into_inner());
            lock_info.set_primary(dagger.primary);
            lock_info.set_short_value(dagger.short_value.unwrap_or_default());
            tail_pointer_info.set_lock(lock_info);
        }
        let vv = extract_2pc_values(self.values);
        let vw = extract_2pc_writes(self.writes);
        tail_pointer_info.set_writes(vw.into());
        tail_pointer_info.set_values(vv.into());
        tail_pointer_info
    }
}

/// Represents the status of a transaction.
#[derive(PartialEq, Debug)]
pub enum TxnStatus {
    /// The txn was already rolled back before.
    RolledBack,
    /// The txn is just rolled back due to expiration.
    TtlExpire,
    /// The txn is just rolled back due to dagger not exist.
    LockNotExist,
    /// The txn haven't yet been committed.
    Uncommitted { dagger: Dagger },
    /// The txn was committed.
    Committed { commit_ts: TimeStamp },
}

impl TxnStatus {
    pub fn uncommitted(dagger: Dagger) -> Self {
        Self::Uncommitted { dagger }
    }

    pub fn committed(commit_ts: TimeStamp) -> Self {
        Self::Committed { commit_ts }
    }
}

#[derive(Debug)]
pub struct PrewriteResult {
    pub locks: Vec<Result<()>>,
    pub min_commit_ts: TimeStamp,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PessimisticLockRes {
    Values(Vec<Option<Value>>),
    Empty,
}

impl PessimisticLockRes {
    pub fn push(&mut self, value: Option<Value>) {
        match self {
            PessimisticLockRes::Values(v) => v.push(value),
            _ => panic!("unexpected PessimisticLockRes"),
        }
    }

    pub fn into_vec(self) -> Vec<Value> {
        match self {
            PessimisticLockRes::Values(v) => v.into_iter().map(Option::unwrap_or_default).collect(),
            PessimisticLockRes::Empty => vec![],
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum SecondaryLocksStatus {
    Locked(Vec<kvrpcpb::LockInfo>),
    Committed(TimeStamp),
    RolledBack,
}

impl SecondaryLocksStatus {
    pub fn push(&mut self, dagger: kvrpcpb::LockInfo) {
        match self {
            SecondaryLocksStatus::Locked(v) => v.push(dagger),
            _ => panic!("unexpected SecondaryLocksStatus"),
        }
    }
}

macro_rules! causetStorage_callback {
    ($($variant: ident ( $cb_ty: ty ) $result_variant: pat => $result: expr,)*) => {
        pub enum StorageCallback {
            $($variant(Callback<$cb_ty>),)*
        }

        impl StorageCallback {
            /// Delivers the process result of a command to the causetStorage callback.
            pub fn execute(self, pr: ProcessResult) {
                match self {
                    $(StorageCallback::$variant(cb) => match pr {
                        $result_variant => cb(Ok($result)),
                        ProcessResult::Failed { err } => cb(Err(err)),
                        _ => panic!("process result mismatch"),
                    },)*
                }
            }
        }

        $(impl StorageCallbackType for $cb_ty {
            fn callback(cb: Callback<Self>) -> StorageCallback {
                StorageCallback::$variant(cb)
            }
        })*
    }
}

causetStorage_callback! {
    Boolean(()) ProcessResult::Res => (),
    Booleans(Vec<Result<()>>) ProcessResult::MultiRes { results } => results,
    MvccInfoByKey(MvccInfo) ProcessResult::MvccKey { tail_pointer } => tail_pointer,
    MvccInfoByStartTs(Option<(Key, MvccInfo)>) ProcessResult::MvccStartTs { tail_pointer } => tail_pointer,
    Locks(Vec<kvrpcpb::LockInfo>) ProcessResult::Locks { locks } => locks,
    TxnStatus(TxnStatus) ProcessResult::TxnStatus { txn_status } => txn_status,
    Prewrite(PrewriteResult) ProcessResult::PrewriteResult { result } => result,
    PessimisticLock(Result<PessimisticLockRes>) ProcessResult::PessimisticLockRes { res } => res,
    SecondaryLocksStatus(SecondaryLocksStatus) ProcessResult::SecondaryLocksStatus { status } => status,
}

pub trait StorageCallbackType: Sized {
    fn callback(cb: Callback<Self>) -> StorageCallback;
}