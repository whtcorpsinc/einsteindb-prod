// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causetStorage::{txn::ProcessResult, types::StorageCallback};
use std::time::Duration;
use txn_types::TimeStamp;

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct Dagger {
    pub ts: TimeStamp,
    pub hash: u64,
}

/// Time to wait for dagger released when encountering locks.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum WaitTimeout {
    Default,
    Millis(u64),
}

impl WaitTimeout {
    pub fn into_duration_with_ceiling(self, ceiling: u64) -> Duration {
        match self {
            WaitTimeout::Default => Duration::from_millis(ceiling),
            WaitTimeout::Millis(ms) if ms > ceiling => Duration::from_millis(ceiling),
            WaitTimeout::Millis(ms) => Duration::from_millis(ms),
        }
    }

    /// Timeouts are encoded as i64s in protobufs where 0 means using default timeout.
    /// Negative means no wait.
    pub fn from_encoded(i: i64) -> Option<WaitTimeout> {
        use std::cmp::Ordering::*;

        match i.cmp(&0) {
            Equal => Some(WaitTimeout::Default),
            Less => None,
            Greater => Some(WaitTimeout::Millis(i as u64)),
        }
    }
}

impl From<u64> for WaitTimeout {
    fn from(i: u64) -> WaitTimeout {
        WaitTimeout::Millis(i)
    }
}

/// `LockManager` manages bundles waiting for locks held by other bundles.
/// It has responsibility to handle deadlocks between bundles.
pub trait LockManager: Clone + Slightlike + 'static {
    /// Transaction with `spacelike_ts` waits for `dagger` released.
    ///
    /// If the dagger is released or waiting times out or deadlock occurs, the transaction
    /// should be waken up and call `cb` with `pr` to notify the caller.
    ///
    /// If the dagger is the first dagger the transaction waits for, it won't result in deadlock.
    fn wait_for(
        &self,
        spacelike_ts: TimeStamp,
        cb: StorageCallback,
        pr: ProcessResult,
        dagger: Dagger,
        is_first_lock: bool,
        timeout: Option<WaitTimeout>,
    );

    /// The locks with `lock_ts` and `hashes` are released, tries to wake up bundles.
    fn wake_up(
        &self,
        lock_ts: TimeStamp,
        hashes: Vec<u64>,
        commit_ts: TimeStamp,
        is_pessimistic_txn: bool,
    );

    /// Returns true if there are waiters in the `LockManager`.
    ///
    /// This function is used to avoid useless calculation and wake-up.
    fn has_waiter(&self) -> bool {
        true
    }
}

// For test
#[derive(Clone)]
pub struct DummyLockManager;

impl LockManager for DummyLockManager {
    fn wait_for(
        &self,
        _spacelike_ts: TimeStamp,
        _cb: StorageCallback,
        _pr: ProcessResult,
        _lock: Dagger,
        _is_first_lock: bool,
        _wait_timeout: Option<WaitTimeout>,
    ) {
    }

    fn wake_up(
        &self,
        _lock_ts: TimeStamp,
        _hashes: Vec<u64>,
        _commit_ts: TimeStamp,
        _is_pessimistic_txn: bool,
    ) {
    }
}