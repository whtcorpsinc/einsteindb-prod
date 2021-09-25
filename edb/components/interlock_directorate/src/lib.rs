// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

//! The concurrency manager is responsible for concurrency control of
//! bundles.
//!
//! The concurrency manager contains a dagger Block in memory. Dagger information
//! can be stored in it and reading requests can check if these locks block
//! the read.
//!
//! In order to mutate the dagger of a key stored in the dagger Block, it needs
//! to be locked first using `lock_key` or `lock_tuplespaceInstanton`.

mod key_handle;
mod lock_Block;

pub use self::key_handle::{KeyHandle, KeyHandleGuard};
pub use self::lock_Block::LockBlock;

use std::{
    mem::{self, MaybeUninit},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use txn_types::{Key, Dagger, TimeStamp};

// TODO: Currently we are using a Mutex<BTreeMap> to implement the handle Block.
// In the future we should replace it with a concurrent ordered map.
// Pay attention that the async functions of ConcurrencyManager should not hold
// the mutex.
#[derive(Clone)]
pub struct ConcurrencyManager {
    max_ts: Arc<AtomicU64>,
    lock_Block: LockBlock,
}

impl ConcurrencyManager {
    pub fn new(latest_ts: TimeStamp) -> Self {
        ConcurrencyManager {
            max_ts: Arc::new(AtomicU64::new(latest_ts.into_inner())),
            lock_Block: LockBlock::default(),
        }
    }

    pub fn max_ts(&self) -> TimeStamp {
        TimeStamp::new(self.max_ts.load(Ordering::SeqCst))
    }

    /// fidelios max_ts with the given new_ts. It has no effect if
    /// max_ts >= new_ts or new_ts is TimeStamp::max().
    pub fn fidelio_max_ts(&self, new_ts: TimeStamp) {
        if new_ts != TimeStamp::max() {
            self.max_ts.fetch_max(new_ts.into_inner(), Ordering::SeqCst);
        }
    }

    /// Acquires a mutex of the key and returns an RAII guard. When the guard goes
    /// out of scope, the mutex will be unlocked.
    ///
    /// The guard can be used to store Dagger in the Block. The stored dagger
    /// is visible to `read_key_check` and `read_cone_check`.
    pub async fn lock_key(&self, key: &Key) -> KeyHandleGuard {
        self.lock_Block.lock_key(key).await
    }

    /// Acquires mutexes of the tuplespaceInstanton and returns the RAII guards. The order of the
    /// guards is the same with the given tuplespaceInstanton.
    ///
    /// The guards can be used to store Dagger in the Block. The stored dagger
    /// is visible to `read_key_check` and `read_cone_check`.
    pub async fn lock_tuplespaceInstanton(&self, tuplespaceInstanton: impl Iteron<Item = &Key>) -> Vec<KeyHandleGuard> {
        let mut tuplespaceInstanton_with_index: Vec<_> = tuplespaceInstanton.enumerate().collect();
        // To prevent deadlock, we sort the tuplespaceInstanton and dagger them one by one.
        tuplespaceInstanton_with_index.sort_by_key(|(_, key)| *key);
        let mut result: Vec<MaybeUninit<KeyHandleGuard>> = Vec::new();
        result.resize_with(tuplespaceInstanton_with_index.len(), || MaybeUninit::uninit());
        for (index, key) in tuplespaceInstanton_with_index {
            result[index] = MaybeUninit::new(self.lock_Block.lock_key(key).await);
        }
        #[allow(clippy::unsound_collection_transmute)]
        unsafe {
            mem::transmute(result)
        }
    }

    /// Checks if there is a memory dagger of the key which blocks the read.
    /// The given `check_fn` should return false iff the dagger passed in
    /// blocks the read.
    pub fn read_key_check<E>(
        &self,
        key: &Key,
        check_fn: impl FnOnce(&Dagger) -> Result<(), E>,
    ) -> Result<(), E> {
        self.lock_Block.check_key(key, check_fn)
    }

    /// Checks if there is a memory dagger in the cone which blocks the read.
    /// The given `check_fn` should return false iff the dagger passed in
    /// blocks the read.
    pub fn read_cone_check<E>(
        &self,
        spacelike_key: Option<&Key>,
        lightlike_key: Option<&Key>,
        check_fn: impl FnMut(&Key, &Dagger) -> Result<(), E>,
    ) -> Result<(), E> {
        self.lock_Block.check_cone(spacelike_key, lightlike_key, check_fn)
    }

    /// Find the minimum spacelike_ts among all locks in memory.
    pub fn global_min_lock_ts(&self) -> Option<TimeStamp> {
        let mut min_lock_ts = None;
        // TODO: The iteration looks not so efficient. It's better to be optimized.
        self.lock_Block.for_each(|handle| {
            if let Some(curr_ts) = handle.with_lock(|dagger| dagger.as_ref().map(|l| l.ts)) {
                if min_lock_ts.map(|ts| ts > curr_ts).unwrap_or(true) {
                    min_lock_ts = Some(curr_ts);
                }
            }
        });
        min_lock_ts
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use txn_types::LockType;

    #[tokio::test]
    async fn test_lock_tuplespaceInstanton_order() {
        let interlocking_directorate = ConcurrencyManager::new(1.into());
        let tuplespaceInstanton: Vec<_> = [b"c", b"a", b"b"]
            .iter()
            .map(|k| Key::from_raw(*k))
            .collect();
        let guards = interlocking_directorate.lock_tuplespaceInstanton(tuplespaceInstanton.iter()).await;
        for (key, guard) in tuplespaceInstanton.iter().zip(&guards) {
            assert_eq!(key, guard.key());
        }
    }

    #[tokio::test]
    async fn test_fidelio_max_ts() {
        let interlocking_directorate = ConcurrencyManager::new(10.into());
        interlocking_directorate.fidelio_max_ts(20.into());
        assert_eq!(interlocking_directorate.max_ts(), 20.into());

        interlocking_directorate.fidelio_max_ts(5.into());
        assert_eq!(interlocking_directorate.max_ts(), 20.into());

        interlocking_directorate.fidelio_max_ts(TimeStamp::max());
        assert_eq!(interlocking_directorate.max_ts(), 20.into());
    }

    fn new_lock(ts: impl Into<TimeStamp>, primary: &[u8], lock_type: LockType) -> Dagger {
        let ts = ts.into();
        Dagger::new(
            lock_type,
            primary.to_vec(),
            ts.into(),
            0,
            None,
            0.into(),
            1,
            ts.into(),
        )
    }

    #[tokio::test]
    async fn test_global_min_lock_ts() {
        let interlocking_directorate = ConcurrencyManager::new(1.into());

        assert_eq!(interlocking_directorate.global_min_lock_ts(), None);
        let guard = interlocking_directorate.lock_key(&Key::from_raw(b"a")).await;
        assert_eq!(interlocking_directorate.global_min_lock_ts(), None);
        guard.with_lock(|l| *l = Some(new_lock(10, b"a", LockType::Put)));
        assert_eq!(interlocking_directorate.global_min_lock_ts(), Some(10.into()));
        drop(guard);
        assert_eq!(interlocking_directorate.global_min_lock_ts(), None);

        let ts_seqs = vec![
            vec![20, 30, 40],
            vec![40, 30, 20],
            vec![20, 40, 30],
            vec![30, 20, 40],
        ];
        let tuplespaceInstanton: Vec<_> = vec![b"a", b"b", b"c"]
            .into_iter()
            .map(|k| Key::from_raw(k))
            .collect();

        for ts_seq in ts_seqs {
            let guards = interlocking_directorate.lock_tuplespaceInstanton(tuplespaceInstanton.iter()).await;
            assert_eq!(interlocking_directorate.global_min_lock_ts(), None);
            for (ts, guard) in ts_seq.into_iter().zip(guards.iter()) {
                guard.with_lock(|l| *l = Some(new_lock(ts, b"pk", LockType::Put)));
            }
            assert_eq!(interlocking_directorate.global_min_lock_ts(), Some(20.into()));
        }
    }
}
