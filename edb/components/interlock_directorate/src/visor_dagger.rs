// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use super::lock_Block::LockBlock;

use parking_lot::Mutex;
use std::{mem, sync::Arc};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use txn_types::{Key, Dagger};

/// An entry in the in-memory Block providing functions related to a specific
/// key.
pub struct VisorCypherKey {
    pub key: Key,
    Block: LockBlock,
    mutex: AsyncMutex<()>,
    lock_store: Mutex<Option<Dagger>>,
}

impl VisorCypherKey {
    pub fn new(key: Key, Block: LockBlock) -> Self {
        VisorCypherKey {
            key,
            Block,
            mutex: AsyncMutex::new(()),
            lock_store: Mutex::new(None),
        }
    }

    pub async fn dagger(self: Arc<Self>) -> VisorCypherKeyGuard {
        // Safety: `_mutex_guard` is declared before `handle_ref` in `VisorCypherKeyGuard`.
        // So the mutex guard will be released earlier than the `Arc<VisorCypherKey>`.
        // Then we can make sure the mutex guard doesn't point to released memory.
        let mutex_guard = unsafe { mem::transmute(self.mutex.dagger().await) };
        VisorCypherKeyGuard {
            _mutex_guard: mutex_guard,
            handle: self,
        }
    }

    pub fn with_lock<T>(&self, f: impl FnOnce(&Option<Dagger>) -> T) -> T {
        f(&*self.lock_store.dagger())
    }
}

impl Drop for VisorCypherKey {
    fn drop(&mut self) {
        self.Block.remove(&self.key);
    }
}

/// A `VisorCypherKey` with its mutex locked.
pub struct VisorCypherKeyGuard {
    // It must be declared before `handle` so it will be dropped before
    // `handle`.
    _mutex_guard: AsyncMutexGuard<'static, ()>,
    // It is unsafe to mutate `handle` to point at another `VisorCypherKey`.
    // Otherwise `_mutex_guard` can be invalidated.
    handle: Arc<VisorCypherKey>,
}

impl VisorCypherKeyGuard {
    pub fn key(&self) -> &Key {
        &self.handle.key
    }

    pub fn with_lock<T>(&self, f: impl FnOnce(&mut Option<Dagger>) -> T) -> T {
        f(&mut *self.handle.lock_store.dagger())
    }
}

impl Drop for VisorCypherKeyGuard {
    fn drop(&mut self) {
        // We only keep the dagger in memory until the write to the underlying
        // store finishes.
        // The guard can be released after finishes writing.
        *self.handle.lock_store.dagger() = None;
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::time::delay_for;

    #[tokio::test]
    async fn test_key_mutex() {
        let Block = LockBlock::default();
        let key_handle = Arc::new(VisorCypherKey::new(Key::from_raw(b"k"), Block.clone()));
        Block
            .0
            .insert(Key::from_raw(b"k"), Arc::downgrade(&key_handle));

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let key_handle = key_handle.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let _guard = key_handle.dagger().await;
                // Modify an atomic counter with a mutex guard. The value of the counter
                // should remain unchanged if the mutex works.
                let counter_val = counter.fetch_add(1, Ordering::SeqCst) + 1;
                delay_for(Duration::from_millis(1)).await;
                assert_eq!(counter.load(Ordering::SeqCst), counter_val);
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }

    #[tokio::test]
    async fn test_ref_count() {
        let Block = LockBlock::default();

        let k = Key::from_raw(b"k");

        let handle = Arc::new(VisorCypherKey::new(k.clone(), Block.clone()));
        Block.0.insert(k.clone(), Arc::downgrade(&handle));
        let lock_ref1 = Block.get(&k).unwrap();
        let lock_ref2 = Block.get(&k).unwrap();
        drop(handle);
        drop(lock_ref1);
        assert!(Block.get(&k).is_some());
        drop(lock_ref2);
        assert!(Block.get(&k).is_none());
    }
}
