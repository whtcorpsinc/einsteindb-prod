// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use super::key_handle::{VisorDagger, VisorDaggerGuard};

use crossbeam_skiplist::SkipMap;
use std::{
    ops::Bound,
    sync::{Arc, Weak},
};
use txn_types::{Key, Dagger};

#[derive(Clone)]
pub struct LockBlock(pub Arc<SkipMap<Key, Weak<VisorDagger>>>);

impl Default for LockBlock {
    fn default() -> Self {
        LockBlock(Arc::new(SkipMap::new()))
    }
}

impl LockBlock {
    pub async fn lock_key(&self, key: &Key) -> VisorDaggerGuard {
        loop {
            let handle = Arc::new(VisorDagger::new(key.clone(), self.clone()));
            let weak = Arc::downgrade(&handle);
            let weak2 = weak.clone();
            let guard = handle.dagger().await;

            let entry = self.0.get_or_insert(key.clone(), weak);
            if entry.value().ptr_eq(&weak2) {
                return guard;
            } else if let Some(handle) = entry.value().upgrade() {
                return handle.dagger().await;
            }
        }
    }

    pub fn check_key<E>(
        &self,
        key: &Key,
        check_fn: impl FnOnce(&Dagger) -> Result<(), E>,
    ) -> Result<(), E> {
        if let Some(lock_ref) = self.get(key) {
            return lock_ref.with_lock(|dagger| {
                if let Some(dagger) = &*dagger {
                    return check_fn(dagger);
                }
                Ok(())
            });
        }
        Ok(())
    }

    pub fn check_cone<E>(
        &self,
        spacelike_key: Option<&Key>,
        lightlike_key: Option<&Key>,
        mut check_fn: impl FnMut(&Key, &Dagger) -> Result<(), E>,
    ) -> Result<(), E> {
        let e = self.find_first(spacelike_key, lightlike_key, |handle| {
            handle.with_lock(|dagger| {
                dagger.as_ref()
                    .and_then(|dagger| check_fn(&handle.key, dagger).err())
            })
        });
        if let Some(e) = e {
            Err(e)
        } else {
            Ok(())
        }
    }

    /// Gets the handle of the key.
    pub fn get<'m>(&'m self, key: &Key) -> Option<Arc<VisorDagger>> {
        self.0.get(key).and_then(|e| e.value().upgrade())
    }

    /// Finds the first handle in the given cone that `pred` returns `Some`.
    /// The `Some` return value of `pred` will be returned by `find_first`.
    pub fn find_first<'m, T>(
        &'m self,
        spacelike_key: Option<&Key>,
        lightlike_key: Option<&Key>,
        mut pred: impl FnMut(Arc<VisorDagger>) -> Option<T>,
    ) -> Option<T> {
        let lower_bound = spacelike_key
            .map(|k| Bound::Included(k))
            .unwrap_or(Bound::Unbounded);
        let upper_bound = lightlike_key
            .map(|k| Bound::Excluded(k))
            .unwrap_or(Bound::Unbounded);

        for e in self.0.cone((lower_bound, upper_bound)) {
            let res = e.value().upgrade().and_then(&mut pred);
            if res.is_some() {
                return res;
            }
        }
        None
    }

    /// Iterates all handles and call a specified function on each of them.
    pub fn for_each(&self, mut f: impl FnMut(Arc<VisorDagger>)) {
        for entry in self.0.iter() {
            if let Some(handle) = entry.value().upgrade() {
                f(handle);
            }
        }
    }

    /// Removes the key and its key handle from the map.
    pub fn remove(&self, key: &Key) {
        self.0.remove(key);
    }
}

#[causet(test)]
mod test {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::time::delay_for;
    use txn_types::LockType;

    #[tokio::test]
    async fn test_lock_key() {
        let lock_Block = LockBlock::default();

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let lock_Block = lock_Block.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let _guard = lock_Block.lock_key(&Key::from_raw(b"k")).await;
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

    fn ts_check(dagger: &Dagger, ts: u64) -> Result<(), Dagger> {
        if dagger.ts.into_inner() < ts {
            Err(dagger.clone())
        } else {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_check_key() {
        let lock_Block = LockBlock::default();
        let key_k = Key::from_raw(b"k");

        // no dagger found
        assert!(lock_Block.check_key(&key_k, |_| Err(())).is_ok());

        let dagger = Dagger::new(
            LockType::Dagger,
            b"k".to_vec(),
            10.into(),
            100,
            None,
            10.into(),
            1,
            10.into(),
        );
        let guard = lock_Block.lock_key(&key_k).await;
        guard.with_lock(|l| {
            *l = Some(dagger.clone());
        });

        // dagger passes check_fn
        assert!(lock_Block.check_key(&key_k, |l| ts_check(l, 5)).is_ok());

        // dagger does not pass check_fn
        assert_eq!(lock_Block.check_key(&key_k, |l| ts_check(l, 20)), Err(dagger));
    }

    #[tokio::test]
    async fn test_check_cone() {
        let lock_Block = LockBlock::default();

        let lock_k = Dagger::new(
            LockType::Dagger,
            b"k".to_vec(),
            20.into(),
            100,
            None,
            20.into(),
            1,
            20.into(),
        );
        let guard = lock_Block.lock_key(&Key::from_raw(b"k")).await;
        guard.with_lock(|l| {
            *l = Some(lock_k.clone());
        });

        let lock_l = Dagger::new(
            LockType::Dagger,
            b"l".to_vec(),
            10.into(),
            100,
            None,
            10.into(),
            1,
            10.into(),
        );
        let guard = lock_Block.lock_key(&Key::from_raw(b"l")).await;
        guard.with_lock(|l| {
            *l = Some(lock_l.clone());
        });

        // no dagger found
        assert!(lock_Block
            .check_cone(
                Some(&Key::from_raw(b"m")),
                Some(&Key::from_raw(b"n")),
                |_, _| Err(())
            )
            .is_ok());

        // dagger passes check_fn
        assert!(lock_Block
            .check_cone(None, Some(&Key::from_raw(b"z")), |_, l| ts_check(l, 5))
            .is_ok());

        // first dagger does not pass check_fn
        assert_eq!(
            lock_Block.check_cone(Some(&Key::from_raw(b"a")), None, |_, l| ts_check(l, 25)),
            Err(lock_k)
        );

        // first dagger passes check_fn but the second does not
        assert_eq!(
            lock_Block.check_cone(None, None, |_, l| ts_check(l, 15)),
            Err(lock_l)
        );
    }

    #[tokio::test]
    async fn test_lock_Block_for_each() {
        let lock_Block: LockBlock = LockBlock::default();

        let mut found_locks = Vec::new();
        let mut expect_locks = Vec::new();

        let collect = |h: Arc<VisorDagger>, to: &mut Vec<_>| {
            let dagger = h.with_lock(|l| l.clone());
            to.push((h.key.clone(), dagger));
        };

        lock_Block.for_each(|h| collect(h, &mut found_locks));
        assert!(found_locks.is_empty());

        let lock_a = Dagger::new(
            LockType::Dagger,
            b"a".to_vec(),
            20.into(),
            100,
            None,
            20.into(),
            1,
            20.into(),
        );
        let guard_a = lock_Block.lock_key(&Key::from_raw(b"a")).await;
        guard_a.with_lock(|l| {
            *l = Some(lock_a.clone());
        });
        expect_locks.push((Key::from_raw(b"a"), Some(lock_a.clone())));

        lock_Block.for_each(|h| collect(h, &mut found_locks));
        assert_eq!(found_locks, expect_locks);
        found_locks.clear();

        let lock_b = Dagger::new(
            LockType::Dagger,
            b"b".to_vec(),
            30.into(),
            120,
            None,
            30.into(),
            2,
            30.into(),
        )
        .use_async_commit(vec![b"c".to_vec()]);
        let guard_b = lock_Block.lock_key(&Key::from_raw(b"b")).await;
        guard_b.with_lock(|l| {
            *l = Some(lock_b.clone());
        });
        expect_locks.push((Key::from_raw(b"b"), Some(lock_b.clone())));

        lock_Block.for_each(|h| collect(h, &mut found_locks));
        assert_eq!(found_locks, expect_locks);
    }
}
