// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use engine_lmdb::LmdbEngine;
use edb::{IterOptions, CausetEngine, ReadOptions, Causet_DEFAULT, Causet_DAGGER, Causet_WRITE};
use ekvproto::meta_timeshare::{Peer, Brane};
use violetabft::StateRole;
use violetabftstore::interlock::*;
use violetabftstore::store::fsm::ObserveID;
use violetabftstore::store::BraneSnapshot;
use violetabftstore::Error as VioletaBftStoreError;
use edb::causet_storage::{Cursor, ScanMode, Snapshot as EngineSnapshot, Statistics};
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::worker::Interlock_Semaphore;
use txn_types::{Key, Dagger, MutationType, Value, WriteRef, WriteType};

use crate::lightlikepoint::{Deregister, OldValueCache, Task};
use crate::{Error as causet_contextError, Result};

/// An Semaphore for causet_context.
///
/// It observes violetabftstore internal events, such as:
///   1. VioletaBft role change events,
///   2. Apply command events.
#[derive(Clone)]
pub struct causet_contextSemaphore {
    sched: Interlock_Semaphore<Task>,
    // A shared registry for managing observed branes.
    // TODO: it may become a bottleneck, find a better way to manage the registry.
    observe_branes: Arc<RwLock<HashMap<u64, ObserveID>>>,
    cmd_batches: RefCell<Vec<CmdBatch>>,
}

impl causet_contextSemaphore {
    /// Create a new `causet_contextSemaphore`.
    ///
    /// Events are strong ordered, so `sched` must be implemented as
    /// a FIFO queue.
    pub fn new(sched: Interlock_Semaphore<Task>) -> causet_contextSemaphore {
        causet_contextSemaphore {
            sched,
            observe_branes: Arc::default(),
            cmd_batches: RefCell::default(),
        }
    }

    pub fn register_to(&self, interlock_host: &mut InterlockHost<LmdbEngine>) {
        // 100 is the priority of the semaphore. causet_context should have a high priority.
        interlock_host
            .registry
            .register_cmd_semaphore(100, BoxCmdSemaphore::new(self.clone()));
        interlock_host
            .registry
            .register_role_semaphore(100, BoxRoleSemaphore::new(self.clone()));
        interlock_host
            .registry
            .register_brane_change_semaphore(100, BoxBraneChangeSemaphore::new(self.clone()));
    }

    /// Subscribe an brane, the semaphore will sink events of the brane into
    /// its interlock_semaphore.
    ///
    /// Return pervious ObserveID if there is one.
    pub fn subscribe_brane(&self, brane_id: u64, observe_id: ObserveID) -> Option<ObserveID> {
        self.observe_branes
            .write()
            .unwrap()
            .insert(brane_id, observe_id)
    }

    /// Stops observe the brane.
    ///
    /// Return SemaphoreID if unsubscribe successfully.
    pub fn unsubscribe_brane(&self, brane_id: u64, observe_id: ObserveID) -> Option<ObserveID> {
        let mut branes = self.observe_branes.write().unwrap();
        // To avoid ABA problem, we must check the unique ObserveID.
        if let Some(oid) = branes.get(&brane_id) {
            if *oid == observe_id {
                return branes.remove(&brane_id);
            }
        }
        None
    }

    /// Check whether the brane is subscribed or not.
    pub fn is_subscribed(&self, brane_id: u64) -> Option<ObserveID> {
        self.observe_branes
            .read()
            .unwrap()
            .get(&brane_id)
            .cloned()
    }
}

impl Interlock for causet_contextSemaphore {}

impl<E: CausetEngine> CmdSemaphore<E> for causet_contextSemaphore {
    fn on_prepare_for_apply(&self, observe_id: ObserveID, brane_id: u64) {
        self.cmd_batches
            .borrow_mut()
            .push(CmdBatch::new(observe_id, brane_id));
    }

    fn on_apply_cmd(&self, observe_id: ObserveID, brane_id: u64, cmd: Cmd) {
        self.cmd_batches
            .borrow_mut()
            .last_mut()
            .expect("should exist some cmd batch")
            .push(observe_id, brane_id, cmd);
    }

    fn on_flush_apply(&self, engine: E) {
        fail_point!("before_causet_context_flush_apply");
        if !self.cmd_batches.borrow().is_empty() {
            let batches = self.cmd_batches.replace(Vec::default());
            let mut brane = Brane::default();
            brane.mut_peers().push(Peer::default());
            // Create a snapshot here for preventing the old value was GC-ed.
            let snapshot =
                BraneSnapshot::from_snapshot(Arc::new(engine.snapshot()), Arc::new(brane));
            let mut reader = OldValueReader::new(snapshot);
            let get_old_value = move |key, old_value_cache: &mut OldValueCache| {
                old_value_cache.access_count += 1;
                if let Some((old_value, mutation_type)) = old_value_cache.cache.remove(&key) {
                    match mutation_type {
                        MutationType::Insert => {
                            assert!(old_value.is_none());
                            return None;
                        }
                        MutationType::Put | MutationType::Delete => {
                            if let Some(old_value) = old_value {
                                let spacelike_ts = old_value.spacelike_ts;
                                return old_value.short_value.or_else(|| {
                                    let prev_key = key.truncate_ts().unwrap().applightlike_ts(spacelike_ts);
                                    let mut opts = ReadOptions::new();
                                    opts.set_fill_cache(false);
                                    reader.get_value_default(&prev_key)
                                });
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                // Cannot get old value from cache, seek for it in engine.
                old_value_cache.miss_count += 1;
                reader.near_seek_old_value(&key).unwrap_or_default()
            };
            if let Err(e) = self.sched.schedule(Task::MultiBatch {
                multi: batches,
                old_value_cb: Box::new(get_old_value),
            }) {
                warn!("schedule causet_context task failed"; "error" => ?e);
            }
        }
    }
}

impl RoleSemaphore for causet_contextSemaphore {
    fn on_role_change(&self, ctx: &mut SemaphoreContext<'_>, role: StateRole) {
        if role != StateRole::Leader {
            let brane_id = ctx.brane().get_id();
            if let Some(observe_id) = self.is_subscribed(brane_id) {
                // Unregister all downstreams.
                let store_err = VioletaBftStoreError::NotLeader(brane_id, None);
                let deregister = Deregister::Brane {
                    brane_id,
                    observe_id,
                    err: causet_contextError::Request(store_err.into()),
                };
                if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                    error!("schedule causet_context task failed"; "error" => ?e);
                }
            }
        }
    }
}

impl BraneChangeSemaphore for causet_contextSemaphore {
    fn on_brane_changed(
        &self,
        ctx: &mut SemaphoreContext<'_>,
        event: BraneChangeEvent,
        _: StateRole,
    ) {
        if let BraneChangeEvent::Destroy = event {
            let brane_id = ctx.brane().get_id();
            if let Some(observe_id) = self.is_subscribed(brane_id) {
                // Unregister all downstreams.
                let store_err = VioletaBftStoreError::BraneNotFound(brane_id);
                let deregister = Deregister::Brane {
                    brane_id,
                    observe_id,
                    err: causet_contextError::Request(store_err.into()),
                };
                if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                    error!("schedule causet_context task failed"; "error" => ?e);
                }
            }
        }
    }
}

struct OldValueReader<S: EngineSnapshot> {
    snapshot: S,
    write_cursor: Cursor<S::Iter>,
    // TODO(5kbpers): add a metric here.
    statistics: Statistics,
}

impl<S: EngineSnapshot> OldValueReader<S> {
    fn new(snapshot: S) -> Self {
        let mut iter_opts = IterOptions::default();
        iter_opts.set_fill_cache(false);
        let write_cursor = snapshot
            .iter_causet(Causet_WRITE, iter_opts, ScanMode::Mixed)
            .unwrap();
        Self {
            snapshot,
            write_cursor,
            statistics: Statistics::default(),
        }
    }

    // return Some(vec![]) if value is empty.
    // return None if key not exist.
    fn get_value_default(&mut self, key: &Key) -> Option<Value> {
        self.statistics.data.get += 1;
        let mut opts = ReadOptions::new();
        opts.set_fill_cache(false);
        self.snapshot
            .get_causet_opt(opts, Causet_DEFAULT, &key)
            .unwrap()
            .map(|v| v.deref().to_vec())
    }

    fn check_lock(&mut self, key: &Key) -> bool {
        self.statistics.dagger.get += 1;
        let mut opts = ReadOptions::new();
        opts.set_fill_cache(false);
        let key_slice = key.as_encoded();
        let user_key = Key::from_encoded_slice(Key::truncate_ts_for(key_slice).unwrap());

        match self.snapshot.get_causet_opt(opts, Causet_DAGGER, &user_key).unwrap() {
            Some(v) => {
                let dagger = Dagger::parse(v.deref()).unwrap();
                dagger.ts == Key::decode_ts_from(key_slice).unwrap()
            }
            None => false,
        }
    }

    // return Some(vec![]) if value is empty.
    // return None if key not exist.
    fn near_seek_old_value(&mut self, key: &Key) -> Result<Option<Value>> {
        let user_key = Key::truncate_ts_for(key.as_encoded()).unwrap();
        if self
            .write_cursor
            .near_seek(key, &mut self.statistics.write)?
            && Key::is_user_key_eq(self.write_cursor.key(&mut self.statistics.write), user_key)
        {
            if self.write_cursor.key(&mut self.statistics.write) == key.as_encoded().as_slice() {
                // Key was committed, move cursor to the next key to seek for old value.
                if !self.write_cursor.next(&mut self.statistics.write) {
                    // Do not has any next key, return empty value.
                    return Ok(Some(Vec::default()));
                }
            } else if !self.check_lock(key) {
                return Ok(None);
            }

            // Key was not committed, check if the dagger is corresponding to the key.
            let mut old_value = Some(Vec::default());
            while Key::is_user_key_eq(self.write_cursor.key(&mut self.statistics.write), user_key) {
                let write =
                    WriteRef::parse(self.write_cursor.value(&mut self.statistics.write)).unwrap();
                old_value = match write.write_type {
                    WriteType::Put => match write.short_value {
                        Some(short_value) => Some(short_value.to_vec()),
                        None => {
                            let key = key.clone().truncate_ts().unwrap().applightlike_ts(write.spacelike_ts);
                            self.get_value_default(&key)
                        }
                    },
                    WriteType::Delete => Some(Vec::default()),
                    WriteType::Rollback | WriteType::Dagger => {
                        if !self.write_cursor.next(&mut self.statistics.write) {
                            Some(Vec::default())
                        } else {
                            continue;
                        }
                    }
                };
                break;
            }
            Ok(old_value)
        } else if self.check_lock(key) {
            Ok(Some(Vec::default()))
        } else {
            Ok(None)
        }
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use engine_lmdb::LmdbEngine;
    use ekvproto::meta_timeshare::Brane;
    use ekvproto::violetabft_cmd_timeshare::*;
    use std::time::Duration;
    use edb::causet_storage::kv::TestEngineBuilder;
    use edb::causet_storage::tail_pointer::tests::*;
    use edb::causet_storage::txn::tests::*;

    #[test]
    fn test_register_and_deregister() {
        let (interlock_semaphore, rx) = violetabftstore::interlock::::worker::dummy_interlock_semaphore();
        let semaphore = causet_contextSemaphore::new(interlock_semaphore);
        let observe_id = ObserveID::new();
        let engine = TestEngineBuilder::new().build().unwrap().get_lmdb();

        <causet_contextSemaphore as CmdSemaphore<LmdbEngine>>::on_prepare_for_apply(&semaphore, observe_id, 0);
        <causet_contextSemaphore as CmdSemaphore<LmdbEngine>>::on_apply_cmd(
            &semaphore,
            observe_id,
            0,
            Cmd::new(0, VioletaBftCmdRequest::default(), VioletaBftCmdResponse::default()),
        );
        semaphore.on_flush_apply(engine);

        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::MultiBatch { multi, .. } => {
                assert_eq!(multi.len(), 1);
                assert_eq!(multi[0].len(), 1);
            }
            _ => panic!("unexpected task"),
        };

        // Does not lightlike unsubscribed brane events.
        let mut brane = Brane::default();
        brane.set_id(1);
        let mut ctx = SemaphoreContext::new(&brane);
        semaphore.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        let oid = ObserveID::new();
        semaphore.subscribe_brane(1, oid);
        let mut ctx = SemaphoreContext::new(&brane);
        semaphore.on_role_change(&mut ctx, StateRole::Follower);
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::Deregister(Deregister::Brane {
                brane_id,
                observe_id,
                ..
            }) => {
                assert_eq!(brane_id, 1);
                assert_eq!(observe_id, oid);
            }
            _ => panic!("unexpected task"),
        };

        // No event if it changes to leader.
        semaphore.on_role_change(&mut ctx, StateRole::Leader);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // unsubscribed fail if semaphore id is different.
        assert_eq!(semaphore.unsubscribe_brane(1, ObserveID::new()), None);

        // No event if it is unsubscribed.
        let oid_ = semaphore.unsubscribe_brane(1, oid).unwrap();
        assert_eq!(oid_, oid);
        semaphore.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // No event if it is unsubscribed.
        brane.set_id(999);
        let mut ctx = SemaphoreContext::new(&brane);
        semaphore.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();
    }

    #[test]
    fn test_old_value_reader() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let kv_engine = engine.get_lmdb();
        let k = b"k";
        let key = Key::from_raw(k);

        let must_get_eq = |ts: u64, value| {
            let mut old_value_reader = OldValueReader::new(Arc::new(kv_engine.snapshot()));
            assert_eq!(
                old_value_reader
                    .near_seek_old_value(&key.clone().applightlike_ts(ts.into()))
                    .unwrap(),
                value
            );
            let mut opts = ReadOptions::new();
            opts.set_fill_cache(false);
        };

        must_prewrite_put(&engine, k, b"v1", k, 1);
        must_get_eq(2, None);
        must_get_eq(1, Some(vec![]));
        must_commit(&engine, k, 1, 1);
        must_get_eq(1, Some(vec![]));

        must_prewrite_put(&engine, k, b"v2", k, 2);
        must_get_eq(2, Some(b"v1".to_vec()));
        must_rollback(&engine, k, 2);

        must_prewrite_put(&engine, k, b"v3", k, 3);
        must_get_eq(3, Some(b"v1".to_vec()));
        must_commit(&engine, k, 3, 3);

        must_prewrite_delete(&engine, k, k, 4);
        must_get_eq(4, Some(b"v3".to_vec()));
        must_commit(&engine, k, 4, 4);

        must_prewrite_put(&engine, k, vec![b'v'; 5120].as_slice(), k, 5);
        must_get_eq(5, Some(vec![]));
        must_commit(&engine, k, 5, 5);

        must_prewrite_delete(&engine, k, k, 6);
        must_get_eq(6, Some(vec![b'v'; 5120]));
    }
}
