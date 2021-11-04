// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use ekvproto::kvrpc_timeshare::{Context, LockInfo};

use test_violetabftstore::{Cluster, ServerCluster, SimulateEngine};
use edb::causet_storage::kv::{Error as KvError, ErrorInner as KvErrorInner, LmdbEngine};
use edb::causet_storage::tail_pointer::{Error as MvccError, ErrorInner as MvccErrorInner, MAX_TXN_WRITE_SIZE};
use edb::causet_storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};
use edb::causet_storage::{
    self, Engine, Error as StorageError, ErrorInner as StorageErrorInner, TxnStatus,
};
use violetabftstore::interlock::::HandyRwLock;
use txn_types::{Key, KvPair, Mutation, TimeStamp, Value};

use super::*;

#[derive(Clone)]
pub struct AssertionStorage<E: Engine> {
    pub store: SyncTestStorage<E>,
    pub ctx: Context,
}

impl Default for AssertionStorage<LmdbEngine> {
    fn default() -> Self {
        AssertionStorage {
            ctx: Context::default(),
            store: SyncTestStorageBuilder::new().build().unwrap(),
        }
    }
}

impl AssertionStorage<SimulateEngine> {
    pub fn new_violetabft_causet_storage_with_store_count(
        count: usize,
        key: &str,
    ) -> (Cluster<ServerCluster>, Self) {
        let (cluster, store, ctx) = new_violetabft_causet_storage_with_store_count(count, key);
        let causet_storage = Self { ctx, store };
        (cluster, causet_storage)
    }

    pub fn fidelio_with_key_byte(&mut self, cluster: &mut Cluster<ServerCluster>, key: &[u8]) {
        // ensure the leader of cone which contains current key has been elected
        cluster.must_get(key);
        let brane = cluster.get_brane(key);
        let leader = cluster.leader_of_brane(brane.get_id()).unwrap();
        if leader.get_store_id() == self.ctx.get_peer().get_store_id() {
            return;
        }
        let engine = cluster.sim.rl().causet_storages[&leader.get_id()].clone();
        self.ctx.set_brane_id(brane.get_id());
        self.ctx.set_brane_epoch(brane.get_brane_epoch().clone());
        self.ctx.set_peer(leader);
        self.store = SyncTestStorageBuilder::from_engine(engine).build().unwrap();
    }

    pub fn delete_ok_for_cluster(
        &mut self,
        cluster: &mut Cluster<ServerCluster>,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let mutations = vec![Mutation::Delete(Key::from_raw(key))];
        let commit_tuplespaceInstanton = vec![Key::from_raw(key)];
        self.two_pc_ok_for_cluster(
            cluster,
            mutations,
            key,
            commit_tuplespaceInstanton,
            spacelike_ts.into(),
            commit_ts.into(),
        );
    }

    fn get_from_custer(
        &mut self,
        cluster: &mut Cluster<ServerCluster>,
        key: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Option<Value> {
        let ts = ts.into();
        for _ in 0..3 {
            let res = self.store.get(self.ctx.clone(), &Key::from_raw(key), ts);
            if let Ok(data) = res {
                return data;
            }
            self.expect_not_leader_or_stale_command(res.unwrap_err());
            self.fidelio_with_key_byte(cluster, key);
        }
        panic!("failed with 3 try");
    }

    pub fn get_none_from_cluster(
        &mut self,
        cluster: &mut Cluster<ServerCluster>,
        key: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        assert_eq!(self.get_from_custer(cluster, key, ts), None);
    }

    pub fn put_ok_for_cluster(
        &mut self,
        cluster: &mut Cluster<ServerCluster>,
        key: &[u8],
        value: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let mutations = vec![Mutation::Put((Key::from_raw(key), value.to_vec()))];
        let commit_tuplespaceInstanton = vec![Key::from_raw(key)];
        self.two_pc_ok_for_cluster(cluster, mutations, key, commit_tuplespaceInstanton, spacelike_ts, commit_ts);
    }

    fn two_pc_ok_for_cluster(
        &mut self,
        cluster: &mut Cluster<ServerCluster>,
        prewrite_mutations: Vec<Mutation>,
        key: &[u8],
        commit_tuplespaceInstanton: Vec<Key>,
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let retry_time = 3;
        let mut success = false;
        let spacelike_ts = spacelike_ts.into();
        for _ in 0..retry_time {
            let res = self.store.prewrite(
                self.ctx.clone(),
                prewrite_mutations.clone(),
                key.to_vec(),
                spacelike_ts,
            );
            if res.is_ok() {
                success = true;
                break;
            }
            self.expect_not_leader_or_stale_command(res.unwrap_err());
            self.fidelio_with_key_byte(cluster, key)
        }
        assert!(success);

        success = false;
        let commit_ts = commit_ts.into();
        for _ in 0..retry_time {
            let res = self
                .store
                .commit(self.ctx.clone(), commit_tuplespaceInstanton.clone(), spacelike_ts, commit_ts);
            if res.is_ok() {
                success = true;
                break;
            }
            self.expect_not_leader_or_stale_command(res.unwrap_err());
            self.fidelio_with_key_byte(cluster, key)
        }
        assert!(success);
    }

    pub fn gc_ok_for_cluster(
        &mut self,
        cluster: &mut Cluster<ServerCluster>,
        brane_key: &[u8],
        safe_point: impl Into<TimeStamp>,
    ) {
        let safe_point = safe_point.into();
        for _ in 0..3 {
            let ret = self.store.gc(self.ctx.clone(), safe_point);
            if ret.is_ok() {
                return;
            }
            self.expect_not_leader_or_stale_command(ret.unwrap_err());
            self.fidelio_with_key_byte(cluster, brane_key);
        }
        panic!("failed with 3 retry!");
    }

    pub fn test_txn_store_gc3_for_cluster(
        &mut self,
        cluster: &mut Cluster<ServerCluster>,
        key_prefix: u8,
    ) {
        let key_len = 10_000;
        let key = vec![key_prefix; 1024];
        for k in 1u64..(MAX_TXN_WRITE_SIZE / key_len * 2) as u64 {
            self.put_ok_for_cluster(cluster, &key, b"", k * 10, k * 10 + 5);
        }

        self.delete_ok_for_cluster(cluster, &key, 1000, 1050);
        self.get_none_from_cluster(cluster, &key, 2000);
        self.gc_ok_for_cluster(cluster, &key, 2000);
        self.get_none_from_cluster(cluster, &key, 3000);
    }
}

impl<E: Engine> AssertionStorage<E> {
    pub fn get_none(&self, key: &[u8], ts: impl Into<TimeStamp>) {
        let key = Key::from_raw(key);
        assert_eq!(
            self.store.get(self.ctx.clone(), &key, ts.into()).unwrap(),
            None
        );
    }

    pub fn get_err(&self, key: &[u8], ts: impl Into<TimeStamp>) {
        let key = Key::from_raw(key);
        assert!(self.store.get(self.ctx.clone(), &key, ts.into()).is_err());
    }

    pub fn get_ok(&self, key: &[u8], ts: impl Into<TimeStamp>, expect: &[u8]) {
        let key = Key::from_raw(key);
        assert_eq!(
            self.store
                .get(self.ctx.clone(), &key, ts.into())
                .unwrap()
                .unwrap(),
            expect
        );
    }

    pub fn batch_get_ok(&self, tuplespaceInstanton: &[&[u8]], ts: impl Into<TimeStamp>, expect: Vec<&[u8]>) {
        let tuplespaceInstanton: Vec<Key> = tuplespaceInstanton.iter().map(|x| Key::from_raw(x)).collect();
        let result: Vec<Vec<u8>> = self
            .store
            .batch_get(self.ctx.clone(), &tuplespaceInstanton, ts.into())
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap().1)
            .collect();
        let expect: Vec<Vec<u8>> = expect.into_iter().map(|x| x.to_vec()).collect();
        assert_eq!(result, expect);
    }

    pub fn batch_get_command_ok(&self, tuplespaceInstanton: &[&[u8]], ts: u64, expect: Vec<&[u8]>) {
        let result: Vec<Option<Vec<u8>>> = self
            .store
            .batch_get_command(self.ctx.clone(), &tuplespaceInstanton, ts)
            .unwrap()
            .into_iter()
            .collect();
        let expect: Vec<Option<Vec<u8>>> = expect
            .into_iter()
            .map(|x| if x.is_empty() { None } else { Some(x.to_vec()) })
            .collect();
        assert_eq!(result, expect);
    }

    fn expect_not_leader_or_stale_command(&self, err: causet_storage::Error) {
        match err {
            StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                MvccError(box MvccErrorInner::Engine(KvError(box KvErrorInner::Request(ref e)))),
            ))))
            | StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Engine(
                KvError(box KvErrorInner::Request(ref e)),
            ))))
            | StorageError(box StorageErrorInner::Engine(KvError(box KvErrorInner::Request(
                ref e,
            )))) => {
                assert!(
                    e.has_not_leader() | e.has_stale_command(),
                    "invalid error {:?}",
                    e
                );
            }
            _ => {
                panic!(
                    "expect not leader error or stale command, but got {:?}",
                    err
                );
            }
        }
    }

    fn expect_invalid_tso_err<T>(
        &self,
        resp: Result<T, causet_storage::Error>,
        sts: impl Into<TimeStamp>,
        cmt_ts: impl Into<TimeStamp>,
    ) where
        T: std::fmt::Debug,
    {
        assert!(resp.is_err());
        let err = resp.unwrap_err();
        match err {
            StorageError(box StorageErrorInner::Txn(TxnError(
                box TxnErrorInner::InvalidTxnTso {
                    spacelike_ts,
                    commit_ts,
                },
            ))) => {
                assert_eq!(sts.into(), spacelike_ts);
                assert_eq!(cmt_ts.into(), commit_ts);
            }
            _ => {
                panic!("expect invalid tso error, but got {:?}", err);
            }
        }
    }

    pub fn put_ok(
        &self,
        key: &[u8],
        value: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let spacelike_ts = spacelike_ts.into();
        self.store
            .prewrite(
                self.ctx.clone(),
                vec![Mutation::Put((Key::from_raw(key), value.to_vec()))],
                key.to_vec(),
                spacelike_ts,
            )
            .unwrap();
        self.store
            .commit(
                self.ctx.clone(),
                vec![Key::from_raw(key)],
                spacelike_ts,
                commit_ts.into(),
            )
            .unwrap();
    }

    pub fn delete_ok(
        &self,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let spacelike_ts = spacelike_ts.into();
        self.store
            .prewrite(
                self.ctx.clone(),
                vec![Mutation::Delete(Key::from_raw(key))],
                key.to_vec(),
                spacelike_ts,
            )
            .unwrap();
        self.store
            .commit(
                self.ctx.clone(),
                vec![Key::from_raw(key)],
                spacelike_ts,
                commit_ts.into(),
            )
            .unwrap();
    }

    pub fn scan_ok(
        &self,
        spacelike_key: &[u8],
        limit: usize,
        ts: impl Into<TimeStamp>,
        expect: Vec<Option<(&[u8], &[u8])>>,
    ) {
        let key_address = Key::from_raw(spacelike_key);
        let result = self
            .store
            .scan(self.ctx.clone(), key_address, None, limit, false, ts.into())
            .unwrap();
        let result: Vec<Option<KvPair>> = result.into_iter().map(Result::ok).collect();
        let expect: Vec<Option<KvPair>> = expect
            .into_iter()
            .map(|x| x.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect();
        assert_eq!(result, expect);
    }

    pub fn reverse_scan_ok(
        &self,
        spacelike_key: &[u8],
        limit: usize,
        ts: impl Into<TimeStamp>,
        expect: Vec<Option<(&[u8], &[u8])>>,
    ) {
        let key_address = Key::from_raw(spacelike_key);
        let result = self
            .store
            .reverse_scan(self.ctx.clone(), key_address, None, limit, false, ts.into())
            .unwrap();
        let result: Vec<Option<KvPair>> = result.into_iter().map(Result::ok).collect();
        let expect: Vec<Option<KvPair>> = expect
            .into_iter()
            .map(|x| x.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect();
        assert_eq!(result, expect);
    }

    pub fn scan_key_only_ok(
        &self,
        spacelike_key: &[u8],
        limit: usize,
        ts: impl Into<TimeStamp>,
        expect: Vec<Option<&[u8]>>,
    ) {
        let key_address = Key::from_raw(spacelike_key);
        let result = self
            .store
            .scan(self.ctx.clone(), key_address, None, limit, true, ts.into())
            .unwrap();
        let result: Vec<Option<KvPair>> = result.into_iter().map(Result::ok).collect();
        let expect: Vec<Option<KvPair>> = expect
            .into_iter()
            .map(|x| x.map(|k| (k.to_vec(), vec![])))
            .collect();
        assert_eq!(result, expect);
    }

    pub fn prewrite_ok(
        &self,
        mutations: Vec<Mutation>,
        primary: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
    ) {
        self.store
            .prewrite(
                self.ctx.clone(),
                mutations,
                primary.to_vec(),
                spacelike_ts.into(),
            )
            .unwrap();
    }

    pub fn prewrite_err(
        &self,
        mutations: Vec<Mutation>,
        primary: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
    ) {
        self.store
            .prewrite(
                self.ctx.clone(),
                mutations,
                primary.to_vec(),
                spacelike_ts.into(),
            )
            .unwrap_err();
    }

    pub fn prewrite_locked(
        &self,
        mutations: Vec<Mutation>,
        primary: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        expect_locks: Vec<(&[u8], &[u8], TimeStamp)>,
    ) {
        let res = self
            .store
            .prewrite(
                self.ctx.clone(),
                mutations,
                primary.to_vec(),
                spacelike_ts.into(),
            )
            .unwrap();
        let locks: Vec<(&[u8], &[u8], TimeStamp)> = res
            .locks
            .iter()
            .filter_map(|x| {
                if let Err(StorageError(box StorageErrorInner::Txn(TxnError(
                    box TxnErrorInner::Mvcc(MvccError(box MvccErrorInner::KeyIsLocked(info))),
                )))) = x
                {
                    Some((
                        info.get_key(),
                        info.get_primary_lock(),
                        info.get_dagger_version().into(),
                    ))
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(expect_locks, locks);
    }

    pub fn prewrite_conflict(
        &self,
        mutations: Vec<Mutation>,
        cur_primary: &[u8],
        cur_spacelike_ts: impl Into<TimeStamp>,
        confl_key: &[u8],
        confl_ts: impl Into<TimeStamp>,
    ) {
        let cur_spacelike_ts = cur_spacelike_ts.into();
        let err = self
            .store
            .prewrite(
                self.ctx.clone(),
                mutations,
                cur_primary.to_vec(),
                cur_spacelike_ts,
            )
            .unwrap_err();

        match err {
            StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                MvccError(box MvccErrorInner::WriteConflict {
                    spacelike_ts,
                    conflict_spacelike_ts,
                    ref key,
                    ref primary,
                    ..
                }),
            )))) => {
                assert_eq!(cur_spacelike_ts, spacelike_ts);
                assert_eq!(confl_ts.into(), conflict_spacelike_ts);
                assert_eq!(key.to_owned(), confl_key.to_owned());
                assert_eq!(primary.to_owned(), cur_primary.to_owned());
            }
            _ => {
                panic!("expect conflict error, but got {:?}", err);
            }
        }
    }

    pub fn commit_ok(
        &self,
        tuplespaceInstanton: Vec<&[u8]>,
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        actual_commit_ts: impl Into<TimeStamp>,
    ) {
        let tuplespaceInstanton: Vec<Key> = tuplespaceInstanton.iter().map(|x| Key::from_raw(x)).collect();
        let txn_status = self
            .store
            .commit(self.ctx.clone(), tuplespaceInstanton, spacelike_ts.into(), commit_ts.into())
            .unwrap();
        assert_eq!(txn_status, TxnStatus::committed(actual_commit_ts.into()));
    }

    pub fn commit_with_illegal_tso(
        &self,
        tuplespaceInstanton: Vec<&[u8]>,
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let spacelike_ts = spacelike_ts.into();
        let commit_ts = commit_ts.into();
        let tuplespaceInstanton: Vec<Key> = tuplespaceInstanton.iter().map(|x| Key::from_raw(x)).collect();
        let resp = self
            .store
            .commit(self.ctx.clone(), tuplespaceInstanton, spacelike_ts, commit_ts);
        self.expect_invalid_tso_err(resp, spacelike_ts, commit_ts);
    }

    pub fn cleanup_ok(
        &self,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) {
        self.store
            .cleanup(
                self.ctx.clone(),
                Key::from_raw(key),
                spacelike_ts.into(),
                current_ts.into(),
            )
            .unwrap();
    }

    pub fn cleanup_err(
        &self,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) {
        assert!(self
            .store
            .cleanup(
                self.ctx.clone(),
                Key::from_raw(key),
                spacelike_ts.into(),
                current_ts.into()
            )
            .is_err());
    }

    pub fn rollback_ok(&self, tuplespaceInstanton: Vec<&[u8]>, spacelike_ts: impl Into<TimeStamp>) {
        let tuplespaceInstanton: Vec<Key> = tuplespaceInstanton.iter().map(|x| Key::from_raw(x)).collect();
        self.store
            .rollback(self.ctx.clone(), tuplespaceInstanton, spacelike_ts.into())
            .unwrap();
    }

    pub fn rollback_err(&self, tuplespaceInstanton: Vec<&[u8]>, spacelike_ts: impl Into<TimeStamp>) {
        let tuplespaceInstanton: Vec<Key> = tuplespaceInstanton.iter().map(|x| Key::from_raw(x)).collect();
        assert!(self
            .store
            .rollback(self.ctx.clone(), tuplespaceInstanton, spacelike_ts.into())
            .is_err());
    }

    pub fn scan_locks_ok(
        &self,
        max_ts: impl Into<TimeStamp>,
        spacelike_key: &[u8],
        limit: usize,
        expect: Vec<LockInfo>,
    ) {
        let spacelike_key = if spacelike_key.is_empty() {
            None
        } else {
            Some(Key::from_raw(&spacelike_key))
        };

        assert_eq!(
            self.store
                .scan_locks(self.ctx.clone(), max_ts.into(), spacelike_key, limit)
                .unwrap(),
            expect
        );
    }

    pub fn resolve_lock_ok(
        &self,
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: Option<impl Into<TimeStamp>>,
    ) {
        self.store
            .resolve_lock(self.ctx.clone(), spacelike_ts.into(), commit_ts.map(Into::into))
            .unwrap();
    }

    pub fn resolve_lock_batch_ok(
        &self,
        spacelike_ts_1: impl Into<TimeStamp>,
        commit_ts_1: impl Into<TimeStamp>,
        spacelike_ts_2: impl Into<TimeStamp>,
        commit_ts_2: impl Into<TimeStamp>,
    ) {
        self.store
            .resolve_lock_batch(
                self.ctx.clone(),
                vec![
                    (spacelike_ts_1.into(), commit_ts_1.into()),
                    (spacelike_ts_2.into(), commit_ts_2.into()),
                ],
            )
            .unwrap();
    }

    pub fn resolve_lock_with_illegal_tso(
        &self,
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: Option<impl Into<TimeStamp>>,
    ) {
        let spacelike_ts = spacelike_ts.into();
        let commit_ts = commit_ts.map(Into::into);
        let resp = self
            .store
            .resolve_lock(self.ctx.clone(), spacelike_ts, commit_ts);
        self.expect_invalid_tso_err(resp, spacelike_ts, commit_ts.unwrap())
    }

    pub fn gc_ok(&self, safe_point: impl Into<TimeStamp>) {
        self.store.gc(self.ctx.clone(), safe_point.into()).unwrap();
    }

    pub fn raw_get_ok(&self, causet: String, key: Vec<u8>, value: Option<Vec<u8>>) {
        assert_eq!(
            self.store.raw_get(self.ctx.clone(), causet, key).unwrap(),
            value
        );
    }

    pub fn raw_put_ok(&self, causet: String, key: Vec<u8>, value: Vec<u8>) {
        self.store
            .raw_put(self.ctx.clone(), causet, key, value)
            .unwrap();
    }

    pub fn raw_put_err(&self, causet: String, key: Vec<u8>, value: Vec<u8>) {
        self.store
            .raw_put(self.ctx.clone(), causet, key, value)
            .unwrap_err();
    }

    pub fn raw_delete_ok(&self, causet: String, key: Vec<u8>) {
        self.store.raw_delete(self.ctx.clone(), causet, key).unwrap()
    }

    pub fn raw_delete_err(&self, causet: String, key: Vec<u8>) {
        self.store
            .raw_delete(self.ctx.clone(), causet, key)
            .unwrap_err();
    }

    pub fn raw_scan_ok(
        &self,
        causet: String,
        spacelike_key: Vec<u8>,
        limit: usize,
        expect: Vec<(&[u8], &[u8])>,
    ) {
        let result: Vec<KvPair> = self
            .store
            .raw_scan(self.ctx.clone(), causet, spacelike_key, None, limit)
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        let expect: Vec<KvPair> = expect
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        assert_eq!(result, expect);
    }

    pub fn test_txn_store_gc(&self, key: &str) {
        let key_bytes = key.as_bytes();
        self.put_ok(key_bytes, b"v1", 5, 10);
        self.put_ok(key_bytes, b"v2", 15, 20);
        self.gc_ok(30);
        self.get_none(key_bytes, 15);
        self.get_ok(key_bytes, 25, b"v2");
    }

    pub fn test_txn_store_gc3(&self, key_prefix: u8) {
        let key_len = 10_000;
        let key = vec![key_prefix; 1024];
        for k in 1u64..(MAX_TXN_WRITE_SIZE / key_len * 2) as u64 {
            self.put_ok(&key, b"", k * 10, k * 10 + 5);
        }
        self.delete_ok(&key, 1000, 1050);
        self.get_none(&key, 2000);
        self.gc_ok(2000);
        self.get_none(&key, 3000);
    }
}
