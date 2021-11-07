// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use futures::executor::block_on;
use ekvproto::kvrpc_timeshare::{Context, GetRequest, LockInfo};
use violetabftstore::interlock::BraneInfoProvider;
use violetabftstore::router::VioletaBftStoreBlackHole;
use edb::server::gc_worker::{AutoGcConfig, GcConfig, GcSafePointProvider, GcWorker};
use edb::causet_storage::config::Config;
use edb::causet_storage::kv::LmdbEngine;
use edb::causet_storage::lock_manager::DummyLockManager;
use edb::causet_storage::{
    txn::commands, Engine, PrewriteResult, Result, causet_storage, TestEngineBuilder, TestStorageBuilder,
    TxnStatus,
};
use violetabftstore::interlock::::collections::HashMap;
use txn_types::{Key, KvPair, Mutation, TimeStamp, Value};

/// A builder to build a `SyncTestStorage`.
///
/// Only used for test purpose.
pub struct SyncTestStorageBuilder<E: Engine> {
    engine: E,
    config: Option<Config>,
    gc_config: Option<GcConfig>,
}

impl SyncTestStorageBuilder<LmdbEngine> {
    pub fn new() -> Self {
        Self {
            engine: TestEngineBuilder::new().build().unwrap(),
            config: None,
            gc_config: None,
        }
    }
}

impl<E: Engine> SyncTestStorageBuilder<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            config: None,
            gc_config: None,
        }
    }

    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn gc_config(mut self, gc_config: GcConfig) -> Self {
        self.gc_config = Some(gc_config);
        self
    }

    pub fn build(mut self) -> Result<SyncTestStorage<E>> {
        let mut builder =
            TestStorageBuilder::from_engine_and_lock_mgr(self.engine.clone(), DummyLockManager {});
        if let Some(config) = self.config.take() {
            builder = builder.config(config);
        }
        let mut gc_worker = GcWorker::new(
            self.engine,
            VioletaBftStoreBlackHole,
            self.gc_config.unwrap_or_default(),
            Default::default(),
        );
        gc_worker.spacelike()?;

        Ok(SyncTestStorage {
            store: builder.build()?,
            gc_worker,
        })
    }
}

/// A `causet_storage` like structure with sync API.
///
/// Only used for test purpose.
#[derive(Clone)]
pub struct SyncTestStorage<E: Engine> {
    gc_worker: GcWorker<E, VioletaBftStoreBlackHole>,
    store: causet_storage<E, DummyLockManager>,
}

impl<E: Engine> SyncTestStorage<E> {
    pub fn spacelike_auto_gc<S: GcSafePointProvider, R: BraneInfoProvider>(
        &mut self,
        causet: AutoGcConfig<S, R>,
    ) {
        self.gc_worker.spacelike_auto_gc(causet).unwrap();
    }

    pub fn get_causet_storage(&self) -> causet_storage<E, DummyLockManager> {
        self.store.clone()
    }

    pub fn get_engine(&self) -> E {
        self.store.get_engine()
    }

    pub fn get(
        &self,
        ctx: Context,
        key: &Key,
        spacelike_ts: impl Into<TimeStamp>,
    ) -> Result<Option<Value>> {
        block_on(self.store.get(ctx, key.to_owned(), spacelike_ts.into()))
    }

    #[allow(dead_code)]
    pub fn batch_get(
        &self,
        ctx: Context,
        tuplespaceInstanton: &[Key],
        spacelike_ts: impl Into<TimeStamp>,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(self.store.batch_get(ctx, tuplespaceInstanton.to_owned(), spacelike_ts.into()))
    }

    pub fn batch_get_command(
        &self,
        ctx: Context,
        tuplespaceInstanton: &[&[u8]],
        spacelike_ts: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let requests: Vec<GetRequest> = tuplespaceInstanton
            .to_owned()
            .into_iter()
            .map(|key| {
                let mut req = GetRequest::default();
                req.set_context(ctx.clone());
                req.set_key(key.to_owned());
                req.set_version(spacelike_ts);
                req
            })
            .collect();
        let resp = block_on(self.store.batch_get_command(requests))?;
        let mut values = vec![];

        for value in resp.into_iter() {
            values.push(value?);
        }
        Ok(values)
    }

    pub fn scan(
        &self,
        ctx: Context,
        spacelike_key: Key,
        lightlike_key: Option<Key>,
        limit: usize,
        key_only: bool,
        spacelike_ts: impl Into<TimeStamp>,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(self.store.scan(
            ctx,
            spacelike_key,
            lightlike_key,
            limit,
            0,
            spacelike_ts.into(),
            key_only,
            false,
        ))
    }

    pub fn reverse_scan(
        &self,
        ctx: Context,
        spacelike_key: Key,
        lightlike_key: Option<Key>,
        limit: usize,
        key_only: bool,
        spacelike_ts: impl Into<TimeStamp>,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(self.store.scan(
            ctx,
            spacelike_key,
            lightlike_key,
            limit,
            0,
            spacelike_ts.into(),
            key_only,
            true,
        ))
    }

    pub fn prewrite(
        &self,
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        spacelike_ts: impl Into<TimeStamp>,
    ) -> Result<PrewriteResult> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Prewrite::with_context(mutations, primary, spacelike_ts.into(), ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn commit(
        &self,
        ctx: Context,
        tuplespaceInstanton: Vec<Key>,
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) -> Result<TxnStatus> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Commit::new(tuplespaceInstanton, spacelike_ts.into(), commit_ts.into(), ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn cleanup(
        &self,
        ctx: Context,
        key: Key,
        spacelike_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Cleanup::new(key, spacelike_ts.into(), current_ts.into(), ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn rollback(
        &self,
        ctx: Context,
        tuplespaceInstanton: Vec<Key>,
        spacelike_ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Rollback::new(tuplespaceInstanton, spacelike_ts.into().into(), ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn scan_locks(
        &self,
        ctx: Context,
        max_ts: impl Into<TimeStamp>,
        spacelike_key: Option<Key>,
        limit: usize,
    ) -> Result<Vec<LockInfo>> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::ScanLock::new(max_ts.into(), spacelike_key, limit, ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn resolve_lock(
        &self,
        ctx: Context,
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: Option<impl Into<TimeStamp>>,
    ) -> Result<()> {
        let mut txn_status = HashMap::default();
        txn_status.insert(
            spacelike_ts.into(),
            commit_ts.map(Into::into).unwrap_or_else(TimeStamp::zero),
        );
        wait_op!(|cb| self.store.sched_txn_command(
            commands::ResolveLockReadPhase::new(txn_status, None, ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn resolve_lock_batch(
        &self,
        ctx: Context,
        txns: Vec<(TimeStamp, TimeStamp)>,
    ) -> Result<()> {
        let txn_status: HashMap<TimeStamp, TimeStamp> = txns.into_iter().collect();
        wait_op!(|cb| self.store.sched_txn_command(
            commands::ResolveLockReadPhase::new(txn_status, None, ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn gc(&self, _: Context, safe_point: impl Into<TimeStamp>) -> Result<()> {
        wait_op!(|cb| self.gc_worker.gc(safe_point.into(), cb)).unwrap()
    }

    pub fn raw_get(&self, ctx: Context, causet: String, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        block_on(self.store.raw_get(ctx, causet, key))
    }

    pub fn raw_put(&self, ctx: Context, causet: String, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        wait_op!(|cb| self.store.raw_put(ctx, causet, key, value, cb)).unwrap()
    }

    pub fn raw_delete(&self, ctx: Context, causet: String, key: Vec<u8>) -> Result<()> {
        wait_op!(|cb| self.store.raw_delete(ctx, causet, key, cb)).unwrap()
    }

    pub fn raw_scan(
        &self,
        ctx: Context,
        causet: String,
        spacelike_key: Vec<u8>,
        lightlike_key: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(
            self.store
                .raw_scan(ctx, causet, spacelike_key, lightlike_key, limit, false, false),
        )
    }

    pub fn reverse_raw_scan(
        &self,
        ctx: Context,
        causet: String,
        spacelike_key: Vec<u8>,
        lightlike_key: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(
            self.store
                .raw_scan(ctx, causet, spacelike_key, lightlike_key, limit, false, true),
        )
    }
}
