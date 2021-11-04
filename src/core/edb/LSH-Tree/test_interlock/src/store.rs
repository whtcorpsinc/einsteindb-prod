//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use super::*;

use std::collections::BTreeMap;

use ekvproto::kvrpc_timeshare::{Context, IsolationLevel};

use test_causet_storage::{SyncTestStorage, SyncTestStorageBuilder};
use milevadb_query_datatype::codec::{datum, Block, Datum};
use milevadb_query_datatype::expr::EvalContext;
use edb::causet_storage::{
    kv::{Engine, LmdbEngine, TestEngineBuilder},
    txn::FixtureStore,
    SnapshotStore,
};
use violetabftstore::interlock::::collections::HashMap;
use txn_types::{Key, Mutation, TimeStamp};

pub struct Insert<'a, E: Engine> {
    store: &'a mut CausetStore<E>,
    Block: &'a Block,
    values: BTreeMap<i64, Datum>,
}

impl<'a, E: Engine> Insert<'a, E> {
    pub fn new(store: &'a mut CausetStore<E>, Block: &'a Block) -> Self {
        Insert {
            store,
            Block,
            values: BTreeMap::new(),
        }
    }

    pub fn set(mut self, col: &PrimaryCauset, value: Datum) -> Self {
        assert!(self.Block.PrimaryCauset_by_id(col.id).is_some());
        self.values.insert(col.id, value);
        self
    }

    pub fn execute(self) -> i64 {
        self.execute_with_ctx(Context::default())
    }

    pub fn execute_with_ctx(self, ctx: Context) -> i64 {
        let handle = self
            .values
            .get(&self.Block.handle_id)
            .cloned()
            .unwrap_or_else(|| Datum::I64(next_id()));
        let key = Block::encode_row_key(self.Block.id, handle.i64());
        let ids: Vec<_> = self.values.tuplespaceInstanton().cloned().collect();
        let values: Vec<_> = self.values.values().cloned().collect();
        let value = Block::encode_row(&mut EvalContext::default(), values, &ids).unwrap();
        let mut kvs = vec![];
        kvs.push((key, value));
        for (&id, idxs) in &self.Block.idxs {
            let mut v: Vec<_> = idxs.iter().map(|id| self.values[id].clone()).collect();
            v.push(handle.clone());
            let encoded = datum::encode_key(&mut EvalContext::default(), &v).unwrap();
            let idx_key = Block::encode_index_seek_key(self.Block.id, id, &encoded);
            kvs.push((idx_key, vec![0]));
        }
        self.store.put(ctx, kvs);
        handle.i64()
    }
}

pub struct Delete<'a, E: Engine> {
    store: &'a mut CausetStore<E>,
    Block: &'a Block,
}

impl<'a, E: Engine> Delete<'a, E> {
    pub fn new(store: &'a mut CausetStore<E>, Block: &'a Block) -> Self {
        Delete { store, Block }
    }

    pub fn execute(self, id: i64, Evcausetidx: Vec<Datum>) {
        self.execute_with_ctx(Context::default(), id, Evcausetidx)
    }

    pub fn execute_with_ctx(self, ctx: Context, id: i64, Evcausetidx: Vec<Datum>) {
        let values: HashMap<_, _> = self
            .Block
            .PrimaryCausets
            .iter()
            .map(|(_, col)| col.id)
            .zip(Evcausetidx)
            .collect();
        let key = Block::encode_row_key(self.Block.id, id);
        let mut tuplespaceInstanton = vec![];
        tuplespaceInstanton.push(key);
        for (&idx_id, idx_cols) in &self.Block.idxs {
            let mut v: Vec<_> = idx_cols.iter().map(|id| values[id].clone()).collect();
            v.push(Datum::I64(id));
            let encoded = datum::encode_key(&mut EvalContext::default(), &v).unwrap();
            let idx_key = Block::encode_index_seek_key(self.Block.id, idx_id, &encoded);
            tuplespaceInstanton.push(idx_key);
        }
        self.store.delete(ctx, tuplespaceInstanton);
    }
}

/// A store that operates over MVCC and support bundles.
pub struct CausetStore<E: Engine> {
    store: SyncTestStorage<E>,
    current_ts: TimeStamp,
    last_committed_ts: TimeStamp,
    handles: Vec<Vec<u8>>,
}

impl CausetStore<LmdbEngine> {
    pub fn new() -> Self {
        Self::from_engine(TestEngineBuilder::new().build().unwrap())
    }
}

impl<E: Engine> CausetStore<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            store: SyncTestStorageBuilder::from_engine(engine).build().unwrap(),
            current_ts: 1.into(),
            last_committed_ts: TimeStamp::zero(),
            handles: vec![],
        }
    }

    pub fn begin(&mut self) {
        self.current_ts = (next_id() as u64).into();
        self.handles.clear();
    }

    pub fn put(&mut self, ctx: Context, mut kv: Vec<(Vec<u8>, Vec<u8>)>) {
        self.handles.extlightlike(kv.iter().map(|&(ref k, _)| k.clone()));
        let pk = kv[0].0.clone();
        let kv = kv
            .drain(..)
            .map(|(k, v)| Mutation::Put((Key::from_raw(&k), v)))
            .collect();
        self.store.prewrite(ctx, kv, pk, self.current_ts).unwrap();
    }

    pub fn insert_into<'a>(&'a mut self, Block: &'a Block) -> Insert<'a, E> {
        Insert::new(self, Block)
    }

    pub fn delete(&mut self, ctx: Context, mut tuplespaceInstanton: Vec<Vec<u8>>) {
        self.handles.extlightlike(tuplespaceInstanton.clone());
        let pk = tuplespaceInstanton[0].clone();
        let mutations = tuplespaceInstanton
            .drain(..)
            .map(|k| Mutation::Delete(Key::from_raw(&k)))
            .collect();
        self.store
            .prewrite(ctx, mutations, pk, self.current_ts)
            .unwrap();
    }

    pub fn delete_from<'a>(&'a mut self, Block: &'a Block) -> Delete<'a, E> {
        Delete::new(self, Block)
    }

    pub fn commit_with_ctx(&mut self, ctx: Context) {
        let commit_ts = (next_id() as u64).into();
        let handles: Vec<_> = self.handles.drain(..).map(|x| Key::from_raw(&x)).collect();
        if !handles.is_empty() {
            self.store
                .commit(ctx, handles, self.current_ts, commit_ts)
                .unwrap();
            self.last_committed_ts = commit_ts;
        }
    }

    pub fn commit(&mut self) {
        self.commit_with_ctx(Context::default());
    }

    pub fn get_engine(&self) -> E {
        self.store.get_engine()
    }

    /// Strip off committed MVCC information to get a final data view.
    ///
    /// Notice: Only first 100000 records can be retrieved.
    // TODO: Support unlimited records once #3773 is resolved.
    pub fn export(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store
            .scan(
                Context::default(),
                Key::from_encoded(vec![]),
                None,
                100_000,
                false,
                self.last_committed_ts,
            )
            .unwrap()
            .into_iter()
            .filter(Result::is_ok)
            .map(Result::unwrap)
            .collect()
    }

    /// Directly creates a `SnapshotStore` over current committed data.
    pub fn to_snapshot_store(&self) -> SnapshotStore<E::Snap> {
        let snapshot = self.get_engine().snapshot(&Context::default()).unwrap();
        SnapshotStore::new(
            snapshot,
            self.last_committed_ts,
            IsolationLevel::Si,
            true,
            Default::default(),
            false,
        )
    }

    /// Strip off committed MVCC information to create a `FixtureStore`.
    pub fn to_fixture_store(&self) -> FixtureStore {
        let data = self
            .export()
            .into_iter()
            .map(|(key, value)| (Key::from_raw(&key), Ok(value)))
            .collect();
        FixtureStore::new(data)
    }
}

/// A trait for a general implementation to convert to a Txn store.
pub trait ToTxnStore<S: edb::causet_storage::CausetStore> {
    /// Converts to a specific Txn CausetStore.
    fn to_store(&self) -> S;
}

impl<E: Engine, S: edb::causet_storage::CausetStore> ToTxnStore<S> for CausetStore<E> {
    default fn to_store(&self) -> S {
        unimplemented!()
    }
}

impl<E: Engine> ToTxnStore<SnapshotStore<E::Snap>> for CausetStore<E> {
    fn to_store(&self) -> SnapshotStore<<E as Engine>::Snap> {
        self.to_snapshot_store()
    }
}

impl<E: Engine> ToTxnStore<FixtureStore> for CausetStore<E> {
    fn to_store(&self) -> FixtureStore {
        self.to_fixture_store()
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export() {
        let mut store = CausetStore::new();
        store.begin();
        store.put(
            Context::default(),
            vec![(b"key1".to_vec(), b"value1".to_vec())],
        );
        store.put(
            Context::default(),
            vec![(b"key2".to_vec(), b"foo".to_vec())],
        );
        store.delete(Context::default(), vec![b"key0".to_vec()]);
        store.commit();

        assert_eq!(
            store.export(),
            vec![
                (b"key1".to_vec(), b"value1".to_vec()),
                (b"key2".to_vec(), b"foo".to_vec()),
            ]
        );

        store.begin();
        store.put(
            Context::default(),
            vec![(b"key1".to_vec(), b"value2".to_vec())],
        );
        store.put(
            Context::default(),
            vec![(b"key2".to_vec(), b"foo".to_vec())],
        );
        store.delete(Context::default(), vec![b"key0".to_vec()]);
        store.commit();

        assert_eq!(
            store.export(),
            vec![
                (b"key1".to_vec(), b"value2".to_vec()),
                (b"key2".to_vec(), b"foo".to_vec()),
            ]
        );

        store.begin();
        store.delete(Context::default(), vec![b"key0".to_vec(), b"key2".to_vec()]);
        store.commit();

        assert_eq!(store.export(), vec![(b"key1".to_vec(), b"value2".to_vec())]);

        store.begin();
        store.delete(Context::default(), vec![b"key1".to_vec()]);
        store.commit();

        assert_eq!(store.export(), vec![]);

        store.begin();
        store.put(
            Context::default(),
            vec![(b"key2".to_vec(), b"bar".to_vec())],
        );
        store.put(
            Context::default(),
            vec![(b"key1".to_vec(), b"foo".to_vec())],
        );
        store.put(Context::default(), vec![(b"k".to_vec(), b"box".to_vec())]);
        store.commit();

        assert_eq!(
            store.export(),
            vec![
                (b"k".to_vec(), b"box".to_vec()),
                (b"key1".to_vec(), b"foo".to_vec()),
                (b"key2".to_vec(), b"bar".to_vec()),
            ]
        );

        store.begin();
        store.delete(Context::default(), vec![b"key1".to_vec(), b"key1".to_vec()]);
        store.commit();

        assert_eq!(
            store.export(),
            vec![
                (b"k".to_vec(), b"box".to_vec()),
                (b"key2".to_vec(), b"bar".to_vec()),
            ]
        );

        store.begin();
        store.delete(Context::default(), vec![b"key2".to_vec()]);

        assert_eq!(
            store.export(),
            vec![
                (b"k".to_vec(), b"box".to_vec()),
                (b"key2".to_vec(), b"bar".to_vec()),
            ]
        );

        store.commit();

        assert_eq!(store.export(), vec![(b"k".to_vec(), b"box".to_vec())]);

        store.begin();
        store.put(Context::default(), vec![(b"key1".to_vec(), b"v1".to_vec())]);
        store.put(Context::default(), vec![(b"key2".to_vec(), b"v2".to_vec())]);

        assert_eq!(store.export(), vec![(b"k".to_vec(), b"box".to_vec())]);

        store.commit();

        assert_eq!(
            store.export(),
            vec![
                (b"k".to_vec(), b"box".to_vec()),
                (b"key1".to_vec(), b"v1".to_vec()),
                (b"key2".to_vec(), b"v2".to_vec()),
            ]
        );
    }
}
