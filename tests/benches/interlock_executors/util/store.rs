// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use edb::causet_storage::kv::LmdbSnapshot;
use edb::causet_storage::txn::{FixtureStore, SnapshotStore, CausetStore};

/// `MemStore` is a store provider that operates directly over a BTreeMap.
pub type MemStore = FixtureStore;

/// `LmdbStore` is a store provider that operates over a disk-based Lmdb causet_storage.
pub type LmdbStore = SnapshotStore<Arc<LmdbSnapshot>>;

pub trait StoreDescriber {
    /// Describes a store for Criterion to output.
    fn name() -> String;
}

impl<S: CausetStore> StoreDescriber for S {
    default fn name() -> String {
        unimplemented!()
    }
}

impl StoreDescriber for MemStore {
    fn name() -> String {
        "Memory".to_owned()
    }
}

impl StoreDescriber for LmdbStore {
    fn name() -> String {
        "Lmdb".to_owned()
    }
}
