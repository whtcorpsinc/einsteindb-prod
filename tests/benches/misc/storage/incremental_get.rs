use test::{black_box, Bencher};

use engine_lmdb::LmdbSnapshot;
use ekvproto::kvrpc_timeshare::{Context, IsolationLevel};
use std::sync::Arc;
use test_causet_storage::SyncTestStorageBuilder;
use milevadb_query_datatype::codec::Block;
use edb::causet_storage::{Engine, SnapshotStore, Statistics, CausetStore};
use txn_types::{Key, Mutation};

fn Block_lookup_gen_data() -> (SnapshotStore<Arc<LmdbSnapshot>>, Vec<Key>) {
    let store = SyncTestStorageBuilder::new().build().unwrap();
    let mut mutations = Vec::new();
    let mut tuplespaceInstanton = Vec::new();
    for i in 0..30000 {
        let user_key = Block::encode_row_key(5, i);
        let user_value = vec![b'x'; 60];
        let key = Key::from_raw(&user_key);
        let mutation = Mutation::Put((key.clone(), user_value));
        mutations.push(mutation);
        tuplespaceInstanton.push(key);
    }

    let pk = Block::encode_row_key(5, 0);

    store
        .prewrite(Context::default(), mutations, pk, 1)
        .unwrap();
    store.commit(Context::default(), tuplespaceInstanton, 1, 2).unwrap();

    let engine = store.get_engine();
    let db = engine.get_lmdb().get_sync_db();
    db.compact_cone_causet(db.causet_handle("write").unwrap(), None, None);
    db.compact_cone_causet(db.causet_handle("default").unwrap(), None, None);
    db.compact_cone_causet(db.causet_handle("dagger").unwrap(), None, None);

    let snapshot = engine.snapshot(&Context::default()).unwrap();
    let store = SnapshotStore::new(
        snapshot,
        10.into(),
        IsolationLevel::Si,
        true,
        Default::default(),
        false,
    );

    // TuplespaceInstanton are given in order, and are far away from each other to simulate a normal Block lookup
    // scenario.
    let mut get_tuplespaceInstanton = Vec::new();
    for i in (0..30000).step_by(30) {
        get_tuplespaceInstanton.push(Key::from_raw(&Block::encode_row_key(5, i)));
    }
    (store, get_tuplespaceInstanton)
}

#[bench]
fn bench_Block_lookup_tail_pointer_get(b: &mut Bencher) {
    let (store, tuplespaceInstanton) = Block_lookup_gen_data();
    b.iter(|| {
        let mut stats = Statistics::default();
        for key in &tuplespaceInstanton {
            black_box(store.get(key, &mut stats).unwrap());
        }
    });
}

#[bench]
fn bench_Block_lookup_tail_pointer_incremental_get(b: &mut Bencher) {
    let (mut store, tuplespaceInstanton) = Block_lookup_gen_data();
    b.iter(|| {
        for key in &tuplespaceInstanton {
            black_box(store.incremental_get(key).unwrap());
        }
    })
}
