// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use engine_lmdb::raw::Cone;
use engine_lmdb::util::get_causet_handle;
use edb::{MiscExt, Causet_WRITE};
use tuplespaceInstanton::{data_key, DATA_MAX_KEY};
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Duration;
use test_violetabftstore::*;
use edb::causet_storage::tail_pointer::{TimeStamp, Write, WriteType};
use violetabftstore::interlock::::config::*;
use txn_types::Key;

fn gen_tail_pointer_put_kv(
    k: &[u8],
    v: &[u8],
    spacelike_ts: TimeStamp,
    commit_ts: TimeStamp,
) -> (Vec<u8>, Vec<u8>) {
    let k = Key::from_encoded(data_key(k));
    let k = k.applightlike_ts(commit_ts);
    let w = Write::new(WriteType::Put, spacelike_ts, Some(v.to_vec()));
    (k.as_encoded().clone(), w.as_ref().to_bytes())
}

fn gen_delete_k(k: &[u8], commit_ts: TimeStamp) -> Vec<u8> {
    let k = Key::from_encoded(data_key(k));
    let k = k.applightlike_ts(commit_ts);
    k.as_encoded().clone()
}

fn test_compact_after_delete<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.causet.violetabft_store.brane_compact_check_interval = ReadableDuration::millis(100);
    cluster.causet.violetabft_store.brane_compact_min_tombstones = 500;
    cluster.causet.violetabft_store.brane_compact_tombstones_percent = 50;
    cluster.causet.violetabft_store.brane_compact_check_step = 1;
    cluster.run();

    for i in 0..1000 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        let (k, v) = gen_tail_pointer_put_kv(k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        cluster.must_put_causet(Causet_WRITE, &k, &v);
    }
    for engines in cluster.engines.values() {
        engines.kv.flush_causet(Causet_WRITE, true).unwrap();
    }
    let (lightlikeer, receiver) = mpsc::channel();
    let sync_lightlikeer = Mutex::new(lightlikeer);
    fail::causet_callback(
        "violetabftstore::compact::CheckAndCompact:AfterCompact",
        move || {
            let lightlikeer = sync_lightlikeer.dagger().unwrap();
            lightlikeer.lightlike(true).unwrap();
        },
    )
    .unwrap();
    for i in 0..1000 {
        let k = format!("k{}", i);
        let k = gen_delete_k(k.as_bytes(), 2.into());
        cluster.must_delete_causet(Causet_WRITE, &k);
    }
    for engines in cluster.engines.values() {
        let causet = get_causet_handle(&engines.kv.as_inner(), Causet_WRITE).unwrap();
        engines.kv.as_inner().flush_causet(causet, true).unwrap();
    }

    // wait for compaction.
    receiver.recv_timeout(Duration::from_millis(5000)).unwrap();

    for engines in cluster.engines.values() {
        let causet_handle = get_causet_handle(&engines.kv.as_inner(), Causet_WRITE).unwrap();
        let approximate_size = engines
            .kv
            .as_inner()
            .get_approximate_sizes_causet(causet_handle, &[Cone::new(b"", DATA_MAX_KEY)])[0];
        assert_eq!(approximate_size, 0);
    }
}

#[test]
fn test_node_compact_after_delete() {
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    test_compact_after_delete(&mut cluster);
}
