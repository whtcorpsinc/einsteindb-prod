// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use edb::Peekable;
use ekvproto::{meta_timeshare, violetabft_server_timeshare};
use test_violetabftstore::*;

fn test_bootstrap_half_way_failure(fp: &str) {
    let fidel_client = Arc::new(TestFidelClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(fidel_client.clone())));
    let mut cluster = Cluster::new(0, 5, sim, fidel_client);

    // Try to spacelike this node, return after persisted some tuplespaceInstanton.
    fail::causet(fp, "return").unwrap();
    cluster.spacelike().unwrap_err();

    let engines = cluster.dbs[0].clone();
    let ident = engines
        .kv
        .get_msg::<violetabft_server_timeshare::StoreIdent>(tuplespaceInstanton::STORE_IDENT_KEY)
        .unwrap()
        .unwrap();
    let store_id = ident.get_store_id();
    debug!("store id {:?}", store_id);
    cluster.set_bootstrapped(store_id, 0);

    // Check whether it can bootstrap cluster successfully.
    fail::remove(fp);
    cluster.spacelike().unwrap();

    assert!(engines
        .kv
        .get_msg::<meta_timeshare::Brane>(tuplespaceInstanton::PREPARE_BOOTSTRAP_KEY)
        .unwrap()
        .is_none());

    let k = b"k1";
    let v = b"v1";
    cluster.must_put(k, v);
    must_get_equal(&cluster.get_engine(store_id), k, v);
    for id in cluster.engines.tuplespaceInstanton() {
        must_get_equal(&cluster.get_engine(*id), k, v);
    }
}

#[test]
fn test_bootstrap_half_way_failure_after_bootstrap_store() {
    let fp = "node_after_bootstrap_store";
    test_bootstrap_half_way_failure(fp);
}

#[test]
fn test_bootstrap_half_way_failure_after_prepare_bootstrap_cluster() {
    let fp = "node_after_prepare_bootstrap_cluster";
    test_bootstrap_half_way_failure(fp);
}

#[test]
fn test_bootstrap_half_way_failure_after_bootstrap_cluster() {
    let fp = "node_after_bootstrap_cluster";
    test_bootstrap_half_way_failure(fp);
}
