// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::path::Path;
use std::sync::{Arc, Mutex};

use tempfile::Builder;

use ekvproto::metapb;
use ekvproto::violetabft_serverpb::BraneLocalState;

use concurrency_manager::ConcurrencyManager;
use engine_lmdb::{Compat, LmdbEngine};
use engine_promises::{Engines, Peekable, ALL_CAUSETS, CAUSET_VIOLETABFT};
use violetabftstore::interlock::InterlockHost;
use violetabftstore::store::fsm::store::StoreMeta;
use violetabftstore::store::{bootstrap_store, fsm, AutoSplitController, SnapManager};
use test_violetabftstore::*;
use einsteindb::import::SSTImporter;
use einsteindb::server::Node;
use einsteindb_util::config::VersionTrack;
use einsteindb_util::worker::{FutureWorker, Worker};

fn test_bootstrap_idempotent<T: Simulator>(cluster: &mut Cluster<T>) {
    // assume that there is a node  bootstrap the cluster and add brane in fidel successfully
    cluster.add_first_brane().unwrap();
    // now at same time spacelike the another node, and will recive cluster is not bootstrap
    // it will try to bootstrap with a new brane, but will failed
    // the brane number still 1
    cluster.spacelike().unwrap();
    cluster.check_branes_number(1);
    cluster.shutdown();
    sleep_ms(500);
    cluster.spacelike().unwrap();
    cluster.check_branes_number(1);
}

#[test]
fn test_node_bootstrap_with_prepared_data() {
    // create a node
    let fidel_client = Arc::new(TestFidelClient::new(0, false));
    let causet = new_einsteindb_config(0);

    let (_, system) = fsm::create_violetabft_batch_system(&causet.violetabft_store);
    let simulate_trans = SimulateTransport::new(ChannelTransport::new());
    let tmp_path = Builder::new().prefix("test_cluster").temfidelir().unwrap();
    let engine = Arc::new(
        engine_lmdb::raw_util::new_engine(tmp_path.path().to_str().unwrap(), None, ALL_CAUSETS, None)
            .unwrap(),
    );
    let tmp_path_violetabft = tmp_path.path().join(Path::new("violetabft"));
    let violetabft_engine = Arc::new(
        engine_lmdb::raw_util::new_engine(tmp_path_violetabft.to_str().unwrap(), None, &[], None)
            .unwrap(),
    );
    let engines = Engines::new(
        LmdbEngine::from_db(Arc::clone(&engine)),
        LmdbEngine::from_db(Arc::clone(&violetabft_engine)),
    );
    let tmp_mgr = Builder::new().prefix("test_cluster").temfidelir().unwrap();

    let mut node = Node::new(
        system,
        &causet.server,
        Arc::new(VersionTrack::new(causet.violetabft_store.clone())),
        Arc::clone(&fidel_client),
        Arc::default(),
    );
    let snap_mgr = SnapManager::new(tmp_mgr.path().to_str().unwrap());
    let fidel_worker = FutureWorker::new("test-fidel-worker");

    // assume there is a node has bootstrapped the cluster and add brane in fidel successfully
    bootstrap_with_first_brane(Arc::clone(&fidel_client)).unwrap();

    // now another node at same time begin bootstrap node, but panic after prepared bootstrap
    // now LMDB must have some prepare data
    bootstrap_store(&engines, 0, 1).unwrap();
    let brane = node.prepare_bootstrap_cluster(&engines, 1).unwrap();
    assert!(engine
        .c()
        .get_msg::<metapb::Brane>(tuplespaceInstanton::PREPARE_BOOTSTRAP_KEY)
        .unwrap()
        .is_some());
    let brane_state_key = tuplespaceInstanton::brane_state_key(brane.get_id());
    assert!(engine
        .c()
        .get_msg_causet::<BraneLocalState>(CAUSET_VIOLETABFT, &brane_state_key)
        .unwrap()
        .is_some());

    // Create interlock.
    let interlock_host = InterlockHost::new(node.get_router());

    let importer = {
        let dir = tmp_path.path().join("import-sst");
        Arc::new(SSTImporter::new(dir, None).unwrap())
    };

    // try to respacelike this node, will clear the prepare data
    node.spacelike(
        engines,
        simulate_trans,
        snap_mgr,
        fidel_worker,
        Arc::new(Mutex::new(StoreMeta::new(0))),
        interlock_host,
        importer,
        Worker::new("split"),
        AutoSplitController::default(),
        ConcurrencyManager::new(1.into()),
    )
    .unwrap();
    assert!(Arc::clone(&engine)
        .c()
        .get_msg::<metapb::Brane>(tuplespaceInstanton::PREPARE_BOOTSTRAP_KEY)
        .unwrap()
        .is_none());
    assert!(engine
        .c()
        .get_msg_causet::<BraneLocalState>(CAUSET_VIOLETABFT, &brane_state_key)
        .unwrap()
        .is_none());
    assert_eq!(fidel_client.get_branes_number() as u32, 1);
    node.stop();
}

#[test]
fn test_node_bootstrap_idempotent() {
    let mut cluster = new_node_cluster(0, 3);
    test_bootstrap_idempotent(&mut cluster);
}
