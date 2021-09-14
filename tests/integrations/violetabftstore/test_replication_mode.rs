// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::thread;
use std::time::Duration;

use ekvproto::replication_mode_timeshare::*;
use fidel_client::FidelClient;
use violetabft::evioletabft_timeshare::ConfChangeType;
use std::sync::mpsc;
use test_violetabftstore::*;
use violetabftstore::interlock::::config::*;
use violetabftstore::interlock::::HandyRwLock;

fn prepare_cluster() -> Cluster<ServerCluster> {
    let mut cluster = new_server_cluster(0, 3);
    cluster.fidel_client.disable_default_operator();
    cluster.fidel_client.configure_dr_auto_sync("zone");
    cluster.causet.violetabft_store.fidel_store_heartbeat_tick_interval = ReadableDuration::millis(50);
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 10;
    cluster.add_label(1, "zone", "ES");
    cluster.add_label(2, "zone", "ES");
    cluster.add_label(3, "zone", "WS");
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v0");
    cluster
}

/// When using DrAutoSync replication mode, data should be replicated to different labels
/// before committed.
#[test]
fn test_dr_auto_sync() {
    let mut cluster = prepare_cluster();
    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));
    let brane = cluster.get_brane(b"k1");
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_put_causet_cmd("default", b"k1", b"v1")],
        false,
    );
    request.mut_header().set_peer(new_peer(1, 1));
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, request, cb)
        .unwrap();
    rx.recv_timeout(Duration::from_millis(100)).unwrap();
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    thread::sleep(Duration::from_millis(100));
    let state = cluster.fidel_client.brane_replication_status(brane.get_id());
    assert_eq!(state.state_id, 1);
    assert_eq!(state.state, BraneReplicationState::IntegrityOverLabel);

    cluster.clear_lightlike_filters();
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_put_causet_cmd("default", b"k2", b"v2")],
        false,
    );
    request.mut_header().set_peer(new_peer(1, 1));
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, request, cb)
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    must_get_none(&cluster.get_engine(1), b"k2");
    let state = cluster.fidel_client.brane_replication_status(brane.get_id());
    assert_eq!(state.state_id, 1);
    assert_eq!(state.state, BraneReplicationState::IntegrityOverLabel);
}

/// Conf change should consider labels when DrAutoSync is chosen.
#[test]
fn test_check_conf_change() {
    let mut cluster = prepare_cluster();
    let fidel_client = cluster.fidel_client.clone();
    fidel_client.must_remove_peer(1, new_peer(2, 2));
    must_get_none(&cluster.get_engine(2), b"k1");
    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));
    fidel_client.must_add_peer(1, new_learner_peer(2, 4));
    let brane = cluster.get_brane(b"k1");
    // Peer 4 can be promoted as there will be enough quorum alive.
    let cc = new_change_peer_request(ConfChangeType::AddNode, new_peer(2, 4));
    let req = new_admin_request(brane.get_id(), brane.get_brane_epoch(), cc);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(!res.get_header().has_error(), "{:?}", res);
    must_get_none(&cluster.get_engine(2), b"k1");
    cluster.clear_lightlike_filters();
    must_get_equal(&cluster.get_engine(2), b"k1", b"v0");

    fidel_client.must_remove_peer(1, new_peer(3, 3));
    must_get_none(&cluster.get_engine(3), b"k1");
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    fidel_client.must_add_peer(1, new_learner_peer(3, 5));
    let brane = cluster.get_brane(b"k1");
    // Peer 5 can not be promoted as there is no enough quorum alive.
    let cc = new_change_peer_request(ConfChangeType::AddNode, new_peer(3, 5));
    let req = new_admin_request(brane.get_id(), brane.get_brane_epoch(), cc);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(
        res.get_header()
            .get_error()
            .get_message()
            .contains("unsafe to perform conf change peer"),
        "{:?}",
        res
    );
}

// Tests if group id is fideliod when adding new node and applying snapshot.
#[test]
fn test_fidelio_group_id() {
    let mut cluster = new_server_cluster(0, 2);
    let fidel_client = cluster.fidel_client.clone();
    cluster.add_label(1, "zone", "ES");
    cluster.add_label(2, "zone", "WS");
    fidel_client.disable_default_operator();
    fidel_client.configure_dr_auto_sync("zone");
    cluster.causet.violetabft_store.fidel_store_heartbeat_tick_interval = ReadableDuration::millis(50);
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 10;
    cluster.run_conf_change();
    cluster.must_put(b"k1", b"v0");
    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k0").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();
    // When a node is spacelikeed, all store information are loaded at once, so we need an extra node
    // to verify resolve will assign group id.
    cluster.add_label(3, "zone", "WS");
    cluster.add_new_engine();
    fidel_client.must_add_peer(left.id, new_peer(2, 2));
    fidel_client.must_add_peer(left.id, new_learner_peer(3, 3));
    fidel_client.must_add_peer(left.id, new_peer(3, 3));
    // If node 3's group id is not assigned, leader will make commit index as the smallest last
    // index of all followers.
    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));
    cluster.must_put(b"k11", b"v11");
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    must_get_equal(&cluster.get_engine(1), b"k11", b"v11");

    // So both node 1 and node 3 have fully resolved all stores. Further fidelios to group ID have
    // to be done when applying conf change and snapshot.
    cluster.clear_lightlike_filters();
    fidel_client.must_add_peer(right.id, new_peer(2, 4));
    fidel_client.must_add_peer(right.id, new_learner_peer(3, 5));
    fidel_client.must_add_peer(right.id, new_peer(3, 5));
    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));
    cluster.must_put(b"k3", b"v3");
    cluster.must_transfer_leader(right.id, new_peer(3, 5));
    cluster.must_put(b"k4", b"v4");
}

/// Tests if replication mode is switched successfully.
#[test]
fn test_switching_replication_mode() {
    let mut cluster = prepare_cluster();
    let brane = cluster.get_brane(b"k1");
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_put_causet_cmd("default", b"k2", b"v2")],
        false,
    );
    request.mut_header().set_peer(new_peer(1, 1));
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, request, cb)
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    must_get_none(&cluster.get_engine(1), b"k2");
    let state = cluster.fidel_client.brane_replication_status(brane.get_id());
    assert_eq!(state.state_id, 1);
    assert_eq!(state.state, BraneReplicationState::IntegrityOverLabel);

    cluster
        .fidel_client
        .switch_replication_mode(DrAutoSyncState::Async);
    rx.recv_timeout(Duration::from_millis(100)).unwrap();
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    thread::sleep(Duration::from_millis(100));
    let state = cluster.fidel_client.brane_replication_status(brane.get_id());
    assert_eq!(state.state_id, 2);
    assert_eq!(state.state, BraneReplicationState::SimpleMajority);

    cluster
        .fidel_client
        .switch_replication_mode(DrAutoSyncState::SyncRecover);
    thread::sleep(Duration::from_millis(100));
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_put_causet_cmd("default", b"k3", b"v3")],
        false,
    );
    request.mut_header().set_peer(new_peer(1, 1));
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, request, cb)
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    must_get_none(&cluster.get_engine(1), b"k3");
    let state = cluster.fidel_client.brane_replication_status(brane.get_id());
    assert_eq!(state.state_id, 3);
    assert_eq!(state.state, BraneReplicationState::SimpleMajority);

    cluster.clear_lightlike_filters();
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
    thread::sleep(Duration::from_millis(100));
    let state = cluster.fidel_client.brane_replication_status(brane.get_id());
    assert_eq!(state.state_id, 3);
    assert_eq!(state.state, BraneReplicationState::IntegrityOverLabel);
}

/// Ensures hibernate brane still works properly when switching replication mode.
#[test]
fn test_switching_replication_mode_hibernate() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.causet.violetabft_store.max_leader_missing_duration = ReadableDuration::hours(1);
    cluster.causet.violetabft_store.peer_stale_state_check_interval = ReadableDuration::minutes(30);
    cluster.causet.violetabft_store.abnormal_leader_missing_duration = ReadableDuration::hours(1);
    let fidel_client = cluster.fidel_client.clone();
    fidel_client.disable_default_operator();
    fidel_client.configure_dr_auto_sync("zone");
    cluster.causet.violetabft_store.fidel_store_heartbeat_tick_interval = ReadableDuration::millis(50);
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 20;
    cluster.add_label(1, "zone", "ES");
    cluster.add_label(2, "zone", "ES");
    cluster.add_label(3, "zone", "WS");
    let r = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v0");

    fidel_client.must_add_peer(r, new_peer(2, 2));
    fidel_client.must_add_peer(r, new_learner_peer(3, 3));
    let state = fidel_client.brane_replication_status(r);
    assert_eq!(state.state_id, 1);
    assert_eq!(state.state, BraneReplicationState::SimpleMajority);

    must_get_equal(&cluster.get_engine(3), b"k1", b"v0");
    // Wait for applightlike response after applying snapshot.
    thread::sleep(Duration::from_millis(50));
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    fidel_client.must_add_peer(r, new_peer(3, 3));
    // Wait for leader become hibernated.
    thread::sleep(
        cluster.causet.violetabft_store.violetabft_base_tick_interval.0
            * 2
            * (cluster.causet.violetabft_store.violetabft_election_timeout_ticks as u32),
    );
    cluster.clear_lightlike_filters();
    // Wait for brane heartbeat.
    thread::sleep(Duration::from_millis(100));
    let state = cluster.fidel_client.brane_replication_status(r);
    assert_eq!(state.state_id, 1);
    assert_eq!(state.state, BraneReplicationState::IntegrityOverLabel);
}

/// Tests if replication mode is switched successfully at runtime.
#[test]
fn test_migrate_replication_mode() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.fidel_client.disable_default_operator();
    cluster.causet.violetabft_store.fidel_store_heartbeat_tick_interval = ReadableDuration::millis(50);
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 10;
    cluster.add_label(1, "zone", "ES");
    cluster.add_label(2, "zone", "ES");
    cluster.add_label(3, "zone", "WS");
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));
    cluster.must_put(b"k1", b"v0");
    // Non exists label key can't tolerate any node unavailable.
    cluster.fidel_client.configure_dr_auto_sync("host");
    thread::sleep(Duration::from_millis(100));
    let brane = cluster.get_brane(b"k1");
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_put_causet_cmd("default", b"k2", b"v2")],
        false,
    );
    request.mut_header().set_peer(new_peer(1, 1));
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, request, cb)
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    must_get_none(&cluster.get_engine(1), b"k2");
    let state = cluster.fidel_client.brane_replication_status(brane.get_id());
    assert_eq!(state.state_id, 1);
    assert_eq!(state.state, BraneReplicationState::SimpleMajority);

    // Correct label key should resume committing log
    cluster.fidel_client.configure_dr_auto_sync("zone");
    rx.recv_timeout(Duration::from_millis(100)).unwrap();
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    thread::sleep(Duration::from_millis(100));
    let state = cluster.fidel_client.brane_replication_status(brane.get_id());
    assert_eq!(state.state_id, 2);
    assert_eq!(state.state, BraneReplicationState::IntegrityOverLabel);
}

/// Tests if labels are loaded correctly after rolling spacelike.
#[test]
fn test_loading_label_after_rolling_spacelike() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.fidel_client.disable_default_operator();
    cluster.causet.violetabft_store.fidel_store_heartbeat_tick_interval = ReadableDuration::millis(50);
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 10;
    cluster.create_engines();
    let r = cluster.bootstrap_conf_change();
    cluster.add_label(1, "zone", "ES");
    cluster.run_node(1).unwrap();
    cluster.add_label(2, "zone", "ES");
    cluster.run_node(2).unwrap();
    cluster.add_label(3, "zone", "WS");
    cluster.run_node(3).unwrap();

    let fidel_client = cluster.fidel_client.clone();
    fidel_client.must_add_peer(r, new_peer(2, 2));
    fidel_client.must_add_peer(r, new_peer(3, 3));
    cluster.must_transfer_leader(r, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));
    cluster.must_put(b"k2", b"v2");
    // Non exists label key can't tolerate any node unavailable.
    cluster.fidel_client.configure_dr_auto_sync("zone");
    thread::sleep(Duration::from_millis(100));
    cluster.must_put(b"k3", b"v3");
    thread::sleep(Duration::from_millis(100));
    let state = cluster.fidel_client.brane_replication_status(r);
    assert_eq!(state.state_id, 1);
    assert_eq!(state.state, BraneReplicationState::IntegrityOverLabel);
}
