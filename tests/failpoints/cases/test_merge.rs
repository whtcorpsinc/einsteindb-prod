// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::*;
use std::thread;
use std::time::*;

use ekvproto::meta_timeshare::Brane;
use ekvproto::violetabft_server_timeshare::{PeerState, VioletaBftMessage, BraneLocalState};
use violetabft::evioletabft_timeshare::MessageType;

use engine_lmdb::Compat;
use edb::{Peekable, Causet_VIOLETABFT};
use fidel_client::FidelClient;
use violetabftstore::store::*;
use test_violetabftstore::*;
use violetabftstore::interlock::::config::*;
use violetabftstore::interlock::::HandyRwLock;

/// Test if merge is rollback as expected.
#[test]
fn test_node_merge_rollback() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run_conf_change();

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    fidel_client.must_add_peer(left.get_id(), new_peer(2, 2));
    fidel_client.must_add_peer(right.get_id(), new_peer(2, 4));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let brane = fidel_client.get_brane(b"k1").unwrap();
    let target_brane = fidel_client.get_brane(b"k3").unwrap();

    let schedule_merge_fp = "on_schedule_merge";
    fail::causet(schedule_merge_fp, "return()").unwrap();

    // The call is finished when prepare_merge is applied.
    cluster.must_try_merge(brane.get_id(), target_brane.get_id());

    // Add a peer to trigger rollback.
    fidel_client.must_add_peer(right.get_id(), new_peer(3, 5));
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");

    let mut brane = fidel_client.get_brane(b"k1").unwrap();
    // After split and prepare_merge, version becomes 1 + 2 = 3;
    assert_eq!(brane.get_brane_epoch().get_version(), 3);
    // After ConfChange and prepare_merge, conf version becomes 1 + 2 = 3;
    assert_eq!(brane.get_brane_epoch().get_conf_ver(), 3);
    fail::remove(schedule_merge_fp);
    // Wait till rollback.
    cluster.must_put(b"k11", b"v11");

    // After rollback, version becomes 3 + 1 = 4;
    brane.mut_brane_epoch().set_version(4);
    for i in 1..3 {
        must_get_equal(&cluster.get_engine(i), b"k11", b"v11");
        let state_key = tuplespaceInstanton::brane_state_key(brane.get_id());
        let state: BraneLocalState = cluster
            .get_engine(i)
            .c()
            .get_msg_causet(Causet_VIOLETABFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Normal);
        assert_eq!(*state.get_brane(), brane);
    }

    fidel_client.must_remove_peer(right.get_id(), new_peer(3, 5));
    fail::causet(schedule_merge_fp, "return()").unwrap();

    let target_brane = fidel_client.get_brane(b"k3").unwrap();
    cluster.must_try_merge(brane.get_id(), target_brane.get_id());
    let mut brane = fidel_client.get_brane(b"k1").unwrap();

    // Split to trigger rollback.
    cluster.must_split(&right, b"k3");
    fail::remove(schedule_merge_fp);
    // Wait till rollback.
    cluster.must_put(b"k12", b"v12");

    // After premerge and rollback, conf_ver becomes 3 + 1 = 4, version becomes 4 + 2 = 6;
    brane.mut_brane_epoch().set_conf_ver(4);
    brane.mut_brane_epoch().set_version(6);
    for i in 1..3 {
        must_get_equal(&cluster.get_engine(i), b"k12", b"v12");
        let state_key = tuplespaceInstanton::brane_state_key(brane.get_id());
        let state: BraneLocalState = cluster
            .get_engine(i)
            .c()
            .get_msg_causet(Causet_VIOLETABFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Normal);
        assert_eq!(*state.get_brane(), brane);
    }
}

/// Test if merge is still working when respacelike a cluster during merge.
#[test]
fn test_node_merge_respacelike() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.run();

    let fidel_client = Arc::clone(&cluster.fidel_client);
    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let schedule_merge_fp = "on_schedule_merge";
    fail::causet(schedule_merge_fp, "return()").unwrap();

    cluster.must_try_merge(left.get_id(), right.get_id());
    let leader = cluster.leader_of_brane(left.get_id()).unwrap();

    cluster.shutdown();
    let engine = cluster.get_engine(leader.get_store_id());
    let state_key = tuplespaceInstanton::brane_state_key(left.get_id());
    let state: BraneLocalState = engine.c().get_msg_causet(Causet_VIOLETABFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Merging, "{:?}", state);
    let state_key = tuplespaceInstanton::brane_state_key(right.get_id());
    let state: BraneLocalState = engine.c().get_msg_causet(Causet_VIOLETABFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Normal, "{:?}", state);
    fail::remove(schedule_merge_fp);
    cluster.spacelike().unwrap();

    // Wait till merge is finished.
    fidel_client.check_merged_timeout(left.get_id(), Duration::from_secs(5));

    cluster.must_put(b"k4", b"v4");

    for i in 1..4 {
        must_get_equal(&cluster.get_engine(i), b"k4", b"v4");
        let state_key = tuplespaceInstanton::brane_state_key(left.get_id());
        let state: BraneLocalState = cluster
            .get_engine(i)
            .c()
            .get_msg_causet(Causet_VIOLETABFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Tombstone, "{:?}", state);
        let state_key = tuplespaceInstanton::brane_state_key(right.get_id());
        let state: BraneLocalState = cluster
            .get_engine(i)
            .c()
            .get_msg_causet(Causet_VIOLETABFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Normal, "{:?}", state);
        assert!(state.get_brane().get_spacelike_key().is_empty());
        assert!(state.get_brane().get_lightlike_key().is_empty());
    }

    // Now test if cluster works fine when it crash after merge is applied
    // but before notifying violetabftstore thread.
    let brane = fidel_client.get_brane(b"k1").unwrap();
    let peer_on_store1 = find_peer(&brane, 1).unwrap().to_owned();
    cluster.must_transfer_leader(brane.get_id(), peer_on_store1);
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();
    let peer_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), peer_on_store1);
    cluster.must_put(b"k11", b"v11");
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    let skip_destroy_fp = "violetabft_store_skip_destroy_peer";
    fail::causet(skip_destroy_fp, "return()").unwrap();
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    fidel_client.must_merge(left.get_id(), right.get_id());
    let peer = find_peer(&right, 3).unwrap().to_owned();
    fidel_client.must_remove_peer(right.get_id(), peer);
    cluster.shutdown();
    fail::remove(skip_destroy_fp);
    cluster.clear_lightlike_filters();
    cluster.spacelike().unwrap();
    must_get_none(&cluster.get_engine(3), b"k1");
    must_get_none(&cluster.get_engine(3), b"k3");
}

/// Test if merge is still working when respacelike a cluster during catching up logs for merge.
#[test]
fn test_node_merge_catch_up_logs_respacelike() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let fidel_client = Arc::clone(&cluster.fidel_client);
    let brane = fidel_client.get_brane(b"k1").unwrap();
    let peer_on_store1 = find_peer(&brane, 1).unwrap().to_owned();
    cluster.must_transfer_leader(brane.get_id(), peer_on_store1);
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    // make sure the peer of left brane on engine 3 has caught up logs.
    cluster.must_put(b"k0", b"v0");
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    ));
    cluster.must_put(b"k11", b"v11");
    must_get_none(&cluster.get_engine(3), b"k11");

    // after source peer is applied but before set it to tombstone
    fail::causet("after_handle_catch_up_logs_for_merge_1003", "return()").unwrap();
    fidel_client.must_merge(left.get_id(), right.get_id());
    thread::sleep(Duration::from_millis(100));
    cluster.shutdown();

    fail::remove("after_handle_catch_up_logs_for_merge_1003");
    cluster.spacelike().unwrap();
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
}

/// Test if leader election is working properly when catching up logs for merge.
#[test]
fn test_node_merge_catch_up_logs_leader_election() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.violetabft_base_tick_interval = ReadableDuration::millis(10);
    cluster.causet.violetabft_store.violetabft_election_timeout_ticks = 25;
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 12;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 12;
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(100);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let fidel_client = Arc::clone(&cluster.fidel_client);
    let brane = fidel_client.get_brane(b"k1").unwrap();
    let peer_on_store1 = find_peer(&brane, 1).unwrap().to_owned();
    cluster.must_transfer_leader(brane.get_id(), peer_on_store1);
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    let state1 = cluster.truncated_state(1000, 1);
    // let the entries committed but not applied
    fail::causet("on_handle_apply_1003", "pause").unwrap();
    for i in 2..20 {
        cluster.must_put(format!("k1{}", i).as_bytes(), b"v");
    }

    // wait to trigger compact violetabft log
    for _ in 0..50 {
        let state2 = cluster.truncated_state(1000, 1);
        if state1.get_index() != state2.get_index() {
            break;
        }
        sleep_ms(10);
    }

    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    ));
    cluster.must_put(b"k11", b"v11");
    must_get_none(&cluster.get_engine(3), b"k11");

    // let peer not destroyed before election timeout
    fail::causet("before_peer_destroy_1003", "pause").unwrap();
    fail::remove("on_handle_apply_1003");
    fidel_client.must_merge(left.get_id(), right.get_id());

    // wait election timeout
    thread::sleep(Duration::from_millis(500));
    fail::remove("before_peer_destroy_1003");

    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
}

// Test if merge is working properly if no need to catch up logs,
// also there may be a propose of compact log after prepare merge is proposed.
#[test]
fn test_node_merge_catch_up_logs_no_need() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.violetabft_base_tick_interval = ReadableDuration::millis(10);
    cluster.causet.violetabft_store.violetabft_election_timeout_ticks = 25;
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 12;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 12;
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(100);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let fidel_client = Arc::clone(&cluster.fidel_client);
    let brane = fidel_client.get_brane(b"k1").unwrap();
    let peer_on_store1 = find_peer(&brane, 1).unwrap().to_owned();
    cluster.must_transfer_leader(brane.get_id(), peer_on_store1);
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    // put some tuplespaceInstanton to trigger compact violetabft log
    for i in 2..20 {
        cluster.must_put(format!("k1{}", i).as_bytes(), b"v");
    }

    // let the peer of left brane on store 3 falls behind.
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    ));

    // make sure the peer is isolated.
    cluster.must_put(b"k11", b"v11");
    must_get_none(&cluster.get_engine(3), b"k11");

    // propose merge but not let apply index make progress.
    fail::causet("apply_after_prepare_merge", "pause").unwrap();
    fidel_client.merge_brane(left.get_id(), right.get_id());
    must_get_none(&cluster.get_engine(3), b"k11");

    // wait to trigger compact violetabft log
    thread::sleep(Duration::from_millis(100));

    // let source brane not merged
    fail::causet("before_handle_catch_up_logs_for_merge", "pause").unwrap();
    fail::causet("after_handle_catch_up_logs_for_merge", "pause").unwrap();
    // due to `before_handle_catch_up_logs_for_merge` failpoint, we already pass `apply_index < catch_up_logs.merge.get_commit()`
    // so now can let apply index make progress.
    fail::remove("apply_after_prepare_merge");

    // make sure all the logs are committed, including the compact command
    cluster.clear_lightlike_filters();
    thread::sleep(Duration::from_millis(50));

    // let merge process continue
    fail::remove("before_handle_catch_up_logs_for_merge");
    fail::remove("after_handle_catch_up_logs_for_merge");
    thread::sleep(Duration::from_millis(50));

    // the source brane should be merged and the peer should be destroyed.
    assert!(fidel_client.check_merged(left.get_id()));
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    cluster.must_brane_not_exist(left.get_id(), 3);
}

/// Test if merging state will be removed after accepting a snapshot.
#[test]
fn test_node_merge_recover_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 12;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 12;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let brane = fidel_client.get_brane(b"k3").unwrap();
    let target_brane = fidel_client.get_brane(b"k1").unwrap();

    let schedule_merge_fp = "on_schedule_merge";
    fail::causet(schedule_merge_fp, "return()").unwrap();

    cluster.must_try_merge(brane.get_id(), target_brane.get_id());

    // Remove a peer to trigger rollback.
    fidel_client.must_remove_peer(left.get_id(), left.get_peers()[0].to_owned());
    must_get_none(&cluster.get_engine(3), b"k4");

    let step_store_3_brane_1 = "step_message_3_1";
    fail::causet(step_store_3_brane_1, "return()").unwrap();
    fail::remove(schedule_merge_fp);

    for i in 0..100 {
        cluster.must_put(format!("k4{}", i).as_bytes(), b"v4");
    }
    fail::remove(step_store_3_brane_1);
    must_get_equal(&cluster.get_engine(3), b"k40", b"v4");
    cluster.must_transfer_leader(1, new_peer(3, 3));
    cluster.must_put(b"k40", b"v5");
}

// Test if a merge handled properly when there are two different snapshots of one brane arrive
// in one violetabftstore tick.
#[test]
fn test_node_merge_multiple_snapshots_together() {
    test_node_merge_multiple_snapshots(true)
}

// Test if a merge handled properly when there are two different snapshots of one brane arrive
// in different violetabftstore tick.
#[test]
fn test_node_merge_multiple_snapshots_not_together() {
    test_node_merge_multiple_snapshots(false)
}

fn test_node_merge_multiple_snapshots(together: bool) {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();
    // make it gc quickly to trigger snapshot easily
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.causet.violetabft_store.violetabft_base_tick_interval = ReadableDuration::millis(10);
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 10;
    cluster.causet.violetabft_store.merge_max_log_gap = 9;
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k3").unwrap();

    let target_leader = right
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 1)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(right.get_id(), target_leader);
    let target_leader = left
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 2)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(left.get_id(), target_leader);
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");

    // So cluster becomes:
    //  left brane: 1         2(leader) I 3
    // right brane: 1(leader) 2         I 3
    // I means isolation.(here just means 3 can not receive applightlike log)
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(right.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    ));
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    ));

    // Add a collect snapshot filter, it will delay snapshots until have collected multiple snapshots from different peers
    cluster.sim.wl().add_recv_filter(
        3,
        Box::new(LeadingDuplicatedSnapshotFilter::new(
            Arc::new(AtomicBool::new(false)),
            together,
        )),
    );
    // Write some data to trigger a snapshot of right brane.
    for i in 200..210 {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }
    // Wait for snapshot to generate and lightlike
    thread::sleep(Duration::from_millis(100));

    // Merge left and right brane, due to isolation, the branes on store 3 are not merged yet.
    fidel_client.must_merge(left.get_id(), right.get_id());
    thread::sleep(Duration::from_millis(200));

    // Let peer of right brane on store 3 to make applightlike response to trigger a new snapshot
    // one is snapshot before merge, the other is snapshot after merge.
    // Here blocks violetabftstore for a while to make it not to apply snapshot and receive new log now.
    fail::causet("on_violetabft_ready", "sleep(100)").unwrap();
    cluster.clear_lightlike_filters();
    thread::sleep(Duration::from_millis(200));
    // Filter message again to make sure peer on store 3 can not catch up CommitMerge log
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    ));
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(right.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    ));
    // Cause filter is added again, no need to block violetabftstore anymore
    fail::causet("on_violetabft_ready", "off").unwrap();

    // Wait some time to let already merged peer on store 1 or store 2 to notify
    // the peer of left brane on store 3 is stale.
    thread::sleep(Duration::from_millis(300));

    cluster.must_put(b"k9", b"v9");
    // let follower can reach the new log, then commit merge
    cluster.clear_lightlike_filters();
    must_get_equal(&cluster.get_engine(3), b"k9", b"v9");
}

fn prepare_request_snapshot_cluster() -> (Cluster<NodeCluster>, Brane, Brane) {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let brane = fidel_client.get_brane(b"k3").unwrap();
    let target_brane = fidel_client.get_brane(b"k1").unwrap();

    // Make sure peer 1 is the leader.
    cluster.must_transfer_leader(brane.get_id(), new_peer(1, 1));

    (cluster, brane, target_brane)
}

// Test if request snapshot is rejected during merging.
#[test]
fn test_node_merge_reject_request_snapshot() {
    let (mut cluster, brane, target_brane) = prepare_request_snapshot_cluster();

    let apply_prepare_merge_fp = "apply_before_prepare_merge";
    fail::causet(apply_prepare_merge_fp, "pause").unwrap();
    let prepare_merge = new_prepare_merge(target_brane);
    let mut req = new_admin_request(brane.get_id(), brane.get_brane_epoch(), prepare_merge);
    req.mut_header().set_peer(new_peer(1, 1));
    cluster
        .sim
        .rl()
        .async_command_on_node(1, req, Callback::None)
        .unwrap();
    sleep_ms(200);

    // Install snapshot filter before requesting snapshot.
    let (tx, rx) = mpsc::channel();
    let notifier = Mutex::new(Some(tx));
    cluster.sim.wl().add_recv_filter(
        2,
        Box::new(RecvSnapshotFilter {
            notifier,
            brane_id: brane.get_id(),
        }),
    );
    cluster.must_request_snapshot(2, brane.get_id());
    // Leader should reject request snapshot while merging.
    rx.recv_timeout(Duration::from_millis(500)).unwrap_err();

    // Transfer leader to peer 3, new leader should reject request snapshot too.
    cluster.must_transfer_leader(brane.get_id(), new_peer(3, 3));
    rx.recv_timeout(Duration::from_millis(500)).unwrap_err();
    fail::remove(apply_prepare_merge_fp);
}

// Test if merge is rejected during requesting snapshot.
#[test]
fn test_node_request_snapshot_reject_merge() {
    let (cluster, brane, target_brane) = prepare_request_snapshot_cluster();

    // Pause generating snapshot.
    let brane_gen_snap_fp = "brane_gen_snap";
    fail::causet(brane_gen_snap_fp, "pause").unwrap();

    // Install snapshot filter before requesting snapshot.
    let (tx, rx) = mpsc::channel();
    let notifier = Mutex::new(Some(tx));
    cluster.sim.wl().add_recv_filter(
        2,
        Box::new(RecvSnapshotFilter {
            notifier,
            brane_id: brane.get_id(),
        }),
    );
    cluster.must_request_snapshot(2, brane.get_id());
    // Leader can not generate a snapshot.
    rx.recv_timeout(Duration::from_millis(500)).unwrap_err();

    let prepare_merge = new_prepare_merge(target_brane.clone());
    let mut req = new_admin_request(brane.get_id(), brane.get_brane_epoch(), prepare_merge);
    req.mut_header().set_peer(new_peer(1, 1));
    cluster
        .sim
        .rl()
        .async_command_on_node(1, req, Callback::None)
        .unwrap();
    sleep_ms(200);

    // Merge will never happen.
    let target = cluster.get_brane(target_brane.get_spacelike_key());
    assert_eq!(target, target_brane);
    fail::remove(brane_gen_snap_fp);
    // Drop cluster to ensure notifier will not lightlike any message after rx is dropped.
    drop(cluster);
}

// Test if compact log is ignored after premerge was applied and respacelike
// I.e. is_merging flag should be set after respacelike
#[test]
fn test_node_merge_respacelike_after_apply_premerge_before_apply_compact_log() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.merge_max_log_gap = 10;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 11;
    // Rely on this config to trigger a compact log
    cluster.causet.violetabft_store.violetabft_log_gc_size_limit = ReadableSize(1);
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(10);

    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();
    // Prevent gc_log_tick to propose a compact log
    let violetabft_gc_log_tick_fp = "on_violetabft_gc_log_tick";
    fail::causet(violetabft_gc_log_tick_fp, "return()").unwrap();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();
    let left_peer_1 = find_peer(&left, 1).cloned().unwrap();
    cluster.must_transfer_leader(left.get_id(), left_peer_1);

    // Make log gap between store 1 and store 3, for min_index in preMerge
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    for i in 0..6 {
        cluster.must_put(format!("k1{}", i).as_bytes(), b"v1");
    }
    // Prevent on_apply_res to fidelio merge_state in Peer
    // If not, almost everything cannot propose including compact log
    let on_apply_res_fp = "on_apply_res";
    fail::causet(on_apply_res_fp, "return()").unwrap();

    cluster.must_try_merge(left.get_id(), right.get_id());

    cluster.clear_lightlike_filters();
    // Prevent apply fsm to apply compact log
    let handle_apply_fp = "on_handle_apply";
    fail::causet(handle_apply_fp, "return()").unwrap();

    let state1 = cluster.truncated_state(left.get_id(), 1);
    fail::remove(violetabft_gc_log_tick_fp);

    // Wait for compact log to be proposed and committed maybe
    sleep_ms(30);

    cluster.shutdown();

    fail::remove(handle_apply_fp);
    fail::remove(on_apply_res_fp);
    // Prevent sched_merge_tick to propose CommitMerge
    let schedule_merge_fp = "on_schedule_merge";
    fail::causet(schedule_merge_fp, "return()").unwrap();

    cluster.spacelike().unwrap();

    // Wait for compact log to apply
    for _ in 0..50 {
        let state2 = cluster.truncated_state(left.get_id(), 1);
        if state1.get_index() != state2.get_index() {
            break;
        }
        sleep_ms(10);
    }
    // Now schedule merge
    fail::remove(schedule_merge_fp);

    fidel_client.check_merged_timeout(left.get_id(), Duration::from_secs(5));

    cluster.must_put(b"k123", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k123", b"v2");
}

/// Tests whether stale merge is rollback properly if it merge to the same target brane again later.
#[test]
fn test_node_failed_merge_before_succeed_merge() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.merge_max_log_gap = 30;
    cluster.causet.violetabft_store.store_batch_system.max_batch_size = 1;
    cluster.causet.violetabft_store.store_batch_system.pool_size = 2;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }
    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k5");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let mut right = fidel_client.get_brane(b"k5").unwrap();
    let left_peer_1 = find_peer(&left, 1).cloned().unwrap();
    cluster.must_transfer_leader(left.get_id(), left_peer_1);

    let left_peer_3 = find_peer(&left, 3).cloned().unwrap();
    assert_eq!(left_peer_3.get_id(), 1003);

    // Prevent sched_merge_tick to propose CommitMerge
    let schedule_merge_fp = "on_schedule_merge";
    fail::causet(schedule_merge_fp, "return()").unwrap();

    // To minimize peers log gap for merging
    cluster.must_put(b"k11", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k11", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k11", b"v2");
    // Make peer 1003 can't receive PrepareMerge and RollbackMerge log
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));

    cluster.must_try_merge(left.get_id(), right.get_id());

    // Change right brane's epoch to make this merge failed
    cluster.must_split(&right, b"k8");
    fail::remove(schedule_merge_fp);
    // Wait for left brane to rollback merge
    cluster.must_put(b"k12", b"v2");
    // Prevent the `PrepareMerge` and `RollbackMerge` log lightlikeing to apply fsm after
    // cleaning lightlike filter. Since this method is just to check `RollbackMerge`,
    // the `PrepareMerge` may escape, but it makes the best effort.
    let before_lightlike_rollback_merge_1003_fp = "before_lightlike_rollback_merge_1003";
    fail::causet(before_lightlike_rollback_merge_1003_fp, "return").unwrap();
    cluster.clear_lightlike_filters();

    right = fidel_client.get_brane(b"k5").unwrap();
    let right_peer_1 = find_peer(&right, 1).cloned().unwrap();
    cluster.must_transfer_leader(right.get_id(), right_peer_1);
    // Add some data for checking data integrity check at a later time
    for i in 0..5 {
        cluster.must_put(format!("k2{}", i).as_bytes(), b"v3");
    }
    // Do a really succeed merge
    fidel_client.must_merge(left.get_id(), right.get_id());
    // Wait right brane to lightlike CatchUpLogs to left brane.
    sleep_ms(100);
    // After executing CatchUpLogs in source peer fsm, the committed log will lightlike
    // to apply fsm in the lightlike of this batch. So even the first `on_ready_prepare_merge`
    // is executed after CatchUplogs, the latter committed logs is still sent to apply fsm
    // if CatchUpLogs and `on_ready_prepare_merge` is in different batch.
    //
    // In this case, the data is complete because the wrong up-to-date msg from the
    // first `on_ready_prepare_merge` is sent after all committed log.
    // Sleep a while to wait apply fsm to lightlike `on_ready_prepare_merge` to peer fsm.
    let after_lightlike_to_apply_1003_fp = "after_lightlike_to_apply_1003";
    fail::causet(after_lightlike_to_apply_1003_fp, "sleep(300)").unwrap();

    fail::remove(before_lightlike_rollback_merge_1003_fp);
    // Wait `after_lightlike_to_apply_1003` timeout
    sleep_ms(300);
    fail::remove(after_lightlike_to_apply_1003_fp);
    // Check the data integrity
    for i in 0..5 {
        must_get_equal(&cluster.get_engine(3), format!("k2{}", i).as_bytes(), b"v3");
    }
}

/// Tests whether the source peer is destroyed correctly when transferring leader during committing merge.
///
/// In the previous merge flow, target peer deletes meta of source peer without marking it as plightlikeing remove.
/// If source peer becomes leader at the same time, it will panic due to corrupted meta.
#[test]
fn test_node_merge_transfer_leader() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.store_batch_system.max_batch_size = 1;
    cluster.causet.violetabft_store.store_batch_system.pool_size = 2;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    let left_peer_1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_peer_1.clone());

    let schedule_merge_fp = "on_schedule_merge";
    fail::causet(schedule_merge_fp, "return()").unwrap();

    cluster.must_try_merge(left.get_id(), right.get_id());

    let left_peer_3 = find_peer(&left, 3).unwrap().to_owned();
    assert_eq!(left_peer_3.get_id(), 1003);
    // Prevent peer 1003 to handle ready when it's leader
    let before_handle_violetabft_ready_1003 = "before_handle_violetabft_ready_1003";
    fail::causet(before_handle_violetabft_ready_1003, "pause").unwrap();

    let epoch = cluster.get_brane_epoch(left.get_id());
    let mut transfer_leader_req =
        new_admin_request(left.get_id(), &epoch, new_transfer_leader_cmd(left_peer_3));
    transfer_leader_req.mut_header().set_peer(left_peer_1);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, transfer_leader_req, Callback::None)
        .unwrap();
    fail::remove(schedule_merge_fp);

    fidel_client.check_merged_timeout(left.get_id(), Duration::from_secs(5));

    fail::remove(before_handle_violetabft_ready_1003);
    sleep_ms(100);
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

#[test]
fn test_node_merge_cascade_merge_with_apply_yield() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k5");
    let brane = fidel_client.get_brane(b"k5").unwrap();
    cluster.must_split(&brane, b"k9");

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    let r1 = fidel_client.get_brane(b"k1").unwrap();
    let r2 = fidel_client.get_brane(b"k5").unwrap();
    let r3 = fidel_client.get_brane(b"k9").unwrap();

    fidel_client.must_merge(r2.get_id(), r1.get_id());
    assert_eq!(r1.get_id(), 1000);
    let yield_apply_1000_fp = "yield_apply_1000";
    fail::causet(yield_apply_1000_fp, "80%3*return()").unwrap();

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v2");
    }

    fidel_client.must_merge(r3.get_id(), r1.get_id());

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v3");
    }
}

// Test if the rollback merge proposal is proposed before the majority of peers want to rollback
#[test]
fn test_node_multiple_rollback_merge() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.right_derive_when_split = true;
    cluster.causet.violetabft_store.merge_check_tick_interval = ReadableDuration::millis(20);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    let left_peer_1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_peer_1.clone());
    assert_eq!(left_peer_1.get_id(), 1001);

    let on_schedule_merge_fp = "on_schedule_merge";
    let on_check_merge_not_1001_fp = "on_check_merge_not_1001";

    let mut right_peer_1_id = find_peer(&right, 1).unwrap().get_id();

    for i in 0..3 {
        fail::causet(on_schedule_merge_fp, "return()").unwrap();
        cluster.must_try_merge(left.get_id(), right.get_id());
        // Change the epoch of target brane and the merge will fail
        fidel_client.must_remove_peer(right.get_id(), new_peer(1, right_peer_1_id));
        right_peer_1_id += 100;
        fidel_client.must_add_peer(right.get_id(), new_peer(1, right_peer_1_id));
        // Only the source leader is running `on_check_merge`
        fail::causet(on_check_merge_not_1001_fp, "return()").unwrap();
        fail::remove(on_schedule_merge_fp);
        // In previous implementation, rollback merge proposal can be proposed by leader itself
        // So wait for the leader propose rollback merge if possible
        sleep_ms(100);
        // Check if the source brane is still in merging mode.
        let mut l_r = fidel_client.get_brane(b"k1").unwrap();
        let req = new_request(
            l_r.get_id(),
            l_r.take_brane_epoch(),
            vec![new_put_causet_cmd(
                "default",
                format!("k1{}", i).as_bytes(),
                b"vv",
            )],
            false,
        );
        let resp = cluster
            .call_command_on_leader(req, Duration::from_millis(100))
            .unwrap();
        if !resp
            .get_header()
            .get_error()
            .get_message()
            .contains("merging mode")
        {
            panic!("resp {:?} does not contain merging mode error", resp);
        }

        fail::remove(on_check_merge_not_1001_fp);
        // Write data for waiting the merge to rollback easily
        cluster.must_put(format!("k1{}", i).as_bytes(), b"vv");
        // Make sure source brane is not merged to target brane
        assert_eq!(fidel_client.get_brane(b"k1").unwrap().get_id(), left.get_id());
    }
}

// In the previous implementation, the source peer will propose rollback merge
// after the local target peer's epoch is larger than recorded previously.
// But it's wrong. This test constructs a case that writing data to the source brane
// after merging. This operation can succeed in the previous implementation which
// causes data loss.
// In the current implementation, the rollback merge proposal can be proposed only when
// the number of peers who want to rollback merge is greater than the majority of all
// peers. If so, this merge is impossible to succeed.
// PS: A peer who wants to rollback merge means its local target peer's epoch is larger
// than recorded.
#[test]
fn test_node_merge_write_data_to_source_brane_after_merging() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.causet.violetabft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    // For snapshot after merging
    cluster.causet.violetabft_store.merge_max_log_gap = 10;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 12;
    cluster.causet.violetabft_store.apply_batch_system.max_batch_size = 1;
    cluster.causet.violetabft_store.apply_batch_system.pool_size = 2;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");

    let mut brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    let right_peer_2 = find_peer(&right, 2).cloned().unwrap();
    assert_eq!(right_peer_2.get_id(), 2);
    let on_handle_apply_2_fp = "on_handle_apply_2";
    fail::causet(on_handle_apply_2_fp, "pause").unwrap();

    let right_peer_1 = find_peer(&right, 1).cloned().unwrap();
    cluster.must_transfer_leader(right.get_id(), right_peer_1);

    let left_peer_3 = find_peer(&left, 3).cloned().unwrap();
    cluster.must_transfer_leader(left.get_id(), left_peer_3.clone());

    let schedule_merge_fp = "on_schedule_merge";
    fail::causet(schedule_merge_fp, "return()").unwrap();

    cluster.must_try_merge(left.get_id(), right.get_id());

    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));

    fail::remove(schedule_merge_fp);

    fidel_client.check_merged_timeout(left.get_id(), Duration::from_secs(5));

    brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let state1 = cluster.apply_state(brane.get_id(), 1);
    for i in 0..15 {
        cluster.must_put(format!("k2{}", i).as_bytes(), b"v2");
    }
    // Wait for log compaction
    for _ in 0..50 {
        let state2 = cluster.apply_state(brane.get_id(), 1);
        if state2.get_truncated_state().get_index() >= state1.get_applied_index() {
            break;
        }
        sleep_ms(10);
    }
    // Ignore this msg to make left brane exist.
    let on_has_merge_target_fp = "on_has_merge_target";
    fail::causet(on_has_merge_target_fp, "return").unwrap();

    cluster.clear_lightlike_filters();
    // On store 3, now the right brane is fideliod by snapshot not applying logs
    // so the left brane still exist.
    // Wait for left brane to rollback merge (in previous wrong implementation)
    sleep_ms(200);
    // Write data to left brane
    let mut new_left = left;
    let mut epoch = new_left.take_brane_epoch();
    // prepareMerge => conf_ver + 1, version + 1
    // rollbackMerge => version + 1
    epoch.set_conf_ver(epoch.get_conf_ver() + 1);
    epoch.set_version(epoch.get_version() + 2);
    let mut req = new_request(
        new_left.get_id(),
        epoch,
        vec![new_put_causet_cmd("default", b"k11", b"v11")],
        false,
    );
    req.mut_header().set_peer(left_peer_3);
    if let Ok(()) = cluster
        .sim
        .rl()
        .async_command_on_node(3, req, Callback::None)
    {
        sleep_ms(200);
        // The write must not succeed
        must_get_none(&cluster.get_engine(2), b"k11");
        must_get_none(&cluster.get_engine(3), b"k11");
    }

    fail::remove(on_handle_apply_2_fp);
}

/// In previous implementation, destroying its source peer(s) and applying snapshot is not **atomic**.
/// It may break the rule of our merging process.
///
/// A edb crash after its source peers have destroyed but this target peer does not become to
/// `Applying` state which means it will not apply snapshot after this edb respacelikes.
/// After this edb respacelikes, a new leader may lightlike logs to this target peer, then the panic may happen
/// because it can not find its source peers when applying `CommitMerge` log.
///
/// This test is to reproduce above situation.
#[test]
fn test_node_merge_crash_before_snapshot_then_catch_up_logs() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.causet.violetabft_store.merge_max_log_gap = 10;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 11;
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(50);
    // Make merge check resume quickly.
    cluster.causet.violetabft_store.violetabft_base_tick_interval = ReadableDuration::millis(10);
    cluster.causet.violetabft_store.violetabft_election_timeout_ticks = 10;
    // election timeout must be greater than lease
    cluster.causet.violetabft_store.violetabft_store_max_leader_lease = ReadableDuration::millis(99);
    cluster.causet.violetabft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    cluster.causet.violetabft_store.peer_stale_state_check_interval = ReadableDuration::millis(500);

    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    let on_violetabft_gc_log_tick_fp = "on_violetabft_gc_log_tick";
    fail::causet(on_violetabft_gc_log_tick_fp, "return()").unwrap();

    cluster.run();

    let mut brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_on_store1);
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();
    cluster.must_transfer_leader(right.get_id(), right_on_store1);

    cluster.must_put(b"k1", b"v1");

    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));

    fidel_client.must_merge(left.get_id(), right.get_id());

    brane = fidel_client.get_brane(b"k1").unwrap();
    // Write some logs and the logs' number is greater than `violetabft_log_gc_count_limit`
    // for latter log compaction
    for i in 2..15 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }

    // Aim at making peer 2 only know the compact log but do not know it is committed
    let condition = Arc::new(AtomicBool::new(false));
    let recv_filter = Box::new(
        BranePacketFilter::new(brane.get_id(), 2)
            .direction(Direction::Recv)
            .when(condition.clone())
            .set_msg_callback(Arc::new(move |msg: &VioletaBftMessage| {
                if !condition.load(Ordering::Acquire)
                    && msg.get_message().get_msg_type() == MessageType::MsgApplightlike
                    && !msg.get_message().get_entries().is_empty()
                {
                    condition.store(true, Ordering::Release);
                }
            })),
    );
    cluster.sim.wl().add_recv_filter(2, recv_filter);

    let state1 = cluster.truncated_state(brane.get_id(), 1);
    // Remove log compaction failpoint
    fail::remove(on_violetabft_gc_log_tick_fp);
    // Wait to trigger compact violetabft log
    let timer = Instant::now();
    loop {
        let state2 = cluster.truncated_state(brane.get_id(), 1);
        if state1.get_index() != state2.get_index() {
            break;
        }
        if timer.elapsed() > Duration::from_secs(3) {
            panic!("log compaction not finish after 3 seconds.");
        }
        sleep_ms(10);
    }

    let peer_on_store3 = find_peer(&brane, 3).unwrap().to_owned();
    assert_eq!(peer_on_store3.get_id(), 3);
    // Make peer 3 do not handle snapshot ready
    // In previous implementation, destroying its source peer and applying snapshot is not atomic.
    // So making its source peer be destroyed and do not apply snapshot to reproduce the problem
    let before_handle_snapshot_ready_3_fp = "before_handle_snapshot_ready_3";
    fail::causet(before_handle_snapshot_ready_3_fp, "return()").unwrap();

    cluster.clear_lightlike_filters();
    // Peer 1 will lightlike snapshot to peer 3
    // Source peer lightlikes msg to others to get target brane info until the election timeout.
    // The max election timeout is 2 * 10 * 10 = 200ms
    let election_timeout = 2
        * cluster.causet.violetabft_store.violetabft_base_tick_interval.as_millis()
        * cluster.causet.violetabft_store.violetabft_election_timeout_ticks as u64;
    sleep_ms(election_timeout + 100);

    cluster.stop_node(1);
    cluster.stop_node(3);

    cluster.sim.wl().clear_recv_filters(2);
    fail::remove(before_handle_snapshot_ready_3_fp);
    cluster.run_node(3).unwrap();
    // Peer 2 will become leader and it don't know the compact log is committed.
    // So it will lightlike logs not snapshot to peer 3
    for i in 20..30 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }
    must_get_equal(&cluster.get_engine(3), b"k29", b"v");
}

/// Test if snapshot is applying correctly when crash happens.
#[test]
fn test_node_merge_crash_when_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.causet.violetabft_store.merge_max_log_gap = 10;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 11;
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(50);
    // Make merge check resume quickly.
    cluster.causet.violetabft_store.violetabft_base_tick_interval = ReadableDuration::millis(10);
    cluster.causet.violetabft_store.violetabft_election_timeout_ticks = 10;
    // election timeout must be greater than lease
    cluster.causet.violetabft_store.violetabft_store_max_leader_lease = ReadableDuration::millis(99);
    cluster.causet.violetabft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    cluster.causet.violetabft_store.peer_stale_state_check_interval = ReadableDuration::millis(500);

    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    let on_violetabft_gc_log_tick_fp = "on_violetabft_gc_log_tick";
    fail::causet(on_violetabft_gc_log_tick_fp, "return()").unwrap();

    cluster.run();

    let mut brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    brane = fidel_client.get_brane(b"k2").unwrap();
    cluster.must_split(&brane, b"k3");

    brane = fidel_client.get_brane(b"k3").unwrap();
    cluster.must_split(&brane, b"k4");

    brane = fidel_client.get_brane(b"k4").unwrap();
    cluster.must_split(&brane, b"k5");

    let r1 = fidel_client.get_brane(b"k1").unwrap();
    let r1_on_store1 = find_peer(&r1, 1).unwrap().to_owned();
    cluster.transfer_leader(r1.get_id(), r1_on_store1);
    let r2 = fidel_client.get_brane(b"k2").unwrap();
    let r2_on_store1 = find_peer(&r2, 1).unwrap().to_owned();
    cluster.transfer_leader(r2.get_id(), r2_on_store1);
    let r3 = fidel_client.get_brane(b"k3").unwrap();
    let r3_on_store1 = find_peer(&r3, 1).unwrap().to_owned();
    cluster.transfer_leader(r3.get_id(), r3_on_store1);
    let r4 = fidel_client.get_brane(b"k4").unwrap();
    let r4_on_store1 = find_peer(&r4, 1).unwrap().to_owned();
    cluster.transfer_leader(r4.get_id(), r4_on_store1);
    let r5 = fidel_client.get_brane(b"k5").unwrap();
    let r5_on_store1 = find_peer(&r5, 1).unwrap().to_owned();
    cluster.transfer_leader(r5.get_id(), r5_on_store1);

    for i in 1..5 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
        must_get_equal(&cluster.get_engine(3), format!("k{}", i).as_bytes(), b"v");
    }

    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));

    fidel_client.must_merge(r2.get_id(), r3.get_id());
    fidel_client.must_merge(r4.get_id(), r3.get_id());
    fidel_client.must_merge(r1.get_id(), r3.get_id());
    fidel_client.must_merge(r5.get_id(), r3.get_id());

    for i in 1..5 {
        for j in 1..20 {
            cluster.must_put(format!("k{}{}", i, j).as_bytes(), b"vvv");
        }
    }

    brane = fidel_client.get_brane(b"k1").unwrap();

    let state1 = cluster.truncated_state(brane.get_id(), 1);
    // Remove log compaction failpoint
    fail::remove(on_violetabft_gc_log_tick_fp);
    // Wait to trigger compact violetabft log
    let timer = Instant::now();
    loop {
        let state2 = cluster.truncated_state(brane.get_id(), 1);
        if state1.get_index() != state2.get_index() {
            break;
        }
        if timer.elapsed() > Duration::from_secs(3) {
            panic!("log compaction not finish after 3 seconds.");
        }
        sleep_ms(10);
    }

    let on_brane_worker_apply_fp = "on_brane_worker_apply";
    fail::causet(on_brane_worker_apply_fp, "return()").unwrap();
    let on_brane_worker_destroy_fp = "on_brane_worker_destroy";
    fail::causet(on_brane_worker_destroy_fp, "return()").unwrap();

    cluster.clear_lightlike_filters();
    let timer = Instant::now();
    loop {
        let local_state = cluster.brane_local_state(brane.get_id(), 3);
        if local_state.get_state() == PeerState::Applying {
            break;
        }
        if timer.elapsed() > Duration::from_secs(1) {
            panic!("not become applying state after 1 seconds.");
        }
        sleep_ms(10);
    }
    cluster.stop_node(3);
    fail::remove(on_brane_worker_apply_fp);
    fail::remove(on_brane_worker_destroy_fp);
    cluster.run_node(3).unwrap();

    for i in 1..5 {
        for j in 1..20 {
            must_get_equal(
                &cluster.get_engine(3),
                format!("k{}{}", i, j).as_bytes(),
                b"vvv",
            );
        }
    }
}
