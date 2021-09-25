// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::iter::*;
use std::sync::atomic::Ordering;
use std::sync::*;
use std::thread;
use std::time::*;

use ekvproto::kvrpc_timeshare::Context;
use ekvproto::violetabft_cmd_timeshare::CmdType;
use ekvproto::violetabft_server_timeshare::{PeerState, BraneLocalState};
use violetabft::evioletabft_timeshare::MessageType;

use engine_lmdb::Compat;
use edb::Peekable;
use edb::{Causet_VIOLETABFT, Causet_WRITE};
use fidel_client::FidelClient;
use violetabftstore::store::*;
use test_violetabftstore::*;
use violetabftstore::interlock::::config::*;
use violetabftstore::interlock::::HandyRwLock;

/// Test if merge is working as expected in a general condition.
#[test]
fn test_node_base_merge() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    for i in 0..3 {
        must_get_equal(&cluster.get_engine(i + 1), b"k1", b"v1");
        must_get_equal(&cluster.get_engine(i + 1), b"k3", b"v3");
    }

    let fidel_client = Arc::clone(&cluster.fidel_client);
    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();
    assert_eq!(brane.get_id(), right.get_id());
    assert_eq!(left.get_lightlike_key(), right.get_spacelike_key());
    assert_eq!(right.get_spacelike_key(), b"k2");
    let get = new_request(
        right.get_id(),
        right.get_brane_epoch().clone(),
        vec![new_get_cmd(b"k1")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(resp.get_header().has_error(), "{:?}", resp);
    assert!(
        resp.get_header().get_error().has_key_not_in_brane(),
        "{:?}",
        resp
    );

    fidel_client.must_merge(left.get_id(), right.get_id());

    let brane = fidel_client.get_brane(b"k1").unwrap();
    assert_eq!(brane.get_id(), right.get_id());
    assert_eq!(brane.get_spacelike_key(), left.get_spacelike_key());
    assert_eq!(brane.get_lightlike_key(), right.get_lightlike_key());
    let origin_epoch = left.get_brane_epoch();
    let new_epoch = brane.get_brane_epoch();
    // PrepareMerge + CommitMerge, so it should be 2.
    assert_eq!(new_epoch.get_version(), origin_epoch.get_version() + 2);
    assert_eq!(new_epoch.get_conf_ver(), origin_epoch.get_conf_ver());
    let get = new_request(
        brane.get_id(),
        new_epoch.to_owned(),
        vec![new_get_cmd(b"k1")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v1");

    let version = left.get_brane_epoch().get_version();
    let conf_ver = left.get_brane_epoch().get_conf_ver();
    'outer: for i in 1..4 {
        let state_key = tuplespaceInstanton::brane_state_key(left.get_id());
        let mut state = BraneLocalState::default();
        for _ in 0..3 {
            state = cluster
                .get_engine(i)
                .c()
                .get_msg_causet(Causet_VIOLETABFT, &state_key)
                .unwrap()
                .unwrap();
            if state.get_state() == PeerState::Tombstone {
                let epoch = state.get_brane().get_brane_epoch();
                assert_eq!(epoch.get_version(), version + 1);
                assert_eq!(epoch.get_conf_ver(), conf_ver + 1);
                continue 'outer;
            }
            thread::sleep(Duration::from_millis(500));
        }
        panic!("store {} is still not merged: {:?}", i, state);
    }

    cluster.must_put(b"k4", b"v4");
}

#[test]
fn test_node_merge_with_slow_learner() {
    let mut cluster = new_node_cluster(0, 2);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 40;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 40;
    cluster.causet.violetabft_store.merge_max_log_gap = 15;
    cluster.fidel_client.disable_default_operator();

    // Create a cluster with peer 1 as leader and peer 2 as learner.
    let r1 = cluster.run_conf_change();
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.must_add_peer(r1, new_learner_peer(2, 2));

    // Split the brane.
    let fidel_client = Arc::clone(&cluster.fidel_client);
    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();
    assert_eq!(brane.get_id(), right.get_id());
    assert_eq!(left.get_lightlike_key(), right.get_spacelike_key());
    assert_eq!(right.get_spacelike_key(), b"k2");

    // Make sure the leader has received the learner's last index.
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k3", b"v3");

    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));
    (0..20).for_each(|i| cluster.must_put(b"k1", format!("v{}", i).as_bytes()));

    // Merge 2 branes under isolation should fail.
    let merge = new_prepare_merge(right.clone());
    let req = new_admin_request(left.get_id(), left.get_brane_epoch(), merge);
    let resp = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(resp
        .get_header()
        .get_error()
        .get_message()
        .contains("log gap"));

    cluster.clear_lightlike_filters();
    cluster.must_put(b"k11", b"v100");
    must_get_equal(&cluster.get_engine(1), b"k11", b"v100");
    must_get_equal(&cluster.get_engine(2), b"k11", b"v100");

    fidel_client.must_merge(left.get_id(), right.get_id());

    // Test slow learner will be cleaned up when merge can't be continued.
    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k5");
    cluster.must_put(b"k4", b"v4");
    cluster.must_put(b"k5", b"v5");
    must_get_equal(&cluster.get_engine(2), b"k4", b"v4");
    must_get_equal(&cluster.get_engine(2), b"k5", b"v5");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k5").unwrap();
    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));
    fidel_client.must_merge(left.get_id(), right.get_id());
    let state1 = cluster.truncated_state(right.get_id(), 1);
    (0..50).for_each(|i| cluster.must_put(b"k2", format!("v{}", i).as_bytes()));

    // Wait to trigger compact violetabft log
    let timer = Instant::now();
    loop {
        let state2 = cluster.truncated_state(right.get_id(), 1);
        if state1.get_index() != state2.get_index() {
            break;
        }
        if timer.elapsed() > Duration::from_secs(3) {
            panic!("log compaction not finish after 3 seconds.");
        }
        sleep_ms(10);
    }
    cluster.clear_lightlike_filters();
    cluster.must_put(b"k6", b"v6");
    must_get_equal(&cluster.get_engine(2), b"k6", b"v6");
}

/// Test whether merge will be aborted if prerequisites is not met.
// FIXME(nrc) failing on CI only
#[causet(feature = "protobuf-codec")]
#[test]
fn test_node_merge_prerequisites_check() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();
    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_on_store1);
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();
    cluster.must_transfer_leader(right.get_id(), right_on_store1);

    // first MsgApplightlike will applightlike log, second MsgApplightlike will set commit index,
    // So only allowing first MsgApplightlike to make source peer have uncommitted entries.
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike)
            .allow(1),
    ));
    // make the source peer's commit index can't be fideliod by MsgHeartbeat.
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(left.get_id(), 3)
            .msg_type(MessageType::MsgHeartbeat)
            .direction(Direction::Recv),
    ));
    cluster.must_split(&left, b"k11");
    let res = cluster.try_merge(left.get_id(), right.get_id());
    // log gap (min_committed, last_index] contains admin entries.
    assert!(res.get_header().has_error(), "{:?}", res);
    cluster.clear_lightlike_filters();
    cluster.must_put(b"k22", b"v22");
    must_get_equal(&cluster.get_engine(3), b"k22", b"v22");

    cluster.add_lightlike_filter(CloneFilterFactory(BranePacketFilter::new(
        right.get_id(),
        3,
    )));
    // It doesn't matter if the index and term is correct.
    let compact_log = new_compact_log_request(100, 10);
    let req = new_admin_request(right.get_id(), right.get_brane_epoch(), compact_log);
    debug!("requesting {:?}", req);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(res.get_header().has_error(), "{:?}", res);
    let res = cluster.try_merge(right.get_id(), left.get_id());
    // log gap (min_matched, last_index] contains admin entries.
    assert!(res.get_header().has_error(), "{:?}", res);
    cluster.clear_lightlike_filters();
    cluster.must_put(b"k23", b"v23");
    must_get_equal(&cluster.get_engine(3), b"k23", b"v23");

    cluster.add_lightlike_filter(CloneFilterFactory(BranePacketFilter::new(
        right.get_id(),
        3,
    )));
    let mut large_bytes = vec![b'k', b'3'];
    // 3M
    large_bytes.extlightlike(repeat(b'0').take(1024 * 1024 * 3));
    cluster.must_put(&large_bytes, &large_bytes);
    cluster.must_put(&large_bytes, &large_bytes);
    // So log gap now contains 12M data, which exceeds the default max entry size.
    let res = cluster.try_merge(right.get_id(), left.get_id());
    // log gap contains admin entries.
    assert!(res.get_header().has_error(), "{:?}", res);
    cluster.clear_lightlike_filters();
    cluster.must_put(b"k24", b"v24");
    must_get_equal(&cluster.get_engine(3), b"k24", b"v24");
}

/// Test if stale peer will be handled properly after merge.
#[test]
fn test_node_check_merged_message() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    // test if stale peer before conf removal is destroyed automatically
    let mut brane = fidel_client.get_brane(b"k1").unwrap();
    fidel_client.must_add_peer(brane.get_id(), new_peer(2, 2));
    fidel_client.must_add_peer(brane.get_id(), new_peer(3, 3));

    cluster.must_split(&brane, b"k2");
    let mut left = fidel_client.get_brane(b"k1").unwrap();
    let mut right = fidel_client.get_brane(b"k2").unwrap();
    fidel_client.must_add_peer(left.get_id(), new_peer(4, 4));
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");
    cluster.add_lightlike_filter(IsolationFilterFactory::new(4));
    fidel_client.must_remove_peer(left.get_id(), new_peer(4, 4));
    fidel_client.must_merge(left.get_id(), right.get_id());
    cluster.clear_lightlike_filters();
    must_get_none(&cluster.get_engine(4), b"k1");

    // test gc work under complicated situation.
    cluster.must_put(b"k5", b"v5");
    brane = fidel_client.get_brane(b"k2").unwrap();
    cluster.must_split(&brane, b"k2");
    brane = fidel_client.get_brane(b"k4").unwrap();
    cluster.must_split(&brane, b"k4");
    left = fidel_client.get_brane(b"k1").unwrap();
    let middle = fidel_client.get_brane(b"k3").unwrap();
    let middle_on_store1 = find_peer(&middle, 1).unwrap().to_owned();
    cluster.must_transfer_leader(middle.get_id(), middle_on_store1);
    right = fidel_client.get_brane(b"k5").unwrap();
    let left_on_store3 = find_peer(&left, 3).unwrap().to_owned();
    fidel_client.must_remove_peer(left.get_id(), left_on_store3);
    must_get_none(&cluster.get_engine(3), b"k1");
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    left = fidel_client.get_brane(b"k1").unwrap();
    fidel_client.must_add_peer(left.get_id(), new_peer(3, 5));
    left = fidel_client.get_brane(b"k1").unwrap();
    fidel_client.must_merge(middle.get_id(), left.get_id());
    fidel_client.must_merge(right.get_id(), left.get_id());
    cluster.must_delete(b"k3");
    cluster.must_delete(b"k5");
    cluster.must_put(b"k4", b"v4");
    cluster.clear_lightlike_filters();
    let engine3 = cluster.get_engine(3);
    must_get_equal(&engine3, b"k1", b"v1");
    must_get_equal(&engine3, b"k4", b"v4");
    must_get_none(&engine3, b"k3");
    must_get_none(&engine3, b"v5");
}

#[test]
fn test_node_merge_slow_split_right() {
    test_node_merge_slow_split(true);
}

#[test]
fn test_node_merge_slow_split_left() {
    test_node_merge_slow_split(false);
}

// Test if a merge handled properly when there is a unfinished slow split before merge.
fn test_node_merge_slow_split(is_right_derive: bool) {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();
    cluster.causet.violetabft_store.right_derive_when_split = is_right_derive;

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
        BranePacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    ));
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(right.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    ));
    cluster.must_split(&right, b"k3");

    // left brane and right brane on store 3 fall behind
    // so after split, the new generated brane is not on store 3 now
    let right1 = fidel_client.get_brane(b"k2").unwrap();
    let right2 = fidel_client.get_brane(b"k3").unwrap();
    assert_ne!(right1.get_id(), right2.get_id());
    fidel_client.must_merge(left.get_id(), right1.get_id());
    // after merge, the left brane still exists on store 3

    cluster.must_put(b"k0", b"v0");
    cluster.clear_lightlike_filters();
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");
}

/// Test various cases that a store is isolated during merge.
#[test]
fn test_node_merge_dist_isolation() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k3").unwrap();

    cluster.must_transfer_leader(right.get_id(), new_peer(1, 1));
    let target_leader = left
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 3)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(left.get_id(), target_leader);
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");

    // So cluster becomes:
    //  left brane: 1         I 2 3(leader)
    // right brane: 1(leader) I 2 3
    // I means isolation.
    cluster.add_lightlike_filter(IsolationFilterFactory::new(1));
    fidel_client.must_merge(left.get_id(), right.get_id());
    cluster.must_put(b"k4", b"v4");
    cluster.clear_lightlike_filters();
    must_get_equal(&cluster.get_engine(1), b"k4", b"v4");

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k3").unwrap();

    cluster.must_put(b"k11", b"v11");
    fidel_client.must_remove_peer(right.get_id(), new_peer(3, 3));
    cluster.must_put(b"k33", b"v33");

    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(right.get_id(), 3).direction(Direction::Recv),
    ));
    fidel_client.must_add_peer(right.get_id(), new_peer(3, 4));
    let right = fidel_client.get_brane(b"k3").unwrap();
    // So cluster becomes:
    //  left brane: 1         2   3(leader)
    // right brane: 1(leader) 2  [3]
    // [x] means a replica exists logically but is not created on the store x yet.
    let res = cluster.try_merge(brane.get_id(), right.get_id());
    // Leader can't find replica 3 of right brane, so it fails.
    assert!(res.get_header().has_error(), "{:?}", res);

    let target_leader = left
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 2)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(left.get_id(), target_leader);
    fidel_client.must_merge(left.get_id(), right.get_id());
    cluster.must_put(b"k4", b"v4");

    cluster.clear_lightlike_filters();
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

/// Similar to `test_node_merge_dist_isolation`, but make the isolated store
/// way behind others so others have to lightlike it a snapshot.
#[test]
fn test_node_merge_brain_split() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 12;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 12;

    cluster.run();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let fidel_client = Arc::clone(&cluster.fidel_client);
    let brane = fidel_client.get_brane(b"k1").unwrap();

    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k3").unwrap();

    // The split branes' leaders could be at store 3, so transfer them to peer 1.
    let left_peer_1 = find_peer(&left, 1).cloned().unwrap();
    cluster.must_transfer_leader(left.get_id(), left_peer_1);
    let right_peer_1 = find_peer(&right, 1).cloned().unwrap();
    cluster.must_transfer_leader(right.get_id(), right_peer_1);

    cluster.must_put(b"k11", b"v11");
    cluster.must_put(b"k21", b"v21");
    // Make sure peers on store 3 have replicated latest fidelio, which means
    // they have already reported their progresses to leader.
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    must_get_equal(&cluster.get_engine(3), b"k21", b"v21");

    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    // So cluster becomes:
    //  left brane: 1(leader) 2 I 3
    // right brane: 1(leader) 2 I 3
    // I means isolation.
    fidel_client.must_merge(left.get_id(), right.get_id());

    for i in 0..100 {
        cluster.must_put(format!("k4{}", i).as_bytes(), b"v4");
    }
    must_get_equal(&cluster.get_engine(2), b"k40", b"v4");
    must_get_equal(&cluster.get_engine(1), b"k40", b"v4");

    cluster.clear_lightlike_filters();

    // Wait until store 3 get data after merging
    must_get_equal(&cluster.get_engine(3), b"k40", b"v4");
    let right_peer_3 = find_peer(&right, 3).cloned().unwrap();
    cluster.must_transfer_leader(right.get_id(), right_peer_3);
    cluster.must_put(b"k40", b"v5");

    // Make sure the two branes are already merged on store 3.
    let state_key = tuplespaceInstanton::brane_state_key(left.get_id());
    let state: BraneLocalState = cluster
        .get_engine(3)
        .c()
        .get_msg_causet(Causet_VIOLETABFT, &state_key)
        .unwrap()
        .unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);
    must_get_equal(&cluster.get_engine(3), b"k40", b"v5");
    for i in 1..100 {
        must_get_equal(&cluster.get_engine(3), format!("k4{}", i).as_bytes(), b"v4");
    }

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let brane = fidel_client.get_brane(b"k2").unwrap();
    cluster.must_split(&brane, b"k3");
    let middle = fidel_client.get_brane(b"k2").unwrap();
    let peer_on_store1 = find_peer(&middle, 1).unwrap().to_owned();
    cluster.must_transfer_leader(middle.get_id(), peer_on_store1);
    cluster.must_put(b"k22", b"v22");
    cluster.must_put(b"k33", b"v33");
    must_get_equal(&cluster.get_engine(3), b"k33", b"v33");
    let left = fidel_client.get_brane(b"k1").unwrap();
    fidel_client.disable_default_operator();
    let peer_on_left = find_peer(&left, 3).unwrap().to_owned();
    fidel_client.must_remove_peer(left.get_id(), peer_on_left);
    let right = fidel_client.get_brane(b"k3").unwrap();
    let peer_on_right = find_peer(&right, 3).unwrap().to_owned();
    fidel_client.must_remove_peer(right.get_id(), peer_on_right);
    must_get_none(&cluster.get_engine(3), b"k11");
    must_get_equal(&cluster.get_engine(3), b"k22", b"v22");
    must_get_none(&cluster.get_engine(3), b"k33");
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    fidel_client.must_add_peer(left.get_id(), new_peer(3, 11));
    fidel_client.must_merge(middle.get_id(), left.get_id());
    fidel_client.must_remove_peer(left.get_id(), new_peer(3, 11));
    fidel_client.must_merge(right.get_id(), left.get_id());
    fidel_client.must_add_peer(left.get_id(), new_peer(3, 12));
    let brane = fidel_client.get_brane(b"k1").unwrap();
    // So cluster becomes
    // store   3: k2 [middle] k3
    // store 1/2: [  new_left ] k4 [left]
    cluster.must_split(&brane, b"k4");
    cluster.must_put(b"k12", b"v12");
    cluster.clear_lightlike_filters();
    must_get_equal(&cluster.get_engine(3), b"k12", b"v12");
}

/// Test whether approximate size and tuplespaceInstanton are fideliod after merge
#[test]
fn test_merge_approximate_size_and_tuplespaceInstanton() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.causet.violetabft_store.split_brane_check_tick_interval = ReadableDuration::millis(20);
    cluster.run();

    let mut cone = 1..;
    let middle_key = put_causet_till_size(&mut cluster, Causet_WRITE, 100, &mut cone);
    let max_key = put_causet_till_size(&mut cluster, Causet_WRITE, 100, &mut cone);

    let fidel_client = Arc::clone(&cluster.fidel_client);
    let brane = fidel_client.get_brane(b"").unwrap();

    cluster.must_split(&brane, &middle_key);
    // make sure split check is invoked so size and tuplespaceInstanton are fideliod.
    thread::sleep(Duration::from_millis(100));

    let left = fidel_client.get_brane(b"").unwrap();
    let right = fidel_client.get_brane(&max_key).unwrap();
    assert_ne!(left, right);

    // make sure all peer's approximate size is not None.
    cluster.must_transfer_leader(right.get_id(), right.get_peers()[0].clone());
    thread::sleep(Duration::from_millis(100));
    cluster.must_transfer_leader(right.get_id(), right.get_peers()[1].clone());
    thread::sleep(Duration::from_millis(100));
    cluster.must_transfer_leader(right.get_id(), right.get_peers()[2].clone());
    thread::sleep(Duration::from_millis(100));

    let size = fidel_client
        .get_brane_approximate_size(right.get_id())
        .unwrap();
    assert_ne!(size, 0);
    let tuplespaceInstanton = fidel_client
        .get_brane_approximate_tuplespaceInstanton(right.get_id())
        .unwrap();
    assert_ne!(tuplespaceInstanton, 0);

    fidel_client.must_merge(left.get_id(), right.get_id());
    // make sure split check is invoked so size and tuplespaceInstanton are fideliod.
    thread::sleep(Duration::from_millis(100));

    let brane = fidel_client.get_brane(b"").unwrap();
    // size and tuplespaceInstanton should be fideliod.
    assert_ne!(
        fidel_client
            .get_brane_approximate_size(brane.get_id())
            .unwrap(),
        size
    );
    assert_ne!(
        fidel_client
            .get_brane_approximate_tuplespaceInstanton(brane.get_id())
            .unwrap(),
        tuplespaceInstanton
    );

    // after merge and then transfer leader, if not fidelio new leader's approximate size, it maybe be stale.
    cluster.must_transfer_leader(brane.get_id(), brane.get_peers()[0].clone());
    // make sure split check is invoked
    thread::sleep(Duration::from_millis(100));
    assert_ne!(
        fidel_client
            .get_brane_approximate_size(brane.get_id())
            .unwrap(),
        size
    );
    assert_ne!(
        fidel_client
            .get_brane_approximate_tuplespaceInstanton(brane.get_id())
            .unwrap(),
        tuplespaceInstanton
    );
}

#[test]
fn test_node_merge_fidelio_brane() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    // Election timeout and max leader lease is 1s.
    configure_for_lease_read(&mut cluster, Some(100), Some(10));

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let fidel_client = Arc::clone(&cluster.fidel_client);
    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    // Make sure the leader is in lease.
    cluster.must_put(b"k1", b"v2");

    // "k3" is not in the cone of left.
    let get = new_request(
        left.get_id(),
        left.get_brane_epoch().clone(),
        vec![new_get_cmd(b"k3")],
        false,
    );
    debug!("requesting key not in cone {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(resp.get_header().has_error(), "{:?}", resp);
    assert!(
        resp.get_header().get_error().has_key_not_in_brane(),
        "{:?}",
        resp
    );

    // Merge right to left.
    fidel_client.must_merge(right.get_id(), left.get_id());

    let origin_leader = cluster.leader_of_brane(left.get_id()).unwrap();
    let new_leader = left
        .get_peers()
        .iter()
        .cloned()
        .find(|p| p.get_id() != origin_leader.get_id())
        .unwrap();

    // Make sure merge is done in the new_leader.
    // There is only one brane in the cluster, "k0" must belongs to it.
    cluster.must_put(b"k0", b"v0");
    must_get_equal(&cluster.get_engine(new_leader.get_store_id()), b"k0", b"v0");

    // Transfer leadership to the new_leader.
    cluster.must_transfer_leader(left.get_id(), new_leader);

    // Make sure the leader is in lease.
    cluster.must_put(b"k0", b"v1");

    let new_brane = fidel_client.get_brane(b"k2").unwrap();
    let get = new_request(
        new_brane.get_id(),
        new_brane.get_brane_epoch().clone(),
        vec![new_get_cmd(b"k3")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Get);
    assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v3");
}

/// Test if merge is working properly when merge entries is empty but commit index is not fideliod.
#[test]
fn test_node_merge_catch_up_logs_empty_entries() {
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

    // first MsgApplightlike will applightlike log, second MsgApplightlike will set commit index,
    // So only allowing first MsgApplightlike to make source peer have uncommitted entries.
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike)
            .allow(1),
    ));
    // make the source peer have no way to know the uncommitted entries can be applied from heartbeat.
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(left.get_id(), 3)
            .msg_type(MessageType::MsgHeartbeat)
            .direction(Direction::Recv),
    ));
    // make the source peer have no way to know the uncommitted entries can be applied from target brane.
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(right.get_id(), 3)
            .msg_type(MessageType::MsgApplightlike)
            .direction(Direction::Recv),
    ));
    fidel_client.must_merge(left.get_id(), right.get_id());
    cluster.must_brane_not_exist(left.get_id(), 2);
    cluster.shutdown();
    cluster.clear_lightlike_filters();

    // as expected, merge process will forward the commit index
    // and the source peer will be destroyed.
    cluster.spacelike().unwrap();
    cluster.must_brane_not_exist(left.get_id(), 3);
}

#[test]
fn test_merge_with_slow_promote() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    fidel_client.must_add_peer(left.get_id(), new_peer(3, left.get_id() + 3));
    fidel_client.must_add_peer(right.get_id(), new_learner_peer(3, right.get_id() + 3));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");

    let delay_filter =
        Box::new(BranePacketFilter::new(right.get_id(), 3).direction(Direction::Recv));
    cluster.sim.wl().add_lightlike_filter(3, delay_filter);

    fidel_client.must_add_peer(right.get_id(), new_peer(3, right.get_id() + 3));
    fidel_client.must_merge(right.get_id(), left.get_id());
    cluster.sim.wl().clear_lightlike_filters(3);
    cluster.must_transfer_leader(left.get_id(), new_peer(3, left.get_id() + 3));
}

#[test]
fn test_request_snapshot_after_propose_merge() {
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

    // Drop applightlike messages, so prepare merge can not be committed.
    cluster.add_lightlike_filter(CloneFilterFactory(DropMessageFilter::new(
        MessageType::MsgApplightlike,
    )));
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
    // Leader should reject request snapshot if there is any proposed merge.
    rx.recv_timeout(Duration::from_millis(500)).unwrap_err();
}

/// Test whether a isolated store recover properly if there is no target peer
/// on this store before isolated.
/// A (-∞, k2), B [k2, +∞) on store 1,2,4
/// store 4 is isolated
/// B merge to A (target peer A is not created on store 4. It‘s just exist logically)
/// A split => C (-∞, k3), A [k3, +∞)
/// Then network recovery
#[test]
fn test_merge_isolated_store_with_no_target_peer() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    cluster.causet.violetabft_store.right_derive_when_split = true;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    let brane = fidel_client.get_brane(b"k1").unwrap();
    // (-∞, k2), [k2, +∞)
    cluster.must_split(&brane, b"k2");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_on_store1);
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();
    cluster.must_transfer_leader(right.get_id(), right_on_store1);

    fidel_client.must_add_peer(right.get_id(), new_peer(4, 4));
    let right_on_store3 = find_peer(&right, 3).unwrap().to_owned();
    fidel_client.must_remove_peer(right.get_id(), right_on_store3);

    cluster.must_put(b"k22", b"v22");
    must_get_equal(&cluster.get_engine(4), b"k22", b"v22");

    cluster.add_lightlike_filter(IsolationFilterFactory::new(4));

    fidel_client.must_add_peer(left.get_id(), new_peer(4, 5));
    let left_on_store3 = find_peer(&left, 3).unwrap().to_owned();
    fidel_client.must_remove_peer(left.get_id(), left_on_store3);

    fidel_client.must_merge(right.get_id(), left.get_id());

    let new_left = fidel_client.get_brane(b"k1").unwrap();
    // (-∞, k3), [k3, +∞)
    cluster.must_split(&new_left, b"k3");
    // Now new_left brane cone is [k3, +∞)
    cluster.must_put(b"k345", b"v345");
    cluster.clear_lightlike_filters();

    must_get_equal(&cluster.get_engine(4), b"k345", b"v345");
}

/// Test whether a isolated peer can recover when two other branes merge to its brane
#[test]
fn test_merge_cascade_merge_isolated() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();

    let mut brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    brane = fidel_client.get_brane(b"k2").unwrap();
    cluster.must_split(&brane, b"k3");

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_put(b"k3", b"v3");

    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");

    let r1 = fidel_client.get_brane(b"k1").unwrap();
    let r2 = fidel_client.get_brane(b"k2").unwrap();
    let r3 = fidel_client.get_brane(b"k3").unwrap();

    let r1_on_store1 = find_peer(&r1, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r1.get_id(), r1_on_store1);
    let r2_on_store2 = find_peer(&r2, 2).unwrap().to_owned();
    cluster.must_transfer_leader(r2.get_id(), r2_on_store2);
    let r3_on_store1 = find_peer(&r3, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r3.get_id(), r3_on_store1);

    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));

    // r1, r3 both merge to r2
    fidel_client.must_merge(r1.get_id(), r2.get_id());
    fidel_client.must_merge(r3.get_id(), r2.get_id());

    cluster.must_put(b"k4", b"v4");

    cluster.clear_lightlike_filters();

    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

// Test if a learner can be destroyed properly when it's isloated and removed by conf change
// before its brane merge to another brane
#[test]
fn test_merge_isloated_not_in_merge_learner() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run_conf_change();

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();
    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();

    fidel_client.must_add_peer(left.get_id(), new_learner_peer(2, 2));
    // Ensure this learner exists
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.stop_node(2);

    fidel_client.must_remove_peer(left.get_id(), new_learner_peer(2, 2));

    fidel_client.must_add_peer(left.get_id(), new_peer(3, 3));
    fidel_client.must_remove_peer(left.get_id(), left_on_store1);

    fidel_client.must_add_peer(right.get_id(), new_peer(3, 4));
    fidel_client.must_remove_peer(right.get_id(), right_on_store1);

    fidel_client.must_merge(left.get_id(), right.get_id());
    // Add a new learner on store 2 to trigger peer 2 lightlike check-stale-peer msg to other peers
    fidel_client.must_add_peer(right.get_id(), new_learner_peer(2, 5));

    cluster.must_put(b"k123", b"v123");

    cluster.run_node(2).unwrap();
    // We can see if the old peer 2 is destroyed
    must_get_equal(&cluster.get_engine(2), b"k123", b"v123");
}

// Test if a learner can be destroyed properly when it's isloated and removed by conf change
// before another brane merge to its brane
#[test]
fn test_merge_isloated_stale_learner() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.right_derive_when_split = true;
    // Do not rely on fidel to remove stale peer
    cluster.causet.violetabft_store.max_leader_missing_duration = ReadableDuration::hours(2);
    cluster.causet.violetabft_store.abnormal_leader_missing_duration = ReadableDuration::minutes(10);
    cluster.causet.violetabft_store.peer_stale_state_check_interval = ReadableDuration::minutes(5);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run_conf_change();

    let mut brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();

    fidel_client.must_add_peer(left.get_id(), new_learner_peer(2, 2));
    // Ensure this learner exists
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.stop_node(2);

    fidel_client.must_remove_peer(left.get_id(), new_learner_peer(2, 2));

    fidel_client.must_merge(right.get_id(), left.get_id());

    brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    let new_left = fidel_client.get_brane(b"k1").unwrap();
    assert_ne!(left.get_id(), new_left.get_id());
    // Add a new learner on store 2 to trigger peer 2 lightlike check-stale-peer msg to other peers
    fidel_client.must_add_peer(new_left.get_id(), new_learner_peer(2, 5));
    cluster.must_put(b"k123", b"v123");

    cluster.run_node(2).unwrap();
    // We can see if the old peer 2 is destroyed
    must_get_equal(&cluster.get_engine(2), b"k123", b"v123");
}

/// Test if a learner can be destroyed properly in such conditions as follows
/// 1. A peer is isolated
/// 2. Be the last removed peer in its peer list
/// 3. Then its brane merges to another brane.
/// 4. Isolation disappears
#[test]
fn test_merge_isloated_not_in_merge_learner_2() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run_conf_change();

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let right = fidel_client.get_brane(b"k2").unwrap();
    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();

    fidel_client.must_add_peer(left.get_id(), new_learner_peer(2, 2));
    // Ensure this learner exists
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.stop_node(2);

    fidel_client.must_add_peer(left.get_id(), new_peer(3, 3));
    fidel_client.must_remove_peer(left.get_id(), left_on_store1);

    fidel_client.must_add_peer(right.get_id(), new_peer(3, 4));
    fidel_client.must_remove_peer(right.get_id(), right_on_store1);
    // The peer list of peer 2 is (1001, 1), (2, 2)
    fidel_client.must_remove_peer(left.get_id(), new_learner_peer(2, 2));

    fidel_client.must_merge(left.get_id(), right.get_id());

    cluster.run_node(2).unwrap();
    // When the abnormal leader missing duration has passed, the check-stale-peer msg will be sent to peer 1001.
    // After that, a new peer list will be returned (2, 2) (3, 3).
    // Then peer 2 lightlikes the check-stale-peer msg to peer 3 and it will get a tombstone response.
    // Finally peer 2 will be destroyed.
    must_get_none(&cluster.get_engine(2), b"k1");
}

/// Test if a peer can be removed if its target peer has been removed and doesn't apply the
/// CommitMerge log.
#[test]
fn test_merge_remove_target_peer_isolated() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_merge(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run_conf_change();

    let mut brane = fidel_client.get_brane(b"k1").unwrap();
    fidel_client.must_add_peer(brane.get_id(), new_peer(2, 2));
    fidel_client.must_add_peer(brane.get_id(), new_peer(3, 3));

    cluster.must_split(&brane, b"k2");
    brane = fidel_client.get_brane(b"k2").unwrap();
    cluster.must_split(&brane, b"k3");

    let r1 = fidel_client.get_brane(b"k1").unwrap();
    let r2 = fidel_client.get_brane(b"k2").unwrap();
    let r3 = fidel_client.get_brane(b"k3").unwrap();

    let r1_on_store1 = find_peer(&r1, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r1.get_id(), r1_on_store1);
    let r2_on_store2 = find_peer(&r2, 2).unwrap().to_owned();
    cluster.must_transfer_leader(r2.get_id(), r2_on_store2);

    for i in 1..4 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    for i in 1..4 {
        must_get_equal(&cluster.get_engine(3), format!("k{}", i).as_bytes(), b"v1");
    }

    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    // Make brane r2's epoch > r2 peer on store 3.
    // r2 peer on store 3 will be removed whose epoch is staler than the epoch when r1 merge to r2.
    fidel_client.must_add_peer(r2.get_id(), new_peer(4, 4));
    fidel_client.must_remove_peer(r2.get_id(), new_peer(4, 4));

    let r2_on_store3 = find_peer(&r2, 3).unwrap().to_owned();
    let r3_on_store3 = find_peer(&r3, 3).unwrap().to_owned();

    fidel_client.must_merge(r1.get_id(), r2.get_id());

    fidel_client.must_remove_peer(r2.get_id(), r2_on_store3);
    fidel_client.must_remove_peer(r3.get_id(), r3_on_store3);

    fidel_client.must_merge(r2.get_id(), r3.get_id());

    cluster.clear_lightlike_filters();

    for i in 1..4 {
        must_get_none(&cluster.get_engine(3), format!("k{}", i).as_bytes());
    }
}

#[test]
fn test_sync_max_ts_after_brane_merge() {
    use edb::causet_storage::{Engine, Snapshot};

    let mut cluster = new_server_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.run();

    // Transfer leader to node 1 first to ensure all operations happen on node 1
    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let brane = cluster.get_brane(b"k1");
    cluster.must_split(&brane, b"k2");
    let left = cluster.get_brane(b"k1");
    let right = cluster.get_brane(b"k3");

    let cm = cluster.sim.read().unwrap().get_interlocking_directorate(1);
    let causet_storage = cluster
        .sim
        .read()
        .unwrap()
        .causet_storages
        .get(&1)
        .unwrap()
        .clone();
    let wait_for_synced = |cluster: &mut Cluster<ServerCluster>| {
        let brane_id = right.get_id();
        let leader = cluster.leader_of_brane(brane_id).unwrap();
        let epoch = cluster.get_brane_epoch(brane_id);
        let mut ctx = Context::default();
        ctx.set_brane_id(brane_id);
        ctx.set_peer(leader.clone());
        ctx.set_brane_epoch(epoch);

        let snapshot = causet_storage.snapshot(&ctx).unwrap();
        let max_ts_sync_status = snapshot.max_ts_sync_status.clone().unwrap();
        for retry in 0..10 {
            if max_ts_sync_status.load(Ordering::SeqCst) & 1 == 1 {
                break;
            }
            thread::sleep(Duration::from_millis(1 << retry));
        }
        assert!(snapshot.is_max_ts_synced());
    };

    wait_for_synced(&mut cluster);
    let max_ts = cm.max_ts();

    cluster.fidel_client.trigger_tso_failure();
    // Merge left to right
    cluster.fidel_client.must_merge(left.get_id(), right.get_id());

    wait_for_synced(&mut cluster);
    let new_max_ts = cm.max_ts();
    assert!(new_max_ts > max_ts);
}
