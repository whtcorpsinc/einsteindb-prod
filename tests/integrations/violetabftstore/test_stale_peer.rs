// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

//! A module contains test cases of stale peers gc.

use std::sync::Arc;
use std::thread;
use std::time::*;

use ekvproto::violetabft_server_timeshare::{PeerState, BraneLocalState};
use violetabft::evioletabft_timeshare::MessageType;

use engine_lmdb::Compat;
use edb::Peekable;
use edb::Causet_VIOLETABFT;
use test_violetabftstore::*;

/// A helper function for testing the behaviour of the gc of stale peer
/// which is out of brane.
/// If a peer detects the leader is missing for a specified long time,
/// it should consider itself as a stale peer which is removed from the brane.
/// This test case covers the following scenario:
/// At first, there are three peer A, B, C in the cluster, and A is leader.
/// Peer B gets down. And then A adds D, E, F into the cluster.
/// Peer D becomes leader of the new cluster, and then removes peer A, B, C.
/// After all these peer in and out, now the cluster has peer D, E, F.
/// If peer B goes up at this moment, it still thinks it is one of the cluster
/// and has peers A, C. However, it could not reach A, C since they are removed from
/// the cluster or probably destroyed.
/// Meantime, D, E, F would not reach B, Since it's not in the cluster anymore.
/// In this case, Peer B would notice that the leader is missing for a long time,
/// and it would check with fidel to confirm whether it's still a member of the cluster.
/// If not, it should destroy itself as a stale peer which is removed out already.
fn test_stale_peer_out_of_brane<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    fidel_client.must_add_peer(r1, new_peer(3, 3));
    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, key, value);

    // Isolate peer 2 from rest of the cluster.
    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));

    // In case 2 is leader, it will fail to pass the healthy nodes check,
    // so remove isolated node first. Because 2 is isolated, so it can't remove itself.
    fidel_client.must_remove_peer(r1, new_peer(2, 2));

    // Add peer [(4, 4), (5, 5), (6, 6)].
    fidel_client.must_add_peer(r1, new_peer(4, 4));
    fidel_client.must_add_peer(r1, new_peer(5, 5));
    fidel_client.must_add_peer(r1, new_peer(6, 6));

    // Remove peer [(1, 1), (3, 3)].
    fidel_client.must_remove_peer(r1, new_peer(1, 1));
    fidel_client.must_remove_peer(r1, new_peer(3, 3));

    // Keep peer 2 isolated. Otherwise whether peer 3 is destroyed or not,
    // it will handle the stale violetabft message from peer 2 and cause peer 2 to
    // destroy itself earlier than this test case expects.

    // Wait for max_leader_missing_duration to time out.
    cluster.must_remove_brane(2, r1);

    // Check whether this brane is still functional properly.
    let (key2, value2) = (b"k2", b"v2");
    cluster.must_put(key2, value2);
    assert_eq!(cluster.get(key2), Some(value2.to_vec()));

    // Check whether peer(2, 2) and its data are destroyed.
    must_get_none(&engine_2, key);
    must_get_none(&engine_2, key2);
    let state_key = tuplespaceInstanton::brane_state_key(1);
    let state: BraneLocalState = engine_2
        .c()
        .get_msg_causet(Causet_VIOLETABFT, &state_key)
        .unwrap()
        .unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);
}

#[test]
fn test_node_stale_peer_out_of_brane() {
    let count = 6;
    let mut cluster = new_node_cluster(0, count);
    test_stale_peer_out_of_brane(&mut cluster);
}

#[test]
fn test_server_stale_peer_out_of_brane() {
    let count = 6;
    let mut cluster = new_server_cluster(0, count);
    test_stale_peer_out_of_brane(&mut cluster);
}

/// A help function for testing the behaviour of the gc of stale peer
/// which is out or brane.
/// If a peer detects the leader is missing for a specified long time,
/// it should consider itself as a stale peer which is removed from the brane.
/// This test case covers the following scenario:
/// A peer, B is initialized as a replicated peer without data after
/// receiving a single violetabft AE message. But then it goes through some process like
/// the case of `test_stale_peer_out_of_brane`, it's removed out of the brane
/// and wouldn't be contacted anymore.
/// In both cases, peer B would notice that the leader is missing for a long time,
/// and it's an initialized peer without any data. It would destroy itself as
/// as stale peer directly and should not impact other brane data on the same store.
fn test_stale_peer_without_data<T: Simulator>(cluster: &mut Cluster<T>, right_derive: bool) {
    cluster.causet.violetabft_store.right_derive_when_split = right_derive;

    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    let brane = cluster.get_brane(b"");
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    cluster.must_split(&brane, b"k2");
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    let engine3 = cluster.get_engine(3);
    if right_derive {
        must_get_none(&engine3, b"k1");
        must_get_equal(&engine3, b"k3", b"v3");
    } else {
        must_get_equal(&engine3, b"k1", b"v1");
        must_get_none(&engine3, b"k3");
    }

    let new_brane = if right_derive {
        cluster.get_brane(b"k1")
    } else {
        cluster.get_brane(b"k3")
    };
    let new_brane_id = new_brane.get_id();
    // Block peer (3, 4) at receiving snapshot, but not the heartbeat
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(new_brane_id, 3).msg_type(MessageType::MsgSnapshot),
    ));

    fidel_client.must_add_peer(new_brane_id, new_peer(3, 4));

    // Wait for the heartbeat broadcasted from peer (1, 1000) to peer (3, 4).
    cluster.must_brane_exist(new_brane_id, 3);

    // And then isolate peer (3, 4) from peer (1, 1000).
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));

    fidel_client.must_remove_peer(new_brane_id, new_peer(3, 4));

    cluster.must_remove_brane(3, new_brane_id);

    // There must be no data on store 3 belongs to new brane
    if right_derive {
        must_get_none(&engine3, b"k1");
    } else {
        must_get_none(&engine3, b"k3");
    }

    // Check whether peer(3, 4) is destroyed.
    // Before peer 4 is destroyed, a tombstone mark will be written into the engine.
    // So we could check the tombstone mark to make sure peer 4 is destroyed.
    let state_key = tuplespaceInstanton::brane_state_key(new_brane_id);
    let state: BraneLocalState = engine3
        .c()
        .get_msg_causet(Causet_VIOLETABFT, &state_key)
        .unwrap()
        .unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);

    // other brane should not be affected.
    if right_derive {
        must_get_equal(&engine3, b"k3", b"v3");
    } else {
        must_get_equal(&engine3, b"k1", b"v1");
    }
}

#[test]
fn test_node_stale_peer_without_data_left_derive_when_split() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_stale_peer_without_data(&mut cluster, false);
}

#[test]
fn test_node_stale_peer_without_data_right_derive_when_split() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_stale_peer_without_data(&mut cluster, true);
}

#[test]
fn test_server_stale_peer_without_data_left_derive_when_split() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_stale_peer_without_data(&mut cluster, false);
}

#[test]
fn test_server_stale_peer_without_data_right_derive_when_split() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_stale_peer_without_data(&mut cluster, true);
}

/// A help function for testing the behaviour of the gc of stale learner
/// which is out or brane.
#[test]
fn test_stale_learner() {
    let mut cluster = new_server_cluster(0, 4);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    fidel_client.must_add_peer(r1, new_learner_peer(3, 3));
    cluster.must_put(b"k1", b"v1");
    let engine3 = cluster.get_engine(3);
    must_get_equal(&engine3, b"k1", b"v1");

    // And then isolate peer on store 3 from leader.
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));

    // Add a new peer to increase the conf version.
    fidel_client.must_add_peer(r1, new_peer(4, 4));

    // It should not be deleted.
    thread::sleep(Duration::from_secs(3));
    must_get_equal(&engine3, b"k1", b"v1");

    // Promote the learner
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    // It should not be deleted.
    thread::sleep(Duration::from_secs(3));
    must_get_equal(&engine3, b"k1", b"v1");

    // Delete the learner
    fidel_client.must_remove_peer(r1, new_peer(3, 3));

    // Check not leader should fail, all data should be removed.
    must_get_none(&engine3, b"k1");
    let state_key = tuplespaceInstanton::brane_state_key(r1);
    let state: BraneLocalState = engine3
        .c()
        .get_msg_causet(Causet_VIOLETABFT, &state_key)
        .unwrap()
        .unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);
}
