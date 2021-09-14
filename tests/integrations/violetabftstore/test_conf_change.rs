// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::*;

use futures::executor::block_on;

use ekvproto::meta_timeshare;
use ekvproto::violetabft_cmd_timeshare::{VioletaBftCmdResponse, VioletaBftResponseHeader};
use ekvproto::violetabft_server_timeshare::*;
use violetabft::evioletabft_timeshare::{ConfChangeType, MessageType};

use engine_lmdb::Compat;
use edb::{Peekable, Causet_VIOLETABFT};
use fidel_client::FidelClient;
use violetabftstore::store::util::is_learner;
use violetabftstore::Result;
use test_violetabftstore::*;
use violetabftstore::interlock::::config::ReadableDuration;
use violetabftstore::interlock::::HandyRwLock;

fn test_simple_conf_change<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // Now brane 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, b"k1");
    // add peer (2,2) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // now peer 2 must have v1 and v2;
    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k2", b"v2");

    let epoch = cluster.fidel_client.get_brane_epoch(r1);

    // Conf version must change.
    assert!(epoch.get_conf_ver() > 1);

    // peer 5 must not exist
    let engine_5 = cluster.get_engine(5);
    must_get_none(&engine_5, b"k1");

    // add peer (3, 3) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    // Remove peer (2, 2) from brane 1.
    fidel_client.must_remove_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 3 must have v1, v2 and v3
    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");
    must_get_equal(&engine_3, b"k2", b"v2");
    must_get_equal(&engine_3, b"k3", b"v3");

    // peer 2 has nothing
    must_get_none(&engine_2, b"k1");
    must_get_none(&engine_2, b"k2");

    // peer 3 must exist
    must_get_equal(&engine_3, b"k3", b"v3");

    // add peer 2 then remove it again.
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    // Force fidelio a2 to check whether peer 2 added ok and received the snapshot.
    let (key, value) = (b"k2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);

    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k2", b"v2");
    must_get_equal(&engine_2, b"k3", b"v3");

    // Make sure peer 2 is not in probe mode.
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.get(b"k4"), Some(b"v4".to_vec()));
    must_get_equal(&engine_2, b"k4", b"v4");

    let resp = call_conf_change(cluster, r1, ConfChangeType::AddNode, new_peer(2, 2)).unwrap();
    let exec_res = resp
        .get_header()
        .get_error()
        .get_message()
        .contains("duplicated");
    assert!(
        exec_res,
        "add duplicated peer should failed, but got {:?}",
        resp
    );

    // Remove peer (2, 2) from brane 1.
    fidel_client.must_remove_peer(r1, new_peer(2, 2));

    // add peer (2, 4) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(2, 4));
    cluster.must_put(b"add_2_4", b"add_2_4");
    must_get_equal(&engine_2, b"add_2_4", b"add_2_4");

    // Remove peer (3, 3) from brane 1.
    fidel_client.must_remove_peer(r1, new_peer(3, 3));

    let (key, value) = (b"k4", b"v4");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 4 in store 2 must have v1, v2, v3, v4, we check v1 and v4 here.
    let engine_2 = cluster.get_engine(2);

    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k4", b"v4");

    // peer 3 has nothing, we check v1 and v4 here.
    must_get_none(&engine_3, b"k1");
    must_get_none(&engine_3, b"k4");

    // TODO: add more tests.
}

fn new_conf_change_peer(store: &meta_timeshare::CausetStore, fidel_client: &Arc<TestFidelClient>) -> meta_timeshare::Peer {
    let peer_id = fidel_client.alloc_id().unwrap();
    new_peer(store.get_id(), peer_id)
}

fn test_fidel_conf_change<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    cluster.spacelike().unwrap();

    let brane = &fidel_client.get_brane(b"").unwrap();
    let brane_id = brane.get_id();

    let mut stores = fidel_client.get_stores().unwrap();

    // Must have only one peer
    assert_eq!(brane.get_peers().len(), 1);

    let peer = &brane.get_peers()[0];

    let i = stores
        .iter()
        .position(|store| store.get_id() == peer.get_store_id())
        .unwrap();
    stores.swap(0, i);

    // Now the first store has first brane. others have none.

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let peer2 = new_conf_change_peer(&stores[1], &fidel_client);
    let engine_2 = cluster.get_engine(peer2.get_store_id());
    assert!(engine_2
        .c()
        .get_value(&tuplespaceInstanton::data_key(b"k1"))
        .unwrap()
        .is_none());
    // add new peer to first brane.
    fidel_client.must_add_peer(brane_id, peer2.clone());

    let (key, value) = (b"k2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // now peer 2 must have v1 and v2;
    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k2", b"v2");

    // add new peer to first brane.
    let peer3 = new_conf_change_peer(&stores[2], &fidel_client);
    let engine_3 = cluster.get_engine(peer3.get_store_id());
    fidel_client.must_add_peer(brane_id, peer3.clone());
    must_get_equal(&engine_3, b"k1", b"v1");

    // Remove peer2 from first brane.
    fidel_client.must_remove_peer(brane_id, peer2);

    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 3 must have v1, v2 and v3
    must_get_equal(&engine_3, b"k1", b"v1");
    must_get_equal(&engine_3, b"k2", b"v2");
    must_get_equal(&engine_3, b"k3", b"v3");

    // peer 2 has nothing
    must_get_none(&engine_2, b"k1");
    must_get_none(&engine_2, b"k2");
    // add peer4 to first brane 1.
    let peer4 = new_conf_change_peer(&stores[1], &fidel_client);
    fidel_client.must_add_peer(brane_id, peer4.clone());
    // Remove peer3 from first brane.
    fidel_client.must_remove_peer(brane_id, peer3);

    let (key, value) = (b"k4", b"v4");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer4 must have v1, v2, v3, v4, we check v1 and v4 here.
    let engine_2 = cluster.get_engine(peer4.get_store_id());

    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k4", b"v4");

    // peer 3 has nothing, we check v1 and v4 here.
    must_get_none(&engine_3, b"k1");
    must_get_none(&engine_3, b"k4");

    // TODO: add more tests.
}

#[test]
fn test_server_simple_conf_change() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_simple_conf_change(&mut cluster);
}

#[test]
fn test_node_fidel_conf_change() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_fidel_conf_change(&mut cluster);
}

#[test]
fn test_server_fidel_conf_change() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_fidel_conf_change(&mut cluster);
}

fn wait_till_reach_count(fidel_client: Arc<TestFidelClient>, brane_id: u64, c: usize) {
    let mut replica_count = 0;
    for _ in 0..1000 {
        let brane = match block_on(fidel_client.get_brane_by_id(brane_id)).unwrap() {
            Some(r) => r,
            None => continue,
        };
        replica_count = brane.get_peers().len();
        if replica_count == c {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!(
        "replica count {} still not meet {} after 10 secs",
        replica_count, c
    );
}

fn test_auto_adjust_replica<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.spacelike().unwrap();

    let fidel_client = Arc::clone(&cluster.fidel_client);
    let mut brane = fidel_client.get_brane(b"").unwrap();
    let brane_id = brane.get_id();

    let stores = fidel_client.get_stores().unwrap();

    // default replica is 5.
    wait_till_reach_count(Arc::clone(&fidel_client), brane_id, 5);

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    brane = block_on(fidel_client.get_brane_by_id(brane_id))
        .unwrap()
        .unwrap();
    let i = stores
        .iter()
        .position(|s| {
            brane
                .get_peers()
                .iter()
                .all(|p| s.get_id() != p.get_store_id())
        })
        .unwrap();

    for peer in brane.get_peers() {
        must_get_equal(&cluster.get_engine(peer.get_store_id()), b"k1", b"v1");
    }

    let mut peer = new_conf_change_peer(&stores[i], &fidel_client);
    peer.set_role(meta_timeshare::PeerRole::Learner);
    let engine = cluster.get_engine(peer.get_store_id());
    must_get_none(&engine, b"k1");

    fidel_client.must_add_peer(brane_id, peer.clone());
    wait_till_reach_count(Arc::clone(&fidel_client), brane_id, 6);
    must_get_equal(&engine, b"k1", b"v1");
    peer.set_role(meta_timeshare::PeerRole::Voter);
    fidel_client.must_add_peer(brane_id, peer);

    // it should remove extra replica.
    fidel_client.enable_default_operator();
    wait_till_reach_count(Arc::clone(&fidel_client), brane_id, 5);

    brane = block_on(fidel_client.get_brane_by_id(brane_id))
        .unwrap()
        .unwrap();
    let peer = brane.get_peers().get(1).unwrap().clone();
    fidel_client.must_remove_peer(brane_id, peer);
    wait_till_reach_count(Arc::clone(&fidel_client), brane_id, 4);

    // it should add missing replica.
    fidel_client.enable_default_operator();
    wait_till_reach_count(Arc::clone(&fidel_client), brane_id, 5);
}

#[test]
fn test_node_auto_adjust_replica() {
    let count = 7;
    let mut cluster = new_node_cluster(0, count);
    test_auto_adjust_replica(&mut cluster);
}

#[test]
fn test_server_auto_adjust_replica() {
    let count = 7;
    let mut cluster = new_server_cluster(0, count);
    test_auto_adjust_replica(&mut cluster);
}

fn test_after_remove_itself<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    // disable auto compact log.
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 10000;

    let r1 = cluster.run_conf_change();
    cluster.must_put(b"kk", b"vv");
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"kk", b"vv");
    fidel_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"kk", b"vv");

    // 1, stop node 2
    // 2, add data to guarantee leader has more logs
    // 3, stop node 3
    // 4, remove leader itself and force compact log
    // 5, spacelike node 2 again, so that we can commit log and apply.
    // For this scenario, peer 1 will do remove itself and then compact log
    // in the same ready result loop.
    cluster.stop_node(2);

    cluster.must_put(b"k1", b"v1");

    let engine1 = cluster.get_engine(1);
    let engine3 = cluster.get_engine(3);
    must_get_equal(&engine1, b"k1", b"v1");
    must_get_equal(&engine3, b"k1", b"v1");

    cluster.stop_node(3);

    fidel_client.remove_peer(1, new_peer(1, 1));

    let epoch = block_on(cluster.fidel_client.get_brane_by_id(1))
        .unwrap()
        .unwrap()
        .take_brane_epoch();

    let put = new_put_cmd(b"test_key", b"test_val");
    let mut req = new_request(1, epoch, vec![put], true);
    req.mut_header().set_peer(new_peer(1, 1));
    // ignore error, we just want to lightlike this command to peer (1, 1),

    // and the command can't be executed because we have only one peer,
    // so here will return timeout error, we should ignore it.
    let _ = cluster.call_command(req, Duration::from_millis(1));

    cluster.run_node(2).unwrap();
    cluster.run_node(3).unwrap();

    for _ in 0..250 {
        let brane: BraneLocalState = engine1
            .c()
            .get_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(r1))
            .unwrap()
            .unwrap();
        if brane.get_state() == PeerState::Tombstone {
            return;
        }
        sleep_ms(20);
    }
    let brane: BraneLocalState = engine1
        .c()
        .get_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(r1))
        .unwrap()
        .unwrap();
    assert_eq!(brane.get_state(), PeerState::Tombstone);

    // TODO: add split after removing itself test later.
}

#[test]
fn test_node_after_remove_itself() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_after_remove_itself(&mut cluster);
}

#[test]
fn test_server_after_remove_itself() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_after_remove_itself(&mut cluster);
}

fn test_split_brain<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    cluster.must_put(b"k0", b"v0");
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k0", b"v0");
    fidel_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    cluster.must_transfer_leader(r1, new_peer(2, 2));
    cluster.must_put(b"kk0", b"vv0");
    must_get_equal(&cluster.get_engine(1), b"kk0", b"vv0");
    must_get_equal(&cluster.get_engine(2), b"kk0", b"vv0");
    must_get_equal(&cluster.get_engine(3), b"kk0", b"vv0");

    // leader isolation
    cluster.add_lightlike_filter(IsolationFilterFactory::new(1));

    // refresh brane info, maybe no need
    cluster.must_put(b"k1", b"v1");

    // add [4,5,6] and remove [2,3]
    fidel_client.must_add_peer(r1, new_peer(4, 4));
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");
    fidel_client.must_add_peer(r1, new_peer(5, 5));
    must_get_equal(&cluster.get_engine(5), b"k1", b"v1");
    fidel_client.must_add_peer(r1, new_peer(6, 6));
    must_get_equal(&cluster.get_engine(6), b"k1", b"v1");
    cluster.must_transfer_leader(r1, new_peer(6, 6));
    fidel_client.must_remove_peer(r1, new_peer(2, 2));
    fidel_client.must_remove_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(6), b"k2", b"v2");
    let brane_detail = cluster.brane_detail(r1, 1);
    let brane_peers = brane_detail.get_brane().get_peers();
    assert_eq!(brane_peers.len(), 3);
    for peer in brane_peers {
        assert!(peer.get_id() < 4);
    }
    assert!(brane_detail.get_leader().get_id() < 4);

    // when network recovers, 1 will lightlike request vote to [2,3]
    cluster.clear_lightlike_filters();
    cluster.partition(vec![1, 2, 3], vec![4, 5, 6]);

    // refresh brane info, maybe no need
    cluster.must_put(b"k3", b"v3");

    // check whether a new cluster [1,2,3] is formed
    // if so, both [1,2,3] and [4,5,6] think they serve for brane r1
    // result in split brain
    let header0 = find_leader_response_header(cluster, r1, new_peer(2, 2));
    assert!(header0.get_error().has_brane_not_found());

    // at least wait for a round of election timeout and check again
    let base_tick = cluster.causet.violetabft_store.violetabft_base_tick_interval.0;
    let election_timeout = base_tick * cluster.causet.violetabft_store.violetabft_election_timeout_ticks as u32;
    thread::sleep(election_timeout * 2);

    let header1 = find_leader_response_header(cluster, r1, new_peer(2, 2));
    assert!(header1.get_error().has_brane_not_found());
}

fn find_leader_response_header<T: Simulator>(
    cluster: &mut Cluster<T>,
    brane_id: u64,
    peer: meta_timeshare::Peer,
) -> VioletaBftResponseHeader {
    let find_leader = new_status_request(brane_id, peer, new_brane_leader_cmd());
    let resp = cluster.call_command(find_leader, Duration::from_secs(5));
    resp.unwrap().take_header()
}

#[test]
fn test_server_split_brain() {
    let count = 6;
    let mut cluster = new_server_cluster(0, count);
    test_split_brain(&mut cluster);
}

#[test]
fn test_node_split_brain() {
    let count = 6;
    let mut cluster = new_node_cluster(0, count);
    test_split_brain(&mut cluster);
}

/// A helper function for testing the conf change is safe.
fn test_conf_change_safe<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let brane_id = cluster.run_conf_change();

    // Test adding nodes.

    // Ensure it works to add one node to a cluster that has only one node.
    cluster.must_put(b"k0", b"v0");
    fidel_client.must_add_peer(brane_id, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k0", b"v0");
    fidel_client.must_add_peer(brane_id, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    // Isolate the leader.
    cluster.must_transfer_leader(brane_id, new_peer(1, 1));
    cluster.add_lightlike_filter(IsolationFilterFactory::new(1));

    // Ensure new leader is elected and it works.
    cluster.must_put(b"k1", b"v1");

    // Ensure the conf change is safe:
    // The "AddNode" request will be rejected
    // if there are only 2 healthy nodes in a cluster of 3 nodes.
    fidel_client.add_peer(brane_id, new_peer(4, 4));
    // Put a new kv to ensure the previous "AddNode" is handled.
    cluster.must_put(b"k2", b"v2");
    fidel_client.must_none_peer(brane_id, new_peer(4, 4));

    // Recover the isolated peer.
    cluster.clear_lightlike_filters();

    // Then new node could be added.
    fidel_client.must_add_peer(brane_id, new_peer(4, 4));

    // Test removing nodes.

    // Ensure nodes could be removed.
    fidel_client.must_remove_peer(brane_id, new_peer(4, 4));

    // Isolate the leader.
    cluster.must_transfer_leader(brane_id, new_peer(1, 1));
    cluster.add_lightlike_filter(IsolationFilterFactory::new(1));

    // Ensure new leader is elected and it works.
    cluster.must_put(b"k3", b"v3");

    // Ensure the conf change is safe:
    // The "RemoveNode" request which asks to remove one healthy node will be rejected
    // if there are only 2 healthy nodes in a cluster of 3 nodes.
    fidel_client.remove_peer(brane_id, new_peer(2, 2));
    cluster.must_put(b"k4", b"v4");
    fidel_client.must_have_peer(brane_id, new_peer(2, 2));

    // In this case, it's fine to remove one unhealthy node.
    fidel_client.must_remove_peer(brane_id, new_peer(1, 1));

    // Ensure it works to remove one node from the cluster that has only two healthy nodes.
    fidel_client.must_remove_peer(brane_id, new_peer(2, 2));
}

fn test_transfer_leader_safe<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let brane_id = cluster.run_conf_change();
    let causet = cluster.causet.clone();
    cluster.must_put(b"k1", b"v1");

    // Test adding nodes.
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    fidel_client.must_add_peer(brane_id, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    fidel_client.must_add_peer(brane_id, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    // transfer to all followers
    let mut leader_id = cluster.leader_of_brane(brane_id).unwrap().get_id();
    for peer in cluster.get_brane(b"").get_peers() {
        if peer.get_id() == leader_id {
            continue;
        }
        cluster.transfer_leader(brane_id, peer.clone());
        cluster.reset_leader_of_brane(brane_id);
        assert_ne!(
            cluster.leader_of_brane(brane_id).unwrap().get_id(),
            peer.get_id()
        );
    }

    // Test transfer leader after a safe duration.
    thread::sleep(causet.violetabft_store.violetabft_reject_transfer_leader_duration.into());
    assert_eq!(
        cluster.leader_of_brane(brane_id).unwrap().get_id(),
        leader_id
    );
    cluster.transfer_leader(brane_id, new_peer(3, 3));
    // Retry for more stability
    for _ in 0..20 {
        cluster.reset_leader_of_brane(brane_id);
        if cluster.leader_of_brane(brane_id) != Some(new_peer(3, 3)) {
            continue;
        }
        break;
    }
    assert_eq!(cluster.leader_of_brane(brane_id).unwrap().get_id(), 3);
    leader_id = 3;

    // Cannot transfer when removed peer
    fidel_client.must_remove_peer(brane_id, new_peer(2, 2));
    for peer in cluster.get_brane(b"").get_peers() {
        if peer.get_id() == leader_id {
            continue;
        }
        cluster.transfer_leader(brane_id, peer.clone());
        cluster.reset_leader_of_brane(brane_id);
        assert_ne!(
            cluster.leader_of_brane(brane_id).unwrap().get_id(),
            peer.get_id()
        );
    }
}

fn test_learner_conf_change<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    assert_eq!(cluster.get(b"k1"), Some(b"v1".to_vec()));

    // Add voter (2, 2) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    // Add learner (4, 10) to brane 1.
    let engine_4 = cluster.get_engine(4);
    fidel_client.must_add_peer(r1, new_learner_peer(4, 10));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&engine_4, b"k1", b"v1");
    must_get_equal(&engine_4, b"k2", b"v2");

    // Can't add duplicate learner.
    let resp = call_conf_change(
        cluster,
        r1,
        ConfChangeType::AddLearnerNode,
        new_learner_peer(4, 11),
    )
    .unwrap();
    let err_msg = resp.get_header().get_error().get_message();
    assert!(err_msg.contains("duplicated"), "{:?}", resp);

    // Remove learner (4, 10) from brane 1.
    fidel_client.must_remove_peer(r1, new_learner_peer(4, 10));
    must_get_none(&engine_4, b"k2"); // Wait for the brane is cleaned.
    fidel_client.must_add_peer(r1, new_learner_peer(4, 12));
    must_get_equal(&engine_4, b"k2", b"v2");

    // Can't transfer leader to learner.
    fidel_client.transfer_leader(r1, new_learner_peer(4, 12));
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(4), b"k3", b"v3");
    fidel_client.brane_leader_must_be(r1, new_peer(1, 1));

    // Promote learner (4, 12) to voter.
    fidel_client.must_add_peer(r1, new_peer(4, 12));
    fidel_client.must_none_plightlikeing_peer(new_peer(4, 12));
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&engine_4, b"k3", b"v3");

    // Transfer leader to (4, 12) and check fidel heartbeats from it to ensure
    // that `Peer::peer` has be fideliod correctly after the peer is promoted.
    fidel_client.transfer_leader(r1, new_peer(4, 12));
    fidel_client.brane_leader_must_be(r1, new_peer(4, 12));

    // Transfer leader to (1, 1) to avoid "brane not found".
    fidel_client.transfer_leader(r1, new_peer(1, 1));
    fidel_client.brane_leader_must_be(r1, new_peer(1, 1));
    // To avoid using stale leader.
    cluster.reset_leader_of_brane(r1);
    // Put a new kv to ensure leader has applied to newest log, so that to avoid
    // false warning about plightlikeing conf change.
    cluster.must_put(b"k4", b"v4");

    let mut add_peer = |peer: meta_timeshare::Peer| {
        let conf_type = if is_learner(&peer) {
            ConfChangeType::AddLearnerNode
        } else {
            ConfChangeType::AddNode
        };
        call_conf_change(cluster, r1, conf_type, peer).unwrap()
    };

    // Add learner on store which already has peer.
    let resp = add_peer(new_learner_peer(4, 13));
    let err_msg = resp.get_header().get_error().get_message();
    assert!(err_msg.contains("duplicated"), "{:?}", err_msg);
    fidel_client.must_have_peer(r1, new_peer(4, 12));

    // Add peer with different id on store which already has learner.
    fidel_client.must_remove_peer(r1, new_peer(4, 12));
    fidel_client.must_add_peer(r1, new_learner_peer(4, 13));

    let resp = add_peer(new_learner_peer(4, 14));
    let err_msg = resp.get_header().get_error().get_message();
    assert!(err_msg.contains("duplicated"), "{:?}", resp);
    fidel_client.must_none_peer(r1, new_learner_peer(4, 14));

    let resp = add_peer(new_peer(4, 15));
    let err_msg = resp.get_header().get_error().get_message();
    assert!(err_msg.contains("duplicated"), "{:?}", resp);
    fidel_client.must_none_peer(r1, new_peer(4, 15));
}

#[test]
fn test_node_conf_change_safe() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_conf_change_safe(&mut cluster);
}

#[test]
fn test_server_safe_conf_change() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_conf_change_safe(&mut cluster);
}

#[test]
fn test_server_transfer_leader_safe() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    configure_for_transfer_leader(&mut cluster);
    test_transfer_leader_safe(&mut cluster);
}

#[test]
fn test_conf_change_remove_leader() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.causet.violetabft_store.allow_remove_leader = false;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    // Transfer leader to the first peer.
    cluster.must_transfer_leader(r1, new_peer(1, 1));
    // Put a new kv to ensure leader has applied to newest log, so that to avoid
    // false warning about plightlikeing conf change.
    cluster.must_put(b"k1", b"v1");

    // Try to remove leader, which should be ignored.
    let res =
        call_conf_change(&mut cluster, r1, ConfChangeType::RemoveNode, new_peer(1, 1)).unwrap();
    assert!(
        res.get_header()
            .get_error()
            .get_message()
            .contains("ignore remove leader"),
        "{:?}",
        res
    );
}

#[test]
fn test_node_learner_conf_change() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_learner_conf_change(&mut cluster);
}

#[test]
fn test_learner_with_slow_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    (0..10).for_each(|_| cluster.must_put(b"k1", b"v1"));

    struct SnapshotFilter {
        count: Arc<AtomicUsize>,
        filter: Arc<AtomicBool>,
    }

    impl Filter for SnapshotFilter {
        fn before(&self, msgs: &mut Vec<VioletaBftMessage>) -> Result<()> {
            let count = msgs
                .iter()
                .filter(|m| {
                    // A snapshot stream should have 2 Solitons at least,
                    // the first for metadata and subsequences for data.
                    m.get_message().get_msg_type() == MessageType::MsgSnapshot
                        && m.get_message().get_snapshot().has_metadata()
                })
                .count();
            self.count.fetch_add(count, Ordering::SeqCst);

            if self.filter.load(Ordering::SeqCst) {
                let old_len = msgs.len();
                msgs.retain(|m| m.get_message().get_msg_type() != MessageType::MsgSnapshot);
                if msgs.len() < old_len {
                    return Err(box_err!("lightlike snapshot fail"));
                }
            }
            Ok(())
        }
    }

    let count = Arc::new(AtomicUsize::new(0));
    let filter = Arc::new(AtomicBool::new(true));
    let snap_filter = Box::new(SnapshotFilter {
        count: Arc::clone(&count),
        filter: Arc::clone(&filter),
    });

    // New added learner should keep plightlikeing until snapshot is applied.
    cluster.sim.wl().add_lightlike_filter(1, snap_filter);
    fidel_client.must_add_peer(r1, new_learner_peer(2, 2));
    for _ in 0..500 {
        sleep_ms(10);
        if count.load(Ordering::SeqCst) > 0 {
            break;
        }
    }
    assert!(count.load(Ordering::SeqCst) > 0);
    assert_eq!(fidel_client.get_plightlikeing_peers()[&2], new_learner_peer(2, 2));

    // Clear snapshot filter and promote peer 2 to voter.
    filter.store(false, Ordering::SeqCst);
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    // Add a learner peer and test promoting it with snapshot instead of proposal.
    fidel_client.must_add_peer(r1, new_learner_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.stop_node(3);
    fidel_client.must_add_peer(r1, new_peer(3, 3));
    // Ensure violetabftstore will gc all applied violetabft logs.
    (0..30).for_each(|_| cluster.must_put(b"k2", b"v2"));

    // peer 3 will be promoted by snapshot instead of normal proposal.
    count.store(0, Ordering::SeqCst);
    cluster.run_node(3).unwrap();
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    // Transfer leader so that peer 3 can report to fidel with `Peer` in memory.
    fidel_client.transfer_leader(r1, new_peer(3, 3));
    fidel_client.brane_leader_must_be(r1, new_peer(3, 3));
    assert!(count.load(Ordering::SeqCst) > 0);
}

fn test_stale_peer<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    fidel_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    fidel_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // replace peer 3 with peer 4 while peer 3 is isolated.
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    fidel_client.must_remove_peer(r1, new_peer(3, 3));
    fidel_client.must_add_peer(r1, new_peer(4, 4));
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");

    // After the peer gets back to the cluster, it knows it's removed.
    cluster.clear_lightlike_filters();
    must_get_none(&cluster.get_engine(3), b"k1");
}

#[test]
fn test_node_stale_peer() {
    let mut cluster = new_node_cluster(0, 4);
    // To avoid stale peers know they are stale from FIDel.
    cluster.causet.violetabft_store.max_leader_missing_duration = ReadableDuration::hours(2);
    test_stale_peer(&mut cluster);
}

fn call_conf_change<T>(
    cluster: &mut Cluster<T>,
    brane_id: u64,
    conf_change_type: ConfChangeType,
    peer: meta_timeshare::Peer,
) -> Result<VioletaBftCmdResponse>
where
    T: Simulator,
{
    let conf_change = new_change_peer_request(conf_change_type, peer);
    let epoch = cluster.fidel_client.get_brane_epoch(brane_id);
    let admin_req = new_admin_request(brane_id, &epoch, conf_change);
    cluster.call_command_on_leader(admin_req, Duration::from_secs(3))
}

/// Tests if conf change relies on heartbeat.
#[test]
fn test_conf_change_fast() {
    let mut cluster = new_server_cluster(0, 3);
    // Sets heartbeat timeout to more than 5 seconds. It also changes the election timeout,
    // but it's OK as the cluster spacelikes with only one peer, it will campaigns immediately.
    configure_for_lease_read(&mut cluster, Some(5000), None);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    let timer = Instant::now();
    // If conf change relies on heartbeat, it will take more than 5 seconds to finish,
    // hence it must timeout.
    fidel_client.must_add_peer(r1, new_learner_peer(2, 2));
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    assert!(timer.elapsed() < Duration::from_secs(5));
}
