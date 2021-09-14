// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use grpcio::{ChannelBuilder, Environment};
use ekvproto::kvrpc_timeshare::*;
use ekvproto::meta_timeshare::{Peer, Brane};
use ekvproto::edb_timeshare::EINSTEINDBClient;

use test_violetabftstore::*;
use violetabftstore::interlock::::HandyRwLock;

fn must_deadlock(client: &EINSTEINDBClient, ctx: Context, key1: &[u8], ts: u64) {
    let key1 = key1.to_vec();
    let mut key2 = key1.clone();
    key2.push(0);
    must_kv_pessimistic_lock(client, ctx.clone(), key1.clone(), ts);
    must_kv_pessimistic_lock(client, ctx.clone(), key2.clone(), ts + 1);

    let (client_clone, ctx_clone, key1_clone) = (client.clone(), ctx.clone(), key1.clone());
    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = kv_pessimistic_lock(
            &client_clone,
            ctx_clone,
            vec![key1_clone],
            ts + 1,
            ts + 1,
            false,
        );
        tx.lightlike(1).unwrap();
    });
    // Sleep to make sure txn(ts+1) is waiting for txn(ts)
    thread::sleep(Duration::from_millis(500));
    let resp = kv_pessimistic_lock(client, ctx.clone(), vec![key2.clone()], ts, ts, false);
    assert_eq!(resp.errors.len(), 1);
    assert!(resp.errors[0].has_deadlock(), "{:?}", resp.get_errors());
    rx.recv().unwrap();

    // Clean up
    must_kv_pessimistic_rollback(client, ctx.clone(), key1, ts);
    must_kv_pessimistic_rollback(client, ctx, key2, ts);
}

fn build_leader_client(cluster: &mut Cluster<ServerCluster>, key: &[u8]) -> (EINSTEINDBClient, Context) {
    let brane_id = cluster.get_brane_id(key);
    let leader = cluster.leader_of_brane(brane_id).unwrap();
    let epoch = cluster.get_brane_epoch(brane_id);

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = EINSTEINDBClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_brane_id(brane_id);
    ctx.set_peer(leader);
    ctx.set_brane_epoch(epoch);

    (client, ctx)
}

/// Creates a deadlock on the store containing key.
fn must_detect_deadlock(cluster: &mut Cluster<ServerCluster>, key: &[u8], ts: u64) {
    let (client, ctx) = build_leader_client(cluster, key);
    must_deadlock(&client, ctx, key, ts);
}

fn deadlock_detector_leader_must_be(cluster: &mut Cluster<ServerCluster>, store_id: u64) {
    let leader_brane = cluster.get_brane(b"");
    assert_eq!(
        cluster
            .leader_of_brane(leader_brane.get_id())
            .unwrap()
            .get_store_id(),
        store_id
    );
    let leader_peer = find_peer_of_store(&leader_brane, store_id);
    cluster
        .fidel_client
        .brane_leader_must_be(leader_brane.get_id(), leader_peer);
}

fn must_transfer_leader(cluster: &mut Cluster<ServerCluster>, brane_key: &[u8], store_id: u64) {
    let brane = cluster.get_brane(brane_key);
    let target_peer = find_peer_of_store(&brane, store_id);
    cluster.must_transfer_leader(brane.get_id(), target_peer.clone());
    cluster
        .fidel_client
        .brane_leader_must_be(brane.get_id(), target_peer);
    // Make sure the new leader can get snapshot locally.
    cluster.must_get(brane_key);
}

/// Transfers the brane containing brane_key from source store to target peer.
///
/// REQUIRE: The source store must be the leader the brane and the target store must not have
/// this brane.
fn must_transfer_brane(
    cluster: &mut Cluster<ServerCluster>,
    brane_key: &[u8],
    source_store_id: u64,
    target_store_id: u64,
    target_peer_id: u64,
) {
    let target_peer = new_peer(target_store_id, target_peer_id);
    let brane = cluster.get_brane(brane_key);
    cluster
        .fidel_client
        .must_add_peer(brane.get_id(), target_peer);
    must_transfer_leader(cluster, brane_key, target_store_id);
    let source_peer = find_peer_of_store(&brane, source_store_id);
    cluster
        .fidel_client
        .must_remove_peer(brane.get_id(), source_peer);
}

fn must_merge_brane(
    cluster: &mut Cluster<ServerCluster>,
    source_brane_key: &[u8],
    target_brane_key: &[u8],
) {
    let (source_id, target_id) = (
        cluster.get_brane(source_brane_key).get_id(),
        cluster.get_brane(target_brane_key).get_id(),
    );
    cluster.fidel_client.must_merge(source_id, target_id);
}

fn find_peer_of_store(brane: &Brane, store_id: u64) -> Peer {
    brane
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .unwrap()
        .clone()
}

/// Creates a cluster with only one brane and store(1) is the leader of the brane.
fn new_cluster_for_deadlock_test(count: usize) -> Cluster<ServerCluster> {
    let mut cluster = new_server_cluster(0, count);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();
    // Brane 1 has 3 peers. And peer(1, 1) is the leader of brane 1.
    let brane_id = cluster.run_conf_change();
    fidel_client.must_add_peer(brane_id, new_peer(2, 2));
    fidel_client.must_add_peer(brane_id, new_peer(3, 3));
    cluster.must_transfer_leader(brane_id, new_peer(1, 1));
    deadlock_detector_leader_must_be(&mut cluster, 1);
    must_detect_deadlock(&mut cluster, b"k", 10);
    cluster
}

#[test]
fn test_detect_deadlock_when_transfer_leader() {
    let mut cluster = new_cluster_for_deadlock_test(3);
    // Transfer the leader of brane 1 to store(2).
    // The leader of deadlock detector should also be transfered to store(2).
    must_transfer_leader(&mut cluster, b"", 2);
    deadlock_detector_leader_must_be(&mut cluster, 2);
    must_detect_deadlock(&mut cluster, b"k", 10);
}

#[test]
fn test_detect_deadlock_when_split_brane() {
    let mut cluster = new_cluster_for_deadlock_test(3);
    let brane = cluster.get_brane(b"");
    cluster.must_split(&brane, b"k1");
    // After split, the leader is still store(1).
    deadlock_detector_leader_must_be(&mut cluster, 1);
    must_detect_deadlock(&mut cluster, b"k", 10);
    // Transfer the new brane's leader to store(2) and deadlock occours on it.
    must_transfer_leader(&mut cluster, b"k1", 2);
    deadlock_detector_leader_must_be(&mut cluster, 1);
    must_detect_deadlock(&mut cluster, b"k1", 10);
}

#[test]
fn test_detect_deadlock_when_transfer_brane() {
    let mut cluster = new_cluster_for_deadlock_test(4);
    // Transfer the leader brane to store(4) and the leader of deadlock detector should be
    // also transfered.
    must_transfer_brane(&mut cluster, b"k", 1, 4, 4);
    deadlock_detector_leader_must_be(&mut cluster, 4);
    must_detect_deadlock(&mut cluster, b"k", 10);

    let brane = cluster.get_brane(b"");
    cluster.must_split(&brane, b"k1");
    // Transfer the new brane to store(1). It shouldn't affect deadlock detector.
    must_transfer_brane(&mut cluster, b"k1", 4, 1, 5);
    deadlock_detector_leader_must_be(&mut cluster, 4);
    must_detect_deadlock(&mut cluster, b"k", 10);
    must_detect_deadlock(&mut cluster, b"k1", 10);

    // Transfer the new brane back to store(4) which will lightlike a role change message with empty
    // key cone. It shouldn't affect deadlock detector.
    must_transfer_brane(&mut cluster, b"k1", 1, 4, 6);
    deadlock_detector_leader_must_be(&mut cluster, 4);
    must_detect_deadlock(&mut cluster, b"k", 10);
    must_detect_deadlock(&mut cluster, b"k1", 10);
}

#[test]
fn test_detect_deadlock_when_merge_brane() {
    let mut cluster = new_cluster_for_deadlock_test(3);

    // Source brane will be destroyed.
    for as_target in &[false, true] {
        let brane = cluster.get_brane(b"");
        cluster.must_split(&brane, b"k1");
        if *as_target {
            must_merge_brane(&mut cluster, b"k1", b"");
        } else {
            must_merge_brane(&mut cluster, b"", b"k1");
        }
        deadlock_detector_leader_must_be(&mut cluster, 1);
        must_detect_deadlock(&mut cluster, b"k", 10);
    }

    // Leaders of two branes are on different store.
    for as_target in &[false, true] {
        let brane = cluster.get_brane(b"");
        cluster.must_split(&brane, b"k1");
        must_transfer_leader(&mut cluster, b"k1", 2);
        if *as_target {
            must_merge_brane(&mut cluster, b"k1", b"");
            deadlock_detector_leader_must_be(&mut cluster, 1);
        } else {
            must_merge_brane(&mut cluster, b"", b"k1");
            deadlock_detector_leader_must_be(&mut cluster, 2);
        }
        must_detect_deadlock(&mut cluster, b"k", 10);
        must_transfer_leader(&mut cluster, b"", 1);
    }
}
