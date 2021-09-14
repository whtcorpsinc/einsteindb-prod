// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Duration;

use grpcio::{ChannelBuilder, Environment};
use ekvproto::{kvrpc_timeshare::*, edb_timeshare::EINSTEINDBClient};
use test_violetabftstore::*;
use violetabftstore::interlock::::{collections::HashMap, HandyRwLock};

// In theory, violetabft can propose conf change as long as there is no plightlikeing one. Replicas
// don't apply logs synchronously, so it's possible the old leader is removed before the new
// leader applies all logs.
// In the current implementation, the new leader rejects conf change until it applies all logs.
// It guarantees the correctness of green GC. This test is to prevent breaking it in the
// future.
#[test]
fn test_collect_lock_from_stale_leader() {
    let mut cluster = new_server_cluster(0, 2);
    cluster.fidel_client.disable_default_operator();
    let brane_id = cluster.run_conf_change();
    let leader = cluster.leader_of_brane(brane_id).unwrap();

    // Create clients.
    let env = Arc::new(Environment::new(1));
    let mut clients = HashMap::default();
    for node_id in cluster.get_node_ids() {
        let channel =
            ChannelBuilder::new(Arc::clone(&env)).connect(cluster.sim.rl().get_addr(node_id));
        let client = EINSTEINDBClient::new(channel);
        clients.insert(node_id, client);
    }

    // Start transferring the brane to store 2.
    let new_peer = new_peer(2, 1003);
    cluster.fidel_client.must_add_peer(brane_id, new_peer.clone());

    // Create the ctx of the first brane.
    let leader_client = clients.get(&leader.get_store_id()).unwrap();
    let mut ctx = Context::default();
    ctx.set_brane_id(brane_id);
    ctx.set_peer(leader.clone());
    ctx.set_brane_epoch(cluster.get_brane_epoch(brane_id));

    // Pause the new peer applying so that when it becomes the leader, it doesn't apply all logs.
    let new_leader_apply_fp = "on_handle_apply_1003";
    fail::causet(new_leader_apply_fp, "pause").unwrap();
    must_kv_prewrite(
        leader_client,
        ctx,
        vec![new_mutation(Op::Put, b"k1", b"v")],
        b"k1".to_vec(),
        10,
    );

    // Leader election only considers the progress of applightlikeing logs, so it can succeed.
    cluster.must_transfer_leader(brane_id, new_peer.clone());
    // It shouldn't succeed in the current implementation.
    cluster.fidel_client.remove_peer(brane_id, leader.clone());
    std::thread::sleep(Duration::from_secs(1));
    cluster.fidel_client.must_have_peer(brane_id, leader);

    // Must scan the dagger from the old leader.
    let locks = must_physical_scan_lock(leader_client, Context::default(), 100, b"", 10);
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].get_key(), b"k1");

    // Can't scan the dagger from the new leader.
    let leader_client = clients.get(&new_peer.get_store_id()).unwrap();
    must_register_lock_semaphore(leader_client, 100);
    let locks = must_check_lock_semaphore(leader_client, 100, true);
    assert!(locks.is_empty());
    let locks = must_physical_scan_lock(leader_client, Context::default(), 100, b"", 10);
    assert!(locks.is_empty());

    fail::remove(new_leader_apply_fp);
}

#[test]
fn test_semaphore_lightlike_error() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let max_ts = 100;
    must_register_lock_semaphore(&client, max_ts);
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![new_mutation(Op::Put, b"k1", b"v")],
        b"k1".to_vec(),
        10,
    );
    assert_eq!(must_check_lock_semaphore(&client, max_ts, true).len(), 1);

    let semaphore_lightlike_fp = "lock_semaphore_lightlike";
    fail::causet(semaphore_lightlike_fp, "return").unwrap();
    must_kv_prewrite(
        &client,
        ctx,
        vec![new_mutation(Op::Put, b"k2", b"v")],
        b"k1".to_vec(),
        10,
    );
    let resp = check_lock_semaphore(&client, max_ts);
    assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
    // Should mark dirty if fails to lightlike locks.
    assert!(!resp.get_is_clean());
}

#[test]
fn test_notify_semaphore_after_apply() {
    let (mut cluster, client, ctx) = must_new_cluster_and_kv_client();
    cluster.fidel_client.disable_default_operator();
    let post_apply_query_fp = "notify_lock_semaphore_query";
    let apply_plain_kvs_fp = "notify_lock_semaphore_snapshot";

    // Write a dagger and pause before notifying the dagger semaphore.
    let max_ts = 100;
    must_register_lock_semaphore(&client, max_ts);
    fail::causet(post_apply_query_fp, "pause").unwrap();
    let key = b"k";
    let (client_clone, ctx_clone) = (client.clone(), ctx.clone());
    std::thread::spawn(move || {
        must_kv_prewrite(
            &client_clone,
            ctx_clone,
            vec![new_mutation(Op::Put, key, b"v")],
            key.to_vec(),
            10,
        );
    });
    // We can use physical_scan_lock to get the dagger because we notify the dagger semaphore after writing data to the rocskdb.
    let mut locks = vec![];
    for _ in 1..100 {
        sleep_ms(10);
        assert!(must_check_lock_semaphore(&client, max_ts, true).is_empty());
        locks.extlightlike(must_physical_scan_lock(
            &client,
            ctx.clone(),
            max_ts,
            b"",
            100,
        ));
        if !locks.is_empty() {
            break;
        }
    }
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].get_key(), key);
    fail::remove(post_apply_query_fp);
    assert_eq!(must_check_lock_semaphore(&client, max_ts, true).len(), 1);

    // Add a new store.
    let store_id = cluster.add_new_engine();
    let channel = ChannelBuilder::new(Arc::new(Environment::new(1)))
        .connect(cluster.sim.rl().get_addr(store_id));
    let replica_client = EINSTEINDBClient::new(channel);

    // Add a new peer and pause before notifying the dagger semaphore.
    must_register_lock_semaphore(&replica_client, max_ts);
    fail::causet(apply_plain_kvs_fp, "pause").unwrap();
    cluster
        .fidel_client
        .must_add_peer(ctx.get_brane_id(), new_peer(store_id, store_id));
    // We can use physical_scan_lock to get the dagger because we notify the dagger semaphore after writing data to the rocskdb.
    let mut locks = vec![];
    for _ in 1..100 {
        sleep_ms(10);
        assert!(must_check_lock_semaphore(&replica_client, max_ts, true).is_empty());
        locks.extlightlike(must_physical_scan_lock(
            &replica_client,
            ctx.clone(),
            max_ts,
            b"",
            100,
        ));
        if !locks.is_empty() {
            break;
        }
    }
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].get_key(), key);
    fail::remove(apply_plain_kvs_fp);
    assert_eq!(
        must_check_lock_semaphore(&replica_client, max_ts, true).len(),
        1
    );
}

// It may cause locks missing during green GC if the violetabftstore notifies the dagger semaphore before writing data to the lmdb:
//   1. CausetStore-1 transfers a brane to store-2 and store-2 is applying logs.
//   2. GC worker registers dagger semaphore on store-2 after calling dagger semaphore's callback and before finishing applying which means the dagger won't be observed.
//   3. GC worker scans locks on each store indeplightlikeently. It's possible GC worker has scanned all locks on store-2 and hasn't scanned locks on store-1.
//   4. CausetStore-2 applies all logs and removes the peer on store-1.
//   5. GC worker can't scan the dagger on store-1 because the peer has been destroyed.
//   6. GC worker can't get the dagger from store-2 because it can't observe the dagger and has scanned it.
#[test]
fn test_collect_applying_locks() {
    let mut cluster = new_server_cluster(0, 2);
    cluster.fidel_client.disable_default_operator();
    let brane_id = cluster.run_conf_change();
    let leader = cluster.leader_of_brane(brane_id).unwrap();

    // Create clients.
    let env = Arc::new(Environment::new(1));
    let mut clients = HashMap::default();
    for node_id in cluster.get_node_ids() {
        let channel =
            ChannelBuilder::new(Arc::clone(&env)).connect(cluster.sim.rl().get_addr(node_id));
        let client = EINSTEINDBClient::new(channel);
        clients.insert(node_id, client);
    }

    // Start transferring the brane to store 2.
    let new_peer = new_peer(2, 1003);
    cluster.fidel_client.must_add_peer(brane_id, new_peer.clone());

    // Create the ctx of the first brane.
    let store_1_client = clients.get(&leader.get_store_id()).unwrap();
    let mut ctx = Context::default();
    ctx.set_brane_id(brane_id);
    ctx.set_peer(leader.clone());
    ctx.set_brane_epoch(cluster.get_brane_epoch(brane_id));

    // Pause store-2 after calling semaphore callbacks and before writing to the lmdb.
    let new_leader_apply_fp = "post_handle_apply_1003";
    fail::causet(new_leader_apply_fp, "pause").unwrap();

    // Write 1 dagger.
    must_kv_prewrite(
        &store_1_client,
        ctx,
        vec![new_mutation(Op::Put, b"k1", b"v")],
        b"k1".to_vec(),
        10,
    );
    // Wait for store-2 applying.
    std::thread::sleep(Duration::from_secs(3));

    // Starting the process of green GC at safe point 20:
    //   1. Register dagger semaphores on all stores.
    //   2. Scan locks physically on each store indeplightlikeently.
    //   3. Get locks from all semaphores.
    let safe_point = 20;

    // Register dagger semaphores.
    clients.iter().for_each(|(_, c)| {
        must_register_lock_semaphore(c, safe_point);
    });

    // Finish scanning locks on store-2 and find nothing.
    let store_2_client = clients.get(&new_peer.get_store_id()).unwrap();
    let locks = must_physical_scan_lock(store_2_client, Context::default(), safe_point, b"", 1);
    assert!(locks.is_empty(), "{:?}", locks);

    // Transfer the brane from store-1 to store-2.
    fail::remove(new_leader_apply_fp);
    cluster.must_transfer_leader(brane_id, new_peer);
    cluster.fidel_client.must_remove_peer(brane_id, leader);
    // Wait for store-1 desroying the brane.
    std::thread::sleep(Duration::from_secs(3));

    // Scan locks on store-1 after the brane has been destroyed.
    let locks = must_physical_scan_lock(store_1_client, Context::default(), safe_point, b"", 1);
    assert!(locks.is_empty(), "{:?}", locks);

    // Check dagger semaphores.
    let mut locks = vec![];
    clients.iter().for_each(|(_, c)| {
        locks.extlightlike(must_check_lock_semaphore(c, safe_point, true));
    });
    // Must observe the applying dagger even through we can't use scan to get it.
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].get_key(), b"k1");
}
