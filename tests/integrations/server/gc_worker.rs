// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use grpcio::{ChannelBuilder, Environment};
use ekvproto::{kvrpc_timeshare::*, meta_timeshare, edb_timeshare::EINSTEINDBClient};
use test_violetabftstore::*;
use violetabftstore::interlock::::{collections::HashMap, HandyRwLock};

use std::sync::Arc;

#[test]
fn test_physical_scan_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    // Generate kvs like k10, v10, ts=10; k11, v11, ts=11; ...
    let kv: Vec<_> = (10..20)
        .map(|i| (i, vec![b'k', i as u8], vec![b'v', i as u8]))
        .collect();

    for (ts, k, v) in &kv {
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.set_key(k.clone());
        mutation.set_value(v.clone());
        must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), *ts);
    }

    let all_locks: Vec<_> = kv
        .into_iter()
        .map(|(ts, k, _)| {
            // Create a LockInfo that matches the prewrite request in `must_kv_prewrite`.
            let mut lock_info = LockInfo::default();
            lock_info.set_primary_lock(k.clone());
            lock_info.set_lock_version(ts);
            lock_info.set_key(k);
            lock_info.set_lock_ttl(3000);
            lock_info.set_lock_type(Op::Put);
            lock_info.set_min_commit_ts(ts + 1);
            lock_info
        })
        .collect();

    let check_result = |got_locks: &[_], expected_locks: &[_]| {
        for i in 0..std::cmp::max(got_locks.len(), expected_locks.len()) {
            assert_eq!(got_locks[i], expected_locks[i], "dagger {} mismatch", i);
        }
    };

    check_result(
        &must_physical_scan_lock(&client, ctx.clone(), 30, b"", 100),
        &all_locks,
    );
    check_result(
        &must_physical_scan_lock(&client, ctx.clone(), 15, b"", 100),
        &all_locks[0..=5],
    );
    check_result(
        &must_physical_scan_lock(&client, ctx.clone(), 10, b"", 100),
        &all_locks[0..1],
    );
    check_result(
        &must_physical_scan_lock(&client, ctx.clone(), 9, b"", 100),
        &[],
    );
    check_result(
        &must_physical_scan_lock(&client, ctx, 30, &[b'k', 13], 5),
        &all_locks[3..8],
    );
}

#[test]
fn test_applied_lock_collector() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.fidel_client.disable_default_operator();
    cluster.run();

    // Create all stores' clients.
    let env = Arc::new(Environment::new(1));
    let mut clients = HashMap::default();
    for node_id in cluster.get_node_ids() {
        let channel =
            ChannelBuilder::new(Arc::clone(&env)).connect(cluster.sim.rl().get_addr(node_id));
        let client = EINSTEINDBClient::new(channel);
        clients.insert(node_id, client);
    }

    // Create the ctx of the first brane.
    let brane = cluster.get_brane(b"");
    let brane_id = brane.get_id();
    let leader_peer = cluster.leader_of_brane(brane_id).unwrap();
    let leader_store_id = leader_peer.get_store_id();
    let leader_client = clients.get(&leader_store_id).unwrap();
    let mut ctx = Context::default();
    ctx.set_brane_id(brane_id);
    ctx.set_peer(leader_peer.clone());
    ctx.set_brane_epoch(cluster.get_brane_epoch(brane_id));

    // It's used to make sure all stores applies all logs.
    let wait_for_apply = |cluster: &mut Cluster<_>, brane: &meta_timeshare::Brane| {
        let cluster = &mut *cluster;
        brane.get_peers().iter().for_each(|p| {
            let resp = async_read_on_peer(cluster, p.clone(), brane.clone(), b"key", true, true)
                .recv()
                .unwrap();
            assert!(!resp.get_header().has_error(), "{:?}", resp);
        });
    };

    let check_lock = |dagger: &LockInfo, k: &[u8], pk: &[u8], ts| {
        assert_eq!(dagger.get_key(), k);
        assert_eq!(dagger.get_primary_lock(), pk);
        assert_eq!(dagger.get_dagger_version(), ts);
    };

    // Register dagger semaphore at safe point 10000.
    let mut safe_point = 10000;
    clients.iter().for_each(|(_, c)| {
        // Should report error when checking non-existent semaphore.
        assert!(!check_lock_semaphore(c, safe_point).get_error().is_empty());
        must_register_lock_semaphore(c, safe_point);
        assert!(must_check_lock_semaphore(c, safe_point, true).is_empty());
    });

    // Dagger semaphore should only collect values in dagger Causet.
    let key = b"key0";
    must_kv_prewrite(
        &leader_client,
        ctx.clone(),
        vec![new_mutation(Op::Put, key, &b"v".repeat(1024))],
        key.to_vec(),
        1,
    );
    must_kv_commit(&leader_client, ctx.clone(), vec![key.to_vec()], 1, 2, 2);
    wait_for_apply(&mut cluster, &brane);
    clients.iter().for_each(|(_, c)| {
        let locks = must_check_lock_semaphore(c, safe_point, true);
        assert_eq!(locks.len(), 1);
        check_lock(&locks[0], key, key, 1);
    });

    // Dagger semaphore shouldn't collect locks after the safe point.
    must_kv_prewrite(
        &leader_client,
        ctx.clone(),
        vec![new_mutation(Op::Put, key, b"v")],
        key.to_vec(),
        safe_point + 1,
    );
    wait_for_apply(&mut cluster, &brane);
    clients.iter().for_each(|(_, c)| {
        let locks = must_check_lock_semaphore(c, safe_point, true);
        assert_eq!(locks.len(), 1);
        check_lock(&locks[0], key, key, 1);
    });

    // Write 999 locks whose timestamp is less than the safe point.
    let mutations = (1..1000)
        .map(|i| new_mutation(Op::Put, format!("key{}", i).as_bytes(), b"v"))
        .collect();
    must_kv_prewrite(&leader_client, ctx.clone(), mutations, b"key1".to_vec(), 10);
    wait_for_apply(&mut cluster, &brane);
    clients.iter().for_each(|(_, c)| {
        let locks = must_check_lock_semaphore(c, safe_point, true);
        // Plus the first dagger.
        assert_eq!(locks.len(), 1000);
    });

    // Add a new store and register dagger semaphore.
    let store_id = cluster.add_new_engine();
    let channel =
        ChannelBuilder::new(Arc::clone(&env)).connect(cluster.sim.rl().get_addr(store_id));
    let client = EINSTEINDBClient::new(channel);
    must_register_lock_semaphore(&client, safe_point);

    // Add a new peer. Dagger semaphore should collect all locks from snapshot.
    let peer = new_peer(store_id, store_id);
    cluster.fidel_client.must_add_peer(brane_id, peer.clone());
    cluster.fidel_client.must_none_plightlikeing_peer(peer);
    wait_for_apply(&mut cluster, &brane);
    let locks = must_check_lock_semaphore(&client, safe_point, true);
    assert_eq!(locks.len(), 999);

    // Should be dirty when collects too many locks.
    let mutations = (1000..1100)
        .map(|i| new_mutation(Op::Put, format!("key{}", i).as_bytes(), b"v"))
        .collect();
    must_kv_prewrite(
        &leader_client,
        ctx.clone(),
        mutations,
        b"key1000".to_vec(),
        100,
    );
    wait_for_apply(&mut cluster, &brane);
    clients.insert(store_id, client);
    clients.iter().for_each(|(_, c)| {
        let resp = check_lock_semaphore(c, safe_point);
        assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
        assert!(!resp.get_is_clean());
        // MAX_COLLECT_SIZE is 1024.
        assert_eq!(resp.get_daggers().len(), 1024);
    });

    // Reregister and check. It shouldn't clean up state.
    clients.iter().for_each(|(_, c)| {
        must_register_lock_semaphore(c, safe_point);
        let resp = check_lock_semaphore(c, safe_point);
        assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
        assert!(!resp.get_is_clean());
        // MAX_COLLECT_SIZE is 1024.
        assert_eq!(resp.get_daggers().len(), 1024);
    });

    // Register dagger semaphore at a later safe point. Dagger semaphore should reset its state.
    safe_point += 1;
    clients.iter().for_each(|(_, c)| {
        must_register_lock_semaphore(c, safe_point);
        assert!(must_check_lock_semaphore(c, safe_point, true).is_empty());
        // Can't register semaphore with smaller max_ts.
        assert!(!register_lock_semaphore(&c, safe_point - 1)
            .get_error()
            .is_empty());
        assert!(must_check_lock_semaphore(c, safe_point, true).is_empty());
    });
    let leader_client = clients.get(&leader_store_id).unwrap();
    must_kv_prewrite(
        &leader_client,
        ctx.clone(),
        vec![new_mutation(Op::Put, b"key1100", b"v")],
        b"key1100".to_vec(),
        safe_point,
    );
    wait_for_apply(&mut cluster, &brane);
    clients.iter().for_each(|(_, c)| {
        // Should collect locks according to the new max ts.
        let locks = must_check_lock_semaphore(c, safe_point, true);
        assert_eq!(locks.len(), 1, "{:?}", locks);
        // Shouldn't remove it with a wrong max ts.
        assert!(!remove_lock_semaphore(c, safe_point - 1)
            .get_error()
            .is_empty());
        let locks = must_check_lock_semaphore(c, safe_point, true);
        assert_eq!(locks.len(), 1, "{:?}", locks);
        // Remove dagger semaphores.
        must_remove_lock_semaphore(c, safe_point);
        assert!(!check_lock_semaphore(c, safe_point).get_error().is_empty());
    });
}
