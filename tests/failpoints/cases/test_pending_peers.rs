// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::Arc;
use std::time::Instant;

use test_violetabftstore::*;

use violetabftstore::interlock::::config::*;

#[test]
fn test_plightlikeing_peers() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.causet.violetabft_store.fidel_heartbeat_tick_interval = ReadableDuration::millis(100);

    let brane_worker_fp = "brane_apply_snap";

    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let brane_id = cluster.run_conf_change();
    fidel_client.must_add_peer(brane_id, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");

    fail::causet(brane_worker_fp, "sleep(2000)").unwrap();
    fidel_client.must_add_peer(brane_id, new_peer(3, 3));
    sleep_ms(1000);
    let plightlikeing_peers = fidel_client.get_plightlikeing_peers();
    // Brane worker is not spacelikeed, snapshot should not be applied yet.
    assert_eq!(plightlikeing_peers[&3], new_peer(3, 3));
    // But it will be applied finally.
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    sleep_ms(100);
    let plightlikeing_peers = fidel_client.get_plightlikeing_peers();
    assert!(plightlikeing_peers.is_empty());
}

// Tests if violetabftstore and apply worker write truncated_state concurrently could lead to
// dirty write.
#[test]
fn test_plightlikeing_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    let election_timeout = configure_for_lease_read(&mut cluster, None, Some(15));
    let gc_limit = cluster.causet.violetabft_store.violetabft_log_gc_count_limit;
    cluster.causet.violetabft_store.fidel_heartbeat_tick_interval = ReadableDuration::millis(100);

    let handle_snapshot_fp = "apply_on_handle_snapshot_1_1";
    let handle_snapshot_finish_fp = "apply_on_handle_snapshot_finish_1_1";
    fail::causet("apply_on_handle_snapshot_sync", "return").unwrap();

    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let brane_id = cluster.run_conf_change();
    fidel_client.must_add_peer(brane_id, new_peer(2, 2));
    cluster.must_transfer_leader(brane_id, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");

    fail::causet(handle_snapshot_fp, "pause").unwrap();
    fidel_client.must_add_peer(brane_id, new_peer(3, 3));
    // Give some time for peer 3 to request snapshot.
    sleep_ms(100);

    // Isolate peer 1 from rest of the cluster.
    cluster.add_lightlike_filter(IsolationFilterFactory::new(1));

    sleep_ms((election_timeout.as_millis() * 2) as _);
    cluster.reset_leader_of_brane(brane_id);
    // Compact logs to force requesting snapshot after clearing lightlike filters.
    let state2 = cluster.truncated_state(1, 2);
    for i in 1..gc_limit * 10 {
        let k = i.to_string().into_bytes();
        cluster.must_put(&k, &k.clone());
    }
    for _ in 0..100 {
        if cluster
            .get_violetabft_engine(2)
            .get(&tuplespaceInstanton::violetabft_log_key(1, state2.get_index() + 5 * gc_limit))
            .unwrap()
            .is_none()
        {
            break;
        }
        sleep_ms(50);
    }

    // Make sure peer 1 has applied snapshot.
    cluster.clear_lightlike_filters();
    let spacelike = Instant::now();
    loop {
        if cluster.fidel_client.get_plightlikeing_peers().get(&1).is_none()
            || spacelike.elapsed() > election_timeout * 10
        {
            break;
        }
        sleep_ms(50);
    }
    let state1 = cluster.truncated_state(1, 1);

    // Peer 2 continues to handle snapshot.
    fail::causet(handle_snapshot_finish_fp, "pause").unwrap();
    fail::remove(handle_snapshot_fp);
    sleep_ms(200);
    let state2 = cluster.truncated_state(1, 1);
    fail::remove(handle_snapshot_finish_fp);
    assert!(
        state1.get_term() <= state2.get_term(),
        "{:?} {:?}",
        state1,
        state2
    );
    assert!(
        state1.get_index() <= state2.get_index(),
        "{:?} {:?}",
        state1,
        state2
    );
}
