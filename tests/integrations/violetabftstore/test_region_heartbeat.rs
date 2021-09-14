// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::sync::mpsc;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use test_violetabftstore::*;
use violetabftstore::interlock::::config::*;
use violetabftstore::interlock::::time::UnixSecs as FidelInstant;
use violetabftstore::interlock::::HandyRwLock;

fn wait_down_peers<T: Simulator>(cluster: &Cluster<T>, count: u64, peer: Option<u64>) {
    let mut peers = cluster.get_down_peers();
    for _ in 1..1000 {
        if peers.len() == count as usize && peer.as_ref().map_or(true, |p| peers.contains_key(p)) {
            return;
        }
        sleep(Duration::from_millis(10));
        peers = cluster.get_down_peers();
    }
    panic!(
        "got {:?}, want {} peers which should include {:?}",
        peers, count, peer
    );
}

fn test_down_peers<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.causet.violetabft_store.max_peer_down_duration = ReadableDuration::secs(1);
    cluster.run();

    // Kill 1, 2
    for len in 1..3 {
        let id = len;
        cluster.stop_node(id);
        wait_down_peers(cluster, len, Some(id));
    }

    // Respacelike 1, 2
    cluster.run_node(1).unwrap();
    cluster.run_node(2).unwrap();
    wait_down_peers(cluster, 0, None);

    cluster.stop_node(1);

    cluster.must_put(b"k1", b"v1");
    // max peer down duration is 500 millis, but we only report down time in seconds,
    // so sleep 1 second to make the old down second is always larger than new down second
    // by at lease 1 second.
    sleep_ms(1000);

    wait_down_peers(cluster, 1, Some(1));
    let down_secs = cluster.get_down_peers()[&1].get_down_seconds();
    let timer = Instant::now();
    let leader = cluster.leader_of_brane(1).unwrap();
    let new_leader = if leader.get_id() == 2 {
        new_peer(3, 3)
    } else {
        new_peer(2, 2)
    };

    cluster.must_transfer_leader(1, new_leader);
    // new leader should reset all down peer list.
    wait_down_peers(cluster, 0, None);
    wait_down_peers(cluster, 1, Some(1));
    assert!(
        cluster.get_down_peers()[&1].get_down_seconds() < down_secs + timer.elapsed().as_secs()
    );

    // Ensure that node will not reuse the previous peer heartbeats.
    cluster.must_transfer_leader(1, leader);
    wait_down_peers(cluster, 0, None);
    wait_down_peers(cluster, 1, Some(1));
    assert!(cluster.get_down_peers()[&1].get_down_seconds() < timer.elapsed().as_secs() + 1);
}

#[test]
fn test_server_down_peers_with_hibernate_branes() {
    let mut cluster = new_server_cluster(0, 5);
    // When hibernate_branes is enabled, down peers are not detected in time
    // by design. So here use a short check interval to trigger brane heartbeat
    // more frequently.
    cluster.causet.violetabft_store.peer_stale_state_check_interval = ReadableDuration::millis(500);
    test_down_peers(&mut cluster);
}

#[test]
fn test_server_down_peers_without_hibernate_branes() {
    let mut cluster = new_server_cluster(0, 5);
    cluster.causet.violetabft_store.hibernate_branes = false;
    test_down_peers(&mut cluster);
}

fn test_plightlikeing_peers<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let brane_id = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");

    let (tx, _) = mpsc::channel();
    cluster
        .sim
        .wl()
        .add_recv_filter(2, Box::new(DropSnapshotFilter::new(tx)));

    fidel_client.must_add_peer(brane_id, new_peer(2, 2));

    let mut tried_times = 0;
    loop {
        tried_times += 1;
        if tried_times > 100 {
            panic!("can't get plightlikeing peer after {} tries.", tried_times);
        }
        let plightlikeing_peers = cluster.fidel_client.get_plightlikeing_peers();
        if plightlikeing_peers.is_empty() {
            sleep(Duration::from_millis(100));
        } else {
            assert_eq!(plightlikeing_peers[&2], new_peer(2, 2));
            break;
        }
    }

    cluster.sim.wl().clear_recv_filters(2);
    cluster.must_put(b"k2", b"v2");

    tried_times = 0;
    loop {
        tried_times += 1;
        let plightlikeing_peers = cluster.fidel_client.get_plightlikeing_peers();
        if !plightlikeing_peers.is_empty() {
            sleep(Duration::from_millis(100));
        } else {
            return;
        }
        if tried_times > 100 {
            panic!(
                "plightlikeing peer {:?} still exists after {} tries.",
                plightlikeing_peers, tried_times
            );
        }
    }
}

#[test]
fn test_node_plightlikeing_peers() {
    let mut cluster = new_node_cluster(0, 3);
    test_plightlikeing_peers(&mut cluster);
}

#[test]
fn test_server_plightlikeing_peers() {
    let mut cluster = new_server_cluster(0, 3);
    test_plightlikeing_peers(&mut cluster);
}

#[test]
fn test_brane_heartbeat_timestamp() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();

    // transfer leader to (2, 2) first to make address resolve happen early.
    cluster.must_transfer_leader(1, new_peer(2, 2));
    let reported_ts = cluster.fidel_client.get_brane_last_report_ts(1).unwrap();
    assert_ne!(reported_ts, FidelInstant::zero());

    sleep(Duration::from_millis(1000));
    cluster.must_transfer_leader(1, new_peer(1, 1));
    sleep(Duration::from_millis(1000));
    cluster.must_transfer_leader(1, new_peer(2, 2));
    for _ in 0..100 {
        sleep_ms(100);
        let reported_ts_now = cluster.fidel_client.get_brane_last_report_ts(1).unwrap();
        if reported_ts_now > reported_ts {
            return;
        }
    }
    panic!("reported ts should be fideliod");
}

// FIXME(nrc) failing on CI only
#[causet(feature = "protobuf-codec")]
#[test]
fn test_brane_heartbeat_term() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();

    // transfer leader to (2, 2) first to make address resolve happen early.
    cluster.must_transfer_leader(1, new_peer(2, 2));
    let reported_term = cluster.fidel_client.get_brane_last_report_term(1).unwrap();
    assert_ne!(reported_term, 0);

    // transfer leader to increase the term
    cluster.must_transfer_leader(1, new_peer(1, 1));
    for _ in 0..100 {
        sleep_ms(100);
        let reported_term_now = cluster.fidel_client.get_brane_last_report_term(1).unwrap();
        if reported_term_now > reported_term {
            return;
        }
    }
    panic!("reported term should be fideliod");
}
