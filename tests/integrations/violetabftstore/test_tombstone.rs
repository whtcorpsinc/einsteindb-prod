// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::sync::Arc;
use std::thread;
use std::time::*;

use crossbeam::channel;
use ekvproto::violetabft_server_timeshare::{PeerState, VioletaBftMessage, BraneLocalState, StoreIdent};
use protobuf::Message;
use violetabft::evioletabft_timeshare::MessageType;
use violetabftstore::interlock::::config::*;

use engine_lmdb::raw::WriBlock;
use engine_lmdb::Compat;
use edb::{Iterable, Peekable};
use edb::{SyncMuBlock, Causet_VIOLETABFT};
use test_violetabftstore::*;

fn test_tombstone<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // add peer (2,2) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, b"k1", b"v1");

    // add peer (3, 3) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");

    // Remove peer (2, 2) from brane 1.
    fidel_client.must_remove_peer(r1, new_peer(2, 2));

    // After new leader is elected, the change peer must be finished.
    cluster.leader_of_brane(r1).unwrap();
    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, b"k1");
    must_get_none(&engine_2, b"k3");
    let mut existing_kvs = vec![];
    for causet in engine_2.causet_names() {
        engine_2
            .c()
            .scan_causet(causet, b"", &[0xFF], false, |k, v| {
                existing_kvs.push((k.to_vec(), v.to_vec()));
                Ok(true)
            })
            .unwrap();
    }
    // only tombstone key and store ident key exist.
    assert_eq!(existing_kvs.len(), 2);
    existing_kvs.sort();
    assert_eq!(existing_kvs[0].0.as_slice(), tuplespaceInstanton::STORE_IDENT_KEY);
    assert_eq!(existing_kvs[1].0, tuplespaceInstanton::brane_state_key(r1));

    let mut ident = StoreIdent::default();
    ident.merge_from_bytes(&existing_kvs[0].1).unwrap();
    assert_eq!(ident.get_store_id(), 2);
    assert_eq!(ident.get_cluster_id(), cluster.id());

    let mut state = BraneLocalState::default();
    state.merge_from_bytes(&existing_kvs[1].1).unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);

    // The peer 2 may be destroyed by:
    // 1. Apply the ConfChange RemovePeer command, the tombstone ConfVer is 4
    // 2. Receive a GC command before applying 1, the tombstone ConfVer is 3
    let conf_ver = state.get_brane().get_brane_epoch().get_conf_ver();
    assert!(conf_ver == 4 || conf_ver == 3);

    // lightlike a stale violetabft message to peer (2, 2)
    let mut violetabft_msg = VioletaBftMessage::default();

    violetabft_msg.set_brane_id(r1);
    // Use an invalid from peer to ignore gc peer message.
    violetabft_msg.set_from_peer(new_peer(0, 0));
    violetabft_msg.set_to_peer(new_peer(2, 2));
    violetabft_msg.mut_brane_epoch().set_conf_ver(0);
    violetabft_msg.mut_brane_epoch().set_version(0);

    cluster.lightlike_violetabft_msg(violetabft_msg).unwrap();

    // We must get BraneNotFound error.
    let brane_status = new_status_request(r1, new_peer(2, 2), new_brane_leader_cmd());
    let resp = cluster
        .call_command(brane_status, Duration::from_secs(5))
        .unwrap();
    assert!(
        resp.get_header().get_error().has_brane_not_found(),
        "brane must not found, but got {:?}",
        resp
    );
}

#[test]
fn test_node_tombstone() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_tombstone(&mut cluster);
}

#[test]
fn test_server_tombstone() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_tombstone(&mut cluster);
}

fn test_fast_destroy<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);

    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");
    // remove peer (3, 3)
    fidel_client.must_remove_peer(1, new_peer(3, 3));

    must_get_none(&engine_3, b"k1");

    cluster.stop_node(3);

    let key = tuplespaceInstanton::brane_state_key(1);
    let state: BraneLocalState = engine_3.c().get_msg_causet(Causet_VIOLETABFT, &key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);

    // Force add some dirty data.
    engine_3.put(&tuplespaceInstanton::data_key(b"k0"), b"v0").unwrap();

    cluster.must_put(b"k2", b"v2");

    // spacelike node again.
    cluster.run_node(3).unwrap();

    // add new peer in node 3
    fidel_client.must_add_peer(1, new_peer(3, 4));

    must_get_equal(&engine_3, b"k2", b"v2");
    // the dirty data must be cleared up.
    must_get_none(&engine_3, b"k0");
}

#[test]
fn test_server_fast_destroy() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_fast_destroy(&mut cluster);
}

fn test_readd_peer<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // add peer (2,2) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, b"k1", b"v1");

    // add peer (3, 3) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");

    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));

    // Remove peer (2, 2) from brane 1.
    fidel_client.must_remove_peer(r1, new_peer(2, 2));

    // After new leader is elected, the change peer must be finished.
    cluster.leader_of_brane(r1).unwrap();
    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    fidel_client.must_add_peer(r1, new_peer(2, 4));

    cluster.clear_lightlike_filters();
    cluster.must_put(b"k4", b"v4");
    let engine = cluster.get_engine(2);
    must_get_equal(&engine, b"k4", b"v4");

    // Stale gc message should be ignored.
    let epoch = fidel_client.get_brane_epoch(r1);
    let mut gc_msg = VioletaBftMessage::default();
    gc_msg.set_brane_id(r1);
    gc_msg.set_from_peer(new_peer(1, 1));
    gc_msg.set_to_peer(new_peer(2, 2));
    gc_msg.set_brane_epoch(epoch);
    gc_msg.set_is_tombstone(true);
    cluster.lightlike_violetabft_msg(gc_msg).unwrap();
    // Fixme: find a better way to check if the message is ignored.
    thread::sleep(Duration::from_secs(1));
    must_get_equal(&engine, b"k4", b"v4");
}

#[test]
fn test_node_readd_peer() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_readd_peer(&mut cluster);
}

#[test]
fn test_server_readd_peer() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_readd_peer(&mut cluster);
}

// Simulate a case that edb exit before a removed peer clean up its stale meta.
#[test]
fn test_server_stale_meta() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    cluster.run();
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    fidel_client.must_remove_peer(1, new_peer(3, 3));
    fidel_client.must_add_peer(1, new_peer(3, 4));
    cluster.shutdown();

    let engine_3 = cluster.get_engine(3);
    let mut state: BraneLocalState = engine_3
        .c()
        .get_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(1))
        .unwrap()
        .unwrap();
    state.set_state(PeerState::Tombstone);

    engine_3
        .c()
        .put_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(1), &state)
        .unwrap();
    cluster.clear_lightlike_filters();

    // avoid TIMEWAIT
    sleep_ms(500);
    cluster.spacelike().unwrap();

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&engine_3, b"k1", b"v1");
}

/// Tests a tombstone peer won't trigger wrong gc message.
///
/// An uninitialized peer's peer list is empty. If a message from a healthy peer passes
/// all the other checks accidentally, it may trigger a tombstone message which will
/// make the healthy peer destroy all its data.
#[test]
fn test_safe_tombstone_gc() {
    let mut cluster = new_node_cluster(0, 5);

    let tick = cluster.causet.violetabft_store.violetabft_election_timeout_ticks;
    let base_tick_interval = cluster.causet.violetabft_store.violetabft_base_tick_interval.0;
    let check_interval = base_tick_interval * (tick as u32 * 2 + 1);
    cluster.causet.violetabft_store.peer_stale_state_check_interval = ReadableDuration(check_interval);
    cluster.causet.violetabft_store.abnormal_leader_missing_duration = ReadableDuration(check_interval * 2);
    cluster.causet.violetabft_store.max_leader_missing_duration = ReadableDuration(check_interval * 2);

    let fidel_client = Arc::clone(&cluster.fidel_client);

    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    let r = cluster.run_conf_change();
    fidel_client.must_add_peer(r, new_peer(2, 2));
    fidel_client.must_add_peer(r, new_peer(3, 3));

    cluster.add_lightlike_filter(IsolationFilterFactory::new(4));

    fidel_client.must_add_peer(r, new_peer(4, 4));
    fidel_client.must_add_peer(r, new_peer(5, 5));
    cluster.must_transfer_leader(r, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(5), b"k1", b"v1");

    let (tx, rx) = channel::unbounded();
    cluster.clear_lightlike_filters();
    cluster.add_lightlike_filter(IsolationFilterFactory::new(5));
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(r, 4)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike)
            .set_msg_callback(Arc::new(move |msg| {
                let _ = tx.lightlike(msg.clone());
            })),
    ));

    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    fidel_client.must_remove_peer(r, new_peer(4, 4));
    let key = tuplespaceInstanton::brane_state_key(r);
    let mut state: Option<BraneLocalState> = None;
    let timer = Instant::now();
    while timer.elapsed() < Duration::from_secs(5) {
        state = cluster.get_engine(4).c().get_msg_causet(Causet_VIOLETABFT, &key).unwrap();
        if state.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    if state.is_none() {
        panic!("brane on store 4 has not been tombstone after 5 seconds.");
    }
    cluster.clear_lightlike_filters();
    cluster.add_lightlike_filter(PartitionFilterFactory::new(vec![1, 2, 3], vec![4, 5]));

    thread::sleep(base_tick_interval * tick as u32 * 3);
    must_get_equal(&cluster.get_engine(5), b"k1", b"v1");
}
