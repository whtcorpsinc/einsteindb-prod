// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crossbeam::channel;
use engine_lmdb::raw::DB;
use engine_lmdb::Compat;
use edb::{Peekable, Causet_VIOLETABFT};
use ekvproto::violetabft_server_timeshare::{PeerState, VioletaBftApplyState, VioletaBftMessage, BraneLocalState};
use violetabft::evioletabft_timeshare::MessageType;
use std::mem;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use test_violetabftstore::*;
use violetabftstore::interlock::::HandyRwLock;

#[test]
fn test_wait_for_apply_index() {
    let mut cluster = new_server_cluster(0, 3);

    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    let p2 = new_peer(2, 2);
    cluster.fidel_client.must_add_peer(r1, p2.clone());
    let p3 = new_peer(3, 3);
    cluster.fidel_client.must_add_peer(r1, p3.clone());
    cluster.must_put(b"k0", b"v0");
    cluster.fidel_client.must_none_plightlikeing_peer(p2.clone());
    cluster.fidel_client.must_none_plightlikeing_peer(p3.clone());

    let brane = cluster.get_brane(b"k0");
    cluster.must_transfer_leader(brane.get_id(), p2);

    // Block all write cmd applying of Peer 3.
    fail::causet("on_apply_write_cmd", "sleep(2000)").unwrap();
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    // Peer 3 does not apply the cmd of putting 'k1' right now, then the follower read must
    // be blocked.
    must_get_none(&cluster.get_engine(3), b"k1");
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_get_causet_cmd("default", b"k1")],
        false,
    );
    request.mut_header().set_peer(p3);
    request.mut_header().set_replica_read(true);
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(3, request, cb)
        .unwrap();
    // Must timeout here
    assert!(rx.recv_timeout(Duration::from_millis(500)).is_err());
    fail::remove("on_apply_write_cmd");

    // After write cmd applied, the follower read will be executed.
    match rx.recv_timeout(Duration::from_secs(3)) {
        Ok(resp) => {
            assert_eq!(resp.get_responses().len(), 1);
            assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v1");
        }
        Err(_) => panic!("follower read failed"),
    }
}

#[test]
fn test_duplicate_read_index_ctx() {
    // Initialize cluster
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    cluster.causet.violetabft_store.violetabft_heartbeat_ticks = 1;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    // Set brane and peers
    let r1 = cluster.run_conf_change();
    let p1 = new_peer(1, 1);
    let p2 = new_peer(2, 2);
    cluster.fidel_client.must_add_peer(r1, p2.clone());
    let p3 = new_peer(3, 3);
    cluster.fidel_client.must_add_peer(r1, p3.clone());
    cluster.must_put(b"k0", b"v0");
    cluster.fidel_client.must_none_plightlikeing_peer(p2.clone());
    cluster.fidel_client.must_none_plightlikeing_peer(p3.clone());
    let brane = cluster.get_brane(b"k0");
    assert_eq!(cluster.leader_of_brane(brane.get_id()).unwrap(), p1);

    // Delay all violetabft messages to peer 1.
    let dropped_msgs = Arc::new(Mutex::new(Vec::new()));
    let (sx, rx) = channel::unbounded();
    let recv_filter = Box::new(
        BranePacketFilter::new(brane.get_id(), 1)
            .direction(Direction::Recv)
            .when(Arc::new(AtomicBool::new(true)))
            .reserve_dropped(Arc::clone(&dropped_msgs))
            .set_msg_callback(Arc::new(move |msg: &VioletaBftMessage| {
                if msg.get_message().get_msg_type() == MessageType::MsgReadIndex {
                    sx.lightlike(()).unwrap();
                }
            })),
    );
    cluster.sim.wl().add_recv_filter(1, recv_filter);

    // lightlike two read index requests to leader
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_read_index_cmd()],
        true,
    );
    request.mut_header().set_peer(p2);
    let (cb2, rx2) = make_cb(&request);
    // lightlike to peer 2
    cluster
        .sim
        .rl()
        .async_command_on_node(2, request.clone(), cb2)
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");
    request.mut_header().set_peer(p3);
    let (cb3, rx3) = make_cb(&request);
    // lightlike to peer 3
    cluster
        .sim
        .rl()
        .async_command_on_node(3, request, cb3)
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let router = cluster.sim.wl().get_router(1).unwrap();
    fail::causet("pause_on_peer_collect_message", "pause").unwrap();
    cluster.sim.wl().clear_recv_filters(1);
    for violetabft_msg in mem::replace(dropped_msgs.dagger().unwrap().as_mut(), vec![]) {
        router.lightlike_violetabft_message(violetabft_msg).unwrap();
    }
    fail::remove("pause_on_peer_collect_message");

    // read index response must not be dropped
    rx2.recv_timeout(Duration::from_secs(5)).unwrap();
    rx3.recv_timeout(Duration::from_secs(5)).unwrap();
}

#[test]
fn test_read_before_init() {
    // Initialize cluster
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    // Set brane and peers
    let r1 = cluster.run_conf_change();
    let p1 = new_peer(1, 1);
    let p2 = new_peer(2, 2);
    cluster.fidel_client.must_add_peer(r1, p2.clone());
    cluster.must_put(b"k0", b"v0");
    cluster.fidel_client.must_none_plightlikeing_peer(p2);
    must_get_equal(&cluster.get_engine(2), b"k0", b"v0");

    fail::causet("before_apply_snap_fidelio_brane", "return").unwrap();
    // Add peer 3
    let p3 = new_peer(3, 3);
    cluster.fidel_client.must_add_peer(r1, p3.clone());
    thread::sleep(Duration::from_millis(500));
    let brane = cluster.get_brane(b"k0");
    assert_eq!(cluster.leader_of_brane(r1).unwrap(), p1);

    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_get_causet_cmd("default", b"k0")],
        false,
    );
    request.mut_header().set_peer(p3);
    request.mut_header().set_replica_read(true);
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(3, request, cb)
        .unwrap();
    let resp = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    fail::remove("before_apply_snap_fidelio_brane");
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("not initialized yet"),
        "{:?}",
        resp.get_header().get_error()
    );
}

#[test]
fn test_read_applying_snapshot() {
    // Initialize cluster
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    // Set brane and peers
    let r1 = cluster.run_conf_change();
    let p1 = new_peer(1, 1);
    let p2 = new_peer(2, 2);
    cluster.fidel_client.must_add_peer(r1, p2.clone());
    cluster.must_put(b"k0", b"v0");
    cluster.fidel_client.must_none_plightlikeing_peer(p2);

    // Don't apply snapshot to init peer 3
    fail::causet("brane_apply_snap", "pause").unwrap();
    let p3 = new_peer(3, 3);
    cluster.fidel_client.must_add_peer(r1, p3.clone());
    thread::sleep(Duration::from_millis(500));

    // Check if peer 3 is applying snapshot
    let brane_key = tuplespaceInstanton::brane_state_key(r1);
    let brane_state: BraneLocalState = cluster
        .get_engine(3)
        .c()
        .get_msg_causet(Causet_VIOLETABFT, &brane_key)
        .unwrap()
        .unwrap();
    assert_eq!(brane_state.get_state(), PeerState::Applying);
    let brane = cluster.get_brane(b"k0");
    assert_eq!(cluster.leader_of_brane(r1).unwrap(), p1);

    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_get_causet_cmd("default", b"k0")],
        false,
    );
    request.mut_header().set_peer(p3);
    request.mut_header().set_replica_read(true);
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(3, request, cb)
        .unwrap();
    let resp = match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(r) => r,
        Err(_) => {
            fail::remove("brane_apply_snap");
            panic!("cannot receive response");
        }
    };
    fail::remove("brane_apply_snap");
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("applying snapshot"),
        "{:?}",
        resp.get_header().get_error()
    );
}

#[test]
fn test_read_after_cleanup_cone_for_snap() {
    let mut cluster = new_server_cluster(1, 3);
    configure_for_snapshot(&mut cluster);
    configure_for_lease_read(&mut cluster, Some(100), Some(10));
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    // Set brane and peers
    let r1 = cluster.run_conf_change();
    let p1 = new_peer(1, 1);
    let p2 = new_peer(2, 2);
    cluster.fidel_client.must_add_peer(r1, p2.clone());
    let p3 = new_peer(3, 3);
    cluster.fidel_client.must_add_peer(r1, p3.clone());
    cluster.must_put(b"k0", b"v0");
    cluster.fidel_client.must_none_plightlikeing_peer(p2);
    cluster.fidel_client.must_none_plightlikeing_peer(p3.clone());
    let brane = cluster.get_brane(b"k0");
    assert_eq!(cluster.leader_of_brane(brane.get_id()).unwrap(), p1);
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");
    cluster.stop_node(3);
    let last_index = cluster.violetabft_local_state(r1, 1).last_index;
    (0..10).for_each(|_| cluster.must_put(b"k1", b"v1"));
    // Ensure logs are compacted, then node 1 will lightlike a snapshot to node 3 later
    must_truncated_to(cluster.get_engine(1), r1, last_index + 1);

    fail::causet("lightlike_snapshot", "pause").unwrap();
    cluster.run_node(3).unwrap();
    // Sleep for a while to ensure peer 3 receives a HeartBeat
    thread::sleep(Duration::from_millis(500));

    // Add filter for delaying ReadIndexResp and MsgSnapshot
    let (read_index_sx, read_index_rx) = channel::unbounded::<VioletaBftMessage>();
    let (snap_sx, snap_rx) = channel::unbounded::<VioletaBftMessage>();
    let recv_filter = Box::new(
        BranePacketFilter::new(brane.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgSnapshot)
            .set_msg_callback(Arc::new(move |msg: &VioletaBftMessage| {
                snap_sx.lightlike(msg.clone()).unwrap();
            })),
    );
    let lightlike_read_index_filter = BranePacketFilter::new(brane.get_id(), 3)
        .direction(Direction::Recv)
        .msg_type(MessageType::MsgReadIndexResp)
        .set_msg_callback(Arc::new(move |msg: &VioletaBftMessage| {
            read_index_sx.lightlike(msg.clone()).unwrap();
        }));
    cluster.sim.wl().add_recv_filter(3, recv_filter);
    cluster.add_lightlike_filter(CloneFilterFactory(lightlike_read_index_filter));
    fail::remove("lightlike_snapshot");
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_get_causet_cmd("default", b"k0")],
        false,
    );
    request.mut_header().set_peer(p3);
    request.mut_header().set_replica_read(true);
    // lightlike follower read request to peer 3
    let (cb1, rx1) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(3, request, cb1)
        .unwrap();
    let read_index_msg = read_index_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let snap_msg = snap_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    fail::causet("apply_snap_cleanup_cone", "pause").unwrap();

    let router = cluster.sim.wl().get_router(3).unwrap();
    fail::causet("pause_on_peer_collect_message", "pause").unwrap();
    cluster.sim.wl().clear_recv_filters(3);
    cluster.clear_lightlike_filters();
    router.lightlike_violetabft_message(snap_msg).unwrap();
    router.lightlike_violetabft_message(read_index_msg).unwrap();
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    fail::remove("pause_on_peer_collect_message");
    must_get_none(&cluster.get_engine(3), b"k0");
    // Should not receive resp
    rx1.recv_timeout(Duration::from_millis(500)).unwrap_err();
    fail::remove("apply_snap_cleanup_cone");
    rx1.recv_timeout(Duration::from_secs(5)).unwrap();
}

fn must_truncated_to(engine: Arc<DB>, brane_id: u64, index: u64) {
    for _ in 1..300 {
        let apply_state: VioletaBftApplyState = engine
            .c()
            .get_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(brane_id))
            .unwrap()
            .unwrap();
        let truncated_index = apply_state.get_truncated_state().get_index();
        if truncated_index > index {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("violetabft log is not truncated to {}", index);
}
