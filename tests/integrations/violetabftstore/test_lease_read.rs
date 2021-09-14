// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

//! A module contains test cases for lease read on VioletaBft leader.

use std::sync::atomic::*;
use std::sync::{mpsc, Arc, Mutex};
use std::time::*;
use std::{mem, thread};

use ekvproto::meta_timeshare;
use ekvproto::violetabft_server_timeshare::VioletaBftLocalState;
use violetabft::evioletabft_timeshare::{ConfChangeType, MessageType};

use engine_lmdb::{Compat, LmdbSnapshot};
use edb::Peekable;
use fidel_client::FidelClient;
use violetabftstore::store::{Callback, BraneSnapshot};
use test_violetabftstore::*;
use violetabftstore::interlock::::config::*;
use violetabftstore::interlock::::HandyRwLock;

// A helper function for testing the lease reads and lease renewing.
// The leader keeps a record of its leader lease, and uses the system's
// monotonic raw clocktime to check whether its lease has expired.
// If the leader lease has not expired, when the leader receives a read request
//   1. with `read_quorum == false`, the leader will serve it by reading local data.
//      This way of handling request is called "lease read".
//   2. with `read_quorum == true`, the leader will serve it by doing index read (see violetabft's doc).
//      This way of handling request is called "index read".
// If the leader lease has expired, leader will serve both kinds of requests by index read, and
// propose an no-op entry to violetabft quorum to renew the lease.
// No matter what status the leader lease is, a write request is always served by writing a VioletaBft
// log to the VioletaBft quorum. It is called "consistent write". All writes are consistent writes.
// Every time the leader performs a consistent read/write, it will try to renew its lease.
fn test_renew_lease<T: Simulator>(cluster: &mut Cluster<T>) {
    // Avoid triggering the log compaction in this test case.
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 100;
    // Increase the VioletaBft tick interval to make this test case running reliably.
    // Use large election timeout to make leadership sBlock.
    configure_for_lease_read(cluster, Some(50), Some(10_000));
    // Override max leader lease to 2 seconds.
    let max_lease = Duration::from_secs(2);
    cluster.causet.violetabft_store.violetabft_store_max_leader_lease = ReadableDuration(max_lease);

    let node_id = 1u64;
    let store_id = 1u64;
    let peer = new_peer(store_id, node_id);
    cluster.fidel_client.disable_default_operator();
    let brane_id = cluster.run_conf_change();

    let key = b"k";
    cluster.must_put(key, b"v0");
    for id in 2..=cluster.engines.len() as u64 {
        cluster.fidel_client.must_add_peer(brane_id, new_peer(id, id));
        must_get_equal(&cluster.get_engine(id), key, b"v0");
    }

    // Write the initial value for a key.
    let key = b"k";
    cluster.must_put(key, b"v1");
    // Force `peer` to become leader.
    let brane = cluster.get_brane(key);
    let brane_id = brane.get_id();
    cluster.must_transfer_leader(brane_id, peer.clone());
    let engine = cluster.get_violetabft_engine(store_id);
    let state_key = tuplespaceInstanton::violetabft_state_key(brane_id);
    let state: VioletaBftLocalState = engine.c().get_msg(&state_key).unwrap().unwrap();
    let last_index = state.get_last_index();

    let detector = LeaseReadFilter::default();
    cluster.add_lightlike_filter(CloneFilterFactory(detector.clone()));

    // Issue a read request and check the value on response.
    must_read_on_peer(cluster, peer.clone(), brane.clone(), key, b"v1");
    assert_eq!(detector.ctx.rl().len(), 0);

    let mut expect_lease_read = 0;

    if cluster.engines.len() > 1 {
        // Wait for the leader lease to expire.
        thread::sleep(max_lease);

        // Issue a read request and check the value on response.
        must_read_on_peer(cluster, peer.clone(), brane.clone(), key, b"v1");

        // Check if the leader does a index read and renewed its lease.
        assert_eq!(cluster.leader_of_brane(brane_id), Some(peer.clone()));
        expect_lease_read += 1;
        assert_eq!(detector.ctx.rl().len(), expect_lease_read);
    }

    // Wait for the leader lease to expire.
    thread::sleep(max_lease);

    // Issue a write request.
    cluster.must_put(key, b"v2");

    // Check if the leader has renewed its lease so that it can do lease read.
    assert_eq!(cluster.leader_of_brane(brane_id), Some(peer.clone()));
    let state: VioletaBftLocalState = engine.c().get_msg(&state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index + 1);

    // Issue a read request and check the value on response.
    must_read_on_peer(cluster, peer, brane, key, b"v2");

    // Check if the leader does a local read.
    assert_eq!(detector.ctx.rl().len(), expect_lease_read);
}

#[test]
fn test_one_node_renew_lease() {
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    test_renew_lease(&mut cluster);
}

#[test]
fn test_node_renew_lease() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_renew_lease(&mut cluster);
}

// A helper function for testing the lease reads when the lease has expired.
// If the leader lease has expired, there may be new leader elected and
// the old leader will fail to renew its lease.
fn test_lease_expired<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    // Avoid triggering the log compaction in this test case.
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 100;
    // Increase the VioletaBft tick interval to make this test case running reliably.
    let election_timeout = configure_for_lease_read(cluster, Some(50), None);

    let node_id = 3u64;
    let store_id = 3u64;
    let peer = new_peer(store_id, node_id);
    cluster.run();

    // Write the initial value for a key.
    let key = b"k";
    cluster.must_put(key, b"v1");
    // Force `peer` to become leader.
    let brane = cluster.get_brane(key);
    let brane_id = brane.get_id();
    cluster.must_transfer_leader(brane_id, peer.clone());

    // Isolate the leader `peer` from other peers.
    cluster.add_lightlike_filter(IsolationFilterFactory::new(store_id));

    // Wait for the leader lease to expire and a new leader is elected.
    thread::sleep(election_timeout * 2);

    // Issue a read request and check the value on response.
    must_error_read_on_peer(cluster, peer, brane, key, Duration::from_secs(1));
}

#[test]
fn test_node_lease_expired() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_lease_expired(&mut cluster);
}

// A helper function for testing the leader holds unsafe lease during the leader transfer
// procedure, so it will not do lease read.
// Since violetabft will not propose any request during leader transfer procedure, consistent read/write
// could not be performed neither.
// When leader transfer procedure aborts later, the leader would use and fidelio the lease as usual.
fn test_lease_unsafe_during_leader_transfers<T: Simulator>(cluster: &mut Cluster<T>) {
    // Avoid triggering the log compaction in this test case.
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 100;
    // Increase the VioletaBft tick interval to make this test case running reliably.
    let election_timeout = configure_for_lease_read(cluster, Some(500), None);

    let store_id = 1u64;
    let peer = new_peer(store_id, 1);
    let peer3_store_id = 3u64;
    let peer3 = new_peer(peer3_store_id, 3);

    cluster.fidel_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k0", b"v0");
    cluster.fidel_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k0", b"v0");
    cluster.fidel_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    let detector = LeaseReadFilter::default();
    cluster.add_lightlike_filter(CloneFilterFactory(detector.clone()));

    // write the initial value for a key.
    let key = b"k";
    cluster.must_put(key, b"v1");
    // Force `peer1` to became leader.
    let brane = cluster.get_brane(key);
    let brane_id = brane.get_id();
    cluster.must_transfer_leader(brane_id, peer.clone());

    // Issue a read request and check the value on response.
    must_read_on_peer(cluster, peer.clone(), brane.clone(), key, b"v1");

    let engine = cluster.get_violetabft_engine(store_id);
    let state_key = tuplespaceInstanton::violetabft_state_key(brane_id);
    let state: VioletaBftLocalState = engine.c().get_msg(&state_key).unwrap().unwrap();
    let last_index = state.get_last_index();

    // Check if the leader does a local read.
    must_read_on_peer(cluster, peer.clone(), brane.clone(), key, b"v1");
    let state: VioletaBftLocalState = engine.c().get_msg(&state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index);
    assert_eq!(detector.ctx.rl().len(), 0);

    // Ensure peer 3 is ready to transfer leader.
    must_get_equal(&cluster.get_engine(3), key, b"v1");

    // Drop MsgTimeoutNow to `peer3` so that the leader transfer procedure would abort later.
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(brane_id, peer3_store_id)
            .msg_type(MessageType::MsgTimeoutNow)
            .direction(Direction::Recv),
    ));

    // Issue a transfer leader request to transfer leader from `peer` to `peer3`.
    cluster.transfer_leader(brane_id, peer3);

    // Delay a while to ensure transfer leader procedure is triggered inside violetabft module.
    thread::sleep(election_timeout / 2);

    // Issue a read request and it will fall back to read index.
    must_read_on_peer(cluster, peer.clone(), brane.clone(), key, b"v1");
    assert_eq!(detector.ctx.rl().len(), 1);

    // And read index should not fidelio lease.
    must_read_on_peer(cluster, peer.clone(), brane.clone(), key, b"v1");
    assert_eq!(detector.ctx.rl().len(), 2);

    // Make sure the leader transfer procedure timeouts.
    thread::sleep(election_timeout * 2);

    // Then the leader transfer procedure aborts, now the leader could do lease read or consistent
    // read/write and renew/reuse the lease as usual.

    // Issue a read request and check the value on response.
    must_read_on_peer(cluster, peer.clone(), brane.clone(), key, b"v1");
    assert_eq!(detector.ctx.rl().len(), 3);

    // Check if the leader also propose an entry to renew its lease.
    let state: VioletaBftLocalState = engine.c().get_msg(&state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index + 1);

    // wait some time for the proposal to be applied.
    thread::sleep(election_timeout / 2);

    // Check if the leader does a local read.
    must_read_on_peer(cluster, peer, brane, key, b"v1");
    let state: VioletaBftLocalState = engine.c().get_msg(&state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index + 1);
    assert_eq!(detector.ctx.rl().len(), 3);
}

#[test]
fn test_node_lease_unsafe_during_leader_transfers() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_lease_unsafe_during_leader_transfers(&mut cluster);
}

#[test]
fn test_node_batch_id_in_lease() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_batch_id_in_lease(&mut cluster);
}

fn test_batch_id_in_lease<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer number check.
    fidel_client.disable_default_operator();

    // Avoid triggering the log compaction in this test case.
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 100;

    // Increase the VioletaBft tick interval to make this test case running reliably.
    let election_timeout = configure_for_lease_read(cluster, Some(100), None);
    cluster.run();

    let (split_key1, split_key2) = (b"k22", b"k44");
    let tuplespaceInstanton = vec![b"k11", b"k33", b"k55"];
    let _ = tuplespaceInstanton.iter().map(|key| {
        cluster.must_put(*key, b"v1");
    });

    let brane = fidel_client.get_brane(tuplespaceInstanton[0]).unwrap();
    cluster.must_split(&brane, split_key1);
    let brane = fidel_client.get_brane(tuplespaceInstanton[1]).unwrap();
    cluster.must_split(&brane, split_key2);
    let mut peers = vec![];

    // Transfer leader together to batch snapshot
    for i in 0..3 {
        let r = fidel_client.get_brane(tuplespaceInstanton[i]).unwrap();
        let peer = cluster.leader_of_brane(r.get_id()).unwrap();
        if peer.get_store_id() != 1 {
            for p in r.get_peers() {
                if p.get_store_id() == 1 {
                    cluster.must_transfer_leader(r.get_id(), p.clone());
                    let peer = cluster.leader_of_brane(r.get_id()).unwrap();
                    assert_eq!(peer.get_store_id(), 1);
                    peers.push(peer);
                    break;
                }
            }
        } else {
            peers.push(peer.clone());
        }
    }
    // Sleep to make sure lease expired
    thread::sleep(election_timeout + Duration::from_millis(200));

    // lightlike request to brane 0 and 1 to renew their lease.
    cluster.must_put(b"k11", b"v2");
    cluster.must_put(b"k33", b"v2");
    assert_eq!(b"v2".to_vec(), cluster.must_get(b"k33").unwrap());
    assert_eq!(b"v2".to_vec(), cluster.must_get(b"k11").unwrap());

    let branes: Vec<_> = tuplespaceInstanton
        .into_iter()
        .map(|key| fidel_client.get_brane(key).unwrap())
        .collect();

    let requests: Vec<(meta_timeshare::Peer, meta_timeshare::Brane)> = peers
        .iter()
        .zip(branes)
        .map(|(p, r)| (p.clone(), r))
        .collect();
    let responses = batch_read_on_peer(cluster, &requests);
    let snaps: Vec<BraneSnapshot<LmdbSnapshot>> = responses
        .into_iter()
        .map(|response| {
            assert!(!response.response.get_header().has_error());
            response.snapshot.unwrap()
        })
        .collect();

    // Snapshot 0 and 1 will use one LmdbSnapshot because we have renew their lease.
    assert!(std::ptr::eq(
        snaps[0].get_snapshot(),
        snaps[1].get_snapshot()
    ));
    assert!(!std::ptr::eq(
        snaps[0].get_snapshot(),
        snaps[2].get_snapshot()
    ));

    // make sure that brane 2 could renew lease.
    cluster.must_put(b"k55", b"v2");
    let responses = batch_read_on_peer(cluster, &requests);
    let snaps2: Vec<BraneSnapshot<LmdbSnapshot>> = responses
        .into_iter()
        .map(|response| {
            assert!(!response.response.get_header().has_error());
            response.snapshot.unwrap()
        })
        .collect();
    assert_eq!(3, snaps2.len());
    assert!(!std::ptr::eq(
        snaps[0].get_snapshot(),
        snaps2[0].get_snapshot()
    ));
    assert!(std::ptr::eq(
        snaps2[0].get_snapshot(),
        snaps2[1].get_snapshot()
    ));
    assert!(std::ptr::eq(
        snaps2[0].get_snapshot(),
        snaps2[2].get_snapshot()
    ));
}

/// test whether the read index callback will be handled when a brane is destroyed.
/// If it's not handled properly, it will cause dead dagger in transaction interlock_semaphore.
#[test]
fn test_node_callback_when_destroyed() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster, None, Some(50));
    cluster.run();
    cluster.must_put(b"k1", b"v1");
    let leader = cluster.leader_of_brane(1).unwrap();
    let cc = new_change_peer_request(ConfChangeType::RemoveNode, leader.clone());
    let epoch = cluster.get_brane_epoch(1);
    let req = new_admin_request(1, &epoch, cc);
    // so the leader can't commit the conf change yet.
    let block = Arc::new(AtomicBool::new(true));
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(1, leader.get_store_id())
            .msg_type(MessageType::MsgApplightlikeResponse)
            .direction(Direction::Recv)
            .when(Arc::clone(&block)),
    ));
    let mut filter = LeaseReadFilter::default();
    filter.take = true;
    // so the leader can't perform read index.
    cluster.add_lightlike_filter(CloneFilterFactory(filter.clone()));
    // it always timeout, no need to wait.
    let _ = cluster.call_command_on_leader(req, Duration::from_millis(500));

    // To make sure `get` is handled before destroy leader, we must issue
    // `get` then unblock applightlike responses.
    let leader_node_id = leader.get_store_id();
    let get = new_get_cmd(b"k1");
    let mut req = new_request(1, epoch, vec![get], true);
    req.mut_header().set_peer(leader);
    let (cb, rx) = make_cb(&req);
    cluster
        .sim
        .rl()
        .async_command_on_node(leader_node_id, req, cb)
        .unwrap();
    // Unblock applightlike responses after we issue the req.
    block.store(false, Ordering::SeqCst);
    let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();

    assert!(
        !filter.ctx.rl().is_empty(),
        "read index should be performed"
    );
    assert!(
        resp.get_header().get_error().has_brane_not_found(),
        "{:?}",
        resp
    );
}

/// Test if the callback proposed by read index is cleared correctly.
#[test]
fn test_lease_read_callback_destroy() {
    // Only server cluster can fake lightlikeing message successfully in violetabftstore layer.
    let mut cluster = new_server_cluster(0, 3);
    // Increase the VioletaBft tick interval to make this test case running reliably.
    let election_timeout = configure_for_lease_read(&mut cluster, Some(50), None);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    // Isolate the target peer to make transfer leader fail.
    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    cluster.transfer_leader(1, new_peer(3, 3));
    thread::sleep(election_timeout * 2);
    // Trigger ReadIndex on the leader.
    assert_eq!(cluster.must_get(b"k1"), Some(b"v1".to_vec()));
    cluster.must_put(b"k2", b"v2");
}

/// A read index request will be applightlikeed to waiting list when there is an on-going request
/// to reduce heartbeat messages. But when leader is in suspect lease, requests should not
/// be batched because lease can be expired at anytime.
#[test]
fn test_read_index_stale_in_suspect_lease() {
    let mut cluster = new_node_cluster(0, 3);

    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let max_lease = Duration::from_secs(2);
    cluster.causet.violetabft_store.violetabft_store_max_leader_lease = ReadableDuration(max_lease);
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 5;

    cluster.fidel_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.fidel_client.must_add_peer(r1, new_peer(2, 2));
    cluster.fidel_client.must_add_peer(r1, new_peer(3, 3));

    let r1 = cluster.get_brane(b"k1");
    // Put and test again to ensure that peer 3 get the latest writes by message applightlike
    // instead of snapshot, so that transfer leader to peer 3 can 100% success.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    // Ensure peer 3 is ready to become leader.
    let rx = async_read_on_peer(&mut cluster, new_peer(3, 3), r1.clone(), b"k2", true, true);
    let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(
        resp.get_responses()[0].get_get().get_value(),
        b"v2",
        "{:?}",
        resp
    );
    let old_leader = cluster.leader_of_brane(r1.get_id()).unwrap();

    // Use a macro instead of a closure to avoid any capture of local variables.
    macro_rules! read_on_old_leader {
        () => {{
            let (tx, rx) = mpsc::sync_channel(1);
            let mut read_request = new_request(
                r1.get_id(),
                r1.get_brane_epoch().clone(),
                vec![new_get_cmd(b"k1")],
                true, // read quorum
            );
            read_request.mut_header().set_peer(new_peer(1, 1));
            let sim = cluster.sim.wl();
            sim.async_command_on_node(
                old_leader.get_id(),
                read_request,
                Callback::Read(Box::new(move |resp| tx.lightlike(resp.response).unwrap())),
            )
            .unwrap();
            rx
        }};
    }

    // Delay all violetabft messages to peer 1.
    let dropped_msgs = Arc::new(Mutex::new(Vec::new()));
    let filter = Box::new(
        BranePacketFilter::new(r1.id, old_leader.store_id)
            .direction(Direction::Recv)
            .skip(MessageType::MsgTransferLeader)
            .reserve_dropped(Arc::clone(&dropped_msgs)),
    );
    cluster
        .sim
        .wl()
        .add_recv_filter(old_leader.get_id(), filter);

    let resp1 = read_on_old_leader!();

    cluster.must_transfer_leader(r1.get_id(), new_peer(3, 3));

    let resp2 = read_on_old_leader!();

    // Unpark all plightlikeing messages and clear all filters.
    let router = cluster.sim.wl().get_router(old_leader.get_id()).unwrap();
    'LOOP: loop {
        for violetabft_msg in mem::replace(dropped_msgs.dagger().unwrap().as_mut(), vec![]) {
            let msg_type = violetabft_msg.get_message().get_msg_type();
            if msg_type == MessageType::MsgHeartbeatResponse {
                router.lightlike_violetabft_message(violetabft_msg).unwrap();
                continue;
            }
            cluster.sim.wl().clear_recv_filters(old_leader.get_id());
            break 'LOOP;
        }
    }

    let resp1 = resp1.recv().unwrap();
    assert!(
        resp1.get_header().get_error().has_stale_command()
            || resp1.get_responses()[0].get_get().get_value() == b"v1"
    );

    // Response 2 should contains an error.
    let resp2 = resp2.recv().unwrap();
    assert!(resp2.get_header().get_error().has_stale_command());
    drop(cluster);
}

#[test]
fn test_local_read_cache() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(50), None);
    cluster.fidel_client.disable_default_operator();
    cluster.run();
    let fidel_client = Arc::clone(&cluster.fidel_client);

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let r1 = cluster.get_brane(b"k1");
    let leader = cluster.leader_of_brane(r1.get_id()).unwrap();
    let new_leader = new_peer((leader.get_id() + 1) % 3 + 1, (leader.get_id() + 1) % 3 + 1);
    cluster.must_transfer_leader(r1.get_id(), new_leader);

    // Add the peer back and make sure it catches up latest logs.
    fidel_client.must_remove_peer(r1.get_id(), leader.clone());
    let replace_peer = new_peer(leader.get_store_id(), 10000);
    fidel_client.must_add_peer(r1.get_id(), replace_peer.clone());
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(leader.get_store_id()), b"k2", b"v2");

    cluster.must_transfer_leader(r1.get_id(), replace_peer);
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(leader.get_store_id()), b"k3", b"v3");
}

/// Test latency changes when a leader becomes follower right after it receives
/// read_index heartbeat response.
#[test]
fn test_not_leader_read_lease() {
    let mut cluster = new_node_cluster(0, 3);
    // Avoid triggering the log compaction in this test case.
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 100;
    // Increase the VioletaBft tick interval to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(50), None);
    let heartbeat_interval = cluster.causet.violetabft_store.violetabft_heartbeat_interval();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");

    // Add a filter to delay heartbeat response until transfer leader begins.
    cluster.sim.wl().add_recv_filter(
        1,
        Box::new(LeadingFilter::new(
            MessageType::MsgHeartbeatResponse,
            MessageType::MsgRequestVote,
        )),
    );
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    ));

    let mut brane = cluster.get_brane(b"k1");
    let brane_id = brane.get_id();
    let mut req = new_request(
        brane_id,
        brane.take_brane_epoch(),
        vec![new_get_cmd(b"k2")],
        true,
    );
    req.mut_header().set_peer(new_peer(1, 1));
    let (cb, rx) = make_cb(&req);
    cluster.sim.rl().async_command_on_node(1, req, cb).unwrap();

    cluster.must_transfer_leader(brane_id, new_peer(3, 3));
    // Even the leader steps down, it should respond to read index in time.
    rx.recv_timeout(heartbeat_interval).unwrap();
}

/// Test whether read index is greater than applied index.
/// 1. Add hearbeat msg filter.
/// 2. Propose a read index request.
/// 3. Put a key and get the latest applied index.
/// 4. Propose another read index request.
/// 5. Remove the filter and check whether the latter read index is greater than applied index.
///
/// In previous implementation, these two read index request will be batched and
/// will get the same read index which breaks the correctness because the latter one
/// is proposed after the applied index has increased and replied to client.
#[test]
fn test_read_index_after_write() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(10));
    let heartbeat_interval = cluster.causet.violetabft_store.violetabft_heartbeat_interval();
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    let brane = fidel_client.get_brane(b"k1").unwrap();
    let brane_on_store1 = find_peer(&brane, 1).unwrap().to_owned();
    cluster.must_transfer_leader(brane.get_id(), brane_on_store1.clone());

    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    // Add heartbeat msg filter to prevent the leader to reply the read index response.
    let filter = Box::new(
        BranePacketFilter::new(brane.get_id(), 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgHeartbeat),
    );
    cluster.sim.wl().add_recv_filter(2, filter);

    let mut req = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_read_index_cmd()],
        true,
    );
    req.mut_header()
        .set_peer(new_peer(1, brane_on_store1.get_id()));
    // Don't care about the first one's read index
    let (cb, _) = make_cb(&req);
    cluster.sim.rl().async_command_on_node(1, req, cb).unwrap();

    cluster.must_put(b"k2", b"v2");
    let applied_index = cluster.apply_state(brane.get_id(), 1).get_applied_index();

    let mut req = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_read_index_cmd()],
        true,
    );
    req.mut_header()
        .set_peer(new_peer(1, brane_on_store1.get_id()));
    let (cb, rx) = make_cb(&req);
    cluster.sim.rl().async_command_on_node(1, req, cb).unwrap();

    cluster.sim.wl().clear_recv_filters(2);

    let response = rx.recv_timeout(heartbeat_interval).unwrap();
    assert!(
        response.get_responses()[0]
            .get_read_index()
            .get_read_index()
            >= applied_index
    );
}
