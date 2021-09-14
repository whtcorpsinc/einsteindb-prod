// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, lightlikeer};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use ekvproto::violetabft_server_timeshare::*;
use violetabft::evioletabft_timeshare::{Message, MessageType};

use engine_lmdb::Compat;
use edb::Peekable;
use violetabftstore::store::*;
use violetabftstore::Result;
use test_violetabftstore::*;
use violetabftstore::interlock::::config::*;
use violetabftstore::interlock::::HandyRwLock;

fn test_huge_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 1000;
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(10);
    cluster.causet.violetabft_store.snap_apply_batch_size = ReadableSize(500);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // at least 4m data
    for i in 0..2 * 1024 {
        let key = format!("{:01024}", i);
        let value = format!("{:01024}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, &format!("{:01024}", 0).into_bytes());
    // add peer (2,2) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    must_get_equal(&engine_2, key, value);

    // now snapshot must be applied on peer 2;
    let key = format!("{:01024}", 0);
    let value = format!("{:01024}", 0);
    must_get_equal(&engine_2, key.as_bytes(), value.as_bytes());
    let stale = Arc::new(AtomicBool::new(false));
    cluster.sim.wl().add_recv_filter(
        3,
        Box::new(LeadingDuplicatedSnapshotFilter::new(
            Arc::clone(&stale),
            false,
        )),
    );
    fidel_client.must_add_peer(r1, new_peer(3, 3));
    let mut i = 2 * 1024;
    loop {
        i += 1;
        let key = format!("{:01024}", i);
        let value = format!("{:01024}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
        if stale.load(Ordering::Relaxed) {
            break;
        }
        if i > 10 * 1024 {
            panic!("snapshot should be sent twice after {} kvs", i);
        }
    }
    cluster.must_put(b"k3", b"v3");
    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k3", b"v3");

    // TODO: add more tests.
}

#[test]
fn test_node_huge_snapshot() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_huge_snapshot(&mut cluster);
}

#[test]
fn test_server_huge_snapshot() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_huge_snapshot(&mut cluster);
}

#[test]
fn test_server_snap_gc() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    cluster.causet.violetabft_store.snap_gc_timeout = ReadableDuration::millis(300);

    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    let (tx, rx) = mpsc::channel();
    // drop all the snapshot so we can detect stale snapfile.
    cluster
        .sim
        .wl()
        .add_recv_filter(3, Box::new(DropSnapshotFilter::new(tx)));
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    let first_snap_idx = rx.recv_timeout(Duration::from_secs(3)).unwrap();

    cluster.must_put(b"k2", b"v2");

    // node 1 and node 2 must have k2, but node 3 must not.
    for i in 1..3 {
        let engine = cluster.get_engine(i);
        must_get_equal(&engine, b"k2", b"v2");
    }

    let engine3 = cluster.get_engine(3);
    must_get_none(&engine3, b"k2");

    for _ in 0..30 {
        // write many logs to force log GC for brane 1 and brane 2.
        // and trigger snapshot more than one time.
        cluster.must_put(b"k1", b"v1");
        cluster.must_put(b"k2", b"v2");
    }

    let mut now = Instant::now();
    loop {
        let snap_index = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        if snap_index != first_snap_idx {
            break;
        }
        if now.elapsed() >= Duration::from_secs(5) {
            panic!("can't get any snap after {}", first_snap_idx);
        }
    }

    let snap_dir = cluster.get_snap_dir(3);
    // it must have more than 2 snaps.
    let snapfiles: Vec<_> = fs::read_dir(snap_dir)
        .unwrap()
        .map(|p| p.unwrap().path())
        .collect();
    assert!(snapfiles.len() >= 2);

    cluster.sim.wl().clear_recv_filters(3);
    debug!("filters cleared.");

    // node 3 must have k1, k2.
    must_get_equal(&engine3, b"k1", b"v1");
    must_get_equal(&engine3, b"k2", b"v2");

    now = Instant::now();
    loop {
        let mut snap_files = vec![];
        for i in 1..4 {
            let snap_dir = cluster.get_snap_dir(i);
            // snapfiles should be gc.
            snap_files.extlightlike(fs::read_dir(snap_dir).unwrap().map(|p| p.unwrap().path()));
        }
        if snap_files.is_empty() {
            return;
        }
        if now.elapsed() > Duration::from_secs(10) {
            panic!("snap files is still not empty: {:?}", snap_files);
        }
        sleep_ms(20);
    }
}

/// A helper function for testing the handling of snapshot is correct
/// when there are multiple snapshots which have overlapped brane cones
/// arrive at the same violetabftstore.
fn test_concurrent_snap<T: Simulator>(cluster: &mut Cluster<T>) {
    // Disable violetabft log gc in this test case.
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::secs(60);

    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    // Force peer 2 to be followers all the way.
    cluster.add_lightlike_filter(CloneFilterFactory(
        BranePacketFilter::new(r1, 2)
            .msg_type(MessageType::MsgRequestVote)
            .direction(Direction::lightlike),
    ));
    cluster.must_transfer_leader(r1, new_peer(1, 1));
    cluster.must_put(b"k3", b"v3");
    // Pile up snapshots of overlapped brane cones and deliver them all at once.
    let (tx, rx) = mpsc::channel();
    cluster
        .sim
        .wl()
        .add_recv_filter(3, Box::new(CollectSnapshotFilter::new(tx)));
    fidel_client.must_add_peer(r1, new_peer(3, 3));
    let brane = cluster.get_brane(b"k1");
    // Ensure the snapshot of cone ("", "") is sent and piled in filter.
    if let Err(e) = rx.recv_timeout(Duration::from_secs(1)) {
        panic!("the snapshot is not sent before split, e: {:?}", e);
    }
    // Split the brane cone and then there should be another snapshot for the split cones.
    cluster.must_split(&brane, b"k2");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
    // Ensure the branes work after split.
    cluster.must_put(b"k11", b"v11");
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

#[test]
fn test_node_concurrent_snap() {
    let mut cluster = new_node_cluster(0, 3);
    test_concurrent_snap(&mut cluster);
}

#[test]
fn test_server_concurrent_snap() {
    let mut cluster = new_server_cluster(0, 3);
    test_concurrent_snap(&mut cluster);
}

fn test_causet_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    configure_for_snapshot(cluster);

    cluster.run();
    let causet = "dagger";
    cluster.must_put_causet(causet, b"k1", b"v1");
    cluster.must_put_causet(causet, b"k2", b"v2");
    let engine1 = cluster.get_engine(1);
    must_get_causet_equal(&engine1, causet, b"k1", b"v1");
    must_get_causet_equal(&engine1, causet, b"k2", b"v2");

    // Isolate node 1.
    cluster.add_lightlike_filter(IsolationFilterFactory::new(1));

    // Write some data to trigger snapshot.
    for i in 100..110 {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        cluster.must_put_causet(causet, key.as_bytes(), value.as_bytes());
    }

    cluster.must_delete_causet(causet, b"k2");

    // Add node 1 back.
    cluster.clear_lightlike_filters();

    // Now snapshot must be applied on node 1.
    must_get_causet_equal(&engine1, causet, b"k1", b"v1");
    must_get_causet_none(&engine1, causet, b"k2");

    // test if node can be safely respacelikeed without losing any data.
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();

    cluster.must_put_causet(causet, b"k3", b"v3");
    must_get_causet_equal(&engine1, causet, b"k3", b"v3");
}

#[test]
fn test_node_causet_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    test_causet_snapshot(&mut cluster);
}

#[test]
fn test_server_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    test_causet_snapshot(&mut cluster);
}

// replace content of all the snapshots with the first snapshot it received.
#[derive(Clone)]
struct StaleSnap {
    inner: Arc<StaleSnapInner>,
}

struct StaleSnapInner {
    first_snap: RwLock<Option<Message>>,
    sent: Mutex<lightlikeer<()>>,
}

impl Filter for StaleSnap {
    fn before(&self, msgs: &mut Vec<VioletaBftMessage>) -> Result<()> {
        let mut res = Vec::with_capacity(msgs.len());
        for mut m in msgs.drain(..) {
            if m.get_message().get_msg_type() == MessageType::MsgSnapshot
                && m.get_to_peer().get_store_id() == 3
            {
                if self.inner.first_snap.rl().is_none() {
                    *self.inner.first_snap.wl() = Some(m.take_message());
                    continue;
                } else {
                    let from = m.get_message().get_from();
                    let to = m.get_message().get_to();
                    m.set_message(self.inner.first_snap.rl().as_ref().unwrap().clone());
                    m.mut_message().set_from(from);
                    m.mut_message().set_to(to);
                    let _ = self.inner.sent.dagger().unwrap().lightlike(());
                }
            }
            res.push(m);
        }
        *msgs = res;
        check_messages(msgs)
    }
}

#[test]
fn test_node_stale_snap() {
    let mut cluster = new_node_cluster(0, 3);
    // disable compact log to make snapshot only be sent when peer is first added.
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 1000;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 1000;

    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    // add peer (2,2) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    let (tx, rx) = mpsc::channel();
    let filter = StaleSnap {
        inner: Arc::new(StaleSnapInner {
            first_snap: RwLock::default(),
            sent: Mutex::new(tx),
        }),
    };
    cluster.add_lightlike_filter(CloneFilterFactory(filter));
    fidel_client.must_add_peer(r1, new_peer(3, 3));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    fidel_client.must_remove_peer(r1, new_peer(3, 3));
    must_get_none(&cluster.get_engine(3), b"k2");
    fidel_client.must_add_peer(r1, new_peer(3, 4));

    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(2), b"k3", b"v3");
    rx.recv().unwrap();
    sleep_ms(2000);
    must_get_none(&cluster.get_engine(3), b"k3");
    cluster.clear_lightlike_filters();
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
}

/// Pause Snap and wait till first applightlike message arrives.
pub struct SnapshotApplightlikeFilter {
    stale: AtomicBool,
    plightlikeing_msg: Mutex<Vec<VioletaBftMessage>>,
    notifier: Mutex<lightlikeer<()>>,
}

impl SnapshotApplightlikeFilter {
    pub fn new(notifier: lightlikeer<()>) -> SnapshotApplightlikeFilter {
        SnapshotApplightlikeFilter {
            stale: AtomicBool::new(false),
            plightlikeing_msg: Mutex::new(vec![]),
            notifier: Mutex::new(notifier),
        }
    }
}

impl Filter for SnapshotApplightlikeFilter {
    fn before(&self, msgs: &mut Vec<VioletaBftMessage>) -> Result<()> {
        if self.stale.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut to_lightlike = vec![];
        let mut plightlikeing_msg = self.plightlikeing_msg.dagger().unwrap();
        let mut stale = false;
        for msg in msgs.drain(..) {
            let should_collect =
                !stale && msg.get_message().get_msg_type() == MessageType::MsgSnapshot;
            stale = !plightlikeing_msg.is_empty()
                && msg.get_message().get_msg_type() == MessageType::MsgApplightlike;
            if should_collect {
                plightlikeing_msg.push(msg);
                self.notifier.dagger().unwrap().lightlike(()).unwrap();
            } else {
                if stale {
                    to_lightlike.extlightlike(plightlikeing_msg.drain(..));
                }
                to_lightlike.push(msg);
            }
        }
        self.stale.store(stale, Ordering::SeqCst);
        msgs.extlightlike(to_lightlike);
        Ok(())
    }
}

fn test_snapshot_with_applightlike<T: Simulator>(cluster: &mut Cluster<T>) {
    configure_for_snapshot(cluster);

    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();
    cluster.run();

    // In case of removing leader, let's transfer leader to some node first.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fidel_client.must_remove_peer(1, new_peer(4, 4));

    let (tx, rx) = mpsc::channel();
    cluster
        .sim
        .wl()
        .add_recv_filter(4, Box::new(SnapshotApplightlikeFilter::new(tx)));
    fidel_client.add_peer(1, new_peer(4, 5));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    let engine4 = cluster.get_engine(4);
    must_get_equal(&engine4, b"k1", b"v1");
    must_get_equal(&engine4, b"k2", b"v2");
}

#[test]
fn test_node_snapshot_with_applightlike() {
    let mut cluster = new_node_cluster(0, 4);
    test_snapshot_with_applightlike(&mut cluster);
}

#[test]
fn test_server_snapshot_with_applightlike() {
    let mut cluster = new_server_cluster(0, 4);
    test_snapshot_with_applightlike(&mut cluster);
}

#[test]
fn test_request_snapshot_apply_repeatedly() {
    let mut cluster = new_node_cluster(0, 2);
    configure_for_request_snapshot(&mut cluster);

    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();
    let brane_id = cluster.run_conf_change();
    fidel_client.must_add_peer(brane_id, new_peer(2, 2));
    cluster.must_transfer_leader(brane_id, new_peer(2, 2));

    sleep_ms(200);
    // Install snapshot filter before requesting snapshot.
    let (tx, rx) = mpsc::channel();
    let notifier = Mutex::new(Some(tx));
    cluster.sim.wl().add_recv_filter(
        1,
        Box::new(RecvSnapshotFilter {
            notifier,
            brane_id,
        }),
    );
    cluster.must_request_snapshot(1, brane_id);
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    sleep_ms(200);
    let engine = cluster.get_violetabft_engine(1);
    let violetabft_key = tuplespaceInstanton::violetabft_state_key(brane_id);
    let violetabft_state: VioletaBftLocalState = engine.c().get_msg(&violetabft_key).unwrap().unwrap();
    assert!(
        violetabft_state.get_last_index() > VIOLETABFT_INIT_LOG_INDEX,
        "{:?}",
        violetabft_state
    );
}
