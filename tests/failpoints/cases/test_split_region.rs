//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use edb::Causet_WRITE;
use ekvproto::meta_timeshare::Brane;
use ekvproto::violetabft_server_timeshare::VioletaBftMessage;
use fidel_client::FidelClient;
use violetabft::evioletabft_timeshare::MessageType;
use violetabftstore::store::util::is_vote_msg;
use violetabftstore::Result;
use violetabftstore::interlock::::HandyRwLock;

use test_violetabftstore::*;
use violetabftstore::interlock::::config::{ReadableDuration, ReadableSize};

#[test]
fn test_follower_slow_split() {
    let mut cluster = new_node_cluster(0, 3);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();
    cluster.run();
    let brane = cluster.get_brane(b"");

    // Only need peer 1 and 3. Stop node 2 to avoid extra vote messages.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fidel_client.must_remove_peer(1, new_peer(2, 2));
    cluster.stop_node(2);

    // Use a channel to retrieve spacelike_key and lightlike_key in pre-vote messages.
    let (cone_tx, cone_rx) = mpsc::channel();
    let prevote_filter = PrevoteConeFilter {
        // Only lightlike 1 pre-vote message to peer 3 so if peer 3 drops it,
        // it needs to spacelike a new election.
        filter: BranePacketFilter::new(1000, 1) // new brane id is 1000
            .msg_type(MessageType::MsgRequestPreVote)
            .direction(Direction::lightlike)
            .allow(1),
        before: Some(Mutex::new(cone_tx)),
        after: None,
    };
    cluster
        .sim
        .wl()
        .add_lightlike_filter(1, Box::new(prevote_filter));

    // Ensure pre-vote response is really lightlikeed.
    let (tx, rx) = mpsc::channel();
    let prevote_resp_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx,
        Arc::from(AtomicBool::new(true)),
    ));
    cluster.sim.wl().add_lightlike_filter(3, prevote_resp_notifier);

    // After split, pre-vote message should be sent to peer 2.
    fail::causet("apply_before_split_1_3", "pause").unwrap();
    cluster.must_split(&brane, b"k2");
    let cone = cone_rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_eq!(cone.0, b"");
    assert_eq!(cone.1, b"k2");

    // After the follower split success, it will response to the plightlikeing vote.
    fail::causet("apply_before_split_1_3", "off").unwrap();
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_ok());
}

#[test]
fn test_split_lost_request_vote() {
    let mut cluster = new_node_cluster(0, 3);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();
    cluster.run();
    let brane = cluster.get_brane(b"");

    // Only need peer 1 and 3. Stop node 2 to avoid extra vote messages.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fidel_client.must_remove_peer(1, new_peer(2, 2));
    cluster.stop_node(2);

    // Use a channel to retrieve spacelike_key and lightlike_key in pre-vote messages.
    let (cone_tx, cone_rx) = mpsc::channel();
    let (after_sent_tx, after_sent_rx) = mpsc::channel();
    let prevote_filter = PrevoteConeFilter {
        // Only lightlike 1 pre-vote message to peer 3 so if peer 3 drops it,
        // it needs to spacelike a new election.
        filter: BranePacketFilter::new(1000, 1) // new brane id is 1000
            .msg_type(MessageType::MsgRequestPreVote)
            .direction(Direction::lightlike)
            .allow(1),
        before: Some(Mutex::new(cone_tx)),
        after: Some(Mutex::new(after_sent_tx)),
    };
    cluster
        .sim
        .wl()
        .add_lightlike_filter(1, Box::new(prevote_filter));

    // Ensure pre-vote response is really sent.
    let (tx, rx) = mpsc::channel();
    let prevote_resp_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx,
        Arc::from(AtomicBool::new(true)),
    ));
    cluster.sim.wl().add_lightlike_filter(3, prevote_resp_notifier);

    // After split, pre-vote message should be sent to peer 3.
    fail::causet("apply_after_split_1_3", "pause").unwrap();
    cluster.must_split(&brane, b"k2");
    let cone = cone_rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_eq!(cone.0, b"");
    assert_eq!(cone.1, b"k2");

    // Make sure the message has sent to peer 3.
    let _sent = after_sent_rx
        .recv_timeout(Duration::from_millis(100))
        .unwrap();

    // Make sure pre-vote is handled.
    let new_brane = cluster.fidel_client.get_brane(b"").unwrap();
    let plightlikeing_create_peer = new_brane
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 3)
        .unwrap()
        .to_owned();
    let _ = read_on_peer(
        &mut cluster,
        plightlikeing_create_peer,
        brane,
        b"k1",
        false,
        Duration::from_millis(100),
    );

    // Make sure pre-vote is cached in plightlikeing votes.
    {
        let store_meta = cluster.store_metas.get(&3).unwrap();
        let meta = store_meta.dagger().unwrap();
        assert!(meta.plightlikeing_votes.iter().any(|m| {
            m.brane_id == new_brane.id
                && violetabftstore::store::util::is_first_vote_msg(m.get_message())
        }));
    }

    // After the follower split success, it will response to the plightlikeing vote.
    fail::causet("apply_after_split_1_3", "off").unwrap();
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_ok());
}

fn gen_split_brane() -> (Brane, Brane, Brane) {
    let mut cluster = new_server_cluster(0, 2);
    let brane_max_size = 50000;
    let brane_split_size = 30000;
    cluster.causet.violetabft_store.split_brane_check_tick_interval = ReadableDuration::millis(20);
    cluster.causet.interlock.brane_max_size = ReadableSize(brane_max_size);
    cluster.causet.interlock.brane_split_size = ReadableSize(brane_split_size);

    let mut cone = 1..;
    cluster.run();
    let fidel_client = Arc::clone(&cluster.fidel_client);
    let brane = fidel_client.get_brane(b"").unwrap();
    let last_key = put_till_size(&mut cluster, brane_split_size, &mut cone);
    let target = fidel_client.get_brane(&last_key).unwrap();

    assert_eq!(brane, target);

    let max_key = put_causet_till_size(&mut cluster, Causet_WRITE, brane_max_size, &mut cone);

    let left = fidel_client.get_brane(b"").unwrap();
    let right = fidel_client.get_brane(&max_key).unwrap();
    if left == right {
        cluster.wait_brane_split_max_cnt(&brane, 20, 10, false);
    }

    let left = fidel_client.get_brane(b"").unwrap();
    let right = fidel_client.get_brane(&max_key).unwrap();

    (brane, left, right)
}

#[test]
fn test_pause_split_when_snap_gen_will_split() {
    let is_generating_snapshot = "is_generating_snapshot";
    fail::causet(is_generating_snapshot, "return()").unwrap();

    let (brane, left, right) = gen_split_brane();

    assert_ne!(left, right);
    assert_eq!(brane.get_spacelike_key(), left.get_spacelike_key());
    assert_eq!(brane.get_lightlike_key(), right.get_lightlike_key());

    fail::remove(is_generating_snapshot);
}

#[test]
fn test_pause_split_when_snap_gen_never_split() {
    let is_generating_snapshot = "is_generating_snapshot";
    let brane_split_skip_max_count = "brane_split_skip_max_count";
    fail::causet(brane_split_skip_max_count, "return()").unwrap();
    fail::causet(is_generating_snapshot, "return()").unwrap();

    let (brane, left, right) = gen_split_brane();

    assert_eq!(brane, left);
    assert_eq!(left, right);

    fail::remove(is_generating_snapshot);
    fail::remove(brane_split_skip_max_count);
}

type Filterlightlikeer<T> = Mutex<mpsc::lightlikeer<T>>;

// Filter prevote message and record the cone.
struct PrevoteConeFilter {
    filter: BranePacketFilter,
    before: Option<Filterlightlikeer<(Vec<u8>, Vec<u8>)>>,
    after: Option<Filterlightlikeer<()>>,
}

impl Filter for PrevoteConeFilter {
    fn before(&self, msgs: &mut Vec<VioletaBftMessage>) -> Result<()> {
        self.filter.before(msgs)?;
        if let Some(msg) = msgs.iter().filter(|m| is_vote_msg(m.get_message())).last() {
            let spacelike_key = msg.get_spacelike_key().to_owned();
            let lightlike_key = msg.get_lightlike_key().to_owned();
            if let Some(before) = self.before.as_ref() {
                let tx = before.dagger().unwrap();
                let _ = tx.lightlike((spacelike_key, lightlike_key));
            }
        }
        Ok(())
    }
    fn after(&self, _: Result<()>) -> Result<()> {
        if let Some(after) = self.after.as_ref() {
            let tx = after.dagger().unwrap();
            let _ = tx.lightlike(());
        }
        Ok(())
    }
}

// Test if a peer is created from splitting when another initialized peer with the same
// brane id has already existed. In previous implementation, it can be created and panic
// will happen because there are two initialized peer with the same brane id.
#[test]
fn test_split_not_to_split_exist_brane() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.right_derive_when_split = true;
    cluster.causet.violetabft_store.apply_batch_system.max_batch_size = 1;
    cluster.causet.violetabft_store.apply_batch_system.pool_size = 2;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    let mut brane_a = fidel_client.get_brane(b"k1").unwrap();
    // [-∞, k2), [k2, +∞)
    //    b         a
    cluster.must_split(&brane_a, b"k2");

    cluster.put(b"k0", b"v0").unwrap();
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    let brane_b = fidel_client.get_brane(b"k0").unwrap();
    let peer_b_1 = find_peer(&brane_b, 1).cloned().unwrap();
    cluster.must_transfer_leader(brane_b.get_id(), peer_b_1);

    let peer_b_3 = find_peer(&brane_b, 3).cloned().unwrap();
    assert_eq!(peer_b_3.get_id(), 1003);
    let on_handle_apply_1003_fp = "on_handle_apply_1003";
    fail::causet(on_handle_apply_1003_fp, "pause").unwrap();
    // [-∞, k1), [k1, k2), [k2, +∞)
    //    c         b          a
    cluster.must_split(&brane_b, b"k1");

    fidel_client.must_remove_peer(brane_b.get_id(), peer_b_3);
    fidel_client.must_add_peer(brane_b.get_id(), new_peer(4, 4));

    let mut brane_c = fidel_client.get_brane(b"k0").unwrap();
    let peer_c_3 = find_peer(&brane_c, 3).cloned().unwrap();
    fidel_client.must_remove_peer(brane_c.get_id(), peer_c_3);
    fidel_client.must_add_peer(brane_c.get_id(), new_peer(4, 5));
    // [-∞, k2), [k2, +∞)
    //     c        a
    fidel_client.must_merge(brane_b.get_id(), brane_c.get_id());

    brane_a = fidel_client.get_brane(b"k2").unwrap();
    let peer_a_3 = find_peer(&brane_a, 3).cloned().unwrap();
    fidel_client.must_remove_peer(brane_a.get_id(), peer_a_3);
    fidel_client.must_add_peer(brane_a.get_id(), new_peer(4, 6));
    // [-∞, +∞)
    //    c
    fidel_client.must_merge(brane_a.get_id(), brane_c.get_id());

    brane_c = fidel_client.get_brane(b"k1").unwrap();
    // [-∞, k2), [k2, +∞)
    //     d        c
    cluster.must_split(&brane_c, b"k2");

    let peer_c_4 = find_peer(&brane_c, 4).cloned().unwrap();
    fidel_client.must_remove_peer(brane_c.get_id(), peer_c_4);
    fidel_client.must_add_peer(brane_c.get_id(), new_peer(3, 7));

    cluster.put(b"k2", b"v2").unwrap();
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");

    fail::remove(on_handle_apply_1003_fp);

    // If peer_c_3 is created, `must_get_none` will fail.
    must_get_none(&cluster.get_engine(3), b"k0");
}

// Test if a peer is created from splitting when another initialized peer with the same
// brane id existed before and has been destroyed now.
#[test]
fn test_split_not_to_split_exist_tombstone_brane() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.causet.violetabft_store.right_derive_when_split = true;
    cluster.causet.violetabft_store.store_batch_system.max_batch_size = 1;
    cluster.causet.violetabft_store.store_batch_system.pool_size = 2;
    cluster.causet.violetabft_store.apply_batch_system.max_batch_size = 1;
    cluster.causet.violetabft_store.apply_batch_system.pool_size = 2;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    fail::causet("on_violetabft_gc_log_tick", "return()").unwrap();
    let r1 = cluster.run_conf_change();

    fidel_client.must_add_peer(r1, new_peer(3, 3));

    assert_eq!(r1, 1);
    let before_check_snapshot_1_2_fp = "before_check_snapshot_1_2";
    fail::causet("before_check_snapshot_1_2", "pause").unwrap();
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");

    let brane = fidel_client.get_brane(b"k1").unwrap();
    cluster.must_split(&brane, b"k2");
    cluster.must_put(b"k22", b"v22");

    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    let left = fidel_client.get_brane(b"k1").unwrap();
    let left_peer_2 = find_peer(&left, 2).cloned().unwrap();
    fidel_client.must_remove_peer(left.get_id(), left_peer_2);
    must_get_none(&cluster.get_engine(2), b"k1");

    let on_handle_apply_2_fp = "on_handle_apply_2";
    fail::causet("on_handle_apply_2", "pause").unwrap();

    fail::remove(before_check_snapshot_1_2_fp);

    // Wait for the logs
    sleep_ms(100);

    // If left_peer_2 can be created, dropping all msg to make it exist.
    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));
    // Also don't lightlike check stale msg to FIDel
    let peer_check_stale_state_fp = "peer_check_stale_state";
    fail::causet(peer_check_stale_state_fp, "return()").unwrap();

    fail::remove(on_handle_apply_2_fp);

    // If value of `k22` is equal to `v22`, the previous split log must be applied.
    must_get_equal(&cluster.get_engine(2), b"k22", b"v22");

    // If left_peer_2 is created, `must_get_none` will fail.
    must_get_none(&cluster.get_engine(2), b"k1");

    fail::remove("on_violetabft_gc_log_tick");
    fail::remove(peer_check_stale_state_fp);
}
