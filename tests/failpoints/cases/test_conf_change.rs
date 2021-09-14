//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use futures::executor::block_on;
use ekvproto::violetabft_server_timeshare::VioletaBftMessage;
use fidel_client::FidelClient;
use violetabft::evioletabft_timeshare::{ConfChangeType, MessageType};
use test_violetabftstore::*;
use violetabftstore::interlock::::config::ReadableDuration;
use violetabftstore::interlock::::HandyRwLock;

#[test]
fn test_destroy_local_reader() {
    // 3 nodes cluster.
    let mut cluster = new_node_cluster(0, 3);

    // Set election timeout and max leader lease to 1s.
    configure_for_lease_read(&mut cluster, Some(100), Some(10));

    let fidel_client = cluster.fidel_client.clone();
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // Now brane 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // add peer (2,2) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    // add peer (3, 3) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    let epoch = fidel_client.get_brane_epoch(r1);

    // Conf version must change.
    assert!(epoch.get_conf_ver() > 2);

    // Transfer leader to peer (2, 2).
    cluster.must_transfer_leader(1, new_peer(2, 2));

    // Remove peer (1, 1) from brane 1.
    fidel_client.must_remove_peer(r1, new_peer(1, 1));

    // Make sure brane 1 is removed from store 1.
    cluster.must_brane_not_exist(r1, 1);

    let brane = block_on(fidel_client.get_brane_by_id(r1)).unwrap().unwrap();

    // Local reader panics if it finds a pushdown_causet.
    let reader_has_pushdown_causet = "localreader_on_find_pushdown_causet";
    fail::causet(reader_has_pushdown_causet, "panic").unwrap();

    let resp = read_on_peer(
        &mut cluster,
        new_peer(1, 1),
        brane,
        key,
        false,
        Duration::from_secs(5),
    );
    debug!("resp: {:?}", resp);
    assert!(resp.unwrap().get_header().has_error());

    fail::remove(reader_has_pushdown_causet);
}

#[test]
fn test_write_after_destroy() {
    // 3 nodes cluster.
    let mut cluster = new_server_cluster(0, 3);

    let fidel_client = cluster.fidel_client.clone();
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // Now brane 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // add peer (2,2) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(2, 2));

    // add peer (3, 3) to brane 1.
    fidel_client.must_add_peer(r1, new_peer(3, 3));
    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");

    let apply_fp = "apply_on_conf_change_1_3_1";
    fail::causet(apply_fp, "pause").unwrap();

    cluster.must_transfer_leader(r1, new_peer(1, 1));
    let conf_change = new_change_peer_request(ConfChangeType::RemoveNode, new_peer(3, 3));
    let mut epoch = cluster.fidel_client.get_brane_epoch(r1);
    let mut admin_req = new_admin_request(r1, &epoch, conf_change);
    admin_req.mut_header().set_peer(new_peer(1, 1));
    let (cb1, rx1) = make_cb(&admin_req);
    let engines_3 = cluster.get_all_engines(3);
    let brane = block_on(cluster.fidel_client.get_brane_by_id(r1))
        .unwrap()
        .unwrap();
    let reqs = vec![new_put_cmd(b"k5", b"v5")];
    let new_version = epoch.get_conf_ver() + 1;
    epoch.set_conf_ver(new_version);
    let mut put = new_request(r1, epoch, reqs, false);
    put.mut_header().set_peer(new_peer(1, 1));
    cluster
        .sim
        .rl()
        .async_command_on_node(1, admin_req, cb1)
        .unwrap();
    for _ in 0..100 {
        let (cb2, _rx2) = make_cb(&put);
        cluster
            .sim
            .rl()
            .async_command_on_node(1, put.clone(), cb2)
            .unwrap();
    }
    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, b"k5", b"v5");
    fail::remove(apply_fp);
    let resp = rx1.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    std::thread::sleep(Duration::from_secs(3));
    must_get_none(&engine_3, b"k5");
    must_brane_cleared(&engines_3, &brane);
}

#[test]
fn test_tick_after_destroy() {
    // 3 nodes cluster.
    let mut cluster = new_server_cluster(0, 3);
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(50);

    let fidel_client = cluster.fidel_client.clone();
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    cluster.run();

    // Now brane 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");

    let tick_fp = "on_violetabft_log_gc_tick_1";
    fail::causet(tick_fp, "return").unwrap();
    let poll_fp = "pause_on_peer_destroy_res";
    fail::causet(poll_fp, "pause").unwrap();

    cluster.must_transfer_leader(1, new_peer(3, 3));

    cluster.add_lightlike_filter(IsolationFilterFactory::new(1));
    fidel_client.must_remove_peer(1, new_peer(1, 1));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&engine_3, b"k2", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");

    fidel_client.must_add_peer(1, new_peer(1, 4));
    cluster.clear_lightlike_filters();
    cluster.must_put(b"k3", b"v3");

    fail::remove(tick_fp);
    thread::sleep(cluster.causet.violetabft_store.violetabft_log_gc_tick_interval.0);
    thread::sleep(Duration::from_millis(100));
    fail::remove(poll_fp);

    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
}

#[test]
fn test_stale_peer_cache() {
    // 3 nodes cluster.
    let mut cluster = new_node_cluster(0, 3);

    cluster.run();
    // Now brane 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::causet("stale_peer_cache_2", "return").unwrap();
    cluster.must_put(b"k2", b"v2");
}

// The test is for this situation:
// suppose there are 3 peers (1, 2, and 3) in a VioletaBft group, and then
// 1. propose to add peer 4 on the current leader 1;
// 2. leader 1 applightlikes entries to peer 3, and peer 3 applys them;
// 3. a new proposal to remove peer 4 is proposed;
// 4. peer 1 lightlikes a snapshot with latest configuration [1, 2, 3] to peer 3;
// 5. peer 3 restores the snapshot into memory;
// 6. then peer 3 calling `VioletaBft::apply_conf_change` to add peer 4;
// 7. so the disk configuration `[1, 2, 3]` is different from memory configuration `[1, 2, 3, 4]`.
#[test]
fn test_redundant_conf_change_by_snapshot() {
    let mut cluster = new_node_cluster(0, 4);
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 5;
    cluster.causet.violetabft_store.merge_max_log_gap = 4;
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(20);

    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    // Add voter 2 and learner 3.
    cluster.run_conf_change();
    fidel_client.must_add_peer(1, new_peer(2, 2));
    fidel_client.must_add_peer(1, new_peer(3, 3));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    fail::causet("apply_on_conf_change_3_1", "pause").unwrap();
    cluster.fidel_client.must_add_peer(1, new_peer(4, 4));

    let filter = Box::new(
        BranePacketFilter::new(1, 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgApplightlike),
    );
    cluster.sim.wl().add_recv_filter(3, filter);

    // propose to remove peer 4, and applightlike more entries to compact violetabft logs.
    cluster.fidel_client.must_remove_peer(1, new_peer(4, 4));
    (0..10).for_each(|_| cluster.must_put(b"k2", b"v2"));
    sleep_ms(50);

    // Clear filters on peer 3, so it can receive and restore a snapshot.
    cluster.sim.wl().clear_recv_filters(3);
    sleep_ms(100);

    // Use a filter to capture messages sent from 3 to 4.
    let (tx, rx) = mpsc::sync_channel(128);
    let cb = Arc::new(move |msg: &VioletaBftMessage| {
        if msg.get_message().get_to() == 4 {
            let _ = tx.lightlike(());
        }
    }) as Arc<dyn Fn(&VioletaBftMessage) + lightlike + Sync>;
    let filter = Box::new(
        BranePacketFilter::new(1, 3)
            .direction(Direction::lightlike)
            .msg_type(MessageType::MsgRequestVote)
            .when(Arc::new(AtomicBool::new(false)))
            .set_msg_callback(cb),
    );
    cluster.sim.wl().add_lightlike_filter(3, filter);

    // Unpause the fail point, so peer 3 can apply the redundant conf change result.
    fail::causet("apply_on_conf_change_3_1", "off").unwrap();

    cluster.must_transfer_leader(1, new_peer(3, 3));
    assert!(rx.try_recv().is_err());

    fail::remove("apply_on_conf_change_3_1");
}

#[test]
fn test_handle_conf_change_when_apply_fsm_resume_plightlikeing_state() {
    let mut cluster = new_node_cluster(0, 3);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k", b"v");

    let brane = fidel_client.get_brane(b"k").unwrap();

    let peer_on_store1 = find_peer(&brane, 1).unwrap().to_owned();
    cluster.must_transfer_leader(brane.get_id(), peer_on_store1);

    let yield_apply_conf_change_3_fp = "yield_apply_conf_change_3";
    fail::causet(yield_apply_conf_change_3_fp, "return()").unwrap();

    // Make store 1 and 3 become quorum
    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));

    fidel_client.must_remove_peer(r1, new_peer(3, 3));
    // Wait for peer fsm to lightlike committed entries to apply fsm
    sleep_ms(100);
    fail::remove(yield_apply_conf_change_3_fp);
    cluster.clear_lightlike_filters();
    // Add new peer 4 to store 3
    fidel_client.must_add_peer(r1, new_peer(3, 4));

    for i in 0..10 {
        cluster.must_put(format!("kk{}", i).as_bytes(), b"v1");
    }
}
