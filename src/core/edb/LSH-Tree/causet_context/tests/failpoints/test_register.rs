// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.
use std::thread;
use std::time::Duration;

use crate::{new_event_feed, TestSuite};
use futures::executor::block_on;
use futures::sink::SinkExt;
use grpcio::WriteFlags;
#[causet(feature = "prost-codec")]
use ekvproto::causet_context_timeshare::event::{Event as Event_oneof_event, LogType as EventLogType};
#[causet(not(feature = "prost-codec"))]
use ekvproto::causet_context_timeshare::*;
use ekvproto::kvrpc_timeshare::*;
use ekvproto::meta_timeshare::BraneEpoch;
use fidel_client::FidelClient;
use violetabft::StateRole;
use violetabftstore::interlock::{SemaphoreContext, RoleSemaphore};
use test_violetabftstore::sleep_ms;

#[test]
fn test_failed_plightlikeing_batch() {
    // For test that a plightlikeing cmd batch contains a error like epoch not match.
    let mut suite = TestSuite::new(3);

    let fp = "causet_context_incremental_scan_spacelike";
    fail::causet(fp, "pause").unwrap();

    let brane = suite.cluster.get_brane(&[]);
    let mut req = suite.new_changedata_request(brane.get_id());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    // Split brane.
    suite.cluster.must_split(&brane, b"k0");
    // Wait for receiving split cmd.
    sleep_ms(200);
    fail::remove(fp);

    loop {
        let mut events = receive_event(false).events.to_vec();
        match events.pop().unwrap().event.unwrap() {
            Event_oneof_event::Error(err) => {
                assert!(err.has_epoch_not_match(), "{:?}", err);
                break;
            }
            Event_oneof_event::Entries(es) => {
                assert!(es.entries.len() == 1, "{:?}", es);
                let e = &es.entries[0];
                assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
            }
            other => panic!("unknown event {:?}", other),
        }
    }
    // Try to subscribe brane again.
    let brane = suite.cluster.get_brane(b"k0");
    // Ensure it is the previous brane.
    assert_eq!(req.get_brane_id(), brane.get_id());
    req.set_brane_epoch(brane.get_brane_epoch().clone());
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_brane_ready_after_deregister() {
    let mut suite = TestSuite::new(1);

    let fp = "causet_context_incremental_scan_spacelike";
    fail::causet(fp, "pause").unwrap();

    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    // Sleep for a while to make sure the brane has been subscribed
    sleep_ms(200);

    // Simulate a role change event
    let brane = suite.cluster.get_brane(&[]);
    let leader = suite.cluster.leader_of_brane(brane.get_id()).unwrap();
    let mut context = SemaphoreContext::new(&brane);
    suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .on_role_change(&mut context, StateRole::Follower);

    // Then causet_context should not panic
    fail::remove(fp);
    receive_event(false);

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_connections_register() {
    let mut suite = TestSuite::new(1);

    let fp = "causet_context_incremental_scan_spacelike";
    fail::causet(fp, "pause").unwrap();

    let (k, v) = ("key1".to_owned(), "value".to_owned());
    // Brane info
    let brane = suite.cluster.get_brane(&[]);
    // Prewrite
    let spacelike_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone().into_bytes();
    mutation.value = v.into_bytes();
    suite.must_kv_prewrite(brane.get_id(), vec![mutation], k.into_bytes(), spacelike_ts);

    let mut req = suite.new_changedata_request(brane.get_id());
    req.set_brane_epoch(BraneEpoch::default());

    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Conn 1
    req.set_brane_epoch(brane.get_brane_epoch().clone());
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    thread::sleep(Duration::from_secs(1));
    // Close conn 1
    event_feed_wrap.as_ref().replace(None);
    // Conn 2
    let (mut req_tx, resp_rx) = suite
        .get_brane_causet_context_client(brane.get_id())
        .event_feed()
        .unwrap();
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    // Split brane.
    suite.cluster.must_split(&brane, b"k0");
    fail::remove(fp);
    // Receive events from conn 2
    // As split happens before remove the pause fail point, so it must receive
    // an epoch not match error.
    let mut events = receive_event(false).events.to_vec();
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_merge() {
    let mut suite = TestSuite::new(1);
    // Split brane
    let brane = suite.cluster.get_brane(&[]);
    suite.cluster.must_split(&brane, b"k1");
    // Subscribe source brane
    let source = suite.cluster.get_brane(b"k0");
    let mut req = suite.new_changedata_request(brane.get_id());
    req.brane_id = source.get_id();
    req.set_brane_epoch(source.get_brane_epoch().clone());
    let (mut source_tx, source_wrap, source_event) =
        new_event_feed(suite.get_brane_causet_context_client(source.get_id()));
    block_on(source_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    // Subscribe target brane
    let target = suite.cluster.get_brane(b"k2");
    req.brane_id = target.get_id();
    req.set_brane_epoch(target.get_brane_epoch().clone());
    let (mut target_tx, target_wrap, target_event) =
        new_event_feed(suite.get_brane_causet_context_client(target.get_id()));
    block_on(target_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    sleep_ms(200);
    // Pause before completing commit merge
    let commit_merge_fp = "before_handle_catch_up_logs_for_merge";
    fail::causet(commit_merge_fp, "pause").unwrap();
    // The call is finished when prepare_merge is applied.
    suite.cluster.try_merge(source.get_id(), target.get_id());
    // Epoch not match after prepare_merge
    let mut events = source_event(false).events.to_vec();
    if events.len() == 1 {
        events.extlightlike(source_event(false).events.into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    let mut events = target_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Continue to commit merge
    let destroy_peer_fp = "destroy_peer";
    fail::causet(destroy_peer_fp, "pause").unwrap();
    fail::remove(commit_merge_fp);
    // Wait until violetabftstore receives MergeResult
    sleep_ms(100);
    // Retry to subscribe source brane
    let mut source_epoch = source.get_brane_epoch().clone();
    source_epoch.set_version(source_epoch.get_version() + 1);
    source_epoch.set_conf_ver(source_epoch.get_conf_ver() + 1);
    req.brane_id = source.get_id();
    req.set_brane_epoch(source_epoch);
    block_on(source_tx.lightlike((req, WriteFlags::default()))).unwrap();
    // Wait until violetabftstore receives ChangeCmd
    sleep_ms(100);
    fail::remove(destroy_peer_fp);
    loop {
        let mut events = source_event(false).events.to_vec();
        assert_eq!(events.len(), 1, "{:?}", events);
        match events.pop().unwrap().event.unwrap() {
            Event_oneof_event::Error(err) => {
                assert!(err.has_brane_not_found(), "{:?}", err);
                break;
            }
            Event_oneof_event::Entries(es) => {
                assert!(es.entries.len() == 1, "{:?}", es);
                let e = &es.entries[0];
                assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
            }
            other => panic!("unknown event {:?}", other),
        }
    }
    let mut events = target_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    source_wrap.as_ref().replace(None);
    target_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_deregister_plightlikeing_downstream() {
    let mut suite = TestSuite::new(1);

    let build_resolver_fp = "before_schedule_resolver_ready";
    fail::causet(build_resolver_fp, "pause").unwrap();
    let mut req = suite.new_changedata_request(1);
    let (mut req_tx1, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx1.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    // Sleep for a while to make sure the brane has been subscribed
    sleep_ms(200);

    let violetabft_capture_fp = "violetabft_on_capture_change";
    fail::causet(violetabft_capture_fp, "pause").unwrap();

    // Conn 2
    let (mut req_tx2, resp_rx2) = suite.get_brane_causet_context_client(1).event_feed().unwrap();
    req.set_brane_epoch(BraneEpoch::default());
    block_on(req_tx2.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    let _resp_rx1 = event_feed_wrap.as_ref().replace(Some(resp_rx2));
    // Sleep for a while to make sure the brane has been subscribed
    sleep_ms(200);
    fail::remove(build_resolver_fp);
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    block_on(req_tx2.lightlike((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    fail::remove(violetabft_capture_fp);

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}
