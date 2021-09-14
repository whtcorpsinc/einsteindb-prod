// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.
use crate::{new_event_feed, TestSuite};
use futures::executor::block_on;
use futures::sink::SinkExt;
use grpcio::WriteFlags;
#[causet(feature = "prost-codec")]
use ekvproto::causet_context_timeshare::event::{Event as Event_oneof_event, LogType as EventLogType};
#[causet(not(feature = "prost-codec"))]
use ekvproto::causet_context_timeshare::*;
use ekvproto::kvrpc_timeshare::*;
use ekvproto::violetabft_server_timeshare::VioletaBftMessage;
use fidel_client::FidelClient;
use violetabft::evioletabft_timeshare::MessageType;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;
use test_violetabftstore::*;
use violetabftstore::interlock::::config::ReadableDuration;
use violetabftstore::interlock::::HandyRwLock;

#[test]
fn test_observe_duplicate_cmd() {
    let mut suite = TestSuite::new(3);

    let brane = suite.cluster.get_brane(&[]);
    let req = suite.new_changedata_request(brane.get_id());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(brane.get_id()));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        other => panic!("unknown event {:?}", other),
    }

    let (k, v) = ("key1".to_owned(), "value".to_owned());
    // Prewrite
    let spacelike_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone().into_bytes();
    mutation.value = v.into_bytes();
    suite.must_kv_prewrite(
        brane.get_id(),
        vec![mutation],
        k.clone().into_bytes(),
        spacelike_ts,
    );
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Prewrite);
        }
        other => panic!("unknown event {:?}", other),
    }
    let fp = "before_causet_context_flush_apply";
    fail::causet(fp, "pause").unwrap();

    // Async commit
    let commit_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    let commit_resp =
        suite.async_kv_commit(brane.get_id(), vec![k.into_bytes()], spacelike_ts, commit_ts);
    sleep_ms(200);
    // Close previous connection and open a new one twice time
    let (mut req_tx, resp_rx) = suite
        .get_brane_causet_context_client(brane.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    let (mut req_tx, resp_rx) = suite
        .get_brane_causet_context_client(brane.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    fail::remove(fp);
    // Receive Commit response
    block_on(commit_resp).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", es);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        other => panic!("unknown event {:?}", other),
    }

    // Make sure resolved ts can be advanced normally even with few tso failures.
    let mut counter = 0;
    loop {
        // Even if there is no write,
        // resolved ts should be advanced regularly.
        let event = receive_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            assert_ne!(0, resolved_ts.ts);
            counter += 1;
        }
        if counter > 5 {
            break;
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_delayed_change_cmd() {
    let mut cluster = new_server_cluster(1, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(20));
    cluster.causet.violetabft_store.violetabft_store_max_leader_lease = ReadableDuration::millis(100);
    cluster.fidel_client.disable_default_operator();
    let mut suite = TestSuite::with_cluster(3, cluster);
    suite.cluster.must_put(b"k1", b"v1");
    let brane = suite.cluster.fidel_client.get_brane(&[]).unwrap();
    let leader = new_peer(1, 1);
    suite
        .cluster
        .must_transfer_leader(brane.get_id(), leader.clone());
    // Wait util lease expired
    sleep_ms(300);

    let (sx, rx) = mpsc::sync_channel::<VioletaBftMessage>(1);
    let lightlike_flag = Arc::new(AtomicBool::new(true));
    let lightlike_read_index_filter = BranePacketFilter::new(brane.get_id(), leader.get_store_id())
        .direction(Direction::lightlike)
        .msg_type(MessageType::MsgHeartbeat)
        .set_msg_callback(Arc::new(move |msg: &VioletaBftMessage| {
            if lightlike_flag.compare_and_swap(true, false, Ordering::SeqCst) {
                sx.lightlike(msg.clone()).unwrap();
            }
        }));
    suite
        .cluster
        .add_lightlike_filter(CloneFilterFactory(lightlike_read_index_filter));

    let req = suite.new_changedata_request(brane.get_id());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(brane.get_id()));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();

    suite.cluster.must_put(b"k2", b"v2");

    let (mut req_tx, resp_rx) = suite
        .get_brane_causet_context_client(brane.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    sleep_ms(200);

    suite
        .cluster
        .sim
        .wl()
        .clear_lightlike_filters(leader.get_store_id());
    rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let mut counter = 0;
    loop {
        let event = receive_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            assert_ne!(0, resolved_ts.ts);
            counter += 1;
        }
        for e in event.events.into_iter() {
            match e.event.unwrap() {
                Event_oneof_event::Entries(es) => {
                    assert!(es.entries.len() == 1, "{:?}", es);
                    let e = &es.entries[0];
                    assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
                }
                other => panic!("unknown event {:?}", other),
            }
        }
        if counter > 3 {
            break;
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}
