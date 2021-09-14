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
use fidel_client::FidelClient;
use test_violetabftstore::sleep_ms;

#[test]
fn test_stale_resolver() {
    let mut suite = TestSuite::new(3);

    let fp = "before_schedule_resolver_ready";
    fail::causet(fp, "pause").unwrap();

    // Close previous connection and open a new one twice time
    let brane = suite.cluster.get_brane(&[]);
    let req = suite.new_changedata_request(brane.get_id());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(brane.get_id()));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    // Sleep for a while to wait the scan is done
    sleep_ms(200);

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

    // Block next scan
    let fp1 = "causet_context_incremental_scan_spacelike";
    fail::causet(fp1, "pause").unwrap();
    // Close previous connection and open two new connections
    let (mut req_tx, resp_rx) = suite
        .get_brane_causet_context_client(brane.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    let (mut req_tx1, resp_rx1) = suite
        .get_brane_causet_context_client(brane.get_id())
        .event_feed()
        .unwrap();
    block_on(req_tx1.lightlike((req, WriteFlags::default()))).unwrap();
    // Unblock the first scan
    fail::remove(fp);
    // Sleep for a while to wait the wrong resolver init
    sleep_ms(100);
    // Async commit
    let commit_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    let commit_resp =
        suite.async_kv_commit(brane.get_id(), vec![k.into_bytes()], spacelike_ts, commit_ts);
    // Receive Commit response
    block_on(commit_resp).unwrap();
    // Unblock all scans
    fail::remove(fp1);
    // Receive events
    let mut events = receive_event(false).events.to_vec();
    while events.len() < 2 {
        events.extlightlike(receive_event(false).events.into_iter());
    }
    assert_eq!(events.len(), 2);
    for event in events {
        match event.event.unwrap() {
            Event_oneof_event::Entries(es) => match es.entries.len() {
                1 => {
                    assert_eq!(es.entries[0].get_type(), EventLogType::Commit, "{:?}", es);
                }
                2 => {
                    let e = &es.entries[0];
                    assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
                    let e = &es.entries[1];
                    assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
                }
                _ => panic!("{:?}", es),
            },
            Event_oneof_event::Error(e) => panic!("{:?}", e),
            other => panic!("unknown event {:?}", other),
        }
    }

    event_feed_wrap.as_ref().replace(Some(resp_rx1));
    // Receive events
    for _ in 0..2 {
        let mut events = receive_event(false).events.to_vec();
        match events.pop().unwrap().event.unwrap() {
            Event_oneof_event::Entries(es) => match es.entries.len() {
                1 => {
                    let e = &es.entries[0];
                    assert_eq!(e.get_type(), EventLogType::Commit, "{:?}", es);
                }
                2 => {
                    let e = &es.entries[0];
                    assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
                    let e = &es.entries[1];
                    assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
                }
                _ => {
                    panic!("unexepected event length {:?}", es);
                }
            },
            Event_oneof_event::Error(e) => panic!("{:?}", e),
            other => panic!("unknown event {:?}", other),
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}
