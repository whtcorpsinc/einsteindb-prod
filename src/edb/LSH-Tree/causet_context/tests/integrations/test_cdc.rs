// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::*;
use std::time::Duration;

use crate::{new_event_feed, TestSuite};
use interlocking_directorate::ConcurrencyManager;
use futures::executor::block_on;
use futures::SinkExt;
use grpcio::WriteFlags;
#[causet(not(feature = "prost-codec"))]
use ekvproto::causet_context_timeshare::*;
#[causet(feature = "prost-codec")]
use ekvproto::causet_context_timeshare::{
    event::{Evcausetidx::OpType as EventEventOpType, Event as Event_oneof_event, LogType as EventLogType},
    ChangeDataEvent,
};
use ekvproto::kvrpc_timeshare::*;
use fidel_client::FidelClient;
use test_violetabftstore::sleep_ms;
use test_violetabftstore::*;
use txn_types::{Key, Dagger, LockType};

use causet_context::Task;

#[test]
fn test_causet_context_basic() {
    let mut suite = TestSuite::new(1);

    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    let event = receive_event(false);
    event.events.into_iter().for_each(|e| {
        match e.event.unwrap() {
            // Even if there is no write,
            // it should always outputs an Initialized event.
            Event_oneof_event::Entries(es) => {
                assert!(es.entries.len() == 1, "{:?}", es);
                let e = &es.entries[0];
                assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
            }
            other => panic!("unknown event {:?}", other),
        }
    });

    // Sleep a while to make sure the stream is registered.
    sleep_ms(1000);
    // There must be a pushdown_causet.
    let interlock_semaphore = suite.lightlikepoints.values().next().unwrap().interlock_semaphore();
    interlock_semaphore
        .schedule(Task::Validate(
            1,
            Box::new(|pushdown_causet| {
                let d = pushdown_causet.unwrap();
                assert_eq!(d.downstreams.len(), 1);
            }),
        ))
        .unwrap();

    let (k, v) = ("key1".to_owned(), "value".to_owned());
    // Prewrite
    let spacelike_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone().into_bytes();
    mutation.value = v.into_bytes();
    suite.must_kv_prewrite(1, vec![mutation], k.clone().into_bytes(), spacelike_ts);
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Prewrite);
        }
        other => panic!("unknown event {:?}", other),
    }

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
    // Commit
    let commit_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k.into_bytes()], spacelike_ts, commit_ts);
    let mut event = receive_event(false);
    let mut events = event.take_events();
    assert_eq!(events.len(), 1, "{:?}", event);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Commit);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Split brane 1
    let brane1 = suite.cluster.get_brane(&[]);
    suite.cluster.must_split(&brane1, b"key2");
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    // The pushdown_causet must be removed.
    interlock_semaphore
        .schedule(Task::Validate(
            1,
            Box::new(|pushdown_causet| {
                assert!(pushdown_causet.is_none());
            }),
        ))
        .unwrap();

    // request again.
    let req = suite.new_changedata_request(1);
    let (mut req_tx, resp_rx) = suite.get_brane_causet_context_client(1).event_feed().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Sleep a while to make sure the stream is registered.
    sleep_ms(200);
    interlock_semaphore
        .schedule(Task::Validate(
            1,
            Box::new(|pushdown_causet| {
                let d = pushdown_causet.unwrap();
                assert_eq!(d.downstreams.len(), 1);
            }),
        ))
        .unwrap();

    // Drop stream and cancel its server streaming.
    event_feed_wrap.as_ref().replace(None);
    // Sleep a while to make sure the stream is deregistered.
    sleep_ms(200);
    interlock_semaphore
        .schedule(Task::Validate(
            1,
            Box::new(|pushdown_causet| {
                assert!(pushdown_causet.is_none());
            }),
        ))
        .unwrap();

    // Stale brane epoch.
    let mut req = suite.new_changedata_request(1);
    req.set_brane_epoch(Default::default()); // Zero brane epoch.
    let (mut req_tx, resp_rx) = suite.get_brane_causet_context_client(1).event_feed().unwrap();
    let _req_tx = block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    suite.stop();
}

#[test]
fn test_causet_context_not_leader() {
    let mut suite = TestSuite::new(3);

    let leader = suite.cluster.leader_of_brane(1).unwrap();
    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    // Make sure brane 1 is registered.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Sleep a while to make sure the stream is registered.
    sleep_ms(1000);
    // There must be a pushdown_causet.
    let interlock_semaphore = suite
        .lightlikepoints
        .get(&leader.get_store_id())
        .unwrap()
        .interlock_semaphore();
    let (tx, rx) = mpsc::channel();
    let tx_ = tx.clone();
    interlock_semaphore
        .schedule(Task::Validate(
            1,
            Box::new(move |pushdown_causet| {
                let d = pushdown_causet.unwrap();
                assert_eq!(d.downstreams.len(), 1);
                tx_.lightlike(()).unwrap();
            }),
        ))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1)
        .is_some());

    // Transfer leader.
    let peer = suite
        .cluster
        .get_brane(&[])
        .take_peers()
        .into_iter()
        .find(|p| *p != leader)
        .unwrap();
    suite.cluster.must_transfer_leader(1, peer);
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_not_leader(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    assert!(!suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1)
        .is_some());

    // Sleep a while to make sure the stream is deregistered.
    sleep_ms(200);
    interlock_semaphore
        .schedule(Task::Validate(
            1,
            Box::new(move |pushdown_causet| {
                assert!(pushdown_causet.is_none());
                tx.lightlike(()).unwrap();
            }),
        ))
        .unwrap();
    rx.recv_timeout(Duration::from_millis(200)).unwrap();

    // Try to subscribe again.
    let _req_tx = block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    // Should failed with not leader error.
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_not_leader(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    assert!(!suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1)
        .is_some());

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_causet_context_stale_epoch_after_brane_ready() {
    let mut suite = TestSuite::new(3);

    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    // Make sure brane 1 is registered.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    let mut req = suite.new_changedata_request(1);
    req.set_brane_epoch(Default::default()); // zero epoch is always stale.
    let (mut req_tx, resp_rx) = suite.get_brane_causet_context_client(1).event_feed().unwrap();
    let _resp_rx = event_feed_wrap.as_ref().replace(Some(resp_rx));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    // Must receive epoch not match error.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    req.set_brane_epoch(suite.get_context(1).take_brane_epoch());
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    // Must receive epoch not match error.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Cancel event feed before finishing test.
    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_causet_context_scan() {
    let mut suite = TestSuite::new(1);

    let (k, v) = (b"key1".to_vec(), b"value".to_vec());
    // Prewrite
    let spacelike_ts1 = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v.clone();
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), spacelike_ts1);
    // Commit
    let commit_ts1 = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k.clone()], spacelike_ts1, commit_ts1);

    // Prewrite again
    let spacelike_ts2 = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v.clone();
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), spacelike_ts2);

    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    if events.len() == 1 {
        events.extlightlike(receive_event(false).events.into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
        // Batch size is set to 2.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
            assert_eq!(e.spacelike_ts, spacelike_ts2.into_inner(), "{:?}", es);
            assert_eq!(e.commit_ts, 0, "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert_eq!(e.value, v, "{:?}", es);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", es);
            assert_eq!(e.spacelike_ts, spacelike_ts1.into_inner(), "{:?}", es);
            assert_eq!(e.commit_ts, commit_ts1.into_inner(), "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert_eq!(e.value, v, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    match events.pop().unwrap().event.unwrap() {
        // Then it outputs Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    // checkpoint_ts = 6;
    let checkpoint_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    // Commit = 7;
    let commit_ts2 = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k.clone()], spacelike_ts2, commit_ts2);
    // Prewrite delete
    // Start = 8;
    let spacelike_ts3 = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Del);
    mutation.key = k.clone();
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), spacelike_ts3);

    let mut req = suite.new_changedata_request(1);
    req.checkpoint_ts = checkpoint_ts.into_inner();
    let (mut req_tx, resp_rx) = suite.get_brane_causet_context_client(1).event_feed().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    if events.len() == 1 {
        events.extlightlike(receive_event(false).events.to_vec());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
        // Batch size is set to 2.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
            assert_eq!(e.get_op_type(), EventEventOpType::Delete, "{:?}", es);
            assert_eq!(e.spacelike_ts, spacelike_ts3.into_inner(), "{:?}", es);
            assert_eq!(e.commit_ts, 0, "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert!(e.value.is_empty(), "{:?}", es);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", es);
            assert_eq!(e.get_op_type(), EventEventOpType::Put, "{:?}", es);
            assert_eq!(e.spacelike_ts, spacelike_ts2.into_inner(), "{:?}", es);
            assert_eq!(e.commit_ts, commit_ts2.into_inner(), "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert_eq!(e.value, v, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        // Then it outputs Initialized event.
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
fn test_causet_context_tso_failure() {
    let mut suite = TestSuite::new(3);

    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    // Make sure brane 1 is registered.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    suite.cluster.fidel_client.trigger_tso_failure();

    // Make sure resolved ts can be advanced normally even with few tso failures.
    let mut counter = 0;
    let mut previous_ts = 0;
    loop {
        // Even if there is no write,
        // resolved ts should be advanced regularly.
        let event = receive_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            assert!(resolved_ts.ts >= previous_ts);
            assert_eq!(resolved_ts.branes, vec![1]);
            previous_ts = resolved_ts.ts;
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
fn test_brane_split() {
    let cluster = new_server_cluster(1, 1);
    cluster.fidel_client.disable_default_operator();
    let mut suite = TestSuite::with_cluster(1, cluster);

    let brane = suite.cluster.get_brane(&[]);
    let mut req = suite.new_changedata_request(brane.get_id());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    // Make sure brane 1 is registered.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Split brane.
    suite.cluster.must_split(&brane, b"k0");
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Try to subscribe brane again.
    let brane = suite.cluster.get_brane(b"k0");
    // Ensure it is the previous brane.
    assert_eq!(req.get_brane_id(), brane.get_id());
    req.set_brane_epoch(brane.get_brane_epoch().clone());
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Try to subscribe brane again.
    let brane1 = suite.cluster.get_brane(&[]);
    req.brane_id = brane1.get_id();
    req.set_brane_epoch(brane1.get_brane_epoch().clone());
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Make sure resolved ts can be advanced normally.
    let mut counter = 0;
    let mut previous_ts = 0;
    loop {
        // Even if there is no write,
        // resolved ts should be advanced regularly.
        let event = receive_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            assert!(resolved_ts.ts >= previous_ts);
            assert!(
                resolved_ts.branes == vec![brane.id, brane1.id]
                    || resolved_ts.branes == vec![brane1.id, brane.id]
            );
            previous_ts = resolved_ts.ts;
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
fn test_duplicate_subscribe() {
    let mut suite = TestSuite::new(1);

    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    // Make sure brane 1 is registered.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Try to subscribe again.
    let _req_tx = block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    // Should receive duplicate request error.
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_duplicate_request(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_causet_context_batch_size_limit() {
    let mut suite = TestSuite::new(1);

    // Prewrite
    let spacelike_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    let mut m1 = Mutation::default();
    let k1 = b"k1".to_vec();
    m1.set_op(Op::Put);
    m1.key = k1.clone();
    m1.value = vec![0; 6 * 1024 * 1024];
    let mut m2 = Mutation::default();
    let k2 = b"k2".to_vec();
    m2.set_op(Op::Put);
    m2.key = k2.clone();
    m2.value = b"v2".to_vec();
    suite.must_kv_prewrite(1, vec![m1, m2], k1.clone(), spacelike_ts);
    // Commit
    let commit_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k1, k2], spacelike_ts, commit_ts);

    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events.len());
    while events.len() < 3 {
        events.extlightlike(receive_event(false).events.into_iter());
    }
    assert_eq!(events.len(), 3, "{:?}", events.len());
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", e.get_type());
            assert_eq!(e.key, b"k1", "{:?}", e.key);
        }
        other => panic!("unknown event {:?}", other),
    }
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", e.get_type());
            assert_eq!(e.key, b"k2", "{:?}", e.key);
        }
        other => panic!("unknown event {:?}", other),
    }
    match events.pop().unwrap().event.unwrap() {
        // Then it outputs Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1);
            let e = &es.entries[0];
            assert_eq!(
                e.get_type(),
                EventLogType::Initialized,
                "{:?}",
                e.get_type()
            );
        }
        other => panic!("unknown event {:?}", other),
    }

    // Prewrite
    let spacelike_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    let mut m3 = Mutation::default();
    let k3 = b"k3".to_vec();
    m3.set_op(Op::Put);
    m3.key = k3.clone();
    m3.value = vec![0; 7 * 1024 * 1024];
    let mut m4 = Mutation::default();
    let k4 = b"k4".to_vec();
    m4.set_op(Op::Put);
    m4.key = k4;
    m4.value = b"v4".to_vec();
    suite.must_kv_prewrite(1, vec![m3, m4], k3, spacelike_ts);

    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", e.get_type());
            assert_eq!(e.key, b"k4", "{:?}", e.key);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", e.get_type());
            assert_eq!(e.key, b"k3", "{:?}", e.key);
        }
        other => panic!("unknown event {:?}", other),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_old_value_basic() {
    let mut suite = TestSuite::new(1);
    let mut req = suite.new_changedata_request(1);
    req.set_extra_op(ExtraOp::ReadOldValue);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    let _req_tx = block_on(req_tx.lightlike((req.clone(), WriteFlags::default()))).unwrap();
    sleep_ms(1000);

    // Insert value
    let mut m1 = Mutation::default();
    let k1 = b"k1".to_vec();
    m1.set_op(Op::Put);
    m1.key = k1.clone();
    m1.value = b"v1".to_vec();
    let m1_spacelike_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_prewrite(1, vec![m1], k1.clone(), m1_spacelike_ts);
    let m1_commit_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], m1_spacelike_ts, m1_commit_ts);
    // Rollback
    let mut m2 = Mutation::default();
    m2.set_op(Op::Put);
    m2.key = k1.clone();
    m2.value = b"v2".to_vec();
    let m2_spacelike_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_prewrite(1, vec![m2], k1.clone(), m2_spacelike_ts);
    suite.must_kv_rollback(1, vec![k1.clone()], m2_spacelike_ts);
    // fidelio value
    let mut m3 = Mutation::default();
    m3.set_op(Op::Put);
    m3.key = k1.clone();
    m3.value = vec![b'3'; 5120];
    let m3_spacelike_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_prewrite(1, vec![m3], k1.clone(), m3_spacelike_ts);
    let m3_commit_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], m3_spacelike_ts, m3_commit_ts);
    // Dagger
    let mut m4 = Mutation::default();
    m4.set_op(Op::Dagger);
    m4.key = k1.clone();
    let m4_spacelike_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_prewrite(1, vec![m4], k1.clone(), m4_spacelike_ts);
    let m4_commit_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], m4_spacelike_ts, m4_commit_ts);
    // Delete value
    let mut m5 = Mutation::default();
    m5.set_op(Op::Del);
    m5.key = k1.clone();
    let m5_spacelike_ts = block_on(suite.cluster.fidel_client.get_tso()).unwrap();
    suite.must_kv_prewrite(1, vec![m5], k1, m5_spacelike_ts);

    let mut event_count = 0;
    loop {
        let events = receive_event(false).events.to_vec();
        for event in events.into_iter() {
            match event.event.unwrap() {
                Event_oneof_event::Entries(mut es) => {
                    for Evcausetidx in es.take_entries().to_vec() {
                        if Evcausetidx.get_type() == EventLogType::Prewrite {
                            if Evcausetidx.get_spacelike_ts() == m2_spacelike_ts.into_inner()
                                || Evcausetidx.get_spacelike_ts() == m3_spacelike_ts.into_inner()
                            {
                                assert_eq!(Evcausetidx.get_old_value(), b"v1");
                                event_count += 1;
                            } else if Evcausetidx.get_spacelike_ts() == m5_spacelike_ts.into_inner() {
                                assert_eq!(Evcausetidx.get_old_value(), vec![b'3'; 5120].as_slice());
                                event_count += 1;
                            }
                        }
                    }
                }
                other => panic!("unknown event {:?}", other),
            }
        }
        if event_count >= 3 {
            break;
        }
    }

    let (mut req_tx, resp_rx) = suite.get_brane_causet_context_client(1).event_feed().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    let mut event_count = 0;
    loop {
        let event = receive_event(false);
        for e in event.events.into_iter() {
            match e.event.unwrap() {
                Event_oneof_event::Entries(mut es) => {
                    for Evcausetidx in es.take_entries().to_vec() {
                        if Evcausetidx.get_type() == EventLogType::Committed
                            && Evcausetidx.get_spacelike_ts() == m1_spacelike_ts.into_inner()
                        {
                            assert_eq!(Evcausetidx.get_old_value(), b"");
                            event_count += 1;
                        } else if Evcausetidx.get_type() == EventLogType::Committed
                            && Evcausetidx.get_spacelike_ts() == m3_spacelike_ts.into_inner()
                        {
                            assert_eq!(Evcausetidx.get_old_value(), b"v1");
                            event_count += 1;
                        } else if Evcausetidx.get_type() == EventLogType::Prewrite
                            && Evcausetidx.get_spacelike_ts() == m5_spacelike_ts.into_inner()
                        {
                            assert_eq!(Evcausetidx.get_old_value(), vec![b'3'; 5120]);
                            event_count += 1;
                        }
                    }
                }
                other => panic!("unknown event {:?}", other),
            }
        }
        if event_count >= 3 {
            break;
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_causet_context_resolve_ts_checking_interlocking_directorate() {
    let mut suite: crate::TestSuite = TestSuite::new(1);
    let cm: ConcurrencyManager = suite.get_txn_interlocking_directorate(1).unwrap();
    let lock_key = |key: &[u8], ts: u64| {
        let guard = block_on(cm.lock_key(&Key::from_raw(key)));
        guard.with_lock(|l| {
            *l = Some(Dagger::new(
                LockType::Put,
                key.to_vec(),
                ts.into(),
                0,
                None,
                0.into(),
                1,
                ts.into(),
            ))
        });
        guard
    };

    cm.fidelio_max_ts(20.into());

    let guard = lock_key(b"a", 80);
    suite.set_tso(100);

    let mut req = suite.new_changedata_request(1);
    req.set_checkpoint_ts(100);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_brane_causet_context_client(1));
    let _req_tx = block_on(req_tx.lightlike((req, WriteFlags::default()))).unwrap();
    // Make sure brane 1 is registered.
    let mut events = receive_event(false).events;
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    fn check_resolved_ts(event: ChangeDataEvent, check_fn: impl Fn(u64)) {
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            check_fn(resolved_ts.ts)
        }
    };

    check_resolved_ts(receive_event(true), |ts| assert_eq!(ts, 80));
    assert!(cm.max_ts() >= 100.into());

    drop(guard);
    for retry in 0.. {
        let event = receive_event(true);
        let mut current_rts = 0;
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            current_rts = resolved_ts.ts;
            if resolved_ts.ts >= 100 {
                break;
            }
        }
        if retry >= 5 {
            panic!(
                "resolved ts didn't push properly after unlocking memlock. current resolved_ts: {}",
                current_rts
            );
        }
    }

    let _guard = lock_key(b"a", 90);
    // The resolved_ts should be blocked by the mem dagger but it's already greater than 90.
    // Retry until receiving an unchanged resovled_ts because the first several resolved ts received
    // might be fideliod before acquiring the dagger.
    let mut last_resolved_ts = 0;
    let mut success = false;
    for _ in 0..5 {
        let event = receive_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            let ts = resolved_ts.ts;
            assert!(ts > 100);
            if ts == last_resolved_ts {
                success = true;
                break;
            }
            assert!(ts > last_resolved_ts);
            last_resolved_ts = ts;
        }
    }
    assert!(success, "resolved_ts not blocked by the memory dagger");

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}
