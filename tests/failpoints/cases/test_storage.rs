// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::{atomic::Ordering, mpsc::channel, Arc};
use std::thread;
use std::time::Duration;

use grpcio::*;
use ekvproto::kvrpc_timeshare::{self, Context, Op, PrewriteRequest, RawPutRequest};
use ekvproto::edb_timeshare::EINSTEINDBClient;

use errors::extract_brane_error;
use test_violetabftstore::{must_get_equal, must_get_none, new_peer, new_server_cluster};
use edb::causet_storage::kv::{Error as KvError, ErrorInner as KvErrorInner};
use edb::causet_storage::lock_manager::DummyLockManager;
use edb::causet_storage::txn::{commands, Error as TxnError, ErrorInner as TxnErrorInner};
use edb::causet_storage::{self, test_util::*, *};
use violetabftstore::interlock::::{collections::HashMap, HandyRwLock};
use txn_types::Key;
use txn_types::{Mutation, TimeStamp};

#[test]
fn test_interlock_semaphore_leader_change_twice() {
    let snapshot_fp = "interlock_semaphore_async_snapshot_finish";
    let mut cluster = new_server_cluster(0, 2);
    cluster.run();
    let brane0 = cluster.get_brane(b"");
    let peers = brane0.get_peers();
    cluster.must_transfer_leader(brane0.get_id(), peers[0].clone());
    let engine0 = cluster.sim.rl().causet_storages[&peers[0].get_id()].clone();
    let causet_storage0 = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
        engine0,
        DummyLockManager {},
    )
    .build()
    .unwrap();

    let mut ctx0 = Context::default();
    ctx0.set_brane_id(brane0.get_id());
    ctx0.set_brane_epoch(brane0.get_brane_epoch().clone());
    ctx0.set_peer(peers[0].clone());
    let (prewrite_tx, prewrite_rx) = channel();
    fail::causet(snapshot_fp, "pause").unwrap();
    causet_storage0
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
                b"k".to_vec(),
                10.into(),
                0,
                false,
                0,
                TimeStamp::default(),
                None,
                ctx0,
            ),
            Box::new(move |res: causet_storage::Result<_>| {
                prewrite_tx.lightlike(res).unwrap();
            }),
        )
        .unwrap();
    // Sleep to make sure the failpoint is triggered.
    thread::sleep(Duration::from_millis(2000));
    // Transfer leader twice, then unblock snapshot.
    cluster.must_transfer_leader(brane0.get_id(), peers[1].clone());
    cluster.must_transfer_leader(brane0.get_id(), peers[0].clone());
    fail::remove(snapshot_fp);

    match prewrite_rx.recv_timeout(Duration::from_secs(5)).unwrap() {
        Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Engine(KvError(
            box KvErrorInner::Request(ref e),
        ))))))
        | Err(Error(box ErrorInner::Engine(KvError(box KvErrorInner::Request(ref e))))) => {
            assert!(e.has_stale_command(), "{:?}", e);
        }
        res => {
            panic!("expect stale command, but got {:?}", res);
        }
    }
}

#[test]
fn test_server_catching_api_error() {
    let violetabftkv_fp = "violetabftkv_early_error_report";
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    let brane = cluster.get_brane(b"");
    let leader = brane.get_peers()[0].clone();

    fail::causet(violetabftkv_fp, "return").unwrap();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = EINSTEINDBClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_brane_id(brane.get_id());
    ctx.set_brane_epoch(brane.get_brane_epoch().clone());
    ctx.set_peer(leader);

    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx.clone());
    let mut mutation = kvrpc_timeshare::Mutation::default();
    mutation.op = Op::Put.into();
    mutation.key = b"k3".to_vec();
    mutation.value = b"v3".to_vec();
    prewrite_req.set_mutations(vec![mutation].into_iter().collect());
    prewrite_req.primary_lock = b"k3".to_vec();
    prewrite_req.spacelike_version = 1;
    prewrite_req.lock_ttl = prewrite_req.spacelike_version + 1;
    let prewrite_resp = client.kv_prewrite(&prewrite_req).unwrap();
    assert!(prewrite_resp.has_brane_error(), "{:?}", prewrite_resp);
    assert!(
        prewrite_resp.get_brane_error().has_brane_not_found(),
        "{:?}",
        prewrite_resp
    );
    must_get_none(&cluster.get_engine(1), b"k3");

    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx);
    put_req.key = b"k3".to_vec();
    put_req.value = b"v3".to_vec();
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(put_resp.has_brane_error(), "{:?}", put_resp);
    assert!(
        put_resp.get_brane_error().has_brane_not_found(),
        "{:?}",
        put_resp
    );
    must_get_none(&cluster.get_engine(1), b"k3");

    fail::remove(violetabftkv_fp);
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_brane_error(), "{:?}", put_resp);
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
}

#[test]
fn test_violetabftkv_early_error_report() {
    let violetabftkv_fp = "violetabftkv_early_error_report";
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    cluster.must_split(&cluster.get_brane(b"k0"), b"k1");

    let env = Arc::new(Environment::new(1));
    let mut clients: HashMap<&[u8], (Context, EINSTEINDBClient)> = HashMap::default();
    for &k in &[b"k0", b"k1"] {
        let brane = cluster.get_brane(k);
        let leader = brane.get_peers()[0].clone();
        let mut ctx = Context::default();
        let channel = ChannelBuilder::new(env.clone())
            .connect(cluster.sim.rl().get_addr(leader.get_store_id()));
        let client = EINSTEINDBClient::new(channel);
        ctx.set_brane_id(brane.get_id());
        ctx.set_brane_epoch(brane.get_brane_epoch().clone());
        ctx.set_peer(leader);
        clients.insert(k, (ctx, client));
    }

    // Inject error to all branes.
    fail::causet(violetabftkv_fp, "return").unwrap();
    for (k, (ctx, client)) in &clients {
        let mut put_req = RawPutRequest::default();
        put_req.set_context(ctx.clone());
        put_req.key = k.to_vec();
        put_req.value = b"v".to_vec();
        let put_resp = client.raw_put(&put_req).unwrap();
        assert!(put_resp.has_brane_error(), "{:?}", put_resp);
        assert!(
            put_resp.get_brane_error().has_brane_not_found(),
            "{:?}",
            put_resp
        );
        must_get_none(&cluster.get_engine(1), k);
    }
    fail::remove(violetabftkv_fp);

    // Inject only one brane
    let injected_brane_id = clients[b"k0".as_ref()].0.get_brane_id();
    fail::causet(violetabftkv_fp, &format!("return({})", injected_brane_id)).unwrap();
    for (k, (ctx, client)) in &clients {
        let mut put_req = RawPutRequest::default();
        put_req.set_context(ctx.clone());
        put_req.key = k.to_vec();
        put_req.value = b"v".to_vec();
        let put_resp = client.raw_put(&put_req).unwrap();
        if ctx.get_brane_id() == injected_brane_id {
            assert!(put_resp.has_brane_error(), "{:?}", put_resp);
            assert!(
                put_resp.get_brane_error().has_brane_not_found(),
                "{:?}",
                put_resp
            );
            must_get_none(&cluster.get_engine(1), k);
        } else {
            assert!(!put_resp.has_brane_error(), "{:?}", put_resp);
            must_get_equal(&cluster.get_engine(1), k, b"v");
        }
    }
    fail::remove(violetabftkv_fp);
}

#[test]
fn test_pipelined_pessimistic_lock() {
    let rockskv_async_write_fp = "rockskv_async_write";
    let rockskv_write_modifies_fp = "rockskv_write_modifies";
    let interlock_semaphore_async_write_finish_fp = "interlock_semaphore_async_write_finish";
    let interlock_semaphore_pipelined_write_finish_fp = "interlock_semaphore_pipelined_write_finish";

    let causet_storage = TestStorageBuilder::new(DummyLockManager {})
        .set_pipelined_pessimistic_lock(true)
        .build()
        .unwrap();

    let (tx, rx) = channel();
    let (key, val) = (Key::from_raw(b"key"), b"val".to_vec());

    // Even if causet_storage fails to write the dagger to engine, client should
    // receive the successful response.
    fail::causet(rockskv_write_modifies_fp, "return()").unwrap();
    fail::causet(interlock_semaphore_async_write_finish_fp, "pause").unwrap();
    causet_storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 10, 10, true),
            expect_pessimistic_lock_res_callback(
                tx.clone(),
                PessimisticLockRes::Values(vec![None]),
            ),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(rockskv_write_modifies_fp);
    fail::remove(interlock_semaphore_async_write_finish_fp);
    causet_storage
        .sched_txn_command(
            commands::PrewritePessimistic::new(
                vec![(Mutation::Put((key.clone(), val.clone())), true)],
                key.to_raw().unwrap(),
                10.into(),
                3000,
                10.into(),
                1,
                11.into(),
                None,
                Context::default(),
            ),
            expect_ok_callback(tx.clone(), 0),
        )
        .unwrap();
    rx.recv().unwrap();
    causet_storage
        .sched_txn_command(
            commands::Commit::new(vec![key.clone()], 10.into(), 20.into(), Context::default()),
            expect_ok_callback(tx.clone(), 0),
        )
        .unwrap();
    rx.recv().unwrap();

    // Should report failure if causet_storage fails to schedule write request to engine.
    fail::causet(rockskv_async_write_fp, "return()").unwrap();
    causet_storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 30, 30, true),
            expect_fail_callback(tx.clone(), 0, |_| ()),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(rockskv_async_write_fp);

    // Shouldn't release latches until async write finished.
    fail::causet(interlock_semaphore_async_write_finish_fp, "pause").unwrap();
    for blocked in &[false, true] {
        causet_storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 40, 40, true),
                expect_pessimistic_lock_res_callback(
                    tx.clone(),
                    PessimisticLockRes::Values(vec![Some(val.clone())]),
                ),
            )
            .unwrap();

        if !*blocked {
            rx.recv().unwrap();
        } else {
            // Blocked by latches.
            rx.recv_timeout(Duration::from_millis(500)).unwrap_err();
        }
    }
    fail::remove(interlock_semaphore_async_write_finish_fp);
    rx.recv().unwrap();
    delete_pessimistic_lock(&causet_storage, key.clone(), 40, 40);

    // Pipelined write is finished before async write.
    fail::causet(interlock_semaphore_async_write_finish_fp, "pause").unwrap();
    causet_storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 50, 50, true),
            expect_pessimistic_lock_res_callback(
                tx.clone(),
                PessimisticLockRes::Values(vec![Some(val.clone())]),
            ),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(interlock_semaphore_async_write_finish_fp);
    delete_pessimistic_lock(&causet_storage, key.clone(), 50, 50);

    // Async write is finished before pipelined write due to thread scheduling.
    // causet_storage should handle it properly.
    fail::causet(interlock_semaphore_pipelined_write_finish_fp, "pause").unwrap();
    causet_storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(
                vec![(key.clone(), false), (Key::from_raw(b"nonexist"), false)],
                60,
                60,
                true,
            ),
            expect_pessimistic_lock_res_callback(
                tx,
                PessimisticLockRes::Values(vec![Some(val), None]),
            ),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(interlock_semaphore_pipelined_write_finish_fp);
    delete_pessimistic_lock(&causet_storage, key, 60, 60);
}

#[test]
fn test_async_commit_prewrite_with_stale_max_ts() {
    let mut cluster = new_server_cluster(0, 2);
    cluster.run();

    let engine = cluster
        .sim
        .read()
        .unwrap()
        .causet_storages
        .get(&1)
        .unwrap()
        .clone();
    let causet_storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
        engine.clone(),
        DummyLockManager {},
    )
    .build()
    .unwrap();

    // Fail to get timestamp from FIDel at first
    fail::causet("test_violetabftstore_get_tso", "pause").unwrap();
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let mut ctx = Context::default();
    ctx.set_brane_id(1);
    ctx.set_brane_epoch(cluster.get_brane_epoch(1));
    ctx.set_peer(cluster.leader_of_brane(1).unwrap());

    let check_max_timestamp_not_synced = |expected: bool| {
        // prewrite
        let (prewrite_tx, prewrite_rx) = channel();
        causet_storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::Put((Key::from_raw(b"k1"), b"v".to_vec()))],
                    b"k1".to_vec(),
                    10.into(),
                    100,
                    false,
                    2,
                    TimeStamp::default(),
                    Some(vec![b"k2".to_vec()]),
                    ctx.clone(),
                ),
                Box::new(move |res: causet_storage::Result<_>| {
                    prewrite_tx.lightlike(res).unwrap();
                }),
            )
            .unwrap();
        let res = prewrite_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let brane_error = extract_brane_error(&res);
        assert_eq!(
            brane_error
                .map(|e| e.has_max_timestamp_not_synced())
                .unwrap_or(false),
            expected
        );

        // pessimistic prewrite
        let (prewrite_tx, prewrite_rx) = channel();
        causet_storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(Mutation::Put((Key::from_raw(b"k1"), b"v".to_vec())), true)],
                    b"k1".to_vec(),
                    10.into(),
                    100,
                    20.into(),
                    2,
                    TimeStamp::default(),
                    Some(vec![b"k2".to_vec()]),
                    ctx.clone(),
                ),
                Box::new(move |res: causet_storage::Result<_>| {
                    prewrite_tx.lightlike(res).unwrap();
                }),
            )
            .unwrap();
        let res = prewrite_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let brane_error = extract_brane_error(&res);
        assert_eq!(
            brane_error
                .map(|e| e.has_max_timestamp_not_synced())
                .unwrap_or(false),
            expected
        );
    };

    // should get max timestamp not synced error
    check_max_timestamp_not_synced(true);

    // can get timestamp from FIDel
    fail::remove("test_violetabftstore_get_tso");

    // wait for timestamp synced
    let snapshot = engine.snapshot(&ctx).unwrap();
    let max_ts_sync_status = snapshot.max_ts_sync_status.clone().unwrap();
    for retry in 0..10 {
        if max_ts_sync_status.load(Ordering::SeqCst) & 1 == 1 {
            break;
        }
        thread::sleep(Duration::from_millis(1 << retry));
    }
    assert!(snapshot.is_max_ts_synced());

    // should NOT get max timestamp not synced error
    check_max_timestamp_not_synced(false);
}
