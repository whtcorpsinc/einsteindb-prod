// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::path::Path;
use std::sync::*;

use futures::executor::block_on;
use futures::{future, TryStreamExt};
use grpcio::{Error, RpcStatusCode};
use ekvproto::interlock::*;
use ekvproto::kvrpc_timeshare::*;
use ekvproto::violetabft_server_timeshare::*;
use ekvproto::{debug_timeshare, meta_timeshare, violetabft_server_timeshare};
use violetabft::evioletabft_timeshare;

use interlocking_directorate::ConcurrencyManager;
use engine_lmdb::raw::WriBlock;
use engine_lmdb::Compat;
use edb::Peekable;
use edb::{MiscExt, SyncMuBlock, Causet_DEFAULT, Causet_DAGGER, Causet_VIOLETABFT, Causet_WRITE};
use violetabftstore::interlock::InterlockHost;
use violetabftstore::store::fsm::store::StoreMeta;
use violetabftstore::store::{AutoSplitController, SnapManager};
use tempfile::Builder;
use test_violetabftstore::*;
use edb::interlock::REQ_TYPE_DAG;
use edb::import::SSTImporter;
use edb::server::gc_worker::sync_gc;
use edb::causet_storage::tail_pointer::{Dagger, LockType, TimeStamp};
use violetabftstore::interlock::::worker::{FutureWorker, Worker};
use violetabftstore::interlock::::HandyRwLock;
use txn_types::Key;

#[test]
fn test_rawkv() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    // Raw put
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    put_req.key = k.clone();
    put_req.value = v.clone();
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_brane_error());
    assert!(put_resp.error.is_empty());

    // Raw get
    let mut get_req = RawGetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k.clone();
    let get_resp = client.raw_get(&get_req).unwrap();
    assert!(!get_resp.has_brane_error());
    assert!(get_resp.error.is_empty());
    assert_eq!(get_resp.value, v);

    // Raw scan
    let mut scan_req = RawScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.spacelike_key = k.clone();
    scan_req.limit = 1;
    let scan_resp = client.raw_scan(&scan_req).unwrap();
    assert!(!scan_resp.has_brane_error());
    assert_eq!(scan_resp.kvs.len(), 1);
    for kv in scan_resp.kvs.into_iter() {
        assert!(!kv.has_error());
        assert_eq!(kv.key, k);
        assert_eq!(kv.value, v);
    }

    // Raw delete
    let mut delete_req = RawDeleteRequest::default();
    delete_req.set_context(ctx);
    delete_req.key = k;
    let delete_resp = client.raw_delete(&delete_req).unwrap();
    assert!(!delete_resp.has_brane_error());
    assert!(delete_resp.error.is_empty());
}

#[test]
fn test_tail_pointer_basic() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    let mut ts = 0;

    // Prewrite
    ts += 1;
    let prewrite_spacelike_version = ts;
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v.clone());
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation],
        k.clone(),
        prewrite_spacelike_version,
    );

    // Commit
    ts += 1;
    let commit_version = ts;
    must_kv_commit(
        &client,
        ctx.clone(),
        vec![k.clone()],
        prewrite_spacelike_version,
        commit_version,
        commit_version,
    );

    // Get
    ts += 1;
    let get_version = ts;
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k.clone();
    get_req.version = get_version;
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(!get_resp.has_brane_error());
    assert!(!get_resp.has_error());
    assert_eq!(get_resp.value, v);

    // Scan
    ts += 1;
    let scan_version = ts;
    let mut scan_req = ScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.spacelike_key = k.clone();
    scan_req.limit = 1;
    scan_req.version = scan_version;
    let scan_resp = client.kv_scan(&scan_req).unwrap();
    assert!(!scan_resp.has_brane_error());
    assert_eq!(scan_resp.pairs.len(), 1);
    for kv in scan_resp.pairs.into_iter() {
        assert!(!kv.has_error());
        assert_eq!(kv.key, k);
        assert_eq!(kv.value, v);
    }

    // Batch get
    ts += 1;
    let batch_get_version = ts;
    let mut batch_get_req = BatchGetRequest::default();
    batch_get_req.set_context(ctx);
    batch_get_req.set_tuplespaceInstanton(vec![k.clone()].into_iter().collect());
    batch_get_req.version = batch_get_version;
    let batch_get_resp = client.kv_batch_get(&batch_get_req).unwrap();
    assert_eq!(batch_get_resp.pairs.len(), 1);
    for kv in batch_get_resp.pairs.into_iter() {
        assert!(!kv.has_error());
        assert_eq!(kv.key, k);
        assert_eq!(kv.value, v);
    }
}

#[test]
fn test_tail_pointer_rollback_and_cleanup() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    let mut ts = 0;

    // Prewrite
    ts += 1;
    let prewrite_spacelike_version = ts;
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v);
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation],
        k.clone(),
        prewrite_spacelike_version,
    );

    // Commit
    ts += 1;
    let commit_version = ts;
    must_kv_commit(
        &client,
        ctx.clone(),
        vec![k.clone()],
        prewrite_spacelike_version,
        commit_version,
        commit_version,
    );

    // Prewrite puts some locks.
    ts += 1;
    let prewrite_spacelike_version2 = ts;
    let (k2, v2) = (b"key2".to_vec(), b"value2".to_vec());
    let mut mut_pri = Mutation::default();
    mut_pri.set_op(Op::Put);
    mut_pri.set_key(k2.clone());
    mut_pri.set_value(v2);
    let mut mut_sec = Mutation::default();
    mut_sec.set_op(Op::Put);
    mut_sec.set_key(k.clone());
    mut_sec.set_value(b"foo".to_vec());
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mut_pri, mut_sec],
        k2.clone(),
        prewrite_spacelike_version2,
    );

    // Scan dagger, expects locks
    ts += 1;
    let scan_lock_max_version = ts;
    let mut scan_lock_req = ScanLockRequest::default();
    scan_lock_req.set_context(ctx.clone());
    scan_lock_req.max_version = scan_lock_max_version;
    let scan_lock_resp = client.kv_scan_lock(&scan_lock_req).unwrap();
    assert!(!scan_lock_resp.has_brane_error());
    assert_eq!(scan_lock_resp.locks.len(), 2);
    for (dagger, key) in scan_lock_resp
        .locks
        .into_iter()
        .zip(vec![k.clone(), k2.clone()])
    {
        assert_eq!(dagger.primary_lock, k2);
        assert_eq!(dagger.key, key);
        assert_eq!(dagger.lock_version, prewrite_spacelike_version2);
    }

    // Rollback
    let rollback_spacelike_version = prewrite_spacelike_version2;
    let mut rollback_req = BatchRollbackRequest::default();
    rollback_req.set_context(ctx.clone());
    rollback_req.spacelike_version = rollback_spacelike_version;
    rollback_req.set_tuplespaceInstanton(vec![k2.clone()].into_iter().collect());
    let rollback_resp = client.kv_batch_rollback(&rollback_req.clone()).unwrap();
    assert!(!rollback_resp.has_brane_error());
    assert!(!rollback_resp.has_error());
    rollback_req.set_tuplespaceInstanton(vec![k].into_iter().collect());
    let rollback_resp2 = client.kv_batch_rollback(&rollback_req).unwrap();
    assert!(!rollback_resp2.has_brane_error());
    assert!(!rollback_resp2.has_error());

    // Cleanup
    let cleanup_spacelike_version = prewrite_spacelike_version2;
    let mut cleanup_req = CleanupRequest::default();
    cleanup_req.set_context(ctx.clone());
    cleanup_req.spacelike_version = cleanup_spacelike_version;
    cleanup_req.set_key(k2);
    let cleanup_resp = client.kv_cleanup(&cleanup_req).unwrap();
    assert!(!cleanup_resp.has_brane_error());
    assert!(!cleanup_resp.has_error());

    // There should be no locks
    ts += 1;
    let scan_lock_max_version2 = ts;
    let mut scan_lock_req = ScanLockRequest::default();
    scan_lock_req.set_context(ctx);
    scan_lock_req.max_version = scan_lock_max_version2;
    let scan_lock_resp = client.kv_scan_lock(&scan_lock_req).unwrap();
    assert!(!scan_lock_resp.has_brane_error());
    assert_eq!(scan_lock_resp.locks.len(), 0);
}

#[test]
fn test_tail_pointer_resolve_lock_gc_and_delete() {
    use ekvproto::kvrpc_timeshare::*;

    let (cluster, client, ctx) = must_new_cluster_and_kv_client();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    let mut ts = 0;

    // Prewrite
    ts += 1;
    let prewrite_spacelike_version = ts;
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v);
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation],
        k.clone(),
        prewrite_spacelike_version,
    );

    // Commit
    ts += 1;
    let commit_version = ts;
    must_kv_commit(
        &client,
        ctx.clone(),
        vec![k.clone()],
        prewrite_spacelike_version,
        commit_version,
        commit_version,
    );

    // Prewrite puts some locks.
    ts += 1;
    let prewrite_spacelike_version2 = ts;
    let (k2, v2) = (b"key2".to_vec(), b"value2".to_vec());
    let new_v = b"new value".to_vec();
    let mut mut_pri = Mutation::default();
    mut_pri.set_op(Op::Put);
    mut_pri.set_key(k.clone());
    mut_pri.set_value(new_v.clone());
    let mut mut_sec = Mutation::default();
    mut_sec.set_op(Op::Put);
    mut_sec.set_key(k2);
    mut_sec.set_value(v2);
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mut_pri, mut_sec],
        k.clone(),
        prewrite_spacelike_version2,
    );

    // Resolve dagger
    ts += 1;
    let resolve_lock_commit_version = ts;
    let mut resolve_lock_req = ResolveLockRequest::default();
    let mut temp_txninfo = TxnInfo::default();
    temp_txninfo.txn = prewrite_spacelike_version2;
    temp_txninfo.status = resolve_lock_commit_version;
    let vec_txninfo = vec![temp_txninfo];
    resolve_lock_req.set_context(ctx.clone());
    resolve_lock_req.set_txn_infos(vec_txninfo.into());
    let resolve_lock_resp = client.kv_resolve_lock(&resolve_lock_req).unwrap();
    assert!(!resolve_lock_resp.has_brane_error());
    assert!(!resolve_lock_resp.has_error());

    // Get `k` at the latest ts.
    ts += 1;
    let get_version1 = ts;
    let mut get_req1 = GetRequest::default();
    get_req1.set_context(ctx.clone());
    get_req1.key = k.clone();
    get_req1.version = get_version1;
    let get_resp1 = client.kv_get(&get_req1).unwrap();
    assert!(!get_resp1.has_brane_error());
    assert!(!get_resp1.has_error());
    assert_eq!(get_resp1.value, new_v);

    // GC `k` at the latest ts.
    ts += 1;
    let gc_safe_ponit = TimeStamp::from(ts);
    let gc_interlock_semaphore = cluster.sim.rl().get_gc_worker(1).interlock_semaphore();
    sync_gc(&gc_interlock_semaphore, 0, vec![], vec![], gc_safe_ponit).unwrap();

    // the `k` at the old ts should be none.
    let get_version2 = commit_version + 1;
    let mut get_req2 = GetRequest::default();
    get_req2.set_context(ctx.clone());
    get_req2.key = k.clone();
    get_req2.version = get_version2;
    let get_resp2 = client.kv_get(&get_req2).unwrap();
    assert!(!get_resp2.has_brane_error());
    assert!(!get_resp2.has_error());
    assert_eq!(get_resp2.value, b"".to_vec());

    // Transaction debugger commands
    // MvccGetByKey
    let mut tail_pointer_get_by_key_req = MvccGetByKeyRequest::default();
    tail_pointer_get_by_key_req.set_context(ctx.clone());
    tail_pointer_get_by_key_req.key = k.clone();
    let tail_pointer_get_by_key_resp = client.tail_pointer_get_by_key(&tail_pointer_get_by_key_req).unwrap();
    assert!(!tail_pointer_get_by_key_resp.has_brane_error());
    assert!(tail_pointer_get_by_key_resp.error.is_empty());
    assert!(tail_pointer_get_by_key_resp.has_info());
    // MvccGetByStartTs
    let mut tail_pointer_get_by_spacelike_ts_req = MvccGetByStartTsRequest::default();
    tail_pointer_get_by_spacelike_ts_req.set_context(ctx.clone());
    tail_pointer_get_by_spacelike_ts_req.spacelike_ts = prewrite_spacelike_version2;
    let tail_pointer_get_by_spacelike_ts_resp = client
        .tail_pointer_get_by_spacelike_ts(&tail_pointer_get_by_spacelike_ts_req)
        .unwrap();
    assert!(!tail_pointer_get_by_spacelike_ts_resp.has_brane_error());
    assert!(tail_pointer_get_by_spacelike_ts_resp.error.is_empty());
    assert!(tail_pointer_get_by_spacelike_ts_resp.has_info());
    assert_eq!(tail_pointer_get_by_spacelike_ts_resp.key, k);

    // Delete cone
    let mut del_req = DeleteConeRequest::default();
    del_req.set_context(ctx);
    del_req.spacelike_key = b"a".to_vec();
    del_req.lightlike_key = b"z".to_vec();
    let del_resp = client.kv_delete_cone(&del_req).unwrap();
    assert!(!del_resp.has_brane_error());
    assert!(del_resp.error.is_empty());
}

// violetabft related RPC is tested as parts of test_snapshot.rs, so skip here.

#[test]
fn test_interlock() {
    let (_cluster, client, _) = must_new_cluster_and_kv_client();
    // SQL push down commands
    let mut req = Request::default();
    req.set_tp(REQ_TYPE_DAG);
    client.interlock(&req).unwrap();
}

#[test]
fn test_split_brane() {
    let (mut cluster, client, ctx) = must_new_cluster_and_kv_client();

    // Split brane commands
    let key = b"b";
    let mut req = SplitBraneRequest::default();
    req.set_context(ctx);
    req.set_split_key(key.to_vec());
    let resp = client.split_brane(&req).unwrap();
    assert_eq!(
        Key::from_encoded(resp.get_left().get_lightlike_key().to_vec())
            .into_raw()
            .unwrap()
            .as_slice(),
        key
    );
    assert_eq!(
        resp.get_left().get_lightlike_key(),
        resp.get_right().get_spacelike_key()
    );

    // Batch split brane
    let brane_id = resp.get_right().get_id();
    let leader = cluster.leader_of_brane(brane_id).unwrap();
    let mut ctx = Context::default();
    ctx.set_brane_id(brane_id);
    ctx.set_peer(leader);
    ctx.set_brane_epoch(resp.get_right().get_brane_epoch().to_owned());
    let mut req = SplitBraneRequest::default();
    req.set_context(ctx);
    let split_tuplespaceInstanton = vec![b"e".to_vec(), b"c".to_vec(), b"d".to_vec()];
    req.set_split_tuplespaceInstanton(split_tuplespaceInstanton.into());
    let resp = client.split_brane(&req).unwrap();
    let result_split_tuplespaceInstanton: Vec<_> = resp
        .get_branes()
        .iter()
        .map(|x| {
            Key::from_encoded(x.get_spacelike_key().to_vec())
                .into_raw()
                .unwrap()
        })
        .collect();
    assert_eq!(
        result_split_tuplespaceInstanton,
        vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]
    );
}

#[test]
fn test_read_index() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    // Read index
    let mut req = ReadIndexRequest::default();
    req.set_context(ctx.clone());
    let mut resp = client.read_index(&req).unwrap();
    let last_index = resp.get_read_index();
    assert_eq!(last_index > 0, true);

    // Raw put
    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx);
    put_req.key = k;
    put_req.value = v;
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_brane_error());
    assert!(put_resp.error.is_empty());

    // Read index again
    resp = client.read_index(&req).unwrap();
    assert_eq!(last_index + 1, resp.get_read_index());
}

#[test]
fn test_debug_get() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();
    let (k, v) = (b"key", b"value");

    // Put some data.
    let engine = cluster.get_engine(store_id);
    let key = tuplespaceInstanton::data_key(k);
    engine.put(&key, v).unwrap();
    assert_eq!(engine.get(&key).unwrap().unwrap(), v);

    // Debug get
    let mut req = debug_timeshare::GetRequest::default();
    req.set_causet(Causet_DEFAULT.to_owned());
    req.set_db(debug_timeshare::Db::Kv);
    req.set_key(key);
    let mut resp = debug_client.get(&req.clone()).unwrap();
    assert_eq!(resp.take_value(), v);

    req.set_key(b"foo".to_vec());
    match debug_client.get(&req).unwrap_err() {
        Error::RpcFailure(status) => {
            assert_eq!(status.status, RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

#[test]
fn test_debug_violetabft_log() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();

    // Put some data.
    let engine = cluster.get_violetabft_engine(store_id);
    let (brane_id, log_index) = (200, 200);
    let key = tuplespaceInstanton::violetabft_log_key(brane_id, log_index);
    let mut entry = evioletabft_timeshare::Entry::default();
    entry.set_term(1);
    entry.set_index(1);
    entry.set_entry_type(evioletabft_timeshare::EntryType::EntryNormal);
    entry.set_data(vec![42]);
    engine.c().put_msg(&key, &entry).unwrap();
    assert_eq!(
        engine.c().get_msg::<evioletabft_timeshare::Entry>(&key).unwrap().unwrap(),
        entry
    );

    // Debug violetabft_log
    let mut req = debug_timeshare::VioletaBftLogRequest::default();
    req.set_brane_id(brane_id);
    req.set_log_index(log_index);
    let resp = debug_client.violetabft_log(&req).unwrap();
    assert_ne!(resp.get_entry(), &evioletabft_timeshare::Entry::default());

    let mut req = debug_timeshare::VioletaBftLogRequest::default();
    req.set_brane_id(brane_id + 1);
    req.set_log_index(brane_id + 1);
    match debug_client.violetabft_log(&req).unwrap_err() {
        Error::RpcFailure(status) => {
            assert_eq!(status.status, RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

#[test]
fn test_debug_brane_info() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();

    let violetabft_engine = cluster.get_violetabft_engine(store_id);
    let kv_engine = cluster.get_engine(store_id);

    let brane_id = 100;
    let violetabft_state_key = tuplespaceInstanton::violetabft_state_key(brane_id);
    let mut violetabft_state = violetabft_server_timeshare::VioletaBftLocalState::default();
    violetabft_state.set_last_index(42);
    violetabft_engine
        .c()
        .put_msg(&violetabft_state_key, &violetabft_state)
        .unwrap();
    assert_eq!(
        violetabft_engine
            .c()
            .get_msg::<violetabft_server_timeshare::VioletaBftLocalState>(&violetabft_state_key)
            .unwrap()
            .unwrap(),
        violetabft_state
    );

    let apply_state_key = tuplespaceInstanton::apply_state_key(brane_id);
    let mut apply_state = violetabft_server_timeshare::VioletaBftApplyState::default();
    apply_state.set_applied_index(42);
    kv_engine
        .c()
        .put_msg_causet(Causet_VIOLETABFT, &apply_state_key, &apply_state)
        .unwrap();
    assert_eq!(
        kv_engine
            .c()
            .get_msg_causet::<violetabft_server_timeshare::VioletaBftApplyState>(Causet_VIOLETABFT, &apply_state_key)
            .unwrap()
            .unwrap(),
        apply_state
    );

    let brane_state_key = tuplespaceInstanton::brane_state_key(brane_id);
    let mut brane_state = violetabft_server_timeshare::BraneLocalState::default();
    brane_state.set_state(violetabft_server_timeshare::PeerState::Tombstone);
    kv_engine
        .c()
        .put_msg_causet(Causet_VIOLETABFT, &brane_state_key, &brane_state)
        .unwrap();
    assert_eq!(
        kv_engine
            .c()
            .get_msg_causet::<violetabft_server_timeshare::BraneLocalState>(Causet_VIOLETABFT, &brane_state_key)
            .unwrap()
            .unwrap(),
        brane_state
    );

    // Debug brane_info
    let mut req = debug_timeshare::BraneInfoRequest::default();
    req.set_brane_id(brane_id);
    let mut resp = debug_client.brane_info(&req.clone()).unwrap();
    assert_eq!(resp.take_violetabft_local_state(), violetabft_state);
    assert_eq!(resp.take_violetabft_apply_state(), apply_state);
    assert_eq!(resp.take_brane_local_state(), brane_state);

    req.set_brane_id(brane_id + 1);
    match debug_client.brane_info(&req).unwrap_err() {
        Error::RpcFailure(status) => {
            assert_eq!(status.status, RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

#[test]
fn test_debug_brane_size() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();
    let engine = cluster.get_engine(store_id);

    // Put some data.
    let brane_id = 100;
    let brane_state_key = tuplespaceInstanton::brane_state_key(brane_id);
    let mut brane = meta_timeshare::Brane::default();
    brane.set_id(brane_id);
    brane.set_spacelike_key(b"a".to_vec());
    brane.set_lightlike_key(b"z".to_vec());
    let mut state = BraneLocalState::default();
    state.set_brane(brane);
    engine
        .c()
        .put_msg_causet(Causet_VIOLETABFT, &brane_state_key, &state)
        .unwrap();

    let causets = vec![Causet_DEFAULT, Causet_DAGGER, Causet_WRITE];
    // At lease 8 bytes for the WRITE causet.
    let (k, v) = (tuplespaceInstanton::data_key(b"kkkk_kkkk"), b"v");
    for causet in &causets {
        let causet_handle = engine.causet_handle(causet).unwrap();
        engine.put_causet(causet_handle, k.as_slice(), v).unwrap();
    }

    let mut req = debug_timeshare::BraneSizeRequest::default();
    req.set_brane_id(brane_id);
    req.set_causets(causets.iter().map(|s| (*s).to_string()).collect());
    let entries: Vec<_> = debug_client
        .brane_size(&req)
        .unwrap()
        .take_entries()
        .into();
    assert_eq!(entries.len(), 3);
    for e in entries {
        causets.iter().find(|&&c| c == e.causet).unwrap();
        assert!(e.size > 0);
    }

    req.set_brane_id(brane_id + 1);
    match debug_client.brane_size(&req).unwrap_err() {
        Error::RpcFailure(status) => {
            assert_eq!(status.status, RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

#[test]
#[causet(feature = "failpoints")]
fn test_debug_fail_point() {
    let (_cluster, debug_client, _) = must_new_cluster_and_debug_client();

    let (fp, act) = ("violetabft_between_save", "off");

    let mut inject_req = debug_timeshare::InjectFailPointRequest::default();
    inject_req.set_name(fp.to_owned());
    inject_req.set_actions(act.to_owned());
    debug_client.inject_fail_point(&inject_req).unwrap();

    let resp = debug_client
        .list_fail_points(&debug_timeshare::ListFailPointsRequest::default())
        .unwrap();
    let entries = resp.get_entries();
    assert!(entries
        .iter()
        .any(|e| e.get_name() == fp && e.get_actions() == act));

    let mut recover_req = debug_timeshare::RecoverFailPointRequest::default();
    recover_req.set_name(fp.to_owned());
    debug_client.recover_fail_point(&recover_req).unwrap();

    let resp = debug_client
        .list_fail_points(&debug_timeshare::ListFailPointsRequest::default())
        .unwrap();
    let entries = resp.get_entries();
    assert!(entries
        .iter()
        .all(|e| !(e.get_name() == fp && e.get_actions() == act)));
}

#[test]
fn test_debug_scan_tail_pointer() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();
    let engine = cluster.get_engine(store_id);

    // Put some data.
    let tuplespaceInstanton = [
        tuplespaceInstanton::data_key(b"meta_lock_1"),
        tuplespaceInstanton::data_key(b"meta_lock_2"),
    ];
    for k in &tuplespaceInstanton {
        let v = Dagger::new(
            LockType::Put,
            b"pk".to_vec(),
            1.into(),
            10,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
        )
        .to_bytes();
        let causet_handle = engine.causet_handle(Causet_DAGGER).unwrap();
        engine.put_causet(causet_handle, k.as_slice(), &v).unwrap();
    }

    let mut req = debug_timeshare::ScanMvccRequest::default();
    req.set_from_key(tuplespaceInstanton::data_key(b"m"));
    req.set_to_key(tuplespaceInstanton::data_key(b"n"));
    req.set_limit(1);

    let receiver = debug_client.scan_tail_pointer(&req).unwrap();
    let future = receiver.try_fold(Vec::new(), |mut tuplespaceInstanton, mut resp| {
        let key = resp.take_key();
        tuplespaceInstanton.push(key);
        future::ok::<_, Error>(tuplespaceInstanton)
    });
    let tuplespaceInstanton = block_on(future).unwrap();
    assert_eq!(tuplespaceInstanton.len(), 1);
    assert_eq!(tuplespaceInstanton[0], tuplespaceInstanton::data_key(b"meta_lock_1"));
}

#[test]
fn test_double_run_node() {
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    cluster.run();
    let id = *cluster.engines.tuplespaceInstanton().next().unwrap();
    let engines = cluster.engines.values().next().unwrap().clone();
    let router = cluster.sim.rl().get_router(id).unwrap();
    let mut sim = cluster.sim.wl();
    let node = sim.get_node(id).unwrap();
    let fidel_worker = FutureWorker::new("test-fidel-worker");
    let simulate_trans = SimulateTransport::new(ChannelTransport::new());
    let tmp = Builder::new().prefix("test_cluster").temfidelir().unwrap();
    let snap_mgr = SnapManager::new(tmp.path().to_str().unwrap());
    let interlock_host = InterlockHost::new(router);
    let importer = {
        let dir = Path::new(engines.kv.path()).join("import-sst");
        Arc::new(SSTImporter::new(dir, None).unwrap())
    };

    let store_meta = Arc::new(Mutex::new(StoreMeta::new(20)));
    let e = node
        .spacelike(
            engines,
            simulate_trans,
            snap_mgr,
            fidel_worker,
            store_meta,
            interlock_host,
            importer,
            Worker::new("split"),
            AutoSplitController::default(),
            ConcurrencyManager::new(1.into()),
        )
        .unwrap_err();
    assert!(format!("{:?}", e).contains("already spacelikeed"), "{:?}", e);
    drop(sim);
    cluster.shutdown();
}

#[test]
fn test_pessimistic_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    // Prewrite
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v.clone());
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), 10);

    // KeyIsLocked
    for &return_values in &[false, true] {
        let resp =
            kv_pessimistic_lock(&client, ctx.clone(), vec![k.clone()], 20, 20, return_values);
        assert!(!resp.has_brane_error(), "{:?}", resp.get_brane_error());
        assert_eq!(resp.errors.len(), 1);
        assert!(resp.errors[0].has_locked());
        assert!(resp.values.is_empty());
    }

    must_kv_commit(&client, ctx.clone(), vec![k.clone()], 10, 30, 30);

    // WriteConflict
    for &return_values in &[false, true] {
        let resp =
            kv_pessimistic_lock(&client, ctx.clone(), vec![k.clone()], 20, 20, return_values);
        assert!(!resp.has_brane_error(), "{:?}", resp.get_brane_error());
        assert_eq!(resp.errors.len(), 1);
        assert!(resp.errors[0].has_conflict());
        assert!(resp.values.is_empty());
    }

    // Return multiple values
    for &return_values in &[false, true] {
        let resp = kv_pessimistic_lock(
            &client,
            ctx.clone(),
            vec![k.clone(), b"nonexsit".to_vec()],
            40,
            40,
            true,
        );
        assert!(!resp.has_brane_error(), "{:?}", resp.get_brane_error());
        assert!(resp.errors.is_empty());
        if return_values {
            assert_eq!(resp.get_values().to_vec(), vec![v.clone(), vec![]]);
        }
        must_kv_pessimistic_rollback(&client, ctx.clone(), k.clone(), 40);
    }
}

#[test]
fn test_check_txn_status_with_max_ts() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let lock_ts = 10;

    // Prewrite
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v.clone());
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), lock_ts);

    // Should return MinCommitTsPushed even if caller_spacelike_ts is max.
    let status = must_check_txn_status(
        &client,
        ctx.clone(),
        &k,
        lock_ts,
        std::u64::MAX,
        lock_ts + 1,
    );
    assert_eq!(status.lock_ttl, 3000);
    assert_eq!(status.action, Action::MinCommitTsPushed);

    // The min_commit_ts of k shouldn't be pushed.
    must_kv_commit(&client, ctx, vec![k], lock_ts, lock_ts + 1, lock_ts + 1);
}
