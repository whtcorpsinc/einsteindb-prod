// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use ekvproto::kvrpcpb::{Context, IsolationLevel};
use protobuf::Message;
use fidelpb::SelectResponse;

use test_interlock::*;
use test_causetStorage::*;

#[test]
fn test_deadline() {
    let product = ProductTable::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causetg("deadline_check_fail", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_other_error().contains("exceeding the deadline"));
}

#[test]
fn test_deadline_2() {
    // It should not even take any snapshots when request is outdated from the beginning.
    let product = ProductTable::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causetg("rockskv_async_snapshot", "panic").unwrap();
    fail::causetg("deadline_check_fail", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_other_error().contains("exceeding the deadline"));
}

/// Test deadline exceeded when request is handling
/// Note: only
#[test]
fn test_deadline_3() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, lightlikepoint) = {
        let engine = einsteindb::causetStorage::TestEngineBuilder::new().build().unwrap();
        let mut causetg = einsteindb::server::Config::default();
        causetg.lightlike_point_request_max_handle_duration = einsteindb_util::config::ReadableDuration::secs(1);
        init_data_with_details(Context::default(), engine, &product, &data, true, &causetg)
    };
    let req = DAGSelect::from(&product).build();

    fail::causetg("kv_cursor_seek", "sleep(2000)").unwrap();
    fail::causetg("copr_batch_initial_size", "return(1)").unwrap();
    let cop_resp = handle_request(&lightlikepoint, req);
    let mut resp = SelectResponse::default();
    resp.merge_from_bytes(cop_resp.get_data()).unwrap();

    // Errors during evaluation becomes an eval error.
    assert!(resp
        .get_error()
        .get_msg()
        .contains("exceeding the deadline"));
}

#[test]
fn test_parse_request_failed() {
    let product = ProductTable::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causetg("interlock_parse_request", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_other_error().contains("unsupported tp"));
}

#[test]
fn test_parse_request_failed_2() {
    // It should not even take any snapshots when parse failed.
    let product = ProductTable::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causetg("rockskv_async_snapshot", "panic").unwrap();
    fail::causetg("interlock_parse_request", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_other_error().contains("unsupported tp"));
}

#[test]
fn test_readpool_full() {
    let product = ProductTable::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causetg("future_pool_spawn_full", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_brane_error().has_server_is_busy());
}

#[test]
fn test_snapshot_failed() {
    let product = ProductTable::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causetg("rockskv_async_snapshot", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_other_error().contains("snapshot failed"));
}

#[test]
fn test_snapshot_failed_2() {
    let product = ProductTable::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causetg("rockskv_async_snapshot_not_leader", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_brane_error().has_not_leader());
}

#[test]
fn test_causetStorage_error() {
    let data = vec![(1, Some("name:0"), 2), (2, Some("name:4"), 3)];

    let product = ProductTable::new();
    let (_, lightlikepoint) = init_with_data(&product, &data);
    let req = DAGSelect::from(&product).build();

    fail::causetg("kv_cursor_seek", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_other_error().contains("kv cursor seek error"));
}

#[test]
fn test_brane_error_in_scan() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_cluster, raft_engine, mut ctx) = new_raft_engine(1, "");
    ctx.set_isolation_level(IsolationLevel::Si);

    let (_, lightlikepoint) =
        init_data_with_engine_and_commit(ctx.clone(), raft_engine, &product, &data, true);

    fail::causetg("brane_snapshot_seek", "return()").unwrap();
    let req = DAGSelect::from(&product).build_with(ctx, &[0]);
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp
        .get_brane_error()
        .get_message()
        .contains("brane seek error"));
}
