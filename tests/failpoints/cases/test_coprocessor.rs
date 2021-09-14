// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use ekvproto::kvrpc_timeshare::{Context, IsolationLevel};
use protobuf::Message;
use fidel_timeshare::SelectResponse;

use test_interlock::*;
use test_causet_storage::*;

#[test]
fn test_deadline() {
    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causet("deadline_check_fail", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_other_error().contains("exceeding the deadline"));
}

#[test]
fn test_deadline_2() {
    // It should not even take any snapshots when request is outdated from the beginning.
    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causet("rockskv_async_snapshot", "panic").unwrap();
    fail::causet("deadline_check_fail", "return()").unwrap();
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

    let product = ProductBlock::new();
    let (_, lightlikepoint) = {
        let engine = edb::causet_storage::TestEngineBuilder::new().build().unwrap();
        let mut causet = edb::server::Config::default();
        causet.lightlike_point_request_max_handle_duration = violetabftstore::interlock::::config::ReadableDuration::secs(1);
        init_data_with_details(Context::default(), engine, &product, &data, true, &causet)
    };
    let req = DAGSelect::from(&product).build();

    fail::causet("kv_cursor_seek", "sleep(2000)").unwrap();
    fail::causet("copr_batch_initial_size", "return(1)").unwrap();
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
    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causet("interlock_parse_request", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_other_error().contains("unsupported tp"));
}

#[test]
fn test_parse_request_failed_2() {
    // It should not even take any snapshots when parse failed.
    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causet("rockskv_async_snapshot", "panic").unwrap();
    fail::causet("interlock_parse_request", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_other_error().contains("unsupported tp"));
}

#[test]
fn test_readpool_full() {
    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causet("future_pool_spawn_full", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_brane_error().has_server_is_busy());
}

#[test]
fn test_snapshot_failed() {
    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causet("rockskv_async_snapshot", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_other_error().contains("snapshot failed"));
}

#[test]
fn test_snapshot_failed_2() {
    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::causet("rockskv_async_snapshot_not_leader", "return()").unwrap();
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp.get_brane_error().has_not_leader());
}

#[test]
fn test_causet_storage_error() {
    let data = vec![(1, Some("name:0"), 2), (2, Some("name:4"), 3)];

    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_with_data(&product, &data);
    let req = DAGSelect::from(&product).build();

    fail::causet("kv_cursor_seek", "return()").unwrap();
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

    let product = ProductBlock::new();
    let (_cluster, violetabft_engine, mut ctx) = new_violetabft_engine(1, "");
    ctx.set_isolation_level(IsolationLevel::Si);

    let (_, lightlikepoint) =
        init_data_with_engine_and_commit(ctx.clone(), violetabft_engine, &product, &data, true);

    fail::causet("brane_snapshot_seek", "return()").unwrap();
    let req = DAGSelect::from(&product).build_with(ctx, &[0]);
    let resp = handle_request(&lightlikepoint, req);

    assert!(resp
        .get_brane_error()
        .get_message()
        .contains("brane seek error"));
}
