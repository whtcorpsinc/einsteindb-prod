// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use ekvproto::interlock::{KeyCone, Request};
use ekvproto::kvrpc_timeshare::{Context, IsolationLevel};
use protobuf::Message;
use fidel_timeshare::{
    AnalyzePrimaryCausetsReq, AnalyzePrimaryCausetsResp, AnalyzeIndexReq, AnalyzeIndexResp, AnalyzeReq,
    AnalyzeType,
};

use test_interlock::*;

pub const REQ_TYPE_ANALYZE: i64 = 104;

fn new_analyze_req(data: Vec<u8>, cone: KeyCone, spacelike_ts: u64) -> Request {
    let mut req = Request::default();
    req.set_data(data);
    req.set_cones(vec![cone].into());
    req.set_spacelike_ts(spacelike_ts);
    req.set_tp(REQ_TYPE_ANALYZE);
    req
}

fn new_analyze_PrimaryCauset_req(
    Block: &Block,
    PrimaryCausets_info_len: usize,
    bucket_size: i64,
    fm_sketch_size: i64,
    sample_size: i64,
    cm_sketch_depth: i32,
    cm_sketch_width: i32,
) -> Request {
    let mut col_req = AnalyzePrimaryCausetsReq::default();
    col_req.set_PrimaryCausets_info(Block.PrimaryCausets_info()[..PrimaryCausets_info_len].into());
    col_req.set_bucket_size(bucket_size);
    col_req.set_sketch_size(fm_sketch_size);
    col_req.set_sample_size(sample_size);
    col_req.set_cmsketch_depth(cm_sketch_depth);
    col_req.set_cmsketch_width(cm_sketch_width);
    let mut analy_req = AnalyzeReq::default();
    analy_req.set_tp(AnalyzeType::TypePrimaryCauset);
    analy_req.set_col_req(col_req);
    new_analyze_req(
        analy_req.write_to_bytes().unwrap(),
        Block.get_record_cone_all(),
        next_id() as u64,
    )
}

fn new_analyze_index_req(
    Block: &Block,
    bucket_size: i64,
    idx: i64,
    cm_sketch_depth: i32,
    cm_sketch_width: i32,
) -> Request {
    let mut idx_req = AnalyzeIndexReq::default();
    idx_req.set_num_PrimaryCausets(2);
    idx_req.set_bucket_size(bucket_size);
    idx_req.set_cmsketch_depth(cm_sketch_depth);
    idx_req.set_cmsketch_width(cm_sketch_width);
    let mut analy_req = AnalyzeReq::default();
    analy_req.set_tp(AnalyzeType::TypeIndex);
    analy_req.set_idx_req(idx_req);
    new_analyze_req(
        analy_req.write_to_bytes().unwrap(),
        Block.get_index_cone_all(idx),
        next_id() as u64,
    )
}

#[test]
fn test_analyze_PrimaryCauset_with_lock() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductBlock::new();
    for &iso_level in &[IsolationLevel::Si, IsolationLevel::Rc] {
        let (_, lightlikepoint) = init_data_with_commit(&product, &data, false);

        let mut req = new_analyze_PrimaryCauset_req(&product, 3, 3, 3, 3, 4, 32);
        let mut ctx = Context::default();
        ctx.set_isolation_level(iso_level);
        req.set_context(ctx);

        let resp = handle_request(&lightlikepoint, req);
        match iso_level {
            IsolationLevel::Si => {
                assert!(resp.get_data().is_empty(), "{:?}", resp);
                assert!(resp.has_locked(), "{:?}", resp);
            }
            IsolationLevel::Rc => {
                let mut analyze_resp = AnalyzePrimaryCausetsResp::default();
                analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
                let hist = analyze_resp.get_pk_hist();
                assert!(hist.get_buckets().is_empty());
                assert_eq!(hist.get_ndv(), 0);
            }
        }
    }
}

#[test]
fn test_analyze_PrimaryCauset() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, None, 4),
    ];

    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_data_with_commit(&product, &data, true);

    let req = new_analyze_PrimaryCauset_req(&product, 3, 3, 3, 3, 4, 32);
    let resp = handle_request(&lightlikepoint, req);
    assert!(!resp.get_data().is_empty());
    let mut analyze_resp = AnalyzePrimaryCausetsResp::default();
    analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
    let hist = analyze_resp.get_pk_hist();
    assert_eq!(hist.get_buckets().len(), 2);
    assert_eq!(hist.get_ndv(), 4);
    let collectors = analyze_resp.get_collectors().to_vec();
    assert_eq!(collectors.len(), product.PrimaryCausets_info().len() - 1);
    assert_eq!(collectors[0].get_null_count(), 1);
    assert_eq!(collectors[0].get_count(), 3);
    let events = collectors[0].get_cm_sketch().get_rows();
    assert_eq!(events.len(), 4);
    let sum: u32 = events.first().unwrap().get_counters().iter().sum();
    assert_eq!(sum, 3);
}

#[test]
fn test_analyze_single_primary_PrimaryCauset() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, None, 4),
    ];

    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_data_with_commit(&product, &data, true);

    let req = new_analyze_PrimaryCauset_req(&product, 1, 3, 3, 3, 4, 32);
    let resp = handle_request(&lightlikepoint, req);
    assert!(!resp.get_data().is_empty());
    let mut analyze_resp = AnalyzePrimaryCausetsResp::default();
    analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
    let hist = analyze_resp.get_pk_hist();
    assert_eq!(hist.get_buckets().len(), 2);
    assert_eq!(hist.get_ndv(), 4);
    let collectors = analyze_resp.get_collectors().to_vec();
    assert_eq!(collectors.len(), 0);
}

#[test]
fn test_analyze_index_with_lock() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductBlock::new();
    for &iso_level in &[IsolationLevel::Si, IsolationLevel::Rc] {
        let (_, lightlikepoint) = init_data_with_commit(&product, &data, false);

        let mut req = new_analyze_index_req(&product, 3, product["name"].index, 4, 32);
        let mut ctx = Context::default();
        ctx.set_isolation_level(iso_level);
        req.set_context(ctx);

        let resp = handle_request(&lightlikepoint, req);
        match iso_level {
            IsolationLevel::Si => {
                assert!(resp.get_data().is_empty(), "{:?}", resp);
                assert!(resp.has_locked(), "{:?}", resp);
            }
            IsolationLevel::Rc => {
                let mut analyze_resp = AnalyzeIndexResp::default();
                analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
                let hist = analyze_resp.get_hist();
                assert!(hist.get_buckets().is_empty());
                assert_eq!(hist.get_ndv(), 0);
            }
        }
    }
}

#[test]
fn test_analyze_index() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, None, 4),
    ];

    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_data_with_commit(&product, &data, true);

    let req = new_analyze_index_req(&product, 3, product["name"].index, 4, 32);
    let resp = handle_request(&lightlikepoint, req);
    assert!(!resp.get_data().is_empty());
    let mut analyze_resp = AnalyzeIndexResp::default();
    analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
    let hist = analyze_resp.get_hist();
    assert_eq!(hist.get_ndv(), 4);
    assert_eq!(hist.get_buckets().len(), 2);
    let events = analyze_resp.get_cms().get_rows();
    assert_eq!(events.len(), 4);
    let sum: u32 = events.first().unwrap().get_counters().iter().sum();
    assert_eq!(sum, 8);
}

#[test]
fn test_invalid_cone() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductBlock::new();
    let (_, lightlikepoint) = init_data_with_commit(&product, &data, true);
    let mut req = new_analyze_index_req(&product, 3, product["name"].index, 4, 32);
    let mut key_cone = KeyCone::default();
    key_cone.set_spacelike(b"xxx".to_vec());
    key_cone.set_lightlike(b"zzz".to_vec());
    req.set_cones(vec![key_cone].into());
    let resp = handle_request(&lightlikepoint, req);
    assert!(!resp.get_other_error().is_empty());
}
