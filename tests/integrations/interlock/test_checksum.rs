//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::u64;

use ekvproto::interlock::{KeyCone, Request};
use ekvproto::kvrpc_timeshare::{Context, IsolationLevel};
use protobuf::Message;
use fidel_timeshare::{ChecksumAlgorithm, ChecksumRequest, ChecksumResponse, ChecksumScanOn};

use test_interlock::*;
use milevadb_query_common::causet_storage::scanner::{ConesScanner, ConesScannerOptions};
use milevadb_query_common::causet_storage::Cone;
use edb::interlock::posetdag::EinsteinDBStorage;
use edb::interlock::*;
use edb::causet_storage::{Engine, SnapshotStore};
use txn_types::TimeStamp;

fn new_checksum_request(cone: KeyCone, scan_on: ChecksumScanOn) -> Request {
    let mut ctx = Context::default();
    ctx.set_isolation_level(IsolationLevel::Si);

    let mut checksum = ChecksumRequest::default();
    checksum.set_scan_on(scan_on);
    checksum.set_algorithm(ChecksumAlgorithm::Crc64Xor);

    let mut req = Request::default();
    req.set_spacelike_ts(u64::MAX);
    req.set_context(ctx);
    req.set_tp(REQ_TYPE_CHECKSUM);
    req.set_data(checksum.write_to_bytes().unwrap());
    req.mut_cones().push(cone);
    req
}

#[test]
fn test_checksum() {
    let data = vec![
        (1, Some("name:1"), 1),
        (2, Some("name:2"), 2),
        (3, Some("name:3"), 3),
        (4, Some("name:4"), 4),
    ];

    let product = ProductBlock::new();
    let (store, lightlikepoint) = init_data_with_commit(&product, &data, true);

    for PrimaryCauset in &[&product["id"], &product["name"], &product["count"]] {
        assert!(PrimaryCauset.index >= 0);
        let (cone, scan_on) = if PrimaryCauset.index == 0 {
            let cone = product.get_record_cone_all();
            (cone, ChecksumScanOn::Block)
        } else {
            let cone = product.get_index_cone_all(PrimaryCauset.index);
            (cone, ChecksumScanOn::Index)
        };
        let request = new_checksum_request(cone.clone(), scan_on);
        let expected = reversed_checksum_crc64_xor(&store, cone);

        let response = handle_request(&lightlikepoint, request);
        let mut resp = ChecksumResponse::default();
        resp.merge_from_bytes(response.get_data()).unwrap();
        assert_eq!(resp.get_checksum(), expected);
        assert_eq!(resp.get_total_kvs(), data.len() as u64);
    }
}

fn reversed_checksum_crc64_xor<E: Engine>(store: &CausetStore<E>, cone: KeyCone) -> u64 {
    let ctx = Context::default();
    let store = SnapshotStore::new(
        store.get_engine().snapshot(&ctx).unwrap(),
        TimeStamp::max(),
        IsolationLevel::Si,
        true,
        Default::default(),
        false,
    );
    let mut scanner = ConesScanner::new(ConesScannerOptions {
        causet_storage: EinsteinDBStorage::new(store, false),
        cones: vec![Cone::from__timeshare_cone(cone, false)],
        scan_backward_in_cone: true,
        is_key_only: false,
        is_scanned_cone_aware: false,
    });

    let mut checksum = 0;
    let digest = crc64fast::Digest::new();
    while let Some((k, v)) = scanner.next().unwrap() {
        let mut digest = digest.clone();
        digest.write(&k);
        digest.write(&v);
        checksum ^= digest.sum64();
    }
    checksum
}
