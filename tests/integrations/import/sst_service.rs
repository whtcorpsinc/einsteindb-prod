//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::executor::block_on;
use futures::{stream, SinkExt};
use tempfile::Builder;
use uuid::Uuid;

use grpcio::{ChannelBuilder, Environment, Result, WriteFlags};
use ekvproto::import_sst_timeshare::*;
use ekvproto::kvrpc_timeshare::*;
use ekvproto::edb_timeshare::*;

use fidel_client::FidelClient;
use test_violetabftstore::*;
use test_sst_importer::*;
use violetabftstore::interlock::::HandyRwLock;

const CLEANUP_SST_MILLIS: u64 = 10;

fn new_cluster() -> (Cluster<ServerCluster>, Context) {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    let cleanup_interval = Duration::from_millis(CLEANUP_SST_MILLIS);
    cluster.causet.violetabft_store.cleanup_import_sst_interval.0 = cleanup_interval;
    cluster.run();

    let brane_id = 1;
    let leader = cluster.leader_of_brane(brane_id).unwrap();
    let epoch = cluster.get_brane_epoch(brane_id);
    let mut ctx = Context::default();
    ctx.set_brane_id(brane_id);
    ctx.set_peer(leader);
    ctx.set_brane_epoch(epoch);

    (cluster, ctx)
}

fn new_cluster_and_edb_import_client(
) -> (Cluster<ServerCluster>, Context, EINSTEINDBClient, ImportSstClient) {
    let (cluster, ctx) = new_cluster();

    let ch = {
        let env = Arc::new(Environment::new(1));
        let node = ctx.get_peer().get_store_id();
        ChannelBuilder::new(env).connect(cluster.sim.rl().get_addr(node))
    };
    let edb = EINSTEINDBClient::new(ch.clone());
    let import = ImportSstClient::new(ch);

    (cluster, ctx, edb, import)
}

#[test]
fn test_upload_sst() {
    let (_cluster, ctx, _, import) = new_cluster_and_edb_import_client();

    let data = vec![1; 1024];
    let crc32 = calc_data_crc32(&data);
    let length = data.len() as u64;

    // Mismatch crc32
    let meta = new_sst_meta(0, length);
    assert!(lightlike_upload_sst(&import, &meta, &data).is_err());

    // Mismatch length
    let meta = new_sst_meta(crc32, 0);
    assert!(lightlike_upload_sst(&import, &meta, &data).is_err());

    let mut meta = new_sst_meta(crc32, length);
    meta.set_brane_id(ctx.get_brane_id());
    meta.set_brane_epoch(ctx.get_brane_epoch().clone());
    lightlike_upload_sst(&import, &meta, &data).unwrap();

    // Can't upload the same uuid file again.
    assert!(lightlike_upload_sst(&import, &meta, &data).is_err());
}

#[test]
fn test_write_sst() {
    let (_cluster, ctx, edb, import) = new_cluster_and_edb_import_client();

    let mut meta = new_sst_meta(0, 0);
    meta.set_brane_id(ctx.get_brane_id());
    meta.set_brane_epoch(ctx.get_brane_epoch().clone());

    let mut tuplespaceInstanton = vec![];
    let mut values = vec![];
    let sst_cone = (0, 10);
    for i in sst_cone.0..sst_cone.1 {
        tuplespaceInstanton.push(vec![i]);
        values.push(vec![i]);
    }
    let resp = lightlike_write_sst(&import, &meta, tuplespaceInstanton, values, 1).unwrap();

    for m in resp.metas.into_iter() {
        let mut ingest = IngestRequest::default();
        ingest.set_context(ctx.clone());
        ingest.set_sst(m.clone());
        let resp = import.ingest(&ingest).unwrap();
        assert!(!resp.has_error());
    }
    check_ingested_txn_kvs(&edb, &ctx, sst_cone, 2);
}

#[test]
fn test_ingest_sst() {
    let (_cluster, ctx, _edb, import) = new_cluster_and_edb_import_client();

    let temp_dir = Builder::new().prefix("test_ingest_sst").temfidelir().unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_cone = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_cone);

    // No brane id and epoch.
    lightlike_upload_sst(&import, &meta, &data).unwrap();

    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta.clone());
    let resp = import.ingest(&ingest).unwrap();
    assert!(resp.has_error());

    // Set brane id and epoch.
    meta.set_brane_id(ctx.get_brane_id());
    meta.set_brane_epoch(ctx.get_brane_epoch().clone());
    lightlike_upload_sst(&import, &meta, &data).unwrap();
    // Can't upload the same file again.
    assert!(lightlike_upload_sst(&import, &meta, &data).is_err());

    ingest.set_sst(meta.clone());
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error());
}

#[test]
fn test_ingest_sst_without_crc32() {
    let (_cluster, ctx, edb, import) = new_cluster_and_edb_import_client();

    let temp_dir = Builder::new()
        .prefix("test_ingest_sst_without_crc32")
        .temfidelir()
        .unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_cone = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_cone);
    meta.set_brane_id(ctx.get_brane_id());
    meta.set_brane_epoch(ctx.get_brane_epoch().clone());

    // Set crc32 == 0 and length != 0 still ingest success
    lightlike_upload_sst(&import, &meta, &data).unwrap();
    meta.set_crc32(0);

    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta.clone());
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error(), "{:?}", resp.get_error());

    // Check ingested kvs
    check_ingested_kvs(&edb, &ctx, sst_cone);
}

#[test]
fn test_download_sst() {
    let (_cluster, ctx, edb, import) = new_cluster_and_edb_import_client();
    let temp_dir = Builder::new()
        .prefix("test_download_sst")
        .temfidelir()
        .unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_cone = (0, 100);
    let (mut meta, _) = gen_sst_file(sst_path, sst_cone);
    meta.set_brane_id(ctx.get_brane_id());
    meta.set_brane_epoch(ctx.get_brane_epoch().clone());

    // Checks that downloading a non-existing causet_storage returns error.
    let mut download = DownloadRequest::default();
    download.set_sst(meta.clone());
    download.set_causet_storage_backlightlike(external_causet_storage::make_local_backlightlike(temp_dir.path()));
    download.set_name("missing.sst".to_owned());

    let result = import.download(&download).unwrap();
    assert!(
        result.has_error(),
        "unexpected download reply: {:?}",
        result
    );

    // Checks that downloading an empty SST returns OK (but cannot be ingested)
    download.set_name("test.sst".to_owned());
    download.mut_sst().mut_cone().set_spacelike(vec![sst_cone.1]);
    download
        .mut_sst()
        .mut_cone()
        .set_lightlike(vec![sst_cone.1 + 1]);
    let result = import.download(&download).unwrap();
    assert!(result.get_is_empty());

    // Now perform a proper download.
    download.mut_sst().mut_cone().set_spacelike(Vec::new());
    download.mut_sst().mut_cone().set_lightlike(Vec::new());
    let result = import.download(&download).unwrap();
    assert!(!result.get_is_empty());
    assert_eq!(result.get_cone().get_spacelike(), &[sst_cone.0]);
    assert_eq!(result.get_cone().get_lightlike(), &[sst_cone.1 - 1]);

    // Do an ingest and verify the result is correct.

    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta);
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error());

    check_ingested_kvs(&edb, &ctx, sst_cone);
}

#[test]
fn test_cleanup_sst() {
    let (mut cluster, ctx, _, import) = new_cluster_and_edb_import_client();

    let temp_dir = Builder::new().prefix("test_cleanup_sst").temfidelir().unwrap();

    let sst_path = temp_dir.path().join("test_split.sst");
    let sst_cone = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_cone);
    meta.set_brane_id(ctx.get_brane_id());
    meta.set_brane_epoch(ctx.get_brane_epoch().clone());

    lightlike_upload_sst(&import, &meta, &data).unwrap();

    // Can not upload the same file when it exists.
    assert!(lightlike_upload_sst(&import, &meta, &data).is_err());

    // The uploaded SST should be deleted if the brane split.
    let brane = cluster.get_brane(&[]);
    cluster.must_split(&brane, &[100]);

    check_sst_deleted(&import, &meta, &data);

    let left = cluster.get_brane(&[]);
    let right = cluster.get_brane(&[100]);

    let sst_path = temp_dir.path().join("test_merge.sst");
    let sst_cone = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_cone);
    meta.set_brane_id(left.get_id());
    meta.set_brane_epoch(left.get_brane_epoch().clone());

    lightlike_upload_sst(&import, &meta, &data).unwrap();

    // The uploaded SST should be deleted if the brane merged.
    cluster.fidel_client.must_merge(left.get_id(), right.get_id());
    let res = block_on(cluster.fidel_client.get_brane_by_id(left.get_id()));
    assert!(res.unwrap().is_none());

    check_sst_deleted(&import, &meta, &data);
}

#[test]
fn test_ingest_sst_brane_not_found() {
    let (_cluster, mut ctx_not_found, _, import) = new_cluster_and_edb_import_client();

    let temp_dir = Builder::new()
        .prefix("test_ingest_sst_errors")
        .temfidelir()
        .unwrap();

    ctx_not_found.set_brane_id(1 << 31); // A large brane id that must no exists.
    let sst_path = temp_dir.path().join("test_split.sst");
    let sst_cone = (0, 100);
    let (mut meta, _data) = gen_sst_file(sst_path, sst_cone);
    meta.set_brane_id(ctx_not_found.get_brane_id());
    meta.set_brane_epoch(ctx_not_found.get_brane_epoch().clone());

    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx_not_found);
    ingest.set_sst(meta);
    let resp = import.ingest(&ingest).unwrap();
    assert!(resp.get_error().has_brane_not_found());
}

fn new_sst_meta(crc32: u32, length: u64) -> SstMeta {
    let mut m = SstMeta::default();
    m.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    m.set_crc32(crc32);
    m.set_length(length);
    m
}

fn lightlike_upload_sst(
    client: &ImportSstClient,
    meta: &SstMeta,
    data: &[u8],
) -> Result<UploadResponse> {
    let mut r1 = UploadRequest::default();
    r1.set_meta(meta.clone());
    let mut r2 = UploadRequest::default();
    r2.set_data(data.to_vec());
    let reqs: Vec<_> = vec![r1, r2]
        .into_iter()
        .map(|r| Result::Ok((r, WriteFlags::default())))
        .collect();
    let (mut tx, rx) = client.upload().unwrap();
    let mut stream = stream::iter(reqs);
    block_on(tx.lightlike_all(&mut stream)).unwrap();
    block_on(tx.close()).unwrap();
    block_on(rx)
}

fn lightlike_write_sst(
    client: &ImportSstClient,
    meta: &SstMeta,
    tuplespaceInstanton: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    commit_ts: u64,
) -> Result<WriteResponse> {
    let mut r1 = WriteRequest::default();
    r1.set_meta(meta.clone());
    let mut r2 = WriteRequest::default();

    let mut batch = WriteBatch::default();
    let mut pairs = vec![];

    for (i, key) in tuplespaceInstanton.iter().enumerate() {
        let mut pair = Pair::default();
        pair.set_key(key.to_vec());
        pair.set_value(values[i].to_vec());
        pairs.push(pair);
    }
    batch.set_commit_ts(commit_ts);
    batch.set_pairs(pairs.into());
    r2.set_batch(batch);

    let reqs: Vec<_> = vec![r1, r2]
        .into_iter()
        .map(|r| Result::Ok((r, WriteFlags::default())))
        .collect();

    let (mut tx, rx) = client.write().unwrap();
    let mut stream = stream::iter(reqs);
    block_on(tx.lightlike_all(&mut stream)).unwrap();
    block_on(tx.close()).unwrap();
    block_on(rx)
}

fn check_ingested_kvs(edb: &EINSTEINDBClient, ctx: &Context, sst_cone: (u8, u8)) {
    for i in sst_cone.0..sst_cone.1 {
        let mut m = RawGetRequest::default();
        m.set_context(ctx.clone());
        m.set_key(vec![i]);
        let resp = edb.raw_get(&m).unwrap();
        assert!(resp.get_error().is_empty());
        assert!(!resp.has_brane_error());
        assert_eq!(resp.get_value(), &[i]);
    }
}

fn check_ingested_txn_kvs(edb: &EINSTEINDBClient, ctx: &Context, sst_cone: (u8, u8), spacelike_ts: u64) {
    for i in sst_cone.0..sst_cone.1 {
        let mut m = GetRequest::default();
        m.set_context(ctx.clone());
        m.set_key(vec![i]);
        m.set_version(spacelike_ts);
        let resp = edb.kv_get(&m).unwrap();
        assert!(!resp.has_brane_error());
        assert_eq!(resp.get_value(), &[i]);
    }
}

fn check_sst_deleted(client: &ImportSstClient, meta: &SstMeta, data: &[u8]) {
    for _ in 0..10 {
        if lightlike_upload_sst(client, meta, data).is_ok() {
            // If we can upload the file, it means the previous file has been deleted.
            return;
        }
        thread::sleep(Duration::from_millis(CLEANUP_SST_MILLIS));
    }
    lightlike_upload_sst(client, meta, data).unwrap();
}
