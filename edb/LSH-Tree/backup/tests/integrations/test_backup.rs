// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::*;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use futures::channel::mpsc as future_mpsc;
use futures::StreamExt;
use futures_executor::block_on;
use futures_util::io::AsyncReadExt;
use grpcio::{ChannelBuilder, Environment};

use backup::Task;
use interlocking_directorate::ConcurrencyManager;
use edb::IterOptions;
use edb::{CfName, Causet_DEFAULT, Causet_WRITE, DATA_KEY_PREFIX_LEN};
use external_causet_storage::*;
use ekvproto::backup::*;
use ekvproto::import_sst_timeshare::*;
use ekvproto::kvrpc_timeshare::*;
use ekvproto::violetabft_cmd_timeshare::{CmdType, VioletaBftCmdRequest, VioletaBftRequestHeader, Request};
use ekvproto::edb_timeshare::EINSTEINDBClient;
use fidel_client::FidelClient;
use tempfile::Builder;
use test_violetabftstore::*;
use milevadb_query_common::causet_storage::scanner::{ConesScanner, ConesScannerOptions};
use milevadb_query_common::causet_storage::{IntervalCone, Cone};
use edb::config::BackupConfig;
use edb::interlock::checksum_crc64_xor;
use edb::interlock::posetdag::EinsteinDBStorage;
use edb::causet_storage::kv::Engine;
use edb::causet_storage::SnapshotStore;
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::file::calc_crc32_bytes;
use violetabftstore::interlock::::worker::Worker;
use violetabftstore::interlock::::HandyRwLock;
use txn_types::TimeStamp;

struct TestSuite {
    cluster: Cluster<ServerCluster>,
    lightlikepoints: HashMap<u64, Worker<Task>>,
    edb_cli: EINSTEINDBClient,
    context: Context,
    ts: TimeStamp,

    _env: Arc<Environment>,
}

// Retry if encounter error
macro_rules! retry_req {
    ($call_req: expr, $check_resp: expr, $resp:ident, $retry:literal, $timeout:literal) => {
        let spacelike = Instant::now();
        let timeout = Duration::from_millis($timeout);
        let mut tried_times = 0;
        while tried_times < $retry || spacelike.elapsed() < timeout {
            if $check_resp {
                break;
            } else {
                thread::sleep(Duration::from_millis(200));
                tried_times += 1;
                $resp = $call_req;
                continue;
            }
        }
    };
}

impl TestSuite {
    fn new(count: usize) -> TestSuite {
        super::init();
        let mut cluster = new_server_cluster(1, count);
        // Increase the VioletaBft tick interval to make this test case running reliably.
        configure_for_lease_read(&mut cluster, Some(100), None);
        cluster.run();

        let interlocking_directorate =
            ConcurrencyManager::new(block_on(cluster.fidel_client.get_tso()).unwrap());
        let mut lightlikepoints = HashMap::default();
        for (id, engines) in &cluster.engines {
            // Create and run backup lightlikepoints.
            let sim = cluster.sim.rl();
            let backup_lightlikepoint = backup::node::new(
                *id,
                sim.causet_storages[&id].clone(),
                sim.brane_info_accessors[&id].clone(),
                engines.kv.as_inner().clone(),
                BackupConfig { num_threads: 4 },
                interlocking_directorate.clone(),
            );
            let mut worker = Worker::new(format!("backup-{}", id));
            worker.spacelike(backup_lightlikepoint).unwrap();
            lightlikepoints.insert(*id, worker);
        }

        // Make sure there is a leader.
        cluster.must_put(b"foo", b"foo");
        let brane_id = 1;
        let leader = cluster.leader_of_brane(brane_id).unwrap();
        let leader_addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();

        let epoch = cluster.get_brane_epoch(brane_id);
        let mut context = Context::default();
        context.set_brane_id(brane_id);
        context.set_peer(leader);
        context.set_brane_epoch(epoch);

        let env = Arc::new(Environment::new(1));
        let channel = ChannelBuilder::new(env.clone()).connect(&leader_addr);
        let edb_cli = EINSTEINDBClient::new(channel);

        TestSuite {
            cluster,
            lightlikepoints,
            edb_cli,
            context,
            ts: TimeStamp::zero(),
            _env: env,
        }
    }

    fn alloc_ts(&mut self) -> TimeStamp {
        *self.ts.incr()
    }

    fn stop(mut self) {
        for (_, mut worker) in self.lightlikepoints {
            worker.stop().unwrap();
        }
        self.cluster.shutdown();
    }

    fn must_raw_put(&self, k: Vec<u8>, v: Vec<u8>, causet: String) {
        let mut request = RawPutRequest::default();
        request.set_context(self.context.clone());
        request.set_key(k);
        request.set_value(v);
        request.set_causet(causet);
        let mut response = self.edb_cli.raw_put(&request).unwrap();
        retry_req!(
            self.edb_cli.raw_put(&request).unwrap(),
            !response.has_brane_error() && response.error.is_empty(),
            response,
            10,   // retry 10 times
            1000  // 1s timeout
        );
        assert!(
            !response.has_brane_error(),
            "{:?}",
            response.get_brane_error(),
        );
        assert!(response.error.is_empty(), "{:?}", response.get_error());
    }

    fn must_kv_prewrite(&self, muts: Vec<Mutation>, pk: Vec<u8>, ts: TimeStamp) {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.context.clone());
        prewrite_req.set_mutations(muts.into_iter().collect());
        prewrite_req.primary_lock = pk;
        prewrite_req.spacelike_version = ts.into_inner();
        prewrite_req.lock_ttl = prewrite_req.spacelike_version + 1;
        let mut prewrite_resp = self.edb_cli.kv_prewrite(&prewrite_req).unwrap();
        retry_req!(
            self.edb_cli.kv_prewrite(&prewrite_req).unwrap(),
            !prewrite_resp.has_brane_error() && prewrite_resp.errors.is_empty(),
            prewrite_resp,
            10,   // retry 10 times
            3000  // 3s timeout
        );
        assert!(
            !prewrite_resp.has_brane_error(),
            "{:?}",
            prewrite_resp.get_brane_error()
        );
        assert!(
            prewrite_resp.errors.is_empty(),
            "{:?}",
            prewrite_resp.get_errors()
        );
    }

    fn must_kv_commit(&self, tuplespaceInstanton: Vec<Vec<u8>>, spacelike_ts: TimeStamp, commit_ts: TimeStamp) {
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(self.context.clone());
        commit_req.spacelike_version = spacelike_ts.into_inner();
        commit_req.set_tuplespaceInstanton(tuplespaceInstanton.into_iter().collect());
        commit_req.commit_version = commit_ts.into_inner();
        let mut commit_resp = self.edb_cli.kv_commit(&commit_req).unwrap();
        retry_req!(
            self.edb_cli.kv_commit(&commit_req).unwrap(),
            !commit_resp.has_brane_error() && !commit_resp.has_error(),
            commit_resp,
            10,   // retry 10 times
            3000  // 3s timeout
        );
        assert!(
            !commit_resp.has_brane_error(),
            "{:?}",
            commit_resp.get_brane_error()
        );
        assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    }

    fn backup(
        &self,
        spacelike_key: Vec<u8>,
        lightlike_key: Vec<u8>,
        begin_ts: TimeStamp,
        backup_ts: TimeStamp,
        path: &Path,
    ) -> future_mpsc::UnboundedReceiver<BackupResponse> {
        let mut req = BackupRequest::default();
        req.set_spacelike_key(spacelike_key);
        req.set_lightlike_key(lightlike_key);
        req.spacelike_version = begin_ts.into_inner();
        req.lightlike_version = backup_ts.into_inner();
        req.set_causet_storage_backlightlike(make_local_backlightlike(path));
        req.set_is_raw_kv(false);
        let (tx, rx) = future_mpsc::unbounded();
        for lightlike in self.lightlikepoints.values() {
            let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
            lightlike.schedule(task).unwrap();
        }
        rx
    }

    fn backup_raw(
        &self,
        spacelike_key: Vec<u8>,
        lightlike_key: Vec<u8>,
        causet: String,
        path: &Path,
    ) -> future_mpsc::UnboundedReceiver<BackupResponse> {
        let mut req = BackupRequest::default();
        req.set_spacelike_key(spacelike_key);
        req.set_lightlike_key(lightlike_key);
        req.set_causet_storage_backlightlike(make_local_backlightlike(path));
        req.set_is_raw_kv(true);
        req.set_causet(causet);
        let (tx, rx) = future_mpsc::unbounded();
        for lightlike in self.lightlikepoints.values() {
            let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
            lightlike.schedule(task).unwrap();
        }
        rx
    }

    fn admin_checksum(&self, backup_ts: TimeStamp, spacelike: String, lightlike: String) -> (u64, u64, u64) {
        let mut checksum = 0;
        let mut total_kvs = 0;
        let mut total_bytes = 0;
        let sim = self.cluster.sim.rl();
        let engine = sim.causet_storages[&self.context.get_peer().get_store_id()].clone();
        let snapshot = engine.snapshot(&self.context.clone()).unwrap();
        let snap_store = SnapshotStore::new(
            snapshot,
            backup_ts,
            IsolationLevel::Si,
            false,
            Default::default(),
            false,
        );
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: EinsteinDBStorage::new(snap_store, false),
            cones: vec![Cone::Interval(IntervalCone::from((spacelike, lightlike)))],
            scan_backward_in_cone: false,
            is_key_only: false,
            is_scanned_cone_aware: false,
        });
        let digest = crc64fast::Digest::new();
        while let Some((k, v)) = scanner.next().unwrap() {
            checksum = checksum_crc64_xor(checksum, digest.clone(), &k, &v);
            total_kvs += 1;
            total_bytes += (k.len() + v.len()) as u64;
        }
        (checksum, total_kvs, total_bytes)
    }

    fn gen_raw_kv(&self, key_idx: u64) -> (String, String) {
        (format!("key_{}", key_idx), format!("value_{}", key_idx))
    }

    fn raw_kv_checksum(&self, spacelike: String, lightlike: String, causet: CfName) -> (u64, u64, u64) {
        let spacelike = spacelike.into_bytes();
        let lightlike = lightlike.into_bytes();

        let mut total_kvs = 0;
        let mut total_bytes = 0;

        let sim = self.cluster.sim.rl();
        let engine = sim.causet_storages[&self.context.get_peer().get_store_id()].clone();
        let snapshot = engine.snapshot(&self.context.clone()).unwrap();
        let mut iter_opt = IterOptions::default();
        if !lightlike.is_empty() {
            iter_opt.set_upper_bound(&lightlike, DATA_KEY_PREFIX_LEN);
        }
        let mut iter = snapshot.iter_causet(causet, iter_opt).unwrap();

        if !iter.seek(&spacelike).unwrap() {
            return (0, 0, 0);
        }
        while iter.valid().unwrap() {
            total_kvs += 1;
            total_bytes += (iter.key().len() + iter.value().len()) as u64;
            iter.next().unwrap();
        }
        (0, total_kvs, total_bytes)
    }
}

// Extrat Causet name from sst name.
fn name_to_causet(name: &str) -> CfName {
    if name.contains(Causet_DEFAULT) {
        Causet_DEFAULT
    } else if name.contains(Causet_WRITE) {
        Causet_WRITE
    } else {
        unreachable!()
    }
}

#[test]
fn test_backup_and_import() {
    let mut suite = TestSuite::new(3);

    // 3 version for each key.
    for _ in 0..3 {
        // 60 tuplespaceInstanton.
        for i in 0..60 {
            let (k, v) = (format!("key_{}", i), format!("value_{}", i));
            // Prewrite
            let spacelike_ts = suite.alloc_ts();
            let mut mutation = Mutation::default();
            mutation.set_op(Op::Put);
            mutation.key = k.clone().into_bytes();
            mutation.value = v.clone().into_bytes();
            suite.must_kv_prewrite(vec![mutation], k.clone().into_bytes(), spacelike_ts);
            // Commit
            let commit_ts = suite.alloc_ts();
            suite.must_kv_commit(vec![k.clone().into_bytes()], spacelike_ts, commit_ts);
        }
    }

    // Push down backup request.
    let tmp = Builder::new().temfidelir().unwrap();
    let backup_ts = suite.alloc_ts();
    let causet_storage_path = tmp.path().join(format!("{}", backup_ts));
    let rx = suite.backup(
        vec![],   // spacelike
        vec![],   // lightlike
        0.into(), // begin_ts
        backup_ts,
        &causet_storage_path,
    );
    let resps1 = block_on(rx.collect::<Vec<_>>());
    // Only leader can handle backup.
    assert_eq!(resps1.len(), 1);
    let files1 = resps1[0].files.clone();
    // Short value is piggybacked in write causet, so we get 1 sst at least.
    assert!(!resps1[0].get_files().is_empty());

    // Delete all data, there should be no backup files.
    suite.cluster.must_delete_cone_causet(Causet_DEFAULT, b"", b"");
    suite.cluster.must_delete_cone_causet(Causet_WRITE, b"", b"");
    // Backup file should have same contents.
    // backup ts + 1 avoid file already exist.
    let rx = suite.backup(
        vec![],   // spacelike
        vec![],   // lightlike
        0.into(), // begin_ts
        backup_ts,
        &tmp.path().join(format!("{}", backup_ts.next())),
    );
    let resps2 = block_on(rx.collect::<Vec<_>>());
    assert!(resps2[0].get_files().is_empty(), "{:?}", resps2);

    // Use importer to restore backup files.
    let backlightlike = make_local_backlightlike(&causet_storage_path);
    let causet_storage = create_causet_storage(&backlightlike).unwrap();
    let brane = suite.cluster.get_brane(b"");
    let mut sst_meta = SstMeta::default();
    sst_meta.brane_id = brane.get_id();
    sst_meta.set_brane_epoch(brane.get_brane_epoch().clone());
    sst_meta.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    let mut metas = vec![];
    for f in files1.clone().into_iter() {
        let mut reader = causet_storage.read(&f.name);
        let mut content = vec![];
        block_on(reader.read_to_lightlike(&mut content)).unwrap();
        let mut m = sst_meta.clone();
        m.crc32 = calc_crc32_bytes(&content);
        m.length = content.len() as _;
        m.causet_name = name_to_causet(&f.name).to_owned();
        metas.push((m, content));
    }

    for (m, c) in &metas {
        for importer in suite.cluster.sim.rl().importers.values() {
            let mut f = importer.create(m).unwrap();
            f.applightlike(c).unwrap();
            f.finish().unwrap();
        }

        // Make ingest command.
        let mut ingest = Request::default();
        ingest.set_cmd_type(CmdType::IngestSst);
        ingest.mut_ingest_sst().set_sst(m.clone());
        let mut header = VioletaBftRequestHeader::default();
        let leader = suite.context.get_peer().clone();
        header.set_peer(leader);
        header.set_brane_id(suite.context.get_brane_id());
        header.set_brane_epoch(suite.context.get_brane_epoch().clone());
        let mut cmd = VioletaBftCmdRequest::default();
        cmd.set_header(header);
        cmd.mut_requests().push(ingest);
        let resp = suite
            .cluster
            .call_command_on_leader(cmd, Duration::from_secs(5))
            .unwrap();
        assert!(!resp.get_header().has_error(), resp);
    }

    // Backup file should have same contents.
    // backup ts + 2 avoid file already exist.
    let rx = suite.backup(
        vec![],   // spacelike
        vec![],   // lightlike
        0.into(), // begin_ts
        backup_ts,
        &tmp.path().join(format!("{}", backup_ts.next().next())),
    );
    let resps3 = block_on(rx.collect::<Vec<_>>());
    assert_eq!(files1, resps3[0].files);

    suite.stop();
}

#[test]
fn test_backup_meta() {
    let mut suite = TestSuite::new(3);
    let key_count = 60;

    // 3 version for each key.
    for _ in 0..3 {
        for i in 0..key_count {
            let (k, v) = (format!("key_{}", i), format!("value_{}", i));
            // Prewrite
            let spacelike_ts = suite.alloc_ts();
            let mut mutation = Mutation::default();
            mutation.set_op(Op::Put);
            mutation.key = k.clone().into_bytes();
            mutation.value = v.clone().into_bytes();
            suite.must_kv_prewrite(vec![mutation], k.clone().into_bytes(), spacelike_ts);
            // Commit
            let commit_ts = suite.alloc_ts();
            suite.must_kv_commit(vec![k.clone().into_bytes()], spacelike_ts, commit_ts);
        }
    }
    let backup_ts = suite.alloc_ts();
    // key are order by lexicographical order, 'a'-'z' will cover all
    let (admin_checksum, admin_total_kvs, admin_total_bytes) =
        suite.admin_checksum(backup_ts, "a".to_owned(), "z".to_owned());

    // Push down backup request.
    let tmp = Builder::new().temfidelir().unwrap();
    let causet_storage_path = tmp.path().join(format!("{}", backup_ts));
    let rx = suite.backup(
        vec![],   // spacelike
        vec![],   // lightlike
        0.into(), // begin_ts
        backup_ts,
        &causet_storage_path,
    );
    let resps1 = block_on(rx.collect::<Vec<_>>());
    // Only leader can handle backup.
    assert_eq!(resps1.len(), 1);
    let files: Vec<_> = resps1[0].files.clone().into_iter().collect();
    // Short value is piggybacked in write causet, so we get 1 sst at least.
    assert!(!files.is_empty());
    let mut checksum = 0;
    let mut total_kvs = 0;
    let mut total_bytes = 0;
    for f in files {
        checksum ^= f.get_crc64xor();
        total_kvs += f.get_total_kvs();
        total_bytes += f.get_total_bytes();
    }
    assert_eq!(total_kvs, key_count);
    assert_eq!(total_kvs, admin_total_kvs);
    assert_eq!(total_bytes, admin_total_bytes);
    assert_eq!(checksum, admin_checksum);

    suite.stop();
}

#[test]
fn test_backup_rawkv() {
    let mut suite = TestSuite::new(3);
    let key_count = 60;

    let causet = String::from(Causet_DEFAULT);
    for i in 0..key_count {
        let (k, v) = suite.gen_raw_kv(i);
        suite.must_raw_put(k.clone().into_bytes(), v.clone().into_bytes(), causet.clone());
    }

    // Push down backup request.
    let tmp = Builder::new().temfidelir().unwrap();
    let backup_ts = suite.alloc_ts();
    let causet_storage_path = tmp.path().join(format!("{}", backup_ts));
    let rx = suite.backup_raw(
        vec![b'a'], // spacelike
        vec![b'z'], // lightlike
        causet.clone(),
        &causet_storage_path,
    );
    let resps1 = block_on(rx.collect::<Vec<_>>());
    // Only leader can handle backup.
    assert_eq!(resps1.len(), 1);
    let files1 = resps1[0].files.clone();
    assert!(!resps1[0].get_files().is_empty());

    // Delete all data, there should be no backup files.
    suite.cluster.must_delete_cone_causet(Causet_DEFAULT, b"", b"");
    // Backup file should have same contents.
    // backup ts + 1 avoid file already exist.
    let rx = suite.backup_raw(
        vec![], // spacelike
        vec![], // lightlike
        causet.clone(),
        &tmp.path().join(format!("{}", backup_ts.next())),
    );
    let resps2 = block_on(rx.collect::<Vec<_>>());
    assert!(resps2[0].get_files().is_empty(), "{:?}", resps2);

    // Use importer to restore backup files.
    let backlightlike = make_local_backlightlike(&causet_storage_path);
    let causet_storage = create_causet_storage(&backlightlike).unwrap();
    let brane = suite.cluster.get_brane(b"");
    let mut sst_meta = SstMeta::default();
    sst_meta.brane_id = brane.get_id();
    sst_meta.set_brane_epoch(brane.get_brane_epoch().clone());
    sst_meta.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    let mut metas = vec![];
    for f in files1.clone().into_iter() {
        let mut reader = causet_storage.read(&f.name);
        let mut content = vec![];
        block_on(reader.read_to_lightlike(&mut content)).unwrap();
        let mut m = sst_meta.clone();
        m.crc32 = calc_crc32_bytes(&content);
        m.length = content.len() as _;
        m.causet_name = name_to_causet(&f.name).to_owned();
        metas.push((m, content));
    }

    for (m, c) in &metas {
        for importer in suite.cluster.sim.rl().importers.values() {
            let mut f = importer.create(m).unwrap();
            f.applightlike(c).unwrap();
            f.finish().unwrap();
        }

        // Make ingest command.
        let mut ingest = Request::default();
        ingest.set_cmd_type(CmdType::IngestSst);
        ingest.mut_ingest_sst().set_sst(m.clone());
        let mut header = VioletaBftRequestHeader::default();
        let leader = suite.context.get_peer().clone();
        header.set_peer(leader);
        header.set_brane_id(suite.context.get_brane_id());
        header.set_brane_epoch(suite.context.get_brane_epoch().clone());
        let mut cmd = VioletaBftCmdRequest::default();
        cmd.set_header(header);
        cmd.mut_requests().push(ingest);
        let resp = suite
            .cluster
            .call_command_on_leader(cmd, Duration::from_secs(5))
            .unwrap();
        assert!(!resp.get_header().has_error(), resp);
    }

    // Backup file should have same contents.
    // backup ts + 2 avoid file already exist.
    // Set non-empty cone to check if it's incorrectly encoded.
    let rx = suite.backup_raw(
        vec![b'a'], // spacelike
        vec![b'z'], // lightlike
        causet,
        &tmp.path().join(format!("{}", backup_ts.next().next())),
    );
    let resps3 = block_on(rx.collect::<Vec<_>>());
    assert_eq!(files1, resps3[0].files);

    suite.stop();
}

#[test]
fn test_backup_raw_meta() {
    let mut suite = TestSuite::new(3);
    let key_count: u64 = 60;
    let causet = String::from(Causet_DEFAULT);

    for i in 0..key_count {
        let (k, v) = suite.gen_raw_kv(i);
        suite.must_raw_put(k.clone().into_bytes(), v.clone().into_bytes(), causet.clone());
    }
    let backup_ts = suite.alloc_ts();
    // TuplespaceInstanton are order by lexicographical order, 'a'-'z' will cover all.
    let (admin_checksum, admin_total_kvs, admin_total_bytes) =
        suite.raw_kv_checksum("a".to_owned(), "z".to_owned(), Causet_DEFAULT);

    // Push down backup request.
    let tmp = Builder::new().temfidelir().unwrap();
    let causet_storage_path = tmp.path().join(format!("{}", backup_ts));
    let rx = suite.backup_raw(
        vec![], // spacelike
        vec![], // lightlike
        causet,
        &causet_storage_path,
    );
    let resps1 = block_on(rx.collect::<Vec<_>>());
    // Only leader can handle backup.
    assert_eq!(resps1.len(), 1);
    let files: Vec<_> = resps1[0].files.clone().into_iter().collect();
    // Short value is piggybacked in write causet, so we get 1 sst at least.
    assert!(!files.is_empty());
    let mut checksum = 0;
    let mut total_kvs = 0;
    let mut total_bytes = 0;
    let mut total_size = 0;
    for f in files {
        checksum ^= f.get_crc64xor();
        total_kvs += f.get_total_kvs();
        total_bytes += f.get_total_bytes();
        total_size += f.get_size();
    }
    assert_eq!(total_kvs, key_count + 1);
    assert_eq!(total_kvs, admin_total_kvs);
    assert_eq!(total_bytes, admin_total_bytes);
    assert_eq!(checksum, admin_checksum);
    assert_eq!(total_size, 1611);
    // please fidelio this number (must be > 0) when the test failed

    suite.stop();
}
