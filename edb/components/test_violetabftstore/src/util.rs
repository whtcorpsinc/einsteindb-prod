//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
use std::{thread, u64};

use rand::RngCore;
use tempfile::{Builder, TempDir};

use ekvproto::encryption_timeshare::EncryptionMethod;
use ekvproto::kvrpc_timeshare::*;
use ekvproto::meta_timeshare::{self, BraneEpoch};
use ekvproto::fidel_timeshare::{
    ChangePeer, CheckPolicy, Merge, BraneHeartbeatResponse, SplitBrane, TransferLeader,
};
use ekvproto::violetabft_cmd_timeshare::{AdminCmdType, CmdType, StatusCmdType};
use ekvproto::violetabft_cmd_timeshare::{AdminRequest, VioletaBftCmdRequest, VioletaBftCmdResponse, Request, StatusRequest};
use ekvproto::violetabft_server_timeshare::{PeerState, VioletaBftLocalState, BraneLocalState};
use ekvproto::edb_timeshare::EINSTEINDBClient;
use violetabft::evioletabft_timeshare::ConfChangeType;

use encryption::{DataKeyManager, FileConfig, MasterKeyConfig};
use engine_lmdb::config::BlobRunMode;
use engine_lmdb::encryption::get_env;
use engine_lmdb::raw::DB;
use engine_lmdb::{CompactionListener, LmdbCompactionJobInfo};
use engine_lmdb::{Compat, LmdbEngine, LmdbSnapshot};
use edb::{Engines, Iterable, Peekable};
use violetabftstore::store::fsm::VioletaBftRouter;
use violetabftstore::store::*;
use violetabftstore::Result;
use edb::config::*;
use edb::causet_storage::config::DEFAULT_LMDB_SUB_DIR;
use violetabftstore::interlock::::config::*;
use violetabftstore::interlock::::{escape, HandyRwLock};

use super::*;

use edb::{ALL_CausetS, Causet_DEFAULT, Causet_VIOLETABFT};
pub use violetabftstore::store::util::{find_peer, new_learner_peer, new_peer};
use violetabftstore::interlock::::time::ThreadReadId;

pub fn must_get(engine: &Arc<DB>, causet: &str, key: &[u8], value: Option<&[u8]>) {
    for _ in 1..300 {
        let res = engine.c().get_value_causet(causet, &tuplespaceInstanton::data_key(key)).unwrap();
        if let (Some(value), Some(res)) = (value, res.as_ref()) {
            assert_eq!(value, &res[..]);
            return;
        }
        if value.is_none() && res.is_none() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    debug!("last try to get {}", hex::encode_upper(key));
    let res = engine.c().get_value_causet(causet, &tuplespaceInstanton::data_key(key)).unwrap();
    if value.is_none() && res.is_none()
        || value.is_some() && res.is_some() && value.unwrap() == &*res.unwrap()
    {
        return;
    }
    panic!(
        "can't get value {:?} for key {}",
        value.map(escape),
        hex::encode_upper(key)
    )
}

pub fn must_get_equal(engine: &Arc<DB>, key: &[u8], value: &[u8]) {
    must_get(engine, "default", key, Some(value));
}

pub fn must_get_none(engine: &Arc<DB>, key: &[u8]) {
    must_get(engine, "default", key, None);
}

pub fn must_get_causet_equal(engine: &Arc<DB>, causet: &str, key: &[u8], value: &[u8]) {
    must_get(engine, causet, key, Some(value));
}

pub fn must_get_causet_none(engine: &Arc<DB>, causet: &str, key: &[u8]) {
    must_get(engine, causet, key, None);
}

pub fn must_brane_cleared(engine: &Engines<LmdbEngine, LmdbEngine>, brane: &meta_timeshare::Brane) {
    let id = brane.get_id();
    let state_key = tuplespaceInstanton::brane_state_key(id);
    let state: BraneLocalState = engine.kv.get_msg_causet(Causet_VIOLETABFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone, "{:?}", state);
    let spacelike_key = tuplespaceInstanton::data_key(brane.get_spacelike_key());
    let lightlike_key = tuplespaceInstanton::data_key(brane.get_lightlike_key());
    for causet in ALL_CausetS {
        engine
            .kv
            .scan_causet(causet, &spacelike_key, &lightlike_key, false, |k, v| {
                panic!(
                    "[brane {}] unexpected ({:?}, {:?}) in causet {:?}",
                    id, k, v, causet
                );
            })
            .unwrap();
    }
    let log_min_key = tuplespaceInstanton::violetabft_log_key(id, 0);
    let log_max_key = tuplespaceInstanton::violetabft_log_key(id, u64::MAX);
    engine
        .violetabft
        .scan(&log_min_key, &log_max_key, false, |k, v| {
            panic!("[brane {}] unexpected log ({:?}, {:?})", id, k, v);
        })
        .unwrap();
    let state_key = tuplespaceInstanton::violetabft_state_key(id);
    let state: Option<VioletaBftLocalState> = engine.violetabft.get_msg(&state_key).unwrap();
    assert!(
        state.is_none(),
        "[brane {}] violetabft state key should be removed: {:?}",
        id,
        state
    );
}

lazy_static! {
    static ref TEST_CONFIG: EINSTEINDBConfig = {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let common_test_causet = manifest_dir.join("src/common-test.toml");
        EINSTEINDBConfig::from_file(&common_test_causet, None)
    };
}

pub fn new_edb_config(cluster_id: u64) -> EINSTEINDBConfig {
    let mut causet = TEST_CONFIG.clone();
    causet.server.cluster_id = cluster_id;
    causet
}

// Create a base request.
pub fn new_base_request(brane_id: u64, epoch: BraneEpoch, read_quorum: bool) -> VioletaBftCmdRequest {
    let mut req = VioletaBftCmdRequest::default();
    req.mut_header().set_brane_id(brane_id);
    req.mut_header().set_brane_epoch(epoch);
    req.mut_header().set_read_quorum(read_quorum);
    req
}

pub fn new_request(
    brane_id: u64,
    epoch: BraneEpoch,
    requests: Vec<Request>,
    read_quorum: bool,
) -> VioletaBftCmdRequest {
    let mut req = new_base_request(brane_id, epoch, read_quorum);
    req.set_requests(requests.into());
    req
}

pub fn new_put_cmd(key: &[u8], value: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Put);
    cmd.mut_put().set_key(key.to_vec());
    cmd.mut_put().set_value(value.to_vec());
    cmd
}

pub fn new_put_causet_cmd(causet: &str, key: &[u8], value: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Put);
    cmd.mut_put().set_key(key.to_vec());
    cmd.mut_put().set_value(value.to_vec());
    cmd.mut_put().set_causet(causet.to_string());
    cmd
}

pub fn new_get_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Get);
    cmd.mut_get().set_key(key.to_vec());
    cmd
}

pub fn new_snap_cmd() -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Snap);
    cmd
}

pub fn new_read_index_cmd() -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::ReadIndex);
    cmd
}

pub fn new_get_causet_cmd(causet: &str, key: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Get);
    cmd.mut_get().set_key(key.to_vec());
    cmd.mut_get().set_causet(causet.to_string());
    cmd
}

pub fn new_delete_cmd(causet: &str, key: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Delete);
    cmd.mut_delete().set_key(key.to_vec());
    cmd.mut_delete().set_causet(causet.to_string());
    cmd
}

pub fn new_delete_cone_cmd(causet: &str, spacelike: &[u8], lightlike: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::DeleteCone);
    cmd.mut_delete_cone().set_spacelike_key(spacelike.to_vec());
    cmd.mut_delete_cone().set_lightlike_key(lightlike.to_vec());
    cmd.mut_delete_cone().set_causet(causet.to_string());
    cmd
}

pub fn new_status_request(
    brane_id: u64,
    peer: meta_timeshare::Peer,
    request: StatusRequest,
) -> VioletaBftCmdRequest {
    let mut req = new_base_request(brane_id, BraneEpoch::default(), false);
    req.mut_header().set_peer(peer);
    req.set_status_request(request);
    req
}

pub fn new_brane_detail_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::default();
    cmd.set_cmd_type(StatusCmdType::BraneDetail);
    cmd
}

pub fn new_brane_leader_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::default();
    cmd.set_cmd_type(StatusCmdType::BraneLeader);
    cmd
}

pub fn new_admin_request(
    brane_id: u64,
    epoch: &BraneEpoch,
    request: AdminRequest,
) -> VioletaBftCmdRequest {
    let mut req = new_base_request(brane_id, epoch.clone(), false);
    req.set_admin_request(request);
    req
}

pub fn new_change_peer_request(change_type: ConfChangeType, peer: meta_timeshare::Peer) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::ChangePeer);
    req.mut_change_peer().set_change_type(change_type);
    req.mut_change_peer().set_peer(peer);
    req
}

pub fn new_compact_log_request(index: u64, term: u64) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::CompactLog);
    req.mut_compact_log().set_compact_index(index);
    req.mut_compact_log().set_compact_term(term);
    req
}

pub fn new_transfer_leader_cmd(peer: meta_timeshare::Peer) -> AdminRequest {
    let mut cmd = AdminRequest::default();
    cmd.set_cmd_type(AdminCmdType::TransferLeader);
    cmd.mut_transfer_leader().set_peer(peer);
    cmd
}

#[allow(dead_code)]
pub fn new_prepare_merge(target_brane: meta_timeshare::Brane) -> AdminRequest {
    let mut cmd = AdminRequest::default();
    cmd.set_cmd_type(AdminCmdType::PrepareMerge);
    cmd.mut_prepare_merge().set_target(target_brane);
    cmd
}

pub fn new_store(store_id: u64, addr: String) -> meta_timeshare::CausetStore {
    let mut store = meta_timeshare::CausetStore::default();
    store.set_id(store_id);
    store.set_address(addr);

    store
}

pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

pub fn is_error_response(resp: &VioletaBftCmdResponse) -> bool {
    resp.get_header().has_error()
}

pub fn new_fidel_change_peer(
    change_type: ConfChangeType,
    peer: meta_timeshare::Peer,
) -> BraneHeartbeatResponse {
    let mut change_peer = ChangePeer::default();
    change_peer.set_change_type(change_type);
    change_peer.set_peer(peer);

    let mut resp = BraneHeartbeatResponse::default();
    resp.set_change_peer(change_peer);
    resp
}

pub fn new_split_brane(policy: CheckPolicy, tuplespaceInstanton: Vec<Vec<u8>>) -> BraneHeartbeatResponse {
    let mut split_brane = SplitBrane::default();
    split_brane.set_policy(policy);
    split_brane.set_tuplespaceInstanton(tuplespaceInstanton.into());
    let mut resp = BraneHeartbeatResponse::default();
    resp.set_split_brane(split_brane);
    resp
}

pub fn new_fidel_transfer_leader(peer: meta_timeshare::Peer) -> BraneHeartbeatResponse {
    let mut transfer_leader = TransferLeader::default();
    transfer_leader.set_peer(peer);

    let mut resp = BraneHeartbeatResponse::default();
    resp.set_transfer_leader(transfer_leader);
    resp
}

pub fn new_fidel_merge_brane(target_brane: meta_timeshare::Brane) -> BraneHeartbeatResponse {
    let mut merge = Merge::default();
    merge.set_target(target_brane);

    let mut resp = BraneHeartbeatResponse::default();
    resp.set_merge(merge);
    resp
}

pub fn make_cb(cmd: &VioletaBftCmdRequest) -> (Callback<LmdbSnapshot>, mpsc::Receiver<VioletaBftCmdResponse>) {
    let mut is_read;
    let mut is_write;
    is_read = cmd.has_status_request();
    is_write = cmd.has_admin_request();
    for req in cmd.get_requests() {
        match req.get_cmd_type() {
            CmdType::Get | CmdType::Snap | CmdType::ReadIndex => is_read = true,
            CmdType::Put | CmdType::Delete | CmdType::DeleteCone | CmdType::IngestSst => {
                is_write = true
            }
            CmdType::Invalid | CmdType::Prewrite => panic!("Invalid VioletaBftCmdRequest: {:?}", cmd),
        }
    }
    assert!(is_read ^ is_write, "Invalid VioletaBftCmdRequest: {:?}", cmd);

    let (tx, rx) = mpsc::channel();
    let cb = if is_read {
        Callback::Read(Box::new(move |resp: ReadResponse<LmdbSnapshot>| {
            // we don't care error actually.
            let _ = tx.lightlike(resp.response);
        }))
    } else {
        Callback::Write(Box::new(move |resp: WriteResponse| {
            // we don't care error actually.
            let _ = tx.lightlike(resp.response);
        }))
    };
    (cb, rx)
}

// Issue a read request on the specified peer.
pub fn read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: meta_timeshare::Peer,
    brane: meta_timeshare::Brane,
    key: &[u8],
    read_quorum: bool,
    timeout: Duration,
) -> Result<VioletaBftCmdResponse> {
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_get_cmd(key)],
        read_quorum,
    );
    request.mut_header().set_peer(peer);
    cluster.read(None, request, timeout)
}

pub fn async_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: meta_timeshare::Peer,
    brane: meta_timeshare::Brane,
    key: &[u8],
    read_quorum: bool,
    replica_read: bool,
) -> mpsc::Receiver<VioletaBftCmdResponse> {
    let node_id = peer.get_store_id();
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_get_cmd(key)],
        read_quorum,
    );
    request.mut_header().set_peer(peer);
    request.mut_header().set_replica_read(replica_read);
    let (tx, rx) = mpsc::sync_channel(1);
    let cb = Callback::Read(Box::new(move |resp| drop(tx.lightlike(resp.response))));
    cluster.sim.wl().async_read(node_id, None, request, cb);
    rx
}

pub fn batch_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    requests: &[(meta_timeshare::Peer, meta_timeshare::Brane)],
) -> Vec<ReadResponse<LmdbSnapshot>> {
    let batch_id = Some(ThreadReadId::new());
    let (tx, rx) = mpsc::sync_channel(3);
    let mut results = vec![];
    let mut len = 0;
    for (peer, brane) in requests {
        let node_id = peer.get_store_id();
        let mut request = new_request(
            brane.get_id(),
            brane.get_brane_epoch().clone(),
            vec![new_snap_cmd()],
            false,
        );
        request.mut_header().set_peer(peer.clone());
        let t = tx.clone();
        let cb = Callback::Read(Box::new(move |resp| {
            t.lightlike((len, resp)).unwrap();
        }));
        cluster
            .sim
            .wl()
            .async_read(node_id, batch_id.clone(), request, cb);
        len += 1;
    }
    while results.len() < len {
        results.push(rx.recv_timeout(Duration::from_secs(1)).unwrap());
    }
    results.sort_by_key(|resp| resp.0);
    results.into_iter().map(|resp| resp.1).collect()
}

pub fn read_index_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: meta_timeshare::Peer,
    brane: meta_timeshare::Brane,
    read_quorum: bool,
    timeout: Duration,
) -> Result<VioletaBftCmdResponse> {
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_read_index_cmd()],
        read_quorum,
    );
    request.mut_header().set_peer(peer);
    cluster.read(None, request, timeout)
}

pub fn must_get_value(resp: &VioletaBftCmdResponse) -> Vec<u8> {
    if resp.get_header().has_error() {
        panic!("failed to read {:?}", resp);
    }
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Get);
    assert!(resp.get_responses()[0].has_get());
    resp.get_responses()[0].get_get().get_value().to_vec()
}

pub fn must_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: meta_timeshare::Peer,
    brane: meta_timeshare::Brane,
    key: &[u8],
    value: &[u8],
) {
    let timeout = Duration::from_secs(5);
    match read_on_peer(cluster, peer, brane, key, false, timeout) {
        Ok(ref resp) if value == must_get_value(resp).as_slice() => (),
        other => panic!(
            "read key {}, expect value {:?}, got {:?}",
            hex::encode_upper(key),
            value,
            other
        ),
    }
}

pub fn must_error_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: meta_timeshare::Peer,
    brane: meta_timeshare::Brane,
    key: &[u8],
    timeout: Duration,
) {
    if let Ok(mut resp) = read_on_peer(cluster, peer, brane, key, false, timeout) {
        if !resp.get_header().has_error() {
            let value = resp.mut_responses()[0].mut_get().take_value();
            panic!(
                "key {}, expect error but got {}",
                hex::encode_upper(key),
                escape(&value)
            );
        }
    }
}

fn dummpy_filter(_: &LmdbCompactionJobInfo) -> bool {
    true
}

pub fn create_test_engine(
    // TODO: pass it in for all cases.
    router: Option<VioletaBftRouter<LmdbEngine, LmdbEngine>>,
    causet: &EINSTEINDBConfig,
) -> (
    Engines<LmdbEngine, LmdbEngine>,
    Option<Arc<DataKeyManager>>,
    TempDir,
) {
    let dir = Builder::new().prefix("test_cluster").temfidelir().unwrap();
    let key_manager =
        DataKeyManager::from_config(&causet.security.encryption, dir.path().to_str().unwrap())
            .unwrap()
            .map(|key_manager| Arc::new(key_manager));

    let env = get_env(key_manager.clone(), None).unwrap();
    let cache = causet.causet_storage.block_cache.build_shared_cache();

    let kv_path = dir.path().join(DEFAULT_LMDB_SUB_DIR);
    let kv_path_str = kv_path.to_str().unwrap();

    let mut kv_db_opt = causet.lmdb.build_opt();
    kv_db_opt.set_env(env.clone());

    if let Some(router) = router {
        let router = Mutex::new(router);
        let cmpacted_handler = Box::new(move |event| {
            router
                .dagger()
                .unwrap()
                .lightlike_control(StoreMsg::CompactedEvent(event))
                .unwrap();
        });
        kv_db_opt.add_event_listener(CompactionListener::new(
            cmpacted_handler,
            Some(dummpy_filter),
        ));
    }

    let kv_causets_opt = causet.lmdb.build_causet_opts(&cache);

    let engine = Arc::new(
        engine_lmdb::raw_util::new_engine_opt(kv_path_str, kv_db_opt, kv_causets_opt).unwrap(),
    );

    let violetabft_path = dir.path().join("violetabft");
    let violetabft_path_str = violetabft_path.to_str().unwrap();

    let mut violetabft_db_opt = causet.violetabftdb.build_opt();
    violetabft_db_opt.set_env(env);

    let violetabft_causets_opt = causet.violetabftdb.build_causet_opts(&cache);
    let violetabft_engine = Arc::new(
        engine_lmdb::raw_util::new_engine_opt(violetabft_path_str, violetabft_db_opt, violetabft_causets_opt).unwrap(),
    );

    let mut engine = LmdbEngine::from_db(engine);
    let mut violetabft_engine = LmdbEngine::from_db(violetabft_engine);
    let shared_block_cache = cache.is_some();
    engine.set_shared_block_cache(shared_block_cache);
    violetabft_engine.set_shared_block_cache(shared_block_cache);
    let engines = Engines::new(engine, violetabft_engine);
    (engines, key_manager, dir)
}

pub fn configure_for_request_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    // We don't want to generate snapshots due to compact log.
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 1000;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 1000;
    cluster.causet.violetabft_store.violetabft_log_gc_size_limit = ReadableSize::mb(20);
}

pub fn configure_for_hibernate<T: Simulator>(cluster: &mut Cluster<T>) {
    // Uses long check interval to make leader keep sleeping during tests.
    cluster.causet.violetabft_store.abnormal_leader_missing_duration = ReadableDuration::secs(20);
    cluster.causet.violetabft_store.max_leader_missing_duration = ReadableDuration::secs(40);
    cluster.causet.violetabft_store.peer_stale_state_check_interval = ReadableDuration::secs(10);
}

pub fn configure_for_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    // Truncate the log quickly so that we can force lightlikeing snapshot.
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 2;
    cluster.causet.violetabft_store.merge_max_log_gap = 1;
    cluster.causet.violetabft_store.snap_mgr_gc_tick_interval = ReadableDuration::millis(50);
}

pub fn configure_for_merge<T: Simulator>(cluster: &mut Cluster<T>) {
    // Avoid log compaction which will prevent merge.
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 1000;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 1000;
    cluster.causet.violetabft_store.violetabft_log_gc_size_limit = ReadableSize::mb(20);
    // Make merge check resume quickly.
    cluster.causet.violetabft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    // When isolated, follower relies on stale check tick to detect failure leader,
    // choose a smaller number to make it recover faster.
    cluster.causet.violetabft_store.peer_stale_state_check_interval = ReadableDuration::millis(500);
}

pub fn ignore_merge_target_integrity<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.causet.violetabft_store.dev_assert = false;
    cluster.fidel_client.ignore_merge_target_integrity();
}

pub fn configure_for_transfer_leader<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.causet.violetabft_store.violetabft_reject_transfer_leader_duration = ReadableDuration::secs(1);
}

pub fn configure_for_lease_read<T: Simulator>(
    cluster: &mut Cluster<T>,
    base_tick_ms: Option<u64>,
    election_ticks: Option<usize>,
) -> Duration {
    if let Some(base_tick_ms) = base_tick_ms {
        cluster.causet.violetabft_store.violetabft_base_tick_interval = ReadableDuration::millis(base_tick_ms);
    }
    let base_tick_interval = cluster.causet.violetabft_store.violetabft_base_tick_interval.0;
    if let Some(election_ticks) = election_ticks {
        cluster.causet.violetabft_store.violetabft_election_timeout_ticks = election_ticks;
    }
    let election_ticks = cluster.causet.violetabft_store.violetabft_election_timeout_ticks as u32;
    let election_timeout = base_tick_interval * election_ticks;
    // Adjust max leader lease.
    cluster.causet.violetabft_store.violetabft_store_max_leader_lease = ReadableDuration(election_timeout);
    // Use large peer check interval, abnormal and max leader missing duration to make a valid config,
    // that is election timeout x 2 < peer stale state check < abnormal < max leader missing duration.
    cluster.causet.violetabft_store.peer_stale_state_check_interval = ReadableDuration(election_timeout * 3);
    cluster.causet.violetabft_store.abnormal_leader_missing_duration =
        ReadableDuration(election_timeout * 4);
    cluster.causet.violetabft_store.max_leader_missing_duration = ReadableDuration(election_timeout * 5);

    election_timeout
}

pub fn configure_for_enable_titan<T: Simulator>(
    cluster: &mut Cluster<T>,
    min_blob_size: ReadableSize,
) {
    cluster.causet.lmdb.titan.enabled = true;
    cluster.causet.lmdb.titan.purge_obsolete_files_period = ReadableDuration::secs(1);
    cluster.causet.lmdb.titan.max_background_gc = 10;
    cluster.causet.lmdb.defaultcauset.titan.min_blob_size = min_blob_size;
    cluster.causet.lmdb.defaultcauset.titan.blob_run_mode = BlobRunMode::Normal;
    cluster.causet.lmdb.defaultcauset.titan.min_gc_batch_size = ReadableSize::kb(0);
}

pub fn configure_for_disable_titan<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.causet.lmdb.titan.enabled = false;
}

pub fn configure_for_encryption<T: Simulator>(cluster: &mut Cluster<T>) {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let master_key_file = manifest_dir.join("src/master-key.data");

    let causet = &mut cluster.causet.security.encryption;
    causet.data_encryption_method = EncryptionMethod::Aes128Ctr;
    causet.data_key_rotation_period = ReadableDuration(Duration::from_millis(100));
    causet.master_key = MasterKeyConfig::File {
        config: FileConfig {
            path: master_key_file.to_str().unwrap().to_owned(),
        },
    }
}

/// Keep putting random kvs until specified size limit is reached.
pub fn put_till_size<T: Simulator>(
    cluster: &mut Cluster<T>,
    limit: u64,
    cone: &mut dyn Iteron<Item = u64>,
) -> Vec<u8> {
    put_causet_till_size(cluster, Causet_DEFAULT, limit, cone)
}

pub fn put_causet_till_size<T: Simulator>(
    cluster: &mut Cluster<T>,
    causet: &'static str,
    limit: u64,
    cone: &mut dyn Iteron<Item = u64>,
) -> Vec<u8> {
    assert!(limit > 0);
    let mut len = 0;
    let mut last_len = 0;
    let mut rng = rand::thread_rng();
    let mut key = vec![];
    while len < limit {
        let key_id = cone.next().unwrap();
        let key_str = format!("{:09}", key_id);
        key = key_str.into_bytes();
        let mut value = vec![0; 64];
        rng.fill_bytes(&mut value);
        cluster.must_put_causet(causet, &key, &value);
        // plus 1 for the extra encoding prefix
        len += key.len() as u64 + 1;
        len += value.len() as u64;
        // Flush memBlock to SST periodically, to make approximate size more accurate.
        if len - last_len >= 1000 {
            cluster.must_flush_causet(causet, true);
            last_len = len;
        }
    }
    // Approximate size of memBlock is inaccurate for small data,
    // we flush it to SST so we can use the size properties instead.
    cluster.must_flush_causet(causet, true);
    key
}

pub fn new_mutation(op: Op, k: &[u8], v: &[u8]) -> Mutation {
    let mut mutation = Mutation::default();
    mutation.set_op(op);
    mutation.set_key(k.to_vec());
    mutation.set_value(v.to_vec());
    mutation
}

pub fn must_kv_prewrite(
    client: &EINSTEINDBClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
) {
    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx);
    prewrite_req.set_mutations(muts.into_iter().collect());
    prewrite_req.primary_lock = pk;
    prewrite_req.spacelike_version = ts;
    prewrite_req.lock_ttl = 3000;
    prewrite_req.min_commit_ts = prewrite_req.spacelike_version + 1;
    let prewrite_resp = client.kv_prewrite(&prewrite_req).unwrap();
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

pub fn must_kv_commit(
    client: &EINSTEINDBClient,
    ctx: Context,
    tuplespaceInstanton: Vec<Vec<u8>>,
    spacelike_ts: u64,
    commit_ts: u64,
    expect_commit_ts: u64,
) {
    let mut commit_req = CommitRequest::default();
    commit_req.set_context(ctx);
    commit_req.spacelike_version = spacelike_ts;
    commit_req.set_tuplespaceInstanton(tuplespaceInstanton.into_iter().collect());
    commit_req.commit_version = commit_ts;
    let commit_resp = client.kv_commit(&commit_req).unwrap();
    assert!(
        !commit_resp.has_brane_error(),
        "{:?}",
        commit_resp.get_brane_error()
    );
    assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    assert_eq!(commit_resp.get_commit_version(), expect_commit_ts);
}

pub fn kv_pessimistic_lock(
    client: &EINSTEINDBClient,
    ctx: Context,
    tuplespaceInstanton: Vec<Vec<u8>>,
    ts: u64,
    for_fidelio_ts: u64,
    return_values: bool,
) -> PessimisticLockResponse {
    let mut req = PessimisticLockRequest::default();
    req.set_context(ctx);
    let primary = tuplespaceInstanton[0].clone();
    let mut mutations = vec![];
    for key in tuplespaceInstanton {
        let mut mutation = Mutation::default();
        mutation.set_op(Op::PessimisticLock);
        mutation.set_key(key);
        mutations.push(mutation);
    }
    req.set_mutations(mutations.into());
    req.primary_lock = primary;
    req.spacelike_version = ts;
    req.for_fidelio_ts = for_fidelio_ts;
    req.lock_ttl = 20;
    req.is_first_lock = false;
    req.return_values = return_values;
    client.kv_pessimistic_lock(&req).unwrap()
}

pub fn must_kv_pessimistic_lock(client: &EINSTEINDBClient, ctx: Context, key: Vec<u8>, ts: u64) {
    let resp = kv_pessimistic_lock(client, ctx, vec![key], ts, ts, false);
    assert!(!resp.has_brane_error(), "{:?}", resp.get_brane_error());
    assert!(resp.errors.is_empty(), "{:?}", resp.get_errors());
}

pub fn must_kv_pessimistic_rollback(client: &EINSTEINDBClient, ctx: Context, key: Vec<u8>, ts: u64) {
    let mut req = PessimisticRollbackRequest::default();
    req.set_context(ctx);
    req.set_tuplespaceInstanton(vec![key].into_iter().collect());
    req.spacelike_version = ts;
    req.for_fidelio_ts = ts;
    let resp = client.kv_pessimistic_rollback(&req).unwrap();
    assert!(!resp.has_brane_error(), "{:?}", resp.get_brane_error());
    assert!(resp.errors.is_empty(), "{:?}", resp.get_errors());
}

pub fn must_check_txn_status(
    client: &EINSTEINDBClient,
    ctx: Context,
    key: &[u8],
    lock_ts: u64,
    caller_spacelike_ts: u64,
    current_ts: u64,
) -> CheckTxnStatusResponse {
    let mut req = CheckTxnStatusRequest::default();
    req.set_context(ctx);
    req.set_primary_key(key.to_vec());
    req.set_lock_ts(lock_ts);
    req.set_caller_spacelike_ts(caller_spacelike_ts);
    req.set_current_ts(current_ts);

    let resp = client.kv_check_txn_status(&req).unwrap();
    assert!(!resp.has_brane_error(), "{:?}", resp.get_brane_error());
    assert!(resp.error.is_none(), "{:?}", resp.get_error());
    resp
}

pub fn must_physical_scan_lock(
    client: &EINSTEINDBClient,
    ctx: Context,
    max_ts: u64,
    spacelike_key: &[u8],
    limit: usize,
) -> Vec<LockInfo> {
    let mut req = PhysicalScanLockRequest::default();
    req.set_context(ctx);
    req.set_max_ts(max_ts);
    req.set_spacelike_key(spacelike_key.to_owned());
    req.set_limit(limit as _);
    let mut resp = client.physical_scan_lock(&req).unwrap();
    resp.take_locks().into()
}

pub fn register_lock_semaphore(client: &EINSTEINDBClient, max_ts: u64) -> RegisterLockSemaphoreResponse {
    let mut req = RegisterLockSemaphoreRequest::default();
    req.set_max_ts(max_ts);
    client.register_lock_semaphore(&req).unwrap()
}

pub fn must_register_lock_semaphore(client: &EINSTEINDBClient, max_ts: u64) {
    let resp = register_lock_semaphore(client, max_ts);
    assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
}

pub fn check_lock_semaphore(client: &EINSTEINDBClient, max_ts: u64) -> CheckLockSemaphoreResponse {
    let mut req = CheckLockSemaphoreRequest::default();
    req.set_max_ts(max_ts);
    client.check_lock_semaphore(&req).unwrap()
}

pub fn must_check_lock_semaphore(client: &EINSTEINDBClient, max_ts: u64, clean: bool) -> Vec<LockInfo> {
    let mut resp = check_lock_semaphore(client, max_ts);
    assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
    assert_eq!(resp.get_is_clean(), clean);
    resp.take_locks().into()
}

pub fn remove_lock_semaphore(client: &EINSTEINDBClient, max_ts: u64) -> RemoveLockSemaphoreResponse {
    let mut req = RemoveLockSemaphoreRequest::default();
    req.set_max_ts(max_ts);
    client.remove_lock_semaphore(&req).unwrap()
}

pub fn must_remove_lock_semaphore(client: &EINSTEINDBClient, max_ts: u64) {
    let resp = remove_lock_semaphore(client, max_ts);
    assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
}
