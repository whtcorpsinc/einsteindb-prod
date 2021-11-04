// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::error::Error as StdError;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::*;
use std::{result, thread};

use futures::executor::block_on;
use ekvproto::error_timeshare::Error as PbError;
use ekvproto::meta_timeshare::{self, Peer, BraneEpoch, StoreLabel};
use ekvproto::fidel_timeshare;
use ekvproto::violetabft_cmd_timeshare::*;
use ekvproto::violetabft_server_timeshare::{
    self, VioletaBftApplyState, VioletaBftLocalState, VioletaBftMessage, VioletaBftTruncatedState, BraneLocalState,
};
use violetabft::evioletabft_timeshare::ConfChangeType;
use tempfile::TempDir;

use encryption::DataKeyManager;
use engine_lmdb::raw::DB;
use engine_lmdb::{Compat, LmdbEngine, LmdbSnapshot};
use edb::{
    CompactExt, Engines, Iterable, MiscExt, MuBlock, Peekable, WriteBatchExt, Causet_VIOLETABFT,
};
use fidel_client::FidelClient;
use violetabftstore::store::fsm::store::{StoreMeta, PENDING_VOTES_CAP};
use violetabftstore::store::fsm::{create_violetabft_batch_system, VioletaBftBatchSystem, VioletaBftRouter};
use violetabftstore::store::transport::CasualRouter;
use violetabftstore::store::*;
use violetabftstore::{Error, Result};
use edb::config::EINSTEINDBConfig;
use edb::server::Result as ServerResult;
use violetabftstore::interlock::::collections::{HashMap, HashSet};
use violetabftstore::interlock::::HandyRwLock;

use super::*;
use violetabftstore::interlock::::time::ThreadReadId;

// We simulate 3 or 5 nodes, each has a store.
// Sometimes, we use fixed id to test, which means the id
// isn't allocated by fidel, and node id, store id are same.
// E,g, for node 1, the node id and store id are both 1.

pub trait Simulator {
    // Pass 0 to let fidel allocate a node id if db is empty.
    // If node id > 0, the node must be created in db already,
    // and the node id must be the same as given argument.
    // Return the node id.
    // TODO: we will rename node name here because now we use store only.
    fn run_node(
        &mut self,
        node_id: u64,
        causet: EINSTEINDBConfig,
        engines: Engines<LmdbEngine, LmdbEngine>,
        store_meta: Arc<Mutex<StoreMeta>>,
        key_manager: Option<Arc<DataKeyManager>>,
        router: VioletaBftRouter<LmdbEngine, LmdbEngine>,
        system: VioletaBftBatchSystem<LmdbEngine, LmdbEngine>,
    ) -> ServerResult<u64>;
    fn stop_node(&mut self, node_id: u64);
    fn get_node_ids(&self) -> HashSet<u64>;
    fn async_command_on_node(
        &self,
        node_id: u64,
        request: VioletaBftCmdRequest,
        cb: Callback<LmdbSnapshot>,
    ) -> Result<()>;
    fn lightlike_violetabft_msg(&mut self, msg: VioletaBftMessage) -> Result<()>;
    fn get_snap_dir(&self, node_id: u64) -> String;
    fn get_router(&self, node_id: u64) -> Option<VioletaBftRouter<LmdbEngine, LmdbEngine>>;
    fn add_lightlike_filter(&mut self, node_id: u64, filter: Box<dyn Filter>);
    fn clear_lightlike_filters(&mut self, node_id: u64);
    fn add_recv_filter(&mut self, node_id: u64, filter: Box<dyn Filter>);
    fn clear_recv_filters(&mut self, node_id: u64);

    fn call_command(&self, request: VioletaBftCmdRequest, timeout: Duration) -> Result<VioletaBftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        self.call_command_on_node(node_id, request, timeout)
    }

    fn read(
        &self,
        batch_id: Option<ThreadReadId>,
        request: VioletaBftCmdRequest,
        timeout: Duration,
    ) -> Result<VioletaBftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        let (cb, rx) = make_cb(&request);
        self.async_read(node_id, batch_id, request, cb);
        rx.recv_timeout(timeout)
            .map_err(|_| Error::Timeout(format!("request timeout for {:?}", timeout)))
    }

    fn async_read(
        &self,
        node_id: u64,
        batch_id: Option<ThreadReadId>,
        request: VioletaBftCmdRequest,
        cb: Callback<LmdbSnapshot>,
    );

    fn call_command_on_node(
        &self,
        node_id: u64,
        request: VioletaBftCmdRequest,
        timeout: Duration,
    ) -> Result<VioletaBftCmdResponse> {
        let (cb, rx) = make_cb(&request);

        match self.async_command_on_node(node_id, request, cb) {
            Ok(()) => {}
            Err(e) => {
                let mut resp = VioletaBftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                return Ok(resp);
            }
        }
        rx.recv_timeout(timeout)
            .map_err(|_| Error::Timeout(format!("request timeout for {:?}", timeout)))
    }
}

pub struct Cluster<T: Simulator> {
    pub causet: EINSTEINDBConfig,
    leaders: HashMap<u64, meta_timeshare::Peer>,
    count: usize,

    pub paths: Vec<TempDir>,
    pub dbs: Vec<Engines<LmdbEngine, LmdbEngine>>,
    pub store_metas: HashMap<u64, Arc<Mutex<StoreMeta>>>,
    key_managers: Vec<Option<Arc<DataKeyManager>>>,
    pub engines: HashMap<u64, Engines<LmdbEngine, LmdbEngine>>,
    key_managers_map: HashMap<u64, Option<Arc<DataKeyManager>>>,
    pub labels: HashMap<u64, HashMap<String, String>>,

    pub sim: Arc<RwLock<T>>,
    pub fidel_client: Arc<TestFidelClient>,
}

impl<T: Simulator> Cluster<T> {
    // Create the default CausetStore cluster.
    pub fn new(
        id: u64,
        count: usize,
        sim: Arc<RwLock<T>>,
        fidel_client: Arc<TestFidelClient>,
    ) -> Cluster<T> {
        // TODO: In the future, maybe it's better to test both case where `use_delete_cone` is true and false
        Cluster {
            causet: new_edb_config(id),
            leaders: HashMap::default(),
            count,
            paths: vec![],
            dbs: vec![],
            store_metas: HashMap::default(),
            key_managers: vec![],
            engines: HashMap::default(),
            key_managers_map: HashMap::default(),
            labels: HashMap::default(),
            sim,
            fidel_client,
        }
    }

    pub fn id(&self) -> u64 {
        self.causet.server.cluster_id
    }

    pub fn pre_spacelike_check(&mut self) -> result::Result<(), Box<dyn StdError>> {
        for path in &self.paths {
            self.causet.causet_storage.data_dir = path.path().to_str().unwrap().to_owned();
            self.causet.validate()?
        }
        Ok(())
    }

    /// Engines in a just created cluster are not bootstraped, which means they are not associated
    /// with a `node_id`. Call `Cluster::spacelike` can bootstrap all nodes in the cluster.
    ///
    /// However sometimes a node can be bootstrapped externally. This function can be called to
    /// mark them as bootstrapped in `Cluster`.
    pub fn set_bootstrapped(&mut self, node_id: u64, offset: usize) {
        let engines = self.dbs[offset].clone();
        let key_mgr = self.key_managers[offset].clone();
        assert!(self.engines.insert(node_id, engines).is_none());
        assert!(self.key_managers_map.insert(node_id, key_mgr).is_none());
    }

    fn create_engine(&mut self, router: Option<VioletaBftRouter<LmdbEngine, LmdbEngine>>) {
        let (engines, key_manager, dir) = create_test_engine(router, &self.causet);
        self.dbs.push(engines);
        self.key_managers.push(key_manager);
        self.paths.push(dir);
    }

    pub fn create_engines(&mut self) {
        for _ in 0..self.count {
            self.create_engine(None);
        }
    }

    pub fn spacelike(&mut self) -> ServerResult<()> {
        // Try recover from last shutdown.
        let node_ids: Vec<u64> = self.engines.iter().map(|(&id, _)| id).collect();
        for node_id in node_ids {
            self.run_node(node_id)?;
        }

        // Try spacelike new nodes.
        for _ in 0..self.count - self.engines.len() {
            let (router, system) = create_violetabft_batch_system(&self.causet.violetabft_store);
            self.create_engine(Some(router.clone()));

            let engines = self.dbs.last().unwrap().clone();
            let key_mgr = self.key_managers.last().unwrap().clone();
            let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_VOTES_CAP)));

            let mut sim = self.sim.wl();
            let node_id = sim.run_node(
                0,
                self.causet.clone(),
                engines.clone(),
                store_meta.clone(),
                key_mgr.clone(),
                router,
                system,
            )?;
            self.engines.insert(node_id, engines);
            self.store_metas.insert(node_id, store_meta);
            self.key_managers_map.insert(node_id, key_mgr);
        }
        Ok(())
    }

    pub fn compact_data(&self) {
        for engine in self.engines.values() {
            let db = &engine.kv;
            db.compact_cone("default", None, None, false, 1).unwrap();
        }
    }

    // Bootstrap the store with fixed ID (like 1, 2, .. 5) and
    // initialize first brane in all stores, then spacelike the cluster.
    pub fn run(&mut self) {
        self.create_engines();
        self.bootstrap_brane().unwrap();
        self.spacelike().unwrap();
    }

    // Bootstrap the store with fixed ID (like 1, 2, .. 5) and
    // initialize first brane in store 1, then spacelike the cluster.
    pub fn run_conf_change(&mut self) -> u64 {
        self.create_engines();
        let brane_id = self.bootstrap_conf_change();
        self.spacelike().unwrap();
        brane_id
    }

    pub fn get_node_ids(&self) -> HashSet<u64> {
        self.sim.rl().get_node_ids()
    }

    pub fn run_node(&mut self, node_id: u64) -> ServerResult<()> {
        debug!("spacelikeing node {}", node_id);
        let engines = self.engines[&node_id].clone();
        let key_mgr = self.key_managers_map[&node_id].clone();
        let (router, system) = create_violetabft_batch_system(&self.causet.violetabft_store);
        let mut causet = self.causet.clone();
        if let Some(labels) = self.labels.get(&node_id) {
            causet.server.labels = labels.to_owned();
        }
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_VOTES_CAP)));
        self.store_metas.insert(node_id, store_meta.clone());
        debug!("calling run node"; "node_id" => node_id);
        // FIXME: lmdb event listeners may not work, because we change the router.
        self.sim
            .wl()
            .run_node(node_id, causet, engines, store_meta, key_mgr, router, system)?;
        debug!("node {} spacelikeed", node_id);
        Ok(())
    }

    pub fn stop_node(&mut self, node_id: u64) {
        debug!("stopping node {}", node_id);
        match self.sim.write() {
            Ok(mut sim) => sim.stop_node(node_id),
            Err(_) => {
                if !thread::panicking() {
                    panic!("failed to acquire write dagger.")
                }
            }
        }
        self.fidel_client.shutdown_store(node_id);
        debug!("node {} stopped", node_id);
    }

    pub fn get_engine(&self, node_id: u64) -> Arc<DB> {
        Arc::clone(&self.engines[&node_id].kv.as_inner())
    }

    pub fn get_violetabft_engine(&self, node_id: u64) -> Arc<DB> {
        Arc::clone(&self.engines[&node_id].violetabft.as_inner())
    }

    pub fn get_all_engines(&self, node_id: u64) -> Engines<LmdbEngine, LmdbEngine> {
        self.engines[&node_id].clone()
    }

    pub fn lightlike_violetabft_msg(&mut self, msg: VioletaBftMessage) -> Result<()> {
        self.sim.wl().lightlike_violetabft_msg(msg)
    }

    pub fn call_command_on_node(
        &self,
        node_id: u64,
        request: VioletaBftCmdRequest,
        timeout: Duration,
    ) -> Result<VioletaBftCmdResponse> {
        match self
            .sim
            .rl()
            .call_command_on_node(node_id, request.clone(), timeout)
        {
            Err(e) => {
                warn!("failed to call command {:?}: {:?}", request, e);
                Err(e)
            }
            a => a,
        }
    }

    pub fn read(
        &self,
        batch_id: Option<ThreadReadId>,
        request: VioletaBftCmdRequest,
        timeout: Duration,
    ) -> Result<VioletaBftCmdResponse> {
        match self.sim.rl().read(batch_id, request.clone(), timeout) {
            Err(e) => {
                warn!("failed to read {:?}: {:?}", request, e);
                Err(e)
            }
            a => a,
        }
    }

    pub fn call_command(
        &self,
        request: VioletaBftCmdRequest,
        timeout: Duration,
    ) -> Result<VioletaBftCmdResponse> {
        let mut is_read = false;
        for req in request.get_requests() {
            match req.get_cmd_type() {
                CmdType::Get | CmdType::Snap | CmdType::ReadIndex => {
                    is_read = true;
                }
                _ => (),
            }
        }
        let ret = if is_read {
            self.sim.rl().read(None, request.clone(), timeout)
        } else {
            self.sim.rl().call_command(request.clone(), timeout)
        };
        match ret {
            Err(e) => {
                warn!("failed to call command {:?}: {:?}", request, e);
                Err(e)
            }
            a => a,
        }
    }

    pub fn call_command_on_leader(
        &mut self,
        mut request: VioletaBftCmdRequest,
        timeout: Duration,
    ) -> Result<VioletaBftCmdResponse> {
        let timer = Instant::now();
        let brane_id = request.get_header().get_brane_id();
        loop {
            let leader = match self.leader_of_brane(brane_id) {
                None => return Err(box_err!("can't get leader of brane {}", brane_id)),
                Some(l) => l,
            };
            request.mut_header().set_peer(leader);
            let resp = match self.call_command(request.clone(), timeout) {
                e @ Err(_) => return e,
                Ok(resp) => resp,
            };
            if self.refresh_leader_if_needed(&resp, brane_id) && timer.elapsed() < timeout {
                warn!(
                    "{:?} is no longer leader, let's retry",
                    request.get_header().get_peer()
                );
                continue;
            }
            return Ok(resp);
        }
    }

    fn valid_leader_id(&self, brane_id: u64, leader_id: u64) -> bool {
        let store_ids = match self.store_ids_of_brane(brane_id) {
            None => return false,
            Some(ids) => ids,
        };
        let node_ids = self.sim.rl().get_node_ids();
        store_ids.contains(&leader_id) && node_ids.contains(&leader_id)
    }

    fn store_ids_of_brane(&self, brane_id: u64) -> Option<Vec<u64>> {
        block_on(self.fidel_client.get_brane_by_id(brane_id))
            .unwrap()
            .map(|brane| brane.get_peers().iter().map(Peer::get_store_id).collect())
    }

    pub fn query_leader(
        &self,
        store_id: u64,
        brane_id: u64,
        timeout: Duration,
    ) -> Option<meta_timeshare::Peer> {
        // To get brane leader, we don't care real peer id, so use 0 instead.
        let peer = new_peer(store_id, 0);
        let find_leader = new_status_request(brane_id, peer, new_brane_leader_cmd());
        let mut resp = match self.call_command(find_leader, timeout) {
            Ok(resp) => resp,
            Err(err) => {
                error!(
                    "fail to get leader of brane {} on store {}, error: {:?}",
                    brane_id, store_id, err
                );
                return None;
            }
        };
        let mut brane_leader = resp.take_status_response().take_brane_leader();
        // NOTE: node id can't be 0.
        if self.valid_leader_id(brane_id, brane_leader.get_leader().get_store_id()) {
            Some(brane_leader.take_leader())
        } else {
            None
        }
    }

    pub fn leader_of_brane(&mut self, brane_id: u64) -> Option<meta_timeshare::Peer> {
        let store_ids = match self.store_ids_of_brane(brane_id) {
            None => return None,
            Some(ids) => ids,
        };
        if let Some(l) = self.leaders.get(&brane_id) {
            // leader may be stopped in some tests.
            if self.valid_leader_id(brane_id, l.get_store_id()) {
                return Some(l.clone());
            }
        }
        self.reset_leader_of_brane(brane_id);
        let mut leader = None;
        let mut leaders = HashMap::default();

        let node_ids = self.sim.rl().get_node_ids();
        // For some tests, we stop the node but fidel still has this information,
        // and we must skip this.
        let alive_store_ids: Vec<_> = store_ids
            .iter()
            .filter(|id| node_ids.contains(id))
            .cloned()
            .collect();
        for _ in 0..500 {
            for store_id in &alive_store_ids {
                let l = match self.query_leader(*store_id, brane_id, Duration::from_secs(1)) {
                    None => continue,
                    Some(l) => l,
                };
                leaders
                    .entry(l.get_id())
                    .or_insert((l, vec![]))
                    .1
                    .push(*store_id);
            }
            if let Some((_, (l, c))) = leaders.drain().max_by_key(|(_, (_, c))| c.len()) {
                // It may be a step down leader.
                if c.contains(&l.get_store_id()) {
                    leader = Some(l);
                    if c.len() > store_ids.len() / 2 {
                        break;
                    }
                }
            }
            sleep_ms(10);
        }

        if let Some(l) = leader {
            self.leaders.insert(brane_id, l);
        }

        self.leaders.get(&brane_id).cloned()
    }

    pub fn check_branes_number(&self, len: u32) {
        assert_eq!(self.fidel_client.get_branes_number() as u32, len)
    }

    // For test when a node is already bootstraped the cluster with the first brane
    // But another node may request bootstrap at same time and get is_bootstrap false
    // Add Brane but not set bootstrap to true
    pub fn add_first_brane(&self) -> Result<()> {
        let mut brane = meta_timeshare::Brane::default();
        let brane_id = self.fidel_client.alloc_id().unwrap();
        let peer_id = self.fidel_client.alloc_id().unwrap();
        brane.set_id(brane_id);
        brane.set_spacelike_key(tuplespaceInstanton::EMPTY_KEY.to_vec());
        brane.set_lightlike_key(tuplespaceInstanton::EMPTY_KEY.to_vec());
        brane.mut_brane_epoch().set_version(INIT_EPOCH_VER);
        brane.mut_brane_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
        let peer = new_peer(peer_id, peer_id);
        brane.mut_peers().push(peer);
        self.fidel_client.add_brane(&brane);
        Ok(())
    }

    /// Multiple nodes with fixed node id, like node 1, 2, .. 5,
    /// First brane 1 is in all stores with peer 1, 2, .. 5.
    /// Peer 1 is in node 1, store 1, etc.
    ///
    /// Must be called after `create_engines`.
    pub fn bootstrap_brane(&mut self) -> Result<()> {
        for (i, engines) in self.dbs.iter().enumerate() {
            let id = i as u64 + 1;
            self.engines.insert(id, engines.clone());
            let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_VOTES_CAP)));
            self.store_metas.insert(id, store_meta);
            self.key_managers_map
                .insert(id, self.key_managers[i].clone());
        }

        let mut brane = meta_timeshare::Brane::default();
        brane.set_id(1);
        brane.set_spacelike_key(tuplespaceInstanton::EMPTY_KEY.to_vec());
        brane.set_lightlike_key(tuplespaceInstanton::EMPTY_KEY.to_vec());
        brane.mut_brane_epoch().set_version(INIT_EPOCH_VER);
        brane.mut_brane_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);

        for (&id, engines) in &self.engines {
            let peer = new_peer(id, id);
            brane.mut_peers().push(peer.clone());
            bootstrap_store(&engines, self.id(), id).unwrap();
        }

        for engines in self.engines.values() {
            prepare_bootstrap_cluster(&engines, &brane)?;
        }

        self.bootstrap_cluster(brane);

        Ok(())
    }

    // Return first brane id.
    pub fn bootstrap_conf_change(&mut self) -> u64 {
        for (i, engines) in self.dbs.iter().enumerate() {
            let id = i as u64 + 1;
            self.engines.insert(id, engines.clone());
            let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_VOTES_CAP)));
            self.store_metas.insert(id, store_meta);
            self.key_managers_map
                .insert(id, self.key_managers[i].clone());
        }

        for (&id, engines) in &self.engines {
            bootstrap_store(&engines, self.id(), id).unwrap();
        }

        let node_id = 1;
        let brane_id = 1;
        let peer_id = 1;

        let brane = initial_brane(node_id, brane_id, peer_id);
        prepare_bootstrap_cluster(&self.engines[&node_id], &brane).unwrap();
        self.bootstrap_cluster(brane);
        brane_id
    }

    // This is only for fixed id test.
    fn bootstrap_cluster(&mut self, brane: meta_timeshare::Brane) {
        self.fidel_client
            .bootstrap_cluster(new_store(1, "".to_owned()), brane)
            .unwrap();
        for id in self.engines.tuplespaceInstanton() {
            let mut store = new_store(*id, "".to_owned());
            if let Some(labels) = self.labels.get(id) {
                for (key, value) in labels.iter() {
                    let mut l = StoreLabel::default();
                    l.key = key.clone();
                    l.value = value.clone();
                    store.labels.push(l);
                }
            }
            self.fidel_client.put_store(store).unwrap();
        }
    }

    pub fn add_label(&mut self, node_id: u64, key: &str, value: &str) {
        self.labels
            .entry(node_id)
            .or_default()
            .insert(key.to_owned(), value.to_owned());
    }

    pub fn add_new_engine(&mut self) -> u64 {
        self.create_engine(None);
        self.count += 1;
        let node_id = self.count as u64;

        let engines = self.dbs.last().unwrap().clone();
        bootstrap_store(&engines, self.id(), node_id).unwrap();
        self.engines.insert(node_id, engines);

        let key_mgr = self.key_managers.last().unwrap().clone();
        self.key_managers_map.insert(node_id, key_mgr);

        self.run_node(node_id).unwrap();
        node_id
    }

    pub fn reset_leader_of_brane(&mut self, brane_id: u64) {
        self.leaders.remove(&brane_id);
    }

    pub fn assert_quorum<F: FnMut(&Arc<DB>) -> bool>(&self, mut condition: F) {
        if self.engines.is_empty() {
            return;
        }
        let half = self.engines.len() / 2;
        let mut qualified_cnt = 0;
        for (id, engines) in &self.engines {
            if !condition(engines.kv.as_inner()) {
                debug!("store {} is not qualified yet.", id);
                continue;
            }
            debug!("store {} is qualified", id);
            qualified_cnt += 1;
            if half < qualified_cnt {
                return;
            }
        }

        panic!(
            "need at lease {} qualified stores, but only got {}",
            half + 1,
            qualified_cnt
        );
    }

    pub fn shutdown(&mut self) {
        debug!("about to shutdown cluster");
        let tuplespaceInstanton;
        match self.sim.read() {
            Ok(s) => tuplespaceInstanton = s.get_node_ids(),
            Err(_) => {
                if thread::panicking() {
                    // Leave the resource to avoid double panic.
                    return;
                } else {
                    panic!("failed to acquire read dagger");
                }
            }
        }
        for id in tuplespaceInstanton {
            self.stop_node(id);
        }
        self.leaders.clear();
        self.store_metas.clear();
        debug!("all nodes are shut down.");
    }

    // If the resp is "not leader error", get the real leader.
    // Otherwise reset or refresh leader if needed.
    // Returns if the request should retry.
    fn refresh_leader_if_needed(&mut self, resp: &VioletaBftCmdResponse, brane_id: u64) -> bool {
        if !is_error_response(resp) {
            return false;
        }

        let err = resp.get_header().get_error();
        if err
            .get_message()
            .contains("peer has not applied to current term")
        {
            // leader peer has not applied to current term
            return true;
        }

        // If command is stale, leadership may have changed.
        // Or epoch not match, it can be introduced by wrong leader.
        if err.has_stale_command() || err.has_epoch_not_match() {
            self.reset_leader_of_brane(brane_id);
            return true;
        }

        if !err.has_not_leader() {
            return false;
        }
        let err = err.get_not_leader();
        if !err.has_leader() {
            self.reset_leader_of_brane(brane_id);
            return true;
        }
        self.leaders.insert(brane_id, err.get_leader().clone());
        true
    }

    pub fn request(
        &mut self,
        key: &[u8],
        reqs: Vec<Request>,
        read_quorum: bool,
        timeout: Duration,
    ) -> VioletaBftCmdResponse {
        let timer = Instant::now();
        let mut tried_times = 0;
        while tried_times < 10 || timer.elapsed() < timeout {
            tried_times += 1;
            let mut brane = self.get_brane(key);
            let brane_id = brane.get_id();
            let req = new_request(
                brane_id,
                brane.take_brane_epoch(),
                reqs.clone(),
                read_quorum,
            );
            let result = self.call_command_on_leader(req, timeout);

            if let Err(Error::Timeout(_)) = result {
                warn!("call command timeout, let's retry");
                sleep_ms(100);
                continue;
            }

            let resp = result.unwrap();
            if resp.get_header().get_error().has_epoch_not_match() {
                warn!("seems split, let's retry");
                sleep_ms(100);
                continue;
            }
            if resp
                .get_header()
                .get_error()
                .get_message()
                .contains("merging mode")
            {
                warn!("seems waiting for merge, let's retry");
                sleep_ms(100);
                continue;
            }
            return resp;
        }
        panic!("request timeout");
    }

    // Get brane when the `filter` returns true.
    pub fn get_brane_with<F>(&self, key: &[u8], filter: F) -> meta_timeshare::Brane
    where
        F: Fn(&meta_timeshare::Brane) -> bool,
    {
        for _ in 0..100 {
            if let Ok(brane) = self.fidel_client.get_brane(key) {
                if filter(&brane) {
                    return brane;
                }
            }
            // We may meet cone gap after split, so here we will
            // retry to get the brane again.
            sleep_ms(20);
        }

        panic!("find no brane for {}", hex::encode_upper(key));
    }

    pub fn get_brane(&self, key: &[u8]) -> meta_timeshare::Brane {
        self.get_brane_with(key, |_| true)
    }

    pub fn get_brane_id(&self, key: &[u8]) -> u64 {
        self.get_brane(key).get_id()
    }

    pub fn get_down_peers(&self) -> HashMap<u64, fidel_timeshare::PeerStats> {
        self.fidel_client.get_down_peers()
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_impl("default", key, false)
    }

    pub fn get_causet(&mut self, causet: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.get_impl(causet, key, false)
    }

    pub fn must_get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_impl("default", key, true)
    }

    fn get_impl(&mut self, causet: &str, key: &[u8], read_quorum: bool) -> Option<Vec<u8>> {
        let mut resp = self.request(
            key,
            vec![new_get_causet_cmd(causet, key)],
            read_quorum,
            Duration::from_secs(5),
        );
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Get);
        if resp.get_responses()[0].has_get() {
            Some(resp.mut_responses()[0].mut_get().take_value())
        } else {
            None
        }
    }

    pub fn async_request(
        &mut self,
        mut req: VioletaBftCmdRequest,
    ) -> Result<mpsc::Receiver<VioletaBftCmdResponse>> {
        let brane_id = req.get_header().get_brane_id();
        let leader = self.leader_of_brane(brane_id).unwrap();
        req.mut_header().set_peer(leader.clone());
        let (cb, rx) = make_cb(&req);
        self.sim
            .rl()
            .async_command_on_node(leader.get_store_id(), req, cb)?;
        Ok(rx)
    }

    pub fn async_put(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<mpsc::Receiver<VioletaBftCmdResponse>> {
        let mut brane = self.get_brane(key);
        let reqs = vec![new_put_cmd(key, value)];
        let put = new_request(brane.get_id(), brane.take_brane_epoch(), reqs, false);
        self.async_request(put)
    }

    pub fn async_remove_peer(
        &mut self,
        brane_id: u64,
        peer: meta_timeshare::Peer,
    ) -> Result<mpsc::Receiver<VioletaBftCmdResponse>> {
        let brane = block_on(self.fidel_client.get_brane_by_id(brane_id))
            .unwrap()
            .unwrap();
        let remove_peer = new_change_peer_request(ConfChangeType::RemoveNode, peer);
        let req = new_admin_request(brane_id, brane.get_brane_epoch(), remove_peer);
        self.async_request(req)
    }

    pub fn async_add_peer(
        &mut self,
        brane_id: u64,
        peer: meta_timeshare::Peer,
    ) -> Result<mpsc::Receiver<VioletaBftCmdResponse>> {
        let brane = block_on(self.fidel_client.get_brane_by_id(brane_id))
            .unwrap()
            .unwrap();
        let add_peer = new_change_peer_request(ConfChangeType::AddNode, peer);
        let req = new_admin_request(brane_id, brane.get_brane_epoch(), add_peer);
        self.async_request(req)
    }

    pub fn must_put(&mut self, key: &[u8], value: &[u8]) {
        self.must_put_causet("default", key, value);
    }

    pub fn must_put_causet(&mut self, causet: &str, key: &[u8], value: &[u8]) {
        let resp = self.request(
            key,
            vec![new_put_causet_cmd(causet, key, value)],
            false,
            Duration::from_secs(5),
        );
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Put);
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> result::Result<(), PbError> {
        let resp = self.request(
            key,
            vec![new_put_causet_cmd("default", key, value)],
            false,
            Duration::from_secs(5),
        );
        if resp.get_header().has_error() {
            Err(resp.get_header().get_error().clone())
        } else {
            Ok(())
        }
    }

    pub fn must_delete(&mut self, key: &[u8]) {
        self.must_delete_causet("default", key)
    }

    pub fn must_delete_causet(&mut self, causet: &str, key: &[u8]) {
        let resp = self.request(
            key,
            vec![new_delete_cmd(causet, key)],
            false,
            Duration::from_secs(5),
        );
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Delete);
    }

    pub fn must_delete_cone_causet(&mut self, causet: &str, spacelike: &[u8], lightlike: &[u8]) {
        let resp = self.request(
            spacelike,
            vec![new_delete_cone_cmd(causet, spacelike, lightlike)],
            false,
            Duration::from_secs(5),
        );
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::DeleteCone);
    }

    pub fn must_notify_delete_cone_causet(&mut self, causet: &str, spacelike: &[u8], lightlike: &[u8]) {
        let mut req = new_delete_cone_cmd(causet, spacelike, lightlike);
        req.mut_delete_cone().set_notify_only(true);
        let resp = self.request(spacelike, vec![req], false, Duration::from_secs(5));
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::DeleteCone);
    }

    pub fn must_flush_causet(&mut self, causet: &str, sync: bool) {
        for engines in &self.dbs {
            engines.kv.flush_causet(causet, sync).unwrap();
        }
    }

    pub fn get_brane_epoch(&self, brane_id: u64) -> BraneEpoch {
        block_on(self.fidel_client.get_brane_by_id(brane_id))
            .unwrap()
            .unwrap()
            .take_brane_epoch()
    }

    pub fn brane_detail(&self, brane_id: u64, store_id: u64) -> BraneDetailResponse {
        let status_cmd = new_brane_detail_cmd();
        let peer = new_peer(store_id, 0);
        let req = new_status_request(brane_id, peer, status_cmd);
        let resp = self.call_command(req, Duration::from_secs(5));
        assert!(resp.is_ok(), "{:?}", resp);

        let mut resp = resp.unwrap();
        assert!(resp.has_status_response());
        let mut status_resp = resp.take_status_response();
        assert_eq!(status_resp.get_cmd_type(), StatusCmdType::BraneDetail);
        assert!(status_resp.has_brane_detail());
        status_resp.take_brane_detail()
    }

    pub fn truncated_state(&self, brane_id: u64, store_id: u64) -> VioletaBftTruncatedState {
        self.apply_state(brane_id, store_id).take_truncated_state()
    }

    pub fn apply_state(&self, brane_id: u64, store_id: u64) -> VioletaBftApplyState {
        let key = tuplespaceInstanton::apply_state_key(brane_id);
        self.get_engine(store_id)
            .c()
            .get_msg_causet::<VioletaBftApplyState>(edb::Causet_VIOLETABFT, &key)
            .unwrap()
            .unwrap()
    }

    pub fn violetabft_local_state(&self, brane_id: u64, store_id: u64) -> VioletaBftLocalState {
        let key = tuplespaceInstanton::violetabft_state_key(brane_id);
        self.get_violetabft_engine(store_id)
            .c()
            .get_msg::<violetabft_server_timeshare::VioletaBftLocalState>(&key)
            .unwrap()
            .unwrap()
    }

    pub fn brane_local_state(&self, brane_id: u64, store_id: u64) -> BraneLocalState {
        self.get_engine(store_id)
            .c()
            .get_msg_causet::<BraneLocalState>(
                edb::Causet_VIOLETABFT,
                &tuplespaceInstanton::brane_state_key(brane_id),
            )
            .unwrap()
            .unwrap()
    }

    pub fn wait_last_index(
        &mut self,
        brane_id: u64,
        store_id: u64,
        expected: u64,
        timeout: Duration,
    ) {
        let timer = Instant::now();
        loop {
            let violetabft_state = self.violetabft_local_state(brane_id, store_id);
            let cur_index = violetabft_state.get_last_index();
            if cur_index >= expected {
                return;
            }
            if timer.elapsed() >= timeout {
                panic!(
                    "[brane {}] last index still not reach {}: {:?}",
                    brane_id, expected, violetabft_state
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn restore_kv_meta(&self, brane_id: u64, store_id: u64, snap: &LmdbSnapshot) {
        let (meta_spacelike, meta_lightlike) = (
            tuplespaceInstanton::brane_meta_prefix(brane_id),
            tuplespaceInstanton::brane_meta_prefix(brane_id + 1),
        );
        let mut kv_wb = self.engines[&store_id].kv.write_batch();
        self.engines[&store_id]
            .kv
            .scan_causet(Causet_VIOLETABFT, &meta_spacelike, &meta_lightlike, false, |k, _| {
                kv_wb.delete(k).unwrap();
                Ok(true)
            })
            .unwrap();
        snap.scan_causet(Causet_VIOLETABFT, &meta_spacelike, &meta_lightlike, false, |k, v| {
            kv_wb.put(k, v).unwrap();
            Ok(true)
        })
        .unwrap();

        let (violetabft_spacelike, violetabft_lightlike) = (
            tuplespaceInstanton::brane_violetabft_prefix(brane_id),
            tuplespaceInstanton::brane_violetabft_prefix(brane_id + 1),
        );
        self.engines[&store_id]
            .kv
            .scan_causet(Causet_VIOLETABFT, &violetabft_spacelike, &violetabft_lightlike, false, |k, _| {
                kv_wb.delete(k).unwrap();
                Ok(true)
            })
            .unwrap();
        snap.scan_causet(Causet_VIOLETABFT, &violetabft_spacelike, &violetabft_lightlike, false, |k, v| {
            kv_wb.put(k, v).unwrap();
            Ok(true)
        })
        .unwrap();
        self.engines[&store_id].kv.write(&kv_wb).unwrap();
    }

    pub fn restore_violetabft(&self, brane_id: u64, store_id: u64, snap: &LmdbSnapshot) {
        let (violetabft_spacelike, violetabft_lightlike) = (
            tuplespaceInstanton::brane_violetabft_prefix(brane_id),
            tuplespaceInstanton::brane_violetabft_prefix(brane_id + 1),
        );
        let mut violetabft_wb = self.engines[&store_id].violetabft.write_batch();
        self.engines[&store_id]
            .violetabft
            .scan(&violetabft_spacelike, &violetabft_lightlike, false, |k, _| {
                violetabft_wb.delete(k).unwrap();
                Ok(true)
            })
            .unwrap();
        snap.scan(&violetabft_spacelike, &violetabft_lightlike, false, |k, v| {
            violetabft_wb.put(k, v).unwrap();
            Ok(true)
        })
        .unwrap();
        self.engines[&store_id].violetabft.write(&violetabft_wb).unwrap();
    }

    pub fn add_lightlike_filter<F: FilterFactory>(&self, factory: F) {
        let mut sim = self.sim.wl();
        for node_id in sim.get_node_ids() {
            for filter in factory.generate(node_id) {
                sim.add_lightlike_filter(node_id, filter);
            }
        }
    }

    pub fn transfer_leader(&mut self, brane_id: u64, leader: meta_timeshare::Peer) {
        let epoch = self.get_brane_epoch(brane_id);
        let transfer_leader = new_admin_request(brane_id, &epoch, new_transfer_leader_cmd(leader));
        let resp = self
            .call_command_on_leader(transfer_leader, Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            resp.get_admin_response().get_cmd_type(),
            AdminCmdType::TransferLeader,
            "{:?}",
            resp
        );
    }

    pub fn must_transfer_leader(&mut self, brane_id: u64, leader: meta_timeshare::Peer) {
        let timer = Instant::now();
        loop {
            self.reset_leader_of_brane(brane_id);
            let cur_leader = self.leader_of_brane(brane_id);
            if cur_leader == Some(leader.clone()) {
                return;
            }
            if timer.elapsed() > Duration::from_secs(5) {
                panic!(
                    "failed to transfer leader to [{}] {:?}, current leader: {:?}",
                    brane_id, leader, cur_leader
                );
            }
            self.transfer_leader(brane_id, leader.clone());
        }
    }

    pub fn get_snap_dir(&self, node_id: u64) -> String {
        self.sim.rl().get_snap_dir(node_id)
    }

    pub fn clear_lightlike_filters(&mut self) {
        let mut sim = self.sim.wl();
        for node_id in sim.get_node_ids() {
            sim.clear_lightlike_filters(node_id);
        }
    }

    // It's similar to `ask_split`, the difference is the msg, it lightlikes, is `Msg::SplitBrane`,
    // and `brane` will not be allegro to that msg.
    // Caller must ensure that the `split_key` is in the `brane`.
    pub fn split_brane(
        &mut self,
        brane: &meta_timeshare::Brane,
        split_key: &[u8],
        cb: Callback<LmdbSnapshot>,
    ) {
        let leader = self.leader_of_brane(brane.get_id()).unwrap();
        let router = self.sim.rl().get_router(leader.get_store_id()).unwrap();
        let split_key = split_key.to_vec();
        CasualRouter::lightlike(
            &router,
            brane.get_id(),
            CasualMessage::SplitBrane {
                brane_epoch: brane.get_brane_epoch().clone(),
                split_tuplespaceInstanton: vec![split_key],
                callback: cb,
            },
        )
        .unwrap();
    }

    pub fn must_split(&mut self, brane: &meta_timeshare::Brane, split_key: &[u8]) {
        let mut try_cnt = 0;
        let split_count = self.fidel_client.get_split_count();
        loop {
            // In case ask split message is ignored, we should retry.
            if try_cnt % 50 == 0 {
                self.reset_leader_of_brane(brane.get_id());
                let key = split_key.to_vec();
                let check = Box::new(move |write_resp: WriteResponse| {
                    let mut resp = write_resp.response;
                    if resp.get_header().has_error() {
                        let error = resp.get_header().get_error();
                        if error.has_epoch_not_match()
                            || error.has_not_leader()
                            || error.has_stale_command()
                            || error
                                .get_message()
                                .contains("peer has not applied to current term")
                        {
                            warn!("fail to split: {:?}, ignore.", error);
                            return;
                        }
                        panic!("failed to split: {:?}", resp);
                    }
                    let admin_resp = resp.mut_admin_response();
                    let split_resp = admin_resp.mut_splits();
                    let branes = split_resp.get_branes();
                    assert_eq!(branes.len(), 2);
                    assert_eq!(branes[0].get_lightlike_key(), key.as_slice());
                    assert_eq!(branes[0].get_lightlike_key(), branes[1].get_spacelike_key());
                });
                self.split_brane(brane, split_key, Callback::Write(check));
            }

            if self.fidel_client.check_split(brane, split_key)
                && self.fidel_client.get_split_count() > split_count
            {
                return;
            }

            if try_cnt > 250 {
                panic!(
                    "brane {:?} has not been split by {}",
                    brane,
                    hex::encode_upper(split_key)
                );
            }
            try_cnt += 1;
            sleep_ms(20);
        }
    }

    pub fn wait_brane_split(&mut self, brane: &meta_timeshare::Brane) {
        self.wait_brane_split_max_cnt(brane, 20, 250, true);
    }

    pub fn wait_brane_split_max_cnt(
        &mut self,
        brane: &meta_timeshare::Brane,
        itvl_ms: u64,
        max_try_cnt: u64,
        is_panic: bool,
    ) {
        let mut try_cnt = 0;
        let split_count = self.fidel_client.get_split_count();
        loop {
            if self.fidel_client.get_split_count() > split_count {
                match self.fidel_client.get_brane(brane.get_spacelike_key()) {
                    Err(_) => {}
                    Ok(left) => {
                        if left.get_lightlike_key() != brane.get_lightlike_key() {
                            return;
                        }
                    }
                };
            }

            if try_cnt > max_try_cnt {
                if is_panic {
                    panic!(
                        "brane {:?} has not been split after {}ms",
                        brane,
                        max_try_cnt * itvl_ms
                    );
                } else {
                    return;
                }
            }
            try_cnt += 1;
            sleep_ms(itvl_ms);
        }
    }

    pub fn try_merge(&mut self, source: u64, target: u64) -> VioletaBftCmdResponse {
        let brane = block_on(self.fidel_client.get_brane_by_id(target))
            .unwrap()
            .unwrap();
        let prepare_merge = new_prepare_merge(brane);
        let source_brane = block_on(self.fidel_client.get_brane_by_id(source))
            .unwrap()
            .unwrap();
        let req = new_admin_request(
            source_brane.get_id(),
            source_brane.get_brane_epoch(),
            prepare_merge,
        );
        self.call_command_on_leader(req, Duration::from_secs(5))
            .unwrap()
    }

    pub fn must_try_merge(&mut self, source: u64, target: u64) {
        let resp = self.try_merge(source, target);
        if is_error_response(&resp) {
            panic!(
                "{} failed to try merge to {}, resp {:?}",
                source, target, resp
            );
        }
    }

    /// Make sure brane exists on that store.
    pub fn must_brane_exist(&mut self, brane_id: u64, store_id: u64) {
        let mut try_cnt = 0;
        loop {
            let find_leader =
                new_status_request(brane_id, new_peer(store_id, 0), new_brane_leader_cmd());
            let resp = self
                .call_command(find_leader, Duration::from_secs(5))
                .unwrap();

            if !is_error_response(&resp) {
                return;
            }

            if try_cnt > 250 {
                panic!(
                    "brane {} doesn't exist on store {} after {} tries",
                    brane_id, store_id, try_cnt
                );
            }
            try_cnt += 1;
            sleep_ms(20);
        }
    }

    /// Make sure brane not exists on that store.
    pub fn must_brane_not_exist(&mut self, brane_id: u64, store_id: u64) {
        let mut try_cnt = 0;
        loop {
            let status_cmd = new_brane_detail_cmd();
            let peer = new_peer(store_id, 0);
            let req = new_status_request(brane_id, peer, status_cmd);
            let resp = self.call_command(req, Duration::from_secs(5)).unwrap();
            if resp.get_header().has_error() && resp.get_header().get_error().has_brane_not_found()
            {
                return;
            }

            if try_cnt > 250 {
                panic!(
                    "brane {} still exists on store {} after {} tries: {:?}",
                    brane_id, store_id, try_cnt, resp
                );
            }
            try_cnt += 1;
            sleep_ms(20);
        }
    }

    pub fn must_remove_brane(&mut self, store_id: u64, brane_id: u64) {
        let timer = Instant::now();
        loop {
            let peer = new_peer(store_id, 0);
            let find_leader = new_status_request(brane_id, peer, new_brane_leader_cmd());
            let resp = self
                .call_command(find_leader, Duration::from_secs(5))
                .unwrap();

            if is_error_response(&resp) {
                assert!(
                    resp.get_header().get_error().has_brane_not_found(),
                    "unexpected error resp: {:?}",
                    resp
                );
                break;
            }
            if timer.elapsed() > Duration::from_secs(60) {
                panic!("brane {} is not removed after 60s.", brane_id);
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    // it's so common that we provide an API for it
    pub fn partition(&self, s1: Vec<u64>, s2: Vec<u64>) {
        self.add_lightlike_filter(PartitionFilterFactory::new(s1, s2));
    }

    // Request a snapshot on the given brane.
    pub fn must_request_snapshot(&self, store_id: u64, brane_id: u64) -> u64 {
        // Request snapshot.
        let (request_tx, request_rx) = mpsc::channel();
        let router = self.sim.rl().get_router(store_id).unwrap();
        CasualRouter::lightlike(
            &router,
            brane_id,
            CasualMessage::AccessPeer(Box::new(move |peer: &mut dyn AbstractPeer| {
                let idx = peer.violetabft_committed_index();
                peer.violetabft_request_snapshot(idx);
                debug!("{} request snapshot at {:?}", idx, peer.meta_peer());
                request_tx.lightlike(idx).unwrap();
            })),
        )
        .unwrap();
        request_rx.recv_timeout(Duration::from_secs(5)).unwrap()
    }
}

impl<T: Simulator> Drop for Cluster<T> {
    fn drop(&mut self) {
        test_util::clear_failpoints();
        self.shutdown();
    }
}
