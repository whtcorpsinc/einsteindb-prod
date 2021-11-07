// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use tempfile::{Builder, TempDir};

use ekvproto::meta_timeshare;
use ekvproto::violetabft_cmd_timeshare::*;
use ekvproto::violetabft_server_timeshare::{self, VioletaBftMessage};
use violetabft::evioletabft_timeshare::MessageType;
use violetabft::SnapshotStatus;

use super::*;
use interlocking_directorate::ConcurrencyManager;
use encryption::DataKeyManager;
use engine_lmdb::{LmdbEngine, LmdbSnapshot};
use edb::{Engines, MiscExt, Peekable};
use violetabftstore::interlock::config::SplitCheckConfigManager;
use violetabftstore::interlock::InterlockHost;
use violetabftstore::errors::Error as VioletaBftError;
use violetabftstore::router::{LocalReadRouter, VioletaBftStoreRouter, ServerVioletaBftStoreRouter};
use violetabftstore::store::config::VioletaBftstoreConfigManager;
use violetabftstore::store::fsm::store::StoreMeta;
use violetabftstore::store::fsm::{VioletaBftBatchSystem, VioletaBftRouter};
use violetabftstore::store::SnapManagerBuilder;
use violetabftstore::store::*;
use violetabftstore::Result;
use edb::config::{ConfigController, Module, EINSTEINDBConfig};
use edb::import::SSTImporter;
use edb::server::Node;
use edb::server::Result as ServerResult;
use violetabftstore::interlock::::collections::{HashMap, HashSet};
use violetabftstore::interlock::::config::VersionTrack;
use violetabftstore::interlock::::time::ThreadReadId;
use violetabftstore::interlock::::worker::{FutureWorker, Worker};

pub struct ChannelTransportCore {
    snap_paths: HashMap<u64, (SnapManager, TempDir)>,
    routers: HashMap<u64, SimulateTransport<ServerVioletaBftStoreRouter<LmdbEngine, LmdbEngine>>>,
}

#[derive(Clone)]
pub struct ChannelTransport {
    core: Arc<Mutex<ChannelTransportCore>>,
}

impl ChannelTransport {
    pub fn new() -> ChannelTransport {
        ChannelTransport {
            core: Arc::new(Mutex::new(ChannelTransportCore {
                snap_paths: HashMap::default(),
                routers: HashMap::default(),
            })),
        }
    }
}

impl Transport for ChannelTransport {
    fn lightlike(&mut self, msg: VioletaBftMessage) -> Result<()> {
        let from_store = msg.get_from_peer().get_store_id();
        let to_store = msg.get_to_peer().get_store_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let brane_id = msg.get_brane_id();
        let is_snapshot = msg.get_message().get_msg_type() == MessageType::MsgSnapshot;

        if is_snapshot {
            let snap = msg.get_message().get_snapshot();
            let key = SnapKey::from_snap(snap).unwrap();
            let from = match self.core.dagger().unwrap().snap_paths.get(&from_store) {
                Some(p) => {
                    p.0.register(key.clone(), SnapEntry::lightlikeing);
                    p.0.get_snapshot_for_lightlikeing(&key).unwrap()
                }
                None => return Err(box_err!("missing temp dir for store {}", from_store)),
            };
            let to = match self.core.dagger().unwrap().snap_paths.get(&to_store) {
                Some(p) => {
                    p.0.register(key.clone(), SnapEntry::Receiving);
                    let data = msg.get_message().get_snapshot().get_data();
                    p.0.get_snapshot_for_receiving(&key, data).unwrap()
                }
                None => return Err(box_err!("missing temp dir for store {}", to_store)),
            };

            defer!({
                let core = self.core.dagger().unwrap();
                core.snap_paths[&from_store]
                    .0
                    .deregister(&key, &SnapEntry::lightlikeing);
                core.snap_paths[&to_store]
                    .0
                    .deregister(&key, &SnapEntry::Receiving);
            });

            copy_snapshot(from, to)?;
        }

        let core = self.core.dagger().unwrap();

        match core.routers.get(&to_store) {
            Some(h) => {
                h.lightlike_violetabft_msg(msg)?;
                if is_snapshot {
                    // should report snapshot finish.
                    let _ = core.routers[&from_store].report_snapshot_status(
                        brane_id,
                        to_peer_id,
                        SnapshotStatus::Finish,
                    );
                }
                Ok(())
            }
            _ => Err(box_err!("missing lightlikeer for store {}", to_store)),
        }
    }

    fn flush(&mut self) {}
}

type SimulateChannelTransport = SimulateTransport<ChannelTransport>;

pub struct NodeCluster {
    trans: ChannelTransport,
    fidel_client: Arc<TestFidelClient>,
    nodes: HashMap<u64, Node<TestFidelClient, LmdbEngine>>,
    simulate_trans: HashMap<u64, SimulateChannelTransport>,
    #[allow(clippy::type_complexity)]
    post_create_interlock_host: Option<Box<dyn Fn(u64, &mut InterlockHost<LmdbEngine>)>>,
}

impl NodeCluster {
    pub fn new(fidel_client: Arc<TestFidelClient>) -> NodeCluster {
        NodeCluster {
            trans: ChannelTransport::new(),
            fidel_client,
            nodes: HashMap::default(),
            simulate_trans: HashMap::default(),
            post_create_interlock_host: None,
        }
    }
}

impl NodeCluster {
    #[allow(dead_code)]
    pub fn get_node_router(
        &self,
        node_id: u64,
    ) -> SimulateTransport<ServerVioletaBftStoreRouter<LmdbEngine, LmdbEngine>> {
        self.trans
            .core
            .dagger()
            .unwrap()
            .routers
            .get(&node_id)
            .cloned()
            .unwrap()
    }

    // Set a function that will be invoked after creating each InterlockHost. The first argument
    // of `op` is the node_id.
    // Set this before invoking `run_node`.
    #[allow(clippy::type_complexity)]
    pub fn post_create_interlock_host(
        &mut self,
        op: Box<dyn Fn(u64, &mut InterlockHost<LmdbEngine>)>,
    ) {
        self.post_create_interlock_host = Some(op)
    }

    pub fn get_node(&mut self, node_id: u64) -> Option<&mut Node<TestFidelClient, LmdbEngine>> {
        self.nodes.get_mut(&node_id)
    }
}

impl Simulator for NodeCluster {
    fn run_node(
        &mut self,
        node_id: u64,
        causet: EINSTEINDBConfig,
        engines: Engines<LmdbEngine, LmdbEngine>,
        store_meta: Arc<Mutex<StoreMeta>>,
        key_manager: Option<Arc<DataKeyManager>>,
        router: VioletaBftRouter<LmdbEngine, LmdbEngine>,
        system: VioletaBftBatchSystem<LmdbEngine, LmdbEngine>,
    ) -> ServerResult<u64> {
        assert!(node_id == 0 || !self.nodes.contains_key(&node_id));
        let fidel_worker = FutureWorker::new("test-fidel-worker");

        let simulate_trans = SimulateTransport::new(self.trans.clone());
        let mut violetabft_store = causet.violetabft_store.clone();
        violetabft_store.validate().unwrap();
        let mut node = Node::new(
            system,
            &causet.server,
            Arc::new(VersionTrack::new(violetabft_store)),
            Arc::clone(&self.fidel_client),
            Arc::default(),
        );

        let (snap_mgr, snap_mgr_path) = if node_id == 0
            || !self
                .trans
                .core
                .dagger()
                .unwrap()
                .snap_paths
                .contains_key(&node_id)
        {
            let tmp = Builder::new().prefix("test_cluster").temfidelir().unwrap();
            let snap_mgr = SnapManagerBuilder::default()
                .encryption_key_manager(key_manager)
                .build(tmp.path().to_str().unwrap());
            (snap_mgr, Some(tmp))
        } else {
            let trans = self.trans.core.dagger().unwrap();
            let &(ref snap_mgr, _) = &trans.snap_paths[&node_id];
            (snap_mgr.clone(), None)
        };

        // Create interlock.
        let mut interlock_host = InterlockHost::new(router.clone());

        if let Some(f) = self.post_create_interlock_host.as_ref() {
            f(node_id, &mut interlock_host);
        }

        let importer = {
            let dir = Path::new(engines.kv.path()).join("import-sst");
            Arc::new(SSTImporter::new(dir, None).unwrap())
        };

        let local_reader = LocalReader::new(engines.kv.clone(), store_meta.clone(), router.clone());
        let causet_controller = ConfigController::new(causet.clone());

        let mut split_check_worker = Worker::new("split-check");
        let split_check_runner = SplitCheckRunner::new(
            engines.kv.clone(),
            router.clone(),
            interlock_host.clone(),
            causet.interlock.clone(),
        );
        split_check_worker.spacelike(split_check_runner).unwrap();
        causet_controller.register(
            Module::Interlock,
            Box::new(SplitCheckConfigManager(split_check_worker.interlock_semaphore())),
        );

        let mut violetabftstore_causet = causet.violetabft_store;
        violetabftstore_causet.validate().unwrap();
        let violetabft_store = Arc::new(VersionTrack::new(violetabftstore_causet));
        causet_controller.register(
            Module::VioletaBftstore,
            Box::new(VioletaBftstoreConfigManager(violetabft_store)),
        );

        node.spacelike(
            engines.clone(),
            simulate_trans.clone(),
            snap_mgr.clone(),
            fidel_worker,
            store_meta,
            interlock_host,
            importer,
            split_check_worker,
            AutoSplitController::default(),
            ConcurrencyManager::new(1.into()),
        )?;
        assert!(engines
            .kv
            .get_msg::<meta_timeshare::Brane>(tuplespaceInstanton::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .is_none());
        assert!(node_id == 0 || node_id == node.id());

        let node_id = node.id();
        debug!(
            "node_id: {} tmp: {:?}",
            node_id,
            snap_mgr_path
                .as_ref()
                .map(|p| p.path().to_str().unwrap().to_owned())
        );
        if let Some(tmp) = snap_mgr_path {
            self.trans
                .core
                .dagger()
                .unwrap()
                .snap_paths
                .insert(node_id, (snap_mgr, tmp));
        }

        let router = ServerVioletaBftStoreRouter::new(router, local_reader);
        self.trans
            .core
            .dagger()
            .unwrap()
            .routers
            .insert(node_id, SimulateTransport::new(router));
        self.nodes.insert(node_id, node);
        self.simulate_trans.insert(node_id, simulate_trans);

        Ok(node_id)
    }

    fn get_snap_dir(&self, node_id: u64) -> String {
        self.trans.core.dagger().unwrap().snap_paths[&node_id]
            .1
            .path()
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn stop_node(&mut self, node_id: u64) {
        if let Some(mut node) = self.nodes.remove(&node_id) {
            node.stop();
        }
        self.trans
            .core
            .dagger()
            .unwrap()
            .routers
            .remove(&node_id)
            .unwrap();
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.nodes.tuplespaceInstanton().cloned().collect()
    }

    fn async_command_on_node(
        &self,
        node_id: u64,
        request: VioletaBftCmdRequest,
        cb: Callback<LmdbSnapshot>,
    ) -> Result<()> {
        if !self
            .trans
            .core
            .dagger()
            .unwrap()
            .routers
            .contains_key(&node_id)
        {
            return Err(box_err!("missing lightlikeer for store {}", node_id));
        }

        let router = self
            .trans
            .core
            .dagger()
            .unwrap()
            .routers
            .get(&node_id)
            .cloned()
            .unwrap();
        router.lightlike_command(request, cb)
    }

    fn async_read(
        &self,
        node_id: u64,
        batch_id: Option<ThreadReadId>,
        request: VioletaBftCmdRequest,
        cb: Callback<LmdbSnapshot>,
    ) {
        if !self
            .trans
            .core
            .dagger()
            .unwrap()
            .routers
            .contains_key(&node_id)
        {
            let mut resp = VioletaBftCmdResponse::default();
            let e: VioletaBftError = box_err!("missing lightlikeer for store {}", node_id);
            resp.mut_header().set_error(e.into());
            cb.invoke_with_response(resp);
            return;
        }
        let mut guard = self.trans.core.dagger().unwrap();
        let router = guard.routers.get_mut(&node_id).unwrap();
        router.read(batch_id, request, cb).unwrap();
    }

    fn lightlike_violetabft_msg(&mut self, msg: violetabft_server_timeshare::VioletaBftMessage) -> Result<()> {
        self.trans.lightlike(msg)
    }

    fn add_lightlike_filter(&mut self, node_id: u64, filter: Box<dyn Filter>) {
        self.simulate_trans
            .get_mut(&node_id)
            .unwrap()
            .add_filter(filter);
    }

    fn clear_lightlike_filters(&mut self, node_id: u64) {
        self.simulate_trans
            .get_mut(&node_id)
            .unwrap()
            .clear_filters();
    }

    fn add_recv_filter(&mut self, node_id: u64, filter: Box<dyn Filter>) {
        let mut trans = self.trans.core.dagger().unwrap();
        trans.routers.get_mut(&node_id).unwrap().add_filter(filter);
    }

    fn clear_recv_filters(&mut self, node_id: u64) {
        let mut trans = self.trans.core.dagger().unwrap();
        trans.routers.get_mut(&node_id).unwrap().clear_filters();
    }

    fn get_router(&self, node_id: u64) -> Option<VioletaBftRouter<LmdbEngine, LmdbEngine>> {
        self.nodes.get(&node_id).map(|node| node.get_router())
    }
}

pub fn new_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let fidel_client = Arc::new(TestFidelClient::new(id, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&fidel_client))));
    Cluster::new(id, count, sim, fidel_client)
}

pub fn new_incompatible_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let fidel_client = Arc::new(TestFidelClient::new(id, true));
    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&fidel_client))));
    Cluster::new(id, count, sim, fidel_client)
}
