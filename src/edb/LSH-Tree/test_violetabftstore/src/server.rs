// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::{thread, usize};

use futures::executor::block_on;
use grpcio::{ChannelBuilder, EnvBuilder, Environment, Error as GrpcError, Service};
use ekvproto::deadlock::create_deadlock;
use ekvproto::debug_timeshare::{create_debug, DebugClient};
use ekvproto::import_sst_timeshare::create_import_sst;
use ekvproto::kvrpc_timeshare::Context;
use ekvproto::meta_timeshare;
use ekvproto::violetabft_cmd_timeshare::*;
use ekvproto::violetabft_server_timeshare;
use ekvproto::edb_timeshare::EINSTEINDBClient;
use tempfile::{Builder, TempDir};
use tokio::runtime::Builder as TokioBuilder;

use super::*;
use interlocking_directorate::ConcurrencyManager;
use encryption::DataKeyManager;
use engine_lmdb::{LmdbEngine, LmdbSnapshot};
use edb::{Engines, MiscExt};
use fidel_client::FidelClient;
use violetabftstore::interlock::{InterlockHost, BraneInfoAccessor};
use violetabftstore::errors::Error as VioletaBftError;
use violetabftstore::router::{
    LocalReadRouter, VioletaBftStoreBlackHole, VioletaBftStoreRouter, ServerVioletaBftStoreRouter,
};
use violetabftstore::store::fsm::store::StoreMeta;
use violetabftstore::store::fsm::{ApplyRouter, VioletaBftBatchSystem, VioletaBftRouter};
use violetabftstore::store::{
    AutoSplitController, Callback, LocalReader, SnapManagerBuilder, SplitCheckRunner,
};
use violetabftstore::Result;
use security::SecurityManager;
use edb::config::{ConfigController, EINSTEINDBConfig};
use edb::interlock;
use edb::import::{ImportSSTService, SSTImporter};
use edb::read_pool::ReadPool;
use edb::server::gc_worker::GcWorker;
use edb::server::load_statistics::ThreadLoad;
use edb::server::lock_manager::LockManager;
use edb::server::resolve::{self, Task as ResolveTask};
use edb::server::service::DebugService;
use edb::server::Result as ServerResult;
use edb::server::{
    create_violetabft_causet_storage, Config, Error, Node, FidelStoreAddrResolver, VioletaBftClient, VioletaBftKv, Server,
    ServerTransport,
};
use edb::causet_storage;
use violetabftstore::interlock::::collections::{HashMap, HashSet};
use violetabftstore::interlock::::config::VersionTrack;
use violetabftstore::interlock::::time::ThreadReadId;
use violetabftstore::interlock::::worker::{FutureWorker, Worker};
use violetabftstore::interlock::::HandyRwLock;

type SimulateStoreTransport = SimulateTransport<ServerVioletaBftStoreRouter<LmdbEngine, LmdbEngine>>;
type SimulateServerTransport =
    SimulateTransport<ServerTransport<SimulateStoreTransport, FidelStoreAddrResolver>>;

pub type SimulateEngine = VioletaBftKv<SimulateStoreTransport>;

struct ServerMeta {
    node: Node<TestFidelClient, LmdbEngine>,
    server: Server<SimulateStoreTransport, FidelStoreAddrResolver>,
    sim_router: SimulateStoreTransport,
    sim_trans: SimulateServerTransport,
    raw_router: VioletaBftRouter<LmdbEngine, LmdbEngine>,
    raw_apply_router: ApplyRouter<LmdbEngine>,
    worker: Worker<ResolveTask>,
    gc_worker: GcWorker<VioletaBftKv<SimulateStoreTransport>, SimulateStoreTransport>,
}

type PlightlikeingServices = Vec<Box<dyn Fn() -> Service>>;
type CopHooks = Vec<Box<dyn Fn(&mut InterlockHost<LmdbEngine>)>>;

pub struct ServerCluster {
    metas: HashMap<u64, ServerMeta>,
    addrs: HashMap<u64, String>,
    pub causet_storages: HashMap<u64, SimulateEngine>,
    pub brane_info_accessors: HashMap<u64, BraneInfoAccessor>,
    pub importers: HashMap<u64, Arc<SSTImporter>>,
    pub plightlikeing_services: HashMap<u64, PlightlikeingServices>,
    pub interlock_hooks: HashMap<u64, CopHooks>,
    snap_paths: HashMap<u64, TempDir>,
    fidel_client: Arc<TestFidelClient>,
    violetabft_client: VioletaBftClient<VioletaBftStoreBlackHole>,
    interlocking_directorates: HashMap<u64, ConcurrencyManager>,
}

impl ServerCluster {
    pub fn new(fidel_client: Arc<TestFidelClient>) -> ServerCluster {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)
                .name_prefix(thd_name!("server-cluster"))
                .build(),
        );
        let security_mgr = Arc::new(SecurityManager::new(&Default::default()).unwrap());
        let violetabft_client = VioletaBftClient::new(
            env,
            Arc::new(Config::default()),
            security_mgr,
            VioletaBftStoreBlackHole,
            Arc::new(ThreadLoad::with_memory_barrier(usize::MAX)),
            None,
        );
        ServerCluster {
            metas: HashMap::default(),
            addrs: HashMap::default(),
            fidel_client,
            causet_storages: HashMap::default(),
            brane_info_accessors: HashMap::default(),
            importers: HashMap::default(),
            snap_paths: HashMap::default(),
            plightlikeing_services: HashMap::default(),
            interlock_hooks: HashMap::default(),
            violetabft_client,
            interlocking_directorates: HashMap::default(),
        }
    }

    pub fn get_addr(&self, node_id: u64) -> &str {
        &self.addrs[&node_id]
    }

    pub fn get_apply_router(&self, node_id: u64) -> ApplyRouter<LmdbEngine> {
        self.metas.get(&node_id).unwrap().raw_apply_router.clone()
    }

    pub fn get_server_router(&self, node_id: u64) -> SimulateStoreTransport {
        self.metas.get(&node_id).unwrap().sim_router.clone()
    }

    /// To trigger GC manually.
    pub fn get_gc_worker(
        &self,
        node_id: u64,
    ) -> &GcWorker<VioletaBftKv<SimulateStoreTransport>, SimulateStoreTransport> {
        &self.metas.get(&node_id).unwrap().gc_worker
    }

    pub fn get_interlocking_directorate(&self, node_id: u64) -> ConcurrencyManager {
        self.interlocking_directorates.get(&node_id).unwrap().clone()
    }
}

impl Simulator for ServerCluster {
    fn run_node(
        &mut self,
        node_id: u64,
        mut causet: EINSTEINDBConfig,
        engines: Engines<LmdbEngine, LmdbEngine>,
        store_meta: Arc<Mutex<StoreMeta>>,
        key_manager: Option<Arc<DataKeyManager>>,
        router: VioletaBftRouter<LmdbEngine, LmdbEngine>,
        system: VioletaBftBatchSystem<LmdbEngine, LmdbEngine>,
    ) -> ServerResult<u64> {
        let (tmp_str, tmp) = if node_id == 0 || !self.snap_paths.contains_key(&node_id) {
            let p = Builder::new().prefix("test_cluster").temfidelir().unwrap();
            (p.path().to_str().unwrap().to_owned(), Some(p))
        } else {
            let p = self.snap_paths[&node_id].path().to_str().unwrap();
            (p.to_owned(), None)
        };

        // Now we cache the store address, so here we should re-use last
        // listening address for the same store.
        if let Some(addr) = self.addrs.get(&node_id) {
            causet.server.addr = addr.clone();
        }

        let local_reader = LocalReader::new(engines.kv.clone(), store_meta.clone(), router.clone());
        let violetabft_router = ServerVioletaBftStoreRouter::new(router.clone(), local_reader);
        let sim_router = SimulateTransport::new(violetabft_router.clone());

        let violetabft_engine = VioletaBftKv::new(sim_router.clone(), engines.kv.clone());

        // Create interlock.
        let mut interlock_host = InterlockHost::new(router.clone());

        let brane_info_accessor = BraneInfoAccessor::new(&mut interlock_host);
        brane_info_accessor.spacelike();

        if let Some(hooks) = self.interlock_hooks.get(&node_id) {
            for hook in hooks {
                hook(&mut interlock_host);
            }
        }

        // Create causet_storage.
        let fidel_worker = FutureWorker::new("test-fidel-worker");
        let causet_storage_read_pool = ReadPool::from(causet_storage::build_read_pool_for_test(
            &edb::config::StorageReadPoolConfig::default_for_test(),
            violetabft_engine.clone(),
        ));

        let engine = VioletaBftKv::new(sim_router.clone(), engines.kv.clone());

        let mut gc_worker = GcWorker::new(
            engine.clone(),
            sim_router.clone(),
            causet.gc.clone(),
            Default::default(),
        );
        gc_worker.spacelike().unwrap();
        gc_worker
            .spacelike_observe_lock_apply(&mut interlock_host)
            .unwrap();

        let latest_ts =
            block_on(self.fidel_client.get_tso()).expect("failed to get timestamp from FIDel");
        let interlocking_directorate = ConcurrencyManager::new(latest_ts);
        let mut lock_mgr = LockManager::new();
        let store = create_violetabft_causet_storage(
            engine,
            &causet.causet_storage,
            causet_storage_read_pool.handle(),
            lock_mgr.clone(),
            interlocking_directorate.clone(),
            false,
        )?;
        self.causet_storages.insert(node_id, violetabft_engine);

        let security_mgr = Arc::new(SecurityManager::new(&causet.security).unwrap());
        // Create import service.
        let importer = {
            let dir = Path::new(engines.kv.path()).join("import-sst");
            Arc::new(SSTImporter::new(dir, None).unwrap())
        };
        let import_service = ImportSSTService::new(
            causet.import.clone(),
            sim_router.clone(),
            engines.kv.clone(),
            Arc::clone(&importer),
            security_mgr.clone(),
        );

        // Create deadlock service.
        let deadlock_service = lock_mgr.deadlock_service(security_mgr.clone());

        // Create fidel client, snapshot manager, server.
        let (worker, resolver, state) =
            resolve::new_resolver(Arc::clone(&self.fidel_client), router.clone()).unwrap();
        let snap_mgr = SnapManagerBuilder::default()
            .encryption_key_manager(key_manager)
            .build(tmp_str);
        let server_causet = Arc::new(causet.server.clone());
        let cop_read_pool = ReadPool::from(interlock::readpool_impl::build_read_pool_for_test(
            &edb::config::CoprReadPoolConfig::default_for_test(),
            store.get_engine(),
        ));
        let causet = interlock::node::new(
            &server_causet,
            cop_read_pool.handle(),
            interlocking_directorate.clone(),
        );
        let mut server = None;
        // Create Debug service.
        let debug_thread_pool = Arc::new(
            TokioBuilder::new()
                .threaded_interlock_semaphore()
                .thread_name(thd_name!("debugger"))
                .core_threads(1)
                .build()
                .unwrap(),
        );
        let debug_thread_handle = debug_thread_pool.handle().clone();
        let debug_service = DebugService::new(
            engines.clone(),
            debug_thread_handle,
            violetabft_router,
            ConfigController::default(),
            security_mgr.clone(),
        );

        for _ in 0..100 {
            let mut svr = Server::new(
                &server_causet,
                &security_mgr,
                store.clone(),
                causet.clone(),
                sim_router.clone(),
                resolver.clone(),
                snap_mgr.clone(),
                gc_worker.clone(),
                None,
                debug_thread_pool.clone(),
            )
            .unwrap();
            svr.register_service(create_import_sst(import_service.clone()));
            svr.register_service(create_debug(debug_service.clone()));
            svr.register_service(create_deadlock(deadlock_service.clone()));
            if let Some(svcs) = self.plightlikeing_services.get(&node_id) {
                for fact in svcs {
                    svr.register_service(fact());
                }
            }
            match svr.build_and_bind() {
                Ok(_) => {
                    server = Some(svr);
                    break;
                }
                Err(Error::Grpc(GrpcError::BindFail(ref addr, ref port))) => {
                    // Servers may meet the error, when we respacelike them.
                    debug!("fail to create a server: bind fail {:?}", (addr, port));
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                Err(ref e) => panic!("fail to create a server: {:?}", e),
            }
        }
        let mut server = server.unwrap();
        let addr = server.listening_addr();
        causet.server.addr = format!("{}", addr);
        let trans = server.transport();
        let simulate_trans = SimulateTransport::new(trans);
        let server_causet = Arc::new(causet.server.clone());
        let apply_router = system.apply_router();

        // Create node.
        let mut violetabft_store = causet.violetabft_store.clone();
        violetabft_store.validate().unwrap();
        let mut node = Node::new(
            system,
            &causet.server,
            Arc::new(VersionTrack::new(violetabft_store)),
            Arc::clone(&self.fidel_client),
            state,
        );

        // Register the role change semaphore of the dagger manager.
        lock_mgr.register_detector_role_change_semaphore(&mut interlock_host);

        let pessimistic_txn_causet = causet.pessimistic_txn.clone();

        let mut split_check_worker = Worker::new("split-check");
        let split_check_runner = SplitCheckRunner::new(
            engines.kv.clone(),
            router.clone(),
            interlock_host.clone(),
            causet.interlock,
        );
        split_check_worker.spacelike(split_check_runner).unwrap();

        node.spacelike(
            engines,
            simulate_trans.clone(),
            snap_mgr,
            fidel_worker,
            store_meta,
            interlock_host,
            importer.clone(),
            split_check_worker,
            AutoSplitController::default(),
            interlocking_directorate.clone(),
        )?;
        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();
        if let Some(tmp) = tmp {
            self.snap_paths.insert(node_id, tmp);
        }
        self.brane_info_accessors
            .insert(node_id, brane_info_accessor);
        self.importers.insert(node_id, importer);

        lock_mgr
            .spacelike(
                node.id(),
                Arc::clone(&self.fidel_client),
                resolver,
                Arc::clone(&security_mgr),
                &pessimistic_txn_causet,
            )
            .unwrap();

        server.spacelike(server_causet, security_mgr).unwrap();

        self.metas.insert(
            node_id,
            ServerMeta {
                raw_router: router,
                raw_apply_router: apply_router,
                node,
                server,
                sim_router,
                sim_trans: simulate_trans,
                worker,
                gc_worker,
            },
        );
        self.addrs.insert(node_id, format!("{}", addr));
        self.interlocking_directorates
            .insert(node_id, interlocking_directorate);

        Ok(node_id)
    }

    fn get_snap_dir(&self, node_id: u64) -> String {
        self.snap_paths[&node_id]
            .path()
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn stop_node(&mut self, node_id: u64) {
        if let Some(mut meta) = self.metas.remove(&node_id) {
            meta.server.stop().unwrap();
            meta.node.stop();
            meta.worker.stop().unwrap().join().unwrap();
        }
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.metas.tuplespaceInstanton().cloned().collect()
    }

    fn async_command_on_node(
        &self,
        node_id: u64,
        request: VioletaBftCmdRequest,
        cb: Callback<LmdbSnapshot>,
    ) -> Result<()> {
        let router = match self.metas.get(&node_id) {
            None => return Err(box_err!("missing lightlikeer for store {}", node_id)),
            Some(meta) => meta.sim_router.clone(),
        };
        router.lightlike_command(request, cb)
    }

    fn async_read(
        &self,
        node_id: u64,
        batch_id: Option<ThreadReadId>,
        request: VioletaBftCmdRequest,
        cb: Callback<LmdbSnapshot>,
    ) {
        match self.metas.get(&node_id) {
            None => {
                let e: VioletaBftError = box_err!("missing lightlikeer for store {}", node_id);
                let mut resp = VioletaBftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                cb.invoke_with_response(resp);
            }
            Some(meta) => {
                meta.sim_router.read(batch_id, request, cb).unwrap();
            }
        };
    }

    fn lightlike_violetabft_msg(&mut self, violetabft_msg: violetabft_server_timeshare::VioletaBftMessage) -> Result<()> {
        let store_id = violetabft_msg.get_to_peer().get_store_id();
        let addr = self.get_addr(store_id).to_owned();
        self.violetabft_client.lightlike(store_id, &addr, violetabft_msg).unwrap();
        self.violetabft_client.flush();
        Ok(())
    }

    fn add_lightlike_filter(&mut self, node_id: u64, filter: Box<dyn Filter>) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .sim_trans
            .add_filter(filter);
    }

    fn clear_lightlike_filters(&mut self, node_id: u64) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .sim_trans
            .clear_filters();
    }

    fn add_recv_filter(&mut self, node_id: u64, filter: Box<dyn Filter>) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .sim_router
            .add_filter(filter);
    }

    fn clear_recv_filters(&mut self, node_id: u64) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .sim_router
            .clear_filters();
    }

    fn get_router(&self, node_id: u64) -> Option<VioletaBftRouter<LmdbEngine, LmdbEngine>> {
        self.metas.get(&node_id).map(|m| m.raw_router.clone())
    }
}

pub fn new_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let fidel_client = Arc::new(TestFidelClient::new(id, false));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&fidel_client))));
    Cluster::new(id, count, sim, fidel_client)
}

pub fn new_incompatible_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let fidel_client = Arc::new(TestFidelClient::new(id, true));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&fidel_client))));
    Cluster::new(id, count, sim, fidel_client)
}

pub fn must_new_cluster() -> (Cluster<ServerCluster>, meta_timeshare::Peer, Context) {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    let brane_id = 1;
    let leader = cluster.leader_of_brane(brane_id).unwrap();
    let epoch = cluster.get_brane_epoch(brane_id);
    let mut ctx = Context::default();
    ctx.set_brane_id(brane_id);
    ctx.set_peer(leader.clone());
    ctx.set_brane_epoch(epoch);

    (cluster, leader, ctx)
}

pub fn must_new_cluster_and_kv_client() -> (Cluster<ServerCluster>, EINSTEINDBClient, Context) {
    let (cluster, leader, ctx) = must_new_cluster();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = EINSTEINDBClient::new(channel);

    (cluster, client, ctx)
}

pub fn must_new_cluster_and_debug_client() -> (Cluster<ServerCluster>, DebugClient, u64) {
    let (cluster, leader, _) = must_new_cluster();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = DebugClient::new(channel);

    (cluster, client, leader.get_store_id())
}
