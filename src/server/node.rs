// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::VioletaBftKv;
use super::Result;
use crate::import::SSTImporter;
use crate::read_pool::ReadPoolHandle;
use crate::server::lock_manager::LockManager;
use crate::server::Config as ServerConfig;
use crate::persistence::{config::Config as StorageConfig, CausetStorage};
use concurrency_manager::ConcurrencyManager;
use engine_lmdb::LmdbEngine;
use engine_promises::{Engines, Peekable, VioletaBftEngine};
use ekvproto::metapb;
use ekvproto::raft_serverpb::StoreIdent;
use ekvproto::replication_modepb::ReplicationStatus;
use fidel_client::{Error as FidelError, FidelClient, INVALID_ID};
use violetabftstore::interlock::dispatcher::InterlockHost;
use violetabftstore::router::{LocalReadRouter, VioletaBftStoreRouter};
use violetabftstore::store::fsm::store::StoreMeta;
use violetabftstore::store::fsm::{ApplyRouter, VioletaBftBatchSystem, VioletaBftRouter};
use violetabftstore::store::AutoSplitController;
use violetabftstore::store::{self, initial_brane, Config as StoreConfig, SnapManager, Transport};
use violetabftstore::store::{GlobalReplicationState, FidelTask, SplitCheckTask};
use einsteindb_util::config::VersionTrack;
use einsteindb_util::worker::FutureWorker;
use einsteindb_util::worker::Worker;

const MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT: u64 = 60;
const CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS: u64 = 3;

/// Creates a new persistence engine which is backed by the VioletaBft consensus
/// protocol.
pub fn create_raft_causetStorage<S>(
    engine: VioletaBftKv<S>,
    causetg: &StorageConfig,
    read_pool: ReadPoolHandle,
    lock_mgr: LockManager,
    concurrency_manager: ConcurrencyManager,
    pipelined_pessimistic_lock: bool,
) -> Result<CausetStorage<VioletaBftKv<S>, LockManager>>
where
    S: VioletaBftStoreRouter<LmdbEngine> + LocalReadRouter<LmdbEngine> + 'static,
{
    let store = CausetStorage::from_engine(
        engine,
        causetg,
        read_pool,
        lock_mgr,
        concurrency_manager,
        pipelined_pessimistic_lock,
    )?;
    Ok(store)
}

/// A wrapper for the violetabftstore which runs Multi-VioletaBft.
// TODO: we will rename another better name like VioletaBftStore later.
pub struct Node<C: FidelClient + 'static, ER: VioletaBftEngine> {
    cluster_id: u64,
    store: metapb::CausetStore,
    store_causetg: Arc<VersionTrack<StoreConfig>>,
    system: VioletaBftBatchSystem<LmdbEngine, ER>,
    has_spacelikeed: bool,

    fidel_client: Arc<C>,
    state: Arc<Mutex<GlobalReplicationState>>,
}

impl<C, ER> Node<C, ER>
where
    C: FidelClient,
    ER: VioletaBftEngine,
{
    /// Creates a new Node.
    pub fn new(
        system: VioletaBftBatchSystem<LmdbEngine, ER>,
        causetg: &ServerConfig,
        store_causetg: Arc<VersionTrack<StoreConfig>>,
        fidel_client: Arc<C>,
        state: Arc<Mutex<GlobalReplicationState>>,
    ) -> Node<C, ER> {
        let mut store = metapb::CausetStore::default();
        store.set_id(INVALID_ID);
        if causetg.advertise_addr.is_empty() {
            store.set_address(causetg.addr.clone());
        } else {
            store.set_address(causetg.advertise_addr.clone())
        }
        if causetg.advertise_status_addr.is_empty() {
            store.set_status_address(causetg.status_addr.clone());
        } else {
            store.set_status_address(causetg.advertise_status_addr.clone())
        }
        store.set_version(env!("CARGO_PKG_VERSION").to_string());

        if let Ok(path) = std::env::current_exe() {
            if let Some(path) = path.parent() {
                store.set_deploy_path(path.to_string_lossy().to_string());
            }
        };

        store.set_spacelike_timestamp(chrono::Local::now().timestamp());
        store.set_git_hash(
            option_env!("EINSTEINDB_BUILD_GIT_HASH")
                .unwrap_or("Unknown git hash")
                .to_string(),
        );

        let mut labels = Vec::new();
        for (k, v) in &causetg.labels {
            let mut label = metapb::StoreLabel::default();
            label.set_key(k.to_owned());
            label.set_value(v.to_owned());
            labels.push(label);
        }
        store.set_labels(labels.into());

        Node {
            cluster_id: causetg.cluster_id,
            store,
            store_causetg,
            fidel_client,
            system,
            has_spacelikeed: false,
            state,
        }
    }

    /// Starts the Node. It tries to bootstrap cluster if the cluster is not
    /// bootstrapped yet. Then it spawns a thread to run the violetabftstore in
    /// background.
    #[allow(clippy::too_many_arguments)]
    pub fn spacelike<T>(
        &mut self,
        engines: Engines<LmdbEngine, ER>,
        trans: T,
        snap_mgr: SnapManager,
        fidel_worker: FutureWorker<FidelTask<LmdbEngine>>,
        store_meta: Arc<Mutex<StoreMeta>>,
        interlock_host: InterlockHost<LmdbEngine>,
        importer: Arc<SSTImporter>,
        split_check_worker: Worker<SplitCheckTask>,
        auto_split_controller: AutoSplitController,
        concurrency_manager: ConcurrencyManager,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        let mut store_id = self.check_store(&engines)?;
        if store_id == INVALID_ID {
            store_id = self.bootstrap_store(&engines)?;
            fail_point!("node_after_bootstrap_store", |_| Err(box_err!(
                "injected error: node_after_bootstrap_store"
            )));
        }
        self.store.set_id(store_id);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.store_id = Some(store_id);
        }
        if let Some(first_brane) = self.check_or_prepare_bootstrap_cluster(&engines, store_id)? {
            info!("trying to bootstrap cluster"; "store_id" => store_id, "brane" => ?first_brane);
            // cluster is not bootstrapped, and we choose first store to bootstrap
            fail_point!("node_after_prepare_bootstrap_cluster", |_| Err(box_err!(
                "injected error: node_after_prepare_bootstrap_cluster"
            )));
            self.bootstrap_cluster(&engines, first_brane)?;
        }

        // Put store only if the cluster is bootstrapped.
        info!("put store to FIDel"; "store" => ?&self.store);
        let status = self.fidel_client.put_store(self.store.clone())?;
        self.load_all_stores(status);

        self.spacelike_store(
            store_id,
            engines,
            trans,
            snap_mgr,
            fidel_worker,
            store_meta,
            interlock_host,
            importer,
            split_check_worker,
            auto_split_controller,
            concurrency_manager,
        )?;

        Ok(())
    }

    /// Gets the store id.
    pub fn id(&self) -> u64 {
        self.store.get_id()
    }

    /// Gets a transmission lightlike of a channel which is used to slightlike `Msg` to the
    /// violetabftstore.
    pub fn get_router(&self) -> VioletaBftRouter<LmdbEngine, ER> {
        self.system.router()
    }
    /// Gets a transmission lightlike of a channel which is used slightlike messages to apply worker.
    pub fn get_apply_router(&self) -> ApplyRouter<LmdbEngine> {
        self.system.apply_router()
    }

    // check store, return store id for the engine.
    // If the store is not bootstrapped, use INVALID_ID.
    fn check_store(&self, engines: &Engines<LmdbEngine, ER>) -> Result<u64> {
        let res = engines.kv.get_msg::<StoreIdent>(tuplespaceInstanton::STORE_IDENT_KEY)?;
        if res.is_none() {
            return Ok(INVALID_ID);
        }

        let ident = res.unwrap();
        if ident.get_cluster_id() != self.cluster_id {
            return Err(box_err!(
                "cluster ID mismatch, local {} != remote {}, \
                 you are trying to connect to another cluster, please reconnect to the correct FIDel",
                ident.get_cluster_id(),
                self.cluster_id
            ));
        }

        let store_id = ident.get_store_id();
        if store_id == INVALID_ID {
            return Err(box_err!("invalid store ident {:?}", ident));
        }
        Ok(store_id)
    }

    fn alloc_id(&self) -> Result<u64> {
        let id = self.fidel_client.alloc_id()?;
        Ok(id)
    }

    fn load_all_stores(&mut self, status: Option<ReplicationStatus>) {
        info!("initializing replication mode"; "status" => ?status, "store_id" => self.store.id);
        let stores = match self.fidel_client.get_all_stores(false) {
            Ok(stores) => stores,
            Err(e) => panic!("failed to load all stores: {:?}", e),
        };
        let mut state = self.state.lock().unwrap();
        if let Some(s) = status {
            state.set_status(s);
        }
        for mut store in stores {
            state
                .group
                .register_store(store.id, store.take_labels().into());
        }
    }

    fn bootstrap_store(&self, engines: &Engines<LmdbEngine, ER>) -> Result<u64> {
        let store_id = self.alloc_id()?;
        debug!("alloc store id"; "store_id" => store_id);

        store::bootstrap_store(&engines, self.cluster_id, store_id)?;

        Ok(store_id)
    }

    // Exported for tests.
    #[doc(hidden)]
    pub fn prepare_bootstrap_cluster(
        &self,
        engines: &Engines<LmdbEngine, ER>,
        store_id: u64,
    ) -> Result<metapb::Brane> {
        let brane_id = self.alloc_id()?;
        debug!(
            "alloc first brane id";
            "brane_id" => brane_id,
            "cluster_id" => self.cluster_id,
            "store_id" => store_id
        );
        let peer_id = self.alloc_id()?;
        debug!(
            "alloc first peer id for first brane";
            "peer_id" => peer_id,
            "brane_id" => brane_id,
        );

        let brane = initial_brane(store_id, brane_id, peer_id);
        store::prepare_bootstrap_cluster(&engines, &brane)?;
        Ok(brane)
    }

    fn check_or_prepare_bootstrap_cluster(
        &self,
        engines: &Engines<LmdbEngine, ER>,
        store_id: u64,
    ) -> Result<Option<metapb::Brane>> {
        if let Some(first_brane) = engines.kv.get_msg(tuplespaceInstanton::PREPARE_BOOTSTRAP_KEY)? {
            Ok(Some(first_brane))
        } else {
            if self.check_cluster_bootstrapped()? {
                Ok(None)
            } else {
                self.prepare_bootstrap_cluster(engines, store_id).map(Some)
            }
        }
    }

    fn bootstrap_cluster(
        &mut self,
        engines: &Engines<LmdbEngine, ER>,
        first_brane: metapb::Brane,
    ) -> Result<()> {
        let brane_id = first_brane.get_id();
        let mut retry = 0;
        while retry < MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self
                .fidel_client
                .bootstrap_cluster(self.store.clone(), first_brane.clone())
            {
                Ok(_) => {
                    info!("bootstrap cluster ok"; "cluster_id" => self.cluster_id);
                    fail_point!("node_after_bootstrap_cluster", |_| Err(box_err!(
                        "injected error: node_after_bootstrap_cluster"
                    )));
                    store::clear_prepare_bootstrap_key(&engines)?;
                    return Ok(());
                }
                Err(FidelError::ClusterBootstrapped(_)) => match self.fidel_client.get_brane(b"") {
                    Ok(brane) => {
                        if brane == first_brane {
                            store::clear_prepare_bootstrap_key(&engines)?;
                            return Ok(());
                        } else {
                            info!("cluster is already bootstrapped"; "cluster_id" => self.cluster_id);
                            store::clear_prepare_bootstrap_cluster(&engines, brane_id)?;
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        warn!("get the first brane failed"; "err" => ?e);
                    }
                },
                // TODO: should we clean brane for other errors too?
                Err(e) => error!(?e; "bootstrap cluster"; "cluster_id" => self.cluster_id,),
            }
            retry += 1;
            thread::sleep(Duration::from_secs(
                CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS,
            ));
        }
        Err(box_err!("bootstrapped cluster failed"))
    }

    fn check_cluster_bootstrapped(&self) -> Result<bool> {
        for _ in 0..MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self.fidel_client.is_cluster_bootstrapped() {
                Ok(b) => return Ok(b),
                Err(e) => {
                    warn!("check cluster bootstrapped failed"; "err" => ?e);
                }
            }
            thread::sleep(Duration::from_secs(
                CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS,
            ));
        }
        Err(box_err!("check cluster bootstrapped failed"))
    }

    #[allow(clippy::too_many_arguments)]
    fn spacelike_store<T>(
        &mut self,
        store_id: u64,
        engines: Engines<LmdbEngine, ER>,
        trans: T,
        snap_mgr: SnapManager,
        fidel_worker: FutureWorker<FidelTask<LmdbEngine>>,
        store_meta: Arc<Mutex<StoreMeta>>,
        interlock_host: InterlockHost<LmdbEngine>,
        importer: Arc<SSTImporter>,
        split_check_worker: Worker<SplitCheckTask>,
        auto_split_controller: AutoSplitController,
        concurrency_manager: ConcurrencyManager,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        info!("spacelike violetabft store thread"; "store_id" => store_id);

        if self.has_spacelikeed {
            return Err(box_err!("{} is already spacelikeed", store_id));
        }
        self.has_spacelikeed = true;
        let causetg = self.store_causetg.clone();
        let fidel_client = Arc::clone(&self.fidel_client);
        let store = self.store.clone();
        self.system.spawn(
            store,
            causetg,
            engines,
            trans,
            fidel_client,
            snap_mgr,
            fidel_worker,
            store_meta,
            interlock_host,
            importer,
            split_check_worker,
            auto_split_controller,
            self.state.clone(),
            concurrency_manager,
        )?;
        Ok(())
    }

    fn stop_store(&mut self, store_id: u64) {
        info!("stop violetabft store thread"; "store_id" => store_id);
        self.system.shutdown();
    }

    /// Stops the Node.
    pub fn stop(&mut self) {
        let store_id = self.store.get_id();
        self.stop_store(store_id)
    }
}
