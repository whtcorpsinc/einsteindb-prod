//! This module spacelikeups all the components of a EinsteinDB server.
//!
//! It is responsible for reading from configs, spacelikeing up the various server components,
//! and handling errors (mostly by aborting and reporting to the user).
//!
//! The entry point is `run_edb`.
//!
//! Components are often used to initialize other components, and/or must be explicitly stopped.
//! We keep these components in the `EinsteinDBServer` struct.

use std::{
    convert::TryFrom,
    env, fmt,
    fs::{self, File},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

use interlocking_directorate::ConcurrencyManager;
use encryption::DataKeyManager;
use engine_lmdb::{encryption::get_env, LmdbEngine};
use edb::{
    compaction_job::CompactionJobInfo, Engines, MetricsFlusher, VioletaBftEngine, Causet_DEFAULT, Causet_WRITE,
};
use fs2::FileExt;
use futures::executor::block_on;
use ekvproto::{
    backup::create_backup, causet_context_timeshare::create_change_data, deadlock::create_deadlock,
    debug_timeshare::create_debug, diagnostics_timeshare::create_diagnostics, import_sst_timeshare::create_import_sst,
};
use fidel_client::{FidelClient, RpcClient};
use violetabft_log_engine::VioletaBftLogEngine;
use violetabftstore::{
    interlock::{
        config::SplitCheckConfigManager, BoxConsistencyCheckSemaphore, ConsistencyCheckMethod,
        InterlockHost, RawConsistencyCheckSemaphore, BraneInfoAccessor,
    },
    router::ServerVioletaBftStoreRouter,
    store::{
        config::VioletaBftstoreConfigManager,
        fsm,
        fsm::store::{VioletaBftBatchSystem, VioletaBftRouter, StoreMeta, PENDING_VOTES_CAP},
        AutoSplitController, GlobalReplicationState, LocalReader, SnapManagerBuilder,
        SplitCheckRunner, SplitConfigManager, StoreMsg,
    },
};
use security::SecurityManager;
use edb::{
    config::{ConfigController, DBConfigManger, DBType, EINSTEINDBConfig},
    interlock,
    import::{ImportSSTService, SSTImporter},
    read_pool::{build_yatp_read_pool, ReadPool},
    server::{
        config::Config as ServerConfig,
        create_violetabft_causet_storage,
        gc_worker::{AutoGcConfig, GcWorker},
        lock_manager::LockManager,
        resolve,
        service::{DebugService, DiagnosticsService},
        status_server::StatusServer,
        Node, VioletaBftKv, Server, CPU_CORES_QUOTA_GAUGE, DEFAULT_CLUSTER_ID,
    },
    causet_storage::{self, config::StorageConfigManger},
};
use violetabftstore::interlock::::config::VersionTrack;
use violetabftstore::interlock::::{
    check_environment_variables,
    config::ensure_dir_exist,
    sys::sys_quota::SysQuota,
    time::Monitor,
    worker::{FutureWorker, Worker},
};
use tokio::runtime::Builder;

use crate::{setup::*, signal_handler};

/// Run a EinsteinDB server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub fn run_edb(config: EINSTEINDBConfig) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);

    // Print version information.
    edb::log_edb_info();

    // Print resource quota.
    SysQuota::new().log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::new().cpu_cores_quota());

    // Do some prepare works before spacelike.
    pre_spacelike();

    let _m = Monitor::default();

    macro_rules! run_impl {
        ($ER: ty) => {{
            let mut edb = EinsteinDBServer::<$ER>::init(config);
            edb.check_conflict_addr();
            edb.init_fs();
            edb.init_yatp();
            edb.init_encryption();
            let engines = edb.init_raw_engines();
            edb.init_engines(engines);
            let gc_worker = edb.init_gc_worker();
            let server_config = edb.init_servers(&gc_worker);
            edb.register_services();
            edb.init_metrics_flusher();
            edb.run_server(server_config);
            edb.run_status_server();

            signal_handler::wait_for_signal(Some(edb.engines.take().unwrap().engines));
            edb.stop();
        }};
    }

    if !config.violetabft_engine.enable {
        run_impl!(LmdbEngine)
    } else {
        run_impl!(VioletaBftLogEngine)
    }
}

const RESERVED_OPEN_FDS: u64 = 1000;

/// A complete EinsteinDB server.
struct EinsteinDBServer<ER: VioletaBftEngine> {
    config: EINSTEINDBConfig,
    causet_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    fidel_client: Arc<RpcClient>,
    router: VioletaBftRouter<LmdbEngine, ER>,
    system: Option<VioletaBftBatchSystem<LmdbEngine, ER>>,
    resolver: resolve::FidelStoreAddrResolver,
    state: Arc<Mutex<GlobalReplicationState>>,
    store_path: PathBuf,
    encryption_key_manager: Option<Arc<DataKeyManager>>,
    engines: Option<EinsteinDBEngines<ER>>,
    servers: Option<Servers<ER>>,
    brane_info_accessor: BraneInfoAccessor,
    interlock_host: Option<InterlockHost<LmdbEngine>>,
    to_stop: Vec<Box<dyn Stop>>,
    lock_files: Vec<File>,
    interlocking_directorate: ConcurrencyManager,
}

struct EinsteinDBEngines<ER: VioletaBftEngine> {
    engines: Engines<LmdbEngine, ER>,
    store_meta: Arc<Mutex<StoreMeta>>,
    engine: VioletaBftKv<ServerVioletaBftStoreRouter<LmdbEngine, ER>>,
}

struct Servers<ER: VioletaBftEngine> {
    lock_mgr: LockManager,
    server: Server<VioletaBftRouter<LmdbEngine, ER>, resolve::FidelStoreAddrResolver>,
    node: Node<RpcClient, ER>,
    importer: Arc<SSTImporter>,
    causet_context_interlock_semaphore: violetabftstore::interlock::::worker::Interlock_Semaphore<causet_context::Task>,
}

impl<ER: VioletaBftEngine> EinsteinDBServer<ER> {
    fn init(mut config: EINSTEINDBConfig) -> EinsteinDBServer<ER> {
        // It is okay use fidel config and security config before `init_config`,
        // because these configs must be provided by command line, and only
        // used during spacelikeup process.
        let security_mgr = Arc::new(
            SecurityManager::new(&config.security)
                .unwrap_or_else(|e| fatal!("failed to create security manager: {}", e)),
        );
        let fidel_client = Self::connect_to_fidel_cluster(&mut config, Arc::clone(&security_mgr));

        // Initialize and check config
        let causet_controller = Self::init_config(config);
        let config = causet_controller.get_current();

        let store_path = Path::new(&config.causet_storage.data_dir).to_owned();

        // Initialize violetabftstore channels.
        let (router, system) = fsm::create_violetabft_batch_system(&config.violetabft_store);

        let (resolve_worker, resolver, state) =
            resolve::new_resolver(Arc::clone(&fidel_client), router.clone())
                .unwrap_or_else(|e| fatal!("failed to spacelike address resolver: {}", e));

        let mut interlock_host = Some(InterlockHost::new(router.clone()));
        match config.interlock.consistency_check_method {
            ConsistencyCheckMethod::Mvcc => {
                // TODO: use tail_pointer consistency checker.
                interlock_host
                    .as_mut()
                    .unwrap()
                    .registry
                    .register_consistency_check_semaphore(
                        100,
                        BoxConsistencyCheckSemaphore::new(RawConsistencyCheckSemaphore::default()),
                    );
            }
            ConsistencyCheckMethod::Raw => {
                interlock_host
                    .as_mut()
                    .unwrap()
                    .registry
                    .register_consistency_check_semaphore(
                        100,
                        BoxConsistencyCheckSemaphore::new(RawConsistencyCheckSemaphore::default()),
                    );
            }
        }
        let brane_info_accessor = BraneInfoAccessor::new(interlock_host.as_mut().unwrap());
        brane_info_accessor.spacelike();

        // Initialize concurrency manager
        let latest_ts = block_on(fidel_client.get_tso()).expect("failed to get timestamp from FIDel");
        let interlocking_directorate = ConcurrencyManager::new(latest_ts.into());

        EinsteinDBServer {
            config,
            causet_controller: Some(causet_controller),
            security_mgr,
            fidel_client,
            router,
            system: Some(system),
            resolver,
            state,
            store_path,
            encryption_key_manager: None,
            engines: None,
            servers: None,
            brane_info_accessor,
            interlock_host,
            to_stop: vec![Box::new(resolve_worker)],
            lock_files: vec![],
            interlocking_directorate,
        }
    }

    /// Initialize and check the config
    ///
    /// Warnings are logged and fatal errors exist.
    ///
    /// #  Fatal errors
    ///
    /// - If `dynamic config` feature is enabled and failed to register config to FIDel
    /// - If some critical configs (like data dir) are differrent from last run
    /// - If the config can't pass `validate()`
    /// - If the max open file descriptor limit is not high enough to support
    ///   the main database and the violetabft database.
    fn init_config(mut config: EINSTEINDBConfig) -> ConfigController {
        ensure_dir_exist(&config.causet_storage.data_dir).unwrap();
        ensure_dir_exist(&config.violetabft_store.violetabftdb_path).unwrap();

        validate_and_persist_config(&mut config, true);
        check_system_config(&config);

        violetabftstore::interlock::::set_panic_hook(false, &config.causet_storage.data_dir);

        info!(
            "using config";
            "config" => serde_json::to_string(&config).unwrap(),
        );
        if config.panic_when_unexpected_key_or_data {
            info!("panic-when-unexpected-key-or-data is on");
            violetabftstore::interlock::::set_panic_when_unexpected_key_or_data(true);
        }

        config.write_into_metrics();

        ConfigController::new(config)
    }

    fn connect_to_fidel_cluster(
        config: &mut EINSTEINDBConfig,
        security_mgr: Arc<SecurityManager>,
    ) -> Arc<RpcClient> {
        let fidel_client = Arc::new(
            RpcClient::new(&config.fidel, security_mgr)
                .unwrap_or_else(|e| fatal!("failed to create rpc client: {}", e)),
        );

        let cluster_id = fidel_client
            .get_cluster_id()
            .unwrap_or_else(|e| fatal!("failed to get cluster id: {}", e));
        if cluster_id == DEFAULT_CLUSTER_ID {
            fatal!("cluster id can't be {}", DEFAULT_CLUSTER_ID);
        }
        config.server.cluster_id = cluster_id;
        info!(
            "connect to FIDel cluster";
            "cluster_id" => cluster_id
        );

        fidel_client
    }

    fn check_conflict_addr(&mut self) {
        let cur_addr: SocketAddr = self
            .config
            .server
            .addr
            .parse()
            .expect("failed to parse into a socket address");
        let cur_ip = cur_addr.ip();
        let cur_port = cur_addr.port();
        let lock_dir = get_dagger_dir();

        let search_base = env::temp_dir().join(&lock_dir);
        std::fs::create_dir_all(&search_base)
            .unwrap_or_else(|_| panic!("create {} failed", search_base.display()));

        for result in fs::read_dir(&search_base).unwrap() {
            if let Ok(entry) = result {
                if !entry.file_type().unwrap().is_file() {
                    continue;
                }
                let file_path = entry.path();
                let file_name = file_path.file_name().unwrap().to_str().unwrap();
                if let Ok(addr) = file_name.replace('_', ":").parse::<SocketAddr>() {
                    let ip = addr.ip();
                    let port = addr.port();
                    if cur_port == port
                        && (cur_ip == ip || cur_ip.is_unspecified() || ip.is_unspecified())
                    {
                        let _ = try_lock_conflict_addr(file_path);
                    }
                }
            }
        }

        let cur_path = search_base.join(cur_addr.to_string().replace(':', "_"));
        let cur_file = try_lock_conflict_addr(cur_path);
        self.lock_files.push(cur_file);
    }

    fn init_fs(&mut self) {
        let lock_path = self.store_path.join(Path::new("LOCK"));

        let f = File::create(lock_path.as_path())
            .unwrap_or_else(|e| fatal!("failed to create dagger at {}: {}", lock_path.display(), e));
        if f.try_lock_exclusive().is_err() {
            fatal!(
                "dagger {} failed, maybe another instance is using this directory.",
                self.store_path.display()
            );
        }
        self.lock_files.push(f);

        if violetabftstore::interlock::::panic_mark_file_exists(&self.config.causet_storage.data_dir) {
            fatal!(
                "panic_mark_file {} exists, there must be something wrong with the db.",
                violetabftstore::interlock::::panic_mark_file_path(&self.config.causet_storage.data_dir).display()
            );
        }

        // We truncate a big file to make sure that both violetabftdb and kvdb of EinsteinDB have enough space
        // to compaction when EinsteinDB recover. This file is created in data_dir rather than db_path,
        // because we must not increase store size of db_path.
        violetabftstore::interlock::::reserve_space_for_recover(
            &self.config.causet_storage.data_dir,
            self.config.causet_storage.reserve_space.0,
        )
        .unwrap();
    }

    fn init_yatp(&self) {
        yatp::metrics::set_namespace(Some("edb"));
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL0_CHANCE.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL_ELAPSED.clone())).unwrap();
    }

    fn init_encryption(&mut self) {
        self.encryption_key_manager = DataKeyManager::from_config(
            &self.config.security.encryption,
            &self.config.causet_storage.data_dir,
        )
        .unwrap()
        .map(|key_manager| Arc::new(key_manager));
    }

    fn create_violetabftstore_compaction_listener(&self) -> engine_lmdb::CompactionListener {
        fn size_change_filter(info: &engine_lmdb::LmdbCompactionJobInfo) -> bool {
            // When calculating brane size, we only consider write and default
            // PrimaryCauset families.
            let causet = info.causet_name();
            if causet != Causet_WRITE && causet != Causet_DEFAULT {
                return false;
            }
            // Compactions in level 0 and level 1 are very frequently.
            if info.output_level() < 2 {
                return false;
            }

            true
        }

        let ch = Mutex::new(self.router.clone());
        let compacted_handler =
            Box::new(move |compacted_event: engine_lmdb::LmdbCompactedEvent| {
                let ch = ch.dagger().unwrap();
                let event = StoreMsg::CompactedEvent(compacted_event);
                if let Err(e) = ch.lightlike_control(event) {
                    error!(?e; "lightlike compaction finished event to violetabftstore failed");
                }
            });
        engine_lmdb::CompactionListener::new(compacted_handler, Some(size_change_filter))
    }

    fn init_engines(&mut self, engines: Engines<LmdbEngine, ER>) {
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_VOTES_CAP)));
        let engine = VioletaBftKv::new(
            ServerVioletaBftStoreRouter::new(
                self.router.clone(),
                LocalReader::new(engines.kv.clone(), store_meta.clone(), self.router.clone()),
            ),
            engines.kv.clone(),
        );

        let causet_controller = self.causet_controller.as_mut().unwrap();
        causet_controller.register(
            edb::config::Module::causet_storage,
            Box::new(StorageConfigManger::new(
                engines.kv.clone(),
                self.config.causet_storage.block_cache.shared,
            )),
        );

        self.engines = Some(EinsteinDBEngines {
            engines,
            store_meta,
            engine,
        });
    }

    fn init_gc_worker(
        &mut self,
    ) -> GcWorker<VioletaBftKv<ServerVioletaBftStoreRouter<LmdbEngine, ER>>, VioletaBftRouter<LmdbEngine, ER>> {
        let engines = self.engines.as_ref().unwrap();
        let mut gc_worker = GcWorker::new(
            engines.engine.clone(),
            self.router.clone(),
            self.config.gc.clone(),
            self.fidel_client.cluster_version(),
        );
        gc_worker
            .spacelike()
            .unwrap_or_else(|e| fatal!("failed to spacelike gc worker: {}", e));
        gc_worker
            .spacelike_observe_lock_apply(self.interlock_host.as_mut().unwrap())
            .unwrap_or_else(|e| fatal!("gc worker failed to observe dagger apply: {}", e));

        gc_worker
    }

    fn init_servers(
        &mut self,
        gc_worker: &GcWorker<
            VioletaBftKv<ServerVioletaBftStoreRouter<LmdbEngine, ER>>,
            VioletaBftRouter<LmdbEngine, ER>,
        >,
    ) -> Arc<ServerConfig> {
        let causet_controller = self.causet_controller.as_mut().unwrap();
        causet_controller.register(
            edb::config::Module::Gc,
            Box::new(gc_worker.get_config_manager()),
        );

        // Create causet_context.
        let mut causet_context_worker = Box::new(violetabftstore::interlock::::worker::Worker::new("causet_context"));
        let causet_context_interlock_semaphore = causet_context_worker.interlock_semaphore();
        let txn_extra_interlock_semaphore = causet_context::causet_contextTxnExtraInterlock_Semaphore::new(causet_context_interlock_semaphore.clone());

        self.engines
            .as_mut()
            .unwrap()
            .engine
            .set_txn_extra_interlock_semaphore(Arc::new(txn_extra_interlock_semaphore));

        // Create InterlockHost.
        let mut interlock_host = self.interlock_host.take().unwrap();

        let lock_mgr = LockManager::new();
        causet_controller.register(
            edb::config::Module::PessimisticTxn,
            Box::new(lock_mgr.config_manager()),
        );
        lock_mgr.register_detector_role_change_semaphore(&mut interlock_host);

        let engines = self.engines.as_ref().unwrap();

        let fidel_worker = FutureWorker::new("fidel-worker");
        let fidel_lightlikeer = fidel_worker.interlock_semaphore();

        let unified_read_pool = if self.config.readpool.is_unified_pool_enabled() {
            Some(build_yatp_read_pool(
                &self.config.readpool.unified,
                fidel_lightlikeer.clone(),
                engines.engine.clone(),
            ))
        } else {
            None
        };

        // The `DebugService` and `DiagnosticsService` will share the same thread pool
        let debug_thread_pool = Arc::new(
            Builder::new()
                .threaded_interlock_semaphore()
                .thread_name(thd_name!("debugger"))
                .core_threads(1)
                .on_thread_spacelike(|| edb_alloc::add_thread_memory_accessor())
                .on_thread_stop(|| edb_alloc::remove_thread_memory_accessor())
                .build()
                .unwrap(),
        );

        let causet_storage_read_pool_handle = if self.config.readpool.causet_storage.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let causet_storage_read_pools = ReadPool::from(causet_storage::build_read_pool(
                &self.config.readpool.causet_storage,
                fidel_lightlikeer.clone(),
                engines.engine.clone(),
            ));
            causet_storage_read_pools.handle()
        };

        let causet_storage = create_violetabft_causet_storage(
            engines.engine.clone(),
            &self.config.causet_storage,
            causet_storage_read_pool_handle,
            lock_mgr.clone(),
            self.interlocking_directorate.clone(),
            self.config.pessimistic_txn.pipelined,
        )
        .unwrap_or_else(|e| fatal!("failed to create violetabft causet_storage: {}", e));

        // Create snapshot manager, server.
        let snap_path = self
            .store_path
            .join(Path::new("snap"))
            .to_str()
            .unwrap()
            .to_owned();

        let bps = i64::try_from(self.config.server.snap_max_write_bytes_per_sec.0)
            .unwrap_or_else(|_| fatal!("snap_max_write_bytes_per_sec > i64::max_value"));

        let snap_mgr = SnapManagerBuilder::default()
            .max_write_bytes_per_sec(bps)
            .max_total_size(self.config.server.snap_max_total_size.0)
            .encryption_key_manager(self.encryption_key_manager.clone())
            .build(snap_path);

        // Create interlock lightlikepoint.
        let cop_read_pool_handle = if self.config.readpool.interlock.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let cop_read_pools = ReadPool::from(interlock::readpool_impl::build_read_pool(
                &self.config.readpool.interlock,
                fidel_lightlikeer,
                engines.engine.clone(),
            ));
            cop_read_pools.handle()
        };

        // Register causet_context
        let causet_context_ob = causet_context::causet_contextSemaphore::new(causet_context_interlock_semaphore.clone());
        causet_context_ob.register_to(&mut interlock_host);

        let server_config = Arc::new(self.config.server.clone());

        // Create server
        let server = Server::new(
            &server_config,
            &self.security_mgr,
            causet_storage,
            interlock::node::new(
                &server_config,
                cop_read_pool_handle,
                self.interlocking_directorate.clone(),
            ),
            self.router.clone(),
            self.resolver.clone(),
            snap_mgr.clone(),
            gc_worker.clone(),
            unified_read_pool,
            debug_thread_pool,
        )
        .unwrap_or_else(|e| fatal!("failed to create server: {}", e));

        let import_path = self.store_path.join("import");
        let importer =
            Arc::new(SSTImporter::new(import_path, self.encryption_key_manager.clone()).unwrap());

        let mut split_check_worker = Worker::new("split-check");
        let split_check_runner = SplitCheckRunner::new(
            engines.engines.kv.clone(),
            self.router.clone(),
            interlock_host.clone(),
            self.config.interlock.clone(),
        );
        split_check_worker.spacelike(split_check_runner).unwrap();
        causet_controller.register(
            edb::config::Module::Interlock,
            Box::new(SplitCheckConfigManager(split_check_worker.interlock_semaphore())),
        );

        self.config
            .violetabft_store
            .validate()
            .unwrap_or_else(|e| fatal!("failed to validate violetabftstore config {}", e));
        let violetabft_store = Arc::new(VersionTrack::new(self.config.violetabft_store.clone()));
        causet_controller.register(
            edb::config::Module::VioletaBftstore,
            Box::new(VioletaBftstoreConfigManager(violetabft_store.clone())),
        );

        let split_config_manager =
            SplitConfigManager(Arc::new(VersionTrack::new(self.config.split.clone())));
        causet_controller.register(
            edb::config::Module::Split,
            Box::new(split_config_manager.clone()),
        );

        let auto_split_controller = AutoSplitController::new(split_config_manager);

        let mut node = Node::new(
            self.system.take().unwrap(),
            &server_config,
            violetabft_store,
            self.fidel_client.clone(),
            self.state.clone(),
        );

        node.spacelike(
            engines.engines.clone(),
            server.transport(),
            snap_mgr,
            fidel_worker,
            engines.store_meta.clone(),
            interlock_host,
            importer.clone(),
            split_check_worker,
            auto_split_controller,
            self.interlocking_directorate.clone(),
        )
        .unwrap_or_else(|e| fatal!("failed to spacelike node: {}", e));

        initial_metric(&self.config.metric, Some(node.id()));

        // Start auto gc
        let auto_gc_config = AutoGcConfig::new(
            self.fidel_client.clone(),
            self.brane_info_accessor.clone(),
            node.id(),
        );
        if let Err(e) = gc_worker.spacelike_auto_gc(auto_gc_config) {
            fatal!("failed to spacelike auto_gc on causet_storage, error: {}", e);
        }

        // Start causet_context.
        let causet_context_lightlikepoint = causet_context::node::new(
            &self.config.causet_context,
            self.fidel_client.clone(),
            causet_context_worker.interlock_semaphore(),
            self.router.clone(),
            causet_context_ob,
            engines.store_meta.clone(),
            self.interlocking_directorate.clone(),
        );
        let causet_context_timer = causet_context_lightlikepoint.new_timer();
        causet_context_worker
            .spacelike_with_timer(causet_context_lightlikepoint, causet_context_timer)
            .unwrap_or_else(|e| fatal!("failed to spacelike causet_context: {}", e));
        self.to_stop.push(causet_context_worker);

        self.servers = Some(Servers {
            lock_mgr,
            server,
            node,
            importer,
            causet_context_interlock_semaphore,
        });

        server_config
    }

    fn register_services(&mut self) {
        let servers = self.servers.as_mut().unwrap();
        let engines = self.engines.as_ref().unwrap();

        // Import SST service.
        let import_service = ImportSSTService::new(
            self.config.import.clone(),
            self.router.clone(),
            engines.engines.kv.clone(),
            servers.importer.clone(),
            self.security_mgr.clone(),
        );
        if servers
            .server
            .register_service(create_import_sst(import_service))
            .is_some()
        {
            fatal!("failed to register import service");
        }

        // Debug service.
        let debug_service = DebugService::new(
            engines.engines.clone(),
            servers.server.get_debug_thread_pool().clone(),
            self.router.clone(),
            self.causet_controller.as_ref().unwrap().clone(),
            self.security_mgr.clone(),
        );
        if servers
            .server
            .register_service(create_debug(debug_service))
            .is_some()
        {
            fatal!("failed to register debug service");
        }

        // Create Diagnostics service
        let diag_service = DiagnosticsService::new(
            servers.server.get_debug_thread_pool().clone(),
            self.config.log_file.clone(),
            self.config.slow_log_file.clone(),
            self.security_mgr.clone(),
        );
        if servers
            .server
            .register_service(create_diagnostics(diag_service))
            .is_some()
        {
            fatal!("failed to register diagnostics service");
        }

        // Dagger manager.
        if servers
            .server
            .register_service(create_deadlock(
                servers.lock_mgr.deadlock_service(self.security_mgr.clone()),
            ))
            .is_some()
        {
            fatal!("failed to register deadlock service");
        }

        servers
            .lock_mgr
            .spacelike(
                servers.node.id(),
                self.fidel_client.clone(),
                self.resolver.clone(),
                self.security_mgr.clone(),
                &self.config.pessimistic_txn,
            )
            .unwrap_or_else(|e| fatal!("failed to spacelike dagger manager: {}", e));

        // Backup service.
        let mut backup_worker = Box::new(violetabftstore::interlock::::worker::Worker::new("backup-lightlikepoint"));
        let backup_interlock_semaphore = backup_worker.interlock_semaphore();
        let backup_service = backup::Service::new(backup_interlock_semaphore, self.security_mgr.clone());
        if servers
            .server
            .register_service(create_backup(backup_service))
            .is_some()
        {
            fatal!("failed to register backup service");
        }

        let backup_lightlikepoint = backup::node::new(
            servers.node.id(),
            engines.engine.clone(),
            self.brane_info_accessor.clone(),
            engines.engines.kv.as_inner().clone(),
            self.config.backup.clone(),
            self.interlocking_directorate.clone(),
        );
        self.causet_controller.as_mut().unwrap().register(
            edb::config::Module::Backup,
            Box::new(backup_lightlikepoint.get_config_manager()),
        );
        let backup_timer = backup_lightlikepoint.new_timer();
        backup_worker
            .spacelike_with_timer(backup_lightlikepoint, backup_timer)
            .unwrap_or_else(|e| fatal!("failed to spacelike backup lightlikepoint: {}", e));

        let causet_context_service =
            causet_context::Service::new(servers.causet_context_interlock_semaphore.clone(), self.security_mgr.clone());
        if servers
            .server
            .register_service(create_change_data(causet_context_service))
            .is_some()
        {
            fatal!("failed to register causet_context service");
        }

        self.to_stop.push(backup_worker);
    }

    fn init_metrics_flusher(&mut self) {
        let mut metrics_flusher = Box::new(MetricsFlusher::new(
            self.engines.as_ref().unwrap().engines.clone(),
        ));

        // Start metrics flusher
        if let Err(e) = metrics_flusher.spacelike() {
            error!(%e; "failed to spacelike metrics flusher");
        }

        self.to_stop.push(metrics_flusher);
    }

    fn run_server(&mut self, server_config: Arc<ServerConfig>) {
        let server = self.servers.as_mut().unwrap();
        server
            .server
            .build_and_bind()
            .unwrap_or_else(|e| fatal!("failed to build server: {}", e));
        server
            .server
            .spacelike(server_config, self.security_mgr.clone())
            .unwrap_or_else(|e| fatal!("failed to spacelike server: {}", e));
    }

    fn run_status_server(&mut self) {
        // Create a status server.
        let status_enabled =
            self.config.metric.address.is_empty() && !self.config.server.status_addr.is_empty();
        if status_enabled {
            let mut status_server = match StatusServer::new(
                self.config.server.status_thread_pool_size,
                Some(self.fidel_client.clone()),
                self.causet_controller.take().unwrap(),
                Arc::new(self.config.security.clone()),
                self.router.clone(),
            ) {
                Ok(status_server) => Box::new(status_server),
                Err(e) => {
                    error!(%e; "failed to spacelike runtime for status service");
                    return;
                }
            };
            // Start the status server.
            if let Err(e) = status_server.spacelike(
                self.config.server.status_addr.clone(),
                self.config.server.advertise_status_addr.clone(),
            ) {
                error!(%e; "failed to bind addr for status service");
            } else {
                self.to_stop.push(status_server);
            }
        }
    }

    fn stop(self) {
        let mut servers = self.servers.unwrap();
        servers
            .server
            .stop()
            .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));

        servers.node.stop();
        self.brane_info_accessor.stop();

        servers.lock_mgr.stop();

        self.to_stop.into_iter().for_each(|s| s.stop());
    }
}

impl EinsteinDBServer<LmdbEngine> {
    fn init_raw_engines(&mut self) -> Engines<LmdbEngine, LmdbEngine> {
        let env = get_env(self.encryption_key_manager.clone(), None /*base_env*/).unwrap();
        let block_cache = self.config.causet_storage.block_cache.build_shared_cache();

        // Create violetabft engine.
        let violetabft_db_path = Path::new(&self.config.violetabft_store.violetabftdb_path);
        let config_violetabftdb = &self.config.violetabftdb;
        let mut violetabft_db_opts = config_violetabftdb.build_opt();
        violetabft_db_opts.set_env(env.clone());
        let violetabft_db_causet_opts = config_violetabftdb.build_causet_opts(&block_cache);
        let violetabft_engine = engine_lmdb::raw_util::new_engine_opt(
            violetabft_db_path.to_str().unwrap(),
            violetabft_db_opts,
            violetabft_db_causet_opts,
        )
        .unwrap_or_else(|s| fatal!("failed to create violetabft engine: {}", s));

        // Create kv engine.
        let mut kv_db_opts = self.config.lmdb.build_opt();
        kv_db_opts.set_env(env);
        kv_db_opts.add_event_listener(self.create_violetabftstore_compaction_listener());
        let kv_causets_opts = self.config.lmdb.build_causet_opts(&block_cache);
        let db_path = self
            .store_path
            .join(Path::new(causet_storage::config::DEFAULT_LMDB_SUB_DIR));
        let kv_engine = engine_lmdb::raw_util::new_engine_opt(
            db_path.to_str().unwrap(),
            kv_db_opts,
            kv_causets_opts,
        )
        .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

        let mut kv_engine = LmdbEngine::from_db(Arc::new(kv_engine));
        let mut violetabft_engine = LmdbEngine::from_db(Arc::new(violetabft_engine));
        let shared_block_cache = block_cache.is_some();
        kv_engine.set_shared_block_cache(shared_block_cache);
        violetabft_engine.set_shared_block_cache(shared_block_cache);
        let engines = Engines::new(kv_engine, violetabft_engine);

        let causet_controller = self.causet_controller.as_mut().unwrap();
        causet_controller.register(
            edb::config::Module::Lmdbdb,
            Box::new(DBConfigManger::new(
                engines.kv.clone(),
                DBType::Kv,
                self.config.causet_storage.block_cache.shared,
            )),
        );
        causet_controller.register(
            edb::config::Module::VioletaBftdb,
            Box::new(DBConfigManger::new(
                engines.violetabft.clone(),
                DBType::VioletaBft,
                self.config.causet_storage.block_cache.shared,
            )),
        );

        engines
    }
}

impl EinsteinDBServer<VioletaBftLogEngine> {
    fn init_raw_engines(&mut self) -> Engines<LmdbEngine, VioletaBftLogEngine> {
        let env = get_env(self.encryption_key_manager.clone(), None /*base_env*/).unwrap();
        let block_cache = self.config.causet_storage.block_cache.build_shared_cache();

        // Create violetabft engine.
        let violetabft_config = self.config.violetabft_engine.config();
        let violetabft_engine = VioletaBftLogEngine::new(violetabft_config);

        // Create kv engine.
        let mut kv_db_opts = self.config.lmdb.build_opt();
        kv_db_opts.set_env(env);
        kv_db_opts.add_event_listener(self.create_violetabftstore_compaction_listener());
        let kv_causets_opts = self.config.lmdb.build_causet_opts(&block_cache);
        let db_path = self
            .store_path
            .join(Path::new(causet_storage::config::DEFAULT_LMDB_SUB_DIR));
        let kv_engine = engine_lmdb::raw_util::new_engine_opt(
            db_path.to_str().unwrap(),
            kv_db_opts,
            kv_causets_opts,
        )
        .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

        let mut kv_engine = LmdbEngine::from_db(Arc::new(kv_engine));
        let shared_block_cache = block_cache.is_some();
        kv_engine.set_shared_block_cache(shared_block_cache);
        let engines = Engines::new(kv_engine, violetabft_engine);

        let causet_controller = self.causet_controller.as_mut().unwrap();
        causet_controller.register(
            edb::config::Module::Lmdbdb,
            Box::new(DBConfigManger::new(
                engines.kv.clone(),
                DBType::Kv,
                self.config.causet_storage.block_cache.shared,
            )),
        );

        engines
    }
}

/// Various sanity-checks and logging before running a server.
///
/// Warnings are logged.
///
/// # Logs
///
/// The presence of these environment variables that affect the database
/// behavior is logged.
///
/// - `GRPC_POLL_STRATEGY`
/// - `http_proxy` and `https_proxy`
///
/// # Warnings
///
/// - if `net.core.somaxconn` < 32768
/// - if `net.ipv4.tcp_syncookies` is not 0
/// - if `vm.swappiness` is not 0
/// - if data directories are not on SSDs
/// - if the "TZ" environment variable is not set on unix
fn pre_spacelike() {
    check_environment_variables();
    for e in violetabftstore::interlock::::config::check_kernel() {
        warn!(
            "check: kernel";
            "err" => %e
        );
    }
}

fn check_system_config(config: &EINSTEINDBConfig) {
    info!("beginning system configuration check");
    let mut lmdb_max_open_files = config.lmdb.max_open_files;
    if config.lmdb.titan.enabled {
        // Noether engine maintains yet another pool of blob files and uses the same max
        // number of open files setup as lmdb does. So we double the max required
        // open files here
        lmdb_max_open_files *= 2;
    }
    if let Err(e) = violetabftstore::interlock::::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (lmdb_max_open_files + config.violetabftdb.max_open_files) as u64,
    ) {
        fatal!("{}", e);
    }

    // Check Lmdb data dir
    if let Err(e) = violetabftstore::interlock::::config::check_data_dir(&config.causet_storage.data_dir) {
        warn!(
            "check: lmdb-data-dir";
            "path" => &config.causet_storage.data_dir,
            "err" => %e
        );
    }
    // Check violetabft data dir
    if let Err(e) = violetabftstore::interlock::::config::check_data_dir(&config.violetabft_store.violetabftdb_path) {
        warn!(
            "check: violetabftdb-path";
            "path" => &config.violetabft_store.violetabftdb_path,
            "err" => %e
        );
    }
}

fn try_lock_conflict_addr<P: AsRef<Path>>(path: P) -> File {
    let f = File::create(path.as_ref()).unwrap_or_else(|e| {
        fatal!(
            "failed to create dagger at {}: {}",
            path.as_ref().display(),
            e
        )
    });

    if f.try_lock_exclusive().is_err() {
        fatal!(
            "{} already in use, maybe another instance is Constrained with this address.",
            path.as_ref().file_name().unwrap().to_str().unwrap()
        );
    }
    f
}

#[causet(unix)]
fn get_dagger_dir() -> String {
    format!("{}_EINSTEINDB_LOCK_FILES", unsafe { libc::getuid() })
}

#[causet(not(unix))]
fn get_dagger_dir() -> String {
    "EINSTEINDB_LOCK_FILES".to_owned()
}

/// A small trait for components which can be trivially stopped. Lets us keep
/// a list of these in `EinsteinDB`, rather than storing each component individually.
trait Stop {
    fn stop(self: Box<Self>);
}

impl<E, R> Stop for StatusServer<E, R>
where
    E: 'static,
    R: 'static + lightlike,
{
    fn stop(self: Box<Self>) {
        (*self).stop()
    }
}

impl<ER: VioletaBftEngine> Stop for MetricsFlusher<LmdbEngine, ER> {
    fn stop(mut self: Box<Self>) {
        (*self).stop()
    }
}

impl<T: fmt::Display + lightlike + 'static> Stop for Worker<T> {
    fn stop(mut self: Box<Self>) {
        if let Some(Err(e)) = Worker::stop(&mut *self).map(JoinHandle::join) {
            info!(
                "ignore failure when stopping worker";
                "err" => ?e
            );
        }
    }
}
