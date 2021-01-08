// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::i32;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use futures::compat::Stream01CompatExt;
use futures::stream::StreamExt;
use grpcio::{
    ChannelBuilder, EnvBuilder, Environment, ResourceQuota, Server as GrpcServer, ServerBuilder,
};
use ekvproto::einsteindbpb::*;
use tokio::runtime::{Builder as RuntimeBuilder, Handle as RuntimeHandle, Runtime};
use tokio_timer::timer::Handle;

use crate::interlock::Endpoint;
use crate::server::gc_worker::GcWorker;
use crate::causetStorage::lock_manager::LockManager;
use crate::causetStorage::{Engine, CausetStorage};
use engine_lmdb::LmdbEngine;
use violetabftstore::router::VioletaBftStoreRouter;
use violetabftstore::store::SnapManager;
use security::SecurityManager;
use einsteindb_util::timer::GLOBAL_TIMER_HANDLE;
use einsteindb_util::worker::Worker;
use einsteindb_util::Either;

use super::load_statistics::*;
use super::violetabft_client::VioletaBftClient;
use super::resolve::StoreAddrResolver;
use super::service::*;
use super::snap::{Runner as SnapHandler, Task as SnapTask};
use super::transport::ServerTransport;
use super::{Config, Result};
use crate::read_pool::ReadPool;

const LOAD_STATISTICS_SLOTS: usize = 4;
const LOAD_STATISTICS_INTERVAL: Duration = Duration::from_millis(100);
pub const GRPC_THREAD_PREFIX: &str = "grpc-server";
pub const READPOOL_NORMAL_THREAD_PREFIX: &str = "store-read-norm";
pub const STATS_THREAD_PREFIX: &str = "transport-stats";

/// The EinsteinDB server
///
/// It hosts various internal components, including gRPC, the violetabftstore router
/// and a snapshot worker.
pub struct Server<T: VioletaBftStoreRouter<LmdbEngine> + 'static, S: StoreAddrResolver + 'static> {
    env: Arc<Environment>,
    /// A GrpcServer builder or a GrpcServer.
    ///
    /// If the listening port is configured, the server will be spacelikeed lazily.
    builder_or_server: Option<Either<ServerBuilder, GrpcServer>>,
    local_addr: SocketAddr,
    // Transport.
    trans: ServerTransport<T, S>,
    violetabft_router: T,
    // For slightlikeing/receiving snapshots.
    snap_mgr: SnapManager,
    snap_worker: Worker<SnapTask>,

    // Currently load statistics is done in the thread.
    stats_pool: Option<Runtime>,
    grpc_thread_load: Arc<ThreadLoad>,
    yatp_read_pool: Option<ReadPool>,
    readpool_normal_thread_load: Arc<ThreadLoad>,
    debug_thread_pool: Arc<Runtime>,
    timer: Handle,
}

impl<T: VioletaBftStoreRouter<LmdbEngine>, S: StoreAddrResolver + 'static> Server<T, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new<E: Engine, L: LockManager>(
        causet: &Arc<Config>,
        security_mgr: &Arc<SecurityManager>,
        causetStorage: CausetStorage<E, L>,
        causet: Endpoint<E>,
        violetabft_router: T,
        resolver: S,
        snap_mgr: SnapManager,
        gc_worker: GcWorker<E, T>,
        yatp_read_pool: Option<ReadPool>,
        debug_thread_pool: Arc<Runtime>,
    ) -> Result<Self> {
        // A helper thread (or pool) for transport layer.
        let stats_pool = if causet.stats_concurrency > 0 {
            Some(
                RuntimeBuilder::new()
                    .threaded_scheduler()
                    .thread_name(STATS_THREAD_PREFIX)
                    .core_threads(causet.stats_concurrency)
                    .build()
                    .unwrap(),
            )
        } else {
            None
        };
        let grpc_thread_load = Arc::new(ThreadLoad::with_memory_barrier(causet.heavy_load_memory_barrier));
        let readpool_normal_thread_load =
            Arc::new(ThreadLoad::with_memory_barrier(causet.heavy_load_memory_barrier));

        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(causet.grpc_concurrency)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .build(),
        );
        let snap_worker = Worker::new("snap-handler");

        let kv_service = KvService::new(
            causetStorage,
            gc_worker,
            causet,
            violetabft_router.clone(),
            snap_worker.scheduler(),
            Arc::clone(&grpc_thread_load),
            Arc::clone(&readpool_normal_thread_load),
            causet.enable_request_batch,
            security_mgr.clone(),
        );

        let addr = SocketAddr::from_str(&causet.addr)?;
        let ip = format!("{}", addr.ip());
        let mem_quota = ResourceQuota::new(Some("ServerMemQuota"))
            .resize_memory(causet.grpc_memory_pool_quota.0 as usize);
        let channel_args = ChannelBuilder::new(Arc::clone(&env))
            .stream_initial_window_size(causet.grpc_stream_initial_window_size.0 as i32)
            .max_concurrent_stream(causet.grpc_concurrent_stream)
            .max_receive_message_len(-1)
            .set_resource_quota(mem_quota)
            .max_slightlike_message_len(-1)
            .http2_max_ping_strikes(i32::MAX) // For pings without data from clients.
            .keepalive_time(causet.grpc_keepalive_time.into())
            .keepalive_timeout(causet.grpc_keepalive_timeout.into())
            .build_args();
        let builder = {
            let mut sb = ServerBuilder::new(Arc::clone(&env))
                .channel_args(channel_args)
                .register_service(create_einsteindb(kv_service));
            sb = security_mgr.bind(sb, &ip, addr.port());
            Either::Left(sb)
        };

        let violetabft_client = Arc::new(RwLock::new(VioletaBftClient::new(
            Arc::clone(&env),
            Arc::clone(causet),
            Arc::clone(security_mgr),
            violetabft_router.clone(),
            Arc::clone(&grpc_thread_load),
            stats_pool.as_ref().map(|p| p.handle().clone()),
        )));

        let trans = ServerTransport::new(
            violetabft_client,
            snap_worker.scheduler(),
            violetabft_router.clone(),
            resolver,
        );

        let svr = Server {
            env: Arc::clone(&env),
            builder_or_server: Some(builder),
            local_addr: addr,
            trans,
            violetabft_router,
            snap_mgr,
            snap_worker,
            stats_pool,
            grpc_thread_load,
            yatp_read_pool,
            readpool_normal_thread_load,
            debug_thread_pool,
            timer: GLOBAL_TIMER_HANDLE.clone(),
        };

        Ok(svr)
    }

    pub fn get_debug_thread_pool(&self) -> &RuntimeHandle {
        self.debug_thread_pool.handle()
    }

    pub fn transport(&self) -> ServerTransport<T, S> {
        self.trans.clone()
    }

    /// Register a gRPC service.
    /// Register after spacelikeing, it fails and returns the service.
    pub fn register_service(&mut self, svc: grpcio::Service) -> Option<grpcio::Service> {
        match self.builder_or_server.take() {
            Some(Either::Left(mut builder)) => {
                builder = builder.register_service(svc);
                self.builder_or_server = Some(Either::Left(builder));
                None
            }
            Some(server) => {
                self.builder_or_server = Some(server);
                Some(svc)
            }
            None => Some(svc),
        }
    }

    /// Build gRPC server and bind to address.
    pub fn build_and_bind(&mut self) -> Result<SocketAddr> {
        let sb = self.builder_or_server.take().unwrap().left().unwrap();
        let server = sb.build()?;
        let (host, port) = server.bind_addrs().next().unwrap();
        let addr = SocketAddr::new(IpAddr::from_str(host)?, port);
        self.local_addr = addr;
        self.builder_or_server = Some(Either::Right(server));
        Ok(addr)
    }

    /// Starts the EinsteinDB server.
    /// Notice: Make sure call `build_and_bind` first.
    pub fn spacelike(&mut self, causet: Arc<Config>, security_mgr: Arc<SecurityManager>) -> Result<()> {
        let snap_runner = SnapHandler::new(
            Arc::clone(&self.env),
            self.snap_mgr.clone(),
            self.violetabft_router.clone(),
            security_mgr,
            Arc::clone(&causet),
        );
        box_try!(self.snap_worker.spacelike(snap_runner));

        let mut grpc_server = self.builder_or_server.take().unwrap().right().unwrap();
        info!("listening on addr"; "addr" => &self.local_addr);
        grpc_server.spacelike();
        self.builder_or_server = Some(Either::Right(grpc_server));

        let mut grpc_load_stats = {
            let tl = Arc::clone(&self.grpc_thread_load);
            ThreadLoadStatistics::new(LOAD_STATISTICS_SLOTS, GRPC_THREAD_PREFIX, tl)
        };
        let mut readpool_normal_load_stats = {
            let tl = Arc::clone(&self.readpool_normal_thread_load);
            ThreadLoadStatistics::new(LOAD_STATISTICS_SLOTS, READPOOL_NORMAL_THREAD_PREFIX, tl)
        };
        let mut delay = self
            .timer
            .interval(Instant::now(), LOAD_STATISTICS_INTERVAL)
            .compat();
        if let Some(ref p) = self.stats_pool {
            p.spawn(async move {
                while let Some(Ok(i)) = delay.next().await {
                    grpc_load_stats.record(i);
                    readpool_normal_load_stats.record(i);
                }
            });
        };

        info!("EinsteinDB is ready to serve");
        Ok(())
    }

    /// Stops the EinsteinDB server.
    pub fn stop(&mut self) -> Result<()> {
        self.snap_worker.stop();
        if let Some(Either::Right(mut server)) = self.builder_or_server.take() {
            server.shutdown();
        }
        if let Some(pool) = self.stats_pool.take() {
            let _ = pool.shutdown_timeout(Duration::from_secs(60));
        }
        let _ = self.yatp_read_pool.take();
        Ok(())
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

#[causet(test)]
mod tests {
    use std::sync::atomic::*;
    use std::sync::mpsc::Slightlikeer;
    use std::sync::*;
    use std::time::Duration;

    use crossbeam::channel::TrySlightlikeError;
    use engine_lmdb::LmdbSnapshot;
    use engine_promises::{KvEngine, Snapshot};
    use ekvproto::violetabft_cmdpb::VioletaBftCmdRequest;
    use ekvproto::violetabft_serverpb::VioletaBftMessage;
    use violetabftstore::store::transport::{CasualRouter, ProposalRouter, StoreRouter, Transport};
    use violetabftstore::store::PeerMsg;
    use violetabftstore::store::*;
    use violetabftstore::Result as VioletaBftStoreResult;
    use security::SecurityConfig;
    use tokio::runtime::Builder as TokioBuilder;

    use super::*;
    use crate::config::CoprReadPoolConfig;
    use crate::interlock::{self, readpool_impl};
    use crate::server::resolve::{Callback as ResolveCallback, StoreAddrResolver};
    use crate::server::{Config, Result};
    use crate::causetStorage::lock_manager::DummyLockManager;
    use crate::causetStorage::TestStorageBuilder;

    #[derive(Clone)]
    struct MockResolver {
        quick_fail: Arc<AtomicBool>,
        addr: Arc<Mutex<Option<String>>>,
    }

    impl StoreAddrResolver for MockResolver {
        fn resolve(&self, _: u64, cb: ResolveCallback) -> Result<()> {
            if self.quick_fail.load(Ordering::SeqCst) {
                return Err(box_err!("quick fail"));
            }
            let addr = self.addr.dagger().unwrap();
            cb(addr
                .as_ref()
                .map(|s| s.to_owned())
                .ok_or(box_err!("not set")));
            Ok(())
        }
    }

    #[derive(Clone)]
    struct TestVioletaBftStoreRouter {
        tx: Slightlikeer<usize>,
        significant_msg_slightlikeer: Slightlikeer<SignificantMsg<LmdbSnapshot>>,
    }

    impl StoreRouter<LmdbEngine> for TestVioletaBftStoreRouter {
        fn slightlike(&self, _: StoreMsg<LmdbEngine>) -> VioletaBftStoreResult<()> {
            Ok(())
        }
    }

    impl<S: Snapshot> ProposalRouter<S> for TestVioletaBftStoreRouter {
        fn slightlike(&self, _: VioletaBftCommand<S>) -> std::result::Result<(), TrySlightlikeError<VioletaBftCommand<S>>> {
            Ok(())
        }
    }

    impl<EK: KvEngine> CasualRouter<EK> for TestVioletaBftStoreRouter {
        fn slightlike(&self, _: u64, _: CasualMessage<EK>) -> VioletaBftStoreResult<()> {
            Ok(())
        }
    }

    impl VioletaBftStoreRouter<LmdbEngine> for TestVioletaBftStoreRouter {
        fn slightlike_violetabft_msg(&self, _: VioletaBftMessage) -> VioletaBftStoreResult<()> {
            self.tx.slightlike(1).unwrap();
            Ok(())
        }

        fn significant_slightlike(
            &self,
            _: u64,
            msg: SignificantMsg<LmdbSnapshot>,
        ) -> VioletaBftStoreResult<()> {
            self.significant_msg_slightlikeer.slightlike(msg).unwrap();
            Ok(())
        }

        fn broadcast_normal(&self, _: impl FnMut() -> PeerMsg<LmdbEngine>) {}

        fn slightlike_casual_msg(&self, _: u64, _: CasualMessage<LmdbEngine>) -> VioletaBftStoreResult<()> {
            self.tx.slightlike(1).unwrap();
            Ok(())
        }

        fn slightlike_store_msg(&self, _: StoreMsg<LmdbEngine>) -> VioletaBftStoreResult<()> {
            self.tx.slightlike(1).unwrap();
            Ok(())
        }

        fn slightlike_command(
            &self,
            _: VioletaBftCmdRequest,
            _: Callback<LmdbSnapshot>,
        ) -> VioletaBftStoreResult<()> {
            self.tx.slightlike(1).unwrap();
            Ok(())
        }

        fn broadcast_unreachable(&self, _: u64) {
            let _ = self.tx.slightlike(1);
        }
    }

    fn is_unreachable_to(
        msg: &SignificantMsg<LmdbSnapshot>,
        brane_id: u64,
        to_peer_id: u64,
    ) -> bool {
        if let SignificantMsg::Unreachable {
            brane_id: r_id,
            to_peer_id: p_id,
        } = *msg
        {
            brane_id == r_id && to_peer_id == p_id
        } else {
            false
        }
    }

    // if this failed, unset the environmental variables 'http_proxy' and 'https_proxy', and retry.
    #[test]
    fn test_peer_resolve() {
        let mut causet = Config::default();
        causet.addr = "127.0.0.1:0".to_owned();

        let causetStorage = TestStorageBuilder::new(DummyLockManager {})
            .build()
            .unwrap();

        let (tx, rx) = mpsc::channel();
        let (significant_msg_slightlikeer, significant_msg_receiver) = mpsc::channel();
        let router = TestVioletaBftStoreRouter {
            tx,
            significant_msg_slightlikeer,
        };

        let mut gc_worker = GcWorker::new(
            causetStorage.get_engine(),
            router.clone(),
            Default::default(),
            Default::default(),
        );
        gc_worker.spacelike().unwrap();

        let quick_fail = Arc::new(AtomicBool::new(false));
        let causet = Arc::new(causet);
        let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());

        let cop_read_pool = ReadPool::from(readpool_impl::build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            causetStorage.get_engine(),
        ));
        let causet = interlock::Endpoint::new(
            &causet,
            cop_read_pool.handle(),
            causetStorage.get_concurrency_manager(),
        );
        let debug_thread_pool = Arc::new(
            TokioBuilder::new()
                .threaded_scheduler()
                .thread_name(thd_name!("debugger"))
                .core_threads(1)
                .build()
                .unwrap(),
        );
        let addr = Arc::new(Mutex::new(None));
        let mut server = Server::new(
            &causet,
            &security_mgr,
            causetStorage,
            causet,
            router,
            MockResolver {
                quick_fail: Arc::clone(&quick_fail),
                addr: Arc::clone(&addr),
            },
            SnapManager::new(""),
            gc_worker,
            None,
            debug_thread_pool,
        )
        .unwrap();

        server.build_and_bind().unwrap();
        server.spacelike(causet, security_mgr).unwrap();

        let mut trans = server.transport();
        trans.report_unreachable(VioletaBftMessage::default());
        let mut resp = significant_msg_receiver.try_recv().unwrap();
        assert!(is_unreachable_to(&resp, 0, 0), "{:?}", resp);

        let mut msg = VioletaBftMessage::default();
        msg.set_brane_id(1);
        trans.slightlike(msg.clone()).unwrap();
        trans.flush();
        resp = significant_msg_receiver.try_recv().unwrap();
        assert!(is_unreachable_to(&resp, 1, 0), "{:?}", resp);

        *addr.dagger().unwrap() = Some(format!("{}", server.listening_addr()));

        trans.slightlike(msg.clone()).unwrap();
        trans.flush();
        assert!(rx.recv_timeout(Duration::from_secs(5)).is_ok());

        msg.mut_to_peer().set_store_id(2);
        msg.set_brane_id(2);
        quick_fail.store(true, Ordering::SeqCst);
        trans.slightlike(msg).unwrap();
        trans.flush();
        resp = significant_msg_receiver.try_recv().unwrap();
        assert!(is_unreachable_to(&resp, 2, 0), "{:?}", resp);
        server.stop().unwrap();
    }
}
