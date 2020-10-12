// Copyright 2017 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};
use einsteindb_util::time::{duration_to_sec, Instant};

use super::batch::ReqBatcher;
use crate::interlock::Endpoint;
use crate::server::gc_worker::GcWorker;
use crate::server::load_statistics::ThreadLoad;
use crate::server::metrics::*;
use crate::server::snap::Task as SnapTask;
use crate::server::Error;
use crate::server::Result as ServerResult;
use crate::causetStorage::{
    errors::{
        extract_committed, extract_key_error, extract_key_errors, extract_kv_pairs,
        extract_brane_error,
    },
    kv::Engine,
    lock_manager::LockManager,
    SecondaryLocksStatus, CausetStorage, TxnStatus,
};
use engine_lmdb::LmdbEngine;
use futures::compat::Future01CompatExt;
use futures::future::{self, Future, FutureExt, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{StreamExt, TryStreamExt};
use grpcio::{
    ClientStreamingSink, DuplexSink, Error as GrpcError, RequestStream, Result as GrpcResult,
    RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink, WriteFlags,
};
use ekvproto::interlock::*;
use ekvproto::kvrpcpb::*;
use ekvproto::violetabft_cmdpb::{CmdType, VioletaBftCmdRequest, VioletaBftRequestHeader, Request as VioletaBftRequest};
use ekvproto::violetabft_serverpb::*;
use ekvproto::einsteindbpb::*;
use violetabftstore::router::VioletaBftStoreRouter;
use violetabftstore::store::{Callback, CasualMessage};
use security::{check_common_name, SecurityManager};
use einsteindb_util::future::{paired_future_callback, poll_future_notify};
use einsteindb_util::mpsc::batch::{unbounded, BatchCollector, BatchReceiver, Slightlikeer};
use einsteindb_util::worker::Scheduler;
use tokio_threadpool::{Builder as ThreadPoolBuilder, ThreadPool};
use txn_types::{self, Key};

const GRPC_MSG_MAX_BATCH_SIZE: usize = 128;
const GRPC_MSG_NOTIFY_SIZE: usize = 8;

/// Service handles the RPC messages for the `EINSTEINDB` service.
pub struct Service<T: VioletaBftStoreRouter<LmdbEngine> + 'static, E: Engine, L: LockManager> {
    /// Used to handle requests related to GC.
    gc_worker: GcWorker<E, T>,
    // For handling KV requests.
    causetStorage: CausetStorage<E, L>,
    // For handling interlock requests.
    causet: Endpoint<E>,
    // For handling violetabft messages.
    ch: T,
    // For handling snapshot.
    snap_scheduler: Scheduler<SnapTask>,

    enable_req_batch: bool,

    timer_pool: Arc<Mutex<ThreadPool>>,

    grpc_thread_load: Arc<ThreadLoad>,

    readpool_normal_thread_load: Arc<ThreadLoad>,

    security_mgr: Arc<SecurityManager>,
}

impl<
        T: VioletaBftStoreRouter<LmdbEngine> + Clone + 'static,
        E: Engine + Clone,
        L: LockManager + Clone,
    > Clone for Service<T, E, L>
{
    fn clone(&self) -> Self {
        Service {
            gc_worker: self.gc_worker.clone(),
            causetStorage: self.causetStorage.clone(),
            causet: self.causet.clone(),
            ch: self.ch.clone(),
            snap_scheduler: self.snap_scheduler.clone(),
            enable_req_batch: self.enable_req_batch,
            timer_pool: self.timer_pool.clone(),
            grpc_thread_load: self.grpc_thread_load.clone(),
            readpool_normal_thread_load: self.readpool_normal_thread_load.clone(),
            security_mgr: self.security_mgr.clone(),
        }
    }
}

impl<T: VioletaBftStoreRouter<LmdbEngine> + 'static, E: Engine, L: LockManager> Service<T, E, L> {
    /// Constructs a new `Service` which provides the `EINSTEINDB` service.
    pub fn new(
        causetStorage: CausetStorage<E, L>,
        gc_worker: GcWorker<E, T>,
        causet: Endpoint<E>,
        ch: T,
        snap_scheduler: Scheduler<SnapTask>,
        grpc_thread_load: Arc<ThreadLoad>,
        readpool_normal_thread_load: Arc<ThreadLoad>,
        enable_req_batch: bool,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        let timer_pool = Arc::new(Mutex::new(
            ThreadPoolBuilder::new()
                .pool_size(1)
                .name_prefix("req_batch_timer_guard")
                .build(),
        ));
        Service {
            gc_worker,
            causetStorage,
            causet,
            ch,
            snap_scheduler,
            grpc_thread_load,
            readpool_normal_thread_load,
            timer_pool,
            enable_req_batch,
            security_mgr,
        }
    }

    fn slightlike_fail_status<M>(
        &self,
        ctx: RpcContext<'_>,
        sink: UnarySink<M>,
        err: Error,
        code: RpcStatusCode,
    ) {
        let status = RpcStatus::new(code, Some(format!("{}", err)));
        ctx.spawn(sink.fail(status).map(|_| ()));
    }
}

macro_rules! handle_request {
    ($fn_name: ident, $future_name: ident, $req_ty: ident, $resp_ty: ident) => {
        fn $fn_name(&mut self, ctx: RpcContext<'_>, req: $req_ty, sink: UnarySink<$resp_ty>) {
            if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
                return;
            }
            let begin_instant = Instant::now_coarse();

            let resp = $future_name(&self.causetStorage, req);
            let task = async move {
                let resp = resp.await?;
                sink.success(resp).await?;
                GRPC_MSG_HISTOGRAM_STATIC
                    .$fn_name
                    .observe(duration_to_sec(begin_instant.elapsed()));
                ServerResult::Ok(())
            }
            .map_err(|e| {
                debug!("kv rpc failed";
                    "request" => stringify!($fn_name),
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.$fn_name.inc();
            })
            .map(|_|());

            ctx.spawn(task);
        }
    }
}

impl<T: VioletaBftStoreRouter<LmdbEngine> + 'static, E: Engine, L: LockManager> EINSTEINDB
    for Service<T, E, L>
{
    handle_request!(kv_get, future_get, GetRequest, GetResponse);
    handle_request!(kv_scan, future_scan, ScanRequest, ScanResponse);
    handle_request!(
        kv_prewrite,
        future_prewrite,
        PrewriteRequest,
        PrewriteResponse
    );
    handle_request!(
        kv_pessimistic_lock,
        future_acquire_pessimistic_lock,
        PessimisticLockRequest,
        PessimisticLockResponse
    );
    handle_request!(
        kv_pessimistic_rollback,
        future_pessimistic_rollback,
        PessimisticRollbackRequest,
        PessimisticRollbackResponse
    );
    handle_request!(kv_commit, future_commit, CommitRequest, CommitResponse);
    handle_request!(kv_cleanup, future_cleanup, CleanupRequest, CleanupResponse);
    handle_request!(
        kv_batch_get,
        future_batch_get,
        BatchGetRequest,
        BatchGetResponse
    );
    handle_request!(
        kv_batch_rollback,
        future_batch_rollback,
        BatchRollbackRequest,
        BatchRollbackResponse
    );
    handle_request!(
        kv_txn_heart_beat,
        future_txn_heart_beat,
        TxnHeartBeatRequest,
        TxnHeartBeatResponse
    );
    handle_request!(
        kv_check_txn_status,
        future_check_txn_status,
        CheckTxnStatusRequest,
        CheckTxnStatusResponse
    );
    handle_request!(
        kv_check_secondary_locks,
        future_check_secondary_locks,
        CheckSecondaryLocksRequest,
        CheckSecondaryLocksResponse
    );
    handle_request!(
        kv_scan_lock,
        future_scan_lock,
        ScanLockRequest,
        ScanLockResponse
    );
    handle_request!(
        kv_resolve_lock,
        future_resolve_lock,
        ResolveLockRequest,
        ResolveLockResponse
    );
    handle_request!(
        kv_delete_cone,
        future_delete_cone,
        DeleteConeRequest,
        DeleteConeResponse
    );
    handle_request!(
        mvcc_get_by_key,
        future_mvcc_get_by_key,
        MvccGetByKeyRequest,
        MvccGetByKeyResponse
    );
    handle_request!(
        mvcc_get_by_spacelike_ts,
        future_mvcc_get_by_spacelike_ts,
        MvccGetByStartTsRequest,
        MvccGetByStartTsResponse
    );
    handle_request!(raw_get, future_raw_get, RawGetRequest, RawGetResponse);
    handle_request!(
        raw_batch_get,
        future_raw_batch_get,
        RawBatchGetRequest,
        RawBatchGetResponse
    );
    handle_request!(raw_scan, future_raw_scan, RawScanRequest, RawScanResponse);
    handle_request!(
        raw_batch_scan,
        future_raw_batch_scan,
        RawBatchScanRequest,
        RawBatchScanResponse
    );
    handle_request!(raw_put, future_raw_put, RawPutRequest, RawPutResponse);
    handle_request!(
        raw_batch_put,
        future_raw_batch_put,
        RawBatchPutRequest,
        RawBatchPutResponse
    );
    handle_request!(
        raw_delete,
        future_raw_delete,
        RawDeleteRequest,
        RawDeleteResponse
    );
    handle_request!(
        raw_batch_delete,
        future_raw_batch_delete,
        RawBatchDeleteRequest,
        RawBatchDeleteResponse
    );
    handle_request!(
        raw_delete_cone,
        future_raw_delete_cone,
        RawDeleteConeRequest,
        RawDeleteConeResponse
    );

    fn kv_import(&mut self, _: RpcContext<'_>, _: ImportRequest, _: UnarySink<ImportResponse>) {
        unimplemented!();
    }

    fn kv_gc(&mut self, ctx: RpcContext<'_>, _: GcRequest, sink: UnarySink<GcResponse>) {
        let e = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
        ctx.spawn(
            sink.fail(e)
                .unwrap_or_else(|e| error!("kv rpc failed"; "err" => ?e)),
        );
    }

    fn interlock(&mut self, ctx: RpcContext<'_>, req: Request, sink: UnarySink<Response>) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();
        let future = future_cop(&self.causet, Some(ctx.peer()), req);
        let task = async move {
            let resp = future.await?;
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .interlock
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "interlock",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.interlock.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn register_lock_observer(
        &mut self,
        ctx: RpcContext<'_>,
        req: RegisterLockObserverRequest,
        sink: UnarySink<RegisterLockObserverResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.spacelike_collecting(req.get_max_ts().into(), cb);

        let task = async move {
            // Here except for the receiving error of `futures::channel::oneshot`,
            // other errors will be returned as the successful response of rpc.
            let res = match res {
                Err(e) => Err(e),
                Ok(_) => f.await?,
            };
            let mut resp = RegisterLockObserverResponse::default();
            if let Err(e) = res {
                resp.set_error(format!("{}", e));
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .register_lock_observer
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "register_lock_observer",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.register_lock_observer.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn check_lock_observer(
        &mut self,
        ctx: RpcContext<'_>,
        req: CheckLockObserverRequest,
        sink: UnarySink<CheckLockObserverResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self
            .gc_worker
            .get_collected_locks(req.get_max_ts().into(), cb);

        let task = async move {
            let res = match res {
                Err(e) => Err(e),
                Ok(_) => f.await?,
            };
            let mut resp = CheckLockObserverResponse::default();
            match res {
                Ok((locks, is_clean)) => {
                    resp.set_is_clean(is_clean);
                    resp.set_locks(locks.into());
                }
                Err(e) => resp.set_error(format!("{}", e)),
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .check_lock_observer
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "check_lock_observer",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.check_lock_observer.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn remove_lock_observer(
        &mut self,
        ctx: RpcContext<'_>,
        req: RemoveLockObserverRequest,
        sink: UnarySink<RemoveLockObserverResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.stop_collecting(req.get_max_ts().into(), cb);

        let task = async move {
            let res = match res {
                Err(e) => Err(e),
                Ok(_) => f.await?,
            };
            let mut resp = RemoveLockObserverResponse::default();
            if let Err(e) = res {
                resp.set_error(format!("{}", e));
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .remove_lock_observer
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "remove_lock_observer",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.remove_lock_observer.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn physical_scan_lock(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: PhysicalScanLockRequest,
        sink: UnarySink<PhysicalScanLockResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.physical_scan_lock(
            req.take_context(),
            req.get_max_ts().into(),
            Key::from_raw(req.get_spacelike_key()),
            req.get_limit() as _,
            cb,
        );

        let task = async move {
            let res = match res {
                Err(e) => Err(e),
                Ok(_) => f.await?,
            };
            let mut resp = PhysicalScanLockResponse::default();
            match res {
                Ok(locks) => resp.set_locks(locks.into()),
                Err(e) => resp.set_error(format!("{}", e)),
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .physical_scan_lock
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "physical_scan_lock",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.physical_scan_lock.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn unsafe_destroy_cone(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: UnsafeDestroyConeRequest,
        sink: UnarySink<UnsafeDestroyConeResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        // DestroyCone is a very dangerous operation. We don't allow passing MIN_KEY as spacelike, or
        // MAX_KEY as lightlike here.
        assert!(!req.get_spacelike_key().is_empty());
        assert!(!req.get_lightlike_key().is_empty());

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.unsafe_destroy_cone(
            req.take_context(),
            Key::from_raw(&req.take_spacelike_key()),
            Key::from_raw(&req.take_lightlike_key()),
            cb,
        );

        let task = async move {
            let res = match res {
                Err(e) => Err(e),
                Ok(_) => f.await?,
            };
            let mut resp = UnsafeDestroyConeResponse::default();
            // Brane error is impossible here.
            if let Err(e) = res {
                resp.set_error(format!("{}", e));
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .unsafe_destroy_cone
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "unsafe_destroy_cone",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.unsafe_destroy_cone.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn interlock_stream(
        &mut self,
        ctx: RpcContext<'_>,
        req: Request,
        mut sink: ServerStreamingSink<Response>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let mut stream = self
            .causet
            .parse_and_handle_stream_request(req, Some(ctx.peer()))
            .map(|resp| {
                GrpcResult::<(Response, WriteFlags)>::Ok((
                    resp,
                    WriteFlags::default().buffer_hint(true),
                ))
            });
        let future = async move {
            match sink.slightlike_all(&mut stream).await.map_err(Error::from) {
                Ok(_) => {
                    GRPC_MSG_HISTOGRAM_STATIC
                        .interlock_stream
                        .observe(duration_to_sec(begin_instant.elapsed()));
                    let _ = sink.close().await;
                }
                Err(e) => {
                    debug!("kv rpc failed";
                        "request" => "interlock_stream",
                        "err" => ?e
                    );
                    GRPC_MSG_FAIL_COUNTER.interlock_stream.inc();
                }
            }
        };

        ctx.spawn(future);
    }

    fn violetabft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<VioletaBftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let ch = self.ch.clone();
        ctx.spawn(async move {
            let res = stream.map_err(Error::from).try_for_each(move |msg| {
                VIOLETABFT_MESSAGE_RECV_COUNTER.inc();
                let ret = ch.slightlike_violetabft_msg(msg).map_err(Error::from);
                future::ready(ret)
            });
            let status = match res.await {
                Err(e) => {
                    let msg = format!("{:?}", e);
                    error!("dispatch violetabft msg from gRPC to violetabftstore fail"; "err" => %msg);
                    RpcStatus::new(RpcStatusCode::UNKNOWN, Some(msg))
                }
                Ok(_) => RpcStatus::new(RpcStatusCode::UNKNOWN, None),
            };
            let _ = sink
                .fail(status)
                .map_err(|e| error!("KvService::violetabft slightlike response fail"; "err" => ?e))
                .await;
        });
    }

    fn batch_violetabft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BatchVioletaBftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        info!("batch_violetabft RPC is called, new gRPC stream established");
        let ch = self.ch.clone();
        ctx.spawn(async move {
            let res = stream.map_err(Error::from).try_for_each(move |mut msgs| {
                let len = msgs.get_msgs().len();
                VIOLETABFT_MESSAGE_RECV_COUNTER.inc_by(len as i64);
                VIOLETABFT_MESSAGE_BATCH_SIZE.observe(len as f64);
                for msg in msgs.take_msgs().into_iter() {
                    if let Err(e) = ch.slightlike_violetabft_msg(msg) {
                        return future::err(Error::from(e));
                    }
                }
                future::ok(())
            });
            let status = match res.await {
                Err(e) => {
                    let msg = format!("{:?}", e);
                    error!("dispatch violetabft msg from gRPC to violetabftstore fail"; "err" => %msg);
                    RpcStatus::new(RpcStatusCode::UNKNOWN, Some(msg))
                }
                Ok(_) => RpcStatus::new(RpcStatusCode::UNKNOWN, None),
            };
            let _ = sink
                .fail(status)
                .map_err(|e| error!("KvService::batch_violetabft slightlike response fail"; "err" => ?e))
                .await;
        });
    }

    fn snapshot(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<SnapshotSoliton>,
        sink: ClientStreamingSink<Done>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let task = SnapTask::Recv { stream, sink };
        if let Err(e) = self.snap_scheduler.schedule(task) {
            let err_msg = format!("{}", e);
            let sink = match e.into_inner() {
                SnapTask::Recv { sink, .. } => sink,
                _ => unreachable!(),
            };
            let status = RpcStatus::new(RpcStatusCode::RESOURCE_EXHAUSTED, Some(err_msg));
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    }

    fn split_brane(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: SplitBraneRequest,
        sink: UnarySink<SplitBraneResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let brane_id = req.get_context().get_brane_id();
        let (cb, f) = paired_future_callback();
        let mut split_tuplespaceInstanton = if !req.get_split_key().is_empty() {
            vec![Key::from_raw(req.get_split_key()).into_encoded()]
        } else {
            req.take_split_tuplespaceInstanton()
                .into_iter()
                .map(|x| Key::from_raw(&x).into_encoded())
                .collect()
        };
        split_tuplespaceInstanton.sort();
        let req = CasualMessage::SplitBrane {
            brane_epoch: req.take_context().take_brane_epoch(),
            split_tuplespaceInstanton,
            callback: Callback::Write(cb),
        };

        if let Err(e) = self.ch.slightlike_casual_msg(brane_id, req) {
            self.slightlike_fail_status(ctx, sink, Error::from(e), RpcStatusCode::RESOURCE_EXHAUSTED);
            return;
        }

        let task = async move {
            let mut res = f.await?;
            let mut resp = SplitBraneResponse::default();
            if res.response.get_header().has_error() {
                resp.set_brane_error(res.response.mut_header().take_error());
            } else {
                let admin_resp = res.response.mut_admin_response();
                let branes: Vec<_> = admin_resp.mut_splits().take_branes().into();
                if branes.len() < 2 {
                    error!(
                        "invalid split response";
                        "brane_id" => brane_id,
                        "resp" => ?admin_resp
                    );
                    resp.mut_brane_error().set_message(format!(
                        "Internal Error: invalid response: {:?}",
                        admin_resp
                    ));
                } else {
                    if branes.len() == 2 {
                        resp.set_left(branes[0].clone());
                        resp.set_right(branes[1].clone());
                    }
                    resp.set_branes(branes.into());
                }
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .split_brane
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "split_brane",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.split_brane.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn read_index(
        &mut self,
        ctx: RpcContext<'_>,
        req: ReadIndexRequest,
        sink: UnarySink<ReadIndexResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let brane_id = req.get_context().get_brane_id();
        let mut cmd = VioletaBftCmdRequest::default();
        let mut header = VioletaBftRequestHeader::default();
        let mut inner_req = VioletaBftRequest::default();
        inner_req.set_cmd_type(CmdType::ReadIndex);
        header.set_brane_id(req.get_context().get_brane_id());
        header.set_peer(req.get_context().get_peer().clone());
        header.set_brane_epoch(req.get_context().get_brane_epoch().clone());
        if req.get_context().get_term() != 0 {
            header.set_term(req.get_context().get_term());
        }
        header.set_sync_log(req.get_context().get_sync_log());
        header.set_read_quorum(true);
        cmd.set_header(header);
        cmd.set_requests(vec![inner_req].into());

        let (cb, f) = paired_future_callback();

        // We must deal with all requests which acquire read-quorum in violetabftstore-thread, so just slightlike it as an command.
        if let Err(e) = self.ch.slightlike_command(cmd, Callback::Read(cb)) {
            self.slightlike_fail_status(ctx, sink, Error::from(e), RpcStatusCode::RESOURCE_EXHAUSTED);
            return;
        }

        let task = async move {
            let mut res = f.await?;
            let mut resp = ReadIndexResponse::default();
            if res.response.get_header().has_error() {
                resp.set_brane_error(res.response.mut_header().take_error());
            } else {
                let violetabft_resps = res.response.get_responses();
                if violetabft_resps.len() != 1 {
                    error!(
                        "invalid read index response";
                        "brane_id" => brane_id,
                        "response" => ?violetabft_resps
                    );
                    resp.mut_brane_error().set_message(format!(
                        "Internal Error: invalid response: {:?}",
                        violetabft_resps
                    ));
                } else {
                    let read_index = violetabft_resps[0].get_read_index().get_read_index();
                    resp.set_read_index(read_index);
                }
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .read_index
                .observe(begin_instant.elapsed_secs());
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "read_index",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.read_index.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn batch_commands(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BatchCommandsRequest>,
        mut sink: DuplexSink<BatchCommandsResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let (tx, rx) = unbounded(GRPC_MSG_NOTIFY_SIZE);

        let ctx = Arc::new(ctx);
        let peer = ctx.peer();
        let causetStorage = self.causetStorage.clone();
        let causet = self.causet.clone();
        let enable_req_batch = self.enable_req_batch;
        let request_handler = stream.try_for_each(move |mut req| {
            let request_ids = req.take_request_ids();
            let requests: Vec<_> = req.take_requests().into();
            let mut batcher = if enable_req_batch && requests.len() > 2 {
                Some(ReqBatcher::new())
            } else {
                None
            };
            GRPC_REQ_BATCH_COMMANDS_SIZE.observe(requests.len() as f64);
            for (id, req) in request_ids.into_iter().zip(requests) {
                handle_batch_commands_request(&mut batcher, &causetStorage, &causet, &peer, id, req, &tx);
            }
            if let Some(mut batch) = batcher {
                batch.commit(&causetStorage, &tx);
                causetStorage.release_snapshot();
            }
            future::ok(())
        });
        ctx.spawn(request_handler.unwrap_or_else(|e| error!("batch_commands error"; "err" => %e)));

        let thread_load = Arc::clone(&self.grpc_thread_load);
        let response_retriever = BatchReceiver::new(
            rx,
            GRPC_MSG_MAX_BATCH_SIZE,
            BatchCommandsResponse::default,
            BatchRespCollector,
        );

        let mut response_retriever = response_retriever
            .inspect(|r| GRPC_RESP_BATCH_COMMANDS_SIZE.observe(r.request_ids.len() as f64))
            .map(move |mut r| {
                r.set_transport_layer_load(thread_load.load() as u64);
                GrpcResult::<(BatchCommandsResponse, WriteFlags)>::Ok((
                    r,
                    WriteFlags::default().buffer_hint(false),
                ))
            });

        let slightlike_task = async move {
            sink.slightlike_all(&mut response_retriever).await?;
            sink.close().await?;
            Ok(())
        }
        .map_err(|e: grpcio::Error| {
            debug!("kv rpc failed";
                "request" => "batch_commands",
                "err" => ?e
            );
        })
        .map(|_| ());

        ctx.spawn(slightlike_task);
    }

    fn ver_get(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerGetRequest,
        _sink: UnarySink<VerGetResponse>,
    ) {
        unimplemented!()
    }

    fn ver_batch_get(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerBatchGetRequest,
        _sink: UnarySink<VerBatchGetResponse>,
    ) {
        unimplemented!()
    }

    fn ver_mut(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerMutRequest,
        _sink: UnarySink<VerMutResponse>,
    ) {
        unimplemented!()
    }

    fn ver_batch_mut(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerBatchMutRequest,
        _sink: UnarySink<VerBatchMutResponse>,
    ) {
        unimplemented!()
    }

    fn ver_scan(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerScanRequest,
        _sink: UnarySink<VerScanResponse>,
    ) {
        unimplemented!()
    }

    fn ver_delete_cone(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerDeleteConeRequest,
        _sink: UnarySink<VerDeleteConeResponse>,
    ) {
        unimplemented!()
    }

    fn batch_interlock(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: BatchRequest,
        _sink: ServerStreamingSink<BatchResponse>,
    ) {
        unimplemented!()
    }
}

fn response_batch_commands_request<F>(
    id: u64,
    resp: F,
    tx: Slightlikeer<(u64, batch_commands_response::Response)>,
    begin_instant: Instant,
    label_enum: GrpcTypeKind,
) where
    F: Future<Output = Result<batch_commands_response::Response, ()>> + Slightlike + 'static,
{
    let task = async move {
        if let Ok(resp) = resp.await {
            if tx.slightlike_and_notify((id, resp)).is_err() {
                error!("KvService response batch commands fail");
            } else {
                GRPC_MSG_HISTOGRAM_STATIC
                    .get(label_enum)
                    .observe(begin_instant.elapsed_secs());
            }
        }
    };
    poll_future_notify(task);
}

fn handle_batch_commands_request<E: Engine, L: LockManager>(
    batcher: &mut Option<ReqBatcher>,
    causetStorage: &CausetStorage<E, L>,
    causet: &Endpoint<E>,
    peer: &str,
    id: u64,
    req: batch_commands_request::Request,
    tx: &Slightlikeer<(u64, batch_commands_response::Response)>,
) {
    // To simplify code and make the logic more clear.
    macro_rules! oneof {
        ($p:path) => {
            |resp| {
                let mut res = batch_commands_response::Response::default();
                res.cmd = Some($p(resp));
                res
            }
        };
    }

    macro_rules! handle_cmd {
        ($($cmd: ident, $future_fn: ident ( $($arg: expr),* ), $metric_name: ident;)*) => {
            match req.cmd {
                None => {
                    // For some invalid requests.
                    let begin_instant = Instant::now();
                    let resp = future::ok(batch_commands_response::Response::default());
                    response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::invalid);
                },
                Some(batch_commands_request::request::Cmd::Get(req)) => {
                    if batcher.as_mut().map_or(false, |req_batch| {
                        req_batch.maybe_commit(causetStorage, tx);
                        req_batch.can_batch_get(&req)
                    }) {
                        batcher.as_mut().unwrap().add_get_request(req, id);
                    } else {
                       let begin_instant = Instant::now();
                       let resp = future_get(causetStorage, req)
                            .map_ok(oneof!(batch_commands_response::response::Cmd::Get))
                            .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_get.inc());
                        response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::kv_get);
                    }
                },
                Some(batch_commands_request::request::Cmd::RawGet(req)) => {
                    if batcher.as_mut().map_or(false, |req_batch| {
                        req_batch.maybe_commit(causetStorage, tx);
                        req_batch.can_batch_raw_get(&req)
                    }) {
                        batcher.as_mut().unwrap().add_raw_get_request(req, id);
                    } else {
                       let begin_instant = Instant::now();
                       let resp = future_raw_get(causetStorage, req)
                            .map_ok(oneof!(batch_commands_response::response::Cmd::RawGet))
                            .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_get.inc());
                        response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::raw_get);
                    }
                },
                $(Some(batch_commands_request::request::Cmd::$cmd(req)) => {
                    let begin_instant = Instant::now();
                    let resp = $future_fn($($arg,)* req)
                        .map_ok(oneof!(batch_commands_response::response::Cmd::$cmd))
                        .map_err(|_| GRPC_MSG_FAIL_COUNTER.$metric_name.inc());
                    response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::$metric_name);
                })*
                Some(batch_commands_request::request::Cmd::Import(_)) => unimplemented!(),
            }
        }
    }

    handle_cmd! {
        Scan, future_scan(causetStorage), kv_scan;
        Prewrite, future_prewrite(causetStorage), kv_prewrite;
        Commit, future_commit(causetStorage), kv_commit;
        Cleanup, future_cleanup(causetStorage), kv_cleanup;
        BatchGet, future_batch_get(causetStorage), kv_batch_get;
        BatchRollback, future_batch_rollback(causetStorage), kv_batch_rollback;
        TxnHeartBeat, future_txn_heart_beat(causetStorage), kv_txn_heart_beat;
        CheckTxnStatus, future_check_txn_status(causetStorage), kv_check_txn_status;
        CheckSecondaryLocks, future_check_secondary_locks(causetStorage), kv_check_secondary_locks;
        ScanLock, future_scan_lock(causetStorage), kv_scan_lock;
        ResolveLock, future_resolve_lock(causetStorage), kv_resolve_lock;
        Gc, future_gc(), kv_gc;
        DeleteCone, future_delete_cone(causetStorage), kv_delete_cone;
        RawBatchGet, future_raw_batch_get(causetStorage), raw_batch_get;
        RawPut, future_raw_put(causetStorage), raw_put;
        RawBatchPut, future_raw_batch_put(causetStorage), raw_batch_put;
        RawDelete, future_raw_delete(causetStorage), raw_delete;
        RawBatchDelete, future_raw_batch_delete(causetStorage), raw_batch_delete;
        RawScan, future_raw_scan(causetStorage), raw_scan;
        RawDeleteCone, future_raw_delete_cone(causetStorage), raw_delete_cone;
        RawBatchScan, future_raw_batch_scan(causetStorage), raw_batch_scan;
        VerGet, future_ver_get(causetStorage), ver_get;
        VerBatchGet, future_ver_batch_get(causetStorage), ver_batch_get;
        VerMut, future_ver_mut(causetStorage), ver_mut;
        VerBatchMut, future_ver_batch_mut(causetStorage), ver_batch_mut;
        VerScan, future_ver_scan(causetStorage), ver_scan;
        VerDeleteCone, future_ver_delete_cone(causetStorage), ver_delete_cone;
        Interlock, future_cop(causet, Some(peer.to_string())), interlock;
        PessimisticLock, future_acquire_pessimistic_lock(causetStorage), kv_pessimistic_lock;
        PessimisticRollback, future_pessimistic_rollback(causetStorage), kv_pessimistic_rollback;
        Empty, future_handle_empty(), invalid;
    }
}

async fn future_handle_empty(
    req: BatchCommandsEmptyRequest,
) -> ServerResult<BatchCommandsEmptyResponse> {
    let mut res = BatchCommandsEmptyResponse::default();
    res.set_test_id(req.get_test_id());
    // `BatchCommandsWaker` processes futures in notify. If delay_time is too small, notify
    // can be called immediately, so the future is polled recursively and lead to deadlock.
    if req.get_delay_time() < 10 {
        Ok(res)
    } else {
        let _ = einsteindb_util::timer::GLOBAL_TIMER_HANDLE
            .delay(
                std::time::Instant::now() + std::time::Duration::from_millis(req.get_delay_time()),
            )
            .compat()
            .await;
        Ok(res)
    }
}

fn future_get<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: GetRequest,
) -> impl Future<Output = ServerResult<GetResponse>> {
    let v = causetStorage.get(
        req.take_context(),
        Key::from_raw(req.get_key()),
        req.get_version().into(),
    );

    async move {
        let v = v.await;
        let mut resp = GetResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else {
            match v {
                Ok(Some(val)) => resp.set_value(val),
                Ok(None) => resp.set_not_found(true),
                Err(e) => resp.set_error(extract_key_error(&e)),
            }
        }
        Ok(resp)
    }
}

fn future_scan<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: ScanRequest,
) -> impl Future<Output = ServerResult<ScanResponse>> {
    let lightlike_key = if req.get_lightlike_key().is_empty() {
        None
    } else {
        Some(Key::from_raw(req.get_lightlike_key()))
    };
    let v = causetStorage.scan(
        req.take_context(),
        Key::from_raw(req.get_spacelike_key()),
        lightlike_key,
        req.get_limit() as usize,
        req.get_sample_step() as usize,
        req.get_version().into(),
        req.get_key_only(),
        req.get_reverse(),
    );

    async move {
        let v = v.await;
        let mut resp = ScanResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else {
            resp.set_pairs(extract_kv_pairs(v).into());
        }
        Ok(resp)
    }
}

fn future_batch_get<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: BatchGetRequest,
) -> impl Future<Output = ServerResult<BatchGetResponse>> {
    let tuplespaceInstanton = req.get_tuplespaceInstanton().iter().map(|x| Key::from_raw(x)).collect();
    let v = causetStorage.batch_get(req.take_context(), tuplespaceInstanton, req.get_version().into());

    async move {
        let v = v.await;
        let mut resp = BatchGetResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else {
            resp.set_pairs(extract_kv_pairs(v).into());
        }
        Ok(resp)
    }
}

async fn future_gc(_: GcRequest) -> ServerResult<GcResponse> {
    Err(Error::Grpc(GrpcError::RpcFailure(RpcStatus::new(
        RpcStatusCode::UNIMPLEMENTED,
        None,
    ))))
}

fn future_delete_cone<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: DeleteConeRequest,
) -> impl Future<Output = ServerResult<DeleteConeResponse>> {
    let (cb, f) = paired_future_callback();
    let res = causetStorage.delete_cone(
        req.take_context(),
        Key::from_raw(req.get_spacelike_key()),
        Key::from_raw(req.get_lightlike_key()),
        req.get_notify_only(),
        cb,
    );

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = DeleteConeResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

fn future_raw_get<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: RawGetRequest,
) -> impl Future<Output = ServerResult<RawGetResponse>> {
    let v = causetStorage.raw_get(req.take_context(), req.take_causet(), req.take_key());

    async move {
        let v = v.await;
        let mut resp = RawGetResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else {
            match v {
                Ok(Some(val)) => resp.set_value(val),
                Ok(None) => resp.set_not_found(true),
                Err(e) => resp.set_error(format!("{}", e)),
            }
        }
        Ok(resp)
    }
}

fn future_raw_batch_get<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: RawBatchGetRequest,
) -> impl Future<Output = ServerResult<RawBatchGetResponse>> {
    let tuplespaceInstanton = req.take_tuplespaceInstanton().into();
    let v = causetStorage.raw_batch_get(req.take_context(), req.take_causet(), tuplespaceInstanton);

    async move {
        let v = v.await;
        let mut resp = RawBatchGetResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else {
            resp.set_pairs(extract_kv_pairs(v).into());
        }
        Ok(resp)
    }
}

fn future_raw_put<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: RawPutRequest,
) -> impl Future<Output = ServerResult<RawPutResponse>> {
    let (cb, f) = paired_future_callback();
    let res = causetStorage.raw_put(
        req.take_context(),
        req.take_causet(),
        req.take_key(),
        req.take_value(),
        cb,
    );

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = RawPutResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

fn future_raw_batch_put<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: RawBatchPutRequest,
) -> impl Future<Output = ServerResult<RawBatchPutResponse>> {
    let causet = req.take_causet();
    let pairs = req
        .take_pairs()
        .into_iter()
        .map(|mut x| (x.take_key(), x.take_value()))
        .collect();

    let (cb, f) = paired_future_callback();
    let res = causetStorage.raw_batch_put(req.take_context(), causet, pairs, cb);

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = RawBatchPutResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

fn future_raw_delete<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: RawDeleteRequest,
) -> impl Future<Output = ServerResult<RawDeleteResponse>> {
    let (cb, f) = paired_future_callback();
    let res = causetStorage.raw_delete(req.take_context(), req.take_causet(), req.take_key(), cb);

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = RawDeleteResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

fn future_raw_batch_delete<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: RawBatchDeleteRequest,
) -> impl Future<Output = ServerResult<RawBatchDeleteResponse>> {
    let causet = req.take_causet();
    let tuplespaceInstanton = req.take_tuplespaceInstanton().into();
    let (cb, f) = paired_future_callback();
    let res = causetStorage.raw_batch_delete(req.take_context(), causet, tuplespaceInstanton, cb);

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = RawBatchDeleteResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

fn future_raw_scan<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: RawScanRequest,
) -> impl Future<Output = ServerResult<RawScanResponse>> {
    let lightlike_key = if req.get_lightlike_key().is_empty() {
        None
    } else {
        Some(req.take_lightlike_key())
    };
    let v = causetStorage.raw_scan(
        req.take_context(),
        req.take_causet(),
        req.take_spacelike_key(),
        lightlike_key,
        req.get_limit() as usize,
        req.get_key_only(),
        req.get_reverse(),
    );

    async move {
        let v = v.await;
        let mut resp = RawScanResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else {
            resp.set_kvs(extract_kv_pairs(v).into());
        }
        Ok(resp)
    }
}

fn future_raw_batch_scan<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: RawBatchScanRequest,
) -> impl Future<Output = ServerResult<RawBatchScanResponse>> {
    let v = causetStorage.raw_batch_scan(
        req.take_context(),
        req.take_causet(),
        req.take_cones().into(),
        req.get_each_limit() as usize,
        req.get_key_only(),
        req.get_reverse(),
    );

    async move {
        let v = v.await;
        let mut resp = RawBatchScanResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else {
            resp.set_kvs(extract_kv_pairs(v).into());
        }
        Ok(resp)
    }
}

fn future_raw_delete_cone<E: Engine, L: LockManager>(
    causetStorage: &CausetStorage<E, L>,
    mut req: RawDeleteConeRequest,
) -> impl Future<Output = ServerResult<RawDeleteConeResponse>> {
    let (cb, f) = paired_future_callback();
    let res = causetStorage.raw_delete_cone(
        req.take_context(),
        req.take_causet(),
        req.take_spacelike_key(),
        req.take_lightlike_key(),
        cb,
    );

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = RawDeleteConeResponse::default();
        if let Some(err) = extract_brane_error(&v) {
            resp.set_brane_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

// unimplemented
fn future_ver_get<E: Engine, L: LockManager>(
    _causetStorage: &CausetStorage<E, L>,
    mut _req: VerGetRequest,
) -> impl Future<Output = ServerResult<VerGetResponse>> {
    let resp = VerGetResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_batch_get<E: Engine, L: LockManager>(
    _causetStorage: &CausetStorage<E, L>,
    mut _req: VerBatchGetRequest,
) -> impl Future<Output = ServerResult<VerBatchGetResponse>> {
    let resp = VerBatchGetResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_mut<E: Engine, L: LockManager>(
    _causetStorage: &CausetStorage<E, L>,
    mut _req: VerMutRequest,
) -> impl Future<Output = ServerResult<VerMutResponse>> {
    let resp = VerMutResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_batch_mut<E: Engine, L: LockManager>(
    _causetStorage: &CausetStorage<E, L>,
    mut _req: VerBatchMutRequest,
) -> impl Future<Output = ServerResult<VerBatchMutResponse>> {
    let resp = VerBatchMutResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_scan<E: Engine, L: LockManager>(
    _causetStorage: &CausetStorage<E, L>,
    mut _req: VerScanRequest,
) -> impl Future<Output = ServerResult<VerScanResponse>> {
    let resp = VerScanResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_delete_cone<E: Engine, L: LockManager>(
    _causetStorage: &CausetStorage<E, L>,
    mut _req: VerDeleteConeRequest,
) -> impl Future<Output = ServerResult<VerDeleteConeResponse>> {
    let resp = VerDeleteConeResponse::default();
    future::ok(resp)
}

fn future_cop<E: Engine>(
    causet: &Endpoint<E>,
    peer: Option<String>,
    req: Request,
) -> impl Future<Output = ServerResult<Response>> {
    let ret = causet.parse_and_handle_unary_request(req, peer);
    async move { Ok(ret.await) }
}

macro_rules! txn_command_future {
    ($fn_name: ident, $req_ty: ident, $resp_ty: ident, ($req: ident) $prelude: stmt; ($v: ident, $resp: ident) { $else_branch: expr }) => {
        fn $fn_name<E: Engine, L: LockManager>(
            causetStorage: &CausetStorage<E, L>,
            $req: $req_ty,
        ) -> impl Future<Output = ServerResult<$resp_ty>> {
            $prelude
            let (cb, f) = paired_future_callback();
            let res = causetStorage.sched_txn_command($req.into(), cb);

            async move {
                let $v = match res {
                    Err(e) => Err(e),
                    Ok(_) => f.await?,
                };
                let mut $resp = $resp_ty::default();
                if let Some(err) = extract_brane_error(&$v) {
                    $resp.set_brane_error(err);
                } else {
                    $else_branch;
                }
                Ok($resp)
            }
        }
    };
    ($fn_name: ident, $req_ty: ident, $resp_ty: ident, ($v: ident, $resp: ident) { $else_branch: expr }) => {
        txn_command_future!($fn_name, $req_ty, $resp_ty, (req) {}; ($v, $resp) { $else_branch });
    };
}

txn_command_future!(future_prewrite, PrewriteRequest, PrewriteResponse, (v, resp) {{
    if let Ok(v) = &v {
        resp.set_min_commit_ts(v.min_commit_ts.into_inner());
    }
    resp.set_errors(extract_key_errors(v.map(|v| v.locks)).into());
}});
txn_command_future!(future_acquire_pessimistic_lock, PessimisticLockRequest, PessimisticLockResponse, (v, resp) {
    match v {
        Ok(Ok(res)) => resp.set_values(res.into_vec().into()),
        Err(e) | Ok(Err(e)) => resp.set_errors(vec![extract_key_error(&e)].into()),
    }
});
txn_command_future!(future_pessimistic_rollback, PessimisticRollbackRequest, PessimisticRollbackResponse, (v, resp) {
    resp.set_errors(extract_key_errors(v).into())
});
txn_command_future!(future_batch_rollback, BatchRollbackRequest, BatchRollbackResponse, (v, resp) {
    if let Err(e) = v {
        resp.set_error(extract_key_error(&e));
    }
});
txn_command_future!(future_resolve_lock, ResolveLockRequest, ResolveLockResponse, (v, resp) {
    if let Err(e) = v {
        resp.set_error(extract_key_error(&e));
    }
});
txn_command_future!(future_commit, CommitRequest, CommitResponse, (v, resp) {
    match v {
        Ok(TxnStatus::Committed { commit_ts }) => {
            resp.set_commit_version(commit_ts.into_inner())
        }
        Ok(_) => unreachable!(),
        Err(e) => resp.set_error(extract_key_error(&e)),
    }
});
txn_command_future!(future_cleanup, CleanupRequest, CleanupResponse, (v, resp) {
    if let Err(e) = v {
        if let Some(ts) = extract_committed(&e) {
            resp.set_commit_version(ts.into_inner());
        } else {
            resp.set_error(extract_key_error(&e));
        }
    }
});
txn_command_future!(future_txn_heart_beat, TxnHeartBeatRequest, TxnHeartBeatResponse, (v, resp) {
    match v {
        Ok(txn_status) => {
            if let TxnStatus::Uncommitted { dagger } = txn_status {
                resp.set_lock_ttl(dagger.ttl);
            } else {
                unreachable!();
            }
        }
        Err(e) => resp.set_error(extract_key_error(&e)),
    }
});
txn_command_future!(future_check_txn_status, CheckTxnStatusRequest, CheckTxnStatusResponse,
    (req) let caller_spacelike_ts = req.get_caller_spacelike_ts().into();
    (v, resp) {
        match v {
            Ok(txn_status) => match txn_status {
                TxnStatus::RolledBack => resp.set_action(Action::NoAction),
                TxnStatus::TtlExpire => resp.set_action(Action::TtlExpireRollback),
                TxnStatus::LockNotExist => resp.set_action(Action::LockNotExistRollback),
                TxnStatus::Committed { commit_ts } => {
                    resp.set_commit_version(commit_ts.into_inner())
                }
                TxnStatus::Uncommitted { dagger } => {
                    // If the caller_spacelike_ts is max, it's a point get in the autocommit transaction.
                    // Even though the min_commit_ts is not pushed, the point get can ingore the dagger
                    // next time because it's not committed. So we pretlightlike it has been pushed.
                    // Also note that min_commit_ts of async commit locks are never pushed.
                    if !dagger.use_async_commit && (dagger.min_commit_ts > caller_spacelike_ts || caller_spacelike_ts.is_max()) {
                        resp.set_action(Action::MinCommitTsPushed);
                    }
                    resp.set_lock_ttl(dagger.ttl);
                    let primary = dagger.primary.clone();
                    resp.set_lock_info(dagger.into_lock_info(primary));
                }
            },
            Err(e) => resp.set_error(extract_key_error(&e)),
        }
});
txn_command_future!(future_check_secondary_locks, CheckSecondaryLocksRequest, CheckSecondaryLocksResponse, (status, resp) {
    match status {
        Ok(SecondaryLocksStatus::Locked(locks)) => {
            resp.set_locks(locks.into());
        },
        Ok(SecondaryLocksStatus::Committed(ts)) => {
            resp.set_commit_ts(ts.into_inner());
        },
        Ok(SecondaryLocksStatus::RolledBack) => {},
        Err(e) => resp.set_error(extract_key_error(&e)),
    }
});
txn_command_future!(future_scan_lock, ScanLockRequest, ScanLockResponse, (v, resp) {
    match v {
        Ok(locks) => resp.set_locks(locks.into()),
        Err(e) => resp.set_error(extract_key_error(&e)),
    }
});
txn_command_future!(future_mvcc_get_by_key, MvccGetByKeyRequest, MvccGetByKeyResponse, (v, resp) {
    match v {
        Ok(mvcc) => resp.set_info(mvcc.into_proto()),
        Err(e) => resp.set_error(format!("{}", e)),
    }
});
txn_command_future!(future_mvcc_get_by_spacelike_ts, MvccGetByStartTsRequest, MvccGetByStartTsResponse, (v, resp) {
    match v {
        Ok(Some((k, vv))) => {
            resp.set_key(k.into_raw().unwrap());
            resp.set_info(vv.into_proto());
        }
        Ok(None) => {
            resp.set_info(Default::default());
        }
        Err(e) => resp.set_error(format!("{}", e)),
    }
});

#[causetg(feature = "protobuf-codec")]
pub mod batch_commands_response {
    pub type Response = ekvproto::einsteindbpb::BatchCommandsResponseResponse;

    pub mod response {
        pub type Cmd = ekvproto::einsteindbpb::BatchCommandsResponse_Response_oneof_cmd;
    }
}

#[causetg(feature = "protobuf-codec")]
pub mod batch_commands_request {
    pub type Request = ekvproto::einsteindbpb::BatchCommandsRequestRequest;

    pub mod request {
        pub type Cmd = ekvproto::einsteindbpb::BatchCommandsRequest_Request_oneof_cmd;
    }
}

#[causetg(feature = "prost-codec")]
pub use ekvproto::einsteindbpb::batch_commands_request;
#[causetg(feature = "prost-codec")]
pub use ekvproto::einsteindbpb::batch_commands_response;

struct BatchRespCollector;
impl BatchCollector<BatchCommandsResponse, (u64, batch_commands_response::Response)>
    for BatchRespCollector
{
    fn collect(
        &mut self,
        v: &mut BatchCommandsResponse,
        e: (u64, batch_commands_response::Response),
    ) -> Option<(u64, batch_commands_response::Response)> {
        v.mut_request_ids().push(e.0);
        v.mut_responses().push(e.1);
        None
    }
}

#[causetg(test)]
mod tests {
    use super::*;
    use futures::channel::oneshot;
    use futures::executor::block_on;
    use std::thread;

    #[test]
    fn test_poll_future_notify_with_slow_source() {
        let (tx, rx) = oneshot::channel::<usize>();
        let (signal_tx, signal_rx) = oneshot::channel();

        thread::Builder::new()
            .name("source".to_owned())
            .spawn(move || {
                block_on(signal_rx).unwrap();
                tx.slightlike(100).unwrap();
            })
            .unwrap();

        let (tx1, rx1) = oneshot::channel::<usize>();
        let task = async move {
            let i = rx.await.unwrap();
            assert_eq!(thread::current().name(), Some("source"));
            tx1.slightlike(i + 100).unwrap();
        };
        poll_future_notify(task);
        signal_tx.slightlike(()).unwrap();
        assert_eq!(block_on(rx1).unwrap(), 200);
    }

    #[test]
    fn test_poll_future_notify_with_slow_poller() {
        let (tx, rx) = oneshot::channel::<usize>();
        let (signal_tx, signal_rx) = oneshot::channel();
        thread::Builder::new()
            .name("source".to_owned())
            .spawn(move || {
                tx.slightlike(100).unwrap();
                signal_tx.slightlike(()).unwrap();
            })
            .unwrap();

        let (tx1, rx1) = oneshot::channel::<usize>();
        block_on(signal_rx).unwrap();
        let task = async move {
            let i = rx.await.unwrap();
            assert_ne!(thread::current().name(), Some("source"));
            tx1.slightlike(i + 100).unwrap();
        };
        poll_future_notify(task);
        assert_eq!(block_on(rx1).unwrap(), 200);
    }
}
