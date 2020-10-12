// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_lmdb::LmdbEngine;
use engine_promises::{Engines, MiscExt, VioletaBftEngine};
use futures::channel::oneshot;
use futures::future::Future;
use futures::future::{FutureExt, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{self, TryStreamExt};
use grpcio::{Error as GrpcError, WriteFlags};
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink};
use ekvproto::debugpb::{self, *};
use ekvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, VioletaBftCmdRequest, VioletaBftRequestHeader, BraneDetailResponse,
    StatusCmdType, StatusRequest,
};
use tokio::runtime::Handle;

use crate::config::ConfigController;
use crate::server::debug::{Debugger, Error, Result};
use violetabftstore::router::VioletaBftStoreRouter;
use violetabftstore::store::msg::Callback;
use security::{check_common_name, SecurityManager};
use einsteindb_util::metrics;

fn error_to_status(e: Error) -> RpcStatus {
    let (code, msg) = match e {
        Error::NotFound(msg) => (RpcStatusCode::NOT_FOUND, Some(msg)),
        Error::InvalidArgument(msg) => (RpcStatusCode::INVALID_ARGUMENT, Some(msg)),
        Error::Other(e) => (RpcStatusCode::UNKNOWN, Some(format!("{:?}", e))),
    };
    RpcStatus::new(code, msg)
}

fn on_grpc_error(tag: &'static str, e: &GrpcError) {
    error!("{} failed: {:?}", tag, e);
}

fn error_to_grpc_error(tag: &'static str, e: Error) -> GrpcError {
    let status = error_to_status(e);
    let e = GrpcError::RpcFailure(status);
    on_grpc_error(tag, &e);
    e
}

/// Service handles the RPC messages for the `Debug` service.
#[derive(Clone)]
pub struct Service<ER: VioletaBftEngine, T: VioletaBftStoreRouter<LmdbEngine>> {
    pool: Handle,
    debugger: Debugger<ER>,
    raft_router: T,
    security_mgr: Arc<SecurityManager>,
}

impl<ER: VioletaBftEngine, T: VioletaBftStoreRouter<LmdbEngine>> Service<ER, T> {
    /// Constructs a new `Service` with `Engines`, a `VioletaBftStoreRouter` and a `GcWorker`.
    pub fn new(
        engines: Engines<LmdbEngine, ER>,
        pool: Handle,
        raft_router: T,
        causetg_controller: ConfigController,
        security_mgr: Arc<SecurityManager>,
    ) -> Service<ER, T> {
        let debugger = Debugger::new(engines, causetg_controller);
        Service {
            pool,
            debugger,
            raft_router,
            security_mgr,
        }
    }

    fn handle_response<F, P>(
        &self,
        ctx: RpcContext<'_>,
        sink: UnarySink<P>,
        resp: F,
        tag: &'static str,
    ) where
        P: Slightlike + 'static,
        F: Future<Output = Result<P>> + Slightlike + 'static,
    {
        let ctx_task = async move {
            match resp.await {
                Ok(resp) => sink.success(resp).await?,
                Err(e) => sink.fail(error_to_status(e)).await?,
            }
            Ok(())
        };
        ctx.spawn(ctx_task.unwrap_or_else(move |e| on_grpc_error(tag, &e)));
    }
}

impl<ER: VioletaBftEngine, T: VioletaBftStoreRouter<LmdbEngine> + 'static> debugpb::Debug for Service<ER, T> {
    fn get(&mut self, ctx: RpcContext<'_>, mut req: GetRequest, sink: UnarySink<GetResponse>) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "debug_get";

        let db = req.get_db();
        let causet = req.take_causet();
        let key = req.take_key();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.get(db, &causet, key.as_slice()) });
        let f = async move {
            let value = join.await.unwrap()?;
            let mut resp = GetResponse::default();
            resp.set_value(value);
            Ok(resp)
        };

        self.handle_response(ctx, sink, f, TAG);
    }

    fn raft_log(
        &mut self,
        ctx: RpcContext<'_>,
        req: VioletaBftLogRequest,
        sink: UnarySink<VioletaBftLogResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "debug_raft_log";

        let brane_id = req.get_brane_id();
        let log_index = req.get_log_index();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.raft_log(brane_id, log_index) });
        let f = async move {
            let entry = join.await.unwrap()?;
            let mut resp = VioletaBftLogResponse::default();
            resp.set_entry(entry);
            Ok(resp)
        };

        self.handle_response(ctx, sink, f, TAG);
    }

    fn brane_info(
        &mut self,
        ctx: RpcContext<'_>,
        req: BraneInfoRequest,
        sink: UnarySink<BraneInfoResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "debug_brane_log";

        let brane_id = req.get_brane_id();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.brane_info(brane_id) });
        let f = async move {
            let brane_info = join.await.unwrap()?;
            let mut resp = BraneInfoResponse::default();
            if let Some(raft_local_state) = brane_info.raft_local_state {
                resp.set_raft_local_state(raft_local_state);
            }
            if let Some(raft_apply_state) = brane_info.raft_apply_state {
                resp.set_raft_apply_state(raft_apply_state);
            }
            if let Some(brane_state) = brane_info.brane_local_state {
                resp.set_brane_local_state(brane_state);
            }
            Ok(resp)
        };

        self.handle_response(ctx, sink, f, TAG);
    }

    fn brane_size(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: BraneSizeRequest,
        sink: UnarySink<BraneSizeResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "debug_brane_size";

        let brane_id = req.get_brane_id();
        let causets = req.take_causets().into();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.brane_size(brane_id, causets) });
        let f = async move {
            let entries = join.await.unwrap()?;
            let mut resp = BraneSizeResponse::default();
            resp.set_entries(
                entries
                    .into_iter()
                    .map(|(causet, size)| {
                        let mut entry = brane_size_response::Entry::default();
                        entry.set_causet(causet);
                        entry.set_size(size as u64);
                        entry
                    })
                    .collect(),
            );
            Ok(resp)
        };

        self.handle_response(ctx, sink, f, TAG);
    }

    fn scan_mvcc(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: ScanMvccRequest,
        mut sink: ServerStreamingSink<ScanMvccResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let debugger = self.debugger.clone();
        let from = req.take_from_key();
        let to = req.take_to_key();
        let limit = req.get_limit();

        let future = async move {
            let iter = debugger.scan_mvcc(&from, &to, limit);
            if iter.is_err() {
                return;
            }
            let mut s = stream::iter(iter.unwrap())
                .map_err(|e| error_to_grpc_error("scan_mvcc", e))
                .map_ok(|(key, mvcc_info)| {
                    let mut resp = ScanMvccResponse::default();
                    resp.set_key(key);
                    resp.set_info(mvcc_info);
                    (resp, WriteFlags::default())
                });
            if let Err(e) = sink.slightlike_all(&mut s).await {
                on_grpc_error("scan_mvcc", &e);
                return;
            }
            let _ = sink.close().await;
        };
        self.pool.spawn(future);
    }

    fn compact(
        &mut self,
        ctx: RpcContext<'_>,
        req: CompactRequest,
        sink: UnarySink<CompactResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let debugger = self.debugger.clone();

        let res = self.pool.spawn(async move {
            let req = req;
            debugger
                .compact(
                    req.get_db(),
                    req.get_causet(),
                    req.get_from_key(),
                    req.get_to_key(),
                    req.get_threads(),
                    req.get_bottommost_level_compaction().into(),
                )
                .map(|_| CompactResponse::default())
        });

        let f = async move { res.await.unwrap() };

        self.handle_response(ctx, sink, f, "debug_compact");
    }

    fn inject_fail_point(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: InjectFailPointRequest,
        sink: UnarySink<InjectFailPointResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "debug_inject_fail_point";

        let f = self
            .pool
            .spawn(async move {
                let name = req.take_name();
                if name.is_empty() {
                    return Err(Error::InvalidArgument("Failure Type INVALID".to_owned()));
                }
                let actions = req.get_actions();
                if let Err(e) = fail::causetg(name, actions) {
                    return Err(box_err!("{:?}", e));
                }
                Ok(InjectFailPointResponse::default())
            })
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn recover_fail_point(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: RecoverFailPointRequest,
        sink: UnarySink<RecoverFailPointResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "debug_recover_fail_point";

        let f = self
            .pool
            .spawn(async move {
                let name = req.take_name();
                if name.is_empty() {
                    return Err(Error::InvalidArgument("Failure Type INVALID".to_owned()));
                }
                fail::remove(name);
                Ok(RecoverFailPointResponse::default())
            })
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn list_fail_points(
        &mut self,
        ctx: RpcContext<'_>,
        _: ListFailPointsRequest,
        sink: UnarySink<ListFailPointsResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "debug_list_fail_points";

        let f = self
            .pool
            .spawn(async move {
                let list = fail::list().into_iter().map(|(name, actions)| {
                    let mut entry = list_fail_points_response::Entry::default();
                    entry.set_name(name);
                    entry.set_actions(actions);
                    entry
                });
                let mut resp = ListFailPointsResponse::default();
                resp.set_entries(list.collect());
                Ok(resp)
            })
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn get_metrics(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetMetricsRequest,
        sink: UnarySink<GetMetricsResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "debug_get_metrics";

        let debugger = self.debugger.clone();
        let f = self
            .pool
            .spawn(async move {
                let mut resp = GetMetricsResponse::default();
                resp.set_store_id(debugger.get_store_id()?);
                resp.set_prometheus(metrics::dump());
                if req.get_all() {
                    let engines = debugger.get_engine();
                    resp.set_lmdb_kv(box_try!(MiscExt::dump_stats(&engines.kv)));
                    resp.set_lmdb_raft(box_try!(VioletaBftEngine::dump_stats(&engines.violetabft)));
                    resp.set_jemalloc(einsteindb_alloc::dump_stats());
                }
                Ok(resp)
            })
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn check_brane_consistency(
        &mut self,
        ctx: RpcContext<'_>,
        req: BraneConsistencyCheckRequest,
        sink: UnarySink<BraneConsistencyCheckResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let brane_id = req.get_brane_id();
        let debugger = self.debugger.clone();
        let router1 = self.raft_router.clone();
        let router2 = self.raft_router.clone();

        let consistency_check_task = async move {
            let store_id = debugger.get_store_id()?;
            let detail = brane_detail(router2, brane_id, store_id).await?;
            consistency_check(router1, detail).await
        };
        let f = self
            .pool
            .spawn(consistency_check_task)
            .map(|res| res.unwrap())
            .map_ok(|_| BraneConsistencyCheckResponse::default());
        self.handle_response(ctx, sink, f, "check_brane_consistency");
    }

    fn modify_einsteindb_config(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: ModifyEINSTEINDBConfigRequest,
        sink: UnarySink<ModifyEINSTEINDBConfigResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "modify_einsteindb_config";

        let config_name = req.take_config_name();
        let config_value = req.take_config_value();
        let debugger = self.debugger.clone();

        let f = self
            .pool
            .spawn(async move { debugger.modify_einsteindb_config(&config_name, &config_value) })
            .map(|res| res.unwrap())
            .map_ok(|_| ModifyEINSTEINDBConfigResponse::default());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn get_brane_properties(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetBranePropertiesRequest,
        sink: UnarySink<GetBranePropertiesResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "get_brane_properties";
        let debugger = self.debugger.clone();

        let f = self
            .pool
            .spawn(async move { debugger.get_brane_properties(req.get_brane_id()) })
            .map(|res| res.unwrap())
            .map_ok(|props| {
                let mut resp = GetBranePropertiesResponse::default();
                for (name, value) in props {
                    let mut prop = Property::default();
                    prop.set_name(name);
                    prop.set_value(value);
                    resp.mut_props().push(prop);
                }
                resp
            });

        self.handle_response(ctx, sink, f, TAG);
    }

    fn get_store_info(
        &mut self,
        ctx: RpcContext<'_>,
        _: GetStoreInfoRequest,
        sink: UnarySink<GetStoreInfoResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "debug_get_store_id";
        let debugger = self.debugger.clone();

        let f = self
            .pool
            .spawn(async move {
                let mut resp = GetStoreInfoResponse::default();
                match debugger.get_store_id() {
                    Ok(store_id) => resp.set_store_id(store_id),
                    Err(_) => resp.set_store_id(0),
                }
                Ok(resp)
            })
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn get_cluster_info(
        &mut self,
        ctx: RpcContext<'_>,
        _: GetClusterInfoRequest,
        sink: UnarySink<GetClusterInfoResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        const TAG: &str = "debug_get_cluster_id";
        let debugger = self.debugger.clone();

        let f = self
            .pool
            .spawn(async move {
                let mut resp = GetClusterInfoResponse::default();
                match debugger.get_cluster_id() {
                    Ok(cluster_id) => resp.set_cluster_id(cluster_id),
                    Err(_) => resp.set_cluster_id(0),
                }
                Ok(resp)
            })
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }
}

fn brane_detail<T: VioletaBftStoreRouter<LmdbEngine>>(
    raft_router: T,
    brane_id: u64,
    store_id: u64,
) -> impl Future<Output = Result<BraneDetailResponse>> {
    let mut header = VioletaBftRequestHeader::default();
    header.set_brane_id(brane_id);
    header.mut_peer().set_store_id(store_id);
    let mut status_request = StatusRequest::default();
    status_request.set_cmd_type(StatusCmdType::BraneDetail);
    let mut raft_cmd = VioletaBftCmdRequest::default();
    raft_cmd.set_header(header);
    raft_cmd.set_status_request(status_request);

    let (tx, rx) = oneshot::channel();
    let cb = Callback::Read(Box::new(|resp| tx.slightlike(resp).unwrap()));

    async move {
        raft_router
            .slightlike_command(raft_cmd, cb)
            .map_err(|e| Error::Other(Box::new(e)))?;

        let mut r = rx.map_err(|e| Error::Other(Box::new(e))).await?;

        if r.response.get_header().has_error() {
            let e = r.response.get_header().get_error();
            warn!("brane_detail got error"; "err" => ?e);
            return Err(Error::Other(e.message.clone().into()));
        }

        let detail = r.response.take_status_response().take_brane_detail();
        debug!("brane_detail got brane detail"; "detail" => ?detail);
        let leader_store_id = detail.get_leader().get_store_id();
        if leader_store_id != store_id {
            let msg = format!("Leader is on store {}", leader_store_id);
            return Err(Error::Other(msg.into()));
        }
        Ok(detail)
    }
}

fn consistency_check<T: VioletaBftStoreRouter<LmdbEngine>>(
    raft_router: T,
    mut detail: BraneDetailResponse,
) -> impl Future<Output = Result<()>> {
    let mut header = VioletaBftRequestHeader::default();
    header.set_brane_id(detail.get_brane().get_id());
    header.set_peer(detail.take_leader());
    let mut admin_request = AdminRequest::default();
    admin_request.set_cmd_type(AdminCmdType::ComputeHash);
    let mut raft_cmd = VioletaBftCmdRequest::default();
    raft_cmd.set_header(header);
    raft_cmd.set_admin_request(admin_request);

    let (tx, rx) = oneshot::channel();
    let cb = Callback::Read(Box::new(|resp| tx.slightlike(resp).unwrap()));

    async move {
        raft_router
            .slightlike_command(raft_cmd, cb)
            .map_err(|e| Error::Other(Box::new(e)))?;

        let r = rx.map_err(|e| Error::Other(Box::new(e))).await?;

        if r.response.get_header().has_error() {
            let e = r.response.get_header().get_error();
            warn!("consistency-check got error"; "err" => ?e);
            return Err(Error::Other(e.message.clone().into()));
        }
        Ok(())
    }
}

#[causetg(feature = "protobuf-codec")]
mod brane_size_response {
    pub type Entry = ekvproto::debugpb::BraneSizeResponseEntry;
}

#[causetg(feature = "protobuf-codec")]
mod list_fail_points_response {
    pub type Entry = ekvproto::debugpb::ListFailPointsResponseEntry;
}
