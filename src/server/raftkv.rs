// Copyright 2016 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};
use std::io::Error as IoError;
use std::result;
use std::{sync::atomic::Ordering, sync::Arc, time::Duration};

use engine_lmdb::{LmdbEngine, LmdbSnapshot, LmdbTablePropertiesCollection};
use engine_promises::CfName;
use engine_promises::CAUSET_DEFAULT;
use engine_promises::{IterOptions, Peekable, ReadOptions, Snapshot, TablePropertiesExt};
use ekvproto::kvrpcpb::Context;
use ekvproto::raft_cmdpb::{
    CmdType, DeleteConeRequest, DeleteRequest, PutRequest, VioletaBftCmdRequest, VioletaBftCmdResponse,
    VioletaBftRequestHeader, Request, Response,
};
use ekvproto::{errorpb, metapb};
use txn_types::{Key, TxnExtraScheduler, Value};

use super::metrics::*;
use crate::causetStorage::kv::{
    write_modifies, Callback, CbContext, Cursor, Engine, Error as KvError,
    ErrorInner as KvErrorInner, Iteron as EngineIterator, Modify, ScanMode,
    Snapshot as EngineSnapshot, WriteData,
};
use crate::causetStorage::{self, kv};
use violetabftstore::errors::Error as VioletaBftServerError;
use violetabftstore::router::{LocalReadRouter, VioletaBftStoreRouter};
use violetabftstore::store::{Callback as StoreCallback, ReadResponse, WriteResponse};
use violetabftstore::store::{BraneIterator, BraneSnapshot};
use einsteindb_util::time::Instant;
use einsteindb_util::time::ThreadReadId;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        RequestFailed(e: errorpb::Error) {
            from()
            display("{}", e.get_message())
        }
        Io(e: IoError) {
            from()
            cause(e)
            display("{}", e)
        }

        Server(e: VioletaBftServerError) {
            from()
            cause(e)
            display("{}", e)
        }
        InvalidResponse(reason: String) {
            display("{}", reason)
        }
        InvalidRequest(reason: String) {
            display("{}", reason)
        }
        Timeout(d: Duration) {
            display("timeout after {:?}", d)
        }
    }
}

fn get_status_kind_from_error(e: &Error) -> RequestStatusKind {
    match *e {
        Error::RequestFailed(ref header) => {
            RequestStatusKind::from(causetStorage::get_error_kind_from_header(header))
        }
        Error::Io(_) => RequestStatusKind::err_io,
        Error::Server(_) => RequestStatusKind::err_server,
        Error::InvalidResponse(_) => RequestStatusKind::err_invalid_resp,
        Error::InvalidRequest(_) => RequestStatusKind::err_invalid_req,
        Error::Timeout(_) => RequestStatusKind::err_timeout,
    }
}

fn get_status_kind_from_engine_error(e: &kv::Error) -> RequestStatusKind {
    match *e {
        KvError(box KvErrorInner::Request(ref header)) => {
            RequestStatusKind::from(causetStorage::get_error_kind_from_header(header))
        }

        KvError(box KvErrorInner::Timeout(_)) => RequestStatusKind::err_timeout,
        KvError(box KvErrorInner::EmptyRequest) => RequestStatusKind::err_empty_request,
        KvError(box KvErrorInner::Other(_)) => RequestStatusKind::err_other,
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for kv::Error {
    fn from(e: Error) -> kv::Error {
        match e {
            Error::RequestFailed(e) => KvError::from(KvErrorInner::Request(e)),
            Error::Server(e) => e.into(),
            e => box_err!(e),
        }
    }
}

impl From<VioletaBftServerError> for KvError {
    fn from(e: VioletaBftServerError) -> KvError {
        KvError(Box::new(KvErrorInner::Request(e.into())))
    }
}

/// `VioletaBftKv` is a causetStorage engine base on `VioletaBftStore`.
#[derive(Clone)]
pub struct VioletaBftKv<S>
where
    S: VioletaBftStoreRouter<LmdbEngine> + LocalReadRouter<LmdbEngine> + 'static,
{
    router: S,
    engine: LmdbEngine,
    txn_extra_scheduler: Option<Arc<dyn TxnExtraScheduler>>,
}

pub enum CmdRes {
    Resp(Vec<Response>),
    Snap(BraneSnapshot<LmdbSnapshot>),
}

fn new_ctx(resp: &VioletaBftCmdResponse) -> CbContext {
    let mut cb_ctx = CbContext::new();
    cb_ctx.term = Some(resp.get_header().get_current_term());
    cb_ctx
}

fn check_raft_cmd_response(resp: &mut VioletaBftCmdResponse, req_cnt: usize) -> Result<()> {
    if resp.get_header().has_error() {
        return Err(Error::RequestFailed(resp.take_header().take_error()));
    }
    if req_cnt != resp.get_responses().len() {
        return Err(Error::InvalidResponse(format!(
            "responses count {} is not equal to requests count {}",
            resp.get_responses().len(),
            req_cnt
        )));
    }

    Ok(())
}

fn on_write_result(mut write_resp: WriteResponse, req_cnt: usize) -> (CbContext, Result<CmdRes>) {
    let cb_ctx = new_ctx(&write_resp.response);
    if let Err(e) = check_raft_cmd_response(&mut write_resp.response, req_cnt) {
        return (cb_ctx, Err(e));
    }
    let resps = write_resp.response.take_responses();
    (cb_ctx, Ok(CmdRes::Resp(resps.into())))
}

fn on_read_result(
    mut read_resp: ReadResponse<LmdbSnapshot>,
    req_cnt: usize,
) -> (CbContext, Result<CmdRes>) {
    let mut cb_ctx = new_ctx(&read_resp.response);
    cb_ctx.txn_extra_op = read_resp.txn_extra_op;
    if let Err(e) = check_raft_cmd_response(&mut read_resp.response, req_cnt) {
        return (cb_ctx, Err(e));
    }
    let resps = read_resp.response.take_responses();
    if !resps.is_empty() || resps[0].get_cmd_type() == CmdType::Snap {
        (cb_ctx, Ok(CmdRes::Snap(read_resp.snapshot.unwrap())))
    } else {
        (cb_ctx, Ok(CmdRes::Resp(resps.into())))
    }
}

impl<S> VioletaBftKv<S>
where
    S: VioletaBftStoreRouter<LmdbEngine> + LocalReadRouter<LmdbEngine> + 'static,
{
    /// Create a VioletaBftKv using specified configuration.
    pub fn new(router: S, engine: LmdbEngine) -> VioletaBftKv<S> {
        VioletaBftKv {
            router,
            engine,
            txn_extra_scheduler: None,
        }
    }

    pub fn set_txn_extra_scheduler(&mut self, txn_extra_scheduler: Arc<dyn TxnExtraScheduler>) {
        self.txn_extra_scheduler = Some(txn_extra_scheduler);
    }

    fn new_request_header(&self, ctx: &Context) -> VioletaBftRequestHeader {
        let mut header = VioletaBftRequestHeader::default();
        header.set_brane_id(ctx.get_brane_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_brane_epoch(ctx.get_brane_epoch().clone());
        if ctx.get_term() != 0 {
            header.set_term(ctx.get_term());
        }
        header.set_sync_log(ctx.get_sync_log());
        header.set_replica_read(ctx.get_replica_read());
        header
    }

    fn exec_snapshot(
        &self,
        read_id: Option<ThreadReadId>,
        ctx: &Context,
        req: Request,
        cb: Callback<CmdRes>,
    ) -> Result<()> {
        let header = self.new_request_header(ctx);
        let mut cmd = VioletaBftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(vec![req].into());
        self.router
            .read(
                read_id,
                cmd,
                StoreCallback::Read(Box::new(move |resp| {
                    let (cb_ctx, res) = on_read_result(resp, 1);
                    cb((cb_ctx, res.map_err(Error::into)));
                })),
            )
            .map_err(From::from)
    }

    fn exec_write_requests(
        &self,
        ctx: &Context,
        reqs: Vec<Request>,
        cb: Callback<CmdRes>,
    ) -> Result<()> {
        #[causetg(feature = "failpoints")]
        {
            // If rid is some, only the specified brane reports error.
            // If rid is None, all branes report error.
            let raftkv_early_error_report_fp = || -> Result<()> {
                fail_point!("raftkv_early_error_report", |rid| {
                    let brane_id = ctx.get_brane_id();
                    rid.and_then(|rid| {
                        let rid: u64 = rid.parse().unwrap();
                        if rid == brane_id {
                            None
                        } else {
                            Some(())
                        }
                    })
                    .ok_or_else(|| VioletaBftServerError::BraneNotFound(brane_id).into())
                });
                Ok(())
            };
            raftkv_early_error_report_fp()?;
        }

        let len = reqs.len();
        let header = self.new_request_header(ctx);
        let mut cmd = VioletaBftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        self.router
            .slightlike_command(
                cmd,
                StoreCallback::Write(Box::new(move |resp| {
                    let (cb_ctx, res) = on_write_result(resp, len);
                    cb((cb_ctx, res.map_err(Error::into)));
                })),
            )
            .map_err(From::from)
    }
}

fn invalid_resp_type(exp: CmdType, act: CmdType) -> Error {
    Error::InvalidResponse(format!(
        "cmd type not match, want {:?}, got {:?}!",
        exp, act
    ))
}

impl<S> Display for VioletaBftKv<S>
where
    S: VioletaBftStoreRouter<LmdbEngine> + LocalReadRouter<LmdbEngine> + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "VioletaBftKv")
    }
}

impl<S> Debug for VioletaBftKv<S>
where
    S: VioletaBftStoreRouter<LmdbEngine> + LocalReadRouter<LmdbEngine> + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "VioletaBftKv")
    }
}

impl<S> Engine for VioletaBftKv<S>
where
    S: VioletaBftStoreRouter<LmdbEngine> + LocalReadRouter<LmdbEngine> + 'static,
{
    type Snap = BraneSnapshot<LmdbSnapshot>;
    type Local = LmdbEngine;

    fn kv_engine(&self) -> LmdbEngine {
        self.engine.clone()
    }

    fn snapshot_on_kv_engine(&self, spacelike_key: &[u8], lightlike_key: &[u8]) -> kv::Result<Self::Snap> {
        let mut brane = metapb::Brane::default();
        brane.set_spacelike_key(spacelike_key.to_owned());
        brane.set_lightlike_key(lightlike_key.to_owned());
        // Use a fake peer to avoid panic.
        brane.mut_peers().push(Default::default());
        Ok(BraneSnapshot::<LmdbSnapshot>::from_raw(
            self.engine.clone(),
            brane,
        ))
    }

    fn modify_on_kv_engine(&self, mut modifies: Vec<Modify>) -> kv::Result<()> {
        for modify in &mut modifies {
            match modify {
                Modify::Delete(_, ref mut key) => {
                    let bytes = tuplespaceInstanton::data_key(key.as_encoded());
                    *key = Key::from_encoded(bytes);
                }
                Modify::Put(_, ref mut key, _) => {
                    let bytes = tuplespaceInstanton::data_key(key.as_encoded());
                    *key = Key::from_encoded(bytes);
                }
                Modify::DeleteCone(_, ref mut key1, ref mut key2, _) => {
                    let bytes = tuplespaceInstanton::data_key(key1.as_encoded());
                    *key1 = Key::from_encoded(bytes);
                    let bytes = tuplespaceInstanton::data_lightlike_key(key2.as_encoded());
                    *key2 = Key::from_encoded(bytes);
                }
            }
        }
        write_modifies(&self.engine, modifies)
    }

    fn async_write(&self, ctx: &Context, batch: WriteData, cb: Callback<()>) -> kv::Result<()> {
        fail_point!("raftkv_async_write");
        if batch.modifies.is_empty() {
            return Err(KvError::from(KvErrorInner::EmptyRequest));
        }

        let mut reqs = Vec::with_capacity(batch.modifies.len());
        for m in batch.modifies {
            let mut req = Request::default();
            match m {
                Modify::Delete(causet, k) => {
                    let mut delete = DeleteRequest::default();
                    delete.set_key(k.into_encoded());
                    if causet != CAUSET_DEFAULT {
                        delete.set_causet(causet.to_string());
                    }
                    req.set_cmd_type(CmdType::Delete);
                    req.set_delete(delete);
                }
                Modify::Put(causet, k, v) => {
                    let mut put = PutRequest::default();
                    put.set_key(k.into_encoded());
                    put.set_value(v);
                    if causet != CAUSET_DEFAULT {
                        put.set_causet(causet.to_string());
                    }
                    req.set_cmd_type(CmdType::Put);
                    req.set_put(put);
                }
                Modify::DeleteCone(causet, spacelike_key, lightlike_key, notify_only) => {
                    let mut delete_cone = DeleteConeRequest::default();
                    delete_cone.set_causet(causet.to_string());
                    delete_cone.set_spacelike_key(spacelike_key.into_encoded());
                    delete_cone.set_lightlike_key(lightlike_key.into_encoded());
                    delete_cone.set_notify_only(notify_only);
                    req.set_cmd_type(CmdType::DeleteCone);
                    req.set_delete_cone(delete_cone);
                }
            }
            reqs.push(req);
        }

        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let begin_instant = Instant::now_coarse();

        if let Some(tx) = self.txn_extra_scheduler.as_ref() {
            if !batch.extra.is_empty() {
                tx.schedule(batch.extra);
            }
        }

        self.exec_write_requests(
            ctx,
            reqs,
            Box::new(move |(cb_ctx, res)| match res {
                Ok(CmdRes::Resp(_)) => {
                    ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .write
                        .observe(begin_instant.elapsed_secs());
                    fail_point!("raftkv_async_write_finish");
                    cb((cb_ctx, Ok(())))
                }
                Ok(CmdRes::Snap(_)) => cb((
                    cb_ctx,
                    Err(box_err!("unexpect snapshot, should mutate instead.")),
                )),
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                    cb((cb_ctx, Err(e)))
                }
            }),
        )
        .map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
            e.into()
        })
    }

    fn async_snapshot(
        &self,
        ctx: &Context,
        read_id: Option<ThreadReadId>,
        cb: Callback<Self::Snap>,
    ) -> kv::Result<()> {
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let begin_instant = Instant::now_coarse();
        self.exec_snapshot(
            read_id,
            ctx,
            req,
            Box::new(move |(cb_ctx, res)| match res {
                Ok(CmdRes::Resp(r)) => cb((
                    cb_ctx,
                    Err(invalid_resp_type(CmdType::Snap, r[0].get_cmd_type()).into()),
                )),
                Ok(CmdRes::Snap(s)) => {
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .snapshot
                        .observe(begin_instant.elapsed_secs());
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    cb((cb_ctx, Ok(s)))
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                    cb((cb_ctx, Err(e)))
                }
            }),
        )
        .map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
            e.into()
        })
    }

    fn release_snapshot(&self) {
        self.router.release_snapshot_cache();
    }

    fn get_properties_causet(
        &self,
        causet: CfName,
        spacelike: &[u8],
        lightlike: &[u8],
    ) -> kv::Result<LmdbTablePropertiesCollection> {
        let spacelike = tuplespaceInstanton::data_key(spacelike);
        let lightlike = tuplespaceInstanton::data_lightlike_key(lightlike);
        self.engine
            .get_cone_properties_causet(causet, &spacelike, &lightlike)
            .map_err(|e| e.into())
    }
}

impl<S: Snapshot> EngineSnapshot for BraneSnapshot<S> {
    type Iter = BraneIterator<S>;

    fn get(&self, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get", |_| Err(box_err!(
            "injected error for get"
        )));
        let v = box_try!(self.get_value(key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_causet(&self, causet: CfName, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get_causet", |_| Err(box_err!(
            "injected error for get_causet"
        )));
        let v = box_try!(self.get_value_causet(causet, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_causet_opt(&self, opts: ReadOptions, causet: CfName, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get_causet", |_| Err(box_err!(
            "injected error for get_causet"
        )));
        let v = box_try!(self.get_value_causet_opt(&opts, causet, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter(&self, iter_opt: IterOptions, mode: ScanMode) -> kv::Result<Cursor<Self::Iter>> {
        fail_point!("raftkv_snapshot_iter", |_| Err(box_err!(
            "injected error for iter"
        )));
        Ok(Cursor::new(BraneSnapshot::iter(self, iter_opt), mode))
    }

    fn iter_causet(
        &self,
        causet: CfName,
        iter_opt: IterOptions,
        mode: ScanMode,
    ) -> kv::Result<Cursor<Self::Iter>> {
        fail_point!("raftkv_snapshot_iter_causet", |_| Err(box_err!(
            "injected error for iter_causet"
        )));
        Ok(Cursor::new(
            BraneSnapshot::iter_causet(self, causet, iter_opt)?,
            mode,
        ))
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        Some(self.get_spacelike_key())
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        Some(self.get_lightlike_key())
    }

    #[inline]
    fn get_data_version(&self) -> Option<u64> {
        self.get_apply_index().ok()
    }

    fn is_max_ts_synced(&self) -> bool {
        self.max_ts_sync_status
            .as_ref()
            .map(|v| v.load(Ordering::SeqCst) & 1 == 1)
            .unwrap_or(false)
    }
}

impl<S: Snapshot> EngineIterator for BraneIterator<S> {
    fn next(&mut self) -> kv::Result<bool> {
        BraneIterator::next(self).map_err(KvError::from)
    }

    fn prev(&mut self) -> kv::Result<bool> {
        BraneIterator::prev(self).map_err(KvError::from)
    }

    fn seek(&mut self, key: &Key) -> kv::Result<bool> {
        fail_point!("raftkv_iter_seek", |_| Err(box_err!(
            "injected error for iter_seek"
        )));
        BraneIterator::seek(self, key.as_encoded()).map_err(From::from)
    }

    fn seek_for_prev(&mut self, key: &Key) -> kv::Result<bool> {
        fail_point!("raftkv_iter_seek_for_prev", |_| Err(box_err!(
            "injected error for iter_seek_for_prev"
        )));
        BraneIterator::seek_for_prev(self, key.as_encoded()).map_err(From::from)
    }

    fn seek_to_first(&mut self) -> kv::Result<bool> {
        BraneIterator::seek_to_first(self).map_err(KvError::from)
    }

    fn seek_to_last(&mut self) -> kv::Result<bool> {
        BraneIterator::seek_to_last(self).map_err(KvError::from)
    }

    fn valid(&self) -> kv::Result<bool> {
        BraneIterator::valid(self).map_err(KvError::from)
    }

    fn validate_key(&self, key: &Key) -> kv::Result<()> {
        self.should_seekable(key.as_encoded()).map_err(From::from)
    }

    fn key(&self) -> &[u8] {
        BraneIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        BraneIterator::value(self)
    }
}
