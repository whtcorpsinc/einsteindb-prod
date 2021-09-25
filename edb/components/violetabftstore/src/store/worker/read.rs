//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::cell::Cell;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam::atomic::AtomicCell;
use crossbeam::TrylightlikeError;
use ekvproto::error_timeshare;
use ekvproto::kvrpc_timeshare::ExtraOp as TxnExtraOp;
use ekvproto::meta_timeshare;
use ekvproto::violetabft_cmd_timeshare::{
    CmdType, VioletaBftCmdRequest, VioletaBftCmdResponse, ReadIndexResponse, Request, Response,
};
use time::Timespec;

use crate::errors::VIOLETABFTSTORE_IS_BUSY;
use crate::store::util::{self, LeaseState, RemoteLease};
use crate::store::{
    cmd_resp, Callback, Peer, ProposalRouter, VioletaBftCommand, ReadResponse, BraneSnapshot,
    RequestInspector, RequestPolicy,
};
use crate::Result;

use edb::{CausetEngine, VioletaBftEngine};
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::time::monotonic_raw_now;
use violetabftstore::interlock::::time::{Instant, ThreadReadId};

use super::metrics::*;
use crate::store::fsm::store::StoreMeta;

pub trait ReadFreeDaemon<E: CausetEngine> {
    fn get_engine(&self) -> &E;
    fn get_snapshot(&mut self, ts: Option<ThreadReadId>) -> Arc<E::Snapshot>;
    fn get_value(&self, req: &Request, brane: &meta_timeshare::Brane) -> Result<Response> {
        let key = req.get_get().get_key();
        // brane key cone has no data prefix, so we must use origin key to check.
        util::check_key_in_brane(key, brane)?;

        let engine = self.get_engine();
        let mut resp = Response::default();
        let res = if !req.get_get().get_causet().is_empty() {
            let causet = req.get_get().get_causet();
            engine
                .get_value_causet(causet, &tuplespaceInstanton::data_key(key))
                .unwrap_or_else(|e| {
                    panic!(
                        "[brane {}] failed to get {} with causet {}: {:?}",
                        brane.get_id(),
                        hex::encode_upper(key),
                        causet,
                        e
                    )
                })
        } else {
            engine.get_value(&tuplespaceInstanton::data_key(key)).unwrap_or_else(|e| {
                panic!(
                    "[brane {}] failed to get {}: {:?}",
                    brane.get_id(),
                    hex::encode_upper(key),
                    e
                )
            })
        };
        if let Some(res) = res {
            resp.mut_get().set_value(res.to_vec());
        }

        Ok(resp)
    }

    fn execute(
        &mut self,
        msg: &VioletaBftCmdRequest,
        brane: &Arc<meta_timeshare::Brane>,
        read_index: Option<u64>,
        mut ts: Option<ThreadReadId>,
    ) -> ReadResponse<E::Snapshot> {
        let requests = msg.get_requests();
        let mut response = ReadResponse {
            response: VioletaBftCmdResponse::default(),
            snapshot: None,
            txn_extra_op: TxnExtraOp::Noop,
        };
        let mut responses = Vec::with_capacity(requests.len());
        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Get => match self.get_value(req, brane.as_ref()) {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!(?e;
                            "failed to execute get command";
                            "brane_id" => brane.get_id(),
                        );
                        response.response = cmd_resp::new_error(e);
                        return response;
                    }
                },
                CmdType::Snap => {
                    let snapshot =
                        BraneSnapshot::from_snapshot(self.get_snapshot(ts.take()), brane.clone());
                    response.snapshot = Some(snapshot);
                    Response::default()
                }
                CmdType::ReadIndex => {
                    let mut resp = Response::default();
                    if let Some(read_index) = read_index {
                        let mut res = ReadIndexResponse::default();
                        res.set_read_index(read_index);
                        resp.set_read_index(res);
                    } else {
                        panic!("[brane {}] can not get readindex", brane.get_id());
                    }
                    resp
                }
                CmdType::Prewrite
                | CmdType::Put
                | CmdType::Delete
                | CmdType::DeleteCone
                | CmdType::IngestSst
                | CmdType::Invalid => unreachable!(),
            };
            resp.set_cmd_type(cmd_type);
            responses.push(resp);
        }
        response.response.set_responses(responses.into());
        response
    }
}

/// A read only pushdown_causet of `Peer`.
#[derive(Clone, Debug)]
pub struct Readpushdown_causet {
    brane: Arc<meta_timeshare::Brane>,
    peer_id: u64,
    term: u64,
    applied_index_term: u64,
    leader_lease: Option<RemoteLease>,
    last_valid_ts: Timespec,

    tag: String,
    invalid: Arc<AtomicBool>,
    pub txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
    max_ts_sync_status: Arc<AtomicU64>,
}

impl Readpushdown_causet {
    pub fn from_peer<EK: CausetEngine, ER: VioletaBftEngine>(peer: &Peer<EK, ER>) -> Readpushdown_causet {
        let brane = peer.brane().clone();
        let brane_id = brane.get_id();
        let peer_id = peer.peer.get_id();
        Readpushdown_causet {
            brane: Arc::new(brane),
            peer_id,
            term: peer.term(),
            applied_index_term: peer.get_store().applied_index_term(),
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            tag: format!("[brane {}] {}", brane_id, peer_id),
            invalid: Arc::new(AtomicBool::new(false)),
            txn_extra_op: peer.txn_extra_op.clone(),
            max_ts_sync_status: peer.max_ts_sync_status.clone(),
        }
    }

    pub fn mark_invalid(&self) {
        self.invalid.store(true, Ordering::Release);
    }

    pub fn fresh_valid_ts(&mut self) {
        self.last_valid_ts = monotonic_raw_now();
    }

    pub fn fidelio(&mut self, progress: Progress) {
        self.fresh_valid_ts();
        match progress {
            Progress::Brane(brane) => {
                self.brane = Arc::new(brane);
            }
            Progress::Term(term) => {
                self.term = term;
            }
            Progress::AppliedIndexTerm(applied_index_term) => {
                self.applied_index_term = applied_index_term;
            }
            Progress::LeaderLease(leader_lease) => {
                self.leader_lease = Some(leader_lease);
            }
        }
    }

    fn is_in_leader_lease(&self, ts: Timespec, metrics: &mut ReadMetrics) -> bool {
        if let Some(ref lease) = self.leader_lease {
            let term = lease.term();
            if term == self.term {
                if lease.inspect(Some(ts)) == LeaseState::Valid {
                    return true;
                } else {
                    metrics.rejected_by_lease_expire += 1;
                    debug!("rejected by lease expire"; "tag" => &self.tag);
                }
            } else {
                metrics.rejected_by_term_mismatch += 1;
                debug!("rejected by term mismatch"; "tag" => &self.tag);
            }
        }

        false
    }
}

impl Display for Readpushdown_causet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Readpushdown_causet for brane {}, \
             leader {} at term {}, applied_index_term {}, has lease {}",
            self.brane.get_id(),
            self.peer_id,
            self.term,
            self.applied_index_term,
            self.leader_lease.is_some(),
        )
    }
}

#[derive(Debug)]
pub enum Progress {
    Brane(meta_timeshare::Brane),
    Term(u64),
    AppliedIndexTerm(u64),
    LeaderLease(RemoteLease),
}

impl Progress {
    pub fn brane(brane: meta_timeshare::Brane) -> Progress {
        Progress::Brane(brane)
    }

    pub fn term(term: u64) -> Progress {
        Progress::Term(term)
    }

    pub fn applied_index_term(applied_index_term: u64) -> Progress {
        Progress::AppliedIndexTerm(applied_index_term)
    }

    pub fn leader_lease(lease: RemoteLease) -> Progress {
        Progress::LeaderLease(lease)
    }
}

pub struct LocalReader<C, E>
where
    C: ProposalRouter<E::Snapshot>,
    E: CausetEngine,
{
    store_id: Cell<Option<u64>>,
    store_meta: Arc<Mutex<StoreMeta>>,
    kv_engine: E,
    metrics: ReadMetrics,
    // brane id -> Readpushdown_causet
    pushdown_causets: HashMap<u64, Option<Readpushdown_causet>>,
    snap_cache: Option<Arc<E::Snapshot>>,
    cache_read_id: ThreadReadId,
    // A channel to violetabftstore.
    router: C,
}

impl<C, E> ReadFreeDaemon<E> for LocalReader<C, E>
where
    C: ProposalRouter<E::Snapshot>,
    E: CausetEngine,
{
    fn get_engine(&self) -> &E {
        &self.kv_engine
    }

    fn get_snapshot(&mut self, create_time: Option<ThreadReadId>) -> Arc<E::Snapshot> {
        self.metrics.local_executed_requests += 1;
        if let Some(ts) = create_time {
            if ts == self.cache_read_id {
                if let Some(snap) = self.snap_cache.as_ref() {
                    self.metrics.local_executed_snapshot_cache_hit += 1;
                    return snap.clone();
                }
            }
            let snap = Arc::new(self.kv_engine.snapshot());
            self.cache_read_id = ts;
            self.snap_cache = Some(snap.clone());
            return snap;
        }
        Arc::new(self.kv_engine.snapshot())
    }
}

impl<C, E> LocalReader<C, E>
where
    C: ProposalRouter<E::Snapshot>,
    E: CausetEngine,
{
    pub fn new(kv_engine: E, store_meta: Arc<Mutex<StoreMeta>>, router: C) -> Self {
        let cache_read_id = ThreadReadId::new();
        LocalReader {
            store_meta,
            kv_engine,
            router,
            snap_cache: None,
            cache_read_id,
            store_id: Cell::new(None),
            metrics: Default::default(),
            pushdown_causets: HashMap::default(),
        }
    }

    fn redirect(&mut self, mut cmd: VioletaBftCommand<E::Snapshot>) {
        debug!("localreader redirects command"; "command" => ?cmd);
        let brane_id = cmd.request.get_header().get_brane_id();
        let mut err = error_timeshare::Error::default();
        match self.router.lightlike(cmd) {
            Ok(()) => return,
            Err(TrylightlikeError::Full(c)) => {
                self.metrics.rejected_by_channel_full += 1;
                err.set_message(VIOLETABFTSTORE_IS_BUSY.to_owned());
                err.mut_server_is_busy()
                    .set_reason(VIOLETABFTSTORE_IS_BUSY.to_owned());
                cmd = c;
            }
            Err(TrylightlikeError::Disconnected(c)) => {
                self.metrics.rejected_by_no_brane += 1;
                err.set_message(format!("brane {} is missing", brane_id));
                err.mut_brane_not_found().set_brane_id(brane_id);
                cmd = c;
            }
        }

        let mut resp = VioletaBftCmdResponse::default();
        resp.mut_header().set_error(err);
        let read_resp = ReadResponse {
            response: resp,
            snapshot: None,
            txn_extra_op: TxnExtraOp::Noop,
        };

        cmd.callback.invoke_read(read_resp);
    }

    fn pre_propose_violetabft_command(&mut self, req: &VioletaBftCmdRequest) -> Result<Option<Readpushdown_causet>> {
        // Check store id.
        if self.store_id.get().is_none() {
            let store_id = self.store_meta.dagger().unwrap().store_id;
            self.store_id.set(store_id);
        }
        let store_id = self.store_id.get().unwrap();

        if let Err(e) = util::check_store_id(req, store_id) {
            self.metrics.rejected_by_store_id_mismatch += 1;
            debug!("rejected by store id not match"; "err" => %e);
            return Err(e);
        }

        // Check brane id.
        let brane_id = req.get_header().get_brane_id();
        let pushdown_causet = match self.pushdown_causets.get_mut(&brane_id) {
            Some(pushdown_causet) => match pushdown_causet.take() {
                Some(d) => d,
                None => return Ok(None),
            },
            None => {
                self.metrics.rejected_by_cache_miss += 1;
                debug!("rejected by cache miss"; "brane_id" => brane_id);
                return Ok(None);
            }
        };

        if pushdown_causet.invalid.load(Ordering::Acquire) {
            self.pushdown_causets.remove(&brane_id);
            return Ok(None);
        }

        fail_point!("localreader_on_find_pushdown_causet");

        // Check peer id.
        if let Err(e) = util::check_peer_id(req, pushdown_causet.peer_id) {
            self.metrics.rejected_by_peer_id_mismatch += 1;
            return Err(e);
        }

        // Check term.
        if let Err(e) = util::check_term(req, pushdown_causet.term) {
            debug!(
                "check term";
                "pushdown_causet_term" => pushdown_causet.term,
                "header_term" => req.get_header().get_term(),
            );
            self.metrics.rejected_by_term_mismatch += 1;
            return Err(e);
        }

        // Check brane epoch.
        if util::check_brane_epoch(req, &pushdown_causet.brane, false).is_err() {
            self.metrics.rejected_by_epoch += 1;
            // Stale epoch, redirect it to violetabftstore to get the latest brane.
            debug!("rejected by epoch not match"; "tag" => &pushdown_causet.tag);
            return Ok(None);
        }

        let mut inspector = Inspector {
            pushdown_causet: &pushdown_causet,
            metrics: &mut self.metrics,
        };
        match inspector.inspect(req) {
            Ok(RequestPolicy::ReadLocal) => Ok(Some(pushdown_causet)),
            // It can not handle other policies.
            Ok(_) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn propose_violetabft_command(
        &mut self,
        mut read_id: Option<ThreadReadId>,
        req: VioletaBftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) {
        let brane_id = req.get_header().get_brane_id();
        loop {
            match self.pre_propose_violetabft_command(&req) {
                Ok(Some(pushdown_causet)) => {
                    let snapshot_ts = match read_id.as_mut() {
                        // If this peer became Leader not long ago and just after the cached
                        // snapshot was created, this snapshot can not see all data of the peer.
                        Some(id) => {
                            if id.create_time <= pushdown_causet.last_valid_ts {
                                id.create_time = monotonic_raw_now();
                            }
                            id.create_time
                        }
                        None => monotonic_raw_now(),
                    };
                    if pushdown_causet.is_in_leader_lease(snapshot_ts, &mut self.metrics) {
                        // Cache snapshot_time for remaining requests in the same batch.
                        let mut response = self.execute(&req, &pushdown_causet.brane, None, read_id);
                        // Leader can read local if and only if it is in lease.
                        cmd_resp::bind_term(&mut response.response, pushdown_causet.term);
                        if let Some(snap) = response.snapshot.as_mut() {
                            snap.max_ts_sync_status = Some(pushdown_causet.max_ts_sync_status.clone());
                        }
                        response.txn_extra_op = pushdown_causet.txn_extra_op.load();
                        cb.invoke_read(response);
                        self.pushdown_causets.insert(brane_id, Some(pushdown_causet));
                        return;
                    }
                    break;
                }
                // It can not handle the request, forwards to violetabftstore.
                Ok(None) => {
                    if self.pushdown_causets.get(&brane_id).is_some() {
                        break;
                    }
                    let meta = self.store_meta.dagger().unwrap();
                    match meta.readers.get(&brane_id).cloned() {
                        Some(reader) => {
                            self.pushdown_causets.insert(brane_id, Some(reader));
                        }
                        None => {
                            self.metrics.rejected_by_no_brane += 1;
                            debug!("rejected by no brane"; "brane_id" => brane_id);
                            break;
                        }
                    }
                }
                Err(e) => {
                    let mut response = cmd_resp::new_error(e);
                    if let Some(Some(ref pushdown_causet)) = self.pushdown_causets.get(&brane_id) {
                        cmd_resp::bind_term(&mut response, pushdown_causet.term);
                    }
                    cb.invoke_read(ReadResponse {
                        response,
                        snapshot: None,
                        txn_extra_op: TxnExtraOp::Noop,
                    });
                    self.pushdown_causets.remove(&brane_id);
                    return;
                }
            }
        }
        // Remove pushdown_causet for ufidelating it by next cmd execution.
        self.pushdown_causets.remove(&brane_id);
        // Forward to violetabftstore.
        let cmd = VioletaBftCommand::new(req, cb);
        self.redirect(cmd);
    }

    /// If read requests are received at the same RPC request, we can create one snapshot for all
    /// of them and check whether the time when the snapshot was created is in lease. We use
    /// ThreadReadId to figure out whether this VioletaBftCommand comes from the same RPC request with
    /// the last VioletaBftCommand which left a snapshot cached in LocalReader. ThreadReadId is composed
    /// by thread_id and a thread_local incremental sequence.
    #[inline]
    pub fn read(
        &mut self,
        read_id: Option<ThreadReadId>,
        req: VioletaBftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) {
        self.propose_violetabft_command(read_id, req, cb);
        self.metrics.maybe_flush();
    }

    pub fn release_snapshot_cache(&mut self) {
        self.snap_cache.take();
    }
}

impl<C, E> Clone for LocalReader<C, E>
where
    C: ProposalRouter<E::Snapshot> + Clone,
    E: CausetEngine,
{
    fn clone(&self) -> Self {
        LocalReader {
            store_meta: self.store_meta.clone(),
            kv_engine: self.kv_engine.clone(),
            router: self.router.clone(),
            store_id: self.store_id.clone(),
            metrics: Default::default(),
            pushdown_causets: HashMap::default(),
            snap_cache: self.snap_cache.clone(),
            cache_read_id: self.cache_read_id.clone(),
        }
    }
}

struct Inspector<'r, 'm> {
    pushdown_causet: &'r Readpushdown_causet,
    metrics: &'m mut ReadMetrics,
}

impl<'r, 'm> RequestInspector for Inspector<'r, 'm> {
    fn has_applied_to_current_term(&mut self) -> bool {
        if self.pushdown_causet.applied_index_term == self.pushdown_causet.term {
            true
        } else {
            debug!(
                "rejected by term check";
                "tag" => &self.pushdown_causet.tag,
                "applied_index_term" => self.pushdown_causet.applied_index_term,
                "pushdown_causet_term" => ?self.pushdown_causet.term,
            );

            // only for metric.
            self.metrics.rejected_by_appiled_term += 1;
            false
        }
    }

    fn inspect_lease(&mut self) -> LeaseState {
        // TODO: disable localreader if we did not enable violetabft's check_quorum.
        if self.pushdown_causet.leader_lease.is_some() {
            // We skip lease check, because it is postponed until `handle_read`.
            LeaseState::Valid
        } else {
            debug!("rejected by leader lease"; "tag" => &self.pushdown_causet.tag);
            self.metrics.rejected_by_no_lease += 1;
            LeaseState::Expired
        }
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 15_000; // 15s

#[derive(Clone)]
struct ReadMetrics {
    local_executed_requests: i64,
    local_executed_snapshot_cache_hit: i64,
    // TODO: record rejected_by_read_quorum.
    rejected_by_store_id_mismatch: i64,
    rejected_by_peer_id_mismatch: i64,
    rejected_by_term_mismatch: i64,
    rejected_by_lease_expire: i64,
    rejected_by_no_brane: i64,
    rejected_by_no_lease: i64,
    rejected_by_epoch: i64,
    rejected_by_appiled_term: i64,
    rejected_by_channel_full: i64,
    rejected_by_cache_miss: i64,

    last_flush_time: Instant,
}

impl Default for ReadMetrics {
    fn default() -> ReadMetrics {
        ReadMetrics {
            local_executed_requests: 0,
            local_executed_snapshot_cache_hit: 0,
            rejected_by_store_id_mismatch: 0,
            rejected_by_peer_id_mismatch: 0,
            rejected_by_term_mismatch: 0,
            rejected_by_lease_expire: 0,
            rejected_by_no_brane: 0,
            rejected_by_no_lease: 0,
            rejected_by_epoch: 0,
            rejected_by_appiled_term: 0,
            rejected_by_channel_full: 0,
            rejected_by_cache_miss: 0,
            last_flush_time: Instant::now(),
        }
    }
}

impl ReadMetrics {
    pub fn maybe_flush(&mut self) {
        if self.last_flush_time.elapsed() >= Duration::from_millis(METRICS_FLUSH_INTERVAL) {
            self.flush();
            self.last_flush_time = Instant::now();
        }
    }

    fn flush(&mut self) {
        if self.rejected_by_store_id_mismatch > 0 {
            LOCAL_READ_REJECT
                .store_id_mismatch
                .inc_by(self.rejected_by_store_id_mismatch);
            self.rejected_by_store_id_mismatch = 0;
        }
        if self.rejected_by_peer_id_mismatch > 0 {
            LOCAL_READ_REJECT
                .peer_id_mismatch
                .inc_by(self.rejected_by_peer_id_mismatch);
            self.rejected_by_peer_id_mismatch = 0;
        }
        if self.rejected_by_term_mismatch > 0 {
            LOCAL_READ_REJECT
                .term_mismatch
                .inc_by(self.rejected_by_term_mismatch);
            self.rejected_by_term_mismatch = 0;
        }
        if self.rejected_by_lease_expire > 0 {
            LOCAL_READ_REJECT
                .lease_expire
                .inc_by(self.rejected_by_lease_expire);
            self.rejected_by_lease_expire = 0;
        }
        if self.rejected_by_no_brane > 0 {
            LOCAL_READ_REJECT
                .no_brane
                .inc_by(self.rejected_by_no_brane);
            self.rejected_by_no_brane = 0;
        }
        if self.rejected_by_no_lease > 0 {
            LOCAL_READ_REJECT.no_lease.inc_by(self.rejected_by_no_lease);
            self.rejected_by_no_lease = 0;
        }
        if self.rejected_by_epoch > 0 {
            LOCAL_READ_REJECT.epoch.inc_by(self.rejected_by_epoch);
            self.rejected_by_epoch = 0;
        }
        if self.rejected_by_appiled_term > 0 {
            LOCAL_READ_REJECT
                .appiled_term
                .inc_by(self.rejected_by_appiled_term);
            self.rejected_by_appiled_term = 0;
        }
        if self.rejected_by_channel_full > 0 {
            LOCAL_READ_REJECT
                .channel_full
                .inc_by(self.rejected_by_channel_full);
            self.rejected_by_channel_full = 0;
        }
        if self.local_executed_snapshot_cache_hit > 0 {
            LOCAL_READ_EXECUTED_CACHE_REQUESTS.inc_by(self.local_executed_snapshot_cache_hit);
            self.local_executed_snapshot_cache_hit = 0;
        }
        if self.local_executed_requests > 0 {
            LOCAL_READ_EXECUTED_REQUESTS.inc_by(self.local_executed_requests);
            self.local_executed_requests = 0;
        }
    }
}

#[causet(test)]
mod tests {
    use std::sync::mpsc::*;
    use std::thread;

    use ekvproto::violetabft_cmd_timeshare::*;
    use tempfile::{Builder, TempDir};
    use time::Duration;

    use crate::store::util::Lease;
    use crate::store::Callback;
    use engine_lmdb::{LmdbEngine, LmdbSnapshot};
    use edb::ALL_CausetS;
    use violetabftstore::interlock::::time::monotonic_raw_now;

    use super::*;

    #[allow(clippy::type_complexity)]
    fn new_reader(
        path: &str,
        store_id: u64,
        store_meta: Arc<Mutex<StoreMeta>>,
    ) -> (
        TempDir,
        LocalReader<Synclightlikeer<VioletaBftCommand<LmdbSnapshot>>, LmdbEngine>,
        Receiver<VioletaBftCommand<LmdbSnapshot>>,
    ) {
        let path = Builder::new().prefix(path).temfidelir().unwrap();
        let db = engine_lmdb::util::new_engine(path.path().to_str().unwrap(), None, ALL_CausetS, None)
            .unwrap();
        let (ch, rx) = sync_channel(1);
        let mut reader = LocalReader::new(db, store_meta, ch);
        reader.store_id = Cell::new(Some(store_id));
        (path, reader, rx)
    }

    fn new_peers(store_id: u64, pr_ids: Vec<u64>) -> Vec<meta_timeshare::Peer> {
        pr_ids
            .into_iter()
            .map(|id| {
                let mut pr = meta_timeshare::Peer::default();
                pr.set_store_id(store_id);
                pr.set_id(id);
                pr
            })
            .collect()
    }

    fn must_redirect(
        reader: &mut LocalReader<Synclightlikeer<VioletaBftCommand<LmdbSnapshot>>, LmdbEngine>,
        rx: &Receiver<VioletaBftCommand<LmdbSnapshot>>,
        cmd: VioletaBftCmdRequest,
    ) {
        reader.propose_violetabft_command(
            None,
            cmd.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        );
        assert_eq!(
            rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                .unwrap()
                .request,
            cmd
        );
    }

    fn must_not_redirect(
        reader: &mut LocalReader<Synclightlikeer<VioletaBftCommand<LmdbSnapshot>>, LmdbEngine>,
        rx: &Receiver<VioletaBftCommand<LmdbSnapshot>>,
        task: VioletaBftCommand<LmdbSnapshot>,
    ) {
        reader.propose_violetabft_command(None, task.request, task.callback);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    }

    #[test]
    fn test_read() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, rx) = new_reader("test-local-reader", store_id, store_meta.clone());

        // brane: 1,
        // peers: 2, 3, 4,
        // leader:2,
        // from "" to "",
        // epoch 1, 1,
        // term 6.
        let mut brane1 = meta_timeshare::Brane::default();
        brane1.set_id(1);
        let prs = new_peers(store_id, vec![2, 3, 4]);
        brane1.set_peers(prs.clone().into());
        let epoch13 = {
            let mut ep = meta_timeshare::BraneEpoch::default();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };
        let leader2 = prs[0].clone();
        brane1.set_brane_epoch(epoch13.clone());
        let term6 = 6;
        let mut lease = Lease::new(Duration::seconds(1)); // 1s is long enough.

        let mut cmd = VioletaBftCmdRequest::default();
        let mut header = VioletaBftRequestHeader::default();
        header.set_brane_id(1);
        header.set_peer(leader2.clone());
        header.set_brane_epoch(epoch13.clone());
        header.set_term(term6);
        cmd.set_header(header);
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        // The brane is not register yet.
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_no_brane, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 1);

        // Register brane 1
        lease.renew(monotonic_raw_now());
        let remote = lease.maybe_new_remote_lease(term6).unwrap();
        // But the applied_index_term is stale.
        {
            let mut meta = store_meta.dagger().unwrap();
            let read_pushdown_causet = Readpushdown_causet {
                tag: String::new(),
                brane: Arc::new(brane1.clone()),
                peer_id: leader2.get_id(),
                term: term6,
                applied_index_term: term6 - 1,
                leader_lease: Some(remote),
                last_valid_ts: Timespec::new(0, 0),
                invalid: Arc::new(AtomicBool::new(false)),
                txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
                max_ts_sync_status: Arc::new(AtomicU64::new(0)),
            };
            meta.readers.insert(1, read_pushdown_causet);
        }

        // The applied_index_term is stale
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_cache_miss, 2);
        assert_eq!(reader.metrics.rejected_by_appiled_term, 1);
        assert!(reader.pushdown_causets.get(&1).is_none());

        // Make the applied_index_term matches current term.
        let pg = Progress::applied_index_term(term6);
        {
            let mut meta = store_meta.dagger().unwrap();
            meta.readers.get_mut(&1).unwrap().fidelio(pg);
        }
        let task =
            VioletaBftCommand::<LmdbSnapshot>::new(cmd.clone(), Callback::Read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 3);

        // Let's read.
        let brane = brane1;
        let task = VioletaBftCommand::<LmdbSnapshot>::new(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<LmdbSnapshot>| {
                let snap = resp.snapshot.unwrap();
                assert_eq!(snap.get_brane(), &brane);
            })),
        );
        must_not_redirect(&mut reader, &rx, task);

        // Wait for expiration.
        thread::sleep(Duration::seconds(1).to_std().unwrap());
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_lease_expire, 1);

        // Renew lease.
        lease.renew(monotonic_raw_now());

        // CausetStore id mismatch.
        let mut cmd_store_id = cmd.clone();
        cmd_store_id
            .mut_header()
            .mut_peer()
            .set_store_id(store_id + 1);
        reader.propose_violetabft_command(
            None,
            cmd_store_id,
            Callback::Read(Box::new(move |resp: ReadResponse<LmdbSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_store_not_match());
                assert!(resp.snapshot.is_none());
            })),
        );
        assert_eq!(reader.metrics.rejected_by_store_id_mismatch, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 3);

        // meta_timeshare::Peer id mismatch.
        let mut cmd_peer_id = cmd.clone();
        cmd_peer_id
            .mut_header()
            .mut_peer()
            .set_id(leader2.get_id() + 1);
        reader.propose_violetabft_command(
            None,
            cmd_peer_id,
            Callback::Read(Box::new(move |resp: ReadResponse<LmdbSnapshot>| {
                assert!(
                    resp.response.get_header().has_error(),
                    "{:?}",
                    resp.response
                );
                assert!(resp.snapshot.is_none());
            })),
        );
        assert_eq!(reader.metrics.rejected_by_peer_id_mismatch, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 4);

        // Read quorum.
        let mut cmd_read_quorum = cmd.clone();
        cmd_read_quorum.mut_header().set_read_quorum(true);
        must_redirect(&mut reader, &rx, cmd_read_quorum);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 5);

        // Term mismatch.
        let mut cmd_term = cmd.clone();
        cmd_term.mut_header().set_term(term6 - 2);
        reader.propose_violetabft_command(
            None,
            cmd_term,
            Callback::Read(Box::new(move |resp: ReadResponse<LmdbSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_stale_command(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        );
        assert_eq!(reader.metrics.rejected_by_term_mismatch, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 6);

        // Stale epoch.
        let mut epoch12 = epoch13;
        epoch12.set_version(2);
        let mut cmd_epoch = cmd.clone();
        cmd_epoch.mut_header().set_brane_epoch(epoch12);
        must_redirect(&mut reader, &rx, cmd_epoch);
        assert_eq!(reader.metrics.rejected_by_epoch, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 7);

        // Expire lease manually, and it can not be renewed.
        let previous_lease_rejection = reader.metrics.rejected_by_lease_expire;
        lease.expire();
        lease.renew(monotonic_raw_now());
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(
            reader.metrics.rejected_by_lease_expire,
            previous_lease_rejection + 1
        );
        assert_eq!(reader.metrics.rejected_by_cache_miss, 8);

        // Channel full.
        reader.propose_violetabft_command(None, cmd.clone(), Callback::None);
        reader.propose_violetabft_command(
            None,
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<LmdbSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_server_is_busy(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        );
        rx.try_recv().unwrap();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(reader.metrics.rejected_by_channel_full, 1);

        // Reject by term mismatch in lease.
        let previous_term_rejection = reader.metrics.rejected_by_term_mismatch;
        let mut cmd9 = cmd;
        cmd9.mut_header().set_term(term6 + 3);
        {
            let mut meta = store_meta.dagger().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .fidelio(Progress::term(term6 + 3));
            meta.readers
                .get_mut(&1)
                .unwrap()
                .fidelio(Progress::applied_index_term(term6 + 3));
        }
        reader.propose_violetabft_command(
            None,
            cmd9.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        );
        assert_eq!(
            rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                .unwrap()
                .request,
            cmd9
        );
        assert_eq!(
            reader.metrics.rejected_by_term_mismatch,
            previous_term_rejection + 1,
        );
    }
}
