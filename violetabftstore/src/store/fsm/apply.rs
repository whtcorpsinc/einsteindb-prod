// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::borrow::Cow;
use std::cmp::{Ord, Ordering as CmpOrdering};
use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[causet(test)]
use std::sync::mpsc::lightlikeer;
use std::sync::mpsc::Synclightlikeer;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec::Drain;
use std::{cmp, usize};

use batch_system::{BasicMailbox, BatchRouter, BatchSystem, Fsm, HandlerBuilder, PollHandler};
use crossbeam::channel::{TryRecvError, TrylightlikeError};
use engine_lmdb::{PerfContext, PerfLevel};
use edb::{CausetEngine, VioletaBftEngine, Snapshot, WriteBatch};
use edb::{ALL_CausetS, Causet_DEFAULT, Causet_DAGGER, Causet_VIOLETABFT, Causet_WRITE};
use ekvproto::import_sst_timeshare::SstMeta;
use ekvproto::kvrpc_timeshare::ExtraOp as TxnExtraOp;
use ekvproto::meta_timeshare::{Peer as PeerMeta, PeerRole, Brane, BraneEpoch};
use ekvproto::violetabft_cmd_timeshare::{
    AdminCmdType, AdminRequest, AdminResponse, ChangePeerRequest, CmdType, CommitMergeRequest,
    VioletaBftCmdRequest, VioletaBftCmdResponse, Request, Response,
};
use ekvproto::violetabft_server_timeshare::{
    MergeState, PeerState, VioletaBftApplyState, VioletaBftTruncatedState, BraneLocalState,
};
use violetabft::evioletabft_timeshare::{ConfChange, ConfChangeType, Entry, EntryType, Snapshot as VioletaBftSnapshot};
use sst_importer::SSTImporter;
use violetabftstore::interlock::::collections::{HashMap, HashMapEntry, HashSet};
use violetabftstore::interlock::::config::{Tracker, VersionTrack};
use violetabftstore::interlock::::mpsc::{loose_bounded, LooseBoundedlightlikeer, Receiver};
use violetabftstore::interlock::::time::{duration_to_sec, Instant};
use violetabftstore::interlock::::worker::Interlock_Semaphore;
use violetabftstore::interlock::::{escape, Either, MustConsumeVec};
use time::Timespec;
use uuid::Builder as UuidBuilder;

use crate::interlock::{Cmd, InterlockHost};
use crate::store::fsm::VioletaBftPollerBuilder;
use crate::store::metrics::*;
use crate::store::msg::{Callback, PeerMsg, ReadResponse, SignificantMsg};
use crate::store::peer::Peer;
use crate::store::peer_causet_storage::{
    self, write_initial_apply_state, write_peer_state, ENTRY_MEM_SIZE,
};
use crate::store::util::{
    check_brane_epoch, compare_brane_epoch, is_learner, TuplespaceInstantonInfoFormatter, PerfContextStatistics,
    ADMIN_CMD_EPOCH_MAP,
};
use crate::store::{cmd_resp, util, Config, BraneSnapshot, BraneTask};
use crate::{observe_perf_context_type, report_perf_context, Error, Result};

use super::metrics::*;

const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;
const APPLY_WB_SHRINK_SIZE: usize = 1024 * 1024;
const SHRINK_PENDING_CMD_QUEUE_CAP: usize = 64;

pub struct PlightlikeingCmd<S>
where
    S: Snapshot,
{
    pub index: u64,
    pub term: u64,
    pub cb: Option<Callback<S>>,
}

impl<S> PlightlikeingCmd<S>
where
    S: Snapshot,
{
    fn new(index: u64, term: u64, cb: Callback<S>) -> PlightlikeingCmd<S> {
        PlightlikeingCmd {
            index,
            term,
            cb: Some(cb),
        }
    }
}

impl<S> Drop for PlightlikeingCmd<S>
where
    S: Snapshot,
{
    fn drop(&mut self) {
        if self.cb.is_some() {
            safe_panic!(
                "callback of plightlikeing command at [index: {}, term: {}] is leak",
                self.index,
                self.term
            );
        }
    }
}

impl<S> Debug for PlightlikeingCmd<S>
where
    S: Snapshot,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PlightlikeingCmd [index: {}, term: {}, has_cb: {}]",
            self.index,
            self.term,
            self.cb.is_some()
        )
    }
}

/// Commands waiting to be committed and applied.
#[derive(Debug)]
pub struct PlightlikeingCmdQueue<S>
where
    S: Snapshot,
{
    normals: VecDeque<PlightlikeingCmd<S>>,
    conf_change: Option<PlightlikeingCmd<S>>,
}

impl<S> PlightlikeingCmdQueue<S>
where
    S: Snapshot,
{
    fn new() -> PlightlikeingCmdQueue<S> {
        PlightlikeingCmdQueue {
            normals: VecDeque::new(),
            conf_change: None,
        }
    }

    fn pop_normal(&mut self, index: u64, term: u64) -> Option<PlightlikeingCmd<S>> {
        self.normals.pop_front().and_then(|cmd| {
            if self.normals.capacity() > SHRINK_PENDING_CMD_QUEUE_CAP
                && self.normals.len() < SHRINK_PENDING_CMD_QUEUE_CAP
            {
                self.normals.shrink_to_fit();
            }
            if (cmd.term, cmd.index) > (term, index) {
                self.normals.push_front(cmd);
                return None;
            }
            Some(cmd)
        })
    }

    fn applightlike_normal(&mut self, cmd: PlightlikeingCmd<S>) {
        self.normals.push_back(cmd);
    }

    fn take_conf_change(&mut self) -> Option<PlightlikeingCmd<S>> {
        // conf change will not be affected when changing between follower and leader,
        // so there is no need to check term.
        self.conf_change.take()
    }

    // TODO: seems we don't need to separate conf change from normal entries.
    fn set_conf_change(&mut self, cmd: PlightlikeingCmd<S>) {
        self.conf_change = Some(cmd);
    }
}

#[derive(Default, Debug)]
pub struct ChangePeer {
    pub index: u64,
    pub conf_change: ConfChange,
    pub peer: PeerMeta,
    pub brane: Brane,
}

#[derive(Debug)]
pub struct Cone {
    pub causet: String,
    pub spacelike_key: Vec<u8>,
    pub lightlike_key: Vec<u8>,
}

impl Cone {
    fn new(causet: String, spacelike_key: Vec<u8>, lightlike_key: Vec<u8>) -> Cone {
        Cone {
            causet,
            spacelike_key,
            lightlike_key,
        }
    }
}

#[derive(Debug)]
pub enum ExecResult<S> {
    ChangePeer(ChangePeer),
    CompactLog {
        state: VioletaBftTruncatedState,
        first_index: u64,
    },
    SplitBrane {
        branes: Vec<Brane>,
        derived: Brane,
        new_split_branes: HashMap<u64, NewSplitPeer>,
    },
    PrepareMerge {
        brane: Brane,
        state: MergeState,
    },
    CommitMerge {
        brane: Brane,
        source: Brane,
    },
    RollbackMerge {
        brane: Brane,
        commit: u64,
    },
    ComputeHash {
        brane: Brane,
        index: u64,
        context: Vec<u8>,
        snap: S,
    },
    VerifyHash {
        index: u64,
        context: Vec<u8>,
        hash: Vec<u8>,
    },
    DeleteCone {
        cones: Vec<Cone>,
    },
    IngestSst {
        ssts: Vec<SstMeta>,
    },
}

/// The possible returned value when applying logs.
pub enum ApplyResult<S> {
    None,
    Yield,
    /// Additional result that needs to be sent back to violetabftstore.
    Res(ExecResult<S>),
    /// It is unable to apply the `CommitMerge` until the source peer
    /// has applied to the required position and sets the atomic boolean
    /// to true.
    WaitMergeSource(Arc<AtomicU64>),
}

struct ExecContext {
    apply_state: VioletaBftApplyState,
    index: u64,
    term: u64,
}

impl ExecContext {
    pub fn new(apply_state: VioletaBftApplyState, index: u64, term: u64) -> ExecContext {
        ExecContext {
            apply_state,
            index,
            term,
        }
    }
}

struct ApplyCallback<EK>
where
    EK: CausetEngine,
{
    brane: Brane,
    cbs: Vec<(Option<Callback<EK::Snapshot>>, Cmd)>,
}

impl<EK> ApplyCallback<EK>
where
    EK: CausetEngine,
{
    fn new(brane: Brane) -> Self {
        let cbs = vec![];
        ApplyCallback { brane, cbs }
    }

    fn invoke_all(self, host: &InterlockHost<EK>) {
        for (cb, mut cmd) in self.cbs {
            host.post_apply(&self.brane, &mut cmd);
            if let Some(cb) = cb {
                cb.invoke_with_response(cmd.response)
            };
        }
    }

    fn push(&mut self, cb: Option<Callback<EK::Snapshot>>, cmd: Cmd) {
        self.cbs.push((cb, cmd));
    }
}

pub trait Notifier<EK: CausetEngine>: lightlike {
    fn notify(&self, apply_res: Vec<ApplyRes<EK::Snapshot>>);
    fn notify_one(&self, brane_id: u64, msg: PeerMsg<EK>);
    fn clone_box(&self) -> Box<dyn Notifier<EK>>;
}

struct ApplyContext<EK, W>
where
    EK: CausetEngine,
    W: WriteBatch<EK>,
{
    tag: String,
    timer: Option<Instant>,
    host: InterlockHost<EK>,
    importer: Arc<SSTImporter>,
    brane_interlock_semaphore: Interlock_Semaphore<BraneTask<EK::Snapshot>>,
    router: ApplyRouter<EK>,
    notifier: Box<dyn Notifier<EK>>,
    engine: EK,
    cbs: MustConsumeVec<ApplyCallback<EK>>,
    apply_res: Vec<ApplyRes<EK::Snapshot>>,
    exec_ctx: Option<ExecContext>,

    kv_wb: Option<W>,
    kv_wb_last_bytes: u64,
    kv_wb_last_tuplespaceInstanton: u64,

    last_applied_index: u64,
    committed_count: usize,

    // Whether synchronize WAL is preferred.
    sync_log_hint: bool,
    // Whether to use the delete cone API instead of deleting one by one.
    use_delete_cone: bool,

    perf_context_statistics: PerfContextStatistics,

    yield_duration: Duration,

    store_id: u64,
    /// brane_id -> (peer_id, is_splitting)
    /// Used for handling race between splitting and creating new peer.
    /// An uninitialized peer can be replaced to the one from splitting iff they are exactly the same peer.
    plightlikeing_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
}

impl<EK, W> ApplyContext<EK, W>
where
    EK: CausetEngine,
    W: WriteBatch<EK>,
{
    pub fn new(
        tag: String,
        host: InterlockHost<EK>,
        importer: Arc<SSTImporter>,
        brane_interlock_semaphore: Interlock_Semaphore<BraneTask<EK::Snapshot>>,
        engine: EK,
        router: ApplyRouter<EK>,
        notifier: Box<dyn Notifier<EK>>,
        causet: &Config,
        store_id: u64,
        plightlikeing_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
    ) -> ApplyContext<EK, W> {
        ApplyContext {
            tag,
            timer: None,
            host,
            importer,
            brane_interlock_semaphore,
            engine,
            router,
            notifier,
            kv_wb: None,
            cbs: MustConsumeVec::new("callback of apply context"),
            apply_res: vec![],
            kv_wb_last_bytes: 0,
            kv_wb_last_tuplespaceInstanton: 0,
            last_applied_index: 0,
            committed_count: 0,
            sync_log_hint: false,
            exec_ctx: None,
            use_delete_cone: causet.use_delete_cone,
            perf_context_statistics: PerfContextStatistics::new(causet.perf_level),
            yield_duration: causet.apply_yield_duration.0,
            store_id,
            plightlikeing_create_peers,
        }
    }

    /// Prepares for applying entries for `pushdown_causet`.
    ///
    /// A general apply progress for a pushdown_causet is:
    /// `prepare_for` -> `commit` [-> `commit` ...] -> `finish_for`.
    /// After all pushdown_causets are handled, `write_to_db` method should be called.
    pub fn prepare_for(&mut self, pushdown_causet: &mut Applypushdown_causet<EK>) {
        self.prepare_write_batch();
        self.cbs.push(ApplyCallback::new(pushdown_causet.brane.clone()));
        self.last_applied_index = pushdown_causet.apply_state.get_applied_index();

        if let Some(observe_cmd) = &pushdown_causet.observe_cmd {
            let brane_id = pushdown_causet.brane_id();
            if observe_cmd.enabled.load(Ordering::Acquire) {
                self.host.prepare_for_apply(observe_cmd.id, brane_id);
            } else {
                info!("brane is no longer semaphored";
                    "brane_id" => brane_id);
                pushdown_causet.observe_cmd.take();
            }
        }
    }

    /// Prepares WriteBatch.
    ///
    /// If `enable_multi_batch_write` was set true, we create `LmdbWriteBatchVec`.
    /// Otherwise create `LmdbWriteBatch`.
    pub fn prepare_write_batch(&mut self) {
        if self.kv_wb.is_none() {
            let kv_wb = W::with_capacity(&self.engine, DEFAULT_APPLY_WB_SIZE);
            self.kv_wb = Some(kv_wb);
            self.kv_wb_last_bytes = 0;
            self.kv_wb_last_tuplespaceInstanton = 0;
        }
    }

    /// Commits all changes have done for pushdown_causet. `persistent` indicates whether
    /// write the changes into lmdb.
    ///
    /// This call is valid only when it's between a `prepare_for` and `finish_for`.
    pub fn commit(&mut self, pushdown_causet: &mut Applypushdown_causet<EK>) {
        if self.last_applied_index < pushdown_causet.apply_state.get_applied_index() {
            pushdown_causet.write_apply_state(self.kv_wb.as_mut().unwrap());
        }
        // last_applied_index doesn't need to be fideliod, set persistent to true will
        // force it call `prepare_for` automatically.
        self.commit_opt(pushdown_causet, true);
    }

    fn commit_opt(&mut self, pushdown_causet: &mut Applypushdown_causet<EK>, persistent: bool) {
        pushdown_causet.fidelio_metrics(self);
        if persistent {
            self.write_to_db();
            self.prepare_for(pushdown_causet);
        }
        self.kv_wb_last_bytes = self.kv_wb().data_size() as u64;
        self.kv_wb_last_tuplespaceInstanton = self.kv_wb().count() as u64;
    }

    /// Writes all the changes into Lmdb.
    /// If it returns true, all plightlikeing writes are persisted in engines.
    pub fn write_to_db(&mut self) -> bool {
        let need_sync = self.sync_log_hint;
        if self.kv_wb.as_ref().map_or(false, |wb| !wb.is_empty()) {
            let mut write_opts = edb::WriteOptions::new();
            write_opts.set_sync(need_sync);
            self.kv_wb()
                .write_to_engine(&self.engine, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("failed to write to engine: {:?}", e);
                });
            report_perf_context!(
                self.perf_context_statistics,
                APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC
            );
            self.sync_log_hint = false;
            let data_size = self.kv_wb().data_size();
            if data_size > APPLY_WB_SHRINK_SIZE {
                // Control the memory usage for the WriteBatch.
                let kv_wb = W::with_capacity(&self.engine, DEFAULT_APPLY_WB_SIZE);
                self.kv_wb = Some(kv_wb);
            } else {
                // Clear data, reuse the WriteBatch, this can reduce memory allocations and deallocations.
                self.kv_wb_mut().clear();
            }
            self.kv_wb_last_bytes = 0;
            self.kv_wb_last_tuplespaceInstanton = 0;
        }
        // Call it before invoking callback for preventing Commit is executed before Prewrite is observed.
        self.host.on_flush_apply(self.engine.clone());

        for cbs in self.cbs.drain(..) {
            cbs.invoke_all(&self.host);
        }
        need_sync
    }

    /// Finishes `Apply`s for the pushdown_causet.
    pub fn finish_for(
        &mut self,
        pushdown_causet: &mut Applypushdown_causet<EK>,
        results: VecDeque<ExecResult<EK::Snapshot>>,
    ) {
        if !pushdown_causet.plightlikeing_remove {
            pushdown_causet.write_apply_state(self.kv_wb.as_mut().unwrap());
        }
        self.commit_opt(pushdown_causet, false);
        self.apply_res.push(ApplyRes {
            brane_id: pushdown_causet.brane_id(),
            apply_state: pushdown_causet.apply_state.clone(),
            exec_res: results,
            metrics: pushdown_causet.metrics.clone(),
            applied_index_term: pushdown_causet.applied_index_term,
        });
    }

    pub fn delta_bytes(&self) -> u64 {
        self.kv_wb().data_size() as u64 - self.kv_wb_last_bytes
    }

    pub fn delta_tuplespaceInstanton(&self) -> u64 {
        self.kv_wb().count() as u64 - self.kv_wb_last_tuplespaceInstanton
    }

    #[inline]
    pub fn kv_wb(&self) -> &W {
        self.kv_wb.as_ref().unwrap()
    }

    #[inline]
    pub fn kv_wb_mut(&mut self) -> &mut W {
        self.kv_wb.as_mut().unwrap()
    }

    /// Flush all plightlikeing writes to engines.
    /// If it returns true, all plightlikeing writes are persisted in engines.
    pub fn flush(&mut self) -> bool {
        // TODO: this check is too hacky, need to be more verbose and less buggy.
        let t = match self.timer.take() {
            Some(t) => t,
            None => return false,
        };

        // Write to engine
        // violetabftstore.sync-log = true means we need prevent data loss when power failure.
        // take violetabft log gc for example, we write kv WAL first, then write violetabft WAL,
        // if power failure happen, violetabft WAL may synced to disk, but kv WAL may not.
        // so we use sync-log flag here.
        let is_synced = self.write_to_db();

        if !self.apply_res.is_empty() {
            let apply_res = std::mem::replace(&mut self.apply_res, vec![]);
            self.notifier.notify(apply_res);
        }

        let elapsed = t.elapsed();
        STORE_APPLY_LOG_HISTOGRAM.observe(duration_to_sec(elapsed) as f64);

        slow_log!(
            elapsed,
            "{} handle ready {} committed entries",
            self.tag,
            self.committed_count
        );
        self.committed_count = 0;
        is_synced
    }
}

/// Calls the callback of `cmd` when the Brane is removed.
fn notify_brane_removed(brane_id: u64, peer_id: u64, mut cmd: PlightlikeingCmd<impl Snapshot>) {
    debug!(
        "brane is removed, notify commands";
        "brane_id" => brane_id,
        "peer_id" => peer_id,
        "index" => cmd.index,
        "term" => cmd.term
    );
    notify_req_brane_removed(brane_id, cmd.cb.take().unwrap());
}

pub fn notify_req_brane_removed(brane_id: u64, cb: Callback<impl Snapshot>) {
    let brane_not_found = Error::BraneNotFound(brane_id);
    let resp = cmd_resp::new_error(brane_not_found);
    cb.invoke_with_response(resp);
}

/// Calls the callback of `cmd` when it can not be processed further.
fn notify_stale_command(
    brane_id: u64,
    peer_id: u64,
    term: u64,
    mut cmd: PlightlikeingCmd<impl Snapshot>,
) {
    info!(
        "command is stale, skip";
        "brane_id" => brane_id,
        "peer_id" => peer_id,
        "index" => cmd.index,
        "term" => cmd.term
    );
    notify_stale_req(term, cmd.cb.take().unwrap());
}

pub fn notify_stale_req(term: u64, cb: Callback<impl Snapshot>) {
    let resp = cmd_resp::err_resp(Error::StaleCommand, term);
    cb.invoke_with_response(resp);
}

/// Checks if a write is needed to be issued before handling the command.
fn should_write_to_engine(cmd: &VioletaBftCmdRequest) -> bool {
    if cmd.has_admin_request() {
        match cmd.get_admin_request().get_cmd_type() {
            // ComputeHash require an up to date snapshot.
            AdminCmdType::ComputeHash |
            // Merge needs to get the latest apply index.
            AdminCmdType::CommitMerge |
            AdminCmdType::RollbackMerge => return true,
            _ => {}
        }
    }

    // Some commands may modify tuplespaceInstanton covered by the current write batch, so we
    // must write the current write batch to the engine first.
    for req in cmd.get_requests() {
        if req.has_delete_cone() {
            return true;
        }
        if req.has_ingest_sst() {
            return true;
        }
    }

    false
}

/// Checks if a write is needed to be issued after handling the command.
fn should_sync_log(cmd: &VioletaBftCmdRequest) -> bool {
    if cmd.has_admin_request() {
        if cmd.get_admin_request().get_cmd_type() == AdminCmdType::CompactLog {
            // We do not need to sync WAL before compact log, because this request will lightlike a msg to
            // violetabft_gc_log thread to delete the entries before this index instead of deleting them in
            // apply thread directly.
            return false;
        }
        return true;
    }

    for req in cmd.get_requests() {
        // After ingest sst, sst files are deleted quickly. As a result,
        // ingest sst command can not be handled again and must be synced.
        // See more in Cleanup worker.
        if req.has_ingest_sst() {
            return true;
        }
    }

    false
}

/// A struct that stores the state related to Merge.
///
/// When executing a `CommitMerge`, the source peer may have not applied
/// to the required index, so the target peer has to abort current execution
/// and wait for it asynchronously.
///
/// When rolling the stack, all states required to recover are stored in
/// this struct.
/// TODO: check whether generator/coroutine is a good choice in this case.
struct WaitSourceMergeState {
    /// A flag that indicates whether the source peer has applied to the required
    /// index. If the source peer is ready, this flag should be set to the brane id
    /// of source peer.
    logs_up_to_date: Arc<AtomicU64>,
}

struct YieldState<EK>
where
    EK: CausetEngine,
{
    /// All of the entries that need to continue to be applied after
    /// the source peer has applied its logs.
    plightlikeing_entries: Vec<Entry>,
    /// All of messages that need to continue to be handled after
    /// the source peer has applied its logs and plightlikeing entries
    /// are all handled.
    plightlikeing_msgs: Vec<Msg<EK>>,
}

impl<EK> Debug for YieldState<EK>
where
    EK: CausetEngine,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("YieldState")
            .field("plightlikeing_entries", &self.plightlikeing_entries.len())
            .field("plightlikeing_msgs", &self.plightlikeing_msgs.len())
            .finish()
    }
}

impl Debug for WaitSourceMergeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitSourceMergeState")
            .field("logs_up_to_date", &self.logs_up_to_date)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct NewSplitPeer {
    pub peer_id: u64,
    // `None` => success,
    // `Some(s)` => fail due to `s`.
    pub result: Option<String>,
}

/// The apply pushdown_causet of a Brane which is responsible for handling committed
/// violetabft log entries of a Brane.
///
/// `Apply` is a term of VioletaBft, which means executing the actual commands.
/// In VioletaBft, once some log entries are committed, for every peer of the VioletaBft
/// group will apply the logs one by one. For write commands, it does write or
/// delete to local engine; for admin commands, it does some meta change of the
/// VioletaBft group.
///
/// `pushdown_causet` is just a structure to congregate all apply related fields of a
/// Brane. The apply worker receives all the apply tasks of different Branes
/// located at this store, and it will get the corresponding apply pushdown_causet to
/// handle the apply task to make the code logic more clear.
#[derive(Debug)]
pub struct Applypushdown_causet<EK>
where
    EK: CausetEngine,
{
    /// The ID of the peer.
    id: u64,
    /// The term of the Brane.
    term: u64,
    /// The Brane information of the peer.
    brane: Brane,
    /// Peer_tag, "[brane brane_id] peer_id".
    tag: String,

    /// If the pushdown_causet should be stopped from polling.
    /// A pushdown_causet can be stopped in conf change, merge or requested by destroy message.
    stopped: bool,
    /// The spacelike time of the current round to execute commands.
    handle_spacelike: Option<Instant>,
    /// Set to true when removing itself because of `ConfChangeType::RemoveNode`, and then
    /// any following committed logs in same Ready should be applied failed.
    plightlikeing_remove: bool,

    /// The commands waiting to be committed and applied
    plightlikeing_cmds: PlightlikeingCmdQueue<EK::Snapshot>,
    /// The counter of plightlikeing request snapshots. See more in `Peer`.
    plightlikeing_request_snapshot_count: Arc<AtomicUsize>,

    /// Indicates the peer is in merging, if that compact log won't be performed.
    is_merging: bool,
    /// Records the epoch version after the last merge.
    last_merge_version: u64,
    yield_state: Option<YieldState<EK>>,
    /// A temporary state that keeps track of the progress of the source peer state when
    /// CommitMerge is unable to be executed.
    wait_merge_state: Option<WaitSourceMergeState>,
    // ID of last brane that reports ready.
    ready_source_brane_id: u64,

    /// EinsteinDB writes apply_state to KV Lmdb, in one write batch together with kv data.
    ///
    /// If we write it to VioletaBft Lmdb, apply_state and kv data (Put, Delete) are in
    /// separate WAL file. When power failure, for current violetabft log, apply_index may synced
    /// to file, but KV data may not synced to file, so we will lose data.
    apply_state: VioletaBftApplyState,
    /// The term of the violetabft log at applied index.
    applied_index_term: u64,
    /// The latest synced apply index.
    last_sync_apply_index: u64,

    /// Info about cmd semaphore.
    observe_cmd: Option<ObserveCmd>,

    /// The local metrics, and it will be flushed periodically.
    metrics: ApplyMetrics,
}

impl<EK> Applypushdown_causet<EK>
where
    EK: CausetEngine,
{
    fn from_registration(reg: Registration) -> Applypushdown_causet<EK> {
        Applypushdown_causet {
            id: reg.id,
            tag: format!("[brane {}] {}", reg.brane.get_id(), reg.id),
            brane: reg.brane,
            plightlikeing_remove: false,
            last_sync_apply_index: reg.apply_state.get_applied_index(),
            apply_state: reg.apply_state,
            applied_index_term: reg.applied_index_term,
            term: reg.term,
            stopped: false,
            handle_spacelike: None,
            ready_source_brane_id: 0,
            yield_state: None,
            wait_merge_state: None,
            is_merging: reg.is_merging,
            plightlikeing_cmds: PlightlikeingCmdQueue::new(),
            metrics: Default::default(),
            last_merge_version: 0,
            plightlikeing_request_snapshot_count: reg.plightlikeing_request_snapshot_count,
            observe_cmd: None,
        }
    }

    pub fn brane_id(&self) -> u64 {
        self.brane.get_id()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Handles all the committed_entries, namely, applies the committed entries.
    fn handle_violetabft_committed_entries<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        mut committed_entries_drainer: Drain<Entry>,
    ) {
        if committed_entries_drainer.len() == 0 {
            return;
        }
        apply_ctx.prepare_for(self);
        // If we lightlike multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        apply_ctx.committed_count += committed_entries_drainer.len();
        let mut results = VecDeque::new();
        while let Some(entry) = committed_entries_drainer.next() {
            if self.plightlikeing_remove {
                // This peer is about to be destroyed, skip everything.
                break;
            }

            let expect_index = self.apply_state.get_applied_index() + 1;
            if expect_index != entry.get_index() {
                panic!(
                    "{} expect index {}, but got {}",
                    self.tag,
                    expect_index,
                    entry.get_index()
                );
            }

            let res = match entry.get_entry_type() {
                EntryType::EntryNormal => self.handle_violetabft_entry_normal(apply_ctx, &entry),
                EntryType::EntryConfChange => self.handle_violetabft_entry_conf_change(apply_ctx, &entry),
                EntryType::EntryConfChangeV2 => unimplemented!(),
            };

            match res {
                ApplyResult::None => {}
                ApplyResult::Res(res) => results.push_back(res),
                ApplyResult::Yield | ApplyResult::WaitMergeSource(_) => {
                    // Both cancel and merge will yield current processing.
                    apply_ctx.committed_count -= committed_entries_drainer.len() + 1;
                    let mut plightlikeing_entries =
                        Vec::with_capacity(committed_entries_drainer.len() + 1);
                    // Note that current entry is skipped when yield.
                    plightlikeing_entries.push(entry);
                    plightlikeing_entries.extlightlike(committed_entries_drainer);
                    apply_ctx.finish_for(self, results);
                    self.yield_state = Some(YieldState {
                        plightlikeing_entries,
                        plightlikeing_msgs: Vec::default(),
                    });
                    if let ApplyResult::WaitMergeSource(logs_up_to_date) = res {
                        self.wait_merge_state = Some(WaitSourceMergeState { logs_up_to_date });
                    }
                    return;
                }
            }
        }

        apply_ctx.finish_for(self, results);

        if self.plightlikeing_remove {
            self.destroy(apply_ctx);
        }
    }

    fn fidelio_metrics<W: WriteBatch<EK>>(&mut self, apply_ctx: &ApplyContext<EK, W>) {
        self.metrics.written_bytes += apply_ctx.delta_bytes();
        self.metrics.written_tuplespaceInstanton += apply_ctx.delta_tuplespaceInstanton();
    }

    fn write_apply_state<W: WriteBatch<EK>>(&self, wb: &mut W) {
        wb.put_msg_causet(
            Causet_VIOLETABFT,
            &tuplespaceInstanton::apply_state_key(self.brane.get_id()),
            &self.apply_state,
        )
        .unwrap_or_else(|e| {
            panic!(
                "{} failed to save apply state to write batch, error: {:?}",
                self.tag, e
            );
        });
    }

    fn handle_violetabft_entry_normal<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        entry: &Entry,
    ) -> ApplyResult<EK::Snapshot> {
        fail_point!("yield_apply_1000", self.brane_id() == 1000, |_| {
            ApplyResult::Yield
        });

        let index = entry.get_index();
        let term = entry.get_term();
        let data = entry.get_data();

        if !data.is_empty() {
            let cmd = util::parse_data_at(data, index, &self.tag);

            if should_write_to_engine(&cmd) || apply_ctx.kv_wb().should_write_to_engine() {
                apply_ctx.commit(self);
                if let Some(spacelike) = self.handle_spacelike.as_ref() {
                    if spacelike.elapsed() >= apply_ctx.yield_duration {
                        return ApplyResult::Yield;
                    }
                }
            }

            return self.process_violetabft_cmd(apply_ctx, index, term, cmd);
        }
        // TOOD(causet_context): should we observe empty cmd, aka leader change?

        self.apply_state.set_applied_index(index);
        self.applied_index_term = term;
        assert!(term > 0);

        // 1. When a peer become leader, it will lightlike an empty entry.
        // 2. When a leader tries to read index during transferring leader,
        //    it will also propose an empty entry. But that entry will not contain
        //    any associated callback. So no need to clear callback.
        while let Some(mut cmd) = self.plightlikeing_cmds.pop_normal(std::u64::MAX, term - 1) {
            apply_ctx.cbs.last_mut().unwrap().push(
                cmd.cb.take(),
                Cmd::new(
                    cmd.index,
                    VioletaBftCmdRequest::default(),
                    cmd_resp::err_resp(Error::StaleCommand, term),
                ),
            );
        }
        ApplyResult::None
    }

    fn handle_violetabft_entry_conf_change<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        entry: &Entry,
    ) -> ApplyResult<EK::Snapshot> {
        // Although conf change can't yield in normal case, it is convenient to
        // simulate yield before applying a conf change log.
        fail_point!("yield_apply_conf_change_3", self.id() == 3, |_| {
            ApplyResult::Yield
        });
        let index = entry.get_index();
        let term = entry.get_term();
        let conf_change: ConfChange = util::parse_data_at(entry.get_data(), index, &self.tag);
        let cmd = util::parse_data_at(conf_change.get_context(), index, &self.tag);
        match self.process_violetabft_cmd(apply_ctx, index, term, cmd) {
            ApplyResult::None => {
                // If failed, tell VioletaBft that the `ConfChange` was aborted.
                ApplyResult::Res(ExecResult::ChangePeer(Default::default()))
            }
            ApplyResult::Res(mut res) => {
                if let ExecResult::ChangePeer(ref mut cp) = res {
                    cp.conf_change = conf_change;
                } else {
                    panic!(
                        "{} unexpected result {:?} for conf change {:?} at {}",
                        self.tag, res, conf_change, index
                    );
                }
                ApplyResult::Res(res)
            }
            ApplyResult::Yield | ApplyResult::WaitMergeSource(_) => unreachable!(),
        }
    }

    fn find_plightlikeing(
        &mut self,
        index: u64,
        term: u64,
        is_conf_change: bool,
    ) -> Option<Callback<EK::Snapshot>> {
        let (brane_id, peer_id) = (self.brane_id(), self.id());
        if is_conf_change {
            if let Some(mut cmd) = self.plightlikeing_cmds.take_conf_change() {
                if cmd.index == index && cmd.term == term {
                    return Some(cmd.cb.take().unwrap());
                } else {
                    notify_stale_command(brane_id, peer_id, self.term, cmd);
                }
            }
            return None;
        }
        while let Some(mut head) = self.plightlikeing_cmds.pop_normal(index, term) {
            if head.term == term {
                if head.index == index {
                    return Some(head.cb.take().unwrap());
                } else {
                    panic!(
                        "{} unexpected callback at term {}, found index {}, expected {}",
                        self.tag, term, head.index, index
                    );
                }
            } else {
                // Because of the lack of original VioletaBftCmdRequest, we skip calling
                // interlock here.
                notify_stale_command(brane_id, peer_id, self.term, head);
            }
        }
        None
    }

    fn process_violetabft_cmd<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        index: u64,
        term: u64,
        cmd: VioletaBftCmdRequest,
    ) -> ApplyResult<EK::Snapshot> {
        if index == 0 {
            panic!(
                "{} processing violetabft command needs a none zero index",
                self.tag
            );
        }

        // Set sync log hint if the cmd requires so.
        apply_ctx.sync_log_hint |= should_sync_log(&cmd);

        let is_conf_change = get_change_peer_cmd(&cmd).is_some();
        apply_ctx.host.pre_apply(&self.brane, &cmd);
        let (mut resp, exec_result) = self.apply_violetabft_cmd(apply_ctx, index, term, &cmd);
        if let ApplyResult::WaitMergeSource(_) = exec_result {
            return exec_result;
        }

        debug!(
            "applied command";
            "brane_id" => self.brane_id(),
            "peer_id" => self.id(),
            "index" => index
        );

        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        cmd_resp::bind_term(&mut resp, self.term);
        let cmd = Cmd::new(index, cmd, resp);
        let cmd_cb = self.find_plightlikeing(index, term, is_conf_change);
        if let Some(observe_cmd) = self.observe_cmd.as_ref() {
            apply_ctx
                .host
                .on_apply_cmd(observe_cmd.id, self.brane_id(), cmd.clone());
        }

        apply_ctx.cbs.last_mut().unwrap().push(cmd_cb, cmd);

        exec_result
    }

    /// Applies violetabft command.
    ///
    /// An apply operation can fail in the following situations:
    ///   1. it encounters an error that will occur on all stores, it can continue
    /// applying next entry safely, like epoch not match for example;
    ///   2. it encounters an error that may not occur on all stores, in this case
    /// we should try to apply the entry again or panic. Considering that this
    /// usually due to disk operation fail, which is rare, so just panic is ok.
    fn apply_violetabft_cmd<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        index: u64,
        term: u64,
        req: &VioletaBftCmdRequest,
    ) -> (VioletaBftCmdResponse, ApplyResult<EK::Snapshot>) {
        // if plightlikeing remove, apply should be aborted already.
        assert!(!self.plightlikeing_remove);

        ctx.exec_ctx = Some(self.new_ctx(index, term));
        ctx.kv_wb_mut().set_save_point();
        let mut origin_epoch = None;
        let (resp, exec_result) = match self.exec_violetabft_cmd(ctx, &req) {
            Ok(a) => {
                ctx.kv_wb_mut().pop_save_point().unwrap();
                if req.has_admin_request() {
                    origin_epoch = Some(self.brane.get_brane_epoch().clone());
                }
                a
            }
            Err(e) => {
                // clear dirty values.
                ctx.kv_wb_mut().rollback_to_save_point().unwrap();
                match e {
                    Error::EpochNotMatch(..) => debug!(
                        "epoch not match";
                        "brane_id" => self.brane_id(),
                        "peer_id" => self.id(),
                        "err" => ?e
                    ),
                    _ => error!(?e;
                        "execute violetabft command";
                        "brane_id" => self.brane_id(),
                        "peer_id" => self.id(),
                    ),
                }
                (cmd_resp::new_error(e), ApplyResult::None)
            }
        };
        if let ApplyResult::WaitMergeSource(_) = exec_result {
            return (resp, exec_result);
        }

        let mut exec_ctx = ctx.exec_ctx.take().unwrap();
        exec_ctx.apply_state.set_applied_index(index);

        self.apply_state = exec_ctx.apply_state;
        self.applied_index_term = term;

        if let ApplyResult::Res(ref exec_result) = exec_result {
            match *exec_result {
                ExecResult::ChangePeer(ref cp) => {
                    self.brane = cp.brane.clone();
                }
                ExecResult::ComputeHash { .. }
                | ExecResult::VerifyHash { .. }
                | ExecResult::CompactLog { .. }
                | ExecResult::DeleteCone { .. }
                | ExecResult::IngestSst { .. } => {}
                ExecResult::SplitBrane { ref derived, .. } => {
                    self.brane = derived.clone();
                    self.metrics.size_diff_hint = 0;
                    self.metrics.delete_tuplespaceInstanton_hint = 0;
                }
                ExecResult::PrepareMerge { ref brane, .. } => {
                    self.brane = brane.clone();
                    self.is_merging = true;
                }
                ExecResult::CommitMerge { ref brane, .. } => {
                    self.brane = brane.clone();
                    self.last_merge_version = brane.get_brane_epoch().get_version();
                }
                ExecResult::RollbackMerge { ref brane, .. } => {
                    self.brane = brane.clone();
                    self.is_merging = false;
                }
            }
        }
        if let Some(epoch) = origin_epoch {
            let cmd_type = req.get_admin_request().get_cmd_type();
            let epoch_state = *ADMIN_CMD_EPOCH_MAP.get(&cmd_type).unwrap();
            // The chenge-epoch behavior **MUST BE** equal to the settings in `ADMIN_CMD_EPOCH_MAP`
            if (epoch_state.change_ver
                && epoch.get_version() == self.brane.get_brane_epoch().get_version())
                || (epoch_state.change_conf_ver
                    && epoch.get_conf_ver() == self.brane.get_brane_epoch().get_conf_ver())
            {
                panic!("{} apply admin cmd {:?} but epoch change is not expected, epoch state {:?}, before {:?}, after {:?}",
                        self.tag, req, epoch_state, epoch, self.brane.get_brane_epoch());
            }
        }

        (resp, exec_result)
    }

    fn destroy<W: WriteBatch<EK>>(&mut self, apply_ctx: &mut ApplyContext<EK, W>) {
        self.stopped = true;
        apply_ctx.router.close(self.brane_id());
        for cmd in self.plightlikeing_cmds.normals.drain(..) {
            notify_brane_removed(self.brane.get_id(), self.id, cmd);
        }
        if let Some(cmd) = self.plightlikeing_cmds.conf_change.take() {
            notify_brane_removed(self.brane.get_id(), self.id, cmd);
        }
    }

    fn clear_all_commands_as_stale(&mut self) {
        let (brane_id, peer_id) = (self.brane_id(), self.id());
        for cmd in self.plightlikeing_cmds.normals.drain(..) {
            notify_stale_command(brane_id, peer_id, self.term, cmd);
        }
        if let Some(cmd) = self.plightlikeing_cmds.conf_change.take() {
            notify_stale_command(brane_id, peer_id, self.term, cmd);
        }
    }

    fn new_ctx(&self, index: u64, term: u64) -> ExecContext {
        ExecContext::new(self.apply_state.clone(), index, term)
    }
}

impl<EK> Applypushdown_causet<EK>
where
    EK: CausetEngine,
{
    // Only errors that will also occur on all other stores should be returned.
    fn exec_violetabft_cmd<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &VioletaBftCmdRequest,
    ) -> Result<(VioletaBftCmdResponse, ApplyResult<EK::Snapshot>)> {
        // Include brane for epoch not match after merge may cause key not in cone.
        let include_brane =
            req.get_header().get_brane_epoch().get_version() >= self.last_merge_version;
        check_brane_epoch(req, &self.brane, include_brane)?;
        if req.has_admin_request() {
            self.exec_admin_cmd(ctx, req)
        } else {
            self.exec_write_cmd(ctx, req)
        }
    }

    fn exec_admin_cmd<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &VioletaBftCmdRequest,
    ) -> Result<(VioletaBftCmdResponse, ApplyResult<EK::Snapshot>)> {
        let request = req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        if cmd_type != AdminCmdType::CompactLog && cmd_type != AdminCmdType::CommitMerge {
            info!(
                "execute admin command";
                "brane_id" => self.brane_id(),
                "peer_id" => self.id(),
                "term" => ctx.exec_ctx.as_ref().unwrap().term,
                "index" => ctx.exec_ctx.as_ref().unwrap().index,
                "command" => ?request
            );
        }

        let (mut response, exec_result) = match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::ChangePeerV2 => panic!("unsupported admin command type"),
            AdminCmdType::Split => self.exec_split(ctx, request),
            AdminCmdType::BatchSplit => self.exec_batch_split(ctx, request),
            AdminCmdType::CompactLog => self.exec_compact_log(ctx, request),
            AdminCmdType::TransferLeader => Err(box_err!("transfer leader won't exec")),
            AdminCmdType::ComputeHash => self.exec_compute_hash(ctx, request),
            AdminCmdType::VerifyHash => self.exec_verify_hash(ctx, request),
            // TODO: is it backward compatible to add new cmd_type?
            AdminCmdType::PrepareMerge => self.exec_prepare_merge(ctx, request),
            AdminCmdType::CommitMerge => self.exec_commit_merge(ctx, request),
            AdminCmdType::RollbackMerge => self.exec_rollback_merge(ctx, request),
            AdminCmdType::InvalidAdmin => Err(box_err!("unsupported admin command type")),
        }?;
        response.set_cmd_type(cmd_type);

        let mut resp = VioletaBftCmdResponse::default();
        if !req.get_header().get_uuid().is_empty() {
            let uuid = req.get_header().get_uuid().to_vec();
            resp.mut_header().set_uuid(uuid);
        }
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    fn exec_write_cmd<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &VioletaBftCmdRequest,
    ) -> Result<(VioletaBftCmdResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!(
            "on_apply_write_cmd",
            causet!(release) || self.id() == 3,
            |_| {
                unimplemented!();
            }
        );

        let requests = req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());

        let mut cones = vec![];
        let mut ssts = vec![];
        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Put => self.handle_put(ctx.kv_wb_mut(), req),
                CmdType::Delete => self.handle_delete(ctx.kv_wb_mut(), req),
                CmdType::DeleteCone => {
                    self.handle_delete_cone(&ctx.engine, req, &mut cones, ctx.use_delete_cone)
                }
                CmdType::IngestSst => {
                    self.handle_ingest_sst(&ctx.importer, &ctx.engine, req, &mut ssts)
                }
                // Readonly commands are handled in violetabftstore directly.
                // Don't panic here in case there are old entries need to be applied.
                // It's also safe to skip them here, because a respacelike must have happened,
                // hence there is no callback to be called.
                CmdType::Snap | CmdType::Get => {
                    warn!(
                        "skip readonly command";
                        "brane_id" => self.brane_id(),
                        "peer_id" => self.id(),
                        "command" => ?req
                    );
                    continue;
                }
                CmdType::Prewrite | CmdType::Invalid | CmdType::ReadIndex => {
                    Err(box_err!("invalid cmd type, message maybe corrupted"))
                }
            }?;

            resp.set_cmd_type(cmd_type);

            responses.push(resp);
        }

        let mut resp = VioletaBftCmdResponse::default();
        if !req.get_header().get_uuid().is_empty() {
            let uuid = req.get_header().get_uuid().to_vec();
            resp.mut_header().set_uuid(uuid);
        }
        resp.set_responses(responses.into());

        assert!(cones.is_empty() || ssts.is_empty());
        let exec_res = if !cones.is_empty() {
            ApplyResult::Res(ExecResult::DeleteCone { cones })
        } else if !ssts.is_empty() {
            ApplyResult::Res(ExecResult::IngestSst { ssts })
        } else {
            ApplyResult::None
        };

        Ok((resp, exec_res))
    }
}

// Write commands related.
impl<EK> Applypushdown_causet<EK>
where
    EK: CausetEngine,
{
    fn handle_put<W: WriteBatch<EK>>(&mut self, wb: &mut W, req: &Request) -> Result<Response> {
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
        // brane key cone has no data prefix, so we must use origin key to check.
        util::check_key_in_brane(key, &self.brane)?;

        let resp = Response::default();
        let key = tuplespaceInstanton::data_key(key);
        self.metrics.size_diff_hint += key.len() as i64;
        self.metrics.size_diff_hint += value.len() as i64;
        if !req.get_put().get_causet().is_empty() {
            let causet = req.get_put().get_causet();
            // TODO: don't allow write preseved causets.
            if causet == Causet_DAGGER {
                self.metrics.lock_causet_written_bytes += key.len() as u64;
                self.metrics.lock_causet_written_bytes += value.len() as u64;
            }
            // TODO: check whether causet exists or not.
            wb.put_causet(causet, &key, value).unwrap_or_else(|e| {
                panic!(
                    "{} failed to write ({}, {}) to causet {}: {:?}",
                    self.tag,
                    hex::encode_upper(&key),
                    escape(value),
                    causet,
                    e
                )
            });
        } else {
            wb.put(&key, value).unwrap_or_else(|e| {
                panic!(
                    "{} failed to write ({}, {}): {:?}",
                    self.tag,
                    hex::encode_upper(&key),
                    escape(value),
                    e
                );
            });
        }
        Ok(resp)
    }

    fn handle_delete<W: WriteBatch<EK>>(&mut self, wb: &mut W, req: &Request) -> Result<Response> {
        let key = req.get_delete().get_key();
        // brane key cone has no data prefix, so we must use origin key to check.
        util::check_key_in_brane(key, &self.brane)?;

        let key = tuplespaceInstanton::data_key(key);
        // since size_diff_hint is not accurate, so we just skip calculate the value size.
        self.metrics.size_diff_hint -= key.len() as i64;
        let resp = Response::default();
        if !req.get_delete().get_causet().is_empty() {
            let causet = req.get_delete().get_causet();
            // TODO: check whether causet exists or not.
            wb.delete_causet(causet, &key).unwrap_or_else(|e| {
                panic!(
                    "{} failed to delete {}: {}",
                    self.tag,
                    hex::encode_upper(&key),
                    e
                )
            });

            if causet == Causet_DAGGER {
                // delete is a kind of write for Lmdb.
                self.metrics.lock_causet_written_bytes += key.len() as u64;
            } else {
                self.metrics.delete_tuplespaceInstanton_hint += 1;
            }
        } else {
            wb.delete(&key).unwrap_or_else(|e| {
                panic!(
                    "{} failed to delete {}: {}",
                    self.tag,
                    hex::encode_upper(&key),
                    e
                )
            });
            self.metrics.delete_tuplespaceInstanton_hint += 1;
        }

        Ok(resp)
    }

    fn handle_delete_cone(
        &mut self,
        engine: &EK,
        req: &Request,
        cones: &mut Vec<Cone>,
        use_delete_cone: bool,
    ) -> Result<Response> {
        let s_key = req.get_delete_cone().get_spacelike_key();
        let e_key = req.get_delete_cone().get_lightlike_key();
        let notify_only = req.get_delete_cone().get_notify_only();
        if !e_key.is_empty() && s_key >= e_key {
            return Err(box_err!(
                "invalid delete cone command, spacelike_key: {:?}, lightlike_key: {:?}",
                s_key,
                e_key
            ));
        }
        // brane key cone has no data prefix, so we must use origin key to check.
        util::check_key_in_brane(s_key, &self.brane)?;
        let lightlike_key = tuplespaceInstanton::data_lightlike_key(e_key);
        let brane_lightlike_key = tuplespaceInstanton::data_lightlike_key(self.brane.get_lightlike_key());
        if lightlike_key > brane_lightlike_key {
            return Err(Error::KeyNotInBrane(e_key.to_vec(), self.brane.clone()));
        }

        let resp = Response::default();
        let mut causet = req.get_delete_cone().get_causet();
        if causet.is_empty() {
            causet = Causet_DEFAULT;
        }
        if ALL_CausetS.iter().find(|x| **x == causet).is_none() {
            return Err(box_err!("invalid delete cone command, causet: {:?}", causet));
        }

        let spacelike_key = tuplespaceInstanton::data_key(s_key);
        // Use delete_files_in_cone to drop as many sst files as possible, this
        // is a way to reclaim disk space quickly after drop a Block/index.
        if !notify_only {
            engine
                .delete_files_in_cone_causet(causet, &spacelike_key, &lightlike_key, /* include_lightlike */ false)
                .unwrap_or_else(|e| {
                    panic!(
                        "{} failed to delete files in cone [{}, {}): {:?}",
                        self.tag,
                        hex::encode_upper(&spacelike_key),
                        hex::encode_upper(&lightlike_key),
                        e
                    )
                });

            // Delete all remaining tuplespaceInstanton.
            engine
                .delete_all_in_cone_causet(causet, &spacelike_key, &lightlike_key, use_delete_cone)
                .unwrap_or_else(|e| {
                    panic!(
                        "{} failed to delete all in cone [{}, {}), causet: {}, err: {:?}",
                        self.tag,
                        hex::encode_upper(&spacelike_key),
                        hex::encode_upper(&lightlike_key),
                        causet,
                        e
                    );
                });
        }

        // TODO: Should this be executed when `notify_only` is set?
        cones.push(Cone::new(causet.to_owned(), spacelike_key, lightlike_key));

        Ok(resp)
    }

    fn handle_ingest_sst(
        &mut self,
        importer: &Arc<SSTImporter>,
        engine: &EK,
        req: &Request,
        ssts: &mut Vec<SstMeta>,
    ) -> Result<Response> {
        let sst = req.get_ingest_sst().get_sst();

        if let Err(e) = check_sst_for_ingestion(sst, &self.brane) {
            error!(?e;
                 "ingest fail";
                 "brane_id" => self.brane_id(),
                 "peer_id" => self.id(),
                 "sst" => ?sst,
                 "brane" => ?&self.brane,
            );
            // This file is not valid, we can delete it here.
            let _ = importer.delete(sst);
            return Err(e);
        }

        importer.ingest(sst, engine).unwrap_or_else(|e| {
            // If this failed, it means that the file is corrupted or something
            // is wrong with the engine, but we can do nothing about that.
            panic!("{} ingest {:?}: {:?}", self.tag, sst, e);
        });

        ssts.push(sst.clone());
        Ok(Response::default())
    }
}

// Admin commands related.
impl<EK> Applypushdown_causet<EK>
where
    EK: CausetEngine,
{
    fn exec_change_peer<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        let request = request.get_change_peer();
        let peer = request.get_peer();
        let store_id = peer.get_store_id();
        let change_type = request.get_change_type();
        let mut brane = self.brane.clone();

        fail_point!(
            "apply_on_conf_change_1_3_1",
            (self.id == 1 || self.id == 3) && self.brane_id() == 1,
            |_| panic!("should not use return")
        );
        fail_point!(
            "apply_on_conf_change_3_1",
            self.id == 3 && self.brane_id() == 1,
            |_| panic!("should not use return")
        );
        fail_point!(
            "apply_on_conf_change_all_1",
            self.brane_id() == 1,
            |_| panic!("should not use return")
        );
        info!(
            "exec ConfChange";
            "brane_id" => self.brane_id(),
            "peer_id" => self.id(),
            "type" => util::conf_change_type_str(change_type),
            "epoch" => ?brane.get_brane_epoch(),
        );

        // TODO: we should need more check, like peer validation, duplicated id, etc.
        let conf_ver = brane.get_brane_epoch().get_conf_ver() + 1;
        brane.mut_brane_epoch().set_conf_ver(conf_ver);

        match change_type {
            ConfChangeType::AddNode => {
                let add_ndoe_fp = || {
                    fail_point!(
                        "apply_on_add_node_1_2",
                        self.id == 2 && self.brane_id() == 1,
                        |_| {}
                    )
                };
                add_ndoe_fp();

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_peer", "all"])
                    .inc();

                let mut exists = false;
                if let Some(p) = util::find_peer_mut(&mut brane, store_id) {
                    exists = true;
                    if !is_learner(p) || p.get_id() != peer.get_id() {
                        error!(
                            "can't add duplicated peer";
                            "brane_id" => self.brane_id(),
                            "peer_id" => self.id(),
                            "peer" => ?peer,
                            "brane" => ?&self.brane
                        );
                        return Err(box_err!(
                            "can't add duplicated peer {:?} to brane {:?}",
                            peer,
                            self.brane
                        ));
                    } else {
                        p.set_role(PeerRole::Voter);
                    }
                }
                if !exists {
                    // TODO: Do we allow adding peer in same node?
                    brane.mut_peers().push(peer.clone());
                }

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_peer", "success"])
                    .inc();
                info!(
                    "add peer successfully";
                    "brane_id" => self.brane_id(),
                    "peer_id" => self.id(),
                    "peer" => ?peer,
                    "brane" => ?&self.brane
                );
            }
            ConfChangeType::RemoveNode => {
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["remove_peer", "all"])
                    .inc();

                if let Some(p) = util::remove_peer(&mut brane, store_id) {
                    // Considering `is_learner` flag in `Peer` here is by design.
                    if &p != peer {
                        error!(
                            "ignore remove unmatched peer";
                            "brane_id" => self.brane_id(),
                            "peer_id" => self.id(),
                            "expect_peer" => ?peer,
                            "get_peeer" => ?p
                        );
                        return Err(box_err!(
                            "remove unmatched peer: expect: {:?}, get {:?}, ignore",
                            peer,
                            p
                        ));
                    }
                    if self.id == peer.get_id() {
                        // Remove ourself, we will destroy all brane data later.
                        // So we need not to apply following logs.
                        self.stopped = true;
                        self.plightlikeing_remove = true;
                    }
                } else {
                    error!(
                        "remove missing peer";
                        "brane_id" => self.brane_id(),
                        "peer_id" => self.id(),
                        "peer" => ?peer,
                        "brane" => ?&self.brane,
                    );
                    return Err(box_err!(
                        "remove missing peer {:?} from brane {:?}",
                        peer,
                        self.brane
                    ));
                }

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["remove_peer", "success"])
                    .inc();
                info!(
                    "remove peer successfully";
                    "brane_id" => self.brane_id(),
                    "peer_id" => self.id(),
                    "peer" => ?peer,
                    "brane" => ?&self.brane
                );
            }
            ConfChangeType::AddLearnerNode => {
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_learner", "all"])
                    .inc();

                if util::find_peer(&brane, store_id).is_some() {
                    error!(
                        "can't add duplicated learner";
                        "brane_id" => self.brane_id(),
                        "peer_id" => self.id(),
                        "peer" => ?peer,
                        "brane" => ?&self.brane
                    );
                    return Err(box_err!(
                        "can't add duplicated learner {:?} to brane {:?}",
                        peer,
                        self.brane
                    ));
                }
                brane.mut_peers().push(peer.clone());

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_learner", "success"])
                    .inc();
                info!(
                    "add learner successfully";
                    "brane_id" => self.brane_id(),
                    "peer_id" => self.id(),
                    "peer" => ?peer,
                    "brane" => ?&self.brane,
                );
            }
        }

        let state = if self.plightlikeing_remove {
            PeerState::Tombstone
        } else {
            PeerState::Normal
        };
        if let Err(e) = write_peer_state(ctx.kv_wb_mut(), &brane, state, None) {
            panic!("{} failed to fidelio brane state: {:?}", self.tag, e);
        }

        let mut resp = AdminResponse::default();
        resp.mut_change_peer().set_brane(brane.clone());

        Ok((
            resp,
            ApplyResult::Res(ExecResult::ChangePeer(ChangePeer {
                index: ctx.exec_ctx.as_ref().unwrap().index,
                conf_change: Default::default(),
                peer: peer.clone(),
                brane,
            })),
        ))
    }

    fn exec_split<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        info!(
            "split is deprecated, redirect to use batch split";
            "brane_id" => self.brane_id(),
            "peer_id" => self.id(),
        );
        let split = req.get_split().to_owned();
        let mut admin_req = AdminRequest::default();
        admin_req
            .mut_splits()
            .set_right_derive(split.get_right_derive());
        admin_req.mut_splits().mut_requests().push(split);
        // This method is executed only when there are unapplied entries after being respacelikeed.
        // So there will be no callback, it's OK to return a response that does not matched
        // with its request.
        self.exec_batch_split(ctx, &admin_req)
    }

    fn exec_batch_split<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!(
            "apply_before_split_1_3",
            self.id == 3 && self.brane_id() == 1,
            |_| { unreachable!() }
        );

        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();

        let split_reqs = req.get_splits();
        let right_derive = split_reqs.get_right_derive();
        if split_reqs.get_requests().is_empty() {
            return Err(box_err!("missing split requests"));
        }
        let mut derived = self.brane.clone();
        let new_brane_cnt = split_reqs.get_requests().len();
        let mut branes = Vec::with_capacity(new_brane_cnt + 1);
        let mut tuplespaceInstanton: VecDeque<Vec<u8>> = VecDeque::with_capacity(new_brane_cnt + 1);
        for req in split_reqs.get_requests() {
            let split_key = req.get_split_key();
            if split_key.is_empty() {
                return Err(box_err!("missing split key"));
            }
            if split_key
                <= tuplespaceInstanton
                    .back()
                    .map_or_else(|| derived.get_spacelike_key(), Vec::as_slice)
            {
                return Err(box_err!("invalid split request: {:?}", split_reqs));
            }
            if req.get_new_peer_ids().len() != derived.get_peers().len() {
                return Err(box_err!(
                    "invalid new peer id count, need {:?}, but got {:?}",
                    derived.get_peers(),
                    req.get_new_peer_ids()
                ));
            }
            tuplespaceInstanton.push_back(split_key.to_vec());
        }

        util::check_key_in_brane(tuplespaceInstanton.back().unwrap(), &self.brane)?;

        info!(
            "split brane";
            "brane_id" => self.brane_id(),
            "peer_id" => self.id(),
            "brane" => ?derived,
            "tuplespaceInstanton" => %TuplespaceInstantonInfoFormatter(tuplespaceInstanton.iter()),
        );
        let new_version = derived.get_brane_epoch().get_version() + new_brane_cnt as u64;
        derived.mut_brane_epoch().set_version(new_version);
        // Note that the split requests only contain ids for new branes, so we need
        // to handle new branes and old brane separately.
        if right_derive {
            // So the cone of new branes is [old_spacelike_key, split_key1, ..., last_split_key].
            tuplespaceInstanton.push_front(derived.get_spacelike_key().to_vec());
        } else {
            // So the cone of new branes is [split_key1, ..., last_split_key, old_lightlike_key].
            tuplespaceInstanton.push_back(derived.get_lightlike_key().to_vec());
            derived.set_lightlike_key(tuplespaceInstanton.front().unwrap().to_vec());
            branes.push(derived.clone());
        }

        let mut new_split_branes: HashMap<u64, NewSplitPeer> = HashMap::default();
        for req in split_reqs.get_requests() {
            let mut new_brane = Brane::default();
            new_brane.set_id(req.get_new_brane_id());
            new_brane.set_brane_epoch(derived.get_brane_epoch().to_owned());
            new_brane.set_spacelike_key(tuplespaceInstanton.pop_front().unwrap());
            new_brane.set_lightlike_key(tuplespaceInstanton.front().unwrap().to_vec());
            new_brane.set_peers(derived.get_peers().to_vec().into());
            for (peer, peer_id) in new_brane
                .mut_peers()
                .iter_mut()
                .zip(req.get_new_peer_ids())
            {
                peer.set_id(*peer_id);
            }
            new_split_branes.insert(
                new_brane.get_id(),
                NewSplitPeer {
                    peer_id: util::find_peer(&new_brane, ctx.store_id).unwrap().get_id(),
                    result: None,
                },
            );
            branes.push(new_brane);
        }

        if right_derive {
            derived.set_spacelike_key(tuplespaceInstanton.pop_front().unwrap());
            branes.push(derived.clone());
        }

        let mut replace_branes = HashSet::default();
        {
            let mut plightlikeing_create_peers = ctx.plightlikeing_create_peers.dagger().unwrap();
            for (brane_id, new_split_peer) in new_split_branes.iter_mut() {
                match plightlikeing_create_peers.entry(*brane_id) {
                    HashMapEntry::Occupied(mut v) => {
                        if *v.get() != (new_split_peer.peer_id, false) {
                            new_split_peer.result =
                                Some(format!("status {:?} is not expected", v.get()));
                        } else {
                            replace_branes.insert(*brane_id);
                            v.insert((new_split_peer.peer_id, true));
                        }
                    }
                    HashMapEntry::Vacant(v) => {
                        v.insert((new_split_peer.peer_id, true));
                    }
                }
            }
        }

        // brane_id -> peer_id
        let mut already_exist_branes = Vec::new();
        for (brane_id, new_split_peer) in new_split_branes.iter_mut() {
            let brane_state_key = tuplespaceInstanton::brane_state_key(*brane_id);
            match ctx
                .engine
                .get_msg_causet::<BraneLocalState>(Causet_VIOLETABFT, &brane_state_key)
            {
                Ok(None) => (),
                Ok(Some(state)) => {
                    if replace_branes.get(brane_id).is_some() {
                        // This peer must be the first one on local store. So if this peer is created on the other side,
                        // it means no `BraneLocalState` in kv engine.
                        panic!("{} failed to replace brane {} peer {} because state {:?} alread exist in kv engine",
                            self.tag, brane_id, new_split_peer.peer_id, state);
                    }
                    already_exist_branes.push((*brane_id, new_split_peer.peer_id));
                    new_split_peer.result = Some(format!("state {:?} exist in kv engine", state));
                }
                e => panic!(
                    "{} failed to get branes state of {}: {:?}",
                    self.tag, brane_id, e
                ),
            }
        }

        if !already_exist_branes.is_empty() {
            let mut plightlikeing_create_peers = ctx.plightlikeing_create_peers.dagger().unwrap();
            for (brane_id, peer_id) in &already_exist_branes {
                assert_eq!(
                    plightlikeing_create_peers.remove(brane_id),
                    Some((*peer_id, true))
                );
            }
        }

        let kv_wb_mut = ctx.kv_wb.as_mut().unwrap();
        for new_brane in &branes {
            if new_brane.get_id() == derived.get_id() {
                continue;
            }
            let new_split_peer = new_split_branes.get(&new_brane.get_id()).unwrap();
            if let Some(ref r) = new_split_peer.result {
                warn!(
                    "new brane from splitting already exists";
                    "new_brane_id" => new_brane.get_id(),
                    "new_peer_id" => new_split_peer.peer_id,
                    "reason" => r,
                    "brane_id" => self.brane_id(),
                    "peer_id" => self.id(),
                );
                continue;
            }
            write_peer_state(kv_wb_mut, new_brane, PeerState::Normal, None)
                .and_then(|_| write_initial_apply_state(kv_wb_mut, new_brane.get_id()))
                .unwrap_or_else(|e| {
                    panic!(
                        "{} fails to save split brane {:?}: {:?}",
                        self.tag, new_brane, e
                    )
                });
        }
        write_peer_state(kv_wb_mut, &derived, PeerState::Normal, None).unwrap_or_else(|e| {
            panic!("{} fails to fidelio brane {:?}: {:?}", self.tag, derived, e)
        });
        let mut resp = AdminResponse::default();
        resp.mut_splits().set_branes(branes.clone().into());
        PEER_ADMIN_CMD_COUNTER.batch_split.success.inc();

        fail_point!(
            "apply_after_split_1_3",
            self.id == 3 && self.brane_id() == 1,
            |_| { unreachable!() }
        );

        Ok((
            resp,
            ApplyResult::Res(ExecResult::SplitBrane {
                branes,
                derived,
                new_split_branes,
            }),
        ))
    }

    fn exec_prepare_merge<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!("apply_before_prepare_merge");

        PEER_ADMIN_CMD_COUNTER.prepare_merge.all.inc();

        let prepare_merge = req.get_prepare_merge();
        let index = prepare_merge.get_min_index();
        let exec_ctx = ctx.exec_ctx.as_ref().unwrap();
        let first_index = peer_causet_storage::first_index(&exec_ctx.apply_state);
        if index < first_index {
            // We filter `CompactLog` command before.
            panic!(
                "{} first index {} > min_index {}, skip pre merge",
                self.tag, first_index, index
            );
        }
        let mut brane = self.brane.clone();
        let brane_version = brane.get_brane_epoch().get_version() + 1;
        brane.mut_brane_epoch().set_version(brane_version);
        // In theory conf version should not be increased when executing prepare_merge.
        // However, we don't want to do conf change after prepare_merge is committed.
        // This can also be done by iterating all proposal to find if prepare_merge is
        // proposed before proposing conf change, but it make things complicated.
        // Another way is make conf change also check brane version, but this is not
        // backward compatible.
        let conf_version = brane.get_brane_epoch().get_conf_ver() + 1;
        brane.mut_brane_epoch().set_conf_ver(conf_version);
        let mut merging_state = MergeState::default();
        merging_state.set_min_index(index);
        merging_state.set_target(prepare_merge.get_target().to_owned());
        merging_state.set_commit(exec_ctx.index);
        write_peer_state(
            ctx.kv_wb.as_mut().unwrap(),
            &brane,
            PeerState::Merging,
            Some(merging_state.clone()),
        )
        .unwrap_or_else(|e| {
            panic!(
                "{} failed to save merging state {:?} for brane {:?}: {:?}",
                self.tag, merging_state, brane, e
            )
        });
        fail_point!("apply_after_prepare_merge");
        PEER_ADMIN_CMD_COUNTER.prepare_merge.success.inc();

        Ok((
            AdminResponse::default(),
            ApplyResult::Res(ExecResult::PrepareMerge {
                brane,
                state: merging_state,
            }),
        ))
    }

    // The target peer should lightlike missing log entries to the source peer.
    //
    // So, the merge process order would be:
    // 1.   `exec_commit_merge` in target apply fsm and lightlike `CatchUpLogs` to source peer fsm
    // 2.   `on_catch_up_logs_for_merge` in source peer fsm
    // 3.   if the source peer has already executed the corresponding `on_ready_prepare_merge`, set plightlikeing_remove and jump to step 6
    // 4.   ... (violetabft applightlike and apply logs)
    // 5.   `on_ready_prepare_merge` in source peer fsm and set plightlikeing_remove (means source brane has finished applying all logs)
    // 6.   `logs_up_to_date_for_merge` in source apply fsm (destroy its apply fsm and lightlike Noop to trigger the target apply fsm)
    // 7.   resume `exec_commit_merge` in target apply fsm
    // 8.   `on_ready_commit_merge` in target peer fsm and lightlike `MergeResult` to source peer fsm
    // 9.   `on_merge_result` in source peer fsm (destroy itself)
    fn exec_commit_merge<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        {
            let apply_before_commit_merge = || {
                fail_point!(
                    "apply_before_commit_merge_except_1_4",
                    self.brane_id() == 1 && self.id != 4,
                    |_| {}
                );
            };
            apply_before_commit_merge();
        }

        PEER_ADMIN_CMD_COUNTER.commit_merge.all.inc();

        let merge = req.get_commit_merge();
        let source_brane = merge.get_source();
        let source_brane_id = source_brane.get_id();

        // No matter whether the source peer has applied to the required index,
        // it's a race to write apply state in both source pushdown_causet and target
        // pushdown_causet. So asking the source pushdown_causet to stop first.
        if self.ready_source_brane_id != source_brane_id {
            if self.ready_source_brane_id != 0 {
                panic!(
                    "{} unexpected ready source brane {}, expecting {}",
                    self.tag, self.ready_source_brane_id, source_brane_id
                );
            }
            info!(
                "asking pushdown_causet to stop";
                "brane_id" => self.brane_id(),
                "peer_id" => self.id(),
                "source_brane_id" => source_brane_id
            );
            fail_point!("before_handle_catch_up_logs_for_merge");
            // lightlikes message to the source peer fsm and pause `exec_commit_merge` process
            let logs_up_to_date = Arc::new(AtomicU64::new(0));
            let msg = SignificantMsg::CatchUpLogs(CatchUpLogs {
                target_brane_id: self.brane_id(),
                merge: merge.to_owned(),
                logs_up_to_date: logs_up_to_date.clone(),
            });
            ctx.notifier
                .notify_one(source_brane_id, PeerMsg::SignificantMsg(msg));
            return Ok((
                AdminResponse::default(),
                ApplyResult::WaitMergeSource(logs_up_to_date),
            ));
        }

        info!(
            "execute CommitMerge";
            "brane_id" => self.brane_id(),
            "peer_id" => self.id(),
            "commit" => merge.get_commit(),
            "entries" => merge.get_entries().len(),
            "term" => ctx.exec_ctx.as_ref().unwrap().term,
            "index" => ctx.exec_ctx.as_ref().unwrap().index,
            "source_brane" => ?source_brane
        );

        self.ready_source_brane_id = 0;

        let brane_state_key = tuplespaceInstanton::brane_state_key(source_brane_id);
        let state: BraneLocalState = match ctx.engine.get_msg_causet(Causet_VIOLETABFT, &brane_state_key) {
            Ok(Some(s)) => s,
            e => panic!(
                "{} failed to get branes state of {:?}: {:?}",
                self.tag, source_brane, e
            ),
        };
        if state.get_state() != PeerState::Merging {
            panic!(
                "{} unexpected state of merging brane {:?}",
                self.tag, state
            );
        }
        let exist_brane = state.get_brane().to_owned();
        if *source_brane != exist_brane {
            panic!(
                "{} source_brane {:?} not match exist brane {:?}",
                self.tag, source_brane, exist_brane
            );
        }
        let mut brane = self.brane.clone();
        // Use a max value so that fidel can ensure overlapped brane has a priority.
        let version = cmp::max(
            source_brane.get_brane_epoch().get_version(),
            brane.get_brane_epoch().get_version(),
        ) + 1;
        brane.mut_brane_epoch().set_version(version);
        if tuplespaceInstanton::enc_lightlike_key(&brane) == tuplespaceInstanton::enc_spacelike_key(source_brane) {
            brane.set_lightlike_key(source_brane.get_lightlike_key().to_vec());
        } else {
            brane.set_spacelike_key(source_brane.get_spacelike_key().to_vec());
        }
        let kv_wb_mut = ctx.kv_wb.as_mut().unwrap();
        write_peer_state(kv_wb_mut, &brane, PeerState::Normal, None)
            .and_then(|_| {
                // TODO: maybe all information needs to be filled?
                let mut merging_state = MergeState::default();
                merging_state.set_target(self.brane.clone());
                write_peer_state(
                    kv_wb_mut,
                    source_brane,
                    PeerState::Tombstone,
                    Some(merging_state),
                )
            })
            .unwrap_or_else(|e| {
                panic!(
                    "{} failed to save merge brane {:?}: {:?}",
                    self.tag, brane, e
                )
            });

        PEER_ADMIN_CMD_COUNTER.commit_merge.success.inc();

        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::CommitMerge {
                brane,
                source: source_brane.to_owned(),
            }),
        ))
    }

    fn exec_rollback_merge<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        PEER_ADMIN_CMD_COUNTER.rollback_merge.all.inc();
        let brane_state_key = tuplespaceInstanton::brane_state_key(self.brane_id());
        let state: BraneLocalState = match ctx.engine.get_msg_causet(Causet_VIOLETABFT, &brane_state_key) {
            Ok(Some(s)) => s,
            e => panic!("{} failed to get branes state: {:?}", self.tag, e),
        };
        assert_eq!(state.get_state(), PeerState::Merging, "{}", self.tag);
        let rollback = req.get_rollback_merge();
        assert_eq!(
            state.get_merge_state().get_commit(),
            rollback.get_commit(),
            "{}",
            self.tag
        );
        let mut brane = self.brane.clone();
        let version = brane.get_brane_epoch().get_version();
        // fidelio version to avoid duplicated rollback requests.
        brane.mut_brane_epoch().set_version(version + 1);
        let kv_wb_mut = ctx.kv_wb.as_mut().unwrap();
        write_peer_state(kv_wb_mut, &brane, PeerState::Normal, None).unwrap_or_else(|e| {
            panic!(
                "{} failed to rollback merge {:?}: {:?}",
                self.tag, rollback, e
            )
        });

        PEER_ADMIN_CMD_COUNTER.rollback_merge.success.inc();
        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::RollbackMerge {
                brane,
                commit: rollback.get_commit(),
            }),
        ))
    }

    fn exec_compact_log<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        PEER_ADMIN_CMD_COUNTER.compact.all.inc();

        let compact_index = req.get_compact_log().get_compact_index();
        let resp = AdminResponse::default();
        let apply_state = &mut ctx.exec_ctx.as_mut().unwrap().apply_state;
        let first_index = peer_causet_storage::first_index(apply_state);
        if compact_index <= first_index {
            debug!(
                "compact index <= first index, no need to compact";
                "brane_id" => self.brane_id(),
                "peer_id" => self.id(),
                "compact_index" => compact_index,
                "first_index" => first_index,
            );
            return Ok((resp, ApplyResult::None));
        }
        if self.is_merging {
            info!(
                "in merging mode, skip compact";
                "brane_id" => self.brane_id(),
                "peer_id" => self.id(),
                "compact_index" => compact_index
            );
            return Ok((resp, ApplyResult::None));
        }

        let compact_term = req.get_compact_log().get_compact_term();
        // TODO: add unit tests to cover all the message integrity checks.
        if compact_term == 0 {
            info!(
                "compact term missing, skip";
                "brane_id" => self.brane_id(),
                "peer_id" => self.id(),
                "command" => ?req.get_compact_log()
            );
            // old format compact log command, safe to ignore.
            return Err(box_err!(
                "command format is outdated, please upgrade leader"
            ));
        }

        // compact failure is safe to be omitted, no need to assert.
        compact_violetabft_log(&self.tag, apply_state, compact_index, compact_term)?;

        PEER_ADMIN_CMD_COUNTER.compact.success.inc();

        Ok((
            resp,
            ApplyResult::Res(ExecResult::CompactLog {
                state: apply_state.get_truncated_state().clone(),
                first_index,
            }),
        ))
    }

    fn exec_compute_hash<W: WriteBatch<EK>>(
        &self,
        ctx: &ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::ComputeHash {
                brane: self.brane.clone(),
                index: ctx.exec_ctx.as_ref().unwrap().index,
                context: req.get_compute_hash().get_context().to_vec(),
                // This snapshot may be held for a long time, which may cause too many
                // open files in lmdb.
                // TODO: figure out another way to do consistency check without snapshot
                // or short life snapshot.
                snap: ctx.engine.snapshot(),
            }),
        ))
    }

    fn exec_verify_hash<W: WriteBatch<EK>>(
        &self,
        _: &ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        let verify_req = req.get_verify_hash();
        let index = verify_req.get_index();
        let context = verify_req.get_context().to_vec();
        let hash = verify_req.get_hash().to_vec();
        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::VerifyHash {
                index,
                context,
                hash,
            }),
        ))
    }
}

pub fn get_change_peer_cmd(msg: &VioletaBftCmdRequest) -> Option<&ChangePeerRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_change_peer() {
        return None;
    }

    Some(req.get_change_peer())
}

fn check_sst_for_ingestion(sst: &SstMeta, brane: &Brane) -> Result<()> {
    let uuid = sst.get_uuid();
    if let Err(e) = UuidBuilder::from_slice(uuid) {
        return Err(box_err!("invalid uuid {:?}: {:?}", uuid, e));
    }

    let causet_name = sst.get_causet_name();
    if causet_name != Causet_DEFAULT && causet_name != Causet_WRITE {
        return Err(box_err!("invalid causet name {}", causet_name));
    }

    let brane_id = sst.get_brane_id();
    if brane_id != brane.get_id() {
        return Err(Error::BraneNotFound(brane_id));
    }

    let epoch = sst.get_brane_epoch();
    let brane_epoch = brane.get_brane_epoch();
    if epoch.get_conf_ver() != brane_epoch.get_conf_ver()
        || epoch.get_version() != brane_epoch.get_version()
    {
        let error = format!("{:?} != {:?}", epoch, brane_epoch);
        return Err(Error::EpochNotMatch(error, vec![brane.clone()]));
    }

    let cone = sst.get_cone();
    util::check_key_in_brane(cone.get_spacelike(), brane)?;
    util::check_key_in_brane(cone.get_lightlike(), brane)?;

    Ok(())
}

/// fidelios the `state` with given `compact_index` and `compact_term`.
///
/// Remember the VioletaBft log is not deleted here.
pub fn compact_violetabft_log(
    tag: &str,
    state: &mut VioletaBftApplyState,
    compact_index: u64,
    compact_term: u64,
) -> Result<()> {
    debug!("{} compact log entries to prior to {}", tag, compact_index);

    if compact_index <= state.get_truncated_state().get_index() {
        return Err(box_err!("try to truncate compacted entries"));
    } else if compact_index > state.get_applied_index() {
        return Err(box_err!(
            "compact index {} > applied index {}",
            compact_index,
            state.get_applied_index()
        ));
    }

    // we don't actually delete the logs now, we add an async task to do it.

    state.mut_truncated_state().set_index(compact_index);
    state.mut_truncated_state().set_term(compact_term);

    Ok(())
}

pub struct Apply<S>
where
    S: Snapshot,
{
    pub peer_id: u64,
    pub brane_id: u64,
    pub term: u64,
    pub entries: Vec<Entry>,
    pub last_committed_index: u64,
    pub committed_index: u64,
    pub committed_term: u64,
    pub cbs: Vec<Proposal<S>>,
    entries_mem_size: i64,
    entries_count: i64,
}

impl<S: Snapshot> Apply<S> {
    pub(crate) fn new(
        peer_id: u64,
        brane_id: u64,
        term: u64,
        entries: Vec<Entry>,
        last_committed_index: u64,
        committed_index: u64,
        committed_term: u64,
        cbs: Vec<Proposal<S>>,
    ) -> Apply<S> {
        let entries_mem_size =
            (ENTRY_MEM_SIZE * entries.capacity()) as i64 + get_entries_mem_size(&entries);
        APPLY_PENDING_BYTES_GAUGE.add(entries_mem_size);
        let entries_count = entries.len() as i64;
        APPLY_PENDING_ENTRIES_GAUGE.add(entries_count);
        Apply {
            peer_id,
            brane_id,
            term,
            entries,
            last_committed_index,
            committed_index,
            committed_term,
            cbs,
            entries_mem_size,
            entries_count,
        }
    }
}

impl<S: Snapshot> Drop for Apply<S> {
    fn drop(&mut self) {
        APPLY_PENDING_BYTES_GAUGE.sub(self.entries_mem_size);
        APPLY_PENDING_ENTRIES_GAUGE.sub(self.entries_count);
    }
}

fn get_entries_mem_size(entries: &[Entry]) -> i64 {
    if entries.is_empty() {
        return 0;
    }
    let data_size: i64 = entries
        .iter()
        .map(|e| (e.data.capacity() + e.context.capacity()) as i64)
        .sum();
    data_size
}

#[derive(Default, Clone)]
pub struct Registration {
    pub id: u64,
    pub term: u64,
    pub apply_state: VioletaBftApplyState,
    pub applied_index_term: u64,
    pub brane: Brane,
    pub plightlikeing_request_snapshot_count: Arc<AtomicUsize>,
    pub is_merging: bool,
}

impl Registration {
    pub fn new<EK: CausetEngine, ER: VioletaBftEngine>(peer: &Peer<EK, ER>) -> Registration {
        Registration {
            id: peer.peer_id(),
            term: peer.term(),
            apply_state: peer.get_store().apply_state().clone(),
            applied_index_term: peer.get_store().applied_index_term(),
            brane: peer.brane().clone(),
            plightlikeing_request_snapshot_count: peer.plightlikeing_request_snapshot_count.clone(),
            is_merging: peer.plightlikeing_merge_state.is_some(),
        }
    }
}

pub struct Proposal<S>
where
    S: Snapshot,
{
    pub is_conf_change: bool,
    pub index: u64,
    pub term: u64,
    pub cb: Callback<S>,
    /// `renew_lease_time` contains the last time when a peer spacelikes to renew lease.
    pub renew_lease_time: Option<Timespec>,
}

pub struct Destroy {
    brane_id: u64,
    merge_from_snapshot: bool,
}

/// A message that asks the pushdown_causet to apply to the given logs and then reply to
/// target mailbox.
#[derive(Default, Debug)]
pub struct CatchUpLogs {
    /// The target brane to be notified when given logs are applied.
    pub target_brane_id: u64,
    /// Merge request that contains logs to be applied.
    pub merge: CommitMergeRequest,
    /// A flag indicate that all source brane's logs are applied.
    ///
    /// This is still necessary although we have a mailbox field already.
    /// Mailbox is used to notify target brane, and trigger a round of polling.
    /// But due to the FIFO natural of channel, we need a flag to check if it's
    /// ready when polling.
    pub logs_up_to_date: Arc<AtomicU64>,
}

pub struct GenSnapTask {
    pub(crate) brane_id: u64,
    commit_index: u64,
    snap_notifier: Synclightlikeer<VioletaBftSnapshot>,
}

impl GenSnapTask {
    pub fn new(
        brane_id: u64,
        commit_index: u64,
        snap_notifier: Synclightlikeer<VioletaBftSnapshot>,
    ) -> GenSnapTask {
        GenSnapTask {
            brane_id,
            commit_index,
            snap_notifier,
        }
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn generate_and_schedule_snapshot<EK>(
        self,
        kv_snap: EK::Snapshot,
        last_applied_index_term: u64,
        last_applied_state: VioletaBftApplyState,
        brane_sched: &Interlock_Semaphore<BraneTask<EK::Snapshot>>,
    ) -> Result<()>
    where
        EK: CausetEngine,
    {
        let snapshot = BraneTask::Gen {
            brane_id: self.brane_id,
            notifier: self.snap_notifier,
            last_applied_index_term,
            last_applied_state,
            // This snapshot may be held for a long time, which may cause too many
            // open files in lmdb.
            kv_snap,
        };
        box_try!(brane_sched.schedule(snapshot));
        Ok(())
    }
}

impl Debug for GenSnapTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenSnapTask")
            .field("brane_id", &self.brane_id)
            .field("commit_index", &self.commit_index)
            .finish()
    }
}

static OBSERVE_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier for checking stale observed commands.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObserveID(usize);

impl ObserveID {
    pub fn new() -> ObserveID {
        ObserveID(OBSERVE_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

struct ObserveCmd {
    id: ObserveID,
    enabled: Arc<AtomicBool>,
}

impl Debug for ObserveCmd {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObserveCmd").field("id", &self.id).finish()
    }
}

#[derive(Debug)]
pub enum ChangeCmd {
    RegisterSemaphore {
        observe_id: ObserveID,
        brane_id: u64,
        enabled: Arc<AtomicBool>,
    },
    Snapshot {
        observe_id: ObserveID,
        brane_id: u64,
    },
}

pub enum Msg<EK>
where
    EK: CausetEngine,
{
    Apply {
        spacelike: Instant,
        apply: Apply<EK::Snapshot>,
    },
    Registration(Registration),
    LogsUpToDate(CatchUpLogs),
    Noop,
    Destroy(Destroy),
    Snapshot(GenSnapTask),
    Change {
        cmd: ChangeCmd,
        brane_epoch: BraneEpoch,
        cb: Callback<EK::Snapshot>,
    },
    #[causet(any(test, feature = "testexport"))]
    #[allow(clippy::type_complexity)]
    Validate(u64, Box<dyn FnOnce(*const u8) + lightlike>),
}

impl<EK> Msg<EK>
where
    EK: CausetEngine,
{
    pub fn apply(apply: Apply<EK::Snapshot>) -> Msg<EK> {
        Msg::Apply {
            spacelike: Instant::now(),
            apply,
        }
    }

    pub fn register<ER: VioletaBftEngine>(peer: &Peer<EK, ER>) -> Msg<EK> {
        Msg::Registration(Registration::new(peer))
    }

    pub fn destroy(brane_id: u64, merge_from_snapshot: bool) -> Msg<EK> {
        Msg::Destroy(Destroy {
            brane_id,
            merge_from_snapshot,
        })
    }
}

impl<EK> Debug for Msg<EK>
where
    EK: CausetEngine,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Msg::Apply { apply, .. } => write!(f, "[brane {}] async apply", apply.brane_id),
            Msg::Registration(ref r) => {
                write!(f, "[brane {}] Reg {:?}", r.brane.get_id(), r.apply_state)
            }
            Msg::LogsUpToDate(_) => write!(f, "logs are fideliod"),
            Msg::Noop => write!(f, "noop"),
            Msg::Destroy(ref d) => write!(f, "[brane {}] destroy", d.brane_id),
            Msg::Snapshot(GenSnapTask { brane_id, .. }) => {
                write!(f, "[brane {}] requests a snapshot", brane_id)
            }
            Msg::Change {
                cmd: ChangeCmd::RegisterSemaphore { brane_id, .. },
                ..
            } => write!(f, "[brane {}] registers cmd semaphore", brane_id),
            Msg::Change {
                cmd: ChangeCmd::Snapshot { brane_id, .. },
                ..
            } => write!(f, "[brane {}] cmd snapshot", brane_id),
            #[causet(any(test, feature = "testexport"))]
            Msg::Validate(brane_id, _) => write!(f, "[brane {}] validate", brane_id),
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct ApplyMetrics {
    /// an inaccurate difference in brane size since last reset.
    pub size_diff_hint: i64,
    /// delete tuplespaceInstanton' count since last reset.
    pub delete_tuplespaceInstanton_hint: u64,

    pub written_bytes: u64,
    pub written_tuplespaceInstanton: u64,
    pub lock_causet_written_bytes: u64,
}

#[derive(Debug)]
pub struct ApplyRes<S>
where
    S: Snapshot,
{
    pub brane_id: u64,
    pub apply_state: VioletaBftApplyState,
    pub applied_index_term: u64,
    pub exec_res: VecDeque<ExecResult<S>>,
    pub metrics: ApplyMetrics,
}

#[derive(Debug)]
pub enum TaskRes<S>
where
    S: Snapshot,
{
    Apply(ApplyRes<S>),
    Destroy {
        // ID of brane that has been destroyed.
        brane_id: u64,
        // ID of peer that has been destroyed.
        peer_id: u64,
        // Whether destroy request is from its target brane's snapshot
        merge_from_snapshot: bool,
    },
}

pub struct ApplyFsm<EK>
where
    EK: CausetEngine,
{
    pushdown_causet: Applypushdown_causet<EK>,
    receiver: Receiver<Msg<EK>>,
    mailbox: Option<BasicMailbox<ApplyFsm<EK>>>,
}

impl<EK> ApplyFsm<EK>
where
    EK: CausetEngine,
{
    fn from_peer<ER: VioletaBftEngine>(
        peer: &Peer<EK, ER>,
    ) -> (LooseBoundedlightlikeer<Msg<EK>>, Box<ApplyFsm<EK>>) {
        let reg = Registration::new(peer);
        ApplyFsm::from_registration(reg)
    }

    fn from_registration(reg: Registration) -> (LooseBoundedlightlikeer<Msg<EK>>, Box<ApplyFsm<EK>>) {
        let (tx, rx) = loose_bounded(usize::MAX);
        let pushdown_causet = Applypushdown_causet::from_registration(reg);
        (
            tx,
            Box::new(ApplyFsm {
                pushdown_causet,
                receiver: rx,
                mailbox: None,
            }),
        )
    }

    /// Handles peer registration. When a peer is created, it will register an apply pushdown_causet.
    fn handle_registration(&mut self, reg: Registration) {
        info!(
            "re-register to apply pushdown_causets";
            "brane_id" => self.pushdown_causet.brane_id(),
            "peer_id" => self.pushdown_causet.id(),
            "term" => reg.term
        );
        assert_eq!(self.pushdown_causet.id, reg.id);
        self.pushdown_causet.term = reg.term;
        self.pushdown_causet.clear_all_commands_as_stale();
        self.pushdown_causet = Applypushdown_causet::from_registration(reg);
    }

    /// Handles apply tasks, and uses the apply pushdown_causet to handle the committed entries.
    fn handle_apply<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        mut apply: Apply<EK::Snapshot>,
    ) {
        if apply_ctx.timer.is_none() {
            apply_ctx.timer = Some(Instant::now_coarse());
        }

        fail_point!("on_handle_apply_1003", self.pushdown_causet.id() == 1003, |_| {});
        fail_point!("on_handle_apply_2", self.pushdown_causet.id() == 2, |_| {});
        fail_point!("on_handle_apply", |_| {});

        if apply.entries.is_empty() || self.pushdown_causet.plightlikeing_remove || self.pushdown_causet.stopped {
            return;
        }

        self.pushdown_causet.metrics = ApplyMetrics::default();
        self.pushdown_causet.term = apply.term;
        let prev_state = (
            self.pushdown_causet.apply_state.get_last_commit_index(),
            self.pushdown_causet.apply_state.get_commit_index(),
            self.pushdown_causet.apply_state.get_commit_term(),
        );
        let cur_state = (
            apply.last_committed_index,
            apply.committed_index,
            apply.committed_term,
        );
        if prev_state.0 > cur_state.0 || prev_state.1 > cur_state.1 || prev_state.2 > cur_state.2 {
            panic!(
                "{} commit state jump backward {:?} -> {:?}",
                self.pushdown_causet.tag, prev_state, cur_state
            );
        }
        // The apply state may not be written to disk if entries is empty,
        // which seems OK.
        self.pushdown_causet.apply_state.set_last_commit_index(cur_state.0);
        self.pushdown_causet.apply_state.set_commit_index(cur_state.1);
        self.pushdown_causet.apply_state.set_commit_term(cur_state.2);

        self.applightlike_proposal(apply.cbs.drain(..));
        self.pushdown_causet
            .handle_violetabft_committed_entries(apply_ctx, apply.entries.drain(..));
        fail_point!("post_handle_apply_1003", self.pushdown_causet.id() == 1003, |_| {});
        if self.pushdown_causet.yield_state.is_some() {
            return;
        }
    }

    /// Handles proposals, and applightlikes the commands to the apply pushdown_causet.
    fn applightlike_proposal(&mut self, props_drainer: Drain<Proposal<EK::Snapshot>>) {
        let (brane_id, peer_id) = (self.pushdown_causet.brane_id(), self.pushdown_causet.id());
        let propose_num = props_drainer.len();
        if self.pushdown_causet.stopped {
            for p in props_drainer {
                let cmd = PlightlikeingCmd::<EK::Snapshot>::new(p.index, p.term, p.cb);
                notify_stale_command(brane_id, peer_id, self.pushdown_causet.term, cmd);
            }
            return;
        }
        for p in props_drainer {
            let cmd = PlightlikeingCmd::new(p.index, p.term, p.cb);
            if p.is_conf_change {
                if let Some(cmd) = self.pushdown_causet.plightlikeing_cmds.take_conf_change() {
                    // if it loses leadership before conf change is replicated, there may be
                    // a stale plightlikeing conf change before next conf change is applied. If it
                    // becomes leader again with the stale plightlikeing conf change, will enter
                    // this block, so we notify leadership may have been changed.
                    notify_stale_command(brane_id, peer_id, self.pushdown_causet.term, cmd);
                }
                self.pushdown_causet.plightlikeing_cmds.set_conf_change(cmd);
            } else {
                self.pushdown_causet.plightlikeing_cmds.applightlike_normal(cmd);
            }
        }
        // TODO: observe it in batch.
        APPLY_PROPOSAL.observe(propose_num as f64);
    }

    fn destroy<W: WriteBatch<EK>>(&mut self, ctx: &mut ApplyContext<EK, W>) {
        let brane_id = self.pushdown_causet.brane_id();
        if ctx.apply_res.iter().any(|res| res.brane_id == brane_id) {
            // Flush before destroying to avoid reordering messages.
            ctx.flush();
        }
        fail_point!(
            "before_peer_destroy_1003",
            self.pushdown_causet.id() == 1003,
            |_| {}
        );
        info!(
            "remove pushdown_causet from apply pushdown_causets";
            "brane_id" => self.pushdown_causet.brane_id(),
            "peer_id" => self.pushdown_causet.id(),
        );
        self.pushdown_causet.destroy(ctx);
    }

    /// Handles peer destroy. When a peer is destroyed, the corresponding apply pushdown_causet should be removed too.
    fn handle_destroy<W: WriteBatch<EK>>(&mut self, ctx: &mut ApplyContext<EK, W>, d: Destroy) {
        assert_eq!(d.brane_id, self.pushdown_causet.brane_id());
        if d.merge_from_snapshot {
            assert_eq!(self.pushdown_causet.stopped, false);
        }
        if !self.pushdown_causet.stopped {
            self.destroy(ctx);
            ctx.notifier.notify_one(
                self.pushdown_causet.brane_id(),
                PeerMsg::ApplyRes {
                    res: TaskRes::Destroy {
                        brane_id: self.pushdown_causet.brane_id(),
                        peer_id: self.pushdown_causet.id,
                        merge_from_snapshot: d.merge_from_snapshot,
                    },
                },
            );
        }
    }

    fn resume_plightlikeing<W: WriteBatch<EK>>(&mut self, ctx: &mut ApplyContext<EK, W>) {
        if let Some(ref state) = self.pushdown_causet.wait_merge_state {
            let source_brane_id = state.logs_up_to_date.load(Ordering::SeqCst);
            if source_brane_id == 0 {
                return;
            }
            self.pushdown_causet.ready_source_brane_id = source_brane_id;
        }
        self.pushdown_causet.wait_merge_state = None;

        let mut state = self.pushdown_causet.yield_state.take().unwrap();

        if ctx.timer.is_none() {
            ctx.timer = Some(Instant::now_coarse());
        }
        if !state.plightlikeing_entries.is_empty() {
            self.pushdown_causet
                .handle_violetabft_committed_entries(ctx, state.plightlikeing_entries.drain(..));
            if let Some(ref mut s) = self.pushdown_causet.yield_state {
                // So the pushdown_causet is expected to yield the CPU.
                // It can either be executing another `CommitMerge` in plightlikeing_msgs
                // or has been written too much data.
                s.plightlikeing_msgs = state.plightlikeing_msgs;
                return;
            }
        }

        if !state.plightlikeing_msgs.is_empty() {
            self.handle_tasks(ctx, &mut state.plightlikeing_msgs);
        }
    }

    fn logs_up_to_date_for_merge<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        catch_up_logs: CatchUpLogs,
    ) {
        fail_point!("after_handle_catch_up_logs_for_merge");
        fail_point!(
            "after_handle_catch_up_logs_for_merge_1003",
            self.pushdown_causet.id() == 1003,
            |_| {}
        );

        let brane_id = self.pushdown_causet.brane_id();
        info!(
            "source logs are all applied now";
            "brane_id" => brane_id,
            "peer_id" => self.pushdown_causet.id(),
        );
        // The source peer fsm will be destroyed when the target peer executes `on_ready_commit_merge`
        // and lightlikes `merge result` to the source peer fsm.
        self.destroy(ctx);
        catch_up_logs
            .logs_up_to_date
            .store(brane_id, Ordering::SeqCst);
        // To trigger the target apply fsm
        if let Some(mailbox) = ctx.router.mailbox(catch_up_logs.target_brane_id) {
            let _ = mailbox.force_lightlike(Msg::Noop);
        } else {
            error!(
                "failed to get mailbox, are we shutting down?";
                "brane_id" => brane_id,
                "peer_id" => self.pushdown_causet.id(),
            );
        }
    }

    #[allow(unused_mut)]
    fn handle_snapshot<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        snap_task: GenSnapTask,
    ) {
        if self.pushdown_causet.plightlikeing_remove || self.pushdown_causet.stopped {
            return;
        }
        let applied_index = self.pushdown_causet.apply_state.get_applied_index();
        assert!(snap_task.commit_index() <= applied_index);
        let mut need_sync = apply_ctx
            .apply_res
            .iter()
            .any(|res| res.brane_id == self.pushdown_causet.brane_id())
            && self.pushdown_causet.last_sync_apply_index != applied_index;
        (|| fail_point!("apply_on_handle_snapshot_sync", |_| { need_sync = true }))();
        if need_sync {
            if apply_ctx.timer.is_none() {
                apply_ctx.timer = Some(Instant::now_coarse());
            }
            apply_ctx.prepare_write_batch();
            self.pushdown_causet.write_apply_state(apply_ctx.kv_wb_mut());
            fail_point!(
                "apply_on_handle_snapshot_1_1",
                self.pushdown_causet.id == 1 && self.pushdown_causet.brane_id() == 1,
                |_| unimplemented!()
            );

            apply_ctx.flush();
            // For now, it's more like last_flush_apply_index.
            // TODO: fidelio it only when `flush()` returns true.
            self.pushdown_causet.last_sync_apply_index = applied_index;
        }

        if let Err(e) = snap_task.generate_and_schedule_snapshot::<EK>(
            apply_ctx.engine.snapshot(),
            self.pushdown_causet.applied_index_term,
            self.pushdown_causet.apply_state.clone(),
            &apply_ctx.brane_interlock_semaphore,
        ) {
            error!(
                "schedule snapshot failed";
                "error" => ?e,
                "brane_id" => self.pushdown_causet.brane_id(),
                "peer_id" => self.pushdown_causet.id()
            );
        }
        self.pushdown_causet
            .plightlikeing_request_snapshot_count
            .fetch_sub(1, Ordering::SeqCst);
        fail_point!(
            "apply_on_handle_snapshot_finish_1_1",
            self.pushdown_causet.id == 1 && self.pushdown_causet.brane_id() == 1,
            |_| unimplemented!()
        );
    }

    fn handle_change<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        cmd: ChangeCmd,
        brane_epoch: BraneEpoch,
        cb: Callback<EK::Snapshot>,
    ) {
        let (observe_id, brane_id, enabled) = match cmd {
            ChangeCmd::RegisterSemaphore {
                observe_id,
                brane_id,
                enabled,
            } => (observe_id, brane_id, Some(enabled)),
            ChangeCmd::Snapshot {
                observe_id,
                brane_id,
            } => (observe_id, brane_id, None),
        };
        if let Some(observe_cmd) = self.pushdown_causet.observe_cmd.as_mut() {
            if observe_cmd.id > observe_id {
                notify_stale_req(self.pushdown_causet.term, cb);
                return;
            }
        }

        assert_eq!(self.pushdown_causet.brane_id(), brane_id);
        let resp = match compare_brane_epoch(
            &brane_epoch,
            &self.pushdown_causet.brane,
            false, /* check_conf_ver */
            true,  /* check_ver */
            true,  /* include_brane */
        ) {
            Ok(()) => {
                // Commit the writebatch for ensuring the following snapshot can get all previous writes.
                if apply_ctx.kv_wb.is_some() && apply_ctx.kv_wb().count() > 0 {
                    apply_ctx.commit(&mut self.pushdown_causet);
                }
                ReadResponse {
                    response: Default::default(),
                    snapshot: Some(BraneSnapshot::from_snapshot(
                        Arc::new(apply_ctx.engine.snapshot()),
                        Arc::new(self.pushdown_causet.brane.clone()),
                    )),
                    txn_extra_op: TxnExtraOp::Noop,
                }
            }
            Err(e) => {
                // Return error if epoch not match
                cb.invoke_read(ReadResponse {
                    response: cmd_resp::new_error(e),
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                });
                return;
            }
        };
        if let Some(enabled) = enabled {
            assert!(
                !self
                    .pushdown_causet
                    .observe_cmd
                    .as_ref()
                    .map_or(false, |o| o.enabled.load(Ordering::SeqCst)),
                "{} semaphore already exists {:?} {:?}",
                self.pushdown_causet.tag,
                self.pushdown_causet.observe_cmd,
                observe_id
            );
            // TODO(causet_context): take observe_cmd when enabled is false.
            self.pushdown_causet.observe_cmd = Some(ObserveCmd {
                id: observe_id,
                enabled,
            });
        }

        cb.invoke_read(resp);
    }

    fn handle_tasks<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        msgs: &mut Vec<Msg<EK>>,
    ) {
        let mut channel_timer = None;
        let mut drainer = msgs.drain(..);
        loop {
            match drainer.next() {
                Some(Msg::Apply { spacelike, apply }) => {
                    if channel_timer.is_none() {
                        channel_timer = Some(spacelike);
                    }
                    self.handle_apply(apply_ctx, apply);
                    if let Some(ref mut state) = self.pushdown_causet.yield_state {
                        state.plightlikeing_msgs = drainer.collect();
                        break;
                    }
                }
                Some(Msg::Registration(reg)) => self.handle_registration(reg),
                Some(Msg::Destroy(d)) => self.handle_destroy(apply_ctx, d),
                Some(Msg::LogsUpToDate(cul)) => self.logs_up_to_date_for_merge(apply_ctx, cul),
                Some(Msg::Noop) => {}
                Some(Msg::Snapshot(snap_task)) => self.handle_snapshot(apply_ctx, snap_task),
                Some(Msg::Change {
                    cmd,
                    brane_epoch,
                    cb,
                }) => self.handle_change(apply_ctx, cmd, brane_epoch, cb),
                #[causet(any(test, feature = "testexport"))]
                Some(Msg::Validate(_, f)) => {
                    let pushdown_causet: *const u8 = unsafe { std::mem::transmute(&self.pushdown_causet) };
                    f(pushdown_causet)
                }
                None => break,
            }
        }
        if let Some(timer) = channel_timer {
            let elapsed = duration_to_sec(timer.elapsed());
            APPLY_TASK_WAIT_TIME_HISTOGRAM.observe(elapsed);
        }
    }
}

impl<EK> Fsm for ApplyFsm<EK>
where
    EK: CausetEngine,
{
    type Message = Msg<EK>;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.pushdown_causet.stopped
    }

    #[inline]
    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        self.mailbox = Some(mailbox.into_owned());
    }

    #[inline]
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        self.mailbox.take()
    }
}

impl<EK> Drop for ApplyFsm<EK>
where
    EK: CausetEngine,
{
    fn drop(&mut self) {
        self.pushdown_causet.clear_all_commands_as_stale();
    }
}

pub struct ControlMsg;

pub struct ControlFsm;

impl Fsm for ControlFsm {
    type Message = ControlMsg;

    #[inline]
    fn is_stopped(&self) -> bool {
        true
    }
}

pub struct ApplyPoller<EK, W>
where
    EK: CausetEngine,
    W: WriteBatch<EK>,
{
    msg_buf: Vec<Msg<EK>>,
    apply_ctx: ApplyContext<EK, W>,
    messages_per_tick: usize,
    causet_tracker: Tracker<Config>,
}

impl<EK, W> PollHandler<ApplyFsm<EK>, ControlFsm> for ApplyPoller<EK, W>
where
    EK: CausetEngine,
    W: WriteBatch<EK>,
{
    fn begin(&mut self, _batch_size: usize) {
        if let Some(incoming) = self.causet_tracker.any_new() {
            match Ord::cmp(&incoming.messages_per_tick, &self.messages_per_tick) {
                CmpOrdering::Greater => {
                    self.msg_buf.reserve(incoming.messages_per_tick);
                    self.messages_per_tick = incoming.messages_per_tick;
                }
                CmpOrdering::Less => {
                    self.msg_buf.shrink_to(incoming.messages_per_tick);
                    self.messages_per_tick = incoming.messages_per_tick;
                }
                _ => {}
            }
        }
        self.apply_ctx.perf_context_statistics.spacelike();
    }

    /// There is no control fsm in apply poller.
    fn handle_control(&mut self, _: &mut ControlFsm) -> Option<usize> {
        unimplemented!()
    }

    fn handle_normal(&mut self, normal: &mut ApplyFsm<EK>) -> Option<usize> {
        let mut expected_msg_count = None;
        normal.pushdown_causet.handle_spacelike = Some(Instant::now_coarse());
        if normal.pushdown_causet.yield_state.is_some() {
            if normal.pushdown_causet.wait_merge_state.is_some() {
                // We need to query the length first, otherwise there is a race
                // condition that new messages are queued after resuming and before
                // query the length.
                expected_msg_count = Some(normal.receiver.len());
            }
            normal.resume_plightlikeing(&mut self.apply_ctx);
            if normal.pushdown_causet.wait_merge_state.is_some() {
                // Yield due to applying CommitMerge, this fsm can be released if its
                // channel msg count equals to expected_msg_count because it will receive
                // a new message if its source brane has applied all needed logs.
                return expected_msg_count;
            } else if normal.pushdown_causet.yield_state.is_some() {
                // Yield due to other reasons, this fsm must not be released because
                // it's possible that no new message will be sent to itself.
                // The remaining messages will be handled in next rounds.
                return None;
            }
            expected_msg_count = None;
        }
        fail_point!("before_handle_normal_3", normal.pushdown_causet.id() == 3, |_| {
            None
        });
        while self.msg_buf.len() < self.messages_per_tick {
            match normal.receiver.try_recv() {
                Ok(msg) => self.msg_buf.push(msg),
                Err(TryRecvError::Empty) => {
                    expected_msg_count = Some(0);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    normal.pushdown_causet.stopped = true;
                    expected_msg_count = Some(0);
                    break;
                }
            }
        }
        normal.handle_tasks(&mut self.apply_ctx, &mut self.msg_buf);
        if normal.pushdown_causet.wait_merge_state.is_some() {
            // Check it again immediately as catching up logs can be very fast.
            expected_msg_count = Some(0);
        } else if normal.pushdown_causet.yield_state.is_some() {
            // Let it continue to run next time.
            expected_msg_count = None;
        }
        expected_msg_count
    }

    fn lightlike(&mut self, fsms: &mut [Box<ApplyFsm<EK>>]) {
        let is_synced = self.apply_ctx.flush();
        if is_synced {
            for fsm in fsms {
                fsm.pushdown_causet.last_sync_apply_index = fsm.pushdown_causet.apply_state.get_applied_index();
            }
        }
    }
}

pub struct Builder<EK: CausetEngine, W: WriteBatch<EK>> {
    tag: String,
    causet: Arc<VersionTrack<Config>>,
    interlock_host: InterlockHost<EK>,
    importer: Arc<SSTImporter>,
    brane_interlock_semaphore: Interlock_Semaphore<BraneTask<<EK as CausetEngine>::Snapshot>>,
    engine: EK,
    lightlikeer: Box<dyn Notifier<EK>>,
    router: ApplyRouter<EK>,
    _phantom: PhantomData<W>,
    store_id: u64,
    plightlikeing_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
}

impl<EK: CausetEngine, W> Builder<EK, W>
where
    W: WriteBatch<EK>,
{
    pub fn new<T, C, ER: VioletaBftEngine>(
        builder: &VioletaBftPollerBuilder<EK, ER, T, C>,
        lightlikeer: Box<dyn Notifier<EK>>,
        router: ApplyRouter<EK>,
    ) -> Builder<EK, W> {
        Builder {
            tag: format!("[store {}]", builder.store.get_id()),
            causet: builder.causet.clone(),
            interlock_host: builder.interlock_host.clone(),
            importer: builder.importer.clone(),
            brane_interlock_semaphore: builder.brane_interlock_semaphore.clone(),
            engine: builder.engines.kv.clone(),
            _phantom: PhantomData,
            lightlikeer,
            router,
            store_id: builder.store.get_id(),
            plightlikeing_create_peers: builder.plightlikeing_create_peers.clone(),
        }
    }
}

impl<EK, W> HandlerBuilder<ApplyFsm<EK>, ControlFsm> for Builder<EK, W>
where
    EK: CausetEngine,
    W: WriteBatch<EK>,
{
    type Handler = ApplyPoller<EK, W>;

    fn build(&mut self) -> ApplyPoller<EK, W> {
        let causet = self.causet.value();
        ApplyPoller {
            msg_buf: Vec::with_capacity(causet.messages_per_tick),
            apply_ctx: ApplyContext::new(
                self.tag.clone(),
                self.interlock_host.clone(),
                self.importer.clone(),
                self.brane_interlock_semaphore.clone(),
                self.engine.clone(),
                self.router.clone(),
                self.lightlikeer.clone_box(),
                &causet,
                self.store_id,
                self.plightlikeing_create_peers.clone(),
            ),
            messages_per_tick: causet.messages_per_tick,
            causet_tracker: self.causet.clone().tracker(self.tag.clone()),
        }
    }
}

#[derive(Clone)]
pub struct ApplyRouter<EK>
where
    EK: CausetEngine,
{
    pub router: BatchRouter<ApplyFsm<EK>, ControlFsm>,
}

impl<EK> Deref for ApplyRouter<EK>
where
    EK: CausetEngine,
{
    type Target = BatchRouter<ApplyFsm<EK>, ControlFsm>;

    fn deref(&self) -> &BatchRouter<ApplyFsm<EK>, ControlFsm> {
        &self.router
    }
}

impl<EK> DerefMut for ApplyRouter<EK>
where
    EK: CausetEngine,
{
    fn deref_mut(&mut self) -> &mut BatchRouter<ApplyFsm<EK>, ControlFsm> {
        &mut self.router
    }
}

impl<EK> ApplyRouter<EK>
where
    EK: CausetEngine,
{
    pub fn schedule_task(&self, brane_id: u64, msg: Msg<EK>) {
        let reg = match self.try_lightlike(brane_id, msg) {
            Either::Left(Ok(())) => return,
            Either::Left(Err(TrylightlikeError::Disconnected(msg))) | Either::Right(msg) => match msg {
                Msg::Registration(reg) => reg,
                Msg::Apply { mut apply, .. } => {
                    info!(
                        "target brane is not found, drop proposals";
                        "brane_id" => brane_id
                    );
                    for p in apply.cbs.drain(..) {
                        let cmd = PlightlikeingCmd::<EK::Snapshot>::new(p.index, p.term, p.cb);
                        notify_brane_removed(apply.brane_id, apply.peer_id, cmd);
                    }
                    return;
                }
                Msg::Destroy(_) | Msg::Noop => {
                    info!(
                        "target brane is not found, drop messages";
                        "brane_id" => brane_id
                    );
                    return;
                }
                Msg::Snapshot(_) => {
                    warn!(
                        "brane is removed before taking snapshot, are we shutting down?";
                        "brane_id" => brane_id
                    );
                    return;
                }
                Msg::LogsUpToDate(cul) => {
                    warn!(
                        "brane is removed before merged, are we shutting down?";
                        "brane_id" => brane_id,
                        "merge" => ?cul.merge,
                    );
                    return;
                }
                Msg::Change {
                    cmd: ChangeCmd::RegisterSemaphore { brane_id, .. },
                    cb,
                    ..
                }
                | Msg::Change {
                    cmd: ChangeCmd::Snapshot { brane_id, .. },
                    cb,
                    ..
                } => {
                    warn!("target brane is not found";
                            "brane_id" => brane_id);
                    let resp = ReadResponse {
                        response: cmd_resp::new_error(Error::BraneNotFound(brane_id)),
                        snapshot: None,
                        txn_extra_op: TxnExtraOp::Noop,
                    };
                    cb.invoke_read(resp);
                    return;
                }
                #[causet(any(test, feature = "testexport"))]
                Msg::Validate(_, _) => return,
            },
            Either::Left(Err(TrylightlikeError::Full(_))) => unreachable!(),
        };

        // Messages in one brane are sent in sequence, so there is no race here.
        // However, this can't be handled inside control fsm, as messages can be
        // queued inside both queue of control fsm and normal fsm, which can reorder
        // messages.
        let (lightlikeer, apply_fsm) = ApplyFsm::from_registration(reg);
        let mailbox = BasicMailbox::new(lightlikeer, apply_fsm);
        self.register(brane_id, mailbox);
    }
}

pub struct ApplyBatchSystem<EK: CausetEngine> {
    system: BatchSystem<ApplyFsm<EK>, ControlFsm>,
}

impl<EK: CausetEngine> Deref for ApplyBatchSystem<EK> {
    type Target = BatchSystem<ApplyFsm<EK>, ControlFsm>;

    fn deref(&self) -> &BatchSystem<ApplyFsm<EK>, ControlFsm> {
        &self.system
    }
}

impl<EK: CausetEngine> DerefMut for ApplyBatchSystem<EK> {
    fn deref_mut(&mut self) -> &mut BatchSystem<ApplyFsm<EK>, ControlFsm> {
        &mut self.system
    }
}

impl<EK: CausetEngine> ApplyBatchSystem<EK> {
    pub fn schedule_all<'a, ER: VioletaBftEngine>(&self, peers: impl Iteron<Item = &'a Peer<EK, ER>>) {
        let mut mailboxes = Vec::with_capacity(peers.size_hint().0);
        for peer in peers {
            let (tx, fsm) = ApplyFsm::from_peer(peer);
            mailboxes.push((peer.brane().get_id(), BasicMailbox::new(tx, fsm)));
        }
        self.router().register_all(mailboxes);
    }
}

pub fn create_apply_batch_system<EK: CausetEngine>(
    causet: &Config,
) -> (ApplyRouter<EK>, ApplyBatchSystem<EK>) {
    let (tx, _) = loose_bounded(usize::MAX);
    let (router, system) =
        batch_system::create_system(&causet.apply_batch_system, tx, Box::new(ControlFsm));
    (ApplyRouter { router }, ApplyBatchSystem { system })
}

#[causet(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::atomic::*;
    use std::sync::*;
    use std::thread;
    use std::time::*;

    use crate::interlock::*;
    use crate::store::msg::WriteResponse;
    use crate::store::peer_causet_storage::VIOLETABFT_INIT_LOG_INDEX;
    use crate::store::util::{new_learner_peer, new_peer};
    use engine_lmdb::{util::new_engine, LmdbEngine, LmdbSnapshot, LmdbWriteBatch};
    use edb::{Peekable as PeekableTrait, WriteBatchExt};
    use ekvproto::meta_timeshare::{self, BraneEpoch};
    use ekvproto::violetabft_cmd_timeshare::*;
    use protobuf::Message;
    use tempfile::{Builder, TempDir};
    use uuid::Uuid;

    use crate::store::{Config, BraneTask};
    use test_sst_importer::*;
    use violetabftstore::interlock::::config::VersionTrack;
    use violetabftstore::interlock::::worker::dummy_interlock_semaphore;

    use super::*;

    pub fn create_tmp_engine(path: &str) -> (TempDir, LmdbEngine) {
        let path = Builder::new().prefix(path).temfidelir().unwrap();
        let engine = new_engine(
            path.path().join("db").to_str().unwrap(),
            None,
            ALL_CausetS,
            None,
        )
        .unwrap();
        (path, engine)
    }

    pub fn create_tmp_importer(path: &str) -> (TempDir, Arc<SSTImporter>) {
        let dir = Builder::new().prefix(path).temfidelir().unwrap();
        let importer = Arc::new(SSTImporter::new(dir.path(), None).unwrap());
        (dir, importer)
    }

    pub fn new_entry(term: u64, index: u64, set_data: bool) -> Entry {
        let mut e = Entry::default();
        e.set_index(index);
        e.set_term(term);
        if set_data {
            let mut cmd = Request::default();
            cmd.set_cmd_type(CmdType::Put);
            cmd.mut_put().set_key(b"key".to_vec());
            cmd.mut_put().set_value(b"value".to_vec());
            let mut req = VioletaBftCmdRequest::default();
            req.mut_requests().push(cmd);
            e.set_data(req.write_to_bytes().unwrap())
        }
        e
    }

    #[derive(Clone)]
    pub struct TestNotifier<EK: CausetEngine> {
        tx: lightlikeer<PeerMsg<EK>>,
    }

    impl<EK: CausetEngine> Notifier<EK> for TestNotifier<EK> {
        fn notify(&self, apply_res: Vec<ApplyRes<EK::Snapshot>>) {
            for r in apply_res {
                let res = TaskRes::Apply(r);
                let _ = self.tx.lightlike(PeerMsg::ApplyRes { res });
            }
        }
        fn notify_one(&self, _: u64, msg: PeerMsg<EK>) {
            let _ = self.tx.lightlike(msg);
        }
        fn clone_box(&self) -> Box<dyn Notifier<EK>> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn test_should_sync_log() {
        // Admin command
        let mut req = VioletaBftCmdRequest::default();
        req.mut_admin_request()
            .set_cmd_type(AdminCmdType::ComputeHash);
        assert_eq!(should_sync_log(&req), true);

        // IngestSst command
        let mut req = Request::default();
        req.set_cmd_type(CmdType::IngestSst);
        req.set_ingest_sst(IngestSstRequest::default());
        let mut cmd = VioletaBftCmdRequest::default();
        cmd.mut_requests().push(req);
        assert_eq!(should_write_to_engine(&cmd), true);
        assert_eq!(should_sync_log(&cmd), true);

        // Normal command
        let req = VioletaBftCmdRequest::default();
        assert_eq!(should_sync_log(&req), false);
    }

    #[test]
    fn test_should_write_to_engine() {
        // ComputeHash command
        let mut req = VioletaBftCmdRequest::default();
        req.mut_admin_request()
            .set_cmd_type(AdminCmdType::ComputeHash);
        assert_eq!(should_write_to_engine(&req), true);

        // IngestSst command
        let mut req = Request::default();
        req.set_cmd_type(CmdType::IngestSst);
        req.set_ingest_sst(IngestSstRequest::default());
        let mut cmd = VioletaBftCmdRequest::default();
        cmd.mut_requests().push(req);
        assert_eq!(should_write_to_engine(&cmd), true);
    }

    fn validate<F>(router: &ApplyRouter<LmdbEngine>, brane_id: u64, validate: F)
    where
        F: FnOnce(&Applypushdown_causet<LmdbEngine>) + lightlike + 'static,
    {
        let (validate_tx, validate_rx) = mpsc::channel();
        router.schedule_task(
            brane_id,
            Msg::Validate(
                brane_id,
                Box::new(move |pushdown_causet: *const u8| {
                    let pushdown_causet = unsafe { &*(pushdown_causet as *const Applypushdown_causet<LmdbEngine>) };
                    validate(pushdown_causet);
                    validate_tx.lightlike(()).unwrap();
                }),
            ),
        );
        validate_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }

    // Make sure msgs are handled in the same batch.
    fn batch_messages(
        router: &ApplyRouter<LmdbEngine>,
        brane_id: u64,
        msgs: Vec<Msg<LmdbEngine>>,
    ) {
        let (notify1, wait1) = mpsc::channel();
        let (notify2, wait2) = mpsc::channel();
        router.schedule_task(
            brane_id,
            Msg::Validate(
                brane_id,
                Box::new(move |_| {
                    notify1.lightlike(()).unwrap();
                    wait2.recv().unwrap();
                }),
            ),
        );
        wait1.recv().unwrap();

        for msg in msgs {
            router.schedule_task(brane_id, msg);
        }

        notify2.lightlike(()).unwrap();
    }

    fn fetch_apply_res(
        receiver: &::std::sync::mpsc::Receiver<PeerMsg<LmdbEngine>>,
    ) -> ApplyRes<LmdbSnapshot> {
        match receiver.recv_timeout(Duration::from_secs(3)) {
            Ok(PeerMsg::ApplyRes { res, .. }) => match res {
                TaskRes::Apply(res) => res,
                e => panic!("unexpected res {:?}", e),
            },
            e => panic!("unexpected res {:?}", e),
        }
    }

    fn proposal<S: Snapshot>(
        is_conf_change: bool,
        index: u64,
        term: u64,
        cb: Callback<S>,
    ) -> Proposal<S> {
        Proposal {
            is_conf_change,
            index,
            term,
            cb,
            renew_lease_time: None,
        }
    }

    fn apply<S: Snapshot>(
        peer_id: u64,
        brane_id: u64,
        term: u64,
        entries: Vec<Entry>,
        last_committed_index: u64,
        committed_term: u64,
        committed_index: u64,
        cbs: Vec<Proposal<S>>,
    ) -> Apply<S> {
        Apply::new(
            peer_id,
            brane_id,
            term,
            entries,
            last_committed_index,
            committed_index,
            committed_term,
            cbs,
        )
    }

    #[test]
    fn test_basic_flow() {
        let (tx, rx) = mpsc::channel();
        let lightlikeer = Box::new(TestNotifier { tx });
        let (_tmp, engine) = create_tmp_engine("apply-basic");
        let (_dir, importer) = create_tmp_importer("apply-basic");
        let (brane_interlock_semaphore, snapshot_rx) = dummy_interlock_semaphore();
        let causet = Arc::new(VersionTrack::new(Config::default()));
        let (router, mut system) = create_apply_batch_system(&causet.value());
        let plightlikeing_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<LmdbEngine, LmdbWriteBatch> {
            tag: "test-store".to_owned(),
            causet,
            interlock_host: InterlockHost::<LmdbEngine>::default(),
            importer,
            brane_interlock_semaphore,
            lightlikeer,
            engine,
            router: router.clone(),
            _phantom: Default::default(),
            store_id: 1,
            plightlikeing_create_peers,
        };
        system.spawn("test-basic".to_owned(), builder);

        let mut reg = Registration::default();
        reg.id = 1;
        reg.brane.set_id(2);
        reg.apply_state.set_applied_index(3);
        reg.term = 4;
        reg.applied_index_term = 5;
        router.schedule_task(2, Msg::Registration(reg.clone()));
        validate(&router, 2, move |pushdown_causet| {
            assert_eq!(pushdown_causet.id, 1);
            assert_eq!(pushdown_causet.tag, "[brane 2] 1");
            assert_eq!(pushdown_causet.brane, reg.brane);
            assert!(!pushdown_causet.plightlikeing_remove);
            assert_eq!(pushdown_causet.apply_state, reg.apply_state);
            assert_eq!(pushdown_causet.term, reg.term);
            assert_eq!(pushdown_causet.applied_index_term, reg.applied_index_term);
        });

        let (resp_tx, resp_rx) = mpsc::channel();
        let p = proposal(
            false,
            1,
            0,
            Callback::Write(Box::new(move |resp: WriteResponse| {
                resp_tx.lightlike(resp.response).unwrap();
            })),
        );
        router.schedule_task(
            1,
            Msg::apply(apply(
                1,
                1,
                0,
                vec![new_entry(0, 1, true)],
                1,
                0,
                1,
                vec![p],
            )),
        );
        // unregistered brane should be ignored and notify failed.
        let resp = resp_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_brane_not_found());
        assert!(rx.try_recv().is_err());

        let (cc_tx, cc_rx) = mpsc::channel();
        let pops = vec![
            proposal(
                false,
                4,
                4,
                Callback::Write(Box::new(move |write: WriteResponse| {
                    cc_tx.lightlike(write.response).unwrap();
                })),
            ),
            proposal(false, 4, 5, Callback::None),
        ];
        router.schedule_task(
            2,
            Msg::apply(apply(1, 2, 11, vec![new_entry(5, 4, true)], 3, 5, 4, pops)),
        );
        // proposal with not commit entry should be ignore
        validate(&router, 2, move |pushdown_causet| {
            assert_eq!(pushdown_causet.term, 11);
        });
        let cc_resp = cc_rx.try_recv().unwrap();
        assert!(cc_resp.get_header().get_error().has_stale_command());
        assert!(rx.recv_timeout(Duration::from_secs(3)).is_ok());

        // Make sure Apply and Snapshot are in the same batch.
        let (snap_tx, _) = mpsc::sync_channel(0);
        batch_messages(
            &router,
            2,
            vec![
                Msg::apply(apply(
                    1,
                    2,
                    11,
                    vec![new_entry(5, 5, false)],
                    5,
                    5,
                    5,
                    vec![],
                )),
                Msg::Snapshot(GenSnapTask::new(2, 0, snap_tx)),
            ],
        );
        let apply_res = match rx.recv_timeout(Duration::from_secs(3)) {
            Ok(PeerMsg::ApplyRes { res, .. }) => match res {
                TaskRes::Apply(res) => res,
                e => panic!("unexpected apply result: {:?}", e),
            },
            e => panic!("unexpected apply result: {:?}", e),
        };
        let apply_state_key = tuplespaceInstanton::apply_state_key(2);
        let apply_state = match snapshot_rx.recv_timeout(Duration::from_secs(3)) {
            Ok(Some(BraneTask::Gen { kv_snap, .. })) => kv_snap
                .get_msg_causet(Causet_VIOLETABFT, &apply_state_key)
                .unwrap()
                .unwrap(),
            e => panic!("unexpected apply result: {:?}", e),
        };
        assert_eq!(apply_res.brane_id, 2);
        assert_eq!(apply_res.apply_state, apply_state);
        assert_eq!(apply_res.apply_state.get_applied_index(), 5);
        assert!(apply_res.exec_res.is_empty());
        // empty entry will make applied_index step forward and should write apply state to engine.
        assert_eq!(apply_res.metrics.written_tuplespaceInstanton, 1);
        assert_eq!(apply_res.applied_index_term, 5);
        validate(&router, 2, |pushdown_causet| {
            assert_eq!(pushdown_causet.term, 11);
            assert_eq!(pushdown_causet.applied_index_term, 5);
            assert_eq!(pushdown_causet.apply_state.get_applied_index(), 5);
            assert_eq!(
                pushdown_causet.apply_state.get_applied_index(),
                pushdown_causet.last_sync_apply_index
            );
        });

        router.schedule_task(2, Msg::destroy(2, false));
        let (brane_id, peer_id) = match rx.recv_timeout(Duration::from_secs(3)) {
            Ok(PeerMsg::ApplyRes { res, .. }) => match res {
                TaskRes::Destroy {
                    brane_id, peer_id, ..
                } => (brane_id, peer_id),
                e => panic!("expected destroy result, but got {:?}", e),
            },
            e => panic!("expected destroy result, but got {:?}", e),
        };
        assert_eq!(peer_id, 1);
        assert_eq!(brane_id, 2);

        // Stopped peer should be removed.
        let (resp_tx, resp_rx) = mpsc::channel();
        let p = proposal(
            false,
            1,
            0,
            Callback::Write(Box::new(move |resp: WriteResponse| {
                resp_tx.lightlike(resp.response).unwrap();
            })),
        );
        router.schedule_task(
            2,
            Msg::apply(apply(
                1,
                1,
                0,
                vec![new_entry(0, 1, true)],
                1,
                0,
                1,
                vec![p],
            )),
        );
        // unregistered brane should be ignored and notify failed.
        let resp = resp_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(
            resp.get_header().get_error().has_brane_not_found(),
            "{:?}",
            resp
        );
        assert!(rx.try_recv().is_err());

        system.shutdown();
    }

    fn cb<S: Snapshot>(idx: u64, term: u64, tx: lightlikeer<VioletaBftCmdResponse>) -> Proposal<S> {
        proposal(
            false,
            idx,
            term,
            Callback::Write(Box::new(move |resp: WriteResponse| {
                tx.lightlike(resp.response).unwrap();
            })),
        )
    }

    struct EntryBuilder {
        entry: Entry,
        req: VioletaBftCmdRequest,
    }

    impl EntryBuilder {
        fn new(index: u64, term: u64) -> EntryBuilder {
            let req = VioletaBftCmdRequest::default();
            let mut entry = Entry::default();
            entry.set_index(index);
            entry.set_term(term);
            EntryBuilder { entry, req }
        }

        fn epoch(mut self, conf_ver: u64, version: u64) -> EntryBuilder {
            let mut epoch = BraneEpoch::default();
            epoch.set_version(version);
            epoch.set_conf_ver(conf_ver);
            self.req.mut_header().set_brane_epoch(epoch);
            self
        }

        fn put(self, key: &[u8], value: &[u8]) -> EntryBuilder {
            self.add_put_req(None, key, value)
        }

        fn put_causet(self, causet: &str, key: &[u8], value: &[u8]) -> EntryBuilder {
            self.add_put_req(Some(causet), key, value)
        }

        fn add_put_req(mut self, causet: Option<&str>, key: &[u8], value: &[u8]) -> EntryBuilder {
            let mut cmd = Request::default();
            cmd.set_cmd_type(CmdType::Put);
            if let Some(causet) = causet {
                cmd.mut_put().set_causet(causet.to_owned());
            }
            cmd.mut_put().set_key(key.to_vec());
            cmd.mut_put().set_value(value.to_vec());
            self.req.mut_requests().push(cmd);
            self
        }

        fn delete(self, key: &[u8]) -> EntryBuilder {
            self.add_delete_req(None, key)
        }

        fn delete_causet(self, causet: &str, key: &[u8]) -> EntryBuilder {
            self.add_delete_req(Some(causet), key)
        }

        fn delete_cone(self, spacelike_key: &[u8], lightlike_key: &[u8]) -> EntryBuilder {
            self.add_delete_cone_req(None, spacelike_key, lightlike_key)
        }

        fn delete_cone_causet(self, causet: &str, spacelike_key: &[u8], lightlike_key: &[u8]) -> EntryBuilder {
            self.add_delete_cone_req(Some(causet), spacelike_key, lightlike_key)
        }

        fn add_delete_req(mut self, causet: Option<&str>, key: &[u8]) -> EntryBuilder {
            let mut cmd = Request::default();
            cmd.set_cmd_type(CmdType::Delete);
            if let Some(causet) = causet {
                cmd.mut_delete().set_causet(causet.to_owned());
            }
            cmd.mut_delete().set_key(key.to_vec());
            self.req.mut_requests().push(cmd);
            self
        }

        fn add_delete_cone_req(
            mut self,
            causet: Option<&str>,
            spacelike_key: &[u8],
            lightlike_key: &[u8],
        ) -> EntryBuilder {
            let mut cmd = Request::default();
            cmd.set_cmd_type(CmdType::DeleteCone);
            if let Some(causet) = causet {
                cmd.mut_delete_cone().set_causet(causet.to_owned());
            }
            cmd.mut_delete_cone().set_spacelike_key(spacelike_key.to_vec());
            cmd.mut_delete_cone().set_lightlike_key(lightlike_key.to_vec());
            self.req.mut_requests().push(cmd);
            self
        }

        fn ingest_sst(mut self, meta: &SstMeta) -> EntryBuilder {
            let mut cmd = Request::default();
            cmd.set_cmd_type(CmdType::IngestSst);
            cmd.mut_ingest_sst().set_sst(meta.clone());
            self.req.mut_requests().push(cmd);
            self
        }

        fn split(mut self, splits: BatchSplitRequest) -> EntryBuilder {
            let mut req = AdminRequest::default();
            req.set_cmd_type(AdminCmdType::BatchSplit);
            req.set_splits(splits);
            self.req.set_admin_request(req);
            self
        }

        fn build(mut self) -> Entry {
            self.entry.set_data(self.req.write_to_bytes().unwrap());
            self.entry
        }
    }

    #[derive(Clone, Default)]
    struct ApplySemaphore {
        pre_admin_count: Arc<AtomicUsize>,
        pre_query_count: Arc<AtomicUsize>,
        post_admin_count: Arc<AtomicUsize>,
        post_query_count: Arc<AtomicUsize>,
        cmd_batches: RefCell<Vec<CmdBatch>>,
        cmd_sink: Option<Arc<Mutex<lightlikeer<CmdBatch>>>>,
    }

    impl Interlock for ApplySemaphore {}

    impl QuerySemaphore for ApplySemaphore {
        fn pre_apply_query(&self, _: &mut SemaphoreContext<'_>, _: &[Request]) {
            self.pre_query_count.fetch_add(1, Ordering::SeqCst);
        }

        fn post_apply_query(&self, _: &mut SemaphoreContext<'_>, _: &mut Cmd) {
            self.post_query_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl CmdSemaphore<LmdbEngine> for ApplySemaphore {
        fn on_prepare_for_apply(&self, observe_id: ObserveID, brane_id: u64) {
            self.cmd_batches
                .borrow_mut()
                .push(CmdBatch::new(observe_id, brane_id));
        }

        fn on_apply_cmd(&self, observe_id: ObserveID, brane_id: u64, cmd: Cmd) {
            self.cmd_batches
                .borrow_mut()
                .last_mut()
                .expect("should exist some cmd batch")
                .push(observe_id, brane_id, cmd);
        }

        fn on_flush_apply(&self, _: LmdbEngine) {
            if !self.cmd_batches.borrow().is_empty() {
                let batches = self.cmd_batches.replace(Vec::default());
                for b in batches {
                    if let Some(sink) = self.cmd_sink.as_ref() {
                        sink.dagger().unwrap().lightlike(b).unwrap();
                    }
                }
            }
        }
    }

    #[test]
    fn test_handle_violetabft_committed_entries() {
        let (_path, engine) = create_tmp_engine("test-pushdown_causet");
        let (import_dir, importer) = create_tmp_importer("test-pushdown_causet");
        let obs = ApplySemaphore::default();
        let mut host = InterlockHost::<LmdbEngine>::default();
        host.registry
            .register_query_semaphore(1, BoxQuerySemaphore::new(obs.clone()));

        let (tx, rx) = mpsc::channel();
        let (brane_interlock_semaphore, _) = dummy_interlock_semaphore();
        let lightlikeer = Box::new(TestNotifier { tx });
        let causet = Arc::new(VersionTrack::new(Config::default()));
        let (router, mut system) = create_apply_batch_system(&causet.value());
        let plightlikeing_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<LmdbEngine, LmdbWriteBatch> {
            tag: "test-store".to_owned(),
            causet,
            lightlikeer,
            brane_interlock_semaphore,
            interlock_host: host,
            importer: importer.clone(),
            engine: engine.clone(),
            router: router.clone(),
            _phantom: Default::default(),
            store_id: 1,
            plightlikeing_create_peers,
        };
        system.spawn("test-handle-violetabft".to_owned(), builder);

        let peer_id = 3;
        let mut reg = Registration::default();
        reg.id = peer_id;
        reg.brane.set_id(1);
        reg.brane.mut_peers().push(new_peer(2, 3));
        reg.brane.set_lightlike_key(b"k5".to_vec());
        reg.brane.mut_brane_epoch().set_conf_ver(1);
        reg.brane.mut_brane_epoch().set_version(3);
        router.schedule_task(1, Msg::Registration(reg));

        let (capture_tx, capture_rx) = mpsc::channel();
        let put_entry = EntryBuilder::new(1, 1)
            .put(b"k1", b"v1")
            .put(b"k2", b"v1")
            .put(b"k3", b"v1")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                1,
                vec![put_entry],
                0,
                1,
                1,
                vec![cb(1, 1, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(resp.get_responses().len(), 3);
        let dk_k1 = tuplespaceInstanton::data_key(b"k1");
        let dk_k2 = tuplespaceInstanton::data_key(b"k2");
        let dk_k3 = tuplespaceInstanton::data_key(b"k3");
        assert_eq!(engine.get_value(&dk_k1).unwrap().unwrap(), b"v1");
        assert_eq!(engine.get_value(&dk_k2).unwrap().unwrap(), b"v1");
        assert_eq!(engine.get_value(&dk_k3).unwrap().unwrap(), b"v1");
        validate(&router, 1, |pushdown_causet| {
            assert_eq!(pushdown_causet.applied_index_term, 1);
            assert_eq!(pushdown_causet.apply_state.get_applied_index(), 1);
        });
        fetch_apply_res(&rx);

        let put_entry = EntryBuilder::new(2, 2)
            .put_causet(Causet_DAGGER, b"k1", b"v1")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(peer_id, 1, 2, vec![put_entry], 1, 2, 2, vec![])),
        );
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.brane_id, 1);
        assert_eq!(apply_res.apply_state.get_applied_index(), 2);
        assert_eq!(apply_res.applied_index_term, 2);
        assert!(apply_res.exec_res.is_empty());
        assert!(apply_res.metrics.written_bytes >= 5);
        assert_eq!(apply_res.metrics.written_tuplespaceInstanton, 2);
        assert_eq!(apply_res.metrics.size_diff_hint, 5);
        assert_eq!(apply_res.metrics.lock_causet_written_bytes, 5);
        assert_eq!(
            engine.get_value_causet(Causet_DAGGER, &dk_k1).unwrap().unwrap(),
            b"v1"
        );

        let put_entry = EntryBuilder::new(3, 2)
            .put(b"k2", b"v2")
            .epoch(1, 1)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                2,
                vec![put_entry],
                2,
                2,
                3,
                vec![cb(3, 2, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_epoch_not_match());
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_index_term, 2);
        assert_eq!(apply_res.apply_state.get_applied_index(), 3);

        let put_entry = EntryBuilder::new(4, 2)
            .put(b"k3", b"v3")
            .put(b"k5", b"v5")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                2,
                vec![put_entry],
                3,
                2,
                4,
                vec![cb(4, 2, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_brane());
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_index_term, 2);
        assert_eq!(apply_res.apply_state.get_applied_index(), 4);
        // a writebatch should be atomic.
        assert_eq!(engine.get_value(&dk_k3).unwrap().unwrap(), b"v1");

        let put_entry = EntryBuilder::new(5, 3)
            .delete(b"k1")
            .delete_causet(Causet_DAGGER, b"k1")
            .delete_causet(Causet_WRITE, b"k1")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                vec![put_entry],
                4,
                3,
                5,
                vec![cb(5, 2, capture_tx.clone()), cb(5, 3, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        // stale command should be cleared.
        assert!(resp.get_header().get_error().has_stale_command());
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert!(engine.get_value(&dk_k1).unwrap().is_none());
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.metrics.lock_causet_written_bytes, 3);
        assert_eq!(apply_res.metrics.delete_tuplespaceInstanton_hint, 2);
        assert_eq!(apply_res.metrics.size_diff_hint, -9);

        let delete_entry = EntryBuilder::new(6, 3).delete(b"k5").epoch(1, 3).build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                vec![delete_entry],
                5,
                3,
                6,
                vec![cb(6, 3, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_brane());
        fetch_apply_res(&rx);

        let delete_cone_entry = EntryBuilder::new(7, 3)
            .delete_cone(b"", b"")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                vec![delete_cone_entry],
                6,
                3,
                7,
                vec![cb(7, 3, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_brane());
        assert_eq!(engine.get_value(&dk_k3).unwrap().unwrap(), b"v1");
        fetch_apply_res(&rx);

        let delete_cone_entry = EntryBuilder::new(8, 3)
            .delete_cone_causet(Causet_DEFAULT, b"", b"k5")
            .delete_cone_causet(Causet_DAGGER, b"", b"k5")
            .delete_cone_causet(Causet_WRITE, b"", b"k5")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                vec![delete_cone_entry],
                7,
                3,
                8,
                vec![cb(8, 3, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert!(engine.get_value(&dk_k1).unwrap().is_none());
        assert!(engine.get_value(&dk_k2).unwrap().is_none());
        assert!(engine.get_value(&dk_k3).unwrap().is_none());
        fetch_apply_res(&rx);

        // UploadSST
        let sst_path = import_dir.path().join("test.sst");
        let mut sst_epoch = BraneEpoch::default();
        sst_epoch.set_conf_ver(1);
        sst_epoch.set_version(3);
        let sst_cone = (0, 100);
        let (mut meta1, data1) = gen_sst_file(&sst_path, sst_cone);
        meta1.set_brane_id(1);
        meta1.set_brane_epoch(sst_epoch);
        let mut file1 = importer.create(&meta1).unwrap();
        file1.applightlike(&data1).unwrap();
        file1.finish().unwrap();
        let (mut meta2, data2) = gen_sst_file(&sst_path, sst_cone);
        meta2.set_brane_id(1);
        meta2.mut_brane_epoch().set_conf_ver(1);
        meta2.mut_brane_epoch().set_version(1234);
        let mut file2 = importer.create(&meta2).unwrap();
        file2.applightlike(&data2).unwrap();
        file2.finish().unwrap();

        // IngestSst
        let put_ok = EntryBuilder::new(9, 3)
            .put(&[sst_cone.0], &[sst_cone.1])
            .epoch(0, 3)
            .build();
        // Add a put above to test flush before ingestion.
        let capture_tx_clone = capture_tx.clone();
        let ingest_ok = EntryBuilder::new(10, 3)
            .ingest_sst(&meta1)
            .epoch(0, 3)
            .build();
        let ingest_epoch_not_match = EntryBuilder::new(11, 3)
            .ingest_sst(&meta2)
            .epoch(0, 3)
            .build();
        let entries = vec![put_ok, ingest_ok, ingest_epoch_not_match];
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                entries,
                8,
                3,
                11,
                vec![
                    cb(9, 3, capture_tx.clone()),
                    proposal(
                        false,
                        10,
                        3,
                        Callback::Write(Box::new(move |resp: WriteResponse| {
                            // Sleep until yield timeout.
                            thread::sleep(Duration::from_millis(500));
                            capture_tx_clone.lightlike(resp.response).unwrap();
                        })),
                    ),
                    cb(11, 3, capture_tx.clone()),
                ],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        check_db_cone(&engine, sst_cone);
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().has_error());
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_index_term, 3);
        assert_eq!(apply_res.apply_state.get_applied_index(), 10);
        // The brane will yield after timeout.
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_index_term, 3);
        assert_eq!(apply_res.apply_state.get_applied_index(), 11);

        let write_batch_max_tuplespaceInstanton = <LmdbEngine as WriteBatchExt>::WRITE_BATCH_MAX_KEYS;

        let mut props = vec![];
        let mut entries = vec![];
        for i in 0..write_batch_max_tuplespaceInstanton {
            let put_entry = EntryBuilder::new(i as u64 + 12, 3)
                .put(b"k", b"v")
                .epoch(1, 3)
                .build();
            entries.push(put_entry);
            props.push(cb(i as u64 + 12, 3, capture_tx.clone()));
        }
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                entries,
                11,
                3,
                write_batch_max_tuplespaceInstanton as u64 + 11,
                props,
            )),
        );
        for _ in 0..write_batch_max_tuplespaceInstanton {
            capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        let index = write_batch_max_tuplespaceInstanton + 11;
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.apply_state.get_applied_index(), index as u64);
        assert_eq!(obs.pre_query_count.load(Ordering::SeqCst), index);
        assert_eq!(obs.post_query_count.load(Ordering::SeqCst), index);

        system.shutdown();
    }

    #[test]
    fn test_cmd_semaphore() {
        let (_path, engine) = create_tmp_engine("test-pushdown_causet");
        let (_import_dir, importer) = create_tmp_importer("test-pushdown_causet");
        let mut host = InterlockHost::<LmdbEngine>::default();
        let mut obs = ApplySemaphore::default();
        let (sink, cmdbatch_rx) = mpsc::channel();
        obs.cmd_sink = Some(Arc::new(Mutex::new(sink)));
        host.registry
            .register_cmd_semaphore(1, BoxCmdSemaphore::new(obs));

        let (tx, rx) = mpsc::channel();
        let (brane_interlock_semaphore, _) = dummy_interlock_semaphore();
        let lightlikeer = Box::new(TestNotifier { tx });
        let causet = Config::default();
        let (router, mut system) = create_apply_batch_system(&causet);
        let plightlikeing_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<LmdbEngine, LmdbWriteBatch> {
            tag: "test-store".to_owned(),
            causet: Arc::new(VersionTrack::new(causet)),
            lightlikeer,
            brane_interlock_semaphore,
            interlock_host: host,
            importer,
            engine,
            router: router.clone(),
            _phantom: Default::default(),
            store_id: 1,
            plightlikeing_create_peers,
        };
        system.spawn("test-handle-violetabft".to_owned(), builder);

        let peer_id = 3;
        let mut reg = Registration::default();
        reg.id = peer_id;
        reg.brane.set_id(1);
        reg.brane.mut_peers().push(new_peer(2, 3));
        reg.brane.set_lightlike_key(b"k5".to_vec());
        reg.brane.mut_brane_epoch().set_conf_ver(1);
        reg.brane.mut_brane_epoch().set_version(3);
        let brane_epoch = reg.brane.get_brane_epoch().clone();
        router.schedule_task(1, Msg::Registration(reg));

        let put_entry = EntryBuilder::new(1, 1)
            .put(b"k1", b"v1")
            .put(b"k2", b"v1")
            .put(b"k3", b"v1")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(peer_id, 1, 1, vec![put_entry], 0, 1, 1, vec![])),
        );
        fetch_apply_res(&rx);
        // It must receive nothing because no brane registered.
        cmdbatch_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();
        let (block_tx, block_rx) = mpsc::channel::<()>();
        router.schedule_task(
            1,
            Msg::Validate(
                1,
                Box::new(move |_| {
                    // Block the apply worker
                    block_rx.recv().unwrap();
                }),
            ),
        );
        let put_entry = EntryBuilder::new(2, 2)
            .put(b"k0", b"v0")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(peer_id, 1, 2, vec![put_entry], 1, 2, 2, vec![])),
        );
        // Register cmd semaphore to brane 1.
        let enabled = Arc::new(AtomicBool::new(true));
        let observe_id = ObserveID::new();
        router.schedule_task(
            1,
            Msg::Change {
                brane_epoch: brane_epoch.clone(),
                cmd: ChangeCmd::RegisterSemaphore {
                    observe_id,
                    brane_id: 1,
                    enabled: enabled.clone(),
                },
                cb: Callback::Read(Box::new(|resp: ReadResponse<LmdbSnapshot>| {
                    assert!(!resp.response.get_header().has_error());
                    assert!(resp.snapshot.is_some());
                    let snap = resp.snapshot.unwrap();
                    assert_eq!(snap.get_value(b"k0").unwrap().unwrap(), b"v0");
                })),
            },
        );
        // Unblock the apply worker
        block_tx.lightlike(()).unwrap();
        fetch_apply_res(&rx);
        let (capture_tx, capture_rx) = mpsc::channel();
        let put_entry = EntryBuilder::new(3, 2)
            .put_causet(Causet_DAGGER, b"k1", b"v1")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                2,
                vec![put_entry],
                2,
                2,
                3,
                vec![cb(3, 2, capture_tx)],
            )),
        );
        fetch_apply_res(&rx);
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(resp.get_responses().len(), 1);
        let cmd_batch = cmdbatch_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(resp, cmd_batch.into_iter(1).next().unwrap().response);

        let put_entry1 = EntryBuilder::new(4, 2)
            .put(b"k2", b"v2")
            .epoch(1, 3)
            .build();
        let put_entry2 = EntryBuilder::new(5, 2)
            .put(b"k2", b"v2")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                2,
                vec![put_entry1, put_entry2],
                3,
                2,
                5,
                vec![],
            )),
        );
        let cmd_batch = cmdbatch_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(2, cmd_batch.len());

        // Stop semaphore regoin 1.
        enabled.store(false, Ordering::SeqCst);
        let put_entry = EntryBuilder::new(6, 2)
            .put(b"k2", b"v2")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(peer_id, 1, 2, vec![put_entry], 5, 2, 6, vec![])),
        );
        // Must not receive new cmd.
        cmdbatch_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // Must response a BraneNotFound error.
        router.schedule_task(
            2,
            Msg::Change {
                brane_epoch,
                cmd: ChangeCmd::RegisterSemaphore {
                    observe_id,
                    brane_id: 2,
                    enabled,
                },
                cb: Callback::Read(Box::new(|resp: ReadResponse<_>| {
                    assert!(resp
                        .response
                        .get_header()
                        .get_error()
                        .has_brane_not_found());
                    assert!(resp.snapshot.is_none());
                })),
            },
        );

        system.shutdown();
    }

    #[test]
    fn test_check_sst_for_ingestion() {
        let mut sst = SstMeta::default();
        let mut brane = Brane::default();

        // Check uuid and causet name
        assert!(check_sst_for_ingestion(&sst, &brane).is_err());
        sst.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        sst.set_causet_name(Causet_DEFAULT.to_owned());
        check_sst_for_ingestion(&sst, &brane).unwrap();
        sst.set_causet_name("test".to_owned());
        assert!(check_sst_for_ingestion(&sst, &brane).is_err());
        sst.set_causet_name(Causet_WRITE.to_owned());
        check_sst_for_ingestion(&sst, &brane).unwrap();

        // Check brane id
        brane.set_id(1);
        sst.set_brane_id(2);
        assert!(check_sst_for_ingestion(&sst, &brane).is_err());
        sst.set_brane_id(1);
        check_sst_for_ingestion(&sst, &brane).unwrap();

        // Check brane epoch
        brane.mut_brane_epoch().set_conf_ver(1);
        assert!(check_sst_for_ingestion(&sst, &brane).is_err());
        sst.mut_brane_epoch().set_conf_ver(1);
        check_sst_for_ingestion(&sst, &brane).unwrap();
        brane.mut_brane_epoch().set_version(1);
        assert!(check_sst_for_ingestion(&sst, &brane).is_err());
        sst.mut_brane_epoch().set_version(1);
        check_sst_for_ingestion(&sst, &brane).unwrap();

        // Check brane cone
        brane.set_spacelike_key(vec![2]);
        brane.set_lightlike_key(vec![8]);
        sst.mut_cone().set_spacelike(vec![1]);
        sst.mut_cone().set_lightlike(vec![8]);
        assert!(check_sst_for_ingestion(&sst, &brane).is_err());
        sst.mut_cone().set_spacelike(vec![2]);
        assert!(check_sst_for_ingestion(&sst, &brane).is_err());
        sst.mut_cone().set_lightlike(vec![7]);
        check_sst_for_ingestion(&sst, &brane).unwrap();
    }

    fn new_split_req(key: &[u8], id: u64, children: Vec<u64>) -> SplitRequest {
        let mut req = SplitRequest::default();
        req.set_split_key(key.to_vec());
        req.set_new_brane_id(id);
        req.set_new_peer_ids(children);
        req
    }

    struct SplitResultChecker<'a> {
        engine: LmdbEngine,
        origin_peers: &'a [meta_timeshare::Peer],
        epoch: Rc<RefCell<BraneEpoch>>,
    }

    impl<'a> SplitResultChecker<'a> {
        fn check(&self, spacelike: &[u8], lightlike: &[u8], id: u64, children: &[u64], check_initial: bool) {
            let key = tuplespaceInstanton::brane_state_key(id);
            let state: BraneLocalState = self.engine.get_msg_causet(Causet_VIOLETABFT, &key).unwrap().unwrap();
            assert_eq!(state.get_state(), PeerState::Normal);
            assert_eq!(state.get_brane().get_id(), id);
            assert_eq!(state.get_brane().get_spacelike_key(), spacelike);
            assert_eq!(state.get_brane().get_lightlike_key(), lightlike);
            let expect_peers: Vec<_> = self
                .origin_peers
                .iter()
                .zip(children)
                .map(|(p, new_id)| {
                    let mut new_peer = meta_timeshare::Peer::clone(p);
                    new_peer.set_id(*new_id);
                    new_peer
                })
                .collect();
            assert_eq!(state.get_brane().get_peers(), expect_peers.as_slice());
            assert!(!state.has_merge_state(), "{:?}", state);
            let epoch = self.epoch.borrow();
            assert_eq!(*state.get_brane().get_brane_epoch(), *epoch);
            if !check_initial {
                return;
            }
            let key = tuplespaceInstanton::apply_state_key(id);
            let initial_state: VioletaBftApplyState =
                self.engine.get_msg_causet(Causet_VIOLETABFT, &key).unwrap().unwrap();
            assert_eq!(initial_state.get_applied_index(), VIOLETABFT_INIT_LOG_INDEX);
            assert_eq!(
                initial_state.get_truncated_state().get_index(),
                VIOLETABFT_INIT_LOG_INDEX
            );
            assert_eq!(
                initial_state.get_truncated_state().get_term(),
                VIOLETABFT_INIT_LOG_INDEX
            );
        }
    }

    fn error_msg(resp: &VioletaBftCmdResponse) -> &str {
        resp.get_header().get_error().get_message()
    }

    #[test]
    fn test_split() {
        let (_path, engine) = create_tmp_engine("test-pushdown_causet");
        let (_import_dir, importer) = create_tmp_importer("test-pushdown_causet");
        let peer_id = 3;
        let mut reg = Registration::default();
        reg.id = peer_id;
        reg.term = 1;
        reg.brane.set_id(1);
        reg.brane.set_lightlike_key(b"k5".to_vec());
        reg.brane.mut_brane_epoch().set_version(3);
        let brane_epoch = reg.brane.get_brane_epoch().clone();
        let peers = vec![new_peer(2, 3), new_peer(4, 5), new_learner_peer(6, 7)];
        reg.brane.set_peers(peers.clone().into());
        let (tx, _rx) = mpsc::channel();
        let lightlikeer = Box::new(TestNotifier { tx });
        let mut host = InterlockHost::<LmdbEngine>::default();
        let mut obs = ApplySemaphore::default();
        let (sink, cmdbatch_rx) = mpsc::channel();
        obs.cmd_sink = Some(Arc::new(Mutex::new(sink)));
        host.registry
            .register_cmd_semaphore(1, BoxCmdSemaphore::new(obs));
        let (brane_interlock_semaphore, _) = dummy_interlock_semaphore();
        let causet = Arc::new(VersionTrack::new(Config::default()));
        let (router, mut system) = create_apply_batch_system(&causet.value());
        let plightlikeing_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<LmdbEngine, LmdbWriteBatch> {
            tag: "test-store".to_owned(),
            causet,
            lightlikeer,
            importer,
            brane_interlock_semaphore,
            interlock_host: host,
            engine: engine.clone(),
            router: router.clone(),
            _phantom: Default::default(),
            store_id: 2,
            plightlikeing_create_peers,
        };
        system.spawn("test-split".to_owned(), builder);

        router.schedule_task(1, Msg::Registration(reg.clone()));
        let enabled = Arc::new(AtomicBool::new(true));
        let observe_id = ObserveID::new();
        router.schedule_task(
            1,
            Msg::Change {
                brane_epoch: brane_epoch.clone(),
                cmd: ChangeCmd::RegisterSemaphore {
                    observe_id,
                    brane_id: 1,
                    enabled: enabled.clone(),
                },
                cb: Callback::Read(Box::new(|resp: ReadResponse<_>| {
                    assert!(!resp.response.get_header().has_error(), "{:?}", resp);
                    assert!(resp.snapshot.is_some());
                })),
            },
        );

        let mut index_id = 1;
        let (capture_tx, capture_rx) = mpsc::channel();
        let epoch = Rc::new(RefCell::new(reg.brane.get_brane_epoch().to_owned()));
        let epoch_ = epoch.clone();
        let mut exec_split = |router: &ApplyRouter<LmdbEngine>, reqs| {
            let epoch = epoch_.borrow();
            let split = EntryBuilder::new(index_id, 1)
                .split(reqs)
                .epoch(epoch.get_conf_ver(), epoch.get_version())
                .build();
            router.schedule_task(
                1,
                Msg::apply(apply(
                    peer_id,
                    1,
                    1,
                    vec![split],
                    index_id - 1,
                    1,
                    index_id,
                    vec![cb(index_id, 1, capture_tx.clone())],
                )),
            );
            index_id += 1;
            capture_rx.recv_timeout(Duration::from_secs(3)).unwrap()
        };

        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(true);
        splits.mut_requests().push(new_split_req(b"k1", 8, vec![]));
        let resp = exec_split(&router, splits.clone());
        // 3 followers are required.
        assert!(error_msg(&resp).contains("id count"), "{:?}", resp);
        cmdbatch_rx.recv_timeout(Duration::from_secs(3)).unwrap();

        splits.mut_requests().clear();
        let resp = exec_split(&router, splits.clone());
        // Empty requests should be rejected.
        assert!(error_msg(&resp).contains("missing"), "{:?}", resp);

        splits
            .mut_requests()
            .push(new_split_req(b"k6", 8, vec![9, 10, 11]));
        let resp = exec_split(&router, splits.clone());
        // Out of cone tuplespaceInstanton should be rejected.
        assert!(
            resp.get_header().get_error().has_key_not_in_brane(),
            "{:?}",
            resp
        );

        splits
            .mut_requests()
            .push(new_split_req(b"", 8, vec![9, 10, 11]));
        let resp = exec_split(&router, splits.clone());
        // Empty key should be rejected.
        assert!(error_msg(&resp).contains("missing"), "{:?}", resp);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 8, vec![9, 10, 11]));
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 8, vec![9, 10, 11]));
        let resp = exec_split(&router, splits.clone());
        // tuplespaceInstanton should be in asclightlike order.
        assert!(error_msg(&resp).contains("invalid"), "{:?}", resp);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 8, vec![9, 10, 11]));
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 8, vec![9, 10]));
        let resp = exec_split(&router, splits.clone());
        // All requests should be checked.
        assert!(error_msg(&resp).contains("id count"), "{:?}", resp);
        let checker = SplitResultChecker {
            engine,
            origin_peers: &peers,
            epoch: epoch.clone(),
        };

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 8, vec![9, 10, 11]));
        let resp = exec_split(&router, splits.clone());
        // Split should succeed.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        let mut new_version = epoch.borrow().get_version() + 1;
        epoch.borrow_mut().set_version(new_version);
        checker.check(b"", b"k1", 8, &[9, 10, 11], true);
        checker.check(b"k1", b"k5", 1, &[3, 5, 7], false);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k4", 12, vec![13, 14, 15]));
        splits.set_right_derive(false);
        let resp = exec_split(&router, splits.clone());
        // Right derive should be respected.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        new_version = epoch.borrow().get_version() + 1;
        epoch.borrow_mut().set_version(new_version);
        checker.check(b"k4", b"k5", 12, &[13, 14, 15], true);
        checker.check(b"k1", b"k4", 1, &[3, 5, 7], false);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 16, vec![17, 18, 19]));
        splits
            .mut_requests()
            .push(new_split_req(b"k3", 20, vec![21, 22, 23]));
        splits.set_right_derive(true);
        let resp = exec_split(&router, splits.clone());
        // Right derive should be respected.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        new_version = epoch.borrow().get_version() + 2;
        epoch.borrow_mut().set_version(new_version);
        checker.check(b"k1", b"k2", 16, &[17, 18, 19], true);
        checker.check(b"k2", b"k3", 20, &[21, 22, 23], true);
        checker.check(b"k3", b"k4", 1, &[3, 5, 7], false);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k31", 24, vec![25, 26, 27]));
        splits
            .mut_requests()
            .push(new_split_req(b"k32", 28, vec![29, 30, 31]));
        splits.set_right_derive(false);
        let resp = exec_split(&router, splits);
        // Right derive should be respected.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        new_version = epoch.borrow().get_version() + 2;
        epoch.borrow_mut().set_version(new_version);
        checker.check(b"k3", b"k31", 1, &[3, 5, 7], false);
        checker.check(b"k31", b"k32", 24, &[25, 26, 27], true);
        checker.check(b"k32", b"k4", 28, &[29, 30, 31], true);

        let (tx, rx) = mpsc::channel();
        enabled.store(false, Ordering::SeqCst);
        router.schedule_task(
            1,
            Msg::Change {
                brane_epoch,
                cmd: ChangeCmd::RegisterSemaphore {
                    observe_id,
                    brane_id: 1,
                    enabled: Arc::new(AtomicBool::new(true)),
                },
                cb: Callback::Read(Box::new(move |resp: ReadResponse<_>| {
                    assert!(
                        resp.response.get_header().get_error().has_epoch_not_match(),
                        "{:?}",
                        resp
                    );
                    assert!(resp.snapshot.is_none());
                    tx.lightlike(()).unwrap();
                })),
            },
        );
        rx.recv_timeout(Duration::from_millis(500)).unwrap();

        system.shutdown();
    }

    #[test]
    fn plightlikeing_cmd_leak() {
        let res = panic_hook::recover_safe(|| {
            let _cmd = PlightlikeingCmd::<LmdbSnapshot>::new(1, 1, Callback::None);
        });
        res.unwrap_err();
    }

    #[test]
    fn plightlikeing_cmd_leak_dtor_not_abort() {
        let res = panic_hook::recover_safe(|| {
            let _cmd = PlightlikeingCmd::<LmdbSnapshot>::new(1, 1, Callback::None);
            panic!("Don't abort");
            // It would abort and fail if there was a double-panic in PlightlikeingCmd dtor.
        });
        res.unwrap_err();
    }
}
