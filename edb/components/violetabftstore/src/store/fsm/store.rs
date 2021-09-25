// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::cmp::{Ord, Ordering as CmpOrdering};
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{mem, thread, u64};

use batch_system::{BasicMailbox, BatchRouter, BatchSystem, Fsm, HandlerBuilder, PollHandler};
use crossbeam::channel::{TryRecvError, TrylightlikeError};
use engine_lmdb::{PerfContext, PerfLevel};
use edb::{Engines, CausetEngine, MuBlock, WriteBatch, WriteBatchExt, WriteOptions};
use edb::{Causet_DEFAULT, Causet_DAGGER, Causet_VIOLETABFT, Causet_WRITE};
use futures::compat::Future01CompatExt;
use futures::FutureExt;
use ekvproto::import_sst_timeshare::SstMeta;
use ekvproto::meta_timeshare::{self, Brane, BraneEpoch};
use ekvproto::fidel_timeshare::StoreStats;
use ekvproto::violetabft_cmd_timeshare::{AdminCmdType, AdminRequest};
use ekvproto::violetabft_server_timeshare::{ExtraMessageType, PeerState, VioletaBftMessage, BraneLocalState};
use ekvproto::replication_mode_timeshare::{ReplicationMode, ReplicationStatus};
use protobuf::Message;
use violetabft::{Ready, StateRole};
use time::{self, Timespec};

use edb::CompactedEvent;
use edb::{VioletaBftEngine, VioletaBftLogBatch};
use tuplespaceInstanton::{self, data_lightlike_key, data_key, enc_lightlike_key, enc_spacelike_key};
use fidel_client::FidelClient;
use sst_importer::SSTImporter;
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::config::{Tracker, VersionTrack};
use violetabftstore::interlock::::mpsc::{self, LooseBoundedlightlikeer, Receiver};
use violetabftstore::interlock::::time::{duration_to_sec, Instant as TiInstant};
use violetabftstore::interlock::::timer::SteadyTimer;
use violetabftstore::interlock::::worker::{FutureInterlock_Semaphore, FutureWorker, Interlock_Semaphore, Worker};
use violetabftstore::interlock::::{is_zero_duration, sys as sys_util, Either, RingQueue};

use crate::interlock::split_semaphore::SplitSemaphore;
use crate::interlock::{BoxAdminSemaphore, InterlockHost, BraneChangeEvent};
use crate::observe_perf_context_type;
use crate::report_perf_context;
use crate::store::config::Config;
use crate::store::fsm::metrics::*;
use crate::store::fsm::peer::{
    maybe_destroy_source, new_admin_request, PeerFsm, PeerFsmpushdown_causet, lightlikeerFsmPair,
};
use crate::store::fsm::ApplyNotifier;
use crate::store::fsm::ApplyTaskRes;
use crate::store::fsm::{
    create_apply_batch_system, ApplyBatchSystem, ApplyPollerBuilder, ApplyRes, ApplyRouter,
};
use crate::store::local_metrics::VioletaBftMetrics;
use crate::store::metrics::*;
use crate::store::peer_causet_storage::{self, HandleVioletaBftReadyContext, InvokeContext};
use crate::store::transport::Transport;
use crate::store::util::{is_initial_msg, PerfContextStatistics};
use crate::store::worker::{
    AutoSplitController, CleanupRunner, CleanupSSTRunner, CleanupSSTTask, CleanupTask,
    CompactRunner, CompactTask, ConsistencyCheckRunner, ConsistencyCheckTask, FidelRunner,
    VioletaBftlogGcRunner, VioletaBftlogGcTask, Readpushdown_causet, BraneRunner, BraneTask, SplitCheckTask,
};
use crate::store::FidelTask;
use crate::store::PeerTicks;
use crate::store::{
    util, Callback, CasualMessage, GlobalReplicationState, MergeResultKind, PeerMsg, VioletaBftCommand,
    SignificantMsg, SnapManager, StoreMsg, StoreTick,
};
use crate::Result;
use interlocking_directorate::ConcurrencyManager;
use violetabftstore::interlock::::future::poll_future_notify;

type Key = Vec<u8>;

const KV_WB_SHRINK_SIZE: usize = 256 * 1024;
const VIOLETABFT_WB_SHRINK_SIZE: usize = 1024 * 1024;
pub const PENDING_VOTES_CAP: usize = 20;
const UNREACHABLE_BACKOFF: Duration = Duration::from_secs(10);

pub struct StoreInfo<E> {
    pub engine: E,
    pub capacity: u64,
}

pub struct StoreMeta {
    /// store id
    pub store_id: Option<u64>,
    /// brane_lightlike_key -> brane_id
    pub brane_cones: BTreeMap<Vec<u8>, u64>,
    /// brane_id -> brane
    pub branes: HashMap<u64, Brane>,
    /// brane_id -> reader
    pub readers: HashMap<u64, Readpushdown_causet>,
    /// `MsgRequestPreVote` or `MsgRequestVote` messages from newly split Branes shouldn't be dropped if there is no
    /// such Brane in this store now. So the messages are recorded temporarily and will be handled later.
    pub plightlikeing_votes: RingQueue<VioletaBftMessage>,
    /// The branes with plightlikeing snapshots.
    pub plightlikeing_snapshot_branes: Vec<Brane>,
    /// A marker used to indicate the peer of a Brane has received a merge target message and waits to be destroyed.
    /// target_brane_id -> (source_brane_id -> merge_target_brane)
    pub plightlikeing_merge_targets: HashMap<u64, HashMap<u64, meta_timeshare::Brane>>,
    /// An inverse mapping of `plightlikeing_merge_targets` used to let source peer help target peer to clean up related entry.
    /// source_brane_id -> target_brane_id
    pub targets_map: HashMap<u64, u64>,
    /// `atomic_snap_branes` and `destroyed_brane_for_snap` are used for making destroy overlapped branes
    /// and apply snapshot atomically.
    /// brane_id -> wait_destroy_branes_map(source_brane_id -> is_ready)
    /// A target peer must wait for all source peer to ready before applying snapshot.
    pub atomic_snap_branes: HashMap<u64, HashMap<u64, bool>>,
    /// source_brane_id -> need_atomic
    /// Used for reminding the source peer to switch to ready in `atomic_snap_branes`.
    pub destroyed_brane_for_snap: HashMap<u64, bool>,
}

impl StoreMeta {
    pub fn new(vote_capacity: usize) -> StoreMeta {
        StoreMeta {
            store_id: None,
            brane_cones: BTreeMap::default(),
            branes: HashMap::default(),
            readers: HashMap::default(),
            plightlikeing_votes: RingQueue::with_capacity(vote_capacity),
            plightlikeing_snapshot_branes: Vec::default(),
            plightlikeing_merge_targets: HashMap::default(),
            targets_map: HashMap::default(),
            atomic_snap_branes: HashMap::default(),
            destroyed_brane_for_snap: HashMap::default(),
        }
    }

    #[inline]
    pub fn set_brane<EK: CausetEngine, ER: VioletaBftEngine>(
        &mut self,
        host: &InterlockHost<EK>,
        brane: Brane,
        peer: &mut crate::store::Peer<EK, ER>,
    ) {
        let prev = self.branes.insert(brane.get_id(), brane.clone());
        if prev.map_or(true, |r| r.get_id() != brane.get_id()) {
            // TODO: may not be a good idea to panic when holding a dagger.
            panic!("{} brane corrupted", peer.tag);
        }
        let reader = self.readers.get_mut(&brane.get_id()).unwrap();
        peer.set_brane(host, reader, brane);
    }
}

pub struct VioletaBftRouter<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    pub router: BatchRouter<PeerFsm<EK, ER>, StoreFsm<EK>>,
}

impl<EK, ER> Clone for VioletaBftRouter<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    fn clone(&self) -> Self {
        VioletaBftRouter {
            router: self.router.clone(),
        }
    }
}

impl<EK, ER> Deref for VioletaBftRouter<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    type Target = BatchRouter<PeerFsm<EK, ER>, StoreFsm<EK>>;

    fn deref(&self) -> &BatchRouter<PeerFsm<EK, ER>, StoreFsm<EK>> {
        &self.router
    }
}

impl<EK, ER> ApplyNotifier<EK> for VioletaBftRouter<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    fn notify(&self, apply_res: Vec<ApplyRes<EK::Snapshot>>) {
        for r in apply_res {
            self.router.try_lightlike(
                r.brane_id,
                PeerMsg::ApplyRes {
                    res: ApplyTaskRes::Apply(r),
                },
            );
        }
    }
    fn notify_one(&self, brane_id: u64, msg: PeerMsg<EK>) {
        self.router.try_lightlike(brane_id, msg);
    }

    fn clone_box(&self) -> Box<dyn ApplyNotifier<EK>> {
        Box::new(self.clone())
    }
}

impl<EK, ER> VioletaBftRouter<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    pub fn lightlike_violetabft_message(
        &self,
        mut msg: VioletaBftMessage,
    ) -> std::result::Result<(), TrylightlikeError<VioletaBftMessage>> {
        let id = msg.get_brane_id();
        match self.try_lightlike(id, PeerMsg::VioletaBftMessage(msg)) {
            Either::Left(Ok(())) => return Ok(()),
            Either::Left(Err(TrylightlikeError::Full(PeerMsg::VioletaBftMessage(m)))) => {
                return Err(TrylightlikeError::Full(m));
            }
            Either::Left(Err(TrylightlikeError::Disconnected(PeerMsg::VioletaBftMessage(m)))) => {
                return Err(TrylightlikeError::Disconnected(m));
            }
            Either::Right(PeerMsg::VioletaBftMessage(m)) => msg = m,
            _ => unreachable!(),
        }
        match self.lightlike_control(StoreMsg::VioletaBftMessage(msg)) {
            Ok(()) => Ok(()),
            Err(TrylightlikeError::Full(StoreMsg::VioletaBftMessage(m))) => Err(TrylightlikeError::Full(m)),
            Err(TrylightlikeError::Disconnected(StoreMsg::VioletaBftMessage(m))) => {
                Err(TrylightlikeError::Disconnected(m))
            }
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn lightlike_violetabft_command(
        &self,
        cmd: VioletaBftCommand<EK::Snapshot>,
    ) -> std::result::Result<(), TrylightlikeError<VioletaBftCommand<EK::Snapshot>>> {
        let brane_id = cmd.request.get_header().get_brane_id();
        match self.lightlike(brane_id, PeerMsg::VioletaBftCommand(cmd)) {
            Ok(()) => Ok(()),
            Err(TrylightlikeError::Full(PeerMsg::VioletaBftCommand(cmd))) => Err(TrylightlikeError::Full(cmd)),
            Err(TrylightlikeError::Disconnected(PeerMsg::VioletaBftCommand(cmd))) => {
                Err(TrylightlikeError::Disconnected(cmd))
            }
            _ => unreachable!(),
        }
    }

    fn report_unreachable(&self, store_id: u64) {
        self.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::StoreUnreachable { store_id })
        });
    }

    fn report_status_fidelio(&self) {
        self.broadcast_normal(|| PeerMsg::fidelioReplicationMode)
    }

    /// Broadcasts resolved result to all branes.
    pub fn report_resolved(&self, store_id: u64, group_id: u64) {
        self.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::StoreResolved { store_id, group_id })
        })
    }
}

#[derive(Default)]
pub struct PeerTickBatch {
    pub ticks: Vec<Box<dyn FnOnce() + lightlike>>,
    pub wait_duration: Duration,
}

impl Clone for PeerTickBatch {
    fn clone(&self) -> PeerTickBatch {
        PeerTickBatch {
            ticks: vec![],
            wait_duration: self.wait_duration,
        }
    }
}

pub struct PollContext<EK, ER, T, C: 'static>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    pub causet: Config,
    pub store: meta_timeshare::CausetStore,
    pub fidel_interlock_semaphore: FutureInterlock_Semaphore<FidelTask<EK>>,
    pub consistency_check_interlock_semaphore: Interlock_Semaphore<ConsistencyCheckTask<EK::Snapshot>>,
    pub split_check_interlock_semaphore: Interlock_Semaphore<SplitCheckTask>,
    // handle Compact, CleanupSST task
    pub cleanup_interlock_semaphore: Interlock_Semaphore<CleanupTask>,
    pub violetabftlog_gc_interlock_semaphore: Interlock_Semaphore<VioletaBftlogGcTask>,
    pub brane_interlock_semaphore: Interlock_Semaphore<BraneTask<EK::Snapshot>>,
    pub apply_router: ApplyRouter<EK>,
    pub router: VioletaBftRouter<EK, ER>,
    pub importer: Arc<SSTImporter>,
    pub store_meta: Arc<Mutex<StoreMeta>>,
    /// brane_id -> (peer_id, is_splitting)
    /// Used for handling race between splitting and creating new peer.
    /// An uninitialized peer can be replaced to the one from splitting iff they are exactly the same peer.
    ///
    /// WARNING:
    /// To avoid deadlock, if you want to use `store_meta` and `plightlikeing_create_peers` together,
    /// the dagger sequence MUST BE:
    /// 1. dagger the store_meta.
    /// 2. dagger the plightlikeing_create_peers.
    pub plightlikeing_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
    pub violetabft_metrics: VioletaBftMetrics,
    pub snap_mgr: SnapManager,
    pub applying_snap_count: Arc<AtomicUsize>,
    pub interlock_host: InterlockHost<EK>,
    pub timer: SteadyTimer,
    pub trans: T,
    pub fidel_client: Arc<C>,
    pub global_replication_state: Arc<Mutex<GlobalReplicationState>>,
    pub global_stat: GlobalStoreStat,
    pub store_stat: LocalStoreStat,
    pub engines: Engines<EK, ER>,
    pub kv_wb: EK::WriteBatch,
    pub violetabft_wb: ER::LogBatch,
    pub plightlikeing_count: usize,
    pub sync_log: bool,
    pub has_ready: bool,
    pub ready_res: Vec<(Ready, InvokeContext)>,
    pub need_flush_trans: bool,
    pub current_time: Option<Timespec>,
    pub perf_context_statistics: PerfContextStatistics,
    pub tick_batch: Vec<PeerTickBatch>,
    pub node_spacelike_time: Option<TiInstant>,
}

impl<EK, ER, T, C> HandleVioletaBftReadyContext<EK::WriteBatch, ER::LogBatch>
    for PollContext<EK, ER, T, C>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    fn wb_mut(&mut self) -> (&mut EK::WriteBatch, &mut ER::LogBatch) {
        (&mut self.kv_wb, &mut self.violetabft_wb)
    }

    #[inline]
    fn kv_wb_mut(&mut self) -> &mut EK::WriteBatch {
        &mut self.kv_wb
    }

    #[inline]
    fn violetabft_wb_mut(&mut self) -> &mut ER::LogBatch {
        &mut self.violetabft_wb
    }

    #[inline]
    fn sync_log(&self) -> bool {
        self.sync_log
    }

    #[inline]
    fn set_sync_log(&mut self, sync: bool) {
        self.sync_log = sync;
    }
}

impl<EK, ER, T, C> PollContext<EK, ER, T, C>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    #[inline]
    pub fn store_id(&self) -> u64 {
        self.store.get_id()
    }

    /// Timeout is calculated from EinsteinDB spacelike, the node should not become
    /// hibernated if it still within the hibernate timeout, see
    /// https://github.com/edb/edb/issues/7747
    pub fn is_hibernate_timeout(&mut self) -> bool {
        let timeout = match self.node_spacelike_time {
            Some(t) => t.elapsed() >= self.causet.hibernate_timeout.0,
            None => return true,
        };
        if timeout {
            self.node_spacelike_time = None;
        }
        timeout
    }

    pub fn fidelio_ticks_timeout(&mut self) {
        self.tick_batch[PeerTicks::VIOLETABFT.bits() as usize].wait_duration =
            self.causet.violetabft_base_tick_interval.0;
        self.tick_batch[PeerTicks::VIOLETABFT_LOG_GC.bits() as usize].wait_duration =
            self.causet.violetabft_log_gc_tick_interval.0;
        self.tick_batch[PeerTicks::FIDel_HEARTBEAT.bits() as usize].wait_duration =
            self.causet.fidel_heartbeat_tick_interval.0;
        self.tick_batch[PeerTicks::SPLIT_REGION_CHECK.bits() as usize].wait_duration =
            self.causet.split_brane_check_tick_interval.0;
        self.tick_batch[PeerTicks::CHECK_PEER_STALE_STATE.bits() as usize].wait_duration =
            self.causet.peer_stale_state_check_interval.0;
        self.tick_batch[PeerTicks::CHECK_MERGE.bits() as usize].wait_duration =
            self.causet.merge_check_tick_interval.0;
    }
}

impl<EK, ER, T: Transport, C> PollContext<EK, ER, T, C>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    #[inline]
    fn schedule_store_tick(&self, tick: StoreTick, timeout: Duration) {
        if !is_zero_duration(&timeout) {
            let mb = self.router.control_mailbox();
            let delay = self.timer.delay(timeout).compat().map(move |_| {
                if let Err(e) = mb.force_lightlike(StoreMsg::Tick(tick)) {
                    info!(
                        "failed to schedule store tick, are we shutting down?";
                        "tick" => ?tick,
                        "err" => ?e
                    );
                }
            });
            poll_future_notify(delay);
        }
    }

    pub fn handle_stale_msg(
        &mut self,
        msg: &VioletaBftMessage,
        cur_epoch: BraneEpoch,
        need_gc: bool,
        target_brane: Option<meta_timeshare::Brane>,
    ) {
        let brane_id = msg.get_brane_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();

        if !need_gc {
            info!(
                "violetabft message is stale, ignore it";
                "brane_id" => brane_id,
                "current_brane_epoch" => ?cur_epoch,
                "msg_type" => ?msg_type,
            );
            self.violetabft_metrics.message_dropped.stale_msg += 1;
            return;
        }

        info!(
            "violetabft message is stale, tell to gc";
            "brane_id" => brane_id,
            "current_brane_epoch" => ?cur_epoch,
            "msg_type" => ?msg_type,
        );

        let mut gc_msg = VioletaBftMessage::default();
        gc_msg.set_brane_id(brane_id);
        gc_msg.set_from_peer(to_peer.clone());
        gc_msg.set_to_peer(from_peer.clone());
        gc_msg.set_brane_epoch(cur_epoch);
        if let Some(r) = target_brane {
            gc_msg.set_merge_target(r);
        } else {
            gc_msg.set_is_tombstone(true);
        }
        if let Err(e) = self.trans.lightlike(gc_msg) {
            error!(?e;
                "lightlike gc message failed";
                "brane_id" => brane_id,
            );
        }
        self.need_flush_trans = true;
    }
}

struct CausetStore {
    // store id, before spacelike the id is 0.
    id: u64,
    last_compact_checked_key: Key,
    stopped: bool,
    spacelike_time: Option<Timespec>,
    consistency_check_time: HashMap<u64, Instant>,
    last_unreachable_report: HashMap<u64, Instant>,
}

pub struct StoreFsm<EK>
where
    EK: CausetEngine,
{
    store: CausetStore,
    receiver: Receiver<StoreMsg<EK>>,
}

impl<EK> StoreFsm<EK>
where
    EK: CausetEngine,
{
    pub fn new(causet: &Config) -> (LooseBoundedlightlikeer<StoreMsg<EK>>, Box<StoreFsm<EK>>) {
        let (tx, rx) = mpsc::loose_bounded(causet.notify_capacity);
        let fsm = Box::new(StoreFsm {
            store: CausetStore {
                id: 0,
                last_compact_checked_key: tuplespaceInstanton::DATA_MIN_KEY.to_vec(),
                stopped: false,
                spacelike_time: None,
                consistency_check_time: HashMap::default(),
                last_unreachable_report: HashMap::default(),
            },
            receiver: rx,
        });
        (tx, fsm)
    }
}

impl<EK> Fsm for StoreFsm<EK>
where
    EK: CausetEngine,
{
    type Message = StoreMsg<EK>;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.store.stopped
    }
}

struct StoreFsmpushdown_causet<
    'a,
    EK: CausetEngine + 'static,
    ER: VioletaBftEngine + 'static,
    T: 'static,
    C: 'static,
> {
    fsm: &'a mut StoreFsm<EK>,
    ctx: &'a mut PollContext<EK, ER, T, C>,
}

impl<'a, EK: CausetEngine + 'static, ER: VioletaBftEngine + 'static, T: Transport, C: FidelClient>
    StoreFsmpushdown_causet<'a, EK, ER, T, C>
{
    fn on_tick(&mut self, tick: StoreTick) {
        let t = TiInstant::now_coarse();
        match tick {
            StoreTick::FidelStoreHeartbeat => self.on_fidel_store_heartbeat_tick(),
            StoreTick::SnapGc => self.on_snap_mgr_gc(),
            StoreTick::CompactLockCf => self.on_compact_lock_causet(),
            StoreTick::CompactCheck => self.on_compact_check_tick(),
            StoreTick::ConsistencyCheck => self.on_consistency_check_tick(),
            StoreTick::CleanupImportSST => self.on_cleanup_import_sst_tick(),
            StoreTick::VioletaBftEnginePurge => self.on_violetabft_engine_purge_tick(),
        }
        let elapsed = t.elapsed();
        VIOLETABFT_EVENT_DURATION
            .get(tick.tag())
            .observe(duration_to_sec(elapsed) as f64);
        slow_log!(
            elapsed,
            "[store {}] handle timeout {:?}",
            self.fsm.store.id,
            tick
        );
    }

    fn handle_msgs(&mut self, msgs: &mut Vec<StoreMsg<EK>>) {
        for m in msgs.drain(..) {
            match m {
                StoreMsg::Tick(tick) => self.on_tick(tick),
                StoreMsg::VioletaBftMessage(msg) => {
                    if let Err(e) = self.on_violetabft_message(msg) {
                        error!(?e;
                            "handle violetabft message failed";
                            "store_id" => self.fsm.store.id,
                        );
                    }
                }
                StoreMsg::CompactedEvent(event) => self.on_compaction_finished(event),
                StoreMsg::ValidateSSTResult { invalid_ssts } => {
                    self.on_validate_sst_result(invalid_ssts)
                }
                StoreMsg::ClearBraneSizeInCone { spacelike_key, lightlike_key } => {
                    self.clear_brane_size_in_cone(&spacelike_key, &lightlike_key)
                }
                StoreMsg::StoreUnreachable { store_id } => {
                    self.on_store_unreachable(store_id);
                }
                StoreMsg::Start { store } => self.spacelike(store),
                #[causet(any(test, feature = "testexport"))]
                StoreMsg::Validate(f) => f(&self.ctx.causet),
                StoreMsg::fidelioReplicationMode(status) => self.on_fidelio_replication_mode(status),
            }
        }
    }

    fn spacelike(&mut self, store: meta_timeshare::CausetStore) {
        if self.fsm.store.spacelike_time.is_some() {
            panic!(
                "[store {}] unable to spacelike again with meta {:?}",
                self.fsm.store.id, store
            );
        }
        self.fsm.store.id = store.get_id();
        self.fsm.store.spacelike_time = Some(time::get_time());
        self.register_cleanup_import_sst_tick();
        self.register_compact_check_tick();
        self.register_fidel_store_heartbeat_tick();
        self.register_compact_lock_causet_tick();
        self.register_snap_mgr_gc_tick();
        self.register_consistency_check_tick();
        self.register_violetabft_engine_purge_tick();
    }
}

pub struct VioletaBftPoller<EK: CausetEngine + 'static, ER: VioletaBftEngine + 'static, T: 'static, C: 'static> {
    tag: String,
    store_msg_buf: Vec<StoreMsg<EK>>,
    peer_msg_buf: Vec<PeerMsg<EK>>,
    previous_metrics: VioletaBftMetrics,
    timer: TiInstant,
    poll_ctx: PollContext<EK, ER, T, C>,
    messages_per_tick: usize,
    causet_tracker: Tracker<Config>,
}

impl<EK: CausetEngine, ER: VioletaBftEngine, T: Transport, C: FidelClient> VioletaBftPoller<EK, ER, T, C> {
    fn handle_violetabft_ready(&mut self, peers: &mut [Box<PeerFsm<EK, ER>>]) {
        // Only enable the fail point when the store id is equal to 3, which is
        // the id of slow store in tests.
        fail_point!("on_violetabft_ready", self.poll_ctx.store_id() == 3, |_| {});
        if self.poll_ctx.need_flush_trans
            && (!self.poll_ctx.kv_wb.is_empty() || !self.poll_ctx.violetabft_wb.is_empty())
        {
            self.poll_ctx.trans.flush();
            self.poll_ctx.need_flush_trans = false;
        }
        let ready_cnt = self.poll_ctx.ready_res.len();
        if ready_cnt != 0 && self.poll_ctx.causet.early_apply {
            let mut batch_pos = 0;
            let mut ready_res = mem::replace(&mut self.poll_ctx.ready_res, vec![]);
            for (ready, invoke_ctx) in &mut ready_res {
                let brane_id = invoke_ctx.brane_id;
                if peers[batch_pos].brane_id() == brane_id {
                } else {
                    while peers[batch_pos].brane_id() != brane_id {
                        batch_pos += 1;
                    }
                }
                PeerFsmpushdown_causet::new(&mut peers[batch_pos], &mut self.poll_ctx)
                    .handle_violetabft_ready_apply(ready, invoke_ctx);
            }
            self.poll_ctx.ready_res = ready_res;
        }
        self.poll_ctx.violetabft_metrics.ready.has_ready_brane += ready_cnt as u64;
        fail_point!("violetabft_before_save");
        if !self.poll_ctx.kv_wb.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.poll_ctx
                .engines
                .kv
                .write_opt(&self.poll_ctx.kv_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save applightlike state result: {:?}", self.tag, e);
                });
            let data_size = self.poll_ctx.kv_wb.data_size();
            if data_size > KV_WB_SHRINK_SIZE {
                self.poll_ctx.kv_wb = self.poll_ctx.engines.kv.write_batch_with_cap(4 * 1024);
            } else {
                self.poll_ctx.kv_wb.clear();
            }
        }
        fail_point!("violetabft_between_save");
        if !self.poll_ctx.violetabft_wb.is_empty() {
            fail_point!(
                "violetabft_before_save_on_store_1",
                self.poll_ctx.store_id() == 1,
                |_| {}
            );

            self.poll_ctx
                .engines
                .violetabft
                .consume_and_shrink(
                    &mut self.poll_ctx.violetabft_wb,
                    true,
                    VIOLETABFT_WB_SHRINK_SIZE,
                    4 * 1024,
                )
                .unwrap_or_else(|e| {
                    panic!("{} failed to save violetabft applightlike result: {:?}", self.tag, e);
                });
        }

        report_perf_context!(
            self.poll_ctx.perf_context_statistics,
            STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC
        );
        fail_point!("violetabft_after_save");
        if ready_cnt != 0 {
            let mut batch_pos = 0;
            let mut ready_res = mem::take(&mut self.poll_ctx.ready_res);
            for (ready, invoke_ctx) in ready_res.drain(..) {
                let brane_id = invoke_ctx.brane_id;
                if peers[batch_pos].brane_id() == brane_id {
                } else {
                    while peers[batch_pos].brane_id() != brane_id {
                        batch_pos += 1;
                    }
                }
                PeerFsmpushdown_causet::new(&mut peers[batch_pos], &mut self.poll_ctx)
                    .post_violetabft_ready_applightlike(ready, invoke_ctx);
            }
        }
        let dur = self.timer.elapsed();
        if !self.poll_ctx.store_stat.is_busy {
            let election_timeout = Duration::from_millis(
                self.poll_ctx.causet.violetabft_base_tick_interval.as_millis()
                    * self.poll_ctx.causet.violetabft_election_timeout_ticks as u64,
            );
            if dur >= election_timeout {
                self.poll_ctx.store_stat.is_busy = true;
            }
        }

        self.poll_ctx
            .violetabft_metrics
            .applightlike_log
            .observe(duration_to_sec(dur) as f64);

        slow_log!(
            dur,
            "{} handle {} plightlikeing peers include {} ready, {} entries, {} messages and {} \
             snapshots",
            self.tag,
            self.poll_ctx.plightlikeing_count,
            ready_cnt,
            self.poll_ctx.violetabft_metrics.ready.applightlike - self.previous_metrics.ready.applightlike,
            self.poll_ctx.violetabft_metrics.ready.message - self.previous_metrics.ready.message,
            self.poll_ctx.violetabft_metrics.ready.snapshot - self.previous_metrics.ready.snapshot
        );
    }

    fn flush_ticks(&mut self) {
        for t in PeerTicks::get_all_ticks() {
            let idx = t.bits() as usize;
            if self.poll_ctx.tick_batch[idx].ticks.is_empty() {
                continue;
            }
            let peer_ticks = std::mem::replace(&mut self.poll_ctx.tick_batch[idx].ticks, vec![]);
            let f = self
                .poll_ctx
                .timer
                .delay(self.poll_ctx.tick_batch[idx].wait_duration)
                .compat()
                .map(move |_| {
                    for tick in peer_ticks {
                        tick();
                    }
                });
            poll_future_notify(f);
        }
    }
}

impl<EK: CausetEngine, ER: VioletaBftEngine, T: Transport, C: FidelClient>
    PollHandler<PeerFsm<EK, ER>, StoreFsm<EK>> for VioletaBftPoller<EK, ER, T, C>
{
    fn begin(&mut self, _batch_size: usize) {
        self.previous_metrics = self.poll_ctx.violetabft_metrics.clone();
        self.poll_ctx.plightlikeing_count = 0;
        self.poll_ctx.sync_log = false;
        self.poll_ctx.has_ready = false;
        self.timer = TiInstant::now_coarse();
        // fidelio config
        self.poll_ctx.perf_context_statistics.spacelike();
        if let Some(incoming) = self.causet_tracker.any_new() {
            match Ord::cmp(
                &incoming.messages_per_tick,
                &self.poll_ctx.causet.messages_per_tick,
            ) {
                CmpOrdering::Greater => {
                    self.store_msg_buf.reserve(incoming.messages_per_tick);
                    self.peer_msg_buf.reserve(incoming.messages_per_tick);
                    self.messages_per_tick = incoming.messages_per_tick;
                }
                CmpOrdering::Less => {
                    self.store_msg_buf.shrink_to(incoming.messages_per_tick);
                    self.peer_msg_buf.shrink_to(incoming.messages_per_tick);
                    self.messages_per_tick = incoming.messages_per_tick;
                }
                _ => {}
            }
            self.poll_ctx.causet = incoming.clone();
            self.poll_ctx.fidelio_ticks_timeout();
        }
    }

    fn handle_control(&mut self, store: &mut StoreFsm<EK>) -> Option<usize> {
        let mut expected_msg_count = None;
        while self.store_msg_buf.len() < self.messages_per_tick {
            match store.receiver.try_recv() {
                Ok(msg) => self.store_msg_buf.push(msg),
                Err(TryRecvError::Empty) => {
                    expected_msg_count = Some(0);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    store.store.stopped = true;
                    expected_msg_count = Some(0);
                    break;
                }
            }
        }
        let mut pushdown_causet = StoreFsmpushdown_causet {
            fsm: store,
            ctx: &mut self.poll_ctx,
        };
        pushdown_causet.handle_msgs(&mut self.store_msg_buf);
        expected_msg_count
    }

    fn handle_normal(&mut self, peer: &mut PeerFsm<EK, ER>) -> Option<usize> {
        let mut expected_msg_count = None;

        fail_point!(
            "pause_on_peer_collect_message",
            peer.peer_id() == 1,
            |_| unreachable!()
        );

        while self.peer_msg_buf.len() < self.messages_per_tick {
            match peer.receiver.try_recv() {
                // TODO: we may need a way to optimize the message copy.
                Ok(msg) => {
                    fail_point!(
                        "pause_on_peer_destroy_res",
                        peer.peer_id() == 1
                            && match msg {
                                PeerMsg::ApplyRes {
                                    res: ApplyTaskRes::Destroy { .. },
                                } => true,
                                _ => false,
                            },
                        |_| unreachable!()
                    );
                    self.peer_msg_buf.push(msg);
                }
                Err(TryRecvError::Empty) => {
                    expected_msg_count = Some(0);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    peer.stop();
                    expected_msg_count = Some(0);
                    break;
                }
            }
        }
        let mut pushdown_causet = PeerFsmpushdown_causet::new(peer, &mut self.poll_ctx);
        pushdown_causet.handle_msgs(&mut self.peer_msg_buf);
        pushdown_causet.collect_ready();
        expected_msg_count
    }

    fn lightlike(&mut self, peers: &mut [Box<PeerFsm<EK, ER>>]) {
        self.flush_ticks();
        if self.poll_ctx.has_ready {
            self.handle_violetabft_ready(peers);
        }
        self.poll_ctx.current_time = None;
        self.poll_ctx
            .violetabft_metrics
            .process_ready
            .observe(duration_to_sec(self.timer.elapsed()) as f64);
        self.poll_ctx.violetabft_metrics.flush();
        self.poll_ctx.store_stat.flush();
    }

    fn pause(&mut self) {
        if self.poll_ctx.need_flush_trans {
            self.poll_ctx.trans.flush();
            self.poll_ctx.need_flush_trans = false;
        }
    }
}

pub struct VioletaBftPollerBuilder<EK: CausetEngine, ER: VioletaBftEngine, T, C> {
    pub causet: Arc<VersionTrack<Config>>,
    pub store: meta_timeshare::CausetStore,
    fidel_interlock_semaphore: FutureInterlock_Semaphore<FidelTask<EK>>,
    consistency_check_interlock_semaphore: Interlock_Semaphore<ConsistencyCheckTask<EK::Snapshot>>,
    split_check_interlock_semaphore: Interlock_Semaphore<SplitCheckTask>,
    cleanup_interlock_semaphore: Interlock_Semaphore<CleanupTask>,
    violetabftlog_gc_interlock_semaphore: Interlock_Semaphore<VioletaBftlogGcTask>,
    pub brane_interlock_semaphore: Interlock_Semaphore<BraneTask<EK::Snapshot>>,
    apply_router: ApplyRouter<EK>,
    pub router: VioletaBftRouter<EK, ER>,
    pub importer: Arc<SSTImporter>,
    pub store_meta: Arc<Mutex<StoreMeta>>,
    pub plightlikeing_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
    snap_mgr: SnapManager,
    pub interlock_host: InterlockHost<EK>,
    trans: T,
    fidel_client: Arc<C>,
    global_stat: GlobalStoreStat,
    pub engines: Engines<EK, ER>,
    applying_snap_count: Arc<AtomicUsize>,
    global_replication_state: Arc<Mutex<GlobalReplicationState>>,
}

impl<EK: CausetEngine, ER: VioletaBftEngine, T, C> VioletaBftPollerBuilder<EK, ER, T, C> {
    /// Initialize this store. It scans the db engine, loads all branes
    /// and their peers from it, and schedules snapshot worker if necessary.
    /// WARN: This store should not be used before initialized.
    fn init(&mut self) -> Result<Vec<lightlikeerFsmPair<EK, ER>>> {
        // Scan brane meta to get saved branes.
        let spacelike_key = tuplespaceInstanton::REGION_META_MIN_KEY;
        let lightlike_key = tuplespaceInstanton::REGION_META_MAX_KEY;
        let kv_engine = self.engines.kv.clone();
        let store_id = self.store.get_id();
        let mut total_count = 0;
        let mut tombstone_count = 0;
        let mut applying_count = 0;
        let mut brane_peers = vec![];

        let t = Instant::now();
        let mut kv_wb = self.engines.kv.write_batch();
        let mut violetabft_wb = self.engines.violetabft.log_batch(4 * 1024);
        let mut applying_branes = vec![];
        let mut merging_count = 0;
        let mut meta = self.store_meta.dagger().unwrap();
        let mut replication_state = self.global_replication_state.dagger().unwrap();
        kv_engine.scan_causet(Causet_VIOLETABFT, spacelike_key, lightlike_key, false, |key, value| {
            let (brane_id, suffix) = box_try!(tuplespaceInstanton::decode_brane_meta_key(key));
            if suffix != tuplespaceInstanton::REGION_STATE_SUFFIX {
                return Ok(true);
            }

            total_count += 1;

            let mut local_state = BraneLocalState::default();
            local_state.merge_from_bytes(value)?;

            let brane = local_state.get_brane();
            if local_state.get_state() == PeerState::Tombstone {
                tombstone_count += 1;
                debug!("brane is tombstone"; "brane" => ?brane, "store_id" => store_id);
                self.clear_stale_meta(&mut kv_wb, &mut violetabft_wb, &local_state);
                return Ok(true);
            }
            if local_state.get_state() == PeerState::Applying {
                // in case of respacelike happen when we just write brane state to Applying,
                // but not write violetabft_local_state to violetabft lmdb in time.
                box_try!(peer_causet_storage::recover_from_applying_state(
                    &self.engines,
                    &mut violetabft_wb,
                    brane_id
                ));
                applying_count += 1;
                applying_branes.push(brane.clone());
                return Ok(true);
            }

            let (tx, mut peer) = box_try!(PeerFsm::create(
                store_id,
                &self.causet.value(),
                self.brane_interlock_semaphore.clone(),
                self.engines.clone(),
                brane,
            ));
            peer.peer.init_replication_mode(&mut *replication_state);
            if local_state.get_state() == PeerState::Merging {
                info!("brane is merging"; "brane" => ?brane, "store_id" => store_id);
                merging_count += 1;
                peer.set_plightlikeing_merge_state(local_state.get_merge_state().to_owned());
            }
            meta.brane_cones.insert(enc_lightlike_key(brane), brane_id);
            meta.branes.insert(brane_id, brane.clone());
            // No need to check duplicated here, because we use brane id as the key
            // in DB.
            brane_peers.push((tx, peer));
            self.interlock_host.on_brane_changed(
                brane,
                BraneChangeEvent::Create,
                StateRole::Follower,
            );
            Ok(true)
        })?;

        if !kv_wb.is_empty() {
            self.engines.kv.write(&kv_wb).unwrap();
            self.engines.kv.sync_wal().unwrap();
        }
        if !violetabft_wb.is_empty() {
            self.engines.violetabft.consume(&mut violetabft_wb, true).unwrap();
        }

        // schedule applying snapshot after violetabft writebatch were written.
        for brane in applying_branes {
            info!("brane is applying snapshot"; "brane" => ?brane, "store_id" => store_id);
            let (tx, mut peer) = PeerFsm::create(
                store_id,
                &self.causet.value(),
                self.brane_interlock_semaphore.clone(),
                self.engines.clone(),
                &brane,
            )?;
            peer.peer.init_replication_mode(&mut *replication_state);
            peer.schedule_applying_snapshot();
            meta.brane_cones
                .insert(enc_lightlike_key(&brane), brane.get_id());
            meta.branes.insert(brane.get_id(), brane);
            brane_peers.push((tx, peer));
        }

        info!(
            "spacelike store";
            "store_id" => store_id,
            "brane_count" => total_count,
            "tombstone_count" => tombstone_count,
            "applying_count" =>  applying_count,
            "merge_count" => merging_count,
            "takes" => ?t.elapsed(),
        );

        self.clear_stale_data(&meta)?;

        Ok(brane_peers)
    }

    fn clear_stale_meta(
        &self,
        kv_wb: &mut EK::WriteBatch,
        violetabft_wb: &mut ER::LogBatch,
        origin_state: &BraneLocalState,
    ) {
        let rid = origin_state.get_brane().get_id();
        let violetabft_state = match self.engines.violetabft.get_violetabft_state(rid).unwrap() {
            // it has been cleaned up.
            None => return,
            Some(value) => value,
        };
        peer_causet_storage::clear_meta(&self.engines, kv_wb, violetabft_wb, rid, &violetabft_state).unwrap();
        let key = tuplespaceInstanton::brane_state_key(rid);
        kv_wb.put_msg_causet(Causet_VIOLETABFT, &key, origin_state).unwrap();
    }

    /// `clear_stale_data` clean up all possible garbage data.
    fn clear_stale_data(&self, meta: &StoreMeta) -> Result<()> {
        let t = Instant::now();

        let mut cones = Vec::new();
        let mut last_spacelike_key = tuplespaceInstanton::data_key(b"");
        for brane_id in meta.brane_cones.values() {
            let brane = &meta.branes[brane_id];
            let spacelike_key = tuplespaceInstanton::enc_spacelike_key(brane);
            cones.push((last_spacelike_key, spacelike_key));
            last_spacelike_key = tuplespaceInstanton::enc_lightlike_key(brane);
        }
        cones.push((last_spacelike_key, tuplespaceInstanton::DATA_MAX_KEY.to_vec()));

        self.engines.kv.roughly_cleanup_cones(&cones)?;

        info!(
            "cleans up garbage data";
            "store_id" => self.store.get_id(),
            "garbage_cone_count" => cones.len(),
            "takes" => ?t.elapsed()
        );

        Ok(())
    }
}

impl<EK, ER, T, C> HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK>> for VioletaBftPollerBuilder<EK, ER, T, C>
where
    EK: CausetEngine + 'static,
    ER: VioletaBftEngine + 'static,
    T: Transport + 'static,
    C: FidelClient + 'static,
{
    type Handler = VioletaBftPoller<EK, ER, T, C>;

    fn build(&mut self) -> VioletaBftPoller<EK, ER, T, C> {
        let mut ctx = PollContext {
            causet: self.causet.value().clone(),
            store: self.store.clone(),
            fidel_interlock_semaphore: self.fidel_interlock_semaphore.clone(),
            consistency_check_interlock_semaphore: self.consistency_check_interlock_semaphore.clone(),
            split_check_interlock_semaphore: self.split_check_interlock_semaphore.clone(),
            brane_interlock_semaphore: self.brane_interlock_semaphore.clone(),
            apply_router: self.apply_router.clone(),
            router: self.router.clone(),
            cleanup_interlock_semaphore: self.cleanup_interlock_semaphore.clone(),
            violetabftlog_gc_interlock_semaphore: self.violetabftlog_gc_interlock_semaphore.clone(),
            importer: self.importer.clone(),
            store_meta: self.store_meta.clone(),
            plightlikeing_create_peers: self.plightlikeing_create_peers.clone(),
            violetabft_metrics: VioletaBftMetrics::default(),
            snap_mgr: self.snap_mgr.clone(),
            applying_snap_count: self.applying_snap_count.clone(),
            interlock_host: self.interlock_host.clone(),
            timer: SteadyTimer::default(),
            trans: self.trans.clone(),
            fidel_client: self.fidel_client.clone(),
            global_replication_state: self.global_replication_state.clone(),
            global_stat: self.global_stat.clone(),
            store_stat: self.global_stat.local(),
            engines: self.engines.clone(),
            kv_wb: self.engines.kv.write_batch(),
            violetabft_wb: self.engines.violetabft.log_batch(4 * 1024),
            plightlikeing_count: 0,
            sync_log: false,
            has_ready: false,
            ready_res: Vec::new(),
            need_flush_trans: false,
            current_time: None,
            perf_context_statistics: PerfContextStatistics::new(self.causet.value().perf_level),
            tick_batch: vec![PeerTickBatch::default(); 256],
            node_spacelike_time: Some(TiInstant::now_coarse()),
        };
        ctx.fidelio_ticks_timeout();
        let tag = format!("[store {}]", ctx.store.get_id());
        VioletaBftPoller {
            tag: tag.clone(),
            store_msg_buf: Vec::with_capacity(ctx.causet.messages_per_tick),
            peer_msg_buf: Vec::with_capacity(ctx.causet.messages_per_tick),
            previous_metrics: ctx.violetabft_metrics.clone(),
            timer: TiInstant::now_coarse(),
            messages_per_tick: ctx.causet.messages_per_tick,
            poll_ctx: ctx,
            causet_tracker: self.causet.clone().tracker(tag),
        }
    }
}

struct Workers<EK: CausetEngine> {
    fidel_worker: FutureWorker<FidelTask<EK>>,
    consistency_check_worker: Worker<ConsistencyCheckTask<EK::Snapshot>>,
    split_check_worker: Worker<SplitCheckTask>,
    // handle Compact, CleanupSST task
    cleanup_worker: Worker<CleanupTask>,
    violetabftlog_gc_worker: Worker<VioletaBftlogGcTask>,
    brane_worker: Worker<BraneTask<EK::Snapshot>>,
    interlock_host: InterlockHost<EK>,
}

pub struct VioletaBftBatchSystem<EK: CausetEngine, ER: VioletaBftEngine> {
    system: BatchSystem<PeerFsm<EK, ER>, StoreFsm<EK>>,
    apply_router: ApplyRouter<EK>,
    apply_system: ApplyBatchSystem<EK>,
    router: VioletaBftRouter<EK, ER>,
    workers: Option<Workers<EK>>,
}

impl<EK: CausetEngine, ER: VioletaBftEngine> VioletaBftBatchSystem<EK, ER> {
    pub fn router(&self) -> VioletaBftRouter<EK, ER> {
        self.router.clone()
    }

    pub fn apply_router(&self) -> ApplyRouter<EK> {
        self.apply_router.clone()
    }

    // TODO: reduce arguments
    pub fn spawn<T: Transport + 'static, C: FidelClient + 'static>(
        &mut self,
        meta: meta_timeshare::CausetStore,
        causet: Arc<VersionTrack<Config>>,
        engines: Engines<EK, ER>,
        trans: T,
        fidel_client: Arc<C>,
        mgr: SnapManager,
        fidel_worker: FutureWorker<FidelTask<EK>>,
        store_meta: Arc<Mutex<StoreMeta>>,
        mut interlock_host: InterlockHost<EK>,
        importer: Arc<SSTImporter>,
        split_check_worker: Worker<SplitCheckTask>,
        auto_split_controller: AutoSplitController,
        global_replication_state: Arc<Mutex<GlobalReplicationState>>,
        interlocking_directorate: ConcurrencyManager,
    ) -> Result<()> {
        assert!(self.workers.is_none());
        // TODO: we can get cluster meta regularly too later.

        // TODO load interlocks from configuration
        interlock_host
            .registry
            .register_admin_semaphore(100, BoxAdminSemaphore::new(SplitSemaphore));

        let workers = Workers {
            split_check_worker,
            brane_worker: Worker::new("snapshot-worker"),
            fidel_worker,
            consistency_check_worker: Worker::new("consistency-check"),
            cleanup_worker: Worker::new("cleanup-worker"),
            violetabftlog_gc_worker: Worker::new("violetabft-gc-worker"),
            interlock_host: interlock_host.clone(),
        };
        let mut builder = VioletaBftPollerBuilder {
            causet,
            store: meta,
            engines,
            router: self.router.clone(),
            split_check_interlock_semaphore: workers.split_check_worker.interlock_semaphore(),
            brane_interlock_semaphore: workers.brane_worker.interlock_semaphore(),
            fidel_interlock_semaphore: workers.fidel_worker.interlock_semaphore(),
            consistency_check_interlock_semaphore: workers.consistency_check_worker.interlock_semaphore(),
            cleanup_interlock_semaphore: workers.cleanup_worker.interlock_semaphore(),
            violetabftlog_gc_interlock_semaphore: workers.violetabftlog_gc_worker.interlock_semaphore(),
            apply_router: self.apply_router.clone(),
            trans,
            fidel_client,
            interlock_host: interlock_host.clone(),
            importer,
            snap_mgr: mgr,
            global_replication_state,
            global_stat: GlobalStoreStat::default(),
            store_meta,
            plightlikeing_create_peers: Arc::new(Mutex::new(HashMap::default())),
            applying_snap_count: Arc::new(AtomicUsize::new(0)),
        };
        let brane_peers = builder.init()?;
        let engine = builder.engines.kv.clone();
        if engine.support_write_batch_vec() {
            self.spacelike_system::<T, C, <EK as WriteBatchExt>::WriteBatchVec>(
                workers,
                brane_peers,
                builder,
                auto_split_controller,
                interlock_host,
                interlocking_directorate,
            )?;
        } else {
            self.spacelike_system::<T, C, <EK as WriteBatchExt>::WriteBatch>(
                workers,
                brane_peers,
                builder,
                auto_split_controller,
                interlock_host,
                interlocking_directorate,
            )?;
        }
        Ok(())
    }

    fn spacelike_system<T: Transport + 'static, C: FidelClient + 'static, W: WriteBatch<EK> + 'static>(
        &mut self,
        mut workers: Workers<EK>,
        brane_peers: Vec<lightlikeerFsmPair<EK, ER>>,
        builder: VioletaBftPollerBuilder<EK, ER, T, C>,
        auto_split_controller: AutoSplitController,
        interlock_host: InterlockHost<EK>,
        interlocking_directorate: ConcurrencyManager,
    ) -> Result<()> {
        builder.snap_mgr.init()?;

        let engines = builder.engines.clone();
        let snap_mgr = builder.snap_mgr.clone();
        let causet = builder.causet.value().clone();
        let store = builder.store.clone();
        let fidel_client = builder.fidel_client.clone();
        let importer = builder.importer.clone();

        let apply_poller_builder = ApplyPollerBuilder::<EK, W>::new(
            &builder,
            Box::new(self.router.clone()),
            self.apply_router.clone(),
        );
        self.apply_system
            .schedule_all(brane_peers.iter().map(|pair| pair.1.get_peer()));

        {
            let mut meta = builder.store_meta.dagger().unwrap();
            for (_, peer_fsm) in &brane_peers {
                let peer = peer_fsm.get_peer();
                meta.readers
                    .insert(peer_fsm.brane_id(), Readpushdown_causet::from_peer(peer));
            }
        }

        let router = Mutex::new(self.router.clone());
        fidel_client.handle_reconnect(move || {
            router
                .dagger()
                .unwrap()
                .broadcast_normal(|| PeerMsg::HeartbeatFidel);
        });

        let tag = format!("violetabftstore-{}", store.get_id());
        self.system.spawn(tag, builder);
        let mut mailboxes = Vec::with_capacity(brane_peers.len());
        let mut address = Vec::with_capacity(brane_peers.len());
        for (tx, fsm) in brane_peers {
            address.push(fsm.brane_id());
            mailboxes.push((fsm.brane_id(), BasicMailbox::new(tx, fsm)));
        }
        self.router.register_all(mailboxes);

        // Make sure Msg::Start is the first message each FSM received.
        for addr in address {
            self.router.force_lightlike(addr, PeerMsg::Start).unwrap();
        }
        self.router
            .lightlike_control(StoreMsg::Start {
                store: store.clone(),
            })
            .unwrap();

        self.apply_system
            .spawn("apply".to_owned(), apply_poller_builder);

        let brane_runner = BraneRunner::new(
            engines.clone(),
            snap_mgr,
            causet.snap_apply_batch_size.0 as usize,
            causet.use_delete_cone,
            workers.interlock_host.clone(),
            self.router(),
        );
        let timer = brane_runner.new_timer();
        box_try!(workers.brane_worker.spacelike_with_timer(brane_runner, timer));

        let violetabftlog_gc_runner = VioletaBftlogGcRunner::new(self.router(), engines.clone());
        let timer = violetabftlog_gc_runner.new_timer();
        box_try!(workers
            .violetabftlog_gc_worker
            .spacelike_with_timer(violetabftlog_gc_runner, timer));

        let compact_runner = CompactRunner::new(engines.kv.clone());
        let cleanup_sst_runner = CleanupSSTRunner::new(
            store.get_id(),
            self.router.clone(),
            Arc::clone(&importer),
            Arc::clone(&fidel_client),
        );
        let cleanup_runner = CleanupRunner::new(compact_runner, cleanup_sst_runner);
        box_try!(workers.cleanup_worker.spacelike(cleanup_runner));

        let fidel_runner = FidelRunner::new(
            store.get_id(),
            Arc::clone(&fidel_client),
            self.router.clone(),
            engines.kv,
            workers.fidel_worker.interlock_semaphore(),
            causet.fidel_store_heartbeat_tick_interval.0,
            auto_split_controller,
            interlocking_directorate,
        );
        box_try!(workers.fidel_worker.spacelike(fidel_runner));

        let consistency_check_runner =
            ConsistencyCheckRunner::<EK, _>::new(self.router.clone(), interlock_host);
        box_try!(workers
            .consistency_check_worker
            .spacelike(consistency_check_runner));

        if let Err(e) = sys_util::thread::set_priority(sys_util::HIGH_PRI) {
            warn!("set thread priority for violetabftstore failed"; "error" => ?e);
        }
        self.workers = Some(workers);
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if self.workers.is_none() {
            return;
        }
        let mut workers = self.workers.take().unwrap();
        // Wait all workers finish.
        let mut handles: Vec<Option<thread::JoinHandle<()>>> = vec![];
        handles.push(workers.split_check_worker.stop());
        handles.push(workers.brane_worker.stop());
        handles.push(workers.fidel_worker.stop());
        handles.push(workers.consistency_check_worker.stop());
        handles.push(workers.cleanup_worker.stop());
        handles.push(workers.violetabftlog_gc_worker.stop());
        self.apply_system.shutdown();
        self.system.shutdown();
        for h in handles {
            if let Some(h) = h {
                h.join().unwrap();
            }
        }
        workers.interlock_host.shutdown();
    }
}

pub fn create_violetabft_batch_system<EK: CausetEngine, ER: VioletaBftEngine>(
    causet: &Config,
) -> (VioletaBftRouter<EK, ER>, VioletaBftBatchSystem<EK, ER>) {
    let (store_tx, store_fsm) = StoreFsm::new(causet);
    let (apply_router, apply_system) = create_apply_batch_system(&causet);
    let (router, system) =
        batch_system::create_system(&causet.store_batch_system, store_tx, store_fsm);
    let violetabft_router = VioletaBftRouter { router };
    let system = VioletaBftBatchSystem {
        system,
        workers: None,
        apply_router,
        apply_system,
        router: violetabft_router.clone(),
    };
    (violetabft_router, system)
}

#[derive(Debug, PartialEq)]
enum CheckMsgStatus {
    // The message is the first request vote message to an existing peer.
    FirstRequestVote,
    // The message can be dropped silently
    DropMsg,
    // Try to create the peer
    NewPeer,
    // Try to create the peer which is the first one of this brane on local store.
    NewPeerFirst,
}

impl<'a, EK: CausetEngine, ER: VioletaBftEngine, T: Transport, C: FidelClient>
    StoreFsmpushdown_causet<'a, EK, ER, T, C>
{
    /// Checks if the message is targeting a stale peer.
    fn check_msg(&mut self, msg: &VioletaBftMessage) -> Result<CheckMsgStatus> {
        let brane_id = msg.get_brane_id();
        let from_epoch = msg.get_brane_epoch();
        let msg_type = msg.get_message().get_msg_type();
        let from_store_id = msg.get_from_peer().get_store_id();
        let to_peer_id = msg.get_to_peer().get_id();

        // Check if the target peer is tombstone.
        let state_key = tuplespaceInstanton::brane_state_key(brane_id);
        let local_state: BraneLocalState =
            match self.ctx.engines.kv.get_msg_causet(Causet_VIOLETABFT, &state_key)? {
                Some(state) => state,
                None => return Ok(CheckMsgStatus::NewPeerFirst),
            };

        if local_state.get_state() != PeerState::Tombstone {
            // Maybe split, but not registered yet.
            if !util::is_first_vote_msg(msg.get_message()) {
                self.ctx.violetabft_metrics.message_dropped.brane_nonexistent += 1;
                return Err(box_err!(
                    "[brane {}] brane not exist but not tombstone: {:?}",
                    brane_id,
                    local_state
                ));
            }
            info!(
                "brane doesn't exist yet, wait for it to be split";
                "brane_id" => brane_id
            );
            return Ok(CheckMsgStatus::FirstRequestVote);
        }
        debug!(
            "brane is in tombstone state";
            "brane_id" => brane_id,
            "brane_local_state" => ?local_state,
        );
        let brane = local_state.get_brane();
        let brane_epoch = brane.get_brane_epoch();
        if local_state.has_merge_state() {
            info!(
                "merged peer receives a stale message";
                "brane_id" => brane_id,
                "current_brane_epoch" => ?brane_epoch,
                "msg_type" => ?msg_type,
            );

            let merge_target = if let Some(peer) = util::find_peer(brane, from_store_id) {
                // Maybe the target is promoted from learner to voter, but the follower
                // doesn't know it. So we only compare peer id.
                assert_eq!(peer.get_id(), msg.get_from_peer().get_id());
                // Let stale peer decides whether it should wait for merging or just remove
                // itself.
                Some(local_state.get_merge_state().get_target().to_owned())
            } else {
                // If a peer is isolated before prepare_merge and conf remove, it should just
                // remove itself.
                None
            };
            self.ctx
                .handle_stale_msg(msg, brane_epoch.clone(), true, merge_target);
            return Ok(CheckMsgStatus::DropMsg);
        }
        // The brane in this peer is already destroyed
        if util::is_epoch_stale(from_epoch, brane_epoch) {
            self.ctx.violetabft_metrics.message_dropped.brane_tombstone_peer += 1;
            info!(
                "tombstone peer receives a stale message";
                "brane_id" => brane_id,
                "from_brane_epoch" => ?from_epoch,
                "current_brane_epoch" => ?brane_epoch,
                "msg_type" => ?msg_type,
            );
            let mut need_gc_msg = util::is_vote_msg(msg.get_message());
            if msg.has_extra_msg() {
                // A learner can't vote so it lightlikes the check-stale-peer msg to others to find out whether
                // it is removed due to conf change or merge.
                need_gc_msg |=
                    msg.get_extra_msg().get_type() == ExtraMessageType::MsgCheckStalePeer;
                // For backward compatibility
                need_gc_msg |= msg.get_extra_msg().get_type() == ExtraMessageType::MsgBraneWakeUp;
            }
            let not_exist = util::find_peer(brane, from_store_id).is_none();
            self.ctx
                .handle_stale_msg(msg, brane_epoch.clone(), need_gc_msg && not_exist, None);

            if need_gc_msg && !not_exist {
                let mut lightlike_msg = VioletaBftMessage::default();
                lightlike_msg.set_brane_id(brane_id);
                lightlike_msg.set_from_peer(msg.get_to_peer().clone());
                lightlike_msg.set_to_peer(msg.get_from_peer().clone());
                lightlike_msg.set_brane_epoch(brane_epoch.clone());
                let extra_msg = lightlike_msg.mut_extra_msg();
                extra_msg.set_type(ExtraMessageType::MsgCheckStalePeerResponse);
                extra_msg.set_check_peers(brane.get_peers().into());
                if let Err(e) = self.ctx.trans.lightlike(lightlike_msg) {
                    error!(?e;
                        "lightlike check stale peer response message failed";
                        "brane_id" => brane_id,
                    );
                }
                self.ctx.need_flush_trans = true;
            }

            return Ok(CheckMsgStatus::DropMsg);
        }
        // A tombstone peer may not apply the conf change log which removes itself.
        // In this case, the local epoch is stale and the local peer can be found from brane.
        // We can compare the local peer id with to_peer_id to verify whether it is correct to create a new peer.
        if let Some(local_peer_id) =
            util::find_peer(brane, self.ctx.store_id()).map(|r| r.get_id())
        {
            if to_peer_id <= local_peer_id {
                self.ctx.violetabft_metrics.message_dropped.brane_tombstone_peer += 1;
                info!(
                    "tombstone peer receives a stale message, local_peer_id >= to_peer_id in msg";
                    "brane_id" => brane_id,
                    "local_peer_id" => local_peer_id,
                    "to_peer_id" => to_peer_id,
                    "msg_type" => ?msg_type
                );
                return Ok(CheckMsgStatus::DropMsg);
            }
        }
        Ok(CheckMsgStatus::NewPeer)
    }

    fn on_violetabft_message(&mut self, mut msg: VioletaBftMessage) -> Result<()> {
        let brane_id = msg.get_brane_id();
        match self.ctx.router.lightlike(brane_id, PeerMsg::VioletaBftMessage(msg)) {
            Ok(()) | Err(TrylightlikeError::Full(_)) => return Ok(()),
            Err(TrylightlikeError::Disconnected(_)) if self.ctx.router.is_shutdown() => return Ok(()),
            Err(TrylightlikeError::Disconnected(PeerMsg::VioletaBftMessage(m))) => msg = m,
            e => panic!(
                "[store {}] [brane {}] unexpected redirect error: {:?}",
                self.fsm.store.id, brane_id, e
            ),
        }

        debug!(
            "handle violetabft message";
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
            "store_id" => self.fsm.store.id,
            "brane_id" => brane_id,
            "msg_type" => ?msg.get_message().get_msg_type(),
        );

        if msg.get_to_peer().get_store_id() != self.ctx.store_id() {
            warn!(
                "store not match, ignore it";
                "store_id" => self.ctx.store_id(),
                "to_store_id" => msg.get_to_peer().get_store_id(),
                "brane_id" => brane_id,
            );
            self.ctx.violetabft_metrics.message_dropped.mismatch_store_id += 1;
            return Ok(());
        }

        if !msg.has_brane_epoch() {
            error!(
                "missing epoch in violetabft message, ignore it";
                "brane_id" => brane_id,
            );
            self.ctx.violetabft_metrics.message_dropped.mismatch_brane_epoch += 1;
            return Ok(());
        }
        if msg.get_is_tombstone() || msg.has_merge_target() {
            // Target tombstone peer doesn't exist, so ignore it.
            return Ok(());
        }
        let check_msg_status = self.check_msg(&msg)?;
        let is_first_request_vote = match check_msg_status {
            CheckMsgStatus::DropMsg => return Ok(()),
            CheckMsgStatus::FirstRequestVote => true,
            CheckMsgStatus::NewPeer | CheckMsgStatus::NewPeerFirst => {
                if !self.maybe_create_peer(
                    brane_id,
                    &msg,
                    check_msg_status == CheckMsgStatus::NewPeerFirst,
                )? {
                    if !util::is_first_vote_msg(msg.get_message()) {
                        // Can not create peer from the message and it's not the
                        // first request vote message.
                        return Ok(());
                    }
                    true
                } else {
                    false
                }
            }
        };
        if is_first_request_vote {
            // To void losing request vote messages, either put it to
            // plightlikeing_votes or force lightlike.
            let mut store_meta = self.ctx.store_meta.dagger().unwrap();
            if !store_meta.branes.contains_key(&brane_id) {
                store_meta.plightlikeing_votes.push(msg);
                return Ok(());
            }
            if let Err(e) = self
                .ctx
                .router
                .force_lightlike(brane_id, PeerMsg::VioletaBftMessage(msg))
            {
                warn!("handle first request vote failed"; "brane_id" => brane_id, "error" => ?e);
            }
            return Ok(());
        }

        let _ = self.ctx.router.lightlike(brane_id, PeerMsg::VioletaBftMessage(msg));
        Ok(())
    }

    /// If target peer doesn't exist, create it.
    ///
    /// return false to indicate that target peer is in invalid state or
    /// doesn't exist and can't be created.
    fn maybe_create_peer(
        &mut self,
        brane_id: u64,
        msg: &VioletaBftMessage,
        is_local_first: bool,
    ) -> Result<bool> {
        if !is_initial_msg(msg.get_message()) {
            let msg_type = msg.get_message().get_msg_type();
            debug!(
                "target peer doesn't exist, stale message";
                "target_peer" => ?msg.get_to_peer(),
                "brane_id" => brane_id,
                "msg_type" => ?msg_type,
            );
            self.ctx.violetabft_metrics.message_dropped.stale_msg += 1;
            return Ok(false);
        }

        if is_local_first {
            let mut plightlikeing_create_peers = self.ctx.plightlikeing_create_peers.dagger().unwrap();
            if plightlikeing_create_peers.contains_key(&brane_id) {
                return Ok(false);
            }
            plightlikeing_create_peers.insert(brane_id, (msg.get_to_peer().get_id(), false));
        }

        let res = self.maybe_create_peer_internal(brane_id, &msg, is_local_first);
        // If failed, i.e. Err or Ok(false), remove this peer data from `plightlikeing_create_peers`.
        if res.as_ref().map_or(true, |b| !*b) && is_local_first {
            let mut plightlikeing_create_peers = self.ctx.plightlikeing_create_peers.dagger().unwrap();
            if let Some(status) = plightlikeing_create_peers.get(&brane_id) {
                if *status == (msg.get_to_peer().get_id(), false) {
                    plightlikeing_create_peers.remove(&brane_id);
                }
            }
        }
        res
    }

    fn maybe_create_peer_internal(
        &mut self,
        brane_id: u64,
        msg: &VioletaBftMessage,
        is_local_first: bool,
    ) -> Result<bool> {
        if is_local_first {
            if self
                .ctx
                .engines
                .kv
                .get_value_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(brane_id))?
                .is_some()
            {
                return Ok(false);
            }
        }

        let target = msg.get_to_peer();

        let mut meta = self.ctx.store_meta.dagger().unwrap();
        if meta.branes.contains_key(&brane_id) {
            return Ok(true);
        }

        if is_local_first {
            let plightlikeing_create_peers = self.ctx.plightlikeing_create_peers.dagger().unwrap();
            match plightlikeing_create_peers.get(&brane_id) {
                Some(status) if *status == (msg.get_to_peer().get_id(), false) => (),
                // If changed, it means this peer has been/will be replaced from the new one from splitting.
                _ => return Ok(false),
            }
            // Note that `StoreMeta` dagger is held and status is (peer_id, false) in `plightlikeing_create_peers` now.
            // If this peer is created from splitting latter and then status in `plightlikeing_create_peers` is changed,
            // that peer creation in `on_ready_split_brane` must be executed **after** current peer creation
            // because of the `StoreMeta` dagger.
        }

        let mut is_overlapped = false;
        let mut branes_to_destroy = vec![];
        for (_, id) in meta.brane_cones.cone((
            Excluded(data_key(msg.get_spacelike_key())),
            Unbounded::<Vec<u8>>,
        )) {
            let exist_brane = &meta.branes[&id];
            if enc_spacelike_key(exist_brane) >= data_lightlike_key(msg.get_lightlike_key()) {
                break;
            }

            debug!(
                "msg is overlapped with exist brane";
                "brane_id" => brane_id,
                "msg" => ?msg,
                "exist_brane" => ?exist_brane,
            );
            let (can_destroy, merge_to_this_peer) = maybe_destroy_source(
                &meta,
                brane_id,
                target.get_id(),
                exist_brane.get_id(),
                msg.get_brane_epoch().to_owned(),
            );
            if can_destroy {
                if !merge_to_this_peer {
                    branes_to_destroy.push(exist_brane.get_id());
                } else {
                    error!(
                        "A new peer has a merge source peer";
                        "brane_id" => brane_id,
                        "peer_id" => target.get_id(),
                        "source_brane" => ?exist_brane,
                    );
                    if self.ctx.causet.dev_assert {
                        panic!("something is wrong, maybe FIDel do not ensure all target peers exist before merging");
                    }
                }
                continue;
            }
            is_overlapped = true;
            if msg.get_brane_epoch().get_version() > exist_brane.get_brane_epoch().get_version()
            {
                // If new brane's epoch version is greater than exist brane's, the exist brane
                // may has been merged/splitted already.
                let _ = self.ctx.router.force_lightlike(
                    exist_brane.get_id(),
                    PeerMsg::CasualMessage(CasualMessage::BraneOverlapped),
                );
            }
        }

        if is_overlapped {
            self.ctx.violetabft_metrics.message_dropped.brane_overlap += 1;
            return Ok(false);
        }

        for id in branes_to_destroy {
            self.ctx
                .router
                .force_lightlike(
                    id,
                    PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
                        target_brane_id: brane_id,
                        target: target.clone(),
                        result: MergeResultKind::Stale,
                    }),
                )
                .unwrap();
        }

        // New created peers should know it's learner or not.
        let (tx, mut peer) = PeerFsm::replicate(
            self.ctx.store_id(),
            &self.ctx.causet,
            self.ctx.brane_interlock_semaphore.clone(),
            self.ctx.engines.clone(),
            brane_id,
            target.clone(),
        )?;

        // WARNING: The checking code must be above this line.
        // Now all checking passed

        let mut replication_state = self.ctx.global_replication_state.dagger().unwrap();
        peer.peer.init_replication_mode(&mut *replication_state);
        drop(replication_state);

        peer.peer.local_first_replicate = is_local_first;

        // Following snapshot may overlap, should insert into brane_cones after
        // snapshot is applied.
        meta.branes
            .insert(brane_id, peer.get_peer().brane().to_owned());

        let mailbox = BasicMailbox::new(tx, peer);
        self.ctx.router.register(brane_id, mailbox);
        self.ctx
            .router
            .force_lightlike(brane_id, PeerMsg::Start)
            .unwrap();
        Ok(true)
    }

    fn on_compaction_finished(&mut self, event: EK::CompactedEvent) {
        if event.is_size_declining_trivial(self.ctx.causet.brane_split_check_diff.0) {
            return;
        }

        let output_level_str = event.output_level_label();
        COMPACTION_DECLINED_BYTES
            .with_label_values(&[&output_level_str])
            .observe(event.total_bytes_declined() as f64);

        // self.causet.brane_split_check_diff.0 / 16 is an experienced value.
        let mut brane_declined_bytes = {
            let meta = self.ctx.store_meta.dagger().unwrap();
            event.calc_cones_declined_bytes(
                &meta.brane_cones,
                self.ctx.causet.brane_split_check_diff.0 / 16,
            )
        };

        COMPACTION_RELATED_REGION_COUNT
            .with_label_values(&[&output_level_str])
            .observe(brane_declined_bytes.len() as f64);

        for (brane_id, declined_bytes) in brane_declined_bytes.drain(..) {
            let _ = self.ctx.router.lightlike(
                brane_id,
                PeerMsg::CasualMessage(CasualMessage::CompactionDeclinedBytes {
                    bytes: declined_bytes,
                }),
            );
        }
    }

    fn register_compact_check_tick(&self) {
        self.ctx.schedule_store_tick(
            StoreTick::CompactCheck,
            self.ctx.causet.brane_compact_check_interval.0,
        )
    }

    fn on_compact_check_tick(&mut self) {
        self.register_compact_check_tick();
        if self.ctx.cleanup_interlock_semaphore.is_busy() {
            debug!(
                "compact worker is busy, check space redundancy next time";
                "store_id" => self.fsm.store.id,
            );
            return;
        }

        if self
            .ctx
            .engines
            .kv
            .auto_compactions_is_disabled()
            .expect("causet")
        {
            debug!(
                "skip compact check when disabled auto compactions";
                "store_id" => self.fsm.store.id,
            );
            return;
        }

        // Start from last checked key.
        let mut cones_need_check =
            Vec::with_capacity(self.ctx.causet.brane_compact_check_step as usize + 1);
        cones_need_check.push(self.fsm.store.last_compact_checked_key.clone());

        let largest_key = {
            let meta = self.ctx.store_meta.dagger().unwrap();
            if meta.brane_cones.is_empty() {
                debug!(
                    "there is no cone need to check";
                    "store_id" => self.fsm.store.id
                );
                return;
            }

            // Collect continuous cones.
            let left_cones = meta.brane_cones.cone((
                Excluded(self.fsm.store.last_compact_checked_key.clone()),
                Unbounded::<Key>,
            ));
            cones_need_check.extlightlike(
                left_cones
                    .take(self.ctx.causet.brane_compact_check_step as usize)
                    .map(|(k, _)| k.to_owned()),
            );

            // fidelio last_compact_checked_key.
            meta.brane_cones.tuplespaceInstanton().last().unwrap().to_vec()
        };

        let last_key = cones_need_check.last().unwrap().clone();
        if last_key == largest_key {
            // Cone [largest key, DATA_MAX_KEY) also need to check.
            if last_key != tuplespaceInstanton::DATA_MAX_KEY.to_vec() {
                cones_need_check.push(tuplespaceInstanton::DATA_MAX_KEY.to_vec());
            }
            // Next task will spacelike from the very beginning.
            self.fsm.store.last_compact_checked_key = tuplespaceInstanton::DATA_MIN_KEY.to_vec();
        } else {
            self.fsm.store.last_compact_checked_key = last_key;
        }

        // Schedule the task.
        let causet_names = vec![Causet_DEFAULT.to_owned(), Causet_WRITE.to_owned()];
        if let Err(e) = self.ctx.cleanup_interlock_semaphore.schedule(CleanupTask::Compact(
            CompactTask::CheckAndCompact {
                causet_names,
                cones: cones_need_check,
                tombstones_num_memory_barrier: self.ctx.causet.brane_compact_min_tombstones,
                tombstones_percent_memory_barrier: self.ctx.causet.brane_compact_tombstones_percent,
            },
        )) {
            error!(
                "schedule space check task failed";
                "store_id" => self.fsm.store.id,
                "err" => ?e,
            );
        }
    }

    fn store_heartbeat_fidel(&mut self) {
        let mut stats = StoreStats::default();

        let used_size = self.ctx.snap_mgr.get_total_snap_size();
        stats.set_used_size(used_size);
        stats.set_store_id(self.ctx.store_id());
        {
            let meta = self.ctx.store_meta.dagger().unwrap();
            stats.set_brane_count(meta.branes.len() as u32);
        }

        let snap_stats = self.ctx.snap_mgr.stats();
        stats.set_lightlikeing_snap_count(snap_stats.lightlikeing_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["lightlikeing"])
            .set(snap_stats.lightlikeing_count as i64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["receiving"])
            .set(snap_stats.receiving_count as i64);

        let apply_snapshot_count = self.ctx.applying_snap_count.load(Ordering::SeqCst);
        stats.set_applying_snap_count(apply_snapshot_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["applying"])
            .set(apply_snapshot_count as i64);

        stats.set_spacelike_time(self.fsm.store.spacelike_time.unwrap().sec as u32);

        // report store write flow to fidel
        stats.set_bytes_written(
            self.ctx
                .global_stat
                .stat
                .engine_total_bytes_written
                .swap(0, Ordering::SeqCst),
        );
        stats.set_tuplespaceInstanton_written(
            self.ctx
                .global_stat
                .stat
                .engine_total_tuplespaceInstanton_written
                .swap(0, Ordering::SeqCst),
        );

        stats.set_is_busy(
            self.ctx
                .global_stat
                .stat
                .is_busy
                .swap(false, Ordering::SeqCst),
        );

        let store_info = StoreInfo {
            engine: self.ctx.engines.kv.clone(),
            capacity: self.ctx.causet.capacity.0,
        };

        let task = FidelTask::StoreHeartbeat { stats, store_info };
        if let Err(e) = self.ctx.fidel_interlock_semaphore.schedule(task) {
            error!("notify fidel failed";
                "store_id" => self.fsm.store.id,
                "err" => ?e
            );
        }
    }

    fn on_fidel_store_heartbeat_tick(&mut self) {
        self.store_heartbeat_fidel();
        self.register_fidel_store_heartbeat_tick();
    }

    fn handle_snap_mgr_gc(&mut self) -> Result<()> {
        fail_point!("peer_2_handle_snap_mgr_gc", self.fsm.store.id == 2, |_| Ok(
            ()
        ));
        let snap_tuplespaceInstanton = self.ctx.snap_mgr.list_idle_snap()?;
        if snap_tuplespaceInstanton.is_empty() {
            return Ok(());
        }
        let (mut last_brane_id, mut tuplespaceInstanton) = (0, vec![]);
        let schedule_gc_snap = |brane_id: u64, snaps| -> Result<()> {
            debug!(
                "schedule snap gc";
                "store_id" => self.fsm.store.id,
                "brane_id" => brane_id,
            );
            let gc_snap = PeerMsg::CasualMessage(CasualMessage::GcSnap { snaps });
            match self.ctx.router.lightlike(brane_id, gc_snap) {
                Ok(()) => Ok(()),
                Err(TrylightlikeError::Disconnected(_)) if self.ctx.router.is_shutdown() => Ok(()),
                Err(TrylightlikeError::Disconnected(PeerMsg::CasualMessage(
                    CasualMessage::GcSnap { snaps },
                ))) => {
                    // The snapshot exists because MsgApplightlike has been rejected. So the
                    // peer must have been exist. But now it's disconnected, so the peer
                    // has to be destroyed instead of being created.
                    info!(
                        "brane is disconnected, remove snaps";
                        "brane_id" => brane_id,
                        "snaps" => ?snaps,
                    );
                    for (key, is_lightlikeing) in snaps {
                        let snap = if is_lightlikeing {
                            self.ctx.snap_mgr.get_snapshot_for_lightlikeing(&key)?
                        } else {
                            self.ctx.snap_mgr.get_snapshot_for_applying(&key)?
                        };
                        self.ctx
                            .snap_mgr
                            .delete_snapshot(&key, snap.as_ref(), false);
                    }
                    Ok(())
                }
                Err(TrylightlikeError::Full(_)) => Ok(()),
                Err(TrylightlikeError::Disconnected(_)) => unreachable!(),
            }
        };
        for (key, is_lightlikeing) in snap_tuplespaceInstanton {
            if last_brane_id == key.brane_id {
                tuplespaceInstanton.push((key, is_lightlikeing));
                continue;
            }

            if !tuplespaceInstanton.is_empty() {
                schedule_gc_snap(last_brane_id, tuplespaceInstanton)?;
                tuplespaceInstanton = vec![];
            }

            last_brane_id = key.brane_id;
            tuplespaceInstanton.push((key, is_lightlikeing));
        }
        if !tuplespaceInstanton.is_empty() {
            schedule_gc_snap(last_brane_id, tuplespaceInstanton)?;
        }
        Ok(())
    }

    fn on_snap_mgr_gc(&mut self) {
        if let Err(e) = self.handle_snap_mgr_gc() {
            error!(?e;
                "handle gc snap failed";
                "store_id" => self.fsm.store.id,
            );
        }
        self.register_snap_mgr_gc_tick();
    }

    fn on_compact_lock_causet(&mut self) {
        // Create a compact dagger causet task(compact whole cone) and schedule directly.
        let lock_causet_bytes_written = self
            .ctx
            .global_stat
            .stat
            .lock_causet_bytes_written
            .load(Ordering::SeqCst);
        if lock_causet_bytes_written > self.ctx.causet.lock_causet_compact_bytes_memory_barrier.0 {
            self.ctx
                .global_stat
                .stat
                .lock_causet_bytes_written
                .fetch_sub(lock_causet_bytes_written, Ordering::SeqCst);

            let task = CompactTask::Compact {
                causet_name: String::from(Causet_DAGGER),
                spacelike_key: None,
                lightlike_key: None,
            };
            if let Err(e) = self
                .ctx
                .cleanup_interlock_semaphore
                .schedule(CleanupTask::Compact(task))
            {
                error!(
                    "schedule compact dagger causet task failed";
                    "store_id" => self.fsm.store.id,
                    "err" => ?e,
                );
            }
        }

        self.register_compact_lock_causet_tick();
    }

    fn register_fidel_store_heartbeat_tick(&self) {
        self.ctx.schedule_store_tick(
            StoreTick::FidelStoreHeartbeat,
            self.ctx.causet.fidel_store_heartbeat_tick_interval.0,
        );
    }

    fn register_snap_mgr_gc_tick(&self) {
        self.ctx
            .schedule_store_tick(StoreTick::SnapGc, self.ctx.causet.snap_mgr_gc_tick_interval.0)
    }

    fn register_compact_lock_causet_tick(&self) {
        self.ctx.schedule_store_tick(
            StoreTick::CompactLockCf,
            self.ctx.causet.lock_causet_compact_interval.0,
        )
    }
}

impl<'a, EK: CausetEngine, ER: VioletaBftEngine, T: Transport, C: FidelClient>
    StoreFsmpushdown_causet<'a, EK, ER, T, C>
{
    fn on_validate_sst_result(&mut self, ssts: Vec<SstMeta>) {
        if ssts.is_empty() {
            return;
        }
        // A stale peer can still ingest a stale SST before it is
        // destroyed. We need to make sure that no stale peer exists.
        let mut delete_ssts = Vec::new();
        {
            let meta = self.ctx.store_meta.dagger().unwrap();
            for sst in ssts {
                if !meta.branes.contains_key(&sst.get_brane_id()) {
                    delete_ssts.push(sst);
                }
            }
        }
        if delete_ssts.is_empty() {
            return;
        }

        let task = CleanupSSTTask::DeleteSST { ssts: delete_ssts };
        if let Err(e) = self
            .ctx
            .cleanup_interlock_semaphore
            .schedule(CleanupTask::CleanupSST(task))
        {
            error!(
                "schedule to delete ssts failed";
                "store_id" => self.fsm.store.id,
                "err" => ?e,
            );
        }
    }

    fn on_cleanup_import_sst(&mut self) -> Result<()> {
        let mut delete_ssts = Vec::new();
        let mut validate_ssts = Vec::new();

        let ssts = box_try!(self.ctx.importer.list_ssts());
        if ssts.is_empty() {
            return Ok(());
        }
        {
            let meta = self.ctx.store_meta.dagger().unwrap();
            for sst in ssts {
                if let Some(r) = meta.branes.get(&sst.get_brane_id()) {
                    let brane_epoch = r.get_brane_epoch();
                    if util::is_epoch_stale(sst.get_brane_epoch(), brane_epoch) {
                        // If the SST epoch is stale, it will not be ingested anymore.
                        delete_ssts.push(sst);
                    }
                } else {
                    // If the peer doesn't exist, we need to validate the SST through FIDel.
                    validate_ssts.push(sst);
                }
            }
        }

        if !delete_ssts.is_empty() {
            let task = CleanupSSTTask::DeleteSST { ssts: delete_ssts };
            if let Err(e) = self
                .ctx
                .cleanup_interlock_semaphore
                .schedule(CleanupTask::CleanupSST(task))
            {
                error!(
                    "schedule to delete ssts failed";
                    "store_id" => self.fsm.store.id,
                    "err" => ?e
                );
            }
        }

        if !validate_ssts.is_empty() {
            let task = CleanupSSTTask::ValidateSST {
                ssts: validate_ssts,
            };
            if let Err(e) = self
                .ctx
                .cleanup_interlock_semaphore
                .schedule(CleanupTask::CleanupSST(task))
            {
                error!(
                   "schedule to validate ssts failed";
                   "store_id" => self.fsm.store.id,
                   "err" => ?e,
                );
            }
        }

        Ok(())
    }

    fn register_consistency_check_tick(&mut self) {
        self.ctx.schedule_store_tick(
            StoreTick::ConsistencyCheck,
            self.ctx.causet.consistency_check_interval.0,
        )
    }

    fn on_consistency_check_tick(&mut self) {
        self.register_consistency_check_tick();
        if self.ctx.consistency_check_interlock_semaphore.is_busy() {
            return;
        }
        let (mut target_brane_id, mut oldest) = (0, Instant::now());
        let target_peer = {
            let meta = self.ctx.store_meta.dagger().unwrap();
            for brane_id in meta.branes.tuplespaceInstanton() {
                match self.fsm.store.consistency_check_time.get(brane_id) {
                    Some(time) => {
                        if *time < oldest {
                            oldest = *time;
                            target_brane_id = *brane_id;
                        }
                    }
                    None => {
                        target_brane_id = *brane_id;
                        break;
                    }
                }
            }
            if target_brane_id == 0 {
                return;
            }
            match util::find_peer(&meta.branes[&target_brane_id], self.ctx.store_id()) {
                None => return,
                Some(p) => p.clone(),
            }
        };
        info!(
            "scheduling consistency check for brane";
            "store_id" => self.fsm.store.id,
            "brane_id" => target_brane_id,
        );
        self.fsm
            .store
            .consistency_check_time
            .insert(target_brane_id, Instant::now());
        let mut request = new_admin_request(target_brane_id, target_peer);
        let mut admin = AdminRequest::default();
        admin.set_cmd_type(AdminCmdType::ComputeHash);
        self.ctx
            .interlock_host
            .on_prepropose_compute_hash(admin.mut_compute_hash());
        request.set_admin_request(admin);

        let _ = self.ctx.router.lightlike(
            target_brane_id,
            PeerMsg::VioletaBftCommand(VioletaBftCommand::new(request, Callback::None)),
        );
    }

    fn on_cleanup_import_sst_tick(&mut self) {
        if let Err(e) = self.on_cleanup_import_sst() {
            error!(?e;
                "cleanup import sst failed";
                "store_id" => self.fsm.store.id,
            );
        }
        self.register_cleanup_import_sst_tick();
    }

    fn register_cleanup_import_sst_tick(&self) {
        self.ctx.schedule_store_tick(
            StoreTick::CleanupImportSST,
            self.ctx.causet.cleanup_import_sst_interval.0,
        )
    }

    fn clear_brane_size_in_cone(&mut self, spacelike_key: &[u8], lightlike_key: &[u8]) {
        let spacelike_key = data_key(spacelike_key);
        let lightlike_key = data_lightlike_key(lightlike_key);

        let mut branes = vec![];
        {
            let meta = self.ctx.store_meta.dagger().unwrap();
            for (_, brane_id) in meta
                .brane_cones
                .cone((Excluded(spacelike_key), Included(lightlike_key)))
            {
                branes.push(*brane_id);
            }
        }
        for brane_id in branes {
            let _ = self.ctx.router.lightlike(
                brane_id,
                PeerMsg::CasualMessage(CasualMessage::ClearBraneSize),
            );
        }
    }

    fn on_store_unreachable(&mut self, store_id: u64) {
        let now = Instant::now();
        if self
            .fsm
            .store
            .last_unreachable_report
            .get(&store_id)
            .map_or(UNREACHABLE_BACKOFF, |t| now.duration_since(*t))
            < UNREACHABLE_BACKOFF
        {
            return;
        }
        info!(
            "broadcasting unreachable";
            "store_id" => self.fsm.store.id,
            "unreachable_store_id" => store_id,
        );
        self.fsm.store.last_unreachable_report.insert(store_id, now);
        // It's possible to acquire the dagger and only lightlike notification to
        // involved branes. However loop over all the branes can take a
        // lot of time, which may block other operations.
        self.ctx.router.report_unreachable(store_id);
    }

    fn on_fidelio_replication_mode(&mut self, status: ReplicationStatus) {
        let mut state = self.ctx.global_replication_state.dagger().unwrap();
        if state.status().mode == status.mode {
            if status.get_mode() == ReplicationMode::Majority {
                return;
            }
            let exist_dr = state.status().get_dr_auto_sync();
            let dr = status.get_dr_auto_sync();
            if exist_dr.state_id == dr.state_id && exist_dr.state == dr.state {
                return;
            }
        }
        info!("ufidelating replication mode"; "status" => ?status);
        state.set_status(status);
        drop(state);
        self.ctx.router.report_status_fidelio()
    }

    fn register_violetabft_engine_purge_tick(&self) {
        self.ctx.schedule_store_tick(
            StoreTick::VioletaBftEnginePurge,
            self.ctx.causet.violetabft_engine_purge_interval.0,
        )
    }

    fn on_violetabft_engine_purge_tick(&self) {
        let interlock_semaphore = &self.ctx.violetabftlog_gc_interlock_semaphore;
        let _ = interlock_semaphore.schedule(VioletaBftlogGcTask::Purge);
        self.register_violetabft_engine_purge_tick();
    }
}

#[causet(test)]
mod tests {
    use engine_lmdb::ConeOffsets;
    use engine_lmdb::ConeProperties;
    use engine_lmdb::LmdbCompactedEvent;

    use super::*;

    #[test]
    fn test_calc_brane_declined_bytes() {
        let prop = ConeProperties {
            offsets: vec![
                (
                    b"a".to_vec(),
                    ConeOffsets {
                        size: 4 * 1024,
                        tuplespaceInstanton: 1,
                    },
                ),
                (
                    b"b".to_vec(),
                    ConeOffsets {
                        size: 8 * 1024,
                        tuplespaceInstanton: 2,
                    },
                ),
                (
                    b"c".to_vec(),
                    ConeOffsets {
                        size: 12 * 1024,
                        tuplespaceInstanton: 3,
                    },
                ),
            ],
        };
        let event = LmdbCompactedEvent {
            causet: "default".to_owned(),
            output_level: 3,
            total_input_bytes: 12 * 1024,
            total_output_bytes: 0,
            spacelike_key: prop.smallest_key().unwrap(),
            lightlike_key: prop.largest_key().unwrap(),
            input_props: vec![prop],
            output_props: vec![],
        };

        let mut brane_cones = BTreeMap::new();
        brane_cones.insert(b"a".to_vec(), 1);
        brane_cones.insert(b"b".to_vec(), 2);
        brane_cones.insert(b"c".to_vec(), 3);

        let declined_bytes = event.calc_cones_declined_bytes(&brane_cones, 1024);
        let expected_declined_bytes = vec![(2, 8192), (3, 4096)];
        assert_eq!(declined_bytes, expected_declined_bytes);
    }
}
