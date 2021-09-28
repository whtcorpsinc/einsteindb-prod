//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::collections::VecDeque;
use std::time::Instant;
use std::{cmp, u64};

use batch_system::{BasicMailbox, Fsm};
use edb::Causet_VIOLETABFT;
use edb::{Engines, CausetEngine, VioletaBftEngine, WriteBatchExt};
use error_code::ErrorCodeExt;
use ekvproto::error_timeshare;
use ekvproto::import_sst_timeshare::SstMeta;
use ekvproto::meta_timeshare::{self, Brane, BraneEpoch};
use ekvproto::fidel_timeshare::CheckPolicy;
use ekvproto::violetabft_cmd_timeshare::{
    AdminCmdType, AdminRequest, CmdType, VioletaBftCmdRequest, VioletaBftCmdResponse, Request, StatusCmdType,
    StatusResponse,
};
use ekvproto::violetabft_server_timeshare::{
    ExtraMessageType, MergeState, PeerState, VioletaBftMessage, VioletaBftSnapshotData, VioletaBftTruncatedState,
    BraneLocalState,
};
use ekvproto::replication_mode_timeshare::{DrAutoSyncState, ReplicationMode};
use fidel_client::FidelClient;
use protobuf::Message;
use violetabft::evioletabft_timeshare::{ConfChangeType, MessageType};
use violetabft::{self, SnapshotStatus, INVALID_INDEX, NO_LIMIT};
use violetabft::{Ready, StateRole};
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::mpsc::{self, LooseBoundedlightlikeer, Receiver};
use violetabftstore::interlock::::time::duration_to_sec;
use violetabftstore::interlock::::worker::{Interlock_Semaphore, Stopped};
use violetabftstore::interlock::::{escape, is_zero_duration, Either};

use crate::interlock::BraneChangeEvent;
use crate::store::cmd_resp::{bind_term, new_error};
use crate::store::fsm::store::{PollContext, StoreMeta};
use crate::store::fsm::{
    apply, ApplyMetrics, ApplyTask, ApplyTaskRes, CatchUpLogs, ChangeCmd, ChangePeer, ExecResult,
};
use crate::store::local_metrics::VioletaBftProposeMetrics;
use crate::store::metrics::*;
use crate::store::msg::Callback;
use crate::store::peer::{ConsistencyState, Peer, StaleState};
use crate::store::peer_causet_storage::{ApplySnapResult, InvokeContext};
use crate::store::transport::Transport;
use crate::store::util::{is_learner, TuplespaceInstantonInfoFormatter};
use crate::store::worker::{
    CleanupSSTTask, CleanupTask, ConsistencyCheckTask, VioletaBftlogGcTask, Readpushdown_causet, BraneTask,
    SplitCheckTask,
};
use crate::store::FidelTask;
use crate::store::{
    util, AbstractPeer, CasualMessage, Config, MergeResultKind, PeerMsg, PeerTicks, VioletaBftCommand,
    SignificantMsg, SnapKey, StoreMsg,
};
use crate::{Error, Result};
use tuplespaceInstanton::{self, enc_lightlike_key, enc_spacelike_key};

const REGION_SPLIT_SKIP_MAX_COUNT: usize = 3;

pub struct DestroyPeerJob {
    pub initialized: bool,
    pub brane_id: u64,
    pub peer: meta_timeshare::Peer,
}

/// Represents state of the group.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum GroupState {
    /// The group is working generally, leader keeps
    /// replicating data to followers.
    Ordered,
    /// The group is out of order. Leadership may not be hold.
    Chaos,
    /// The group is about to be out of order. It leave some
    /// safe space to avoid stepping chaos too often.
    PreChaos,
    /// The group is hibernated.
    Idle,
}

pub struct PeerFsm<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    pub peer: Peer<EK, ER>,
    /// A registry for all scheduled ticks. This can avoid scheduling ticks twice accidentally.
    tick_registry: PeerTicks,
    /// Ticks for speed up campaign in chaos state.
    ///
    /// Followers will keep ticking in Idle mode to measure how many ticks have been skipped.
    /// Once it becomes chaos, those skipped ticks will be ticked so that it can campaign
    /// quickly instead of waiting an election timeout.
    ///
    /// This will be reset to 0 once it receives any messages from leader.
    missing_ticks: usize,
    group_state: GroupState,
    stopped: bool,
    has_ready: bool,
    early_apply: bool,
    mailbox: Option<BasicMailbox<PeerFsm<EK, ER>>>,
    pub receiver: Receiver<PeerMsg<EK>>,
    /// when snapshot is generating or lightlikeing, skip split check at most REGION_SPLIT_SKIT_MAX_COUNT times.
    skip_split_count: usize,
    /// Sometimes applied violetabft logs won't be compacted in time, because less compact means less
    /// sync-log in apply threads. Stale logs will be deleted if the skip time reaches this
    /// `skip_gc_violetabft_log_ticks`.
    skip_gc_violetabft_log_ticks: usize,

    // Batch violetabft command which has the same header into an entry
    batch_req_builder: BatchVioletaBftCmdRequestBuilder<EK>,
}

pub struct BatchVioletaBftCmdRequestBuilder<E>
where
    E: CausetEngine,
{
    violetabft_entry_max_size: f64,
    batch_req_size: u32,
    request: Option<VioletaBftCmdRequest>,
    callbacks: Vec<(Callback<E::Snapshot>, usize)>,
}

impl<EK, ER> Drop for PeerFsm<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    fn drop(&mut self) {
        self.peer.stop();
        while let Ok(msg) = self.receiver.try_recv() {
            let callback = match msg {
                PeerMsg::VioletaBftCommand(cmd) => cmd.callback,
                PeerMsg::CasualMessage(CasualMessage::SplitBrane { callback, .. }) => callback,
                _ => continue,
            };

            let mut err = error_timeshare::Error::default();
            err.set_message("brane is not found".to_owned());
            err.mut_brane_not_found().set_brane_id(self.brane_id());
            let mut resp = VioletaBftCmdResponse::default();
            resp.mut_header().set_error(err);
            callback.invoke_with_response(resp);
        }
    }
}

pub type lightlikeerFsmPair<EK, ER> = (LooseBoundedlightlikeer<PeerMsg<EK>>, Box<PeerFsm<EK, ER>>);

impl<EK, ER> PeerFsm<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    // If we create the peer actively, like bootstrap/split/merge brane, we should
    // use this function to create the peer. The brane must contain the peer info
    // for this store.
    pub fn create(
        store_id: u64,
        causet: &Config,
        sched: Interlock_Semaphore<BraneTask<EK::Snapshot>>,
        engines: Engines<EK, ER>,
        brane: &meta_timeshare::Brane,
    ) -> Result<lightlikeerFsmPair<EK, ER>> {
        let meta_peer = match util::find_peer(brane, store_id) {
            None => {
                return Err(box_err!(
                    "find no peer for store {} in brane {:?}",
                    store_id,
                    brane
                ));
            }
            Some(peer) => peer.clone(),
        };

        info!(
            "create peer";
            "brane_id" => brane.get_id(),
            "peer_id" => meta_peer.get_id(),
        );
        let (tx, rx) = mpsc::loose_bounded(causet.notify_capacity);
        Ok((
            tx,
            Box::new(PeerFsm {
                early_apply: causet.early_apply,
                peer: Peer::new(store_id, causet, sched, engines, brane, meta_peer)?,
                tick_registry: PeerTicks::empty(),
                missing_ticks: 0,
                group_state: GroupState::Ordered,
                stopped: false,
                has_ready: false,
                mailbox: None,
                receiver: rx,
                skip_split_count: 0,
                skip_gc_violetabft_log_ticks: 0,
                batch_req_builder: BatchVioletaBftCmdRequestBuilder::new(
                    causet.violetabft_entry_max_size.0 as f64,
                ),
            }),
        ))
    }

    // The peer can be created from another node with violetabft membership changes, and we only
    // know the brane_id and peer_id when creating this replicated peer, the brane info
    // will be retrieved later after applying snapshot.
    pub fn replicate(
        store_id: u64,
        causet: &Config,
        sched: Interlock_Semaphore<BraneTask<EK::Snapshot>>,
        engines: Engines<EK, ER>,
        brane_id: u64,
        peer: meta_timeshare::Peer,
    ) -> Result<lightlikeerFsmPair<EK, ER>> {
        // We will remove tombstone key when apply snapshot
        info!(
            "replicate peer";
            "brane_id" => brane_id,
            "peer_id" => peer.get_id(),
        );

        let mut brane = meta_timeshare::Brane::default();
        brane.set_id(brane_id);

        let (tx, rx) = mpsc::loose_bounded(causet.notify_capacity);
        Ok((
            tx,
            Box::new(PeerFsm {
                early_apply: causet.early_apply,
                peer: Peer::new(store_id, causet, sched, engines, &brane, peer)?,
                tick_registry: PeerTicks::empty(),
                missing_ticks: 0,
                group_state: GroupState::Ordered,
                stopped: false,
                has_ready: false,
                mailbox: None,
                receiver: rx,
                skip_split_count: 0,
                skip_gc_violetabft_log_ticks: 0,
                batch_req_builder: BatchVioletaBftCmdRequestBuilder::new(
                    causet.violetabft_entry_max_size.0 as f64,
                ),
            }),
        ))
    }

    #[inline]
    pub fn brane_id(&self) -> u64 {
        self.peer.brane().get_id()
    }

    #[inline]
    pub fn get_peer(&self) -> &Peer<EK, ER> {
        &self.peer
    }

    #[inline]
    pub fn peer_id(&self) -> u64 {
        self.peer.peer_id()
    }

    #[inline]
    pub fn stop(&mut self) {
        self.stopped = true;
    }

    pub fn set_plightlikeing_merge_state(&mut self, state: MergeState) {
        self.peer.plightlikeing_merge_state = Some(state);
    }

    pub fn schedule_applying_snapshot(&mut self) {
        self.peer.mut_store().schedule_applying_snapshot();
    }
}

impl<E> BatchVioletaBftCmdRequestBuilder<E>
where
    E: CausetEngine,
{
    fn new(violetabft_entry_max_size: f64) -> BatchVioletaBftCmdRequestBuilder<E> {
        BatchVioletaBftCmdRequestBuilder {
            violetabft_entry_max_size,
            request: None,
            batch_req_size: 0,
            callbacks: vec![],
        }
    }

    fn can_batch(&self, req: &VioletaBftCmdRequest, req_size: u32) -> bool {
        // No batch request whose size exceed 20% of violetabft_entry_max_size,
        // so total size of request in batch_violetabft_request would not exceed
        // (40% + 20%) of violetabft_entry_max_size
        if req.get_requests().is_empty() || f64::from(req_size) > self.violetabft_entry_max_size * 0.2 {
            return false;
        }
        for r in req.get_requests() {
            match r.get_cmd_type() {
                CmdType::Delete | CmdType::Put => (),
                _ => {
                    return false;
                }
            }
        }

        if let Some(batch_req) = self.request.as_ref() {
            if batch_req.get_header() != req.get_header() {
                return false;
            }
        }
        true
    }

    fn add(&mut self, cmd: VioletaBftCommand<E::Snapshot>, req_size: u32) {
        let req_num = cmd.request.get_requests().len();
        let VioletaBftCommand {
            mut request,
            callback,
            ..
        } = cmd;
        if let Some(batch_req) = self.request.as_mut() {
            let requests: Vec<_> = request.take_requests().into();
            for q in requests {
                batch_req.mut_requests().push(q);
            }
        } else {
            self.request = Some(request);
        };
        self.callbacks.push((callback, req_num));
        self.batch_req_size += req_size;
    }

    fn should_finish(&self) -> bool {
        if let Some(batch_req) = self.request.as_ref() {
            // Limit the size of batch request so that it will not exceed violetabft_entry_max_size after
            // adding header.
            if f64::from(self.batch_req_size) > self.violetabft_entry_max_size * 0.4 {
                return true;
            }
            if batch_req.get_requests().len() > <E as WriteBatchExt>::WRITE_BATCH_MAX_KEYS {
                return true;
            }
        }
        false
    }

    fn build(&mut self, metric: &mut VioletaBftProposeMetrics) -> Option<VioletaBftCommand<E::Snapshot>> {
        if let Some(req) = self.request.take() {
            self.batch_req_size = 0;
            if self.callbacks.len() == 1 {
                let (cb, _) = self.callbacks.pop().unwrap();
                return Some(VioletaBftCommand::new(req, cb));
            }
            metric.batch += self.callbacks.len() - 1;
            let cbs = std::mem::replace(&mut self.callbacks, vec![]);
            let cb = Callback::Write(Box::new(move |resp| {
                let mut last_index = 0;
                let has_error = resp.response.get_header().has_error();
                for (cb, req_num) in cbs {
                    let next_index = last_index + req_num;
                    let mut cmd_resp = VioletaBftCmdResponse::default();
                    cmd_resp.set_header(resp.response.get_header().clone());
                    if !has_error {
                        cmd_resp.set_responses(
                            resp.response.get_responses()[last_index..next_index].into(),
                        );
                    }
                    cb.invoke_with_response(cmd_resp);
                    last_index = next_index;
                }
            }));
            return Some(VioletaBftCommand::new(req, cb));
        }
        None
    }
}

impl<EK, ER> Fsm for PeerFsm<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    type Message = PeerMsg<EK>;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.stopped
    }

    /// Set a mailbox to Fsm, which should be used to lightlike message to itself.
    #[inline]
    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        self.mailbox = Some(mailbox.into_owned());
    }

    /// Take the mailbox from Fsm. Implementation should ensure there will be
    /// no reference to mailbox after calling this method.
    #[inline]
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        self.mailbox.take()
    }
}

pub struct PeerFsmpushdown_causet<'a, EK, ER, T: 'static, C: 'static>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    fsm: &'a mut PeerFsm<EK, ER>,
    ctx: &'a mut PollContext<EK, ER, T, C>,
}

impl<'a, EK, ER, T: Transport, C: FidelClient> PeerFsmpushdown_causet<'a, EK, ER, T, C>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    pub fn new(
        fsm: &'a mut PeerFsm<EK, ER>,
        ctx: &'a mut PollContext<EK, ER, T, C>,
    ) -> PeerFsmpushdown_causet<'a, EK, ER, T, C> {
        PeerFsmpushdown_causet { fsm, ctx }
    }

    pub fn handle_msgs(&mut self, msgs: &mut Vec<PeerMsg<EK>>) {
        for m in msgs.drain(..) {
            match m {
                PeerMsg::VioletaBftMessage(msg) => {
                    if let Err(e) = self.on_violetabft_message(msg) {
                        error!(%e;
                            "handle violetabft message err";
                            "brane_id" => self.fsm.brane_id(),
                            "peer_id" => self.fsm.peer_id(),
                        );
                    }
                }
                PeerMsg::VioletaBftCommand(cmd) => {
                    self.ctx
                        .violetabft_metrics
                        .propose
                        .request_wait_time
                        .observe(duration_to_sec(cmd.lightlike_time.elapsed()) as f64);
                    let req_size = cmd.request.compute_size();
                    if self.fsm.batch_req_builder.can_batch(&cmd.request, req_size) {
                        self.fsm.batch_req_builder.add(cmd, req_size);
                        if self.fsm.batch_req_builder.should_finish() {
                            self.propose_batch_violetabft_command();
                        }
                    } else {
                        self.propose_batch_violetabft_command();
                        self.propose_violetabft_command(cmd.request, cmd.callback)
                    }
                }
                PeerMsg::Tick(tick) => self.on_tick(tick),
                PeerMsg::ApplyRes { res } => {
                    self.on_apply_res(res);
                }
                PeerMsg::SignificantMsg(msg) => self.on_significant_msg(msg),
                PeerMsg::CasualMessage(msg) => self.on_casual_msg(msg),
                PeerMsg::Start => self.spacelike(),
                PeerMsg::HeartbeatFidel => {
                    if self.fsm.peer.is_leader() {
                        self.register_fidel_heartbeat_tick()
                    }
                }
                PeerMsg::Noop => {}
                PeerMsg::fidelioReplicationMode => self.on_fidelio_replication_mode(),
            }
        }
        // Propose batch request which may be still waiting for more violetabft-command
        self.propose_batch_violetabft_command();
    }

    fn propose_batch_violetabft_command(&mut self) {
        if let Some(cmd) = self
            .fsm
            .batch_req_builder
            .build(&mut self.ctx.violetabft_metrics.propose)
        {
            self.propose_violetabft_command(cmd.request, cmd.callback)
        }
    }

    fn on_fidelio_replication_mode(&mut self) {
        self.fsm
            .peer
            .switch_replication_mode(&self.ctx.global_replication_state);
        if self.fsm.peer.is_leader() {
            self.reset_violetabft_tick(GroupState::Ordered);
            self.register_fidel_heartbeat_tick();
        }
    }

    fn on_casual_msg(&mut self, msg: CasualMessage<EK>) {
        match msg {
            CasualMessage::SplitBrane {
                brane_epoch,
                split_tuplespaceInstanton,
                callback,
            } => {
                info!(
                    "on split";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "split_tuplespaceInstanton" => %TuplespaceInstantonInfoFormatter(split_tuplespaceInstanton.iter()),
                );
                self.on_prepare_split_brane(brane_epoch, split_tuplespaceInstanton, callback);
            }
            CasualMessage::ComputeHashResult {
                index,
                context,
                hash,
            } => {
                self.on_hash_computed(index, context, hash);
            }
            CasualMessage::BraneApproximateSize { size } => {
                self.on_approximate_brane_size(size);
            }
            CasualMessage::BraneApproximateTuplespaceInstanton { tuplespaceInstanton } => {
                self.on_approximate_brane_tuplespaceInstanton(tuplespaceInstanton);
            }
            CasualMessage::CompactionDeclinedBytes { bytes } => {
                self.on_compaction_declined_bytes(bytes);
            }
            CasualMessage::HalfSplitBrane {
                brane_epoch,
                policy,
            } => {
                self.on_schedule_half_split_brane(&brane_epoch, policy);
            }
            CasualMessage::GcSnap { snaps } => {
                self.on_gc_snap(snaps);
            }
            CasualMessage::ClearBraneSize => {
                self.on_clear_brane_size();
            }
            CasualMessage::BraneOverlapped => {
                debug!("spacelike ticking for overlapped"; "brane_id" => self.brane_id(), "peer_id" => self.fsm.peer_id());
                // Maybe do some safe check first?
                self.fsm.group_state = GroupState::Chaos;
                self.register_violetabft_base_tick();

                if is_learner(&self.fsm.peer.peer) {
                    // FIXME: should use `bcast_check_stale_peer_message` instead.
                    // lightlikeing a new enum type msg to a old edb may cause panic during rolling fidelio
                    // we should change the protobuf behavior and check if properly handled in all place
                    self.fsm.peer.bcast_wake_up_message(&mut self.ctx);
                }
            }
            CasualMessage::SnapshotGenerated => {
                // Resume snapshot handling again to avoid waiting another heartbeat.
                self.fsm.peer.ping();
                self.fsm.has_ready = true;
            }
            CasualMessage::ForceCompactVioletaBftLogs => {
                self.on_violetabft_gc_log_tick(true);
            }
            CasualMessage::AccessPeer(cb) => cb(&mut self.fsm.peer as &mut dyn AbstractPeer),
        }
    }

    fn on_tick(&mut self, tick: PeerTicks) {
        if self.fsm.stopped {
            return;
        }
        trace!(
            "tick";
            "tick" => ?tick,
            "peer_id" => self.fsm.peer_id(),
            "brane_id" => self.brane_id(),
        );
        self.fsm.tick_registry.remove(tick);
        match tick {
            PeerTicks::VIOLETABFT => self.on_violetabft_base_tick(),
            PeerTicks::VIOLETABFT_LOG_GC => self.on_violetabft_gc_log_tick(false),
            PeerTicks::FIDel_HEARTBEAT => self.on_fidel_heartbeat_tick(),
            PeerTicks::SPLIT_REGION_CHECK => self.on_split_brane_check_tick(),
            PeerTicks::CHECK_MERGE => self.on_check_merge(),
            PeerTicks::CHECK_PEER_STALE_STATE => self.on_check_peer_stale_state_tick(),
            _ => unreachable!(),
        }
    }

    fn spacelike(&mut self) {
        self.register_violetabft_base_tick();
        self.register_violetabft_gc_log_tick();
        self.register_fidel_heartbeat_tick();
        self.register_split_brane_check_tick();
        self.register_check_peer_stale_state_tick();
        self.on_check_merge();
        // Apply committed entries more quickly.
        if self.fsm.peer.violetabft_group.store().committed_index()
            > self.fsm.peer.violetabft_group.store().applied_index()
        {
            self.fsm.has_ready = true;
        }
    }

    fn on_gc_snap(&mut self, snaps: Vec<(SnapKey, bool)>) {
        let s = self.fsm.peer.get_store();
        let compacted_idx = s.truncated_index();
        let compacted_term = s.truncated_term();
        let is_applying_snap = s.is_applying_snapshot();
        for (key, is_lightlikeing) in snaps {
            if is_lightlikeing {
                let s = match self.ctx.snap_mgr.get_snapshot_for_lightlikeing(&key) {
                    Ok(s) => s,
                    Err(e) => {
                        error!(%e;
                            "failed to load snapshot";
                            "brane_id" => self.fsm.brane_id(),
                            "peer_id" => self.fsm.peer_id(),
                            "snapshot" => ?key,
                        );
                        continue;
                    }
                };
                if key.term < compacted_term || key.idx < compacted_idx {
                    info!(
                        "deleting compacted snap file";
                        "brane_id" => self.fsm.brane_id(),
                        "peer_id" => self.fsm.peer_id(),
                        "snap_file" => %key,
                    );
                    self.ctx.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                } else if let Ok(meta) = s.meta() {
                    let modified = match meta.modified() {
                        Ok(m) => m,
                        Err(e) => {
                            error!(
                                "failed to load snapshot";
                                "brane_id" => self.fsm.brane_id(),
                                "peer_id" => self.fsm.peer_id(),
                                "snapshot" => ?key,
                                "err" => %e,
                            );
                            continue;
                        }
                    };
                    if let Ok(elapsed) = modified.elapsed() {
                        if elapsed > self.ctx.causet.snap_gc_timeout.0 {
                            info!(
                                "deleting expired snap file";
                                "brane_id" => self.fsm.brane_id(),
                                "peer_id" => self.fsm.peer_id(),
                                "snap_file" => %key,
                            );
                            self.ctx.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                        }
                    }
                }
            } else if key.term <= compacted_term
                && (key.idx < compacted_idx || key.idx == compacted_idx && !is_applying_snap)
            {
                info!(
                    "deleting applied snap file";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "snap_file" => %key,
                );
                let a = match self.ctx.snap_mgr.get_snapshot_for_applying(&key) {
                    Ok(a) => a,
                    Err(e) => {
                        error!(%e;
                            "failed to load snapshot";
                            "brane_id" => self.fsm.brane_id(),
                            "peer_id" => self.fsm.peer_id(),
                            "snap_file" => %key,
                        );
                        continue;
                    }
                };
                self.ctx.snap_mgr.delete_snapshot(&key, a.as_ref(), false);
            }
        }
    }

    fn on_clear_brane_size(&mut self) {
        self.fsm.peer.approximate_size = None;
        self.fsm.peer.approximate_tuplespaceInstanton = None;
        self.register_split_brane_check_tick();
    }

    fn on_capture_change(
        &mut self,
        cmd: ChangeCmd,
        brane_epoch: BraneEpoch,
        cb: Callback<EK::Snapshot>,
    ) {
        fail_point!("violetabft_on_capture_change");
        let brane_id = self.brane_id();
        let msg =
            new_read_index_request(brane_id, brane_epoch.clone(), self.fsm.peer.peer.clone());
        let apply_router = self.ctx.apply_router.clone();
        self.propose_violetabft_command(
            msg,
            Callback::Read(Box::new(move |resp| {
                // Return the error
                if resp.response.get_header().has_error() {
                    cb.invoke_read(resp);
                    return;
                }
                apply_router.schedule_task(
                    brane_id,
                    ApplyTask::Change {
                        cmd,
                        brane_epoch,
                        cb,
                    },
                )
            })),
        );
    }

    fn on_significant_msg(&mut self, msg: SignificantMsg<EK::Snapshot>) {
        match msg {
            SignificantMsg::SnapshotStatus {
                to_peer_id, status, ..
            } => {
                // Report snapshot status to the corresponding peer.
                self.report_snapshot_status(to_peer_id, status);
            }
            SignificantMsg::Unreachable { to_peer_id, .. } => {
                if self.fsm.peer.is_leader() {
                    self.fsm.peer.violetabft_group.report_unreachable(to_peer_id);
                } else if to_peer_id == self.fsm.peer.leader_id() {
                    self.fsm.group_state = GroupState::Chaos;
                    self.register_violetabft_base_tick();
                }
            }
            SignificantMsg::StoreUnreachable { store_id } => {
                if let Some(peer_id) = util::find_peer(self.brane(), store_id).map(|p| p.get_id())
                {
                    if self.fsm.peer.is_leader() {
                        self.fsm.peer.violetabft_group.report_unreachable(peer_id);
                    } else if peer_id == self.fsm.peer.leader_id() {
                        self.fsm.group_state = GroupState::Chaos;
                        self.register_violetabft_base_tick();
                    }
                }
            }
            SignificantMsg::MergeResult {
                target_brane_id,
                target,
                result,
            } => {
                self.on_merge_result(target_brane_id, target, result);
            }
            SignificantMsg::CatchUpLogs(catch_up_logs) => {
                self.on_catch_up_logs_for_merge(catch_up_logs);
            }
            SignificantMsg::StoreResolved { store_id, group_id } => {
                let state = self.ctx.global_replication_state.dagger().unwrap();
                if state.status().get_mode() != ReplicationMode::DrAutoSync {
                    return;
                }
                if state.status().get_dr_auto_sync().get_state() == DrAutoSyncState::Async {
                    return;
                }
                drop(state);
                self.fsm
                    .peer
                    .violetabft_group
                    .violetabft
                    .assign_commit_groups(&[(store_id, group_id)]);
            }
            SignificantMsg::CaptureChange {
                cmd,
                brane_epoch,
                callback,
            } => self.on_capture_change(cmd, brane_epoch, callback),
            SignificantMsg::LeaderCallback(cb) => {
                self.on_leader_callback(cb);
            }
        }
    }

    fn report_snapshot_status(&mut self, to_peer_id: u64, status: SnapshotStatus) {
        let to_peer = match self.fsm.peer.get_peer_from_cache(to_peer_id) {
            Some(peer) => peer,
            None => {
                // If to_peer is gone, ignore this snapshot status
                warn!(
                    "peer not found, ignore snapshot status";
                    "brane_id" => self.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "to_peer_id" => to_peer_id,
                    "status" => ?status,
                );
                return;
            }
        };
        info!(
            "report snapshot status";
            "brane_id" => self.fsm.brane_id(),
            "peer_id" => self.fsm.peer_id(),
            "to" => ?to_peer,
            "status" => ?status,
        );
        self.fsm.peer.violetabft_group.report_snapshot(to_peer_id, status)
    }

    fn on_leader_callback(&mut self, cb: Callback<EK::Snapshot>) {
        let msg = new_read_index_request(
            self.brane_id(),
            self.brane().get_brane_epoch().clone(),
            self.fsm.peer.peer.clone(),
        );
        self.propose_violetabft_command(msg, cb);
    }

    fn on_role_changed(&mut self, ready: &Ready) {
        // fidelio leader lease when the VioletaBft state changes.
        if let Some(ss) = ready.ss() {
            if StateRole::Leader == ss.violetabft_state {
                self.fsm.missing_ticks = 0;
                self.register_split_brane_check_tick();
                self.fsm.peer.heartbeat_fidel(&self.ctx);
                self.register_fidel_heartbeat_tick();
            }
        }
    }

    pub fn collect_ready(&mut self) {
        let has_ready = self.fsm.has_ready;
        self.fsm.has_ready = false;
        if !has_ready || self.fsm.stopped {
            return;
        }
        self.ctx.plightlikeing_count += 1;
        self.ctx.has_ready = true;
        let res = self.fsm.peer.handle_violetabft_ready_applightlike(self.ctx);
        if let Some(r) = res {
            self.on_role_changed(&r.0);
            if !r.0.entries().is_empty() {
                self.register_violetabft_gc_log_tick();
                self.register_split_brane_check_tick();
            }
            self.ctx.ready_res.push(r);
        }
    }

    #[inline]
    pub fn handle_violetabft_ready_apply(&mut self, ready: &mut Ready, invoke_ctx: &InvokeContext) {
        self.fsm.early_apply = ready
            .committed_entries
            .as_ref()
            .and_then(|e| e.last())
            .map_or(false, |e| {
                self.fsm.peer.can_early_apply(e.get_term(), e.get_index())
            });
        if !self.fsm.early_apply {
            return;
        }
        self.fsm
            .peer
            .handle_violetabft_ready_apply(self.ctx, ready, invoke_ctx);
    }

    pub fn post_violetabft_ready_applightlike(&mut self, mut ready: Ready, invoke_ctx: InvokeContext) {
        let is_merging = self.fsm.peer.plightlikeing_merge_state.is_some();
        if !self.fsm.early_apply {
            self.fsm
                .peer
                .handle_violetabft_ready_apply(self.ctx, &mut ready, &invoke_ctx);
        }
        let res = self
            .fsm
            .peer
            .post_violetabft_ready_applightlike(self.ctx, &mut ready, invoke_ctx);
        self.fsm.peer.handle_violetabft_ready_advance(ready);
        let mut has_snapshot = false;
        if let Some(apply_res) = res {
            self.on_ready_apply_snapshot(apply_res);
            has_snapshot = true;
            self.register_violetabft_base_tick();
        }
        if self.fsm.peer.leader_unreachable {
            self.fsm.group_state = GroupState::Chaos;
            self.register_violetabft_base_tick();
            self.fsm.peer.leader_unreachable = false;
        }
        if is_merging && has_snapshot {
            // After applying a snapshot, merge is rollbacked implicitly.
            self.on_ready_rollback_merge(0, None);
        }
    }

    #[inline]
    fn brane_id(&self) -> u64 {
        self.fsm.peer.brane().get_id()
    }

    #[inline]
    fn brane(&self) -> &Brane {
        self.fsm.peer.brane()
    }

    #[inline]
    fn store_id(&self) -> u64 {
        self.fsm.peer.peer.get_store_id()
    }

    #[inline]
    fn schedule_tick(&mut self, tick: PeerTicks) {
        if self.fsm.tick_registry.contains(tick) {
            return;
        }
        let idx = tick.bits() as usize;
        if is_zero_duration(&self.ctx.tick_batch[idx].wait_duration) {
            return;
        }
        trace!(
            "schedule tick";
            "tick" => ?tick,
            "timeout" => ?self.ctx.tick_batch[idx].wait_duration,
            "brane_id" => self.brane_id(),
            "peer_id" => self.fsm.peer_id(),
        );
        self.fsm.tick_registry.insert(tick);

        let brane_id = self.brane_id();
        let mb = match self.ctx.router.mailbox(brane_id) {
            Some(mb) => mb,
            None => {
                self.fsm.tick_registry.remove(tick);
                error!(
                    "failed to get mailbox";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "tick" => ?tick,
                );
                return;
            }
        };
        let peer_id = self.fsm.peer.peer_id();
        let cb = Box::new(move || {
            // This can happen only when the peer is about to be destroyed
            // or the node is shutting down. So it's OK to not to clean up
            // registry.
            if let Err(e) = mb.force_lightlike(PeerMsg::Tick(tick)) {
                debug!(
                    "failed to schedule peer tick";
                    "brane_id" => brane_id,
                    "peer_id" => peer_id,
                    "tick" => ?tick,
                    "err" => %e,
                );
            }
        });
        self.ctx.tick_batch[idx].ticks.push(cb);
    }

    fn register_violetabft_base_tick(&mut self) {
        // If we register violetabft base tick failed, the whole violetabft can't run correctly,
        // TODO: shutdown the store?
        self.schedule_tick(PeerTicks::VIOLETABFT)
    }

    fn on_violetabft_base_tick(&mut self) {
        if self.fsm.peer.plightlikeing_remove {
            self.fsm.peer.mut_store().flush_cache_metrics();
            return;
        }
        // When having plightlikeing snapshot, if election timeout is met, it can't pass
        // the plightlikeing conf change check because first index has been fideliod to
        // a value that is larger than last index.
        if self.fsm.peer.is_applying_snapshot() || self.fsm.peer.has_plightlikeing_snapshot() {
            // need to check if snapshot is applied.
            self.fsm.has_ready = true;
            self.fsm.missing_ticks = 0;
            self.register_violetabft_base_tick();
            return;
        }

        self.fsm.peer.retry_plightlikeing_reads(&self.ctx.causet);

        let mut res = None;
        if self.ctx.causet.hibernate_branes {
            if self.fsm.group_state == GroupState::Idle {
                // missing_ticks should be less than election timeout ticks otherwise
                // follower may tick more than an election timeout in chaos state.
                // Before stopping tick, `missing_tick` should be `violetabft_election_timeout_ticks` - 2
                // - `violetabft_heartbeat_ticks` (default 10 - 2 - 2 = 6)
                // and the follwer's `election_elapsed` in violetabft-rs is 1.
                // After the group state becomes Chaos, the next tick will call `violetabft_group.tick`
                // `missing_tick` + 1 times(default 7).
                // Then the follower's `election_elapsed` will be 1 + `missing_tick` + 1
                // (default 1 + 6 + 1 = 8) which is less than the min election timeout.
                // The reason is that we don't want let all followers become (pre)candidate if one
                // follower may receive a request, then becomes (pre)candidate and lightlikes (pre)vote msg
                // to others. As long as the leader can wake up and broadcast hearbeats in one `violetabft_heartbeat_ticks`
                // time(default 2s), no more followers will wake up and lightlikes vote msg again.
                if self.fsm.missing_ticks + 2 + self.ctx.causet.violetabft_heartbeat_ticks
                    < self.ctx.causet.violetabft_election_timeout_ticks
                {
                    self.register_violetabft_base_tick();
                    self.fsm.missing_ticks += 1;
                }
                return;
            }
            res = Some(self.fsm.peer.check_before_tick(&self.ctx.causet));
            if self.fsm.missing_ticks > 0 {
                for _ in 0..self.fsm.missing_ticks {
                    if self.fsm.peer.violetabft_group.tick() {
                        self.fsm.has_ready = true;
                    }
                }
                self.fsm.missing_ticks = 0;
            }
        }
        if self.fsm.peer.violetabft_group.tick() {
            self.fsm.has_ready = true;
        }

        self.fsm.peer.mut_store().flush_cache_metrics();

        // Keep ticking if there are still plightlikeing read requests or this node is within hibernate timeout.
        if res.is_none() /* hibernate_brane is false */ ||
            !self.fsm.peer.check_after_tick(self.fsm.group_state, res.unwrap()) ||
            (self.fsm.peer.is_leader() && !self.ctx.is_hibernate_timeout())
        {
            self.register_violetabft_base_tick();
            return;
        }

        debug!("stop ticking"; "brane_id" => self.brane_id(), "peer_id" => self.fsm.peer_id(), "res" => ?res);
        self.fsm.group_state = GroupState::Idle;
        // Followers will stop ticking at L789. Keep ticking for followers
        // to allow it to campaign quickly when abnormal situation is detected.
        if !self.fsm.peer.is_leader() {
            self.register_violetabft_base_tick();
        } else {
            self.register_fidel_heartbeat_tick();
        }
    }

    fn on_apply_res(&mut self, res: ApplyTaskRes<EK::Snapshot>) {
        fail_point!("on_apply_res", |_| {});
        match res {
            ApplyTaskRes::Apply(mut res) => {
                debug!(
                    "async apply finish";
                    "brane_id" => self.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "res" => ?res,
                );
                self.on_ready_result(&mut res.exec_res, &res.metrics);
                if self.fsm.stopped {
                    return;
                }
                self.fsm.has_ready |= self.fsm.peer.post_apply(
                    self.ctx,
                    res.apply_state,
                    res.applied_index_term,
                    &res.metrics,
                );
                // After applying, several metrics are fideliod, report it to fidel to
                // get fair schedule.
                self.register_fidel_heartbeat_tick();
            }
            ApplyTaskRes::Destroy {
                brane_id,
                peer_id,
                merge_from_snapshot,
            } => {
                assert_eq!(peer_id, self.fsm.peer.peer_id());
                if !merge_from_snapshot {
                    self.destroy_peer(false);
                } else {
                    // Wait for its target peer to apply snapshot and then lightlike `MergeResult` back
                    // to destroy itself
                    let mut meta = self.ctx.store_meta.dagger().unwrap();
                    // The `need_atomic` flag must be true
                    assert!(*meta.destroyed_brane_for_snap.get(&brane_id).unwrap());

                    let target_brane_id = *meta.targets_map.get(&brane_id).unwrap();
                    let is_ready = meta
                        .atomic_snap_branes
                        .get_mut(&target_brane_id)
                        .unwrap()
                        .get_mut(&brane_id)
                        .unwrap();
                    *is_ready = true;
                }
            }
        }
    }

    fn on_violetabft_message(&mut self, mut msg: VioletaBftMessage) -> Result<()> {
        debug!(
            "handle violetabft message";
            "brane_id" => self.brane_id(),
            "peer_id" => self.fsm.peer_id(),
            "message_type" => ?msg.get_message().get_msg_type(),
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
        );

        if !self.validate_violetabft_msg(&msg) {
            return Ok(());
        }
        if self.fsm.peer.plightlikeing_remove || self.fsm.stopped {
            return Ok(());
        }

        if msg.get_is_tombstone() {
            // we receive a message tells us to remove ourself.
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }

        if msg.has_merge_target() {
            fail_point!("on_has_merge_target", |_| Ok(()));
            if self.need_gc_merge(&msg)? {
                self.on_stale_merge(msg.get_merge_target().get_id());
            }
            return Ok(());
        }

        if self.check_msg(&msg) {
            return Ok(());
        }

        if msg.has_extra_msg() {
            self.on_extra_message(msg);
            return Ok(());
        }

        let is_snapshot = msg.get_message().has_snapshot();
        let branes_to_destroy = match self.check_snapshot(&msg)? {
            Either::Left(key) => {
                // If the snapshot file is not used again, then it's OK to
                // delete them here. If the snapshot file will be reused when
                // receiving, then it will fail to pass the check again, so
                // missing snapshot files should not be noticed.
                let s = self.ctx.snap_mgr.get_snapshot_for_applying(&key)?;
                self.ctx.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                return Ok(());
            }
            Either::Right(v) => v,
        };

        if !self.check_request_snapshot(&msg) {
            return Ok(());
        }

        if util::is_vote_msg(&msg.get_message())
            || msg.get_message().get_msg_type() == MessageType::MsgTimeoutNow
        {
            if self.fsm.group_state != GroupState::Chaos {
                self.fsm.group_state = GroupState::Chaos;
                self.register_violetabft_base_tick();
            }
        } else if msg.get_from_peer().get_id() == self.fsm.peer.leader_id() {
            self.reset_violetabft_tick(GroupState::Ordered);
        }

        let from_peer_id = msg.get_from_peer().get_id();
        self.fsm.peer.insert_peer_cache(msg.take_from_peer());

        let result = self.fsm.peer.step(self.ctx, msg.take_message());

        if is_snapshot {
            if !self.fsm.peer.has_plightlikeing_snapshot() {
                // This snapshot is rejected by violetabft-rs.
                let mut meta = self.ctx.store_meta.dagger().unwrap();
                meta.plightlikeing_snapshot_branes
                    .retain(|r| self.fsm.brane_id() != r.get_id());
            } else {
                // This snapshot may be accepted by violetabft-rs.
                // If it's rejected by violetabft-rs, the snapshot brane in `plightlikeing_snapshot_branes`
                // will be removed together with the latest snapshot brane after applying that snapshot.
                // But if `branes_to_destroy` is not empty, the plightlikeing snapshot must be this msg's snapshot
                // because this kind of snapshot is exclusive.
                self.destroy_branes_for_snapshot(branes_to_destroy);
            }
        }

        if result.is_err() {
            return result;
        }

        if self.fsm.peer.any_new_peer_catch_up(from_peer_id) {
            self.fsm.peer.heartbeat_fidel(self.ctx);
            self.fsm.peer.should_wake_up = true;
        }

        if self.fsm.peer.should_wake_up {
            self.reset_violetabft_tick(GroupState::Ordered);
        }

        self.fsm.has_ready = true;
        Ok(())
    }

    fn on_extra_message(&mut self, mut msg: VioletaBftMessage) {
        match msg.get_extra_msg().get_type() {
            ExtraMessageType::MsgBraneWakeUp | ExtraMessageType::MsgCheckStalePeer => {
                if self.fsm.group_state == GroupState::Idle {
                    self.reset_violetabft_tick(GroupState::Ordered);
                }
            }
            ExtraMessageType::MsgWantRollbackMerge => {
                self.fsm.peer.maybe_add_want_rollback_merge_peer(
                    msg.get_from_peer().get_id(),
                    msg.get_extra_msg(),
                );
            }
            ExtraMessageType::MsgCheckStalePeerResponse => {
                self.fsm.peer.on_check_stale_peer_response(
                    msg.get_brane_epoch().get_conf_ver(),
                    msg.mut_extra_msg().take_check_peers().into(),
                );
            }
        }
    }

    fn reset_violetabft_tick(&mut self, state: GroupState) {
        self.fsm.group_state = state;
        self.fsm.missing_ticks = 0;
        self.fsm.peer.should_wake_up = false;
        self.register_violetabft_base_tick();
    }

    // return false means the message is invalid, and can be ignored.
    fn validate_violetabft_msg(&mut self, msg: &VioletaBftMessage) -> bool {
        let brane_id = msg.get_brane_id();
        let to = msg.get_to_peer();

        if to.get_store_id() != self.store_id() {
            warn!(
                "store not match, ignore it";
                "brane_id" => brane_id,
                "to_store_id" => to.get_store_id(),
                "my_store_id" => self.store_id(),
            );
            self.ctx.violetabft_metrics.message_dropped.mismatch_store_id += 1;
            return false;
        }

        if !msg.has_brane_epoch() {
            error!(
                "missing epoch in violetabft message, ignore it";
                "brane_id" => brane_id,
            );
            self.ctx.violetabft_metrics.message_dropped.mismatch_brane_epoch += 1;
            return false;
        }

        true
    }

    /// Checks if the message is sent to the correct peer.
    ///
    /// Returns true means that the message can be dropped silently.
    fn check_msg(&mut self, msg: &VioletaBftMessage) -> bool {
        let from_epoch = msg.get_brane_epoch();
        let from_store_id = msg.get_from_peer().get_store_id();

        // Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
        // a. 1 removes 2, 2 may still lightlike MsgApplightlikeResponse to 1.
        //  We should ignore this stale message and let 2 remove itself after
        //  applying the ConfChange log.
        // b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
        //  lightlike stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
        // c. 2 is isolated but can communicate with 3. 1 removes 3.
        //  2 will lightlike stale MsgRequestVote to 3, 3 should ignore this message.
        // d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
        //  2 will lightlike stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
        // e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
        //  After 2 rejoins the cluster, 2 may lightlike stale MsgRequestVote to 1 and 3,
        //  1 and 3 will ignore this message. Later 4 will lightlike messages to 2 and 2 will
        //  rejoin the violetabft group again.
        // f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
        //  unlike case e, 2 will be stale forever.
        // TODO: for case f, if 2 is stale for a long time, 2 will communicate with fidel and fidel will
        // tell 2 is stale, so 2 can remove itself.
        if util::is_epoch_stale(from_epoch, self.fsm.peer.brane().get_brane_epoch())
            && util::find_peer(self.fsm.peer.brane(), from_store_id).is_none()
        {
            let mut need_gc_msg = util::is_vote_msg(msg.get_message());
            if msg.has_extra_msg() {
                // A learner can't vote so it lightlikes the check-stale-peer msg to others to find out whether
                // it is removed due to conf change or merge.
                need_gc_msg |=
                    msg.get_extra_msg().get_type() == ExtraMessageType::MsgCheckStalePeer;
                // For backward compatibility
                need_gc_msg |= msg.get_extra_msg().get_type() == ExtraMessageType::MsgBraneWakeUp;
            }
            // The message is stale and not in current brane.
            self.ctx.handle_stale_msg(
                msg,
                self.fsm.peer.brane().get_brane_epoch().clone(),
                need_gc_msg,
                None,
            );
            return true;
        }

        let target = msg.get_to_peer();
        match target.get_id().cmp(&self.fsm.peer.peer_id()) {
            cmp::Ordering::Less => {
                info!(
                    "target peer id is smaller, msg maybe stale";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_peer" => ?target,
                );
                self.ctx.violetabft_metrics.message_dropped.stale_msg += 1;
                true
            }
            cmp::Ordering::Greater => {
                match self.fsm.peer.maybe_destroy(&self.ctx) {
                    Some(job) => {
                        info!(
                            "target peer id is larger, destroying self";
                            "brane_id" => self.fsm.brane_id(),
                            "peer_id" => self.fsm.peer_id(),
                            "target_peer" => ?target,
                        );
                        if self.handle_destroy_peer(job) {
                            if let Err(e) = self
                                .ctx
                                .router
                                .lightlike_control(StoreMsg::VioletaBftMessage(msg.clone()))
                            {
                                info!(
                                    "failed to lightlike back store message, are we shutting down?";
                                    "brane_id" => self.fsm.brane_id(),
                                    "peer_id" => self.fsm.peer_id(),
                                    "err" => %e,
                                );
                            }
                        }
                    }
                    None => self.ctx.violetabft_metrics.message_dropped.applying_snap += 1,
                }
                true
            }
            cmp::Ordering::Equal => false,
        }
    }

    /// Check if it's necessary to gc the source merge peer.
    ///
    /// If the target merge peer won't be created on this store,
    /// then it's appropriate to destroy it immediately.
    fn need_gc_merge(&mut self, msg: &VioletaBftMessage) -> Result<bool> {
        let merge_target = msg.get_merge_target();
        let target_brane_id = merge_target.get_id();
        debug!(
            "receive merge target";
            "brane_id" => self.fsm.brane_id(),
            "peer_id" => self.fsm.peer_id(),
            "merge_target" => ?merge_target,
        );

        // When receiving message that has a merge target, it indicates that the source peer on this
        // store is stale, the peers on other stores are already merged. The epoch in merge target
        // is the state of target peer at the time when source peer is merged. So here we record the
        // merge target epoch version to let the target peer on this store to decide whether to
        // destroy the source peer.
        let mut meta = self.ctx.store_meta.dagger().unwrap();
        meta.targets_map.insert(self.brane_id(), target_brane_id);
        let v = meta
            .plightlikeing_merge_targets
            .entry(target_brane_id)
            .or_default();
        let mut no_cone_merge_target = merge_target.clone();
        no_cone_merge_target.clear_spacelike_key();
        no_cone_merge_target.clear_lightlike_key();
        if let Some(pre_merge_target) = v.insert(self.brane_id(), no_cone_merge_target) {
            // Merge target epoch records the version of target brane when source brane is merged.
            // So it must be same no matter when receiving merge target.
            if pre_merge_target.get_brane_epoch().get_version()
                != merge_target.get_brane_epoch().get_version()
            {
                panic!(
                    "conflict merge target epoch version {:?} {:?}",
                    pre_merge_target.get_brane_epoch().get_version(),
                    merge_target.get_brane_epoch()
                );
            }
        }

        if let Some(r) = meta.branes.get(&target_brane_id) {
            // In the case that the source peer's cone isn't overlapped with target's anymore:
            //     | brane 2 | brane 3 | brane 1 |
            //                   || merge 3 into 2
            //                   \/
            //     |       brane 2      | brane 1 |
            //                   || merge 1 into 2
            //                   \/
            //     |            brane 2            |
            //                   || split 2 into 4
            //                   \/
            //     |        brane 4       |brane 2|
            // so the new target peer can't find the source peer.
            // e.g. new brane 2 is overlapped with brane 1
            //
            // If that, source peer still need to decide whether to destroy itself. When the target
            // peer has already moved on, source peer can destroy itself.
            if util::is_epoch_stale(merge_target.get_brane_epoch(), r.get_brane_epoch()) {
                return Ok(true);
            }
            return Ok(false);
        }
        drop(meta);

        // All of the target peers must exist before merging which is guaranteed by FIDel.
        // Now the target peer is not in brane map, so if everything is ok, the merge target
        // brane should be staler than the local target brane
        if self.is_merge_target_brane_stale(merge_target)? {
            Ok(true)
        } else {
            if self.ctx.causet.dev_assert {
                panic!(
                    "something is wrong, maybe FIDel do not ensure all target peers exist before merging"
                );
            }
            error!(
                "something is wrong, maybe FIDel do not ensure all target peers exist before merging"
            );
            Ok(false)
        }
    }

    fn handle_gc_peer_msg(&mut self, msg: &VioletaBftMessage) {
        let from_epoch = msg.get_brane_epoch();
        if !util::is_epoch_stale(self.fsm.peer.brane().get_brane_epoch(), from_epoch) {
            return;
        }

        if self.fsm.peer.peer != *msg.get_to_peer() {
            info!(
                "receive stale gc message, ignore.";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            self.ctx.violetabft_metrics.message_dropped.stale_msg += 1;
            return;
        }
        // TODO: ask fidel to guarantee we are stale now.
        info!(
            "receives gc message, trying to remove";
            "brane_id" => self.fsm.brane_id(),
            "peer_id" => self.fsm.peer_id(),
            "to_peer" => ?msg.get_to_peer(),
        );
        match self.fsm.peer.maybe_destroy(&self.ctx) {
            None => self.ctx.violetabft_metrics.message_dropped.applying_snap += 1,
            Some(job) => {
                self.handle_destroy_peer(job);
            }
        }
    }

    // Returns `Vec<(u64, bool)>` indicated (source_brane_id, merge_to_this_peer) if the `msg`
    // doesn't contain a snapshot or this snapshot doesn't conflict with any other snapshots or branes.
    // Otherwise a `SnapKey` is returned.
    fn check_snapshot(&mut self, msg: &VioletaBftMessage) -> Result<Either<SnapKey, Vec<(u64, bool)>>> {
        if !msg.get_message().has_snapshot() {
            return Ok(Either::Right(vec![]));
        }

        let before_check_snapshot_1_2 = || {
            fail_point!(
                "before_check_snapshot_1_2",
                self.fsm.brane_id() == 1 && self.fsm.peer_id() == 2,
                |_| {}
            );
        };
        before_check_snapshot_1_2();

        let brane_id = msg.get_brane_id();
        let snap = msg.get_message().get_snapshot();
        let key = SnapKey::from_brane_snap(brane_id, snap);
        let mut snap_data = VioletaBftSnapshotData::default();
        snap_data.merge_from_bytes(snap.get_data())?;
        let snap_brane = snap_data.take_brane();
        let peer_id = msg.get_to_peer().get_id();
        let snap_enc_spacelike_key = enc_spacelike_key(&snap_brane);
        let snap_enc_lightlike_key = enc_lightlike_key(&snap_brane);

        if snap_brane
            .get_peers()
            .iter()
            .all(|p| p.get_id() != peer_id)
        {
            info!(
                "snapshot doesn't contain to peer, skip";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "snap" => ?snap_brane,
                "to_peer" => ?msg.get_to_peer(),
            );
            self.ctx.violetabft_metrics.message_dropped.brane_no_peer += 1;
            return Ok(Either::Left(key));
        }

        let mut meta = self.ctx.store_meta.dagger().unwrap();
        if meta.branes[&self.brane_id()] != *self.brane() {
            if !self.fsm.peer.is_initialized() {
                info!(
                    "stale pushdown_causet detected, skip";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                self.ctx.violetabft_metrics.message_dropped.stale_msg += 1;
                return Ok(Either::Left(key));
            } else {
                panic!(
                    "{} meta corrupted: {:?} != {:?}",
                    self.fsm.peer.tag,
                    meta.branes[&self.brane_id()],
                    self.brane()
                );
            }
        }

        if meta.atomic_snap_branes.contains_key(&brane_id) {
            info!(
                "atomic snapshot is applying, skip";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return Ok(Either::Left(key));
        }

        for brane in &meta.plightlikeing_snapshot_branes {
            if enc_spacelike_key(brane) < snap_enc_lightlike_key &&
               enc_lightlike_key(brane) > snap_enc_spacelike_key &&
               // Same brane can overlap, we will apply the latest version of snapshot.
               brane.get_id() != snap_brane.get_id()
            {
                info!(
                    "plightlikeing brane overlapped";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "brane" => ?brane,
                    "snap" => ?snap_brane,
                );
                self.ctx.violetabft_metrics.message_dropped.brane_overlap += 1;
                return Ok(Either::Left(key));
            }
        }

        let mut is_overlapped = false;
        let mut branes_to_destroy = vec![];
        // In some extreme cases, it may cause source peer destroyed improperly so that a later
        // CommitMerge may panic because source is already destroyed, so just drop the message:
        // 1. A new snapshot is received whereas a snapshot is still in applying, and the snapshot
        // under applying is generated before merge and the new snapshot is generated after merge.
        // After the applying snapshot is finished, the log may able to catch up and so a
        // CommitMerge will be applied.
        // 2. There is a CommitMerge plightlikeing in apply thread.
        let ready = !self.fsm.peer.is_applying_snapshot()
            && !self.fsm.peer.has_plightlikeing_snapshot()
            // It must be ensured that all logs have been applied.
            // Suppose apply fsm is applying a `CommitMerge` log and this snapshot is generated after
            // merge, its corresponding source peer can not be destroy by this snapshot.
            && self.fsm.peer.ready_to_handle_plightlikeing_snap();
        for exist_brane in meta
            .brane_cones
            .cone((Excluded(snap_enc_spacelike_key), Unbounded::<Vec<u8>>))
            .map(|(_, &brane_id)| &meta.branes[&brane_id])
            .take_while(|r| enc_spacelike_key(r) < snap_enc_lightlike_key)
            .filter(|r| r.get_id() != brane_id)
        {
            info!(
                "brane overlapped";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "exist" => ?exist_brane,
                "snap" => ?snap_brane,
            );
            let (can_destroy, merge_to_this_peer) = maybe_destroy_source(
                &meta,
                self.fsm.brane_id(),
                self.fsm.peer_id(),
                exist_brane.get_id(),
                snap_brane.get_brane_epoch().to_owned(),
            );
            if ready && can_destroy {
                // The snapshot that we decide to whether destroy peer based on must can be applied.
                // So here not to destroy peer immediately, or the snapshot maybe dropped in later
                // check but the peer is already destroyed.
                branes_to_destroy.push((exist_brane.get_id(), merge_to_this_peer));
                continue;
            }
            is_overlapped = true;
            if !can_destroy
                && snap_brane.get_brane_epoch().get_version()
                    > exist_brane.get_brane_epoch().get_version()
            {
                // If snapshot's epoch version is greater than exist brane's, the exist brane
                // may has been merged/splitted already.
                let _ = self.ctx.router.force_lightlike(
                    exist_brane.get_id(),
                    PeerMsg::CasualMessage(CasualMessage::BraneOverlapped),
                );
            }
        }
        if is_overlapped {
            self.ctx.violetabft_metrics.message_dropped.brane_overlap += 1;
            return Ok(Either::Left(key));
        }

        // Check if snapshot file exists.
        self.ctx.snap_mgr.get_snapshot_for_applying(&key)?;

        // WARNING: The checking code must be above this line.
        // Now all checking passed.

        if self.fsm.peer.local_first_replicate && !self.fsm.peer.is_initialized() {
            // If the peer is not initialized and passes the snapshot cone check, `is_splitting` flag must
            // be false.
            // 1. If `is_splitting` is set to true, then the uninitialized peer is created before split is applied
            //    and the peer id is the same as split one. So there should be no initialized peer before.
            // 2. If the peer is also created by splitting, then the snapshot cone is not overlapped with
            //    parent peer. It means leader has applied merge and split at least one time. However,
            //    the prerequisite of merge includes the initialization of all target peers and source peers,
            //    which is conflict with 1.
            let plightlikeing_create_peers = self.ctx.plightlikeing_create_peers.dagger().unwrap();
            let status = plightlikeing_create_peers.get(&brane_id).cloned();
            if status != Some((self.fsm.peer_id(), false)) {
                drop(plightlikeing_create_peers);
                panic!("{} status {:?} is not expected", self.fsm.peer.tag, status);
            }
        }
        meta.plightlikeing_snapshot_branes.push(snap_brane);

        Ok(Either::Right(branes_to_destroy))
    }

    fn destroy_branes_for_snapshot(&mut self, branes_to_destroy: Vec<(u64, bool)>) {
        if branes_to_destroy.is_empty() {
            return;
        }
        let mut meta = self.ctx.store_meta.dagger().unwrap();
        assert!(!meta.atomic_snap_branes.contains_key(&self.fsm.brane_id()));
        for (source_brane_id, merge_to_this_peer) in branes_to_destroy {
            if !meta.branes.contains_key(&source_brane_id) {
                if merge_to_this_peer {
                    drop(meta);
                    panic!(
                        "{}'s source brane {} has been destroyed",
                        self.fsm.peer.tag, source_brane_id
                    );
                }
                continue;
            }
            info!(
                "source brane destroy due to target brane's snapshot";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "source_brane_id" => source_brane_id,
                "need_atomic" => merge_to_this_peer,
            );
            meta.atomic_snap_branes
                .entry(self.fsm.brane_id())
                .or_default()
                .insert(source_brane_id, false);
            meta.destroyed_brane_for_snap
                .insert(source_brane_id, merge_to_this_peer);

            let result = if merge_to_this_peer {
                MergeResultKind::FromTargetSnapshotStep1
            } else {
                MergeResultKind::Stale
            };
            // Use `unwrap` is ok because the StoreMeta dagger is held and these source peers still
            // exist in branes and brane_cones map.
            // It deplightlikes on the implementation of `destroy_peer`
            self.ctx
                .router
                .force_lightlike(
                    source_brane_id,
                    PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
                        target_brane_id: self.fsm.brane_id(),
                        target: self.fsm.peer.peer.clone(),
                        result,
                    }),
                )
                .unwrap();
        }
    }

    // Check if this peer can handle request_snapshot.
    fn check_request_snapshot(&mut self, msg: &VioletaBftMessage) -> bool {
        let m = msg.get_message();
        let request_index = m.get_request_snapshot();
        if request_index == violetabft::INVALID_INDEX {
            // If it's not a request snapshot, then go on.
            return true;
        }
        self.fsm
            .peer
            .ready_to_handle_request_snapshot(request_index)
    }

    fn handle_destroy_peer(&mut self, job: DestroyPeerJob) -> bool {
        // The initialized flag implicitly means whether apply fsm exists or not.
        if job.initialized {
            // Destroy the apply fsm first, wait for the reply msg from apply fsm
            self.ctx
                .apply_router
                .schedule_task(job.brane_id, ApplyTask::destroy(job.brane_id, false));
            false
        } else {
            // Destroy the peer fsm directly
            self.destroy_peer(false);
            true
        }
    }

    fn destroy_peer(&mut self, merged_by_target: bool) {
        fail_point!("destroy_peer");
        info!(
            "spacelikes destroy";
            "brane_id" => self.fsm.brane_id(),
            "peer_id" => self.fsm.peer_id(),
            "merged_by_target" => merged_by_target,
        );
        let brane_id = self.brane_id();
        // We can't destroy a peer which is applying snapshot.
        assert!(!self.fsm.peer.is_applying_snapshot());

        // Mark itself as plightlikeing_remove
        self.fsm.peer.plightlikeing_remove = true;

        let mut meta = self.ctx.store_meta.dagger().unwrap();

        if meta.atomic_snap_branes.contains_key(&self.brane_id()) {
            drop(meta);
            panic!(
                "{} is applying atomic snapshot during destroying",
                self.fsm.peer.tag
            );
        }

        // It's possible that this brane gets a snapshot then gets a stale peer msg.
        // So the data in `plightlikeing_snapshot_branes` should be removed here.
        meta.plightlikeing_snapshot_branes
            .retain(|r| self.fsm.brane_id() != r.get_id());

        // Destroy read pushdown_causets.
        if let Some(reader) = meta.readers.remove(&brane_id) {
            reader.mark_invalid();
        }

        // Trigger brane change semaphore
        self.ctx.interlock_host.on_brane_changed(
            self.fsm.peer.brane(),
            BraneChangeEvent::Destroy,
            self.fsm.peer.get_role(),
        );
        let task = FidelTask::DestroyPeer { brane_id };
        if let Err(e) = self.ctx.fidel_interlock_semaphore.schedule(task) {
            error!(
                "failed to notify fidel";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
        let is_initialized = self.fsm.peer.is_initialized();
        if let Err(e) = self.fsm.peer.destroy(self.ctx, merged_by_target) {
            // If not panic here, the peer will be recreated in the next respacelike,
            // then it will be gc again. But if some overlap brane is created
            // before respacelikeing, the gc action will delete the overlap brane's
            // data too.
            panic!("{} destroy err {:?}", self.fsm.peer.tag, e);
        }
        // Some places use `force_lightlike().unwrap()` if the StoreMeta dagger is held.
        // So in here, it's necessary to held the StoreMeta dagger when closing the router.
        self.ctx.router.close(brane_id);
        self.fsm.stop();

        if is_initialized
            && !merged_by_target
            && meta
                .brane_cones
                .remove(&enc_lightlike_key(self.fsm.peer.brane()))
                .is_none()
        {
            panic!("{} meta corruption detected", self.fsm.peer.tag);
        }
        if meta.branes.remove(&brane_id).is_none() && !merged_by_target {
            panic!("{} meta corruption detected", self.fsm.peer.tag)
        }

        if self.fsm.peer.local_first_replicate {
            let mut plightlikeing_create_peers = self.ctx.plightlikeing_create_peers.dagger().unwrap();
            if is_initialized {
                assert!(plightlikeing_create_peers.get(&brane_id).is_none());
            } else {
                // If this brane's data in `plightlikeing_create_peers` is not equal to `(peer_id, false)`,
                // it means this peer will be replaced by the split one.
                if let Some(status) = plightlikeing_create_peers.get(&brane_id) {
                    if *status == (self.fsm.peer_id(), false) {
                        plightlikeing_create_peers.remove(&brane_id);
                    }
                }
            }
        }

        // Clear merge related structures.
        if let Some(&need_atomic) = meta.destroyed_brane_for_snap.get(&brane_id) {
            if need_atomic {
                panic!(
                    "{} should destroy with target brane atomically",
                    self.fsm.peer.tag
                );
            } else {
                let target_brane_id = *meta.targets_map.get(&brane_id).unwrap();
                let is_ready = meta
                    .atomic_snap_branes
                    .get_mut(&target_brane_id)
                    .unwrap()
                    .get_mut(&brane_id)
                    .unwrap();
                *is_ready = true;
            }
        }

        meta.plightlikeing_merge_targets.remove(&brane_id);
        if let Some(target) = meta.targets_map.remove(&brane_id) {
            if meta.plightlikeing_merge_targets.contains_key(&target) {
                meta.plightlikeing_merge_targets
                    .get_mut(&target)
                    .unwrap()
                    .remove(&brane_id);
                // When the target doesn't exist(add peer but the store is isolated), source peer decide to destroy by itself.
                // Without target, the `plightlikeing_merge_targets` for target won't be removed, so here source peer help target to clear.
                if meta.branes.get(&target).is_none()
                    && meta.plightlikeing_merge_targets.get(&target).unwrap().is_empty()
                {
                    meta.plightlikeing_merge_targets.remove(&target);
                }
            }
        }
    }

    fn on_ready_change_peer(&mut self, cp: ChangePeer) {
        if cp.conf_change.get_node_id() == violetabft::INVALID_ID {
            // Apply failed, skip.
            return;
        }

        let change_type = cp.conf_change.get_change_type();
        if cp.index >= self.fsm.peer.violetabft_group.violetabft.violetabft_log.first_index() {
            match self.fsm.peer.violetabft_group.apply_conf_change(&cp.conf_change) {
                Ok(_) => {}
                // FIDel could dispatch redundant conf changes.
                Err(violetabft::Error::NotExists(_, _)) | Err(violetabft::Error::Exists(_, _)) => {}
                _ => unreachable!(),
            }
        } else {
            // Please take a look at test case test_redundant_conf_change_by_snapshot.
        }

        {
            let mut meta = self.ctx.store_meta.dagger().unwrap();
            meta.set_brane(&self.ctx.interlock_host, cp.brane, &mut self.fsm.peer);
        }

        let peer_id = cp.peer.get_id();
        let now = Instant::now();
        match change_type {
            ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                let peer = cp.peer.clone();
                let group_id = self
                    .ctx
                    .global_replication_state
                    .dagger()
                    .unwrap()
                    .group
                    .group_id(self.fsm.peer.replication_mode_version, peer.store_id);
                if group_id.unwrap_or(0) != 0 {
                    info!("ufidelating group"; "peer_id" => peer.id, "group_id" => group_id.unwrap());
                    self.fsm
                        .peer
                        .violetabft_group
                        .violetabft
                        .assign_commit_groups(&[(peer.id, group_id.unwrap())]);
                }
                if self.fsm.peer.peer_id() == peer_id {
                    self.fsm.peer.peer = peer.clone();
                }

                // Add this peer to cache and heartbeats.
                let id = peer.get_id();
                self.fsm.peer.peer_heartbeats.insert(id, now);
                if self.fsm.peer.is_leader() {
                    // Speed up snapshot instead of waiting another heartbeat.
                    self.fsm.peer.ping();
                    self.fsm.has_ready = true;
                    self.fsm.peer.peers_spacelike_plightlikeing_time.push((id, now));
                }
                self.fsm.peer.insert_peer_cache(peer);
            }
            ConfChangeType::RemoveNode => {
                // Remove this peer from cache.
                self.fsm.peer.peer_heartbeats.remove(&peer_id);
                if self.fsm.peer.is_leader() {
                    self.fsm
                        .peer
                        .peers_spacelike_plightlikeing_time
                        .retain(|&(p, _)| p != peer_id);
                }
                self.fsm.peer.remove_peer_from_cache(peer_id);
            }
        }

        // In TuringString matching above, if the peer is the leader,
        // it will push the change peer into `peers_spacelike_plightlikeing_time`
        // without checking if it is duplicated. We move `heartbeat_fidel` here
        // to utilize `collect_plightlikeing_peers` in `heartbeat_fidel` to avoid
        // adding the redundant peer.
        if self.fsm.peer.is_leader() {
            // Notify fidel immediately.
            info!(
                "notify fidel with change peer brane";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "brane" => ?self.fsm.peer.brane(),
            );
            self.fsm.peer.heartbeat_fidel(self.ctx);
        }
        let my_peer_id = self.fsm.peer.peer_id();

        let peer = cp.peer;

        // We only care remove itself now.
        if change_type == ConfChangeType::RemoveNode && peer.get_store_id() == self.store_id() {
            if my_peer_id == peer.get_id() {
                self.destroy_peer(false);
            } else {
                panic!(
                    "{} trying to remove unknown peer {:?}",
                    self.fsm.peer.tag, peer
                );
            }
        }
    }

    fn on_ready_compact_log(&mut self, first_index: u64, state: VioletaBftTruncatedState) {
        let total_cnt = self.fsm.peer.last_applying_idx - first_index;
        // the size of current CompactLog command can be ignored.
        let remain_cnt = self.fsm.peer.last_applying_idx - state.get_index() - 1;
        self.fsm.peer.violetabft_log_size_hint =
            self.fsm.peer.violetabft_log_size_hint * remain_cnt / total_cnt;
        let compact_to = state.get_index() + 1;
        let task = VioletaBftlogGcTask::gc(
            self.fsm.peer.get_store().get_brane_id(),
            self.fsm.peer.last_compacted_idx,
            compact_to,
        );
        self.fsm.peer.last_compacted_idx = compact_to;
        self.fsm.peer.mut_store().compact_to(compact_to);
        if let Err(e) = self.ctx.violetabftlog_gc_interlock_semaphore.schedule(task) {
            error!(
                "failed to schedule compact task";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
    }

    fn on_ready_split_brane(
        &mut self,
        derived: meta_timeshare::Brane,
        branes: Vec<meta_timeshare::Brane>,
        new_split_branes: HashMap<u64, apply::NewSplitPeer>,
    ) {
        self.register_split_brane_check_tick();
        let mut meta = self.ctx.store_meta.dagger().unwrap();
        let brane_id = derived.get_id();
        meta.set_brane(&self.ctx.interlock_host, derived, &mut self.fsm.peer);
        self.fsm.peer.post_split();
        let is_leader = self.fsm.peer.is_leader();
        if is_leader {
            self.fsm.peer.heartbeat_fidel(self.ctx);
            // Notify fidel immediately to let it fidelio the brane meta.
            info!(
                "notify fidel with split";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "split_count" => branes.len(),
            );
            // Now fidel only uses ReportBatchSplit for history operation show,
            // so we lightlike it indeplightlikeently here.
            let task = FidelTask::ReportBatchSplit {
                branes: branes.to_vec(),
            };
            if let Err(e) = self.ctx.fidel_interlock_semaphore.schedule(task) {
                error!(
                    "failed to notify fidel";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "err" => %e,
                );
            }
        }

        let last_key = enc_lightlike_key(branes.last().unwrap());
        if meta.brane_cones.remove(&last_key).is_none() {
            panic!("{} original brane should exists", self.fsm.peer.tag);
        }
        // It's not correct anymore, so set it to None to let split checker fidelio it.
        self.fsm.peer.approximate_size = None;
        let last_brane_id = branes.last().unwrap().get_id();
        for new_brane in branes {
            let new_brane_id = new_brane.get_id();

            let not_exist = meta
                .brane_cones
                .insert(enc_lightlike_key(&new_brane), new_brane_id)
                .is_none();
            assert!(not_exist, "[brane {}] should not exists", new_brane_id);

            if new_brane_id == brane_id {
                continue;
            }

            // Create new brane
            let new_split_peer = new_split_branes.get(&new_brane.get_id()).unwrap();
            if new_split_peer.result.is_some() {
                if let Err(e) = self
                    .fsm
                    .peer
                    .mut_store()
                    .clear_extra_split_data(enc_spacelike_key(&new_brane), enc_lightlike_key(&new_brane))
                {
                    error!(?e;
                        "failed to cleanup extra split data, may leave some dirty data";
                        "brane_id" => new_brane.get_id(),
                    );
                }
                continue;
            }

            {
                let mut plightlikeing_create_peers = self.ctx.plightlikeing_create_peers.dagger().unwrap();
                assert_eq!(
                    plightlikeing_create_peers.remove(&new_brane_id),
                    Some((new_split_peer.peer_id, true))
                );
            }

            // Insert new branes and validation
            info!(
                "insert new brane";
                "brane_id" => new_brane_id,
                "brane" => ?new_brane,
            );
            if let Some(r) = meta.branes.get(&new_brane_id) {
                // Suppose a new node is added by conf change and the snapshot comes slowly.
                // Then, the brane splits and the first vote message comes to the new node
                // before the old snapshot, which will create an uninitialized peer on the
                // store. After that, the old snapshot comes, followed with the last split
                // proposal. After it's applied, the uninitialized peer will be met.
                // We can remove this uninitialized peer directly.
                if util::is_brane_initialized(r) {
                    panic!(
                        "[brane {}] duplicated brane {:?} for split brane {:?}",
                        new_brane_id, r, new_brane
                    );
                }
                self.ctx.router.close(new_brane_id);
            }

            let (lightlikeer, mut new_peer) = match PeerFsm::create(
                self.ctx.store_id(),
                &self.ctx.causet,
                self.ctx.brane_interlock_semaphore.clone(),
                self.ctx.engines.clone(),
                &new_brane,
            ) {
                Ok((lightlikeer, new_peer)) => (lightlikeer, new_peer),
                Err(e) => {
                    // peer information is already written into db, can't recover.
                    // there is probably a bug.
                    panic!("create new split brane {:?} err {:?}", new_brane, e);
                }
            };
            let mut replication_state = self.ctx.global_replication_state.dagger().unwrap();
            new_peer.peer.init_replication_mode(&mut *replication_state);
            drop(replication_state);

            let meta_peer = new_peer.peer.peer.clone();

            for p in new_brane.get_peers() {
                // Add this peer to cache.
                new_peer.peer.insert_peer_cache(p.clone());
            }

            // New peer derive write flow from parent brane,
            // this will be used by balance write flow.
            new_peer.peer.peer_stat = self.fsm.peer.peer_stat.clone();
            let campaigned = new_peer.peer.maybe_campaign(is_leader);
            new_peer.has_ready |= campaigned;

            if is_leader {
                // The new peer is likely to become leader, lightlike a heartbeat immediately to reduce
                // client query miss.
                new_peer.peer.heartbeat_fidel(self.ctx);
            }

            new_peer.peer.activate(self.ctx);
            meta.branes.insert(new_brane_id, new_brane);
            meta.readers
                .insert(new_brane_id, Readpushdown_causet::from_peer(new_peer.get_peer()));
            if last_brane_id == new_brane_id {
                // To prevent from big brane, the right brane needs run split
                // check again after split.
                new_peer.peer.size_diff_hint = self.ctx.causet.brane_split_check_diff.0;
            }
            let mailbox = BasicMailbox::new(lightlikeer, new_peer);
            self.ctx.router.register(new_brane_id, mailbox);
            self.ctx
                .router
                .force_lightlike(new_brane_id, PeerMsg::Start)
                .unwrap();

            if !campaigned {
                if let Some(msg) = meta
                    .plightlikeing_votes
                    .swap_remove_front(|m| m.get_to_peer() == &meta_peer)
                {
                    if let Err(e) = self
                        .ctx
                        .router
                        .force_lightlike(new_brane_id, PeerMsg::VioletaBftMessage(msg))
                    {
                        warn!("handle first requset vote failed"; "brane_id" => brane_id, "error" => ?e);
                    }
                }
            }
        }
    }

    fn register_merge_check_tick(&mut self) {
        self.schedule_tick(PeerTicks::CHECK_MERGE)
    }

    /// Check if merge target brane is staler than the local one in kv engine.
    /// It should be called when target brane is not in brane map in memory.
    /// If everything is ok, the answer should always be true because FIDel should ensure all target peers exist.
    /// So if not, error log will be printed and return false.
    fn is_merge_target_brane_stale(&self, target_brane: &meta_timeshare::Brane) -> Result<bool> {
        let target_brane_id = target_brane.get_id();
        let target_peer_id = util::find_peer(target_brane, self.ctx.store_id())
            .unwrap()
            .get_id();

        let state_key = tuplespaceInstanton::brane_state_key(target_brane_id);
        if let Some(target_state) = self
            .ctx
            .engines
            .kv
            .get_msg_causet::<BraneLocalState>(Causet_VIOLETABFT, &state_key)?
        {
            if util::is_epoch_stale(
                target_brane.get_brane_epoch(),
                target_state.get_brane().get_brane_epoch(),
            ) {
                return Ok(true);
            }
            // The local target brane epoch is staler than target brane's.
            // In the case where the peer is destroyed by receiving gc msg rather than applying conf change,
            // the epoch may staler but it's legal, so check peer id to assure that.
            if let Some(local_target_peer_id) =
                util::find_peer(target_state.get_brane(), self.ctx.store_id()).map(|r| r.get_id())
            {
                match local_target_peer_id.cmp(&target_peer_id) {
                    cmp::Ordering::Equal => {
                        if target_state.get_state() == PeerState::Tombstone {
                            // The local target peer has already been destroyed.
                            return Ok(true);
                        }
                        error!(
                            "the local target peer state is not tombstone in kv engine";
                            "target_peer_id" => target_peer_id,
                            "target_peer_state" => ?target_state.get_state(),
                            "target_brane" => ?target_brane,
                            "brane_id" => self.fsm.brane_id(),
                            "peer_id" => self.fsm.peer_id(),
                        );
                    }
                    cmp::Ordering::Greater => {
                        // The local target peer id is greater than the one in target brane, but its epoch
                        // is staler than target_brane's. That is contradictory.
                        panic!("{} local target peer id {} is greater than the one in target brane {}, but its epoch is staler, local target brane {:?},
                                    target brane {:?}", self.fsm.peer.tag, local_target_peer_id, target_peer_id, target_state.get_brane(), target_brane);
                    }
                    cmp::Ordering::Less => {
                        error!(
                            "the local target peer id in kv engine is less than the one in target brane";
                            "local_target_peer_id" => local_target_peer_id,
                            "target_peer_id" => target_peer_id,
                            "target_brane" => ?target_brane,
                            "brane_id" => self.fsm.brane_id(),
                            "peer_id" => self.fsm.peer_id(),
                        );
                    }
                }
            } else {
                // Can't get local target peer id probably because this target peer is removed by applying conf change
                error!(
                    "the local target peer does not exist in target brane state";
                    "target_brane" => ?target_brane,
                    "local_target" => ?target_state.get_brane(),
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
            }
        } else {
            error!(
                "failed to load target peer's BraneLocalState from kv engine";
                "target_peer_id" => target_peer_id,
                "target_brane" => ?target_brane,
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
            );
        }
        Ok(false)
    }

    fn validate_merge_peer(&self, target_brane: &meta_timeshare::Brane) -> Result<bool> {
        let target_brane_id = target_brane.get_id();
        let exist_brane = {
            let meta = self.ctx.store_meta.dagger().unwrap();
            meta.branes.get(&target_brane_id).cloned()
        };
        if let Some(r) = exist_brane {
            let exist_epoch = r.get_brane_epoch();
            let expect_epoch = target_brane.get_brane_epoch();
            // exist_epoch > expect_epoch
            if util::is_epoch_stale(expect_epoch, exist_epoch) {
                return Err(box_err!(
                    "target brane changed {:?} -> {:?}",
                    target_brane,
                    r
                ));
            }
            // exist_epoch < expect_epoch
            if util::is_epoch_stale(exist_epoch, expect_epoch) {
                info!(
                    "target brane still not catch up, skip.";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_brane" => ?target_brane,
                    "exist_brane" => ?r,
                );
                return Ok(false);
            }
            return Ok(true);
        }

        // All of the target peers must exist before merging which is guaranteed by FIDel.
        // Now the target peer is not in brane map.
        match self.is_merge_target_brane_stale(target_brane) {
            Err(e) => {
                error!(%e;
                    "failed to load brane state, ignore";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_brane_id" => target_brane_id,
                );
                Ok(false)
            }
            Ok(true) => Err(box_err!("brane {} is destroyed", target_brane_id)),
            Ok(false) => {
                if self.ctx.causet.dev_assert {
                    panic!(
                        "something is wrong, maybe FIDel do not ensure all target peers exist before merging"
                    );
                }
                error!("something is wrong, maybe FIDel do not ensure all target peers exist before merging");
                Ok(false)
            }
        }
    }

    fn schedule_merge(&mut self) -> Result<()> {
        fail_point!("on_schedule_merge", |_| Ok(()));
        let (request, target_id) = {
            let state = self.fsm.peer.plightlikeing_merge_state.as_ref().unwrap();
            let expect_brane = state.get_target();
            if !self.validate_merge_peer(expect_brane)? {
                // Wait till next round.
                return Ok(());
            }
            let target_id = expect_brane.get_id();
            let sibling_brane = expect_brane;

            let (min_index, _) = self.fsm.peer.get_min_progress()?;
            let low = cmp::max(min_index + 1, state.get_min_index());
            // TODO: move this into violetabft module.
            // > over >= to include the PrepareMerge proposal.
            let entries = if low > state.get_commit() {
                vec![]
            } else {
                match self
                    .fsm
                    .peer
                    .get_store()
                    .entries(low, state.get_commit() + 1, NO_LIMIT)
                {
                    Ok(ents) => ents,
                    Err(e) => panic!(
                        "[brane {}] {} failed to get merge entires: {:?}, low:{}, commit: {}",
                        self.fsm.brane_id(),
                        self.fsm.peer_id(),
                        e,
                        low,
                        state.get_commit()
                    ),
                }
            };

            let sibling_peer = util::find_peer(&sibling_brane, self.store_id()).unwrap();
            let mut request = new_admin_request(sibling_brane.get_id(), sibling_peer.clone());
            request
                .mut_header()
                .set_brane_epoch(sibling_brane.get_brane_epoch().clone());
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(AdminCmdType::CommitMerge);
            admin
                .mut_commit_merge()
                .set_source(self.fsm.peer.brane().clone());
            admin.mut_commit_merge().set_commit(state.get_commit());
            admin.mut_commit_merge().set_entries(entries.into());
            request.set_admin_request(admin);
            (request, target_id)
        };
        // Please note that, here assumes that the unit of network isolation is store rather than
        // peer. So a quorum stores of source brane should also be the quorum stores of target
        // brane. Otherwise we need to enable proposal forwarding.
        self.ctx
            .router
            .force_lightlike(
                target_id,
                PeerMsg::VioletaBftCommand(VioletaBftCommand::new(request, Callback::None)),
            )
            .map_err(|_| Error::BraneNotFound(target_id))
    }

    fn rollback_merge(&mut self) {
        let req = {
            let state = self.fsm.peer.plightlikeing_merge_state.as_ref().unwrap();
            let mut request =
                new_admin_request(self.fsm.peer.brane().get_id(), self.fsm.peer.peer.clone());
            request
                .mut_header()
                .set_brane_epoch(self.fsm.peer.brane().get_brane_epoch().clone());
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(AdminCmdType::RollbackMerge);
            admin.mut_rollback_merge().set_commit(state.get_commit());
            request.set_admin_request(admin);
            request
        };
        self.propose_violetabft_command(req, Callback::None);
    }

    fn on_check_merge(&mut self) {
        if self.fsm.stopped
            || self.fsm.peer.plightlikeing_remove
            || self.fsm.peer.plightlikeing_merge_state.is_none()
        {
            return;
        }
        self.register_merge_check_tick();
        fail_point!(
            "on_check_merge_not_1001",
            self.fsm.peer_id() != 1001,
            |_| {}
        );
        if let Err(e) = self.schedule_merge() {
            if self.fsm.peer.is_leader() {
                self.fsm
                    .peer
                    .add_want_rollback_merge_peer(self.fsm.peer_id());
                if self
                    .fsm
                    .peer
                    .violetabft_group
                    .violetabft
                    .prs()
                    .has_quorum(&self.fsm.peer.want_rollback_merge_peers)
                {
                    info!(
                        "failed to schedule merge, rollback";
                        "brane_id" => self.fsm.brane_id(),
                        "peer_id" => self.fsm.peer_id(),
                        "err" => %e,
                        "error_code" => %e.error_code(),
                    );
                    self.rollback_merge();
                }
            } else if !is_learner(&self.fsm.peer.peer) {
                info!(
                    "want to rollback merge";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "leader_id" => self.fsm.peer.leader_id(),
                    "err" => %e,
                    "error_code" => %e.error_code(),
                );
                if self.fsm.peer.leader_id() != violetabft::INVALID_ID {
                    self.fsm.peer.lightlike_want_rollback_merge(
                        self.fsm
                            .peer
                            .plightlikeing_merge_state
                            .as_ref()
                            .unwrap()
                            .get_commit(),
                        &mut self.ctx,
                    );
                }
            }
        }
    }

    fn on_ready_prepare_merge(&mut self, brane: meta_timeshare::Brane, state: MergeState) {
        {
            let mut meta = self.ctx.store_meta.dagger().unwrap();
            meta.set_brane(&self.ctx.interlock_host, brane, &mut self.fsm.peer);
        }

        self.fsm.peer.plightlikeing_merge_state = Some(state);
        let state = self.fsm.peer.plightlikeing_merge_state.as_ref().unwrap();

        if let Some(ref catch_up_logs) = self.fsm.peer.catch_up_logs {
            if state.get_commit() == catch_up_logs.merge.get_commit() {
                assert_eq!(state.get_target().get_id(), catch_up_logs.target_brane_id);
                // Indicate that `on_catch_up_logs_for_merge` has already executed.
                // Mark plightlikeing_remove because its apply fsm will be destroyed.
                self.fsm.peer.plightlikeing_remove = true;
                // lightlike CatchUpLogs back to destroy source apply fsm,
                // then it will lightlike `Noop` to trigger target apply fsm.
                self.ctx.apply_router.schedule_task(
                    self.fsm.brane_id(),
                    ApplyTask::LogsUpToDate(self.fsm.peer.catch_up_logs.take().unwrap()),
                );
                return;
            }
        }

        self.on_check_merge();
    }

    fn on_catch_up_logs_for_merge(&mut self, mut catch_up_logs: CatchUpLogs) {
        let brane_id = self.fsm.brane_id();
        assert_eq!(brane_id, catch_up_logs.merge.get_source().get_id());

        if let Some(ref cul) = self.fsm.peer.catch_up_logs {
            panic!(
                "{} get catch_up_logs from {} but has already got from {}",
                self.fsm.peer.tag, catch_up_logs.target_brane_id, cul.target_brane_id
            )
        }

        if let Some(ref plightlikeing_merge_state) = self.fsm.peer.plightlikeing_merge_state {
            if plightlikeing_merge_state.get_commit() == catch_up_logs.merge.get_commit() {
                assert_eq!(
                    plightlikeing_merge_state.get_target().get_id(),
                    catch_up_logs.target_brane_id
                );
                // Indicate that `on_ready_prepare_merge` has already executed.
                // Mark plightlikeing_remove because its apply fsm will be destroyed.
                self.fsm.peer.plightlikeing_remove = true;
                // Just for saving memory.
                catch_up_logs.merge.clear_entries();
                // lightlike CatchUpLogs back to destroy source apply fsm,
                // then it will lightlike `Noop` to trigger target apply fsm.
                self.ctx
                    .apply_router
                    .schedule_task(brane_id, ApplyTask::LogsUpToDate(catch_up_logs));
                return;
            }
        }

        // Directly applightlike these logs to violetabft log and then commit them.
        match self
            .fsm
            .peer
            .maybe_applightlike_merge_entries(&catch_up_logs.merge)
        {
            Some(last_index) => {
                info!(
                    "applightlike and commit entries to source brane";
                    "brane_id" => brane_id,
                    "peer_id" => self.fsm.peer.peer_id(),
                    "last_index" => last_index,
                );
                // Now it has some committed entries, so mark it to take `Ready` in next round.
                self.fsm.has_ready = true;
            }
            None => {
                info!(
                    "no need to catch up logs";
                    "brane_id" => brane_id,
                    "peer_id" => self.fsm.peer.peer_id(),
                );
            }
        }
        // Just for saving memory.
        catch_up_logs.merge.clear_entries();
        self.fsm.peer.catch_up_logs = Some(catch_up_logs);
    }

    fn on_ready_commit_merge(&mut self, brane: meta_timeshare::Brane, source: meta_timeshare::Brane) {
        self.register_split_brane_check_tick();
        let mut meta = self.ctx.store_meta.dagger().unwrap();

        let prev = meta.brane_cones.remove(&enc_lightlike_key(&source));
        assert_eq!(prev, Some(source.get_id()));
        let prev = if brane.get_lightlike_key() == source.get_lightlike_key() {
            meta.brane_cones.remove(&enc_spacelike_key(&source))
        } else {
            meta.brane_cones.remove(&enc_lightlike_key(&brane))
        };
        if prev != Some(brane.get_id()) {
            panic!(
                "{} meta corrupted: prev: {:?}, cones: {:?}",
                self.fsm.peer.tag, prev, meta.brane_cones
            );
        }
        meta.brane_cones
            .insert(enc_lightlike_key(&brane), brane.get_id());
        assert!(meta.branes.remove(&source.get_id()).is_some());
        meta.set_brane(&self.ctx.interlock_host, brane, &mut self.fsm.peer);
        let reader = meta.readers.remove(&source.get_id()).unwrap();
        reader.mark_invalid();

        // If a follower merges into a leader, a more recent read may happen
        // on the leader of the follower. So max ts should be fideliod after
        // a brane merge.
        self.fsm
            .peer
            .require_ufidelating_max_ts(&self.ctx.fidel_interlock_semaphore);

        drop(meta);

        // make approximate size and tuplespaceInstanton fideliod in time.
        // the reason why follower need to fidelio is that there is a issue that after merge
        // and then transfer leader, the new leader may have stale size and tuplespaceInstanton.
        self.fsm.peer.size_diff_hint = self.ctx.causet.brane_split_check_diff.0;
        if self.fsm.peer.is_leader() {
            info!(
                "notify fidel with merge";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "source_brane" => ?source,
                "target_brane" => ?self.fsm.peer.brane(),
            );
            self.fsm.peer.heartbeat_fidel(self.ctx);
        }
        if let Err(e) = self.ctx.router.force_lightlike(
            source.get_id(),
            PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
                target_brane_id: self.fsm.brane_id(),
                target: self.fsm.peer.peer.clone(),
                result: MergeResultKind::FromTargetLog,
            }),
        ) {
            if !self.ctx.router.is_shutdown() {
                panic!(
                    "{} failed to lightlike merge result(FromTargetLog) to source brane {}, err {}",
                    self.fsm.peer.tag,
                    source.get_id(),
                    e
                );
            }
        }
    }

    /// Handle rollbacking Merge result.
    ///
    /// If commit is 0, it means that Merge is rollbacked by a snapshot; otherwise
    /// it's rollbacked by a proposal, and its value should be equal to the commit
    /// index of previous PrepareMerge.
    fn on_ready_rollback_merge(&mut self, commit: u64, brane: Option<meta_timeshare::Brane>) {
        let plightlikeing_commit = self
            .fsm
            .peer
            .plightlikeing_merge_state
            .as_ref()
            .unwrap()
            .get_commit();
        if commit != 0 && plightlikeing_commit != commit {
            panic!(
                "{} rollbacks a wrong merge: {} != {}",
                self.fsm.peer.tag, plightlikeing_commit, commit
            );
        }
        // Clear merge releted data
        self.fsm.peer.plightlikeing_merge_state = None;
        self.fsm.peer.want_rollback_merge_peers.clear();

        if let Some(r) = brane {
            let mut meta = self.ctx.store_meta.dagger().unwrap();
            meta.set_brane(&self.ctx.interlock_host, r, &mut self.fsm.peer);
        }
        if self.fsm.peer.is_leader() {
            info!(
                "notify fidel with rollback merge";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "commit_index" => commit,
            );
            self.fsm.peer.heartbeat_fidel(self.ctx);
        }
    }

    fn on_merge_result(
        &mut self,
        target_brane_id: u64,
        target: meta_timeshare::Peer,
        result: MergeResultKind,
    ) {
        let exists = self
            .fsm
            .peer
            .plightlikeing_merge_state
            .as_ref()
            .map_or(true, |s| s.get_target().get_peers().contains(&target));
        if !exists {
            panic!(
                "{} unexpected merge result: {:?} {:?} {:?}",
                self.fsm.peer.tag, self.fsm.peer.plightlikeing_merge_state, target, result
            );
        }
        // Because of the checking before proposing `PrepareMerge`, which is
        // no `CompactLog` proposal between the smallest commit index and the latest index.
        // If the merge succeed, all source peers are impossible in apply snapshot state
        // and must be initialized.
        {
            let meta = self.ctx.store_meta.dagger().unwrap();
            if meta.atomic_snap_branes.contains_key(&self.brane_id()) {
                panic!(
                    "{} is applying atomic snapshot on getting merge result, target brane id {}, target peer {:?}, merge result type {:?}",
                    self.fsm.peer.tag, target_brane_id, target, result
                );
            }
        }
        if self.fsm.peer.is_applying_snapshot() {
            panic!(
                "{} is applying snapshot on getting merge result, target brane id {}, target peer {:?}, merge result type {:?}",
                self.fsm.peer.tag, target_brane_id, target, result
            );
        }
        if !self.fsm.peer.is_initialized() {
            panic!(
                "{} is not initialized on getting merge result, target brane id {}, target peer {:?}, merge result type {:?}",
                self.fsm.peer.tag, target_brane_id, target, result
            );
        }
        match result {
            MergeResultKind::FromTargetLog => {
                info!(
                    "merge finished";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_brane" => ?self.fsm.peer.plightlikeing_merge_state.as_ref().unwrap().target,
                );
                self.destroy_peer(true);
            }
            MergeResultKind::FromTargetSnapshotStep1 => {
                info!(
                    "merge finished with target snapshot";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_brane_id" => target_brane_id,
                );
                self.fsm.peer.plightlikeing_remove = true;
                // Destroy apply fsm at first
                self.ctx.apply_router.schedule_task(
                    self.fsm.brane_id(),
                    ApplyTask::destroy(self.fsm.brane_id(), true),
                );
            }
            MergeResultKind::FromTargetSnapshotStep2 => {
                // `merge_by_target` is true because this brane's cone already belongs to
                // its target brane so we must not clear data otherwise its target brane's
                // data will corrupt.
                self.destroy_peer(true);
            }
            MergeResultKind::Stale => {
                self.on_stale_merge(target_brane_id);
            }
        };
    }

    fn on_stale_merge(&mut self, target_brane_id: u64) {
        if self.fsm.peer.plightlikeing_remove {
            return;
        }
        info!(
            "successful merge can't be continued, try to gc stale peer";
            "brane_id" => self.fsm.brane_id(),
            "peer_id" => self.fsm.peer_id(),
            "target_brane_id" => target_brane_id,
            "merge_state" => ?self.fsm.peer.plightlikeing_merge_state,
        );
        // Because of the checking before proposing `PrepareMerge`, which is
        // no `CompactLog` proposal between the smallest commit index and the latest index.
        // If the merge succeed, all source peers are impossible in apply snapshot state
        // and must be initialized.
        // So `maybe_destroy` must succeed here.
        let job = self.fsm.peer.maybe_destroy(&self.ctx).unwrap();
        self.handle_destroy_peer(job);
    }

    fn on_ready_apply_snapshot(&mut self, apply_result: ApplySnapResult) {
        let prev_brane = apply_result.prev_brane;
        let brane = apply_result.brane;

        info!(
            "snapshot is applied";
            "brane_id" => self.fsm.brane_id(),
            "peer_id" => self.fsm.peer_id(),
            "brane" => ?brane,
        );

        if prev_brane.get_peers() != brane.get_peers() {
            let mut state = self.ctx.global_replication_state.dagger().unwrap();
            let gb = state
                .calculate_commit_group(self.fsm.peer.replication_mode_version, brane.get_peers());
            self.fsm.peer.violetabft_group.violetabft.clear_commit_group();
            self.fsm.peer.violetabft_group.violetabft.assign_commit_groups(gb);
        }

        let mut meta = self.ctx.store_meta.dagger().unwrap();
        debug!(
            "check snapshot cone";
            "brane_id" => self.fsm.brane_id(),
            "peer_id" => self.fsm.peer_id(),
            "prev_brane" => ?prev_brane,
        );

        // Remove this brane's snapshot brane from the `plightlikeing_snapshot_branes`
        // The `plightlikeing_snapshot_branes` is only used to occupy the key cone, so if this
        // peer is added to `brane_cones`, it can be remove from `plightlikeing_snapshot_branes`
        meta.plightlikeing_snapshot_branes
            .retain(|r| self.fsm.brane_id() != r.get_id());

        // Remove its source peers' metadata
        for r in &apply_result.destroyed_branes {
            let prev = meta.brane_cones.remove(&enc_lightlike_key(&r));
            assert_eq!(prev, Some(r.get_id()));
            assert!(meta.branes.remove(&r.get_id()).is_some());
            let reader = meta.readers.remove(&r.get_id()).unwrap();
            reader.mark_invalid();
        }
        // Remove the data from `atomic_snap_branes` and `destroyed_brane_for_snap`
        // which are added before applying snapshot
        if let Some(wait_destroy_branes) = meta.atomic_snap_branes.remove(&self.fsm.brane_id()) {
            for (source_brane_id, _) in wait_destroy_branes {
                assert_eq!(
                    meta.destroyed_brane_for_snap
                        .remove(&source_brane_id)
                        .is_some(),
                    true
                );
            }
        }

        if util::is_brane_initialized(&prev_brane) {
            info!(
                "brane changed after applying snapshot";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "prev_brane" => ?prev_brane,
                "brane" => ?brane,
            );
            let prev = meta.brane_cones.remove(&enc_lightlike_key(&prev_brane));
            if prev != Some(brane.get_id()) {
                panic!(
                    "{} meta corrupted, expect {:?} got {:?}",
                    self.fsm.peer.tag, prev_brane, prev,
                );
            }
        } else if self.fsm.peer.local_first_replicate {
            // This peer is uninitialized previously.
            // More accurately, the `BraneLocalState` has been persisted so the data can be removed from `plightlikeing_create_peers`.
            let mut plightlikeing_create_peers = self.ctx.plightlikeing_create_peers.dagger().unwrap();
            assert_eq!(
                plightlikeing_create_peers.remove(&self.fsm.brane_id()),
                Some((self.fsm.peer_id(), false))
            );
        }

        if let Some(r) = meta
            .brane_cones
            .insert(enc_lightlike_key(&brane), brane.get_id())
        {
            panic!("{} unexpected brane {:?}", self.fsm.peer.tag, r);
        }
        let prev = meta.branes.insert(brane.get_id(), brane);
        assert_eq!(prev, Some(prev_brane));

        drop(meta);

        for r in &apply_result.destroyed_branes {
            if let Err(e) = self.ctx.router.force_lightlike(
                r.get_id(),
                PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
                    target_brane_id: self.fsm.brane_id(),
                    target: self.fsm.peer.peer.clone(),
                    result: MergeResultKind::FromTargetSnapshotStep2,
                }),
            ) {
                if !self.ctx.router.is_shutdown() {
                    panic!("{} failed to lightlike merge result(FromTargetSnapshotStep2) to source brane {}, err {}", self.fsm.peer.tag, r.get_id(), e);
                }
            }
        }
    }

    fn on_ready_result(
        &mut self,
        exec_results: &mut VecDeque<ExecResult<EK::Snapshot>>,
        metrics: &ApplyMetrics,
    ) {
        // handle executing committed log results
        while let Some(result) = exec_results.pop_front() {
            match result {
                ExecResult::ChangePeer(cp) => self.on_ready_change_peer(cp),
                ExecResult::CompactLog { first_index, state } => {
                    self.on_ready_compact_log(first_index, state)
                }
                ExecResult::SplitBrane {
                    derived,
                    branes,
                    new_split_branes,
                } => self.on_ready_split_brane(derived, branes, new_split_branes),
                ExecResult::PrepareMerge { brane, state } => {
                    self.on_ready_prepare_merge(brane, state)
                }
                ExecResult::CommitMerge { brane, source } => {
                    self.on_ready_commit_merge(brane.clone(), source.clone())
                }
                ExecResult::RollbackMerge { brane, commit } => {
                    self.on_ready_rollback_merge(commit, Some(brane))
                }
                ExecResult::ComputeHash {
                    brane,
                    index,
                    context,
                    snap,
                } => self.on_ready_compute_hash(brane, index, context, snap),
                ExecResult::VerifyHash {
                    index,
                    context,
                    hash,
                } => self.on_ready_verify_hash(index, context, hash),
                ExecResult::DeleteCone { .. } => {
                    // TODO: clean user properties?
                }
                ExecResult::IngestSst { ssts } => self.on_ingest_sst_result(ssts),
            }
        }

        // fidelio metrics only when all exec_results are finished in case the metrics is counted multiple times
        // when waiting for commit merge
        self.ctx.store_stat.lock_causet_bytes_written += metrics.lock_causet_written_bytes;
        self.ctx.store_stat.engine_total_bytes_written += metrics.written_bytes;
        self.ctx.store_stat.engine_total_tuplespaceInstanton_written += metrics.written_tuplespaceInstanton;
    }

    /// Check if a request is valid if it has valid prepare_merge/commit_merge proposal.
    fn check_merge_proposal(&self, msg: &mut VioletaBftCmdRequest) -> Result<()> {
        if !msg.get_admin_request().has_prepare_merge()
            && !msg.get_admin_request().has_commit_merge()
        {
            return Ok(());
        }

        let brane = self.fsm.peer.brane();
        if msg.get_admin_request().has_prepare_merge() {
            let target_brane = msg.get_admin_request().get_prepare_merge().get_target();
            {
                let meta = self.ctx.store_meta.dagger().unwrap();
                match meta.branes.get(&target_brane.get_id()) {
                    Some(r) => {
                        if r != target_brane {
                            return Err(box_err!(
                                "target brane not matched, skip proposing: {:?} != {:?}",
                                r,
                                target_brane
                            ));
                        }
                    }
                    None => {
                        return Err(box_err!(
                            "target brane {} doesn't exist.",
                            target_brane.get_id()
                        ));
                    }
                }
            }
            if !util::is_sibling_branes(target_brane, brane) {
                return Err(box_err!(
                    "{:?} and {:?} are not sibling, skip proposing.",
                    target_brane,
                    brane
                ));
            }
            if !util::brane_on_same_stores(target_brane, brane) {
                return Err(box_err!(
                    "peers doesn't match {:?} != {:?}, reject merge",
                    brane.get_peers(),
                    target_brane.get_peers()
                ));
            }
        } else {
            let source_brane = msg.get_admin_request().get_commit_merge().get_source();
            if !util::is_sibling_branes(source_brane, brane) {
                return Err(box_err!(
                    "{:?} and {:?} should be sibling",
                    source_brane,
                    brane
                ));
            }
            if !util::brane_on_same_stores(source_brane, brane) {
                return Err(box_err!(
                    "peers not matched: {:?} {:?}",
                    source_brane,
                    brane
                ));
            }
        }

        Ok(())
    }

    fn pre_propose_violetabft_command(
        &mut self,
        msg: &VioletaBftCmdRequest,
    ) -> Result<Option<VioletaBftCmdResponse>> {
        // Check store_id, make sure that the msg is dispatched to the right place.
        if let Err(e) = util::check_store_id(msg, self.store_id()) {
            self.ctx.violetabft_metrics.invalid_proposal.mismatch_store_id += 1;
            return Err(e);
        }
        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            let resp = self.execute_status_command(msg)?;
            return Ok(Some(resp));
        }

        // Check whether the store has the right peer to handle the request.
        let brane_id = self.brane_id();
        let leader_id = self.fsm.peer.leader_id();
        let request = msg.get_requests();

        // ReadIndex can be processed on the replicas.
        let is_read_index_request =
            request.len() == 1 && request[0].get_cmd_type() == CmdType::ReadIndex;
        let mut read_only = true;
        for r in msg.get_requests() {
            match r.get_cmd_type() {
                CmdType::Get | CmdType::Snap | CmdType::ReadIndex => (),
                _ => read_only = false,
            }
        }
        let allow_replica_read = read_only && msg.get_header().get_replica_read();
        if !(self.fsm.peer.is_leader() || is_read_index_request || allow_replica_read) {
            self.ctx.violetabft_metrics.invalid_proposal.not_leader += 1;
            let leader = self.fsm.peer.get_peer_from_cache(leader_id);
            self.fsm.group_state = GroupState::Chaos;
            self.register_violetabft_base_tick();
            return Err(Error::NotLeader(brane_id, leader));
        }
        // peer_id must be the same as peer's.
        if let Err(e) = util::check_peer_id(msg, self.fsm.peer.peer_id()) {
            self.ctx.violetabft_metrics.invalid_proposal.mismatch_peer_id += 1;
            return Err(e);
        }
        // check whether the peer is initialized.
        if !self.fsm.peer.is_initialized() {
            self.ctx
                .violetabft_metrics
                .invalid_proposal
                .brane_not_initialized += 1;
            return Err(Error::BraneNotInitialized(brane_id));
        }
        // If the peer is applying snapshot, it may drop some lightlikeing messages, that could
        // make clients wait for response until timeout.
        if self.fsm.peer.is_applying_snapshot() {
            self.ctx.violetabft_metrics.invalid_proposal.is_applying_snapshot += 1;
            // TODO: replace to a more suiBlock error.
            return Err(Error::Other(box_err!(
                "{} peer is applying snapshot",
                self.fsm.peer.tag
            )));
        }
        // Check whether the term is stale.
        if let Err(e) = util::check_term(msg, self.fsm.peer.term()) {
            self.ctx.violetabft_metrics.invalid_proposal.stale_command += 1;
            return Err(e);
        }

        match util::check_brane_epoch(msg, self.fsm.peer.brane(), true) {
            Err(Error::EpochNotMatch(msg, mut new_branes)) => {
                // Attach the brane which might be split from the current brane. But it doesn't
                // matter if the brane is not split from the current brane. If the brane meta
                // received by the EinsteinDB driver is newer than the meta cached in the driver, the meta is
                // fideliod.
                let sibling_brane = self.find_sibling_brane();
                if let Some(sibling_brane) = sibling_brane {
                    new_branes.push(sibling_brane);
                }
                self.ctx.violetabft_metrics.invalid_proposal.epoch_not_match += 1;
                Err(Error::EpochNotMatch(msg, new_branes))
            }
            Err(e) => Err(e),
            Ok(()) => Ok(None),
        }
    }

    fn propose_violetabft_command(&mut self, mut msg: VioletaBftCmdRequest, cb: Callback<EK::Snapshot>) {
        match self.pre_propose_violetabft_command(&msg) {
            Ok(Some(resp)) => {
                cb.invoke_with_response(resp);
                return;
            }
            Err(e) => {
                debug!(
                    "failed to propose";
                    "brane_id" => self.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "message" => ?msg,
                    "err" => %e,
                );
                cb.invoke_with_response(new_error(e));
                return;
            }
            _ => (),
        }

        if self.fsm.peer.plightlikeing_remove {
            apply::notify_req_brane_removed(self.brane_id(), cb);
            return;
        }

        if let Err(e) = self.check_merge_proposal(&mut msg) {
            warn!(
                "failed to propose merge";
                "brane_id" => self.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "message" => ?msg,
                "err" => %e,
                "error_code" => %e.error_code(),
            );
            cb.invoke_with_response(new_error(e));
            return;
        }

        // Note:
        // The peer that is being checked is a leader. It might step down to be a follower later. It
        // doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
        // command log entry can't be committed.

        let mut resp = VioletaBftCmdResponse::default();
        let term = self.fsm.peer.term();
        bind_term(&mut resp, term);
        if self.fsm.peer.propose(self.ctx, cb, msg, resp) {
            self.fsm.has_ready = true;
        }

        if self.fsm.peer.should_wake_up {
            self.reset_violetabft_tick(GroupState::Ordered);
        }

        self.register_fidel_heartbeat_tick();

        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.
    }

    fn find_sibling_brane(&self) -> Option<Brane> {
        let spacelike = if self.ctx.causet.right_derive_when_split {
            Included(enc_spacelike_key(self.fsm.peer.brane()))
        } else {
            Excluded(enc_lightlike_key(self.fsm.peer.brane()))
        };
        let meta = self.ctx.store_meta.dagger().unwrap();
        meta.brane_cones
            .cone((spacelike, Unbounded::<Vec<u8>>))
            .next()
            .map(|(_, brane_id)| meta.branes[brane_id].to_owned())
    }

    fn register_violetabft_gc_log_tick(&mut self) {
        self.schedule_tick(PeerTicks::VIOLETABFT_LOG_GC)
    }

    #[allow(clippy::if_same_then_else)]
    fn on_violetabft_gc_log_tick(&mut self, force_compact: bool) {
        if !self.fsm.peer.get_store().is_cache_empty() || !self.ctx.causet.hibernate_branes {
            self.register_violetabft_gc_log_tick();
        }
        fail_point!("on_violetabft_log_gc_tick_1", self.fsm.peer_id() == 1, |_| {});
        fail_point!("on_violetabft_gc_log_tick", |_| {});
        debug_assert!(!self.fsm.stopped);

        // As leader, we would not keep caches for the peers that didn't response heartbeat in the
        // last few seconds. That happens probably because another EinsteinDB is down. In this case if we
        // do not clean up the cache, it may keep growing.
        let drop_cache_duration =
            self.ctx.causet.violetabft_heartbeat_interval() + self.ctx.causet.violetabft_entry_cache_life_time.0;
        let cache_alive_limit = Instant::now() - drop_cache_duration;

        let mut total_gc_logs = 0;

        let applied_idx = self.fsm.peer.get_store().applied_index();
        if !self.fsm.peer.is_leader() {
            self.fsm.peer.mut_store().compact_to(applied_idx + 1);
            return;
        }

        // Leader will replicate the compact log command to followers,
        // If we use current replicated_index (like 10) as the compact index,
        // when we replicate this log, the newest replicated_index will be 11,
        // but we only compact the log to 10, not 11, at that time,
        // the first index is 10, and replicated_index is 11, with an extra log,
        // and we will do compact again with compact index 11, in cycles...
        // So we introduce a memory_barrier, if replicated index - first index > memory_barrier,
        // we will try to compact log.
        // violetabft log entries[..............................................]
        //                  ^                                       ^
        //                  |-----------------memory_barrier------------ |
        //              first_index                         replicated_index
        // `alive_cache_idx` is the smallest `replicated_index` of healthy up nodes.
        // `alive_cache_idx` is only used to gc cache.
        let truncated_idx = self.fsm.peer.get_store().truncated_index();
        let last_idx = self.fsm.peer.get_store().last_index();
        let (mut replicated_idx, mut alive_cache_idx) = (last_idx, last_idx);
        for (peer_id, p) in self.fsm.peer.violetabft_group.violetabft.prs().iter() {
            if replicated_idx > p.matched {
                replicated_idx = p.matched;
            }
            if let Some(last_heartbeat) = self.fsm.peer.peer_heartbeats.get(peer_id) {
                if alive_cache_idx > p.matched
                    && p.matched >= truncated_idx
                    && *last_heartbeat > cache_alive_limit
                {
                    alive_cache_idx = p.matched;
                }
            }
        }
        // When an election happened or a new peer is added, replicated_idx can be 0.
        if replicated_idx > 0 {
            assert!(
                last_idx >= replicated_idx,
                "expect last index {} >= replicated index {}",
                last_idx,
                replicated_idx
            );
            REGION_MAX_LOG_LAG.observe((last_idx - replicated_idx) as f64);
        }
        self.fsm
            .peer
            .mut_store()
            .maybe_gc_cache(alive_cache_idx, applied_idx);

        let first_idx = self.fsm.peer.get_store().first_index();

        let mut compact_idx = if force_compact
            // Too many logs between applied index and first index.
            || (applied_idx > first_idx && applied_idx - first_idx >= self.ctx.causet.violetabft_log_gc_count_limit)
            // VioletaBft log size ecceeds the limit.
            || (self.fsm.peer.violetabft_log_size_hint >= self.ctx.causet.violetabft_log_gc_size_limit.0)
        {
            applied_idx
        } else if replicated_idx < first_idx || last_idx - first_idx < 3 {
            // In the current implementation one compaction can't delete all stale VioletaBft logs.
            // There will be at least 3 entries left after one compaction:
            // |------------- entries needs to be compacted ----------|
            // [entries...][the entry at `compact_idx`][the last entry][new compaction entry]
            //             |-------------------- entries will be left ----------------------|
            return;
        } else if replicated_idx - first_idx < self.ctx.causet.violetabft_log_gc_memory_barrier
            && self.fsm.skip_gc_violetabft_log_ticks < self.ctx.causet.violetabft_log_reserve_max_ticks
        {
            // Logs will only be kept `max_ticks` * `violetabft_log_gc_tick_interval`.
            self.fsm.skip_gc_violetabft_log_ticks += 1;
            self.register_violetabft_gc_log_tick();
            return;
        } else {
            replicated_idx
        };
        assert!(compact_idx >= first_idx);
        // Have no idea why subtract 1 here, but original code did this by magic.
        compact_idx -= 1;
        if compact_idx < first_idx {
            // In case compact_idx == first_idx before subtraction.
            return;
        }
        total_gc_logs += compact_idx - first_idx;

        // Create a compact log request and notify directly.
        let brane_id = self.fsm.peer.brane().get_id();
        let peer = self.fsm.peer.peer.clone();
        let term = self.fsm.peer.get_index_term(compact_idx);
        let request = new_compact_log_request(brane_id, peer, compact_idx, term);
        self.propose_violetabft_command(request, Callback::None);

        self.fsm.skip_gc_violetabft_log_ticks = 0;
        self.register_violetabft_gc_log_tick();
        PEER_GC_VIOLETABFT_LOG_COUNTER.inc_by(total_gc_logs as i64);
    }

    fn register_split_brane_check_tick(&mut self) {
        self.schedule_tick(PeerTicks::SPLIT_REGION_CHECK)
    }

    #[inline]
    fn brane_split_skip_max_count(&self) -> usize {
        fail_point!("brane_split_skip_max_count", |_| { usize::max_value() });
        REGION_SPLIT_SKIP_MAX_COUNT
    }

    fn on_split_brane_check_tick(&mut self) {
        if !self.ctx.causet.hibernate_branes {
            self.register_split_brane_check_tick();
        }
        if !self.fsm.peer.is_leader() {
            return;
        }

        // To avoid frequent scan, we only add new scan tasks if all previous tasks
        // have finished.
        // TODO: check whether a gc progress has been spacelikeed.
        if self.ctx.split_check_interlock_semaphore.is_busy() {
            self.register_split_brane_check_tick();
            return;
        }

        // When respacelike, the approximate size will be None. The split check will first
        // check the brane size, and then check whether the brane should split. This
        // should work even if we change the brane max size.
        // If peer says should fidelio approximate size, fidelio brane size and check
        // whether the brane should split.
        if self.fsm.peer.approximate_size.is_some()
            && self.fsm.peer.compaction_declined_bytes < self.ctx.causet.brane_split_check_diff.0
            && self.fsm.peer.size_diff_hint < self.ctx.causet.brane_split_check_diff.0
        {
            return;
        }

        // bulk insert too fast may cause snapshot stale very soon, worst case it stale before
        // lightlikeing. so when snapshot is generating or lightlikeing, skip split check at most 3 times.
        // There is a trade off between brane size and snapshot success rate. Split check is
        // triggered every 10 seconds. If a snapshot can't be generated in 30 seconds, it might be
        // just too large to be generated. Split it into smaller size can help generation. check
        // issue 330 for more info.
        if self.fsm.peer.get_store().is_generating_snapshot()
            && self.fsm.skip_split_count < self.brane_split_skip_max_count()
        {
            self.fsm.skip_split_count += 1;
            return;
        }
        self.fsm.skip_split_count = 0;

        let task =
            SplitCheckTask::split_check(self.fsm.peer.brane().clone(), true, CheckPolicy::Scan);
        if let Err(e) = self.ctx.split_check_interlock_semaphore.schedule(task) {
            error!(
                "failed to schedule split check";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
        self.fsm.peer.size_diff_hint = 0;
        self.fsm.peer.compaction_declined_bytes = 0;
        self.register_split_brane_check_tick();
    }

    fn on_prepare_split_brane(
        &mut self,
        brane_epoch: meta_timeshare::BraneEpoch,
        split_tuplespaceInstanton: Vec<Vec<u8>>,
        cb: Callback<EK::Snapshot>,
    ) {
        if let Err(e) = self.validate_split_brane(&brane_epoch, &split_tuplespaceInstanton) {
            cb.invoke_with_response(new_error(e));
            return;
        }
        let brane = self.fsm.peer.brane();
        let task = FidelTask::AskBatchSplit {
            brane: brane.clone(),
            split_tuplespaceInstanton,
            peer: self.fsm.peer.peer.clone(),
            right_derive: self.ctx.causet.right_derive_when_split,
            callback: cb,
        };
        if let Err(Stopped(t)) = self.ctx.fidel_interlock_semaphore.schedule(task) {
            error!(
                "failed to notify fidel to split: Stopped";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            match t {
                FidelTask::AskBatchSplit { callback, .. } => {
                    callback.invoke_with_response(new_error(box_err!(
                        "{} failed to split: Stopped",
                        self.fsm.peer.tag
                    )));
                }
                _ => unreachable!(),
            }
        }
    }

    fn validate_split_brane(
        &mut self,
        epoch: &meta_timeshare::BraneEpoch,
        split_tuplespaceInstanton: &[Vec<u8>],
    ) -> Result<()> {
        if split_tuplespaceInstanton.is_empty() {
            error!(
                "no split key is specified.";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return Err(box_err!("{} no split key is specified.", self.fsm.peer.tag));
        }
        for key in split_tuplespaceInstanton {
            if key.is_empty() {
                error!(
                    "split key should not be empty!!!";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                return Err(box_err!(
                    "{} split key should not be empty",
                    self.fsm.peer.tag
                ));
            }
        }
        if !self.fsm.peer.is_leader() {
            // brane on this store is no longer leader, skipped.
            info!(
                "not leader, skip.";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return Err(Error::NotLeader(
                self.brane_id(),
                self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()),
            ));
        }

        let brane = self.fsm.peer.brane();
        let latest_epoch = brane.get_brane_epoch();

        // This is a little difference for `check_brane_epoch` in brane split case.
        // Here we just need to check `version` because `conf_ver` will be fidelio
        // to the latest value of the peer, and then lightlike to FIDel.
        if latest_epoch.get_version() != epoch.get_version() {
            info!(
                "epoch changed, retry later";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "prev_epoch" => ?brane.get_brane_epoch(),
                "epoch" => ?epoch,
            );
            return Err(Error::EpochNotMatch(
                format!(
                    "{} epoch changed {:?} != {:?}, retry later",
                    self.fsm.peer.tag, latest_epoch, epoch
                ),
                vec![brane.to_owned()],
            ));
        }
        Ok(())
    }

    fn on_approximate_brane_size(&mut self, size: u64) {
        self.fsm.peer.approximate_size = Some(size);
        self.register_split_brane_check_tick();
        self.register_fidel_heartbeat_tick();
    }

    fn on_approximate_brane_tuplespaceInstanton(&mut self, tuplespaceInstanton: u64) {
        self.fsm.peer.approximate_tuplespaceInstanton = Some(tuplespaceInstanton);
        self.register_split_brane_check_tick();
        self.register_fidel_heartbeat_tick();
    }

    fn on_compaction_declined_bytes(&mut self, declined_bytes: u64) {
        self.fsm.peer.compaction_declined_bytes += declined_bytes;
        if self.fsm.peer.compaction_declined_bytes >= self.ctx.causet.brane_split_check_diff.0 {
            fidelio_REGION_SIZE_BY_COMPACTION_COUNTER.inc();
        }
        self.register_split_brane_check_tick();
    }

    fn on_schedule_half_split_brane(
        &mut self,
        brane_epoch: &meta_timeshare::BraneEpoch,
        policy: CheckPolicy,
    ) {
        if !self.fsm.peer.is_leader() {
            // brane on this store is no longer leader, skipped.
            warn!(
                "not leader, skip";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }

        let brane = self.fsm.peer.brane();
        if util::is_epoch_stale(brane_epoch, brane.get_brane_epoch()) {
            warn!(
                "receive a stale halfsplit message";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }

        let task = SplitCheckTask::split_check(brane.clone(), false, policy);
        if let Err(e) = self.ctx.split_check_interlock_semaphore.schedule(task) {
            error!(
                "failed to schedule split check";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
    }

    fn on_fidel_heartbeat_tick(&mut self) {
        if !self.ctx.causet.hibernate_branes {
            self.register_fidel_heartbeat_tick();
        }
        self.fsm.peer.check_peers();

        if !self.fsm.peer.is_leader() {
            return;
        }
        self.fsm.peer.heartbeat_fidel(self.ctx);
        if self.ctx.causet.hibernate_branes && self.fsm.peer.replication_mode_need_catch_up() {
            self.register_fidel_heartbeat_tick();
        }
    }

    fn register_fidel_heartbeat_tick(&mut self) {
        self.schedule_tick(PeerTicks::FIDel_HEARTBEAT)
    }

    fn on_check_peer_stale_state_tick(&mut self) {
        if self.fsm.peer.plightlikeing_remove {
            return;
        }

        self.register_check_peer_stale_state_tick();

        if self.fsm.peer.is_applying_snapshot() || self.fsm.peer.has_plightlikeing_snapshot() {
            return;
        }

        if self.ctx.causet.hibernate_branes {
            if self.fsm.group_state == GroupState::Idle {
                self.fsm.peer.ping();
                if !self.fsm.peer.is_leader() {
                    // If leader is able to receive messge but can't lightlike out any,
                    // follower should be able to spacelike an election.
                    self.fsm.group_state = GroupState::PreChaos;
                } else {
                    self.fsm.has_ready = true;
                    // Schedule a fidel heartbeat to discover down and plightlikeing peer when
                    // hibernate_branes is enabled.
                    self.register_fidel_heartbeat_tick();
                }
            } else if self.fsm.group_state == GroupState::PreChaos {
                self.fsm.group_state = GroupState::Chaos;
            } else if self.fsm.group_state == GroupState::Chaos {
                // Register tick if it's not yet. Only when it fails to receive ping from leader
                // after two stale check can a follower actually tick.
                self.register_violetabft_base_tick();
            }
        }

        // If this peer detects the leader is missing for a long long time,
        // it should consider itself as a stale peer which is removed from
        // the original cluster.
        // This most likely happens in the following scenario:
        // At first, there are three peer A, B, C in the cluster, and A is leader.
        // Peer B gets down. And then A adds D, E, F into the cluster.
        // Peer D becomes leader of the new cluster, and then removes peer A, B, C.
        // After all these peer in and out, now the cluster has peer D, E, F.
        // If peer B goes up at this moment, it still thinks it is one of the cluster
        // and has peers A, C. However, it could not reach A, C since they are removed
        // from the cluster or probably destroyed.
        // Meantime, D, E, F would not reach B, since it's not in the cluster anymore.
        // In this case, peer B would notice that the leader is missing for a long time,
        // and it would check with fidel to confirm whether it's still a member of the cluster.
        // If not, it destroys itself as a stale peer which is removed out already.
        let state = self.fsm.peer.check_stale_state(self.ctx);
        fail_point!("peer_check_stale_state", state != StaleState::Valid, |_| {});
        match state {
            StaleState::Valid => (),
            StaleState::LeaderMissing => {
                warn!(
                    "leader missing longer than abnormal_leader_missing_duration";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "expect" => %self.ctx.causet.abnormal_leader_missing_duration,
                );
                self.ctx
                    .violetabft_metrics
                    .leader_missing
                    .dagger()
                    .unwrap()
                    .insert(self.brane_id());
            }
            StaleState::ToValidate => {
                // for peer B in case 1 above
                warn!(
                    "leader missing longer than max_leader_missing_duration. \
                     To check with fidel and other peers whether it's still valid";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "expect" => %self.ctx.causet.max_leader_missing_duration,
                );

                self.fsm.peer.bcast_check_stale_peer_message(&mut self.ctx);

                let task = FidelTask::ValidatePeer {
                    peer: self.fsm.peer.peer.clone(),
                    brane: self.fsm.peer.brane().clone(),
                };
                if let Err(e) = self.ctx.fidel_interlock_semaphore.schedule(task) {
                    error!(
                        "failed to notify fidel";
                        "brane_id" => self.fsm.brane_id(),
                        "peer_id" => self.fsm.peer_id(),
                        "err" => %e,
                    )
                }
            }
        }
    }

    fn register_check_peer_stale_state_tick(&mut self) {
        self.schedule_tick(PeerTicks::CHECK_PEER_STALE_STATE)
    }
}

impl<'a, EK, ER, T: Transport, C: FidelClient> PeerFsmpushdown_causet<'a, EK, ER, T, C>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    fn on_ready_compute_hash(
        &mut self,
        brane: meta_timeshare::Brane,
        index: u64,
        context: Vec<u8>,
        snap: EK::Snapshot,
    ) {
        self.fsm.peer.consistency_state.last_check_time = Instant::now();
        let task = ConsistencyCheckTask::compute_hash(brane, index, context, snap);
        info!(
            "schedule compute hash task";
            "brane_id" => self.fsm.brane_id(),
            "peer_id" => self.fsm.peer_id(),
            "task" => %task,
        );
        if let Err(e) = self.ctx.consistency_check_interlock_semaphore.schedule(task) {
            error!(
                "schedule failed";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
    }

    fn on_ready_verify_hash(
        &mut self,
        expected_index: u64,
        context: Vec<u8>,
        expected_hash: Vec<u8>,
    ) {
        self.verify_and_store_hash(expected_index, context, expected_hash);
    }

    fn on_hash_computed(&mut self, index: u64, context: Vec<u8>, hash: Vec<u8>) {
        if !self.verify_and_store_hash(index, context, hash) {
            return;
        }

        let req = new_verify_hash_request(
            self.brane_id(),
            self.fsm.peer.peer.clone(),
            &self.fsm.peer.consistency_state,
        );
        self.propose_violetabft_command(req, Callback::None);
    }

    fn on_ingest_sst_result(&mut self, ssts: Vec<SstMeta>) {
        for sst in &ssts {
            self.fsm.peer.size_diff_hint += sst.get_length();
        }
        self.register_split_brane_check_tick();

        let task = CleanupSSTTask::DeleteSST { ssts };
        if let Err(e) = self
            .ctx
            .cleanup_interlock_semaphore
            .schedule(CleanupTask::CleanupSST(task))
        {
            error!(
                "schedule to delete ssts";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
    }

    /// Verify and store the hash to state. return true means the hash has been stored successfully.
    // TODO: Consider context in the function.
    fn verify_and_store_hash(
        &mut self,
        expected_index: u64,
        _context: Vec<u8>,
        expected_hash: Vec<u8>,
    ) -> bool {
        if expected_index < self.fsm.peer.consistency_state.index {
            REGION_HASH_COUNTER.verify.miss.inc();
            warn!(
                "has scheduled a new hash, skip.";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "index" => self.fsm.peer.consistency_state.index,
                "expected_index" => expected_index,
            );
            return false;
        }
        if self.fsm.peer.consistency_state.index == expected_index {
            if self.fsm.peer.consistency_state.hash.is_empty() {
                warn!(
                    "duplicated consistency check detected, skip.";
                    "brane_id" => self.fsm.brane_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                return false;
            }
            if self.fsm.peer.consistency_state.hash != expected_hash {
                panic!(
                    "{} hash at {} not correct, want \"{}\", got \"{}\"!!!",
                    self.fsm.peer.tag,
                    self.fsm.peer.consistency_state.index,
                    escape(&expected_hash),
                    escape(&self.fsm.peer.consistency_state.hash)
                );
            }
            info!(
                "consistency check pass.";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "index" => self.fsm.peer.consistency_state.index
            );
            REGION_HASH_COUNTER.verify.matched.inc();
            self.fsm.peer.consistency_state.hash = vec![];
            return false;
        }
        if self.fsm.peer.consistency_state.index != INVALID_INDEX
            && !self.fsm.peer.consistency_state.hash.is_empty()
        {
            // Maybe computing is too slow or computed result is dropped due to channel full.
            // If computing is too slow, miss count will be increased twice.
            REGION_HASH_COUNTER.verify.miss.inc();
            warn!(
                "hash belongs to wrong index, skip.";
                "brane_id" => self.fsm.brane_id(),
                "peer_id" => self.fsm.peer_id(),
                "index" => self.fsm.peer.consistency_state.index,
                "expected_index" => expected_index,
            );
        }

        info!(
            "save hash for consistency check later.";
            "brane_id" => self.fsm.brane_id(),
            "peer_id" => self.fsm.peer_id(),
            "index" => expected_index,
        );
        self.fsm.peer.consistency_state.index = expected_index;
        self.fsm.peer.consistency_state.hash = expected_hash;
        true
    }
}

/// Checks merge target, returns whether the source peer should be destroyed and whether the source peer is
/// merged to this target peer.
///
/// It returns (`can_destroy`, `merge_to_this_peer`).
///
/// `can_destroy` is true when there is a network isolation which leads to a follower of a merge target
/// Brane's log falls behind and then receive a snapshot with epoch version after merge.
///
/// `merge_to_this_peer` is true when `can_destroy` is true and the source peer is merged to this target peer.
pub fn maybe_destroy_source(
    meta: &StoreMeta,
    target_brane_id: u64,
    target_peer_id: u64,
    source_brane_id: u64,
    brane_epoch: BraneEpoch,
) -> (bool, bool) {
    if let Some(merge_targets) = meta.plightlikeing_merge_targets.get(&target_brane_id) {
        if let Some(target_brane) = merge_targets.get(&source_brane_id) {
            info!(
                "[brane {}] checking source {} epoch: {:?}, merge target epoch: {:?}",
                target_brane_id,
                source_brane_id,
                brane_epoch,
                target_brane.get_brane_epoch(),
            );
            // The target peer will move on, namely, it will apply a snapshot generated after merge,
            // so destroy source peer.
            if brane_epoch.get_version() > target_brane.get_brane_epoch().get_version() {
                return (
                    true,
                    target_peer_id
                        == util::find_peer(target_brane, meta.store_id.unwrap())
                            .unwrap()
                            .get_id(),
                );
            }
            // Wait till the target peer has caught up logs and source peer will be destroyed at that time.
            return (false, false);
        }
    }
    (false, false)
}

pub fn new_read_index_request(
    brane_id: u64,
    brane_epoch: BraneEpoch,
    peer: meta_timeshare::Peer,
) -> VioletaBftCmdRequest {
    let mut request = VioletaBftCmdRequest::default();
    request.mut_header().set_brane_id(brane_id);
    request.mut_header().set_brane_epoch(brane_epoch);
    request.mut_header().set_peer(peer);
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::ReadIndex);
    request
}

pub fn new_admin_request(brane_id: u64, peer: meta_timeshare::Peer) -> VioletaBftCmdRequest {
    let mut request = VioletaBftCmdRequest::default();
    request.mut_header().set_brane_id(brane_id);
    request.mut_header().set_peer(peer);
    request
}

fn new_verify_hash_request(
    brane_id: u64,
    peer: meta_timeshare::Peer,
    state: &ConsistencyState,
) -> VioletaBftCmdRequest {
    let mut request = new_admin_request(brane_id, peer);

    let mut admin = AdminRequest::default();
    admin.set_cmd_type(AdminCmdType::VerifyHash);
    admin.mut_verify_hash().set_index(state.index);
    admin.mut_verify_hash().set_context(state.context.clone());
    admin.mut_verify_hash().set_hash(state.hash.clone());
    request.set_admin_request(admin);
    request
}

fn new_compact_log_request(
    brane_id: u64,
    peer: meta_timeshare::Peer,
    compact_index: u64,
    compact_term: u64,
) -> VioletaBftCmdRequest {
    let mut request = new_admin_request(brane_id, peer);

    let mut admin = AdminRequest::default();
    admin.set_cmd_type(AdminCmdType::CompactLog);
    admin.mut_compact_log().set_compact_index(compact_index);
    admin.mut_compact_log().set_compact_term(compact_term);
    request.set_admin_request(admin);
    request
}

impl<'a, EK, ER, T: Transport, C: FidelClient> PeerFsmpushdown_causet<'a, EK, ER, T, C>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    // Handle status commands here, separate the logic, maybe we can move it
    // to another file later.
    // Unlike other commands (write or admin), status commands only show current
    // store status, so no need to handle it in violetabft group.
    fn execute_status_command(&mut self, request: &VioletaBftCmdRequest) -> Result<VioletaBftCmdResponse> {
        let cmd_type = request.get_status_request().get_cmd_type();

        let mut response = match cmd_type {
            StatusCmdType::BraneLeader => self.execute_brane_leader(),
            StatusCmdType::BraneDetail => self.execute_brane_detail(request),
            StatusCmdType::InvalidStatus => {
                Err(box_err!("{} invalid status command!", self.fsm.peer.tag))
            }
        }?;
        response.set_cmd_type(cmd_type);

        let mut resp = VioletaBftCmdResponse::default();
        resp.set_status_response(response);
        // Bind peer current term here.
        bind_term(&mut resp, self.fsm.peer.term());
        Ok(resp)
    }

    fn execute_brane_leader(&mut self) -> Result<StatusResponse> {
        let mut resp = StatusResponse::default();
        if let Some(leader) = self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()) {
            resp.mut_brane_leader().set_leader(leader);
        }

        Ok(resp)
    }

    fn execute_brane_detail(&mut self, request: &VioletaBftCmdRequest) -> Result<StatusResponse> {
        if !self.fsm.peer.get_store().is_initialized() {
            let brane_id = request.get_header().get_brane_id();
            return Err(Error::BraneNotInitialized(brane_id));
        }
        let mut resp = StatusResponse::default();
        resp.mut_brane_detail()
            .set_brane(self.fsm.peer.brane().clone());
        if let Some(leader) = self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()) {
            resp.mut_brane_detail().set_leader(leader);
        }

        Ok(resp)
    }
}

#[causet(test)]
mod tests {
    use super::BatchVioletaBftCmdRequestBuilder;
    use crate::store::local_metrics::VioletaBftProposeMetrics;
    use crate::store::msg::{Callback, VioletaBftCommand};

    use engine_lmdb::LmdbEngine;
    use ekvproto::violetabft_cmd_timeshare::{
        AdminRequest, CmdType, PutRequest, VioletaBftCmdRequest, VioletaBftCmdResponse, Request, Response,
        StatusRequest,
    };
    use protobuf::Message;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_batch_violetabft_cmd_request_builder() {
        let max_batch_size = 1000.0;
        let mut builder = BatchVioletaBftCmdRequestBuilder::<LmdbEngine>::new(max_batch_size);
        let mut q = Request::default();
        let mut metric = VioletaBftProposeMetrics::default();

        let mut req = VioletaBftCmdRequest::default();
        req.set_admin_request(AdminRequest::default());
        assert!(!builder.can_batch(&req, 0));

        let mut req = VioletaBftCmdRequest::default();
        req.set_status_request(StatusRequest::default());
        assert!(!builder.can_batch(&req, 0));

        let mut req = VioletaBftCmdRequest::default();
        let mut put = PutRequest::default();
        put.set_key(b"aaaa".to_vec());
        put.set_value(b"bbbb".to_vec());
        q.set_cmd_type(CmdType::Put);
        q.set_put(put);
        req.mut_requests().push(q.clone());
        let _ = q.take_put();
        let req_size = req.compute_size();
        assert!(builder.can_batch(&req, req_size));

        let mut req = VioletaBftCmdRequest::default();
        q.set_cmd_type(CmdType::Snap);
        req.mut_requests().push(q.clone());
        let mut put = PutRequest::default();
        put.set_key(b"aaaa".to_vec());
        put.set_value(b"bbbb".to_vec());
        q.set_cmd_type(CmdType::Put);
        q.set_put(put);
        req.mut_requests().push(q.clone());
        let req_size = req.compute_size();
        assert!(!builder.can_batch(&req, req_size));

        let mut req = VioletaBftCmdRequest::default();
        let mut put = PutRequest::default();
        put.set_key(b"aaaa".to_vec());
        put.set_value(vec![8 as u8; 2000]);
        q.set_cmd_type(CmdType::Put);
        q.set_put(put);
        req.mut_requests().push(q.clone());
        let req_size = req.compute_size();
        assert!(!builder.can_batch(&req, req_size));

        // Check batch callback
        let mut req = VioletaBftCmdRequest::default();
        let mut put = PutRequest::default();
        put.set_key(b"aaaa".to_vec());
        put.set_value(vec![8 as u8; 20]);
        q.set_cmd_type(CmdType::Put);
        q.set_put(put);
        req.mut_requests().push(q);
        let mut cbs_flags = vec![];
        let mut response = VioletaBftCmdResponse::default();
        for _ in 0..10 {
            let flag = Arc::new(AtomicBool::new(false));
            cbs_flags.push(flag.clone());
            let cb = Callback::Write(Box::new(move |_resp| {
                flag.store(true, Ordering::Release);
            }));
            response.mut_responses().push(Response::default());
            let cmd = VioletaBftCommand::new(req.clone(), cb);
            builder.add(cmd, 100);
        }
        let cmd = builder.build(&mut metric).unwrap();
        assert_eq!(10, cmd.request.get_requests().len());
        cmd.callback.invoke_with_response(response);
        for flag in cbs_flags {
            assert!(flag.load(Ordering::Acquire));
        }
    }
}
