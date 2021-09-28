// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::fmt;
use std::time::Instant;

use edb::{CompactedEvent, CausetEngine, Snapshot};
use ekvproto::import_sst_timeshare::SstMeta;
use ekvproto::kvrpc_timeshare::ExtraOp as TxnExtraOp;
use ekvproto::meta_timeshare;
use ekvproto::meta_timeshare::BraneEpoch;
use ekvproto::fidel_timeshare::CheckPolicy;
use ekvproto::violetabft_cmd_timeshare::{VioletaBftCmdRequest, VioletaBftCmdResponse};
use ekvproto::violetabft_server_timeshare::VioletaBftMessage;
use ekvproto::replication_mode_timeshare::ReplicationStatus;
use violetabft::SnapshotStatus;

use crate::store::fsm::apply::TaskRes as ApplyTaskRes;
use crate::store::fsm::apply::{CatchUpLogs, ChangeCmd};
use crate::store::metrics::VioletaBftEventDurationType;
use crate::store::util::TuplespaceInstantonInfoFormatter;
use crate::store::SnapKey;
use violetabftstore::interlock::::escape;

use super::{AbstractPeer, BraneSnapshot};

#[derive(Debug)]
pub struct ReadResponse<S: Snapshot> {
    pub response: VioletaBftCmdResponse,
    pub snapshot: Option<BraneSnapshot<S>>,
    pub txn_extra_op: TxnExtraOp,
}

#[derive(Debug)]
pub struct WriteResponse {
    pub response: VioletaBftCmdResponse,
}

// This is only necessary because of seeming limitations in derive(Clone) w/r/t
// generics. If it can be deleted in the future in favor of derive, it should
// be.
impl<S> Clone for ReadResponse<S>
where
    S: Snapshot,
{
    fn clone(&self) -> ReadResponse<S> {
        ReadResponse {
            response: self.response.clone(),
            snapshot: self.snapshot.clone(),
            txn_extra_op: self.txn_extra_op,
        }
    }
}

pub type ReadCallback<S> = Box<dyn FnOnce(ReadResponse<S>) + lightlike>;
pub type WriteCallback = Box<dyn FnOnce(WriteResponse) + lightlike>;

/// Variants of callbacks for `Msg`.
///  - `Read`: a callback for read only requests including `StatusRequest`,
///         `GetRequest` and `SnapRequest`
///  - `Write`: a callback for write only requests including `AdminRequest`
///          `PutRequest`, `DeleteRequest` and `DeleteConeRequest`.
pub enum Callback<S: Snapshot> {
    /// No callback.
    None,
    /// Read callback.
    Read(ReadCallback<S>),
    /// Write callback.
    Write(WriteCallback),
}

impl<S> Callback<S>
where
    S: Snapshot,
{
    pub fn invoke_with_response(self, resp: VioletaBftCmdResponse) {
        match self {
            Callback::None => (),
            Callback::Read(read) => {
                let resp = ReadResponse {
                    response: resp,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                };
                read(resp);
            }
            Callback::Write(write) => {
                let resp = WriteResponse { response: resp };
                write(resp);
            }
        }
    }

    pub fn invoke_read(self, args: ReadResponse<S>) {
        match self {
            Callback::Read(read) => read(args),
            other => panic!("expect Callback::Read(..), got {:?}", other),
        }
    }

    pub fn is_none(&self) -> bool {
        match self {
            Callback::None => true,
            _ => false,
        }
    }
}

impl<S> fmt::Debug for Callback<S>
where
    S: Snapshot,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Callback::None => write!(fmt, "Callback::None"),
            Callback::Read(_) => write!(fmt, "Callback::Read(..)"),
            Callback::Write(_) => write!(fmt, "Callback::Write(..)"),
        }
    }
}

bitflags! {
    pub struct PeerTicks: u8 {
        const VIOLETABFT                   = 0b00000001;
        const VIOLETABFT_LOG_GC            = 0b00000010;
        const SPLIT_REGION_CHECK     = 0b00000100;
        const FIDel_HEARTBEAT           = 0b00001000;
        const CHECK_MERGE            = 0b00010000;
        const CHECK_PEER_STALE_STATE = 0b00100000;
    }
}

impl PeerTicks {
    #[inline]
    pub fn tag(self) -> &'static str {
        match self {
            PeerTicks::VIOLETABFT => "violetabft",
            PeerTicks::VIOLETABFT_LOG_GC => "violetabft_log_gc",
            PeerTicks::SPLIT_REGION_CHECK => "split_brane_check",
            PeerTicks::FIDel_HEARTBEAT => "fidel_heartbeat",
            PeerTicks::CHECK_MERGE => "check_merge",
            PeerTicks::CHECK_PEER_STALE_STATE => "check_peer_stale_state",
            _ => unreachable!(),
        }
    }
    pub fn get_all_ticks() -> &'static [PeerTicks] {
        const TICKS: &[PeerTicks] = &[
            PeerTicks::VIOLETABFT,
            PeerTicks::VIOLETABFT_LOG_GC,
            PeerTicks::SPLIT_REGION_CHECK,
            PeerTicks::FIDel_HEARTBEAT,
            PeerTicks::CHECK_MERGE,
            PeerTicks::CHECK_PEER_STALE_STATE,
        ];
        TICKS
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StoreTick {
    CompactCheck,
    FidelStoreHeartbeat,
    SnapGc,
    CompactLockCf,
    ConsistencyCheck,
    CleanupImportSST,
    VioletaBftEnginePurge,
}

impl StoreTick {
    #[inline]
    pub fn tag(self) -> VioletaBftEventDurationType {
        match self {
            StoreTick::CompactCheck => VioletaBftEventDurationType::compact_check,
            StoreTick::FidelStoreHeartbeat => VioletaBftEventDurationType::fidel_store_heartbeat,
            StoreTick::SnapGc => VioletaBftEventDurationType::snap_gc,
            StoreTick::CompactLockCf => VioletaBftEventDurationType::compact_lock_causet,
            StoreTick::ConsistencyCheck => VioletaBftEventDurationType::consistency_check,
            StoreTick::CleanupImportSST => VioletaBftEventDurationType::cleanup_import_sst,
            StoreTick::VioletaBftEnginePurge => VioletaBftEventDurationType::violetabft_engine_purge,
        }
    }
}

#[derive(Debug)]
pub enum MergeResultKind {
    /// Its target peer applys `CommitMerge` log.
    FromTargetLog,
    /// Its target peer receives snapshot.
    /// In step 1, this peer should mark `plightlikeing_move` is true and destroy its apply fsm.
    /// Then its target peer will remove this peer data and apply snapshot atomically.
    FromTargetSnapshotStep1,
    /// In step 2, this peer should destroy its peer fsm.
    FromTargetSnapshotStep2,
    /// This peer is no longer needed by its target peer so it can be destroyed by itself.
    /// It happens if and only if its target peer has been removed by conf change.
    Stale,
}

/// Some significant messages sent to violetabftstore. VioletaBftstore will dispatch these messages to VioletaBft
/// groups to fidelio some important internal status.
#[derive(Debug)]
pub enum SignificantMsg<SK>
where
    SK: Snapshot,
{
    /// Reports whether the snapshot lightlikeing is successful or not.
    SnapshotStatus {
        brane_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    },
    StoreUnreachable {
        store_id: u64,
    },
    /// Reports `to_peer_id` is unreachable.
    Unreachable {
        brane_id: u64,
        to_peer_id: u64,
    },
    /// Source brane catch up logs for merging
    CatchUpLogs(CatchUpLogs),
    /// Result of the fact that the brane is merged.
    MergeResult {
        target_brane_id: u64,
        target: meta_timeshare::Peer,
        result: MergeResultKind,
    },
    StoreResolved {
        store_id: u64,
        group_id: u64,
    },
    /// Capture the changes of the brane.
    CaptureChange {
        cmd: ChangeCmd,
        brane_epoch: BraneEpoch,
        callback: Callback<SK>,
    },
    LeaderCallback(Callback<SK>),
}

/// Message that will be sent to a peer.
///
/// These messages are not significant and can be dropped occasionally.
pub enum CasualMessage<EK: CausetEngine> {
    /// Split the target brane into several partitions.
    SplitBrane {
        brane_epoch: BraneEpoch,
        // It's an encoded key.
        // TODO: support meta key.
        split_tuplespaceInstanton: Vec<Vec<u8>>,
        callback: Callback<EK::Snapshot>,
    },

    /// Hash result of ComputeHash command.
    ComputeHashResult {
        index: u64,
        context: Vec<u8>,
        hash: Vec<u8>,
    },

    /// Approximate size of target brane.
    BraneApproximateSize {
        size: u64,
    },

    /// Approximate key count of target brane.
    BraneApproximateTuplespaceInstanton {
        tuplespaceInstanton: u64,
    },
    CompactionDeclinedBytes {
        bytes: u64,
    },
    /// Half split the target brane.
    HalfSplitBrane {
        brane_epoch: BraneEpoch,
        policy: CheckPolicy,
    },
    /// Remove snapshot files in `snaps`.
    GcSnap {
        snaps: Vec<(SnapKey, bool)>,
    },
    /// Clear brane size cache.
    ClearBraneSize,
    /// Indicate a target brane is overlapped.
    BraneOverlapped,
    /// Notifies that a new snapshot has been generated.
    SnapshotGenerated,

    /// Generally VioletaBft leader keeps as more as possible logs for followers,
    /// however `ForceCompactVioletaBftLogs` only cares the leader itself.
    ForceCompactVioletaBftLogs,

    /// A message to access peer's internal state.
    AccessPeer(Box<dyn FnOnce(&mut dyn AbstractPeer) + lightlike + 'static>),
}

impl<EK: CausetEngine> fmt::Debug for CasualMessage<EK> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CasualMessage::ComputeHashResult {
                index,
                context,
                ref hash,
            } => write!(
                fmt,
                "ComputeHashResult [index: {}, context: {}, hash: {}]",
                index,
                hex::encode_upper(&context),
                escape(hash)
            ),
            CasualMessage::SplitBrane { ref split_tuplespaceInstanton, .. } => write!(
                fmt,
                "Split brane with {}",
                TuplespaceInstantonInfoFormatter(split_tuplespaceInstanton.iter())
            ),
            CasualMessage::BraneApproximateSize { size } => {
                write!(fmt, "Brane's approximate size [size: {:?}]", size)
            }
            CasualMessage::BraneApproximateTuplespaceInstanton { tuplespaceInstanton } => {
                write!(fmt, "Brane's approximate tuplespaceInstanton [tuplespaceInstanton: {:?}]", tuplespaceInstanton)
            }
            CasualMessage::CompactionDeclinedBytes { bytes } => {
                write!(fmt, "compaction declined bytes {}", bytes)
            }
            CasualMessage::HalfSplitBrane { .. } => write!(fmt, "Half Split"),
            CasualMessage::GcSnap { ref snaps } => write! {
                fmt,
                "gc snaps {:?}",
                snaps
            },
            CasualMessage::ClearBraneSize => write! {
                fmt,
                "clear brane size"
            },
            CasualMessage::BraneOverlapped => write!(fmt, "BraneOverlapped"),
            CasualMessage::SnapshotGenerated => write!(fmt, "SnapshotGenerated"),
            CasualMessage::ForceCompactVioletaBftLogs => write!(fmt, "ForceCompactVioletaBftLogs"),
            CasualMessage::AccessPeer(_) => write!(fmt, "AccessPeer"),
        }
    }
}

/// VioletaBft command is the command that is expected to be proposed by the
/// leader of the target violetabft group.
#[derive(Debug)]
pub struct VioletaBftCommand<S: Snapshot> {
    pub lightlike_time: Instant,
    pub request: VioletaBftCmdRequest,
    pub callback: Callback<S>,
}

impl<S: Snapshot> VioletaBftCommand<S> {
    #[inline]
    pub fn new(request: VioletaBftCmdRequest, callback: Callback<S>) -> VioletaBftCommand<S> {
        VioletaBftCommand {
            request,
            callback,
            lightlike_time: Instant::now(),
        }
    }
}

/// Message that can be sent to a peer.
pub enum PeerMsg<EK: CausetEngine> {
    /// VioletaBft message is the message sent between violetabft nodes in the same
    /// violetabft group. Messages need to be redirected to violetabftstore if target
    /// peer doesn't exist.
    VioletaBftMessage(VioletaBftMessage),
    /// VioletaBft command is the command that is expected to be proposed by the
    /// leader of the target violetabft group. If it's failed to be sent, callback
    /// usually needs to be called before dropping in case of resource leak.
    VioletaBftCommand(VioletaBftCommand<EK::Snapshot>),
    /// Tick is periodical task. If target peer doesn't exist there is a potential
    /// that the violetabft node will not work anymore.
    Tick(PeerTicks),
    /// Result of applying committed entries. The message can't be lost.
    ApplyRes { res: ApplyTaskRes<EK::Snapshot> },
    /// Message that can't be lost but rarely created. If they are lost, real bad
    /// things happen like some peers will be considered dead in the group.
    SignificantMsg(SignificantMsg<EK::Snapshot>),
    /// Start the FSM.
    Start,
    /// A message only used to notify a peer.
    Noop,
    /// Message that is not important and can be dropped occasionally.
    CasualMessage(CasualMessage<EK>),
    /// Ask brane to report a heartbeat to FIDel.
    HeartbeatFidel,
    /// Asks brane to change replication mode.
    fidelioReplicationMode,
}

impl<EK: CausetEngine> fmt::Debug for PeerMsg<EK> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerMsg::VioletaBftMessage(_) => write!(fmt, "VioletaBft Message"),
            PeerMsg::VioletaBftCommand(_) => write!(fmt, "VioletaBft Command"),
            PeerMsg::Tick(tick) => write! {
                fmt,
                "{:?}",
                tick
            },
            PeerMsg::SignificantMsg(msg) => write!(fmt, "{:?}", msg),
            PeerMsg::ApplyRes { res } => write!(fmt, "ApplyRes {:?}", res),
            PeerMsg::Start => write!(fmt, "Startup"),
            PeerMsg::Noop => write!(fmt, "Noop"),
            PeerMsg::CasualMessage(msg) => write!(fmt, "CasualMessage {:?}", msg),
            PeerMsg::HeartbeatFidel => write!(fmt, "HeartbeatFidel"),
            PeerMsg::fidelioReplicationMode => write!(fmt, "fidelioReplicationMode"),
        }
    }
}

pub enum StoreMsg<EK>
where
    EK: CausetEngine,
{
    VioletaBftMessage(VioletaBftMessage),

    ValidateSSTResult {
        invalid_ssts: Vec<SstMeta>,
    },

    // Clear brane size and tuplespaceInstanton for all branes in the cone, so we can force them to re-calculate
    // their size later.
    ClearBraneSizeInCone {
        spacelike_key: Vec<u8>,
        lightlike_key: Vec<u8>,
    },
    StoreUnreachable {
        store_id: u64,
    },

    // Compaction finished event
    CompactedEvent(EK::CompactedEvent),
    Tick(StoreTick),
    Start {
        store: meta_timeshare::CausetStore,
    },

    /// Message only used for test.
    #[causet(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&crate::store::Config) + lightlike>),
    /// Asks the store to fidelio replication mode.
    fidelioReplicationMode(ReplicationStatus),
}

impl<EK> fmt::Debug for StoreMsg<EK>
where
    EK: CausetEngine,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            StoreMsg::VioletaBftMessage(_) => write!(fmt, "VioletaBft Message"),
            StoreMsg::StoreUnreachable { store_id } => {
                write!(fmt, "CausetStore {}  is unreachable", store_id)
            }
            StoreMsg::CompactedEvent(ref event) => write!(fmt, "CompactedEvent causet {}", event.causet()),
            StoreMsg::ValidateSSTResult { .. } => write!(fmt, "Validate SST Result"),
            StoreMsg::ClearBraneSizeInCone {
                ref spacelike_key,
                ref lightlike_key,
            } => write!(
                fmt,
                "Clear Brane size in cone {:?} to {:?}",
                spacelike_key, lightlike_key
            ),
            StoreMsg::Tick(tick) => write!(fmt, "StoreTick {:?}", tick),
            StoreMsg::Start { ref store } => write!(fmt, "Start store {:?}", store),
            #[causet(any(test, feature = "testexport"))]
            StoreMsg::Validate(_) => write!(fmt, "Validate config"),
            StoreMsg::fidelioReplicationMode(_) => write!(fmt, "fidelioReplicationMode"),
        }
    }
}
