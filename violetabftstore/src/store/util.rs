// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::option::Option;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::{fmt, u64};

use engine_lmdb::{set_perf_level, PerfContext, PerfLevel};
use ekvproto::kvrpc_timeshare::KeyCone;
use ekvproto::meta_timeshare::{self, PeerRole};
use ekvproto::violetabft_cmd_timeshare::{AdminCmdType, ChangePeerRequest, ChangePeerV2Request, VioletaBftCmdRequest};
use protobuf::{self, Message};
use violetabft::evioletabft_timeshare::{self, ConfChangeType, ConfState, MessageType};
use violetabft::INVALID_INDEX;
use violetabft_proto::ConfChangeI;
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::time::monotonic_raw_now;
use time::{Duration, Timespec};

use super::peer_causet_storage;
use crate::{Error, Result};
use violetabftstore::interlock::::Either;

pub fn find_peer(brane: &meta_timeshare::Brane, store_id: u64) -> Option<&meta_timeshare::Peer> {
    brane
        .get_peers()
        .iter()
        .find(|&p| p.get_store_id() == store_id)
}

pub fn find_peer_mut(brane: &mut meta_timeshare::Brane, store_id: u64) -> Option<&mut meta_timeshare::Peer> {
    brane
        .mut_peers()
        .iter_mut()
        .find(|p| p.get_store_id() == store_id)
}

pub fn remove_peer(brane: &mut meta_timeshare::Brane, store_id: u64) -> Option<meta_timeshare::Peer> {
    brane
        .get_peers()
        .iter()
        .position(|x| x.get_store_id() == store_id)
        .map(|i| brane.mut_peers().remove(i))
}

// a helper function to create peer easily.
pub fn new_peer(store_id: u64, peer_id: u64) -> meta_timeshare::Peer {
    let mut peer = meta_timeshare::Peer::default();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer.set_role(meta_timeshare::PeerRole::Voter);
    peer
}

// a helper function to create learner peer easily.
pub fn new_learner_peer(store_id: u64, peer_id: u64) -> meta_timeshare::Peer {
    let mut peer = meta_timeshare::Peer::default();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer.set_role(meta_timeshare::PeerRole::Learner);
    peer
}

/// Check if key in brane cone (`spacelike_key`, `lightlike_key`).
pub fn check_key_in_brane_exclusive(key: &[u8], brane: &meta_timeshare::Brane) -> Result<()> {
    let lightlike_key = brane.get_lightlike_key();
    let spacelike_key = brane.get_spacelike_key();
    if spacelike_key < key && (key < lightlike_key || lightlike_key.is_empty()) {
        Ok(())
    } else {
        Err(Error::KeyNotInBrane(key.to_vec(), brane.clone()))
    }
}

/// Check if key in brane cone [`spacelike_key`, `lightlike_key`].
pub fn check_key_in_brane_inclusive(key: &[u8], brane: &meta_timeshare::Brane) -> Result<()> {
    let lightlike_key = brane.get_lightlike_key();
    let spacelike_key = brane.get_spacelike_key();
    if key >= spacelike_key && (lightlike_key.is_empty() || key <= lightlike_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInBrane(key.to_vec(), brane.clone()))
    }
}

/// Check if key in brane cone [`spacelike_key`, `lightlike_key`).
pub fn check_key_in_brane(key: &[u8], brane: &meta_timeshare::Brane) -> Result<()> {
    let lightlike_key = brane.get_lightlike_key();
    let spacelike_key = brane.get_spacelike_key();
    if key >= spacelike_key && (lightlike_key.is_empty() || key < lightlike_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInBrane(key.to_vec(), brane.clone()))
    }
}

/// `is_first_vote_msg` checks `msg` is the first vote (or prevote) message or not. It's used for
/// when the message is received but there is no such brane in `CausetStore::brane_peers` and the
/// brane overlaps with others. In this case we should put `msg` into `plightlikeing_votes` instead of
/// create the peer.
#[inline]
pub fn is_first_vote_msg(msg: &evioletabft_timeshare::Message) -> bool {
    match msg.get_msg_type() {
        MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
            msg.get_term() == peer_causet_storage::VIOLETABFT_INIT_LOG_TERM + 1
        }
        _ => false,
    }
}

#[inline]
pub fn is_vote_msg(msg: &evioletabft_timeshare::Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote || msg_type == MessageType::MsgRequestPreVote
}

/// `is_initial_msg` checks whether the `msg` can be used to initialize a new peer or not.
// There could be two cases:
// 1. Target peer already exists but has not established communication with leader yet
// 2. Target peer is added newly due to member change or brane split, but it's not
//    created yet
// For both cases the brane spacelike key and lightlike key are attached in RequestVote and
// Heartbeat message for the store of that peer to check whether to create a new peer
// when receiving these messages, or just to wait for a plightlikeing brane split to perform
// later.
#[inline]
pub fn is_initial_msg(msg: &evioletabft_timeshare::Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        // the peer has not been knownCauset to this leader, it may exist or not.
        || (msg_type == MessageType::MsgHeartbeat && msg.get_commit() == INVALID_INDEX)
}

const STR_CONF_CHANGE_ADD_NODE: &str = "AddNode";
const STR_CONF_CHANGE_REMOVE_NODE: &str = "RemoveNode";
const STR_CONF_CHANGE_ADDLEARNER_NODE: &str = "AddLearner";

pub fn conf_change_type_str(conf_type: evioletabft_timeshare::ConfChangeType) -> &'static str {
    match conf_type {
        ConfChangeType::AddNode => STR_CONF_CHANGE_ADD_NODE,
        ConfChangeType::RemoveNode => STR_CONF_CHANGE_REMOVE_NODE,
        ConfChangeType::AddLearnerNode => STR_CONF_CHANGE_ADDLEARNER_NODE,
    }
}

// check whether epoch is staler than check_epoch.
pub fn is_epoch_stale(epoch: &meta_timeshare::BraneEpoch, check_epoch: &meta_timeshare::BraneEpoch) -> bool {
    epoch.get_version() < check_epoch.get_version()
        || epoch.get_conf_ver() < check_epoch.get_conf_ver()
}

#[derive(Debug, Copy, Clone)]
pub struct AdminCmdEpochState {
    pub check_ver: bool,
    pub check_conf_ver: bool,
    pub change_ver: bool,
    pub change_conf_ver: bool,
}

impl AdminCmdEpochState {
    fn new(
        check_ver: bool,
        check_conf_ver: bool,
        change_ver: bool,
        change_conf_ver: bool,
    ) -> AdminCmdEpochState {
        AdminCmdEpochState {
            check_ver,
            check_conf_ver,
            change_ver,
            change_conf_ver,
        }
    }
}

lazy_static! {
    /// WARNING: the existing settings in `ADMIN_CMD_EPOCH_MAP` **MUST NOT** be changed!!!
    /// Changing any admin cmd's `AdminCmdEpochState` or the epoch-change behavior during applying
    /// will break upgrade compatibility and correctness deplightlikeency of `CmdEpochChecker`.
    /// Please remember it is very difficult to fix the issues arising from not following this rule.
    ///
    /// If you really want to change an admin cmd behavior, please add a new admin cmd and **do not**
    /// delete the old one.
    pub static ref ADMIN_CMD_EPOCH_MAP: HashMap<AdminCmdType, AdminCmdEpochState> = [
        (AdminCmdType::InvalidAdmin, AdminCmdEpochState::new(false, false, false, false)),
        (AdminCmdType::CompactLog, AdminCmdEpochState::new(false, false, false, false)),
        (AdminCmdType::ComputeHash, AdminCmdEpochState::new(false, false, false, false)),
        (AdminCmdType::VerifyHash, AdminCmdEpochState::new(false, false, false, false)),
        // Change peer
        (AdminCmdType::ChangePeer, AdminCmdEpochState::new(false, true, false, true)),
        (AdminCmdType::ChangePeerV2, AdminCmdEpochState::new(false, true, false, true)),
        // Split
        (AdminCmdType::Split, AdminCmdEpochState::new(true, true, true, false)),
        (AdminCmdType::BatchSplit, AdminCmdEpochState::new(true, true, true, false)),
        // Merge
        (AdminCmdType::PrepareMerge, AdminCmdEpochState::new(true, true, true, true)),
        (AdminCmdType::CommitMerge, AdminCmdEpochState::new(true, true, true, false)),
        (AdminCmdType::RollbackMerge, AdminCmdEpochState::new(true, true, true, false)),
        // Transfer leader
        (AdminCmdType::TransferLeader, AdminCmdEpochState::new(true, true, false, false)),
    ].iter().copied().collect();
}

/// WARNING: `NORMAL_REQ_CHECK_VER` and `NORMAL_REQ_CHECK_CONF_VER` **MUST NOT** be changed.
/// The reason is the same as `ADMIN_CMD_EPOCH_MAP`.
pub static NORMAL_REQ_CHECK_VER: bool = true;
pub static NORMAL_REQ_CHECK_CONF_VER: bool = false;

pub fn check_brane_epoch(
    req: &VioletaBftCmdRequest,
    brane: &meta_timeshare::Brane,
    include_brane: bool,
) -> Result<()> {
    let (check_ver, check_conf_ver) = if !req.has_admin_request() {
        // for get/set/delete, we don't care conf_version.
        (NORMAL_REQ_CHECK_VER, NORMAL_REQ_CHECK_CONF_VER)
    } else {
        let epoch_state = *ADMIN_CMD_EPOCH_MAP
            .get(&req.get_admin_request().get_cmd_type())
            .unwrap();
        (epoch_state.check_ver, epoch_state.check_conf_ver)
    };

    if !check_ver && !check_conf_ver {
        return Ok(());
    }

    if !req.get_header().has_brane_epoch() {
        return Err(box_err!("missing epoch!"));
    }

    let from_epoch = req.get_header().get_brane_epoch();
    compare_brane_epoch(
        from_epoch,
        brane,
        check_conf_ver,
        check_ver,
        include_brane,
    )
}

pub fn compare_brane_epoch(
    from_epoch: &meta_timeshare::BraneEpoch,
    brane: &meta_timeshare::Brane,
    check_conf_ver: bool,
    check_ver: bool,
    include_brane: bool,
) -> Result<()> {
    // We must check epochs strictly to avoid key not in brane error.
    //
    // A 3 nodes EinsteinDB cluster with merge enabled, after commit merge, EinsteinDB A
    // tells MilevaDB with a epoch not match error contains the latest target Brane
    // info, MilevaDB fidelios its brane cache and lightlikes requests to EinsteinDB B,
    // and EinsteinDB B has not applied commit merge yet, since the brane epoch in
    // request is higher than EinsteinDB B, the request must be denied due to epoch
    // not match, so it does not read on a stale snapshot, thus avoid the
    // KeyNotInBrane error.
    let current_epoch = brane.get_brane_epoch();
    if (check_conf_ver && from_epoch.get_conf_ver() != current_epoch.get_conf_ver())
        || (check_ver && from_epoch.get_version() != current_epoch.get_version())
    {
        debug!(
            "epoch not match";
            "brane_id" => brane.get_id(),
            "from_epoch" => ?from_epoch,
            "current_epoch" => ?current_epoch,
        );
        let branes = if include_brane {
            vec![brane.to_owned()]
        } else {
            vec![]
        };
        return Err(Error::EpochNotMatch(
            format!(
                "current epoch of brane {} is {:?}, but you \
                 sent {:?}",
                brane.get_id(),
                current_epoch,
                from_epoch
            ),
            branes,
        ));
    }

    Ok(())
}

#[inline]
pub fn check_store_id(req: &VioletaBftCmdRequest, store_id: u64) -> Result<()> {
    let peer = req.get_header().get_peer();
    if peer.get_store_id() == store_id {
        Ok(())
    } else {
        Err(Error::StoreNotMatch(peer.get_store_id(), store_id))
    }
}

#[inline]
pub fn check_term(req: &VioletaBftCmdRequest, term: u64) -> Result<()> {
    let header = req.get_header();
    if header.get_term() == 0 || term <= header.get_term() + 1 {
        Ok(())
    } else {
        // If header's term is 2 verions behind current term,
        // leadership may have been changed away.
        Err(Error::StaleCommand)
    }
}

#[inline]
pub fn check_peer_id(req: &VioletaBftCmdRequest, peer_id: u64) -> Result<()> {
    let header = req.get_header();
    if header.get_peer().get_id() == peer_id {
        Ok(())
    } else {
        Err(box_err!(
            "mismatch peer id {} != {}",
            header.get_peer().get_id(),
            peer_id
        ))
    }
}

#[inline]
pub fn build_key_cone(spacelike_key: &[u8], lightlike_key: &[u8], reverse_scan: bool) -> KeyCone {
    let mut cone = KeyCone::default();
    if reverse_scan {
        cone.set_spacelike_key(lightlike_key.to_vec());
        cone.set_lightlike_key(spacelike_key.to_vec());
    } else {
        cone.set_spacelike_key(spacelike_key.to_vec());
        cone.set_lightlike_key(lightlike_key.to_vec());
    }
    cone
}

/// Check if replicas of two branes are on the same stores.
pub fn brane_on_same_stores(lhs: &meta_timeshare::Brane, rhs: &meta_timeshare::Brane) -> bool {
    if lhs.get_peers().len() != rhs.get_peers().len() {
        return false;
    }

    // Because every store can only have one replica for the same brane,
    // so just one round check is enough.
    lhs.get_peers().iter().all(|lp| {
        rhs.get_peers()
            .iter()
            .any(|rp| rp.get_store_id() == lp.get_store_id() && rp.get_role() == lp.get_role())
    })
}

#[inline]
pub fn is_brane_initialized(r: &meta_timeshare::Brane) -> bool {
    !r.get_peers().is_empty()
}

/// Lease records an expired time, for examining the current moment is in lease or not.
/// It's dedicated to the VioletaBft leader lease mechanism, contains either state of
///   1. Suspect Timestamp
///      A suspicious leader lease timestamp, which marks the leader may still hold or lose
///      its lease until the clock time goes over this timestamp.
///   2. Valid Timestamp
///      A valid leader lease timestamp, which marks the leader holds the lease for now.
///      The lease is valid until the clock time goes over this timestamp.
///
/// ```text
/// Time
/// |---------------------------------->
///         ^               ^
///        Now           Suspect TS
/// State:  |    Suspect    |   Suspect
///
/// |---------------------------------->
///         ^               ^
///        Now           Valid TS
/// State:  |     Valid     |   Expired
/// ```
///
/// Note:
///   - Valid timestamp would increase when violetabft log entries are applied in current term.
///   - Suspect timestamp would be set after the message `MsgTimeoutNow` is sent by current peer.
///     The message `MsgTimeoutNow` spacelikes a leader transfer procedure. During this procedure,
///     current peer as an old leader may still hold its lease or lose it.
///     It's possible there is a new leader elected and current peer as an old leader
///     doesn't step down due to network partition from the new leader. In that case,
///     current peer lose its leader lease.
///     Within this suspect leader lease expire time, read requests could not be performed
///     locally.
///   - The valid leader lease should be `lease = max_lease - (commit_ts - lightlike_ts)`
///     And the expired timestamp for that leader lease is `commit_ts + lease`,
///     which is `lightlike_ts + max_lease` in short.
pub struct Lease {
    // A suspect timestamp is in the Either::Left(_),
    // a valid timestamp is in the Either::Right(_).
    bound: Option<Either<Timespec, Timespec>>,
    max_lease: Duration,

    max_drift: Duration,
    last_fidelio: Timespec,
    remote: Option<RemoteLease>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LeaseState {
    /// The lease is suspicious, may be invalid.
    Suspect,
    /// The lease is valid.
    Valid,
    /// The lease is expired.
    Expired,
}

impl Lease {
    pub fn new(max_lease: Duration) -> Lease {
        Lease {
            bound: None,
            max_lease,

            max_drift: max_lease / 3,
            last_fidelio: Timespec::new(0, 0),
            remote: None,
        }
    }

    /// The valid leader lease should be `lease = max_lease - (commit_ts - lightlike_ts)`
    /// And the expired timestamp for that leader lease is `commit_ts + lease`,
    /// which is `lightlike_ts + max_lease` in short.
    fn next_expired_time(&self, lightlike_ts: Timespec) -> Timespec {
        lightlike_ts + self.max_lease
    }

    /// Renew the lease to the bound.
    pub fn renew(&mut self, lightlike_ts: Timespec) {
        let bound = self.next_expired_time(lightlike_ts);
        match self.bound {
            // Longer than suspect ts or longer than valid ts.
            Some(Either::Left(ts)) | Some(Either::Right(ts)) => {
                if ts <= bound {
                    self.bound = Some(Either::Right(bound));
                }
            }
            // Or an empty lease
            None => {
                self.bound = Some(Either::Right(bound));
            }
        }
        // Renew remote if it's valid.
        if let Some(Either::Right(bound)) = self.bound {
            if bound - self.last_fidelio > self.max_drift {
                self.last_fidelio = bound;
                if let Some(ref r) = self.remote {
                    r.renew(bound);
                }
            }
        }
    }

    /// Suspect the lease to the bound.
    pub fn suspect(&mut self, lightlike_ts: Timespec) {
        self.expire_remote_lease();
        let bound = self.next_expired_time(lightlike_ts);
        self.bound = Some(Either::Left(bound));
    }

    /// Inspect the lease state for the ts or now.
    pub fn inspect(&self, ts: Option<Timespec>) -> LeaseState {
        match self.bound {
            Some(Either::Left(_)) => LeaseState::Suspect,
            Some(Either::Right(bound)) => {
                if ts.unwrap_or_else(monotonic_raw_now) < bound {
                    LeaseState::Valid
                } else {
                    LeaseState::Expired
                }
            }
            None => LeaseState::Expired,
        }
    }

    pub fn expire(&mut self) {
        self.expire_remote_lease();
        self.bound = None;
    }

    pub fn expire_remote_lease(&mut self) {
        // Expire remote lease if there is any.
        if let Some(r) = self.remote.take() {
            r.expire();
        }
    }

    /// Return a new `RemoteLease` if there is none.
    pub fn maybe_new_remote_lease(&mut self, term: u64) -> Option<RemoteLease> {
        if let Some(ref remote) = self.remote {
            if remote.term() == term {
                // At most one connected RemoteLease in the same term.
                return None;
            } else {
                // Term has changed. It is unreachable in the current implementation,
                // because we expire remote lease when leaders step down.
                unreachable!("Must expire the old remote lease first!");
            }
        }
        let expired_time = match self.bound {
            Some(Either::Right(ts)) => timespec_to_u64(ts),
            _ => 0,
        };
        let remote = RemoteLease {
            expired_time: Arc::new(AtomicU64::new(expired_time)),
            term,
        };
        // Clone the remote.
        let remote_clone = remote.clone();
        self.remote = Some(remote);
        Some(remote_clone)
    }
}

impl fmt::Debug for Lease {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fmter = fmt.debug_struct("Lease");
        match self.bound {
            Some(Either::Left(ts)) => fmter.field("suspect", &ts).finish(),
            Some(Either::Right(ts)) => fmter.field("valid", &ts).finish(),
            None => fmter.finish(),
        }
    }
}

/// A remote lease, it can only be derived by `Lease`. It will be sent
/// to the local read thread, so name it remote. If Lease expires, the remote must
/// expire too.
#[derive(Clone)]
pub struct RemoteLease {
    expired_time: Arc<AtomicU64>,
    term: u64,
}

impl RemoteLease {
    pub fn inspect(&self, ts: Option<Timespec>) -> LeaseState {
        let expired_time = self.expired_time.load(AtomicOrdering::Acquire);
        if ts.unwrap_or_else(monotonic_raw_now) < u64_to_timespec(expired_time) {
            LeaseState::Valid
        } else {
            LeaseState::Expired
        }
    }

    fn renew(&self, bound: Timespec) {
        self.expired_time
            .store(timespec_to_u64(bound), AtomicOrdering::Release);
    }

    fn expire(&self) {
        self.expired_time.store(0, AtomicOrdering::Release);
    }

    pub fn term(&self) -> u64 {
        self.term
    }
}

impl fmt::Debug for RemoteLease {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("RemoteLease")
            .field(
                "expired_time",
                &u64_to_timespec(self.expired_time.load(AtomicOrdering::Relaxed)),
            )
            .field("term", &self.term)
            .finish()
    }
}

// Contants used in `timespec_to_u64` and `u64_to_timespec`.
const NSEC_PER_MSEC: i32 = 1_000_000;
const TIMESPEC_NSEC_SHIFT: usize = 32 - NSEC_PER_MSEC.leading_zeros() as usize;

const MSEC_PER_SEC: i64 = 1_000;
const TIMESPEC_SEC_SHIFT: usize = 64 - MSEC_PER_SEC.leading_zeros() as usize;

const TIMESPEC_NSEC_MASK: u64 = (1 << TIMESPEC_SEC_SHIFT) - 1;

/// Convert Timespec to u64. It's millisecond precision and
/// covers a cone of about 571232829 years in total.
///
/// # Panics
///
/// If Timespecs have negative sec.
#[inline]
fn timespec_to_u64(ts: Timespec) -> u64 {
    // > Darwin's and Linux's struct timespec functions handle pre-
    // > epoch timestamps using a "two steps back, one step forward" representation,
    // > though the man pages do not actually document this. For example, the time
    // > -1.2 seconds before the epoch is represented by `Timespec { sec: -2_i64,
    // > nsec: 800_000_000 }`.
    //
    // Quote from crate time,
    //   https://github.com/rust-lang-deprecated/time/blob/
    //   e313afbd9aad2ba7035a23754b5d47105988789d/src/lib.rs#L77
    assert!(ts.sec >= 0 && ts.sec < (1i64 << (64 - TIMESPEC_SEC_SHIFT)));
    assert!(ts.nsec >= 0);

    // Round down to millisecond precision.
    let ms = ts.nsec >> TIMESPEC_NSEC_SHIFT;
    let sec = ts.sec << TIMESPEC_SEC_SHIFT;
    sec as u64 | ms as u64
}

/// Convert Timespec to u64.
///
/// # Panics
///
/// If nsec is negative or GE than 1_000_000_000(nano seconds pre second).
#[inline]
fn u64_to_timespec(u: u64) -> Timespec {
    let sec = u >> TIMESPEC_SEC_SHIFT;
    let nsec = (u & TIMESPEC_NSEC_MASK) << TIMESPEC_NSEC_SHIFT;
    Timespec::new(sec as i64, nsec as i32)
}

/// Parse data of entry `index`.
///
/// # Panics
///
/// If `data` is corrupted, this function will panic.
// TODO: make sure received entries are not corrupted
#[inline]
pub fn parse_data_at<T: Message + Default>(data: &[u8], index: u64, tag: &str) -> T {
    let mut result = T::default();
    result.merge_from_bytes(data).unwrap_or_else(|e| {
        panic!("{} data is corrupted at {}: {:?}", tag, index, e);
    });
    result
}

/// Check if two branes are sibling.
///
/// They are sibling only when they share borders and don't overlap.
pub fn is_sibling_branes(lhs: &meta_timeshare::Brane, rhs: &meta_timeshare::Brane) -> bool {
    if lhs.get_id() == rhs.get_id() {
        return false;
    }
    if lhs.get_spacelike_key() == rhs.get_lightlike_key() && !rhs.get_lightlike_key().is_empty() {
        return true;
    }
    if lhs.get_lightlike_key() == rhs.get_spacelike_key() && !lhs.get_lightlike_key().is_empty() {
        return true;
    }
    false
}

pub fn conf_state_from_brane(brane: &meta_timeshare::Brane) -> ConfState {
    let mut conf_state = ConfState::default();
    let mut in_joint = false;
    for p in brane.get_peers() {
        match p.get_role() {
            PeerRole::Voter => {
                conf_state.mut_voters().push(p.get_id());
                conf_state.mut_voters_outgoing().push(p.get_id());
            }
            PeerRole::Learner => conf_state.mut_learners().push(p.get_id()),
            role => {
                in_joint = true;
                match role {
                    PeerRole::IncomingVoter => conf_state.mut_voters().push(p.get_id()),
                    PeerRole::DemotingVoter => {
                        conf_state.mut_voters_outgoing().push(p.get_id());
                        conf_state.mut_learners_next().push(p.get_id());
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
    if !in_joint {
        conf_state.mut_voters_outgoing().clear();
    }
    conf_state
}

pub fn is_learner(peer: &meta_timeshare::Peer) -> bool {
    peer.get_role() == meta_timeshare::PeerRole::Learner
}

pub struct TuplespaceInstantonInfoFormatter<
    'a,
    I: std::iter::DoubleEndedIterator<Item = &'a Vec<u8>>
        + std::iter::ExactSizeIterator<Item = &'a Vec<u8>>
        + Clone,
>(pub I);

impl<
        'a,
        I: std::iter::DoubleEndedIterator<Item = &'a Vec<u8>>
            + std::iter::ExactSizeIterator<Item = &'a Vec<u8>>
            + Clone,
    > fmt::Display for TuplespaceInstantonInfoFormatter<'a, I>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut it = self.0.clone();
        match it.len() {
            0 => write!(f, "(no key)"),
            1 => write!(f, "key {}", hex::encode_upper(it.next().unwrap())),
            _ => write!(
                f,
                "{} tuplespaceInstanton cone from {} to {}",
                it.len(),
                hex::encode_upper(it.next().unwrap()),
                hex::encode_upper(it.next_back().unwrap())
            ),
        }
    }
}

pub fn integration_on_half_fail_quorum_fn(voters: usize) -> usize {
    (voters + 1) / 2 + 1
}

#[macro_export]
macro_rules! report_perf_context {
    ($ctx: expr, $metric: ident) => {
        if $ctx.perf_level != PerfLevel::Disable {
            let perf_context = PerfContext::get();
            let pre_and_post_process = perf_context.write_pre_and_post_process_time();
            let write_thread_wait = perf_context.write_thread_wait_nanos();
            observe_perf_context_type!($ctx, perf_context, $metric, write_wal_time);
            observe_perf_context_type!($ctx, perf_context, $metric, write_memBlock_time);
            observe_perf_context_type!($ctx, perf_context, $metric, db_mutex_lock_nanos);
            observe_perf_context_type!($ctx, $metric, pre_and_post_process);
            observe_perf_context_type!($ctx, $metric, write_thread_wait);
            observe_perf_context_type!(
                $ctx,
                perf_context,
                $metric,
                write_scheduling_flushes_compactions_time
            );
            observe_perf_context_type!($ctx, perf_context, $metric, db_condition_wait_nanos);
            observe_perf_context_type!($ctx, perf_context, $metric, write_delay_time);
        }
    };
}

#[macro_export]
macro_rules! observe_perf_context_type {
    ($s:expr, $metric: expr, $v:ident) => {
        $metric.$v.observe((($v) - $s.$v) as f64 / 1_000_000_000.0);
        $s.$v = $v;
    };
    ($s:expr, $context: expr, $metric: expr, $v:ident) => {
        let $v = $context.$v();
        $metric.$v.observe((($v) - $s.$v) as f64 / 1_000_000_000.0);
        $s.$v = $v;
    };
}

pub struct PerfContextStatistics {
    pub perf_level: PerfLevel,
    pub write_wal_time: u64,
    pub pre_and_post_process: u64,
    pub write_memBlock_time: u64,
    pub write_thread_wait: u64,
    pub db_mutex_lock_nanos: u64,
    pub write_scheduling_flushes_compactions_time: u64,
    pub db_condition_wait_nanos: u64,
    pub write_delay_time: u64,
}

impl PerfContextStatistics {
    /// Create an instance which stores instant statistics values, retrieved at creation.
    pub fn new(perf_level: PerfLevel) -> Self {
        PerfContextStatistics {
            perf_level,
            write_wal_time: 0,
            pre_and_post_process: 0,
            write_thread_wait: 0,
            write_memBlock_time: 0,
            db_mutex_lock_nanos: 0,
            write_scheduling_flushes_compactions_time: 0,
            db_condition_wait_nanos: 0,
            write_delay_time: 0,
        }
    }

    pub fn spacelike(&mut self) {
        if self.perf_level == PerfLevel::Disable {
            return;
        }
        PerfContext::get().reset();
        set_perf_level(self.perf_level);
        self.write_wal_time = 0;
        self.pre_and_post_process = 0;
        self.db_mutex_lock_nanos = 0;
        self.write_thread_wait = 0;
        self.write_memBlock_time = 0;
        self.write_scheduling_flushes_compactions_time = 0;
        self.db_condition_wait_nanos = 0;
        self.write_delay_time = 0;
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum ConfChangeKind {
    // Only contains one configuration change
    Simple,
    // Enter joint state
    EnterJoint,
    // Leave joint state
    LeaveJoint,
}

impl ConfChangeKind {
    pub fn confchange_kind(change_num: usize) -> ConfChangeKind {
        match change_num {
            0 => ConfChangeKind::LeaveJoint,
            1 => ConfChangeKind::Simple,
            _ => ConfChangeKind::EnterJoint,
        }
    }
}

/// Abstracts over ChangePeerV2Request and (legacy) ChangePeerRequest to allow
/// treating them in a unified manner.
pub trait ChangePeerI {
    type CC: ConfChangeI;
    type CP: AsRef<[ChangePeerRequest]>;

    fn get_change_peers(&self) -> Self::CP;

    fn to_confchange(&self, _: Vec<u8>) -> Self::CC;
}

impl<'a> ChangePeerI for &'a ChangePeerRequest {
    type CC = evioletabft_timeshare::ConfChange;
    type CP = Vec<ChangePeerRequest>;

    fn get_change_peers(&self) -> Vec<ChangePeerRequest> {
        vec![ChangePeerRequest::clone(self)]
    }

    fn to_confchange(&self, ctx: Vec<u8>) -> evioletabft_timeshare::ConfChange {
        let mut cc = evioletabft_timeshare::ConfChange::default();
        cc.set_change_type(self.get_change_type());
        cc.set_node_id(self.get_peer().get_id());
        cc.set_context(ctx);
        cc
    }
}

impl<'a> ChangePeerI for &'a ChangePeerV2Request {
    type CC = evioletabft_timeshare::ConfChangeV2;
    type CP = &'a [ChangePeerRequest];

    fn get_change_peers(&self) -> &'a [ChangePeerRequest] {
        self.get_changes()
    }

    fn to_confchange(&self, ctx: Vec<u8>) -> evioletabft_timeshare::ConfChangeV2 {
        let mut cc = evioletabft_timeshare::ConfChangeV2::default();
        let changes: Vec<_> = self
            .get_changes()
            .iter()
            .map(|c| {
                let mut ccs = evioletabft_timeshare::ConfChangeSingle::default();
                ccs.set_change_type(c.get_change_type());
                ccs.set_node_id(c.get_peer().get_id());
                ccs
            })
            .collect();

        if changes.len() <= 1 {
            // Leave joint or simple confchange
            cc.set_transition(evioletabft_timeshare::ConfChangeTransition::Auto);
        } else {
            // Enter joint
            cc.set_transition(evioletabft_timeshare::ConfChangeTransition::Explicit);
        }
        cc.set_changes(changes.into());
        cc.set_context(ctx);
        cc
    }
}

#[causet(test)]
mod tests {
    use std::thread;

    use ekvproto::meta_timeshare::{self, BraneEpoch};
    use ekvproto::violetabft_cmd_timeshare::AdminRequest;
    use violetabft::evioletabft_timeshare::{ConfChangeType, Message, MessageType};
    use time::Duration as TimeDuration;

    use crate::store::peer_causet_storage;
    use violetabftstore::interlock::::time::{monotonic_now, monotonic_raw_now};

    use super::*;

    #[test]
    fn test_lease() {
        #[inline]
        fn sleep_test(duration: TimeDuration, lease: &Lease, state: LeaseState) {
            // In linux, sleep uses CLOCK_MONOTONIC.
            let monotonic_spacelike = monotonic_now();
            // In linux, lease uses CLOCK_MONOTONIC_RAW.
            let monotonic_raw_spacelike = monotonic_raw_now();
            thread::sleep(duration.to_std().unwrap());
            let monotonic_lightlike = monotonic_now();
            let monotonic_raw_lightlike = monotonic_raw_now();
            assert_eq!(
                lease.inspect(Some(monotonic_raw_lightlike)),
                state,
                "elapsed monotonic_raw: {:?}, monotonic: {:?}",
                monotonic_raw_lightlike - monotonic_raw_spacelike,
                monotonic_lightlike - monotonic_spacelike
            );
            assert_eq!(lease.inspect(None), state);
        }

        let duration = TimeDuration::milliseconds(1500);

        // Empty lease.
        let mut lease = Lease::new(duration);
        let remote = lease.maybe_new_remote_lease(1).unwrap();
        let inspect_test = |lease: &Lease, ts: Option<Timespec>, state: LeaseState| {
            assert_eq!(lease.inspect(ts), state);
            if state == LeaseState::Expired || state == LeaseState::Suspect {
                assert_eq!(remote.inspect(ts), LeaseState::Expired);
            }
        };

        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Expired);

        let now = monotonic_raw_now();
        let next_expired_time = lease.next_expired_time(now);
        assert_eq!(now + duration, next_expired_time);

        // Transit to the Valid state.
        lease.renew(now);
        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Valid);
        inspect_test(&lease, None, LeaseState::Valid);

        // After lease expired time.
        sleep_test(duration, &lease, LeaseState::Expired);
        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Expired);
        inspect_test(&lease, None, LeaseState::Expired);

        // Transit to the Suspect state.
        lease.suspect(monotonic_raw_now());
        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Suspect);
        inspect_test(&lease, None, LeaseState::Suspect);

        // After lease expired time, still suspect.
        sleep_test(duration, &lease, LeaseState::Suspect);
        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Suspect);

        // Clear lease.
        lease.expire();
        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Expired);
        inspect_test(&lease, None, LeaseState::Expired);

        // An expired remote lease can never renew.
        lease.renew(monotonic_raw_now() + TimeDuration::minutes(1));
        assert_eq!(
            remote.inspect(Some(monotonic_raw_now())),
            LeaseState::Expired
        );

        // A new remote lease.
        let m1 = lease.maybe_new_remote_lease(1).unwrap();
        assert_eq!(m1.inspect(Some(monotonic_raw_now())), LeaseState::Valid);
    }

    #[test]
    fn test_timespec_u64() {
        let cases = vec![
            (Timespec::new(0, 0), 0x0000_0000_0000_0000u64),
            (Timespec::new(0, 1), 0x0000_0000_0000_0000u64), // 1ns is round down to 0ms.
            (Timespec::new(0, 999_999), 0x0000_0000_0000_0000u64), // 999_999ns is round down to 0ms.
            (
                // 1_048_575ns is round down to 0ms.
                Timespec::new(0, 1_048_575 /* 0x0FFFFF */),
                0x0000_0000_0000_0000u64,
            ),
            (
                // 1_048_576ns is round down to 1ms.
                Timespec::new(0, 1_048_576 /* 0x100000 */),
                0x0000_0000_0000_0001u64,
            ),
            (Timespec::new(1, 0), 1 << TIMESPEC_SEC_SHIFT),
            (Timespec::new(1, 0x100000), 1 << TIMESPEC_SEC_SHIFT | 1),
            (
                // Max seconds.
                Timespec::new((1i64 << (64 - TIMESPEC_SEC_SHIFT)) - 1, 0),
                (-1i64 as u64) << TIMESPEC_SEC_SHIFT,
            ),
            (
                // Max nano seconds.
                Timespec::new(
                    0,
                    (999_999_999 >> TIMESPEC_NSEC_SHIFT) << TIMESPEC_NSEC_SHIFT,
                ),
                999_999_999 >> TIMESPEC_NSEC_SHIFT,
            ),
            (
                Timespec::new(
                    (1i64 << (64 - TIMESPEC_SEC_SHIFT)) - 1,
                    (999_999_999 >> TIMESPEC_NSEC_SHIFT) << TIMESPEC_NSEC_SHIFT,
                ),
                (-1i64 as u64) << TIMESPEC_SEC_SHIFT | (999_999_999 >> TIMESPEC_NSEC_SHIFT),
            ),
        ];

        for (ts, u) in cases {
            assert!(u64_to_timespec(timespec_to_u64(ts)) <= ts);
            assert!(u64_to_timespec(u) <= ts);
            assert_eq!(timespec_to_u64(u64_to_timespec(u)), u);
            assert_eq!(timespec_to_u64(ts), u);
        }

        let spacelike = monotonic_raw_now();
        let mut now = monotonic_raw_now();
        while now - spacelike < Duration::seconds(1) {
            let u = timespec_to_u64(now);
            let round = u64_to_timespec(u);
            assert!(round <= now, "{:064b} = {:?} > {:?}", u, round, now);
            now = monotonic_raw_now();
        }
    }

    // Tests the util function `check_key_in_brane`.
    #[test]
    fn test_check_key_in_brane() {
        let test_cases = vec![
            ("", "", "", true, true, false),
            ("", "", "6", true, true, false),
            ("", "3", "6", false, false, false),
            ("4", "3", "6", true, true, true),
            ("4", "3", "", true, true, true),
            ("3", "3", "", true, true, false),
            ("2", "3", "6", false, false, false),
            ("", "3", "6", false, false, false),
            ("", "3", "", false, false, false),
            ("6", "3", "6", false, true, false),
        ];
        for (key, spacelike_key, lightlike_key, is_in_brane, inclusive, exclusive) in test_cases {
            let mut brane = meta_timeshare::Brane::default();
            brane.set_spacelike_key(spacelike_key.as_bytes().to_vec());
            brane.set_lightlike_key(lightlike_key.as_bytes().to_vec());
            let mut result = check_key_in_brane(key.as_bytes(), &brane);
            assert_eq!(result.is_ok(), is_in_brane);
            result = check_key_in_brane_inclusive(key.as_bytes(), &brane);
            assert_eq!(result.is_ok(), inclusive);
            result = check_key_in_brane_exclusive(key.as_bytes(), &brane);
            assert_eq!(result.is_ok(), exclusive);
        }
    }

    fn gen_brane(
        voters: &[u64],
        learners: &[u64],
        incomming_v: &[u64],
        demoting_v: &[u64],
    ) -> meta_timeshare::Brane {
        let mut brane = meta_timeshare::Brane::default();
        macro_rules! push_peer {
            ($ids: ident, $role: expr) => {
                for id in $ids {
                    let mut peer = meta_timeshare::Peer::default();
                    peer.set_id(*id);
                    peer.set_role($role);
                    brane.mut_peers().push(peer);
                }
            };
        }
        push_peer!(voters, meta_timeshare::PeerRole::Voter);
        push_peer!(learners, meta_timeshare::PeerRole::Learner);
        push_peer!(incomming_v, meta_timeshare::PeerRole::IncomingVoter);
        push_peer!(demoting_v, meta_timeshare::PeerRole::DemotingVoter);
        brane
    }

    #[test]
    fn test_conf_state_from_brane() {
        let cases = vec![
            (vec![1], vec![2], vec![], vec![]),
            (vec![], vec![], vec![1], vec![2]),
            (vec![1, 2], vec![], vec![], vec![]),
            (vec![1, 2], vec![], vec![3], vec![]),
            (vec![1], vec![2], vec![3, 4], vec![5, 6]),
        ];

        for (voter, learner, incomming, demoting) in cases {
            let brane = gen_brane(
                voter.as_slice(),
                learner.as_slice(),
                incomming.as_slice(),
                demoting.as_slice(),
            );
            let cs = conf_state_from_brane(&brane);
            if incomming.is_empty() && demoting.is_empty() {
                // Not in joint
                assert!(cs.get_voters_outgoing().is_empty());
                assert!(cs.get_learners_next().is_empty());
                assert!(voter.iter().all(|id| cs.get_voters().contains(id)));
                assert!(learner.iter().all(|id| cs.get_learners().contains(id)));
            } else {
                // In joint
                assert!(voter.iter().all(
                    |id| cs.get_voters().contains(id) && cs.get_voters_outgoing().contains(id)
                ));
                assert!(learner.iter().all(|id| cs.get_learners().contains(id)));
                assert!(incomming.iter().all(|id| cs.get_voters().contains(id)));
                assert!(demoting
                    .iter()
                    .all(|id| cs.get_voters_outgoing().contains(id)
                        && cs.get_learners_next().contains(id)));
            }
        }
    }

    #[test]
    fn test_changepeer_v2_to_confchange() {
        let mut req = ChangePeerV2Request::default();

        // Zore change for leave joint
        assert_eq!(
            (&req).to_confchange(vec![]).get_transition(),
            evioletabft_timeshare::ConfChangeTransition::Auto
        );

        // One change for simple confchange
        req.mut_changes().push(ChangePeerRequest::default());
        assert_eq!(
            (&req).to_confchange(vec![]).get_transition(),
            evioletabft_timeshare::ConfChangeTransition::Auto
        );

        // More than one change for enter joint
        req.mut_changes().push(ChangePeerRequest::default());
        assert_eq!(
            (&req).to_confchange(vec![]).get_transition(),
            evioletabft_timeshare::ConfChangeTransition::Explicit
        );
        req.mut_changes().push(ChangePeerRequest::default());
        assert_eq!(
            (&req).to_confchange(vec![]).get_transition(),
            evioletabft_timeshare::ConfChangeTransition::Explicit
        );
    }

    #[test]
    fn test_peer() {
        let mut brane = meta_timeshare::Brane::default();
        brane.set_id(1);
        brane.mut_peers().push(new_peer(1, 1));
        brane.mut_peers().push(new_learner_peer(2, 2));

        assert!(!is_learner(find_peer(&brane, 1).unwrap()));
        assert!(is_learner(find_peer(&brane, 2).unwrap()));

        assert!(remove_peer(&mut brane, 1).is_some());
        assert!(remove_peer(&mut brane, 1).is_none());
        assert!(find_peer(&brane, 1).is_none());
    }

    #[test]
    fn test_first_vote_msg() {
        let tbl = vec![
            (
                MessageType::MsgRequestVote,
                peer_causet_storage::VIOLETABFT_INIT_LOG_TERM + 1,
                true,
            ),
            (
                MessageType::MsgRequestPreVote,
                peer_causet_storage::VIOLETABFT_INIT_LOG_TERM + 1,
                true,
            ),
            (
                MessageType::MsgRequestVote,
                peer_causet_storage::VIOLETABFT_INIT_LOG_TERM,
                false,
            ),
            (
                MessageType::MsgRequestPreVote,
                peer_causet_storage::VIOLETABFT_INIT_LOG_TERM,
                false,
            ),
            (
                MessageType::MsgHup,
                peer_causet_storage::VIOLETABFT_INIT_LOG_TERM + 1,
                false,
            ),
        ];

        for (msg_type, term, is_vote) in tbl {
            let mut msg = Message::default();
            msg.set_msg_type(msg_type);
            msg.set_term(term);
            assert_eq!(is_first_vote_msg(&msg), is_vote);
        }
    }

    #[test]
    fn test_is_initial_msg() {
        let tbl = vec![
            (MessageType::MsgRequestVote, INVALID_INDEX, true),
            (MessageType::MsgRequestPreVote, INVALID_INDEX, true),
            (MessageType::MsgHeartbeat, INVALID_INDEX, true),
            (MessageType::MsgHeartbeat, 100, false),
            (MessageType::MsgApplightlike, 100, false),
        ];

        for (msg_type, commit, can_create) in tbl {
            let mut msg = Message::default();
            msg.set_msg_type(msg_type);
            msg.set_commit(commit);
            assert_eq!(is_initial_msg(&msg), can_create);
        }
    }

    #[test]
    fn test_conf_change_type_str() {
        assert_eq!(
            conf_change_type_str(ConfChangeType::AddNode),
            STR_CONF_CHANGE_ADD_NODE
        );
        assert_eq!(
            conf_change_type_str(ConfChangeType::RemoveNode),
            STR_CONF_CHANGE_REMOVE_NODE
        );
    }

    #[test]
    fn test_epoch_stale() {
        let mut epoch = meta_timeshare::BraneEpoch::default();
        epoch.set_version(10);
        epoch.set_conf_ver(10);

        let tbl = vec![
            (11, 10, true),
            (10, 11, true),
            (10, 10, false),
            (10, 9, false),
        ];

        for (version, conf_version, is_stale) in tbl {
            let mut check_epoch = meta_timeshare::BraneEpoch::default();
            check_epoch.set_version(version);
            check_epoch.set_conf_ver(conf_version);
            assert_eq!(is_epoch_stale(&epoch, &check_epoch), is_stale);
        }
    }

    #[test]
    fn test_on_same_store() {
        let cases = vec![
            (vec![2, 3, 4], vec![], vec![1, 2, 3], vec![], false),
            (vec![2, 3, 1], vec![], vec![1, 2, 3], vec![], true),
            (vec![2, 3, 4], vec![], vec![1, 2], vec![], false),
            (vec![1, 2, 3], vec![], vec![1, 2, 3], vec![], true),
            (vec![1, 3], vec![2, 4], vec![1, 2], vec![3, 4], false),
            (vec![1, 3], vec![2, 4], vec![1, 3], vec![], false),
            (vec![1, 3], vec![2, 4], vec![], vec![2, 4], false),
            (vec![1, 3], vec![2, 4], vec![3, 1], vec![4, 2], true),
        ];

        for (s1, s2, s3, s4, exp) in cases {
            let mut r1 = meta_timeshare::Brane::default();
            for (store_id, peer_id) in s1.into_iter().zip(0..) {
                r1.mut_peers().push(new_peer(store_id, peer_id));
            }
            for (store_id, peer_id) in s2.into_iter().zip(0..) {
                r1.mut_peers().push(new_learner_peer(store_id, peer_id));
            }

            let mut r2 = meta_timeshare::Brane::default();
            for (store_id, peer_id) in s3.into_iter().zip(10..) {
                r2.mut_peers().push(new_peer(store_id, peer_id));
            }
            for (store_id, peer_id) in s4.into_iter().zip(10..) {
                r2.mut_peers().push(new_learner_peer(store_id, peer_id));
            }
            let res = super::brane_on_same_stores(&r1, &r2);
            assert_eq!(res, exp, "{:?} vs {:?}", r1, r2);
        }
    }

    fn split(mut r: meta_timeshare::Brane, key: &[u8]) -> (meta_timeshare::Brane, meta_timeshare::Brane) {
        let mut r2 = r.clone();
        r.set_lightlike_key(key.to_owned());
        r2.set_id(r.get_id() + 1);
        r2.set_spacelike_key(key.to_owned());
        (r, r2)
    }

    fn check_sibling(r1: &meta_timeshare::Brane, r2: &meta_timeshare::Brane, is_sibling: bool) {
        assert_eq!(is_sibling_branes(r1, r2), is_sibling);
        assert_eq!(is_sibling_branes(r2, r1), is_sibling);
    }

    #[test]
    fn test_brane_sibling() {
        let r1 = meta_timeshare::Brane::default();
        check_sibling(&r1, &r1, false);

        let (r1, r2) = split(r1, b"k1");
        check_sibling(&r1, &r2, true);

        let (r2, r3) = split(r2, b"k2");
        check_sibling(&r2, &r3, true);

        let (r3, r4) = split(r3, b"k3");
        check_sibling(&r3, &r4, true);
        check_sibling(&r1, &r2, true);
        check_sibling(&r2, &r3, true);
        check_sibling(&r1, &r3, false);
        check_sibling(&r2, &r4, false);
        check_sibling(&r1, &r4, false);
    }

    #[test]
    fn test_check_store_id() {
        let mut req = VioletaBftCmdRequest::default();
        req.mut_header().mut_peer().set_store_id(1);
        check_store_id(&req, 1).unwrap();
        check_store_id(&req, 2).unwrap_err();
    }

    #[test]
    fn test_check_peer_id() {
        let mut req = VioletaBftCmdRequest::default();
        req.mut_header().mut_peer().set_id(1);
        check_peer_id(&req, 1).unwrap();
        check_peer_id(&req, 2).unwrap_err();
    }

    #[test]
    fn test_check_term() {
        let mut req = VioletaBftCmdRequest::default();
        req.mut_header().set_term(7);
        check_term(&req, 7).unwrap();
        check_term(&req, 8).unwrap();
        // If header's term is 2 verions behind current term,
        // leadership may have been changed away.
        check_term(&req, 9).unwrap_err();
        check_term(&req, 10).unwrap_err();
    }

    #[test]
    fn test_check_brane_epoch() {
        let mut epoch = BraneEpoch::default();
        epoch.set_conf_ver(2);
        epoch.set_version(2);
        let mut brane = meta_timeshare::Brane::default();
        brane.set_brane_epoch(epoch.clone());

        // Epoch is required for most requests even if it's empty.
        check_brane_epoch(&VioletaBftCmdRequest::default(), &brane, false).unwrap_err();

        // These admin commands do not require epoch.
        for ty in &[
            AdminCmdType::CompactLog,
            AdminCmdType::InvalidAdmin,
            AdminCmdType::ComputeHash,
            AdminCmdType::VerifyHash,
        ] {
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(*ty);
            let mut req = VioletaBftCmdRequest::default();
            req.set_admin_request(admin);

            // It is Okay if req does not have brane epoch.
            check_brane_epoch(&req, &brane, false).unwrap();

            req.mut_header().set_brane_epoch(epoch.clone());
            check_brane_epoch(&req, &brane, true).unwrap();
            check_brane_epoch(&req, &brane, false).unwrap();
        }

        // These admin commands requires epoch.version.
        for ty in &[
            AdminCmdType::Split,
            AdminCmdType::BatchSplit,
            AdminCmdType::PrepareMerge,
            AdminCmdType::CommitMerge,
            AdminCmdType::RollbackMerge,
            AdminCmdType::TransferLeader,
        ] {
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(*ty);
            let mut req = VioletaBftCmdRequest::default();
            req.set_admin_request(admin);

            // Error if req does not have brane epoch.
            check_brane_epoch(&req, &brane, false).unwrap_err();

            let mut stale_version_epoch = epoch.clone();
            stale_version_epoch.set_version(1);
            let mut stale_brane = meta_timeshare::Brane::default();
            stale_brane.set_brane_epoch(stale_version_epoch.clone());
            req.mut_header()
                .set_brane_epoch(stale_version_epoch.clone());
            check_brane_epoch(&req, &stale_brane, false).unwrap();

            let mut latest_version_epoch = epoch.clone();
            latest_version_epoch.set_version(3);
            for epoch in &[stale_version_epoch, latest_version_epoch] {
                req.mut_header().set_brane_epoch(epoch.clone());
                check_brane_epoch(&req, &brane, false).unwrap_err();
                check_brane_epoch(&req, &brane, true).unwrap_err();
            }
        }

        // These admin commands requires epoch.conf_version.
        for ty in &[
            AdminCmdType::Split,
            AdminCmdType::BatchSplit,
            AdminCmdType::ChangePeer,
            AdminCmdType::ChangePeerV2,
            AdminCmdType::PrepareMerge,
            AdminCmdType::CommitMerge,
            AdminCmdType::RollbackMerge,
            AdminCmdType::TransferLeader,
        ] {
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(*ty);
            let mut req = VioletaBftCmdRequest::default();
            req.set_admin_request(admin);

            // Error if req does not have brane epoch.
            check_brane_epoch(&req, &brane, false).unwrap_err();

            let mut stale_conf_epoch = epoch.clone();
            stale_conf_epoch.set_conf_ver(1);
            let mut stale_brane = meta_timeshare::Brane::default();
            stale_brane.set_brane_epoch(stale_conf_epoch.clone());
            req.mut_header().set_brane_epoch(stale_conf_epoch.clone());
            check_brane_epoch(&req, &stale_brane, false).unwrap();

            let mut latest_conf_epoch = epoch.clone();
            latest_conf_epoch.set_conf_ver(3);
            for epoch in &[stale_conf_epoch, latest_conf_epoch] {
                req.mut_header().set_brane_epoch(epoch.clone());
                check_brane_epoch(&req, &brane, false).unwrap_err();
                check_brane_epoch(&req, &brane, true).unwrap_err();
            }
        }
    }

    #[test]
    fn test_integration_on_half_fail_quorum_fn() {
        let voters = vec![1, 2, 3, 4, 5, 6, 7];
        let quorum = vec![2, 2, 3, 3, 4, 4, 5];
        for (voter_count, expected_quorum) in voters.into_iter().zip(quorum) {
            let quorum = super::integration_on_half_fail_quorum_fn(voter_count);
            assert_eq!(quorum, expected_quorum);
        }
    }

    #[test]
    fn test_is_brane_initialized() {
        let mut brane = meta_timeshare::Brane::default();
        assert!(!is_brane_initialized(&brane));
        let peers = vec![new_peer(1, 2)];
        brane.set_peers(peers.into());
        assert!(is_brane_initialized(&brane));
    }

    #[test]
    fn test_admin_cmd_epoch_map_include_all_cmd_type() {
        #[causet(feature = "protobuf-codec")]
        use protobuf::ProtobufEnum;
        for cmd_type in AdminCmdType::values() {
            assert!(ADMIN_CMD_EPOCH_MAP.contains_key(cmd_type));
        }
    }
}
