// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use ekvproto::metapb::PeerRole;
use violetabft::{Progress, ProgressState, StateRole};
use violetabftstore::store::AbstractPeer;
use std::collections::HashMap;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum VioletaBftProgressState {
    Probe,
    Replicate,
    Snapshot,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct VioletaBftProgress {
    pub matched: u64,
    pub next_idx: u64,
    pub state: VioletaBftProgressState,
    pub paused: bool,
    pub plightlikeing_snapshot: u64,
    pub plightlikeing_request_snapshot: u64,
    pub recent_active: bool,
}

impl VioletaBftProgress {
    fn new(progress: &Progress) -> Self {
        Self {
            matched: progress.matched,
            next_idx: progress.next_idx,
            state: progress.state.into(),
            paused: progress.paused,
            plightlikeing_snapshot: progress.plightlikeing_snapshot,
            plightlikeing_request_snapshot: progress.plightlikeing_request_snapshot,
            recent_active: progress.recent_active,
        }
    }
}

impl From<ProgressState> for VioletaBftProgressState {
    fn from(state: ProgressState) -> Self {
        match state {
            ProgressState::Probe => VioletaBftProgressState::Probe,
            ProgressState::Replicate => VioletaBftProgressState::Replicate,
            ProgressState::Snapshot => VioletaBftProgressState::Snapshot,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct VioletaBftHardState {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum VioletaBftStateRole {
    Follower,
    Candidate,
    Leader,
    PreCandidate,
}

impl From<StateRole> for VioletaBftStateRole {
    fn from(role: StateRole) -> Self {
        match role {
            StateRole::Follower => VioletaBftStateRole::Follower,
            StateRole::Candidate => VioletaBftStateRole::Candidate,
            StateRole::Leader => VioletaBftStateRole::Leader,
            StateRole::PreCandidate => VioletaBftStateRole::PreCandidate,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct VioletaBftSoftState {
    pub leader_id: u64,
    pub violetabft_state: VioletaBftStateRole,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VioletaBftStatus {
    pub id: u64,
    pub hard_state: VioletaBftHardState,
    pub soft_state: VioletaBftSoftState,
    pub applied: u64,
    pub voters: HashMap<u64, VioletaBftProgress>,
    pub learners: HashMap<u64, VioletaBftProgress>,
}

impl<'a> From<violetabft::Status<'a>> for VioletaBftStatus {
    fn from(status: violetabft::Status<'a>) -> Self {
        let id = status.id;
        let hard_state = VioletaBftHardState {
            term: status.hs.get_term(),
            vote: status.hs.get_vote(),
            commit: status.hs.get_commit(),
        };
        let soft_state = VioletaBftSoftState {
            leader_id: status.ss.leader_id,
            violetabft_state: status.ss.violetabft_state.into(),
        };
        let applied = status.applied;
        let mut voters = HashMap::new();
        let mut learners = HashMap::new();
        if let Some(progress) = status.progress {
            for (id, pr) in progress.iter() {
                if progress.conf().voters().contains(*id) {
                    voters.insert(*id, VioletaBftProgress::new(pr));
                } else {
                    learners.insert(*id, VioletaBftProgress::new(pr));
                }
            }
        }
        Self {
            id,
            hard_state,
            soft_state,
            applied,
            voters,
            learners,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum VioletaBftPeerRole {
    Voter,
    Learner,
    IncomingVoter,
    DemotingVoter,
}

impl From<PeerRole> for VioletaBftPeerRole {
    fn from(role: PeerRole) -> Self {
        match role {
            PeerRole::Voter => VioletaBftPeerRole::Voter,
            PeerRole::Learner => VioletaBftPeerRole::Learner,
            PeerRole::IncomingVoter => VioletaBftPeerRole::IncomingVoter,
            PeerRole::DemotingVoter => VioletaBftPeerRole::DemotingVoter,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Epoch {
    pub conf_ver: u64,
    pub version: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct BranePeer {
    pub id: u64,
    pub store_id: u64,
    pub role: VioletaBftPeerRole,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct BraneMergeState {
    pub min_index: u64,
    pub commit: u64,
    pub brane_id: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct VioletaBftTruncatedState {
    pub index: u64,
    pub term: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct VioletaBftApplyState {
    pub applied_index: u64,
    pub last_commit_index: u64,
    pub commit_index: u64,
    pub commit_term: u64,
    pub truncated_state: VioletaBftTruncatedState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BraneMeta {
    pub id: u64,
    pub spacelike_key: Vec<u8>,
    pub lightlike_key: Vec<u8>,
    pub epoch: Epoch,
    pub peers: Vec<BranePeer>,
    pub merge_state: Option<BraneMergeState>,
    pub violetabft_status: VioletaBftStatus,
    pub violetabft_apply: VioletaBftApplyState,
}

impl BraneMeta {
    pub fn new(abstract_peer: &dyn AbstractPeer) -> Self {
        let brane = abstract_peer.brane();
        let apply_state = abstract_peer.apply_state();
        let epoch = brane.get_brane_epoch();
        let spacelike_key = brane.get_spacelike_key();
        let lightlike_key = brane.get_lightlike_key();
        let raw_peers = brane.get_peers();
        let mut peers = Vec::with_capacity(raw_peers.len());
        for peer in raw_peers {
            peers.push(BranePeer {
                id: peer.get_id(),
                store_id: peer.get_store_id(),
                role: peer.get_role().into(),
            });
        }

        Self {
            id: brane.get_id(),
            spacelike_key: spacelike_key.to_owned(),
            lightlike_key: lightlike_key.to_owned(),
            epoch: Epoch {
                conf_ver: epoch.get_conf_ver(),
                version: epoch.get_version(),
            },
            peers,
            merge_state: abstract_peer
                .plightlikeing_merge_state()
                .map(|state| BraneMergeState {
                    min_index: state.get_min_index(),
                    commit: state.get_commit(),
                    brane_id: state.get_target().get_id(),
                }),
            violetabft_status: abstract_peer.violetabft_status().into(),
            violetabft_apply: VioletaBftApplyState {
                applied_index: apply_state.get_applied_index(),
                last_commit_index: apply_state.get_last_commit_index(),
                commit_index: apply_state.get_commit_index(),
                commit_term: apply_state.get_commit_term(),
                truncated_state: VioletaBftTruncatedState {
                    index: apply_state.get_truncated_state().get_index(),
                    term: apply_state.get_truncated_state().get_term(),
                },
            },
        }
    }
}
