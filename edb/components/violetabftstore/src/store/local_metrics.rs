// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use prometheus::local::LocalHistogram;
use std::sync::{Arc, Mutex};

use violetabftstore::interlock::::collections::HashSet;

use super::metrics::*;

/// The buffered metrics counters for violetabft ready handling.
#[derive(Debug, Default, Clone)]
pub struct VioletaBftReadyMetrics {
    pub message: u64,
    pub commit: u64,
    pub applightlike: u64,
    pub snapshot: u64,
    pub plightlikeing_brane: u64,
    pub has_ready_brane: u64,
}

impl VioletaBftReadyMetrics {
    /// Flushes all metrics
    fn flush(&mut self) {
        // reset all buffered metrics once they have been added
        if self.message > 0 {
            STORE_VIOLETABFT_READY_COUNTER.message.inc_by(self.message as i64);
            self.message = 0;
        }
        if self.commit > 0 {
            STORE_VIOLETABFT_READY_COUNTER.commit.inc_by(self.commit as i64);
            self.commit = 0;
        }
        if self.applightlike > 0 {
            STORE_VIOLETABFT_READY_COUNTER.applightlike.inc_by(self.applightlike as i64);
            self.applightlike = 0;
        }
        if self.snapshot > 0 {
            STORE_VIOLETABFT_READY_COUNTER
                .snapshot
                .inc_by(self.snapshot as i64);
            self.snapshot = 0;
        }
        if self.plightlikeing_brane > 0 {
            STORE_VIOLETABFT_READY_COUNTER
                .plightlikeing_brane
                .inc_by(self.plightlikeing_brane as i64);
            self.plightlikeing_brane = 0;
        }
        if self.has_ready_brane > 0 {
            STORE_VIOLETABFT_READY_COUNTER
                .has_ready_brane
                .inc_by(self.has_ready_brane as i64);
            self.has_ready_brane = 0;
        }
    }
}

/// The buffered metrics counters for violetabft message.
#[derive(Debug, Default, Clone)]
pub struct VioletaBftMessageMetrics {
    pub applightlike: u64,
    pub applightlike_resp: u64,
    pub prevote: u64,
    pub prevote_resp: u64,
    pub vote: u64,
    pub vote_resp: u64,
    pub snapshot: u64,
    pub request_snapshot: u64,
    pub heartbeat: u64,
    pub heartbeat_resp: u64,
    pub transfer_leader: u64,
    pub timeout_now: u64,
    pub read_index: u64,
    pub read_index_resp: u64,
}

impl VioletaBftMessageMetrics {
    /// Flushes all metrics
    fn flush(&mut self) {
        // reset all buffered metrics once they have been added
        if self.applightlike > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .applightlike
                .inc_by(self.applightlike as i64);
            self.applightlike = 0;
        }
        if self.applightlike_resp > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .applightlike_resp
                .inc_by(self.applightlike_resp as i64);
            self.applightlike_resp = 0;
        }
        if self.prevote > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .prevote
                .inc_by(self.prevote as i64);
            self.prevote = 0;
        }
        if self.prevote_resp > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .prevote_resp
                .inc_by(self.prevote_resp as i64);
            self.prevote_resp = 0;
        }
        if self.vote > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .vote
                .inc_by(self.vote as i64);
            self.vote = 0;
        }
        if self.vote_resp > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .vote_resp
                .inc_by(self.vote_resp as i64);
            self.vote_resp = 0;
        }
        if self.snapshot > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .snapshot
                .inc_by(self.snapshot as i64);
            self.snapshot = 0;
        }
        if self.request_snapshot > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .request_snapshot
                .inc_by(self.request_snapshot as i64);
            self.request_snapshot = 0;
        }
        if self.heartbeat > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .heartbeat
                .inc_by(self.heartbeat as i64);
            self.heartbeat = 0;
        }
        if self.heartbeat_resp > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .heartbeat_resp
                .inc_by(self.heartbeat_resp as i64);
            self.heartbeat_resp = 0;
        }
        if self.transfer_leader > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .transfer_leader
                .inc_by(self.transfer_leader as i64);
            self.transfer_leader = 0;
        }
        if self.timeout_now > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .timeout_now
                .inc_by(self.timeout_now as i64);
            self.timeout_now = 0;
        }
        if self.read_index > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .read_index
                .inc_by(self.read_index as i64);
            self.read_index = 0;
        }
        if self.read_index_resp > 0 {
            STORE_VIOLETABFT_SENT_MESSAGE_COUNTER
                .read_index_resp
                .inc_by(self.read_index_resp as i64);
            self.read_index_resp = 0;
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct VioletaBftMessageDropMetrics {
    pub mismatch_store_id: u64,
    pub mismatch_brane_epoch: u64,
    pub stale_msg: u64,
    pub brane_overlap: u64,
    pub brane_no_peer: u64,
    pub brane_tombstone_peer: u64,
    pub brane_nonexistent: u64,
    pub applying_snap: u64,
}

impl VioletaBftMessageDropMetrics {
    fn flush(&mut self) {
        if self.mismatch_store_id > 0 {
            STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER
                .mismatch_store_id
                .inc_by(self.mismatch_store_id as i64);
            self.mismatch_store_id = 0;
        }
        if self.mismatch_brane_epoch > 0 {
            STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER
                .mismatch_brane_epoch
                .inc_by(self.mismatch_brane_epoch as i64);
            self.mismatch_brane_epoch = 0;
        }
        if self.stale_msg > 0 {
            STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER
                .stale_msg
                .inc_by(self.stale_msg as i64);
            self.stale_msg = 0;
        }
        if self.brane_overlap > 0 {
            STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER
                .brane_overlap
                .inc_by(self.brane_overlap as i64);
            self.brane_overlap = 0;
        }
        if self.brane_no_peer > 0 {
            STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER
                .brane_no_peer
                .inc_by(self.brane_no_peer as i64);
            self.brane_no_peer = 0;
        }
        if self.brane_tombstone_peer > 0 {
            STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER
                .brane_tombstone_peer
                .inc_by(self.brane_tombstone_peer as i64);
            self.brane_tombstone_peer = 0;
        }
        if self.brane_nonexistent > 0 {
            STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER
                .brane_nonexistent
                .inc_by(self.brane_nonexistent as i64);
            self.brane_nonexistent = 0;
        }
        if self.applying_snap > 0 {
            STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER
                .applying_snap
                .inc_by(self.applying_snap as i64);
            self.applying_snap = 0;
        }
    }
}

/// The buffered metrics counters for violetabft propose.
#[derive(Clone)]
pub struct VioletaBftProposeMetrics {
    pub all: u64,
    pub local_read: u64,
    pub read_index: u64,
    pub unsafe_read_index: u64,
    pub normal: u64,
    pub batch: usize,
    pub transfer_leader: u64,
    pub conf_change: u64,
    pub request_wait_time: LocalHistogram,
}

impl Default for VioletaBftProposeMetrics {
    fn default() -> VioletaBftProposeMetrics {
        VioletaBftProposeMetrics {
            all: 0,
            local_read: 0,
            read_index: 0,
            unsafe_read_index: 0,
            normal: 0,
            transfer_leader: 0,
            conf_change: 0,
            batch: 0,
            request_wait_time: REQUEST_WAIT_TIME_HISTOGRAM.local(),
        }
    }
}

impl VioletaBftProposeMetrics {
    /// Flushes all metrics
    fn flush(&mut self) {
        // reset all buffered metrics once they have been added
        if self.all > 0 {
            PEER_PROPOSAL_COUNTER.all.inc_by(self.all as i64);
            self.all = 0;
        }
        if self.local_read > 0 {
            PEER_PROPOSAL_COUNTER
                .local_read
                .inc_by(self.local_read as i64);
            self.local_read = 0;
        }
        if self.read_index > 0 {
            PEER_PROPOSAL_COUNTER
                .read_index
                .inc_by(self.read_index as i64);
            self.read_index = 0;
        }
        if self.unsafe_read_index > 0 {
            PEER_PROPOSAL_COUNTER
                .unsafe_read_index
                .inc_by(self.unsafe_read_index as i64);
            self.unsafe_read_index = 0;
        }
        if self.normal > 0 {
            PEER_PROPOSAL_COUNTER.normal.inc_by(self.normal as i64);
            self.normal = 0;
        }
        if self.transfer_leader > 0 {
            PEER_PROPOSAL_COUNTER
                .transfer_leader
                .inc_by(self.transfer_leader as i64);
            self.transfer_leader = 0;
        }
        if self.conf_change > 0 {
            PEER_PROPOSAL_COUNTER
                .conf_change
                .inc_by(self.conf_change as i64);
            self.conf_change = 0;
        }
        if self.batch > 0 {
            PEER_PROPOSAL_COUNTER.batch.inc_by(self.batch as i64);
            self.batch = 0;
        }
        self.request_wait_time.flush();
    }
}

/// The buffered metrics counter for invalid propose
#[derive(Clone)]
pub struct VioletaBftInvalidProposeMetrics {
    pub mismatch_store_id: u64,
    pub brane_not_found: u64,
    pub not_leader: u64,
    pub mismatch_peer_id: u64,
    pub stale_command: u64,
    pub epoch_not_match: u64,
    pub read_index_no_leader: u64,
    pub brane_not_initialized: u64,
    pub is_applying_snapshot: u64,
}

impl Default for VioletaBftInvalidProposeMetrics {
    fn default() -> VioletaBftInvalidProposeMetrics {
        VioletaBftInvalidProposeMetrics {
            mismatch_store_id: 0,
            brane_not_found: 0,
            not_leader: 0,
            mismatch_peer_id: 0,
            stale_command: 0,
            epoch_not_match: 0,
            read_index_no_leader: 0,
            brane_not_initialized: 0,
            is_applying_snapshot: 0,
        }
    }
}

impl VioletaBftInvalidProposeMetrics {
    fn flush(&mut self) {
        if self.mismatch_store_id > 0 {
            VIOLETABFT_INVALID_PROPOSAL_COUNTER
                .mismatch_store_id
                .inc_by(self.mismatch_store_id as i64);
            self.mismatch_store_id = 0;
        }
        if self.brane_not_found > 0 {
            VIOLETABFT_INVALID_PROPOSAL_COUNTER
                .brane_not_found
                .inc_by(self.brane_not_found as i64);
            self.brane_not_found = 0;
        }
        if self.not_leader > 0 {
            VIOLETABFT_INVALID_PROPOSAL_COUNTER
                .not_leader
                .inc_by(self.not_leader as i64);
            self.not_leader = 0;
        }
        if self.mismatch_peer_id > 0 {
            VIOLETABFT_INVALID_PROPOSAL_COUNTER
                .mismatch_peer_id
                .inc_by(self.mismatch_peer_id as i64);
            self.mismatch_peer_id = 0;
        }
        if self.stale_command > 0 {
            VIOLETABFT_INVALID_PROPOSAL_COUNTER
                .stale_command
                .inc_by(self.stale_command as i64);
            self.stale_command = 0;
        }
        if self.epoch_not_match > 0 {
            VIOLETABFT_INVALID_PROPOSAL_COUNTER
                .epoch_not_match
                .inc_by(self.epoch_not_match as i64);
            self.epoch_not_match = 0;
        }
        if self.read_index_no_leader > 0 {
            VIOLETABFT_INVALID_PROPOSAL_COUNTER
                .read_index_no_leader
                .inc_by(self.read_index_no_leader as i64);
            self.read_index_no_leader = 0;
        }
        if self.brane_not_initialized > 0 {
            VIOLETABFT_INVALID_PROPOSAL_COUNTER
                .brane_not_initialized
                .inc_by(self.brane_not_initialized as i64);
            self.brane_not_initialized = 0;
        }
        if self.is_applying_snapshot > 0 {
            VIOLETABFT_INVALID_PROPOSAL_COUNTER
                .is_applying_snapshot
                .inc_by(self.is_applying_snapshot as i64);
            self.is_applying_snapshot = 0;
        }
    }
}
/// The buffered metrics counters for violetabft.
#[derive(Clone)]
pub struct VioletaBftMetrics {
    pub ready: VioletaBftReadyMetrics,
    pub message: VioletaBftMessageMetrics,
    pub message_dropped: VioletaBftMessageDropMetrics,
    pub propose: VioletaBftProposeMetrics,
    pub process_ready: LocalHistogram,
    pub applightlike_log: LocalHistogram,
    pub commit_log: LocalHistogram,
    pub leader_missing: Arc<Mutex<HashSet<u64>>>,
    pub invalid_proposal: VioletaBftInvalidProposeMetrics,
}

impl Default for VioletaBftMetrics {
    fn default() -> VioletaBftMetrics {
        VioletaBftMetrics {
            ready: Default::default(),
            message: Default::default(),
            message_dropped: Default::default(),
            propose: Default::default(),
            process_ready: PEER_VIOLETABFT_PROCESS_DURATION
                .with_label_values(&["ready"])
                .local(),
            applightlike_log: PEER_APPEND_LOG_HISTOGRAM.local(),
            commit_log: PEER_COMMIT_LOG_HISTOGRAM.local(),
            leader_missing: Arc::default(),
            invalid_proposal: Default::default(),
        }
    }
}

impl VioletaBftMetrics {
    /// Flushs all metrics
    pub fn flush(&mut self) {
        self.ready.flush();
        self.message.flush();
        self.propose.flush();
        self.process_ready.flush();
        self.applightlike_log.flush();
        self.commit_log.flush();
        self.message_dropped.flush();
        self.invalid_proposal.flush();
        let mut missing = self.leader_missing.dagger().unwrap();
        LEADER_MISSING.set(missing.len() as i64);
        missing.clear();
    }
}
