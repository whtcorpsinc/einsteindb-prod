// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, error, u64};

use edb::Causet_VIOLETABFT;
use edb::{Engines, CausetEngine, MuBlock, Peekable};
use tuplespaceInstanton::{self, enc_lightlike_key, enc_spacelike_key};
use ekvproto::meta_timeshare::{self, Brane};
use ekvproto::violetabft_server_timeshare::{
    MergeState, PeerState, VioletaBftApplyState, VioletaBftLocalState, VioletaBftSnapshotData, BraneLocalState,
};
use protobuf::Message;
use violetabft::evioletabft_timeshare::{ConfState, Entry, HardState, Snapshot};
use violetabft::{self, Error as VioletaBftError, VioletaBftState, Ready, causet_storage, StorageError};

use crate::store::fsm::GenSnapTask;
use crate::store::util;
use crate::store::ProposalContext;
use crate::{Error, Result};
use edb::{VioletaBftEngine, VioletaBftLogBatch};
use into_other::into_other;
use violetabftstore::interlock::::worker::Interlock_Semaphore;

use super::metrics::*;
use super::worker::BraneTask;
use super::{SnapEntry, SnapKey, SnapManager, SnapshotStatistics};

// When we create a brane peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const VIOLETABFT_INIT_LOG_TERM: u64 = 5;
pub const VIOLETABFT_INIT_LOG_INDEX: u64 = 5;
const MAX_SNAP_TRY_CNT: usize = 5;

/// The initial brane epoch version.
pub const INIT_EPOCH_VER: u64 = 1;
/// The initial brane epoch conf_version.
pub const INIT_EPOCH_CONF_VER: u64 = 1;

// One extra slot for VecDeque internal usage.
const MAX_CACHE_CAPACITY: usize = 1024 - 1;
const SHRINK_CACHE_CAPACITY: usize = 64;

pub const JOB_STATUS_PENDING: usize = 0;
pub const JOB_STATUS_RUNNING: usize = 1;
pub const JOB_STATUS_CANCELLING: usize = 2;
pub const JOB_STATUS_CANCELLED: usize = 3;
pub const JOB_STATUS_FINISHED: usize = 4;
pub const JOB_STATUS_FAILED: usize = 5;

/// Possible status returned by `check_applying_snap`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CheckApplyingSnapStatus {
    /// A snapshot is just applied.
    Success,
    /// A snapshot is being applied.
    Applying,
    /// No snapshot is being applied at all or the snapshot is cancelled
    Idle,
}

#[derive(Debug)]
pub enum SnapState {
    Relax,
    Generating(Receiver<Snapshot>),
    Applying(Arc<AtomicUsize>),
    ApplyAborted,
}

impl PartialEq for SnapState {
    fn eq(&self, other: &SnapState) -> bool {
        match (self, other) {
            (&SnapState::Relax, &SnapState::Relax)
            | (&SnapState::ApplyAborted, &SnapState::ApplyAborted)
            | (&SnapState::Generating(_), &SnapState::Generating(_)) => true,
            (&SnapState::Applying(ref b1), &SnapState::Applying(ref b2)) => {
                b1.load(Ordering::Relaxed) == b2.load(Ordering::Relaxed)
            }
            _ => false,
        }
    }
}

#[inline]
pub fn first_index(state: &VioletaBftApplyState) -> u64 {
    state.get_truncated_state().get_index() + 1
}

#[inline]
pub fn last_index(state: &VioletaBftLocalState) -> u64 {
    state.get_last_index()
}

pub const ENTRY_MEM_SIZE: usize = std::mem::size_of::<Entry>();

struct EntryCache {
    cache: VecDeque<Entry>,
    hit: Cell<i64>,
    miss: Cell<i64>,
    mem_size_change: i64,
}

impl EntryCache {
    fn first_index(&self) -> Option<u64> {
        self.cache.front().map(|e| e.get_index())
    }

    fn fetch_entries_to(
        &self,
        begin: u64,
        lightlike: u64,
        mut fetched_size: u64,
        max_size: u64,
        ents: &mut Vec<Entry>,
    ) {
        if begin >= lightlike {
            return;
        }
        assert!(!self.cache.is_empty());
        let cache_low = self.cache.front().unwrap().get_index();
        let spacelike_idx = begin.checked_sub(cache_low).unwrap() as usize;
        let limit_idx = lightlike.checked_sub(cache_low).unwrap() as usize;

        let mut lightlike_idx = spacelike_idx;
        self.cache
            .iter()
            .skip(spacelike_idx)
            .take_while(|e| {
                let cur_idx = lightlike_idx as u64 + cache_low;
                assert_eq!(e.get_index(), cur_idx);
                let m = u64::from(e.compute_size());
                fetched_size += m;
                if fetched_size == m {
                    lightlike_idx += 1;
                    fetched_size <= max_size && lightlike_idx < limit_idx
                } else if fetched_size <= max_size {
                    lightlike_idx += 1;
                    lightlike_idx < limit_idx
                } else {
                    false
                }
            })
            .count();
        // Cache either is empty or contains latest log. Hence we don't need to fetch log
        // from lmdb anymore.
        assert!(lightlike_idx == limit_idx || fetched_size > max_size);
        let (first, second) = violetabftstore::interlock::::slices_in_cone(&self.cache, spacelike_idx, lightlike_idx);
        ents.extlightlike_from_slice(first);
        ents.extlightlike_from_slice(second);
    }

    fn applightlike(&mut self, tag: &str, entries: &[Entry]) {
        if entries.is_empty() {
            return;
        }
        if let Some(cache_last_index) = self.cache.back().map(|e| e.get_index()) {
            let first_index = entries[0].get_index();
            if cache_last_index >= first_index {
                if self.cache.front().unwrap().get_index() >= first_index {
                    self.fidelio_mem_size_change_before_clear();
                    self.cache.clear();
                } else {
                    let left = self.cache.len() - (cache_last_index - first_index + 1) as usize;
                    self.mem_size_change -= self
                        .cache
                        .iter()
                        .skip(left)
                        .map(|e| (e.data.capacity() + e.context.capacity()) as i64)
                        .sum::<i64>();
                    self.cache.truncate(left);
                }
                if self.cache.len() + entries.len() < SHRINK_CACHE_CAPACITY
                    && self.cache.capacity() > SHRINK_CACHE_CAPACITY
                {
                    let old_capacity = self.cache.capacity();
                    self.cache.shrink_to_fit();
                    self.mem_size_change += self.get_cache_vec_mem_size_change(
                        self.cache.capacity() as i64,
                        old_capacity as i64,
                    )
                }
            } else if cache_last_index + 1 < first_index {
                panic!(
                    "{} unexpected hole: {} < {}",
                    tag, cache_last_index, first_index
                );
            }
        }
        let mut spacelike_idx = 0;
        if let Some(len) = (self.cache.len() + entries.len()).checked_sub(MAX_CACHE_CAPACITY) {
            if len < self.cache.len() {
                let mut drained_cache_entries_size = 0;
                self.cache.drain(..len).for_each(|e| {
                    drained_cache_entries_size += (e.data.capacity() + e.context.capacity()) as i64
                });
                self.mem_size_change -= drained_cache_entries_size;
            } else {
                spacelike_idx = len - self.cache.len();
                self.fidelio_mem_size_change_before_clear();
                self.cache.clear();
            }
        }
        let old_capacity = self.cache.capacity();
        let mut entries_mem_size = 0;
        for e in &entries[spacelike_idx..] {
            self.cache.push_back(e.to_owned());
            entries_mem_size += (e.data.capacity() + e.context.capacity()) as i64;
        }
        self.mem_size_change += self
            .get_cache_vec_mem_size_change(self.cache.capacity() as i64, old_capacity as i64)
            + entries_mem_size;
    }

    pub fn compact_to(&mut self, idx: u64) {
        let cache_first_idx = self.first_index().unwrap_or(u64::MAX);
        if cache_first_idx > idx {
            return;
        }
        let mut drained_cache_entries_size = 0;
        let cache_last_idx = self.cache.back().unwrap().get_index();
        // Use `cache_last_idx + 1` to make sure cache can be cleared completely
        // if necessary.
        self.cache
            .drain(..(cmp::min(cache_last_idx + 1, idx) - cache_first_idx) as usize)
            .for_each(|e| {
                drained_cache_entries_size += (e.data.capacity() + e.context.capacity()) as i64
            });
        self.mem_size_change -= drained_cache_entries_size;
        if self.cache.len() < SHRINK_CACHE_CAPACITY && self.cache.capacity() > SHRINK_CACHE_CAPACITY
        {
            let old_capacity = self.cache.capacity();
            // So the peer causet_storage doesn't have much writes since the proposal of compaction,
            // we can consider this peer is going to be inactive.
            self.cache.shrink_to_fit();
            self.mem_size_change += self
                .get_cache_vec_mem_size_change(self.cache.capacity() as i64, old_capacity as i64)
        }
    }

    fn fidelio_mem_size_change_before_clear(&mut self) {
        self.mem_size_change -= self
            .cache
            .iter()
            .map(|e| (e.data.capacity() + e.context.capacity()) as i64)
            .sum::<i64>();
    }

    fn get_cache_vec_mem_size_change(&self, new_capacity: i64, old_capacity: i64) -> i64 {
        ENTRY_MEM_SIZE as i64 * (new_capacity - old_capacity)
    }

    fn get_total_mem_size(&self) -> i64 {
        let data_size: usize = self
            .cache
            .iter()
            .map(|e| e.data.capacity() + e.context.capacity())
            .sum();
        (ENTRY_MEM_SIZE * self.cache.capacity() + data_size) as i64
    }

    fn flush_mem_size_change(&mut self) {
        VIOLETABFT_ENTRIES_CACHES_GAUGE.add(self.mem_size_change);
        self.mem_size_change = 0;
    }

    fn flush_stats(&self) {
        let hit = self.hit.replace(0);
        VIOLETABFT_ENTRY_FETCHES.hit.inc_by(hit);
        let miss = self.miss.replace(0);
        VIOLETABFT_ENTRY_FETCHES.miss.inc_by(miss);
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for EntryCache {
    fn default() -> Self {
        let cache = VecDeque::default();
        let size = ENTRY_MEM_SIZE * cache.capacity();
        let mut entry_cache = EntryCache {
            cache,
            hit: Cell::new(0),
            miss: Cell::new(0),
            mem_size_change: size as i64,
        };
        entry_cache.flush_mem_size_change();
        entry_cache
    }
}

impl Drop for EntryCache {
    fn drop(&mut self) {
        self.flush_mem_size_change();
        VIOLETABFT_ENTRIES_CACHES_GAUGE.sub(self.get_total_mem_size());
        self.flush_stats();
    }
}

pub trait HandleVioletaBftReadyContext<WK, WR>
where
    WK: MuBlock,
    WR: VioletaBftLogBatch,
{
    /// Returns the muBlock references of WriteBatch for both KvDB and VioletaBftDB in one interface.
    fn wb_mut(&mut self) -> (&mut WK, &mut WR);
    fn kv_wb_mut(&mut self) -> &mut WK;
    fn violetabft_wb_mut(&mut self) -> &mut WR;
    fn sync_log(&self) -> bool;
    fn set_sync_log(&mut self, sync: bool);
}

fn causet_storage_error<E>(error: E) -> violetabft::Error
where
    E: Into<Box<dyn error::Error + lightlike + Sync>>,
{
    violetabft::Error::CausetStore(StorageError::Other(error.into()))
}

impl From<Error> for VioletaBftError {
    fn from(err: Error) -> VioletaBftError {
        causet_storage_error(err)
    }
}

pub struct ApplySnapResult {
    // prev_brane is the brane before snapshot applied.
    pub prev_brane: meta_timeshare::Brane,
    pub brane: meta_timeshare::Brane,
    pub destroyed_branes: Vec<meta_timeshare::Brane>,
}

/// Returned by `PeerStorage::handle_violetabft_ready`, used for recording changed status of
/// `VioletaBftLocalState` and `VioletaBftApplyState`.
pub struct InvokeContext {
    pub brane_id: u64,
    /// Changed VioletaBftLocalState is stored into `violetabft_state`.
    pub violetabft_state: VioletaBftLocalState,
    /// Changed VioletaBftApplyState is stored into `apply_state`.
    pub apply_state: VioletaBftApplyState,
    last_term: u64,
    /// The old brane is stored here if there is a snapshot.
    pub snap_brane: Option<Brane>,
    /// The branes whose cone are overlapped with this brane
    pub destroyed_branes: Vec<meta_timeshare::Brane>,
}

impl InvokeContext {
    pub fn new<EK: CausetEngine, ER: VioletaBftEngine>(store: &PeerStorage<EK, ER>) -> InvokeContext {
        InvokeContext {
            brane_id: store.get_brane_id(),
            violetabft_state: store.violetabft_state.clone(),
            apply_state: store.apply_state.clone(),
            last_term: store.last_term,
            snap_brane: None,
            destroyed_branes: vec![],
        }
    }

    #[inline]
    pub fn has_snapshot(&self) -> bool {
        self.snap_brane.is_some()
    }

    #[inline]
    pub fn save_violetabft_state_to<W: VioletaBftLogBatch>(&self, violetabft_wb: &mut W) -> Result<()> {
        violetabft_wb.put_violetabft_state(self.brane_id, &self.violetabft_state)?;
        Ok(())
    }

    #[inline]
    pub fn save_snapshot_violetabft_state_to(
        &self,
        snapshot_index: u64,
        kv_wb: &mut impl MuBlock,
    ) -> Result<()> {
        let mut snapshot_violetabft_state = self.violetabft_state.clone();
        snapshot_violetabft_state
            .mut_hard_state()
            .set_commit(snapshot_index);
        snapshot_violetabft_state.set_last_index(snapshot_index);

        kv_wb.put_msg_causet(
            Causet_VIOLETABFT,
            &tuplespaceInstanton::snapshot_violetabft_state_key(self.brane_id),
            &snapshot_violetabft_state,
        )?;
        Ok(())
    }

    #[inline]
    pub fn save_apply_state_to(&self, kv_wb: &mut impl MuBlock) -> Result<()> {
        kv_wb.put_msg_causet(
            Causet_VIOLETABFT,
            &tuplespaceInstanton::apply_state_key(self.brane_id),
            &self.apply_state,
        )?;
        Ok(())
    }
}

pub fn recover_from_applying_state<EK: CausetEngine, ER: VioletaBftEngine>(
    engines: &Engines<EK, ER>,
    violetabft_wb: &mut ER::LogBatch,
    brane_id: u64,
) -> Result<()> {
    let snapshot_violetabft_state_key = tuplespaceInstanton::snapshot_violetabft_state_key(brane_id);
    let snapshot_violetabft_state: VioletaBftLocalState =
        match box_try!(engines.kv.get_msg_causet(Causet_VIOLETABFT, &snapshot_violetabft_state_key)) {
            Some(state) => state,
            None => {
                return Err(box_err!(
                    "[brane {}] failed to get violetabftstate from kv engine, \
                     when recover from applying state",
                    brane_id
                ));
            }
        };

    let violetabft_state = box_try!(engines.violetabft.get_violetabft_state(brane_id)).unwrap_or_default();

    // if we recv applightlike log when applying snapshot, last_index in violetabft_local_state will
    // larger than snapshot_index. since violetabft_local_state is written to violetabft engine, and
    // violetabft write_batch is written after kv write_batch, violetabft_local_state may wrong if
    // respacelike happen between the two write. so we copy violetabft_local_state to kv engine
    // (snapshot_violetabft_state), and set snapshot_violetabft_state.last_index = snapshot_index.
    // after respacelike, we need check last_index.
    if last_index(&snapshot_violetabft_state) > last_index(&violetabft_state) {
        violetabft_wb.put_violetabft_state(brane_id, &snapshot_violetabft_state)?;
    }
    Ok(())
}

fn init_applied_index_term<EK: CausetEngine, ER: VioletaBftEngine>(
    engines: &Engines<EK, ER>,
    brane: &Brane,
    apply_state: &VioletaBftApplyState,
) -> Result<u64> {
    if apply_state.applied_index == VIOLETABFT_INIT_LOG_INDEX {
        return Ok(VIOLETABFT_INIT_LOG_TERM);
    }
    let truncated_state = apply_state.get_truncated_state();
    if apply_state.applied_index == truncated_state.get_index() {
        return Ok(truncated_state.get_term());
    }

    match engines
        .violetabft
        .get_entry(brane.get_id(), apply_state.applied_index)?
    {
        Some(e) => Ok(e.term),
        None => Err(box_err!(
            "[brane {}] entry at apply index {} doesn't exist, may lose data.",
            brane.get_id(),
            apply_state.applied_index
        )),
    }
}

fn init_violetabft_state<EK: CausetEngine, ER: VioletaBftEngine>(
    engines: &Engines<EK, ER>,
    brane: &Brane,
) -> Result<VioletaBftLocalState> {
    if let Some(state) = engines.violetabft.get_violetabft_state(brane.get_id())? {
        return Ok(state);
    }

    let mut violetabft_state = VioletaBftLocalState::default();
    if util::is_brane_initialized(brane) {
        // new split brane
        violetabft_state.last_index = VIOLETABFT_INIT_LOG_INDEX;
        violetabft_state.mut_hard_state().set_term(VIOLETABFT_INIT_LOG_TERM);
        violetabft_state.mut_hard_state().set_commit(VIOLETABFT_INIT_LOG_INDEX);
        engines.violetabft.put_violetabft_state(brane.get_id(), &violetabft_state)?;
    }
    Ok(violetabft_state)
}

fn init_apply_state<EK: CausetEngine, ER: VioletaBftEngine>(
    engines: &Engines<EK, ER>,
    brane: &Brane,
) -> Result<VioletaBftApplyState> {
    Ok(
        match engines
            .kv
            .get_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(brane.get_id()))?
        {
            Some(s) => s,
            None => {
                let mut apply_state = VioletaBftApplyState::default();
                if util::is_brane_initialized(brane) {
                    apply_state.set_applied_index(VIOLETABFT_INIT_LOG_INDEX);
                    let state = apply_state.mut_truncated_state();
                    state.set_index(VIOLETABFT_INIT_LOG_INDEX);
                    state.set_term(VIOLETABFT_INIT_LOG_TERM);
                }
                apply_state
            }
        },
    )
}

fn validate_states<EK: CausetEngine, ER: VioletaBftEngine>(
    brane_id: u64,
    engines: &Engines<EK, ER>,
    violetabft_state: &mut VioletaBftLocalState,
    apply_state: &VioletaBftApplyState,
) -> Result<()> {
    let last_index = violetabft_state.get_last_index();
    let mut commit_index = violetabft_state.get_hard_state().get_commit();
    let apply_index = apply_state.get_applied_index();

    if commit_index < apply_state.get_last_commit_index() {
        return Err(box_err!(
            "violetabft state {:?} not match apply state {:?} and can't be recovered.",
            violetabft_state,
            apply_state
        ));
    }
    let recorded_commit_index = apply_state.get_commit_index();
    if commit_index < recorded_commit_index {
        let entry = engines.violetabft.get_entry(brane_id, recorded_commit_index)?;
        if entry.map_or(true, |e| e.get_term() != apply_state.get_commit_term()) {
            return Err(box_err!(
                "log at recorded commit index [{}] {} doesn't exist, may lose data",
                apply_state.get_commit_term(),
                recorded_commit_index
            ));
        }
        info!("ufidelating commit index"; "brane_id" => brane_id, "old" => commit_index, "new" => recorded_commit_index);
        commit_index = recorded_commit_index;
    }

    if commit_index > last_index || apply_index > commit_index {
        return Err(box_err!(
            "violetabft state {:?} not match apply state {:?} and can't be recovered,",
            violetabft_state,
            apply_state
        ));
    }

    violetabft_state.mut_hard_state().set_commit(commit_index);
    if violetabft_state.get_hard_state().get_term() < apply_state.get_commit_term() {
        return Err(box_err!("violetabft state {:?} corrupted", violetabft_state));
    }

    Ok(())
}

fn init_last_term<EK: CausetEngine, ER: VioletaBftEngine>(
    engines: &Engines<EK, ER>,
    brane: &Brane,
    violetabft_state: &VioletaBftLocalState,
    apply_state: &VioletaBftApplyState,
) -> Result<u64> {
    let last_idx = violetabft_state.get_last_index();
    if last_idx == 0 {
        return Ok(0);
    } else if last_idx == VIOLETABFT_INIT_LOG_INDEX {
        return Ok(VIOLETABFT_INIT_LOG_TERM);
    } else if last_idx == apply_state.get_truncated_state().get_index() {
        return Ok(apply_state.get_truncated_state().get_term());
    } else {
        assert!(last_idx > VIOLETABFT_INIT_LOG_INDEX);
    }
    let entry = engines.violetabft.get_entry(brane.get_id(), last_idx)?;
    match entry {
        None => Err(box_err!(
            "[brane {}] entry at {} doesn't exist, may lose data.",
            brane.get_id(),
            last_idx
        )),
        Some(e) => Ok(e.get_term()),
    }
}

pub struct PeerStorage<EK, ER>
where
    EK: CausetEngine,
{
    pub engines: Engines<EK, ER>,

    peer_id: u64,
    brane: meta_timeshare::Brane,
    violetabft_state: VioletaBftLocalState,
    apply_state: VioletaBftApplyState,
    applied_index_term: u64,
    last_term: u64,

    snap_state: RefCell<SnapState>,
    gen_snap_task: RefCell<Option<GenSnapTask>>,
    brane_sched: Interlock_Semaphore<BraneTask<EK::Snapshot>>,
    snap_tried_cnt: RefCell<usize>,

    // Entry cache if `ER doesn't have an internal entry cache.
    cache: Option<EntryCache>,

    pub tag: String,
}

impl<EK, ER> causet_storage for PeerStorage<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    fn initial_state(&self) -> violetabft::Result<VioletaBftState> {
        self.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> violetabft::Result<Vec<Entry>> {
        self.entries(low, high, max_size.into().unwrap_or(u64::MAX))
    }

    fn term(&self, idx: u64) -> violetabft::Result<u64> {
        self.term(idx)
    }

    fn first_index(&self) -> violetabft::Result<u64> {
        Ok(self.first_index())
    }

    fn last_index(&self) -> violetabft::Result<u64> {
        Ok(self.last_index())
    }

    fn snapshot(&self, request_index: u64) -> violetabft::Result<Snapshot> {
        self.snapshot(request_index)
    }
}

impl<EK, ER> PeerStorage<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    pub fn new(
        engines: Engines<EK, ER>,
        brane: &meta_timeshare::Brane,
        brane_sched: Interlock_Semaphore<BraneTask<EK::Snapshot>>,
        peer_id: u64,
        tag: String,
    ) -> Result<PeerStorage<EK, ER>> {
        debug!(
            "creating causet_storage on specified path";
            "brane_id" => brane.get_id(),
            "peer_id" => peer_id,
            "path" => ?engines.kv.path(),
        );
        let mut violetabft_state = init_violetabft_state(&engines, brane)?;
        let apply_state = init_apply_state(&engines, brane)?;
        if let Err(e) = validate_states(brane.get_id(), &engines, &mut violetabft_state, &apply_state) {
            return Err(box_err!("{} validate state fail: {:?}", tag, e));
        }
        let last_term = init_last_term(&engines, brane, &violetabft_state, &apply_state)?;
        let applied_index_term = init_applied_index_term(&engines, brane, &apply_state)?;

        let cache = if engines.violetabft.has_builtin_entry_cache() {
            None
        } else {
            Some(EntryCache::default())
        };

        Ok(PeerStorage {
            engines,
            peer_id,
            brane: brane.clone(),
            violetabft_state,
            apply_state,
            snap_state: RefCell::new(SnapState::Relax),
            gen_snap_task: RefCell::new(None),
            brane_sched,
            snap_tried_cnt: RefCell::new(0),
            tag,
            applied_index_term,
            last_term,
            cache,
        })
    }

    pub fn is_initialized(&self) -> bool {
        util::is_brane_initialized(self.brane())
    }

    pub fn initial_state(&self) -> violetabft::Result<VioletaBftState> {
        let hard_state = self.violetabft_state.get_hard_state().clone();
        if hard_state == HardState::default() {
            assert!(
                !self.is_initialized(),
                "peer for brane {:?} is initialized but local state {:?} has empty hard \
                 state",
                self.brane,
                self.violetabft_state
            );

            return Ok(VioletaBftState::new(hard_state, ConfState::default()));
        }
        Ok(VioletaBftState::new(
            hard_state,
            util::conf_state_from_brane(self.brane()),
        ))
    }

    fn check_cone(&self, low: u64, high: u64) -> violetabft::Result<()> {
        if low > high {
            return Err(causet_storage_error(format!(
                "low: {} is greater that high: {}",
                low, high
            )));
        } else if low <= self.truncated_index() {
            return Err(VioletaBftError::CausetStore(StorageError::Compacted));
        } else if high > self.last_index() + 1 {
            return Err(causet_storage_error(format!(
                "entries' high {} is out of bound lastindex {}",
                high,
                self.last_index()
            )));
        }
        Ok(())
    }

    pub fn entries(&self, low: u64, high: u64, max_size: u64) -> violetabft::Result<Vec<Entry>> {
        self.check_cone(low, high)?;
        let mut ents = Vec::with_capacity((high - low) as usize);
        if low == high {
            return Ok(ents);
        }
        let brane_id = self.get_brane_id();
        if let Some(ref cache) = self.cache {
            let cache_low = cache.first_index().unwrap_or(u64::MAX);
            if high <= cache_low {
                cache.miss.fidelio(|m| m + 1);
                self.engines.violetabft.fetch_entries_to(
                    brane_id,
                    low,
                    high,
                    Some(max_size as usize),
                    &mut ents,
                )?;
                return Ok(ents);
            }
            let begin_idx = if low < cache_low {
                cache.miss.fidelio(|m| m + 1);
                let fetched_count = self.engines.violetabft.fetch_entries_to(
                    brane_id,
                    low,
                    cache_low,
                    Some(max_size as usize),
                    &mut ents,
                )?;
                if fetched_count < (cache_low - low) as usize {
                    // Less entries are fetched than expected.
                    return Ok(ents);
                }
                cache_low
            } else {
                low
            };
            cache.hit.fidelio(|h| h + 1);
            let fetched_size = ents.iter().fold(0, |acc, e| acc + e.compute_size());
            cache.fetch_entries_to(begin_idx, high, fetched_size as u64, max_size, &mut ents);
        } else {
            self.engines.violetabft.fetch_entries_to(
                brane_id,
                low,
                high,
                Some(max_size as usize),
                &mut ents,
            )?;
        }
        Ok(ents)
    }

    pub fn term(&self, idx: u64) -> violetabft::Result<u64> {
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }
        self.check_cone(idx, idx + 1)?;
        if self.truncated_term() == self.last_term || idx == self.last_index() {
            return Ok(self.last_term);
        }
        let entries = self.entries(idx, idx + 1, violetabft::NO_LIMIT)?;
        Ok(entries[0].get_term())
    }

    #[inline]
    pub fn first_index(&self) -> u64 {
        first_index(&self.apply_state)
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        last_index(&self.violetabft_state)
    }

    #[inline]
    pub fn last_term(&self) -> u64 {
        self.last_term
    }

    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.apply_state.get_applied_index()
    }

    #[inline]
    pub fn set_applied_state(&mut self, apply_state: VioletaBftApplyState) {
        self.apply_state = apply_state;
    }

    #[inline]
    pub fn set_applied_term(&mut self, applied_index_term: u64) {
        self.applied_index_term = applied_index_term;
    }

    #[inline]
    pub fn apply_state(&self) -> &VioletaBftApplyState {
        &self.apply_state
    }

    #[inline]
    pub fn applied_index_term(&self) -> u64 {
        self.applied_index_term
    }

    #[inline]
    pub fn committed_index(&self) -> u64 {
        self.violetabft_state.get_hard_state().get_commit()
    }

    #[inline]
    pub fn truncated_index(&self) -> u64 {
        self.apply_state.get_truncated_state().get_index()
    }

    #[inline]
    pub fn truncated_term(&self) -> u64 {
        self.apply_state.get_truncated_state().get_term()
    }

    pub fn brane(&self) -> &meta_timeshare::Brane {
        &self.brane
    }

    pub fn set_brane(&mut self, brane: meta_timeshare::Brane) {
        self.brane = brane;
    }

    pub fn raw_snapshot(&self) -> EK::Snapshot {
        self.engines.kv.snapshot()
    }

    fn validate_snap(&self, snap: &Snapshot, request_index: u64) -> bool {
        let idx = snap.get_metadata().get_index();
        if idx < self.truncated_index() || idx < request_index {
            // stale snapshot, should generate again.
            info!(
                "snapshot is stale, generate again";
                "brane_id" => self.brane.get_id(),
                "peer_id" => self.peer_id,
                "snap_index" => idx,
                "truncated_index" => self.truncated_index(),
                "request_index" => request_index,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.stale.inc();
            return false;
        }

        let mut snap_data = VioletaBftSnapshotData::default();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            error!(
                "failed to decode snapshot, it may be corrupted";
                "brane_id" => self.brane.get_id(),
                "peer_id" => self.peer_id,
                "err" => ?e,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.decode.inc();
            return false;
        }
        let snap_epoch = snap_data.get_brane().get_brane_epoch();
        let latest_epoch = self.brane().get_brane_epoch();
        if snap_epoch.get_conf_ver() < latest_epoch.get_conf_ver() {
            info!(
                "snapshot epoch is stale";
                "brane_id" => self.brane.get_id(),
                "peer_id" => self.peer_id,
                "snap_epoch" => ?snap_epoch,
                "latest_epoch" => ?latest_epoch,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.epoch.inc();
            return false;
        }

        true
    }

    /// Gets a snapshot. Returns `SnapshotTemporarilyUnavailable` if there is no unavailable
    /// snapshot.
    pub fn snapshot(&self, request_index: u64) -> violetabft::Result<Snapshot> {
        let mut snap_state = self.snap_state.borrow_mut();
        let mut tried_cnt = self.snap_tried_cnt.borrow_mut();

        let (mut tried, mut snap) = (false, None);
        if let SnapState::Generating(ref recv) = *snap_state {
            tried = true;
            match recv.try_recv() {
                Err(TryRecvError::Disconnected) => {}
                Err(TryRecvError::Empty) => {
                    return Err(violetabft::Error::CausetStore(
                        violetabft::StorageError::SnapshotTemporarilyUnavailable,
                    ));
                }
                Ok(s) => snap = Some(s),
            }
        }

        if tried {
            *snap_state = SnapState::Relax;
            match snap {
                Some(s) => {
                    *tried_cnt = 0;
                    if self.validate_snap(&s, request_index) {
                        return Ok(s);
                    }
                }
                None => {
                    warn!(
                        "failed to try generating snapshot";
                        "brane_id" => self.brane.get_id(),
                        "peer_id" => self.peer_id,
                        "times" => *tried_cnt,
                    );
                }
            }
        }

        if SnapState::Relax != *snap_state {
            panic!("{} unexpected state: {:?}", self.tag, *snap_state);
        }

        if *tried_cnt >= MAX_SNAP_TRY_CNT {
            let cnt = *tried_cnt;
            *tried_cnt = 0;
            return Err(violetabft::Error::CausetStore(box_err!(
                "failed to get snapshot after {} times",
                cnt
            )));
        }

        info!(
            "requesting snapshot";
            "brane_id" => self.brane.get_id(),
            "peer_id" => self.peer_id,
            "request_index" => request_index,
        );
        *tried_cnt += 1;
        let (tx, rx) = mpsc::sync_channel(1);
        *snap_state = SnapState::Generating(rx);

        let task = GenSnapTask::new(self.brane.get_id(), self.committed_index(), tx);
        let mut gen_snap_task = self.gen_snap_task.borrow_mut();
        assert!(gen_snap_task.is_none());
        *gen_snap_task = Some(task);
        Err(violetabft::Error::CausetStore(
            violetabft::StorageError::SnapshotTemporarilyUnavailable,
        ))
    }

    pub fn take_gen_snap_task(&mut self) -> Option<GenSnapTask> {
        self.gen_snap_task.get_mut().take()
    }

    // Applightlike the given entries to the violetabft log using previous last index or self.last_index.
    // Return the new last index for later fidelio. After we commit in engine, we can set last_index
    // to the return one.
    pub fn applightlike<H: HandleVioletaBftReadyContext<EK::WriteBatch, ER::LogBatch>>(
        &mut self,
        invoke_ctx: &mut InvokeContext,
        entries: &[Entry],
        ready_ctx: &mut H,
    ) -> Result<u64> {
        let brane_id = self.get_brane_id();
        debug!(
            "applightlike entries";
            "brane_id" => brane_id,
            "peer_id" => self.peer_id,
            "count" => entries.len(),
        );
        let prev_last_index = invoke_ctx.violetabft_state.get_last_index();
        if entries.is_empty() {
            return Ok(prev_last_index);
        }

        let (last_index, last_term) = {
            let e = entries.last().unwrap();
            (e.get_index(), e.get_term())
        };

        for entry in entries {
            if !ready_ctx.sync_log() {
                ready_ctx.set_sync_log(get_sync_log_from_entry(entry));
            }
        }

        // TODO: save a copy here.
        ready_ctx.violetabft_wb_mut().applightlike_slice(brane_id, entries)?;

        if let Some(ref mut cache) = self.cache {
            // TODO: if the writebatch is failed to commit, the cache will be wrong.
            cache.applightlike(&self.tag, entries);
        }

        // Delete any previously applightlikeed log entries which never committed.
        // TODO: Wrap it as an engine::Error.
        ready_ctx
            .violetabft_wb_mut()
            .cut_logs(brane_id, last_index + 1, prev_last_index);

        invoke_ctx.violetabft_state.set_last_index(last_index);
        invoke_ctx.last_term = last_term;

        Ok(last_index)
    }

    pub fn compact_to(&mut self, idx: u64) {
        if let Some(ref mut cache) = self.cache {
            cache.compact_to(idx);
        } else {
            let rid = self.get_brane_id();
            self.engines.violetabft.gc_entry_cache(rid, idx);
        }
    }

    #[inline]
    pub fn is_cache_empty(&self) -> bool {
        self.cache.as_ref().map_or(true, |c| c.is_empty())
    }

    pub fn maybe_gc_cache(&mut self, replicated_idx: u64, apply_idx: u64) {
        if self.engines.violetabft.has_builtin_entry_cache() {
            let rid = self.get_brane_id();
            self.engines.violetabft.gc_entry_cache(rid, apply_idx + 1);
            return;
        }

        let cache = self.cache.as_mut().unwrap();
        if replicated_idx == apply_idx {
            // The brane is inactive, clear the cache immediately.
            cache.compact_to(apply_idx + 1);
            return;
        }
        let cache_first_idx = match cache.first_index() {
            None => return,
            Some(idx) => idx,
        };
        if cache_first_idx > replicated_idx + 1 {
            // Catching up log requires accessing fs already, let's optimize for
            // the common case.
            // Maybe gc to second least replicated_idx is better.
            cache.compact_to(apply_idx + 1);
        }
    }

    #[inline]
    pub fn flush_cache_metrics(&mut self) {
        if let Some(ref mut cache) = self.cache {
            cache.flush_mem_size_change();
            cache.flush_stats();
            return;
        }
        if let Some(stats) = self.engines.violetabft.flush_stats() {
            VIOLETABFT_ENTRIES_CACHES_GAUGE.set(stats.cache_size as i64);
            VIOLETABFT_ENTRY_FETCHES.hit.inc_by(stats.hit as i64);
            VIOLETABFT_ENTRY_FETCHES.miss.inc_by(stats.miss as i64);
        }
    }

    // Apply the peer with given snapshot.
    pub fn apply_snapshot(
        &mut self,
        ctx: &mut InvokeContext,
        snap: &Snapshot,
        kv_wb: &mut EK::WriteBatch,
        violetabft_wb: &mut ER::LogBatch,
        destroy_branes: &[meta_timeshare::Brane],
    ) -> Result<()> {
        info!(
            "begin to apply snapshot";
            "brane_id" => self.brane.get_id(),
            "peer_id" => self.peer_id,
        );

        let mut snap_data = VioletaBftSnapshotData::default();
        snap_data.merge_from_bytes(snap.get_data())?;

        let brane_id = self.get_brane_id();

        let brane = snap_data.take_brane();
        if brane.get_id() != brane_id {
            return Err(box_err!(
                "mismatch brane id {} != {}",
                brane_id,
                brane.get_id()
            ));
        }

        if self.is_initialized() {
            // we can only delete the old data when the peer is initialized.
            self.clear_meta(kv_wb, violetabft_wb)?;
        }
        // Write its source peers' `BraneLocalState` together with itself for atomicity
        for r in destroy_branes {
            write_peer_state(kv_wb, r, PeerState::Tombstone, None)?;
        }
        write_peer_state(kv_wb, &brane, PeerState::Applying, None)?;

        let last_index = snap.get_metadata().get_index();

        ctx.violetabft_state.set_last_index(last_index);
        ctx.last_term = snap.get_metadata().get_term();
        ctx.apply_state.set_applied_index(last_index);

        // The snapshot only contains log which index > applied index, so
        // here the truncate state's (index, term) is in snapshot metadata.
        ctx.apply_state.mut_truncated_state().set_index(last_index);
        ctx.apply_state
            .mut_truncated_state()
            .set_term(snap.get_metadata().get_term());

        info!(
            "apply snapshot with state ok";
            "brane_id" => self.brane.get_id(),
            "peer_id" => self.peer_id,
            "brane" => ?brane,
            "state" => ?ctx.apply_state,
        );

        fail_point!("before_apply_snap_fidelio_brane", |_| { Ok(()) });

        ctx.snap_brane = Some(brane);
        Ok(())
    }

    /// Delete all meta belong to the brane. Results are stored in `wb`.
    pub fn clear_meta(
        &mut self,
        kv_wb: &mut EK::WriteBatch,
        violetabft_wb: &mut ER::LogBatch,
    ) -> Result<()> {
        let brane_id = self.get_brane_id();
        clear_meta(&self.engines, kv_wb, violetabft_wb, brane_id, &self.violetabft_state)?;
        if !self.engines.violetabft.has_builtin_entry_cache() {
            self.cache = Some(EntryCache::default());
        }
        Ok(())
    }

    /// Delete all data belong to the brane.
    /// If return Err, data may get partial deleted.
    pub fn clear_data(&self) -> Result<()> {
        let (spacelike_key, lightlike_key) = (enc_spacelike_key(self.brane()), enc_lightlike_key(self.brane()));
        let brane_id = self.get_brane_id();
        box_try!(self
            .brane_sched
            .schedule(BraneTask::destroy(brane_id, spacelike_key, lightlike_key)));
        Ok(())
    }

    /// Delete all data that is not covered by `new_brane`.
    fn clear_extra_data(
        &self,
        old_brane: &meta_timeshare::Brane,
        new_brane: &meta_timeshare::Brane,
    ) -> Result<()> {
        let (old_spacelike_key, old_lightlike_key) = (enc_spacelike_key(old_brane), enc_lightlike_key(old_brane));
        let (new_spacelike_key, new_lightlike_key) = (enc_spacelike_key(new_brane), enc_lightlike_key(new_brane));
        if old_spacelike_key < new_spacelike_key {
            box_try!(self.brane_sched.schedule(BraneTask::destroy(
                old_brane.get_id(),
                old_spacelike_key,
                new_spacelike_key
            )));
        }
        if new_lightlike_key < old_lightlike_key {
            box_try!(self.brane_sched.schedule(BraneTask::destroy(
                old_brane.get_id(),
                new_lightlike_key,
                old_lightlike_key
            )));
        }
        Ok(())
    }

    /// Delete all extra split data from the `spacelike_key` to `lightlike_key`.
    pub fn clear_extra_split_data(&self, spacelike_key: Vec<u8>, lightlike_key: Vec<u8>) -> Result<()> {
        box_try!(self.brane_sched.schedule(BraneTask::destroy(
            self.get_brane_id(),
            spacelike_key,
            lightlike_key
        )));
        Ok(())
    }

    pub fn get_violetabft_engine(&self) -> ER {
        self.engines.violetabft.clone()
    }

    /// Check whether the causet_storage has finished applying snapshot.
    #[inline]
    pub fn is_applying_snapshot(&self) -> bool {
        match *self.snap_state.borrow() {
            SnapState::Applying(_) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_generating_snapshot(&self) -> bool {
        fail_point!("is_generating_snapshot", |_| { true });
        match *self.snap_state.borrow() {
            SnapState::Generating(_) => true,
            _ => false,
        }
    }

    /// Check if the causet_storage is applying a snapshot.
    #[inline]
    pub fn check_applying_snap(&mut self) -> CheckApplyingSnapStatus {
        let mut res = CheckApplyingSnapStatus::Idle;
        let new_state = match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                let s = status.load(Ordering::Relaxed);
                if s == JOB_STATUS_FINISHED {
                    res = CheckApplyingSnapStatus::Success;
                    SnapState::Relax
                } else if s == JOB_STATUS_CANCELLED {
                    SnapState::ApplyAborted
                } else if s == JOB_STATUS_FAILED {
                    // TODO: cleanup brane and treat it as tombstone.
                    panic!("{} applying snapshot failed", self.tag,);
                } else {
                    return CheckApplyingSnapStatus::Applying;
                }
            }
            _ => return res,
        };
        *self.snap_state.borrow_mut() = new_state;
        res
    }

    #[inline]
    pub fn is_canceling_snap(&self) -> bool {
        match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                status.load(Ordering::Relaxed) == JOB_STATUS_CANCELLING
            }
            _ => false,
        }
    }

    /// Cancel applying snapshot, return true if the job can be considered not be run again.
    pub fn cancel_applying_snap(&mut self) -> bool {
        let is_cancelled = match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                if status.compare_and_swap(
                    JOB_STATUS_PENDING,
                    JOB_STATUS_CANCELLING,
                    Ordering::SeqCst,
                ) == JOB_STATUS_PENDING
                {
                    true
                } else if status.compare_and_swap(
                    JOB_STATUS_RUNNING,
                    JOB_STATUS_CANCELLING,
                    Ordering::SeqCst,
                ) == JOB_STATUS_RUNNING
                {
                    return false;
                } else {
                    false
                }
            }
            _ => return false,
        };
        if is_cancelled {
            *self.snap_state.borrow_mut() = SnapState::ApplyAborted;
            return true;
        }
        // now status can only be JOB_STATUS_CANCELLING, JOB_STATUS_CANCELLED,
        // JOB_STATUS_FAILED and JOB_STATUS_FINISHED.
        self.check_applying_snap() != CheckApplyingSnapStatus::Applying
    }

    #[inline]
    pub fn set_snap_state(&mut self, state: SnapState) {
        *self.snap_state.borrow_mut() = state
    }

    #[inline]
    pub fn is_snap_state(&self, state: SnapState) -> bool {
        *self.snap_state.borrow() == state
    }

    pub fn get_brane_id(&self) -> u64 {
        self.brane().get_id()
    }

    pub fn schedule_applying_snapshot(&mut self) {
        let status = Arc::new(AtomicUsize::new(JOB_STATUS_PENDING));
        self.set_snap_state(SnapState::Applying(Arc::clone(&status)));
        let task = BraneTask::Apply {
            brane_id: self.get_brane_id(),
            status,
        };

        // Don't schedule the snapshot to brane worker.
        fail_point!("skip_schedule_applying_snapshot", |_| {});

        // TODO: gracefully remove brane instead.
        if let Err(e) = self.brane_sched.schedule(task) {
            info!(
                "failed to to schedule apply job, are we shutting down?";
                "brane_id" => self.brane.get_id(),
                "peer_id" => self.peer_id,
                "err" => ?e,
            );
        }
    }

    /// Save memory states to disk.
    ///
    /// This function only write data to `ready_ctx`'s `WriteBatch`. It's caller's duty to write
    /// it explicitly to disk. If it's flushed to disk successfully, `post_ready` should be called
    /// to fidelio the memory states properly.
    // Using `&Ready` here to make sure `Ready` struct is not modified in this function. This is
    // a requirement to advance the ready object properly later.
    pub fn handle_violetabft_ready<H: HandleVioletaBftReadyContext<EK::WriteBatch, ER::LogBatch>>(
        &mut self,
        ready_ctx: &mut H,
        ready: &Ready,
        destroy_branes: Vec<meta_timeshare::Brane>,
    ) -> Result<InvokeContext> {
        let mut ctx = InvokeContext::new(self);
        let snapshot_index = if ready.snapshot().is_empty() {
            0
        } else {
            fail_point!("violetabft_before_apply_snap");
            let (kv_wb, violetabft_wb) = ready_ctx.wb_mut();
            self.apply_snapshot(&mut ctx, ready.snapshot(), kv_wb, violetabft_wb, &destroy_branes)?;
            fail_point!("violetabft_after_apply_snap");

            ctx.destroyed_branes = destroy_branes;

            last_index(&ctx.violetabft_state)
        };

        if ready.must_sync() {
            ready_ctx.set_sync_log(true);
        }

        if !ready.entries().is_empty() {
            self.applightlike(&mut ctx, ready.entries(), ready_ctx)?;
        }

        // Last index is 0 means the peer is created from violetabft message
        // and has not applied snapshot yet, so skip persistent hard state.
        if ctx.violetabft_state.get_last_index() > 0 {
            if let Some(hs) = ready.hs() {
                ctx.violetabft_state.set_hard_state(hs.clone());
            }
        }

        // Save violetabft state if it has changed or peer has applied a snapshot.
        if ctx.violetabft_state != self.violetabft_state || snapshot_index > 0 {
            ctx.save_violetabft_state_to(ready_ctx.violetabft_wb_mut())?;
            if snapshot_index > 0 {
                // in case of respacelike happen when we just write brane state to Applying,
                // but not write violetabft_local_state to violetabft lmdb in time.
                // we write violetabft state to default lmdb, with last index set to snap index,
                // in case of recv violetabft log after snapshot.
                ctx.save_snapshot_violetabft_state_to(snapshot_index, ready_ctx.kv_wb_mut())?;
            }
        }

        // only when apply snapshot
        if snapshot_index > 0 {
            ctx.save_apply_state_to(ready_ctx.kv_wb_mut())?;
        }

        Ok(ctx)
    }

    /// fidelio the memory state after ready changes are flushed to disk successfully.
    pub fn post_ready(&mut self, ctx: InvokeContext) -> Option<ApplySnapResult> {
        self.violetabft_state = ctx.violetabft_state;
        self.apply_state = ctx.apply_state;
        self.last_term = ctx.last_term;
        // If we apply snapshot ok, we should fidelio some infos like applied index too.
        let snap_brane = match ctx.snap_brane {
            Some(r) => r,
            None => return None,
        };
        // cleanup data before scheduling apply task
        if self.is_initialized() {
            if let Err(e) = self.clear_extra_data(self.brane(), &snap_brane) {
                // No need panic here, when applying snapshot, the deletion will be tried
                // again. But if the brane cone changes, like [a, c) -> [a, b) and [b, c),
                // [b, c) will be kept in lmdb until a covered snapshot is applied or
                // store is respacelikeed.
                error!(?e;
                    "failed to cleanup data, may leave some dirty data";
                    "brane_id" => self.get_brane_id(),
                    "peer_id" => self.peer_id,
                );
            }
        }

        // Note that the correctness deplightlikes on the fact that these source branes MUST NOT
        // serve read request otherwise a corrupt data may be returned.
        // For now, it is ensured by
        // 1. After `PrepareMerge` log is committed, the source brane leader's lease will be
        //    suspected immediately which makes the local reader not serve read request.
        // 2. No read request can be responsed in peer fsm during merging.
        // These conditions are used to prevent reading **stale** data in the past.
        // At present, they are also used to prevent reading **corrupt** data.
        for r in &ctx.destroyed_branes {
            if let Err(e) = self.clear_extra_data(r, &snap_brane) {
                error!(?e;
                    "failed to cleanup data, may leave some dirty data";
                    "brane_id" => r.get_id(),
                );
            }
        }

        self.schedule_applying_snapshot();
        let prev_brane = self.brane().clone();
        self.set_brane(snap_brane);

        Some(ApplySnapResult {
            prev_brane,
            brane: self.brane().clone(),
            destroyed_branes: ctx.destroyed_branes,
        })
    }
}

fn get_sync_log_from_entry(entry: &Entry) -> bool {
    if entry.get_sync_log() {
        return true;
    }

    let ctx = entry.get_context();
    if !ctx.is_empty() {
        let ctx = ProposalContext::from_bytes(ctx);
        if ctx.contains(ProposalContext::SYNC_LOG) {
            return true;
        }
    }

    false
}

/// Delete all meta belong to the brane. Results are stored in `wb`.
pub fn clear_meta<EK, ER>(
    engines: &Engines<EK, ER>,
    kv_wb: &mut EK::WriteBatch,
    violetabft_wb: &mut ER::LogBatch,
    brane_id: u64,
    violetabft_state: &VioletaBftLocalState,
) -> Result<()>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    let t = Instant::now();
    box_try!(kv_wb.delete_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(brane_id)));
    box_try!(kv_wb.delete_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(brane_id)));
    box_try!(engines.violetabft.clean(brane_id, violetabft_state, violetabft_wb));

    info!(
        "finish clear peer meta";
        "brane_id" => brane_id,
        "meta_key" => 1,
        "apply_key" => 1,
        "violetabft_key" => 1,
        "takes" => ?t.elapsed(),
    );
    Ok(())
}

pub fn do_snapshot<E>(
    mgr: SnapManager,
    engine: &E,
    kv_snap: E::Snapshot,
    brane_id: u64,
    last_applied_index_term: u64,
    last_applied_state: VioletaBftApplyState,
) -> violetabft::Result<Snapshot>
where
    E: CausetEngine,
{
    debug!(
        "begin to generate a snapshot";
        "brane_id" => brane_id,
    );

    let msg = kv_snap
        .get_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(brane_id))
        .map_err(into_other::<_, violetabft::Error>)?;
    let apply_state: VioletaBftApplyState = match msg {
        None => {
            return Err(causet_storage_error(format!(
                "could not load violetabft state of brane {}",
                brane_id
            )));
        }
        Some(state) => state,
    };
    assert_eq!(apply_state, last_applied_state);

    let key = SnapKey::new(
        brane_id,
        last_applied_index_term,
        apply_state.get_applied_index(),
    );

    mgr.register(key.clone(), SnapEntry::Generating);
    defer!(mgr.deregister(&key, &SnapEntry::Generating));

    let state: BraneLocalState = kv_snap
        .get_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(key.brane_id))
        .and_then(|res| match res {
            None => Err(box_err!("brane {} could not find brane info", brane_id)),
            Some(state) => Ok(state),
        })
        .map_err(into_other::<_, violetabft::Error>)?;

    if state.get_state() != PeerState::Normal {
        return Err(causet_storage_error(format!(
            "snap job for {} seems stale, skip.",
            brane_id
        )));
    }

    let mut snapshot = Snapshot::default();

    // Set snapshot metadata.
    snapshot.mut_metadata().set_index(key.idx);
    snapshot.mut_metadata().set_term(key.term);

    let conf_state = util::conf_state_from_brane(state.get_brane());
    snapshot.mut_metadata().set_conf_state(conf_state);

    let mut s = mgr.get_snapshot_for_building(&key)?;
    // Set snapshot data.
    let mut snap_data = VioletaBftSnapshotData::default();
    snap_data.set_brane(state.get_brane().clone());
    let mut stat = SnapshotStatistics::new();
    s.build(
        engine,
        &kv_snap,
        state.get_brane(),
        &mut snap_data,
        &mut stat,
    )?;
    let v = snap_data.write_to_bytes()?;
    snapshot.set_data(v);

    SNAPSHOT_KV_COUNT_HISTOGRAM.observe(stat.kv_count as f64);
    SNAPSHOT_SIZE_HISTOGRAM.observe(stat.size as f64);

    Ok(snapshot)
}

// When we bootstrap the brane we must call this to initialize brane local state first.
pub fn write_initial_violetabft_state<W: VioletaBftLogBatch>(violetabft_wb: &mut W, brane_id: u64) -> Result<()> {
    let mut violetabft_state = VioletaBftLocalState::default();
    violetabft_state.last_index = VIOLETABFT_INIT_LOG_INDEX;
    violetabft_state.mut_hard_state().set_term(VIOLETABFT_INIT_LOG_TERM);
    violetabft_state.mut_hard_state().set_commit(VIOLETABFT_INIT_LOG_INDEX);
    violetabft_wb.put_violetabft_state(brane_id, &violetabft_state)?;
    Ok(())
}

// When we bootstrap the brane or handling split new brane, we must
// call this to initialize brane apply state first.
pub fn write_initial_apply_state<T: MuBlock>(kv_wb: &mut T, brane_id: u64) -> Result<()> {
    let mut apply_state = VioletaBftApplyState::default();
    apply_state.set_applied_index(VIOLETABFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_index(VIOLETABFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_term(VIOLETABFT_INIT_LOG_TERM);

    kv_wb.put_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(brane_id), &apply_state)?;
    Ok(())
}

pub fn write_peer_state<T: MuBlock>(
    kv_wb: &mut T,
    brane: &meta_timeshare::Brane,
    state: PeerState,
    merge_state: Option<MergeState>,
) -> Result<()> {
    let brane_id = brane.get_id();
    let mut brane_state = BraneLocalState::default();
    brane_state.set_state(state);
    brane_state.set_brane(brane.clone());
    if let Some(state) = merge_state {
        brane_state.set_merge_state(state);
    }

    debug!(
        "writing merge state";
        "brane_id" => brane_id,
        "state" => ?brane_state,
    );
    kv_wb.put_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(brane_id), &brane_state)?;
    Ok(())
}

#[causet(test)]
mod tests {
    use crate::interlock::InterlockHost;
    use crate::store::fsm::apply::compact_violetabft_log;
    use crate::store::worker::BraneRunner;
    use crate::store::worker::BraneTask;
    use crate::store::{bootstrap_store, initial_brane, prepare_bootstrap_cluster};
    use engine_lmdb::util::new_engine;
    use engine_lmdb::{LmdbEngine, LmdbSnapshot, LmdbWriteBatch};
    use edb::Engines;
    use edb::{Iterable, SyncMuBlock, WriteBatchExt};
    use edb::{ALL_CausetS, Causet_DEFAULT};
    use ekvproto::violetabft_server_timeshare::VioletaBftSnapshotData;
    use violetabft::evioletabft_timeshare::HardState;
    use violetabft::evioletabft_timeshare::{ConfState, Entry};
    use violetabft::{Error as VioletaBftError, StorageError};
    use std::cell::RefCell;
    use std::path::Path;
    use std::sync::atomic::*;
    use std::sync::mpsc::*;
    use std::sync::*;
    use std::time::Duration;
    use tempfile::{Builder, TempDir};
    use violetabftstore::interlock::::worker::{Interlock_Semaphore, Worker};

    use super::*;

    fn new_causet_storage(
        sched: Interlock_Semaphore<BraneTask<LmdbSnapshot>>,
        path: &TempDir,
    ) -> PeerStorage<LmdbEngine, LmdbEngine> {
        let kv_db = new_engine(path.path().to_str().unwrap(), None, ALL_CausetS, None).unwrap();
        let violetabft_path = path.path().join(Path::new("violetabft"));
        let violetabft_db = new_engine(violetabft_path.to_str().unwrap(), None, &[Causet_DEFAULT], None).unwrap();
        let engines = Engines::new(kv_db, violetabft_db);
        bootstrap_store(&engines, 1, 1).unwrap();

        let brane = initial_brane(1, 1, 1);
        prepare_bootstrap_cluster(&engines, &brane).unwrap();
        PeerStorage::new(engines, &brane, sched, 0, "".to_owned()).unwrap()
    }

    struct ReadyContext {
        kv_wb: LmdbWriteBatch,
        violetabft_wb: LmdbWriteBatch,
        sync_log: bool,
    }

    impl ReadyContext {
        fn new(s: &PeerStorage<LmdbEngine, LmdbEngine>) -> ReadyContext {
            ReadyContext {
                kv_wb: s.engines.kv.write_batch(),
                violetabft_wb: s.engines.violetabft.write_batch(),
                sync_log: false,
            }
        }
    }

    impl HandleVioletaBftReadyContext<LmdbWriteBatch, LmdbWriteBatch> for ReadyContext {
        fn wb_mut(&mut self) -> (&mut LmdbWriteBatch, &mut LmdbWriteBatch) {
            (&mut self.kv_wb, &mut self.violetabft_wb)
        }
        fn kv_wb_mut(&mut self) -> &mut LmdbWriteBatch {
            &mut self.kv_wb
        }
        fn violetabft_wb_mut(&mut self) -> &mut LmdbWriteBatch {
            &mut self.violetabft_wb
        }
        fn sync_log(&self) -> bool {
            self.sync_log
        }
        fn set_sync_log(&mut self, sync: bool) {
            self.sync_log = sync;
        }
    }

    fn new_causet_storage_from_ents(
        sched: Interlock_Semaphore<BraneTask<LmdbSnapshot>>,
        path: &TempDir,
        ents: &[Entry],
    ) -> PeerStorage<LmdbEngine, LmdbEngine> {
        let mut store = new_causet_storage(sched, path);
        let mut kv_wb = store.engines.kv.write_batch();
        let mut ctx = InvokeContext::new(&store);
        let mut ready_ctx = ReadyContext::new(&store);
        store.applightlike(&mut ctx, &ents[1..], &mut ready_ctx).unwrap();
        ctx.apply_state
            .mut_truncated_state()
            .set_index(ents[0].get_index());
        ctx.apply_state
            .mut_truncated_state()
            .set_term(ents[0].get_term());
        ctx.apply_state
            .set_applied_index(ents.last().unwrap().get_index());
        ctx.save_apply_state_to(&mut kv_wb).unwrap();
        store.engines.violetabft.write(&ready_ctx.violetabft_wb).unwrap();
        store.engines.kv.write(&kv_wb).unwrap();
        store.violetabft_state = ctx.violetabft_state;
        store.apply_state = ctx.apply_state;
        store
    }

    fn applightlike_ents(store: &mut PeerStorage<LmdbEngine, LmdbEngine>, ents: &[Entry]) {
        let mut ctx = InvokeContext::new(store);
        let mut ready_ctx = ReadyContext::new(store);
        store.applightlike(&mut ctx, ents, &mut ready_ctx).unwrap();
        ctx.save_violetabft_state_to(&mut ready_ctx.violetabft_wb).unwrap();
        store.engines.violetabft.write(&ready_ctx.violetabft_wb).unwrap();
        store.violetabft_state = ctx.violetabft_state;
    }

    fn validate_cache(store: &PeerStorage<LmdbEngine, LmdbEngine>, exp_ents: &[Entry]) {
        assert_eq!(store.cache.as_ref().unwrap().cache, exp_ents);
        for e in exp_ents {
            let key = tuplespaceInstanton::violetabft_log_key(store.get_brane_id(), e.get_index());
            let bytes = store.engines.violetabft.get_value(&key).unwrap().unwrap();
            let mut entry = Entry::default();
            entry.merge_from_bytes(&bytes).unwrap();
            assert_eq!(entry, *e);
        }
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
        e.set_index(index);
        e.set_term(term);
        e
    }

    fn size_of<T: protobuf::Message>(m: &T) -> u32 {
        m.compute_size()
    }

    #[test]
    fn test_causet_storage_term() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];

        let mut tests = vec![
            (2, Err(VioletaBftError::CausetStore(StorageError::Compacted))),
            (3, Ok(3)),
            (4, Ok(4)),
            (5, Ok(5)),
        ];
        for (i, (idx, wterm)) in tests.drain(..).enumerate() {
            let td = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
            let worker = Worker::new("snap-manager");
            let sched = worker.interlock_semaphore();
            let store = new_causet_storage_from_ents(sched, &td, &ents);
            let t = store.term(idx);
            if wterm != t {
                panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
            }
        }
    }

    fn get_meta_key_count(store: &PeerStorage<LmdbEngine, LmdbEngine>) -> usize {
        let brane_id = store.get_brane_id();
        let mut count = 0;
        let (meta_spacelike, meta_lightlike) = (
            tuplespaceInstanton::brane_meta_prefix(brane_id),
            tuplespaceInstanton::brane_meta_prefix(brane_id + 1),
        );
        store
            .engines
            .kv
            .scan_causet(Causet_VIOLETABFT, &meta_spacelike, &meta_lightlike, false, |_, _| {
                count += 1;
                Ok(true)
            })
            .unwrap();

        let (violetabft_spacelike, violetabft_lightlike) = (
            tuplespaceInstanton::brane_violetabft_prefix(brane_id),
            tuplespaceInstanton::brane_violetabft_prefix(brane_id + 1),
        );
        store
            .engines
            .kv
            .scan_causet(Causet_VIOLETABFT, &violetabft_spacelike, &violetabft_lightlike, false, |_, _| {
                count += 1;
                Ok(true)
            })
            .unwrap();

        store
            .engines
            .violetabft
            .scan(&violetabft_spacelike, &violetabft_lightlike, false, |_, _| {
                count += 1;
                Ok(true)
            })
            .unwrap();

        count
    }

    #[test]
    fn test_causet_storage_clear_meta() {
        let td = Builder::new().prefix("edb-store").temfidelir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.interlock_semaphore();
        let mut store = new_causet_storage_from_ents(sched, &td, &[new_entry(3, 3), new_entry(4, 4)]);
        applightlike_ents(&mut store, &[new_entry(5, 5), new_entry(6, 6)]);

        assert_eq!(6, get_meta_key_count(&store));

        let mut kv_wb = store.engines.kv.write_batch();
        let mut violetabft_wb = store.engines.violetabft.write_batch();
        store.clear_meta(&mut kv_wb, &mut violetabft_wb).unwrap();
        store.engines.kv.write(&kv_wb).unwrap();
        store.engines.violetabft.write(&violetabft_wb).unwrap();

        assert_eq!(0, get_meta_key_count(&store));
    }

    #[test]
    fn test_causet_storage_entries() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let max_u64 = u64::max_value();
        let mut tests = vec![
            (
                2,
                6,
                max_u64,
                Err(VioletaBftError::CausetStore(StorageError::Compacted)),
            ),
            (
                3,
                4,
                max_u64,
                Err(VioletaBftError::CausetStore(StorageError::Compacted)),
            ),
            (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
            (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (
                4,
                7,
                max_u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
            // even if maxsize is zero, the first entry should be returned
            (4, 7, 0, Ok(vec![new_entry(4, 4)])),
            // limit to 2
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            // all
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
        ];

        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            let td = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
            let worker = Worker::new("snap-manager");
            let sched = worker.interlock_semaphore();
            let store = new_causet_storage_from_ents(sched, &td, &ents);
            let e = store.entries(lo, hi, maxsize);
            if e != wentries {
                panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
            }
        }
    }

    // last_index and first_index are not mutated by PeerStorage on its own,
    // so we don't test them here.

    #[test]
    fn test_causet_storage_compact() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (2, Err(VioletaBftError::CausetStore(StorageError::Compacted))),
            (3, Err(VioletaBftError::CausetStore(StorageError::Compacted))),
            (4, Ok(())),
            (5, Ok(())),
        ];
        for (i, (idx, werr)) in tests.drain(..).enumerate() {
            let td = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
            let worker = Worker::new("snap-manager");
            let sched = worker.interlock_semaphore();
            let store = new_causet_storage_from_ents(sched, &td, &ents);
            let mut ctx = InvokeContext::new(&store);
            let res = store
                .term(idx)
                .map_err(From::from)
                .and_then(|term| compact_violetabft_log(&store.tag, &mut ctx.apply_state, idx, term));
            // TODO check exact error type after refactoring error.
            if res.is_err() ^ werr.is_err() {
                panic!("#{}: want {:?}, got {:?}", i, werr, res);
            }
            if res.is_ok() {
                let mut kv_wb = store.engines.kv.write_batch();
                ctx.save_apply_state_to(&mut kv_wb).unwrap();
                store.engines.kv.write(&kv_wb).unwrap();
            }
        }
    }

    fn generate_and_schedule_snapshot(
        gen_task: GenSnapTask,
        engines: &Engines<LmdbEngine, LmdbEngine>,
        sched: &Interlock_Semaphore<BraneTask<LmdbSnapshot>>,
    ) -> Result<()> {
        let apply_state: VioletaBftApplyState = engines
            .kv
            .get_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(gen_task.brane_id))
            .unwrap()
            .unwrap();
        let idx = apply_state.get_applied_index();
        let entry = engines
            .violetabft
            .get_msg::<Entry>(&tuplespaceInstanton::violetabft_log_key(gen_task.brane_id, idx))
            .unwrap()
            .unwrap();
        gen_task.generate_and_schedule_snapshot::<LmdbEngine>(
            engines.kv.clone().snapshot(),
            entry.get_term(),
            apply_state,
            sched,
        )
    }

    #[test]
    fn test_causet_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::default();
        cs.set_voters(vec![1, 2, 3]);

        let td = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
        let snap_dir = Builder::new().prefix("snap_dir").temfidelir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let mut worker = Worker::new("brane-worker");
        let sched = worker.interlock_semaphore();
        let mut s = new_causet_storage_from_ents(sched.clone(), &td, &ents);
        let (router, _) = mpsc::sync_channel(100);
        let runner = BraneRunner::new(
            s.engines.clone(),
            mgr,
            0,
            true,
            InterlockHost::<LmdbEngine>::default(),
            router,
        );
        worker.spacelike(runner).unwrap();
        let snap = s.snapshot(0);
        let unavailable = VioletaBftError::CausetStore(StorageError::SnapshotTemporarilyUnavailable);
        assert_eq!(snap.unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);
        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap();
        let snap = match *s.snap_state.borrow() {
            SnapState::Generating(ref rx) => rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(snap.get_metadata().get_index(), 5);
        assert_eq!(snap.get_metadata().get_term(), 5);
        assert!(!snap.get_data().is_empty());

        let mut data = VioletaBftSnapshotData::default();
        protobuf::Message::merge_from_bytes(&mut data, snap.get_data()).unwrap();
        assert_eq!(data.get_brane().get_id(), 1);
        assert_eq!(data.get_brane().get_peers().len(), 1);

        let (tx, rx) = channel();
        s.set_snap_state(SnapState::Generating(rx));
        // Empty channel should cause snapshot call to wait.
        assert_eq!(s.snapshot(0).unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        tx.lightlike(snap.clone()).unwrap();
        assert_eq!(s.snapshot(0), Ok(snap.clone()));
        assert_eq!(*s.snap_tried_cnt.borrow(), 0);

        let (tx, rx) = channel();
        tx.lightlike(snap.clone()).unwrap();
        s.set_snap_state(SnapState::Generating(rx));
        // stale snapshot should be abandoned, snapshot index < request index.
        assert_eq!(
            s.snapshot(snap.get_metadata().get_index() + 1).unwrap_err(),
            unavailable
        );
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);
        // Drop the task.
        let _ = s.gen_snap_task.borrow_mut().take().unwrap();

        let mut ctx = InvokeContext::new(&s);
        let mut kv_wb = s.engines.kv.write_batch();
        let mut ready_ctx = ReadyContext::new(&s);
        s.applightlike(
            &mut ctx,
            &[new_entry(6, 5), new_entry(7, 5)],
            &mut ready_ctx,
        )
        .unwrap();
        let mut hs = HardState::default();
        hs.set_commit(7);
        hs.set_term(5);
        ctx.violetabft_state.set_hard_state(hs);
        ctx.violetabft_state.set_last_index(7);
        ctx.apply_state.set_applied_index(7);
        ctx.save_violetabft_state_to(&mut ready_ctx.violetabft_wb).unwrap();
        ctx.save_apply_state_to(&mut kv_wb).unwrap();
        s.engines.kv.write(&kv_wb).unwrap();
        s.engines.violetabft.write(&ready_ctx.violetabft_wb).unwrap();
        s.apply_state = ctx.apply_state;
        s.violetabft_state = ctx.violetabft_state;
        ctx = InvokeContext::new(&s);
        let term = s.term(7).unwrap();
        compact_violetabft_log(&s.tag, &mut ctx.apply_state, 7, term).unwrap();
        kv_wb = s.engines.kv.write_batch();
        ctx.save_apply_state_to(&mut kv_wb).unwrap();
        s.engines.kv.write(&kv_wb).unwrap();
        s.apply_state = ctx.apply_state;

        let (tx, rx) = channel();
        tx.lightlike(snap).unwrap();
        s.set_snap_state(SnapState::Generating(rx));
        *s.snap_tried_cnt.borrow_mut() = 1;
        // stale snapshot should be abandoned, snapshot index < truncated index.
        assert_eq!(s.snapshot(0).unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap();
        match *s.snap_state.borrow() {
            SnapState::Generating(ref rx) => {
                rx.recv_timeout(Duration::from_secs(3)).unwrap();
                worker.stop().unwrap().join().unwrap();
                match rx.recv_timeout(Duration::from_secs(3)) {
                    Err(RecvTimeoutError::Disconnected) => {}
                    res => panic!("unexpected result: {:?}", res),
                }
            }
            ref s => panic!("unexpected state {:?}", s),
        }
        // Disconnected channel should trigger another try.
        assert_eq!(s.snapshot(0).unwrap_err(), unavailable);
        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap_err();
        assert_eq!(*s.snap_tried_cnt.borrow(), 2);

        for cnt in 2..super::MAX_SNAP_TRY_CNT {
            // Scheduled job failed should trigger .
            assert_eq!(s.snapshot(0).unwrap_err(), unavailable);
            let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
            generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap_err();
            assert_eq!(*s.snap_tried_cnt.borrow(), cnt + 1);
        }

        // When retry too many times, it should report a different error.
        match s.snapshot(0) {
            Err(VioletaBftError::CausetStore(StorageError::Other(_))) => {}
            res => panic!("unexpected res: {:?}", res),
        }
    }

    #[test]
    fn test_causet_storage_applightlike() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                vec![new_entry(4, 4), new_entry(5, 5)],
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                vec![new_entry(4, 6), new_entry(5, 6)],
            ),
            (
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
            ),
            // truncate incoming entries, truncate the existing entries and applightlike
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                vec![new_entry(4, 5)],
            ),
            // truncate the existing entries and applightlike
            (vec![new_entry(4, 5)], vec![new_entry(4, 5)]),
            // direct applightlike
            (
                vec![new_entry(6, 5)],
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
            ),
        ];
        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            let td = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
            let worker = Worker::new("snap-manager");
            let sched = worker.interlock_semaphore();
            let mut store = new_causet_storage_from_ents(sched, &td, &ents);
            applightlike_ents(&mut store, &entries);
            let li = store.last_index();
            let actual_entries = store.entries(4, li + 1, u64::max_value()).unwrap();
            if actual_entries != wentries {
                panic!("#{}: want {:?}, got {:?}", i, wentries, actual_entries);
            }
        }
    }

    #[test]
    fn test_causet_storage_cache_fetch() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let td = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.interlock_semaphore();
        let mut store = new_causet_storage_from_ents(sched, &td, &ents);
        store.cache.as_mut().unwrap().cache.clear();
        // empty cache should fetch data from lmdb directly.
        let mut res = store.entries(4, 6, u64::max_value()).unwrap();
        assert_eq!(*res, ents[1..]);

        let entries = vec![new_entry(6, 5), new_entry(7, 5)];
        applightlike_ents(&mut store, &entries);
        validate_cache(&store, &entries);

        // direct cache access
        res = store.entries(6, 8, u64::max_value()).unwrap();
        assert_eq!(res, entries);

        // size limit should be supported correctly.
        res = store.entries(4, 8, 0).unwrap();
        assert_eq!(res, vec![new_entry(4, 4)]);
        let mut size = ents[1..].iter().map(|e| u64::from(e.compute_size())).sum();
        res = store.entries(4, 8, size).unwrap();
        let mut exp_res = ents[1..].to_vec();
        assert_eq!(res, exp_res);
        for e in &entries {
            size += u64::from(e.compute_size());
            exp_res.push(e.clone());
            res = store.entries(4, 8, size).unwrap();
            assert_eq!(res, exp_res);
        }

        // cone limit should be supported correctly.
        for low in 4..9 {
            for high in low..9 {
                let res = store.entries(low, high, u64::max_value()).unwrap();
                assert_eq!(*res, exp_res[low as usize - 4..high as usize - 4]);
            }
        }
    }

    #[test]
    fn test_causet_storage_cache_fidelio() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let td = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.interlock_semaphore();
        let mut store = new_causet_storage_from_ents(sched, &td, &ents);
        store.cache.as_mut().unwrap().cache.clear();

        // initial cache
        let mut entries = vec![new_entry(6, 5), new_entry(7, 5)];
        applightlike_ents(&mut store, &entries);
        validate_cache(&store, &entries);

        // rewrite
        entries = vec![new_entry(6, 6), new_entry(7, 6)];
        applightlike_ents(&mut store, &entries);
        validate_cache(&store, &entries);

        // rewrite old entry
        entries = vec![new_entry(5, 6), new_entry(6, 6)];
        applightlike_ents(&mut store, &entries);
        validate_cache(&store, &entries);

        // partial rewrite
        entries = vec![new_entry(6, 7), new_entry(7, 7)];
        applightlike_ents(&mut store, &entries);
        let mut exp_res = vec![new_entry(5, 6), new_entry(6, 7), new_entry(7, 7)];
        validate_cache(&store, &exp_res);

        // direct applightlike
        entries = vec![new_entry(8, 7), new_entry(9, 7)];
        applightlike_ents(&mut store, &entries);
        exp_res.extlightlike_from_slice(&entries);
        validate_cache(&store, &exp_res);

        // rewrite middle
        entries = vec![new_entry(7, 8)];
        applightlike_ents(&mut store, &entries);
        exp_res.truncate(2);
        exp_res.push(new_entry(7, 8));
        validate_cache(&store, &exp_res);

        let cap = MAX_CACHE_CAPACITY as u64;

        // result overflow
        entries = (3..=cap).map(|i| new_entry(i + 5, 8)).collect();
        applightlike_ents(&mut store, &entries);
        exp_res.remove(0);
        exp_res.extlightlike_from_slice(&entries);
        validate_cache(&store, &exp_res);

        // input overflow
        entries = (0..=cap).map(|i| new_entry(i + cap + 6, 8)).collect();
        applightlike_ents(&mut store, &entries);
        exp_res = entries[entries.len() - cap as usize..].to_vec();
        validate_cache(&store, &exp_res);

        // compact
        store.compact_to(cap + 10);
        exp_res = (cap + 10..cap * 2 + 7).map(|i| new_entry(i, 8)).collect();
        validate_cache(&store, &exp_res);

        // compact shrink
        assert!(store.cache.as_ref().unwrap().cache.capacity() >= cap as usize);
        store.compact_to(cap * 2);
        exp_res = (cap * 2..cap * 2 + 7).map(|i| new_entry(i, 8)).collect();
        validate_cache(&store, &exp_res);
        assert!(store.cache.as_ref().unwrap().cache.capacity() < cap as usize);

        // applightlike shrink
        entries = (0..=cap).map(|i| new_entry(i, 8)).collect();
        applightlike_ents(&mut store, &entries);
        assert!(store.cache.as_ref().unwrap().cache.capacity() >= cap as usize);
        applightlike_ents(&mut store, &[new_entry(6, 8)]);
        exp_res = (1..7).map(|i| new_entry(i, 8)).collect();
        validate_cache(&store, &exp_res);
        assert!(store.cache.as_ref().unwrap().cache.capacity() < cap as usize);

        // compact all
        store.compact_to(cap + 2);
        validate_cache(&store, &[]);
        // invalid compaction should be ignored.
        store.compact_to(cap);
    }

    #[test]
    fn test_causet_storage_apply_snapshot() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let mut cs = ConfState::default();
        cs.set_voters(vec![1, 2, 3]);

        let td1 = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
        let snap_dir = Builder::new().prefix("snap").temfidelir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let mut worker = Worker::new("snap-manager");
        let sched = worker.interlock_semaphore();
        let s1 = new_causet_storage_from_ents(sched.clone(), &td1, &ents);
        let (router, _) = mpsc::sync_channel(100);
        let runner = BraneRunner::new(
            s1.engines.clone(),
            mgr,
            0,
            true,
            InterlockHost::<LmdbEngine>::default(),
            router,
        );
        worker.spacelike(runner).unwrap();
        assert!(s1.snapshot(0).is_err());
        let gen_task = s1.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s1.engines, &sched).unwrap();

        let snap1 = match *s1.snap_state.borrow() {
            SnapState::Generating(ref rx) => rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(s1.truncated_index(), 3);
        assert_eq!(s1.truncated_term(), 3);
        worker.stop().unwrap().join().unwrap();

        let td2 = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
        let mut s2 = new_causet_storage(sched.clone(), &td2);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
        let mut ctx = InvokeContext::new(&s2);
        assert_ne!(ctx.last_term, snap1.get_metadata().get_term());
        let mut kv_wb = s2.engines.kv.write_batch();
        let mut violetabft_wb = s2.engines.violetabft.write_batch();
        s2.apply_snapshot(&mut ctx, &snap1, &mut kv_wb, &mut violetabft_wb, &[])
            .unwrap();
        assert_eq!(ctx.last_term, snap1.get_metadata().get_term());
        assert_eq!(ctx.apply_state.get_applied_index(), 6);
        assert_eq!(ctx.violetabft_state.get_last_index(), 6);
        assert_eq!(ctx.apply_state.get_truncated_state().get_index(), 6);
        assert_eq!(ctx.apply_state.get_truncated_state().get_term(), 6);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
        validate_cache(&s2, &[]);

        let td3 = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
        let ents = &[new_entry(3, 3), new_entry(4, 3)];
        let mut s3 = new_causet_storage_from_ents(sched, &td3, ents);
        validate_cache(&s3, &ents[1..]);
        let mut ctx = InvokeContext::new(&s3);
        assert_ne!(ctx.last_term, snap1.get_metadata().get_term());
        let mut kv_wb = s3.engines.kv.write_batch();
        let mut violetabft_wb = s3.engines.violetabft.write_batch();
        s3.apply_snapshot(&mut ctx, &snap1, &mut kv_wb, &mut violetabft_wb, &[])
            .unwrap();
        assert_eq!(ctx.last_term, snap1.get_metadata().get_term());
        assert_eq!(ctx.apply_state.get_applied_index(), 6);
        assert_eq!(ctx.violetabft_state.get_last_index(), 6);
        assert_eq!(ctx.apply_state.get_truncated_state().get_index(), 6);
        assert_eq!(ctx.apply_state.get_truncated_state().get_term(), 6);
        validate_cache(&s3, &[]);
    }

    #[test]
    fn test_canceling_snapshot() {
        let td = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.interlock_semaphore();
        let mut s = new_causet_storage(sched, &td);

        // PENDING can be canceled directly.
        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_PENDING,
        ))));
        assert!(s.cancel_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);

        // RUNNING can't be canceled directly.
        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_RUNNING,
        ))));
        assert!(!s.cancel_applying_snap());
        assert_eq!(
            *s.snap_state.borrow(),
            SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_CANCELLING)))
        );
        // CANCEL can't be canceled again.
        assert!(!s.cancel_applying_snap());

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_CANCELLED,
        ))));
        // canceled snapshot can be cancel directly.
        assert!(s.cancel_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_FINISHED,
        ))));
        assert!(s.cancel_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::Relax);

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_FAILED,
        ))));
        let res = panic_hook::recover_safe(|| s.cancel_applying_snap());
        assert!(res.is_err());
    }

    #[test]
    fn test_try_finish_snapshot() {
        let td = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.interlock_semaphore();
        let mut s = new_causet_storage(sched, &td);

        // PENDING can be finished.
        let mut snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_PENDING)));
        s.snap_state = RefCell::new(snap_state);
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Applying);
        assert_eq!(
            *s.snap_state.borrow(),
            SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_PENDING)))
        );

        // RUNNING can't be finished.
        snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)));
        s.snap_state = RefCell::new(snap_state);
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Applying);
        assert_eq!(
            *s.snap_state.borrow(),
            SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)))
        );

        snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_CANCELLED)));
        s.snap_state = RefCell::new(snap_state);
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Idle);
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);
        // ApplyAborted is not applying snapshot.
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Idle);
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_FINISHED,
        ))));
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Success);
        assert_eq!(*s.snap_state.borrow(), SnapState::Relax);
        // Relax is not applying snapshot.
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Idle);
        assert_eq!(*s.snap_state.borrow(), SnapState::Relax);

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_FAILED,
        ))));
        let res = panic_hook::recover_safe(|| s.check_applying_snap());
        assert!(res.is_err());
    }

    #[test]
    fn test_sync_log() {
        let mut tbl = vec![];

        // Do not sync empty entrise.
        tbl.push((Entry::default(), false));

        // Sync if sync_log is set.
        let mut e = Entry::default();
        e.set_sync_log(true);
        tbl.push((e, true));

        // Sync if context is marked sync.
        let context = ProposalContext::SYNC_LOG.to_vec();
        let mut e = Entry::default();
        e.set_context(context);
        tbl.push((e.clone(), true));

        // Sync if sync_log is set and context is marked sync_log.
        e.set_sync_log(true);
        tbl.push((e, true));

        for (e, sync) in tbl {
            assert_eq!(get_sync_log_from_entry(&e), sync, "{:?}", e);
        }
    }

    #[test]
    fn test_validate_states() {
        let td = Builder::new().prefix("edb-store-test").temfidelir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.interlock_semaphore();
        let kv_db = new_engine(td.path().to_str().unwrap(), None, ALL_CausetS, None).unwrap();
        let violetabft_path = td.path().join(Path::new("violetabft"));
        let violetabft_db = new_engine(violetabft_path.to_str().unwrap(), None, &[Causet_DEFAULT], None).unwrap();
        let engines = Engines::new(kv_db, violetabft_db);
        bootstrap_store(&engines, 1, 1).unwrap();

        let brane = initial_brane(1, 1, 1);
        prepare_bootstrap_cluster(&engines, &brane).unwrap();
        let build_causet_storage = || -> Result<PeerStorage<LmdbEngine, LmdbEngine>> {
            PeerStorage::new(engines.clone(), &brane, sched.clone(), 0, "".to_owned())
        };
        let mut s = build_causet_storage().unwrap();
        let mut violetabft_state = VioletaBftLocalState::default();
        violetabft_state.set_last_index(VIOLETABFT_INIT_LOG_INDEX);
        violetabft_state.mut_hard_state().set_term(VIOLETABFT_INIT_LOG_TERM);
        violetabft_state.mut_hard_state().set_commit(VIOLETABFT_INIT_LOG_INDEX);
        let initial_state = s.initial_state().unwrap();
        assert_eq!(initial_state.hard_state, *violetabft_state.get_hard_state());

        // last_index < commit_index is invalid.
        let violetabft_state_key = tuplespaceInstanton::violetabft_state_key(1);
        violetabft_state.set_last_index(11);
        let log_key = tuplespaceInstanton::violetabft_log_key(1, 11);
        engines
            .violetabft
            .put_msg(&log_key, &new_entry(11, VIOLETABFT_INIT_LOG_TERM))
            .unwrap();
        violetabft_state.mut_hard_state().set_commit(12);
        engines.violetabft.put_msg(&violetabft_state_key, &violetabft_state).unwrap();
        assert!(build_causet_storage().is_err());

        let log_key = tuplespaceInstanton::violetabft_log_key(1, 20);
        engines
            .violetabft
            .put_msg(&log_key, &new_entry(20, VIOLETABFT_INIT_LOG_TERM))
            .unwrap();
        violetabft_state.set_last_index(20);
        engines.violetabft.put_msg(&violetabft_state_key, &violetabft_state).unwrap();
        s = build_causet_storage().unwrap();
        let initial_state = s.initial_state().unwrap();
        assert_eq!(initial_state.hard_state, *violetabft_state.get_hard_state());

        // Missing last log is invalid.
        engines.violetabft.delete(&log_key).unwrap();
        assert!(build_causet_storage().is_err());
        engines
            .violetabft
            .put_msg(&log_key, &new_entry(20, VIOLETABFT_INIT_LOG_TERM))
            .unwrap();

        // applied_index > commit_index is invalid.
        let mut apply_state = VioletaBftApplyState::default();
        apply_state.set_applied_index(13);
        apply_state.mut_truncated_state().set_index(13);
        apply_state
            .mut_truncated_state()
            .set_term(VIOLETABFT_INIT_LOG_TERM);
        let apply_state_key = tuplespaceInstanton::apply_state_key(1);
        engines
            .kv
            .put_msg_causet(Causet_VIOLETABFT, &apply_state_key, &apply_state)
            .unwrap();
        assert!(build_causet_storage().is_err());

        // It should not recover if corresponding log doesn't exist.
        apply_state.set_commit_index(14);
        apply_state.set_commit_term(VIOLETABFT_INIT_LOG_TERM);
        engines
            .kv
            .put_msg_causet(Causet_VIOLETABFT, &apply_state_key, &apply_state)
            .unwrap();
        assert!(build_causet_storage().is_err());

        let log_key = tuplespaceInstanton::violetabft_log_key(1, 14);
        engines
            .violetabft
            .put_msg(&log_key, &new_entry(14, VIOLETABFT_INIT_LOG_TERM))
            .unwrap();
        violetabft_state.mut_hard_state().set_commit(14);
        s = build_causet_storage().unwrap();
        let initial_state = s.initial_state().unwrap();
        assert_eq!(initial_state.hard_state, *violetabft_state.get_hard_state());

        // log term miss match is invalid.
        engines
            .violetabft
            .put_msg(&log_key, &new_entry(14, VIOLETABFT_INIT_LOG_TERM - 1))
            .unwrap();
        assert!(build_causet_storage().is_err());

        // hard state term miss match is invalid.
        engines
            .violetabft
            .put_msg(&log_key, &new_entry(14, VIOLETABFT_INIT_LOG_TERM))
            .unwrap();
        violetabft_state.mut_hard_state().set_term(VIOLETABFT_INIT_LOG_TERM - 1);
        engines.violetabft.put_msg(&violetabft_state_key, &violetabft_state).unwrap();
        assert!(build_causet_storage().is_err());

        // last index < recorded_commit_index is invalid.
        violetabft_state.mut_hard_state().set_term(VIOLETABFT_INIT_LOG_TERM);
        violetabft_state.set_last_index(13);
        let log_key = tuplespaceInstanton::violetabft_log_key(1, 13);
        engines
            .violetabft
            .put_msg(&log_key, &new_entry(13, VIOLETABFT_INIT_LOG_TERM))
            .unwrap();
        engines.violetabft.put_msg(&violetabft_state_key, &violetabft_state).unwrap();
        assert!(build_causet_storage().is_err());

        // last_commit_index > commit_index is invalid.
        violetabft_state.set_last_index(20);
        violetabft_state.mut_hard_state().set_commit(12);
        engines.violetabft.put_msg(&violetabft_state_key, &violetabft_state).unwrap();
        apply_state.set_last_commit_index(13);
        engines
            .kv
            .put_msg_causet(Causet_VIOLETABFT, &apply_state_key, &apply_state)
            .unwrap();
        assert!(build_causet_storage().is_err());
    }
}
