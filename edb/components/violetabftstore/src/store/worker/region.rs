// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::collections::Bound::{Excluded, Included, Unbounded};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Synclightlikeer;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::u64;

use edb::Causet_VIOLETABFT;
use edb::{Engines, CausetEngine, MuBlock, VioletaBftEngine};
use ekvproto::violetabft_server_timeshare::{PeerState, VioletaBftApplyState, BraneLocalState};
use violetabft::evioletabft_timeshare::Snapshot as VioletaBftSnapshot;

use crate::interlock::InterlockHost;
use crate::store::peer_causet_storage::{
    JOB_STATUS_CANCELLED, JOB_STATUS_CANCELLING, JOB_STATUS_FAILED, JOB_STATUS_FINISHED,
    JOB_STATUS_PENDING, JOB_STATUS_RUNNING,
};
use crate::store::snap::{plain_file_used, Error, Result, SNAPSHOT_CausetS};
use crate::store::transport::CasualRouter;
use crate::store::{
    self, check_abort, ApplyOptions, CasualMessage, SnapEntry, SnapKey, SnapManager,
};
use yatp::pool::{Builder, ThreadPool};
use yatp::task::future::TaskCell;

use violetabftstore::interlock::::timer::Timer;
use violetabftstore::interlock::::worker::{Runnable, RunnableWithTimer};

use super::metrics::*;

const GENERATE_POOL_SIZE: usize = 2;

// used to periodically check whether we should delete a stale peer's cone in brane runner
pub const STALE_PEER_CHECK_INTERVAL: u64 = 10_000; // 10000 milliseconds

// used to periodically check whether schedule plightlikeing applies in brane runner
pub const PENDING_APPLY_CHECK_INTERVAL: u64 = 1_000; // 1000 milliseconds

const CLEANUP_MAX_DURATION: Duration = Duration::from_secs(5);

/// Brane related task
#[derive(Debug)]
pub enum Task<S> {
    Gen {
        brane_id: u64,
        last_applied_index_term: u64,
        last_applied_state: VioletaBftApplyState,
        kv_snap: S,
        notifier: Synclightlikeer<VioletaBftSnapshot>,
    },
    Apply {
        brane_id: u64,
        status: Arc<AtomicUsize>,
    },
    /// Destroy data between [spacelike_key, lightlike_key).
    ///
    /// The deletion may and may not succeed.
    Destroy {
        brane_id: u64,
        spacelike_key: Vec<u8>,
        lightlike_key: Vec<u8>,
    },
}

impl<S> Task<S> {
    pub fn destroy(brane_id: u64, spacelike_key: Vec<u8>, lightlike_key: Vec<u8>) -> Task<S> {
        Task::Destroy {
            brane_id,
            spacelike_key,
            lightlike_key,
        }
    }
}

impl<S> Display for Task<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Gen { brane_id, .. } => write!(f, "Snap gen for {}", brane_id),
            Task::Apply { brane_id, .. } => write!(f, "Snap apply for {}", brane_id),
            Task::Destroy {
                brane_id,
                ref spacelike_key,
                ref lightlike_key,
            } => write!(
                f,
                "Destroy {} [{}, {})",
                brane_id,
                hex::encode_upper(spacelike_key),
                hex::encode_upper(lightlike_key)
            ),
        }
    }
}

#[derive(Clone)]
struct StalePeerInfo {
    // the spacelike_key is stored as a key in PlightlikeingDeleteCones
    // below are stored as a value in PlightlikeingDeleteCones
    pub brane_id: u64,
    pub lightlike_key: Vec<u8>,
    // Once the oldest snapshot sequence exceeds this, it ensures that no one is
    // reading on this peer anymore. So we can safely call `delete_files_in_cone`
    // , which may break the consistency of snapshot, of this peer cone.
    pub stale_sequence: u64,
}

/// A structure records all cones to be deleted with some delay.
/// The delay is because there may be some interlock requests related to these cones.
#[derive(Clone, Default)]
struct PlightlikeingDeleteCones {
    cones: BTreeMap<Vec<u8>, StalePeerInfo>, // spacelike_key -> StalePeerInfo
}

impl PlightlikeingDeleteCones {
    /// Finds cones that overlap with [spacelike_key, lightlike_key).
    fn find_overlap_cones(
        &self,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
    ) -> Vec<(u64, Vec<u8>, Vec<u8>, u64)> {
        let mut cones = Vec::new();
        // find the first cone that may overlap with [spacelike_key, lightlike_key)
        let sub_cone = self.cones.cone((Unbounded, Excluded(spacelike_key.to_vec())));
        if let Some((s_key, peer_info)) = sub_cone.last() {
            if peer_info.lightlike_key > spacelike_key.to_vec() {
                cones.push((
                    peer_info.brane_id,
                    s_key.clone(),
                    peer_info.lightlike_key.clone(),
                    peer_info.stale_sequence,
                ));
            }
        }

        // find the rest cones that overlap with [spacelike_key, lightlike_key)
        for (s_key, peer_info) in self
            .cones
            .cone((Included(spacelike_key.to_vec()), Excluded(lightlike_key.to_vec())))
        {
            cones.push((
                peer_info.brane_id,
                s_key.clone(),
                peer_info.lightlike_key.clone(),
                peer_info.stale_sequence,
            ));
        }
        cones
    }

    /// Gets cones that overlap with [spacelike_key, lightlike_key).
    pub fn drain_overlap_cones(
        &mut self,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
    ) -> Vec<(u64, Vec<u8>, Vec<u8>, u64)> {
        let cones = self.find_overlap_cones(spacelike_key, lightlike_key);

        for &(_, ref s_key, _, _) in &cones {
            self.cones.remove(s_key).unwrap();
        }
        cones
    }

    /// Removes and returns the peer info with the `spacelike_key`.
    fn remove(&mut self, spacelike_key: &[u8]) -> Option<(u64, Vec<u8>, Vec<u8>)> {
        self.cones
            .remove(spacelike_key)
            .map(|peer_info| (peer_info.brane_id, spacelike_key.to_owned(), peer_info.lightlike_key))
    }

    /// Inserts a new cone waiting to be deleted.
    ///
    /// Before an insert is called, it must call drain_overlap_cones to clean the overlapping cone.
    fn insert(&mut self, brane_id: u64, spacelike_key: &[u8], lightlike_key: &[u8], stale_sequence: u64) {
        if !self.find_overlap_cones(&spacelike_key, &lightlike_key).is_empty() {
            panic!(
                "[brane {}] register deleting data in [{}, {}) failed due to overlap",
                brane_id,
                hex::encode_upper(&spacelike_key),
                hex::encode_upper(&lightlike_key),
            );
        }
        let info = StalePeerInfo {
            brane_id,
            lightlike_key: lightlike_key.to_owned(),
            stale_sequence,
        };
        self.cones.insert(spacelike_key.to_owned(), info);
    }

    /// Gets all stale cones info.
    pub fn stale_cones(&self, oldest_sequence: u64) -> impl Iteron<Item = (u64, &[u8], &[u8])> {
        self.cones
            .iter()
            .filter(move |&(_, info)| info.stale_sequence < oldest_sequence)
            .map(|(spacelike_key, info)| {
                (
                    info.brane_id,
                    spacelike_key.as_slice(),
                    info.lightlike_key.as_slice(),
                )
            })
    }

    pub fn len(&self) -> usize {
        self.cones.len()
    }
}

#[derive(Clone)]
struct SnapContext<EK, ER, R>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    engines: Engines<EK, ER>,
    batch_size: usize,
    mgr: SnapManager,
    use_delete_cone: bool,
    plightlikeing_delete_cones: PlightlikeingDeleteCones,
    interlock_host: InterlockHost<EK>,
    router: R,
}

impl<EK, ER, R> SnapContext<EK, ER, R>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
    R: CasualRouter<EK>,
{
    /// Generates the snapshot of the Brane.
    fn generate_snap(
        &self,
        brane_id: u64,
        last_applied_index_term: u64,
        last_applied_state: VioletaBftApplyState,
        kv_snap: EK::Snapshot,
        notifier: Synclightlikeer<VioletaBftSnapshot>,
    ) -> Result<()> {
        // do we need to check leader here?
        let snap = box_try!(store::do_snapshot::<EK>(
            self.mgr.clone(),
            &self.engines.kv,
            kv_snap,
            brane_id,
            last_applied_index_term,
            last_applied_state,
        ));
        // Only enable the fail point when the brane id is equal to 1, which is
        // the id of bootstrapped brane in tests.
        fail_point!("brane_gen_snap", brane_id == 1, |_| Ok(()));
        if let Err(e) = notifier.try_lightlike(snap) {
            info!(
                "failed to notify snap result, leadership may have changed, ignore error";
                "brane_id" => brane_id,
                "err" => %e,
            );
        }
        // The error can be ignored as snapshot will be sent in next heartbeat in the lightlike.
        let _ = self
            .router
            .lightlike(brane_id, CasualMessage::SnapshotGenerated);
        Ok(())
    }

    /// Handles the task of generating snapshot of the Brane. It calls `generate_snap` to do the actual work.
    fn handle_gen(
        &self,
        brane_id: u64,
        last_applied_index_term: u64,
        last_applied_state: VioletaBftApplyState,
        kv_snap: EK::Snapshot,
        notifier: Synclightlikeer<VioletaBftSnapshot>,
    ) {
        SNAP_COUNTER.generate.all.inc();
        let spacelike = violetabftstore::interlock::::time::Instant::now();

        if let Err(e) = self.generate_snap(
            brane_id,
            last_applied_index_term,
            last_applied_state,
            kv_snap,
            notifier,
        ) {
            error!(%e; "failed to generate snap!!!"; "brane_id" => brane_id,);
            return;
        }

        SNAP_COUNTER.generate.success.inc();
        SNAP_HISTOGRAM.generate.observe(spacelike.elapsed_secs());
    }

    /// Applies snapshot data of the Brane.
    fn apply_snap(&mut self, brane_id: u64, abort: Arc<AtomicUsize>) -> Result<()> {
        info!("begin apply snap data"; "brane_id" => brane_id);
        fail_point!("brane_apply_snap", |_| { Ok(()) });
        check_abort(&abort)?;
        let brane_key = tuplespaceInstanton::brane_state_key(brane_id);
        let mut brane_state: BraneLocalState =
            match box_try!(self.engines.kv.get_msg_causet(Causet_VIOLETABFT, &brane_key)) {
                Some(state) => state,
                None => {
                    return Err(box_err!(
                        "failed to get brane_state from {}",
                        hex::encode_upper(&brane_key)
                    ));
                }
            };

        // clear up origin data.
        let brane = brane_state.get_brane().clone();
        let spacelike_key = tuplespaceInstanton::enc_spacelike_key(&brane);
        let lightlike_key = tuplespaceInstanton::enc_lightlike_key(&brane);
        check_abort(&abort)?;
        self.cleanup_overlap_cones(&spacelike_key, &lightlike_key);
        box_try!(self
            .engines
            .kv
            .delete_all_in_cone(&spacelike_key, &lightlike_key, self.use_delete_cone));
        check_abort(&abort)?;
        fail_point!("apply_snap_cleanup_cone");

        let state_key = tuplespaceInstanton::apply_state_key(brane_id);
        let apply_state: VioletaBftApplyState =
            match box_try!(self.engines.kv.get_msg_causet(Causet_VIOLETABFT, &state_key)) {
                Some(state) => state,
                None => {
                    return Err(box_err!(
                        "failed to get violetabftstate from {}",
                        hex::encode_upper(&state_key)
                    ));
                }
            };
        let term = apply_state.get_truncated_state().get_term();
        let idx = apply_state.get_truncated_state().get_index();
        let snap_key = SnapKey::new(brane_id, term, idx);
        self.mgr.register(snap_key.clone(), SnapEntry::Applying);
        defer!({
            self.mgr.deregister(&snap_key, &SnapEntry::Applying);
        });
        let mut s = box_try!(self.mgr.get_snapshot_for_applying_to_engine(&snap_key));
        if !s.exists() {
            return Err(box_err!("missing snapshot file {}", s.path()));
        }
        check_abort(&abort)?;
        let timer = Instant::now();
        let options = ApplyOptions {
            db: self.engines.kv.clone(),
            brane,
            abort: Arc::clone(&abort),
            write_batch_size: self.batch_size,
            interlock_host: self.interlock_host.clone(),
        };
        s.apply(options)?;

        let mut wb = self.engines.kv.write_batch();
        brane_state.set_state(PeerState::Normal);
        box_try!(wb.put_msg_causet(Causet_VIOLETABFT, &brane_key, &brane_state));
        box_try!(wb.delete_causet(Causet_VIOLETABFT, &tuplespaceInstanton::snapshot_violetabft_state_key(brane_id)));
        self.engines.kv.write(&wb).unwrap_or_else(|e| {
            panic!("{} failed to save apply_snap result: {:?}", brane_id, e);
        });
        info!(
            "apply new data";
            "brane_id" => brane_id,
            "time_takes" => ?timer.elapsed(),
        );
        Ok(())
    }

    /// Tries to apply the snapshot of the specified Brane. It calls `apply_snap` to do the actual work.
    fn handle_apply(&mut self, brane_id: u64, status: Arc<AtomicUsize>) {
        status.compare_and_swap(JOB_STATUS_PENDING, JOB_STATUS_RUNNING, Ordering::SeqCst);
        SNAP_COUNTER.apply.all.inc();
        // let apply_histogram = SNAP_HISTOGRAM.with_label_values(&["apply"]);
        // let timer = apply_histogram.spacelike_coarse_timer();
        let spacelike = violetabftstore::interlock::::time::Instant::now();

        match self.apply_snap(brane_id, Arc::clone(&status)) {
            Ok(()) => {
                status.swap(JOB_STATUS_FINISHED, Ordering::SeqCst);
                SNAP_COUNTER.apply.success.inc();
            }
            Err(Error::Abort) => {
                warn!("applying snapshot is aborted"; "brane_id" => brane_id);
                assert_eq!(
                    status.swap(JOB_STATUS_CANCELLED, Ordering::SeqCst),
                    JOB_STATUS_CANCELLING
                );
                SNAP_COUNTER.apply.abort.inc();
            }
            Err(e) => {
                error!(%e; "failed to apply snap!!!");

                status.swap(JOB_STATUS_FAILED, Ordering::SeqCst);
                SNAP_COUNTER.apply.fail.inc();
            }
        }

        SNAP_HISTOGRAM.apply.observe(spacelike.elapsed_secs());
    }

    /// Cleans up the data within the cone.
    fn cleanup_cone(
        &self,
        brane_id: u64,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        use_delete_files: bool,
    ) {
        if use_delete_files {
            if let Err(e) = self
                .engines
                .kv
                .delete_all_files_in_cone(spacelike_key, lightlike_key)
            {
                error!(%e;
                    "failed to delete files in cone";
                    "brane_id" => brane_id,
                    "spacelike_key" => log_wrappers::Key(spacelike_key),
                    "lightlike_key" => log_wrappers::Key(lightlike_key),
                );
                return;
            }
        }
        if let Err(e) =
            self.engines
                .kv
                .delete_all_in_cone(spacelike_key, lightlike_key, self.use_delete_cone)
        {
            error!(%e;
                "failed to delete data in cone";
                "brane_id" => brane_id,
                "spacelike_key" => log_wrappers::Key(spacelike_key),
                "lightlike_key" => log_wrappers::Key(lightlike_key),
            );
        } else {
            info!(
                "succeed in deleting data in cone";
                "brane_id" => brane_id,
                "spacelike_key" => log_wrappers::Key(spacelike_key),
                "lightlike_key" => log_wrappers::Key(lightlike_key),
            );
        }
    }

    /// Gets the overlapping cones and cleans them up.
    fn cleanup_overlap_cones(&mut self, spacelike_key: &[u8], lightlike_key: &[u8]) {
        let overlap_cones = self
            .plightlikeing_delete_cones
            .drain_overlap_cones(spacelike_key, lightlike_key);
        if overlap_cones.is_empty() {
            return;
        }
        let oldest_sequence = self
            .engines
            .kv
            .get_oldest_snapshot_sequence_number()
            .unwrap_or(u64::MAX);
        for (brane_id, s_key, e_key, stale_sequence) in overlap_cones {
            // `delete_files_in_cone` may break current lmdb snapshots consistency,
            // so do not use it unless we can make sure there is no reader of the destroyed peer anymore.
            let use_delete_files = stale_sequence < oldest_sequence;
            if !use_delete_files {
                SNAP_COUNTER_VEC
                    .with_label_values(&["overlap", "not_delete_files"])
                    .inc();
            }
            self.cleanup_cone(brane_id, &s_key, &e_key, use_delete_files);
        }
    }

    /// Inserts a new plightlikeing cone, and it will be cleaned up with some delay.
    fn insert_plightlikeing_delete_cone(&mut self, brane_id: u64, spacelike_key: &[u8], lightlike_key: &[u8]) {
        self.cleanup_overlap_cones(spacelike_key, lightlike_key);

        info!(
            "register deleting data in cone";
            "brane_id" => brane_id,
            "spacelike_key" => log_wrappers::Key(spacelike_key),
            "lightlike_key" => log_wrappers::Key(lightlike_key),
        );

        self.plightlikeing_delete_cones.insert(
            brane_id,
            spacelike_key,
            lightlike_key,
            self.engines.kv.get_latest_sequence_number(),
        );
    }

    /// Cleans up stale cones.
    fn clean_stale_cones(&mut self) {
        STALE_PEER_PENDING_DELETE_RANGE_GAUGE.set(self.plightlikeing_delete_cones.len() as f64);

        let oldest_sequence = self
            .engines
            .kv
            .get_oldest_snapshot_sequence_number()
            .unwrap_or(u64::MAX);
        let mut cleaned_cone_tuplespaceInstanton = vec![];
        {
            let now = Instant::now();
            for (brane_id, spacelike_key, lightlike_key) in
                self.plightlikeing_delete_cones.stale_cones(oldest_sequence)
            {
                self.cleanup_cone(
                    brane_id, spacelike_key, lightlike_key, true, /* use_delete_files */
                );
                cleaned_cone_tuplespaceInstanton.push(spacelike_key.to_vec());
                let elapsed = now.elapsed();
                if elapsed >= CLEANUP_MAX_DURATION {
                    let len = cleaned_cone_tuplespaceInstanton.len();
                    let elapsed = elapsed.as_millis() as f64 / 1000f64;
                    info!("clean stale cones, now backoff"; "key_count" => len, "time_takes" => elapsed);
                    break;
                }
            }
        }
        for key in cleaned_cone_tuplespaceInstanton {
            assert!(
                self.plightlikeing_delete_cones.remove(&key).is_some(),
                "cleanup plightlikeing_delete_cones {} should exist",
                hex::encode_upper(&key)
            );
        }
    }

    /// Checks the number of files at level 0 to avoid write stall after ingesting sst.
    /// Returns true if the ingestion causes write stall.
    fn ingest_maybe_stall(&self) -> bool {
        for causet in SNAPSHOT_CausetS {
            // no need to check dagger causet
            if plain_file_used(causet) {
                continue;
            }
            if self
                .engines
                .kv
                .ingest_maybe_slowdown_writes(causet)
                .expect("causet")
            {
                return true;
            }
        }
        false
    }
}

pub struct Runner<EK, ER, R>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    pool: ThreadPool<TaskCell>,
    ctx: SnapContext<EK, ER, R>,
    // we may delay some apply tasks if level 0 files to write stall memory_barrier,
    // plightlikeing_applies records all delayed apply task, and will check again later
    plightlikeing_applies: VecDeque<Task<EK::Snapshot>>,
}

impl<EK, ER, R> Runner<EK, ER, R>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
    R: CasualRouter<EK>,
{
    pub fn new(
        engines: Engines<EK, ER>,
        mgr: SnapManager,
        batch_size: usize,
        use_delete_cone: bool,
        interlock_host: InterlockHost<EK>,
        router: R,
    ) -> Runner<EK, ER, R> {
        Runner {
            pool: Builder::new(thd_name!("snap-generator"))
                .max_thread_count(GENERATE_POOL_SIZE)
                .build_future_pool(),

            ctx: SnapContext {
                engines,
                mgr,
                batch_size,
                use_delete_cone,
                plightlikeing_delete_cones: PlightlikeingDeleteCones::default(),
                interlock_host,
                router,
            },
            plightlikeing_applies: VecDeque::new(),
        }
    }

    pub fn new_timer(&self) -> Timer<Event> {
        let mut timer = Timer::new(2);
        timer.add_task(
            Duration::from_millis(PENDING_APPLY_CHECK_INTERVAL),
            Event::CheckApply,
        );
        timer.add_task(
            Duration::from_millis(STALE_PEER_CHECK_INTERVAL),
            Event::CheckStalePeer,
        );
        timer
    }

    /// Tries to apply plightlikeing tasks if there is some.
    fn handle_plightlikeing_applies(&mut self) {
        fail_point!("apply_plightlikeing_snapshot", |_| {});
        while !self.plightlikeing_applies.is_empty() {
            // should not handle too many applies than the number of files that can be ingested.
            // check level 0 every time because we can not make sure how does the number of level 0 files change.
            if self.ctx.ingest_maybe_stall() {
                break;
            }
            if let Some(Task::Apply { brane_id, status }) = self.plightlikeing_applies.pop_front() {
                self.ctx.handle_apply(brane_id, status);
            }
        }
    }
}

impl<EK, ER, R> Runnable for Runner<EK, ER, R>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
    R: CasualRouter<EK> + lightlike + Clone + 'static,
{
    type Task = Task<EK::Snapshot>;

    fn run(&mut self, task: Task<EK::Snapshot>) {
        match task {
            Task::Gen {
                brane_id,
                last_applied_index_term,
                last_applied_state,
                kv_snap,
                notifier,
            } => {
                // It is safe for now to handle generating and applying snapshot concurrently,
                // but it may not when merge is implemented.
                let ctx = self.ctx.clone();

                self.pool.spawn(async move {
                    edb_alloc::add_thread_memory_accessor();
                    ctx.handle_gen(
                        brane_id,
                        last_applied_index_term,
                        last_applied_state,
                        kv_snap,
                        notifier,
                    );
                    edb_alloc::remove_thread_memory_accessor();
                });
            }
            task @ Task::Apply { .. } => {
                fail_point!("on_brane_worker_apply", true, |_| {});
                // to makes sure applying snapshots in order.
                self.plightlikeing_applies.push_back(task);
                self.handle_plightlikeing_applies();
                if !self.plightlikeing_applies.is_empty() {
                    // delay the apply and retry later
                    SNAP_COUNTER.apply.delay.inc()
                }
            }
            Task::Destroy {
                brane_id,
                spacelike_key,
                lightlike_key,
            } => {
                fail_point!("on_brane_worker_destroy", true, |_| {});
                // try to delay the cone deletion because
                // there might be a interlock request related to this cone
                self.ctx
                    .insert_plightlikeing_delete_cone(brane_id, &spacelike_key, &lightlike_key);

                // try to delete stale cones if there are any
                self.ctx.clean_stale_cones();
            }
        }
    }

    fn shutdown(&mut self) {
        self.pool.shutdown();
    }
}

/// Brane related timeout event
pub enum Event {
    CheckStalePeer,
    CheckApply,
}

impl<EK, ER, R> RunnableWithTimer for Runner<EK, ER, R>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
    R: CasualRouter<EK> + lightlike + Clone + 'static,
{
    type TimeoutTask = Event;

    fn on_timeout(&mut self, timer: &mut Timer<Event>, event: Event) {
        match event {
            Event::CheckApply => {
                self.handle_plightlikeing_applies();
                timer.add_task(
                    Duration::from_millis(PENDING_APPLY_CHECK_INTERVAL),
                    Event::CheckApply,
                );
            }
            Event::CheckStalePeer => {
                self.ctx.clean_stale_cones();
                timer.add_task(
                    Duration::from_millis(STALE_PEER_CHECK_INTERVAL),
                    Event::CheckStalePeer,
                );
            }
        }
    }
}

#[causet(test)]
mod tests {
    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::Duration;

    use crate::interlock::InterlockHost;
    use crate::store::peer_causet_storage::JOB_STATUS_PENDING;
    use crate::store::snap::tests::get_test_db_for_branes;
    use crate::store::worker::BraneRunner;
    use crate::store::{CasualMessage, SnapKey, SnapManager};
    use engine_lmdb::raw::PrimaryCausetNetworkOptions;
    use engine_lmdb::LmdbEngine;
    use edb::{
        CausetHandleExt, CausetNamesExt, CompactExt, MiscExt, MuBlock, Peekable, SyncMuBlock, WriteBatchExt,
    };
    use edb::{Engines, CausetEngine};
    use edb::{Causet_DEFAULT, Causet_VIOLETABFT};
    use ekvproto::violetabft_server_timeshare::{PeerState, VioletaBftApplyState, BraneLocalState};
    use violetabft::evioletabft_timeshare::Entry;
    use tempfile::Builder;
    use violetabftstore::interlock::::timer::Timer;
    use violetabftstore::interlock::::worker::Worker;

    use super::Event;
    use super::PlightlikeingDeleteCones;
    use super::Task;

    fn insert_cone(
        plightlikeing_delete_cones: &mut PlightlikeingDeleteCones,
        id: u64,
        s: &str,
        e: &str,
        stale_sequence: u64,
    ) {
        plightlikeing_delete_cones.insert(id, s.as_bytes(), e.as_bytes(), stale_sequence);
    }

    #[test]
    #[allow(clippy::string_lit_as_bytes)]
    fn test_plightlikeing_delete_cones() {
        let mut plightlikeing_delete_cones = PlightlikeingDeleteCones::default();
        let id = 0;

        let timeout1 = 10;
        insert_cone(&mut plightlikeing_delete_cones, id, "a", "c", timeout1);
        insert_cone(&mut plightlikeing_delete_cones, id, "m", "n", timeout1);
        insert_cone(&mut plightlikeing_delete_cones, id, "x", "z", timeout1);
        insert_cone(&mut plightlikeing_delete_cones, id + 1, "f", "i", timeout1);
        insert_cone(&mut plightlikeing_delete_cones, id + 1, "p", "t", timeout1);
        assert_eq!(plightlikeing_delete_cones.len(), 5);

        //  a____c    f____i    m____n    p____t    x____z
        //              g___________________q
        // when we want to insert [g, q), we first extract overlap cones,
        // which are [f, i), [m, n), [p, t)
        let timeout2 = 12;
        let overlap_cones =
            plightlikeing_delete_cones.drain_overlap_cones(&b"g".to_vec(), &b"q".to_vec());
        assert_eq!(
            overlap_cones,
            [
                (id + 1, b"f".to_vec(), b"i".to_vec(), timeout1),
                (id, b"m".to_vec(), b"n".to_vec(), timeout1),
                (id + 1, b"p".to_vec(), b"t".to_vec(), timeout1),
            ]
        );
        assert_eq!(plightlikeing_delete_cones.len(), 2);
        insert_cone(&mut plightlikeing_delete_cones, id + 2, "g", "q", timeout2);
        assert_eq!(plightlikeing_delete_cones.len(), 3);

        // at t1, [a, c) and [x, z) will timeout
        {
            let now = 11;
            let cones: Vec<_> = plightlikeing_delete_cones.stale_cones(now).collect();
            assert_eq!(
                cones,
                [
                    (id, "a".as_bytes(), "c".as_bytes()),
                    (id, "x".as_bytes(), "z".as_bytes()),
                ]
            );
            for spacelike_key in cones
                .into_iter()
                .map(|(_, spacelike, _)| spacelike.to_vec())
                .collect::<Vec<Vec<u8>>>()
            {
                plightlikeing_delete_cones.remove(&spacelike_key);
            }
            assert_eq!(plightlikeing_delete_cones.len(), 1);
        }

        // at t2, [g, q) will timeout
        {
            let now = 14;
            let cones: Vec<_> = plightlikeing_delete_cones.stale_cones(now).collect();
            assert_eq!(cones, [(id + 2, "g".as_bytes(), "q".as_bytes())]);
            for spacelike_key in cones
                .into_iter()
                .map(|(_, spacelike, _)| spacelike.to_vec())
                .collect::<Vec<Vec<u8>>>()
            {
                plightlikeing_delete_cones.remove(&spacelike_key);
            }
            assert_eq!(plightlikeing_delete_cones.len(), 0);
        }
    }

    #[test]
    fn test_stale_peer() {
        let temp_dir = Builder::new().prefix("test_stale_peer").temfidelir().unwrap();
        let engine = get_test_db_for_branes(&temp_dir, None, None, None, None, &[1]).unwrap();

        let snap_dir = Builder::new().prefix("snap_dir").temfidelir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let mut worker = Worker::new("brane-worker");
        let sched = worker.interlock_semaphore();
        let engines = Engines::new(engine.kv.clone(), engine.violetabft.clone());
        let (router, _) = mpsc::sync_channel(1);
        let runner = BraneRunner::new(
            engines,
            mgr,
            0,
            true,
            InterlockHost::<LmdbEngine>::default(),
            router,
        );
        let mut timer = Timer::new(1);
        timer.add_task(Duration::from_millis(100), Event::CheckStalePeer);
        worker.spacelike_with_timer(runner, timer).unwrap();

        engine.kv.put(b"k1", b"v1").unwrap();
        let snap = engine.kv.snapshot();
        engine.kv.put(b"k2", b"v2").unwrap();

        sched
            .schedule(Task::Destroy {
                brane_id: 1,
                spacelike_key: b"k1".to_vec(),
                lightlike_key: b"k2".to_vec(),
            })
            .unwrap();
        drop(snap);

        thread::sleep(Duration::from_millis(100));
        assert!(engine.kv.get_value(b"k1").unwrap().is_none());
        assert_eq!(engine.kv.get_value(b"k2").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_plightlikeing_applies() {
        let temp_dir = Builder::new()
            .prefix("test_plightlikeing_applies")
            .temfidelir()
            .unwrap();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_level_zero_slowdown_writes_trigger(5);
        causet_opts.set_disable_auto_compactions(true);
        let kv_causets_opts = vec![
            engine_lmdb::raw_util::CausetOptions::new("default", causet_opts.clone()),
            engine_lmdb::raw_util::CausetOptions::new("write", causet_opts.clone()),
            engine_lmdb::raw_util::CausetOptions::new("dagger", causet_opts.clone()),
            engine_lmdb::raw_util::CausetOptions::new("violetabft", causet_opts.clone()),
        ];
        let violetabft_causets_opt = engine_lmdb::raw_util::CausetOptions::new(Causet_DEFAULT, causet_opts);
        let engine = get_test_db_for_branes(
            &temp_dir,
            None,
            Some(violetabft_causets_opt),
            None,
            Some(kv_causets_opts),
            &[1, 2, 3, 4, 5, 6],
        )
        .unwrap();

        for causet_name in engine.kv.causet_names() {
            for i in 0..6 {
                let causet = engine.kv.causet_handle(causet_name).unwrap();
                engine.kv.put_causet(causet_name, &[i], &[i]).unwrap();
                engine.kv.put_causet(causet_name, &[i + 1], &[i + 1]).unwrap();
                engine.kv.flush_causet(causet_name, true).unwrap();
                // check level 0 files
                assert_eq!(
                    engine_lmdb::util::get_causet_num_files_at_level(
                        &engine.kv.as_inner(),
                        causet.as_inner(),
                        0
                    )
                    .unwrap(),
                    u64::from(i) + 1
                );
            }
        }

        let engines = Engines::new(engine.kv.clone(), engine.violetabft.clone());
        let snap_dir = Builder::new().prefix("snap_dir").temfidelir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let mut worker = Worker::new("snap-manager");
        let sched = worker.interlock_semaphore();
        let (router, receiver) = mpsc::sync_channel(1);
        let runner = BraneRunner::new(
            engines.clone(),
            mgr,
            0,
            true,
            InterlockHost::<LmdbEngine>::default(),
            router,
        );
        let mut timer = Timer::new(1);
        timer.add_task(Duration::from_millis(100), Event::CheckApply);
        worker.spacelike_with_timer(runner, timer).unwrap();

        let gen_and_apply_snap = |id: u64| {
            // construct snapshot
            let (tx, rx) = mpsc::sync_channel(1);
            let apply_state: VioletaBftApplyState = engines
                .kv
                .get_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(id))
                .unwrap()
                .unwrap();
            let idx = apply_state.get_applied_index();
            let entry = engines
                .violetabft
                .get_msg::<Entry>(&tuplespaceInstanton::violetabft_log_key(id, idx))
                .unwrap()
                .unwrap();
            sched
                .schedule(Task::Gen {
                    brane_id: id,
                    kv_snap: engines.kv.snapshot(),
                    last_applied_index_term: entry.get_term(),
                    last_applied_state: apply_state,
                    notifier: tx,
                })
                .unwrap();
            let s1 = rx.recv().unwrap();
            match receiver.recv() {
                Ok((brane_id, CasualMessage::SnapshotGenerated)) => {
                    assert_eq!(brane_id, id);
                }
                msg => panic!("expected SnapshotGenerated, but got {:?}", msg),
            }
            let data = s1.get_data();
            let key = SnapKey::from_snap(&s1).unwrap();
            let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
            let mut s2 = mgr.get_snapshot_for_lightlikeing(&key).unwrap();
            let mut s3 = mgr.get_snapshot_for_receiving(&key, &data[..]).unwrap();
            io::copy(&mut s2, &mut s3).unwrap();
            s3.save().unwrap();

            // set applying state
            let mut wb = engine.kv.write_batch();
            let brane_key = tuplespaceInstanton::brane_state_key(id);
            let mut brane_state = engine
                .kv
                .get_msg_causet::<BraneLocalState>(Causet_VIOLETABFT, &brane_key)
                .unwrap()
                .unwrap();
            brane_state.set_state(PeerState::Applying);
            wb.put_msg_causet(Causet_VIOLETABFT, &brane_key, &brane_state).unwrap();
            engine.kv.write(&wb).unwrap();

            // apply snapshot
            let status = Arc::new(AtomicUsize::new(JOB_STATUS_PENDING));
            sched
                .schedule(Task::Apply {
                    brane_id: id,
                    status,
                })
                .unwrap();
        };
        let wait_apply_finish = |id: u64| {
            let brane_key = tuplespaceInstanton::brane_state_key(id);
            loop {
                thread::sleep(Duration::from_millis(100));
                if engine
                    .kv
                    .get_msg_causet::<BraneLocalState>(Causet_VIOLETABFT, &brane_key)
                    .unwrap()
                    .unwrap()
                    .get_state()
                    == PeerState::Normal
                {
                    break;
                }
            }
        };
        let causet = engine.kv.causet_handle(Causet_DEFAULT).unwrap().as_inner();

        // snapshot will not ingest cause already write stall
        gen_and_apply_snap(1);
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            6
        );

        // compact all files to the bottomest level
        engine.kv.compact_files_in_cone(None, None, None).unwrap();
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            0
        );

        wait_apply_finish(1);

        // the plightlikeing apply task should be finished and snapshots are ingested.
        // note that when ingest sst, it may flush memBlock if overlap,
        // so here will two level 0 files.
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            2
        );

        // no write stall, ingest without delay
        gen_and_apply_snap(2);
        wait_apply_finish(2);
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            4
        );

        // snapshot will not ingest cause it may cause write stall
        gen_and_apply_snap(3);
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            4
        );
        gen_and_apply_snap(4);
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            4
        );
        gen_and_apply_snap(5);
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            4
        );

        // compact all files to the bottomest level
        engine.kv.compact_files_in_cone(None, None, None).unwrap();
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            0
        );

        // make sure have checked plightlikeing applies
        wait_apply_finish(4);

        // before two plightlikeing apply tasks should be finished and snapshots are ingested
        // and one still in plightlikeing.
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            4
        );

        // make sure have checked plightlikeing applies
        engine.kv.compact_files_in_cone(None, None, None).unwrap();
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            0
        );
        wait_apply_finish(5);

        // the last one plightlikeing task finished
        assert_eq!(
            engine_lmdb::util::get_causet_num_files_at_level(engine.kv.as_inner(), causet, 0).unwrap(),
            2
        );
    }
}
