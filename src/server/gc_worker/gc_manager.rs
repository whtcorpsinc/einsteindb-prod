// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use fidel_client::ClusterVersion;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{mpsc, Arc};
use std::thread::{self, Builder as ThreadBuilder, JoinHandle};
use std::time::{Duration, Instant};
use einsteindb_util::worker::FutureInterlock_Semaphore;
use txn_types::{Key, TimeStamp};

use crate::server::metrics::*;
use violetabftstore::interlock::BraneInfoProvider;
use violetabftstore::store::util::find_peer;

use super::config::GcWorkerConfigManager;
use super::gc_worker::{sync_gc, GcSafePointProvider, GcTask};
use super::{is_compaction_filter_allowd, Result};

const POLL_SAFE_POINT_INTERVAL_SECS: u64 = 60;

const BEGIN_KEY: &[u8] = b"";

const PROCESS_TYPE_GC: &str = "gc";
const PROCESS_TYPE_SCAN: &str = "scan";

/// The configurations of automatic GC.
pub struct AutoGcConfig<S: GcSafePointProvider, R: BraneInfoProvider> {
    pub safe_point_provider: S,
    pub brane_info_provider: R,

    /// Used to find which peer of a brane is on this EinsteinDB, so that we can compose a `Context`.
    pub self_store_id: u64,

    pub poll_safe_point_interval: Duration,

    /// If this is set, safe_point will be checked before doing GC on every brane while working.
    /// Otherwise safe_point will be only checked when `poll_safe_point_interval` has past since
    /// last checking.
    pub always_check_safe_point: bool,

    /// This will be called when a round of GC has finished and goes back to idle state.
    /// This field is for test purpose.
    pub post_a_round_of_gc: Option<Box<dyn Fn() + Slightlike>>,
}

impl<S: GcSafePointProvider, R: BraneInfoProvider> AutoGcConfig<S, R> {
    /// Creates a new config.
    pub fn new(safe_point_provider: S, brane_info_provider: R, self_store_id: u64) -> Self {
        Self {
            safe_point_provider,
            brane_info_provider,
            self_store_id,
            poll_safe_point_interval: Duration::from_secs(POLL_SAFE_POINT_INTERVAL_SECS),
            always_check_safe_point: false,
            post_a_round_of_gc: None,
        }
    }

    /// Creates a config for test purpose. The interval to poll safe point is as short as 0.1s and
    /// during GC it never skips checking safe point.
    pub fn new_test_causet(
        safe_point_provider: S,
        brane_info_provider: R,
        self_store_id: u64,
    ) -> Self {
        Self {
            safe_point_provider,
            brane_info_provider,
            self_store_id,
            poll_safe_point_interval: Duration::from_millis(100),
            always_check_safe_point: true,
            post_a_round_of_gc: None,
        }
    }
}

/// The only error that will break `GcManager`'s process is that the `GcManager` is interrupted by
/// others, maybe due to EinsteinDB shutting down.
#[derive(Debug)]
enum GcManagerError {
    Stopped,
}

type GcManagerResult<T> = std::result::Result<T, GcManagerError>;

/// Used to check if `GcManager` should be stopped.
///
/// When `GcManager` is running, it might take very long time to GC a round. It should be able to
/// break at any time so that we can shut down EinsteinDB in time.
pub(super) struct GcManagerContext {
    /// Used to receive stop signal. The slightlikeer side is hold in `GcManagerHandle`.
    /// If this field is `None`, the `GcManagerContext` will never stop.
    stop_signal_receiver: Option<mpsc::Receiver<()>>,
    /// Whether an stop signal is received.
    is_stopped: bool,
}

impl GcManagerContext {
    pub fn new() -> Self {
        Self {
            stop_signal_receiver: None,
            is_stopped: false,
        }
    }

    /// Sets the receiver that used to receive the stop signal. `GcManagerContext` will be
    /// considered to be stopped as soon as a message is received from the receiver.
    pub fn set_stop_signal_receiver(&mut self, rx: mpsc::Receiver<()>) {
        self.stop_signal_receiver = Some(rx);
    }

    /// Sleeps for a while. if a stop message is received, returns immediately with
    /// `GcManagerError::Stopped`.
    fn sleep_or_stop(&mut self, timeout: Duration) -> GcManagerResult<()> {
        if self.is_stopped {
            return Err(GcManagerError::Stopped);
        }
        match self.stop_signal_receiver.as_ref() {
            Some(rx) => match rx.recv_timeout(timeout) {
                Ok(_) => {
                    self.is_stopped = true;
                    Err(GcManagerError::Stopped)
                }
                Err(mpsc::RecvTimeoutError::Timeout) => Ok(()),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("stop_signal_receiver unexpectedly disconnected")
                }
            },
            None => {
                thread::sleep(timeout);
                Ok(())
            }
        }
    }

    /// Checks if a stop message has been fired. Returns `GcManagerError::Stopped` if there's such
    /// a message.
    fn check_stopped(&mut self) -> GcManagerResult<()> {
        if self.is_stopped {
            return Err(GcManagerError::Stopped);
        }
        match self.stop_signal_receiver.as_ref() {
            Some(rx) => match rx.try_recv() {
                Ok(_) => {
                    self.is_stopped = true;
                    Err(GcManagerError::Stopped)
                }
                Err(mpsc::TryRecvError::Empty) => Ok(()),
                Err(mpsc::TryRecvError::Disconnected) => {
                    error!("stop_signal_receiver unexpectedly disconnected, gc_manager will stop");
                    Err(GcManagerError::Stopped)
                }
            },
            None => Ok(()),
        }
    }
}

/// Used to represent the state of `GcManager`.
#[derive(PartialEq)]
enum GcManagerState {
    None,
    Init,
    Idle,
    Working,
}

impl GcManagerState {
    pub fn tag(&self) -> &str {
        match self {
            GcManagerState::None => "",
            GcManagerState::Init => "initializing",
            GcManagerState::Idle => "idle",
            GcManagerState::Working => "working",
        }
    }
}

#[inline]
fn set_status_metrics(state: GcManagerState) {
    for s in &[
        GcManagerState::Init,
        GcManagerState::Idle,
        GcManagerState::Working,
    ] {
        AUTO_GC_STATUS_GAUGE_VEC
            .with_label_values(&[s.tag()])
            .set(if state == *s { 1 } else { 0 });
    }
}

/// Wraps `JoinHandle` of `GcManager` and helps to stop the `GcManager` synchronously.
pub(super) struct GcManagerHandle {
    join_handle: JoinHandle<()>,
    stop_signal_slightlikeer: mpsc::Slightlikeer<()>,
}

impl GcManagerHandle {
    /// Stops the `GcManager`.
    pub fn stop(self) -> Result<()> {
        let res: Result<()> = self
            .stop_signal_slightlikeer
            .slightlike(())
            .map_err(|e| box_err!("failed to slightlike stop signal to gc worker thread: {:?}", e));
        if res.is_err() {
            return res;
        }
        self.join_handle
            .join()
            .map_err(|e| box_err!("failed to join gc worker thread: {:?}", e))
    }
}

/// Controls how GC runs automatically on the EinsteinDB.
/// It polls safe point periodically, and when the safe point is ufidelated, `GcManager` will spacelike to
/// scan all branes (whose leader is on this EinsteinDB), and does GC on all those branes.
pub(super) struct GcManager<S: GcSafePointProvider, R: BraneInfoProvider> {
    causet: AutoGcConfig<S, R>,

    /// The current safe point. `GcManager` will try to ufidelate it periodically. When `safe_point` is
    /// ufidelated, `GCManager` will spacelike to do GC on all branes.
    safe_point: Arc<AtomicU64>,

    safe_point_last_check_time: Instant,

    /// Used to schedule `GcTask`s.
    worker_interlock_semaphore: FutureInterlock_Semaphore<GcTask>,

    /// Holds the running status. It will tell us if `GcManager` should stop working and exit.
    gc_manager_ctx: GcManagerContext,

    causet_tracker: GcWorkerConfigManager,
    cluster_version: ClusterVersion,
}

impl<S: GcSafePointProvider, R: BraneInfoProvider> GcManager<S, R> {
    pub fn new(
        causet: AutoGcConfig<S, R>,
        safe_point: Arc<AtomicU64>,
        worker_interlock_semaphore: FutureInterlock_Semaphore<GcTask>,
        causet_tracker: GcWorkerConfigManager,
        cluster_version: ClusterVersion,
    ) -> GcManager<S, R> {
        GcManager {
            causet,
            safe_point,
            safe_point_last_check_time: Instant::now(),
            worker_interlock_semaphore,
            gc_manager_ctx: GcManagerContext::new(),
            causet_tracker,
            cluster_version,
        }
    }

    fn curr_safe_point(&self) -> TimeStamp {
        let ts = self.safe_point.load(AtomicOrdering::Relaxed);
        TimeStamp::new(ts)
    }

    fn save_safe_point(&self, ts: TimeStamp) {
        self.safe_point
            .store(ts.into_inner(), AtomicOrdering::Relaxed);
    }

    /// Starts working in another thread. This function moves the `GcManager` and returns a handler
    /// of it.
    pub fn spacelike(mut self) -> Result<GcManagerHandle> {
        set_status_metrics(GcManagerState::Init);
        self.initialize();

        let (tx, rx) = mpsc::channel();
        self.gc_manager_ctx.set_stop_signal_receiver(rx);
        let res: Result<_> = ThreadBuilder::new()
            .name(thd_name!("gc-manager"))
            .spawn(move || {
                einsteindb_alloc::add_thread_memory_accessor();
                self.run();
                einsteindb_alloc::remove_thread_memory_accessor();
            })
            .map_err(|e| box_err!("failed to spacelike gc manager: {:?}", e));
        res.map(|join_handle| GcManagerHandle {
            join_handle,
            stop_signal_slightlikeer: tx,
        })
    }

    /// Polls safe point and does GC in a loop, again and again, until interrupted by invoking
    /// `GcManagerHandle::stop`.
    fn run(&mut self) {
        debug!("gc-manager is spacelikeed");
        self.run_impl().unwrap_err();
        set_status_metrics(GcManagerState::None);
        debug!("gc-manager is stopped");
    }

    fn run_impl(&mut self) -> GcManagerResult<()> {
        loop {
            AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                .with_label_values(&[PROCESS_TYPE_GC])
                .set(0);
            AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                .with_label_values(&[PROCESS_TYPE_SCAN])
                .set(0);

            set_status_metrics(GcManagerState::Idle);
            self.wait_for_next_safe_point()?;

            // Don't need to run GC any more if compaction filter is enabled.
            if !is_compaction_filter_allowd(&*self.causet_tracker.value(), &self.cluster_version) {
                set_status_metrics(GcManagerState::Working);
                self.gc_a_round()?;
                if let Some(on_finished) = self.causet.post_a_round_of_gc.as_ref() {
                    on_finished();
                }
            }
        }
    }

    /// Sets the initial state of the `GCManger`.
    /// The only task of initializing is to simply get the current safe point as the initial value
    /// of `safe_point`. EinsteinDB won't do any GC automatically until the first time `safe_point` was
    /// ufidelated to a greater value than initial value.
    fn initialize(&mut self) {
        debug!("gc-manager is initializing");
        self.save_safe_point(TimeStamp::zero());
        self.try_ufidelate_safe_point();
        debug!("gc-manager spacelikeed"; "safe_point" => self.curr_safe_point());
    }

    /// Waits until the safe_point ufidelates. Returns the new safe point.
    fn wait_for_next_safe_point(&mut self) -> GcManagerResult<TimeStamp> {
        loop {
            if self.try_ufidelate_safe_point() {
                return Ok(self.curr_safe_point());
            }

            self.gc_manager_ctx
                .sleep_or_stop(self.causet.poll_safe_point_interval)?;
        }
    }

    /// Tries to ufidelate the safe point. Returns true if safe point has been ufidelated to a greater
    /// value. Returns false if safe point didn't change or we encountered an error.
    fn try_ufidelate_safe_point(&mut self) -> bool {
        self.safe_point_last_check_time = Instant::now();

        let safe_point = match self.causet.safe_point_provider.get_safe_point() {
            Ok(res) => res,
            // Return false directly so we will check it a while later.
            Err(e) => {
                error!(?e; "failed to get safe point from fidel");
                return false;
            }
        };

        let old_safe_point = self.curr_safe_point();
        match safe_point.cmp(&old_safe_point) {
            Ordering::Less => {
                panic!(
                    "got new safe point {} which is less than current safe point {}. \
                     there must be something wrong.",
                    safe_point, old_safe_point,
                );
            }
            Ordering::Equal => false,
            Ordering::Greater => {
                debug!("gc_worker: ufidelate safe point"; "safe_point" => safe_point);
                self.save_safe_point(safe_point);
                AUTO_GC_SAFE_POINT_GAUGE.set(safe_point.into_inner() as i64);
                true
            }
        }
    }

    /// Scans all branes on the EinsteinDB whose leader is this EinsteinDB, and does GC on all of them.
    /// Branes are scanned and GC-ed in lexicographical order.
    ///
    /// While the `gc_a_round` function is running, it will periodically check whether safe_point is
    /// ufidelated before the function `gc_a_round` finishes. If so, *Rewinding* will occur. For
    /// example, when we just spacelikes to do GC, our progress is like this: ('^' means our current
    /// progress)
    ///
    /// ```text
    /// | brane 1 | brane 2 | brane 3| brane 4 | brane 5 | brane 6 |
    /// ^
    /// ```
    ///
    /// And after a while, our GC progress is like this:
    ///
    /// ```text
    /// | brane 1 | brane 2 | brane 3| brane 4 | brane 5 | brane 6 |
    /// ----------------------^
    /// ```
    ///
    /// At this time we found that safe point was ufidelated, so rewinding will happen. First we
    /// continue working to the lightlike: ('#' indicates the position that safe point ufidelates)
    ///
    /// ```text
    /// | brane 1 | brane 2 | brane 3| brane 4 | brane 5 | brane 6 |
    /// ----------------------#------------------------------------------^
    /// ```
    ///
    /// Then brane 1-2 were GC-ed with the old safe point and brane 3-6 were GC-ed with the new
    /// new one. Then, we *rewind* to the very beginning and continue GC to the position that safe
    /// point ufidelates:
    ///
    /// ```text
    /// | brane 1 | brane 2 | brane 3| brane 4 | brane 5 | brane 6 |
    /// ----------------------#------------------------------------------^
    /// ----------------------^
    /// ```
    ///
    /// Then GC finishes.
    /// If safe point ufidelates again at some time, it will still try to GC all branes with the
    /// latest safe point. If safe point always ufidelates before `gc_a_round` finishes, `gc_a_round`
    /// may never stop, but it doesn't matter.
    fn gc_a_round(&mut self) -> GcManagerResult<()> {
        let mut need_rewind = false;
        // Represents where we should stop doing GC. `None` means the very lightlike of the EinsteinDB.
        let mut lightlike = None;
        // Represents where we have GC-ed to. `None` means the very lightlike of the EinsteinDB.
        let mut progress = Some(Key::from_encoded(BEGIN_KEY.to_vec()));

        // Records how many brane we have GC-ed.
        let mut processed_branes = 0;

        info!(
            "gc_worker: spacelike auto gc"; "safe_point" => self.curr_safe_point()
        );

        // The following loop iterates all branes whose leader is on this EinsteinDB and does GC on them.
        // At the same time, check whether safe_point is ufidelated periodically. If it's ufidelated,
        // rewinding will happen.
        loop {
            self.gc_manager_ctx.check_stopped()?;

            // Check the current GC progress and determine if we are going to rewind or we have
            // finished the round of GC.
            if need_rewind {
                if progress.is_none() {
                    // We have worked to the lightlike and we need to rewind. Respacelike from beginning.
                    progress = Some(Key::from_encoded(BEGIN_KEY.to_vec()));
                    need_rewind = false;
                    info!(
                        "gc_worker: auto gc rewinds"; "processed_branes" => processed_branes
                    );

                    processed_branes = 0;
                    // Set the metric to zero to show that rewinding has happened.
                    AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                        .with_label_values(&[PROCESS_TYPE_GC])
                        .set(0);
                    AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                        .with_label_values(&[PROCESS_TYPE_SCAN])
                        .set(0);
                }
            } else {
                // We are not going to rewind, So we will stop if `progress` reaches `lightlike`.
                let finished = match (progress.as_ref(), lightlike.as_ref()) {
                    (None, _) => true,
                    (Some(p), Some(e)) => p >= e,
                    _ => false,
                };
                if finished {
                    // We have worked to the lightlike of the EinsteinDB or our progress has reached `lightlike`, and we
                    // don't need to rewind. In this case, the round of GC has finished.
                    info!(
                        "gc_worker: finished auto gc"; "processed_branes" => processed_branes
                    );
                    return Ok(());
                }
            }

            assert!(progress.is_some());

            // Before doing GC, check whether safe_point is ufidelated periodically to determine if
            // rewinding is needed.
            self.check_if_need_rewind(&progress, &mut need_rewind, &mut lightlike);

            progress = self.gc_next_brane(progress.unwrap(), &mut processed_branes)?;
        }
    }

    /// Checks whether we need to rewind in this round of GC. Only used in `gc_a_round`.
    fn check_if_need_rewind(
        &mut self,
        progress: &Option<Key>,
        need_rewind: &mut bool,
        lightlike: &mut Option<Key>,
    ) {
        if self.safe_point_last_check_time.elapsed() < self.causet.poll_safe_point_interval
            && !self.causet.always_check_safe_point
        {
            // Skip this check.
            return;
        }

        if !self.try_ufidelate_safe_point() {
            // Safe point not ufidelated. Skip it.
            return;
        }

        if progress.as_ref().unwrap().as_encoded().is_empty() {
            // `progress` is empty means the spacelikeing. We don't need to rewind. We just
            // continue GC to the lightlike.
            *need_rewind = false;
            *lightlike = None;
            info!(
                "gc_worker: auto gc will go to the lightlike"; "safe_point" => self.curr_safe_point()
            );
        } else {
            *need_rewind = true;
            *lightlike = progress.clone();
            info!(
                "gc_worker: auto gc will go to rewind"; "safe_point" => self.curr_safe_point(),
                "next_rewind_key" => %(lightlike.as_ref().unwrap())
            );
        }
    }

    /// Does GC on the next brane after `from_key`. Returns the lightlike key of the brane it processed.
    /// If we have processed to the lightlike of all branes, returns `None`.
    fn gc_next_brane(
        &mut self,
        from_key: Key,
        processed_branes: &mut usize,
    ) -> GcManagerResult<Option<Key>> {
        // Get the information of the next brane to do GC.
        let (cone, next_key) = self.get_next_gc_context(from_key);
        let (brane_id, spacelike, lightlike) = match cone {
            Some((r, s, e)) => (r, s, e),
            None => return Ok(None),
        };

        let hex_spacelike = hex::encode_upper(&spacelike);
        let hex_lightlike = hex::encode_upper(&lightlike);
        debug!("trying gc"; "spacelike_key" => &hex_spacelike, "lightlike_key" => &hex_lightlike);

        if let Err(e) = sync_gc(
            &self.worker_interlock_semaphore,
            brane_id,
            spacelike,
            lightlike,
            self.curr_safe_point(),
        ) {
            // Ignore the error and continue, since it's useless to retry this.
            // TODO: Find a better way to handle errors. Maybe we should retry.
            warn!("failed gc"; "spacelike_key" => &hex_spacelike, "lightlike_key" => &hex_lightlike, "err" => ?e);
        }

        *processed_branes += 1;
        AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
            .with_label_values(&[PROCESS_TYPE_GC])
            .inc();

        Ok(next_key)
    }

    /// Gets the next brane with lightlike_key greater than given key.
    /// Returns a tuple with 2 fields:
    /// the first is the next brane can be sent to GC worker;
    /// the second is the next key which can be passed into this method later.
    #[allow(clippy::type_complexity)]
    fn get_next_gc_context(&mut self, key: Key) -> (Option<(u64, Vec<u8>, Vec<u8>)>, Option<Key>) {
        let (tx, rx) = mpsc::channel();
        let store_id = self.causet.self_store_id;

        let res = self.causet.brane_info_provider.seek_brane(
            key.as_encoded(),
            Box::new(move |iter| {
                let mut scanned_branes = 0;
                for info in iter {
                    scanned_branes += 1;
                    if find_peer(&info.brane, store_id).is_some() {
                        let _ = tx.slightlike((Some(info.brane.clone()), scanned_branes));
                        return;
                    }
                }
                let _ = tx.slightlike((None, scanned_branes));
            }),
        );

        if let Err(e) = res {
            error!(?e; "gc_worker: failed to get next brane information");
            return (None, None);
        };

        let seek_brane_res = rx.recv().map(|(brane, scanned_branes)| {
            AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                .with_label_values(&[PROCESS_TYPE_SCAN])
                .add(scanned_branes);
            brane
        });

        match seek_brane_res {
            Ok(Some(mut brane)) => {
                let r = brane.get_id();
                let (s, e) = (brane.take_spacelike_key(), brane.take_lightlike_key());
                let next_key = if e.is_empty() {
                    None
                } else {
                    Some(Key::from_encoded_slice(&e))
                };
                (Some((r, s, e)), next_key)
            }
            Ok(None) => (None, None),
            Err(e) => {
                error!("failed to get next brane information"; "err" => ?e);
                (None, None)
            }
        }
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use crate::causetStorage::Callback;
    use ekvproto::metapb;
    use violetabft::StateRole;
    use violetabftstore::interlock::Result as CopResult;
    use violetabftstore::interlock::{BraneInfo, SeekBraneCallback};
    use violetabftstore::store::util::new_peer;
    use std::collections::BTreeMap;
    use std::mem;
    use std::sync::mpsc::{channel, Receiver, Slightlikeer};
    use einsteindb_util::worker::{FutureRunnable, FutureWorker};

    fn take_callback(t: &mut GcTask) -> Callback<()> {
        let callback = match t {
            GcTask::Gc {
                ref mut callback, ..
            } => callback,
            GcTask::UnsafeDestroyCone {
                ref mut callback, ..
            } => callback,
            GcTask::PhysicalScanLock { .. } => unreachable!(),
            GcTask::Validate(_) => unreachable!(),
        };
        mem::replace(callback, Box::new(|_| {}))
    }

    struct MockSafePointProvider {
        rx: Receiver<TimeStamp>,
    }

    impl GcSafePointProvider for MockSafePointProvider {
        fn get_safe_point(&self) -> Result<TimeStamp> {
            // Error will be ignored by `GcManager`, which is equivalent to that the safe_point
            // is not ufidelated.
            self.rx.try_recv().map_err(|e| box_err!(e))
        }
    }

    #[derive(Clone)]
    struct MockBraneInfoProvider {
        // spacelike_key -> (brane_id, lightlike_key)
        branes: BTreeMap<Vec<u8>, BraneInfo>,
    }

    impl BraneInfoProvider for MockBraneInfoProvider {
        fn seek_brane(&self, from: &[u8], callback: SeekBraneCallback) -> CopResult<()> {
            let from = from.to_vec();
            callback(&mut self.branes.cone(from..).map(|(_, v)| v));
            Ok(())
        }
    }

    struct MockGcRunner {
        tx: Slightlikeer<GcTask>,
    }

    impl FutureRunnable<GcTask> for MockGcRunner {
        fn run(&mut self, mut t: GcTask) {
            let cb = take_callback(&mut t);
            self.tx.slightlike(t).unwrap();
            cb(Ok(()));
        }
    }

    /// A set of utilities that helps testing `GcManager`.
    /// The safe_point polling interval is set to 100 ms.
    struct GcManagerTestUtil {
        gc_manager: Option<GcManager<MockSafePointProvider, MockBraneInfoProvider>>,
        worker: FutureWorker<GcTask>,
        safe_point_slightlikeer: Slightlikeer<TimeStamp>,
        gc_task_receiver: Receiver<GcTask>,
    }

    impl GcManagerTestUtil {
        pub fn new(branes: BTreeMap<Vec<u8>, BraneInfo>) -> Self {
            let mut worker = FutureWorker::new("test-gc-worker");
            let (gc_task_slightlikeer, gc_task_receiver) = channel();
            worker.spacelike(MockGcRunner { tx: gc_task_slightlikeer }).unwrap();

            let (safe_point_slightlikeer, safe_point_receiver) = channel();

            let mut causet = AutoGcConfig::new(
                MockSafePointProvider {
                    rx: safe_point_receiver,
                },
                MockBraneInfoProvider { branes },
                1,
            );
            causet.poll_safe_point_interval = Duration::from_millis(100);
            causet.always_check_safe_point = true;

            let gc_manager = GcManager::new(
                causet,
                Arc::new(AtomicU64::new(0)),
                worker.interlock_semaphore(),
                GcWorkerConfigManager::default(),
                Default::default(),
            );
            Self {
                gc_manager: Some(gc_manager),
                worker,
                safe_point_slightlikeer,
                gc_task_receiver,
            }
        }

        /// Collect `GcTask`s that `GcManager` tried to execute.
        pub fn collect_scheduled_tasks(&self) -> Vec<GcTask> {
            self.gc_task_receiver.try_iter().collect()
        }

        pub fn add_next_safe_point(&self, safe_point: impl Into<TimeStamp>) {
            self.safe_point_slightlikeer.slightlike(safe_point.into()).unwrap();
        }

        pub fn stop(&mut self) {
            self.worker.stop().unwrap().join().unwrap();
        }
    }

    /// Run a round of auto GC and check if it correctly GC branes as expected.
    ///
    /// Param `branes` is a `Vec` of tuples which is `(spacelike_key, lightlike_key, brane_id)`
    ///
    /// The first value in param `safe_points` will be used to initialize the GcManager, and the remaining
    /// values will be checked before every time GC-ing a brane. If the length of `safe_points` is
    /// less than executed GC tasks, the last value will be used for extra GC tasks.
    ///
    /// Param `expected_gc_tasks` is a `Vec` of tuples which is `(brane_id, safe_point)`.
    fn test_auto_gc(
        branes: Vec<(Vec<u8>, Vec<u8>, u64)>,
        safe_points: Vec<impl Into<TimeStamp> + Copy>,
        expected_gc_tasks: Vec<(u64, impl Into<TimeStamp>)>,
    ) {
        let branes: BTreeMap<_, _> = branes
            .into_iter()
            .map(|(spacelike_key, lightlike_key, id)| {
                let mut r = metapb::Brane::default();
                r.set_id(id);
                r.set_spacelike_key(spacelike_key.clone());
                r.set_lightlike_key(lightlike_key);
                r.mut_peers().push(new_peer(1, 1));
                let info = BraneInfo::new(r, StateRole::Leader);
                (spacelike_key, info)
            })
            .collect();

        let mut test_util = GcManagerTestUtil::new(branes);

        for safe_point in &safe_points {
            test_util.add_next_safe_point(*safe_point);
        }
        test_util.gc_manager.as_mut().unwrap().initialize();

        test_util.gc_manager.as_mut().unwrap().gc_a_round().unwrap();
        test_util.stop();

        let gc_tasks: Vec<_> = test_util
            .collect_scheduled_tasks()
            .iter()
            .map(|task| match task {
                GcTask::Gc {
                    brane_id,
                    safe_point,
                    ..
                } => (*brane_id, *safe_point),
                _ => unreachable!(),
            })
            .collect();

        // Following code asserts gc_tasks == expected_gc_tasks.
        assert_eq!(gc_tasks.len(), expected_gc_tasks.len());
        let all_passed = gc_tasks.into_iter().zip(expected_gc_tasks.into_iter()).all(
            |((brane, safe_point), (expect_brane, expect_safe_point))| {
                brane == expect_brane && safe_point == expect_safe_point.into()
            },
        );
        assert!(all_passed);
    }

    #[test]
    fn test_ufidelate_safe_point() {
        let mut test_util = GcManagerTestUtil::new(BTreeMap::new());
        let mut gc_manager = test_util.gc_manager.take().unwrap();
        assert_eq!(gc_manager.curr_safe_point(), TimeStamp::zero());
        test_util.add_next_safe_point(233);
        assert!(gc_manager.try_ufidelate_safe_point());
        assert_eq!(gc_manager.curr_safe_point(), 233.into());

        let (tx, rx) = channel();
        ThreadBuilder::new()
            .spawn(move || {
                let safe_point = gc_manager.wait_for_next_safe_point().unwrap();
                tx.slightlike(safe_point).unwrap();
            })
            .unwrap();
        test_util.add_next_safe_point(233);
        test_util.add_next_safe_point(233);
        test_util.add_next_safe_point(234);
        assert_eq!(rx.recv().unwrap(), 234.into());

        test_util.stop();
    }

    #[test]
    fn test_gc_manager_initialize() {
        let mut test_util = GcManagerTestUtil::new(BTreeMap::new());
        let mut gc_manager = test_util.gc_manager.take().unwrap();
        assert_eq!(gc_manager.curr_safe_point(), TimeStamp::zero());
        test_util.add_next_safe_point(0);
        test_util.add_next_safe_point(5);
        gc_manager.initialize();
        assert_eq!(gc_manager.curr_safe_point(), TimeStamp::zero());
        assert!(gc_manager.try_ufidelate_safe_point());
        assert_eq!(gc_manager.curr_safe_point(), 5.into());
    }

    #[test]
    fn test_auto_gc_a_round_without_rewind() {
        // First brane spacelikes with empty and last brane lightlikes with empty.
        let branes = vec![
            (b"".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"".to_vec(), 4),
        ];
        test_auto_gc(
            branes,
            vec![233],
            vec![(1, 233), (2, 233), (3, 233), (4, 233)],
        );

        // First brane doesn't spacelikes with empty and last brane doesn't lightlikes with empty.
        let branes = vec![
            (b"0".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"8".to_vec(), 4),
        ];
        test_auto_gc(
            branes,
            vec![233],
            vec![(1, 233), (2, 233), (3, 233), (4, 233)],
        );
    }

    #[test]
    fn test_auto_gc_rewinding() {
        for branes in vec![
            // First brane spacelikes with empty and last brane lightlikes with empty.
            vec![
                (b"".to_vec(), b"1".to_vec(), 1),
                (b"1".to_vec(), b"2".to_vec(), 2),
                (b"3".to_vec(), b"4".to_vec(), 3),
                (b"7".to_vec(), b"".to_vec(), 4),
            ],
            // First brane doesn't spacelikes with empty and last brane doesn't lightlikes with empty.
            vec![
                (b"0".to_vec(), b"1".to_vec(), 1),
                (b"1".to_vec(), b"2".to_vec(), 2),
                (b"3".to_vec(), b"4".to_vec(), 3),
                (b"7".to_vec(), b"8".to_vec(), 4),
            ],
        ] {
            test_auto_gc(
                branes.clone(),
                vec![233, 234],
                vec![(1, 234), (2, 234), (3, 234), (4, 234)],
            );
            test_auto_gc(
                branes.clone(),
                vec![233, 233, 234],
                vec![(1, 233), (2, 234), (3, 234), (4, 234), (1, 234)],
            );
            test_auto_gc(
                branes.clone(),
                vec![233, 233, 233, 233, 234],
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 233),
                    (4, 234),
                    (1, 234),
                    (2, 234),
                    (3, 234),
                ],
            );
            test_auto_gc(
                branes.clone(),
                vec![233, 233, 233, 234, 235],
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 234),
                    (4, 235),
                    (1, 235),
                    (2, 235),
                    (3, 235),
                ],
            );

            let mut safe_points = vec![233, 233, 233, 234, 234, 234, 235];
            // The logic of `gc_a_round` wastes a loop when the last brane's lightlike_key is not null, so it
            // will check safe point one more time before GC-ing the first brane after rewinding.
            if !branes.last().unwrap().1.is_empty() {
                safe_points.insert(5, 234);
            }
            test_auto_gc(
                branes.clone(),
                safe_points,
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 234),
                    (4, 234),
                    (1, 234),
                    (2, 235),
                    (3, 235),
                    (4, 235),
                    (1, 235),
                ],
            );
        }
    }
}
