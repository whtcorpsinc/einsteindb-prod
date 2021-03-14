//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::f64::INFINITY;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use concurrency_manager::ConcurrencyManager;
use engine_lmdb::LmdbEngine;
use engine_promises::{MiscExt, CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_WRITE};
use futures::executor::block_on;
use ekvproto::kvrpcpb::{Context, IsolationLevel, LockInfo};
use fidel_client::{ClusterVersion, FidelClient};
use violetabftstore::interlock::{InterlockHost, BraneInfoProvider};
use violetabftstore::router::VioletaBftStoreRouter;
use violetabftstore::store::msg::StoreMsg;
use einsteindb_util::config::{Tracker, VersionTrack};
use einsteindb_util::time::{duration_to_sec, Limiter, SlowTimer};
use einsteindb_util::worker::{
    FutureRunnable, FutureInterlock_Semaphore, FutureWorker, Stopped as FutureWorkerStopped,
};
use txn_types::{Key, TimeStamp};

use crate::server::metrics::*;
use crate::causetStorage::kv::{Engine, ScanMode, Statistics};
use crate::causetStorage::tail_pointer::{check_need_gc, Error as MvccError, GcInfo, MvccReader, MvccTxn};

use super::applied_lock_collector::{AppliedLockCollector, Callback as LockCollectorCallback};
use super::config::{GcConfig, GcWorkerConfigManager};
use super::gc_manager::{AutoGcConfig, GcManager, GcManagerHandle};
use super::{Callback, CompactionFilterInitializer, Error, ErrorInner, Result};

/// After the GC scan of a key, output a message to the log if there are at least this many
/// versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at least this many
/// versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

pub const GC_MAX_EXECUTING_TASKS: usize = 10;
const GC_TASK_SLOW_SECONDS: u64 = 30;

/// Provides safe point.
pub trait GcSafePointProvider: Slightlike + 'static {
    fn get_safe_point(&self) -> Result<TimeStamp>;
}

impl<T: FidelClient + 'static> GcSafePointProvider for Arc<T> {
    fn get_safe_point(&self) -> Result<TimeStamp> {
        block_on(self.get_gc_safe_point())
            .map(Into::into)
            .map_err(|e| box_err!("failed to get safe point from FIDel: {:?}", e))
    }
}

pub enum GcTask {
    Gc {
        brane_id: u64,
        spacelike_key: Vec<u8>,
        lightlike_key: Vec<u8>,
        safe_point: TimeStamp,
        callback: Callback<()>,
    },
    UnsafeDestroyCone {
        ctx: Context,
        spacelike_key: Key,
        lightlike_key: Key,
        callback: Callback<()>,
    },
    PhysicalScanLock {
        ctx: Context,
        max_ts: TimeStamp,
        spacelike_key: Key,
        limit: usize,
        callback: Callback<Vec<LockInfo>>,
    },
    #[causet(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&GcConfig, &Limiter) + Slightlike>),
}

impl GcTask {
    pub fn get_enum_label(&self) -> GcCommandKind {
        match self {
            GcTask::Gc { .. } => GcCommandKind::gc,
            GcTask::UnsafeDestroyCone { .. } => GcCommandKind::unsafe_destroy_cone,
            GcTask::PhysicalScanLock { .. } => GcCommandKind::physical_scan_lock,
            #[causet(any(test, feature = "testexport"))]
            GcTask::Validate(_) => GcCommandKind::validate_config,
        }
    }
}

impl Display for GcTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GcTask::Gc {
                spacelike_key,
                lightlike_key,
                safe_point,
                ..
            } => f
                .debug_struct("GC")
                .field("spacelike_key", &hex::encode_upper(&spacelike_key))
                .field("lightlike_key", &hex::encode_upper(&lightlike_key))
                .field("safe_point", safe_point)
                .finish(),
            GcTask::UnsafeDestroyCone {
                spacelike_key, lightlike_key, ..
            } => f
                .debug_struct("UnsafeDestroyCone")
                .field("spacelike_key", &format!("{}", spacelike_key))
                .field("lightlike_key", &format!("{}", lightlike_key))
                .finish(),
            GcTask::PhysicalScanLock { max_ts, .. } => f
                .debug_struct("PhysicalScanLock")
                .field("max_ts", max_ts)
                .finish(),
            #[causet(any(test, feature = "testexport"))]
            GcTask::Validate(_) => write!(f, "Validate gc worker config"),
        }
    }
}

/// Used to perform GC operations on the engine.
struct GcRunner<E, RR>
where
    E: Engine,
    RR: VioletaBftStoreRouter<LmdbEngine>,
{
    engine: E,

    violetabft_store_router: RR,

    /// Used to limit the write flow of GC.
    limiter: Limiter,

    causet: GcConfig,
    causet_tracker: Tracker<GcConfig>,

    stats: Statistics,
}

impl<E, RR> GcRunner<E, RR>
where
    E: Engine,
    RR: VioletaBftStoreRouter<LmdbEngine>,
{
    pub fn new(
        engine: E,
        violetabft_store_router: RR,
        causet_tracker: Tracker<GcConfig>,
        causet: GcConfig,
    ) -> Self {
        let limiter = Limiter::new(if causet.max_write_bytes_per_sec.0 > 0 {
            causet.max_write_bytes_per_sec.0 as f64
        } else {
            INFINITY
        });
        Self {
            engine,
            violetabft_store_router,
            limiter,
            causet,
            causet_tracker,
            stats: Statistics::default(),
        }
    }

    /// Check need gc without getting snapshot.
    /// If this is not supported or any error happens, returns true to do further check after
    /// getting snapshot.
    fn need_gc(&self, spacelike_key: &[u8], lightlike_key: &[u8], safe_point: TimeStamp) -> bool {
        let collection = match self
            .engine
            .get_properties_causet(CAUSET_WRITE, &spacelike_key, &lightlike_key)
        {
            Ok(c) => c,
            Err(_) => return true,
        };
        check_need_gc(safe_point, self.causet.ratio_memory_barrier, &collection)
    }

    /// Cleans up outdated data.
    fn gc_key(
        &mut self,
        safe_point: TimeStamp,
        key: &Key,
        gc_info: &mut GcInfo,
        txn: &mut MvccTxn<E::Snap>,
    ) -> Result<()> {
        let next_gc_info = txn.gc(key.clone(), safe_point)?;
        gc_info.found_versions += next_gc_info.found_versions;
        gc_info.deleted_versions += next_gc_info.deleted_versions;
        gc_info.is_completed = next_gc_info.is_completed;
        self.stats.add(&txn.take_statistics());
        Ok(())
    }

    fn new_txn(snap: E::Snap) -> MvccTxn<E::Snap> {
        // TODO txn only used for GC, but this is hacky, maybe need an Option?
        let concurrency_manager = ConcurrencyManager::new(1.into());
        MvccTxn::for_scan(
            snap,
            Some(ScanMode::Forward),
            TimeStamp::zero(),
            false,
            concurrency_manager,
        )
    }

    fn flush_txn(txn: MvccTxn<E::Snap>, limiter: &Limiter, engine: &E) -> Result<()> {
        let write_size = txn.write_size();
        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            limiter.blocking_consume(write_size);
            engine.modify_on_kv_engine(modifies)?;
        }
        Ok(())
    }

    fn gc(&mut self, spacelike_key: &[u8], lightlike_key: &[u8], safe_point: TimeStamp) -> Result<()> {
        if !self.need_gc(spacelike_key, lightlike_key, safe_point) {
            GC_SKIPPED_COUNTER.inc();
            return Ok(());
        }

        let mut reader = MvccReader::new(
            self.engine.snapshot_on_kv_engine(spacelike_key, lightlike_key)?,
            Some(ScanMode::Forward),
            false,
            IsolationLevel::Si,
        );

        let mut next_key = Some(Key::from_encoded_slice(spacelike_key));
        while next_key.is_some() {
            // Scans at most `GcConfig.batch_tuplespaceInstanton` tuplespaceInstanton.
            let (tuplespaceInstanton, ufidelated_next_key) = reader.scan_tuplespaceInstanton(next_key, self.causet.batch_tuplespaceInstanton)?;
            next_key = ufidelated_next_key;

            if tuplespaceInstanton.is_empty() {
                GC_EMPTY_RANGE_COUNTER.inc();
                break;
            }

            let mut tuplespaceInstanton = tuplespaceInstanton.into_iter();
            let mut txn = Self::new_txn(self.engine.snapshot_on_kv_engine(spacelike_key, lightlike_key)?);
            let (mut next_gc_key, mut gc_info) = (tuplespaceInstanton.next(), GcInfo::default());
            while let Some(ref key) = next_gc_key {
                if let Err(e) = self.gc_key(safe_point, key, &mut gc_info, &mut txn) {
                    error!(?e; "GC meets failure"; "key" => %key,);
                    // Switch to the next key if meets failure.
                    gc_info.is_completed = true;
                }
                if gc_info.is_completed {
                    if gc_info.found_versions >= GC_LOG_FOUND_VERSION_THRESHOLD {
                        debug!(
                            "GC found plenty versions for a key";
                            "key" => %key,
                            "versions" => gc_info.found_versions,
                        );
                    }
                    if gc_info.deleted_versions as usize >= GC_LOG_DELETED_VERSION_THRESHOLD {
                        debug!(
                            "GC deleted plenty versions for a key";
                            "key" => %key,
                            "versions" => gc_info.deleted_versions,
                        );
                    }
                    next_gc_key = tuplespaceInstanton.next();
                    gc_info = GcInfo::default();
                } else {
                    Self::flush_txn(txn, &self.limiter, &self.engine)?;
                    txn = Self::new_txn(self.engine.snapshot_on_kv_engine(spacelike_key, lightlike_key)?);
                }
            }
            Self::flush_txn(txn, &self.limiter, &self.engine)?;
        }

        self.stats.add(reader.get_statistics());
        debug!(
            "gc has finished";
            "spacelike_key" => hex::encode_upper(spacelike_key),
            "lightlike_key" => hex::encode_upper(lightlike_key),
            "safe_point" => safe_point
        );
        Ok(())
    }

    fn unsafe_destroy_cone(&self, _: &Context, spacelike_key: &Key, lightlike_key: &Key) -> Result<()> {
        info!(
            "unsafe destroy cone spacelikeed";
            "spacelike_key" => %spacelike_key, "lightlike_key" => %lightlike_key
        );

        let local_causetStorage = self.engine.kv_engine();

        // Convert tuplespaceInstanton to Lmdb layer form
        // TODO: Logic coupled with violetabftstore's implementation. Maybe better design is to do it in
        // somewhere of the same layer with apply_worker.
        let spacelike_data_key = tuplespaceInstanton::data_key(spacelike_key.as_encoded());
        let lightlike_data_key = tuplespaceInstanton::data_lightlike_key(lightlike_key.as_encoded());

        let causets = &[CAUSET_DAGGER, CAUSET_DEFAULT, CAUSET_WRITE];

        // First, call delete_files_in_cone to free as much disk space as possible
        let delete_files_spacelike_time = Instant::now();
        for causet in causets {
            local_causetStorage
                .delete_files_in_cone_causet(causet, &spacelike_data_key, &lightlike_data_key, false)
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!("unsafe destroy cone failed at delete_files_in_cone_causet"; "err" => ?e);
                    e
                })?;
        }

        info!(
            "unsafe destroy cone finished deleting files in cone";
            "spacelike_key" => %spacelike_key, "lightlike_key" => %lightlike_key,
            "cost_time" => ?delete_files_spacelike_time.elapsed(),
        );

        // Then, delete all remaining tuplespaceInstanton in the cone.
        let cleanup_all_spacelike_time = Instant::now();
        for causet in causets {
            // TODO: set use_delete_cone with config here.
            local_causetStorage
                .delete_all_in_cone_causet(causet, &spacelike_data_key, &lightlike_data_key, false)
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!("unsafe destroy cone failed at delete_all_in_cone_causet"; "err" => ?e);
                    e
                })?;
        }

        let cleanup_all_time_cost = cleanup_all_spacelike_time.elapsed();

        self.violetabft_store_router
            .slightlike_store_msg(StoreMsg::ClearBraneSizeInCone {
                spacelike_key: spacelike_key.as_encoded().to_vec(),
                lightlike_key: lightlike_key.as_encoded().to_vec(),
            })
            .unwrap_or_else(|e| {
                // Warn and ignore it.
                warn!("unsafe destroy cone: failed slightlikeing ClearBraneSizeInCone"; "err" => ?e);
            });

        info!(
            "unsafe destroy cone finished cleaning up all";
            "spacelike_key" => %spacelike_key, "lightlike_key" => %lightlike_key, "cost_time" => ?cleanup_all_time_cost,
        );
        Ok(())
    }

    fn handle_physical_scan_lock(
        &self,
        _: &Context,
        max_ts: TimeStamp,
        spacelike_key: &Key,
        limit: usize,
    ) -> Result<Vec<LockInfo>> {
        let snap = self
            .engine
            .snapshot_on_kv_engine(spacelike_key.as_encoded(), &[])
            .unwrap();
        let mut reader = MvccReader::new(snap, Some(ScanMode::Forward), false, IsolationLevel::Si);
        let (locks, _) = reader.scan_locks(Some(spacelike_key), |l| l.ts <= max_ts, limit)?;

        let mut lock_infos = Vec::with_capacity(locks.len());
        for (key, dagger) in locks {
            let raw_key = key.into_raw().map_err(MvccError::from)?;
            lock_infos.push(dagger.into_lock_info(raw_key));
        }
        Ok(lock_infos)
    }

    fn ufidelate_statistics_metrics(&mut self) {
        let stats = mem::take(&mut self.stats);

        for (causet, details) in stats.details_enum().iter() {
            for (tag, count) in details.iter() {
                GC_KEYS_COUNTER_STATIC
                    .get(*causet)
                    .get(*tag)
                    .inc_by(*count as i64);
            }
        }
    }

    fn refresh_causet(&mut self) {
        if let Some(incoming) = self.causet_tracker.any_new() {
            let limit = incoming.max_write_bytes_per_sec.0;
            self.limiter
                .set_speed_limit(if limit > 0 { limit as f64 } else { INFINITY });
            self.causet = incoming.clone();
        }
    }
}

impl<E, RR> FutureRunnable<GcTask> for GcRunner<E, RR>
where
    E: Engine,
    RR: VioletaBftStoreRouter<LmdbEngine>,
{
    #[inline]
    fn run(&mut self, task: GcTask) {
        let enum_label = task.get_enum_label();

        GC_GCTASK_COUNTER_STATIC.get(enum_label).inc();

        let timer = SlowTimer::from_secs(GC_TASK_SLOW_SECONDS);
        let ufidelate_metrics = |is_err| {
            GC_TASK_DURATION_HISTOGRAM_VEC
                .with_label_values(&[enum_label.get_str()])
                .observe(duration_to_sec(timer.elapsed()));

            if is_err {
                GC_GCTASK_FAIL_COUNTER_STATIC.get(enum_label).inc();
            }
        };

        // Refresh config before handle task
        self.refresh_causet();

        match task {
            GcTask::Gc {
                spacelike_key,
                lightlike_key,
                safe_point,
                callback,
                ..
            } => {
                let res = self.gc(&spacelike_key, &lightlike_key, safe_point);
                ufidelate_metrics(res.is_err());
                callback(res);
                self.ufidelate_statistics_metrics();
                slow_log!(
                    T timer,
                    "GC on cone [{}, {}), safe_point {}",
                    hex::encode_upper(&spacelike_key),
                    hex::encode_upper(&lightlike_key),
                    safe_point
                );
            }
            GcTask::UnsafeDestroyCone {
                ctx,
                spacelike_key,
                lightlike_key,
                callback,
            } => {
                let res = self.unsafe_destroy_cone(&ctx, &spacelike_key, &lightlike_key);
                ufidelate_metrics(res.is_err());
                callback(res);
                slow_log!(
                    T timer,
                    "UnsafeDestroyCone spacelike_key {:?}, lightlike_key {:?}",
                    spacelike_key,
                    lightlike_key
                );
            }
            GcTask::PhysicalScanLock {
                ctx,
                max_ts,
                spacelike_key,
                limit,
                callback,
            } => {
                let res = self.handle_physical_scan_lock(&ctx, max_ts, &spacelike_key, limit);
                ufidelate_metrics(res.is_err());
                callback(res);
                slow_log!(
                    T timer,
                    "PhysicalScanLock spacelike_key {:?}, max_ts {}, limit {}",
                    spacelike_key,
                    max_ts,
                    limit,
                );
            }
            #[causet(any(test, feature = "testexport"))]
            GcTask::Validate(f) => {
                f(&self.causet, &self.limiter);
            }
        };
    }
}

/// When we failed to schedule a `GcTask` to `GcRunner`, use this to handle the `ScheduleError`.
fn handle_gc_task_schedule_error(e: FutureWorkerStopped<GcTask>) -> Result<()> {
    error!("failed to schedule gc task"; "err" => %e);
    Err(box_err!("failed to schedule gc task: {:?}", e))
}

/// Schedules a `GcTask` to the `GcRunner`.
fn schedule_gc(
    interlock_semaphore: &FutureInterlock_Semaphore<GcTask>,
    brane_id: u64,
    spacelike_key: Vec<u8>,
    lightlike_key: Vec<u8>,
    safe_point: TimeStamp,
    callback: Callback<()>,
) -> Result<()> {
    interlock_semaphore
        .schedule(GcTask::Gc {
            brane_id,
            spacelike_key,
            lightlike_key,
            safe_point,
            callback,
        })
        .or_else(handle_gc_task_schedule_error)
}

/// Does GC synchronously.
pub fn sync_gc(
    interlock_semaphore: &FutureInterlock_Semaphore<GcTask>,
    brane_id: u64,
    spacelike_key: Vec<u8>,
    lightlike_key: Vec<u8>,
    safe_point: TimeStamp,
) -> Result<()> {
    wait_op!(|callback| schedule_gc(interlock_semaphore, brane_id, spacelike_key, lightlike_key, safe_point, callback))
        .unwrap_or_else(|| {
            error!("failed to receive result of gc");
            Err(box_err!("gc_worker: failed to receive result of gc"))
        })
}

/// Used to schedule GC operations.
pub struct GcWorker<E, RR>
where
    E: Engine,
    RR: VioletaBftStoreRouter<LmdbEngine> + 'static,
{
    engine: E,

    /// `violetabft_store_router` is useful to signal violetabftstore clean brane size informations.
    violetabft_store_router: RR,

    config_manager: GcWorkerConfigManager,

    /// How many requests are scheduled from outside and unfinished.
    scheduled_tasks: Arc<AtomicUsize>,

    /// How many strong references. The worker will be stopped
    /// once there are no more references.
    refs: Arc<AtomicUsize>,
    worker: Arc<Mutex<FutureWorker<GcTask>>>,
    worker_interlock_semaphore: FutureInterlock_Semaphore<GcTask>,

    applied_lock_collector: Option<Arc<AppliedLockCollector>>,

    gc_manager_handle: Arc<Mutex<Option<GcManagerHandle>>>,
    cluster_version: ClusterVersion,
}

impl<E, RR> Clone for GcWorker<E, RR>
where
    E: Engine,
    RR: VioletaBftStoreRouter<LmdbEngine>,
{
    #[inline]
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, Ordering::SeqCst);

        Self {
            engine: self.engine.clone(),
            violetabft_store_router: self.violetabft_store_router.clone(),
            config_manager: self.config_manager.clone(),
            scheduled_tasks: self.scheduled_tasks.clone(),
            refs: self.refs.clone(),
            worker: self.worker.clone(),
            worker_interlock_semaphore: self.worker_interlock_semaphore.clone(),
            applied_lock_collector: self.applied_lock_collector.clone(),
            gc_manager_handle: self.gc_manager_handle.clone(),
            cluster_version: self.cluster_version.clone(),
        }
    }
}

impl<E, RR> Drop for GcWorker<E, RR>
where
    E: Engine,
    RR: VioletaBftStoreRouter<LmdbEngine> + 'static,
{
    #[inline]
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, Ordering::SeqCst);

        if refs != 1 {
            return;
        }

        let r = self.stop();
        if let Err(e) = r {
            error!(?e; "Failed to stop gc_worker");
        }
    }
}

impl<E, RR> GcWorker<E, RR>
where
    E: Engine,
    RR: VioletaBftStoreRouter<LmdbEngine>,
{
    pub fn new(
        engine: E,
        violetabft_store_router: RR,
        causet: GcConfig,
        cluster_version: ClusterVersion,
    ) -> GcWorker<E, RR> {
        let worker = Arc::new(Mutex::new(FutureWorker::new("gc-worker")));
        let worker_interlock_semaphore = worker.dagger().unwrap().interlock_semaphore();
        GcWorker {
            engine,
            violetabft_store_router,
            config_manager: GcWorkerConfigManager(Arc::new(VersionTrack::new(causet))),
            scheduled_tasks: Arc::new(AtomicUsize::new(0)),
            refs: Arc::new(AtomicUsize::new(1)),
            worker,
            worker_interlock_semaphore,
            applied_lock_collector: None,
            gc_manager_handle: Arc::new(Mutex::new(None)),
            cluster_version,
        }
    }

    pub fn spacelike_auto_gc<S: GcSafePointProvider, R: BraneInfoProvider>(
        &self,
        causet: AutoGcConfig<S, R>,
    ) -> Result<()> {
        let safe_point = Arc::new(AtomicU64::new(0));

        let kvdb = self.engine.kv_engine();
        let causet_mgr = self.config_manager.clone();
        let cluster_version = self.cluster_version.clone();
        kvdb.init_compaction_filter(safe_point.clone(), causet_mgr, cluster_version);

        let mut handle = self.gc_manager_handle.dagger().unwrap();
        assert!(handle.is_none());
        let new_handle = GcManager::new(
            causet,
            safe_point,
            self.worker_interlock_semaphore.clone(),
            self.config_manager.clone(),
            self.cluster_version.clone(),
        )
        .spacelike()?;
        *handle = Some(new_handle);
        Ok(())
    }

    pub fn spacelike(&mut self) -> Result<()> {
        let runner = GcRunner::new(
            self.engine.clone(),
            self.violetabft_store_router.clone(),
            self.config_manager.0.clone().tracker("gc-woker".to_owned()),
            self.config_manager.value().clone(),
        );
        self.worker
            .dagger()
            .unwrap()
            .spacelike(runner)
            .map_err(|e| box_err!("failed to spacelike gc_worker, err: {:?}", e))
    }

    pub fn spacelike_observe_lock_apply(
        &mut self,
        interlock_host: &mut InterlockHost<LmdbEngine>,
    ) -> Result<()> {
        assert!(self.applied_lock_collector.is_none());
        let collector = Arc::new(AppliedLockCollector::new(interlock_host)?);
        self.applied_lock_collector = Some(collector);
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        // Stop GcManager.
        if let Some(h) = self.gc_manager_handle.dagger().unwrap().take() {
            h.stop()?;
        }
        // Stop self.
        if let Some(h) = self.worker.dagger().unwrap().stop() {
            if let Err(e) = h.join() {
                return Err(box_err!("failed to join gc_worker handle, err: {:?}", e));
            }
        }
        Ok(())
    }

    pub fn interlock_semaphore(&self) -> FutureInterlock_Semaphore<GcTask> {
        self.worker_interlock_semaphore.clone()
    }

    /// Check whether GCWorker is busy. If busy, callback will be invoked with an error that
    /// indicates GCWorker is busy; otherwise, return a new callback that invokes the original
    /// callback as well as decrease the scheduled task counter.
    fn check_is_busy<T: 'static>(&self, callback: Callback<T>) -> Option<Callback<T>> {
        if self.scheduled_tasks.fetch_add(1, Ordering::SeqCst) >= GC_MAX_EXECUTING_TASKS {
            self.scheduled_tasks.fetch_sub(1, Ordering::SeqCst);
            callback(Err(Error::from(ErrorInner::GcWorkerTooBusy)));
            return None;
        }
        let scheduled_tasks = Arc::clone(&self.scheduled_tasks);
        Some(Box::new(move |r| {
            scheduled_tasks.fetch_sub(1, Ordering::SeqCst);
            callback(r);
        }))
    }

    /// Only for tests.
    pub fn gc(&self, safe_point: TimeStamp, callback: Callback<()>) -> Result<()> {
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            let spacelike_key = vec![];
            let lightlike_key = vec![];
            self.worker_interlock_semaphore
                .schedule(GcTask::Gc {
                    brane_id: 0,
                    spacelike_key,
                    lightlike_key,
                    safe_point,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    /// Cleans up all tuplespaceInstanton in a cone and quickly free the disk space. The cone might span over
    /// multiple branes, and the `ctx` doesn't indicate brane. The request will be done directly
    /// on Lmdb, bypassing the VioletaBft layer. User must promise that, after calling `destroy_cone`,
    /// the cone will never be accessed any more. However, `destroy_cone` is allowed to be called
    /// multiple times on an single cone.
    pub fn unsafe_destroy_cone(
        &self,
        ctx: Context,
        spacelike_key: Key,
        lightlike_key: Key,
        callback: Callback<()>,
    ) -> Result<()> {
        GC_COMMAND_COUNTER_VEC_STATIC.unsafe_destroy_cone.inc();
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            self.worker_interlock_semaphore
                .schedule(GcTask::UnsafeDestroyCone {
                    ctx,
                    spacelike_key,
                    lightlike_key,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    pub fn get_config_manager(&self) -> GcWorkerConfigManager {
        self.config_manager.clone()
    }

    pub fn physical_scan_lock(
        &self,
        ctx: Context,
        max_ts: TimeStamp,
        spacelike_key: Key,
        limit: usize,
        callback: Callback<Vec<LockInfo>>,
    ) -> Result<()> {
        GC_COMMAND_COUNTER_VEC_STATIC.physical_scan_lock.inc();
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            self.worker_interlock_semaphore
                .schedule(GcTask::PhysicalScanLock {
                    ctx,
                    max_ts,
                    spacelike_key,
                    limit,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    pub fn spacelike_collecting(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<()>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.spacelike_collecting(max_ts, callback))
    }

    pub fn get_collected_locks(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<(Vec<LockInfo>, bool)>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.get_collected_locks(max_ts, callback))
    }

    pub fn stop_collecting(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<()>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.stop_collecting(max_ts, callback))
    }
}

#[causet(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::mpsc::channel;

    use engine_lmdb::LmdbSnapshot;
    use engine_promises::KvEngine;
    use futures::executor::block_on;
    use ekvproto::{kvrpcpb::Op, metapb};
    use violetabftstore::router::VioletaBftStoreBlackHole;
    use violetabftstore::store::BraneSnapshot;
    use einsteindb_util::codec::number::NumberEncoder;
    use einsteindb_util::future::paired_future_callback;
    use einsteindb_util::time::ThreadReadId;
    use txn_types::Mutation;

    use crate::causetStorage::kv::{
        self, write_modifies, Callback as EngineCallback, Modify, Result as EngineResult,
        TestEngineBuilder, WriteData,
    };
    use crate::causetStorage::lock_manager::DummyLockManager;
    use crate::causetStorage::{txn::commands, Engine, CausetStorage, TestStorageBuilder};

    use super::*;

    /// A wrapper of engine that adds the 'z' prefix to tuplespaceInstanton internally.
    /// For test engines, they writes tuplespaceInstanton into db directly, but in production a 'z' prefix will be
    /// added to tuplespaceInstanton by violetabftstore layer before writing to db. Some functionalities of `GCWorker`
    /// bypasses VioletaBft layer, so they needs to know how data is actually represented in db. This
    /// wrapper allows test engines write 'z'-prefixed tuplespaceInstanton to db.
    #[derive(Clone)]
    struct PrefixedEngine(kv::LmdbEngine);

    impl Engine for PrefixedEngine {
        // Use BraneSnapshot which can remove the z prefix internally.
        type Snap = BraneSnapshot<LmdbSnapshot>;
        type Local = LmdbEngine;

        fn kv_engine(&self) -> LmdbEngine {
            self.0.kv_engine()
        }

        fn snapshot_on_kv_engine(
            &self,
            spacelike_key: &[u8],
            lightlike_key: &[u8],
        ) -> kv::Result<Self::Snap> {
            let mut brane = metapb::Brane::default();
            brane.set_spacelike_key(spacelike_key.to_owned());
            brane.set_lightlike_key(lightlike_key.to_owned());
            // Use a fake peer to avoid panic.
            brane.mut_peers().push(Default::default());
            Ok(BraneSnapshot::from_snapshot(
                Arc::new(self.kv_engine().snapshot()),
                Arc::new(brane),
            ))
        }

        fn modify_on_kv_engine(&self, mut modifies: Vec<Modify>) -> kv::Result<()> {
            for modify in &mut modifies {
                match modify {
                    Modify::Delete(_, ref mut key) => {
                        let bytes = tuplespaceInstanton::data_key(key.as_encoded());
                        *key = Key::from_encoded(bytes);
                    }
                    Modify::Put(_, ref mut key, _) => {
                        let bytes = tuplespaceInstanton::data_key(key.as_encoded());
                        *key = Key::from_encoded(bytes);
                    }
                    Modify::DeleteCone(_, ref mut key1, ref mut key2, _) => {
                        let bytes = tuplespaceInstanton::data_key(key1.as_encoded());
                        *key1 = Key::from_encoded(bytes);
                        let bytes = tuplespaceInstanton::data_lightlike_key(key2.as_encoded());
                        *key2 = Key::from_encoded(bytes);
                    }
                }
            }
            write_modifies(&self.kv_engine(), modifies)
        }

        fn async_write(
            &self,
            ctx: &Context,
            mut batch: WriteData,
            callback: EngineCallback<()>,
        ) -> EngineResult<()> {
            batch.modifies.iter_mut().for_each(|modify| match modify {
                Modify::Delete(_, ref mut key) => {
                    *key = Key::from_encoded(tuplespaceInstanton::data_key(key.as_encoded()));
                }
                Modify::Put(_, ref mut key, _) => {
                    *key = Key::from_encoded(tuplespaceInstanton::data_key(key.as_encoded()));
                }
                Modify::DeleteCone(_, ref mut spacelike_key, ref mut lightlike_key, _) => {
                    *spacelike_key = Key::from_encoded(tuplespaceInstanton::data_key(spacelike_key.as_encoded()));
                    *lightlike_key = Key::from_encoded(tuplespaceInstanton::data_lightlike_key(lightlike_key.as_encoded()));
                }
            });
            self.0.async_write(ctx, batch, callback)
        }

        fn async_snapshot(
            &self,
            ctx: &Context,
            _read_id: Option<ThreadReadId>,
            callback: EngineCallback<Self::Snap>,
        ) -> EngineResult<()> {
            self.0.async_snapshot(
                ctx,
                None,
                Box::new(move |(cb_ctx, r)| {
                    callback((
                        cb_ctx,
                        r.map(|snap| {
                            let mut brane = metapb::Brane::default();
                            // Add a peer to pass initialized check.
                            brane.mut_peers().push(metapb::Peer::default());
                            BraneSnapshot::from_snapshot(snap, Arc::new(brane))
                        }),
                    ))
                }),
            )
        }
    }

    /// Assert the data in `causetStorage` is the same as `expected_data`. TuplespaceInstanton in `expected_data` should
    /// be encoded form without ts.
    fn check_data<E: Engine>(
        causetStorage: &CausetStorage<E, DummyLockManager>,
        expected_data: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) {
        let scan_res = block_on(causetStorage.scan(
            Context::default(),
            Key::from_encoded_slice(b""),
            None,
            expected_data.len() + 1,
            0,
            1.into(),
            false,
            false,
        ))
        .unwrap();

        let all_equal = scan_res
            .into_iter()
            .map(|res| res.unwrap())
            .zip(expected_data.iter())
            .all(|((k1, v1), (k2, v2))| &k1 == k2 && &v1 == v2);
        assert!(all_equal);
    }

    fn test_destroy_cone_impl(
        init_tuplespaceInstanton: &[Vec<u8>],
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
    ) -> Result<()> {
        // Return Result from this function so we can use the `wait_op` macro here.

        let engine = TestEngineBuilder::new().build().unwrap();
        let causetStorage =
            TestStorageBuilder::from_engine_and_lock_mgr(engine.clone(), DummyLockManager {})
                .build()
                .unwrap();
        let mut gc_worker = GcWorker::new(
            engine,
            VioletaBftStoreBlackHole,
            GcConfig::default(),
            ClusterVersion::new(semver::Version::new(5, 0, 0)),
        );
        gc_worker.spacelike().unwrap();
        // Convert tuplespaceInstanton to key value pairs, where the value is "value-{key}".
        let data: BTreeMap<_, _> = init_tuplespaceInstanton
            .iter()
            .map(|key| {
                let mut value = b"value-".to_vec();
                value.extlightlike_from_slice(key);
                (Key::from_raw(key).into_encoded(), value)
            })
            .collect();

        // Generate `Mutation`s from these tuplespaceInstanton.
        let mutations: Vec<_> = init_tuplespaceInstanton
            .iter()
            .map(|key| {
                let mut value = b"value-".to_vec();
                value.extlightlike_from_slice(key);
                Mutation::Put((Key::from_raw(key), value))
            })
            .collect();
        let primary = init_tuplespaceInstanton[0].clone();

        let spacelike_ts = spacelike_ts.into();

        // Write these data to the causetStorage.
        wait_op!(|cb| causetStorage.sched_txn_command(
            commands::Prewrite::with_defaults(mutations, primary, spacelike_ts),
            cb,
        ))
        .unwrap()
        .unwrap();

        // Commit.
        let tuplespaceInstanton: Vec<_> = init_tuplespaceInstanton.iter().map(|k| Key::from_raw(k)).collect();
        wait_op!(|cb| causetStorage.sched_txn_command(
            commands::Commit::new(tuplespaceInstanton, spacelike_ts, commit_ts.into(), Context::default()),
            cb
        ))
        .unwrap()
        .unwrap();

        // Assert these data is successfully written to the causetStorage.
        check_data(&causetStorage, &data);

        let spacelike_key = Key::from_raw(spacelike_key);
        let lightlike_key = Key::from_raw(lightlike_key);

        // Calculate expected data set after deleting the cone.
        let data: BTreeMap<_, _> = data
            .into_iter()
            .filter(|(k, _)| k < spacelike_key.as_encoded() || k >= lightlike_key.as_encoded())
            .collect();

        // Invoke unsafe destroy cone.
        wait_op!(|cb| gc_worker.unsafe_destroy_cone(Context::default(), spacelike_key, lightlike_key, cb))
            .unwrap()
            .unwrap();

        // Check remaining data is as expected.
        check_data(&causetStorage, &data);

        Ok(())
    }

    #[test]
    fn test_destroy_cone() {
        test_destroy_cone_impl(
            &[
                b"key1".to_vec(),
                b"key2".to_vec(),
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
            ],
            5,
            10,
            b"key2",
            b"key4",
        )
        .unwrap();

        test_destroy_cone_impl(
            &[b"key1".to_vec(), b"key9".to_vec()],
            5,
            10,
            b"key3",
            b"key7",
        )
        .unwrap();

        test_destroy_cone_impl(
            &[
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
                b"key6".to_vec(),
                b"key7".to_vec(),
            ],
            5,
            10,
            b"key1",
            b"key9",
        )
        .unwrap();

        test_destroy_cone_impl(
            &[
                b"key1".to_vec(),
                b"key2".to_vec(),
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
            ],
            5,
            10,
            b"key2\x00",
            b"key4",
        )
        .unwrap();

        test_destroy_cone_impl(
            &[
                b"key1".to_vec(),
                b"key1\x00".to_vec(),
                b"key1\x00\x00".to_vec(),
                b"key1\x00\x00\x00".to_vec(),
            ],
            5,
            10,
            b"key1\x00",
            b"key1\x00\x00",
        )
        .unwrap();

        test_destroy_cone_impl(
            &[
                b"key1".to_vec(),
                b"key1\x00".to_vec(),
                b"key1\x00\x00".to_vec(),
                b"key1\x00\x00\x00".to_vec(),
            ],
            5,
            10,
            b"key1\x00",
            b"key1\x00",
        )
        .unwrap();
    }

    #[test]
    fn test_physical_scan_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let prefixed_engine = PrefixedEngine(engine);
        let causetStorage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
            prefixed_engine.clone(),
            DummyLockManager {},
        )
        .build()
        .unwrap();
        let mut gc_worker = GcWorker::new(
            prefixed_engine,
            VioletaBftStoreBlackHole,
            GcConfig::default(),
            ClusterVersion::default(),
        );
        gc_worker.spacelike().unwrap();

        let physical_scan_lock = |max_ts: u64, spacelike_key, limit| {
            let (cb, f) = paired_future_callback();
            gc_worker
                .physical_scan_lock(Context::default(), max_ts.into(), spacelike_key, limit, cb)
                .unwrap();
            block_on(f).unwrap()
        };

        let mut expected_lock_info = Vec::new();

        // Put locks into the causetStorage.
        for i in 0..50 {
            let mut k = vec![];
            k.encode_u64(i).unwrap();
            let v = k.clone();

            let mutation = Mutation::Put((Key::from_raw(&k), v));

            let lock_ts = 10 + i % 3;

            // Collect all locks with ts <= 11 to check the result of physical_scan_lock.
            if lock_ts <= 11 {
                let mut info = LockInfo::default();
                info.set_primary_lock(k.clone());
                info.set_lock_version(lock_ts);
                info.set_key(k.clone());
                info.set_lock_type(Op::Put);
                expected_lock_info.push(info)
            }

            let (tx, rx) = channel();
            causetStorage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(vec![mutation], k, lock_ts.into()),
                    Box::new(move |res| tx.slightlike(res).unwrap()),
                )
                .unwrap();
            rx.recv()
                .unwrap()
                .unwrap()
                .locks
                .into_iter()
                .for_each(|r| r.unwrap());
        }

        let res = physical_scan_lock(11, Key::from_raw(b""), 50).unwrap();
        assert_eq!(res, expected_lock_info);

        let res = physical_scan_lock(11, Key::from_raw(b""), 5).unwrap();
        assert_eq!(res[..], expected_lock_info[..5]);

        let mut spacelike_key = vec![];
        spacelike_key.encode_u64(4).unwrap();
        let res = physical_scan_lock(11, Key::from_raw(&spacelike_key), 6).unwrap();
        // expected_locks[3] is the key 4.
        assert_eq!(res[..], expected_lock_info[3..9]);
    }
}
