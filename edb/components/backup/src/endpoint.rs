// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::f64::INFINITY;
use std::fmt;
use std::sync::atomic::*;
use std::sync::*;
use std::{borrow::Cow, time::*};

use interlocking_directorate::ConcurrencyManager;
use configuration::Configuration;
use engine_lmdb::raw::DB;
use edb::{name_to_causet, CfName, IterOptions, SstCompressionType, DATA_KEY_PREFIX_LEN};
use external_causet_storage::*;
use futures::channel::mpsc::*;
use ekvproto::backup::*;
use ekvproto::kvrpc_timeshare::{Context, IsolationLevel};
use ekvproto::meta_timeshare::*;
use violetabft::StateRole;
use violetabftstore::interlock::BraneInfoProvider;
use violetabftstore::store::util::find_peer;
use einsteindb::config::BackupConfig;
use einsteindb::causet_storage::kv::{Engine, ScanMode, Snapshot};
use einsteindb::causet_storage::tail_pointer::Error as MvccError;
use einsteindb::causet_storage::txn::{
    EntryBatch, Error as TxnError, SnapshotStore, TxnEntryScanner, TxnEntryStore,
};
use einsteindb::causet_storage::Statistics;
use einsteindb_util::threadpool::{DefaultContext, ThreadPool, ThreadPoolBuilder};
use einsteindb_util::time::Limiter;
use einsteindb_util::timer::Timer;
use einsteindb_util::worker::{Runnable, RunnableWithTimer};
use txn_types::{Key, Dagger, TimeStamp};

use crate::metrics::*;
use crate::*;

const WORKER_TAKE_RANGE: usize = 6;
const BACKUP_BATCH_LIMIT: usize = 1024;

// if thread pool has been idle for such long time, we will shutdown it.
const IDLE_THREADPOOL_DURATION: u64 = 30 * 60 * 1000; // 30 mins

#[derive(Clone)]
struct Request {
    spacelike_key: Vec<u8>,
    lightlike_key: Vec<u8>,
    spacelike_ts: TimeStamp,
    lightlike_ts: TimeStamp,
    limiter: Limiter,
    backlightlike: StorageBacklightlike,
    cancel: Arc<AtomicBool>,
    is_raw_kv: bool,
    causet: CfName,
    compression_type: CompressionType,
    compression_level: i32,
}

/// Backup Task.
pub struct Task {
    request: Request,
    pub(crate) resp: UnboundedLightlike<BackupResponse>,
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackupTask")
            .field("spacelike_ts", &self.request.spacelike_ts)
            .field("lightlike_ts", &self.request.lightlike_ts)
            .field("spacelike_key", &hex::encode_upper(&self.request.spacelike_key))
            .field("lightlike_key", &hex::encode_upper(&self.request.lightlike_key))
            .field("is_raw_kv", &self.request.is_raw_kv)
            .field("causet", &self.request.causet)
            .finish()
    }
}

#[derive(Clone)]
struct LimitedStorage {
    limiter: Limiter,
    causet_storage: Arc<dyn ExternalStorage>,
}

impl Task {
    /// Create a backup task based on the given backup request.
    pub fn new(
        req: BackupRequest,
        resp: UnboundedLightlikeValue<BackupResponse>,
    ) -> Result<(Task, Arc<AtomicBool>)> {
        let cancel = Arc::new(AtomicBool::new(false));

        let speed_limit = req.get_rate_limit();
        let limiter = Limiter::new(if speed_limit > 0 {
            speed_limit as f64
        } else {
            INFINITY
        });
        let causet = name_to_causet(req.get_causet()).ok_or_else(|| crate::Error::InvalidCf {
            causet: req.get_causet().to_owned(),
        })?;

        // Check causet_storage backlightlike eagerly.
        create_causet_storage(req.get_causet_storage_backlightlike())?;

        let task = Task {
            request: Request {
                spacelike_key: req.get_spacelike_key().to_owned(),
                lightlike_key: req.get_lightlike_key().to_owned(),
                spacelike_ts: req.get_spacelike_version().into(),
                lightlike_ts: req.get_lightlike_version().into(),
                backlightlike: req.get_causet_storage_backlightlike().clone(),
                limiter,
                cancel: cancel.clone(),
                is_raw_kv: req.get_is_raw_kv(),
                causet,
                compression_type: req.get_compression_type(),
                compression_level: req.get_compression_level(),
            },
            resp,
        };
        Ok((task, cancel))
    }

    /// Check whether the task is canceled.
    pub fn has_canceled(&self) -> bool {
        self.request.cancel.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct BackupCone {
    spacelike_key: Option<Key>,
    lightlike_key: Option<Key>,
    brane: Brane,
    leader: Peer,
    is_raw_kv: bool,
    causet: CfName,
}

impl BackupCone {
    /// Get entries from the scanner and save them to causet_storage
    fn backup<E: Engine>(
        &self,
        writer: &mut BackupWriter,
        engine: &E,
        interlocking_directorate: ConcurrencyManager,
        backup_ts: TimeStamp,
        begin_ts: TimeStamp,
    ) -> Result<Statistics> {
        assert!(!self.is_raw_kv);

        let mut ctx = Context::default();
        ctx.set_brane_id(self.brane.get_id());
        ctx.set_brane_epoch(self.brane.get_brane_epoch().to_owned());
        ctx.set_peer(self.leader.clone());

        // fidelio max_ts and check the in-memory dagger Block before getting the snapshot
        interlocking_directorate.fidelio_max_ts(backup_ts);
        interlocking_directorate
            .read_cone_check(
                self.spacelike_key.as_ref(),
                self.lightlike_key.as_ref(),
                |key, dagger| {
                    Dagger::check_ts_conflict(
                        Cow::Borrowed(dagger),
                        &key,
                        backup_ts,
                        &Default::default(),
                    )
                },
            )
            .map_err(MvccError::from)
            .map_err(TxnError::from)?;

        let snapshot = match engine.snapshot(&ctx) {
            Ok(s) => s,
            Err(e) => {
                error!(?e; "backup snapshot failed");
                return Err(e.into());
            }
        };
        let snap_store = SnapshotStore::new(
            snapshot,
            backup_ts,
            IsolationLevel::Si,
            false, /* fill_cache */
            Default::default(),
            false,
        );
        let spacelike_key = self.spacelike_key.clone();
        let lightlike_key = self.lightlike_key.clone();
        // Incremental backup needs to output delete records.
        let incremental = !begin_ts.is_zero();
        let mut scanner = snap_store
            .entry_scanner(spacelike_key, lightlike_key, begin_ts, incremental)
            .unwrap();

        let spacelike = Instant::now();
        let mut batch = EntryBatch::with_capacity(BACKUP_BATCH_LIMIT);
        loop {
            if let Err(e) = scanner.scan_entries(&mut batch) {
                error!(?e; "backup scan entries failed");
                return Err(e.into());
            };
            if batch.is_empty() {
                break;
            }
            debug!("backup scan entries"; "len" => batch.len());
            // Build sst files.
            if let Err(e) = writer.write(batch.drain(), true) {
                error!(?e; "backup build sst failed");
                return Err(e);
            }
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["scan"])
            .observe(spacelike.elapsed().as_secs_f64());
        let stat = scanner.take_statistics();
        Ok(stat)
    }

    fn backup_raw<E: Engine>(
        &self,
        writer: &mut BackupRawKVWriter,
        engine: &E,
    ) -> Result<Statistics> {
        assert!(self.is_raw_kv);

        let mut ctx = Context::default();
        ctx.set_brane_id(self.brane.get_id());
        ctx.set_brane_epoch(self.brane.get_brane_epoch().to_owned());
        ctx.set_peer(self.leader.clone());
        let snapshot = match engine.snapshot(&ctx) {
            Ok(s) => s,
            Err(e) => {
                error!(?e; "backup raw kv snapshot failed");
                return Err(e.into());
            }
        };
        let spacelike = Instant::now();
        let mut statistics = Statistics::default();
        let causetstatistics = statistics.mut_causet_statistics(self.causet);
        let mut option = IterOptions::default();
        if let Some(lightlike) = self.lightlike_key.clone() {
            option.set_upper_bound(lightlike.as_encoded(), DATA_KEY_PREFIX_LEN);
        }
        let mut cursor = snapshot.iter_causet(self.causet, option, ScanMode::Forward)?;
        if let Some(begin) = self.spacelike_key.clone() {
            if !cursor.seek(&begin, causetstatistics)? {
                return Ok(statistics);
            }
        } else {
            if !cursor.seek_to_first(causetstatistics) {
                return Ok(statistics);
            }
        }
        let mut batch = vec![];
        loop {
            while cursor.valid()? && batch.len() < BACKUP_BATCH_LIMIT {
                batch.push(Ok((
                    cursor.key(causetstatistics).to_owned(),
                    cursor.value(causetstatistics).to_owned(),
                )));
                cursor.next(causetstatistics);
            }
            if batch.is_empty() {
                break;
            }
            debug!("backup scan raw kv entries"; "len" => batch.len());
            // Build sst files.
            if let Err(e) = writer.write(batch.drain(..), false) {
                error!(?e; "backup raw kv build sst failed");
                return Err(e);
            }
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["raw_scan"])
            .observe(spacelike.elapsed().as_secs_f64());
        Ok(statistics)
    }

    fn backup_to_file<E: Engine>(
        &self,
        engine: &E,
        db: Arc<DB>,
        causet_storage: &LimitedStorage,
        interlocking_directorate: ConcurrencyManager,
        file_name: String,
        backup_ts: TimeStamp,
        spacelike_ts: TimeStamp,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
    ) -> Result<(Vec<File>, Statistics)> {
        let mut writer = match BackupWriter::new(
            db,
            &file_name,
            causet_storage.limiter.clone(),
            compression_type,
            compression_level,
        ) {
            Ok(w) => w,
            Err(e) => {
                error!(?e; "backup writer failed");
                return Err(e);
            }
        };
        let stat = match self.backup(
            &mut writer,
            engine,
            interlocking_directorate,
            backup_ts,
            spacelike_ts,
        ) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };
        // Save sst files to causet_storage.
        match writer.save(&causet_storage.causet_storage) {
            Ok(files) => Ok((files, stat)),
            Err(e) => {
                error!(?e; "backup save file failed");
                Err(e)
            }
        }
    }

    fn backup_raw_kv_to_file<E: Engine>(
        &self,
        engine: &E,
        db: Arc<DB>,
        causet_storage: &LimitedStorage,
        file_name: String,
        causet: CfName,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
    ) -> Result<(Vec<File>, Statistics)> {
        let mut writer = match BackupRawKVWriter::new(
            db,
            &file_name,
            causet,
            causet_storage.limiter.clone(),
            compression_type,
            compression_level,
        ) {
            Ok(w) => w,
            Err(e) => {
                error!(?e; "backup writer failed");
                return Err(e);
            }
        };
        let stat = match self.backup_raw(&mut writer, engine) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };
        // Save sst files to causet_storage.
        match writer.save(&causet_storage.causet_storage) {
            Ok(files) => Ok((files, stat)),
            Err(e) => {
                error!(?e; "backup save file failed");
                Err(e)
            }
        }
    }
}

#[derive(Clone)]
pub struct ConfigManager(Arc<RwLock<BackupConfig>>);

impl configuration::ConfigManager for ConfigManager {
    fn dispatch(&mut self, change: configuration::ConfigChange) -> configuration::Result<()> {
        self.0.write().unwrap().fidelio(change);
        Ok(())
    }
}

#[causet(test)]
impl ConfigManager {
    fn set_num_threads(&self, num_threads: usize) {
        self.0.write().unwrap().num_threads = num_threads;
    }
}

/// The lightlikepoint of backup.
///
/// It coordinates backup tasks and dispatches them to different workers.
pub struct node<E: Engine, R: BraneInfoProvider> {
    store_id: u64,
    pool: RefCell<ControlThreadPool>,
    pool_idle_memory_barrier: u64,
    db: Arc<DB>,
    config_manager: ConfigManager,
    interlocking_directorate: ConcurrencyManager,

    pub(crate) engine: E,
    pub(crate) brane_info: R,
}

/// The progress of a backup task
pub struct Progress<R: BraneInfoProvider> {
    store_id: u64,
    next_spacelike: Option<Key>,
    lightlike_key: Option<Key>,
    brane_info: R,
    finished: bool,
    is_raw_kv: bool,
    causet: CfName,
}

impl<R: BraneInfoProvider> Progress<R> {
    fn new(
        store_id: u64,
        next_spacelike: Option<Key>,
        lightlike_key: Option<Key>,
        brane_info: R,
        is_raw_kv: bool,
        causet: CfName,
    ) -> Self {
        Progress {
            store_id,
            next_spacelike,
            lightlike_key,
            brane_info,
            finished: Default::default(),
            is_raw_kv,
            causet,
        }
    }

    /// Forward the progress by `cones` BackupCones
    ///
    /// The size of the returned BackupCones should <= `cones`
    fn forward(&mut self, limit: usize) -> Vec<BackupCone> {
        if self.finished {
            return Vec::new();
        }
        let store_id = self.store_id;
        let (tx, rx) = mpsc::channel();
        let spacelike_key_ = self
            .next_spacelike
            .clone()
            .map_or_else(Vec::new, |k| k.into_encoded());

        let spacelike_key = self.next_spacelike.clone();
        let lightlike_key = self.lightlike_key.clone();
        let raw_kv = self.is_raw_kv;
        let causet_name = self.causet;
        let res = self.brane_info.seek_brane(
            &spacelike_key_,
            Box::new(move |iter| {
                let mut lightlikeed = 0;
                for info in iter {
                    let brane = &info.brane;
                    if lightlike_key.is_some() {
                        let lightlike_slice = lightlike_key.as_ref().unwrap().as_encoded().as_slice();
                        if lightlike_slice <= brane.get_spacelike_key() {
                            // We have reached the lightlike.
                            // The cone is defined as [spacelike, lightlike) so break if
                            // brane spacelike key is greater or equal to lightlike key.
                            break;
                        }
                    }
                    if info.role == StateRole::Leader {
                        let ekey = get_min_lightlike_key(lightlike_key.as_ref(), &brane);
                        let skey = get_max_spacelike_key(spacelike_key.as_ref(), &brane);
                        assert!(!(skey == ekey && ekey.is_some()), "{:?} {:?}", skey, ekey);
                        let leader = find_peer(brane, store_id).unwrap().to_owned();
                        let backup_cone = BackupCone {
                            spacelike_key: skey,
                            lightlike_key: ekey,
                            brane: brane.clone(),
                            leader,
                            is_raw_kv: raw_kv,
                            causet: causet_name,
                        };
                        tx.lightlike(backup_cone).unwrap();
                        lightlikeed += 1;
                        if lightlikeed >= limit {
                            break;
                        }
                    }
                }
            }),
        );
        if let Err(e) = res {
            // TODO: handle error.
            error!(?e; "backup seek brane failed");
        }

        let bcones: Vec<_> = rx.iter().collect();
        if let Some(b) = bcones.last() {
            // The brane's lightlike key is empty means it is the last
            // brane, we need to set the `finished` flag here in case
            // we run with `next_spacelike` set to None
            if b.brane.get_lightlike_key().is_empty() || b.lightlike_key == self.lightlike_key {
                self.finished = true;
            }
            self.next_spacelike = b.lightlike_key.clone();
        } else {
            self.finished = true;
        }
        bcones
    }
}

struct ControlThreadPool {
    size: usize,
    workers: Option<ThreadPool<DefaultContext>>,
    last_active: Instant,
}

impl ControlThreadPool {
    fn new() -> Self {
        ControlThreadPool {
            size: 0,
            workers: None,
            last_active: Instant::now(),
        }
    }

    fn spawn<F>(&mut self, func: F)
    where
        F: FnOnce() + lightlike + 'static,
    {
        self.workers.as_ref().unwrap().execute(|_| func());
    }

    /// Lazily adjust the thread pool's size
    ///
    /// Resizing if the thread pool need to explightlike or there
    /// are too many idle threads. Otherwise do nothing.
    fn adjust_with(&mut self, new_size: usize) {
        if self.size >= new_size && self.size - new_size <= 10 {
            return;
        }
        let workers = ThreadPoolBuilder::with_default_factory("backup-worker".to_owned())
            .thread_count(new_size)
            .build();
        let _ = self.workers.replace(workers);
        self.size = new_size;
        BACKUP_THREAD_POOL_SIZE_GAUGE.set(new_size as i64);
    }

    fn heartbeat(&mut self) {
        self.last_active = Instant::now();
    }

    /// Shutdown the thread pool if it has been idle for a long time.
    fn check_active(&mut self, idle_memory_barrier: Duration) {
        if self.last_active.elapsed() >= idle_memory_barrier {
            self.size = 0;
            if let Some(w) = self.workers.take() {
                let spacelike = Instant::now();
                drop(w);
                slow_log!(spacelike.elapsed(), "backup thread pool shutdown too long");
            }
        }
    }
}

#[test]
fn test_control_thread_pool_adjust_keep_tasks() {
    use std::thread::sleep;

    let counter = Arc::new(AtomicU32::new(0));
    let mut pool = ControlThreadPool::new();
    pool.adjust_with(3);

    for i in 0..8 {
        let ctr = counter.clone();
        pool.spawn(move || {
            sleep(Duration::from_millis(100));
            ctr.fetch_or(1 << i, Ordering::SeqCst);
        });
    }

    sleep(Duration::from_millis(150));
    pool.adjust_with(4);

    for i in 8..16 {
        let ctr = counter.clone();
        pool.spawn(move || {
            sleep(Duration::from_millis(100));
            ctr.fetch_or(1 << i, Ordering::SeqCst);
        });
    }

    sleep(Duration::from_millis(250));
    assert_eq!(counter.load(Ordering::SeqCst), 0xffff);
}

impl<E: Engine, R: BraneInfoProvider> node<E, R> {
    pub fn new(
        store_id: u64,
        engine: E,
        brane_info: R,
        db: Arc<DB>,
        config: BackupConfig,
        interlocking_directorate: ConcurrencyManager,
    ) -> node<E, R> {
        node {
            store_id,
            engine,
            brane_info,
            pool: RefCell::new(ControlThreadPool::new()),
            pool_idle_memory_barrier: IDLE_THREADPOOL_DURATION,
            db,
            config_manager: ConfigManager(Arc::new(RwLock::new(config))),
            interlocking_directorate,
        }
    }

    pub fn new_timer(&self) -> Timer<()> {
        let mut timer = Timer::new(1);
        timer.add_task(Duration::from_millis(self.pool_idle_memory_barrier), ());
        timer
    }

    pub fn get_config_manager(&self) -> ConfigManager {
        self.config_manager.clone()
    }

    fn spawn_backup_worker(
        &self,
        prs: Arc<Mutex<Progress<R>>>,
        request: Request,
        tx: UnboundedLightlikeValue<BackupResponse>,
    ) {
        let spacelike_ts = request.spacelike_ts;
        let lightlike_ts = request.lightlike_ts;
        let backup_ts = request.lightlike_ts;
        let engine = self.engine.clone();
        let db = self.db.clone();
        let store_id = self.store_id;
        let interlocking_directorate = self.interlocking_directorate.clone();
        // TODO: make it async.
        self.pool.borrow_mut().spawn(move || loop {
            let (bcones, is_raw_kv, causet) = {
                // Release dagger as soon as possible.
                // It is critical to speed up backup, otherwise workers are
                // blocked by each other.
                let mut progress = prs.dagger().unwrap();
                (
                    progress.forward(WORKER_TAKE_RANGE),
                    progress.is_raw_kv,
                    progress.causet,
                )
            };
            if bcones.is_empty() {
                return;
            }

            einsteindb_alloc::add_thread_memory_accessor();

            // causet_storage backlightlike has been checked in `Task::new()`.
            let backlightlike = create_causet_storage(&request.backlightlike).unwrap();
            let causet_storage = LimitedStorage {
                limiter: request.limiter.clone(),
                causet_storage: backlightlike,
            };
            for bcone in bcones {
                if request.cancel.load(Ordering::SeqCst) {
                    warn!("backup task has canceled"; "cone" => ?bcone);
                    return;
                }
                // TODO: make file_name unique and short
                let key = bcone.spacelike_key.clone().and_then(|k| {
                    // use spacelike_key sha256 instead of spacelike_key to avoid file name too long os error
                    let input = if is_raw_kv {
                        k.into_encoded()
                    } else {
                        k.into_raw().unwrap()
                    };
                    einsteindb_util::file::sha256(&input).ok().map(|b| hex::encode(b))
                });
                let name = backup_file_name(store_id, &bcone.brane, key);
                let ct = to_sst_compression_type(request.compression_type);

                let (res, spacelike_key, lightlike_key) = if is_raw_kv {
                    (
                        bcone.backup_raw_kv_to_file(
                            &engine,
                            db.clone(),
                            &causet_storage,
                            name,
                            causet,
                            ct,
                            request.compression_level,
                        ),
                        bcone
                            .spacelike_key
                            .map_or_else(|| vec![], |k| k.into_encoded()),
                        bcone.lightlike_key.map_or_else(|| vec![], |k| k.into_encoded()),
                    )
                } else {
                    (
                        bcone.backup_to_file(
                            &engine,
                            db.clone(),
                            &causet_storage,
                            interlocking_directorate.clone(),
                            name,
                            backup_ts,
                            spacelike_ts,
                            ct,
                            request.compression_level,
                        ),
                        bcone
                            .spacelike_key
                            .map_or_else(|| vec![], |k| k.into_raw().unwrap()),
                        bcone
                            .lightlike_key
                            .map_or_else(|| vec![], |k| k.into_raw().unwrap()),
                    )
                };

                let mut response = BackupResponse::default();
                match res {
                    Err(e) => {
                        error!(?e; "backup brane failed";
                            "brane" => ?bcone.brane,
                            "spacelike_key" => hex::encode_upper(&spacelike_key),
                            "lightlike_key" => hex::encode_upper(&lightlike_key),
                        );
                        response.set_error(e.into());
                    }
                    Ok((mut files, stat)) => {
                        debug!("backup brane finish";
                            "brane" => ?bcone.brane,
                            "spacelike_key" => hex::encode_upper(&spacelike_key),
                            "lightlike_key" => hex::encode_upper(&lightlike_key),
                            "details" => ?stat);

                        for file in files.iter_mut() {
                            file.set_spacelike_key(spacelike_key.clone());
                            file.set_lightlike_key(lightlike_key.clone());
                            file.set_spacelike_version(spacelike_ts.into_inner());
                            file.set_lightlike_version(lightlike_ts.into_inner());
                        }
                        response.set_files(files.into());
                    }
                }
                response.set_spacelike_key(spacelike_key);
                response.set_lightlike_key(lightlike_key);

                if let Err(e) = tx.unbounded_lightlike(response) {
                    error!(?e; "backup failed to lightlike response");
                    return;
                }
            }

            einsteindb_alloc::remove_thread_memory_accessor();
        });
    }

    pub fn handle_backup_task(&self, task: Task) {
        let Task { request, resp } = task;
        let is_raw_kv = request.is_raw_kv;
        let spacelike_key = if request.spacelike_key.is_empty() {
            None
        } else {
            // TODO: if is_raw_kv is written everywhere. It need to be simplified.
            if is_raw_kv {
                Some(Key::from_encoded(request.spacelike_key.clone()))
            } else {
                Some(Key::from_raw(&request.spacelike_key))
            }
        };
        let lightlike_key = if request.lightlike_key.is_empty() {
            None
        } else {
            if is_raw_kv {
                Some(Key::from_encoded(request.lightlike_key.clone()))
            } else {
                Some(Key::from_raw(&request.lightlike_key))
            }
        };

        let prs = Arc::new(Mutex::new(Progress::new(
            self.store_id,
            spacelike_key,
            lightlike_key,
            self.brane_info.clone(),
            is_raw_kv,
            request.causet,
        )));
        let concurrency = self.config_manager.0.read().unwrap().num_threads;
        self.pool.borrow_mut().adjust_with(concurrency);
        for _ in 0..concurrency {
            self.spawn_backup_worker(prs.clone(), request.clone(), resp.clone());
        }
    }
}

impl<E: Engine, R: BraneInfoProvider> Runnable for node<E, R> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        if task.has_canceled() {
            warn!("backup task has canceled"; "task" => %task);
            return;
        }
        info!("run backup task"; "task" => %task);
        self.handle_backup_task(task);
        self.pool.borrow_mut().heartbeat();
    }
}

impl<E: Engine, R: BraneInfoProvider> RunnableWithTimer for node<E, R> {
    type TimeoutTask = ();

    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        let pool_idle_duration = Duration::from_millis(self.pool_idle_memory_barrier);
        self.pool.borrow_mut().check_active(pool_idle_duration);
        timer.add_task(pool_idle_duration, ());
    }
}

/// Get the min lightlike key from the given `lightlike_key` and `Brane`'s lightlike key.
fn get_min_lightlike_key(lightlike_key: Option<&Key>, brane: &Brane) -> Option<Key> {
    let brane_lightlike = if brane.get_lightlike_key().is_empty() {
        None
    } else {
        Some(Key::from_encoded_slice(brane.get_lightlike_key()))
    };
    if brane.get_lightlike_key().is_empty() {
        lightlike_key.cloned()
    } else if lightlike_key.is_none() {
        brane_lightlike
    } else {
        let lightlike_slice = lightlike_key.as_ref().unwrap().as_encoded().as_slice();
        if lightlike_slice < brane.get_lightlike_key() {
            lightlike_key.cloned()
        } else {
            brane_lightlike
        }
    }
}

/// Get the max spacelike key from the given `spacelike_key` and `Brane`'s spacelike key.
fn get_max_spacelike_key(spacelike_key: Option<&Key>, brane: &Brane) -> Option<Key> {
    let brane_spacelike = if brane.get_spacelike_key().is_empty() {
        None
    } else {
        Some(Key::from_encoded_slice(brane.get_spacelike_key()))
    };
    if spacelike_key.is_none() {
        brane_spacelike
    } else {
        let spacelike_slice = spacelike_key.as_ref().unwrap().as_encoded().as_slice();
        if spacelike_slice < brane.get_spacelike_key() {
            brane_spacelike
        } else {
            spacelike_key.cloned()
        }
    }
}

/// Construct an backup file name based on the given store id and brane.
/// A name consists with three parts: store id, brane_id and a epoch version.
fn backup_file_name(store_id: u64, brane: &Brane, key: Option<String>) -> String {
    match key {
        Some(k) => format!(
            "{}_{}_{}_{}",
            store_id,
            brane.get_id(),
            brane.get_brane_epoch().get_version(),
            k
        ),
        None => format!(
            "{}_{}_{}",
            store_id,
            brane.get_id(),
            brane.get_brane_epoch().get_version()
        ),
    }
}

// convert BackupCompresionType to rocks db DBCompressionType
fn to_sst_compression_type(ct: CompressionType) -> Option<SstCompressionType> {
    match ct {
        CompressionType::Lz4 => Some(SstCompressionType::Lz4),
        CompressionType::Snappy => Some(SstCompressionType::Snappy),
        CompressionType::Zstd => Some(SstCompressionType::Zstd),
        CompressionType::Unknown => None,
    }
}

#[causet(test)]
pub mod tests {
    use super::*;
    use external_causet_storage::{make_local_backlightlike, make_noop_backlightlike};
    use futures::executor::block_on;
    use futures::stream::StreamExt;
    use ekvproto::meta_timeshare;
    use violetabftstore::interlock::BraneCollector;
    use violetabftstore::interlock::Result as CopResult;
    use violetabftstore::interlock::SeekBraneCallback;
    use violetabftstore::store::util::new_peer;
    use std::thread;
    use tempfile::TempDir;
    use einsteindb::causet_storage::tail_pointer::tests::*;
    use einsteindb::causet_storage::txn::tests::must_commit;
    use einsteindb::causet_storage::{LmdbEngine, TestEngineBuilder};
    use einsteindb_util::time::Instant;
    use txn_types::SHORT_VALUE_MAX_LEN;

    #[derive(Clone)]
    pub struct MockBraneInfoProvider {
        branes: Arc<Mutex<BraneCollector>>,
        cancel: Option<Arc<AtomicBool>>,
    }
    impl MockBraneInfoProvider {
        pub fn new() -> Self {
            MockBraneInfoProvider {
                branes: Arc::new(Mutex::new(BraneCollector::new())),
                cancel: None,
            }
        }
        pub fn set_branes(&self, branes: Vec<(Vec<u8>, Vec<u8>, u64)>) {
            let mut map = self.branes.dagger().unwrap();
            for (mut spacelike_key, mut lightlike_key, id) in branes {
                if !spacelike_key.is_empty() {
                    spacelike_key = Key::from_raw(&spacelike_key).into_encoded();
                }
                if !lightlike_key.is_empty() {
                    lightlike_key = Key::from_raw(&lightlike_key).into_encoded();
                }
                let mut r = meta_timeshare::Brane::default();
                r.set_id(id);
                r.set_spacelike_key(spacelike_key.clone());
                r.set_lightlike_key(lightlike_key);
                r.mut_peers().push(new_peer(1, 1));
                map.create_brane(r, StateRole::Leader);
            }
        }
        fn canecl_on_seek(&mut self, cancel: Arc<AtomicBool>) {
            self.cancel = Some(cancel);
        }
    }
    impl BraneInfoProvider for MockBraneInfoProvider {
        fn seek_brane(&self, from: &[u8], callback: SeekBraneCallback) -> CopResult<()> {
            let from = from.to_vec();
            let branes = self.branes.dagger().unwrap();
            if let Some(c) = self.cancel.as_ref() {
                c.store(true, Ordering::SeqCst);
            }
            branes.handle_seek_brane(from, callback);
            Ok(())
        }
    }

    pub fn new_lightlikepoint() -> (TempDir, node<LmdbEngine, MockBraneInfoProvider>) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .causets(&[
                edb::Causet_DEFAULT,
                edb::Causet_DAGGER,
                edb::Causet_WRITE,
            ])
            .build()
            .unwrap();
        let interlocking_directorate = ConcurrencyManager::new(1.into());
        let db = rocks.get_lmdb().get_sync_db();
        (
            temp,
            node::new(
                1,
                rocks,
                MockBraneInfoProvider::new(),
                db,
                BackupConfig { num_threads: 4 },
                interlocking_directorate,
            ),
        )
    }

    pub fn check_response<F>(rx: UnboundedReceiver<BackupResponse>, check: F)
    where
        F: FnOnce(Option<BackupResponse>),
    {
        let rx = rx.fuse();
        let (resp, rx) = block_on(rx.into_future());
        check(resp);
        let (none, _rx) = block_on(rx.into_future());
        assert!(none.is_none(), "{:?}", none);
    }
    #[test]
    fn test_seek_cone() {
        let (_tmp, lightlikepoint) = new_lightlikepoint();

        lightlikepoint.brane_info.set_branes(vec![
            (b"".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"9".to_vec(), 4),
            (b"9".to_vec(), b"".to_vec(), 5),
        ]);
        // Test seek backup cone.
        let test_seek_backup_cone =
            |spacelike_key: &[u8], lightlike_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
                let spacelike_key = if spacelike_key.is_empty() {
                    None
                } else {
                    Some(Key::from_raw(spacelike_key))
                };
                let lightlike_key = if lightlike_key.is_empty() {
                    None
                } else {
                    Some(Key::from_raw(lightlike_key))
                };
                let mut prs = Progress::new(
                    lightlikepoint.store_id,
                    spacelike_key,
                    lightlike_key,
                    lightlikepoint.brane_info.clone(),
                    false,
                    edb::Causet_DEFAULT,
                );

                let mut cones = Vec::with_capacity(expect.len());
                while cones.len() != expect.len() {
                    let n = (rand::random::<usize>() % 3) + 1;
                    let mut r = prs.forward(n);
                    // The returned backup cones should <= n
                    assert!(r.len() <= n);

                    if r.is_empty() {
                        // if return a empty vec then the progress is finished
                        assert_eq!(
                            cones.len(),
                            expect.len(),
                            "got {:?}, expect {:?}",
                            cones,
                            expect
                        );
                    }
                    cones.applightlike(&mut r);
                }

                for (a, b) in cones.into_iter().zip(expect) {
                    assert_eq!(
                        a.spacelike_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        b.0
                    );
                    assert_eq!(
                        a.lightlike_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        b.1
                    );
                }
            };

        // Test whether responses contain correct cone.
        #[allow(clippy::blocks_in_if_conditions)]
        let test_handle_backup_task_cone =
            |spacelike_key: &[u8], lightlike_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
                let tmp = TempDir::new().unwrap();
                let backlightlike = external_causet_storage::make_local_backlightlike(tmp.path());
                let (tx, rx) = unbounded();
                let task = Task {
                    request: Request {
                        spacelike_key: spacelike_key.to_vec(),
                        lightlike_key: lightlike_key.to_vec(),
                        spacelike_ts: 1.into(),
                        lightlike_ts: 1.into(),
                        backlightlike,
                        limiter: Limiter::new(INFINITY),
                        cancel: Arc::default(),
                        is_raw_kv: false,
                        causet: edb::Causet_DEFAULT,
                        compression_type: CompressionType::Unknown,
                        compression_level: 0,
                    },
                    resp: tx,
                };
                lightlikepoint.handle_backup_task(task);
                let resps: Vec<_> = block_on(rx.collect());
                for a in &resps {
                    assert!(
                        expect
                            .iter()
                            .any(|b| { a.get_spacelike_key() == b.0 && a.get_lightlike_key() == b.1 }),
                        "{:?} {:?}",
                        resps,
                        expect
                    );
                }
                assert_eq!(resps.len(), expect.len());
            };

        // Backup cone from case.0 to case.1,
        // the case.2 is the expected results.
        type Case<'a> = (&'a [u8], &'a [u8], Vec<(&'a [u8], &'a [u8])>);

        let case: Vec<Case> = vec![
            (b"", b"1", vec![(b"", b"1")]),
            (b"", b"2", vec![(b"", b"1"), (b"1", b"2")]),
            (b"1", b"2", vec![(b"1", b"2")]),
            (b"1", b"3", vec![(b"1", b"2")]),
            (b"1", b"4", vec![(b"1", b"2"), (b"3", b"4")]),
            (b"4", b"6", vec![]),
            (b"4", b"5", vec![]),
            (b"2", b"7", vec![(b"3", b"4")]),
            (b"7", b"8", vec![(b"7", b"8")]),
            (b"3", b"", vec![(b"3", b"4"), (b"7", b"9"), (b"9", b"")]),
            (b"5", b"", vec![(b"7", b"9"), (b"9", b"")]),
            (b"7", b"", vec![(b"7", b"9"), (b"9", b"")]),
            (b"8", b"91", vec![(b"8", b"9"), (b"9", b"91")]),
            (b"8", b"", vec![(b"8", b"9"), (b"9", b"")]),
            (
                b"",
                b"",
                vec![
                    (b"", b"1"),
                    (b"1", b"2"),
                    (b"3", b"4"),
                    (b"7", b"9"),
                    (b"9", b""),
                ],
            ),
        ];
        for (spacelike_key, lightlike_key, cones) in case {
            test_seek_backup_cone(spacelike_key, lightlike_key, cones.clone());
            test_handle_backup_task_cone(spacelike_key, lightlike_key, cones);
        }
    }

    #[test]
    fn test_handle_backup_task() {
        let (tmp, lightlikepoint) = new_lightlikepoint();
        let engine = lightlikepoint.engine.clone();

        lightlikepoint
            .brane_info
            .set_branes(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts = TimeStamp::new(1);
        let mut alloc_ts = || *ts.incr();
        let mut backup_tss = vec![];
        // Multi-versions for key 0..9.
        for len in &[SHORT_VALUE_MAX_LEN - 1, SHORT_VALUE_MAX_LEN * 2] {
            for i in 0..10u8 {
                let spacelike = alloc_ts();
                let commit = alloc_ts();
                let key = format!("{}", i);
                must_prewrite_put(
                    &engine,
                    key.as_bytes(),
                    &vec![i; *len],
                    key.as_bytes(),
                    spacelike,
                );
                must_commit(&engine, key.as_bytes(), spacelike, commit);
                backup_tss.push((alloc_ts(), len));
            }
        }

        // TODO: check key number for each snapshot.
        let limiter = Limiter::new(10.0 * 1024.0 * 1024.0 /* 10 MB/s */);
        for (ts, len) in backup_tss {
            let mut req = BackupRequest::default();
            req.set_spacelike_key(vec![]);
            req.set_lightlike_key(vec![b'5']);
            req.set_spacelike_version(0);
            req.set_lightlike_version(ts.into_inner());
            let (tx, rx) = unbounded();
            // Empty path should return an error.
            Task::new(req.clone(), tx.clone()).unwrap_err();

            // Set an unique path to avoid AlreadyExists error.
            req.set_causet_storage_backlightlike(make_local_backlightlike(&tmp.path().join(ts.to_string())));
            if len % 2 == 0 {
                req.set_rate_limit(10 * 1024 * 1024);
            }
            let (mut task, _) = Task::new(req, tx).unwrap();
            if len % 2 == 0 {
                // Make sure the rate limiter is set.
                assert!(task.request.limiter.speed_limit().is_finite());
                // Share the same rate limiter.
                task.request.limiter = limiter.clone();
            }
            lightlikepoint.handle_backup_task(task);
            let (resp, rx) = block_on(rx.into_future());
            let resp = resp.unwrap();
            assert!(!resp.has_error(), "{:?}", resp);
            let file_len = if *len <= SHORT_VALUE_MAX_LEN { 1 } else { 2 };
            assert_eq!(
                resp.get_files().len(),
                file_len, /* default and write */
                "{:?}",
                resp
            );
            let (none, _rx) = block_on(rx.into_future());
            assert!(none.is_none(), "{:?}", none);
        }
    }

    #[test]
    fn test_scan_error() {
        let (tmp, lightlikepoint) = new_lightlikepoint();
        let engine = lightlikepoint.engine.clone();

        lightlikepoint
            .brane_info
            .set_branes(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts: TimeStamp = 1.into();
        let mut alloc_ts = || *ts.incr();
        let spacelike = alloc_ts();
        let key = format!("{}", spacelike);
        must_prewrite_put(
            &engine,
            key.as_bytes(),
            key.as_bytes(),
            key.as_bytes(),
            spacelike,
        );

        let now = alloc_ts();
        let mut req = BackupRequest::default();
        req.set_spacelike_key(vec![]);
        req.set_lightlike_key(vec![b'5']);
        req.set_spacelike_version(now.into_inner());
        req.set_lightlike_version(now.into_inner());
        req.set_concurrency(4);
        // Set an unique path to avoid AlreadyExists error.
        req.set_causet_storage_backlightlike(make_local_backlightlike(&tmp.path().join(now.to_string())));
        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req.clone(), tx).unwrap();
        lightlikepoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_kv_error(), "{:?}", resp);
            assert!(resp.get_error().get_kv_error().has_locked(), "{:?}", resp);
            assert_eq!(resp.get_files().len(), 0, "{:?}", resp);
        });

        // Commit the perwrite.
        let commit = alloc_ts();
        must_commit(&engine, key.as_bytes(), spacelike, commit);

        // Test whether it can correctly convert not leader to brane error.
        engine.trigger_not_leader();
        let now = alloc_ts();
        req.set_spacelike_version(now.into_inner());
        req.set_lightlike_version(now.into_inner());
        // Set an unique path to avoid AlreadyExists error.
        req.set_causet_storage_backlightlike(make_local_backlightlike(&tmp.path().join(now.to_string())));
        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req, tx).unwrap();
        lightlikepoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_brane_error(), "{:?}", resp);
            assert!(
                resp.get_error().get_brane_error().has_not_leader(),
                "{:?}",
                resp
            );
        });
    }

    #[test]
    fn test_cancel() {
        let (temp, mut lightlikepoint) = new_lightlikepoint();
        let engine = lightlikepoint.engine.clone();

        lightlikepoint
            .brane_info
            .set_branes(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts: TimeStamp = 1.into();
        let mut alloc_ts = || *ts.incr();
        let spacelike = alloc_ts();
        let key = format!("{}", spacelike);
        must_prewrite_put(
            &engine,
            key.as_bytes(),
            key.as_bytes(),
            key.as_bytes(),
            spacelike,
        );
        // Commit the perwrite.
        let commit = alloc_ts();
        must_commit(&engine, key.as_bytes(), spacelike, commit);

        let now = alloc_ts();
        let mut req = BackupRequest::default();
        req.set_spacelike_key(vec![]);
        req.set_lightlike_key(vec![]);
        req.set_spacelike_version(now.into_inner());
        req.set_lightlike_version(now.into_inner());
        req.set_concurrency(4);
        req.set_causet_storage_backlightlike(make_local_backlightlike(temp.path()));

        // Cancel the task before spacelikeing the task.
        let (tx, rx) = unbounded();
        let (task, cancel) = Task::new(req.clone(), tx).unwrap();
        // Cancel the task.
        cancel.store(true, Ordering::SeqCst);
        lightlikepoint.handle_backup_task(task);
        check_response(rx, |resp| {
            assert!(resp.is_none());
        });

        // Cancel the task during backup.
        let (tx, rx) = unbounded();
        let (task, cancel) = Task::new(req, tx).unwrap();
        lightlikepoint.brane_info.canecl_on_seek(cancel);
        lightlikepoint.handle_backup_task(task);
        check_response(rx, |resp| {
            assert!(resp.is_none());
        });
    }

    #[test]
    fn test_busy() {
        let (_tmp, lightlikepoint) = new_lightlikepoint();
        let engine = lightlikepoint.engine.clone();

        lightlikepoint
            .brane_info
            .set_branes(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut req = BackupRequest::default();
        req.set_spacelike_key(vec![]);
        req.set_lightlike_key(vec![]);
        req.set_spacelike_version(1);
        req.set_lightlike_version(1);
        req.set_concurrency(4);
        req.set_causet_storage_backlightlike(make_noop_backlightlike());

        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req, tx).unwrap();
        // Pause the engine 6 seconds to trigger Timeout error.
        // The Timeout error is translated to server is busy.
        engine.pause(Duration::from_secs(6));
        lightlikepoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_brane_error(), "{:?}", resp);
            assert!(
                resp.get_error().get_brane_error().has_server_is_busy(),
                "{:?}",
                resp
            );
        });
    }

    #[test]
    fn test_adjust_thread_pool_size() {
        let (_tmp, lightlikepoint) = new_lightlikepoint();
        lightlikepoint
            .brane_info
            .set_branes(vec![(b"".to_vec(), b"".to_vec(), 1)]);

        let mut req = BackupRequest::default();
        req.set_spacelike_key(vec![]);
        req.set_lightlike_key(vec![]);
        req.set_spacelike_version(1);
        req.set_lightlike_version(1);
        req.set_causet_storage_backlightlike(make_noop_backlightlike());

        let (tx, _) = unbounded();

        // expand thread pool is needed
        lightlikepoint.get_config_manager().set_num_threads(15);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        lightlikepoint.handle_backup_task(task);
        assert!(lightlikepoint.pool.borrow().size == 15);

        // shrink thread pool only if there are too many idle threads
        lightlikepoint.get_config_manager().set_num_threads(10);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        lightlikepoint.handle_backup_task(task);
        assert!(lightlikepoint.pool.borrow().size == 15);

        lightlikepoint.get_config_manager().set_num_threads(3);
        let (task, _) = Task::new(req, tx).unwrap();
        lightlikepoint.handle_backup_task(task);
        assert!(lightlikepoint.pool.borrow().size == 3);
    }

    #[test]
    fn test_thread_pool_shutdown_when_idle() {
        let (_, mut lightlikepoint) = new_lightlikepoint();

        // set the idle memory_barrier to 100ms
        lightlikepoint.pool_idle_memory_barrier = 100;
        let mut backup_timer = lightlikepoint.new_timer();
        let lightlikepoint = Arc::new(Mutex::new(lightlikepoint));
        let interlock_semaphore = {
            let lightlikepoint = lightlikepoint.clone();
            let (tx, rx) = einsteindb_util::mpsc::unbounded();
            thread::spawn(move || loop {
                let tick_time = backup_timer.next_timeout().unwrap();
                let timeout = tick_time.checked_sub(Instant::now()).unwrap_or_default();
                let task = match rx.recv_timeout(timeout) {
                    Ok(Some(task)) => Some(task),
                    _ => None,
                };
                if let Some(task) = task {
                    let mut lightlikepoint = lightlikepoint.dagger().unwrap();
                    lightlikepoint.run(task);
                }
                lightlikepoint.dagger().unwrap().on_timeout(&mut backup_timer, ());
            });
            tx
        };

        let mut req = BackupRequest::default();
        req.set_spacelike_key(vec![]);
        req.set_lightlike_key(vec![]);
        req.set_spacelike_version(1);
        req.set_lightlike_version(1);
        req.set_causet_storage_backlightlike(make_noop_backlightlike());

        lightlikepoint
            .dagger()
            .unwrap()
            .get_config_manager()
            .set_num_threads(10);

        let (tx, resp_rx) = unbounded();
        let (task, _) = Task::new(req, tx).unwrap();

        // if not task arrive after create the thread pool is empty
        assert_eq!(lightlikepoint.dagger().unwrap().pool.borrow().size, 0);

        interlock_semaphore.lightlike(Some(task)).unwrap();
        // wait until the task finish
        let _ = block_on(resp_rx.into_future());
        assert_eq!(lightlikepoint.dagger().unwrap().pool.borrow().size, 10);

        // thread pool not yet shutdown
        thread::sleep(Duration::from_millis(50));
        assert_eq!(lightlikepoint.dagger().unwrap().pool.borrow().size, 10);

        // thread pool shutdown if not task arrive more than 100ms
        thread::sleep(Duration::from_millis(100));
        assert_eq!(lightlikepoint.dagger().unwrap().pool.borrow().size, 0);
    }
    // TODO: brane err in txn(engine(request))
}
