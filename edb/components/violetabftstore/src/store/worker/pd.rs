// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::{self, lightlikeer};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread::{Builder, JoinHandle};
use std::time::Duration;
use std::{cmp, io};

use futures::future::TryFutureExt;
use tokio::task::spawn_local;

use edb::{CausetEngine, VioletaBftEngine};
use ekvproto::meta_timeshare;
use ekvproto::fidel_timeshare;
use ekvproto::violetabft_cmd_timeshare::{AdminCmdType, AdminRequest, VioletaBftCmdRequest, SplitRequest};
use ekvproto::violetabft_server_timeshare::VioletaBftMessage;
use ekvproto::replication_mode_timeshare::BraneReplicationStatus;
use prometheus::local::LocalHistogram;
use violetabft::evioletabft_timeshare::ConfChangeType;

use crate::interlock::{get_brane_approximate_tuplespaceInstanton, get_brane_approximate_size};
use crate::store::cmd_resp::new_error;
use crate::store::metrics::*;
use crate::store::util::is_epoch_stale;
use crate::store::util::TuplespaceInstantonInfoFormatter;
use crate::store::worker::split_controller::{SplitInfo, TOP_N};
use crate::store::worker::{AutoSplitController, ReadStats};
use crate::store::Callback;
use crate::store::StoreInfo;
use crate::store::{CasualMessage, PeerMsg, VioletaBftCommand, VioletaBftRouter, StoreMsg};

use interlocking_directorate::ConcurrencyManager;
use fidel_client::metrics::*;
use fidel_client::{Error, FidelClient, BraneStat};
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::metrics::ThreadInfoStatistics;
use violetabftstore::interlock::::time::UnixSecs;
use violetabftstore::interlock::::worker::{FutureRunnable as Runnable, FutureInterlock_Semaphore as Interlock_Semaphore, Stopped};

type RecordPairVec = Vec<fidel_timeshare::RecordPair>;

#[derive(Default, Debug, Clone)]
pub struct FlowStatistics {
    pub read_tuplespaceInstanton: usize,
    pub read_bytes: usize,
}

impl FlowStatistics {
    pub fn add(&mut self, other: &Self) {
        self.read_bytes = self.read_bytes.saturating_add(other.read_bytes);
        self.read_tuplespaceInstanton = self.read_tuplespaceInstanton.saturating_add(other.read_tuplespaceInstanton);
    }
}

// Reports flow statistics to outside.
pub trait FlowStatsReporter: lightlike + Clone + Sync + 'static {
    // TODO: maybe we need to return a Result later?
    fn report_read_stats(&self, read_stats: ReadStats);
}

impl<E> FlowStatsReporter for Interlock_Semaphore<Task<E>>
where
    E: CausetEngine,
{
    fn report_read_stats(&self, read_stats: ReadStats) {
        if let Err(e) = self.schedule(Task::ReadStats { read_stats }) {
            error!("Failed to lightlike read flow statistics"; "err" => ?e);
        }
    }
}

/// Uses an asynchronous thread to tell FIDel something.
pub enum Task<E>
where
    E: CausetEngine,
{
    AskSplit {
        brane: meta_timeshare::Brane,
        split_key: Vec<u8>,
        peer: meta_timeshare::Peer,
        // If true, right Brane derives origin brane_id.
        right_derive: bool,
        callback: Callback<E::Snapshot>,
    },
    AskBatchSplit {
        brane: meta_timeshare::Brane,
        split_tuplespaceInstanton: Vec<Vec<u8>>,
        peer: meta_timeshare::Peer,
        // If true, right Brane derives origin brane_id.
        right_derive: bool,
        callback: Callback<E::Snapshot>,
    },
    AutoSplit {
        split_infos: Vec<SplitInfo>,
    },
    Heartbeat {
        term: u64,
        brane: meta_timeshare::Brane,
        peer: meta_timeshare::Peer,
        down_peers: Vec<fidel_timeshare::PeerStats>,
        plightlikeing_peers: Vec<meta_timeshare::Peer>,
        written_bytes: u64,
        written_tuplespaceInstanton: u64,
        approximate_size: Option<u64>,
        approximate_tuplespaceInstanton: Option<u64>,
        replication_status: Option<BraneReplicationStatus>,
    },
    StoreHeartbeat {
        stats: fidel_timeshare::StoreStats,
        store_info: StoreInfo<E>,
    },
    ReportBatchSplit {
        branes: Vec<meta_timeshare::Brane>,
    },
    ValidatePeer {
        brane: meta_timeshare::Brane,
        peer: meta_timeshare::Peer,
    },
    ReadStats {
        read_stats: ReadStats,
    },
    DestroyPeer {
        brane_id: u64,
    },
    StoreInfos {
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    },
    fidelioMaxTimestamp {
        brane_id: u64,
        initial_status: u64,
        max_ts_sync_status: Arc<AtomicU64>,
    },
}

pub struct StoreStat {
    pub engine_total_bytes_read: u64,
    pub engine_total_tuplespaceInstanton_read: u64,
    pub engine_last_total_bytes_read: u64,
    pub engine_last_total_tuplespaceInstanton_read: u64,
    pub last_report_ts: UnixSecs,

    pub brane_bytes_read: LocalHistogram,
    pub brane_tuplespaceInstanton_read: LocalHistogram,
    pub brane_bytes_written: LocalHistogram,
    pub brane_tuplespaceInstanton_written: LocalHistogram,

    pub store_cpu_usages: RecordPairVec,
    pub store_read_io_rates: RecordPairVec,
    pub store_write_io_rates: RecordPairVec,
}

impl Default for StoreStat {
    fn default() -> StoreStat {
        StoreStat {
            brane_bytes_read: REGION_READ_BYTES_HISTOGRAM.local(),
            brane_tuplespaceInstanton_read: REGION_READ_KEYS_HISTOGRAM.local(),
            brane_bytes_written: REGION_WRITTEN_BYTES_HISTOGRAM.local(),
            brane_tuplespaceInstanton_written: REGION_WRITTEN_KEYS_HISTOGRAM.local(),

            last_report_ts: UnixSecs::zero(),
            engine_total_bytes_read: 0,
            engine_total_tuplespaceInstanton_read: 0,
            engine_last_total_bytes_read: 0,
            engine_last_total_tuplespaceInstanton_read: 0,

            store_cpu_usages: RecordPairVec::default(),
            store_read_io_rates: RecordPairVec::default(),
            store_write_io_rates: RecordPairVec::default(),
        }
    }
}

#[derive(Default)]
pub struct PeerStat {
    pub read_bytes: u64,
    pub read_tuplespaceInstanton: u64,
    pub last_read_bytes: u64,
    pub last_read_tuplespaceInstanton: u64,
    pub last_written_bytes: u64,
    pub last_written_tuplespaceInstanton: u64,
    pub last_report_ts: UnixSecs,
}

impl<E> Display for Task<E>
where
    E: CausetEngine,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::AskSplit {
                ref brane,
                ref split_key,
                ..
            } => write!(
                f,
                "ask split brane {} with key {}",
                brane.get_id(),
                hex::encode_upper(&split_key),
            ),
            Task::AutoSplit {
                ref split_infos,
            } => write!(
                f,
                "auto split split branes, num is {}",
                split_infos.len(),
            ),
            Task::AskBatchSplit {
                ref brane,
                ref split_tuplespaceInstanton,
                ..
            } => write!(
                f,
                "ask split brane {} with {}",
                brane.get_id(),
                TuplespaceInstantonInfoFormatter(split_tuplespaceInstanton.iter())
            ),
            Task::Heartbeat {
                ref brane,
                ref peer,
                ref replication_status,
                ..
            } => write!(
                f,
                "heartbeat for brane {:?}, leader {}, replication status {:?}",
                brane,
                peer.get_id(),
                replication_status
            ),
            Task::StoreHeartbeat { ref stats, .. } => {
                write!(f, "store heartbeat stats: {:?}", stats)
            }
            Task::ReportBatchSplit { ref branes } => write!(f, "report split {:?}", branes),
            Task::ValidatePeer {
                ref brane,
                ref peer,
            } => write!(
                f,
                "validate peer {:?} with brane {:?}",
                peer, brane
            ),
            Task::ReadStats { ref read_stats } => {
                write!(f, "get the read statistics {:?}", read_stats)
            }
            Task::DestroyPeer { ref brane_id } => {
                write!(f, "destroy peer of brane {}", brane_id)
            }
            Task::StoreInfos {
                ref cpu_usages,
                ref read_io_rates,
                ref write_io_rates,
            } => write!(
                f,
                "get store's informations: cpu_usages {:?}, read_io_rates {:?}, write_io_rates {:?}",
                cpu_usages, read_io_rates, write_io_rates,
            ),
            Task::fidelioMaxTimestamp { brane_id, ..} => write!(
                f,
                "fidelio the max timestamp for brane {} in the concurrency manager",
                brane_id
            ),
        }
    }
}

const DEFAULT_QPS_INFO_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_COLLECT_INTERVAL: Duration = Duration::from_secs(1);

#[inline]
fn convert_record_pairs(m: HashMap<String, u64>) -> RecordPairVec {
    m.into_iter()
        .map(|(k, v)| {
            let mut pair = fidel_timeshare::RecordPair::default();
            pair.set_key(k);
            pair.set_value(v);
            pair
        })
        .collect()
}

struct StatsMonitor<E>
where
    E: CausetEngine,
{
    interlock_semaphore: Interlock_Semaphore<Task<E>>,
    handle: Option<JoinHandle<()>>,
    timer: Option<lightlikeer<bool>>,
    lightlikeer: Option<lightlikeer<ReadStats>>,
    thread_info_interval: Duration,
    qps_info_interval: Duration,
    collect_interval: Duration,
}

impl<E> StatsMonitor<E>
where
    E: CausetEngine,
{
    pub fn new(interval: Duration, interlock_semaphore: Interlock_Semaphore<Task<E>>) -> Self {
        StatsMonitor {
            interlock_semaphore,
            handle: None,
            timer: None,
            lightlikeer: None,
            thread_info_interval: interval,
            qps_info_interval: cmp::min(DEFAULT_QPS_INFO_INTERVAL, interval),
            collect_interval: cmp::min(DEFAULT_COLLECT_INTERVAL, interval),
        }
    }

    // Collecting thread information and obtaining qps information for auto split.
    // They run together in the same thread by taking modulo at different intervals.
    pub fn spacelike(
        &mut self,
        mut auto_split_controller: AutoSplitController,
    ) -> Result<(), io::Error> {
        if self.collect_interval < DEFAULT_COLLECT_INTERVAL {
            info!("it seems we are running tests, skip stats monitoring.");
            return Ok(());
        }
        let mut timer_cnt = 0; // to run functions with different intervals in a loop
        let collect_interval = self.collect_interval;
        if self.thread_info_interval < self.collect_interval {
            info!("running in test mode, skip spacelikeing monitor.");
            return Ok(());
        }
        let thread_info_interval = self
            .thread_info_interval
            .div_duration_f64(self.collect_interval) as i32;
        let qps_info_interval = self
            .qps_info_interval
            .div_duration_f64(self.collect_interval) as i32;
        let (tx, rx) = mpsc::channel();
        self.timer = Some(tx);

        let (lightlikeer, receiver) = mpsc::channel();
        self.lightlikeer = Some(lightlikeer);

        let interlock_semaphore = self.interlock_semaphore.clone();

        let h = Builder::new()
            .name(thd_name!("stats-monitor"))
            .spawn(move || {
                edb_alloc::add_thread_memory_accessor();
                let mut thread_stats = ThreadInfoStatistics::new();
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(collect_interval) {
                    if timer_cnt % thread_info_interval == 0 {
                        thread_stats.record();
                        let cpu_usages = convert_record_pairs(thread_stats.get_cpu_usages());
                        let read_io_rates = convert_record_pairs(thread_stats.get_read_io_rates());
                        let write_io_rates =
                            convert_record_pairs(thread_stats.get_write_io_rates());

                        let task = Task::StoreInfos {
                            cpu_usages,
                            read_io_rates,
                            write_io_rates,
                        };
                        if let Err(e) = interlock_semaphore.schedule(task) {
                            error!(
                                "failed to lightlike store infos to fidel worker";
                                "err" => ?e,
                            );
                        }
                    }
                    if timer_cnt % qps_info_interval == 0 {
                        let mut others = vec![];
                        while let Ok(other) = receiver.try_recv() {
                            others.push(other);
                        }
                        let (top, split_infos) = auto_split_controller.flush(others);
                        auto_split_controller.clear();
                        let task = Task::AutoSplit { split_infos };
                        if let Err(e) = interlock_semaphore.schedule(task) {
                            error!(
                                "failed to lightlike split infos to fidel worker";
                                "err" => ?e,
                            );
                        }

                        for i in 0..TOP_N {
                            if i < top.len() {
                                READ_QPS_TOPN
                                    .with_label_values(&[&i.to_string()])
                                    .set(top[i] as f64);
                            } else {
                                READ_QPS_TOPN.with_label_values(&[&i.to_string()]).set(0.0);
                            }
                        }
                    }
                    // modules timer_cnt with the least common multiple of intervals to avoid overflow
                    timer_cnt = (timer_cnt + 1) % (qps_info_interval * thread_info_interval);
                    auto_split_controller.refresh_causet();
                }
                edb_alloc::remove_thread_memory_accessor();
            })?;

        self.handle = Some(h);
        Ok(())
    }

    pub fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            drop(self.timer.take());
            drop(self.lightlikeer.take());
            if let Err(e) = h.join() {
                error!("join stats collector failed"; "err" => ?e);
            }
        }
    }

    pub fn get_lightlikeer(&self) -> &Option<lightlikeer<ReadStats>> {
        &self.lightlikeer
    }
}

pub struct Runner<EK, ER, T>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
    T: FidelClient + 'static,
{
    store_id: u64,
    fidel_client: Arc<T>,
    router: VioletaBftRouter<EK, ER>,
    db: EK,
    brane_peers: HashMap<u64, PeerStat>,
    store_stat: StoreStat,
    is_hb_receiver_scheduled: bool,
    // Records the boot time.
    spacelike_ts: UnixSecs,

    // use for Runner inner handle function to lightlike Task to itself
    // actually it is the lightlikeer connected to Runner's Worker which
    // calls Runner's run() on Task received.
    interlock_semaphore: Interlock_Semaphore<Task<EK>>,
    stats_monitor: StatsMonitor<EK>,

    interlocking_directorate: ConcurrencyManager,
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
    T: FidelClient + 'static,
{
    const INTERVAL_DIVISOR: u32 = 2;

    pub fn new(
        store_id: u64,
        fidel_client: Arc<T>,
        router: VioletaBftRouter<EK, ER>,
        db: EK,
        interlock_semaphore: Interlock_Semaphore<Task<EK>>,
        store_heartbeat_interval: Duration,
        auto_split_controller: AutoSplitController,
        interlocking_directorate: ConcurrencyManager,
    ) -> Runner<EK, ER, T> {
        let interval = store_heartbeat_interval / Self::INTERVAL_DIVISOR;
        let mut stats_monitor = StatsMonitor::new(interval, interlock_semaphore.clone());
        if let Err(e) = stats_monitor.spacelike(auto_split_controller) {
            error!("failed to spacelike stats collector, error = {:?}", e);
        }

        Runner {
            store_id,
            fidel_client,
            router,
            db,
            is_hb_receiver_scheduled: false,
            brane_peers: HashMap::default(),
            store_stat: StoreStat::default(),
            spacelike_ts: UnixSecs::now(),
            interlock_semaphore,
            stats_monitor,
            interlocking_directorate,
        }
    }

    // Deprecate
    fn handle_ask_split(
        &self,
        mut brane: meta_timeshare::Brane,
        split_key: Vec<u8>,
        peer: meta_timeshare::Peer,
        right_derive: bool,
        callback: Callback<EK::Snapshot>,
        task: String,
    ) {
        let router = self.router.clone();
        let resp = self.fidel_client.ask_split(brane.clone());
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    info!(
                        "try to split brane";
                        "brane_id" => brane.get_id(),
                        "new_brane_id" => resp.get_new_brane_id(),
                        "brane" => ?brane,
                        "task"=>task,
                    );

                    let req = new_split_brane_request(
                        split_key,
                        resp.get_new_brane_id(),
                        resp.take_new_peer_ids(),
                        right_derive,
                    );
                    let brane_id = brane.get_id();
                    let epoch = brane.take_brane_epoch();
                    lightlike_admin_request(&router, brane_id, epoch, peer, req, callback)
                }
                Err(e) => {
                    warn!("failed to ask split";
                    "brane_id" => brane.get_id(),
                    "err" => ?e,
                    "task"=>task);
                }
            }
        };
        spawn_local(f);
    }

    // Note: The parameter doesn't contain `self` because this function may
    // be called in an asynchronous context.
    fn handle_ask_batch_split(
        router: VioletaBftRouter<EK, ER>,
        interlock_semaphore: Interlock_Semaphore<Task<EK>>,
        fidel_client: Arc<T>,
        mut brane: meta_timeshare::Brane,
        mut split_tuplespaceInstanton: Vec<Vec<u8>>,
        peer: meta_timeshare::Peer,
        right_derive: bool,
        callback: Callback<EK::Snapshot>,
        task: String,
    ) {
        if split_tuplespaceInstanton.is_empty() {
            info!("empty split key, skip ask batch split";
                "brane_id" => brane.get_id());
            return;
        }
        let resp = fidel_client.ask_batch_split(brane.clone(), split_tuplespaceInstanton.len());
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    info!(
                        "try to batch split brane";
                        "brane_id" => brane.get_id(),
                        "new_brane_ids" => ?resp.get_ids(),
                        "brane" => ?brane,
                        "task" => task,
                    );

                    let req = new_batch_split_brane_request(
                        split_tuplespaceInstanton,
                        resp.take_ids().into(),
                        right_derive,
                    );
                    let brane_id = brane.get_id();
                    let epoch = brane.take_brane_epoch();
                    lightlike_admin_request(&router, brane_id, epoch, peer, req, callback)
                }
                // When rolling fidelio, there might be some old version edbs that don't support batch split in cluster.
                // In this situation, FIDel version check would refuse `ask_batch_split`.
                // But if fidelio time is long, it may cause large Branes, so call `ask_split` instead.
                Err(Error::Incompatible) => {
                    let (brane_id, peer_id) = (brane.id, peer.id);
                    info!(
                        "ask_batch_split is incompatible, use ask_split instead";
                        "brane_id" => brane_id
                    );
                    let task = Task::AskSplit {
                        brane,
                        split_key: split_tuplespaceInstanton.pop().unwrap(),
                        peer,
                        right_derive,
                        callback,
                    };
                    if let Err(Stopped(t)) = interlock_semaphore.schedule(task) {
                        error!(
                            "failed to notify fidel to split: Stopped";
                            "brane_id" => brane_id,
                            "peer_id" =>  peer_id
                        );
                        match t {
                            Task::AskSplit { callback, .. } => {
                                callback.invoke_with_response(new_error(box_err!(
                                    "failed to split: Stopped"
                                )));
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "ask batch split failed";
                        "brane_id" => brane.get_id(),
                        "err" => ?e,
                    );
                }
            }
        };
        spawn_local(f);
    }

    fn handle_heartbeat(
        &self,
        term: u64,
        brane: meta_timeshare::Brane,
        peer: meta_timeshare::Peer,
        brane_stat: BraneStat,
        replication_status: Option<BraneReplicationStatus>,
    ) {
        self.store_stat
            .brane_bytes_written
            .observe(brane_stat.written_bytes as f64);
        self.store_stat
            .brane_tuplespaceInstanton_written
            .observe(brane_stat.written_tuplespaceInstanton as f64);
        self.store_stat
            .brane_bytes_read
            .observe(brane_stat.read_bytes as f64);
        self.store_stat
            .brane_tuplespaceInstanton_read
            .observe(brane_stat.read_tuplespaceInstanton as f64);

        let f = self
            .fidel_client
            .brane_heartbeat(term, brane.clone(), peer, brane_stat, replication_status)
            .map_err(move |e| {
                debug!(
                    "failed to lightlike heartbeat";
                    "brane_id" => brane.get_id(),
                    "err" => ?e
                );
            });
        spawn_local(f);
    }

    fn handle_store_heartbeat(&mut self, mut stats: fidel_timeshare::StoreStats, store_info: StoreInfo<EK>) {
        let disk_stats = match fs2::statvfs(store_info.engine.path()) {
            Err(e) => {
                error!(
                    "get disk stat for lmdb failed";
                    "engine_path" => store_info.engine.path(),
                    "err" => ?e
                );
                return;
            }
            Ok(stats) => stats,
        };

        let disk_cap = disk_stats.total_space();
        let capacity = if store_info.capacity == 0 || disk_cap < store_info.capacity {
            disk_cap
        } else {
            store_info.capacity
        };
        stats.set_capacity(capacity);

        // already include size of snapshot files
        let used_size =
            stats.get_used_size() + store_info.engine.get_engine_used_size().expect("causet");
        stats.set_used_size(used_size);

        let mut available = if capacity > used_size {
            capacity - used_size
        } else {
            warn!("no available space");
            0
        };

        // We only care about lmdb SST file size, so we should check disk available here.
        if available > disk_stats.free_space() {
            available = disk_stats.free_space();
        }

        stats.set_available(available);
        stats.set_bytes_read(
            self.store_stat.engine_total_bytes_read - self.store_stat.engine_last_total_bytes_read,
        );
        stats.set_tuplespaceInstanton_read(
            self.store_stat.engine_total_tuplespaceInstanton_read - self.store_stat.engine_last_total_tuplespaceInstanton_read,
        );

        stats.set_cpu_usages(self.store_stat.store_cpu_usages.clone().into());
        stats.set_read_io_rates(self.store_stat.store_read_io_rates.clone().into());
        stats.set_write_io_rates(self.store_stat.store_write_io_rates.clone().into());

        let mut interval = fidel_timeshare::TimeInterval::default();
        interval.set_spacelike_timestamp(self.store_stat.last_report_ts.into_inner());
        stats.set_interval(interval);
        self.store_stat.engine_last_total_bytes_read = self.store_stat.engine_total_bytes_read;
        self.store_stat.engine_last_total_tuplespaceInstanton_read = self.store_stat.engine_total_tuplespaceInstanton_read;
        self.store_stat.last_report_ts = UnixSecs::now();
        self.store_stat.brane_bytes_written.flush();
        self.store_stat.brane_tuplespaceInstanton_written.flush();
        self.store_stat.brane_bytes_read.flush();
        self.store_stat.brane_tuplespaceInstanton_read.flush();

        STORE_SIZE_GAUGE_VEC
            .with_label_values(&["capacity"])
            .set(capacity as i64);
        STORE_SIZE_GAUGE_VEC
            .with_label_values(&["available"])
            .set(available as i64);

        let router = self.router.clone();
        let resp = self.fidel_client.store_heartbeat(stats);
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    if let Some(status) = resp.replication_status.take() {
                        let _ = router.lightlike_control(StoreMsg::fidelioReplicationMode(status));
                    }
                }
                Err(e) => {
                    error!("store heartbeat failed"; "err" => ?e);
                }
            }
        };
        spawn_local(f);
    }

    fn handle_report_batch_split(&self, branes: Vec<meta_timeshare::Brane>) {
        let f = self.fidel_client.report_batch_split(branes).map_err(|e| {
            warn!("report split failed"; "err" => ?e);
        });
        spawn_local(f);
    }

    fn handle_validate_peer(&self, local_brane: meta_timeshare::Brane, peer: meta_timeshare::Peer) {
        let router = self.router.clone();
        let resp = self.fidel_client.get_brane_by_id(local_brane.get_id());
        let f = async move {
            match resp.await {
                Ok(Some(fidel_brane)) => {
                    if is_epoch_stale(
                        fidel_brane.get_brane_epoch(),
                        local_brane.get_brane_epoch(),
                    ) {
                        // The local Brane epoch is fresher than Brane epoch in FIDel
                        // This means the Brane info in FIDel is not fideliod to the latest even
                        // after `max_leader_missing_duration`. Something is wrong in the system.
                        // Just add a log here for this situation.
                        info!(
                            "local brane epoch is greater the \
                             brane epoch in FIDel ignore validate peer";
                            "brane_id" => local_brane.get_id(),
                            "peer_id" => peer.get_id(),
                            "local_brane_epoch" => ?local_brane.get_brane_epoch(),
                            "fidel_brane_epoch" => ?fidel_brane.get_brane_epoch()
                        );
                        FIDel_VALIDATE_PEER_COUNTER_VEC
                            .with_label_values(&["brane epoch error"])
                            .inc();
                        return;
                    }

                    if fidel_brane
                        .get_peers()
                        .iter()
                        .all(|p| p.get_id() != peer.get_id())
                    {
                        // Peer is not a member of this Brane anymore. Probably it's removed out.
                        // lightlike it a violetabft massage to destroy it since it's obsolete.
                        info!(
                            "peer is not a valid member of brane, to be \
                             destroyed soon";
                            "brane_id" => local_brane.get_id(),
                            "peer_id" => peer.get_id(),
                            "fidel_brane" => ?fidel_brane
                        );
                        FIDel_VALIDATE_PEER_COUNTER_VEC
                            .with_label_values(&["peer stale"])
                            .inc();
                        lightlike_destroy_peer_message(&router, local_brane, peer, fidel_brane);
                    } else {
                        info!(
                            "peer is still a valid member of brane";
                            "brane_id" => local_brane.get_id(),
                            "peer_id" => peer.get_id(),
                            "fidel_brane" => ?fidel_brane
                        );
                        FIDel_VALIDATE_PEER_COUNTER_VEC
                            .with_label_values(&["peer valid"])
                            .inc();
                    }
                }
                Ok(None) => {
                    // splitted Brane has not yet reported to FIDel.
                    // TODO: handle merge
                }
                Err(e) => {
                    error!("get brane failed"; "err" => ?e);
                }
            }
        };
        spawn_local(f);
    }

    fn schedule_heartbeat_receiver(&mut self) {
        let router = self.router.clone();
        let store_id = self.store_id;

        let fut = self.fidel_client
            .handle_brane_heartbeat_response(self.store_id, move |mut resp| {
                let brane_id = resp.get_brane_id();
                let epoch = resp.take_brane_epoch();
                let peer = resp.take_target_peer();

                if resp.has_change_peer() {
                    FIDel_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["change peer"])
                        .inc();

                    let mut change_peer = resp.take_change_peer();
                    info!(
                        "try to change peer";
                        "brane_id" => brane_id,
                        "change_type" => ?change_peer.get_change_type(),
                        "peer" => ?change_peer.get_peer()
                    );
                    let req = new_change_peer_request(
                        change_peer.get_change_type(),
                        change_peer.take_peer(),
                    );
                    lightlike_admin_request(&router, brane_id, epoch, peer, req, Callback::None);
                } else if resp.has_transfer_leader() {
                    FIDel_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["transfer leader"])
                        .inc();

                    let mut transfer_leader = resp.take_transfer_leader();
                    info!(
                        "try to transfer leader";
                        "brane_id" => brane_id,
                        "from_peer" => ?peer,
                        "to_peer" => ?transfer_leader.get_peer()
                    );
                    let req = new_transfer_leader_request(transfer_leader.take_peer());
                    lightlike_admin_request(&router, brane_id, epoch, peer, req, Callback::None);
                } else if resp.has_split_brane() {
                    FIDel_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["split brane"])
                        .inc();

                    let mut split_brane = resp.take_split_brane();
                    info!("try to split"; "brane_id" => brane_id, "brane_epoch" => ?epoch);
                    let msg = if split_brane.get_policy() == fidel_timeshare::CheckPolicy::Usekey {
                        CasualMessage::SplitBrane {
                            brane_epoch: epoch,
                            split_tuplespaceInstanton: split_brane.take_tuplespaceInstanton().into(),
                            callback: Callback::None,
                        }
                    } else {
                        CasualMessage::HalfSplitBrane {
                            brane_epoch: epoch,
                            policy: split_brane.get_policy(),
                        }
                    };
                    if let Err(e) = router.lightlike(brane_id, PeerMsg::CasualMessage(msg)) {
                        error!("lightlike halfsplit request failed"; "brane_id" => brane_id, "err" => ?e);
                    }
                } else if resp.has_merge() {
                    FIDel_HEARTBEAT_COUNTER_VEC.with_label_values(&["merge"]).inc();

                    let merge = resp.take_merge();
                    info!("try to merge"; "brane_id" => brane_id, "merge" => ?merge);
                    let req = new_merge_request(merge);
                    lightlike_admin_request(&router, brane_id, epoch, peer, req, Callback::None)
                } else {
                    FIDel_HEARTBEAT_COUNTER_VEC.with_label_values(&["noop"]).inc();
                }
            });
        let f = async move {
            match fut.await {
                Ok(_) => {
                    info!(
                        "brane heartbeat response handler exit";
                        "store_id" => store_id,
                    );
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        };
        spawn_local(f);
        self.is_hb_receiver_scheduled = true;
    }

    fn handle_read_stats(&mut self, read_stats: ReadStats) {
        for (brane_id, stats) in &read_stats.flows {
            let peer_stat = self
                .brane_peers
                .entry(*brane_id)
                .or_insert_with(PeerStat::default);
            peer_stat.read_bytes += stats.read_bytes as u64;
            peer_stat.read_tuplespaceInstanton += stats.read_tuplespaceInstanton as u64;
            self.store_stat.engine_total_bytes_read += stats.read_bytes as u64;
            self.store_stat.engine_total_tuplespaceInstanton_read += stats.read_tuplespaceInstanton as u64;
        }
        if !read_stats.brane_infos.is_empty() {
            if let Some(lightlikeer) = self.stats_monitor.get_lightlikeer() {
                if lightlikeer.lightlike(read_stats).is_err() {
                    warn!("lightlike read_stats failed, are we shutting down?")
                }
            }
        }
    }

    fn handle_destroy_peer(&mut self, brane_id: u64) {
        match self.brane_peers.remove(&brane_id) {
            None => {}
            Some(_) => info!("remove peer statistic record in fidel"; "brane_id" => brane_id),
        }
    }

    fn handle_store_infos(
        &mut self,
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    ) {
        self.store_stat.store_cpu_usages = cpu_usages;
        self.store_stat.store_read_io_rates = read_io_rates;
        self.store_stat.store_write_io_rates = write_io_rates;
    }

    fn handle_fidelio_max_timestamp(
        &mut self,
        brane_id: u64,
        initial_status: u64,
        max_ts_sync_status: Arc<AtomicU64>,
    ) {
        let fidel_client = self.fidel_client.clone();
        let interlocking_directorate = self.interlocking_directorate.clone();
        let f = async move {
            let mut success = false;
            while max_ts_sync_status.load(Ordering::SeqCst) == initial_status {
                match fidel_client.get_tso().await {
                    Ok(ts) => {
                        interlocking_directorate.fidelio_max_ts(ts);
                        // Set the least significant bit to 1 to mark it as synced.
                        let old_value = max_ts_sync_status.compare_and_swap(
                            initial_status,
                            initial_status | 1,
                            Ordering::SeqCst,
                        );
                        success = old_value == initial_status;
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "failed to fidelio max timestamp for brane {}: {:?}",
                            brane_id, e
                        );
                    }
                }
            }
            if success {
                info!("succeed to fidelio max timestamp"; "brane_id" => brane_id);
            } else {
                info!(
                    "ufidelating max timestamp is stale";
                    "brane_id" => brane_id,
                    "initial_status" => initial_status,
                );
            }
        };
        spawn_local(f);
    }
}

impl<EK, ER, T> Runnable<Task<EK>> for Runner<EK, ER, T>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
    T: FidelClient,
{
    fn run(&mut self, task: Task<EK>) {
        debug!("executing task"; "task" => %task);

        if !self.is_hb_receiver_scheduled {
            self.schedule_heartbeat_receiver();
        }

        match task {
            // AskSplit has deprecated, use AskBatchSplit
            Task::AskSplit {
                brane,
                split_key,
                peer,
                right_derive,
                callback,
            } => self.handle_ask_split(
                brane,
                split_key,
                peer,
                right_derive,
                callback,
                String::from("ask_split"),
            ),
            Task::AskBatchSplit {
                brane,
                split_tuplespaceInstanton,
                peer,
                right_derive,
                callback,
            } => Self::handle_ask_batch_split(
                self.router.clone(),
                self.interlock_semaphore.clone(),
                self.fidel_client.clone(),
                brane,
                split_tuplespaceInstanton,
                peer,
                right_derive,
                callback,
                String::from("batch_split"),
            ),
            Task::AutoSplit { split_infos } => {
                let fidel_client = self.fidel_client.clone();
                let router = self.router.clone();
                let interlock_semaphore = self.interlock_semaphore.clone();

                let f = async move {
                    for split_info in split_infos {
                        if let Ok(Some(brane)) =
                            fidel_client.get_brane_by_id(split_info.brane_id).await
                        {
                            Self::handle_ask_batch_split(
                                router.clone(),
                                interlock_semaphore.clone(),
                                fidel_client.clone(),
                                brane,
                                vec![split_info.split_key],
                                split_info.peer,
                                true,
                                Callback::None,
                                String::from("auto_split"),
                            );
                        }
                    }
                };
                spawn_local(f);
            }

            Task::Heartbeat {
                term,
                brane,
                peer,
                down_peers,
                plightlikeing_peers,
                written_bytes,
                written_tuplespaceInstanton,
                approximate_size,
                approximate_tuplespaceInstanton,
                replication_status,
            } => {
                let approximate_size = approximate_size.unwrap_or_else(|| {
                    get_brane_approximate_size(&self.db, &brane, 0).unwrap_or_default()
                });
                let approximate_tuplespaceInstanton = approximate_tuplespaceInstanton.unwrap_or_else(|| {
                    get_brane_approximate_tuplespaceInstanton(&self.db, &brane, 0).unwrap_or_default()
                });
                let (
                    read_bytes_delta,
                    read_tuplespaceInstanton_delta,
                    written_bytes_delta,
                    written_tuplespaceInstanton_delta,
                    last_report_ts,
                ) = {
                    let peer_stat = self
                        .brane_peers
                        .entry(brane.get_id())
                        .or_insert_with(PeerStat::default);
                    let read_bytes_delta = peer_stat.read_bytes - peer_stat.last_read_bytes;
                    let read_tuplespaceInstanton_delta = peer_stat.read_tuplespaceInstanton - peer_stat.last_read_tuplespaceInstanton;
                    let written_bytes_delta = written_bytes - peer_stat.last_written_bytes;
                    let written_tuplespaceInstanton_delta = written_tuplespaceInstanton - peer_stat.last_written_tuplespaceInstanton;
                    let mut last_report_ts = peer_stat.last_report_ts;
                    peer_stat.last_written_bytes = written_bytes;
                    peer_stat.last_written_tuplespaceInstanton = written_tuplespaceInstanton;
                    peer_stat.last_read_bytes = peer_stat.read_bytes;
                    peer_stat.last_read_tuplespaceInstanton = peer_stat.read_tuplespaceInstanton;
                    peer_stat.last_report_ts = UnixSecs::now();
                    if last_report_ts.is_zero() {
                        last_report_ts = self.spacelike_ts;
                    }
                    (
                        read_bytes_delta,
                        read_tuplespaceInstanton_delta,
                        written_bytes_delta,
                        written_tuplespaceInstanton_delta,
                        last_report_ts,
                    )
                };
                self.handle_heartbeat(
                    term,
                    brane,
                    peer,
                    BraneStat {
                        down_peers,
                        plightlikeing_peers,
                        written_bytes: written_bytes_delta,
                        written_tuplespaceInstanton: written_tuplespaceInstanton_delta,
                        read_bytes: read_bytes_delta,
                        read_tuplespaceInstanton: read_tuplespaceInstanton_delta,
                        approximate_size,
                        approximate_tuplespaceInstanton,
                        last_report_ts,
                    },
                    replication_status,
                )
            }
            Task::StoreHeartbeat { stats, store_info } => {
                self.handle_store_heartbeat(stats, store_info)
            }
            Task::ReportBatchSplit { branes } => self.handle_report_batch_split(branes),
            Task::ValidatePeer { brane, peer } => self.handle_validate_peer(brane, peer),
            Task::ReadStats { read_stats } => self.handle_read_stats(read_stats),
            Task::DestroyPeer { brane_id } => self.handle_destroy_peer(brane_id),
            Task::StoreInfos {
                cpu_usages,
                read_io_rates,
                write_io_rates,
            } => self.handle_store_infos(cpu_usages, read_io_rates, write_io_rates),
            Task::fidelioMaxTimestamp {
                brane_id,
                initial_status,
                max_ts_sync_status,
            } => self.handle_fidelio_max_timestamp(brane_id, initial_status, max_ts_sync_status),
        };
    }

    fn shutdown(&mut self) {
        self.stats_monitor.stop();
    }
}

fn new_change_peer_request(change_type: ConfChangeType, peer: meta_timeshare::Peer) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::ChangePeer);
    req.mut_change_peer().set_change_type(change_type);
    req.mut_change_peer().set_peer(peer);
    req
}

fn new_split_brane_request(
    split_key: Vec<u8>,
    new_brane_id: u64,
    peer_ids: Vec<u64>,
    right_derive: bool,
) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::Split);
    req.mut_split().set_split_key(split_key);
    req.mut_split().set_new_brane_id(new_brane_id);
    req.mut_split().set_new_peer_ids(peer_ids);
    req.mut_split().set_right_derive(right_derive);
    req
}

fn new_batch_split_brane_request(
    split_tuplespaceInstanton: Vec<Vec<u8>>,
    ids: Vec<fidel_timeshare::SplitId>,
    right_derive: bool,
) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::BatchSplit);
    req.mut_splits().set_right_derive(right_derive);
    let mut requests = Vec::with_capacity(ids.len());
    for (mut id, key) in ids.into_iter().zip(split_tuplespaceInstanton) {
        let mut split = SplitRequest::default();
        split.set_split_key(key);
        split.set_new_brane_id(id.get_new_brane_id());
        split.set_new_peer_ids(id.take_new_peer_ids());
        requests.push(split);
    }
    req.mut_splits().set_requests(requests.into());
    req
}

fn new_transfer_leader_request(peer: meta_timeshare::Peer) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::TransferLeader);
    req.mut_transfer_leader().set_peer(peer);
    req
}

fn new_merge_request(merge: fidel_timeshare::Merge) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::PrepareMerge);
    req.mut_prepare_merge()
        .set_target(merge.get_target().to_owned());
    req
}

fn lightlike_admin_request<EK, ER>(
    router: &VioletaBftRouter<EK, ER>,
    brane_id: u64,
    epoch: meta_timeshare::BraneEpoch,
    peer: meta_timeshare::Peer,
    request: AdminRequest,
    callback: Callback<EK::Snapshot>,
) where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    let cmd_type = request.get_cmd_type();

    let mut req = VioletaBftCmdRequest::default();
    req.mut_header().set_brane_id(brane_id);
    req.mut_header().set_brane_epoch(epoch);
    req.mut_header().set_peer(peer);

    req.set_admin_request(request);

    if let Err(e) = router.lightlike_violetabft_command(VioletaBftCommand::new(req, callback)) {
        error!(
            "lightlike request failed";
            "brane_id" => brane_id, "cmd_type" => ?cmd_type, "err" => ?e,
        );
    }
}

/// lightlikes a violetabft message to destroy the specified stale Peer
fn lightlike_destroy_peer_message<EK, ER>(
    router: &VioletaBftRouter<EK, ER>,
    local_brane: meta_timeshare::Brane,
    peer: meta_timeshare::Peer,
    fidel_brane: meta_timeshare::Brane,
) where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    let mut message = VioletaBftMessage::default();
    message.set_brane_id(local_brane.get_id());
    message.set_from_peer(peer.clone());
    message.set_to_peer(peer);
    message.set_brane_epoch(fidel_brane.get_brane_epoch().clone());
    message.set_is_tombstone(true);
    if let Err(e) = router.lightlike_violetabft_message(message) {
        error!(
            "lightlike gc peer request failed";
            "brane_id" => local_brane.get_id(),
            "err" => ?e
        )
    }
}

#[causet(not(target_os = "macos"))]
#[causet(test)]
mod tests {
    use engine_lmdb::LmdbEngine;
    use std::sync::Mutex;
    use std::time::Instant;
    use violetabftstore::interlock::::worker::FutureWorker;

    use super::*;

    struct RunnerTest {
        store_stat: Arc<Mutex<StoreStat>>,
        stats_monitor: StatsMonitor<LmdbEngine>,
    }

    impl RunnerTest {
        fn new(
            interval: u64,
            interlock_semaphore: Interlock_Semaphore<Task<LmdbEngine>>,
            store_stat: Arc<Mutex<StoreStat>>,
        ) -> RunnerTest {
            let mut stats_monitor = StatsMonitor::new(Duration::from_secs(interval), interlock_semaphore);

            if let Err(e) = stats_monitor.spacelike(AutoSplitController::default()) {
                error!("failed to spacelike stats collector, error = {:?}", e);
            }

            RunnerTest {
                store_stat,
                stats_monitor,
            }
        }

        fn handle_store_infos(
            &mut self,
            cpu_usages: RecordPairVec,
            read_io_rates: RecordPairVec,
            write_io_rates: RecordPairVec,
        ) {
            let mut store_stat = self.store_stat.dagger().unwrap();
            store_stat.store_cpu_usages = cpu_usages;
            store_stat.store_read_io_rates = read_io_rates;
            store_stat.store_write_io_rates = write_io_rates;
        }
    }

    impl Runnable<Task<LmdbEngine>> for RunnerTest {
        fn run(&mut self, task: Task<LmdbEngine>) {
            if let Task::StoreInfos {
                cpu_usages,
                read_io_rates,
                write_io_rates,
            } = task
            {
                self.handle_store_infos(cpu_usages, read_io_rates, write_io_rates)
            };
        }

        fn shutdown(&mut self) {
            self.stats_monitor.stop();
        }
    }

    fn sum_record_pairs(pairs: &[fidel_timeshare::RecordPair]) -> u64 {
        let mut sum = 0;
        for record in pairs.iter() {
            sum += record.get_value();
        }
        sum
    }

    #[test]
    fn test_collect_stats() {
        let mut fidel_worker = FutureWorker::new("test-fidel-worker");
        let store_stat = Arc::new(Mutex::new(StoreStat::default()));
        let runner = RunnerTest::new(1, fidel_worker.interlock_semaphore(), Arc::clone(&store_stat));
        fidel_worker.spacelike(runner).unwrap();

        let spacelike = Instant::now();
        loop {
            if (Instant::now() - spacelike).as_secs() > 2 {
                break;
            }
        }

        let total_cpu_usages = sum_record_pairs(&store_stat.dagger().unwrap().store_cpu_usages);
        assert!(total_cpu_usages > 90);

        fidel_worker.stop();
    }
}
