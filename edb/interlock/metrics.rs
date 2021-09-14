// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;

use crate::causet_storage::{FlowStatsReporter, Statistics};
use ekvproto::meta_timeshare;
use violetabftstore::store::util::build_key_cone;
use violetabftstore::store::ReadStats;
use violetabftstore::interlock::::collections::HashMap;

use crate::server::metrics::{GcTuplespaceInstantonCauset, GcTuplespaceInstantonDetail};
use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum ReqTag {
        select,
        index,
        analyze_Block,
        analyze_index,
        checksum_Block,
        checksum_index,
        test,
    }

    pub label_enum Causet {
        default,
        dagger,
        write,
    }

    pub label_enum ScanTuplespaceInstantonKind {
        processed_tuplespaceInstanton,
        total,
    }

    pub label_enum ScanKind {
        processed_tuplespaceInstanton,
        get,
        next,
        prev,
        seek,
        seek_for_prev,
        over_seek_bound,
        next_tombstone,
        prev_tombstone,
        seek_tombstone,
        seek_for_prev_tombstone,
    }

    pub label_enum WaitType {
        all,
        schedule,
        snapshot,
    }

    pub label_enum PerfMetric {
        internal_key_skipped_count,
        internal_delete_skipped_count,
        block_cache_hit_count,
        block_read_count,
        block_read_byte,
        encrypt_data_nanos,
        decrypt_data_nanos,
    }

    pub struct CoprReqHistogram: LocalHistogram {
        "req" => ReqTag,
    }

    pub struct ReqWaitHistogram: LocalHistogram {
        "req" => ReqTag,
        "type" => WaitType,
    }

    pub struct PerfCounter: LocalIntCounter {
        "req" => ReqTag,
        "metric" => PerfMetric,
    }

    pub struct CoprScanTuplespaceInstantonHistogram: LocalHistogram {
        "req" => ReqTag,
        "kind" => ScanTuplespaceInstantonKind,
    }

    pub struct CoprScanDetails : LocalIntCounter {
        "req" => ReqTag,
        "causet" => Causet,
        "tag" => ScanKind,
    }
}

lazy_static! {
    pub static ref COPR_REQ_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "edb_interlock_request_duration_seconds",
        "Bucketed histogram of interlock request duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_HISTOGRAM_STATIC: CoprReqHistogram =
        auto_flush_from!(COPR_REQ_HISTOGRAM_VEC, CoprReqHistogram);
    pub static ref COPR_REQ_HANDLE_TIME: HistogramVec = register_histogram_vec!(
        "edb_interlock_request_handle_seconds",
        "Bucketed histogram of interlock handle request duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_HANDLE_TIME_STATIC: CoprReqHistogram =
        auto_flush_from!(COPR_REQ_HANDLE_TIME, CoprReqHistogram);
    pub static ref COPR_REQ_WAIT_TIME: HistogramVec = register_histogram_vec!(
        "edb_interlock_request_wait_seconds",
        "Bucketed histogram of interlock request wait duration",
        &["req", "type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_WAIT_TIME_STATIC: ReqWaitHistogram =
        auto_flush_from!(COPR_REQ_WAIT_TIME, ReqWaitHistogram);
    pub static ref COPR_REQ_HANDLER_BUILD_TIME: HistogramVec = register_histogram_vec!(
        "edb_interlock_request_handler_build_seconds",
        "Bucketed histogram of interlock request handler build duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_HANDLER_BUILD_TIME_STATIC: CoprReqHistogram =
        auto_flush_from!(COPR_REQ_HANDLER_BUILD_TIME, CoprReqHistogram);
    pub static ref COPR_REQ_ERROR: IntCounterVec = register_int_counter_vec!(
        "edb_interlock_request_error",
        "Total number of push down request error.",
        &["reason"]
    )
    .unwrap();
    pub static ref COPR_SCAN_KEYS: HistogramVec = register_histogram_vec!(
        "edb_interlock_scan_tuplespaceInstanton",
        "Bucketed histogram of interlock per request scan tuplespaceInstanton",
        &["req", "kind"],
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_SCAN_KEYS_STATIC: CoprScanTuplespaceInstantonHistogram =
        auto_flush_from!(COPR_SCAN_KEYS, CoprScanTuplespaceInstantonHistogram);
    pub static ref COPR_SCAN_DETAILS: IntCounterVec = register_int_counter_vec!(
        "edb_interlock_scan_details",
        "Bucketed counter of interlock scan details for each Causet",
        &["req", "causet", "tag"]
    )
    .unwrap();
    pub static ref COPR_SCAN_DETAILS_STATIC: CoprScanDetails =
        auto_flush_from!(COPR_SCAN_DETAILS, CoprScanDetails);
    pub static ref COPR_LMDB_PERF_COUNTER: IntCounterVec = register_int_counter_vec!(
        "edb_interlock_lmdb_perf",
        "Total number of Lmdb internal operations from PerfContext",
        &["req", "metric"]
    )
    .unwrap();
    pub static ref COPR_LMDB_PERF_COUNTER_STATIC: PerfCounter =
        auto_flush_from!(COPR_LMDB_PERF_COUNTER, PerfCounter);
    pub static ref COPR_DAG_REQ_COUNT: IntCounterVec = register_int_counter_vec!(
        "edb_interlock_dag_request_count",
        "Total number of DAG requests",
        &["vec_type"]
    )
    .unwrap();
    pub static ref COPR_RESP_SIZE: IntCounter = register_int_counter!(
        "edb_interlock_response_bytes",
        "Total bytes of response body"
    )
    .unwrap();
    pub static ref COPR_ACQUIRE_SEMAPHORE_TYPE: CoprAcquireSemaphoreTypeCounterVec =
        register_static_int_counter_vec!(
            CoprAcquireSemaphoreTypeCounterVec,
            "edb_interlock_acquire_semaphore_type",
            "The acquire type of the interlock semaphore",
            &["type"],
        )
        .unwrap();
    pub static ref COPR_WAITING_FOR_SEMAPHORE: IntGauge = register_int_gauge!(
        "edb_interlock_waiting_for_semaphore",
        "The number of tasks waiting for the semaphore"
    )
    .unwrap();
}

make_static_metric! {
    pub label_enum AcquireSemaphoreType {
        unacquired,
        acquired,
    }

    pub struct CoprAcquireSemaphoreTypeCounterVec: IntCounter {
        "type" => AcquireSemaphoreType,
    }
}

pub struct CopLocalMetrics {
    local_scan_details: HashMap<ReqTag, Statistics>,
    local_read_stats: ReadStats,
}

thread_local! {
    pub static TLS_COP_METRICS: RefCell<CopLocalMetrics> = RefCell::new(
        CopLocalMetrics {
            local_scan_details:
                HashMap::default(),
            local_read_stats:
                ReadStats::default(),
        }
    );
}

impl Into<Causet> for GcTuplespaceInstantonCauset {
    fn into(self) -> Causet {
        match self {
            GcTuplespaceInstantonCauset::default => Causet::default,
            GcTuplespaceInstantonCauset::dagger => Causet::dagger,
            GcTuplespaceInstantonCauset::write => Causet::write,
        }
    }
}

impl Into<ScanKind> for GcTuplespaceInstantonDetail {
    fn into(self) -> ScanKind {
        match self {
            GcTuplespaceInstantonDetail::processed_tuplespaceInstanton => ScanKind::processed_tuplespaceInstanton,
            GcTuplespaceInstantonDetail::get => ScanKind::get,
            GcTuplespaceInstantonDetail::next => ScanKind::next,
            GcTuplespaceInstantonDetail::prev => ScanKind::prev,
            GcTuplespaceInstantonDetail::seek => ScanKind::seek,
            GcTuplespaceInstantonDetail::seek_for_prev => ScanKind::seek_for_prev,
            GcTuplespaceInstantonDetail::over_seek_bound => ScanKind::over_seek_bound,
            GcTuplespaceInstantonDetail::next_tombstone => ScanKind::next_tombstone,
            GcTuplespaceInstantonDetail::prev_tombstone => ScanKind::prev_tombstone,
            GcTuplespaceInstantonDetail::seek_tombstone => ScanKind::seek_tombstone,
            GcTuplespaceInstantonDetail::seek_for_prev_tombstone => ScanKind::seek_for_prev_tombstone,
        }
    }
}

pub fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_COP_METRICS.with(|m| {
        // Flush Prometheus metrics
        let mut m = m.borrow_mut();

        for (req_tag, stat) in m.local_scan_details.drain() {
            for (causet, causet_details) in stat.details_enum().iter() {
                for (tag, count) in causet_details.iter() {
                    COPR_SCAN_DETAILS_STATIC
                        .get(req_tag)
                        .get((*causet).into())
                        .get((*tag).into())
                        .inc_by(*count as i64);
                }
            }
        }

        // Report FIDel metrics
        if !m.local_read_stats.is_empty() {
            let mut read_stats = ReadStats::default();
            mem::swap(&mut read_stats, &mut m.local_read_stats);
            reporter.report_read_stats(read_stats);
        }
    });
}

pub fn tls_collect_scan_details(cmd: ReqTag, stats: &Statistics) {
    TLS_COP_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_collect_read_flow(brane_id: u64, statistics: &Statistics) {
    TLS_COP_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.local_read_stats.add_flow(
            brane_id,
            &statistics.write.flow_stats,
            &statistics.data.flow_stats,
        );
    });
}

pub fn tls_collect_qps(
    brane_id: u64,
    peer: &meta_timeshare::Peer,
    spacelike_key: &[u8],
    lightlike_key: &[u8],
    reverse_scan: bool,
) {
    TLS_COP_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        let key_cone = build_key_cone(spacelike_key, lightlike_key, reverse_scan);
        m.local_read_stats.add_qps(brane_id, peer, key_cone);
    });
}
