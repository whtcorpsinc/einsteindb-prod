// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

use std::cell::RefCell;
use std::mem;

use crate::server::metrics::{GcTuplespaceInstantonCAUSET as ServerGcTuplespaceInstantonCAUSET, GcTuplespaceInstantonDetail as ServerGcTuplespaceInstantonDetail};
use crate::causetStorage::kv::{FlowStatsReporter, Statistics};
use ekvproto::kvrpcpb::KeyCone;
use ekvproto::metapb;
use violetabftstore::store::util::build_key_cone;
use violetabftstore::store::ReadStats;
use einsteindb_util::collections::HashMap;

struct StorageLocalMetrics {
    local_scan_details: HashMap<CommandKind, Statistics>,
    local_read_stats: ReadStats,
}

thread_local! {
    static TLS_STORAGE_METRICS: RefCell<StorageLocalMetrics> = RefCell::new(
        StorageLocalMetrics {
            local_scan_details: HashMap::default(),
            local_read_stats:ReadStats::default(),
        }
    );
}

pub fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();

        for (cmd, stat) in m.local_scan_details.drain() {
            for (causet, causet_details) in stat.details_enum().iter() {
                for (tag, count) in causet_details.iter() {
                    KV_COMMAND_SCAN_DETAILS_STATIC
                        .get(cmd)
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

pub fn tls_collect_scan_details(cmd: CommandKind, stats: &Statistics) {
    TLS_STORAGE_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_collect_read_flow(brane_id: u64, statistics: &Statistics) {
    TLS_STORAGE_METRICS.with(|m| {
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
    peer: &metapb::Peer,
    spacelike_key: &[u8],
    lightlike_key: &[u8],
    reverse_scan: bool,
) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        let key_cone = build_key_cone(spacelike_key, lightlike_key, reverse_scan);
        m.local_read_stats.add_qps(brane_id, peer, key_cone);
    });
}

pub fn tls_collect_qps_batch(brane_id: u64, peer: &metapb::Peer, key_cones: Vec<KeyCone>) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.local_read_stats
            .add_qps_batch(brane_id, peer, key_cones);
    });
}

make_auto_flush_static_metric! {
    pub label_enum CommandKind {
        get,
        raw_batch_get_command,
        scan,
        batch_get,
        batch_get_command,
        prewrite,
        acquire_pessimistic_lock,
        commit,
        cleanup,
        rollback,
        pessimistic_rollback,
        txn_heart_beat,
        check_txn_status,
        check_secondary_locks,
        scan_lock,
        resolve_lock,
        resolve_lock_lite,
        delete_cone,
        pause,
        key_tail_pointer,
        spacelike_ts_tail_pointer,
        raw_get,
        raw_batch_get,
        raw_scan,
        raw_batch_scan,
        raw_put,
        raw_batch_put,
        raw_delete,
        raw_delete_cone,
        raw_batch_delete,
    }

    pub label_enum CommandStageKind {
        new,
        snapshot,
        async_snapshot_err,
        snapshot_ok,
        snapshot_err,
        read_finish,
        next_cmd,
        lock_wait,
        process,
        prepare_write_err,
        write,
        write_finish,
        async_write_err,
        error,
        pipelined_write,
        pipelined_write_finish,
    }

    pub label_enum CommandPriority {
        low,
        normal,
        high,
    }

    pub label_enum GcTuplespaceInstantonCAUSET {
        default,
        dagger,
        write,
    }

    pub label_enum GcTuplespaceInstantonDetail {
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

    pub struct CommandScanDetails: LocalIntCounter {
        "req" => CommandKind,
        "causet" => GcTuplespaceInstantonCAUSET,
        "tag" => GcTuplespaceInstantonDetail,
    }

    pub struct SchedDurationVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct ProcessingReadVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct KReadVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct KvCommandCounterVec: LocalIntCounter {
        "type" => CommandKind,
    }

    pub struct SchedStageCounterVec: LocalIntCounter {
        "type" => CommandKind,
        "stage" => CommandStageKind,
    }

    pub struct SchedLatchDurationVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct KvCommandTuplespaceInstantonWrittenVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct SchedTooBusyVec: LocalIntCounter {
        "type" => CommandKind,
    }

    pub struct SchedCommandPriCounterVec: LocalIntCounter {
        "priority" => CommandPriority,
    }
}

impl Into<GcTuplespaceInstantonCAUSET> for ServerGcTuplespaceInstantonCAUSET {
    fn into(self) -> GcTuplespaceInstantonCAUSET {
        match self {
            ServerGcTuplespaceInstantonCAUSET::default => GcTuplespaceInstantonCAUSET::default,
            ServerGcTuplespaceInstantonCAUSET::dagger => GcTuplespaceInstantonCAUSET::dagger,
            ServerGcTuplespaceInstantonCAUSET::write => GcTuplespaceInstantonCAUSET::write,
        }
    }
}

impl Into<GcTuplespaceInstantonDetail> for ServerGcTuplespaceInstantonDetail {
    fn into(self) -> GcTuplespaceInstantonDetail {
        match self {
            ServerGcTuplespaceInstantonDetail::processed_tuplespaceInstanton => GcTuplespaceInstantonDetail::processed_tuplespaceInstanton,
            ServerGcTuplespaceInstantonDetail::get => GcTuplespaceInstantonDetail::get,
            ServerGcTuplespaceInstantonDetail::next => GcTuplespaceInstantonDetail::next,
            ServerGcTuplespaceInstantonDetail::prev => GcTuplespaceInstantonDetail::prev,
            ServerGcTuplespaceInstantonDetail::seek => GcTuplespaceInstantonDetail::seek,
            ServerGcTuplespaceInstantonDetail::seek_for_prev => GcTuplespaceInstantonDetail::seek_for_prev,
            ServerGcTuplespaceInstantonDetail::over_seek_bound => GcTuplespaceInstantonDetail::over_seek_bound,
            ServerGcTuplespaceInstantonDetail::next_tombstone => GcTuplespaceInstantonDetail::next_tombstone,
            ServerGcTuplespaceInstantonDetail::prev_tombstone => GcTuplespaceInstantonDetail::prev_tombstone,
            ServerGcTuplespaceInstantonDetail::seek_tombstone => GcTuplespaceInstantonDetail::seek_tombstone,
            ServerGcTuplespaceInstantonDetail::seek_for_prev_tombstone => GcTuplespaceInstantonDetail::seek_for_prev_tombstone,
        }
    }
}

lazy_static! {
    pub static ref KV_COMMAND_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_causetStorage_command_total",
        "Total number of commands received.",
        &["type"]
    )
    .unwrap();
    pub static ref KV_COMMAND_COUNTER_VEC_STATIC: KvCommandCounterVec =
        auto_flush_from!(KV_COMMAND_COUNTER_VEC, KvCommandCounterVec);
    pub static ref SCHED_STAGE_COUNTER: IntCounterVec = {
        register_int_counter_vec!(
            "einsteindb_scheduler_stage_total",
            "Total number of commands on each stage.",
            &["type", "stage"]
        )
        .unwrap()
    };
    pub static ref SCHED_STAGE_COUNTER_VEC: SchedStageCounterVec =
        auto_flush_from!(SCHED_STAGE_COUNTER, SchedStageCounterVec);
    pub static ref SCHED_WRITING_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "einsteindb_scheduler_writing_bytes",
        "Total number of writing kv."
    )
    .unwrap();
    pub static ref SCHED_CONTEX_GAUGE: IntGauge = register_int_gauge!(
        "einsteindb_scheduler_contex_total",
        "Total number of plightlikeing commands."
    )
    .unwrap();
    pub static ref SCHED_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einsteindb_scheduler_command_duration_seconds",
        "Bucketed histogram of command execution",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_HISTOGRAM_VEC_STATIC: SchedDurationVec =
        auto_flush_from!(SCHED_HISTOGRAM_VEC, SchedDurationVec);
    pub static ref SCHED_LATCH_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "einsteindb_scheduler_latch_wait_duration_seconds",
        "Bucketed histogram of latch wait",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_LATCH_HISTOGRAM_VEC: SchedLatchDurationVec =
        auto_flush_from!(SCHED_LATCH_HISTOGRAM, SchedLatchDurationVec);
    pub static ref SCHED_PROCESSING_READ_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einsteindb_scheduler_processing_read_duration_seconds",
        "Bucketed histogram of processing read duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_PROCESSING_READ_HISTOGRAM_STATIC: ProcessingReadVec =
        auto_flush_from!(SCHED_PROCESSING_READ_HISTOGRAM_VEC, ProcessingReadVec);
    pub static ref SCHED_PROCESSING_WRITE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einsteindb_scheduler_processing_write_duration_seconds",
        "Bucketed histogram of processing write duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_TOO_BUSY_COUNTER: IntCounterVec = register_int_counter_vec!(
        "einsteindb_scheduler_too_busy_total",
        "Total count of scheduler too busy",
        &["type"]
    )
    .unwrap();
    pub static ref SCHED_TOO_BUSY_COUNTER_VEC: SchedTooBusyVec =
        auto_flush_from!(SCHED_TOO_BUSY_COUNTER, SchedTooBusyVec);
    pub static ref SCHED_COMMANDS_PRI_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_scheduler_commands_pri_total",
        "Total count of different priority commands",
        &["priority"]
    )
    .unwrap();
    pub static ref SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC: SchedCommandPriCounterVec =
        auto_flush_from!(SCHED_COMMANDS_PRI_COUNTER_VEC, SchedCommandPriCounterVec);
    pub static ref KV_COMMAND_KEYREAD_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einsteindb_scheduler_kv_command_key_read",
        "Bucketed histogram of tuplespaceInstanton read of a kv command",
        &["type"],
        exponential_buckets(1.0, 2.0, 21).unwrap()
    )
    .unwrap();
    pub static ref KV_COMMAND_KEYREAD_HISTOGRAM_STATIC: KReadVec =
        auto_flush_from!(KV_COMMAND_KEYREAD_HISTOGRAM_VEC, KReadVec);
    pub static ref KV_COMMAND_SCAN_DETAILS: IntCounterVec = register_int_counter_vec!(
        "einsteindb_scheduler_kv_scan_details",
        "Bucketed counter of kv tuplespaceInstanton scan details for each causet",
        &["req", "causet", "tag"]
    )
    .unwrap();
    pub static ref KV_COMMAND_SCAN_DETAILS_STATIC: CommandScanDetails =
        auto_flush_from!(KV_COMMAND_SCAN_DETAILS, CommandScanDetails);
    pub static ref KV_COMMAND_KEYWRITE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "einsteindb_scheduler_kv_command_key_write",
        "Bucketed histogram of tuplespaceInstanton write of a kv command",
        &["type"],
        exponential_buckets(1.0, 2.0, 21).unwrap()
    )
    .unwrap();
    pub static ref KV_COMMAND_KEYWRITE_HISTOGRAM_VEC: KvCommandTuplespaceInstantonWrittenVec =
        auto_flush_from!(KV_COMMAND_KEYWRITE_HISTOGRAM, KvCommandTuplespaceInstantonWrittenVec);
    pub static ref REQUEST_EXCEED_BOUND: IntCounter = register_int_counter!(
        "einsteindb_request_exceed_bound",
        "Counter of request exceed bound"
    )
    .unwrap();
}
