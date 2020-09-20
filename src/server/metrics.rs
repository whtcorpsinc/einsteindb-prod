// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

use crate::persistence::ErrorHeaderKind;
use prometheus::exponential_buckets;

make_auto_flush_static_metric! {
    pub label_enum GrpcTypeKind {
        invalid,
        kv_get,
        kv_scan,
        kv_prewrite,
        kv_pessimistic_lock,
        kv_pessimistic_rollback,
        kv_commit,
        kv_cleanup,
        kv_batch_get,
        kv_batch_get_command,
        kv_batch_rollback,
        kv_txn_heart_beat,
        kv_check_txn_status,
        kv_check_secondary_locks,
        kv_scan_lock,
        kv_resolve_lock,
        kv_gc,
        kv_delete_cone,
        raw_get,
        raw_batch_get,
        raw_batch_get_command,
        raw_scan,
        raw_batch_scan,
        raw_put,
        raw_batch_put,
        raw_delete,
        raw_delete_cone,
        raw_batch_delete,
        ver_get,
        ver_batch_get,
        ver_mut,
        ver_batch_mut,
        ver_scan,
        ver_delete_cone,
        unsafe_destroy_cone,
        physical_scan_lock,
        register_lock_observer,
        check_lock_observer,
        remove_lock_observer,
        interlock,
        interlock_stream,
        mvcc_get_by_key,
        mvcc_get_by_spacelike_ts,
        split_brane,
        read_index,
    }

    pub label_enum GcCommandKind {
        gc,
        unsafe_destroy_cone,
        physical_scan_lock,
        validate_config,
    }

    pub label_enum SnapTask {
        slightlike,
        recv,
    }

    pub label_enum ResolveStore {
        resolving,
        resolve,
        failed,
        success,
        tombstone,
    }

    pub label_enum GcTuplespaceInstantonCAUSET {
        default,
        lock,
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

    pub struct GcCommandCounterVec: LocalIntCounter {
        "type" => GcCommandKind,
    }

    pub struct SnapTaskCounterVec: LocalIntCounter {
        "type" => SnapTask,
    }

    pub struct GcTaskCounterVec: LocalIntCounter {
        "task" => GcCommandKind,
    }

    pub struct GcTaskFailCounterVec: LocalIntCounter {
        "task" => GcCommandKind,
    }

    pub struct ResolveStoreCounterVec: LocalIntCounter {
        "type" => ResolveStore
    }

    pub struct GrpcMsgFailCounterVec: LocalIntCounter {
        "type" => GrpcTypeKind,
    }

    pub struct GcTuplespaceInstantonCounterVec: LocalIntCounter {
        "causet" => GcTuplespaceInstantonCAUSET,
        "tag" => GcTuplespaceInstantonDetail,
    }

    pub struct GrpcMsgHistogramVec: LocalHistogram {
        "type" => GrpcTypeKind,
    }
}

make_static_metric! {
    pub label_enum GlobalGrpcTypeKind {
        kv_get,
    }

    pub label_enum BatchableRequestKind {
        point_get,
        prewrite,
        commit,
    }

    pub struct GrpcMsgHistogramGlobal: Histogram {
        "type" => GlobalGrpcTypeKind,
    }

    pub struct RequestBatchSizeHistogramVec: Histogram {
        "type" => BatchableRequestKind,
    }

    pub struct RequestBatchRatioHistogramVec: Histogram {
        "type" => BatchableRequestKind,
    }
}

lazy_static! {
    pub static ref GC_COMMAND_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "gc_command_total",
        "Total number of GC commands received.",
        &["type"]
    )
    .unwrap();
    pub static ref SNAP_TASK_COUNTER: IntCounterVec = register_int_counter_vec!(
        "einsteindb_server_snapshot_task_total",
        "Total number of snapshot task",
        &["type"]
    )
    .unwrap();
    pub static ref GC_GCTASK_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_gcworker_gc_tasks_vec",
        "Counter of gc tasks processed by gc_worker",
        &["task"]
    )
    .unwrap();
    pub static ref GC_GCTASK_FAIL_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_gcworker_gc_task_fail_vec",
        "Counter of gc tasks that is failed",
        &["task"]
    )
    .unwrap();
    pub static ref RESOLVE_STORE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "einsteindb_server_resolve_store_total",
        "Total number of resolving store",
        &["type"]
    )
    .unwrap();
    pub static ref GRPC_MSG_FAIL_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_grpc_msg_fail_total",
        "Total number of handle grpc message failure",
        &["type"]
    )
    .unwrap();
    pub static ref GC_KEYS_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_gcworker_gc_tuplespaceInstanton",
        "Counter of tuplespaceInstanton affected during gc",
        &["causet", "tag"]
    )
    .unwrap();
    pub static ref GRPC_MSG_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einsteindb_grpc_msg_duration_seconds",
        "Bucketed histogram of grpc server messages",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
}

lazy_static! {
    pub static ref GRPC_MSG_HISTOGRAM_STATIC: GrpcMsgHistogramVec =
        auto_flush_from!(GRPC_MSG_HISTOGRAM_VEC, GrpcMsgHistogramVec);
    pub static ref GRPC_MSG_HISTOGRAM_GLOBAL: GrpcMsgHistogramGlobal =
        GrpcMsgHistogramGlobal::from(&GRPC_MSG_HISTOGRAM_VEC);
    pub static ref GC_COMMAND_COUNTER_VEC_STATIC: GcCommandCounterVec =
        auto_flush_from!(GC_COMMAND_COUNTER_VEC, GcCommandCounterVec);
    pub static ref SNAP_TASK_COUNTER_STATIC: SnapTaskCounterVec =
        auto_flush_from!(SNAP_TASK_COUNTER, SnapTaskCounterVec);
    pub static ref GC_GCTASK_COUNTER_STATIC: GcTaskCounterVec =
        auto_flush_from!(GC_GCTASK_COUNTER_VEC, GcTaskCounterVec);
    pub static ref GC_GCTASK_FAIL_COUNTER_STATIC: GcTaskFailCounterVec =
        auto_flush_from!(GC_GCTASK_FAIL_COUNTER_VEC, GcTaskFailCounterVec);
    pub static ref RESOLVE_STORE_COUNTER_STATIC: ResolveStoreCounterVec =
        auto_flush_from!(RESOLVE_STORE_COUNTER, ResolveStoreCounterVec);
    pub static ref GRPC_MSG_FAIL_COUNTER: GrpcMsgFailCounterVec =
        auto_flush_from!(GRPC_MSG_FAIL_COUNTER_VEC, GrpcMsgFailCounterVec);
    pub static ref GC_KEYS_COUNTER_STATIC: GcTuplespaceInstantonCounterVec =
        auto_flush_from!(GC_KEYS_COUNTER_VEC, GcTuplespaceInstantonCounterVec);
}

lazy_static! {
    pub static ref SEND_SNAP_HISTOGRAM: Histogram = register_histogram!(
        "einsteindb_server_slightlike_snapshot_duration_seconds",
        "Bucketed histogram of server slightlike snapshots duration",
        exponential_buckets(0.05, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref GRPC_REQ_BATCH_COMMANDS_SIZE: Histogram = register_histogram!(
        "einsteindb_server_grpc_req_batch_size",
        "grpc batch size of gRPC requests",
        exponential_buckets(1f64, 2f64, 10).unwrap()
    )
    .unwrap();
    pub static ref GRPC_RESP_BATCH_COMMANDS_SIZE: Histogram = register_histogram!(
        "einsteindb_server_grpc_resp_batch_size",
        "grpc batch size of gRPC responses",
        exponential_buckets(1f64, 2f64, 10).unwrap()
    )
    .unwrap();
    pub static ref GC_EMPTY_RANGE_COUNTER: IntCounter = register_int_counter!(
        "einsteindb_causetStorage_gc_empty_cone_total",
        "Total number of empty cone found by gc"
    )
    .unwrap();
    pub static ref GC_SKIPPED_COUNTER: IntCounter = register_int_counter!(
        "einsteindb_causetStorage_gc_skipped_counter",
        "Total number of gc command skipped owing to optimization"
    )
    .unwrap();
    pub static ref GC_TASK_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einsteindb_gcworker_gc_task_duration_vec",
        "Duration of gc tasks execution",
        &["task"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref GC_TOO_BUSY_COUNTER: IntCounter = register_int_counter!(
        "einsteindb_gc_worker_too_busy",
        "Counter of occurrence of gc_worker being too busy"
    )
    .unwrap();
    pub static ref AUTO_GC_STATUS_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_gcworker_autogc_status",
        "State of the auto gc manager",
        &["state"]
    )
    .unwrap();
    pub static ref AUTO_GC_SAFE_POINT_GAUGE: IntGauge = register_int_gauge!(
        "einsteindb_gcworker_autogc_safe_point",
        "Safe point used for auto gc"
    )
    .unwrap();
    pub static ref AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_gcworker_autogc_processed_branes",
        "Processed branes by auto gc",
        &["type"]
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_RECV_COUNTER: IntCounter = register_int_counter!(
        "einsteindb_server_raft_message_recv_total",
        "Total number of violetabft messages received"
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_BATCH_SIZE: Histogram = register_histogram!(
        "einsteindb_server_raft_message_batch_size",
        "VioletaBft messages batch size",
        exponential_buckets(1f64, 2f64, 10).unwrap()
    )
    .unwrap();
    pub static ref REPORT_FAILURE_MSG_COUNTER: IntCounterVec = register_int_counter_vec!(
        "einsteindb_server_report_failure_msg_total",
        "Total number of reporting failure messages",
        &["type", "store_id"]
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_FLUSH_COUNTER: IntCounter = register_int_counter!(
        "einsteindb_server_raft_message_flush_total",
        "Total number of violetabft messages flushed immediately"
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_DELAY_FLUSH_COUNTER: IntCounter = register_int_counter!(
        "einsteindb_server_raft_message_delay_flush_total",
        "Total number of violetabft messages flushed delay"
    )
    .unwrap();
    pub static ref CONFIG_LMDB_GAUGE: GaugeVec = register_gauge_vec!(
        "einsteindb_config_rocksdb",
        "Config information of rocksdb",
        &["causet", "name"]
    )
    .unwrap();
    pub static ref REQUEST_BATCH_SIZE_HISTOGRAM_VEC: RequestBatchSizeHistogramVec =
        register_static_histogram_vec!(
            RequestBatchSizeHistogramVec,
            "einsteindb_server_request_batch_size",
            "Size of request batch input",
            &["type"],
            exponential_buckets(1f64, 5f64, 10).unwrap()
        )
        .unwrap();
    pub static ref REQUEST_BATCH_RATIO_HISTOGRAM_VEC: RequestBatchRatioHistogramVec =
        register_static_histogram_vec!(
            RequestBatchRatioHistogramVec,
            "einsteindb_server_request_batch_ratio",
            "Ratio of request batch output to input",
            &["type"],
            exponential_buckets(1f64, 5f64, 10).unwrap()
        )
        .unwrap();
    pub static ref CPU_CORES_QUOTA_GAUGE: Gauge = register_gauge!(
        "einsteindb_server_cpu_cores_quota",
        "Total CPU cores quota for EinsteinDB server"
    )
    .unwrap();
}

make_auto_flush_static_metric! {
    pub label_enum RequestStatusKind {
        all,
        success,
        err_timeout,
        err_empty_request,
        err_other,
        err_io,
        err_server,
        err_invalid_resp,
        err_invalid_req,
        err_not_leader,
        err_brane_not_found,
        err_key_not_in_brane,
        err_epoch_not_match,
        err_server_is_busy,
        err_stale_command,
        err_store_not_match,
        err_raft_entry_too_large,
    }

    pub label_enum RequestTypeKind {
        write,
        snapshot,
    }

    pub struct AsyncRequestsCounterVec: LocalIntCounter {
        "type" => RequestTypeKind,
        "status" => RequestStatusKind,
    }

    pub struct AsyncRequestsDurationVec: LocalHistogram {
        "type" => RequestTypeKind,
    }
}

impl From<ErrorHeaderKind> for RequestStatusKind {
    fn from(kind: ErrorHeaderKind) -> Self {
        match kind {
            ErrorHeaderKind::NotLeader => RequestStatusKind::err_not_leader,
            ErrorHeaderKind::BraneNotFound => RequestStatusKind::err_brane_not_found,
            ErrorHeaderKind::KeyNotInBrane => RequestStatusKind::err_key_not_in_brane,
            ErrorHeaderKind::EpochNotMatch => RequestStatusKind::err_epoch_not_match,
            ErrorHeaderKind::ServerIsBusy => RequestStatusKind::err_server_is_busy,
            ErrorHeaderKind::StaleCommand => RequestStatusKind::err_stale_command,
            ErrorHeaderKind::StoreNotMatch => RequestStatusKind::err_store_not_match,
            ErrorHeaderKind::VioletaBftEntryTooLarge => RequestStatusKind::err_raft_entry_too_large,
            ErrorHeaderKind::Other => RequestStatusKind::err_other,
        }
    }
}

lazy_static! {
    pub static ref ASYNC_REQUESTS_COUNTER: IntCounterVec = register_int_counter_vec!(
        "einsteindb_causetStorage_engine_async_request_total",
        "Total number of engine asynchronous requests",
        &["type", "status"]
    )
    .unwrap();
    pub static ref ASYNC_REQUESTS_DURATIONS: HistogramVec = register_histogram_vec!(
        "einsteindb_causetStorage_engine_async_request_duration_seconds",
        "Bucketed histogram of processing successful asynchronous requests.",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
}

lazy_static! {
    pub static ref ASYNC_REQUESTS_COUNTER_VEC: AsyncRequestsCounterVec =
        auto_flush_from!(ASYNC_REQUESTS_COUNTER, AsyncRequestsCounterVec);
    pub static ref ASYNC_REQUESTS_DURATIONS_VEC: AsyncRequestsDurationVec =
        auto_flush_from!(ASYNC_REQUESTS_DURATIONS, AsyncRequestsDurationVec);
}
