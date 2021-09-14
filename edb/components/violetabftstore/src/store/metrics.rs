// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum PerfContextType {
        write_wal_time,
        write_delay_time,
        write_scheduling_flushes_compactions_time,
        db_condition_wait_nanos,
        write_memBlock_time,
        pre_and_post_process,
        write_thread_wait,
        db_mutex_lock_nanos,
    }
    pub label_enum ProposalType {
        all,
        local_read,
        read_index,
        unsafe_read_index,
        normal,
        transfer_leader,
        conf_change,
        batch,
    }

    pub label_enum AdminCmdType {
        conf_change,
        add_peer,
        remove_peer,
        add_learner,
        batch_split : "batch-split",
        prepare_merge,
        commit_merge,
        rollback_merge,
        compact,
    }

    pub label_enum AdminCmdStatus {
        reject_unsafe,
        all,
        success,
    }

    pub label_enum VioletaBftReadyType {
        message,
        commit,
        applightlike,
        snapshot,
        plightlikeing_brane,
        has_ready_brane,
    }

    pub label_enum MessageCounterType {
        applightlike,
        applightlike_resp,
        prevote,
        prevote_resp,
        vote,
        vote_resp,
        snapshot,
        request_snapshot,
        heartbeat,
        heartbeat_resp,
        transfer_leader,
        timeout_now,
        read_index,
        read_index_resp,
    }

    pub label_enum VioletaBftDroppedMessage {
        mismatch_store_id,
        mismatch_brane_epoch,
        stale_msg,
        brane_overlap,
        brane_no_peer,
        brane_tombstone_peer,
        brane_nonexistent,
        applying_snap,
    }

    pub label_enum SnapValidationType {
        stale,
        decode,
        epoch,
    }

    pub label_enum BraneHashType {
        verify,
        compute,
    }

    pub label_enum BraneHashResult {
        miss,
        matched,
        all,
        failed,
    }

    pub label_enum CfNames {
        default,
        dagger,
        write,
        violetabft,
    }

    pub label_enum VioletaBftEntryType {
        hit,
        miss
    }

    pub label_enum VioletaBftInvalidProposal {
        mismatch_store_id,
        brane_not_found,
        not_leader,
        mismatch_peer_id,
        stale_command,
        epoch_not_match,
        read_index_no_leader,
        brane_not_initialized,
        is_applying_snapshot,
    }
    pub label_enum VioletaBftEventDurationType {
        compact_check,
        fidel_store_heartbeat,
        snap_gc,
        compact_lock_causet,
        consistency_check,
        cleanup_import_sst,
        violetabft_engine_purge,
    }

    pub struct VioletaBftEventDuration : LocalHistogram {
        "type" => VioletaBftEventDurationType
    }
    pub struct VioletaBftInvalidProposalCount : LocalIntCounter {
        "type" => VioletaBftInvalidProposal
    }
    pub struct VioletaBftEntryFetches : LocalIntCounter {
        "type" => VioletaBftEntryType
    }
    pub struct SnapCf : LocalHistogram {
        "type" => CfNames,
    }
    pub struct SnapCfSize : LocalHistogram {
        "type" => CfNames,
    }
    pub struct BraneHashCounter: LocalIntCounter {
        "type" => BraneHashType,
        "result" => BraneHashResult,
    }
    pub struct ProposalVec: LocalIntCounter {
        "type" => ProposalType,
    }

    pub struct AdminCmdVec : LocalIntCounter {
        "type" => AdminCmdType,
        "status" => AdminCmdStatus,
    }

    pub struct VioletaBftReadyVec : LocalIntCounter {
        "type" => VioletaBftReadyType,
    }

    pub struct MessageCounterVec : LocalIntCounter {
        "type" => MessageCounterType,
    }

    pub struct VioletaBftDropedVec : LocalIntCounter {
        "type" => VioletaBftDroppedMessage,
    }

    pub struct SnapValidVec : LocalIntCounter {
        "type" => SnapValidationType
    }
    pub struct PerfContextTimeDuration : LocalHistogram {
        "type" => PerfContextType
    }
}

lazy_static! {
    pub static ref PEER_PROPOSAL_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "edb_violetabftstore_proposal_total",
            "Total number of proposal made.",
            &["type"]
        ).unwrap();
    pub static ref PEER_PROPOSAL_COUNTER: ProposalVec =
        auto_flush_from!(PEER_PROPOSAL_COUNTER_VEC, ProposalVec);

    pub static ref PEER_ADMIN_CMD_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "edb_violetabftstore_admin_cmd_total",
            "Total number of admin cmd processed.",
            &["type", "status"]
        ).unwrap();
    pub static ref PEER_ADMIN_CMD_COUNTER: AdminCmdVec =
        auto_flush_from!(PEER_ADMIN_CMD_COUNTER_VEC, AdminCmdVec);

    pub static ref PEER_APPEND_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "edb_violetabftstore_applightlike_log_duration_seconds",
            "Bucketed histogram of peer applightlikeing log duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref PEER_COMMIT_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "edb_violetabftstore_commit_log_duration_seconds",
            "Bucketed histogram of peer commits logs duration.",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_APPLY_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "edb_violetabftstore_apply_log_duration_seconds",
            "Bucketed histogram of peer applying log duration.",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref APPLY_TASK_WAIT_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "edb_violetabftstore_apply_wait_time_duration_secs",
            "Bucketed histogram of apply task wait time duration.",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_VIOLETABFT_READY_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "edb_violetabftstore_violetabft_ready_handled_total",
            "Total number of violetabft ready handled.",
            &["type"]
        ).unwrap();
    pub static ref STORE_VIOLETABFT_READY_COUNTER: VioletaBftReadyVec =
        auto_flush_from!(STORE_VIOLETABFT_READY_COUNTER_VEC, VioletaBftReadyVec);

    pub static ref STORE_VIOLETABFT_SENT_MESSAGE_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "edb_violetabftstore_violetabft_sent_message_total",
            "Total number of violetabft ready sent messages.",
            &["type"]
        ).unwrap();
    pub static ref STORE_VIOLETABFT_SENT_MESSAGE_COUNTER: MessageCounterVec =
        auto_flush_from!(STORE_VIOLETABFT_SENT_MESSAGE_COUNTER_VEC, MessageCounterVec);

    pub static ref STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "edb_violetabftstore_violetabft_dropped_message_total",
            "Total number of violetabft dropped messages.",
            &["type"]
        ).unwrap();
    pub static ref STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER: VioletaBftDropedVec =
        auto_flush_from!(STORE_VIOLETABFT_DROPPED_MESSAGE_COUNTER_VEC, VioletaBftDropedVec);

    pub static ref STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC: IntGaugeVec =
        register_int_gauge_vec!(
            "edb_violetabftstore_snapshot_traffic_total",
            "Total number of violetabftstore snapshot traffic.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "edb_violetabftstore_snapshot_validation_failure_total",
            "Total number of violetabftstore snapshot validation failure.",
            &["type"]
        ).unwrap();
    pub static ref STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER: SnapValidVec =
        auto_flush_from!(STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER_VEC, SnapValidVec);

    pub static ref PEER_VIOLETABFT_PROCESS_DURATION: HistogramVec =
        register_histogram_vec!(
            "edb_violetabftstore_violetabft_process_duration_secs",
            "Bucketed histogram of peer processing violetabft duration.",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref PEER_PROPOSE_LOG_SIZE_HISTOGRAM: Histogram =
        register_histogram!(
            "edb_violetabftstore_propose_log_size",
            "Bucketed histogram of peer proposing log size.",
            vec![256.0, 512.0, 1024.0, 4096.0, 65536.0, 262144.0, 524288.0, 1048576.0,
                    2097152.0, 4194304.0, 8388608.0, 16777216.0]
        ).unwrap();

    pub static ref REGION_HASH_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "edb_violetabftstore_hash_total",
            "Total number of hash has been computed.",
            &["type", "result"]
        ).unwrap();
    pub static ref REGION_HASH_COUNTER: BraneHashCounter =
        auto_flush_from!(REGION_HASH_COUNTER_VEC, BraneHashCounter);

    pub static ref REGION_MAX_LOG_LAG: Histogram =
        register_histogram!(
            "edb_violetabftstore_log_lag",
            "Bucketed histogram of log lag in a brane.",
            vec![2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0,
                    512.0, 1024.0, 5120.0, 10240.0]
        ).unwrap();

    pub static ref REQUEST_WAIT_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "edb_violetabftstore_request_wait_time_duration_secs",
            "Bucketed histogram of request wait time duration.",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref PEER_GC_VIOLETABFT_LOG_COUNTER: IntCounter =
        register_int_counter!(
            "edb_violetabftstore_gc_violetabft_log_total",
            "Total number of GC violetabft log."
        ).unwrap();

    pub static ref fidelio_REGION_SIZE_BY_COMPACTION_COUNTER: IntCounter =
        register_int_counter!(
            "fidelio_brane_size_count_by_compaction",
            "Total number of fidelio brane size caused by compaction."
        ).unwrap();

    pub static ref COMPACTION_RELATED_REGION_COUNT: HistogramVec =
        register_histogram_vec!(
            "compaction_related_brane_count",
            "Associated number of branes for each compaction job.",
            &["output_level"],
            exponential_buckets(1.0, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref COMPACTION_DECLINED_BYTES: HistogramVec =
        register_histogram_vec!(
            "compaction_declined_bytes",
            "Total bytes declined for each compaction job.",
            &["output_level"],
            exponential_buckets(1024.0, 2.0, 30).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_Causet_KV_COUNT_VEC: HistogramVec =
        register_histogram_vec!(
            "edb_snapshot_causet_kv_count",
            "Total number of kv in each causet file of snapshot.",
            &["type"],
            exponential_buckets(100.0, 2.0, 20).unwrap()
        ).unwrap();
    pub static ref SNAPSHOT_Causet_KV_COUNT: SnapCf =
        auto_flush_from!(SNAPSHOT_Causet_KV_COUNT_VEC, SnapCf);

    pub static ref SNAPSHOT_Causet_SIZE_VEC: HistogramVec =
        register_histogram_vec!(
            "edb_snapshot_causet_size",
            "Total size of each causet file of snapshot.",
            &["type"],
            exponential_buckets(1024.0, 2.0, 31).unwrap()
        ).unwrap();
    pub static ref SNAPSHOT_Causet_SIZE: SnapCfSize =
        auto_flush_from!(SNAPSHOT_Causet_SIZE_VEC, SnapCfSize);
    pub static ref SNAPSHOT_BUILD_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "edb_snapshot_build_time_duration_secs",
            "Bucketed histogram of snapshot build time duration.",
            exponential_buckets(0.05, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_KV_COUNT_HISTOGRAM: Histogram =
        register_histogram!(
            "edb_snapshot_kv_count",
            "Total number of kv in snapshot.",
            exponential_buckets(100.0, 2.0, 20).unwrap() //100,100*2^1,...100M
        ).unwrap();

    pub static ref SNAPSHOT_SIZE_HISTOGRAM: Histogram =
        register_histogram!(
            "edb_snapshot_size",
            "Size of snapshot.",
            exponential_buckets(1024.0, 2.0, 22).unwrap() // 1024,1024*2^1,..,4G
        ).unwrap();

    pub static ref VIOLETABFT_ENTRY_FETCHES_VEC: IntCounterVec =
        register_int_counter_vec!(
            "edb_violetabftstore_entry_fetches",
            "Total number of violetabft entry fetches.",
            &["type"]
        ).unwrap();
    pub static ref VIOLETABFT_ENTRY_FETCHES: VioletaBftEntryFetches =
        auto_flush_from!(VIOLETABFT_ENTRY_FETCHES_VEC, VioletaBftEntryFetches);

    pub static ref LEADER_MISSING: IntGauge =
        register_int_gauge!(
            "edb_violetabftstore_leader_missing",
            "Total number of leader missed brane."
        ).unwrap();

    pub static ref INGEST_SST_DURATION_SECONDS: Histogram =
        register_histogram!(
            "edb_snapshot_ingest_sst_duration_seconds",
            "Bucketed histogram of lmdb ingestion durations.",
            exponential_buckets(0.005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref VIOLETABFT_INVALID_PROPOSAL_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "edb_violetabftstore_violetabft_invalid_proposal_total",
            "Total number of violetabft invalid proposal.",
            &["type"]
        ).unwrap();
    pub static ref VIOLETABFT_INVALID_PROPOSAL_COUNTER: VioletaBftInvalidProposalCount =
        auto_flush_from!(VIOLETABFT_INVALID_PROPOSAL_COUNTER_VEC, VioletaBftInvalidProposalCount);

    pub static ref VIOLETABFT_EVENT_DURATION_VEC: HistogramVec =
        register_histogram_vec!(
            "edb_violetabftstore_event_duration",
            "Duration of violetabft store events.",
            &["type"],
            exponential_buckets(0.001, 1.59, 20).unwrap() // max 10s
        ).unwrap();
    pub static ref VIOLETABFT_EVENT_DURATION: VioletaBftEventDuration =
        auto_flush_from!(VIOLETABFT_EVENT_DURATION_VEC, VioletaBftEventDuration);

    pub static ref VIOLETABFT_READ_INDEX_PENDING_DURATION: Histogram =
        register_histogram!(
            "edb_violetabftstore_read_index_plightlikeing_duration",
            "Duration of plightlikeing read index.",
            exponential_buckets(0.001, 2.0, 20).unwrap() // max 1000s
        ).unwrap();

    pub static ref VIOLETABFT_READ_INDEX_PENDING_COUNT: IntGauge =
        register_int_gauge!(
            "edb_violetabftstore_read_index_plightlikeing",
            "Plightlikeing read index count."
        ).unwrap();

    pub static ref APPLY_PERF_CONTEXT_TIME_HISTOGRAM: HistogramVec =
        register_histogram_vec!(
            "edb_violetabftstore_apply_perf_context_time_duration_secs",
            "Bucketed histogram of request wait time duration.",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_PERF_CONTEXT_TIME_HISTOGRAM: HistogramVec =
        register_histogram_vec!(
            "edb_violetabftstore_store_perf_context_time_duration_secs",
            "Bucketed histogram of request wait time duration.",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC: PerfContextTimeDuration=
        auto_flush_from!(APPLY_PERF_CONTEXT_TIME_HISTOGRAM, PerfContextTimeDuration);

    pub static ref STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC: PerfContextTimeDuration=
        auto_flush_from!(STORE_PERF_CONTEXT_TIME_HISTOGRAM, PerfContextTimeDuration);

    pub static ref READ_QPS_TOPN: GaugeVec =
        register_gauge_vec!(
            "edb_read_qps_topn",
            "Collect topN of read qps.",
        &["order"]
        ).unwrap();

    pub static ref VIOLETABFT_ENTRIES_CACHES_GAUGE: IntGauge = register_int_gauge!(
        "edb_violetabft_entries_caches",
        "Total memory size of violetabft entries caches."
        ).unwrap();

    pub static ref APPLY_PENDING_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "edb_violetabftstore_apply_plightlikeing_bytes",
        "The bytes plightlikeing in the channel of apply FSMs."
    )
    .unwrap();

    pub static ref APPLY_PENDING_ENTRIES_GAUGE: IntGauge = register_int_gauge!(
            "edb_violetabftstore_apply_plightlikeing_entries",
            "The number of plightlikeing entries in the channel of apply FSMs."
    )
    .unwrap();
}
