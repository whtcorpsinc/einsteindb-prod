// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum SnapType {
       generate,
       apply,
    }

    pub label_enum SnapStatus {
        all,
        success,
        abort,
        fail,
        delay,
        ignore,
    }

    pub struct SnapCounter: LocalIntCounter {
        "type" => SnapType,
        "status" => SnapStatus,
    }

    pub struct CheckSplitCounter : LocalIntCounter {
        "type" => SnapStatus,
    }

    pub struct SnapHistogram : LocalHistogram {
        "type" => SnapType,
    }
}

make_static_metric! {
    pub label_enum RejectReason {
        store_id_mismatch,
        peer_id_mismatch,
        term_mismatch,
        lease_expire,
        no_brane,
        no_lease,
        epoch,
        appiled_term,
        channel_full,
    }

    pub struct ReadRejectCounter : IntCounter {
       "reason" => RejectReason
    }
}

lazy_static! {
    pub static ref SNAP_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "edb_violetabftstore_snapshot_total",
        "Total number of violetabftstore snapshot processed.",
        &["type", "status"]
    )
    .unwrap();
    pub static ref SNAP_COUNTER: SnapCounter = auto_flush_from!(SNAP_COUNTER_VEC, SnapCounter);
    pub static ref CHECK_SPILT_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "edb_violetabftstore_check_split_total",
        "Total number of violetabftstore split check.",
        &["type"]
    )
    .unwrap();
    pub static ref CHECK_SPILT_COUNTER: CheckSplitCounter =
        auto_flush_from!(CHECK_SPILT_COUNTER_VEC, CheckSplitCounter);
    pub static ref SNAP_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "edb_violetabftstore_snapshot_duration_seconds",
        "Bucketed histogram of violetabftstore snapshot process duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SNAP_HISTOGRAM: SnapHistogram =
        auto_flush_from!(SNAP_HISTOGRAM_VEC, SnapHistogram);
    pub static ref CHECK_SPILT_HISTOGRAM: Histogram = register_histogram!(
        "edb_violetabftstore_check_split_duration_seconds",
        "Bucketed histogram of violetabftstore split check duration",
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COMPACT_RANGE_Causet: HistogramVec = register_histogram_vec!(
        "edb_compact_cone_causet_duration_seconds",
        "Bucketed histogram of compact cone for causet execution",
        &["causet"]
    )
    .unwrap();
    pub static ref REGION_HASH_HISTOGRAM: Histogram = register_histogram!(
        "edb_violetabftstore_hash_duration_seconds",
        "Bucketed histogram of violetabftstore hash computation duration"
    )
    .unwrap();
    pub static ref STALE_PEER_PENDING_DELETE_RANGE_GAUGE: Gauge = register_gauge!(
        "edb_plightlikeing_delete_cones_of_stale_peer",
        "Total number of edb plightlikeing delete cone of stale peer"
    )
    .unwrap();
    pub static ref LOCAL_READ_REJECT_VEC: IntCounterVec = register_int_counter_vec!(
        "edb_violetabftstore_local_read_reject_total",
        "Total number of rejections from the local reader.",
        &["reason"]
    )
    .unwrap();
    pub static ref LOCAL_READ_REJECT: ReadRejectCounter =
        ReadRejectCounter::from(&LOCAL_READ_REJECT_VEC);
    pub static ref LOCAL_READ_EXECUTED_REQUESTS: IntCounter = register_int_counter!(
        "edb_violetabftstore_local_read_executed_requests",
        "Total number of requests directly executed by local reader."
    )
    .unwrap();
    pub static ref LOCAL_READ_EXECUTED_CACHE_REQUESTS: IntCounter = register_int_counter!(
        "edb_violetabftstore_local_read_cache_requests",
        "Total number of requests directly executed by local reader."
    )
    .unwrap();
}
