// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum FreeDaemonName {
        batch_Block_scan,
        batch_index_scan,
        batch_selection,
        batch_simple_aggr,
        batch_fast_hash_aggr,
        batch_slow_hash_aggr,
        batch_stream_aggr,
        batch_limit,
        batch_top_n,
        Block_scan,
        index_scan,
        selection,
        hash_aggr,
        stream_aggr,
        top_n,
        limit,
    }

    pub struct LocalCoprFreeDaemonCount: LocalIntCounter {
        "type" => FreeDaemonName,
    }
}

lazy_static::lazy_static! {
    static ref COPR_EXECUTOR_COUNT: IntCounterVec = register_int_counter_vec!(
        "edb_interlock_executor_count",
        "Total number of each executor",
        &["type"]
    )
    .unwrap();
}

lazy_static::lazy_static! {
    pub static ref EXECUTOR_COUNT_METRICS: LocalCoprFreeDaemonCount =
        auto_flush_from!(COPR_EXECUTOR_COUNT, LocalCoprFreeDaemonCount);
}
