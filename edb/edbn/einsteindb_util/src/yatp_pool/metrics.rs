// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use prometheus::*;

lazy_static! {
    pub static ref FUTUREPOOL_RUNNING_TASK_VEC: IntGaugeVec = register_int_gauge_vec!(
        "edb_futurepool_plightlikeing_task_total",
        "Current future_pool plightlikeing + running tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref FUTUREPOOL_HANDLED_TASK_VEC: IntCounterVec = register_int_counter_vec!(
        "edb_futurepool_handled_task_total",
        "Total number of future_pool handled tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref FUTUREPOOL_SCHEDULE_DURATION_VEC: HistogramVec = register_histogram_vec!(
        "edb_futurepool_schedule_duration",
        "Histogram of future_pool handle duration.",
        &["name"],
        exponential_buckets(0.0005, 2.0, 15).unwrap()
    )
    .unwrap();
}
