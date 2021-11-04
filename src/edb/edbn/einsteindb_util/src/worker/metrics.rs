// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use prometheus::*;

lazy_static! {
    pub static ref WORKER_PENDING_TASK_VEC: IntGaugeVec = register_int_gauge_vec!(
        "edb_worker_plightlikeing_task_total",
        "Current worker plightlikeing + running tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref WORKER_HANDLED_TASK_VEC: IntCounterVec = register_int_counter_vec!(
        "edb_worker_handled_task_total",
        "Total number of worker handled tasks.",
        &["name"]
    )
    .unwrap();
}
