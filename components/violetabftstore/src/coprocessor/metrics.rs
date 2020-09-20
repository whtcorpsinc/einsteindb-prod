// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use prometheus::{exponential_buckets, Histogram, IntGaugeVec};

lazy_static! {
    pub static ref REGION_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "einsteindb_violetabftstore_brane_size",
        "Bucketed histogram of approximate brane size.",
        exponential_buckets(1024.0 * 1024.0, 2.0, 20).unwrap() // max bucket would be 512GB
    ).unwrap();

    pub static ref REGION_KEYS_HISTOGRAM: Histogram = register_histogram!(
        "einsteindb_violetabftstore_brane_tuplespaceInstanton",
        "Bucketed histogram of approximate brane tuplespaceInstanton.",
        exponential_buckets(1.0, 2.0, 30).unwrap()
    ).unwrap();

    pub static ref REGION_COUNT_GAUGE_VEC: IntGaugeVec =
    register_int_gauge_vec!(
        "einsteindb_violetabftstore_brane_count",
        "Number of branes collected in brane_collector",
        &["type"]
    ).unwrap();
}
