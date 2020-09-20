// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref CDC_RESOLVED_TS_GAP_HISTOGRAM: Histogram = register_histogram!(
        "einsteindb_cdc_resolved_ts_gap_seconds",
        "Bucketed histogram of the gap between cdc resolved ts and current tso",
        exponential_buckets(0.001, 2.0, 24).unwrap()
    )
    .unwrap();
    pub static ref CDC_SCAN_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "einsteindb_cdc_scan_duration_seconds",
        "Bucketed histogram of cdc async scan duration",
        exponential_buckets(0.005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref CDC_MIN_RESOLVED_TS_REGION: IntGauge = register_int_gauge!(
        "einsteindb_cdc_min_resolved_ts_brane",
        "The brane which has minimal resolved ts"
    )
    .unwrap();
    pub static ref CDC_MIN_RESOLVED_TS: IntGauge = register_int_gauge!(
        "einsteindb_cdc_min_resolved_ts",
        "The minimal resolved ts for current branes"
    )
    .unwrap();
    pub static ref CDC_PENDING_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "einsteindb_cdc_plightlikeing_bytes",
        "Bytes in memory of a plightlikeing brane"
    )
    .unwrap();
    pub static ref CDC_CAPTURED_REGION_COUNT: IntGauge = register_int_gauge!(
        "einsteindb_cdc_captured_brane_total",
        "Total number of CDC captured branes"
    )
    .unwrap();
    pub static ref CDC_OLD_VALUE_CACHE_MISS: IntGauge = register_int_gauge!(
        "einsteindb_cdc_old_value_cache_miss",
        "Count of old value cache missing"
    )
    .unwrap();
    pub static ref CDC_OLD_VALUE_CACHE_ACCESS: IntGauge = register_int_gauge!(
        "einsteindb_cdc_old_value_cache_access",
        "Count of old value cache accessing"
    )
    .unwrap();
    pub static ref CDC_OLD_VALUE_CACHE_BYTES: IntGauge =
        register_int_gauge!("einsteindb_cdc_old_value_cache_bytes", "Bytes of old value cache").unwrap();
}
