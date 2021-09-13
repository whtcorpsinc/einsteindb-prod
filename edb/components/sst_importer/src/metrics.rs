//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use prometheus::*;

lazy_static! {
    pub static ref IMPORT_RPC_DURATION: HistogramVec = register_histogram_vec!(
        "edb_import_rpc_duration",
        "Bucketed histogram of import rpc duration",
        &["request", "result"],
        exponential_buckets(0.001, 2.0, 30).unwrap()
    )
    .unwrap();
    pub static ref IMPORT_UPLOAD_Soliton_BYTES: Histogram = register_histogram!(
        "edb_import_upload_Soliton_bytes",
        "Bucketed histogram of import upload Soliton bytes",
        exponential_buckets(1024.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORT_UPLOAD_Soliton_DURATION: Histogram = register_histogram!(
        "edb_import_upload_Soliton_duration",
        "Bucketed histogram of import upload Soliton duration",
        exponential_buckets(0.001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORT_WRITE_Soliton_DURATION: Histogram = register_histogram!(
        "edb_import_write_Soliton_duration",
        "Bucketed histogram of import write Soliton duration",
        exponential_buckets(0.001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORTER_DOWNLOAD_DURATION: HistogramVec = register_histogram_vec!(
        "edb_import_download_duration",
        "Bucketed histogram of importer download duration",
        &["type"],
        exponential_buckets(0.001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORTER_DOWNLOAD_BYTES: Histogram = register_histogram!(
        "edb_import_download_bytes",
        "Bucketed histogram of importer download bytes",
        exponential_buckets(1024.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORTER_INGEST_DURATION: HistogramVec = register_histogram_vec!(
        "edb_import_ingest_duration",
        "Bucketed histogram of importer ingest duration",
        &["type"],
        exponential_buckets(0.001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORTER_INGEST_BYTES: Histogram = register_histogram!(
        "edb_import_ingest_bytes",
        "Bucketed histogram of importer ingest bytes",
        exponential_buckets(1024.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORTER_ERROR_VEC: IntCounterVec = register_int_counter_vec!(
        "edb_import_error_counter",
        "Total number of importer errors",
        &["error"]
    )
    .unwrap();
}
