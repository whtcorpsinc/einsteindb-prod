// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::borrow::ToOwned;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::Local;
use clap::ArgMatches;
use einsteindb::config::{check_critical_config, persist_config, MetricConfig, EINSTEINDBConfig};
use einsteindb::causetStorage::config::DEFAULT_LMDB_SUB_DIR;
use einsteindb_util::collections::HashMap;
use einsteindb_util::{self, config, logger};

// A workaround for checking if log is initialized.
pub static LOG_INITIALIZED: AtomicBool = AtomicBool::new(false);

// The info log file names does not lightlike with ".log" since it conflict with rocksdb WAL files.
pub const DEFAULT_LMDB_LOG_FILE: &str = "rocksdb.info";
pub const DEFAULT_RAFTDB_LOG_FILE: &str = "raftdb.info";

#[macro_export]
macro_rules! fatal {
    ($lvl:expr $(, $arg:expr)*) => ({
        if $crate::setup::LOG_INITIALIZED.load(::std::sync::atomic::Ordering::SeqCst) {
            crit!($lvl $(, $arg)*);
        } else {
            eprintln!($lvl $(, $arg)*);
        }
        slog_global::clear_global();
        ::std::process::exit(1)
    })
}

// TODO: There is a very small chance that duplicate files will be generated if there are
// a lot of logs written in a very short time. Consider rename the rotated file with a version
// number while rotate by size.
fn rename_by_timestamp(path: &Path) -> io::Result<PathBuf> {
    let mut new_path = path.to_path_buf().into_os_string();
    new_path.push(format!(
        ".{}",
        Local::now().format(logger::DATETIME_ROTATE_SUFFIX)
    ));
    Ok(PathBuf::from(new_path))
}

fn make_engine_log_path(path: &str, sub_path: &str, filename: &str) -> String {
    let mut path = Path::new(path).to_path_buf();
    if !sub_path.is_empty() {
        path = path.join(Path::new(sub_path));
    }
    let path = path.to_str().unwrap_or_else(|| {
        fatal!(
            "failed to construct engine log dir {:?}, {:?}",
            path,
            sub_path
        );
    });
    config::ensure_dir_exist(path).unwrap_or_else(|e| {
        fatal!("failed to create engine log dir: {}", e);
    });
    config::canonicalize_log_dir(&path, filename).unwrap_or_else(|e| {
        fatal!("failed to canonicalize engine log dir {:?}: {}", path, e);
    })
}

#[allow(dead_code)]
pub fn initial_logger(config: &EINSTEINDBConfig) {
    fn build_logger<D>(drainer: D, config: &EINSTEINDBConfig)
    where
        D: slog::Drain + Slightlike + 'static,
        <D as slog::Drain>::Err: std::fmt::Display,
    {
        // use async drainer and init std log.
        logger::init_log(
            drainer,
            config.log_level,
            true,
            true,
            vec![],
            config.slow_log_memory_barrier.as_millis(),
        )
        .unwrap_or_else(|e| {
            fatal!("failed to initialize log: {}", e);
        });
    }

    if config.log_file.is_empty() {
        let writer = logger::term_writer();
        match config.log_format {
            config::LogFormat::Text => build_logger(logger::text_format(writer), config),
            config::LogFormat::Json => build_logger(logger::json_format(writer), config),
        };
    } else {
        let writer = logger::file_writer(
            &config.log_file,
            config.log_rotation_timespan,
            config.log_rotation_size,
            rename_by_timestamp,
        )
        .unwrap_or_else(|e| {
            fatal!(
                "failed to initialize log with file {}: {}",
                config.log_file,
                e
            );
        });

        let slow_log_writer = if config.slow_log_file.is_empty() {
            None
        } else {
            let slow_log_writer = logger::file_writer(
                &config.slow_log_file,
                config.log_rotation_timespan,
                config.log_rotation_size,
                rename_by_timestamp,
            )
            .unwrap_or_else(|e| {
                fatal!(
                    "failed to initialize slow-log with file {}: {}",
                    config.slow_log_file,
                    e
                );
            });
            Some(slow_log_writer)
        };

        let rocksdb_info_log_path = if !config.rocksdb.info_log_dir.is_empty() {
            make_engine_log_path(&config.rocksdb.info_log_dir, "", DEFAULT_LMDB_LOG_FILE)
        } else {
            make_engine_log_path(
                &config.causetStorage.data_dir,
                DEFAULT_LMDB_SUB_DIR,
                DEFAULT_LMDB_LOG_FILE,
            )
        };
        let raftdb_info_log_path = if !config.raftdb.info_log_dir.is_empty() {
            make_engine_log_path(&config.raftdb.info_log_dir, "", DEFAULT_RAFTDB_LOG_FILE)
        } else {
            if !config.raft_store.raftdb_path.is_empty() {
                make_engine_log_path(
                    &config.raft_store.raftdb_path.clone(),
                    "",
                    DEFAULT_RAFTDB_LOG_FILE,
                )
            } else {
                make_engine_log_path(&config.causetStorage.data_dir, "violetabft", DEFAULT_RAFTDB_LOG_FILE)
            }
        };
        let rocksdb_log_writer = logger::file_writer(
            &rocksdb_info_log_path,
            config.log_rotation_timespan,
            config.log_rotation_size,
            rename_by_timestamp,
        )
        .unwrap_or_else(|e| {
            fatal!(
                "failed to initialize rocksdb log with file {}: {}",
                rocksdb_info_log_path,
                e
            );
        });

        let raftdb_log_writer = logger::file_writer(
            &raftdb_info_log_path,
            config.log_rotation_timespan,
            config.log_rotation_size,
            rename_by_timestamp,
        )
        .unwrap_or_else(|e| {
            fatal!(
                "failed to initialize raftdb log with file {}: {}",
                raftdb_info_log_path,
                e
            );
        });

        match config.log_format {
            config::LogFormat::Text => build_logger_with_slow_log(
                logger::text_format(writer),
                logger::rocks_text_format(rocksdb_log_writer),
                logger::text_format(raftdb_log_writer),
                slow_log_writer.map(logger::text_format),
                config,
            ),
            config::LogFormat::Json => build_logger_with_slow_log(
                logger::json_format(writer),
                logger::json_format(rocksdb_log_writer),
                logger::json_format(raftdb_log_writer),
                slow_log_writer.map(logger::json_format),
                config,
            ),
        };

        fn build_logger_with_slow_log<N, R, S, T>(
            normal: N,
            rocksdb: R,
            raftdb: T,
            slow: Option<S>,
            config: &EINSTEINDBConfig,
        ) where
            N: slog::Drain<Ok = (), Err = io::Error> + Slightlike + 'static,
            R: slog::Drain<Ok = (), Err = io::Error> + Slightlike + 'static,
            S: slog::Drain<Ok = (), Err = io::Error> + Slightlike + 'static,
            T: slog::Drain<Ok = (), Err = io::Error> + Slightlike + 'static,
        {
            let drainer = logger::LogDispatcher::new(normal, rocksdb, raftdb, slow);

            // use async drainer and init std log.
            logger::init_log(
                drainer,
                config.log_level,
                true,
                true,
                vec![],
                config.slow_log_memory_barrier.as_millis(),
            )
            .unwrap_or_else(|e| {
                fatal!("failed to initialize log: {}", e);
            });
        }
    };
    LOG_INITIALIZED.store(true, Ordering::SeqCst);
}

#[allow(dead_code)]
pub fn initial_metric(causetg: &MetricConfig, node_id: Option<u64>) {
    einsteindb_util::metrics::monitor_process()
        .unwrap_or_else(|e| fatal!("failed to spacelike process monitor: {}", e));
    einsteindb_util::metrics::monitor_threads("einsteindb")
        .unwrap_or_else(|e| fatal!("failed to spacelike thread monitor: {}", e));
    einsteindb_util::metrics::monitor_allocator_stats("einsteindb")
        .unwrap_or_else(|e| fatal!("failed to monitor allocator stats: {}", e));

    if causetg.interval.as_secs() == 0 || causetg.address.is_empty() {
        return;
    }

    let mut push_job = causetg.job.clone();
    if let Some(id) = node_id {
        push_job.push_str(&format!("_{}", id));
    }

    info!("spacelike prometheus client");
    einsteindb_util::metrics::run_prometheus(causetg.interval.0, &causetg.address, &push_job);
}

#[allow(dead_code)]
pub fn overwrite_config_with_cmd_args(config: &mut EINSTEINDBConfig, matches: &ArgMatches<'_>) {
    if let Some(level) = matches.value_of("log-level") {
        config.log_level = logger::get_level_by_string(level).unwrap();
    }

    if let Some(file) = matches.value_of("log-file") {
        config.log_file = file.to_owned();
    }

    if let Some(addr) = matches.value_of("addr") {
        config.server.addr = addr.to_owned();
    }

    if let Some(advertise_addr) = matches.value_of("advertise-addr") {
        config.server.advertise_addr = advertise_addr.to_owned();
    }

    if let Some(status_addr) = matches.value_of("status-addr") {
        config.server.status_addr = status_addr.to_owned();
    }

    if let Some(advertise_status_addr) = matches.value_of("advertise-status-addr") {
        config.server.advertise_status_addr = advertise_status_addr.to_owned();
    }

    if let Some(data_dir) = matches.value_of("data-dir") {
        config.causetStorage.data_dir = data_dir.to_owned();
    }

    if let Some(lightlikepoints) = matches.values_of("fidel-lightlikepoints") {
        config.fidel.lightlikepoints = lightlikepoints.map(ToOwned::to_owned).collect();
    }

    if let Some(labels_vec) = matches.values_of("labels") {
        let mut labels = HashMap::default();
        for label in labels_vec {
            let mut parts = label.split('=');
            let key = parts.next().unwrap().to_owned();
            let value = match parts.next() {
                None => fatal!("invalid label: {}", label),
                Some(v) => v.to_owned(),
            };
            if parts.next().is_some() {
                fatal!("invalid label: {}", label);
            }
            labels.insert(key, value);
        }
        config.server.labels = labels;
    }

    if let Some(capacity_str) = matches.value_of("capacity") {
        let capacity = capacity_str.parse().unwrap_or_else(|e| {
            fatal!("invalid capacity: {}", e);
        });
        config.raft_store.capacity = capacity;
    }

    if let Some(metrics_addr) = matches.value_of("metrics-addr") {
        config.metric.address = metrics_addr.to_owned()
    }
}

#[allow(dead_code)]
pub fn validate_and_persist_config(config: &mut EINSTEINDBConfig, persist: bool) {
    config.compatible_adjust();
    if let Err(e) = config.validate() {
        fatal!("invalid configuration: {}", e);
    }

    if let Err(e) = check_critical_config(config) {
        fatal!("critical config check failed: {}", e);
    }

    if persist {
        if let Err(e) = persist_config(&config) {
            fatal!("persist critical config failed: {}", e);
        }
    }
}

pub fn ensure_no_unrecognized_config(unrecognized_tuplespaceInstanton: &[String]) {
    if !unrecognized_tuplespaceInstanton.is_empty() {
        fatal!(
            "unknown configuration options: {}",
            unrecognized_tuplespaceInstanton.join(", ")
        );
    }
}
