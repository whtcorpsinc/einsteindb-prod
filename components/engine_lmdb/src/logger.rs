// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
use lmdb::{DBInfoLogLevel as InfoLogLevel, Logger};

// TODO(yiwu): abstract the Logger interface.
#[derive(Default)]
pub struct LmdbdbLogger;

impl Logger for LmdbdbLogger {
    fn logv(&self, log_level: InfoLogLevel, log: &str) {
        match log_level {
            InfoLogLevel::Header => info!(#"lmdb_log_header", "{}", log),
            InfoLogLevel::Debug => debug!(#"lmdb_log", "{}", log),
            InfoLogLevel::Info => info!(#"lmdb_log", "{}", log),
            InfoLogLevel::Warn => warn!(#"lmdb_log", "{}", log),
            InfoLogLevel::Error => error!(#"lmdb_log", "{}", log),
            InfoLogLevel::Fatal => crit!(#"lmdb_log", "{}", log),
            _ => {}
        }
    }
}

#[derive(Default)]
pub struct VioletaBftDBLogger;

impl Logger for VioletaBftDBLogger {
    fn logv(&self, log_level: InfoLogLevel, log: &str) {
        match log_level {
            InfoLogLevel::Header => info!(#"raftdb_log_header", "{}", log),
            InfoLogLevel::Debug => debug!(#"raftdb_log", "{}", log),
            InfoLogLevel::Info => info!(#"raftdb_log", "{}", log),
            InfoLogLevel::Warn => warn!(#"raftdb_log", "{}", log),
            InfoLogLevel::Error => error!(#"raftdb_log", "{}", log),
            InfoLogLevel::Fatal => crit!(#"raftdb_log", "{}", log),
            _ => {}
        }
    }
}
