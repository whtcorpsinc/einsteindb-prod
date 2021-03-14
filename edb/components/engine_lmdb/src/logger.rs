// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.
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
            InfoLogLevel::Header => info!(#"violetabftdb_log_header", "{}", log),
            InfoLogLevel::Debug => debug!(#"violetabftdb_log", "{}", log),
            InfoLogLevel::Info => info!(#"violetabftdb_log", "{}", log),
            InfoLogLevel::Warn => warn!(#"violetabftdb_log", "{}", log),
            InfoLogLevel::Error => error!(#"violetabftdb_log", "{}", log),
            InfoLogLevel::Fatal => crit!(#"violetabftdb_log", "{}", log),
            _ => {}
        }
    }
}
