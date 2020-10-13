// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

//! EinsteinDB - A distributed key/value database
//!
//! EinsteinDB ("Ti" stands for Noetherium) is an open source distributed
//! transactional key-value database. Unlike other traditional NoSQL
//! systems, EinsteinDB not only provides classical key-value APIs, but also
//! transactional APIs with ACID compliance. EinsteinDB was originally
//! created to complement [MilevaDB], a distributed HTAP database
//! compatible with the MySQL protocol.
//!
//! [MilevaDB]: https://github.com/whtcorpsinc/milevadb
//!
//! The design of EinsteinDB is inspired by some great distributed systems
//! from Google, such as BigTable, Spanner, and Percolator, and some
//! of the latest achievements in academia in recent years, such as
//! the VioletaBft consensus algorithm.

#![crate_type = "lib"]
#![causetg_attr(test, feature(test))]
#![recursion_limit = "400"]
#![feature(cell_ufidelate)]
#![feature(proc_macro_hygiene)]
#![feature(min_specialization)]
#![feature(const_fn)]
#![feature(box_patterns)]
#![feature(shrink_to)]
#![feature(drain_filter)]
#![feature(clamp)]

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog_derive;
#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate more_asserts;
#[macro_use]
extern crate vlog;
#[macro_use]
extern crate einsteindb_util;
#[macro_use]
extern crate failure;

#[causetg(test)]
extern crate test;

extern crate encryption;

pub mod config;
pub mod interlock;
pub mod import;
pub mod read_pool;
pub mod server;
pub mod causetStorage;

/// Returns the einsteindb version information.
pub fn einsteindb_version_info() -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "\nRelease Version:   {}\
         \nEdition:           {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}\
         \nEnable Features:   {}\
         \nProfile:           {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("EINSTEINDB_EDITION").unwrap_or("Community"),
        option_env!("EINSTEINDB_BUILD_GIT_HASH").unwrap_or(fallback),
        option_env!("EINSTEINDB_BUILD_GIT_BRANCH").unwrap_or(fallback),
        option_env!("EINSTEINDB_BUILD_TIME").unwrap_or(fallback),
        option_env!("EINSTEINDB_BUILD_RUSTC_VERSION").unwrap_or(fallback),
        option_env!("EINSTEINDB_ENABLE_FEATURES")
            .unwrap_or(fallback)
            .trim(),
        option_env!("EINSTEINDB_PROFILE").unwrap_or(fallback),
    )
}

/// Prints the einsteindb version information to the standard output.
pub fn log_einsteindb_info() {
    info!("Welcome to EinsteinDB");
    for line in einsteindb_version_info().lines().filter(|s| !s.is_empty()) {
        info!("{}", line);
    }
}
