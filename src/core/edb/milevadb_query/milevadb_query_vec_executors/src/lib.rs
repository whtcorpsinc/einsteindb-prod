// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

//! This crate implements a simple SQL query engine to work with MilevaDB pushed down executors.
//!
//! The query engine is able to scan and understand events stored by MilevaDB, run against a
//! series of executors and then return the execution result. The query engine is provided via
//! EinsteinDB Interlock interface. However standalone UDF functions are also exported and can be used
//! standalone.

#![allow(incomplete_features)]
#![feature(proc_macro_hygiene)]
#![feature(specialization)]
#![feature(const_fn)]
#![feature(decl_macro)]

#[macro_use(box_try, warn)]
extern crate violetabftstore::interlock::;

#[macro_use(other_err)]
extern crate milevadb_query_common;

#[causet(test)]
pub use milevadb_query_vec_aggr::*;
#[causet(test)]
pub use milevadb_query_vec_expr::function::*;
#[causet(test)]
pub use milevadb_query_vec_expr::*;
mod fast_hash_aggr_executor;
mod index_scan_executor;
pub mod interface;
mod limit_executor;
pub mod runner;
mod selection_executor;
mod simple_aggr_executor;
mod slow_hash_aggr_executor;
mod stream_aggr_executor;
mod Block_scan_executor;
mod top_n_executor;
mod util;

pub use self::fast_hash_aggr_executor::BatchFastHashAggregationFreeDaemon;
pub use self::index_scan_executor::BatchIndexScanFreeDaemon;
pub use self::limit_executor::BatchLimitFreeDaemon;
pub use self::selection_executor::BatchSelectionFreeDaemon;
pub use self::simple_aggr_executor::BatchSimpleAggregationFreeDaemon;
pub use self::slow_hash_aggr_executor::BatchSlowHashAggregationFreeDaemon;
pub use self::stream_aggr_executor::BatchStreamAggregationFreeDaemon;
pub use self::Block_scan_executor::BatchBlockScanFreeDaemon;
pub use self::top_n_executor::BatchTopNFreeDaemon;
