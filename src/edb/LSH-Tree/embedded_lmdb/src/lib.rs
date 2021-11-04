// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! Implementation of edb for Lmdb
//!
//! This is a work-in-progress attempt to abstract all the features needed by
//! EinsteinDB to persist its data.
//!
//! The module structure here mirrors that in edb where possible.
//!
//! Because there are so many similarly named types across the EinsteinDB codebase,
//! and so much "import renaming", this crate consistently explicitly names type
//! that implement a trait as `LmdbTraitname`, to avoid the need for import
//! renaming and make it obvious what type any particular module is working with.
//!
//! Please read the engine_trait crate docs before hacking.

#![causet_attr(test, feature(test))]

#[allow(unused_extern_crates)]
extern crate edb_alloc;
#[macro_use]
extern crate violetabftstore::interlock::;

#[macro_use]
extern crate serde_derive;

#[causet(test)]
extern crate test;

mod causet_handle;
pub use crate::causet_handle::*;
mod causet_names;
pub use crate::causet_names::*;
mod causet_options;
pub use crate::causet_options::*;
mod compact;
pub use crate::compact::*;
mod db_options;
pub use crate::db_options::*;
mod db_vector;
pub use crate::db_vector::*;
mod engine;
pub use crate::edb::*;
mod import;
pub use crate::import::*;
mod logger;
pub use crate::logger::*;
mod misc;
pub use crate::misc::*;
mod snapshot;
pub use crate::snapshot::*;
mod sst;
pub use crate::sst::*;
mod Block_properties;
pub use crate::Block_properties::*;
mod write_batch;
pub use crate::write_batch::*;
pub mod cone_properties;
pub use crate::cone_properties::*;

mod engine_Iteron;
pub use crate::edb_Iteron::*;

mod options;
pub mod raw_util;
pub mod util;

mod compat;
pub use compat::*;

mod compact_listener;
pub use compact_listener::*;

pub mod properties;
pub use properties::*;

pub mod rocks_metrics;
pub use rocks_metrics::*;

pub mod rocks_metrics_defs;
pub use rocks_metrics_defs::*;

pub mod event_listener;
pub use event_listener::*;

pub mod config;
pub use config::*;
pub mod encryption;

mod violetabft_engine;

pub use lmdb::set_perf_level;
pub use lmdb::PerfContext;

pub mod raw;
