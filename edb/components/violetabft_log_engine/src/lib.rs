// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! Implementation of edb for VioletaBftEngine
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

#[macro_use]
extern crate violetabftstore::interlock::;

extern crate slog_global;

extern crate serde_derive;

extern crate violetabft;

mod engine;
pub use engine::{VioletaBftEngineConfig, VioletaBftLogBatch, VioletaBftLogEngine, RecoveryMode};
