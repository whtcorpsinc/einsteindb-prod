// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]
#![feature(box_patterns)]

#[macro_use]
extern crate failure;
#[allow(unused_extern_crates)]
extern crate einsteindb_alloc;
#[macro_use]
extern crate einsteindb_util;

mod lightlikepoint;
mod errors;
mod metrics;
mod service;
mod writer;

pub use lightlikepoint::{Endpoint, Task};
pub use errors::{Error, Result};
pub use service::Service;
pub use writer::{BackupRawKVWriter, BackupWriter};
