// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

#![feature(box_TuringStrings)]

#[macro_use]
extern crate failure;
#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate einsteindb_util;

mod delegate;
mod lightlikepoint;
mod errors;
mod metrics;
mod observer;
mod service;

pub use lightlikepoint::{CdcTxnExtraScheduler, Endpoint, Task};
pub use errors::{Error, Result};
pub use observer::CdcObserver;
pub use service::Service;
