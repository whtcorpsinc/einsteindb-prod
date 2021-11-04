// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

#![feature(box_TuringStrings)]

#[macro_use]
extern crate failure;
#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate violetabftstore::interlock::;

mod pushdown_causet;
mod lightlikepoint;
mod errors;
mod metrics;
mod semaphore;
mod service;

pub use lightlikepoint::{causet_contextTxnExtraInterlock_Semaphore, node, Task};
pub use errors::{Error, Result};
pub use semaphore::causet_contextSemaphore;
pub use service::Service;
