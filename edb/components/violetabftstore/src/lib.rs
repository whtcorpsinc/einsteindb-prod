// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

#![causet_attr(test, feature(test))]
#![feature(cell_fidelio)]
#![feature(shrink_to)]
#![feature(div_duration)]
#![feature(min_specialization)]

#[macro_use]
extern crate bitflags;
#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_with;
#[macro_use]
extern crate violetabftstore::interlock::;

#[causet(test)]
extern crate test;

pub mod interlock;
pub mod errors;
pub mod router;
pub mod store;
pub use self::interlock::{BraneInfo, BraneInfoAccessor, SeekBraneCallback};
pub use self::errors::{DiscardReason, Error, Result};
