// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

//! This crate stores data types which used by other milevadb query related crates.

#![feature(proc_macro_hygiene)]
#![feature(min_specialization)]
#![feature(test)]
#![feature(decl_macro)]
#![feature(str_internals)]
#![feature(ptr_offset_from)]

#[macro_use]
extern crate failure;
#[macro_use]
extern crate num_derive;
#[macro_use]
extern crate static_assertions;
#[macro_use(box_err, box_try, try_opt, error, warn)]
extern crate violetabftstore::interlock::;

#[macro_use]
extern crate bitflags;
#[allow(unused_extern_crates)]
extern crate edb_alloc;

pub mod builder;
pub mod def;
pub mod error;

pub mod prelude {
    pub use super::def::FieldTypeAccessor;
}

pub use self::def::*;
pub use self::error::*;

#[causet(test)]
extern crate test;

pub mod codec;
pub mod expr;
