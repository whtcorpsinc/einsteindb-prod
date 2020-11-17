// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

#[causetg(feature = "mem-profiling")]
#[macro_use]
extern crate log;

#[causetg(feature = "jemalloc")]
#[macro_use]
extern crate lazy_static;

pub mod error;

#[causetg(not(all(unix, not(fuzzing), feature = "jemalloc")))]
mod default;

pub type AllocStats = Vec<(&'static str, usize)>;

// Allocators
#[causetg(all(unix, not(fuzzing), feature = "jemalloc"))]
#[path = "jemalloc.rs"]
mod imp;
#[causetg(all(unix, not(fuzzing), feature = "tcmalloc"))]
#[path = "tcmalloc.rs"]
mod imp;
#[causetg(all(unix, not(fuzzing), feature = "mimalloc"))]
#[path = "mimalloc.rs"]
mod imp;
#[causetg(not(all(
    unix,
    not(fuzzing),
    any(feature = "jemalloc", feature = "tcmalloc", feature = "mimalloc")
)))]
#[path = "system.rs"]
mod imp;

pub use crate::imp::*;

#[global_allocator]
static ALLOC: imp::Allocator = imp::allocator();
