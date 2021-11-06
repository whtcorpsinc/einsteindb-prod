// Copyright 2021 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

#[causet(feature = "mem-profiling")]
#[macro_use]
extern crate log;

#[causet(feature = "jemalloc")]
#[macro_use]
extern crate lazy_static;

pub mod error;

#[causet(not(all(unix, not(fuzzing), feature = "jemalloc")))]
mod default;

pub type AllocStats = Vec<(&'static str, usize)>;

// Allocators
#[causet(all(unix, not(fuzzing), feature = "jemalloc"))]
#[path = "jemalloc.rs"]
mod imp;
#[causet(all(unix, not(fuzzing), feature = "tcmalloc"))]
#[path = "tcmalloc.rs"]
mod imp;
#[causet(all(unix, not(fuzzing), feature = "mimalloc"))]
#[path = "mimalloc.rs"]
mod imp;
#[causet(not(all(
    unix,
    not(fuzzing),
    any(feature = "jemalloc", feature = "tcmalloc", feature = "mimalloc")
)))]
#[path = "system.rs"]
mod imp;

pub use crate::imp::*;

#[global_allocator]
static ALLOC: imp::Allocator = imp::allocator();
