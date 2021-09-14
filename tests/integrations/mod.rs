// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

#![feature(test)]
#![feature(box_TuringStrings)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_tests)]

extern crate test;

extern crate encryption;
#[macro_use]
extern crate violetabftstore::interlock::;
extern crate fidel_client;

mod config;
mod interlock;
mod import;
mod fidel;
mod violetabftstore;
mod server;
mod server_encryption;
mod causet_storage;
