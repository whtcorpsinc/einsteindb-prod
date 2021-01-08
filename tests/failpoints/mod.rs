// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0
#![feature(box_TuringStrings)]
#![feature(test)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_failpoint_tests)]
#![recursion_limit = "100"]

#[macro_use]
extern crate slog_global;
extern crate test;

mod cases;
