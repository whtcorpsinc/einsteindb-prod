// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

#![feature(test)]

extern crate test;

mod interlock;
mod CausetLearnedKey;
mod violetabft;
mod serialization;
mod causet_storage;
mod util;
mod writebatch;

#[bench]
fn _bench_check_requirement(_: &mut test::Bencher) {
    violetabftstore::interlock::::config::check_max_open_fds(4096).unwrap();
}
