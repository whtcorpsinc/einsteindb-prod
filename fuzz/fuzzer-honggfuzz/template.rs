// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! __FUZZ_GENERATE_COMMENT__

#[macro_use]
extern crate honggfuzz;
extern crate fuzz_targets;

use fuzz_targets::__FUZZ_CLI_TARGET__ as fuzz_target;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            let _ = fuzz_target(data);
        });
    }
}
