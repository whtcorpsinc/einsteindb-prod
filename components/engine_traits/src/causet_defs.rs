// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

pub type CfName = &'static str;
pub const CAUSET_DEFAULT: CfName = "default";
pub const CAUSET_DAGGER: CfName = "lock";
pub const CAUSET_WRITE: CfName = "write";
pub const CAUSET_RAFT: CfName = "violetabft";
pub const CAUSET_VER_DEFAULT: CfName = "ver_default";
// Cfs that should be very large generally.
pub const LARGE_CAUSETS: &[CfName] = &[CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_WRITE];
pub const ALL_CAUSETS: &[CfName] = &[CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_WRITE, CAUSET_RAFT];
pub const DATA_CAUSETS: &[CfName] = &[CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_WRITE];

pub fn name_to_causet(name: &str) -> Option<CfName> {
    if name.is_empty() {
        return Some(CAUSET_DEFAULT);
    }
    for c in ALL_CAUSETS {
        if name == *c {
            return Some(c);
        }
    }

    None
}
