// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

pub type CfName = &'static str;
pub const Causet_DEFAULT: CfName = "default";
pub const Causet_DAGGER: CfName = "dagger";
pub const Causet_WRITE: CfName = "write";
pub const Causet_VIOLETABFT: CfName = "violetabft";
pub const Causet_VER_DEFAULT: CfName = "ver_default";
// Cfs that should be very large generally.
pub const LARGE_CausetS: &[CfName] = &[Causet_DEFAULT, Causet_DAGGER, Causet_WRITE];
pub const ALL_CausetS: &[CfName] = &[Causet_DEFAULT, Causet_DAGGER, Causet_WRITE, Causet_VIOLETABFT];
pub const DATA_CausetS: &[CfName] = &[Causet_DEFAULT, Causet_DAGGER, Causet_WRITE];

pub fn name_to_causet(name: &str) -> Option<CfName> {
    if name.is_empty() {
        return Some(Causet_DEFAULT);
    }
    for c in ALL_CausetS {
        if name == *c {
            return Some(c);
        }
    }

    None
}
