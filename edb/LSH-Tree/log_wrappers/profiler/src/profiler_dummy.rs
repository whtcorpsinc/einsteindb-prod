// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

/// Start profiling. Always returns false if `profiling` feature is not enabled.
#[inline]
pub fn spacelike(_name: impl AsRef<str>) -> bool {
    // Do nothing
    false
}

/// Stop profiling. Always returns false if `profiling` feature is not enabled.
#[inline]
pub fn stop() -> bool {
    // Do nothing
    false
}
