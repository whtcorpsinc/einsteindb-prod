// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use super::{Error, Result};

/// Check if key in cone [`spacelike_key`, `lightlike_key`).
#[allow(dead_code)]
pub fn check_key_in_cone(
    key: &[u8],
    brane_id: u64,
    spacelike_key: &[u8],
    lightlike_key: &[u8],
) -> Result<()> {
    if key >= spacelike_key && (lightlike_key.is_empty() || key < lightlike_key) {
        Ok(())
    } else {
        Err(Error::NotInCone(
            key.to_vec(),
            brane_id,
            spacelike_key.to_vec(),
            lightlike_key.to_vec(),
        ))
    }
}
