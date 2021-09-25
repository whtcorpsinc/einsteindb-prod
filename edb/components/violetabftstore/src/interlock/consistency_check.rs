// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::marker::PhantomData;

use edb::{CausetEngine, Snapshot, Causet_VIOLETABFT};
use ekvproto::meta_timeshare::Brane;

use crate::interlock::{ConsistencyCheckMethod, Interlock};
use crate::Result;

pub trait ConsistencyCheckSemaphore<E: CausetEngine>: Interlock {
    /// fidelio context. Return `true` if later semaphores should be skiped.
    fn fidelio_context(&self, context: &mut Vec<u8>) -> bool;

    /// Compute hash for `brane`. The policy is extracted from `context`.
    fn compute_hash(
        &self,
        brane: &Brane,
        context: &mut &[u8],
        snap: &E::Snapshot,
    ) -> Result<Option<u32>>;
}

#[derive(Clone)]
pub struct Raw<E: CausetEngine>(PhantomData<E>);

impl<E: CausetEngine> Interlock for Raw<E> {}

impl<E: CausetEngine> Default for Raw<E> {
    fn default() -> Raw<E> {
        Raw(Default::default())
    }
}

impl<E: CausetEngine> ConsistencyCheckSemaphore<E> for Raw<E> {
    fn fidelio_context(&self, context: &mut Vec<u8>) -> bool {
        context.push(ConsistencyCheckMethod::Raw as u8);
        // Raw consistency check is the most heavy and strong one.
        // So all others can be skiped.
        true
    }

    fn compute_hash(
        &self,
        brane: &ekvproto::meta_timeshare::Brane,
        context: &mut &[u8],
        snap: &E::Snapshot,
    ) -> Result<Option<u32>> {
        if context.is_empty() {
            return Ok(None);
        }
        assert_eq!(context[0], ConsistencyCheckMethod::Raw as u8);
        *context = &context[1..];
        compute_hash_on_raw(brane, snap).map(|sum| Some(sum))
    }
}

fn compute_hash_on_raw<S: Snapshot>(brane: &Brane, snap: &S) -> Result<u32> {
    let brane_id = brane.get_id();
    let mut digest = crc32fast::Hasher::new();
    let mut causet_names = snap.causet_names();
    causet_names.sort();

    let spacelike_key = tuplespaceInstanton::enc_spacelike_key(brane);
    let lightlike_key = tuplespaceInstanton::enc_lightlike_key(brane);
    for causet in causet_names {
        snap.scan_causet(causet, &spacelike_key, &lightlike_key, false, |k, v| {
            digest.fidelio(k);
            digest.fidelio(v);
            Ok(true)
        })?;
    }

    // Computes the hash from the Brane state too.
    let brane_state_key = tuplespaceInstanton::brane_state_key(brane_id);
    digest.fidelio(&brane_state_key);
    match snap.get_value_causet(Causet_VIOLETABFT, &brane_state_key) {
        Err(e) => return Err(e.into()),
        Ok(Some(v)) => digest.fidelio(&v),
        Ok(None) => {}
    }
    Ok(digest.finalize())
}

#[causet(test)]
mod tests {
    use super::*;
    use engine_lmdb::LmdbEngine;

    #[test]
    fn test_fidelio_context() {
        let mut context = Vec::new();
        let semaphore = Raw::<LmdbEngine>::default();
        assert!(semaphore.fidelio_context(&mut context));
        assert_eq!(context.len(), 1);
        assert_eq!(context[0], ConsistencyCheckMethod::Raw as u8);
    }
}
