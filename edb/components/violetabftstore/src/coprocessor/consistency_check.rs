// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::marker::PhantomData;

use engine_promises::{KvEngine, Snapshot, CAUSET_VIOLETABFT};
use ekvproto::metapb::Brane;

use crate::interlock::{ConsistencyCheckMethod, Interlock};
use crate::Result;

pub trait ConsistencyCheckSemaphore<E: KvEngine>: Interlock {
    /// Ufidelate context. Return `true` if later semaphores should be skiped.
    fn ufidelate_context(&self, context: &mut Vec<u8>) -> bool;

    /// Compute hash for `brane`. The policy is extracted from `context`.
    fn compute_hash(
        &self,
        brane: &Brane,
        context: &mut &[u8],
        snap: &E::Snapshot,
    ) -> Result<Option<u32>>;
}

#[derive(Clone)]
pub struct Raw<E: KvEngine>(PhantomData<E>);

impl<E: KvEngine> Interlock for Raw<E> {}

impl<E: KvEngine> Default for Raw<E> {
    fn default() -> Raw<E> {
        Raw(Default::default())
    }
}

impl<E: KvEngine> ConsistencyCheckSemaphore<E> for Raw<E> {
    fn ufidelate_context(&self, context: &mut Vec<u8>) -> bool {
        context.push(ConsistencyCheckMethod::Raw as u8);
        // Raw consistency check is the most heavy and strong one.
        // So all others can be skiped.
        true
    }

    fn compute_hash(
        &self,
        brane: &ekvproto::metapb::Brane,
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
            digest.ufidelate(k);
            digest.ufidelate(v);
            Ok(true)
        })?;
    }

    // Computes the hash from the Brane state too.
    let brane_state_key = tuplespaceInstanton::brane_state_key(brane_id);
    digest.ufidelate(&brane_state_key);
    match snap.get_value_causet(CAUSET_VIOLETABFT, &brane_state_key) {
        Err(e) => return Err(e.into()),
        Ok(Some(v)) => digest.ufidelate(&v),
        Ok(None) => {}
    }
    Ok(digest.finalize())
}

#[causet(test)]
mod tests {
    use super::*;
    use engine_lmdb::LmdbEngine;

    #[test]
    fn test_ufidelate_context() {
        let mut context = Vec::new();
        let semaphore = Raw::<LmdbEngine>::default();
        assert!(semaphore.ufidelate_context(&mut context));
        assert_eq!(context.len(), 1);
        assert_eq!(context[0], ConsistencyCheckMethod::Raw as u8);
    }
}
