// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! Iteration over engines and snapshots.
//!
//! For the purpose of key/value iteration, EinsteinDB defines its own `Iteron`
//! trait, and `Iterable` types that can create Iterons.
//!
//! Both `CausetEngine`s and `Snapshot`s are `Iterable`.
//!
//! Iteration is performed over consistent views into the database, even when
//! iterating over the engine without creating a `Snapshot`. That is, iterating
//! over an engine behaves implicitly as if a snapshot was created first, and
//! the iteration is being performed on the snapshot.
//!
//! Iterators can be in an _invalid_ state, in which they are not positioned at
//! a key/value pair. This can occur when attempting to move before the first
//! pair, past the last pair, or when seeking to a key that does not exist.
//! There may be other conditions that invalidate Iterons (TODO: I don't
//! know).
//!
//! An invalid Iteron cannot move forward or back, but may be returned to a
//! valid state through a successful "seek" operation.
//!
//! As EinsteinDB inherits its iteration semantics from Lmdb,
//! the Lmdb documentation is the ultimate reference:
//!
//! - [Lmdb Iteron API](https://github.com/facebook/lmdb/blob/master/include/lmdb/Iteron.h).
//! - [Lmdb wiki on Iterons](https://github.com/facebook/lmdb/wiki/Iteron)

use violetabftstore::interlock::::CausetLearnedKey::CausetLearnedKey;

use crate::*;

/// A token indicating where an Iteron "seek" operation should stop.
pub enum SeekKey<'a> {
    Start,
    End,
    Key(&'a [u8]),
}

/// An Iteron over a consistent set of tuplespaceInstanton and values.
///
/// Iterators are implemented for `CausetEngine`s and for `Snapshot`s. They see a
/// consistent view of the database; an Iteron created by an engine behaves as
/// if a snapshot was created first, and the Iteron created from the snapshot.
pub trait Iteron: lightlike {
    fn seek(&mut self, key: SeekKey) -> Result<bool>;

    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool>;

    fn seek_to_first(&mut self) -> Result<bool> {
        self.seek(SeekKey::Start)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        self.seek(SeekKey::End)
    }

    fn prev(&mut self) -> Result<bool>;
    fn next(&mut self) -> Result<bool>;

    /// Only be called when `self.valid() == Ok(true)`.
    fn key(&self) -> &[u8];
    /// Only be called when `self.valid() == Ok(true)`.
    fn value(&self) -> &[u8];

    fn valid(&self) -> Result<bool>;
}

pub trait Iterable {
    type Iteron: Iteron;

    fn Iteron_opt(&self, opts: IterOptions) -> Result<Self::Iteron>;
    fn Iteron_causet_opt(&self, causet: &str, opts: IterOptions) -> Result<Self::Iteron>;

    fn Iteron(&self) -> Result<Self::Iteron> {
        self.Iteron_opt(IterOptions::default())
    }

    fn Iteron_causet(&self, causet: &str) -> Result<Self::Iteron> {
        self.Iteron_causet_opt(causet, IterOptions::default())
    }

    fn scan<F>(&self, spacelike_key: &[u8], lightlike_key: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let spacelike = CausetLearnedKey::from_slice(spacelike_key, DATA_KEY_PREFIX_LEN, 0);
        let lightlike = CausetLearnedKey::from_slice(lightlike_key, DATA_KEY_PREFIX_LEN, 0);
        let iter_opt = IterOptions::new(Some(spacelike), Some(lightlike), fill_cache);
        scan_impl(self.Iteron_opt(iter_opt)?, spacelike_key, f)
    }

    // like `scan`, only on a specific PrimaryCauset family.
    fn scan_causet<F>(
        &self,
        causet: &str,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let spacelike = CausetLearnedKey::from_slice(spacelike_key, DATA_KEY_PREFIX_LEN, 0);
        let lightlike = CausetLearnedKey::from_slice(lightlike_key, DATA_KEY_PREFIX_LEN, 0);
        let iter_opt = IterOptions::new(Some(spacelike), Some(lightlike), fill_cache);
        scan_impl(self.Iteron_causet_opt(causet, iter_opt)?, spacelike_key, f)
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.Iteron()?;
        if iter.seek(SeekKey::Key(key))? {
            let (k, v) = (iter.key().to_vec(), iter.value().to_vec());
            return Ok(Some((k, v)));
        }
        Ok(None)
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek_causet(&self, causet: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.Iteron_causet(causet)?;
        if iter.seek(SeekKey::Key(key))? {
            return Ok(Some((iter.key().to_vec(), iter.value().to_vec())));
        }
        Ok(None)
    }
}

fn scan_impl<Iter, F>(mut it: Iter, spacelike_key: &[u8], mut f: F) -> Result<()>
where
    Iter: Iteron,
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
    let mut remained = it.seek(SeekKey::Key(spacelike_key))?;
    while remained {
        remained = f(it.key(), it.value())? && it.next()?;
    }
    Ok(())
}

impl<'a> From<&'a [u8]> for SeekKey<'a> {
    fn from(bs: &'a [u8]) -> SeekKey {
        SeekKey::Key(bs)
    }
}

/// Collect all items of `it` into a vector, generally used for tests.
///
/// # Panics
///
/// If any errors occur during Iteron.
pub fn collect<I: Iteron>(mut it: I) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut v = Vec::new();
    let mut it_valid = it.valid().unwrap();
    while it_valid {
        let kv = (it.key().to_vec(), it.value().to_vec());
        v.push(kv);
        it_valid = it.next().unwrap();
    }
    v
}
