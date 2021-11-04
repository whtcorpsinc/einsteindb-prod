// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use edb::{self, IterOptions, Iterable, Peekable, ReadOptions, Result, Snapshot};
use lmdb::lmdb_options::UnsafeSnap;
use lmdb::{DBIterator, DB};

use crate::db_vector::LmdbDBVector;
use crate::options::LmdbReadOptions;
use crate::util::get_causet_handle;
use crate::LmdbEngineIterator;

pub struct LmdbSnapshot {
    db: Arc<DB>,
    snap: UnsafeSnap,
}

unsafe impl lightlike for LmdbSnapshot {}
unsafe impl Sync for LmdbSnapshot {}

impl LmdbSnapshot {
    pub fn new(db: Arc<DB>) -> Self {
        unsafe {
            LmdbSnapshot {
                snap: db.unsafe_snap(),
                db,
            }
        }
    }
}

impl Snapshot for LmdbSnapshot {
    fn causet_names(&self) -> Vec<&str> {
        self.db.causet_names()
    }
}

impl Debug for LmdbSnapshot {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(fmt, "Engine Snapshot Impl")
    }
}

impl Drop for LmdbSnapshot {
    fn drop(&mut self) {
        unsafe {
            self.db.release_snap(&self.snap);
        }
    }
}

impl Iterable for LmdbSnapshot {
    type Iteron = LmdbEngineIterator;

    fn Iteron_opt(&self, opts: IterOptions) -> Result<Self::Iteron> {
        let opt: LmdbReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(LmdbEngineIterator::from_raw(DBIterator::new(
            self.db.clone(),
            opt,
        )))
    }

    fn Iteron_causet_opt(&self, causet: &str, opts: IterOptions) -> Result<Self::Iteron> {
        let opt: LmdbReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = get_causet_handle(self.db.as_ref(), causet)?;
        Ok(LmdbEngineIterator::from_raw(DBIterator::new_causet(
            self.db.clone(),
            handle,
            opt,
        )))
    }
}

impl Peekable for LmdbSnapshot {
    type DBVector = LmdbDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<LmdbDBVector>> {
        let opt: LmdbReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_opt(key, &opt)?;
        Ok(v.map(LmdbDBVector::from_raw))
    }

    fn get_value_causet_opt(
        &self,
        opts: &ReadOptions,
        causet: &str,
        key: &[u8],
    ) -> Result<Option<LmdbDBVector>> {
        let opt: LmdbReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = get_causet_handle(self.db.as_ref(), causet)?;
        let v = self.db.get_causet_opt(handle, key, &opt)?;
        Ok(v.map(LmdbDBVector::from_raw))
    }
}
