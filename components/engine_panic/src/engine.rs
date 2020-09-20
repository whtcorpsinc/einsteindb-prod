// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::db_vector::PanicDBVector;
use crate::snapshot::PanicSnapshot;
use crate::write_batch::PanicWriteBatch;
use engine_promises::{
    IterOptions, Iterable, Iteron, KvEngine, Peekable, ReadOptions, Result, SeekKey, SyncMutable,
    WriteOptions,
};

#[derive(Clone, Debug)]
pub struct PanicEngine;

impl KvEngine for PanicEngine {
    type Snapshot = PanicSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        panic!()
    }
    fn sync(&self) -> Result<()> {
        panic!()
    }
    fn bad_downcast<T: 'static>(&self) -> &T {
        panic!()
    }
}

impl Peekable for PanicEngine {
    type DBVector = PanicDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        panic!()
    }
    fn get_value_causet_opt(
        &self,
        opts: &ReadOptions,
        causet: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        panic!()
    }
}

impl SyncMutable for PanicEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }
    fn put_causet(&self, causet: &str, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_causet(&self, causet: &str, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_cone_causet(&self, causet: &str, begin_key: &[u8], lightlike_key: &[u8]) -> Result<()> {
        panic!()
    }
}

impl Iterable for PanicEngine {
    type Iteron = PanicEngineIterator;

    fn Iteron_opt(&self, opts: IterOptions) -> Result<Self::Iteron> {
        panic!()
    }
    fn Iteron_causet_opt(&self, causet: &str, opts: IterOptions) -> Result<Self::Iteron> {
        panic!()
    }
}

pub struct PanicEngineIterator;

impl Iteron for PanicEngineIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
        panic!()
    }

    fn prev(&mut self) -> Result<bool> {
        panic!()
    }
    fn next(&mut self) -> Result<bool> {
        panic!()
    }

    fn key(&self) -> &[u8] {
        panic!()
    }
    fn value(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> Result<bool> {
        panic!()
    }
}
