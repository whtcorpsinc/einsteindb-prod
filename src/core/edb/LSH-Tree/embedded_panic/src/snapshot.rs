// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::db_vector::PanicDBVector;
use crate::edb::PanicEngine;
use edb::{
    IterOptions, Iterable, Iteron, Peekable, ReadOptions, Result, SeekKey, Snapshot,
};
use std::ops::Deref;

#[derive(Clone, Debug)]
pub struct PanicSnapshot;

impl Snapshot for PanicSnapshot {
    fn causet_names(&self) -> Vec<&str> {
        panic!()
    }
}

impl Peekable for PanicSnapshot {
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

impl Iterable for PanicSnapshot {
    type Iteron = PanicSnapshotIterator;

    fn Iteron_opt(&self, opts: IterOptions) -> Result<Self::Iteron> {
        panic!()
    }
    fn Iteron_causet_opt(&self, causet: &str, opts: IterOptions) -> Result<Self::Iteron> {
        panic!()
    }
}

pub struct PanicSnapshotIterator;

impl Iteron for PanicSnapshotIterator {
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
