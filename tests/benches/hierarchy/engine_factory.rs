//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::fmt;

use edb::causet_storage::{
    kv::{BTreeEngine, LmdbEngine},
    Engine, TestEngineBuilder,
};

pub trait EngineFactory<E: Engine>: Clone + Copy + fmt::Debug + 'static {
    fn build(&self) -> E;
}

#[derive(Clone, Copy)]
pub struct BTreeEngineFactory {}

impl EngineFactory<BTreeEngine> for BTreeEngineFactory {
    fn build(&self) -> BTreeEngine {
        BTreeEngine::default()
    }
}

impl fmt::Debug for BTreeEngineFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BTree")
    }
}

#[derive(Clone, Copy)]
pub struct LmdbEngineFactory {}

impl EngineFactory<LmdbEngine> for LmdbEngineFactory {
    fn build(&self) -> LmdbEngine {
        TestEngineBuilder::new().build().unwrap()
    }
}

impl fmt::Debug for LmdbEngineFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Lmdb")
    }
}
