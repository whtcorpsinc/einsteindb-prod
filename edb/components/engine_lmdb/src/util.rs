// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_options::LmdbPrimaryCausetNetworkOptions;
use crate::db_options::LmdbDBOptions;
use crate::engine::LmdbEngine;
use crate::raw_util::new_engine as new_engine_raw;
use crate::raw_util::new_engine_opt as new_engine_opt_raw;
use crate::raw_util::CAUSETOptions;
use crate::rocks_metrics_defs::*;
use engine_promises::Engines;
use engine_promises::Cone;
use engine_promises::CAUSET_DEFAULT;
use engine_promises::{Error, Result};
use lmdb::Cone as LmdbCone;
use lmdb::{CAUSETHandle, SliceTransform, DB};
use std::str::FromStr;
use std::sync::Arc;

pub fn new_temp_engine(path: &tempfile::TempDir) -> Engines<LmdbEngine, LmdbEngine> {
    let violetabft_path = path.path().join(std::path::Path::new("violetabft"));
    Engines::new(
        new_engine(
            path.path().to_str().unwrap(),
            None,
            engine_promises::ALL_CAUSETS,
            None,
        )
        .unwrap(),
        new_engine(
            violetabft_path.to_str().unwrap(),
            None,
            &[engine_promises::CAUSET_DEFAULT],
            None,
        )
        .unwrap(),
    )
}

pub fn new_default_engine(path: &str) -> Result<LmdbEngine> {
    let engine =
        new_engine_raw(path, None, &[CAUSET_DEFAULT], None).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = LmdbEngine::from_db(engine);
    Ok(engine)
}

pub struct LmdbCAUSETOptions<'a> {
    causet: &'a str,
    options: LmdbPrimaryCausetNetworkOptions,
}

impl<'a> LmdbCAUSETOptions<'a> {
    pub fn new(causet: &'a str, options: LmdbPrimaryCausetNetworkOptions) -> LmdbCAUSETOptions<'a> {
        LmdbCAUSETOptions { causet, options }
    }

    pub fn into_raw(self) -> CAUSETOptions<'a> {
        CAUSETOptions::new(self.causet, self.options.into_raw())
    }
}

pub fn new_engine(
    path: &str,
    db_opts: Option<LmdbDBOptions>,
    causets: &[&str],
    opts: Option<Vec<LmdbCAUSETOptions<'_>>>,
) -> Result<LmdbEngine> {
    let db_opts = db_opts.map(LmdbDBOptions::into_raw);
    let opts = opts.map(|o| o.into_iter().map(LmdbCAUSETOptions::into_raw).collect());
    let engine = new_engine_raw(path, db_opts, causets, opts).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = LmdbEngine::from_db(engine);
    Ok(engine)
}

pub fn new_engine_opt(
    path: &str,
    db_opt: LmdbDBOptions,
    causets_opts: Vec<LmdbCAUSETOptions<'_>>,
) -> Result<LmdbEngine> {
    let db_opt = db_opt.into_raw();
    let causets_opts = causets_opts.into_iter().map(LmdbCAUSETOptions::into_raw).collect();
    let engine =
        new_engine_opt_raw(path, db_opt, causets_opts).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = LmdbEngine::from_db(engine);
    Ok(engine)
}

pub fn get_causet_handle<'a>(db: &'a DB, causet: &str) -> Result<&'a CAUSETHandle> {
    let handle = db
        .causet_handle(causet)
        .ok_or_else(|| Error::Engine(format!("causet {} not found", causet)))?;
    Ok(handle)
}

pub fn cone_to_rocks_cone<'a>(cone: &Cone<'a>) -> LmdbCone<'a> {
    LmdbCone::new(cone.spacelike_key, cone.lightlike_key)
}

pub fn get_engine_causet_used_size(engine: &DB, handle: &CAUSETHandle) -> u64 {
    let mut causet_used_size = engine
        .get_property_int_causet(handle, LMDB_TOTAL_SST_FILES_SIZE)
        .expect("lmdb is too old, missing total-sst-files-size property");
    // For memBlock
    if let Some(mem_Block) = engine.get_property_int_causet(handle, LMDB_CUR_SIZE_ALL_MEM_BlockS) {
        causet_used_size += mem_Block;
    }
    // For blob files
    if let Some(live_blob) = engine.get_property_int_causet(handle, LMDB_TITANDB_LIVE_BLOB_FILE_SIZE)
    {
        causet_used_size += live_blob;
    }
    if let Some(obsolete_blob) =
        engine.get_property_int_causet(handle, LMDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE)
    {
        causet_used_size += obsolete_blob;
    }

    causet_used_size
}

/// Gets engine's compression ratio at given level.
pub fn get_engine_compression_ratio_at_level(
    engine: &DB,
    handle: &CAUSETHandle,
    level: usize,
) -> Option<f64> {
    let prop = format!("{}{}", LMDB_COMPRESSION_RATIO_AT_LEVEL, level);
    if let Some(v) = engine.get_property_value_causet(handle, &prop) {
        if let Ok(f) = f64::from_str(&v) {
            // Lmdb returns -1.0 if the level is empty.
            if f >= 0.0 {
                return Some(f);
            }
        }
    }
    None
}

/// Gets the number of files at given level of given PrimaryCauset family.
pub fn get_causet_num_files_at_level(engine: &DB, handle: &CAUSETHandle, level: usize) -> Option<u64> {
    let prop = format!("{}{}", LMDB_NUM_FILES_AT_LEVEL, level);
    engine.get_property_int_causet(handle, &prop)
}

/// Gets the number of blob files at given level of given PrimaryCauset family.
pub fn get_causet_num_blob_files_at_level(engine: &DB, handle: &CAUSETHandle, level: usize) -> Option<u64> {
    let prop = format!("{}{}", LMDB_TITANDB_NUM_BLOB_FILES_AT_LEVEL, level);
    engine.get_property_int_causet(handle, &prop)
}

/// Gets the number of immuBlock mem-Block of given PrimaryCauset family.
pub fn get_num_immuBlock_mem_Block(engine: &DB, handle: &CAUSETHandle) -> Option<u64> {
    engine.get_property_int_causet(handle, LMDB_NUM_IMMUBlock_MEM_Block)
}

pub struct FixedSuffixSliceTransform {
    pub suffix_len: usize,
}

impl FixedSuffixSliceTransform {
    pub fn new(suffix_len: usize) -> FixedSuffixSliceTransform {
        FixedSuffixSliceTransform { suffix_len }
    }
}

impl SliceTransform for FixedSuffixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        let mid = key.len() - self.suffix_len;
        let (left, _) = key.split_at(mid);
        left
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.suffix_len
    }

    fn in_cone(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct FixedPrefixSliceTransform {
    pub prefix_len: usize,
}

impl FixedPrefixSliceTransform {
    pub fn new(prefix_len: usize) -> FixedPrefixSliceTransform {
        FixedPrefixSliceTransform { prefix_len }
    }
}

impl SliceTransform for FixedPrefixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        &key[..self.prefix_len]
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.prefix_len
    }

    fn in_cone(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct NoopSliceTransform;

impl SliceTransform for NoopSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        key
    }

    fn in_domain(&mut self, _: &[u8]) -> bool {
        true
    }

    fn in_cone(&mut self, _: &[u8]) -> bool {
        true
    }
}
