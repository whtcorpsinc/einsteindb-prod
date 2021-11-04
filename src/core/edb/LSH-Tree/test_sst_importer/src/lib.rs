//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use engine_lmdb::LmdbEngine;
use engine_lmdb::LmdbSstReader;
pub use engine_lmdb::LmdbSstWriter;
use engine_lmdb::LmdbSstWriterBuilder;
use edb::CausetEngine;
use edb::SstReader;
use edb::SstWriter;
use edb::SstWriterBuilder;
use ekvproto::import_sst_timeshare::*;
use uuid::Uuid;

use engine_lmdb::raw::{
    PrimaryCausetNetworkOptions, DBEntryType, BlockPropertiesCollector, BlockPropertiesCollectorFactory,
};
use engine_lmdb::raw_util::{new_engine, CausetOptions};
use std::sync::Arc;

pub use engine_lmdb::LmdbEngine as TestEngine;

pub const PROP_TEST_MARKER_Causet_NAME: &[u8] = b"edb.test_marker_causet_name";

pub fn new_test_engine(path: &str, causets: &[&str]) -> LmdbEngine {
    new_test_engine_with_options(path, causets, |_, _| {})
}

pub fn new_test_engine_with_options<F>(path: &str, causets: &[&str], mut apply: F) -> LmdbEngine
where
    F: FnMut(&str, &mut PrimaryCausetNetworkOptions),
{
    let causet_opts = causets
        .iter()
        .map(|causet| {
            let mut opt = PrimaryCausetNetworkOptions::new();
            apply(*causet, &mut opt);
            opt.add_Block_properties_collector_factory(
                "edb.test_properties",
                Box::new(TestPropertiesCollectorFactory::new(*causet)),
            );
            CausetOptions::new(*causet, opt)
        })
        .collect();
    let db = new_engine(path, None, causets, Some(causet_opts)).expect("rocks test engine");
    LmdbEngine::from_db(Arc::new(db))
}

pub fn new_sst_reader(path: &str) -> LmdbSstReader {
    LmdbSstReader::open(path).expect("test sst reader")
}

pub fn new_sst_writer(path: &str) -> LmdbSstWriter {
    LmdbSstWriterBuilder::new()
        .build(path)
        .expect("test writer builder")
}

pub fn calc_data_crc32(data: &[u8]) -> u32 {
    let mut digest = crc32fast::Hasher::new();
    digest.fidelio(data);
    digest.finalize()
}

pub fn check_db_cone<E>(db: &E, cone: (u8, u8))
where
    E: CausetEngine,
{
    for i in cone.0..cone.1 {
        let k = tuplespaceInstanton::data_key(&[i]);
        assert_eq!(db.get_value(&k).unwrap().unwrap(), &[i]);
    }
}

pub fn gen_sst_file<P: AsRef<Path>>(path: P, cone: (u8, u8)) -> (SstMeta, Vec<u8>) {
    let mut w = LmdbSstWriterBuilder::new()
        .build(path.as_ref().to_str().unwrap())
        .unwrap();
    for i in cone.0..cone.1 {
        let k = tuplespaceInstanton::data_key(&[i]);
        w.put(&k, &[i]).unwrap();
    }
    w.finish().unwrap();

    read_sst_file(path, cone)
}

pub fn read_sst_file<P: AsRef<Path>>(path: P, cone: (u8, u8)) -> (SstMeta, Vec<u8>) {
    let data = fs::read(path).unwrap();
    let crc32 = calc_data_crc32(&data);

    let mut meta = SstMeta::default();
    meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    meta.mut_cone().set_spacelike(vec![cone.0]);
    meta.mut_cone().set_lightlike(vec![cone.1]);
    meta.set_crc32(crc32);
    meta.set_length(data.len() as u64);
    meta.set_causet_name("default".to_owned());

    (meta, data)
}

#[derive(Default)]
struct TestPropertiesCollectorFactory {
    causet: String,
}

impl TestPropertiesCollectorFactory {
    pub fn new(causet: impl Into<String>) -> Self {
        Self { causet: causet.into() }
    }
}

impl BlockPropertiesCollectorFactory for TestPropertiesCollectorFactory {
    fn create_Block_properties_collector(&mut self, _: u32) -> Box<dyn BlockPropertiesCollector> {
        Box::new(TestPropertiesCollector::new(self.causet.clone()))
    }
}

struct TestPropertiesCollector {
    causet: String,
}

impl TestPropertiesCollector {
    pub fn new(causet: String) -> Self {
        Self { causet }
    }
}

impl BlockPropertiesCollector for TestPropertiesCollector {
    fn add(&mut self, _: &[u8], _: &[u8], _: DBEntryType, _: u64, _: u64) {}

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        std::iter::once((
            PROP_TEST_MARKER_Causet_NAME.to_owned(),
            self.causet.as_bytes().to_owned(),
        ))
        .collect()
    }
}
