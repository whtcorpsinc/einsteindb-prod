// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use engine_lmdb::LmdbEngine;
use ekvproto::violetabft_server_timeshare::VioletaBftMessage;
use violetabftstore::interlock::InterlockHost;
use violetabftstore::store::config::{Config, VioletaBftstoreConfigManager};
use violetabftstore::store::fsm::StoreMeta;
use violetabftstore::store::fsm::*;
use violetabftstore::store::{AutoSplitController, SnapManager, StoreMsg, Transport};
use violetabftstore::Result;
use edb::config::{ConfigController, Module, EINSTEINDBConfig};
use edb::import::SSTImporter;

use interlocking_directorate::ConcurrencyManager;
use edb::{Engines, ALL_CausetS};
use tempfile::TempDir;
use test_violetabftstore::TestFidelClient;
use violetabftstore::interlock::::config::VersionTrack;
use violetabftstore::interlock::::worker::{FutureWorker, Worker};

#[derive(Clone)]
struct MockTransport;
impl Transport for MockTransport {
    fn lightlike(&mut self, _: VioletaBftMessage) -> Result<()> {
        unimplemented!()
    }
    fn flush(&mut self) {
        unimplemented!()
    }
}

fn create_tmp_engine(dir: &TempDir) -> Engines<LmdbEngine, LmdbEngine> {
    let db = Arc::new(
        engine_lmdb::raw_util::new_engine(
            dir.path().join("db").to_str().unwrap(),
            None,
            ALL_CausetS,
            None,
        )
        .unwrap(),
    );
    let violetabft_db = Arc::new(
        engine_lmdb::raw_util::new_engine(
            dir.path().join("violetabft").to_str().unwrap(),
            None,
            &[],
            None,
        )
        .unwrap(),
    );
    Engines::new(LmdbEngine::from_db(db), LmdbEngine::from_db(violetabft_db))
}

fn spacelike_violetabftstore(
    causet: EINSTEINDBConfig,
    dir: &TempDir,
) -> (
    ConfigController,
    VioletaBftRouter<LmdbEngine, LmdbEngine>,
    ApplyRouter<LmdbEngine>,
    VioletaBftBatchSystem<LmdbEngine, LmdbEngine>,
) {
    let (violetabft_router, mut system) = create_violetabft_batch_system(&causet.violetabft_store);
    let engines = create_tmp_engine(dir);
    let host = InterlockHost::default();
    let importer = {
        let p = dir
            .path()
            .join("store-config-importer")
            .as_path()
            .display()
            .to_string();
        Arc::new(SSTImporter::new(&p, None).unwrap())
    };
    let snap_mgr = {
        let p = dir
            .path()
            .join("store-config-snp")
            .as_path()
            .display()
            .to_string();
        SnapManager::new(p)
    };
    let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
    let causet_track = Arc::new(VersionTrack::new(causet.violetabft_store.clone()));
    let causet_controller = ConfigController::new(causet);
    causet_controller.register(
        Module::VioletaBftstore,
        Box::new(VioletaBftstoreConfigManager(causet_track.clone())),
    );
    let fidel_worker = FutureWorker::new("store-config");

    system
        .spawn(
            Default::default(),
            causet_track,
            engines,
            MockTransport,
            Arc::new(TestFidelClient::new(0, true)),
            snap_mgr,
            fidel_worker,
            store_meta,
            host,
            importer,
            Worker::new("split"),
            AutoSplitController::default(),
            Arc::default(),
            ConcurrencyManager::new(1.into()),
        )
        .unwrap();
    (causet_controller, violetabft_router, system.apply_router(), system)
}

fn validate_store<F>(router: &VioletaBftRouter<LmdbEngine, LmdbEngine>, f: F)
where
    F: FnOnce(&Config) + lightlike + 'static,
{
    let (tx, rx) = mpsc::channel();
    router
        .lightlike_control(StoreMsg::Validate(Box::new(move |causet: &Config| {
            f(causet);
            tx.lightlike(()).unwrap();
        })))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[test]
fn test_fidelio_violetabftstore_config() {
    let (mut config, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    config.validate().unwrap();
    let (causet_controller, router, _, mut system) = spacelike_violetabftstore(config.clone(), &_dir);

    // dispatch fideliod config
    let change = {
        let mut m = std::collections::HashMap::new();
        m.insert("violetabftstore.messages-per-tick".to_owned(), "12345".to_owned());
        m.insert(
            "violetabftstore.violetabft-log-gc-memory_barrier".to_owned(),
            "54321".to_owned(),
        );
        m
    };
    causet_controller.fidelio(change).unwrap();

    // config should be fideliod
    let mut violetabft_store = config.violetabft_store;
    violetabft_store.messages_per_tick = 12345;
    violetabft_store.violetabft_log_gc_memory_barrier = 54321;
    validate_store(&router, move |causet: &Config| {
        assert_eq!(causet, &violetabft_store);
    });

    system.shutdown();
}
