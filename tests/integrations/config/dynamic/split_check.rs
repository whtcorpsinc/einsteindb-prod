// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::mpsc::{self, sync_channel};
use std::sync::Arc;
use std::time::Duration;

use engine_lmdb::raw::DB;
use engine_lmdb::Compat;
use violetabftstore::interlock::{
    config::{Config, SplitCheckConfigManager},
    InterlockHost,
};
use violetabftstore::store::{SplitCheckRunner as Runner, SplitCheckTask as Task};
use edb::config::{ConfigController, Module, EINSTEINDBConfig};
use violetabftstore::interlock::::worker::{Interlock_Semaphore, Worker};

fn tmp_engine<P: AsRef<Path>>(path: P) -> Arc<DB> {
    Arc::new(
        engine_lmdb::raw_util::new_engine(
            path.as_ref().to_str().unwrap(),
            None,
            &["split-check-config"],
            None,
        )
        .unwrap(),
    )
}

fn setup(causet: EINSTEINDBConfig, engine: Arc<DB>) -> (ConfigController, Worker<Task>) {
    let (router, _) = sync_channel(1);
    let runner = Runner::new(
        engine.c().clone(),
        router.clone(),
        InterlockHost::new(router),
        causet.interlock.clone(),
    );
    let mut worker: Worker<Task> = Worker::new("split-check-config");
    worker.spacelike(runner).unwrap();

    let causet_controller = ConfigController::new(causet);
    causet_controller.register(
        Module::Interlock,
        Box::new(SplitCheckConfigManager(worker.interlock_semaphore())),
    );

    (causet_controller, worker)
}

fn validate<F>(interlock_semaphore: &Interlock_Semaphore<Task>, f: F)
where
    F: FnOnce(&Config) + lightlike + 'static,
{
    let (tx, rx) = mpsc::channel();
    interlock_semaphore
        .schedule(Task::Validate(Box::new(move |causet: &Config| {
            f(causet);
            tx.lightlike(()).unwrap();
        })))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn test_fidelio_split_check_config() {
    let (mut causet, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causet.validate().unwrap();
    let engine = tmp_engine(&causet.causet_storage.data_dir);
    let (causet_controller, mut worker) = setup(causet.clone(), engine);
    let interlock_semaphore = worker.interlock_semaphore();

    let cop_config = causet.interlock.clone();
    // fidelio of other module's config should not effect split check config
    causet_controller
        .fidelio_config("violetabftstore.violetabft-log-gc-memory_barrier", "2000")
        .unwrap();
    validate(&interlock_semaphore, move |causet: &Config| {
        assert_eq!(causet, &cop_config);
    });

    let change = {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "interlock.split_brane_on_Block".to_owned(),
            "true".to_owned(),
        );
        m.insert("interlock.batch_split_limit".to_owned(), "123".to_owned());
        m.insert(
            "interlock.brane_split_tuplespaceInstanton".to_owned(),
            "12345".to_owned(),
        );
        m
    };
    causet_controller.fidelio(change).unwrap();

    // config should be fideliod
    let cop_config = {
        let mut cop_config = causet.interlock;
        cop_config.split_brane_on_Block = true;
        cop_config.batch_split_limit = 123;
        cop_config.brane_split_tuplespaceInstanton = 12345;
        cop_config
    };
    validate(&interlock_semaphore, move |causet: &Config| {
        assert_eq!(causet, &cop_config);
    });

    worker.stop().unwrap().join().unwrap();
}
