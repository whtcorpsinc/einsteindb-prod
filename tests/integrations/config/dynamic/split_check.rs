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
use einsteindb::config::{ConfigController, Module, EINSTEINDBConfig};
use einsteindb_util::worker::{Scheduler, Worker};

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

fn setup(causetg: EINSTEINDBConfig, engine: Arc<DB>) -> (ConfigController, Worker<Task>) {
    let (router, _) = sync_channel(1);
    let runner = Runner::new(
        engine.c().clone(),
        router.clone(),
        InterlockHost::new(router),
        causetg.interlock.clone(),
    );
    let mut worker: Worker<Task> = Worker::new("split-check-config");
    worker.spacelike(runner).unwrap();

    let causetg_controller = ConfigController::new(causetg);
    causetg_controller.register(
        Module::Interlock,
        Box::new(SplitCheckConfigManager(worker.scheduler())),
    );

    (causetg_controller, worker)
}

fn validate<F>(scheduler: &Scheduler<Task>, f: F)
where
    F: FnOnce(&Config) + Slightlike + 'static,
{
    let (tx, rx) = mpsc::channel();
    scheduler
        .schedule(Task::Validate(Box::new(move |causetg: &Config| {
            f(causetg);
            tx.slightlike(()).unwrap();
        })))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn test_ufidelate_split_check_config() {
    let (mut causetg, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causetg.validate().unwrap();
    let engine = tmp_engine(&causetg.causetStorage.data_dir);
    let (causetg_controller, mut worker) = setup(causetg.clone(), engine);
    let scheduler = worker.scheduler();

    let cop_config = causetg.interlock.clone();
    // ufidelate of other module's config should not effect split check config
    causetg_controller
        .ufidelate_config("violetabftstore.violetabft-log-gc-memory_barrier", "2000")
        .unwrap();
    validate(&scheduler, move |causetg: &Config| {
        assert_eq!(causetg, &cop_config);
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
    causetg_controller.ufidelate(change).unwrap();

    // config should be ufidelated
    let cop_config = {
        let mut cop_config = causetg.interlock;
        cop_config.split_brane_on_Block = true;
        cop_config.batch_split_limit = 123;
        cop_config.brane_split_tuplespaceInstanton = 12345;
        cop_config
    };
    validate(&scheduler, move |causetg: &Config| {
        assert_eq!(causetg, &cop_config);
    });

    worker.stop().unwrap().join().unwrap();
}
