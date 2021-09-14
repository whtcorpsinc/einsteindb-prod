// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use violetabftstore::router::VioletaBftStoreBlackHole;
use std::f64::INFINITY;
use std::sync::mpsc::channel;
use std::time::Duration;
use edb::config::{ConfigController, Module, EINSTEINDBConfig};
use edb::server::gc_worker::GcConfig;
use edb::server::gc_worker::{GcTask, GcWorker};
use edb::causet_storage::kv::TestEngineBuilder;
use violetabftstore::interlock::::config::ReadableSize;
use violetabftstore::interlock::::time::Limiter;
use violetabftstore::interlock::::worker::FutureInterlock_Semaphore;

#[test]
fn test_gc_config_validate() {
    let causet = GcConfig::default();
    causet.validate().unwrap();

    let mut invalid_causet = GcConfig::default();
    invalid_causet.batch_tuplespaceInstanton = 0;
    assert!(invalid_causet.validate().is_err());
}

fn setup_causet_controller(
    causet: EINSTEINDBConfig,
) -> (
    GcWorker<edb::causet_storage::kv::LmdbEngine, VioletaBftStoreBlackHole>,
    ConfigController,
) {
    let engine = TestEngineBuilder::new().build().unwrap();
    let mut gc_worker = GcWorker::new(
        engine,
        VioletaBftStoreBlackHole,
        causet.gc.clone(),
        Default::default(),
    );
    gc_worker.spacelike().unwrap();

    let causet_controller = ConfigController::new(causet);
    causet_controller.register(Module::Gc, Box::new(gc_worker.get_config_manager()));

    (gc_worker, causet_controller)
}

fn validate<F>(interlock_semaphore: &FutureInterlock_Semaphore<GcTask>, f: F)
where
    F: FnOnce(&GcConfig, &Limiter) + lightlike + 'static,
{
    let (tx, rx) = channel();
    interlock_semaphore
        .schedule(GcTask::Validate(Box::new(
            move |causet: &GcConfig, limiter: &Limiter| {
                f(causet, limiter);
                tx.lightlike(()).unwrap();
            },
        )))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[allow(clippy::float_cmp)]
#[test]
fn test_gc_worker_config_fidelio() {
    let (mut causet, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causet.validate().unwrap();
    let (gc_worker, causet_controller) = setup_causet_controller(causet.clone());
    let interlock_semaphore = gc_worker.interlock_semaphore();

    // fidelio of other module's config should not effect gc worker config
    causet_controller
        .fidelio_config("violetabftstore.violetabft-log-gc-memory_barrier", "2000")
        .unwrap();
    validate(&interlock_semaphore, move |causet: &GcConfig, _| {
        assert_eq!(causet, &GcConfig::default());
    });

    // fidelio gc worker config
    let change = {
        let mut change = std::collections::HashMap::new();
        change.insert("gc.ratio-memory_barrier".to_owned(), "1.23".to_owned());
        change.insert("gc.batch-tuplespaceInstanton".to_owned(), "1234".to_owned());
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "1KB".to_owned());
        change.insert("gc.enable-compaction-filter".to_owned(), "true".to_owned());
        change
    };
    causet_controller.fidelio(change).unwrap();
    validate(&interlock_semaphore, move |causet: &GcConfig, _| {
        assert_eq!(causet.ratio_memory_barrier, 1.23);
        assert_eq!(causet.batch_tuplespaceInstanton, 1234);
        assert_eq!(causet.max_write_bytes_per_sec, ReadableSize::kb(1));
        assert!(causet.enable_compaction_filter);
    });
}

#[test]
#[allow(clippy::float_cmp)]
fn test_change_io_limit_by_config_manager() {
    let (mut causet, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causet.validate().unwrap();
    let (gc_worker, causet_controller) = setup_causet_controller(causet.clone());
    let interlock_semaphore = gc_worker.interlock_semaphore();

    validate(&interlock_semaphore, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });

    // Enable io iolimit
    causet_controller
        .fidelio_config("gc.max-write-bytes-per-sec", "1024")
        .unwrap();
    validate(&interlock_semaphore, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 1024.0);
    });

    // Change io iolimit
    causet_controller
        .fidelio_config("gc.max-write-bytes-per-sec", "2048")
        .unwrap();
    validate(&interlock_semaphore, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 2048.0);
    });

    // Disable io iolimit
    causet_controller
        .fidelio_config("gc.max-write-bytes-per-sec", "0")
        .unwrap();
    validate(&interlock_semaphore, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });
}

#[test]
#[allow(clippy::float_cmp)]
fn test_change_io_limit_by_debugger() {
    // Debugger use GcWorkerConfigManager to change io limit
    let (mut causet, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causet.validate().unwrap();
    let (gc_worker, _) = setup_causet_controller(causet);
    let interlock_semaphore = gc_worker.interlock_semaphore();
    let config_manager = gc_worker.get_config_manager();

    validate(&interlock_semaphore, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });

    // Enable io iolimit
    config_manager.fidelio(|causet: &mut GcConfig| causet.max_write_bytes_per_sec = ReadableSize(1024));
    validate(&interlock_semaphore, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 1024.0);
    });

    // Change io iolimit
    config_manager.fidelio(|causet: &mut GcConfig| causet.max_write_bytes_per_sec = ReadableSize(2048));
    validate(&interlock_semaphore, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 2048.0);
    });

    // Disable io iolimit
    config_manager.fidelio(|causet: &mut GcConfig| causet.max_write_bytes_per_sec = ReadableSize(0));
    validate(&interlock_semaphore, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });
}
