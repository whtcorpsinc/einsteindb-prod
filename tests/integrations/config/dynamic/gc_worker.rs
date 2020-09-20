// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use violetabftstore::router::VioletaBftStoreBlackHole;
use std::f64::INFINITY;
use std::sync::mpsc::channel;
use std::time::Duration;
use einsteindb::config::{ConfigController, Module, EINSTEINDBConfig};
use einsteindb::server::gc_worker::GcConfig;
use einsteindb::server::gc_worker::{GcTask, GcWorker};
use einsteindb::causetStorage::kv::TestEngineBuilder;
use einsteindb_util::config::ReadableSize;
use einsteindb_util::time::Limiter;
use einsteindb_util::worker::FutureScheduler;

#[test]
fn test_gc_config_validate() {
    let causetg = GcConfig::default();
    causetg.validate().unwrap();

    let mut invalid_causetg = GcConfig::default();
    invalid_causetg.batch_tuplespaceInstanton = 0;
    assert!(invalid_causetg.validate().is_err());
}

fn setup_causetg_controller(
    causetg: EINSTEINDBConfig,
) -> (
    GcWorker<einsteindb::causetStorage::kv::LmdbEngine, VioletaBftStoreBlackHole>,
    ConfigController,
) {
    let engine = TestEngineBuilder::new().build().unwrap();
    let mut gc_worker = GcWorker::new(
        engine,
        VioletaBftStoreBlackHole,
        causetg.gc.clone(),
        Default::default(),
    );
    gc_worker.spacelike().unwrap();

    let causetg_controller = ConfigController::new(causetg);
    causetg_controller.register(Module::Gc, Box::new(gc_worker.get_config_manager()));

    (gc_worker, causetg_controller)
}

fn validate<F>(scheduler: &FutureScheduler<GcTask>, f: F)
where
    F: FnOnce(&GcConfig, &Limiter) + Slightlike + 'static,
{
    let (tx, rx) = channel();
    scheduler
        .schedule(GcTask::Validate(Box::new(
            move |causetg: &GcConfig, limiter: &Limiter| {
                f(causetg, limiter);
                tx.slightlike(()).unwrap();
            },
        )))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[allow(clippy::float_cmp)]
#[test]
fn test_gc_worker_config_ufidelate() {
    let (mut causetg, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causetg.validate().unwrap();
    let (gc_worker, causetg_controller) = setup_causetg_controller(causetg.clone());
    let scheduler = gc_worker.scheduler();

    // ufidelate of other module's config should not effect gc worker config
    causetg_controller
        .ufidelate_config("violetabftstore.violetabft-log-gc-memory_barrier", "2000")
        .unwrap();
    validate(&scheduler, move |causetg: &GcConfig, _| {
        assert_eq!(causetg, &GcConfig::default());
    });

    // Ufidelate gc worker config
    let change = {
        let mut change = std::collections::HashMap::new();
        change.insert("gc.ratio-memory_barrier".to_owned(), "1.23".to_owned());
        change.insert("gc.batch-tuplespaceInstanton".to_owned(), "1234".to_owned());
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "1KB".to_owned());
        change.insert("gc.enable-compaction-filter".to_owned(), "true".to_owned());
        change
    };
    causetg_controller.ufidelate(change).unwrap();
    validate(&scheduler, move |causetg: &GcConfig, _| {
        assert_eq!(causetg.ratio_memory_barrier, 1.23);
        assert_eq!(causetg.batch_tuplespaceInstanton, 1234);
        assert_eq!(causetg.max_write_bytes_per_sec, ReadableSize::kb(1));
        assert!(causetg.enable_compaction_filter);
    });
}

#[test]
#[allow(clippy::float_cmp)]
fn test_change_io_limit_by_config_manager() {
    let (mut causetg, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causetg.validate().unwrap();
    let (gc_worker, causetg_controller) = setup_causetg_controller(causetg.clone());
    let scheduler = gc_worker.scheduler();

    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });

    // Enable io iolimit
    causetg_controller
        .ufidelate_config("gc.max-write-bytes-per-sec", "1024")
        .unwrap();
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 1024.0);
    });

    // Change io iolimit
    causetg_controller
        .ufidelate_config("gc.max-write-bytes-per-sec", "2048")
        .unwrap();
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 2048.0);
    });

    // Disable io iolimit
    causetg_controller
        .ufidelate_config("gc.max-write-bytes-per-sec", "0")
        .unwrap();
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });
}

#[test]
#[allow(clippy::float_cmp)]
fn test_change_io_limit_by_debugger() {
    // Debugger use GcWorkerConfigManager to change io limit
    let (mut causetg, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causetg.validate().unwrap();
    let (gc_worker, _) = setup_causetg_controller(causetg);
    let scheduler = gc_worker.scheduler();
    let config_manager = gc_worker.get_config_manager();

    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });

    // Enable io iolimit
    config_manager.ufidelate(|causetg: &mut GcConfig| causetg.max_write_bytes_per_sec = ReadableSize(1024));
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 1024.0);
    });

    // Change io iolimit
    config_manager.ufidelate(|causetg: &mut GcConfig| causetg.max_write_bytes_per_sec = ReadableSize(2048));
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 2048.0);
    });

    // Disable io iolimit
    config_manager.ufidelate(|causetg: &mut GcConfig| causetg.max_write_bytes_per_sec = ReadableSize(0));
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });
}
