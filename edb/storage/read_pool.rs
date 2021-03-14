// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::config::StorageReadPoolConfig;
use crate::causetStorage::kv::{destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter};
use crate::causetStorage::metrics;
use std::sync::{Arc, Mutex};
use einsteindb_util::yatp_pool::{Config, DefaultTicker, FuturePool, PoolTicker, YatpPoolBuilder};

#[derive(Clone)]
struct FuturePoolTicker<R: FlowStatsReporter> {
    pub reporter: R,
}

impl<R: FlowStatsReporter> PoolTicker for FuturePoolTicker<R> {
    fn on_tick(&mut self) {
        metrics::tls_flush(&self.reporter);
    }
}

pub fn build_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &StorageReadPoolConfig,
    reporter: R,
    engine: E,
) -> Vec<FuturePool> {
    let names = vec!["store-read-low", "store-read-normal", "store-read-high"];
    let configs: Vec<Config> = config.to_yatp_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let reporter = reporter.clone();
            let engine = Arc::new(Mutex::new(engine.clone()));
            YatpPoolBuilder::new(FuturePoolTicker { reporter })
                .name_prefix(name)
                .config(config)
                .after_spacelike(move || set_tls_engine(engine.dagger().unwrap().clone()))
                .before_stop(move || unsafe {
                    // Safety: we call `set_` and `destroy_` with the same engine type.
                    destroy_tls_engine::<E>();
                })
                .build_future_pool()
        })
        .collect()
}

pub fn build_read_pool_for_test<E: Engine>(
    config: &StorageReadPoolConfig,
    engine: E,
) -> Vec<FuturePool> {
    let names = vec!["store-read-low", "store-read-normal", "store-read-high"];
    let configs: Vec<Config> = config.to_yatp_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let engine = Arc::new(Mutex::new(engine.clone()));
            YatpPoolBuilder::new(DefaultTicker::default())
                .config(config)
                .name_prefix(name)
                .after_spacelike(move || set_tls_engine(engine.dagger().unwrap().clone()))
                // Safety: we call `set_` and `destroy_` with the same engine type.
                .before_stop(|| unsafe { destroy_tls_engine::<E>() })
                .build_future_pool()
        })
        .collect()
}
