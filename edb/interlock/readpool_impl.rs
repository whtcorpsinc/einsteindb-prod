// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use crate::config::CoprReadPoolConfig;
use crate::causet_storage::kv::{destroy_tls_engine, set_tls_engine};
use crate::causet_storage::{Engine, FlowStatsReporter};
use violetabftstore::interlock::::yatp_pool::{Config, DefaultTicker, FuturePool, PoolTicker, YatpPoolBuilder};

use super::metrics::*;

#[derive(Clone)]
struct FuturePoolTicker<R: FlowStatsReporter> {
    pub reporter: R,
}

impl<R: FlowStatsReporter> PoolTicker for FuturePoolTicker<R> {
    fn on_tick(&mut self) {
        tls_flush(&self.reporter);
    }
}

pub fn build_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &CoprReadPoolConfig,
    reporter: R,
    engine: E,
) -> Vec<FuturePool> {
    let names = vec!["causet-low", "causet-normal", "causet-high"];
    let configs: Vec<Config> = config.to_yatp_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let reporter = reporter.clone();
            let engine = Arc::new(Mutex::new(engine.clone()));
            YatpPoolBuilder::new(FuturePoolTicker { reporter })
                .config(config)
                .name_prefix(name)
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
    config: &CoprReadPoolConfig,
    engine: E,
) -> Vec<FuturePool> {
    let configs: Vec<Config> = config.to_yatp_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .map(|config| {
            let engine = Arc::new(Mutex::new(engine.clone()));
            YatpPoolBuilder::new(DefaultTicker::default())
                .config(config)
                .after_spacelike(move || set_tls_engine(engine.dagger().unwrap().clone()))
                // Safety: we call `set_` and `destroy_` with the same engine type.
                .before_stop(|| unsafe { destroy_tls_engine::<E>() })
                .build_future_pool()
        })
        .collect()
}
