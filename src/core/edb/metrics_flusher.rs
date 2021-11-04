// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::io;
use std::result::Result;
use std::sync::mpsc::{self, lightlikeer};
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::{Duration, Instant};

use crate::violetabft_engine::VioletaBftEngine;

use crate::*;

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const FLUSHER_RESET_INTERVAL: Duration = Duration::from_millis(60_000);

pub struct MetricsFlusher<K: CausetEngine, R: VioletaBftEngine> {
    pub engines: Engines<K, R>,
    interval: Duration,
    handle: Option<JoinHandle<()>>,
    lightlikeer: Option<lightlikeer<bool>>,
}

impl<K: CausetEngine, R: VioletaBftEngine> MetricsFlusher<K, R> {
    pub fn new(engines: Engines<K, R>) -> Self {
        MetricsFlusher {
            engines,
            interval: DEFAULT_FLUSH_INTERVAL,
            handle: None,
            lightlikeer: None,
        }
    }

    pub fn set_flush_interval(&mut self, interval: Duration) {
        self.interval = interval;
    }

    pub fn spacelike(&mut self) -> Result<(), io::Error> {
        let (kv_db, violetabft_db) = (self.engines.kv.clone(), self.engines.violetabft.clone());
        let interval = self.interval;
        let (tx, rx) = mpsc::channel();
        self.lightlikeer = Some(tx);
        let h = ThreadBuilder::new()
            .name("metrics-flusher".to_owned())
            .spawn(move || {
                edb_alloc::add_thread_memory_accessor();
                let mut last_reset = Instant::now();
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(interval) {
                    kv_db.flush_metrics("kv");
                    violetabft_db.flush_metrics("violetabft");
                    if last_reset.elapsed() >= FLUSHER_RESET_INTERVAL {
                        kv_db.reset_statistics();
                        violetabft_db.reset_statistics();
                        last_reset = Instant::now();
                    }
                }
                edb_alloc::remove_thread_memory_accessor();
            })?;

        self.handle = Some(h);
        Ok(())
    }

    pub fn stop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }
        drop(self.lightlikeer.take().unwrap());
        if let Err(e) = h.unwrap().join() {
            error!("join metrics flusher failed"; "err" => ?e);
            return;
        }
    }
}
