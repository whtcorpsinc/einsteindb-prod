// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use prometheus::{exponential_buckets, Histogram};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

lazy_static! {
    pub static ref APPLY_PROPOSAL: Histogram = register_histogram!(
        "edb_violetabftstore_apply_proposal",
        "The count of proposals sent by a brane at once",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
}

#[derive(Default)]
pub struct StoreStat {
    pub lock_causet_bytes_written: AtomicU64,
    pub engine_total_bytes_written: AtomicU64,
    pub engine_total_tuplespaceInstanton_written: AtomicU64,
    pub is_busy: AtomicBool,
}

#[derive(Clone, Default)]
pub struct GlobalStoreStat {
    pub stat: Arc<StoreStat>,
}

impl GlobalStoreStat {
    #[inline]
    pub fn local(&self) -> LocalStoreStat {
        LocalStoreStat {
            lock_causet_bytes_written: 0,
            engine_total_bytes_written: 0,
            engine_total_tuplespaceInstanton_written: 0,
            is_busy: false,

            global: self.clone(),
        }
    }
}

pub struct LocalStoreStat {
    pub lock_causet_bytes_written: u64,
    pub engine_total_bytes_written: u64,
    pub engine_total_tuplespaceInstanton_written: u64,
    pub is_busy: bool,

    global: GlobalStoreStat,
}

impl Clone for LocalStoreStat {
    #[inline]
    fn clone(&self) -> LocalStoreStat {
        self.global.local()
    }
}

impl LocalStoreStat {
    pub fn flush(&mut self) {
        if self.lock_causet_bytes_written != 0 {
            self.global
                .stat
                .lock_causet_bytes_written
                .fetch_add(self.lock_causet_bytes_written, Ordering::Relaxed);
            self.lock_causet_bytes_written = 0;
        }
        if self.engine_total_bytes_written != 0 {
            self.global
                .stat
                .engine_total_bytes_written
                .fetch_add(self.engine_total_bytes_written, Ordering::Relaxed);
            self.engine_total_bytes_written = 0;
        }
        if self.engine_total_tuplespaceInstanton_written != 0 {
            self.global
                .stat
                .engine_total_tuplespaceInstanton_written
                .fetch_add(self.engine_total_tuplespaceInstanton_written, Ordering::Relaxed);
            self.engine_total_tuplespaceInstanton_written = 0;
        }
        if self.is_busy {
            self.global.stat.is_busy.store(true, Ordering::Relaxed);
            self.is_busy = false;
        }
    }
}
