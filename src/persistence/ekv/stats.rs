// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::server::metrics::{GcTuplespaceInstantonCAUSET, GcTuplespaceInstantonDetail};
use engine_promises::{CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_WRITE};
use ekvproto::kvrpcpb::{ScanDetail, ScanInfo};
pub use violetabftstore::store::{FlowStatistics, FlowStatsReporter};

const STAT_PROCESSED_KEYS: &str = "processed_tuplespaceInstanton";
const STAT_GET: &str = "get";
const STAT_NEXT: &str = "next";
const STAT_PREV: &str = "prev";
const STAT_SEEK: &str = "seek";
const STAT_SEEK_FOR_PREV: &str = "seek_for_prev";
const STAT_OVER_SEEK_BOUND: &str = "over_seek_bound";
const STAT_NEXT_TOMBSTONE: &str = "next_tombstone";
const STAT_PREV_TOMBSTONE: &str = "prev_tombstone";
const STAT_SEEK_TOMBSTONE: &str = "seek_tombstone";
const STAT_SEEK_FOR_PREV_TOMBSTONE: &str = "seek_for_prev_tombstone";

/// Statistics collects the ops taken when fetching data.
#[derive(Default, Clone, Debug)]
pub struct CfStatistics {
    // How many tuplespaceInstanton that's visible to user
    pub processed_tuplespaceInstanton: usize,

    pub get: usize,
    pub next: usize,
    pub prev: usize,
    pub seek: usize,
    pub seek_for_prev: usize,
    pub over_seek_bound: usize,

    pub flow_stats: FlowStatistics,

    pub next_tombstone: usize,
    pub prev_tombstone: usize,
    pub seek_tombstone: usize,
    pub seek_for_prev_tombstone: usize,
}

impl CfStatistics {
    #[inline]
    pub fn total_op_count(&self) -> usize {
        self.get + self.next + self.prev + self.seek + self.seek_for_prev
    }

    pub fn details(&self) -> [(&'static str, usize); 11] {
        [
            (STAT_PROCESSED_KEYS, self.processed_tuplespaceInstanton),
            (STAT_GET, self.get),
            (STAT_NEXT, self.next),
            (STAT_PREV, self.prev),
            (STAT_SEEK, self.seek),
            (STAT_SEEK_FOR_PREV, self.seek_for_prev),
            (STAT_OVER_SEEK_BOUND, self.over_seek_bound),
            (STAT_NEXT_TOMBSTONE, self.next_tombstone),
            (STAT_PREV_TOMBSTONE, self.prev_tombstone),
            (STAT_SEEK_TOMBSTONE, self.seek_tombstone),
            (STAT_SEEK_FOR_PREV_TOMBSTONE, self.seek_for_prev_tombstone),
        ]
    }

    pub fn details_enum(&self) -> [(GcTuplespaceInstantonDetail, usize); 11] {
        [
            (GcTuplespaceInstantonDetail::processed_tuplespaceInstanton, self.processed_tuplespaceInstanton),
            (GcTuplespaceInstantonDetail::get, self.get),
            (GcTuplespaceInstantonDetail::next, self.next),
            (GcTuplespaceInstantonDetail::prev, self.prev),
            (GcTuplespaceInstantonDetail::seek, self.seek),
            (GcTuplespaceInstantonDetail::seek_for_prev, self.seek_for_prev),
            (GcTuplespaceInstantonDetail::over_seek_bound, self.over_seek_bound),
            (GcTuplespaceInstantonDetail::next_tombstone, self.next_tombstone),
            (GcTuplespaceInstantonDetail::prev_tombstone, self.prev_tombstone),
            (GcTuplespaceInstantonDetail::seek_tombstone, self.seek_tombstone),
            (
                GcTuplespaceInstantonDetail::seek_for_prev_tombstone,
                self.seek_for_prev_tombstone,
            ),
        ]
    }

    pub fn add(&mut self, other: &Self) {
        self.processed_tuplespaceInstanton = self.processed_tuplespaceInstanton.saturating_add(other.processed_tuplespaceInstanton);
        self.get = self.get.saturating_add(other.get);
        self.next = self.next.saturating_add(other.next);
        self.prev = self.prev.saturating_add(other.prev);
        self.seek = self.seek.saturating_add(other.seek);
        self.seek_for_prev = self.seek_for_prev.saturating_add(other.seek_for_prev);
        self.over_seek_bound = self.over_seek_bound.saturating_add(other.over_seek_bound);
        self.flow_stats.add(&other.flow_stats);
        self.next_tombstone = self.next_tombstone.saturating_add(other.next_tombstone);
        self.prev_tombstone = self.prev_tombstone.saturating_add(other.prev_tombstone);
        self.seek_tombstone = self.seek_tombstone.saturating_add(other.seek_tombstone);
        self.seek_for_prev_tombstone = self
            .seek_for_prev_tombstone
            .saturating_add(other.seek_for_prev_tombstone);
    }

    /// Deprecated
    pub fn scan_info(&self) -> ScanInfo {
        let mut info = ScanInfo::default();
        info.set_processed(self.processed_tuplespaceInstanton as i64);
        info.set_total(self.total_op_count() as i64);
        info
    }
}

#[derive(Default, Clone, Debug)]
pub struct Statistics {
    pub lock: CfStatistics,
    pub write: CfStatistics,
    pub data: CfStatistics,
}

impl Statistics {
    pub fn details(&self) -> [(&'static str, [(&'static str, usize); 11]); 3] {
        [
            (CAUSET_DEFAULT, self.data.details()),
            (CAUSET_DAGGER, self.lock.details()),
            (CAUSET_WRITE, self.write.details()),
        ]
    }

    pub fn details_enum(&self) -> [(GcTuplespaceInstantonCAUSET, [(GcTuplespaceInstantonDetail, usize); 11]); 3] {
        [
            (GcTuplespaceInstantonCAUSET::default, self.data.details_enum()),
            (GcTuplespaceInstantonCAUSET::lock, self.lock.details_enum()),
            (GcTuplespaceInstantonCAUSET::write, self.write.details_enum()),
        ]
    }

    pub fn add(&mut self, other: &Self) {
        self.lock.add(&other.lock);
        self.write.add(&other.write);
        self.data.add(&other.data);
    }

    /// Deprecated
    pub fn scan_detail(&self) -> ScanDetail {
        let mut detail = ScanDetail::default();
        detail.set_data(self.data.scan_info());
        detail.set_lock(self.lock.scan_info());
        detail.set_write(self.write.scan_info());
        detail
    }

    pub fn mut_causet_statistics(&mut self, causet: &str) -> &mut CfStatistics {
        if causet.is_empty() {
            return &mut self.data;
        }
        match causet {
            CAUSET_DEFAULT => &mut self.data,
            CAUSET_DAGGER => &mut self.lock,
            CAUSET_WRITE => &mut self.write,
            _ => unreachable!(),
        }
    }
}

#[derive(Default, Debug)]
pub struct StatisticsSummary {
    pub stat: Statistics,
    pub count: u64,
}

impl StatisticsSummary {
    pub fn add_statistics(&mut self, v: &Statistics) {
        self.stat.add(v);
        self.count += 1;
    }
}
