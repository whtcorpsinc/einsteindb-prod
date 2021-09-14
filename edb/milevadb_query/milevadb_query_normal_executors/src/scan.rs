// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use ekvproto::interlock::KeyCone;
use fidel_timeshare::PrimaryCausetInfo;

use super::{FreeDaemon, Event};
use milevadb_query_common::execute_stats::ExecuteStats;
use milevadb_query_common::causet_storage::scanner::{ConesScanner, ConesScannerOptions};
use milevadb_query_common::causet_storage::{IntervalCone, Cone, causet_storage};
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::Block;
use milevadb_query_datatype::expr::{EvalContext, EvalWarnings};

// an InnerFreeDaemon is used in ScanFreeDaemon,
// hold the different logics between Block scan and index scan
pub trait InnerFreeDaemon: lightlike {
    fn decode_row(
        &self,
        ctx: &mut EvalContext,
        key: Vec<u8>,
        value: Vec<u8>,
        PrimaryCausets: Arc<Vec<PrimaryCausetInfo>>,
    ) -> Result<Option<Event>>;
}

// FreeDaemon for Block scan and index scan
pub struct ScanFreeDaemon<S: causet_storage, T: InnerFreeDaemon> {
    inner: T,
    context: EvalContext,
    scanner: ConesScanner<S>,
    PrimaryCausets: Arc<Vec<PrimaryCausetInfo>>,
}

pub struct ScanFreeDaemonOptions<S, T> {
    pub inner: T,
    pub context: EvalContext,
    pub PrimaryCausets: Vec<PrimaryCausetInfo>,
    pub key_cones: Vec<KeyCone>,
    pub causet_storage: S,
    pub is_backward: bool,
    pub is_key_only: bool,
    pub accept_point_cone: bool,
    pub is_scanned_cone_aware: bool,
}

impl<S: causet_storage, T: InnerFreeDaemon> ScanFreeDaemon<S, T> {
    pub fn new(
        ScanFreeDaemonOptions {
            inner,
            context,
            PrimaryCausets,
            mut key_cones,
            causet_storage,
            is_backward,
            is_key_only,
            accept_point_cone,
            is_scanned_cone_aware,
        }: ScanFreeDaemonOptions<S, T>,
    ) -> Result<Self> {
        box_try!(Block::check_Block_cones(&key_cones));
        if is_backward {
            key_cones.reverse();
        }

        let scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage,
            cones: key_cones
                .into_iter()
                .map(|r| Cone::from__timeshare_cone(r, accept_point_cone))
                .collect(),
            scan_backward_in_cone: is_backward,
            is_key_only,
            is_scanned_cone_aware,
        });

        Ok(Self {
            inner,
            context,
            scanner,
            PrimaryCausets: Arc::new(PrimaryCausets),
        })
    }
}

impl<S: causet_storage, T: InnerFreeDaemon> FreeDaemon for ScanFreeDaemon<S, T> {
    type StorageStats = S::Statistics;

    fn next(&mut self) -> Result<Option<Event>> {
        let some_row = self.scanner.next()?;
        if let Some((key, value)) = some_row {
            self.inner
                .decode_row(&mut self.context, key, value, self.PrimaryCausets.clone())
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.scanner
            .collect_scanned_rows_per_cone(&mut dest.scanned_rows_per_cone);
    }

    #[inline]
    fn collect_causet_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.scanner.collect_causet_storage_stats(dest);
    }

    #[inline]
    fn get_len_of_PrimaryCausets(&self) -> usize {
        self.PrimaryCausets.len()
    }

    #[inline]
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        None
    }

    #[inline]
    fn take_scanned_cone(&mut self) -> IntervalCone {
        self.scanner.take_scanned_cone()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.scanner.can_be_cached()
    }
}
