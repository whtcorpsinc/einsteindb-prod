// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use fidelpb::FieldType;

use crate::interface::*;
use milevadb_query_common::causetStorage::IntervalCone;

/// A simple mock executor that will return batch data according to a fixture without any
/// modification.
///
/// Normally this should be only used in tests.
pub struct MockFreeDaemon {
    schema: Vec<FieldType>,
    results: std::vec::IntoIter<BatchExecuteResult>,
}

impl MockFreeDaemon {
    pub fn new(schema: Vec<FieldType>, results: Vec<BatchExecuteResult>) -> Self {
        assert!(!results.is_empty());
        Self {
            schema,
            results: results.into_iter(),
        }
    }
}

impl BatchFreeDaemon for MockFreeDaemon {
    type StorageStats = ();

    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        self.results.next().unwrap()
    }

    fn collect_exec_stats(&mut self, _dest: &mut ExecuteStats) {
        // Do nothing
    }

    fn collect_causetStorage_stats(&mut self, _dest: &mut Self::StorageStats) {
        // Do nothing
    }

    fn take_scanned_cone(&mut self) -> IntervalCone {
        // Do nothing
        unreachable!()
    }

    fn can_be_cached(&self) -> bool {
        false
    }
}
