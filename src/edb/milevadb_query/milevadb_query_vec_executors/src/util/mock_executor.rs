// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use fidel_timeshare::FieldType;

use crate::interface::*;
use milevadb_query_common::causet_storage::IntervalCone;

/// A simple mock executor that will return batch data according to a fixture without any
/// modification.
///
/// Normally this should be only used in tests.
pub struct MockFreeDaemon {
    schemaReplicant: Vec<FieldType>,
    results: std::vec::IntoIter<BatchExecuteResult>,
}

impl MockFreeDaemon {
    pub fn new(schemaReplicant: Vec<FieldType>, results: Vec<BatchExecuteResult>) -> Self {
        assert!(!results.is_empty());
        Self {
            schemaReplicant,
            results: results.into_iter(),
        }
    }
}

impl BatchFreeDaemon for MockFreeDaemon {
    type StorageStats = ();

    fn schemaReplicant(&self) -> &[FieldType] {
        &self.schemaReplicant
    }

    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        self.results.next().unwrap()
    }

    fn collect_exec_stats(&mut self, _dest: &mut ExecuteStats) {
        // Do nothing
    }

    fn collect_causet_storage_stats(&mut self, _dest: &mut Self::StorageStats) {
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
