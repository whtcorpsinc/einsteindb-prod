// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use fidel_timeshare::FieldType;

use crate::interface::*;
use milevadb_query_common::causet_storage::IntervalCone;
use milevadb_query_common::Result;

/// FreeDaemon that retrieves events from the source executor
/// and only produces part of the events.
pub struct BatchLimitFreeDaemon<Src: BatchFreeDaemon> {
    src: Src,
    remaining_rows: usize,
}

impl<Src: BatchFreeDaemon> BatchLimitFreeDaemon<Src> {
    pub fn new(src: Src, limit: usize) -> Result<Self> {
        Ok(Self {
            src,
            remaining_rows: limit,
        })
    }
}

impl<Src: BatchFreeDaemon> BatchFreeDaemon for BatchLimitFreeDaemon<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schemaReplicant(&self) -> &[FieldType] {
        self.src.schemaReplicant()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let mut result = self.src.next_batch(scan_rows);
        if result.logical_rows.len() < self.remaining_rows {
            self.remaining_rows -= result.logical_rows.len();
        } else {
            // We don't need to touch the physical data.
            result.logical_rows.truncate(self.remaining_rows);
            result.is_drained = Ok(true);
            self.remaining_rows = 0;
        }

        result
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.src.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_causet_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.src.collect_causet_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_cone(&mut self) -> IntervalCone {
        self.src.take_scanned_cone()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.src.can_be_cached()
    }
}

#[causet(test)]
mod tests {
    use super::*;

    use milevadb_query_datatype::FieldTypeTp;

    use crate::util::mock_executor::MockFreeDaemon;
    use milevadb_query_datatype::codec::batch::LazyBatchPrimaryCausetVec;
    use milevadb_query_datatype::codec::data_type::VectorValue;
    use milevadb_query_datatype::expr::EvalWarnings;

    #[test]
    fn test_limit_0() {
        let src_exec = MockFreeDaemon::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![BatchExecuteResult {
                physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![VectorValue::Int(
                    vec![None, Some(50), None].into(),
                )]),
                logical_rows: vec![1, 2],
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            }],
        );

        let mut exec = BatchLimitFreeDaemon::new(src_exec, 0).unwrap();

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_PrimaryCausets.rows_len(), 3);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_error_before_limit() {
        let src_exec = MockFreeDaemon::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![BatchExecuteResult {
                physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![VectorValue::Int(
                    vec![None, Some(50), None].into(),
                )]),
                logical_rows: vec![1, 2],
                warnings: EvalWarnings::default(),
                is_drained: Err(other_err!("foo")),
            }],
        );

        let mut exec = BatchLimitFreeDaemon::new(src_exec, 10).unwrap();

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_PrimaryCausets.rows_len(), 3);
        assert!(r.is_drained.is_err());
    }

    #[test]
    fn test_drain_before_limit() {
        let src_exec = MockFreeDaemon::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![VectorValue::Int(
                        vec![Some(-5), None, None].into(),
                    )]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None].into(),
                    )]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        let mut exec = BatchLimitFreeDaemon::new(src_exec, 10).unwrap();

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_PrimaryCausets.rows_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_PrimaryCausets.rows_len(), 3);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_error_when_limit() {
        let src_exec = MockFreeDaemon::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![VectorValue::Int(
                        vec![Some(-5), Some(-1), None].into(),
                    )]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None].into(),
                    )]),
                    logical_rows: vec![0, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Err(other_err!("foo")),
                },
            ],
        );

        let mut exec = BatchLimitFreeDaemon::new(src_exec, 4).unwrap();

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_PrimaryCausets.rows_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0, 2]);
        assert_eq!(r.physical_PrimaryCausets.rows_len(), 3);
        assert!(r.is_drained.unwrap()); // No errors
    }

    #[test]
    fn test_drain_after_limit() {
        let src_exec = MockFreeDaemon::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![VectorValue::Int(
                        vec![Some(-5), Some(-1), None].into(),
                    )]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None, None, Some(1)].into(),
                    )]),
                    logical_rows: vec![0, 4, 1, 3],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        let mut exec = BatchLimitFreeDaemon::new(src_exec, 4).unwrap();

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_PrimaryCausets.rows_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0, 4]);
        assert_eq!(r.physical_PrimaryCausets.rows_len(), 5);
        assert!(r.is_drained.unwrap());
    }
}
