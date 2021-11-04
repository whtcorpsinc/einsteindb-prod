// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use fidel_timeshare::Limit;

use crate::{FreeDaemon, Event};
use milevadb_query_common::execute_stats::ExecuteStats;
use milevadb_query_common::causet_storage::IntervalCone;
use milevadb_query_common::Result;
use milevadb_query_datatype::expr::EvalWarnings;

/// Retrieves events from the source executor and only produces part of the events.
pub struct LimitFreeDaemon<Src: FreeDaemon> {
    limit: u64,
    cursor: u64,
    src: Src,
}

impl<Src: FreeDaemon> LimitFreeDaemon<Src> {
    pub fn new(limit: Limit, src: Src) -> Self {
        LimitFreeDaemon {
            limit: limit.get_limit(),
            cursor: 0,
            src,
        }
    }
}

impl<Src: FreeDaemon> FreeDaemon for LimitFreeDaemon<Src> {
    type StorageStats = Src::StorageStats;

    fn next(&mut self) -> Result<Option<Event>> {
        if self.cursor >= self.limit {
            return Ok(None);
        }
        if let Some(Evcausetidx) = self.src.next()? {
            self.cursor += 1;
            Ok(Some(Evcausetidx))
        } else {
            Ok(None)
        }
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
    fn get_len_of_PrimaryCausets(&self) -> usize {
        self.src.get_len_of_PrimaryCausets()
    }

    #[inline]
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        self.src.take_eval_warnings()
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
    use milevadb_query_datatype::codec::datum::Datum;
    use milevadb_query_datatype::FieldTypeTp;

    use super::super::tests::*;
    use super::*;

    #[test]
    fn test_limit_executor() {
        // prepare data and store
        let tid = 1;
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
        ];
        let raw_data = vec![
            vec![Datum::I64(1), Datum::Bytes(b"a".to_vec())],
            vec![Datum::I64(2), Datum::Bytes(b"b".to_vec())],
            vec![Datum::I64(3), Datum::Bytes(b"c".to_vec())],
            vec![Datum::I64(4), Datum::Bytes(b"d".to_vec())],
            vec![Datum::I64(5), Datum::Bytes(b"e".to_vec())],
            vec![Datum::I64(6), Datum::Bytes(b"f".to_vec())],
            vec![Datum::I64(7), Datum::Bytes(b"g".to_vec())],
        ];
        // prepare cone
        let cone1 = get_cone(tid, 0, 4);
        let cone2 = get_cone(tid, 5, 10);
        let key_cones = vec![cone1, cone2];
        let ts_ect = gen_Block_scan_executor(tid, cis, &raw_data, Some(key_cones));

        // init Limit meta
        let mut limit_meta = Limit::default();
        let limit = 5;
        limit_meta.set_limit(limit);
        // init topn executor
        let mut limit_ect = LimitFreeDaemon::new(limit_meta, ts_ect);
        let mut limit_rows = Vec::with_capacity(limit as usize);
        while let Some(Evcausetidx) = limit_ect.next().unwrap() {
            limit_rows.push(Evcausetidx.take_origin().unwrap());
        }
        assert_eq!(limit_rows.len(), limit as usize);
        let expect_row_handles = vec![1, 2, 3, 5, 6];
        for (Evcausetidx, handle) in limit_rows.iter().zip(expect_row_handles) {
            assert_eq!(Evcausetidx.handle, handle);
        }
    }
}
