// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::Arc;

use fidel_timeshare::Selection;

use super::{FreeDaemon, ExprPrimaryCausetRefVisitor, Event};
use milevadb_query_common::execute_stats::ExecuteStats;
use milevadb_query_common::causet_storage::IntervalCone;
use milevadb_query_common::Result;
use milevadb_query_datatype::expr::{EvalConfig, EvalContext, EvalWarnings};
use milevadb_query_normal_expr::Expression;

/// Retrieves events from the source executor and filter events by expressions.
pub struct SelectionFreeDaemon<Src: FreeDaemon> {
    conditions: Vec<Expression>,
    related_cols_offset: Vec<usize>, // offset of related PrimaryCausets
    ctx: EvalContext,
    src: Src,
}

impl<Src: FreeDaemon> SelectionFreeDaemon<Src> {
    pub fn new(mut meta: Selection, eval_causet: Arc<EvalConfig>, src: Src) -> Result<Self> {
        let conditions: Vec<_> = meta.take_conditions().into();
        let mut visitor = ExprPrimaryCausetRefVisitor::new(src.get_len_of_PrimaryCausets());
        visitor.batch_visit(&conditions)?;
        let mut ctx = EvalContext::new(eval_causet);
        Ok(SelectionFreeDaemon {
            conditions: Expression::batch_build(&mut ctx, conditions)?,
            related_cols_offset: visitor.PrimaryCauset_offsets(),
            ctx,
            src,
        })
    }
}

impl<Src: FreeDaemon> FreeDaemon for SelectionFreeDaemon<Src> {
    type StorageStats = Src::StorageStats;

    fn next(&mut self) -> Result<Option<Event>> {
        'next: while let Some(Evcausetidx) = self.src.next()? {
            let Evcausetidx = Evcausetidx.take_origin()?;
            let cols = Evcausetidx.inflate_cols_with_offsets(&mut self.ctx, &self.related_cols_offset)?;
            for filter in &self.conditions {
                let val = filter.eval(&mut self.ctx, &cols)?;
                if !val.into_bool(&mut self.ctx)?.unwrap_or(false) {
                    continue 'next;
                }
            }
            return Ok(Some(Event::Origin(Evcausetidx)));
        }
        Ok(None)
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

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        if let Some(mut warnings) = self.src.take_eval_warnings() {
            warnings.merge(&mut self.ctx.take_warnings());
            Some(warnings)
        } else {
            Some(self.ctx.take_warnings())
        }
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
    use std::i64;
    use std::sync::Arc;

    use codec::prelude::NumberEncoder;
    use milevadb_query_datatype::FieldTypeTp;
    use fidel_timeshare::{Expr, ExprType, ScalarFuncSig};

    use super::super::tests::*;
    use super::*;
    use milevadb_query_datatype::codec::datum::Datum;

    fn new_const_expr() -> Expr {
        let mut expr = Expr::default();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::NullEqInt);
        expr.mut_children().push({
            let mut lhs = Expr::default();
            lhs.set_tp(ExprType::Null);
            lhs
        });
        expr.mut_children().push({
            let mut rhs = Expr::default();
            rhs.set_tp(ExprType::Null);
            rhs
        });
        expr
    }

    fn new_col_gt_u64_expr(offset: i64, val: u64) -> Expr {
        let mut expr = Expr::default();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::GtInt);
        expr.mut_children().push({
            let mut lhs = Expr::default();
            lhs.set_tp(ExprType::PrimaryCausetRef);
            lhs.mut_val().write_i64(offset).unwrap();
            lhs
        });
        expr.mut_children().push({
            let mut rhs = Expr::default();
            rhs.set_tp(ExprType::Uint64);
            rhs.mut_val().write_u64(val).unwrap();
            rhs
        });
        expr
    }

    #[test]
    fn test_selection_executor_simple() {
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
        ];
        let raw_data = vec![
            vec![
                Datum::I64(1),
                Datum::Bytes(b"a".to_vec()),
                Datum::Dec(7.into()),
            ],
            vec![
                Datum::I64(2),
                Datum::Bytes(b"b".to_vec()),
                Datum::Dec(7.into()),
            ],
            vec![
                Datum::I64(3),
                Datum::Bytes(b"b".to_vec()),
                Datum::Dec(8.into()),
            ],
            vec![
                Datum::I64(4),
                Datum::Bytes(b"d".to_vec()),
                Datum::Dec(3.into()),
            ],
            vec![
                Datum::I64(5),
                Datum::Bytes(b"f".to_vec()),
                Datum::Dec(5.into()),
            ],
            vec![
                Datum::I64(6),
                Datum::Bytes(b"e".to_vec()),
                Datum::Dec(9.into()),
            ],
            vec![
                Datum::I64(7),
                Datum::Bytes(b"f".to_vec()),
                Datum::Dec(6.into()),
            ],
        ];

        let causet_set_Block_scan = gen_Block_scan_executor(1, cis, &raw_data, None);

        // selection executor
        let mut selection = Selection::default();
        let expr = new_const_expr();
        selection.mut_conditions().push(expr);

        let mut selection_executor =
            SelectionFreeDaemon::new(selection, Arc::new(EvalConfig::default()), causet_set_Block_scan)
                .unwrap();

        let mut selection_rows = Vec::with_capacity(raw_data.len());
        while let Some(Evcausetidx) = selection_executor.next().unwrap() {
            selection_rows.push(Evcausetidx.take_origin().unwrap());
        }

        assert_eq!(selection_rows.len(), raw_data.len());
        let expect_row_handles = raw_data.iter().map(|r| r[0].i64()).collect::<Vec<_>>();
        let result_row = selection_rows.iter().map(|r| r.handle).collect::<Vec<_>>();
        assert_eq!(result_row, expect_row_handles);
    }

    #[test]
    fn test_selection_executor_condition() {
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::LongLong),
        ];
        let raw_data = vec![
            vec![Datum::I64(1), Datum::Bytes(b"a".to_vec()), Datum::I64(7)],
            vec![Datum::I64(2), Datum::Bytes(b"b".to_vec()), Datum::I64(7)],
            vec![Datum::I64(3), Datum::Bytes(b"b".to_vec()), Datum::I64(8)],
            vec![Datum::I64(4), Datum::Bytes(b"d".to_vec()), Datum::I64(3)],
            vec![Datum::I64(5), Datum::Bytes(b"f".to_vec()), Datum::I64(5)],
            vec![Datum::I64(6), Datum::Bytes(b"e".to_vec()), Datum::I64(9)],
            vec![Datum::I64(7), Datum::Bytes(b"f".to_vec()), Datum::I64(6)],
        ];

        let causet_set_Block_scan = gen_Block_scan_executor(1, cis, &raw_data, None);

        // selection executor
        let mut selection = Selection::default();
        let expr = new_col_gt_u64_expr(2, 5);
        selection.mut_conditions().push(expr);

        let mut selection_executor =
            SelectionFreeDaemon::new(selection, Arc::new(EvalConfig::default()), causet_set_Block_scan)
                .unwrap();

        let mut selection_rows = Vec::with_capacity(raw_data.len());
        while let Some(Evcausetidx) = selection_executor.next().unwrap() {
            selection_rows.push(Evcausetidx.take_origin().unwrap());
        }

        let expect_row_handles = raw_data
            .iter()
            .filter(|r| r[2].i64() > 5)
            .map(|r| r[0].i64())
            .collect::<Vec<_>>();
        assert!(expect_row_handles.len() < raw_data.len());
        assert_eq!(selection_rows.len(), expect_row_handles.len());
        let result_row = selection_rows.iter().map(|r| r.handle).collect::<Vec<_>>();
        assert_eq!(result_row, expect_row_handles);
        let expected_counts = vec![raw_data.len()];
        let mut exec_stats = ExecuteStats::new(0);
        selection_executor.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.scanned_rows_per_cone);
    }
}
