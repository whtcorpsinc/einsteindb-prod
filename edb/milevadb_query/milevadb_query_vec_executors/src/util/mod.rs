// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

pub mod aggr_executor;
pub mod hash_aggr_helper;
#[causet(test)]
pub mod mock_executor;
pub mod scan_executor;

use fidel_timeshare::FieldType;

use milevadb_query_common::Result;
use milevadb_query_datatype::codec::batch::LazyBatchPrimaryCausetVec;
use milevadb_query_datatype::expr::EvalContext;
use milevadb_query_vec_expr::RpnExpression;
use milevadb_query_vec_expr::RpnStackNode;

/// Decodes all PrimaryCausets that are not decoded.
pub fn ensure_PrimaryCausets_decoded(
    ctx: &mut EvalContext,
    exprs: &[RpnExpression],
    schemaReplicant: &[FieldType],
    input_physical_PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
    input_logical_rows: &[usize],
) -> Result<()> {
    for expr in exprs {
        expr.ensure_PrimaryCausets_decoded(ctx, schemaReplicant, input_physical_PrimaryCausets, input_logical_rows)?;
    }
    Ok(())
}

/// Evaluates expressions and outputs the result into the given Vec. Lifetime of the expressions
/// are erased.
pub unsafe fn eval_exprs_decoded_no_lifetime<'a>(
    ctx: &mut EvalContext,
    exprs: &[RpnExpression],
    schemaReplicant: &[FieldType],
    input_physical_PrimaryCausets: &LazyBatchPrimaryCausetVec,
    input_logical_rows: &[usize],
    output: &mut Vec<RpnStackNode<'a>>,
) -> Result<()> {
    unsafe fn erase_lifetime<'a, T: ?Sized>(v: &T) -> &'a T {
        &*(v as *const T)
    }

    for expr in exprs {
        output.push(erase_lifetime(expr).eval_decoded(
            ctx,
            erase_lifetime(schemaReplicant),
            erase_lifetime(input_physical_PrimaryCausets),
            erase_lifetime(input_logical_rows),
            input_logical_rows.len(),
        )?)
    }
    Ok(())
}
