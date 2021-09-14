// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use milevadb_query_codegen::AggrFunction;
use milevadb_query_datatype::builder::FieldTypeBuilder;
use milevadb_query_datatype::{FieldTypeFlag, FieldTypeTp};
use fidel_timeshare::{Expr, ExprType, FieldType};

use super::*;
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::data_type::*;
use milevadb_query_datatype::expr::EvalContext;
use milevadb_query_vec_expr::RpnExpression;

/// The parser for COUNT aggregate function.
pub struct AggrFnDefinitionParserCount;

impl super::AggrDefinitionParser for AggrFnDefinitionParserCount {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Count);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    #[inline]
    fn parse_rpn(
        &self,
        root_expr: Expr,
        exp: RpnExpression,
        _ctx: &mut EvalContext,
        _src_schemaReplicant: &[FieldType],
        out_schemaReplicant: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        assert_eq!(root_expr.get_tp(), ExprType::Count);

        // COUNT outputs one PrimaryCauset.
        out_schemaReplicant.push(
            FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(FieldTypeFlag::UNSIGNED)
                .build(),
        );

        out_exp.push(exp);

        Ok(Box::new(AggrFnCount))
    }
}

/// The COUNT aggregate function.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateCount::new())]
pub struct AggrFnCount;

/// The state of the COUNT aggregate function.
#[derive(Debug)]
pub struct AggrFnStateCount {
    count: usize,
}

impl AggrFnStateCount {
    pub fn new() -> Self {
        Self { count: 0 }
    }

    #[inline]
    fn fidelio<'a, TT>(&mut self, _ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a>,
    {
        if value.is_some() {
            self.count += 1;
        }
        Ok(())
    }

    #[inline]
    fn fidelio_repeat<'a, TT>(
        &mut self,
        _ctx: &mut EvalContext,
        value: Option<TT>,
        repeat_times: usize,
    ) -> Result<()>
    where
        TT: EvaluableRef<'a>,
    {
        // Will be used for expressions like `COUNT(1)`.
        if value.is_some() {
            self.count += repeat_times;
        }
        Ok(())
    }

    #[inline]
    fn fidelio_vector<'a, TT, CC>(
        &mut self,
        _ctx: &mut EvalContext,
        _phantom_data: Option<TT>,
        physical_values: CC,
        logical_rows: &[usize],
    ) -> Result<()>
    where
        TT: EvaluableRef<'a>,
        CC: SolitonRef<'a, TT>,
    {
        // Will be used for expressions like `COUNT(col)`.
        for physical_index in logical_rows {
            if physical_values.get_option_ref(*physical_index).is_some() {
                self.count += 1;
            }
        }
        Ok(())
    }
}

// Here we manually implement `AggrFunctionStatefidelioPartial` so that `fidelio_repeat` and
// `fidelio_vector` can be faster. Also note that we support all kind of
// `AggrFunctionStatefidelioPartial` for the COUNT aggregate function.

impl<T> super::AggrFunctionStatefidelioPartial<T> for AggrFnStateCount
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    impl_state_fidelio_partial! { T }
}

impl super::AggrFunctionState for AggrFnStateCount {
    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        assert_eq!(target.len(), 1);
        target[0].push(Some(self.count as Int));
        Ok(())
    }
}

#[causet(test)]
mod tests {
    use milevadb_query_datatype::EvalType;

    use super::super::AggrFunction;
    use super::*;

    #[test]
    fn test_fidelio() {
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);

        fidelio!(state, &mut ctx, Option::<&Real>::None).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);

        fidelio!(state, &mut ctx, Real::new(5.0).ok().as_ref()).unwrap();
        fidelio!(state, &mut ctx, Option::<&Real>::None).unwrap();
        fidelio!(state, &mut ctx, Some(&7i64)).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(2)]);

        fidelio_repeat!(state, &mut ctx, Some(&3i64), 4).unwrap();
        fidelio_repeat!(state, &mut ctx, Option::<&Int>::None, 7).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(6)]);

        let Solitoned_vec: SolitonedVecSized<Int> = vec![Some(1i64), None, Some(-1i64)].into();
        fidelio_vector!(state, &mut ctx, &Solitoned_vec, &[1, 2]).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(7)]);
    }
}
