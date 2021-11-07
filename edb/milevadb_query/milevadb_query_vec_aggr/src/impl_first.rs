// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use milevadb_query_codegen::AggrFunction;
use milevadb_query_datatype::EvalType;
use fidel_timeshare::{Expr, ExprType, FieldType};

use super::*;
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::data_type::*;
use milevadb_query_datatype::expr::EvalContext;
use milevadb_query_vec_expr::RpnExpression;

/// The parser for FIRST aggregate function.
pub struct AggrFnDefinitionParserFirst;

impl super::AggrDefinitionParser for AggrFnDefinitionParserFirst {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::First);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    #[inline]
    fn parse_rpn(
        &self,
        mut root_expr: Expr,
        exp: RpnExpression,
        _ctx: &mut EvalContext,
        src_schemaReplicant: &[FieldType],
        out_schemaReplicant: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        use std::convert::TryFrom;
        use milevadb_query_datatype::FieldTypeAccessor;

        assert_eq!(root_expr.get_tp(), ExprType::First);

        let eval_type =
            EvalType::try_from(exp.ret_field_type(src_schemaReplicant).as_accessor().tp()).unwrap();

        let out_ft = root_expr.take_field_type();

        let out_et = box_try!(EvalType::try_from(out_ft.as_accessor().tp()));

        if out_et != eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                out_ft.as_accessor().tp()
            ));
        }

        // FIRST outputs one PrimaryCauset with the same type as its child
        out_schemaReplicant.push(out_ft);
        out_exp.push(exp);

        match_template::match_template! {
            TT = [Int, Real, Duration, Decimal, DateTime],
            match eval_type {
                EvalType::TT => Ok(Box::new(AggrFnFirst::<&'static TT>::new())),
                EvalType::Json => Ok(Box::new(AggrFnFirst::<JsonRef<'static>>::new())),
                EvalType::Bytes => Ok(Box::new(AggrFnFirst::<BytesRef<'static>>::new())),
            }
        }
    }
}

/// The FIRST aggregate function.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateFirst::<T>::new())]
pub struct AggrFnFirst<T>(PhantomData<T>)
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>;

impl<T> AggrFnFirst<T>
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    fn new() -> Self {
        AggrFnFirst(PhantomData)
    }
}

/// The state of the FIRST aggregate function.
#[derive(Debug)]
pub enum AggrFnStateFirst<T>
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    Empty,
    Valued(Option<T::EvaluableType>),
}

impl<T> AggrFnStateFirst<T>
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    pub fn new() -> Self {
        AggrFnStateFirst::Empty
    }

    #[inline]
    fn fidelio<'a, TT>(&mut self, _ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = T::EvaluableType>,
    {
        if let AggrFnStateFirst::Empty = self {
            // TODO: avoid this clone
            *self = AggrFnStateFirst::Valued(value.map(|x| x.to_owned_value()));
        }
        Ok(())
    }

    #[inline]
    fn fidelio_repeat<'a, TT>(
        &mut self,
        ctx: &mut EvalContext,
        value: Option<TT>,
        repeat_times: usize,
    ) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = T::EvaluableType>,
    {
        assert!(repeat_times > 0);
        self.fidelio(ctx, value)
    }

    #[inline]
    fn fidelio_vector<'a, TT, CC>(
        &mut self,
        ctx: &mut EvalContext,
        _phantom_data: Option<TT>,
        physical_values: CC,
        logical_rows: &[usize],
    ) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = T::EvaluableType>,
        CC: SolitonRef<'a, TT>,
    {
        if let Some(physical_index) = logical_rows.first() {
            self.fidelio(ctx, physical_values.get_option_ref(*physical_index))?;
        }
        Ok(())
    }
}

// Here we manually implement `AggrFunctionStatefidelioPartial` instead of implementing
// `ConcreteAggrFunctionState` so that `fidelio_repeat` and `fidelio_vector` can be faster.
impl<T> super::AggrFunctionStatefidelioPartial<T> for AggrFnStateFirst<T>
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    // SolitonedType has been implemented in AggrFunctionStatefidelioPartial<T1> for AggrFnStateFirst<T2>
    impl_state_fidelio_partial! { T }
}

// In order to make `AggrFnStateFirst` satisfy the `AggrFunctionState` trait, we default impl all
// `AggrFunctionStatefidelioPartial` of `Evaluable` for all `AggrFnStateFirst`.
impl_unmatched_function_state! { AggrFnStateFirst<T> }

impl<T> super::AggrFunctionState for AggrFnStateFirst<T>
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        assert_eq!(target.len(), 1);
        let res = if let AggrFnStateFirst::Valued(v) = self {
            v.clone()
        } else {
            None
        };
        target[0].push(res);
        Ok(())
    }
}

#[causet(test)]
mod tests {
    use super::super::AggrFunction;
    use super::*;

    use milevadb_query_datatype::FieldTypeTp;
    use fidel_timeshare_helper::ExprDefBuilder;

    use crate::AggrDefinitionParser;

    #[test]
    fn test_fidelio() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<&'static Int>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        fidelio!(state, &mut ctx, Some(&1)).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None, Some(1)]);

        fidelio!(state, &mut ctx, Some(&2)).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None, Some(1), Some(1)]);
    }

    #[test]
    fn test_fidelio_repeat() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<BytesRef<'static>>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Bytes)];

        fidelio_repeat!(state, &mut ctx, Some(&[1u8] as BytesRef), 2).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_bytes_vec(), &[Some(vec![1])]);

        fidelio_repeat!(state, &mut ctx, Some(&[2u8] as BytesRef), 3).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_bytes_vec(), &[Some(vec![1]), Some(vec![1])]);
    }

    #[test]
    fn test_fidelio_vector() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<&'static Int>::new();
        let mut state = function.create_state();
        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        fidelio_vector!(
            state,
            &mut ctx,
            &SolitonedVecSized::from_slice(&[Some(0); 0]),
            &[]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        result[0].clear();
        fidelio_vector!(
            state,
            &mut ctx,
            &SolitonedVecSized::from_slice(&[Some(1)]),
            &[]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        result[0].clear();
        fidelio_vector!(
            state,
            &mut ctx,
            &SolitonedVecSized::from_slice(&[None, Some(2)]),
            &[0, 1]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        result[0].clear();
        fidelio_vector!(
            state,
            &mut ctx,
            &SolitonedVecSized::from_slice(&[Some(1)]),
            &[0]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        // Reset state
        let mut state = function.create_state();

        result[0].clear();
        fidelio_vector!(
            state,
            &mut ctx,
            &SolitonedVecSized::from_slice(&[None, Some(2)]),
            &[1, 0]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(2)]);
    }

    #[test]
    fn test_illegal_request() {
        let expr = ExprDefBuilder::aggr_func(ExprType::First, FieldTypeTp::Double) // Expect LongLong but give Double
            .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
            .build();
        AggrFnDefinitionParserFirst.check_supported(&expr).unwrap();

        let src_schemaReplicant = [FieldTypeTp::LongLong.into()];
        let mut schemaReplicant = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        AggrFnDefinitionParserFirst
            .parse(expr, &mut ctx, &src_schemaReplicant, &mut schemaReplicant, &mut exp)
            .unwrap_err();
    }
}
