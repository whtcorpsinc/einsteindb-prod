// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use milevadb_query_codegen::AggrFunction;
use fidel_timeshare::{Expr, ExprType, FieldType};

use super::*;
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::data_type::*;
use milevadb_query_datatype::expr::EvalContext;
use milevadb_query_vec_expr::RpnExpression;

/// A trait for all bit operations
pub trait BitOp: Clone + std::fmt::Debug + lightlike + Sync + 'static {
    /// Returns the bit operation type
    fn tp() -> ExprType;

    /// Returns the bit operation initial state
    fn init_state() -> u64;

    /// Executes the special bit operation
    fn op(lhs: &mut u64, rhs: u64);
}

macro_rules! bit_op {
    ($name:ident, $tp:path, $init:tt, $op:tt) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name;
        impl BitOp for $name {
            fn tp() -> ExprType {
                $tp
            }

            fn init_state() -> u64 {
                $init
            }

            fn op(lhs: &mut u64, rhs: u64) {
                *lhs $op rhs
            }
        }
    };
}

bit_op!(BitAnd, ExprType::AggBitAnd, 0xffff_ffff_ffff_ffff, &=);
bit_op!(BitOr, ExprType::AggBitOr, 0, |=);
bit_op!(BitXor, ExprType::AggBitXor, 0, ^=);

/// The parser for bit operation aggregate functions.
pub struct AggrFnDefinitionParserBitOp<T: BitOp>(std::marker::PhantomData<T>);

impl<T: BitOp> AggrFnDefinitionParserBitOp<T> {
    pub fn new() -> Self {
        AggrFnDefinitionParserBitOp(std::marker::PhantomData)
    }
}

impl<T: BitOp> super::AggrDefinitionParser for AggrFnDefinitionParserBitOp<T> {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), T::tp());
        super::util::check_aggr_exp_supported_one_child(aggr_def)?;
        Ok(())
    }

    #[inline]
    fn parse_rpn(
        &self,
        mut root_expr: Expr,
        mut exp: RpnExpression,
        _ctx: &mut EvalContext,
        src_schemaReplicant: &[FieldType],
        out_schemaReplicant: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        assert_eq!(root_expr.get_tp(), T::tp());

        // bit operation outputs one PrimaryCauset.
        out_schemaReplicant.push(root_expr.take_field_type());

        super::util::rewrite_exp_for_bit_op(src_schemaReplicant, &mut exp).unwrap();
        out_exp.push(exp);

        Ok(Box::new(AggrFnBitOp::<T>(std::marker::PhantomData)))
    }
}

/// The bit operation aggregate functions.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateBitOp::<T>::new())]
pub struct AggrFnBitOp<T: BitOp>(std::marker::PhantomData<T>);

/// The state of the BitAnd aggregate function.
#[derive(Debug)]
pub struct AggrFnStateBitOp<T: BitOp> {
    c: u64,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: BitOp> AggrFnStateBitOp<T> {
    pub fn new() -> Self {
        Self {
            c: T::init_state(),
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn fidelio_concrete<'a, TT>(&mut self, _ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = Int>,
    {
        match value {
            None => Ok(()),
            Some(value) => {
                T::op(&mut self.c, value.to_owned_value() as u64);
                Ok(())
            }
        }
    }
}

impl<T: BitOp> super::ConcreteAggrFunctionState for AggrFnStateBitOp<T> {
    type ParameterType = &'static Int;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push_int(Some(self.c as Int));
        Ok(())
    }
}

#[causet(test)]
mod tests {
    use super::super::AggrFunction;
    use super::*;

    use milevadb_query_datatype::FieldTypeTp;
    use milevadb_query_datatype::{EvalType, FieldTypeAccessor};
    use fidel_timeshare_helper::ExprDefBuilder;

    use crate::parser::AggrDefinitionParser;
    use milevadb_query_datatype::codec::batch::{LazyBatchPrimaryCauset, LazyBatchPrimaryCausetVec};

    #[test]
    fn test_bit_and() {
        let mut ctx = EvalContext::default();
        let function = AggrFnBitOp::<BitAnd>(std::marker::PhantomData);
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].to_int_vec(),
            &[Some(0xffff_ffff_ffff_ffff_u64 as i64)]
        );

        fidelio!(state, &mut ctx, Option::<&Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].to_int_vec(),
            &[Some(0xffff_ffff_ffff_ffff_u64 as i64)]
        );

        // 7 & 4 == 4
        fidelio!(state, &mut ctx, Some(&7i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(7)]);

        fidelio!(state, &mut ctx, Some(&4i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(4)]);

        fidelio_repeat!(state, &mut ctx, Some(&4), 10).unwrap();
        fidelio_repeat!(state, &mut ctx, Option::<&Int>::None, 7).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(4)]);

        // Reset the state
        let mut state = function.create_state();
        // 7 & 1 == 1
        fidelio!(state, &mut ctx, Some(&7i64)).unwrap();
        let int_vec = vec![Some(1i64), None, Some(1i64)];
        let int_vec: SolitonedVecSized<Int> = int_vec.into();
        fidelio_vector!(state, &mut ctx, &int_vec, &[0, 1, 2]).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(1)]);

        // 7 & 1 & 2 == 0
        fidelio!(state, &mut ctx, Some(&2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);
    }

    #[test]
    fn test_bit_or() {
        let mut ctx = EvalContext::default();
        let function = AggrFnBitOp::<BitOr>(std::marker::PhantomData);
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);

        fidelio!(state, &mut ctx, Option::<&Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);

        // 1 | 4 == 5
        fidelio!(state, &mut ctx, Some(&1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(1)]);

        fidelio!(state, &mut ctx, Some(&4i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(5)]);

        fidelio_repeat!(state, &mut ctx, Some(&8), 10).unwrap();
        fidelio_repeat!(state, &mut ctx, Option::<&Int>::None, 7).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(13)]);

        // 13 | 2 == 15
        fidelio!(state, &mut ctx, Some(&2i64)).unwrap();
        let Solitoned_vec: SolitonedVecSized<Int> = vec![Some(2i64), None, Some(1i64)].into();
        fidelio_vector!(state, &mut ctx, &Solitoned_vec, &[0, 1, 2]).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(15)]);

        // 15 | 2 == 15
        fidelio!(state, &mut ctx, Some(&2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(15)]);

        // 15 | 2 | -1 == 18446744073709551615
        fidelio!(state, &mut ctx, Some(&-1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].to_int_vec(),
            &[Some(18446744073709551615u64 as i64)]
        );
    }

    #[test]
    fn test_bit_xor() {
        let mut ctx = EvalContext::default();
        let function = AggrFnBitOp::<BitXor>(std::marker::PhantomData);
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);

        fidelio!(state, &mut ctx, Option::<&Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);

        // 1 ^ 5 == 4
        fidelio!(state, &mut ctx, Some(&1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(1)]);

        fidelio!(state, &mut ctx, Some(&5i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(4)]);

        // 1 ^ 5 ^ 8 == 12
        fidelio_repeat!(state, &mut ctx, Some(&8), 9).unwrap();
        fidelio_repeat!(state, &mut ctx, Option::<&Int>::None, 7).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(12)]);

        // Will not change due to xor even times
        fidelio_repeat!(state, &mut ctx, Some(&9), 10).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(12)]);

        // 1 ^ 5 ^ 8 ^ ^ 2 ^ 2 ^ 1 == 13
        fidelio!(state, &mut ctx, Some(&2i64)).unwrap();
        let Solitoned_vec: SolitonedVecSized<Int> = vec![Some(2i64), None, Some(1i64)].into();
        fidelio_vector!(state, &mut ctx, &Solitoned_vec, &[0, 1, 2]).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(13)]);

        // 13 ^ 2 == 15
        fidelio!(state, &mut ctx, Some(&2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(15)]);

        // 15 ^ 2 ^ -1 == 18446744073709551602
        fidelio!(state, &mut ctx, Some(&2i64)).unwrap();
        fidelio!(state, &mut ctx, Some(&-1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].to_int_vec(),
            &[Some(18446744073709551602u64 as i64)]
        );
    }

    #[test]
    fn test_integration() {
        let bit_and_parser = AggrFnDefinitionParserBitOp::<BitAnd>::new();
        let bit_or_parser = AggrFnDefinitionParserBitOp::<BitOr>::new();
        let bit_xor_parser = AggrFnDefinitionParserBitOp::<BitXor>::new();

        let bit_and = ExprDefBuilder::aggr_func(ExprType::AggBitAnd, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
            .build();
        bit_and_parser.check_supported(&bit_and).unwrap();

        let bit_or = ExprDefBuilder::aggr_func(ExprType::AggBitOr, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
            .build();
        bit_or_parser.check_supported(&bit_or).unwrap();

        let bit_xor = ExprDefBuilder::aggr_func(ExprType::AggBitXor, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
            .build();
        bit_xor_parser.check_supported(&bit_xor).unwrap();

        let src_schemaReplicant = [FieldTypeTp::LongLong.into()];
        let mut PrimaryCausets = LazyBatchPrimaryCausetVec::from(vec![{
            let mut col = LazyBatchPrimaryCauset::decoded_with_capacity_and_tp(0, EvalType::Int);
            col.mut_decoded().push_int(Some(1000));
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(23));
            col.mut_decoded().push_int(Some(42));
            col.mut_decoded().push_int(None);
            col.mut_decoded().push_int(Some(99));
            col.mut_decoded().push_int(Some(-1));
            col.mut_decoded().push_int(Some(1000));
            col
        }]);
        let logical_rows = vec![6, 3, 4, 5, 1, 2];

        let mut schemaReplicant = vec![];
        let mut exp = vec![];

        let mut ctx = EvalContext::default();
        let bit_and_fn = bit_and_parser
            .parse(bit_and, &mut ctx, &src_schemaReplicant, &mut schemaReplicant, &mut exp)
            .unwrap();
        assert_eq!(schemaReplicant.len(), 1);
        assert_eq!(schemaReplicant[0].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 1);

        let bit_or_fn = bit_or_parser
            .parse(bit_or, &mut ctx, &src_schemaReplicant, &mut schemaReplicant, &mut exp)
            .unwrap();
        assert_eq!(schemaReplicant.len(), 2);
        assert_eq!(schemaReplicant[1].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 2);

        let bit_xor_fn = bit_xor_parser
            .parse(bit_xor, &mut ctx, &src_schemaReplicant, &mut schemaReplicant, &mut exp)
            .unwrap();
        assert_eq!(schemaReplicant.len(), 3);
        assert_eq!(schemaReplicant[2].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 3);

        let mut bit_and_state = bit_and_fn.create_state();
        let mut bit_or_state = bit_or_fn.create_state();
        let mut bit_xor_state = bit_xor_fn.create_state();

        let mut aggr_result = [VectorValue::with_capacity(0, EvalType::Int)];

        // bit and
        {
            let bit_and_result = exp[0]
                .eval(&mut ctx, &src_schemaReplicant, &mut PrimaryCausets, &logical_rows, 6)
                .unwrap();
            let bit_and_result = bit_and_result.vector_value().unwrap();
            let bit_and_slice = bit_and_result.as_ref().to_int_vec();
            let bit_and_vec: SolitonedVecSized<Int> = bit_and_slice.into();

            fidelio_vector!(
                bit_and_state,
                &mut ctx,
                &bit_and_vec,
                bit_and_result.logical_rows()
            )
            .unwrap();
            bit_and_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        // bit or
        {
            let bit_or_result = exp[1]
                .eval(&mut ctx, &src_schemaReplicant, &mut PrimaryCausets, &logical_rows, 6)
                .unwrap();
            let bit_or_result = bit_or_result.vector_value().unwrap();
            let bit_or_slice = bit_or_result.as_ref().to_int_vec();
            let bit_or_vec: SolitonedVecSized<Int> = bit_or_slice.into();

            fidelio_vector!(
                bit_or_state,
                &mut ctx,
                &bit_or_vec,
                bit_or_result.logical_rows()
            )
            .unwrap();
            bit_or_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        // bit xor
        {
            let bit_xor_result = exp[2]
                .eval(&mut ctx, &src_schemaReplicant, &mut PrimaryCausets, &logical_rows, 6)
                .unwrap();
            let bit_xor_result = bit_xor_result.vector_value().unwrap();
            let bit_xor_slice = bit_xor_result.as_ref().to_int_vec();
            let bit_xor_vec: SolitonedVecSized<Int> = bit_xor_slice.into();

            fidelio_vector!(
                bit_xor_state,
                &mut ctx,
                &bit_xor_vec,
                bit_xor_result.logical_rows()
            )
            .unwrap();
            bit_xor_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        assert_eq!(
            aggr_result[0].to_int_vec(),
            &[
                Some(0),
                Some(18446744073709551615u64 as i64),
                Some(18446744073709551520u64 as i64)
            ]
        );
    }
}
