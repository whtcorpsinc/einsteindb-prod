// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;
use std::hash::Hash;
use std::sync::Arc;

use milevadb_query_datatype::Collation;
use milevadb_query_datatype::{EvalType, FieldTypeAccessor};
use violetabftstore::interlock::::box_try;
use violetabftstore::interlock::::collections::HashMap;
use fidel_timeshare::Aggregation;
use fidel_timeshare::{Expr, FieldType};

use crate::interface::*;
use crate::util::aggr_executor::*;
use crate::util::hash_aggr_helper::HashAggregationHelper;
use milevadb_query_common::causet_storage::IntervalCone;
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::batch::{LazyBatchPrimaryCauset, LazyBatchPrimaryCausetVec};
use milevadb_query_datatype::codec::collation::{match_template_collator, SortKey};
use milevadb_query_datatype::codec::data_type::*;
use milevadb_query_datatype::expr::{EvalConfig, EvalContext};
use milevadb_query_vec_aggr::*;
use milevadb_query_vec_expr::{RpnExpression, RpnExpressionBuilder, RpnStackNode};

pub macro match_template_hashable($t:tt, $($tail:tt)*) {
    match_template::match_template! {
        $t = [Int, Real, Bytes, Duration, Decimal, DateTime],
        $($tail)*
    }
}


pub struct BatchFastHashAggregationFreeDaemon<Src: BatchFreeDaemon>(
    AggregationFreeDaemon<Src, FastHashAggregationImpl>,
);

impl<Src: BatchFreeDaemon> BatchFreeDaemon for BatchFastHashAggregationFreeDaemon<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schemaReplicant(&self) -> &[FieldType] {
        self.0.schemaReplicant()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(scan_rows)
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.0.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_causet_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.0.collect_causet_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_cone(&mut self) -> IntervalCone {
        self.0.take_scanned_cone()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.0.can_be_cached()
    }
}

// We assign a dummy type `Box<dyn BatchFreeDaemon<StorageStats = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchFastHashAggregationFreeDaemon<Box<dyn BatchFreeDaemon<StorageStats = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        let group_by_definitions = descriptor.get_group_by();
        assert!(!group_by_definitions.is_empty());
        if group_by_definitions.len() > 1 {
            return Err(other_err!("Multi group is not supported"));
        }

        let def = &group_by_definitions[0];

        // Only a subset of all eval types are supported.
        let eval_type = box_try!(EvalType::try_from(def.get_field_type().as_accessor().tp()));
        match eval_type {
            EvalType::Int
            | EvalType::Real
            | EvalType::Bytes
            | EvalType::Duration
            | EvalType::Decimal
            | EvalType::DateTime => {}
            _ => return Err(other_err!("Eval type {} is not supported", eval_type)),
        }

        RpnExpressionBuilder::check_expr_tree_supported(def)?;

        let aggr_definitions = descriptor.get_agg_func();
        for def in aggr_definitions {
            AllAggrDefinitionParser.check_supported(def)?;
        }
        Ok(())
    }
}

impl<Src: BatchFreeDaemon> BatchFastHashAggregationFreeDaemon<Src> {
    #[causet(test)]
    pub fn new_for_test(
        src: Src,
        group_by_exp: RpnExpression,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Self {
        Self::new_impl(
            Arc::new(EvalConfig::default()),
            src,
            group_by_exp,
            aggr_defs,
            aggr_def_parser,
        )
        .unwrap()
    }

    pub fn new(
        config: Arc<EvalConfig>,
        src: Src,
        group_by_exp_defs: Vec<Expr>,
        aggr_defs: Vec<Expr>,
    ) -> Result<Self> {
        assert_eq!(group_by_exp_defs.len(), 1);
        let mut ctx = EvalContext::new(config.clone());
        let group_by_exp = RpnExpressionBuilder::build_from_expr_tree(
            group_by_exp_defs.into_iter().next().unwrap(),
            &mut ctx,
            src.schemaReplicant().len(),
        )?;
        Self::new_impl(
            config,
            src,
            group_by_exp,
            aggr_defs,
            AllAggrDefinitionParser,
        )
    }

    #[inline]
    fn new_impl(
        config: Arc<EvalConfig>,
        src: Src,
        group_by_exp: RpnExpression,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        let group_by_field_type = group_by_exp.ret_field_type(src.schemaReplicant()).clone();
        let group_by_eval_type =
            EvalType::try_from(group_by_field_type.as_accessor().tp()).unwrap();
        let groups = match_template_hashable! {
            TT, match group_by_eval_type {
                EvalType::TT => Groups::TT(HashMap::default()),
                _ => unreachable!(),
            }
        };

        let aggr_impl = FastHashAggregationImpl {
            states: Vec::with_capacity(1024),
            groups,
            group_by_exp,
            group_by_field_type,
            states_offset_each_logical_row: Vec::with_capacity(crate::runner::BATCH_MAX_SIZE),
        };

        Ok(Self(AggregationFreeDaemon::new(
            aggr_impl,
            src,
            config,
            aggr_defs,
            aggr_def_parser,
        )?))
    }
}

/// All groups.
enum Groups {
    // The value of each hash Block is the spacelike index in `FastHashAggregationImpl::states`
    // field. When there are new groups (i.e. new entry in the hash Block), the states of the groups
    // will be applightlikeed to `states`.
    Int(HashMap<Option<Int>, usize>),
    Real(HashMap<Option<Real>, usize>),
    Bytes(HashMap<Option<Bytes>, usize>),
    Duration(HashMap<Option<Duration>, usize>),
    Decimal(HashMap<Option<Decimal>, usize>),
    DateTime(HashMap<Option<DateTime>, usize>),
}

impl Groups {
    fn eval_type(&self) -> EvalType {
        match_template_hashable! {
            TT, match self {
                Groups::TT(_) => {
                    EvalType::TT
                },
            }
        }
    }

    fn len(&self) -> usize {
        match_template_hashable! {
            TT, match self {
                Groups::TT(groups) => {
                    groups.len()
                },
            }
        }
    }
}

pub struct FastHashAggregationImpl {
    states: Vec<Box<dyn AggrFunctionState>>,
    groups: Groups,
    group_by_exp: RpnExpression,
    group_by_field_type: FieldType,
    states_offset_each_logical_row: Vec<usize>,
}

impl<Src: BatchFreeDaemon> AggregationFreeDaemonImpl<Src> for FastHashAggregationImpl {
    #[inline]
    fn prepare_entities(&mut self, entities: &mut Entities<Src>) {
        entities.schemaReplicant.push(self.group_by_field_type.clone());
    }

    #[inline]
    fn process_batch_input(
        &mut self,
        entities: &mut Entities<Src>,
        mut input_physical_PrimaryCausets: LazyBatchPrimaryCausetVec,
        input_logical_rows: &[usize],
    ) -> Result<()> {
        // 1. Calculate which group each src Evcausetidx belongs to.
        self.states_offset_each_logical_row.clear();
        let group_by_result = self.group_by_exp.eval(
            &mut entities.context,
            entities.src.schemaReplicant(),
            &mut input_physical_PrimaryCausets,
            input_logical_rows,
            input_logical_rows.len(),
        )?;

        match group_by_result {
            RpnStackNode::Scalar { value, .. } => {
                match_template::match_template! {
                    TT = [Int, Bytes, Real, Duration, Decimal, DateTime],
                    match value {
                        ScalarValue::TT(v) => {
                            if let Groups::TT(group) = &mut self.groups {
                                handle_scalar_group_each_row(
                                    v,
                                    &entities.each_aggr_fn,
                                    group,
                                    &mut self.states,
                                    input_logical_rows.len(),
                                    &mut self.states_offset_each_logical_row,
                                )?;
                            } else {
                                panic!();
                            }
                        },
                        _ => unreachable!(),
                    }
                }
            }
            RpnStackNode::Vector { value, .. } => {
                let group_by_physical_vec = value.as_ref();
                let group_by_logical_rows = value.logical_rows_struct();

                match_template::match_template! {
                    TT = [Int, Real, Duration, Decimal, DateTime],
                    match group_by_physical_vec {
                        VectorValue::TT(v) => {
                            if let Groups::TT(group) = &mut self.groups {
                                calc_groups_each_row(
                                    v,
                                    group_by_logical_rows,
                                    &entities.each_aggr_fn,
                                    group,
                                    &mut self.states,
                                    &mut self.states_offset_each_logical_row,
                                    |val| Ok(val.map(|x| x.to_owned_value()))
                                )?;
                            } else {
                                panic!();
                            }
                        },
                        VectorValue::Bytes(v) => {
                            if let Groups::Bytes(group) = &mut self.groups {
                                match_template_collator!(
                                    TT,
                                    match self.group_by_field_type.collation().map_err(milevadb_query_datatype::codec::Error::from)? {
                                        Collation::TT => {
                                            #[allow(clippy::transmute_ptr_to_ptr)]
                                            let group: &mut HashMap<Option<SortKey<Bytes, TT>>, usize> =
                                                unsafe { std::mem::transmute(group) };
                                            calc_groups_each_row(
                                                v,
                                                group_by_logical_rows,
                                                &entities.each_aggr_fn,
                                                group,
                                                &mut self.states,
                                                &mut self.states_offset_each_logical_row,
                                                |val| Ok(SortKey::map_option_owned(val.map(|x| x.to_owned_value()))?)
                                            )?;
                                        }
                                    }
                                )
                            } else {
                                panic!();
                            }
                        },
                        _ => unreachable!(),
                    }
                }
            }
        };

        // 2. fidelio states according to the group.
        HashAggregationHelper::fidelio_each_row_states_by_offset(
            entities,
            &mut input_physical_PrimaryCausets,
            input_logical_rows,
            &mut self.states,
            &self.states_offset_each_logical_row,
        )?;

        Ok(())
    }

    #[inline]
    fn groups_len(&self) -> usize {
        self.groups.len()
    }

    #[inline]
    fn iterate_available_groups(
        &mut self,
        entities: &mut Entities<Src>,
        src_is_drained: bool,
        mut iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchPrimaryCauset>> {
        assert!(src_is_drained);

        let aggr_fns_len = entities.each_aggr_fn.len();
        let mut group_by_PrimaryCauset = LazyBatchPrimaryCauset::decoded_with_capacity_and_tp(
            self.groups.len(),
            self.groups.eval_type(),
        );

        match_template_hashable! {
            TT, match &mut self.groups {
                Groups::TT(groups) => {
                    // Take the map out, and then use `HashMap::into_iter()`. This should be faster
                    // then using `HashMap::drain()`.
                    // TODO: Verify performance difference.
                    let groups = std::mem::take(groups);
                    for (group_key, states_offset) in groups {
                        iteratee(entities, &self.states[states_offset..states_offset + aggr_fns_len])?;
                        group_by_PrimaryCauset.mut_decoded().push(group_key);
                    }
                }
            }
        }

        Ok(vec![group_by_PrimaryCauset])
    }

    /// Fast hash aggregation can output aggregate results only if the source is drained.
    #[inline]
    fn is_partial_results_ready(&self) -> bool {
        false
    }
}

fn calc_groups_each_row<'a, TT: EvaluableRef<'a>, T: 'a + SolitonRef<'a, TT>, S, F>(
    physical_PrimaryCauset: T,
    logical_rows: LogicalEvents<'a>,
    aggr_fns: &[Box<dyn AggrFunction>],
    group: &mut HashMap<Option<S>, usize>,
    states: &mut Vec<Box<dyn AggrFunctionState>>,
    states_offset_each_logical_row: &mut Vec<usize>,
    map_to_sort_key: F,
) -> Result<()>
where
    S: Hash + Eq + Clone,
    F: Fn(Option<TT>) -> Result<Option<S>>,
{
    for physical_idx in logical_rows {
        let val = map_to_sort_key(physical_PrimaryCauset.get_option_ref(physical_idx))?;

        // Not using the entry API so that when entry exists there is no clone.
        match group.get(&val) {
            Some(offset) => {
                // Group exists, use the offset of existing group.
                states_offset_each_logical_row.push(*offset);
            }
            None => {
                // Group does not exist, prepare groups.
                let offset = states.len();
                states_offset_each_logical_row.push(offset);
                group.insert(val, offset);
                for aggr_fn in aggr_fns {
                    states.push(aggr_fn.create_state());
                }
            }
        }
    }

    Ok(())
}

fn handle_scalar_group_each_row<T>(
    scalar_value: &Option<T>,
    aggr_fns: &[Box<dyn AggrFunction>],
    group: &mut HashMap<Option<T>, usize>,
    states: &mut Vec<Box<dyn AggrFunctionState>>,
    logical_row_len: usize,
    states_offset_each_logical_row: &mut Vec<usize>,
) -> Result<()>
where
    T: Hash + Eq + Clone,
{
    if group.is_empty() {
        group.insert(scalar_value.clone(), 0);
        for aggr_fn in aggr_fns {
            states.push(aggr_fn.create_state());
        }
    } else if !group.contains_key(scalar_value) {
        // As a constant group-by expression, all scalar results it produce
        // should be the same.
        panic!();
    }

    for _ in 0..logical_row_len {
        states_offset_each_logical_row.push(0);
    }

    Ok(())
}

#[causet(test)]
mod tests {
    use super::*;

    use milevadb_query_datatype::FieldTypeTp;
    use fidel_timeshare::ScalarFuncSig;

    use crate::util::aggr_executor::tests::*;
    use crate::util::mock_executor::MockFreeDaemon;
    use crate::BatchSlowHashAggregationFreeDaemon;
    use milevadb_query_datatype::expr::EvalWarnings;
    use milevadb_query_vec_expr::impl_arithmetic::{arithmetic_fn_meta, RealPlus};
    use milevadb_query_vec_expr::{RpnExpression, RpnExpressionBuilder};
    use fidel_timeshare::ExprType;
    use fidel_timeshare_helper::ExprDefBuilder;

    // Test cases also cover BatchSlowHashAggregationFreeDaemon.

    #[test]
    fn test_it_works_integration() {
        // This test creates a hash aggregation executor with the following aggregate functions:
        // - COUNT(1)
        // - COUNT(col_1 + 5.0)
        // - AVG(col_0)
        // And group by:
        // - col_0 + col_1

        let group_by_exp = || {
            RpnExpressionBuilder::new_for_test()
                .push_PrimaryCauset_ref_for_test(0)
                .push_PrimaryCauset_ref_for_test(1)
                .push_fn_call_for_test(arithmetic_fn_meta::<RealPlus>(), 2, FieldTypeTp::Double)
                .build_for_test()
        };

        let aggr_definitions = vec![
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(
                    ExprDefBuilder::scalar_func(ScalarFuncSig::PlusReal, FieldTypeTp::Double)
                        .push_child(ExprDefBuilder::PrimaryCauset_ref(1, FieldTypeTp::Double))
                        .push_child(ExprDefBuilder::constant_real(5.0)),
                )
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::Double))
                .build(),
        ];

        let exec_fast = |src_exec| {
            Box::new(BatchFastHashAggregationFreeDaemon::new_for_test(
                src_exec,
                group_by_exp(),
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let exec_slow = |src_exec| {
            Box::new(BatchSlowHashAggregationFreeDaemon::new_for_test(
                src_exec,
                vec![group_by_exp()],
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let executor_builders: Vec<Box<dyn FnOnce(MockFreeDaemon) -> _>> =
            vec![Box::new(exec_fast), Box::new(exec_slow)];

        for exec_builder in executor_builders {
            let src_exec = make_src_executor_1();
            let mut exec = exec_builder(src_exec);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let mut r = exec.next_batch(1);
            // col_0 + col_1 can result in [NULL, 9.0, 6.0], thus there will be three groups.
            assert_eq!(&r.logical_rows, &[0, 1, 2]);
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 3);
            assert_eq!(r.physical_PrimaryCausets.PrimaryCausets_len(), 5); // 4 result PrimaryCauset, 1 group by PrimaryCauset

            // Let's check group by PrimaryCauset first. Group by PrimaryCauset is decoded in fast hash agg,
            // but not decoded in slow hash agg. So decode it anyway.
            r.physical_PrimaryCausets[4]
                .ensure_all_decoded_for_test(&mut EvalContext::default(), &exec.schemaReplicant()[4])
                .unwrap();

            // The Evcausetidx order is not defined. Let's sort it by the group by PrimaryCauset before asserting.
            let mut sort_PrimaryCauset: Vec<(usize, _)> = r.physical_PrimaryCausets[4]
                .decoded()
                .to_real_vec()
                .iter()
                .map(|v| {
                    use std::hash::Hasher;
                    let mut s = std::collections::hash_map::DefaultHasher::new();
                    v.hash(&mut s);
                    s.finish()
                })
                .enumerate()
                .collect();
            sort_PrimaryCauset.sort_by(|a, b| a.1.cmp(&b.1));

            // Use the order of the sorted PrimaryCauset to sort other PrimaryCausets
            let ordered_PrimaryCauset: Vec<_> = sort_PrimaryCauset
                .iter()
                .map(|(idx, _)| r.physical_PrimaryCausets[4].decoded().to_real_vec()[*idx])
                .collect();
            assert_eq!(
                &ordered_PrimaryCauset,
                &[Real::new(9.0).ok(), Real::new(6.0).ok(), None]
            );
            let ordered_PrimaryCauset: Vec<_> = sort_PrimaryCauset
                .iter()
                .map(|(idx, _)| r.physical_PrimaryCausets[0].decoded().to_int_vec()[*idx])
                .collect();
            assert_eq!(&ordered_PrimaryCauset, &[Some(1), Some(1), Some(3)]);
            let ordered_PrimaryCauset: Vec<_> = sort_PrimaryCauset
                .iter()
                .map(|(idx, _)| r.physical_PrimaryCausets[1].decoded().to_int_vec()[*idx])
                .collect();
            assert_eq!(&ordered_PrimaryCauset, &[Some(1), Some(1), Some(2)]);
            let ordered_PrimaryCauset: Vec<_> = sort_PrimaryCauset
                .iter()
                .map(|(idx, _)| r.physical_PrimaryCausets[2].decoded().to_int_vec()[*idx])
                .collect();
            assert_eq!(&ordered_PrimaryCauset, &[Some(1), Some(1), Some(0)]);
            let ordered_PrimaryCauset: Vec<_> = sort_PrimaryCauset
                .iter()
                .map(|(idx, _)| r.physical_PrimaryCausets[3].decoded().to_real_vec()[*idx])
                .collect();
            assert_eq!(
                &ordered_PrimaryCauset,
                &[Real::new(7.0).ok(), Real::new(1.5).ok(), None]
            );
        }
    }

    #[test]
    fn test_group_by_a_constant() {
        // This test creates a hash aggregation executor with the following aggregate functions:
        // - COUNT(1)
        // - COUNT(col_1 + 5.0)
        // - AVG(col_0)
        // And group by:
        // - 1 (Constant)

        use fidel_timeshare::ExprType;
        use fidel_timeshare_helper::ExprDefBuilder;

        let group_by_exp = || {
            RpnExpressionBuilder::new_for_test()
                .push_constant_for_test(1) // Group by a constant
                .build_for_test()
        };

        let aggr_definitions = || {
            vec![
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(
                        ExprDefBuilder::scalar_func(ScalarFuncSig::PlusReal, FieldTypeTp::Double)
                            .push_child(ExprDefBuilder::PrimaryCauset_ref(1, FieldTypeTp::Double))
                            .push_child(ExprDefBuilder::constant_real(5.0)),
                    )
                    .build(),
                ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double)
                    .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::Double))
                    .build(),
            ]
        };

        let exec_fast = |src_exec| {
            Box::new(BatchFastHashAggregationFreeDaemon::new_for_test(
                src_exec,
                group_by_exp(),
                aggr_definitions(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let exec_slow = |src_exec| {
            Box::new(BatchSlowHashAggregationFreeDaemon::new_for_test(
                src_exec,
                vec![group_by_exp()],
                aggr_definitions(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };
        let executor_builders: Vec<Box<dyn FnOnce(MockFreeDaemon) -> _>> =
            // vec![Box::new(exec_fast)];
            vec![Box::new(exec_fast), Box::new(exec_slow)];

        for exec_builder in executor_builders {
            let src_exec = make_src_executor_1();
            let mut exec = exec_builder(src_exec);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let mut r = exec.next_batch(1);
            assert_eq!(&r.logical_rows, &[0]);
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 1);
            assert_eq!(r.physical_PrimaryCausets.PrimaryCausets_len(), 5); // 4 result PrimaryCauset, 1 group by PrimaryCauset

            r.physical_PrimaryCausets[4]
                .ensure_all_decoded_for_test(&mut EvalContext::default(), &exec.schemaReplicant()[4])
                .unwrap();

            // Group by a constant, So should be only one group.
            assert_eq!(r.physical_PrimaryCausets[4].decoded().to_int_vec().len(), 1);

            assert_eq!(r.physical_PrimaryCausets[0].decoded().to_int_vec(), &[Some(5)]);
            assert_eq!(r.physical_PrimaryCausets[1].decoded().to_int_vec(), &[Some(4)]);
            assert_eq!(r.physical_PrimaryCausets[2].decoded().to_int_vec(), &[Some(2)]);
            assert_eq!(
                r.physical_PrimaryCausets[3].decoded().to_real_vec(),
                &[Real::new(8.5).ok()]
            );
        }
    }

    #[test]
    fn test_collation() {
        use fidel_timeshare::ExprType;
        use fidel_timeshare_helper::ExprDefBuilder;

        // This test creates a hash aggregation executor with the following aggregate functions:
        // - COUNT(col_0)
        // - AVG(col_1)
        // And group by:
        // - col_4

        let group_by_exp = || {
            RpnExpressionBuilder::new_for_test()
                .push_PrimaryCauset_ref_for_test(4)
                .build_for_test()
        };

        let aggr_definitions = vec![
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::Double))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::PrimaryCauset_ref(1, FieldTypeTp::Double))
                .build(),
        ];

        let exec_fast = |src_exec| {
            Box::new(BatchFastHashAggregationFreeDaemon::new_for_test(
                src_exec,
                group_by_exp(),
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let exec_slow = |src_exec| {
            Box::new(BatchSlowHashAggregationFreeDaemon::new_for_test(
                src_exec,
                vec![group_by_exp()],
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let executor_builders: Vec<Box<dyn FnOnce(MockFreeDaemon) -> _>> =
            vec![Box::new(exec_fast), Box::new(exec_slow)];

        for exec_builder in executor_builders {
            let src_exec = make_src_executor_1();
            let mut exec = exec_builder(src_exec);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let mut r = exec.next_batch(1);
            // col_4 can result in [NULL, "aa", "aaa"], thus there will be three groups.
            assert_eq!(&r.logical_rows, &[0, 1, 2]);
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 3);
            assert_eq!(r.physical_PrimaryCausets.PrimaryCausets_len(), 4); // 3 result PrimaryCauset, 1 group by PrimaryCauset

            // Let's check group by PrimaryCauset first. Group by PrimaryCauset is decoded in fast hash agg,
            // but not decoded in slow hash agg. So decode it anyway.
            r.physical_PrimaryCausets[3]
                .ensure_all_decoded_for_test(&mut EvalContext::default(), &exec.schemaReplicant()[3])
                .unwrap();

            // The Evcausetidx order is not defined. Let's sort it by the group by PrimaryCauset before asserting.
            let mut sort_PrimaryCauset: Vec<(usize, _)> = r.physical_PrimaryCausets[3]
                .decoded()
                .to_bytes_vec()
                .into_iter()
                .enumerate()
                .collect();
            sort_PrimaryCauset.sort_by(|a, b| a.1.cmp(&b.1));

            // Use the order of the sorted PrimaryCauset to sort other PrimaryCausets
            let ordered_PrimaryCauset: Vec<_> = sort_PrimaryCauset
                .iter()
                .map(|(idx, _)| r.physical_PrimaryCausets[3].decoded().to_bytes_vec()[*idx].clone())
                .collect();
            assert_eq!(
                &ordered_PrimaryCauset,
                &[None, Some(b"aa".to_vec()), Some(b"aaa".to_vec())]
            );
            let ordered_PrimaryCauset: Vec<_> = sort_PrimaryCauset
                .iter()
                .map(|(idx, _)| r.physical_PrimaryCausets[0].decoded().to_int_vec()[*idx])
                .collect();
            assert_eq!(&ordered_PrimaryCauset, &[Some(0), Some(0), Some(2)]);
            let ordered_PrimaryCauset: Vec<_> = sort_PrimaryCauset
                .iter()
                .map(|(idx, _)| r.physical_PrimaryCausets[1].decoded().to_int_vec()[*idx])
                .collect();
            assert_eq!(&ordered_PrimaryCauset, &[Some(1), Some(1), Some(2)]);
            let ordered_PrimaryCauset: Vec<_> = sort_PrimaryCauset
                .iter()
                .map(|(idx, _)| r.physical_PrimaryCausets[2].decoded().to_real_vec()[*idx])
                .collect();
            assert_eq!(
                &ordered_PrimaryCauset,
                &[
                    Real::new(4.5).ok(),
                    Real::new(1.0).ok(),
                    Real::new(6.5).ok()
                ]
            );
        }
    }

    #[test]
    fn test_no_row() {
        struct MyParser;

        impl AggrDefinitionParser for MyParser {
            fn check_supported(&self, _aggr_def: &Expr) -> Result<()> {
                unreachable!()
            }

            fn parse(
                &self,
                _aggr_def: Expr,
                _ctx: &mut EvalContext,
                _src_schemaReplicant: &[FieldType],
                out_schemaReplicant: &mut Vec<FieldType>,
                out_exp: &mut Vec<RpnExpression>,
            ) -> Result<Box<dyn AggrFunction>> {
                out_schemaReplicant.push(FieldTypeTp::LongLong.into());
                out_exp.push(
                    RpnExpressionBuilder::new_for_test()
                        .push_constant_for_test(1)
                        .build_for_test(),
                );
                Ok(Box::new(AggrFnUnreachable))
            }
        }

        let exec_fast_col = |src_exec| {
            Box::new(BatchFastHashAggregationFreeDaemon::new_for_test(
                src_exec,
                RpnExpressionBuilder::new_for_test()
                    .push_PrimaryCauset_ref_for_test(0)
                    .build_for_test(),
                vec![Expr::default()],
                MyParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let exec_fast_const = |src_exec| {
            Box::new(BatchFastHashAggregationFreeDaemon::new_for_test(
                src_exec,
                RpnExpressionBuilder::new_for_test()
                    .push_constant_for_test(1)
                    .build_for_test(),
                vec![Expr::default()],
                MyParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let exec_slow_col = |src_exec| {
            Box::new(BatchSlowHashAggregationFreeDaemon::new_for_test(
                src_exec,
                vec![RpnExpressionBuilder::new_for_test()
                    .push_PrimaryCauset_ref_for_test(0)
                    .build_for_test()],
                vec![Expr::default()],
                MyParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let exec_slow_const = |src_exec| {
            Box::new(BatchSlowHashAggregationFreeDaemon::new_for_test(
                src_exec,
                vec![RpnExpressionBuilder::new_for_test()
                    .push_constant_for_test(0)
                    .build_for_test()],
                vec![Expr::default()],
                MyParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let exec_slow_col_const = |src_exec| {
            Box::new(BatchSlowHashAggregationFreeDaemon::new_for_test(
                src_exec,
                vec![
                    RpnExpressionBuilder::new_for_test()
                        .push_PrimaryCauset_ref_for_test(0)
                        .build_for_test(),
                    RpnExpressionBuilder::new_for_test()
                        .push_constant_for_test(0)
                        .build_for_test(),
                ],
                vec![Expr::default()],
                MyParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let executor_builders: Vec<Box<dyn FnOnce(MockFreeDaemon) -> _>> = vec![
            Box::new(exec_fast_col),
            Box::new(exec_fast_const),
            Box::new(exec_slow_col),
            Box::new(exec_slow_const),
            Box::new(exec_slow_col_const),
        ];

        for exec_builder in executor_builders {
            let src_exec = MockFreeDaemon::new(
                vec![FieldTypeTp::LongLong.into()],
                vec![
                    BatchExecuteResult {
                        physical_PrimaryCausets: LazyBatchPrimaryCausetVec::empty(),
                        logical_rows: Vec::new(),
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(false),
                    },
                    BatchExecuteResult {
                        physical_PrimaryCausets: LazyBatchPrimaryCausetVec::empty(),
                        logical_rows: Vec::new(),
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(true),
                    },
                ],
            );
            let mut exec = exec_builder(src_exec);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(r.is_drained.unwrap());
        }
    }

    /// Only have GROUP BY PrimaryCausets but no Aggregate Functions.
    ///
    /// E.g. SELECT 1 FROM t GROUP BY x
    #[test]
    fn test_no_aggr_fn() {
        let exec_fast_col = |src_exec| {
            Box::new(BatchFastHashAggregationFreeDaemon::new_for_test(
                src_exec,
                RpnExpressionBuilder::new_for_test()
                    .push_PrimaryCauset_ref_for_test(0)
                    .build_for_test(),
                vec![],
                AllAggrDefinitionParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let exec_slow_col = |src_exec| {
            Box::new(BatchSlowHashAggregationFreeDaemon::new_for_test(
                src_exec,
                vec![RpnExpressionBuilder::new_for_test()
                    .push_PrimaryCauset_ref_for_test(0)
                    .build_for_test()],
                vec![],
                AllAggrDefinitionParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let executor_builders: Vec<Box<dyn FnOnce(MockFreeDaemon) -> _>> =
            vec![Box::new(exec_fast_col), Box::new(exec_slow_col)];

        for exec_builder in executor_builders {
            let src_exec = make_src_executor_1();
            let mut exec = exec_builder(src_exec);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let mut r = exec.next_batch(1);
            assert_eq!(&r.logical_rows, &[0, 1, 2]);
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 3);
            assert_eq!(r.physical_PrimaryCausets.PrimaryCausets_len(), 1); // 0 result PrimaryCauset, 1 group by PrimaryCauset
            r.physical_PrimaryCausets[0]
                .ensure_all_decoded_for_test(&mut EvalContext::default(), &exec.schemaReplicant()[0])
                .unwrap();
            let mut sort_PrimaryCauset: Vec<(usize, _)> = r.physical_PrimaryCausets[0]
                .decoded()
                .to_real_vec()
                .iter()
                .map(|v| {
                    use std::hash::{Hash, Hasher};
                    let mut s = std::collections::hash_map::DefaultHasher::new();
                    v.hash(&mut s);
                    s.finish()
                })
                .enumerate()
                .collect();
            sort_PrimaryCauset.sort_by(|a, b| a.1.cmp(&b.1));
            let ordered_PrimaryCauset: Vec<_> = sort_PrimaryCauset
                .iter()
                .map(|(idx, _)| r.physical_PrimaryCausets[0].decoded().to_real_vec()[*idx])
                .collect();
            assert_eq!(
                &ordered_PrimaryCauset,
                &[Real::new(1.5).ok(), None, Real::new(7.0).ok()]
            );
        }

        let exec_fast_const = |src_exec| {
            Box::new(BatchFastHashAggregationFreeDaemon::new_for_test(
                src_exec,
                RpnExpressionBuilder::new_for_test()
                    .push_constant_for_test(1)
                    .build_for_test(),
                vec![],
                AllAggrDefinitionParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let exec_slow_const = |src_exec| {
            Box::new(BatchSlowHashAggregationFreeDaemon::new_for_test(
                src_exec,
                vec![RpnExpressionBuilder::new_for_test()
                    .push_constant_for_test(1)
                    .build_for_test()],
                vec![],
                AllAggrDefinitionParser,
            )) as Box<dyn BatchFreeDaemon<StorageStats = ()>>
        };

        let executor_builders: Vec<Box<dyn FnOnce(MockFreeDaemon) -> _>> =
            vec![Box::new(exec_fast_const), Box::new(exec_slow_const)];

        for exec_builder in executor_builders {
            let src_exec = make_src_executor_1();
            let mut exec = exec_builder(src_exec);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let mut r = exec.next_batch(1);
            assert_eq!(&r.logical_rows, &[0]);
            assert_eq!(r.physical_PrimaryCausets.rows_len(), 1);
            assert_eq!(r.physical_PrimaryCausets.PrimaryCausets_len(), 1); // 0 result PrimaryCauset, 1 group by PrimaryCauset
            r.physical_PrimaryCausets[0]
                .ensure_all_decoded_for_test(&mut EvalContext::default(), &exec.schemaReplicant()[0])
                .unwrap();
            assert_eq!(r.physical_PrimaryCausets[0].decoded().to_int_vec(), &[Some(1)]);
        }
    }
}
