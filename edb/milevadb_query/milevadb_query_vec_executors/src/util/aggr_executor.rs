// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! Concept:
//!
//! ```ignore
//! SELECT COUNT(1), COUNT(COL) FROM Block GROUP BY COL+1, COL2
//!        ^^^^^     ^^^^^                                         : Aggregate Functions
//!              ^         ^^^                                     : Aggregate Function Expressions
//!                                                 ^^^^^  ^^^^    : Group By Expressions
//! ```
//!
//! The SQL above has 2 GROUP BY PrimaryCausets, so we say it's *group by cardinality* is 2.
//!
//! In the result:
//!
//! ```ignore
//!     COUNT(1)     COUNT(COL)         COL+1      COL2
//!     1            1                  1          1            <--- Each Evcausetidx is the result
//!     1            2                  1          1            <--- of a group
//!
//!     ^^^^^^^^^    ^^^^^^^^^^^                                : Aggregate Result PrimaryCauset
//!                                     ^^^^^^     ^^^^^        : Group By PrimaryCauset
//! ```
//!
//! Some aggregate function output multiple results, for example, `AVG(Int)` output two results:
//! count and sum. In this case we say that the result of `AVG(Int)` has a *cardinality* of 2.
//!

use std::convert::TryFrom;
use std::sync::Arc;

use milevadb_query_datatype::{EvalType, FieldTypeAccessor};
use fidel_timeshare::{Expr, FieldType};

use crate::interface::*;
use milevadb_query_common::causet_storage::IntervalCone;
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::batch::{LazyBatchPrimaryCauset, LazyBatchPrimaryCausetVec};
use milevadb_query_datatype::codec::data_type::*;
use milevadb_query_datatype::expr::{EvalConfig, EvalContext};
use milevadb_query_vec_aggr::*;
use milevadb_query_vec_expr::RpnExpression;

pub trait AggregationFreeDaemonImpl<Src: BatchFreeDaemon>: lightlike {
    /// Accepts entities without any group by PrimaryCausets and modifies them optionally.
    ///
    /// Implementors should modify the `schemaReplicant` entity when there are group by PrimaryCausets.
    ///
    /// This function will be called only once.
    fn prepare_entities(&mut self, entities: &mut Entities<Src>);

    /// Processes a set of PrimaryCausets which are emitted from the underlying executor.
    ///
    /// Implementors should fidelio the aggregate function states according to the data of
    /// these PrimaryCausets.
    fn process_batch_input(
        &mut self,
        entities: &mut Entities<Src>,
        input_physical_PrimaryCausets: LazyBatchPrimaryCausetVec,
        input_logical_rows: &[usize],
    ) -> Result<()>;

    /// Returns the current number of groups.
    ///
    /// Note that this number can be inaccurate because it is a hint for the capacity of the vector.
    fn groups_len(&self) -> usize;

    /// Iterates aggregate function states for each available group.
    ///
    /// Implementors should call `iteratee` for each group with the aggregate function states of
    /// that group as the argument.
    ///
    /// Implementors may return the content of each group as extra PrimaryCausets in the return value
    /// if there are group by PrimaryCausets.
    ///
    /// Implementors should not iterate the same group multiple times for the same partial
    /// input data.
    fn iterate_available_groups(
        &mut self,
        entities: &mut Entities<Src>,
        src_is_drained: bool,
        iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchPrimaryCauset>>;

    /// Returns whether we can now output partial aggregate results when the source is not drained.
    ///
    /// This method is called only when the source is not drained because aggregate result is always
    /// ready if the source is drained and no error occurs.
    fn is_partial_results_ready(&self) -> bool;
}

/// Some common data that need to be accessed by both `AggregationFreeDaemon`
/// and `AggregationFreeDaemonImpl`.
pub struct Entities<Src: BatchFreeDaemon> {
    pub src: Src,
    pub context: EvalContext,

    /// The schemaReplicant of the aggregation executor. It consists of aggregate result PrimaryCausets and
    /// group by PrimaryCausets.
    pub schemaReplicant: Vec<FieldType>,

    /// The aggregate function.
    pub each_aggr_fn: Vec<Box<dyn AggrFunction>>,

    /// The (output result) cardinality of each aggregate function.
    pub each_aggr_cardinality: Vec<usize>,

    /// The (input) expression of each aggregate function.
    pub each_aggr_exprs: Vec<RpnExpression>,

    /// The eval type of the result PrimaryCausets of all aggregate functions. One aggregate function
    /// may have multiple result PrimaryCausets.
    pub all_result_PrimaryCauset_types: Vec<EvalType>,
}

/// A shared executor implementation for simple aggregation, hash aggregation and
/// stream aggregation. Implementation differences are further given via `AggregationFreeDaemonImpl`.
pub struct AggregationFreeDaemon<Src: BatchFreeDaemon, I: AggregationFreeDaemonImpl<Src>> {
    imp: I,
    is_lightlikeed: bool,
    entities: Entities<Src>,
}

impl<Src: BatchFreeDaemon, I: AggregationFreeDaemonImpl<Src>> AggregationFreeDaemon<Src, I> {
    pub fn new(
        mut imp: I,
        src: Src,
        config: Arc<EvalConfig>,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        let aggr_fn_len = aggr_defs.len();
        let src_schemaReplicant = src.schemaReplicant();

        let mut schemaReplicant = Vec::with_capacity(aggr_fn_len * 2);
        let mut each_aggr_fn = Vec::with_capacity(aggr_fn_len);
        let mut each_aggr_cardinality = Vec::with_capacity(aggr_fn_len);
        let mut each_aggr_exprs = Vec::with_capacity(aggr_fn_len);
        let mut ctx = EvalContext::new(config.clone());

        for aggr_def in aggr_defs {
            let schemaReplicant_len = schemaReplicant.len();
            let each_aggr_exprs_len = each_aggr_exprs.len();

            let aggr_fn = aggr_def_parser.parse(
                aggr_def,
                &mut ctx,
                src_schemaReplicant,
                &mut schemaReplicant,
                &mut each_aggr_exprs,
            )?;

            assert!(schemaReplicant.len() > schemaReplicant_len);
            // Currently only support 1 parameter aggregate functions, so let's simply assert it.
            assert_eq!(each_aggr_exprs.len(), each_aggr_exprs_len + 1);

            each_aggr_fn.push(aggr_fn);
            each_aggr_cardinality.push(schemaReplicant.len() - schemaReplicant_len);
        }

        let all_result_PrimaryCauset_types = schemaReplicant
            .iter()
            .map(|ft| {
                // The unwrap is fine because aggregate function parser should never return an
                // eval type that we cannot process later. If we made a mistake there, then we
                // should panic.
                EvalType::try_from(ft.as_accessor().tp()).unwrap()
            })
            .collect();

        let mut entities = Entities {
            src,
            context: EvalContext::new(config),
            schemaReplicant,
            each_aggr_fn,
            each_aggr_cardinality,
            each_aggr_exprs,
            all_result_PrimaryCauset_types,
        };
        imp.prepare_entities(&mut entities);

        Ok(Self {
            imp,
            is_lightlikeed: false,
            entities,
        })
    }

    /// Returns partial results of aggregation if available and whether the source is drained
    #[inline]
    fn handle_next_batch(&mut self) -> Result<(Option<LazyBatchPrimaryCausetVec>, bool)> {
        // Use max batch size from the beginning because aggregation
        // always needs to calculate over all data.
        let src_result = self.entities.src.next_batch(crate::runner::BATCH_MAX_SIZE);

        self.entities.context.warnings = src_result.warnings;

        // When there are errors in the underlying executor, there must be no aggregate output.
        // Thus we even don't need to fidelio the aggregate function state and can return directly.
        let src_is_drained = src_result.is_drained?;

        // Consume all data from the underlying executor. We directly return when there are errors
        // for the same reason as above.
        if !src_result.logical_rows.is_empty() {
            self.imp.process_batch_input(
                &mut self.entities,
                src_result.physical_PrimaryCausets,
                &src_result.logical_rows,
            )?;
        }

        // aggregate result is always available when source is drained
        let result = if src_is_drained || self.imp.is_partial_results_ready() {
            Some(self.aggregate_partial_results(src_is_drained)?)
        } else {
            None
        };
        Ok((result, src_is_drained))
    }

    /// Generates aggregation results of available groups.
    fn aggregate_partial_results(&mut self, src_is_drained: bool) -> Result<LazyBatchPrimaryCausetVec> {
        let groups_len = self.imp.groups_len();
        let mut all_result_PrimaryCausets: Vec<_> = self
            .entities
            .all_result_PrimaryCauset_types
            .iter()
            .map(|eval_type| VectorValue::with_capacity(groups_len, *eval_type))
            .collect();

        // Pull aggregate results of each available group
        let group_by_PrimaryCausets = self.imp.iterate_available_groups(
            &mut self.entities,
            src_is_drained,
            |entities, states| {
                assert_eq!(states.len(), entities.each_aggr_cardinality.len());

                let mut offset = 0;
                for (state, result_cardinality) in
                    states.iter().zip(&entities.each_aggr_cardinality)
                {
                    assert!(*result_cardinality > 0);

                    state.push_result(
                        &mut entities.context,
                        &mut all_result_PrimaryCausets[offset..offset + *result_cardinality],
                    )?;

                    offset += *result_cardinality;
                }

                Ok(())
            },
        )?;

        // The return PrimaryCausets consist of aggregate result PrimaryCausets and group by PrimaryCausets.
        let PrimaryCausets: Vec<_> = all_result_PrimaryCausets
            .into_iter()
            .map(|c| LazyBatchPrimaryCauset::Decoded(c))
            .chain(group_by_PrimaryCausets)
            .collect();
        let ret = LazyBatchPrimaryCausetVec::from(PrimaryCausets);
        ret.assert_PrimaryCausets_equal_length();
        Ok(ret)
    }
}

impl<Src: BatchFreeDaemon, I: AggregationFreeDaemonImpl<Src>> BatchFreeDaemon
    for AggregationFreeDaemon<Src, I>
{
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schemaReplicant(&self) -> &[FieldType] {
        self.entities.schemaReplicant.as_slice()
    }

    #[inline]
    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_lightlikeed);

        let result = self.handle_next_batch();

        match result {
            Err(e) => {
                // When there are error, we can just return empty data.
                self.is_lightlikeed = true;
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: self.entities.context.take_warnings(),
                    is_drained: Err(e),
                }
            }
            Ok((data, src_is_drained)) => {
                self.is_lightlikeed = src_is_drained;
                let logical_PrimaryCausets = data.unwrap_or_else(LazyBatchPrimaryCausetVec::empty);
                let logical_rows = (0..logical_PrimaryCausets.rows_len()).collect();
                BatchExecuteResult {
                    physical_PrimaryCausets: logical_PrimaryCausets,
                    logical_rows,
                    warnings: self.entities.context.take_warnings(),
                    is_drained: Ok(src_is_drained),
                }
            }
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.entities.src.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_causet_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.entities.src.collect_causet_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_cone(&mut self) -> IntervalCone {
        self.entities.src.take_scanned_cone()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.entities.src.can_be_cached()
    }
}

/// Shared test facilities for different aggregation executors.
#[causet(test)]
pub mod tests {
    use milevadb_query_codegen::AggrFunction;
    use milevadb_query_datatype::builder::FieldTypeBuilder;
    use milevadb_query_datatype::{Collation, FieldTypeTp};

    use crate::interface::*;
    use crate::util::mock_executor::MockFreeDaemon;
    use milevadb_query_common::Result;
    use milevadb_query_datatype::codec::batch::LazyBatchPrimaryCausetVec;
    use milevadb_query_datatype::codec::data_type::*;
    use milevadb_query_datatype::expr::{EvalContext, EvalWarnings};
    use milevadb_query_vec_aggr::*;

    #[derive(Debug, AggrFunction)]
    #[aggr_function(state = AggrFnUnreachableState)]
    pub struct AggrFnUnreachable;

    #[derive(Debug)]
    pub struct AggrFnUnreachableState;

    impl ConcreteAggrFunctionState for AggrFnUnreachableState {
        type ParameterType = &'static Real;

        unsafe fn fidelio_concrete_unsafe(
            &mut self,
            _ctx: &mut EvalContext,
            _value: Option<Self::ParameterType>,
        ) -> Result<()> {
            unreachable!()
        }

        fn push_result(&self, _ctx: &mut EvalContext, _target: &mut [VectorValue]) -> Result<()> {
            unreachable!()
        }
    }

    /// Builds an executor that will return these logical data:
    ///
    /// == SchemaReplicant ==
    /// Col0(Real)   Col1(Real)  Col2(Bytes) Col3(Int)  Col4(Bytes-utf8_general_ci)
    /// == Call #1 ==
    /// NULL         1.0         abc         1          aa
    /// 7.0          2.0         NULL        NULL       aaa
    /// NULL         NULL        ""          NULL       áá
    /// NULL         4.5         HelloWorld  NULL       NULL
    /// == Call #2 ==
    /// == Call #3 ==
    /// 1.5          4.5         aaaaa       5          ááá
    /// (drained)
    pub fn make_src_executor_1() -> MockFreeDaemon {
        MockFreeDaemon::new(
            vec![
                FieldTypeTp::Double.into(),
                FieldTypeTp::Double.into(),
                FieldTypeTp::VarString.into(),
                FieldTypeTp::LongLong.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarString)
                    .collation(Collation::Utf8Mb4GeneralCi)
                    .into(),
            ],
            vec![
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![
                        VectorValue::Real(
                            vec![None, None, None, Real::new(-5.0).ok(), Real::new(7.0).ok()]
                                .into(),
                        ),
                        VectorValue::Real(
                            vec![
                                None,
                                Real::new(4.5).ok(),
                                Real::new(1.0).ok(),
                                None,
                                Real::new(2.0).ok(),
                            ]
                            .into(),
                        ),
                        VectorValue::Bytes(
                            vec![
                                Some(vec![]),
                                Some(b"HelloWorld".to_vec()),
                                Some(b"abc".to_vec()),
                                None,
                                None,
                            ]
                            .into(),
                        ),
                        VectorValue::Int(vec![None, None, Some(1), Some(10), None].into()),
                        VectorValue::Bytes(
                            vec![
                                Some("áá".as_bytes().to_vec()),
                                None,
                                Some(b"aa".to_vec()),
                                Some("ááá".as_bytes().to_vec()),
                                Some(b"aaa".to_vec()),
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![2, 4, 0, 1],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![
                        VectorValue::Real(vec![None].into()),
                        VectorValue::Real(vec![Real::new(-10.0).ok()].into()),
                        VectorValue::Bytes(vec![Some(b"foo".to_vec())].into()),
                        VectorValue::Int(vec![None].into()),
                        VectorValue::Bytes(vec![None].into()),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_PrimaryCausets: LazyBatchPrimaryCausetVec::from(vec![
                        VectorValue::Real(vec![Real::new(5.5).ok(), Real::new(1.5).ok()].into()),
                        VectorValue::Real(vec![None, Real::new(4.5).ok()].into()),
                        VectorValue::Bytes(vec![None, Some(b"aaaaa".to_vec())].into()),
                        VectorValue::Int(vec![None, Some(5)].into()),
                        VectorValue::Bytes(
                            vec![
                                Some("áá".as_bytes().to_vec()),
                                Some("ááá".as_bytes().to_vec()),
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![1],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }
}
