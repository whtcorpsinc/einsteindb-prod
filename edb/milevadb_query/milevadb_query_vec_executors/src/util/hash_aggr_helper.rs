// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use super::aggr_executor::*;
use crate::interface::*;
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::batch::LazyBatchPrimaryCausetVec;
use milevadb_query_datatype::codec::data_type::*;
use milevadb_query_vec_aggr::{fidelio, AggrFunctionState};
use milevadb_query_vec_expr::RpnStackNode;

pub struct HashAggregationHelper;

impl HashAggregationHelper {
    /// fidelios states for each Evcausetidx.
    ///
    /// Each Evcausetidx may belong to a different group. States of all groups should be passed in altogether
    /// in a single vector and the states of each Evcausetidx should be specified by an offset vector.
    pub fn fidelio_each_row_states_by_offset<Src: BatchFreeDaemon>(
        entities: &mut Entities<Src>,
        input_physical_PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
        input_logical_rows: &[usize],
        states: &mut [Box<dyn AggrFunctionState>],
        states_offset_each_logical_row: &[usize],
    ) -> Result<()> {
        let logical_rows_len = input_logical_rows.len();
        let src_schemaReplicant = entities.src.schemaReplicant();

        for idx in 0..entities.each_aggr_fn.len() {
            let aggr_expr = &entities.each_aggr_exprs[idx];
            let aggr_expr_result = aggr_expr.eval(
                &mut entities.context,
                src_schemaReplicant,
                input_physical_PrimaryCausets,
                input_logical_rows,
                logical_rows_len,
            )?;
            match aggr_expr_result {
                RpnStackNode::Scalar { value, .. } => {
                    match_template_evaluable! {
                        TT, match value.as_scalar_value_ref() {
                            ScalarValueRef::TT(scalar_value) => {
                                for offset in states_offset_each_logical_row {
                                    let aggr_fn_state = &mut states[*offset + idx];
                                    fidelio!(aggr_fn_state, &mut entities.context, scalar_value)?;
                                }
                            },
                        }
                    }
                }
                RpnStackNode::Vector { value, .. } => {
                    let physical_vec = value.as_ref();
                    let logical_rows = value.logical_rows_struct();
                    match_template_evaluable! {
                        TT, match physical_vec {
                            VectorValue::TT(vec) => {
                                for (states_offset, physical_idx) in states_offset_each_logical_row
                                    .iter()
                                    .zip(logical_rows)
                                {
                                    let aggr_fn_state = &mut states[*states_offset + idx];
                                    fidelio!(aggr_fn_state, &mut entities.context, vec.get_option_ref(physical_idx))?;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
