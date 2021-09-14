// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use protobuf::Message;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fail::fail_point;
use ekvproto::interlock::KeyCone;
use milevadb_query_datatype::{EvalType, FieldTypeAccessor};
use violetabftstore::interlock::::deadline::Deadline;
use fidel_timeshare::StreamResponse;
use fidel_timeshare::{self, ExecType, FreeDaemonExecutionSummary, FieldType};
use fidel_timeshare::{Soliton, PosetDagRequest, EncodeType, SelectResponse};
use yatp::task::future::reschedule;

use super::interface::{BatchFreeDaemon, ExecuteStats};
use super::*;
use milevadb_query_common::metrics::*;
use milevadb_query_common::causet_storage::{IntervalCone, causet_storage};
use milevadb_query_common::Result;
use milevadb_query_datatype::expr::{EvalConfig, EvalContext, EvalWarnings};

// TODO: The value is chosen according to some very subjective experience, which is not tuned
// carefully. We need to benchmark to find a best value. Also we may consider accepting this value
// from MilevaDB side.
const BATCH_INITIAL_SIZE: usize = 32;

// TODO: This value is chosen based on MonetDB/X100's research without our own benchmarks.
pub use milevadb_query_vec_expr::types::BATCH_MAX_SIZE;

// TODO: Maybe there can be some better strategy. Needs benchmarks and tunes.
const BATCH_GROW_FACTOR: usize = 2;

/// Batch executors are run in coroutines. `MAX_TIME_SLICE` is the maximum time a coroutine
/// can run without being yielded.
pub const MAX_TIME_SLICE: Duration = Duration::from_millis(1);

pub struct BatchFreeDaemonsRunner<SS> {
    /// The deadline of this handler. For each check point (e.g. each iteration) we need to check
    /// whether or not the deadline is exceeded and break the process if so.
    // TODO: Deprecate it using a better deadline mechanism.
    deadline: Deadline,

    out_most_executor: Box<dyn BatchFreeDaemon<StorageStats = SS>>,

    /// The offset of the PrimaryCausets need to be outputted. For example, MilevaDB may only needs a subset
    /// of the PrimaryCausets in the result so that unrelated PrimaryCausets don't need to be encoded and
    /// returned back.
    output_offsets: Vec<u32>,

    config: Arc<EvalConfig>,

    /// Whether or not execution summary need to be collected.
    collect_exec_summary: bool,

    exec_stats: ExecuteStats,

    /// Maximum events to return in batch stream mode.
    stream_row_limit: usize,

    /// The encoding method for the response.
    /// Possible encoding methods are:
    /// 1. default: result is encoded Evcausetidx by Evcausetidx using datum format.
    /// 2. Soliton: result is encoded PrimaryCauset by PrimaryCauset using Soliton format.
    encode_type: EncodeType,
}

// We assign a dummy type `()` so that we can omit the type when calling `check_supported`.
impl BatchFreeDaemonsRunner<()> {
    /// Given a list of executor descriptors and checks whether all executor descriptors can
    /// be used to build batch executors.
    pub fn check_supported(exec_descriptors: &[fidel_timeshare::FreeDaemon]) -> Result<()> {
        for ed in exec_descriptors {
            match ed.get_tp() {
                ExecType::TypeBlockScan => {
                    let descriptor = ed.get_tbl_scan();
                    BatchBlockScanFreeDaemon::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchBlockScanFreeDaemon: {}", e))?;
                }
                ExecType::TypeIndexScan => {
                    let descriptor = ed.get_idx_scan();
                    BatchIndexScanFreeDaemon::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchIndexScanFreeDaemon: {}", e))?;
                }
                ExecType::TypeSelection => {
                    let descriptor = ed.get_selection();
                    BatchSelectionFreeDaemon::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchSelectionFreeDaemon: {}", e))?;
                }
                ExecType::TypeAggregation | ExecType::TypeStreamAgg
                    if ed.get_aggregation().get_group_by().is_empty() =>
                {
                    let descriptor = ed.get_aggregation();
                    BatchSimpleAggregationFreeDaemon::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchSimpleAggregationFreeDaemon: {}", e))?;
                }
                ExecType::TypeAggregation => {
                    let descriptor = ed.get_aggregation();
                    if BatchFastHashAggregationFreeDaemon::check_supported(&descriptor).is_err() {
                        BatchSlowHashAggregationFreeDaemon::check_supported(&descriptor)
                            .map_err(|e| other_err!("BatchSlowHashAggregationFreeDaemon: {}", e))?;
                    }
                }
                ExecType::TypeStreamAgg => {
                    // Note: We won't check whether the source of stream aggregation is in order.
                    //       It is undefined behavior if the source is unordered.
                    let descriptor = ed.get_aggregation();
                    BatchStreamAggregationFreeDaemon::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchStreamAggregationFreeDaemon: {}", e))?;
                }
                ExecType::TypeLimit => {}
                ExecType::TypeTopN => {
                    let descriptor = ed.get_top_n();
                    BatchTopNFreeDaemon::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchTopNFreeDaemon: {}", e))?;
                }
            }
        }

        Ok(())
    }
}

#[inline]
fn is_arrow_encodable(schemaReplicant: &[FieldType]) -> bool {
    schemaReplicant
        .iter()
        .all(|schemaReplicant| EvalType::try_from(schemaReplicant.as_accessor().tp()).is_ok())
}

#[allow(clippy::explicit_counter_loop)]
pub fn build_executors<S: causet_storage + 'static>(
    executor_descriptors: Vec<fidel_timeshare::FreeDaemon>,
    causet_storage: S,
    cones: Vec<KeyCone>,
    config: Arc<EvalConfig>,
    is_scanned_cone_aware: bool,
) -> Result<Box<dyn BatchFreeDaemon<StorageStats = S::Statistics>>> {
    let mut executor_descriptors = executor_descriptors.into_iter();
    let mut first_ed = executor_descriptors
        .next()
        .ok_or_else(|| other_err!("No executors"))?;

    let mut executor: Box<dyn BatchFreeDaemon<StorageStats = S::Statistics>>;
    let mut summary_slot_index = 0;

    match first_ed.get_tp() {
        ExecType::TypeBlockScan => {
            EXECUTOR_COUNT_METRICS.batch_Block_scan.inc();

            let mut descriptor = first_ed.take_tbl_scan();
            let PrimaryCausets_info = descriptor.take_PrimaryCausets().into();
            let primary_PrimaryCauset_ids = descriptor.take_primary_PrimaryCauset_ids().into();

            executor = Box::new(
                BatchBlockScanFreeDaemon::new(
                    causet_storage,
                    config.clone(),
                    PrimaryCausets_info,
                    cones,
                    primary_PrimaryCauset_ids,
                    descriptor.get_desc(),
                    is_scanned_cone_aware,
                )?
                .collect_summary(summary_slot_index),
            );
        }
        ExecType::TypeIndexScan => {
            EXECUTOR_COUNT_METRICS.batch_index_scan.inc();

            let mut descriptor = first_ed.take_idx_scan();
            let PrimaryCausets_info = descriptor.take_PrimaryCausets().into();
            let primary_PrimaryCauset_ids_len = descriptor.take_primary_PrimaryCauset_ids().len();
            executor = Box::new(
                BatchIndexScanFreeDaemon::new(
                    causet_storage,
                    config.clone(),
                    PrimaryCausets_info,
                    cones,
                    primary_PrimaryCauset_ids_len,
                    descriptor.get_desc(),
                    descriptor.get_unique(),
                    is_scanned_cone_aware,
                )?
                .collect_summary(summary_slot_index),
            );
        }
        _ => {
            return Err(other_err!(
                "Unexpected first executor {:?}",
                first_ed.get_tp()
            ));
        }
    }

    for mut ed in executor_descriptors {
        summary_slot_index += 1;

        let new_executor: Box<dyn BatchFreeDaemon<StorageStats = S::Statistics>> = match ed.get_tp() {
            ExecType::TypeSelection => {
                EXECUTOR_COUNT_METRICS.batch_selection.inc();

                Box::new(
                    BatchSelectionFreeDaemon::new(
                        config.clone(),
                        executor,
                        ed.take_selection().take_conditions().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeAggregation | ExecType::TypeStreamAgg
                if ed.get_aggregation().get_group_by().is_empty() =>
            {
                EXECUTOR_COUNT_METRICS.batch_simple_aggr.inc();

                Box::new(
                    BatchSimpleAggregationFreeDaemon::new(
                        config.clone(),
                        executor,
                        ed.mut_aggregation().take_agg_func().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeAggregation => {
                if BatchFastHashAggregationFreeDaemon::check_supported(&ed.get_aggregation()).is_ok()
                {
                    EXECUTOR_COUNT_METRICS.batch_fast_hash_aggr.inc();

                    Box::new(
                        BatchFastHashAggregationFreeDaemon::new(
                            config.clone(),
                            executor,
                            ed.mut_aggregation().take_group_by().into(),
                            ed.mut_aggregation().take_agg_func().into(),
                        )?
                        .collect_summary(summary_slot_index),
                    )
                } else {
                    EXECUTOR_COUNT_METRICS.batch_slow_hash_aggr.inc();

                    Box::new(
                        BatchSlowHashAggregationFreeDaemon::new(
                            config.clone(),
                            executor,
                            ed.mut_aggregation().take_group_by().into(),
                            ed.mut_aggregation().take_agg_func().into(),
                        )?
                        .collect_summary(summary_slot_index),
                    )
                }
            }
            ExecType::TypeStreamAgg => {
                EXECUTOR_COUNT_METRICS.batch_stream_aggr.inc();

                Box::new(
                    BatchStreamAggregationFreeDaemon::new(
                        config.clone(),
                        executor,
                        ed.mut_aggregation().take_group_by().into(),
                        ed.mut_aggregation().take_agg_func().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeLimit => {
                EXECUTOR_COUNT_METRICS.batch_limit.inc();

                Box::new(
                    BatchLimitFreeDaemon::new(executor, ed.get_limit().get_limit() as usize)?
                        .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeTopN => {
                EXECUTOR_COUNT_METRICS.batch_top_n.inc();

                let mut d = ed.take_top_n();
                let order_bys = d.get_order_by().len();
                let mut order_exprs_def = Vec::with_capacity(order_bys);
                let mut order_is_desc = Vec::with_capacity(order_bys);
                for mut item in d.take_order_by().into_iter() {
                    order_exprs_def.push(item.take_expr());
                    order_is_desc.push(item.get_desc());
                }

                Box::new(
                    BatchTopNFreeDaemon::new(
                        config.clone(),
                        executor,
                        order_exprs_def,
                        order_is_desc,
                        d.get_limit() as usize,
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            _ => {
                return Err(other_err!(
                    "Unexpected non-first executor {:?}",
                    ed.get_tp()
                ));
            }
        };
        executor = new_executor;
    }

    Ok(executor)
}

impl<SS: 'static> BatchFreeDaemonsRunner<SS> {
    pub fn from_request<S: causet_storage<Statistics = SS> + 'static>(
        mut req: PosetDagRequest,
        cones: Vec<KeyCone>,
        causet_storage: S,
        deadline: Deadline,
        stream_row_limit: usize,
        is_streaming: bool,
    ) -> Result<Self> {
        let executors_len = req.get_executors().len();
        let collect_exec_summary = req.get_collect_execution_summaries();
        let config = Arc::new(EvalConfig::from_request(&req)?);

        let out_most_executor = build_executors(
            req.take_executors().into(),
            causet_storage,
            cones,
            config.clone(),
            is_streaming, // For streaming request, executors will continue scan from cone lightlike where last scan is finished
        )?;

        let encode_type = if !is_arrow_encodable(out_most_executor.schemaReplicant()) {
            EncodeType::TypeDefault
        } else {
            req.get_encode_type()
        };

        // Check output offsets
        let output_offsets = req.take_output_offsets();
        let schemaReplicant_len = out_most_executor.schemaReplicant().len();
        for offset in &output_offsets {
            if (*offset as usize) >= schemaReplicant_len {
                return Err(other_err!(
                    "Invalid output offset (schemaReplicant has {} PrimaryCausets, access index {})",
                    schemaReplicant_len,
                    offset
                ));
            }
        }

        let exec_stats = ExecuteStats::new(executors_len);

        Ok(Self {
            deadline,
            out_most_executor,
            output_offsets,
            config,
            collect_exec_summary,
            exec_stats,
            encode_type,
            stream_row_limit,
        })
    }

    fn batch_initial_size() -> usize {
        fail_point!("copr_batch_initial_size", |r| r
            .map_or(1, |e| e.parse().unwrap()));
        BATCH_INITIAL_SIZE
    }

    pub async fn handle_request(&mut self) -> Result<SelectResponse> {
        let mut Solitons = vec![];
        let mut batch_size = Self::batch_initial_size();
        let mut warnings = self.config.new_eval_warnings();
        let mut ctx = EvalContext::new(self.config.clone());

        let mut time_slice_spacelike = Instant::now();
        loop {
            let time_slice_len = time_slice_spacelike.elapsed();
            // Check whether we should yield from the execution
            if time_slice_len > MAX_TIME_SLICE {
                reschedule().await;
                time_slice_spacelike = Instant::now();
            }

            let mut Soliton = Soliton::default();

            let (drained, record_len) = self.internal_handle_request(
                false,
                batch_size,
                &mut Soliton,
                &mut warnings,
                &mut ctx,
            )?;

            if record_len > 0 {
                Solitons.push(Soliton);
            }

            if drained {
                self.out_most_executor
                    .collect_exec_stats(&mut self.exec_stats);

                let mut sel_resp = SelectResponse::default();
                sel_resp.set_Solitons(Solitons.into());
                sel_resp.set_encode_type(self.encode_type);

                // TODO: output_counts should not be i64. Let's fix it in Interlock DAG V2.
                sel_resp.set_output_counts(
                    self.exec_stats
                        .scanned_rows_per_cone
                        .iter()
                        .map(|v| *v as i64)
                        .collect(),
                );

                if self.collect_exec_summary {
                    let summaries = self
                        .exec_stats
                        .summary_per_executor
                        .iter()
                        .map(|summary| {
                            let mut ret = FreeDaemonExecutionSummary::default();
                            ret.set_num_iterations(summary.num_iterations as u64);
                            ret.set_num_produced_rows(summary.num_produced_rows as u64);
                            ret.set_time_processed_ns(summary.time_processed_ns as u64);
                            ret
                        })
                        .collect::<Vec<_>>();
                    sel_resp.set_execution_summaries(summaries.into());
                }

                sel_resp.set_warnings(warnings.warnings.into());
                sel_resp.set_warning_count(warnings.warning_cnt as i64);

                // In case of this function is called multiple times.
                self.exec_stats.clear();

                return Ok(sel_resp);
            }

            // Grow batch size
            grow_batch_size(&mut batch_size);
        }
    }

    pub fn handle_streaming_request(
        &mut self,
    ) -> Result<(Option<(StreamResponse, IntervalCone)>, bool)> {
        let mut warnings = self.config.new_eval_warnings();

        let (mut record_len, mut is_drained) = (0, false);
        let mut Soliton = Soliton::default();
        let mut ctx = EvalContext::new(self.config.clone());
        let batch_size = self.stream_row_limit.min(BATCH_MAX_SIZE);

        // record count less than batch size and is not drained
        while record_len < self.stream_row_limit && !is_drained {
            let mut current_Soliton = Soliton::default();
            let (drained, len) = self.internal_handle_request(
                true,
                batch_size.min(self.stream_row_limit - record_len),
                &mut current_Soliton,
                &mut warnings,
                &mut ctx,
            )?;
            Soliton
                .mut_rows_data()
                .extlightlike_from_slice(current_Soliton.get_rows_data());
            record_len += len;
            is_drained = drained;
        }

        if !is_drained || record_len > 0 {
            let cone = self.out_most_executor.take_scanned_cone();
            return self
                .make_stream_response(Soliton, warnings)
                .map(|r| (Some((r, cone)), is_drained));
        }
        Ok((None, true))
    }

    pub fn collect_causet_storage_stats(&mut self, dest: &mut SS) {
        self.out_most_executor.collect_causet_storage_stats(dest);
    }

    pub fn can_be_cached(&self) -> bool {
        self.out_most_executor.can_be_cached()
    }

    fn internal_handle_request(
        &mut self,
        is_streaming: bool,
        batch_size: usize,
        Soliton: &mut Soliton,
        warnings: &mut EvalWarnings,
        ctx: &mut EvalContext,
    ) -> Result<(bool, usize)> {
        let mut record_len = 0;

        self.deadline.check()?;

        let mut result = self.out_most_executor.next_batch(batch_size);

        let is_drained = result.is_drained?;

        if !result.logical_rows.is_empty() {
            assert_eq!(
                result.physical_PrimaryCausets.PrimaryCausets_len(),
                self.out_most_executor.schemaReplicant().len()
            );
            {
                let data = Soliton.mut_rows_data();
                // Although `schemaReplicant()` can be deeply nested, it is ok since we process data in
                // batch.
                if is_streaming || self.encode_type == EncodeType::TypeDefault {
                    data.reserve(
                        result
                            .physical_PrimaryCausets
                            .maximum_encoded_size(&result.logical_rows, &self.output_offsets),
                    );
                    result.physical_PrimaryCausets.encode(
                        &result.logical_rows,
                        &self.output_offsets,
                        self.out_most_executor.schemaReplicant(),
                        data,
                        ctx,
                    )?;
                } else {
                    data.reserve(
                        result
                            .physical_PrimaryCausets
                            .maximum_encoded_size_Soliton(&result.logical_rows, &self.output_offsets),
                    );
                    result.physical_PrimaryCausets.encode_Soliton(
                        &result.logical_rows,
                        &self.output_offsets,
                        self.out_most_executor.schemaReplicant(),
                        data,
                        ctx,
                    )?;
                }
            }
            record_len += result.logical_rows.len();
        }

        warnings.merge(&mut result.warnings);
        Ok((is_drained, record_len))
    }

    fn make_stream_response(
        &mut self,
        Soliton: Soliton,
        warnings: EvalWarnings,
    ) -> Result<StreamResponse> {
        self.out_most_executor
            .collect_exec_stats(&mut self.exec_stats);

        let mut s_resp = StreamResponse::default();
        s_resp.set_data(box_try!(Soliton.write_to_bytes()));

        s_resp.set_output_counts(
            self.exec_stats
                .scanned_rows_per_cone
                .iter()
                .map(|v| *v as i64)
                .collect(),
        );

        s_resp.set_warnings(warnings.warnings.into());
        s_resp.set_warning_count(warnings.warning_cnt as i64);

        self.exec_stats.clear();

        Ok(s_resp)
    }
}

#[inline]
fn grow_batch_size(batch_size: &mut usize) {
    if *batch_size < BATCH_MAX_SIZE {
        *batch_size *= BATCH_GROW_FACTOR;
        if *batch_size > BATCH_MAX_SIZE {
            *batch_size = BATCH_MAX_SIZE
        }
    }
}
