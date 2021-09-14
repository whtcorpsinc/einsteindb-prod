// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::Arc;

use ekvproto::interlock::KeyCone;
use protobuf::Message;
use fidel_timeshare::{self, ExecType, FreeDaemonExecutionSummary};
use fidel_timeshare::{Soliton, PosetDagRequest, SelectResponse, StreamResponse};

use violetabftstore::interlock::::deadline::Deadline;

use super::FreeDaemon;
use milevadb_query_common::execute_stats::*;
use milevadb_query_common::metrics::*;
use milevadb_query_common::causet_storage::{IntervalCone, causet_storage};
use milevadb_query_common::Result;
use milevadb_query_datatype::expr::{EvalConfig, EvalContext};

pub struct FreeDaemonsRunner<SS> {
    deadline: Deadline,
    executor: Box<dyn FreeDaemon<StorageStats = SS> + lightlike>,
    output_offsets: Vec<u32>,
    batch_row_limit: usize,
    collect_exec_summary: bool,
    context: EvalContext,
    exec_stats: ExecuteStats,
}

/// Builds a normal executor pipeline.
///
/// Normal executors iterate events one by one.
#[allow(clippy::explicit_counter_loop)]
pub fn build_executors<S: causet_storage + 'static, C: ExecSummaryCollector + 'static>(
    exec_descriptors: Vec<fidel_timeshare::FreeDaemon>,
    causet_storage: S,
    cones: Vec<KeyCone>,
    ctx: Arc<EvalConfig>,
    is_streaming: bool,
) -> Result<Box<dyn FreeDaemon<StorageStats = S::Statistics> + lightlike>> {
    let mut exec_descriptors = exec_descriptors.into_iter();
    let first = exec_descriptors
        .next()
        .ok_or_else(|| other_err!("No executor specified"))?;

    let mut src = build_first_executor::<_, C>(first, causet_storage, cones, ctx.clone(), is_streaming)?;
    let mut summary_slot_index = 0;

    for mut exec in exec_descriptors {
        summary_slot_index += 1;

        let curr: Box<dyn FreeDaemon<StorageStats = S::Statistics> + lightlike> = match exec.get_tp() {
            ExecType::TypeSelection => {
                EXECUTOR_COUNT_METRICS.selection.inc();

                Box::new(
                    super::SelectionFreeDaemon::new(exec.take_selection(), Arc::clone(&ctx), src)?
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeAggregation => {
                EXECUTOR_COUNT_METRICS.hash_aggr.inc();

                Box::new(
                    super::HashAggFreeDaemon::new(exec.take_aggregation(), Arc::clone(&ctx), src)?
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeStreamAgg => {
                EXECUTOR_COUNT_METRICS.stream_aggr.inc();

                Box::new(
                    super::StreamAggFreeDaemon::new(Arc::clone(&ctx), src, exec.take_aggregation())?
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeTopN => {
                EXECUTOR_COUNT_METRICS.top_n.inc();

                Box::new(
                    super::TopNFreeDaemon::new(exec.take_top_n(), Arc::clone(&ctx), src)?
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeLimit => {
                EXECUTOR_COUNT_METRICS.limit.inc();

                Box::new(
                    super::LimitFreeDaemon::new(exec.take_limit(), src)
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            _ => {
                return Err(other_err!(
                    "Unexpected non-first executor {:?}",
                    exec.get_tp()
                ));
            }
        };
        src = curr;
    }

    Ok(src)
}

/// Builds the inner-most executor for the normal executor pipeline, which can produce events to
/// other executors and never receive events from other executors.
///
/// The inner-most executor must be a Block scan executor or an index scan executor.
fn build_first_executor<S: causet_storage + 'static, C: ExecSummaryCollector + 'static>(
    mut first: fidel_timeshare::FreeDaemon,
    causet_storage: S,
    cones: Vec<KeyCone>,
    context: Arc<EvalConfig>,
    is_streaming: bool,
) -> Result<Box<dyn FreeDaemon<StorageStats = S::Statistics> + lightlike>> {
    let context = EvalContext::new(context);
    match first.get_tp() {
        ExecType::TypeBlockScan => {
            EXECUTOR_COUNT_METRICS.Block_scan.inc();

            let ex = Box::new(
                super::ScanFreeDaemon::Block_scan(
                    first.take_tbl_scan(),
                    context,
                    cones,
                    causet_storage,
                    is_streaming,
                )?
                .with_summary_collector(C::new(0)),
            );
            Ok(ex)
        }
        ExecType::TypeIndexScan => {
            EXECUTOR_COUNT_METRICS.index_scan.inc();

            let unique = first.get_idx_scan().get_unique();
            let ex = Box::new(
                super::ScanFreeDaemon::index_scan(
                    first.take_idx_scan(),
                    context,
                    cones,
                    causet_storage,
                    unique,
                    is_streaming,
                )?
                .with_summary_collector(C::new(0)),
            );
            Ok(ex)
        }
        _ => Err(other_err!("Unexpected first scanner: {:?}", first.get_tp())),
    }
}

impl<SS: 'static> FreeDaemonsRunner<SS> {
    pub fn from_request<S: causet_storage<Statistics = SS> + 'static>(
        mut req: PosetDagRequest,
        cones: Vec<KeyCone>,
        causet_storage: S,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
    ) -> Result<Self> {
        let executors_len = req.get_executors().len();
        let collect_exec_summary = req.get_collect_execution_summaries();
        let config = Arc::new(EvalConfig::from_request(&req)?);
        let context = EvalContext::new(config.clone());

        let executor = if !(req.get_collect_execution_summaries()) {
            build_executors::<_, ExecSummaryCollectorDisabled>(
                req.take_executors().into(),
                causet_storage,
                cones,
                config,
                is_streaming,
            )?
        } else {
            build_executors::<_, ExecSummaryCollectorEnabled>(
                req.take_executors().into(),
                causet_storage,
                cones,
                config,
                is_streaming,
            )?
        };

        let exec_stats = ExecuteStats::new(if collect_exec_summary {
            executors_len
        } else {
            0 // Avoid allocation for executor summaries when it is not needed
        });

        Ok(Self {
            deadline,
            executor,
            output_offsets: req.take_output_offsets(),
            batch_row_limit,
            collect_exec_summary,
            context,
            exec_stats,
        })
    }

    fn make_stream_response(&mut self, Soliton: Soliton) -> Result<StreamResponse> {
        self.executor.collect_exec_stats(&mut self.exec_stats);

        let mut s_resp = StreamResponse::default();
        s_resp.set_data(box_try!(Soliton.write_to_bytes()));
        if let Some(eval_warnings) = self.executor.take_eval_warnings() {
            s_resp.set_warnings(eval_warnings.warnings.into());
            s_resp.set_warning_count(eval_warnings.warning_cnt as i64);
        }
        s_resp.set_output_counts(
            self.exec_stats
                .scanned_rows_per_cone
                .iter()
                .map(|v| *v as i64)
                .collect(),
        );

        self.exec_stats.clear();

        Ok(s_resp)
    }

    pub fn handle_request(&mut self) -> Result<SelectResponse> {
        let mut record_cnt = 0;
        let mut Solitons = Vec::new();
        loop {
            match self.executor.next()? {
                Some(Evcausetidx) => {
                    self.deadline.check()?;
                    if Solitons.is_empty() || record_cnt >= self.batch_row_limit {
                        let Soliton = Soliton::default();
                        Solitons.push(Soliton);
                        record_cnt = 0;
                    }
                    let Soliton = Solitons.last_mut().unwrap();
                    record_cnt += 1;
                    // for default encode type
                    let value = Evcausetidx.get_binary(&mut self.context, &self.output_offsets)?;
                    Soliton.mut_rows_data().extlightlike_from_slice(&value);
                }
                None => {
                    self.executor.collect_exec_stats(&mut self.exec_stats);

                    let mut sel_resp = SelectResponse::default();
                    sel_resp.set_Solitons(Solitons.into());
                    if let Some(eval_warnings) = self.executor.take_eval_warnings() {
                        sel_resp.set_warnings(eval_warnings.warnings.into());
                        sel_resp.set_warning_count(eval_warnings.warning_cnt as i64);
                    }
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

                    // In case of this function is called multiple times.
                    self.exec_stats.clear();

                    return Ok(sel_resp);
                }
            }
        }
    }

    // TODO: IntervalCone should be placed inside `StreamResponse`.
    pub fn handle_streaming_request(
        &mut self,
    ) -> Result<(Option<(StreamResponse, IntervalCone)>, bool)> {
        let (mut record_cnt, mut finished) = (0, false);
        let mut Soliton = Soliton::default();
        while record_cnt < self.batch_row_limit {
            match self.executor.next()? {
                Some(Evcausetidx) => {
                    self.deadline.check()?;
                    record_cnt += 1;
                    let value = Evcausetidx.get_binary(&mut self.context, &self.output_offsets)?;
                    Soliton.mut_rows_data().extlightlike_from_slice(&value);
                }
                None => {
                    finished = true;
                    break;
                }
            }
        }
        if record_cnt > 0 {
            let cone = self.executor.take_scanned_cone();
            return self
                .make_stream_response(Soliton)
                .map(|r| (Some((r, cone)), finished));
        }
        Ok((None, true))
    }

    #[inline]
    pub fn collect_causet_storage_stats(&mut self, dest: &mut SS) {
        // TODO: A better way is to fill causet_storage stats in `handle_request`, or
        // return SelectResponse in `handle_request`.
        self.executor.collect_causet_storage_stats(dest);
    }

    #[inline]
    pub fn can_be_cached(&self) -> bool {
        self.executor.can_be_cached()
    }
}
