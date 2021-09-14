// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use derive_more::{Add, AddAssign};

/// Execution summaries to support `EXPLAIN ANALYZE` statements. We don't use
/// `FreeDaemonExecutionSummary` directly since it is less efficient.
#[derive(Debug, Default, Copy, Clone, Add, AddAssign, PartialEq, Eq)]
pub struct ExecSummary {
    /// Total time cost in this executor.
    pub time_processed_ns: usize,

    /// How many events this executor produced totally.
    pub num_produced_rows: usize,

    /// How many times executor's `next_batch()` is called.
    pub num_iterations: usize,
}

/// A trait for all execution summary collectors.
pub trait ExecSummaryCollector: lightlike {
    type DurationRecorder;

    /// Creates a new instance with specified output slot index.
    fn new(output_index: usize) -> Self
    where
        Self: Sized;

    /// Returns an instance that will record elapsed duration and increase
    /// the iterations counter. The instance should be later passed back to
    /// `on_finish_iterate` when processing of `next_batch` is completed.
    fn on_spacelike_iterate(&mut self) -> Self::DurationRecorder;

    // Increases the process time and produced events counter.
    // It should be called when `next_batch` is completed.
    fn on_finish_iterate(&mut self, dr: Self::DurationRecorder, events: usize);

    /// Takes and applightlikes current execution summary into `target`.
    fn collect(&mut self, target: &mut [ExecSummary]);
}

/// A normal `ExecSummaryCollector` that simply collects execution summaries.
/// It acts like `collect = true`.
pub struct ExecSummaryCollectorEnabled {
    output_index: usize,
    counts: ExecSummary,
}

impl ExecSummaryCollector for ExecSummaryCollectorEnabled {
    type DurationRecorder = violetabftstore::interlock::::time::Instant;

    #[inline]
    fn new(output_index: usize) -> ExecSummaryCollectorEnabled {
        ExecSummaryCollectorEnabled {
            output_index,
            counts: Default::default(),
        }
    }

    #[inline]
    fn on_spacelike_iterate(&mut self) -> Self::DurationRecorder {
        self.counts.num_iterations += 1;
        violetabftstore::interlock::::time::Instant::now_coarse()
    }

    #[inline]
    fn on_finish_iterate(&mut self, dr: Self::DurationRecorder, events: usize) {
        self.counts.num_produced_rows += events;
        let elapsed_time = violetabftstore::interlock::::time::duration_to_nanos(dr.elapsed()) as usize;
        self.counts.time_processed_ns += elapsed_time;
    }

    #[inline]
    fn collect(&mut self, target: &mut [ExecSummary]) {
        let current_summary = std::mem::take(&mut self.counts);
        target[self.output_index] += current_summary;
    }
}

/// A `ExecSummaryCollector` that does not collect anything. Acts like `collect = false`.
pub struct ExecSummaryCollectorDisabled;

impl ExecSummaryCollector for ExecSummaryCollectorDisabled {
    type DurationRecorder = ();

    #[inline]
    fn new(_output_index: usize) -> ExecSummaryCollectorDisabled {
        ExecSummaryCollectorDisabled
    }

    #[inline]
    fn on_spacelike_iterate(&mut self) -> Self::DurationRecorder {}

    #[inline]
    fn on_finish_iterate(&mut self, _dr: Self::DurationRecorder, _rows: usize) {}

    #[inline]
    fn collect(&mut self, _target: &mut [ExecSummary]) {}
}

/// Combines an `ExecSummaryCollector` with another type. This inner type `T`
/// typically `FreeDaemon`/`BatchFreeDaemon`, such that `WithSummaryCollector<C, T>`
/// would implement the same trait and collects the statistics into `C`.
pub struct WithSummaryCollector<C: ExecSummaryCollector, T> {
    pub summary_collector: C,
    pub inner: T,
}

/// Execution statistics to be flowed between parent and child executors at once during
/// `collect_exec_stats()` invocation.
pub struct ExecuteStats {
    /// The execution summary of each executor. If execution summary is not needed, it will
    /// be zero sized.
    pub summary_per_executor: Vec<ExecSummary>,

    /// For each cone given in the request, how many events are scanned.
    pub scanned_rows_per_cone: Vec<usize>,
}

impl ExecuteStats {
    /// Creates a new statistics instance.
    ///
    /// If execution summary does not need to be collected, it is safe to pass 0 to the `executors`
    /// argument, which will avoid one allocation.
    pub fn new(executors_len: usize) -> Self {
        Self {
            summary_per_executor: vec![ExecSummary::default(); executors_len],
            scanned_rows_per_cone: Vec::new(),
        }
    }

    /// Clears the statistics instance.
    pub fn clear(&mut self) {
        for item in self.summary_per_executor.iter_mut() {
            *item = ExecSummary::default();
        }
        self.scanned_rows_per_cone.clear();
    }
}
