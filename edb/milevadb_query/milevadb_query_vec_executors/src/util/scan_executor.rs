// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use ekvproto::interlock::KeyCone;
use fidel_timeshare::PrimaryCausetInfo;
use fidel_timeshare::FieldType;

use crate::interface::*;
use milevadb_query_common::causet_storage::scanner::{ConesScanner, ConesScannerOptions};
use milevadb_query_common::causet_storage::{IntervalCone, Cone, causet_storage};
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::batch::LazyBatchPrimaryCausetVec;
use milevadb_query_datatype::expr::EvalContext;

/// Common interfaces for Block scan and index scan implementations.
pub trait ScanFreeDaemonImpl: lightlike {
    /// Gets the schemaReplicant.
    fn schemaReplicant(&self) -> &[FieldType];

    /// Gets a muBlock reference of the executor context.
    fn mut_context(&mut self) -> &mut EvalContext;

    fn build_PrimaryCauset_vec(&self, scan_rows: usize) -> LazyBatchPrimaryCausetVec;

    /// Accepts a key value pair and fills the PrimaryCauset vector.
    ///
    /// The PrimaryCauset vector does not need to be regular when there are errors during this process.
    /// However if there is no error, the PrimaryCauset vector must be regular.
    fn process_kv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
    ) -> Result<()>;
}

/// A shared executor implementation for both Block scan and index scan. Implementation differences
/// between Block scan and index scan are further given via `ScanFreeDaemonImpl`.
pub struct ScanFreeDaemon<S: causet_storage, I: ScanFreeDaemonImpl> {
    /// The internal scanning implementation.
    imp: I,

    /// The scanner that scans over cones.
    scanner: ConesScanner<S>,

    /// A flag indicating whether this executor is lightlikeed. When Block is drained or there was an
    /// error scanning the Block, this flag will be set to `true` and `next_batch` should be never
    /// called again.
    is_lightlikeed: bool,
}

pub struct ScanFreeDaemonOptions<S, I> {
    pub imp: I,
    pub causet_storage: S,
    pub key_cones: Vec<KeyCone>,
    pub is_backward: bool,
    pub is_key_only: bool,
    pub accept_point_cone: bool,
    pub is_scanned_cone_aware: bool,
}

impl<S: causet_storage, I: ScanFreeDaemonImpl> ScanFreeDaemon<S, I> {
    pub fn new(
        ScanFreeDaemonOptions {
            imp,
            causet_storage,
            mut key_cones,
            is_backward,
            is_key_only,
            accept_point_cone,
            is_scanned_cone_aware,
        }: ScanFreeDaemonOptions<S, I>,
    ) -> Result<Self> {
        milevadb_query_datatype::codec::Block::check_Block_cones(&key_cones)?;
        if is_backward {
            key_cones.reverse();
        }
        Ok(Self {
            imp,
            scanner: ConesScanner::new(ConesScannerOptions {
                causet_storage,
                cones: key_cones
                    .into_iter()
                    .map(|r| Cone::from__timeshare_cone(r, accept_point_cone))
                    .collect(),
                scan_backward_in_cone: is_backward,
                is_key_only,
                is_scanned_cone_aware,
            }),
            is_lightlikeed: false,
        })
    }

    /// Fills a PrimaryCauset vector and returns whether or not all cones are drained.
    ///
    /// The PrimaryCausets are ensured to be regular even if there are errors during the process.
    fn fill_PrimaryCauset_vec(
        &mut self,
        scan_rows: usize,
        PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
    ) -> Result<bool> {
        assert!(scan_rows > 0);

        for _ in 0..scan_rows {
            let some_row = self.scanner.next()?;
            if let Some((key, value)) = some_row {
                // Retrieved one Evcausetidx from point cone or non-point cone.

                if let Err(e) = self.imp.process_kv_pair(&key, &value, PrimaryCausets) {
                    // When there are errors in `process_kv_pair`, PrimaryCausets' length may not be
                    // identical. For example, the filling process may be partially done so that
                    // first several PrimaryCausets have N events while the rest have N-1 events. Since we do
                    // not immediately fail when there are errors, these irregular PrimaryCausets may
                    // further cause future executors to panic. So let's truncate these PrimaryCausets to
                    // make they all have N-1 events in that case.
                    PrimaryCausets.truncate_into_equal_length();
                    return Err(e);
                }
            } else {
                // Drained
                return Ok(true);
            }
        }

        // Not drained
        Ok(false)
    }
}

/// Extracts `FieldType` from `PrimaryCausetInfo`.
// TODO: Embed FieldType in PrimaryCausetInfo directly in Cop DAG v2 to remove this function.
pub fn field_type_from_PrimaryCauset_info(ci: &PrimaryCausetInfo) -> FieldType {
    let mut field_type = FieldType::default();
    field_type.set_tp(ci.get_tp());
    field_type.set_flag(ci.get_flag() as u32); // FIXME: This `as u32` is really awful.
    field_type.set_flen(ci.get_PrimaryCauset_len());
    field_type.set_decimal(ci.get_decimal());
    field_type.set_collate(ci.get_collation());
    // Note: Charset is not provided in PrimaryCauset info.
    field_type
}

/// Checks whether the given PrimaryCausets info are supported.
pub fn check_PrimaryCausets_info_supported(PrimaryCausets_info: &[PrimaryCausetInfo]) -> Result<()> {
    use std::convert::TryFrom;
    use milevadb_query_datatype::EvalType;
    use milevadb_query_datatype::FieldTypeAccessor;

    for PrimaryCauset in PrimaryCausets_info {
        if PrimaryCauset.get_pk_handle() {
            box_try!(EvalType::try_from(PrimaryCauset.as_accessor().tp()));
        }
    }
    Ok(())
}

impl<S: causet_storage, I: ScanFreeDaemonImpl> BatchFreeDaemon for ScanFreeDaemon<S, I> {
    type StorageStats = S::Statistics;

    #[inline]
    fn schemaReplicant(&self) -> &[FieldType] {
        self.imp.schemaReplicant()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_lightlikeed);
        assert!(scan_rows > 0);

        let mut logical_PrimaryCausets = self.imp.build_PrimaryCauset_vec(scan_rows);
        let is_drained = self.fill_PrimaryCauset_vec(scan_rows, &mut logical_PrimaryCausets);

        logical_PrimaryCausets.assert_PrimaryCausets_equal_length();
        let logical_rows = (0..logical_PrimaryCausets.rows_len()).collect();

        // TODO
        // If `is_drained.is_err()`, it means that there is an error after *successfully* retrieving
        // these events. After that, if we only consumes some of the events (TopN / Limit), we should
        // ignore this error.

        match &is_drained {
            // Note: `self.is_lightlikeed` is only used for assertion purpose.
            Err(_) | Ok(true) => self.is_lightlikeed = true,
            Ok(false) => {}
        };

        BatchExecuteResult {
            physical_PrimaryCausets: logical_PrimaryCausets,
            logical_rows,
            is_drained,
            warnings: self.imp.mut_context().take_warnings(),
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.scanner
            .collect_scanned_rows_per_cone(&mut dest.scanned_rows_per_cone);
    }

    #[inline]
    fn collect_causet_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.scanner.collect_causet_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_cone(&mut self) -> IntervalCone {
        // TODO: check if there is a better way to reuse this method impl.
        self.scanner.take_scanned_cone()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.scanner.can_be_cached()
    }
}
