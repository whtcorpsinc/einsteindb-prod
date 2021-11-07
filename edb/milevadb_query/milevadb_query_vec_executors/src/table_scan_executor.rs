// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use smallvec::SmallVec;
use std::collections::HashSet;
use std::sync::Arc;

use ekvproto::interlock::KeyCone;
use milevadb_query_datatype::{EvalType, FieldTypeAccessor};
use violetabftstore::interlock::::collections::HashMap;
use fidel_timeshare::PrimaryCausetInfo;
use fidel_timeshare::FieldType;
use fidel_timeshare::BlockScan;

use super::util::scan_executor::*;
use crate::interface::*;
use milevadb_query_common::causet_storage::{IntervalCone, causet_storage};
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::batch::{LazyBatchPrimaryCauset, LazyBatchPrimaryCausetVec};
use milevadb_query_datatype::codec::Evcausetidx;
use milevadb_query_datatype::expr::{EvalConfig, EvalContext};

pub struct BatchBlockScanFreeDaemon<S: causet_storage>(ScanFreeDaemon<S, BlockScanFreeDaemonImpl>);

type HandleIndicesVec = SmallVec<[usize; 2]>;

// We assign a dummy type `Box<dyn causet_storage<Statistics = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchBlockScanFreeDaemon<Box<dyn causet_storage<Statistics = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &BlockScan) -> Result<()> {
        check_PrimaryCausets_info_supported(descriptor.get_PrimaryCausets())
    }
}

impl<S: causet_storage> BatchBlockScanFreeDaemon<S> {
    pub fn new(
        causet_storage: S,
        config: Arc<EvalConfig>,
        PrimaryCausets_info: Vec<PrimaryCausetInfo>,
        key_cones: Vec<KeyCone>,
        primary_PrimaryCauset_ids: Vec<i64>,
        is_backward: bool,
        is_scanned_cone_aware: bool,
    ) -> Result<Self> {
        let is_PrimaryCauset_filled = vec![false; PrimaryCausets_info.len()];
        let mut is_key_only = true;
        let mut handle_indices = HandleIndicesVec::new();
        let mut schemaReplicant = Vec::with_capacity(PrimaryCausets_info.len());
        let mut PrimaryCausets_default_value = Vec::with_capacity(PrimaryCausets_info.len());
        let mut PrimaryCauset_id_index = HashMap::default();

        let primary_PrimaryCauset_ids_set = primary_PrimaryCauset_ids.iter().collect::<HashSet<_>>();
        for (index, mut ci) in PrimaryCausets_info.into_iter().enumerate() {
            // For each PrimaryCauset info, we need to extract the following info:
            // - Corresponding field type (push into `schemaReplicant`).
            schemaReplicant.push(field_type_from_PrimaryCauset_info(&ci));

            // - Prepare PrimaryCauset default value (will be used to fill missing PrimaryCauset later).
            PrimaryCausets_default_value.push(ci.take_default_val());

            // - CausetStore the index of the PK handles.
            // - Check whether or not we don't need KV values (iff PK handle is given).
            if ci.get_pk_handle() {
                handle_indices.push(index);
            } else {
                if !primary_PrimaryCauset_ids_set.contains(&ci.get_PrimaryCauset_id()) {
                    is_key_only = false;
                }
                PrimaryCauset_id_index.insert(ci.get_PrimaryCauset_id(), index);
            }

            // Note: if two PK handles are given, we will only preserve the *last* one. Also if two
            // PrimaryCausets with the same PrimaryCauset id are given, we will only preserve the *last* one.
        }

        let no_common_handle = primary_PrimaryCauset_ids.is_empty();
        let imp = BlockScanFreeDaemonImpl {
            context: EvalContext::new(config),
            schemaReplicant,
            PrimaryCausets_default_value,
            PrimaryCauset_id_index,
            handle_indices,
            primary_PrimaryCauset_ids,
            is_PrimaryCauset_filled,
        };
        let wrapper = ScanFreeDaemon::new(ScanFreeDaemonOptions {
            imp,
            causet_storage,
            key_cones,
            is_backward,
            is_key_only,
            accept_point_cone: no_common_handle,
            is_scanned_cone_aware,
        })?;
        Ok(Self(wrapper))
    }
}

impl<S: causet_storage> BatchFreeDaemon for BatchBlockScanFreeDaemon<S> {
    type StorageStats = S::Statistics;

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

struct BlockScanFreeDaemonImpl {
    /// Note: Although called `EvalContext`, it is some kind of execution context instead.
    // TODO: Rename EvalContext to ExecContext.
    context: EvalContext,

    /// The schemaReplicant of the output. All of the output come from specific PrimaryCausets in the underlying
    /// causet_storage.
    schemaReplicant: Vec<FieldType>,

    /// The default value of corresponding PrimaryCausets in the schemaReplicant. When PrimaryCauset data is missing,
    /// the default value will be used to fill the output.
    PrimaryCausets_default_value: Vec<Vec<u8>>,

    /// The output position in the schemaReplicant giving the PrimaryCauset id.
    PrimaryCauset_id_index: HashMap<i64, usize>,

    /// Vec of indices in output Evcausetidx to put the handle. The indices must be sorted in the vec.
    handle_indices: HandleIndicesVec,

    /// Vec of Primary key PrimaryCauset's IDs.
    primary_PrimaryCauset_ids: Vec<i64>,

    /// A vector of flags indicating whether corresponding PrimaryCauset is filled in `next_batch`.
    /// It is a struct level field in order to prevent repeated memory allocations since its length
    /// is fixed for each `next_batch` call.
    is_PrimaryCauset_filled: Vec<bool>,
}

impl BlockScanFreeDaemonImpl {
    fn process_v1(
        &mut self,
        key: &[u8],
        value: &[u8],
        PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
        decoded_PrimaryCausets: &mut usize,
    ) -> Result<()> {
        use codec::prelude::NumberDecoder;
        use milevadb_query_datatype::codec::datum;
        // The layout of value is: [col_id_1, value_1, col_id_2, value_2, ...]
        // where each element is datum encoded.
        // The PrimaryCauset id datum must be in var i64 type.
        let PrimaryCausets_len = PrimaryCausets.PrimaryCausets_len();
        let mut remaining = value;
        while !remaining.is_empty() && *decoded_PrimaryCausets < PrimaryCausets_len {
            if remaining[0] != datum::VAR_INT_FLAG {
                return Err(other_err!(
                    "Unable to decode Evcausetidx: PrimaryCauset id must be VAR_INT"
                ));
            }
            remaining = &remaining[1..];
            let PrimaryCauset_id = box_try!(remaining.read_var_i64());
            let (val, new_remaining) = datum::split_datum(remaining, false)?;
            // Note: The produced PrimaryCausets may be not in the same length if there is error due
            // to corrupted data. It will be handled in `ScanFreeDaemon`.
            let some_index = self.PrimaryCauset_id_index.get(&PrimaryCauset_id);
            if let Some(index) = some_index {
                let index = *index;
                if !self.is_PrimaryCauset_filled[index] {
                    PrimaryCausets[index].mut_raw().push(val);
                    *decoded_PrimaryCausets += 1;
                    self.is_PrimaryCauset_filled[index] = true;
                } else {
                    // This indicates that there are duplicated elements in the Evcausetidx, which is
                    // unexpected. We won't abort the request or overwrite the previous element,
                    // but will output a log anyway.
                    warn!(
                        "Ignored duplicated Evcausetidx datum in Block scan";
                        "key" => hex::encode_upper(&key),
                        "value" => hex::encode_upper(&value),
                        "dup_PrimaryCauset_id" => PrimaryCauset_id,
                    );
                }
            }
            remaining = new_remaining;
        }
        Ok(())
    }

    fn process_v2(
        &mut self,
        value: &[u8],
        PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
        decoded_PrimaryCausets: &mut usize,
    ) -> Result<()> {
        use milevadb_query_datatype::codec::datum;
        use milevadb_query_datatype::codec::Evcausetidx::v2::{EventSlice, V1CompatibleEncoder};

        let Evcausetidx = EventSlice::from_bytes(value)?;
        for (col_id, idx) in &self.PrimaryCauset_id_index {
            if let Some((spacelike, offset)) = Evcausetidx.search_in_non_null_ids(*col_id)? {
                let mut buffer_to_write = PrimaryCausets[*idx].mut_raw().begin_concat_extlightlike();
                buffer_to_write
                    .write_v2_as_datum(&Evcausetidx.values()[spacelike..offset], &self.schemaReplicant[*idx])?;
                *decoded_PrimaryCausets += 1;
                self.is_PrimaryCauset_filled[*idx] = true;
            } else if Evcausetidx.search_in_null_ids(*col_id) {
                PrimaryCausets[*idx].mut_raw().push(datum::DATUM_DATA_NULL);
                *decoded_PrimaryCausets += 1;
                self.is_PrimaryCauset_filled[*idx] = true;
            } else {
                // This PrimaryCauset is missing. It will be filled with default values later.
            }
        }
        Ok(())
    }
}

impl ScanFreeDaemonImpl for BlockScanFreeDaemonImpl {
    #[inline]
    fn schemaReplicant(&self) -> &[FieldType] {
        &self.schemaReplicant
    }

    #[inline]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
    }

    /// Constructs empty PrimaryCausets, with PK in decoded format and the rest in raw format.
    fn build_PrimaryCauset_vec(&self, scan_rows: usize) -> LazyBatchPrimaryCausetVec {
        let PrimaryCausets_len = self.schemaReplicant.len();
        let mut PrimaryCausets = Vec::with_capacity(PrimaryCausets_len);

        // If there are any PK PrimaryCausets, for each of them, fill non-PK PrimaryCausets before it and push the
        // PK PrimaryCauset.
        // For example, consider:
        //                  non-pk non-pk non-pk pk non-pk non-pk pk pk non-pk non-pk
        // handle_indices:                       ^3               ^6 ^7
        // Each turn of the following loop will push this to `PrimaryCausets`:
        // 1st turn: [non-pk, non-pk, non-pk, pk]
        // 2nd turn: [non-pk, non-pk, pk]
        // 3rd turn: [pk]
        let mut last_index = 0usize;
        for handle_index in &self.handle_indices {
            // `handle_indices` is expected to be sorted.
            assert!(*handle_index >= last_index);

            // Fill last `handle_index - 1` PrimaryCausets.
            for _ in last_index..*handle_index {
                PrimaryCausets.push(LazyBatchPrimaryCauset::raw_with_capacity(scan_rows));
            }

            // For PK handles, we construct a decoded `VectorValue` because it is directly
            // stored as i64, without a datum flag, at the lightlike of key.
            PrimaryCausets.push(LazyBatchPrimaryCauset::decoded_with_capacity_and_tp(
                scan_rows,
                EvalType::Int,
            ));

            last_index = *handle_index + 1;
        }

        // Then fill remaining PrimaryCausets after the last handle PrimaryCauset. If there are no PK PrimaryCausets,
        // the previous loop will be skipped and this loop will be run on 0..PrimaryCausets_len.
        // For the example above, this loop will push: [non-pk, non-pk]
        for _ in last_index..PrimaryCausets_len {
            PrimaryCausets.push(LazyBatchPrimaryCauset::raw_with_capacity(scan_rows));
        }

        assert_eq!(PrimaryCausets.len(), PrimaryCausets_len);
        LazyBatchPrimaryCausetVec::from(PrimaryCausets)
    }

    fn process_kv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
    ) -> Result<()> {
        use milevadb_query_datatype::codec::{datum, Block};

        let PrimaryCausets_len = self.schemaReplicant.len();
        let mut decoded_PrimaryCausets = 0;

        if value.is_empty() || (value.len() == 1 && value[0] == datum::NIL_FLAG) {
            // Do nothing
        } else {
            match value[0] {
                Evcausetidx::v2::CODEC_VERSION => self.process_v2(value, PrimaryCausets, &mut decoded_PrimaryCausets)?,
                _ => self.process_v1(key, value, PrimaryCausets, &mut decoded_PrimaryCausets)?,
            }
        }

        if !self.handle_indices.is_empty() {
            // In this case, An int handle is expected.
            let handle = Block::decode_int_handle(key)?;

            for handle_index in &self.handle_indices {
                // TODO: We should avoid calling `push_int` repeatedly. Instead we should specialize
                // a `&mut Vec` first. However it is hard to program due to lifetime restriction.
                if !self.is_PrimaryCauset_filled[*handle_index] {
                    PrimaryCausets[*handle_index].mut_decoded().push_int(Some(handle));
                    decoded_PrimaryCausets += 1;
                    self.is_PrimaryCauset_filled[*handle_index] = true;
                }
            }
        } else if !self.primary_PrimaryCauset_ids.is_empty() {
            // Otherwise, if `primary_PrimaryCauset_ids` is not empty, we try to extract the values of the PrimaryCausets from the common handle.
            let mut handle = Block::decode_common_handle(key)?;
            for primary_id in self.primary_PrimaryCauset_ids.iter() {
                let index = self.PrimaryCauset_id_index.get(primary_id);
                let (datum, remain) = datum::split_datum(handle, false)?;
                handle = remain;

                // If the PrimaryCauset info of the coresponding primary PrimaryCauset id is missing, we ignore this slice of the datum.
                if let Some(&index) = index {
                    if !self.is_PrimaryCauset_filled[index] {
                        PrimaryCausets[index].mut_raw().push(datum);
                        decoded_PrimaryCausets += 1;
                        self.is_PrimaryCauset_filled[index] = true;
                    }
                }
            }
        } else {
            Block::check_record_key(key)?;
        }

        // Some fields may be missing in the Evcausetidx, we push corresponding default value to make all
        // PrimaryCausets in same length.
        for i in 0..PrimaryCausets_len {
            if !self.is_PrimaryCauset_filled[i] {
                // Missing fields must not be a primary key, so it must be
                // `LazyBatchPrimaryCauset::raw`.

                let default_value = if !self.PrimaryCausets_default_value[i].is_empty() {
                    // default value is provided, use the default value
                    self.PrimaryCausets_default_value[i].as_slice()
                } else if !self.schemaReplicant[i]
                    .as_accessor()
                    .flag()
                    .contains(milevadb_query_datatype::FieldTypeFlag::NOT_NULL)
                {
                    // NULL is allowed, use NULL
                    datum::DATUM_DATA_NULL
                } else {
                    return Err(other_err!(
                        "Data is corrupted, missing data for NOT NULL PrimaryCauset (offset = {})",
                        i
                    ));
                };

                PrimaryCausets[i].mut_raw().push(default_value);
            } else {
                // Reset to not-filled, prepare for next function call.
                self.is_PrimaryCauset_filled[i] = false;
            }
        }

        Ok(())
    }
}

#[causet(test)]
mod tests {
    use super::*;

    use std::iter;
    use std::sync::Arc;

    use ekvproto::interlock::KeyCone;
    use milevadb_query_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
    use fidel_timeshare::PrimaryCausetInfo;
    use fidel_timeshare::FieldType;

    use milevadb_query_common::execute_stats::*;
    use milevadb_query_common::causet_storage::test_fixture::FixtureStorage;
    use milevadb_query_common::util::convert_to_prefix_next;
    use milevadb_query_datatype::codec::batch::LazyBatchPrimaryCausetVec;
    use milevadb_query_datatype::codec::data_type::*;
    use milevadb_query_datatype::codec::{datum, Block, Datum};
    use milevadb_query_datatype::expr::EvalConfig;

    /// Test Helper for normal test with fixed schemaReplicant and data.
    /// Block SchemaReplicant:  ID (INT, PK),   Foo (INT),     Bar (FLOAT, Default 4.5)
    /// PrimaryCauset id:     1,              2,             4
    /// PrimaryCauset offset: 0,              1,             2
    /// Block Data:    1,              10,            5.2
    ///                3,              -5,            NULL
    ///                4,              NULL,          4.5 (DEFAULT)
    ///                5,              NULL,          0.1
    ///                6,              NULL,          4.5 (DEFAULT)
    struct BlockScanTestHelper {
        // ID(INT,PK), Foo(INT), Bar(Float,Default 4.5)
        pub data: Vec<(i64, Option<i64>, Option<Real>)>,
        pub Block_id: i64,
        pub PrimaryCausets_info: Vec<PrimaryCausetInfo>,
        pub field_types: Vec<FieldType>,
        pub store: FixtureStorage,
    }

    impl BlockScanTestHelper {
        /// create the BlockScanTestHelper with fixed schemaReplicant and data.
        fn new() -> BlockScanTestHelper {
            const Block_ID: i64 = 7;
            // [(row_id, PrimaryCausets)] where each PrimaryCauset: (PrimaryCauset id, datum)
            let data = vec![
                (
                    1,
                    vec![
                        // A full Evcausetidx.
                        (2, Datum::I64(10)),
                        (4, Datum::F64(5.2)),
                    ],
                ),
                (
                    3,
                    vec![
                        (4, Datum::Null),
                        // Bar PrimaryCauset is null, even if default value is provided the final result
                        // should be null.
                        (2, Datum::I64(-5)),
                        // Orders should not matter.
                    ],
                ),
                (
                    4,
                    vec![
                        (2, Datum::Null),
                        // Bar PrimaryCauset is missing, default value should be used.
                    ],
                ),
                (
                    5,
                    vec![
                        // Foo PrimaryCauset is missing, NULL should be used.
                        (4, Datum::F64(0.1)),
                    ],
                ),
                (
                    6,
                    vec![
                        // Empty Evcausetidx
                    ],
                ),
            ];

            let expect_rows = vec![
                (1, Some(10), Real::new(5.2).ok()),
                (3, Some(-5), None),
                (4, None, Real::new(4.5).ok()),
                (5, None, Real::new(0.1).ok()),
                (6, None, Real::new(4.5).ok()),
            ];

            let mut ctx = EvalContext::default();

            // The PrimaryCauset info for each PrimaryCauset in `data`.
            let PrimaryCausets_info = vec![
                {
                    let mut ci = PrimaryCausetInfo::default();
                    ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                    ci.set_pk_handle(true);
                    ci.set_PrimaryCauset_id(1);
                    ci
                },
                {
                    let mut ci = PrimaryCausetInfo::default();
                    ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                    ci.set_PrimaryCauset_id(2);
                    ci
                },
                {
                    let mut ci = PrimaryCausetInfo::default();
                    ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                    ci.set_PrimaryCauset_id(4);
                    ci.set_default_val(datum::encode_value(&mut ctx, &[Datum::F64(4.5)]).unwrap());
                    ci
                },
            ];

            let field_types = vec![
                FieldTypeTp::LongLong.into(),
                FieldTypeTp::LongLong.into(),
                FieldTypeTp::Double.into(),
            ];

            let store = {
                let kv: Vec<_> = data
                    .iter()
                    .map(|(row_id, PrimaryCausets)| {
                        let key = Block::encode_row_key(Block_ID, *row_id);
                        let value = {
                            let Evcausetidx = PrimaryCausets.iter().map(|(_, datum)| datum.clone()).collect();
                            let col_ids: Vec<_> = PrimaryCausets.iter().map(|(id, _)| *id).collect();
                            Block::encode_row(&mut ctx, Evcausetidx, &col_ids).unwrap()
                        };
                        (key, value)
                    })
                    .collect();
                FixtureStorage::from(kv)
            };

            BlockScanTestHelper {
                data: expect_rows,
                Block_id: Block_ID,
                PrimaryCausets_info,
                field_types,
                store,
            }
        }

        /// The point cone representation for each Evcausetidx in `data`.
        fn point_cones(&self) -> Vec<KeyCone> {
            self.data
                .iter()
                .map(|(row_id, _, _)| {
                    let mut r = KeyCone::default();
                    r.set_spacelike(Block::encode_row_key(self.Block_id, *row_id));
                    r.set_lightlike(r.get_spacelike().to_vec());
                    convert_to_prefix_next(r.mut_lightlike());
                    r
                })
                .collect()
        }

        /// Returns whole Block's cones which include point cone and non-point cone.
        fn mixed_cones_for_whole_Block(&self) -> Vec<KeyCone> {
            vec![
                self.Block_cone(std::i64::MIN, 3),
                {
                    let mut r = KeyCone::default();
                    r.set_spacelike(Block::encode_row_key(self.Block_id, 3));
                    r.set_lightlike(r.get_spacelike().to_vec());
                    convert_to_prefix_next(r.mut_lightlike());
                    r
                },
                self.Block_cone(4, std::i64::MAX),
            ]
        }

        fn store(&self) -> FixtureStorage {
            self.store.clone()
        }

        /// index of pk in self.PrimaryCausets_info.
        fn idx_pk(&self) -> usize {
            0
        }

        fn PrimaryCausets_info_by_idx(&self, col_index: &[usize]) -> Vec<PrimaryCausetInfo> {
            col_index
                .iter()
                .map(|id| self.PrimaryCausets_info[*id].clone())
                .collect()
        }

        /// Get PrimaryCauset's field type by the index in self.PrimaryCausets_info.
        fn get_field_type(&self, col_idx: usize) -> &FieldType {
            &self.field_types[col_idx]
        }

        /// Returns the cone for handle in [spacelike_id,lightlike_id)
        fn Block_cone(&self, spacelike_id: i64, lightlike_id: i64) -> KeyCone {
            let mut cone = KeyCone::default();
            cone.set_spacelike(Block::encode_row_key(self.Block_id, spacelike_id));
            cone.set_lightlike(Block::encode_row_key(self.Block_id, lightlike_id));
            cone
        }

        /// Returns the cone for the whole Block.
        fn whole_Block_cone(&self) -> KeyCone {
            self.Block_cone(std::i64::MIN, std::i64::MAX)
        }

        /// Returns the values spacelike from `spacelike_row` limit `events`.
        fn get_expect_values_by_cone(&self, spacelike_row: usize, events: usize) -> Vec<VectorValue> {
            let mut pks = VectorValue::with_capacity(self.data.len(), EvalType::Int);
            let mut foos = VectorValue::with_capacity(self.data.len(), EvalType::Int);
            let mut bars = VectorValue::with_capacity(self.data.len(), EvalType::Real);
            assert!(spacelike_row + events <= self.data.len());
            for id in spacelike_row..spacelike_row + events {
                let (handle, foo, bar) = self.data[id];
                pks.push_int(Some(handle));
                foos.push_int(foo);
                bars.push_real(bar);
            }
            vec![pks, foos, bars]
        }

        /// check whether the data of PrimaryCausets in `col_idxs` are as expected.
        /// col_idxs: the idx of PrimaryCauset which the `PrimaryCausets` included.
        fn expect_Block_values(
            &self,
            col_idxs: &[usize],
            spacelike_row: usize,
            expect_rows: usize,
            mut PrimaryCausets: LazyBatchPrimaryCausetVec,
        ) {
            let values = self.get_expect_values_by_cone(spacelike_row, expect_rows);
            assert_eq!(PrimaryCausets.PrimaryCausets_len(), col_idxs.len());
            assert_eq!(PrimaryCausets.rows_len(), expect_rows);
            for id in 0..col_idxs.len() {
                let col_idx = col_idxs[id];
                if col_idx == self.idx_pk() {
                    assert!(PrimaryCausets[id].is_decoded());
                } else {
                    assert!(PrimaryCausets[id].is_raw());
                    PrimaryCausets[id]
                        .ensure_all_decoded_for_test(
                            &mut EvalContext::default(),
                            self.get_field_type(col_idx),
                        )
                        .unwrap();
                }
                assert_eq!(PrimaryCausets[id].decoded(), &values[col_idx]);
            }
        }
    }

    /// test basic `Blockscan` with cones,
    /// `col_idxs`: idxs of PrimaryCausets used in scan.
    /// `batch_expect_rows`: `expect_rows` used in `next_batch`.
    fn test_basic_scan(
        helper: &BlockScanTestHelper,
        cones: Vec<KeyCone>,
        col_idxs: &[usize],
        batch_expect_rows: &[usize],
    ) {
        let PrimaryCausets_info = helper.PrimaryCausets_info_by_idx(col_idxs);
        let mut executor = BatchBlockScanFreeDaemon::new(
            helper.store(),
            Arc::new(EvalConfig::default()),
            PrimaryCausets_info,
            cones,
            vec![],
            false,
            false,
        )
        .unwrap();

        let total_rows = helper.data.len();
        let mut spacelike_row = 0;
        for expect_rows in batch_expect_rows {
            let expect_rows = *expect_rows;
            let expect_drained = spacelike_row + expect_rows > total_rows;
            let result = executor.next_batch(expect_rows);
            assert_eq!(*result.is_drained.as_ref().unwrap(), expect_drained);
            if expect_drained {
                // all remaining events are fetched
                helper.expect_Block_values(
                    col_idxs,
                    spacelike_row,
                    total_rows - spacelike_row,
                    result.physical_PrimaryCausets,
                );
                return;
            }
            // we should get expect_rows in this case.
            helper.expect_Block_values(col_idxs, spacelike_row, expect_rows, result.physical_PrimaryCausets);
            spacelike_row += expect_rows;
        }
    }

    #[test]
    fn test_basic() {
        let helper = BlockScanTestHelper::new();
        // cones to scan in each test case
        let test_cones = vec![
            helper.point_cones(),                 // point scan
            vec![helper.whole_Block_cone()],      // cone scan
            helper.mixed_cones_for_whole_Block(), // mixed cone scan and point scan
        ];
        // cols to scan in each test case.
        let test_cols = vec![
            // scan single PrimaryCauset
            vec![0],
            vec![1],
            vec![2],
            // scan multiple PrimaryCausets
            vec![0, 1],
            vec![0, 2],
            vec![1, 2],
            //PK is the last PrimaryCauset in schemaReplicant
            vec![2, 1, 0],
            //PK is the first PrimaryCauset in schemaReplicant
            vec![0, 1, 2],
            // PK is in the middle of the schemaReplicant
            vec![1, 0, 2],
        ];
        // expect_rows used in next_batch for each test case.
        let test_batch_rows = vec![
            // Fetched multiple times but totally it fetched exactly the same number of events
            // (so that it will be drained next time and at that time no Evcausetidx will be get).
            vec![1, 1, 1, 1, 1, 1],
            vec![1, 2, 2, 2],
            // Fetch a lot of events once.
            vec![10, 10],
        ];

        for cones in test_cones {
            for cols in &test_cols {
                for batch_expect_rows in &test_batch_rows {
                    test_basic_scan(&helper, cones.clone(), cols, batch_expect_rows);
                }
            }
        }
    }

    #[test]
    fn test_execution_summary() {
        let helper = BlockScanTestHelper::new();

        let mut executor = BatchBlockScanFreeDaemon::new(
            helper.store(),
            Arc::new(EvalConfig::default()),
            helper.PrimaryCausets_info_by_idx(&[0]),
            vec![helper.whole_Block_cone()],
            vec![],
            false,
            false,
        )
        .unwrap()
        .collect_summary(1);

        executor.next_batch(1);
        executor.next_batch(2);

        let mut s = ExecuteStats::new(2);
        executor.collect_exec_stats(&mut s);

        assert_eq!(s.scanned_rows_per_cone.len(), 1);
        assert_eq!(s.scanned_rows_per_cone[0], 3);
        // 0 remains Default because our output index is 1
        assert_eq!(s.summary_per_executor[0], ExecSummary::default());
        let exec_summary = s.summary_per_executor[1];
        assert_eq!(3, exec_summary.num_produced_rows);
        assert_eq!(2, exec_summary.num_iterations);

        executor.collect_exec_stats(&mut s);

        // Collected statistics remain unchanged because of no newly generated delta statistics.
        assert_eq!(s.scanned_rows_per_cone.len(), 2);
        assert_eq!(s.scanned_rows_per_cone[0], 3);
        assert_eq!(s.scanned_rows_per_cone[1], 0);
        assert_eq!(s.summary_per_executor[0], ExecSummary::default());
        let exec_summary = s.summary_per_executor[1];
        assert_eq!(3, exec_summary.num_produced_rows);
        assert_eq!(2, exec_summary.num_iterations);

        // Reset collected statistics so that now we will only collect statistics in this round.
        s.clear();
        executor.next_batch(10);
        executor.collect_exec_stats(&mut s);

        assert_eq!(s.scanned_rows_per_cone.len(), 1);
        assert_eq!(s.scanned_rows_per_cone[0], 2);
        assert_eq!(s.summary_per_executor[0], ExecSummary::default());
        let exec_summary = s.summary_per_executor[1];
        assert_eq!(2, exec_summary.num_produced_rows);
        assert_eq!(1, exec_summary.num_iterations);
    }

    #[test]
    fn test_corrupted_data() {
        const Block_ID: i64 = 5;

        let PrimaryCausets_info = vec![
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci.set_PrimaryCauset_id(1);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_PrimaryCauset_id(2);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_PrimaryCauset_id(3);
                ci
            },
        ];
        let schemaReplicant = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
        ];

        let mut ctx = EvalContext::default();
        let mut kv = vec![];
        {
            // Evcausetidx 0, which is not corrupted
            let key = Block::encode_row_key(Block_ID, 0);
            let value =
                Block::encode_row(&mut ctx, vec![Datum::I64(5), Datum::I64(7)], &[2, 3]).unwrap();
            kv.push((key, value));
        }
        {
            // Evcausetidx 1, which is not corrupted
            let key = Block::encode_row_key(Block_ID, 1);
            let value = vec![];
            kv.push((key, value));
        }
        {
            // Evcausetidx 2, which is partially corrupted
            let key = Block::encode_row_key(Block_ID, 2);
            let mut value =
                Block::encode_row(&mut ctx, vec![Datum::I64(5), Datum::I64(7)], &[2, 3]).unwrap();
            // resize the value to make it partially corrupted
            value.truncate(value.len() - 3);
            kv.push((key, value));
        }
        {
            // Evcausetidx 3, which is totally corrupted due to invalid datum flag for PrimaryCauset id
            let key = Block::encode_row_key(Block_ID, 3);
            // this datum flag does not exist
            let value = vec![255];
            kv.push((key, value));
        }
        {
            // Evcausetidx 4, which is totally corrupted due to missing datum for PrimaryCauset value
            let key = Block::encode_row_key(Block_ID, 4);
            let value = datum::encode_value(&mut ctx, &[Datum::I64(2)]).unwrap(); // col_id = 2
            kv.push((key, value));
        }

        let key_cone_point: Vec<_> = kv
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let mut r = KeyCone::default();
                r.set_spacelike(Block::encode_row_key(Block_ID, index as i64));
                r.set_lightlike(r.get_spacelike().to_vec());
                convert_to_prefix_next(r.mut_lightlike());
                r
            })
            .collect();

        let store = FixtureStorage::from(kv);

        // For Evcausetidx 0 + Evcausetidx 1 + (Evcausetidx 2 ~ Evcausetidx 4), we should only get Evcausetidx 0, Evcausetidx 1 and an error.
        for corrupted_row_index in 2..=4 {
            let mut executor = BatchBlockScanFreeDaemon::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                PrimaryCausets_info.clone(),
                vec![
                    key_cone_point[0].clone(),
                    key_cone_point[1].clone(),
                    key_cone_point[corrupted_row_index].clone(),
                ],
                vec![],
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.is_err());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 3);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 2);
            assert!(result.physical_PrimaryCausets[0].is_decoded());
            assert_eq!(
                result.physical_PrimaryCausets[0].decoded().to_int_vec(),
                &[Some(0), Some(1)]
            );
            assert!(result.physical_PrimaryCausets[1].is_raw());
            result.physical_PrimaryCausets[1]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[1])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[1].decoded().to_int_vec(),
                &[Some(5), None]
            );
            assert!(result.physical_PrimaryCausets[2].is_raw());
            result.physical_PrimaryCausets[2]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[2])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[2].decoded().to_int_vec(),
                &[Some(7), None]
            );
        }
    }

    #[test]
    fn test_locked_data() {
        const Block_ID: i64 = 42;

        let PrimaryCausets_info = vec![
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci.set_PrimaryCauset_id(1);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_PrimaryCauset_id(2);
                ci
            },
        ];
        let schemaReplicant = vec![FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()];

        let mut ctx = EvalContext::default();
        let mut kv = vec![];
        {
            // Evcausetidx 0: ok
            let key = Block::encode_row_key(Block_ID, 0);
            let value = Block::encode_row(&mut ctx, vec![Datum::I64(7)], &[2]).unwrap();
            kv.push((key, Ok(value)));
        }
        {
            // Evcausetidx 1: causet_storage error
            let key = Block::encode_row_key(Block_ID, 1);
            let value: std::result::Result<
                _,
                Box<dyn lightlike + Sync + Fn() -> milevadb_query_common::error::StorageError>,
            > = Err(Box::new(|| failure::format_err!("locked").into()));
            kv.push((key, value));
        }
        {
            // Evcausetidx 2: not locked
            let key = Block::encode_row_key(Block_ID, 2);
            let value = Block::encode_row(&mut ctx, vec![Datum::I64(5)], &[2]).unwrap();
            kv.push((key, Ok(value)));
        }

        let key_cone_point: Vec<_> = kv
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let mut r = KeyCone::default();
                r.set_spacelike(Block::encode_row_key(Block_ID, index as i64));
                r.set_lightlike(r.get_spacelike().to_vec());
                convert_to_prefix_next(r.mut_lightlike());
                r
            })
            .collect();

        let store = FixtureStorage::new(kv.into_iter().collect());

        // Case 1: Evcausetidx 0 + Evcausetidx 1 + Evcausetidx 2
        // We should get Evcausetidx 0 and error because no further events should be scanned when there is
        // an error.
        {
            let mut executor = BatchBlockScanFreeDaemon::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                PrimaryCausets_info.clone(),
                vec![
                    key_cone_point[0].clone(),
                    key_cone_point[1].clone(),
                    key_cone_point[2].clone(),
                ],
                vec![],
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.is_err());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 2);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 1);
            assert!(result.physical_PrimaryCausets[0].is_decoded());
            assert_eq!(
                result.physical_PrimaryCausets[0].decoded().to_int_vec(),
                &[Some(0)]
            );
            assert!(result.physical_PrimaryCausets[1].is_raw());
            result.physical_PrimaryCausets[1]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[1])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[1].decoded().to_int_vec(),
                &[Some(7)]
            );
        }

        // Let's also repeat case 1 for smaller batch size
        {
            let mut executor = BatchBlockScanFreeDaemon::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                PrimaryCausets_info.clone(),
                vec![
                    key_cone_point[0].clone(),
                    key_cone_point[1].clone(),
                    key_cone_point[2].clone(),
                ],
                vec![],
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(1);
            assert!(!result.is_drained.is_err());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 2);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 1);
            assert!(result.physical_PrimaryCausets[0].is_decoded());
            assert_eq!(
                result.physical_PrimaryCausets[0].decoded().to_int_vec(),
                &[Some(0)]
            );
            assert!(result.physical_PrimaryCausets[1].is_raw());
            result.physical_PrimaryCausets[1]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[1])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[1].decoded().to_int_vec(),
                &[Some(7)]
            );

            let result = executor.next_batch(1);
            assert!(result.is_drained.is_err());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 2);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 0);
        }

        // Case 2: Evcausetidx 1 + Evcausetidx 2
        // We should get error and no Evcausetidx, for the same reason as above.
        {
            let mut executor = BatchBlockScanFreeDaemon::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                PrimaryCausets_info.clone(),
                vec![key_cone_point[1].clone(), key_cone_point[2].clone()],
                vec![],
                false,
                false,
            )
            .unwrap();

            let result = executor.next_batch(10);
            assert!(result.is_drained.is_err());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 2);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 0);
        }

        // Case 3: Evcausetidx 2 + Evcausetidx 0
        // We should get Evcausetidx 2 and Evcausetidx 0. There is no error.
        {
            let mut executor = BatchBlockScanFreeDaemon::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                PrimaryCausets_info.clone(),
                vec![key_cone_point[2].clone(), key_cone_point[0].clone()],
                vec![],
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(!result.is_drained.is_err());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 2);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 2);
            assert!(result.physical_PrimaryCausets[0].is_decoded());
            assert_eq!(
                result.physical_PrimaryCausets[0].decoded().to_int_vec(),
                &[Some(2), Some(0)]
            );
            assert!(result.physical_PrimaryCausets[1].is_raw());
            result.physical_PrimaryCausets[1]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[1])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[1].decoded().to_int_vec(),
                &[Some(5), Some(7)]
            );
        }

        // Case 4: Evcausetidx 1
        // We should get error.
        {
            let mut executor = BatchBlockScanFreeDaemon::new(
                store,
                Arc::new(EvalConfig::default()),
                PrimaryCausets_info,
                vec![key_cone_point[1].clone()],
                vec![],
                false,
                false,
            )
            .unwrap();

            let result = executor.next_batch(10);
            assert!(result.is_drained.is_err());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 2);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 0);
        }
    }

    fn test_multi_handle_PrimaryCauset_impl(PrimaryCausets_is_pk: &[bool]) {
        const Block_ID: i64 = 42;

        // This test makes a pk PrimaryCauset with id = 1 and non-pk PrimaryCausets with id
        // in 10 to 10 + PrimaryCausets_is_pk.len().
        // PK PrimaryCausets will be set to PrimaryCauset 1 and others will be set to PrimaryCauset 10 + i, where i is
        // the index of each PrimaryCauset.

        let mut PrimaryCausets_info = Vec::new();
        for (i, is_pk) in PrimaryCausets_is_pk.iter().enumerate() {
            let mut ci = PrimaryCausetInfo::default();
            ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
            ci.set_pk_handle(*is_pk);
            ci.set_PrimaryCauset_id(if *is_pk { 1 } else { i as i64 + 10 });
            PrimaryCausets_info.push(ci);
        }

        let mut schemaReplicant = Vec::new();
        schemaReplicant.resize(PrimaryCausets_is_pk.len(), FieldTypeTp::LongLong.into());

        let key = Block::encode_row_key(Block_ID, 1);
        let col_ids = (10..10 + schemaReplicant.len() as i64).collect::<Vec<_>>();
        let Evcausetidx = col_ids.iter().map(|i| Datum::I64(*i)).collect();
        let value = Block::encode_row(&mut EvalContext::default(), Evcausetidx, &col_ids).unwrap();

        let mut key_cone = KeyCone::default();
        key_cone.set_spacelike(Block::encode_row_key(Block_ID, std::i64::MIN));
        key_cone.set_lightlike(Block::encode_row_key(Block_ID, std::i64::MAX));

        let store = FixtureStorage::new(iter::once((key, (Ok(value)))).collect());

        let mut executor = BatchBlockScanFreeDaemon::new(
            store,
            Arc::new(EvalConfig::default()),
            PrimaryCausets_info,
            vec![key_cone],
            vec![],
            false,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert_eq!(result.is_drained.unwrap(), true);
        assert_eq!(result.logical_rows.len(), 1);
        assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), PrimaryCausets_is_pk.len());
        for i in 0..PrimaryCausets_is_pk.len() {
            result.physical_PrimaryCausets[i]
                .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[i])
                .unwrap();
            if PrimaryCausets_is_pk[i] {
                assert_eq!(
                    result.physical_PrimaryCausets[i].decoded().to_int_vec(),
                    &[Some(1)]
                );
            } else {
                assert_eq!(
                    result.physical_PrimaryCausets[i].decoded().to_int_vec(),
                    &[Some(i as i64 + 10)]
                );
            }
        }
    }

    #[test]
    fn test_multi_handle_PrimaryCauset() {
        test_multi_handle_PrimaryCauset_impl(&[true]);
        test_multi_handle_PrimaryCauset_impl(&[false]);
        test_multi_handle_PrimaryCauset_impl(&[true, false]);
        test_multi_handle_PrimaryCauset_impl(&[false, true]);
        test_multi_handle_PrimaryCauset_impl(&[true, true]);
        test_multi_handle_PrimaryCauset_impl(&[true, false, true]);
        test_multi_handle_PrimaryCauset_impl(&[
            false, false, false, true, false, false, true, true, false, false,
        ]);
    }

    #[derive(Copy, Clone)]
    struct PrimaryCauset {
        is_primary_PrimaryCauset: bool,
        has_PrimaryCauset_info: bool,
    }

    fn test_common_handle_impl(PrimaryCausets: &[PrimaryCauset]) {
        const Block_ID: i64 = 2333;

        // Prepare some Block meta data
        let mut PrimaryCausets_info = vec![];
        let mut schemaReplicant = vec![];
        let mut handle = vec![];
        let mut primary_PrimaryCauset_ids = vec![];
        let mut missed_PrimaryCausets_info = vec![];
        let PrimaryCauset_ids = (0..PrimaryCausets.len() as i64).collect::<Vec<_>>();
        let mut Evcausetidx = vec![];

        for (i, &PrimaryCauset) in PrimaryCausets.iter().enumerate() {
            let PrimaryCauset {
                is_primary_PrimaryCauset,
                has_PrimaryCauset_info,
            } = PrimaryCauset;

            if has_PrimaryCauset_info {
                let mut ci = PrimaryCausetInfo::default();

                ci.set_PrimaryCauset_id(i as i64);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);

                PrimaryCausets_info.push(ci);
                schemaReplicant.push(FieldTypeTp::LongLong.into());
            } else {
                missed_PrimaryCausets_info.push(i as i64);
            }

            if is_primary_PrimaryCauset {
                handle.push(Datum::I64(i as i64));
                primary_PrimaryCauset_ids.push(i as i64);
            }

            Evcausetidx.push(Datum::I64(i as i64));
        }

        let handle = datum::encode_key(&mut EvalContext::default(), &handle).unwrap();

        let key = Block::encode_common_handle_for_test(Block_ID, &handle);
        let value = Block::encode_row(&mut EvalContext::default(), Evcausetidx, &PrimaryCauset_ids).unwrap();

        // Constructs a cone that includes the constructed key.
        let mut key_cone = KeyCone::default();
        let begin = Block::encode_common_handle_for_test(Block_ID - 1, &handle);
        let lightlike = Block::encode_common_handle_for_test(Block_ID + 1, &handle);
        key_cone.set_spacelike(begin);
        key_cone.set_lightlike(lightlike);

        let store = FixtureStorage::new(iter::once((key, (Ok(value)))).collect());

        let mut executor = BatchBlockScanFreeDaemon::new(
            store,
            Arc::new(EvalConfig::default()),
            PrimaryCausets_info.clone(),
            vec![key_cone],
            primary_PrimaryCauset_ids,
            false,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert_eq!(result.is_drained.unwrap(), true);
        assert_eq!(result.logical_rows.len(), 1);
        assert_eq!(
            result.physical_PrimaryCausets.PrimaryCausets_len(),
            PrimaryCausets.len() - missed_PrimaryCausets_info.len()
        );
        // We expect we fill the primary PrimaryCauset with the value allegro in the common handle.
        for i in 0..result.physical_PrimaryCausets.PrimaryCausets_len() {
            result.physical_PrimaryCausets[i]
                .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[i])
                .unwrap();

            assert_eq!(
                result.physical_PrimaryCausets[i].decoded().to_int_vec(),
                &[Some(PrimaryCausets_info[i].get_PrimaryCauset_id())]
            );
        }
    }
    #[test]
    fn test_common_handle() {
        test_common_handle_impl(&[PrimaryCauset {
            is_primary_PrimaryCauset: true,
            has_PrimaryCauset_info: true,
        }]);

        test_common_handle_impl(&[
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: false,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: true,
            },
        ]);

        test_common_handle_impl(&[
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: false,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: false,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: true,
            },
        ]);

        test_common_handle_impl(&[
            PrimaryCauset {
                is_primary_PrimaryCauset: false,
                has_PrimaryCauset_info: false,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: true,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: false,
            },
        ]);

        test_common_handle_impl(&[
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: false,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: false,
                has_PrimaryCauset_info: true,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: false,
            },
        ]);

        test_common_handle_impl(&[
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: false,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: true,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: false,
                has_PrimaryCauset_info: true,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: false,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: true,
            },
            PrimaryCauset {
                is_primary_PrimaryCauset: true,
                has_PrimaryCauset_info: false,
            },
        ]);
    }
}
