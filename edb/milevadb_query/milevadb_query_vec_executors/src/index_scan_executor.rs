// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use ekvproto::interlock::KeyCone;
use milevadb_query_datatype::EvalType;
use fidel_timeshare::PrimaryCausetInfo;
use fidel_timeshare::FieldType;
use fidel_timeshare::IndexScan;

use super::util::scan_executor::*;
use crate::interface::*;
use codec::prelude::NumberDecoder;
use milevadb_query_common::causet_storage::{IntervalCone, causet_storage};
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::batch::{LazyBatchPrimaryCauset, LazyBatchPrimaryCausetVec};
use milevadb_query_datatype::codec::Block::{check_index_key, MAX_OLD_ENCODED_VALUE_LEN};
use milevadb_query_datatype::codec::{datum, Block};
use milevadb_query_datatype::expr::{EvalConfig, EvalContext};

use DecodeHandleStrategy::*;

pub struct BatchIndexScanFreeDaemon<S: causet_storage>(ScanFreeDaemon<S, IndexScanFreeDaemonImpl>);

// We assign a dummy type `Box<dyn causet_storage<Statistics = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchIndexScanFreeDaemon<Box<dyn causet_storage<Statistics = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &IndexScan) -> Result<()> {
        check_PrimaryCausets_info_supported(descriptor.get_PrimaryCausets())
    }
}

impl<S: causet_storage> BatchIndexScanFreeDaemon<S> {
    pub fn new(
        causet_storage: S,
        config: Arc<EvalConfig>,
        PrimaryCausets_info: Vec<PrimaryCausetInfo>,
        key_cones: Vec<KeyCone>,
        primary_PrimaryCauset_ids_len: usize,
        is_backward: bool,
        unique: bool,
        is_scanned_cone_aware: bool,
    ) -> Result<Self> {
        // Note 1: `unique = true` doesn't completely mean that it is a unique index scan. Instead
        // it just means that we can use point-get for this index. In the following scenarios
        // `unique` will be `false`:
        // - scan from a non-unique index
        // - scan from a unique index with like: where unique-index like xxx
        //
        // Note 2: Unlike Block scan executor, the accepted `PrimaryCausets_info` of index scan executor is
        // strictly stipulated. The order of PrimaryCausets in the schemaReplicant must be the same as index data
        // stored and if PK handle is needed it must be placed as the last one.
        //
        // Note 3: Currently MilevaDB may lightlike multiple PK handles to EinsteinDB (but only the last one is
        // real). We accept this kind of request for compatibility considerations, but will be
        // forbidden soon.

        let is_int_handle = PrimaryCausets_info.last().map_or(false, |ci| ci.get_pk_handle());
        let is_common_handle = primary_PrimaryCauset_ids_len > 0;
        let (decode_handle_strategy, handle_PrimaryCauset_cnt) = match (is_int_handle, is_common_handle) {
            (false, false) => (NoDecode, 0),
            (false, true) => (DecodeCommonHandle, primary_PrimaryCauset_ids_len),
            (true, false) => (DecodeIntHandle, 1),
            // MilevaDB may accidentally push down both int handle or common handle.
            // However, we still try to decode int handle.
            _ => {
                return Err(other_err!(
                    "Both int handle and common handle are push downed"
                ));
            }
        };

        if handle_PrimaryCauset_cnt > PrimaryCausets_info.len() {
            return Err(other_err!(
                "The number of handle PrimaryCausets exceeds the length of `PrimaryCausets_info`"
            ));
        }

        let schemaReplicant: Vec<_> = PrimaryCausets_info
            .iter()
            .map(|ci| field_type_from_PrimaryCauset_info(&ci))
            .collect();

        let PrimaryCausets_id_without_handle: Vec<_> = PrimaryCausets_info
            [..PrimaryCausets_info.len() - handle_PrimaryCauset_cnt]
            .iter()
            .map(|ci| ci.get_PrimaryCauset_id())
            .collect();

        let imp = IndexScanFreeDaemonImpl {
            context: EvalContext::new(config),
            schemaReplicant,
            PrimaryCausets_id_without_handle,
            decode_handle_strategy,
        };
        let wrapper = ScanFreeDaemon::new(ScanFreeDaemonOptions {
            imp,
            causet_storage,
            key_cones,
            is_backward,
            is_key_only: false,
            accept_point_cone: unique,
            is_scanned_cone_aware,
        })?;
        Ok(Self(wrapper))
    }
}

impl<S: causet_storage> BatchFreeDaemon for BatchIndexScanFreeDaemon<S> {
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

#[derive(PartialEq, Debug)]
enum DecodeHandleStrategy {
    NoDecode,
    DecodeIntHandle,
    DecodeCommonHandle,
}

struct IndexScanFreeDaemonImpl {
    /// See `BlockScanFreeDaemonImpl`'s `context`.
    context: EvalContext,

    /// See `BlockScanFreeDaemonImpl`'s `schemaReplicant`.
    schemaReplicant: Vec<FieldType>,

    /// ID of interested PrimaryCausets (exclude PK handle PrimaryCauset).
    PrimaryCausets_id_without_handle: Vec<i64>,

    /// The strategy to decode handles.
    /// Handle will be always placed in the last PrimaryCauset.
    decode_handle_strategy: DecodeHandleStrategy,
}

impl ScanFreeDaemonImpl for IndexScanFreeDaemonImpl {
    #[inline]
    fn schemaReplicant(&self) -> &[FieldType] {
        &self.schemaReplicant
    }

    #[inline]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
    }

    /// Constructs empty PrimaryCausets, with PK containing int handle in decoded format and the rest in raw format.
    ///
    /// Note: the structure of the constructed PrimaryCauset is the same as Block scan executor but due
    /// to different reasons.
    fn build_PrimaryCauset_vec(&self, scan_rows: usize) -> LazyBatchPrimaryCausetVec {
        let PrimaryCausets_len = self.schemaReplicant.len();
        let mut PrimaryCausets = Vec::with_capacity(PrimaryCausets_len);

        for _ in 0..self.PrimaryCausets_id_without_handle.len() {
            PrimaryCausets.push(LazyBatchPrimaryCauset::raw_with_capacity(scan_rows));
        }

        match self.decode_handle_strategy {
            NoDecode => {}
            DecodeIntHandle => {
                PrimaryCausets.push(LazyBatchPrimaryCauset::decoded_with_capacity_and_tp(
                    scan_rows,
                    EvalType::Int,
                ));
            }
            DecodeCommonHandle => {
                for _ in self.PrimaryCausets_id_without_handle.len()..PrimaryCausets_len {
                    PrimaryCausets.push(LazyBatchPrimaryCauset::raw_with_capacity(scan_rows));
                }
            }
        }

        assert_eq!(PrimaryCausets.len(), PrimaryCausets_len);
        LazyBatchPrimaryCausetVec::from(PrimaryCausets)
    }

    // Currently, we have 6 foramts of index value.
    // Value layout:
    // +--With Restore Data(for indices on string PrimaryCausets)
    // |  |
    // |  +--Non Unique (TailLen = len(PaddingData) + len(Flag), TailLen < 8 always)
    // |  |  |
    // |  |  |  Layout: TailLen |      RestoreData  |      PaddingData
    // |  |  |  Length: 1       | size(RestoreData) | size(paddingData)
    // |  |  |
    // |  |  |  The length >= 10 always because of padding.
    // |  |
    // |  |
    // |  +--Unique Common Handle
    // |  |  |
    // |  |  |
    // |  |  |  Layout: 0x00 | CHandle Flag | CHandle Len | CHandle       | RestoreData
    // |  |  |  Length: 1    | 1            | 2           | size(CHandle) | size(RestoreData)
    // |  |  |
    // |  |  |  The length > 10 always because of CHandle size.
    // |  |
    // |  |
    // |  +--Unique Integer Handle (TailLen = len(Handle) + len(Flag), TailLen == 8 || TailLen == 9)
    // |     |
    // |     |  Layout: 0x08 |    RestoreData    |  Handle
    // |     |  Length: 1    | size(RestoreData) |   8
    // |     |
    // |     |  The length >= 10 always since size(RestoreData) > 0.
    // |
    // |
    // +--Without Restore Data
    // |
    // +--Non Unique
    // |  |
    // |  |  Layout: '0'
    // |  |  Length:  1
    // |
    // +--Unique Common Handle
    // |  |
    // |  |  Layout: 0x00 | CHandle Flag | CHandle Len | CHandle
    //    |  Length: 1    | 1            | 2           | size(CHandle)
    // |
    // |
    // +--Unique Integer Handle
    // |
    // |  Layout: Handle
    // |  Length:   8
    // For more intuitive explanation, check the docs: https://docs.google.com/document/d/1Co5iMiaxitv3okJmLYLJxZYCNChcjzswJMRr-_45Eqg/edit?usp=sharing
    #[inline]
    fn process_kv_pair(
        &mut self,
        mut key: &[u8],
        value: &[u8],
        PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
    ) -> Result<()> {
        check_index_key(key)?;
        key = &key[Block::PREFIX_LEN + Block::ID_LEN..];
        if value.len() > MAX_OLD_ENCODED_VALUE_LEN {
            if value[0] <= 1 && value[1] == Block::INDEX_VALUE_COMMON_HANDLE_FLAG {
                if self.decode_handle_strategy != DecodeCommonHandle {
                    return Err(other_err!("Expect to decode index values with common handles in `DecodeCommonHandle` mode."));
                }
                self.process_unique_common_handle_kv(key, value, PrimaryCausets)
            } else {
                self.process_new_collation_kv(key, value, PrimaryCausets)
            }
        } else {
            self.process_old_collation_kv(key, value, PrimaryCausets)
        }
    }
}

impl IndexScanFreeDaemonImpl {
    #[inline]
    fn decode_handle_from_value(&self, mut value: &[u8]) -> Result<i64> {
        // NOTE: it is not `number::decode_i64`.
        value
            .read_u64()
            .map_err(|_| other_err!("Failed to decode handle in value as i64"))
            .map(|x| x as i64)
    }

    #[inline]
    fn decode_handle_from_key(&self, key: &[u8]) -> Result<i64> {
        let flag = key[0];
        let mut val = &key[1..];

        // TODO: Better to use `push_datum`. This requires us to allow `push_datum`
        // receiving optional time zone first.

        match flag {
            datum::INT_FLAG => val
                .read_i64()
                .map_err(|_| other_err!("Failed to decode handle in key as i64")),
            datum::UINT_FLAG => val
                .read_u64()
                .map_err(|_| other_err!("Failed to decode handle in key as u64"))
                .map(|x| x as i64),
            _ => Err(other_err!("Unexpected handle flag {}", flag)),
        }
    }

    fn extract_PrimaryCausets_from_row_format(
        &mut self,
        value: &[u8],
        PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
    ) -> Result<()> {
        use milevadb_query_datatype::codec::Evcausetidx::v2::{EventSlice, V1CompatibleEncoder};

        let Evcausetidx = EventSlice::from_bytes(value)?;
        for (idx, col_id) in self.PrimaryCausets_id_without_handle.iter().enumerate() {
            if let Some((spacelike, offset)) = Evcausetidx.search_in_non_null_ids(*col_id)? {
                let mut buffer_to_write = PrimaryCausets[idx].mut_raw().begin_concat_extlightlike();
                buffer_to_write
                    .write_v2_as_datum(&Evcausetidx.values()[spacelike..offset], &self.schemaReplicant[idx])?;
            } else if Evcausetidx.search_in_null_ids(*col_id) {
                PrimaryCausets[idx].mut_raw().push(datum::DATUM_DATA_NULL);
            } else {
                return Err(other_err!("Unexpected missing PrimaryCauset {}", col_id));
            }
        }
        Ok(())
    }

    fn extract_PrimaryCausets_from_datum_format(
        datum: &mut &[u8],
        PrimaryCausets: &mut [LazyBatchPrimaryCauset],
    ) -> Result<()> {
        for (i, PrimaryCauset) in PrimaryCausets.iter_mut().enumerate() {
            if datum.is_empty() {
                return Err(other_err!("{}th PrimaryCauset is missing value", i));
            }
            let (value, remaining) = datum::split_datum(datum, false)?;
            PrimaryCauset.mut_raw().push(value);
            *datum = remaining;
        }
        Ok(())
    }

    // Process index values that contain common handle flags.
    // NOTE: If there is no restore data in index value, we need to extract the index PrimaryCauset from the key.
    // 1. Unique common handle in new collation
    // |  Layout: 0x00 | CHandle Flag | CHandle Len | CHandle       | RestoreData
    // |  Length: 1    | 1            | 2           | size(CHandle) | size(RestoreData)
    //
    // 2. Unique common handle in old collation
    // |  Layout: 0x00 | CHandle Flag | CHandle Len | CHandle       |
    // |  Length: 1    | 1            | 2           | size(CHandle) |
    fn process_unique_common_handle_kv(
        &mut self,
        mut key_payload: &[u8],
        value: &[u8],
        PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
    ) -> Result<()> {
        let handle_len = (&value[2..]).read_u16().map_err(|_| {
            other_err!(
                "Fail to read common handle's length from value: {}",
                hex::encode_upper(value)
            )
        })? as usize;
        let handle_lightlike_offset = 4 + handle_len;

        if handle_lightlike_offset > value.len() {
            return Err(other_err!("`handle_len` is corrupted: {}", handle_len));
        }

        // If there are some restore data, the index value is in new collation.
        if handle_lightlike_offset < value.len() {
            let restore_values = &value[handle_lightlike_offset..];
            self.extract_PrimaryCausets_from_row_format(restore_values, PrimaryCausets)?;
        } else {
            // Otherwise, the index value is in old collation, we should extract the index PrimaryCausets from the key.
            Self::extract_PrimaryCausets_from_datum_format(
                &mut key_payload,
                &mut PrimaryCausets[..self.PrimaryCausets_id_without_handle.len()],
            )?;
        }

        // Decode the common handles.
        let mut common_handle = &value[4..handle_lightlike_offset];
        Self::extract_PrimaryCausets_from_datum_format(
            &mut common_handle,
            &mut PrimaryCausets[self.PrimaryCausets_id_without_handle.len()..],
        )?;

        Ok(())
    }

    // Process index values that are in new collation but don't contain common handle.
    // NOTE: We should extract the index PrimaryCausets from the key if there are common handles in the key or the tail length of the value is less than 8.
    // These index values have 2 types.
    // 1. Non-unique index
    //      * Key contains a int handle.
    //      * Key contains a common handle.
    // 2. Unique index
    //      * Value contains a int handle.
    fn process_new_collation_kv(
        &mut self,
        mut key_payload: &[u8],
        value: &[u8],
        PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
    ) -> Result<()> {
        let tail_len = value[0] as usize;

        if tail_len > value.len() {
            return Err(other_err!("`tail_len`: {} is corrupted", tail_len));
        }

        let restore_values = &value[1..value.len() - tail_len];
        self.extract_PrimaryCausets_from_row_format(restore_values, PrimaryCausets)?;

        match self.decode_handle_strategy {
            NoDecode => {}
            // This is a non-unique index value, we should extract the int handle from the key.
            DecodeIntHandle if tail_len < 8 => {
                datum::skip_n(&mut key_payload, self.PrimaryCausets_id_without_handle.len())?;
                let handle = self.decode_handle_from_key(key_payload)?;
                PrimaryCausets[self.PrimaryCausets_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle));
            }
            // This is a unique index value, we should extract the int handle from the value.
            DecodeIntHandle => {
                let handle = self.decode_handle_from_value(&value[value.len() - tail_len..])?;
                PrimaryCausets[self.PrimaryCausets_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle));
            }
            // This is a non-unique index value, we should extract the common handle from the key.
            DecodeCommonHandle => {
                datum::skip_n(&mut key_payload, self.PrimaryCausets_id_without_handle.len())?;
                Self::extract_PrimaryCausets_from_datum_format(
                    &mut key_payload,
                    &mut PrimaryCausets[self.PrimaryCausets_id_without_handle.len()..],
                )?;
            }
        }
        Ok(())
    }

    // Process index values that are in old collation but don't contain common handles.
    // NOTE: We should extract the index PrimaryCausets from the key first, and extract the handles from value if there is no handle in the key.
    // Otherwise, extract the handles from the key.
    fn process_old_collation_kv(
        &mut self,
        mut key_payload: &[u8],
        value: &[u8],
        PrimaryCausets: &mut LazyBatchPrimaryCausetVec,
    ) -> Result<()> {
        Self::extract_PrimaryCausets_from_datum_format(
            &mut key_payload,
            &mut PrimaryCausets[..self.PrimaryCausets_id_without_handle.len()],
        )?;

        match self.decode_handle_strategy {
            NoDecode => {}
            // For normal index, it is placed at the lightlike and any PrimaryCausets prior to it are
            // ensured to be interested. For unique index, it is placed in the value.
            DecodeIntHandle if key_payload.is_empty() => {
                // This is a unique index, and we should look up PK int handle in the value.
                let handle_val = self.decode_handle_from_value(value)?;
                PrimaryCausets[self.PrimaryCausets_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle_val));
            }
            DecodeIntHandle => {
                // This is a normal index, and we should look up PK handle in the key.
                let handle_val = self.decode_handle_from_key(key_payload)?;
                PrimaryCausets[self.PrimaryCausets_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle_val));
            }
            DecodeCommonHandle => {
                // Otherwise, if the handle is common handle, we extract it from the key.
                Self::extract_PrimaryCausets_from_datum_format(
                    &mut key_payload,
                    &mut PrimaryCausets[self.PrimaryCausets_id_without_handle.len()..],
                )?;
            }
        }

        Ok(())
    }
}

#[causet(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use codec::prelude::NumberEncoder;
    use ekvproto::interlock::KeyCone;
    use milevadb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    use fidel_timeshare::PrimaryCausetInfo;

    use milevadb_query_common::causet_storage::test_fixture::FixtureStorage;
    use milevadb_query_common::util::convert_to_prefix_next;
    use milevadb_query_datatype::codec::data_type::*;
    use milevadb_query_datatype::codec::{
        datum,
        Evcausetidx::v2::encoder_for_test::{PrimaryCauset, EventEncoder},
        Block, Datum,
    };
    use milevadb_query_datatype::expr::EvalConfig;

    #[test]
    fn test_basic() {
        const Block_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let mut ctx = EvalContext::default();

        // Index schemaReplicant: (INT, FLOAT)

        // the elements in data are: [int index, float index, handle id].
        let data = vec![
            [Datum::I64(-5), Datum::F64(0.3), Datum::I64(10)],
            [Datum::I64(5), Datum::F64(5.1), Datum::I64(5)],
            [Datum::I64(5), Datum::F64(10.5), Datum::I64(2)],
        ];

        // The PrimaryCauset info for each PrimaryCauset in `data`. Used to build the executor.
        let PrimaryCausets_info = vec![
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci
            },
        ];

        // The schemaReplicant of these PrimaryCausets. Used to check executor output.
        let schemaReplicant = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        // Case 1. Normal index.

        // For a normal index, the PK handle is stored in the key and nothing interesting is stored
        // in the value. So let's build corresponding KV data.

        let store = {
            let kv: Vec<_> = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&mut ctx, datums).unwrap();
                    let key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &index_data);
                    let value = vec![];
                    (key, value)
                })
                .collect();
            FixtureStorage::from(kv)
        };

        {
            // Case 1.1. Normal index, without PK, scan total index in reverse order.

            let key_cones = vec![{
                let mut cone = KeyCone::default();
                let spacelike_data = datum::encode_key(&mut ctx, &[Datum::Min]).unwrap();
                let spacelike_key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &spacelike_data);
                cone.set_spacelike(spacelike_key);
                let lightlike_data = datum::encode_key(&mut ctx, &[Datum::Max]).unwrap();
                let lightlike_key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &lightlike_data);
                cone.set_lightlike(lightlike_key);
                cone
            }];

            let mut executor = BatchIndexScanFreeDaemon::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![PrimaryCausets_info[0].clone(), PrimaryCausets_info[1].clone()],
                key_cones,
                0,
                true,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 2);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 3);
            assert!(result.physical_PrimaryCausets[0].is_raw());
            result.physical_PrimaryCausets[0]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[0])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[0].decoded().to_int_vec(),
                &[Some(5), Some(5), Some(-5)]
            );
            assert!(result.physical_PrimaryCausets[1].is_raw());
            result.physical_PrimaryCausets[1]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[1])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[1].decoded().to_real_vec(),
                &[
                    Real::new(10.5).ok(),
                    Real::new(5.1).ok(),
                    Real::new(0.3).ok()
                ]
            );
        }

        {
            // Case 1.2. Normal index, with PK, scan index prefix.

            let key_cones = vec![{
                let mut cone = KeyCone::default();
                let spacelike_data = datum::encode_key(&mut ctx, &[Datum::I64(2)]).unwrap();
                let spacelike_key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &spacelike_data);
                cone.set_spacelike(spacelike_key);
                let lightlike_data = datum::encode_key(&mut ctx, &[Datum::I64(6)]).unwrap();
                let lightlike_key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &lightlike_data);
                cone.set_lightlike(lightlike_key);
                cone
            }];

            let mut executor = BatchIndexScanFreeDaemon::new(
                store,
                Arc::new(EvalConfig::default()),
                vec![
                    PrimaryCausets_info[0].clone(),
                    PrimaryCausets_info[1].clone(),
                    PrimaryCausets_info[2].clone(),
                ],
                key_cones,
                0,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 3);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 2);
            assert!(result.physical_PrimaryCausets[0].is_raw());
            result.physical_PrimaryCausets[0]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[0])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[0].decoded().to_int_vec(),
                &[Some(5), Some(5)]
            );
            assert!(result.physical_PrimaryCausets[1].is_raw());
            result.physical_PrimaryCausets[1]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[1])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[1].decoded().to_real_vec(),
                &[Real::new(5.1).ok(), Real::new(10.5).ok()]
            );
            assert!(result.physical_PrimaryCausets[2].is_decoded());
            assert_eq!(
                result.physical_PrimaryCausets[2].decoded().to_int_vec(),
                &[Some(5), Some(2)]
            );
        }

        // Case 2. Unique index.

        // For a unique index, the PK handle is stored in the value.

        let store = {
            let kv: Vec<_> = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&mut ctx, &datums[0..2]).unwrap();
                    let key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &index_data);
                    // PK handle in the value
                    let mut value = vec![];
                    value
                        .write_u64(datums[2].as_int().unwrap().unwrap() as u64)
                        .unwrap();
                    (key, value)
                })
                .collect();
            FixtureStorage::from(kv)
        };

        {
            // Case 2.1. Unique index, prefix cone scan.

            let key_cones = vec![{
                let mut cone = KeyCone::default();
                let spacelike_data = datum::encode_key(&mut ctx, &[Datum::I64(5)]).unwrap();
                let spacelike_key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &spacelike_data);
                cone.set_spacelike(spacelike_key);
                cone.set_lightlike(cone.get_spacelike().to_vec());
                convert_to_prefix_next(cone.mut_lightlike());
                cone
            }];

            let mut executor = BatchIndexScanFreeDaemon::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    PrimaryCausets_info[0].clone(),
                    PrimaryCausets_info[1].clone(),
                    PrimaryCausets_info[2].clone(),
                ],
                key_cones,
                0,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 3);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 2);
            assert!(result.physical_PrimaryCausets[0].is_raw());
            result.physical_PrimaryCausets[0]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[0])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[0].decoded().to_int_vec(),
                &[Some(5), Some(5)]
            );
            assert!(result.physical_PrimaryCausets[1].is_raw());
            result.physical_PrimaryCausets[1]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[1])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[1].decoded().to_real_vec(),
                &[Real::new(5.1).ok(), Real::new(10.5).ok()]
            );
            assert!(result.physical_PrimaryCausets[2].is_decoded());
            assert_eq!(
                result.physical_PrimaryCausets[2].decoded().to_int_vec(),
                &[Some(5), Some(2)]
            );
        }

        {
            // Case 2.2. Unique index, point scan.

            let key_cones = vec![{
                let mut cone = KeyCone::default();
                let spacelike_data =
                    datum::encode_key(&mut ctx, &[Datum::I64(5), Datum::F64(5.1)]).unwrap();
                let spacelike_key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &spacelike_data);
                cone.set_spacelike(spacelike_key);
                cone.set_lightlike(cone.get_spacelike().to_vec());
                convert_to_prefix_next(cone.mut_lightlike());
                cone
            }];

            let mut executor = BatchIndexScanFreeDaemon::new(
                store,
                Arc::new(EvalConfig::default()),
                vec![
                    PrimaryCausets_info[0].clone(),
                    PrimaryCausets_info[1].clone(),
                    PrimaryCausets_info[2].clone(),
                ],
                key_cones,
                0,
                false,
                true,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 3);
            assert_eq!(result.physical_PrimaryCausets.rows_len(), 1);
            assert!(result.physical_PrimaryCausets[0].is_raw());
            result.physical_PrimaryCausets[0]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[0])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[0].decoded().to_int_vec(),
                &[Some(5)]
            );
            assert!(result.physical_PrimaryCausets[1].is_raw());
            result.physical_PrimaryCausets[1]
                .ensure_all_decoded_for_test(&mut ctx, &schemaReplicant[1])
                .unwrap();
            assert_eq!(
                result.physical_PrimaryCausets[1].decoded().to_real_vec(),
                &[Real::new(5.1).ok()]
            );
            assert!(result.physical_PrimaryCausets[2].is_decoded());
            assert_eq!(
                result.physical_PrimaryCausets[2].decoded().to_int_vec(),
                &[Some(5)]
            );
        }
    }

    #[test]
    fn test_unique_common_handle_index() {
        const Block_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let PrimaryCausets_info = vec![
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(3);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
        ];

        // The schemaReplicant of these PrimaryCausets. Used to check executor output.
        let schemaReplicant: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
        ];

        let PrimaryCausets = vec![PrimaryCauset::new(1, 2), PrimaryCauset::new(2, 3), PrimaryCauset::new(3, 4.0)];
        let datums = vec![Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];

        let mut value_prefix = vec![];
        let mut restore_data = vec![];
        let common_handle =
            datum::encode_value(&mut EvalContext::default(), &[Datum::F64(4.0)]).unwrap();

        restore_data
            .write_row(&mut EvalContext::default(), PrimaryCausets)
            .unwrap();

        // Tail length
        value_prefix.push(0);
        // Common handle flag
        value_prefix.push(127);
        // Common handle length
        value_prefix.write_u16(common_handle.len() as u16).unwrap();

        // Common handle
        value_prefix.extlightlike(common_handle);

        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..2]).unwrap();
        let key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &index_data);

        let key_cones = vec![{
            let mut cone = KeyCone::default();
            let spacelike_key = key.clone();
            cone.set_spacelike(spacelike_key);
            cone.set_lightlike(cone.get_spacelike().to_vec());
            convert_to_prefix_next(cone.mut_lightlike());
            cone
        }];

        // 1. New collation unique common handle.
        let mut value = value_prefix.clone();
        value.extlightlike(restore_data);
        let store = FixtureStorage::from(vec![(key.clone(), value)]);
        let mut executor = BatchIndexScanFreeDaemon::new(
            store,
            Arc::new(EvalConfig::default()),
            PrimaryCausets_info.clone(),
            key_cones.clone(),
            1,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 3);
        assert_eq!(result.physical_PrimaryCausets.rows_len(), 1);
        assert!(result.physical_PrimaryCausets[0].is_raw());
        result.physical_PrimaryCausets[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[0])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_PrimaryCausets[1].is_raw());
        result.physical_PrimaryCausets[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[1])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_PrimaryCausets[2].is_raw());
        result.physical_PrimaryCausets[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[2])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );

        let value = value_prefix;
        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanFreeDaemon::new(
            store,
            Arc::new(EvalConfig::default()),
            PrimaryCausets_info,
            key_cones,
            1,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 3);
        assert_eq!(result.physical_PrimaryCausets.rows_len(), 1);
        assert!(result.physical_PrimaryCausets[0].is_raw());
        result.physical_PrimaryCausets[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[0])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_PrimaryCausets[1].is_raw());
        result.physical_PrimaryCausets[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[1])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_PrimaryCausets[2].is_raw());
        result.physical_PrimaryCausets[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[2])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
    }

    #[test]
    fn test_old_collation_non_unique_common_handle_index() {
        const Block_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let PrimaryCausets_info = vec![
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(3);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
        ];

        // The schemaReplicant of these PrimaryCausets. Used to check executor output.
        let schemaReplicant: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
        ];

        let datums = vec![Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];

        let common_handle = datum::encode_key(
            &mut EvalContext::default(),
            &[Datum::U64(3), Datum::F64(4.0)],
        )
        .unwrap();

        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..1]).unwrap();
        let mut key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &index_data);
        key.extlightlike(common_handle);

        let key_cones = vec![{
            let mut cone = KeyCone::default();
            let spacelike_key = key.clone();
            cone.set_spacelike(spacelike_key);
            cone.set_lightlike(cone.get_spacelike().to_vec());
            convert_to_prefix_next(cone.mut_lightlike());
            cone
        }];

        let store = FixtureStorage::from(vec![(key, vec![])]);
        let mut executor = BatchIndexScanFreeDaemon::new(
            store,
            Arc::new(EvalConfig::default()),
            PrimaryCausets_info,
            key_cones,
            2,
            false,
            false,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 3);
        assert_eq!(result.physical_PrimaryCausets.rows_len(), 1);
        assert!(result.physical_PrimaryCausets[0].is_raw());
        result.physical_PrimaryCausets[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[0])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_PrimaryCausets[1].is_raw());
        result.physical_PrimaryCausets[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[1])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_PrimaryCausets[2].is_raw());
        result.physical_PrimaryCausets[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[2])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
    }

    #[test]
    fn test_new_collation_unique_int_handle_index() {
        const Block_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let PrimaryCausets_info = vec![
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(3);
                ci.set_pk_handle(true);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
        ];

        // The schemaReplicant of these PrimaryCausets. Used to check executor output.
        let schemaReplicant: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        let PrimaryCausets = vec![PrimaryCauset::new(1, 2), PrimaryCauset::new(2, 3.0), PrimaryCauset::new(3, 4)];
        let datums = vec![Datum::U64(2), Datum::F64(3.0), Datum::U64(4)];
        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..2]).unwrap();
        let key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &index_data);

        let mut restore_data = vec![];
        restore_data
            .write_row(&mut EvalContext::default(), PrimaryCausets)
            .unwrap();
        let mut value = vec![8];
        value.extlightlike(restore_data);
        value
            .write_u64(datums[2].as_int().unwrap().unwrap() as u64)
            .unwrap();

        let key_cones = vec![{
            let mut cone = KeyCone::default();
            let spacelike_key = key.clone();
            cone.set_spacelike(spacelike_key);
            cone.set_lightlike(cone.get_spacelike().to_vec());
            convert_to_prefix_next(cone.mut_lightlike());
            cone
        }];

        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanFreeDaemon::new(
            store,
            Arc::new(EvalConfig::default()),
            PrimaryCausets_info,
            key_cones,
            0,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 3);
        assert_eq!(result.physical_PrimaryCausets.rows_len(), 1);
        assert!(result.physical_PrimaryCausets[0].is_raw());
        result.physical_PrimaryCausets[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[0])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_PrimaryCausets[1].is_raw());
        result.physical_PrimaryCausets[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[1])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[1].decoded().to_real_vec(),
            &[Real::new(3.0).ok()]
        );
        assert!(result.physical_PrimaryCausets[2].is_decoded());
        assert_eq!(
            result.physical_PrimaryCausets[2].decoded().to_int_vec(),
            &[Some(4)]
        );
    }

    #[test]
    fn test_new_collation_non_unique_int_handle_index() {
        const Block_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let PrimaryCausets_info = vec![
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(3);
                ci.set_pk_handle(true);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
        ];

        // The schemaReplicant of these PrimaryCausets. Used to check executor output.
        let schemaReplicant: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        let PrimaryCausets = vec![PrimaryCauset::new(1, 2), PrimaryCauset::new(2, 3.0), PrimaryCauset::new(3, 4)];
        let datums = vec![Datum::U64(2), Datum::F64(3.0), Datum::U64(4)];
        let index_data = datum::encode_key(&mut EvalContext::default(), &datums).unwrap();
        let key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &index_data);

        let mut restore_data = vec![];
        restore_data
            .write_row(&mut EvalContext::default(), PrimaryCausets)
            .unwrap();
        let mut value = vec![0];
        value.extlightlike(restore_data);

        let key_cones = vec![{
            let mut cone = KeyCone::default();
            let spacelike_key = key.clone();
            cone.set_spacelike(spacelike_key);
            cone.set_lightlike(cone.get_spacelike().to_vec());
            convert_to_prefix_next(cone.mut_lightlike());
            cone
        }];

        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanFreeDaemon::new(
            store,
            Arc::new(EvalConfig::default()),
            PrimaryCausets_info,
            key_cones,
            0,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 3);
        assert_eq!(result.physical_PrimaryCausets.rows_len(), 1);
        assert!(result.physical_PrimaryCausets[0].is_raw());
        result.physical_PrimaryCausets[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[0])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_PrimaryCausets[1].is_raw());
        result.physical_PrimaryCausets[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[1])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[1].decoded().to_real_vec(),
            &[Real::new(3.0).ok()]
        );
        assert!(result.physical_PrimaryCausets[2].is_decoded());
        assert_eq!(
            result.physical_PrimaryCausets[2].decoded().to_int_vec(),
            &[Some(4)]
        );
    }

    #[test]
    fn test_new_collation_non_unique_common_handle_index() {
        const Block_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let PrimaryCausets_info = vec![
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(3);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
        ];

        // The schemaReplicant of these PrimaryCausets. Used to check executor output.
        let schemaReplicant: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
        ];

        let PrimaryCausets = vec![PrimaryCauset::new(1, 2), PrimaryCauset::new(2, 3), PrimaryCauset::new(3, 4.0)];
        let datums = vec![Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];
        let index_data = datum::encode_key(&mut EvalContext::default(), &datums).unwrap();
        let key = Block::encode_index_seek_key(Block_ID, INDEX_ID, &index_data);

        let mut restore_data = vec![];
        restore_data
            .write_row(&mut EvalContext::default(), PrimaryCausets)
            .unwrap();
        let mut value = vec![0];
        value.extlightlike(restore_data);

        let key_cones = vec![{
            let mut cone = KeyCone::default();
            let spacelike_key = key.clone();
            cone.set_spacelike(spacelike_key);
            cone.set_lightlike(cone.get_spacelike().to_vec());
            convert_to_prefix_next(cone.mut_lightlike());
            cone
        }];

        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanFreeDaemon::new(
            store,
            Arc::new(EvalConfig::default()),
            PrimaryCausets_info,
            key_cones,
            1,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_PrimaryCausets.PrimaryCausets_len(), 3);
        assert_eq!(result.physical_PrimaryCausets.rows_len(), 1);
        assert!(result.physical_PrimaryCausets[0].is_raw());
        result.physical_PrimaryCausets[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[0])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_PrimaryCausets[1].is_raw());
        result.physical_PrimaryCausets[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[1])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_PrimaryCausets[2].is_raw());
        result.physical_PrimaryCausets[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schemaReplicant[2])
            .unwrap();
        assert_eq!(
            result.physical_PrimaryCausets[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
    }
}
