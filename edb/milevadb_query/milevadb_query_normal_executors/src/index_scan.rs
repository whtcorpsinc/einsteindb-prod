// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::Arc;

use ekvproto::interlock::KeyCone;
use fidel_timeshare::PrimaryCausetInfo;
use fidel_timeshare::IndexScan;

use super::{scan::InnerFreeDaemon, Event, ScanFreeDaemon, ScanFreeDaemonOptions};
use milevadb_query_common::causet_storage::causet_storage;
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::Block::{self, check_index_key};
use milevadb_query_datatype::expr::EvalContext;

pub struct IndexInnerFreeDaemon {
    pk_col: Option<PrimaryCausetInfo>,
    col_ids: Vec<i64>,
}

impl IndexInnerFreeDaemon {
    fn new(meta: &mut IndexScan) -> Self {
        let mut pk_col = None;
        let cols = meta.mut_PrimaryCausets();
        if cols.last().map_or(false, PrimaryCausetInfo::get_pk_handle) {
            pk_col = Some(cols.pop().unwrap());
        }
        let col_ids = cols.iter().map(PrimaryCausetInfo::get_PrimaryCauset_id).collect();
        Self { pk_col, col_ids }
    }
}

impl InnerFreeDaemon for IndexInnerFreeDaemon {
    fn decode_row(
        &self,
        ctx: &mut EvalContext,
        key: Vec<u8>,
        value: Vec<u8>,
        PrimaryCausets: Arc<Vec<PrimaryCausetInfo>>,
    ) -> Result<Option<Event>> {
        use byteorder::{BigEndian, ReadBytesExt};
        use milevadb_query_datatype::codec::datum;
        use milevadb_query_datatype::prelude::*;
        use milevadb_query_datatype::FieldTypeFlag;

        check_index_key(key.as_slice())?;
        let (mut values, handle) = box_try!(Block::cut_idx_key(key, &self.col_ids));
        let handle = match handle {
            None => box_try!(value.as_slice().read_i64::<BigEndian>()),
            Some(h) => h,
        };

        if let Some(ref pk_col) = self.pk_col {
            let handle_datum = if pk_col
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::UNSIGNED)
            {
                // PK PrimaryCauset is unsigned
                datum::Datum::U64(handle as u64)
            } else {
                datum::Datum::I64(handle)
            };
            let mut bytes = box_try!(datum::encode_key(ctx, &[handle_datum]));
            values.applightlike(pk_col.get_PrimaryCauset_id(), &mut bytes);
        }
        Ok(Some(Event::origin(handle, values, PrimaryCausets)))
    }
}

pub type IndexScanFreeDaemon<S> = ScanFreeDaemon<S, IndexInnerFreeDaemon>;

impl<S: causet_storage> IndexScanFreeDaemon<S> {
    pub fn index_scan(
        mut meta: IndexScan,
        context: EvalContext,
        key_cones: Vec<KeyCone>,
        causet_storage: S,
        unique: bool,
        is_scanned_cone_aware: bool,
    ) -> Result<Self> {
        let PrimaryCausets = meta.get_PrimaryCausets().to_vec();
        let inner = IndexInnerFreeDaemon::new(&mut meta);
        Self::new(ScanFreeDaemonOptions {
            inner,
            context,
            PrimaryCausets,
            key_cones,
            causet_storage,
            is_backward: meta.get_desc(),
            is_key_only: false,
            accept_point_cone: unique,
            is_scanned_cone_aware,
        })
    }

    pub fn index_scan_with_cols_len(
        context: EvalContext,
        cols: i64,
        key_cones: Vec<KeyCone>,
        causet_storage: S,
    ) -> Result<Self> {
        let col_ids: Vec<i64> = (0..cols).collect();
        let inner = IndexInnerFreeDaemon {
            col_ids,
            pk_col: None,
        };
        Self::new(ScanFreeDaemonOptions {
            inner,
            context,
            PrimaryCausets: vec![],
            key_cones,
            causet_storage,
            is_backward: false,
            is_key_only: false,
            accept_point_cone: false,
            is_scanned_cone_aware: false,
        })
    }
}

#[causet(test)]
pub mod tests {
    use byteorder::{BigEndian, WriteBytesExt};
    use std::i64;

    use milevadb_query_datatype::FieldTypeTp;
    use fidel_timeshare::PrimaryCausetInfo;

    use super::super::tests::*;
    use super::super::FreeDaemon;
    use super::*;
    use milevadb_query_common::execute_stats::ExecuteStats;

    use milevadb_query_common::causet_storage::test_fixture::FixtureStorage;
    use milevadb_query_datatype::codec::datum::{self, Datum};
    use milevadb_query_datatype::codec::Block::generate_index_data_for_test;

    const Block_ID: i64 = 1;
    const INDEX_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    // get_idx_cone get cone for index in [("abc",spacelike),("abc", lightlike))
    pub fn get_idx_cone(
        Block_id: i64,
        idx_id: i64,
        spacelike: i64,
        lightlike: i64,
        val_spacelike: &Datum,
        val_lightlike: &Datum,
        unique: bool,
    ) -> KeyCone {
        let (_, spacelike_key) =
            generate_index_data_for_test(Block_id, idx_id, spacelike, val_spacelike, unique);
        let (_, lightlike_key) = generate_index_data_for_test(Block_id, idx_id, lightlike, val_lightlike, unique);
        let mut key_cone = KeyCone::default();
        key_cone.set_spacelike(spacelike_key);
        key_cone.set_lightlike(lightlike_key);
        key_cone
    }

    pub fn prepare_index_data(
        key_number: usize,
        Block_id: i64,
        index_id: i64,
        unique: bool,
    ) -> BlockData {
        let cols = vec![
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
        ];

        let mut kv_data = Vec::new();
        let mut expect_rows = Vec::new();

        let idx_col_val = Datum::Bytes(b"abc".to_vec());
        for handle in 0..key_number {
            let (expect_row, idx_key) = generate_index_data_for_test(
                Block_id,
                index_id,
                handle as i64,
                &idx_col_val,
                unique,
            );
            expect_rows.push(expect_row);
            let value = if unique {
                let mut value = Vec::with_capacity(8);
                value.write_i64::<BigEndian>(handle as i64).unwrap();
                value
            } else {
                vec![1; 0]
            };
            kv_data.push((idx_key, value));
        }
        BlockData {
            kv_data,
            expect_rows,
            cols,
        }
    }

    pub struct IndexTestWrapper {
        data: BlockData,
        pub store: FixtureStorage,
        pub scan: IndexScan,
        pub cones: Vec<KeyCone>,
        cols: Vec<PrimaryCausetInfo>,
    }

    impl IndexTestWrapper {
        fn include_pk_cols() -> IndexTestWrapper {
            let unique = false;
            let test_data = prepare_index_data(KEY_NUMBER, Block_ID, INDEX_ID, unique);
            let mut wrapper = IndexTestWrapper::new(unique, test_data);
            let mut cols = wrapper.data.cols.clone();
            cols.push(wrapper.data.get_col_pk());
            wrapper.scan.set_PrimaryCausets(cols.clone().into());
            wrapper.cols = cols;
            wrapper
        }

        pub fn new(unique: bool, test_data: BlockData) -> IndexTestWrapper {
            let store = FixtureStorage::from(test_data.kv_data.clone());
            let mut scan = IndexScan::default();
            // prepare cols
            let cols = test_data.cols.clone();
            let col_req = cols.clone().into();
            scan.set_PrimaryCausets(col_req);
            // prepare cone
            let val_spacelike = Datum::Bytes(b"a".to_vec());
            let val_lightlike = Datum::Bytes(b"z".to_vec());
            let cone = get_idx_cone(
                Block_ID,
                INDEX_ID,
                0,
                i64::MAX,
                &val_spacelike,
                &val_lightlike,
                unique,
            );
            let key_cones = vec![cone];
            IndexTestWrapper {
                data: test_data,
                store,
                scan,
                cones: key_cones,
                cols,
            }
        }
    }

    #[test]
    fn test_multiple_cones() {
        let unique = false;
        let test_data = prepare_index_data(KEY_NUMBER, Block_ID, INDEX_ID, unique);
        let mut wrapper = IndexTestWrapper::new(unique, test_data);
        let val_spacelike = Datum::Bytes(b"abc".to_vec());
        let val_lightlike = Datum::Bytes(b"abc".to_vec());
        let r1 = get_idx_cone(
            Block_ID,
            INDEX_ID,
            0,
            (KEY_NUMBER / 3) as i64,
            &val_spacelike,
            &val_lightlike,
            unique,
        );
        let r2 = get_idx_cone(
            Block_ID,
            INDEX_ID,
            (KEY_NUMBER / 3) as i64,
            (KEY_NUMBER / 2) as i64,
            &val_spacelike,
            &val_lightlike,
            unique,
        );
        wrapper.cones = vec![r1, r2];

        let mut scanner = IndexScanFreeDaemon::index_scan(
            wrapper.scan,
            EvalContext::default(),
            wrapper.cones,
            wrapper.store,
            false,
            false,
        )
        .unwrap();

        for handle in 0..KEY_NUMBER / 2 {
            let Evcausetidx = scanner.next().unwrap().unwrap().take_origin().unwrap();
            assert_eq!(Evcausetidx.handle, handle as i64);
            assert_eq!(Evcausetidx.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_PrimaryCauset_id();
                let v = Evcausetidx.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_unique_index_scan() {
        let unique = true;
        let test_data = prepare_index_data(KEY_NUMBER, Block_ID, INDEX_ID, unique);
        let mut wrapper = IndexTestWrapper::new(unique, test_data);

        let val_spacelike = Datum::Bytes(b"abc".to_vec());
        let val_lightlike = Datum::Bytes(b"abc".to_vec());
        // point get
        let r1 = get_idx_cone(Block_ID, INDEX_ID, 0, 1, &val_spacelike, &val_lightlike, unique);
        // cone seek
        let r2 = get_idx_cone(Block_ID, INDEX_ID, 1, 4, &val_spacelike, &val_lightlike, unique);
        // point get
        let r3 = get_idx_cone(Block_ID, INDEX_ID, 4, 5, &val_spacelike, &val_lightlike, unique);
        //cone seek
        let r4 = get_idx_cone(
            Block_ID,
            INDEX_ID,
            5,
            (KEY_NUMBER + 1) as i64,
            &val_spacelike,
            &val_lightlike,
            unique,
        );
        let r5 = get_idx_cone(
            Block_ID,
            INDEX_ID,
            (KEY_NUMBER + 1) as i64,
            (KEY_NUMBER + 2) as i64,
            &val_spacelike,
            &val_lightlike,
            unique,
        ); // point get but miss
        wrapper.cones = vec![r1, r2, r3, r4, r5];

        let mut scanner = IndexScanFreeDaemon::index_scan(
            wrapper.scan,
            EvalContext::default(),
            wrapper.cones,
            wrapper.store,
            unique,
            false,
        )
        .unwrap();
        for handle in 0..KEY_NUMBER {
            let Evcausetidx = scanner.next().unwrap().unwrap().take_origin().unwrap();
            assert_eq!(Evcausetidx.handle, handle as i64);
            assert_eq!(Evcausetidx.data.len(), 2);
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_PrimaryCauset_id();
                let v = Evcausetidx.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
        let expected_counts = vec![1, 3, 1, 5, 0];
        let mut exec_stats = ExecuteStats::new(0);
        scanner.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.scanned_rows_per_cone);
    }

    #[test]
    fn test_reverse_scan() {
        let unique = false;
        let test_data = prepare_index_data(KEY_NUMBER, Block_ID, INDEX_ID, unique);
        let mut wrapper = IndexTestWrapper::new(unique, test_data);
        wrapper.scan.set_desc(true);

        let val_spacelike = Datum::Bytes(b"abc".to_vec());
        let val_lightlike = Datum::Bytes(b"abc".to_vec());
        let r1 = get_idx_cone(
            Block_ID,
            INDEX_ID,
            0,
            (KEY_NUMBER / 2) as i64,
            &val_spacelike,
            &val_lightlike,
            unique,
        );
        let r2 = get_idx_cone(
            Block_ID,
            INDEX_ID,
            (KEY_NUMBER / 2) as i64,
            i64::MAX,
            &val_spacelike,
            &val_lightlike,
            unique,
        );
        wrapper.cones = vec![r1, r2];

        let mut scanner = IndexScanFreeDaemon::index_scan(
            wrapper.scan,
            EvalContext::default(),
            wrapper.cones,
            wrapper.store,
            unique,
            false,
        )
        .unwrap();

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let Evcausetidx = scanner.next().unwrap().unwrap().take_origin().unwrap();
            assert_eq!(Evcausetidx.handle, handle as i64);
            assert_eq!(Evcausetidx.data.len(), 2);
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_PrimaryCauset_id();
                let v = Evcausetidx.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_include_pk() {
        let wrapper = IndexTestWrapper::include_pk_cols();

        let mut scanner = IndexScanFreeDaemon::index_scan(
            wrapper.scan,
            EvalContext::default(),
            wrapper.cones,
            wrapper.store,
            false,
            false,
        )
        .unwrap();

        for handle in 0..KEY_NUMBER {
            let Evcausetidx = scanner.next().unwrap().unwrap().take_origin().unwrap();
            assert_eq!(Evcausetidx.handle, handle as i64);
            assert_eq!(Evcausetidx.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            let handle_datum = datum::Datum::I64(handle as i64);
            let pk = datum::encode_key(&mut EvalContext::default(), &[handle_datum]).unwrap();
            for col in &wrapper.cols {
                let cid = col.get_PrimaryCauset_id();
                let v = Evcausetidx.data.get(cid).unwrap();
                if col.get_pk_handle() {
                    assert_eq!(pk, v.to_vec());
                    continue;
                }
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }
}
