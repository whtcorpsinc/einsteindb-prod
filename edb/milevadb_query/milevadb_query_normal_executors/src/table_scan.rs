// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::Arc;

use ekvproto::interlock::KeyCone;
use violetabftstore::interlock::::collections::HashSet;
use fidel_timeshare::PrimaryCausetInfo;
use fidel_timeshare::BlockScan;

use super::{scan::InnerFreeDaemon, Event, ScanFreeDaemon, ScanFreeDaemonOptions};
use milevadb_query_common::causet_storage::causet_storage;
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::Block::{self, check_record_key};
use milevadb_query_datatype::expr::EvalContext;

pub struct BlockInnerFreeDaemon {
    col_ids: HashSet<i64>,
}

impl BlockInnerFreeDaemon {
    fn new(meta: &BlockScan) -> Self {
        let col_ids = meta
            .get_PrimaryCausets()
            .iter()
            .filter(|c| !c.get_pk_handle())
            .map(PrimaryCausetInfo::get_PrimaryCauset_id)
            .collect();
        Self { col_ids }
    }

    fn is_key_only(&self) -> bool {
        self.col_ids.is_empty()
    }
}

impl InnerFreeDaemon for BlockInnerFreeDaemon {
    fn decode_row(
        &self,
        _ctx: &mut EvalContext,
        key: Vec<u8>,
        value: Vec<u8>,
        PrimaryCausets: Arc<Vec<PrimaryCausetInfo>>,
    ) -> Result<Option<Event>> {
        check_record_key(key.as_slice())?;
        let row_data = box_try!(Block::cut_row(value, &self.col_ids, PrimaryCausets.clone()));
        let h = box_try!(Block::decode_int_handle(&key));
        Ok(Some(Event::origin(h, row_data, PrimaryCausets)))
    }
}

pub type BlockScanFreeDaemon<S> = ScanFreeDaemon<S, BlockInnerFreeDaemon>;

impl<S: causet_storage> BlockScanFreeDaemon<S> {
    pub fn Block_scan(
        mut meta: BlockScan,
        context: EvalContext,
        key_cones: Vec<KeyCone>,
        causet_storage: S,
        is_scanned_cone_aware: bool,
    ) -> Result<Self> {
        let inner = BlockInnerFreeDaemon::new(&meta);
        let is_key_only = inner.is_key_only();

        Self::new(ScanFreeDaemonOptions {
            inner,
            context,
            PrimaryCausets: meta.take_PrimaryCausets().to_vec(),
            key_cones,
            causet_storage,
            is_backward: meta.get_desc(),
            is_key_only,
            accept_point_cone: true,
            is_scanned_cone_aware,
        })
    }
}

#[causet(test)]
mod tests {
    use std::i64;

    use ekvproto::interlock::KeyCone;
    use fidel_timeshare::{PrimaryCausetInfo, BlockScan};

    use super::super::tests::*;
    use super::super::FreeDaemon;
    use milevadb_query_common::execute_stats::ExecuteStats;
    use milevadb_query_common::causet_storage::test_fixture::FixtureStorage;
    use milevadb_query_datatype::expr::EvalContext;

    const Block_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    struct BlockScanTestWrapper {
        data: BlockData,
        store: FixtureStorage,
        Block_scan: BlockScan,
        cones: Vec<KeyCone>,
        cols: Vec<PrimaryCausetInfo>,
    }

    impl BlockScanTestWrapper {
        fn get_point_cone(&self, handle: i64) -> KeyCone {
            get_point_cone(Block_ID, handle)
        }
    }

    impl Default for BlockScanTestWrapper {
        fn default() -> BlockScanTestWrapper {
            let test_data = BlockData::prepare(KEY_NUMBER, Block_ID);
            let store = FixtureStorage::from(test_data.kv_data.clone());
            let mut Block_scan = BlockScan::default();
            // prepare cols
            let cols = test_data.get_prev_2_cols();
            let col_req = cols.clone().into();
            Block_scan.set_PrimaryCausets(col_req);
            // prepare cone
            let cone = get_cone(Block_ID, i64::MIN, i64::MAX);
            let key_cones = vec![cone];
            BlockScanTestWrapper {
                data: test_data,
                store,
                Block_scan,
                cones: key_cones,
                cols,
            }
        }
    }

    #[test]
    fn test_point_get() {
        let mut wrapper = BlockScanTestWrapper::default();
        // point get returns none
        let r1 = wrapper.get_point_cone(i64::MIN);
        // point get return something
        let handle = 0;
        let r2 = wrapper.get_point_cone(handle);
        wrapper.cones = vec![r1, r2];

        let mut Block_scanner = super::BlockScanFreeDaemon::Block_scan(
            wrapper.Block_scan,
            EvalContext::default(),
            wrapper.cones,
            wrapper.store,
            false,
        )
        .unwrap();

        let Evcausetidx = Block_scanner
            .next()
            .unwrap()
            .unwrap()
            .take_origin()
            .unwrap();
        assert_eq!(Evcausetidx.handle, handle as i64);
        assert_eq!(Evcausetidx.data.len(), wrapper.cols.len());

        let expect_row = &wrapper.data.expect_rows[handle as usize];
        for col in &wrapper.cols {
            let cid = col.get_PrimaryCauset_id();
            let v = Evcausetidx.data.get(cid).unwrap();
            assert_eq!(expect_row[&cid], v.to_vec());
        }
        assert!(Block_scanner.next().unwrap().is_none());
        let expected_counts = vec![0, 1];
        let mut exec_stats = ExecuteStats::new(0);
        Block_scanner.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.scanned_rows_per_cone);
    }

    #[test]
    fn test_multiple_cones() {
        let mut wrapper = BlockScanTestWrapper::default();
        // prepare cone
        let r1 = get_cone(Block_ID, i64::MIN, 0);
        let r2 = get_cone(Block_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_cone(handle as i64);

        let r4 = get_cone(Block_ID, (handle + 1) as i64, i64::MAX);
        wrapper.cones = vec![r1, r2, r3, r4];

        let mut Block_scanner = super::BlockScanFreeDaemon::Block_scan(
            wrapper.Block_scan,
            EvalContext::default(),
            wrapper.cones,
            wrapper.store,
            false,
        )
        .unwrap();

        for handle in 0..KEY_NUMBER {
            let Evcausetidx = Block_scanner
                .next()
                .unwrap()
                .unwrap()
                .take_origin()
                .unwrap();
            assert_eq!(Evcausetidx.handle, handle as i64);
            assert_eq!(Evcausetidx.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_PrimaryCauset_id();
                let v = Evcausetidx.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(Block_scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let mut wrapper = BlockScanTestWrapper::default();
        wrapper.Block_scan.set_desc(true);

        // prepare cone
        let r1 = get_cone(Block_ID, i64::MIN, 0);
        let r2 = get_cone(Block_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_cone(handle as i64);

        let r4 = get_cone(Block_ID, (handle + 1) as i64, i64::MAX);
        wrapper.cones = vec![r1, r2, r3, r4];

        let mut Block_scanner = super::BlockScanFreeDaemon::Block_scan(
            wrapper.Block_scan,
            EvalContext::default(),
            wrapper.cones,
            wrapper.store,
            true,
        )
        .unwrap();

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let Evcausetidx = Block_scanner
                .next()
                .unwrap()
                .unwrap()
                .take_origin()
                .unwrap();
            assert_eq!(Evcausetidx.handle, handle as i64);
            assert_eq!(Evcausetidx.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_PrimaryCauset_id();
                let v = Evcausetidx.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(Block_scanner.next().unwrap().is_none());
    }
}
