// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use criterion::black_box;

use ekvproto::interlock::KeyCone;
use fidel_timeshare::PrimaryCausetInfo;
use fidel_timeshare::BlockScan;

use test_interlock::*;
use milevadb_query_datatype::expr::{EvalConfig, EvalContext};
use milevadb_query_normal_executors::FreeDaemon;
use milevadb_query_normal_executors::BlockScanFreeDaemon;
use milevadb_query_vec_executors::interface::*;
use milevadb_query_vec_executors::BatchBlockScanFreeDaemon;
use edb::interlock::posetdag::EinsteinDBStorage;
use edb::interlock::RequestHandler;
use edb::causet_storage::{LmdbEngine, Statistics, CausetStore as TxnStore};

use crate::util::executor_descriptor::Block_scan;
use crate::util::scan_bencher;

pub type BlockScanParam = ();

pub struct NormalBlockScanFreeDaemonBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanFreeDaemonBuilder
    for NormalBlockScanFreeDaemonBuilder<T>
{
    type T = T;
    type E = Box<dyn FreeDaemon<StorageStats = Statistics>>;
    type P = BlockScanParam;

    fn build(
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        _: (),
    ) -> Self::E {
        let mut req = BlockScan::default();
        req.set_PrimaryCausets(PrimaryCausets.into());

        let mut executor = BlockScanFreeDaemon::Block_scan(
            black_box(req),
            black_box(EvalContext::default()),
            black_box(cones.to_vec()),
            black_box(EinsteinDBStorage::new(
                ToTxnStore::<Self::T>::to_store(store),
                false,
            )),
            black_box(false),
        )
        .unwrap();
        // There is a step of building scanner in the first `next()` which cost time,
        // so we next() before hand.
        executor.next().unwrap().unwrap();
        Box::new(executor) as Box<dyn FreeDaemon<StorageStats = Statistics>>
    }
}

pub struct BatchBlockScanFreeDaemonBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanFreeDaemonBuilder for BatchBlockScanFreeDaemonBuilder<T> {
    type T = T;
    type E = Box<dyn BatchFreeDaemon<StorageStats = Statistics>>;
    type P = BlockScanParam;

    fn build(
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        _: (),
    ) -> Self::E {
        let mut executor = BatchBlockScanFreeDaemon::new(
            black_box(EinsteinDBStorage::new(
                ToTxnStore::<Self::T>::to_store(store),
                false,
            )),
            black_box(Arc::new(EvalConfig::default())),
            black_box(PrimaryCausets.to_vec()),
            black_box(cones.to_vec()),
            black_box(vec![]),
            black_box(false),
            black_box(false),
        )
        .unwrap();
        // There is a step of building scanner in the first `next()` which cost time,
        // so we next() before hand.
        executor.next_batch(1);
        Box::new(executor) as Box<dyn BatchFreeDaemon<StorageStats = Statistics>>
    }
}

pub struct BlockScanFreeDaemonDAGBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanFreeDaemonDAGHandlerBuilder
    for BlockScanFreeDaemonDAGBuilder<T>
{
    type T = T;
    type P = BlockScanParam;

    fn build(
        _batch: bool,
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        _: (),
    ) -> Box<dyn RequestHandler> {
        let exec = Block_scan(PrimaryCausets);
        crate::util::build_dag_handler::<T>(&[exec], cones, store)
    }
}

pub type NormalBlockScanNext1Bencher<T> =
    scan_bencher::NormalScanNext1Bencher<NormalBlockScanFreeDaemonBuilder<T>>;
pub type NormalBlockScanNext1024Bencher<T> =
    scan_bencher::NormalScanNext1024Bencher<NormalBlockScanFreeDaemonBuilder<T>>;
pub type BatchBlockScanNext1024Bencher<T> =
    scan_bencher::BatchScanNext1024Bencher<BatchBlockScanFreeDaemonBuilder<T>>;
pub type BlockScanDAGBencher<T> = scan_bencher::ScanDAGBencher<BlockScanFreeDaemonDAGBuilder<T>>;
