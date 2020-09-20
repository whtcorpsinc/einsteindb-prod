// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use criterion::black_box;

use ekvproto::interlock::KeyCone;
use fidelpb::PrimaryCausetInfo;
use fidelpb::TableScan;

use test_interlock::*;
use milevadb_query_datatype::expr::{EvalConfig, EvalContext};
use milevadb_query_normal_executors::FreeDaemon;
use milevadb_query_normal_executors::TableScanFreeDaemon;
use milevadb_query_vec_executors::interface::*;
use milevadb_query_vec_executors::BatchTableScanFreeDaemon;
use einsteindb::interlock::dag::EinsteinDBStorage;
use einsteindb::interlock::RequestHandler;
use einsteindb::causetStorage::{LmdbEngine, Statistics, CausetStore as TxnStore};

use crate::util::executor_descriptor::table_scan;
use crate::util::scan_bencher;

pub type TableScanParam = ();

pub struct NormalTableScanFreeDaemonBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanFreeDaemonBuilder
    for NormalTableScanFreeDaemonBuilder<T>
{
    type T = T;
    type E = Box<dyn FreeDaemon<StorageStats = Statistics>>;
    type P = TableScanParam;

    fn build(
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        _: (),
    ) -> Self::E {
        let mut req = TableScan::default();
        req.set_PrimaryCausets(PrimaryCausets.into());

        let mut executor = TableScanFreeDaemon::table_scan(
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

pub struct BatchTableScanFreeDaemonBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanFreeDaemonBuilder for BatchTableScanFreeDaemonBuilder<T> {
    type T = T;
    type E = Box<dyn BatchFreeDaemon<StorageStats = Statistics>>;
    type P = TableScanParam;

    fn build(
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        _: (),
    ) -> Self::E {
        let mut executor = BatchTableScanFreeDaemon::new(
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

pub struct TableScanFreeDaemonDAGBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanFreeDaemonDAGHandlerBuilder
    for TableScanFreeDaemonDAGBuilder<T>
{
    type T = T;
    type P = TableScanParam;

    fn build(
        _batch: bool,
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        _: (),
    ) -> Box<dyn RequestHandler> {
        let exec = table_scan(PrimaryCausets);
        crate::util::build_dag_handler::<T>(&[exec], cones, store)
    }
}

pub type NormalTableScanNext1Bencher<T> =
    scan_bencher::NormalScanNext1Bencher<NormalTableScanFreeDaemonBuilder<T>>;
pub type NormalTableScanNext1024Bencher<T> =
    scan_bencher::NormalScanNext1024Bencher<NormalTableScanFreeDaemonBuilder<T>>;
pub type BatchTableScanNext1024Bencher<T> =
    scan_bencher::BatchScanNext1024Bencher<BatchTableScanFreeDaemonBuilder<T>>;
pub type TableScanDAGBencher<T> = scan_bencher::ScanDAGBencher<TableScanFreeDaemonDAGBuilder<T>>;
