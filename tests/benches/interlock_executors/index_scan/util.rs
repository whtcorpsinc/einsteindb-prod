// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use criterion::black_box;

use ekvproto::interlock::KeyCone;
use fidel_timeshare::PrimaryCausetInfo;
use fidel_timeshare::IndexScan;

use test_interlock::*;
use milevadb_query_datatype::expr::{EvalConfig, EvalContext};
use milevadb_query_normal_executors::{FreeDaemon, IndexScanFreeDaemon};
use milevadb_query_vec_executors::interface::*;
use milevadb_query_vec_executors::BatchIndexScanFreeDaemon;
use edb::interlock::posetdag::EinsteinDBStorage;
use edb::interlock::RequestHandler;
use edb::causet_storage::{LmdbEngine, Statistics, CausetStore as TxnStore};

use crate::util::executor_descriptor::index_scan;
use crate::util::scan_bencher;

pub type IndexScanParam = bool;

pub struct NormalIndexScanFreeDaemonBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanFreeDaemonBuilder
    for NormalIndexScanFreeDaemonBuilder<T>
{
    type T = T;
    type E = Box<dyn FreeDaemon<StorageStats = Statistics>>;
    type P = IndexScanParam;

    fn build(
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        unique: bool,
    ) -> Self::E {
        let mut req = IndexScan::default();
        req.set_PrimaryCausets(PrimaryCausets.into());

        let mut executor = IndexScanFreeDaemon::index_scan(
            black_box(req),
            black_box(EvalContext::default()),
            black_box(cones.to_vec()),
            // TODO: Change to use `FixtureStorage` directly instead of
            // `EinsteinDBStorage<FixtureStore<..>>`
            black_box(EinsteinDBStorage::new(
                ToTxnStore::<Self::T>::to_store(store),
                false,
            )),
            black_box(unique),
            black_box(false),
        )
        .unwrap();
        // There is a step of building scanner in the first `next()` which cost time,
        // so we next() before hand.
        executor.next().unwrap().unwrap();
        Box::new(executor) as Box<dyn FreeDaemon<StorageStats = Statistics>>
    }
}

pub struct BatchIndexScanFreeDaemonBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanFreeDaemonBuilder for BatchIndexScanFreeDaemonBuilder<T> {
    type T = T;
    type E = Box<dyn BatchFreeDaemon<StorageStats = Statistics>>;
    type P = IndexScanParam;

    fn build(
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        unique: bool,
    ) -> Self::E {
        let mut executor = BatchIndexScanFreeDaemon::new(
            black_box(EinsteinDBStorage::new(
                ToTxnStore::<Self::T>::to_store(store),
                false,
            )),
            black_box(Arc::new(EvalConfig::default())),
            black_box(PrimaryCausets.to_vec()),
            black_box(cones.to_vec()),
            black_box(0),
            black_box(false),
            black_box(unique),
            black_box(false),
        )
        .unwrap();
        // There is a step of building scanner in the first `next()` which cost time,
        // so we next() before hand.
        executor.next_batch(1);
        Box::new(executor) as Box<dyn BatchFreeDaemon<StorageStats = Statistics>>
    }
}

pub struct IndexScanFreeDaemonDAGBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanFreeDaemonDAGHandlerBuilder
    for IndexScanFreeDaemonDAGBuilder<T>
{
    type T = T;
    type P = IndexScanParam;

    fn build(
        _batch: bool,
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        unique: bool,
    ) -> Box<dyn RequestHandler> {
        let exec = index_scan(PrimaryCausets, unique);
        crate::util::build_dag_handler::<T>(&[exec], cones, store)
    }
}

pub type NormalIndexScanNext1Bencher<T> =
    scan_bencher::NormalScanNext1Bencher<NormalIndexScanFreeDaemonBuilder<T>>;
pub type NormalIndexScanNext1024Bencher<T> =
    scan_bencher::NormalScanNext1024Bencher<NormalIndexScanFreeDaemonBuilder<T>>;
pub type BatchIndexScanNext1024Bencher<T> =
    scan_bencher::BatchScanNext1024Bencher<BatchIndexScanFreeDaemonBuilder<T>>;
pub type IndexScanDAGBencher<T> = scan_bencher::ScanDAGBencher<IndexScanFreeDaemonDAGBuilder<T>>;
