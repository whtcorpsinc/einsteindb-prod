// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use criterion::measurement::Measurement;

use ekvproto::interlock::KeyCone;
use fidel_timeshare::PrimaryCausetInfo;

use test_interlock::*;
use milevadb_query_normal_executors::FreeDaemon;
use milevadb_query_vec_executors::interface::*;
use edb::interlock::RequestHandler;
use edb::causet_storage::{LmdbEngine, CausetStore as TxnStore};

use crate::util::bencher::Bencher;
use crate::util::store::StoreDescriber;

pub trait ScanFreeDaemonBuilder: 'static {
    type T: TxnStore + 'static;
    type E;
    type P: Copy + 'static;
    fn build(
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        parameters: Self::P,
    ) -> Self::E;
}

pub trait ScanFreeDaemonDAGHandlerBuilder: 'static {
    type T: TxnStore + 'static;
    type P: Copy + 'static;
    fn build(
        batch: bool,
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        parameters: Self::P,
    ) -> Box<dyn RequestHandler>;
}

/// Benchers shared for Block scan and index scan.
pub trait ScanBencher<P, M>: 'static
where
    P: Copy + 'static,
    M: Measurement,
{
    fn name(&self) -> String;

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        parameters: P,
    );

    fn box_clone(&self) -> Box<dyn ScanBencher<P, M>>;
}

impl<P, M> Clone for Box<dyn ScanBencher<P, M>>
where
    P: Copy + 'static,
    M: Measurement + 'static,
{
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

pub struct NormalScanNext1Bencher<B>
where
    B: ScanFreeDaemonBuilder,
    B::E: FreeDaemon,
{
    _phantom: PhantomData<B>,
}

impl<B> NormalScanNext1Bencher<B>
where
    B: ScanFreeDaemonBuilder,
    B::E: FreeDaemon,
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<B, M> ScanBencher<B::P, M> for NormalScanNext1Bencher<B>
where
    B: ScanFreeDaemonBuilder,
    B::E: FreeDaemon,
    M: Measurement,
{
    fn name(&self) -> String {
        format!("{}/normal/next=1", <B::T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        parameters: B::P,
    ) {
        crate::util::bencher::NormalNext1Bencher::new(|| {
            B::build(PrimaryCausets, cones, store, parameters)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn ScanBencher<B::P, M>> {
        Box::new(Self::new())
    }
}

pub struct NormalScanNext1024Bencher<B>
where
    B: ScanFreeDaemonBuilder,
    B::E: FreeDaemon,
{
    _phantom: PhantomData<B>,
}

impl<B> NormalScanNext1024Bencher<B>
where
    B: ScanFreeDaemonBuilder,
    B::E: FreeDaemon,
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<B, M> ScanBencher<B::P, M> for NormalScanNext1024Bencher<B>
where
    B: ScanFreeDaemonBuilder,
    B::E: FreeDaemon,
    M: Measurement,
{
    fn name(&self) -> String {
        format!("{}/normal/next=1024", <B::T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        parameters: B::P,
    ) {
        crate::util::bencher::NormalNext1024Bencher::new(|| {
            B::build(PrimaryCausets, cones, store, parameters)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn ScanBencher<B::P, M>> {
        Box::new(Self::new())
    }
}

pub struct BatchScanNext1024Bencher<B>
where
    B: ScanFreeDaemonBuilder,
    B::E: BatchFreeDaemon,
{
    _phantom: PhantomData<B>,
}

impl<B> BatchScanNext1024Bencher<B>
where
    B: ScanFreeDaemonBuilder,
    B::E: BatchFreeDaemon,
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<B, M> ScanBencher<B::P, M> for BatchScanNext1024Bencher<B>
where
    B: ScanFreeDaemonBuilder,
    B::E: BatchFreeDaemon,
    M: Measurement,
{
    fn name(&self) -> String {
        format!("{}/batch/next=1024", <B::T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        parameters: B::P,
    ) {
        crate::util::bencher::BatchNext1024Bencher::new(|| {
            B::build(PrimaryCausets, cones, store, parameters)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn ScanBencher<B::P, M>> {
        Box::new(Self::new())
    }
}

pub struct ScanDAGBencher<B: ScanFreeDaemonDAGHandlerBuilder> {
    batch: bool,
    display_Block_rows: usize,
    _phantom: PhantomData<B>,
}

impl<B: ScanFreeDaemonDAGHandlerBuilder> ScanDAGBencher<B> {
    pub fn new(batch: bool, display_Block_rows: usize) -> Self {
        Self {
            batch,
            display_Block_rows,
            _phantom: PhantomData,
        }
    }
}

impl<B, M> ScanBencher<B::P, M> for ScanDAGBencher<B>
where
    B: ScanFreeDaemonDAGHandlerBuilder,
    M: Measurement,
{
    fn name(&self) -> String {
        let tag = if self.batch { "batch" } else { "normal" };
        format!(
            "{}/{}/with_dag/events={}",
            <B::T as StoreDescriber>::name(),
            tag,
            self.display_Block_rows
        )
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        PrimaryCausets: &[PrimaryCausetInfo],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
        parameters: B::P,
    ) {
        crate::util::bencher::DAGHandleBencher::new(|| {
            B::build(self.batch, PrimaryCausets, cones, store, parameters)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn ScanBencher<B::P, M>> {
        Box::new(Self::new(self.batch, self.display_Block_rows))
    }
}
