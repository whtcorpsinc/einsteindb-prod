// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::black_box;
use criterion::measurement::Measurement;

use fidel_timeshare::Expr;

use milevadb_query_datatype::expr::EvalConfig;
use milevadb_query_normal_executors::{FreeDaemon, StreamAggFreeDaemon};
use milevadb_query_vec_executors::interface::BatchFreeDaemon;
use milevadb_query_vec_executors::BatchStreamAggregationFreeDaemon;
use edb::causet_storage::Statistics;

use crate::util::bencher::Bencher;
use crate::util::executor_descriptor::stream_aggregate;
use crate::util::FixtureBuilder;

pub trait StreamAggrBencher<M>
where
    M: Measurement,
{
    fn name(&self) -> &'static str;

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    );

    fn box_clone(&self) -> Box<dyn StreamAggrBencher<M>>;
}

impl<M> Clone for Box<dyn StreamAggrBencher<M>>
where
    M: Measurement,
{
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use normal stream aggregation executor to bench the giving aggregate
/// expression.
pub struct NormalBencher;

impl<M> StreamAggrBencher<M> for NormalBencher
where
    M: Measurement,
{
    fn name(&self) -> &'static str {
        "normal"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    ) {
        crate::util::bencher::NormalNextAllBencher::new(|| {
            let meta = stream_aggregate(aggr_expr, group_by_expr).take_aggregation();
            let src = fb.clone().build_normal_fixture_executor();
            Box::new(
                StreamAggFreeDaemon::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(meta),
                )
                .unwrap(),
            ) as Box<dyn FreeDaemon<StorageStats = Statistics>>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn StreamAggrBencher<M>> {
        Box::new(Self)
    }
}

/// A bencher that will use batch stream aggregation executor to bench the giving aggregate
/// expression.
pub struct BatchBencher;

impl<M> StreamAggrBencher<M> for BatchBencher
where
    M: Measurement,
{
    fn name(&self) -> &'static str {
        "batch"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    ) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            let src = fb.clone().build_batch_fixture_executor();
            Box::new(
                BatchStreamAggregationFreeDaemon::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(group_by_expr.to_vec()),
                    black_box(aggr_expr.to_vec()),
                )
                .unwrap(),
            ) as Box<dyn BatchFreeDaemon<StorageStats = Statistics>>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn StreamAggrBencher<M>> {
        Box::new(Self)
    }
}
