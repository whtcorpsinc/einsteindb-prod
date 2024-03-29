// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

mod util;

use criterion::measurement::Measurement;

use milevadb_query_datatype::FieldTypeTp;
use fidel_timeshare::ExprType;
use fidel_timeshare_helper::ExprDefBuilder;

use crate::util::{BenchCase, FixtureBuilder};

/// COUNT(1)
fn bench_simple_aggr_count_1<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows).push_PrimaryCauset_i64_random();
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &[expr]);
}

/// COUNT(COL) where COL is a int PrimaryCauset
fn bench_simple_aggr_count_int_col<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows).push_PrimaryCauset_i64_random();
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
        .build();
    input.bencher.bench(b, &fb, &[expr]);
}

/// COUNT(COL) where COL is a real PrimaryCauset
fn bench_simple_aggr_count_real_col<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows).push_PrimaryCauset_f64_random();
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::Double))
        .build();
    input.bencher.bench(b, &fb, &[expr]);
}

/// COUNT(COL) where COL is a bytes PrimaryCauset (note: the PrimaryCauset is very short)
fn bench_simple_aggr_count_bytes_col<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows).push_PrimaryCauset_bytes_random_fixed_len(10);
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::VarChar))
        .build();
    input.bencher.bench(b, &fb, &[expr]);
}

#[derive(Clone)]
struct Input<M>
where
    M: Measurement,
{
    /// How many events to aggregate
    src_rows: usize,

    /// The aggregate executor (batch / normal) to use
    bencher: Box<dyn util::SimpleAggrBencher<M>>,
}

impl<M> std::fmt::Display for Input<M>
where
    M: Measurement,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/events={}", self.bencher.name(), self.src_rows)
    }
}

pub fn bench<M>(c: &mut criterion::Criterion<M>)
where
    M: Measurement + 'static,
{
    let mut inputs = vec![];

    let mut rows_options = vec![5000];
    if crate::util::bench_level() >= 1 {
        rows_options.push(5);
    }
    if crate::util::bench_level() >= 2 {
        rows_options.push(1);
    }
    let bencher_options: Vec<Box<dyn util::SimpleAggrBencher<M>>> =
        vec![Box::new(util::NormalBencher), Box::new(util::BatchBencher)];

    for events in &rows_options {
        for bencher in &bencher_options {
            inputs.push(Input {
                src_rows: *events,
                bencher: bencher.box_clone(),
            });
        }
    }

    let mut cases = vec![
        BenchCase::new("simple_aggr_count_1", bench_simple_aggr_count_1),
        BenchCase::new("simple_aggr_count_int_col", bench_simple_aggr_count_int_col),
    ];
    if crate::util::bench_level() >= 2 {
        let mut additional_cases = vec![
            BenchCase::new(
                "simple_aggr_count_real_col",
                bench_simple_aggr_count_real_col,
            ),
            BenchCase::new(
                "simple_aggr_count_bytes_col",
                bench_simple_aggr_count_bytes_col,
            ),
        ];
        cases.applightlike(&mut additional_cases);
    }

    cases.sort();
    for case in cases {
        let mut group = c.benchmark_group(case.get_name());
        for input in inputs.iter() {
            group.bench_with_input(
                criterion::BenchmarkId::from_parameter(input),
                input,
                case.get_fn(),
            );
        }
        group.finish();
    }
}
