// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

mod util;

use criterion::measurement::Measurement;

use milevadb_query_datatype::FieldTypeTp;
use fidel_timeshare::ScalarFuncSig;
use fidel_timeshare_helper::ExprDefBuilder;

use crate::util::{BenchCase, FixtureBuilder};

fn bench_top_n_1_order_by_impl<M>(
    PrimaryCausets: usize,
    n: usize,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    assert!(PrimaryCausets >= 1);
    assert!(n > 0);
    let mut fb = FixtureBuilder::new(input.src_rows);
    for _ in 0..PrimaryCausets {
        fb = fb.push_PrimaryCauset_i64_random();
    }
    let order_by = vec![ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong).build()];
    input.bencher.bench(b, &fb, &order_by, &[false], n);
}

/// ORDER BY col LIMIT 10. 1 projection field.
fn bench_top_n_1_order_by_1_PrimaryCauset_limit_10<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_top_n_1_order_by_impl(1, 10, b, input);
}

/// ORDER BY col LIMIT 4000. 1 projection field.
fn bench_top_n_1_order_by_1_PrimaryCauset_limit_4000<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_top_n_1_order_by_impl(1, 4000, b, input);
}

/// ORDER BY col LIMIT 10. 50 projection fields.
fn bench_top_n_1_order_by_50_PrimaryCauset_limit_10<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_top_n_1_order_by_impl(50, 10, b, input);
}

/// ORDER BY col LIMIT 4000. 50 projection fields.
fn bench_top_n_1_order_by_50_PrimaryCauset_limit_4000<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_top_n_1_order_by_impl(50, 4000, b, input);
}

fn bench_top_n_3_order_by_impl<M>(
    PrimaryCausets: usize,
    n: usize,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    assert!(PrimaryCausets >= 3);
    assert!(n > 0);
    let mut fb = FixtureBuilder::new(input.src_rows);
    for _ in 0..PrimaryCausets {
        fb = fb.push_PrimaryCauset_i64_random();
    }
    let order_by = vec![
        ExprDefBuilder::scalar_func(ScalarFuncSig::IntIsNull, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
            .build(),
        ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong).build(),
        ExprDefBuilder::PrimaryCauset_ref(1, FieldTypeTp::LongLong).build(),
    ];
    input
        .bencher
        .bench(b, &fb, &order_by, &[false, false, true], n);
}

/// ORDER BY isnull(col0), col0, col1 DESC LIMIT 10. 3 projection fields.
fn bench_top_n_3_order_by_3_PrimaryCauset_limit_10<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_top_n_3_order_by_impl(3, 10, b, input)
}

/// ORDER BY isnull(col0), col0, col1 DESC LIMIT 4000. 3 projection fields.
fn bench_top_n_3_order_by_3_PrimaryCauset_limit_4000<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_top_n_3_order_by_impl(3, 4000, b, input)
}

/// ORDER BY isnull(col0), col0, col1 DESC LIMIT 10. 50 projection fields.
fn bench_top_n_3_order_by_50_PrimaryCauset_limit_10<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_top_n_3_order_by_impl(50, 10, b, input)
}

/// ORDER BY isnull(col0), col0, col1 DESC LIMIT 4000. 50 projection fields.
fn bench_top_n_3_order_by_50_PrimaryCauset_limit_4000<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_top_n_3_order_by_impl(50, 4000, b, input)
}

#[derive(Clone)]
struct Input<M>
where
    M: Measurement,
{
    /// How many events to sort
    src_rows: usize,

    /// The top n executor (batch / normal) to use
    bencher: Box<dyn util::TopNBencher<M>>,
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
    let bencher_options: Vec<Box<dyn util::TopNBencher<M>>> =
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
        BenchCase::new(
            "top_n_3_order_by_3_PrimaryCauset_limit_10",
            bench_top_n_3_order_by_3_PrimaryCauset_limit_10,
        ),
        BenchCase::new(
            "top_n_3_order_by_3_PrimaryCauset_limit_4000",
            bench_top_n_3_order_by_3_PrimaryCauset_limit_4000,
        ),
        BenchCase::new(
            "top_n_3_order_by_50_PrimaryCauset_limit_10",
            bench_top_n_3_order_by_50_PrimaryCauset_limit_10,
        ),
        BenchCase::new(
            "top_n_3_order_by_50_PrimaryCauset_limit_4000",
            bench_top_n_3_order_by_50_PrimaryCauset_limit_4000,
        ),
    ];
    if crate::util::bench_level() >= 1 {
        let mut additional_cases = vec![
            BenchCase::new(
                "top_n_1_order_by_1_PrimaryCauset_limit_10",
                bench_top_n_1_order_by_1_PrimaryCauset_limit_10,
            ),
            BenchCase::new(
                "top_n_1_order_by_1_PrimaryCauset_limit_4000",
                bench_top_n_1_order_by_1_PrimaryCauset_limit_4000,
            ),
            BenchCase::new(
                "top_n_1_order_by_50_PrimaryCauset_limit_10",
                bench_top_n_1_order_by_50_PrimaryCauset_limit_10,
            ),
            BenchCase::new(
                "top_n_1_order_by_50_PrimaryCauset_limit_4000",
                bench_top_n_1_order_by_50_PrimaryCauset_limit_4000,
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
