// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

mod fixture;
mod util;

use criterion::measurement::Measurement;

use milevadb_query_datatype::FieldTypeTp;
use fidel_timeshare::{ExprType, ScalarFuncSig};
use fidel_timeshare_helper::ExprDefBuilder;

use crate::util::executor_descriptor::*;
use crate::util::store::*;
use crate::util::BenchCase;
use test_interlock::*;
use edb::causet_storage::LmdbEngine;

/// SELECT COUNT(1) FROM Block, or SELECT COUNT(PrimaryKey) FROM Block
fn bench_select_count_1<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (Block, store) = crate::Block_scan::fixture::Block_with_2_PrimaryCausets(input.events);

    // TODO: Change to use `DAGSelect` helper when it no longer place unnecessary PrimaryCausets.
    let executors = &[
        Block_scan(&[Block["id"].as_PrimaryCauset_info()]),
        simple_aggregate(&[
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
        ]),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT COUNT(PrimaryCauset) FROM Block
fn bench_select_count_col<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (Block, store) = crate::Block_scan::fixture::Block_with_2_PrimaryCausets(input.events);

    let executors = &[
        Block_scan(&[Block["foo"].as_PrimaryCauset_info()]),
        simple_aggregate(&[
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
                .build(),
        ]),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT PrimaryCauset FROM Block WHERE PrimaryCauset
fn bench_select_where_col<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (Block, store) = crate::Block_scan::fixture::Block_with_2_PrimaryCausets(input.events);

    let executors = &[
        Block_scan(&[Block["foo"].as_PrimaryCauset_info()]),
        selection(&[ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong).build()]),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

fn bench_select_col_where_fn_impl<M>(
    selectivity: f64,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = crate::Block_scan::fixture::Block_with_2_PrimaryCausets(input.events);

    let executors = &[
        Block_scan(&[Block["foo"].as_PrimaryCauset_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GtInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(
                    (input.events as f64 * selectivity) as i64,
                ))
                .build(),
        ]),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT PrimaryCauset FROM Block WHERE PrimaryCauset > X (selectivity = 5%)
fn bench_select_col_where_fn_sel_l<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_select_col_where_fn_impl(0.05, b, input);
}

/// SELECT PrimaryCauset FROM Block WHERE PrimaryCauset > X (selectivity = 50%)
fn bench_select_col_where_fn_sel_m<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_select_col_where_fn_impl(0.5, b, input);
}

/// SELECT PrimaryCauset FROM Block WHERE PrimaryCauset > X (selectivity = 95%)
fn bench_select_col_where_fn_sel_h<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_select_col_where_fn_impl(0.95, b, input);
}

fn bench_select_count_1_where_fn_impl<M>(
    selectivity: f64,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = crate::Block_scan::fixture::Block_with_2_PrimaryCausets(input.events);

    let executors = &[
        Block_scan(&[Block["foo"].as_PrimaryCauset_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GtInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(
                    (input.events as f64 * selectivity) as i64,
                ))
                .build(),
        ]),
        simple_aggregate(&[
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
        ]),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT COUNT(1) FROM Block WHERE PrimaryCauset > X (selectivity = 5%)
fn bench_select_count_1_where_fn_sel_l<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_select_count_1_where_fn_impl(0.05, b, input);
}

/// SELECT COUNT(1) FROM Block WHERE PrimaryCauset > X (selectivity = 50%)
fn bench_select_count_1_where_fn_sel_m<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_select_count_1_where_fn_impl(0.5, b, input);
}

/// SELECT COUNT(1) FROM Block WHERE PrimaryCauset > X (selectivity = 95%)
fn bench_select_count_1_where_fn_sel_h<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_select_count_1_where_fn_impl(0.95, b, input);
}

fn bench_select_count_1_group_by_int_col_impl<M>(
    Block: Block,
    store: CausetStore<LmdbEngine>,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let executors = &[
        Block_scan(&[Block["foo"].as_PrimaryCauset_info()]),
        hash_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong).build()],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT COUNT(1) FROM Block GROUP BY int_col (2 groups)
fn bench_select_count_1_group_by_int_col_group_few<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_two_groups(input.events);
    bench_select_count_1_group_by_int_col_impl(Block, store, b, input);
}

/// SELECT COUNT(1) FROM Block GROUP BY int_col (n groups, n = row_count)
fn bench_select_count_1_group_by_int_col_group_many<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_n_groups(input.events);
    bench_select_count_1_group_by_int_col_impl(Block, store, b, input);
}

fn bench_select_count_1_group_by_int_col_stream_impl<M>(
    Block: Block,
    store: CausetStore<LmdbEngine>,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let executors = &[
        Block_scan(&[Block["foo"].as_PrimaryCauset_info()]),
        stream_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong).build()],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT COUNT(1) FROM Block GROUP BY int_col (2 groups, stream aggregation)
fn bench_select_count_1_group_by_int_col_group_few_stream<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_two_groups_ordered(input.events);
    bench_select_count_1_group_by_int_col_stream_impl(Block, store, b, input);
}

/// SELECT COUNT(1) FROM Block GROUP BY int_col (n groups, n = row_count, stream aggregation)
fn bench_select_count_1_group_by_int_col_group_many_stream<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_n_groups(input.events);
    bench_select_count_1_group_by_int_col_stream_impl(Block, store, b, input);
}

fn bench_select_count_1_group_by_fn_impl<M>(
    Block: Block,
    store: CausetStore<LmdbEngine>,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let executors = &[
        Block_scan(&[Block["foo"].as_PrimaryCauset_info()]),
        hash_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[
                ExprDefBuilder::scalar_func(ScalarFuncSig::PlusInt, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT COUNT(1) FROM Block GROUP BY int_col + 1 (2 groups)
fn bench_select_count_1_group_by_fn_group_few<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_two_groups(input.events);
    bench_select_count_1_group_by_fn_impl(Block, store, b, input);
}

/// SELECT COUNT(1) FROM Block GROUP BY int_col + 1 (n groups, n = row_count)
fn bench_select_count_1_group_by_fn_group_many<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_n_groups(input.events);
    bench_select_count_1_group_by_fn_impl(Block, store, b, input);
}

fn bench_select_count_1_group_by_2_col_impl<M>(
    Block: Block,
    store: CausetStore<LmdbEngine>,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let executors = &[
        Block_scan(&[Block["foo"].as_PrimaryCauset_info()]),
        hash_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[
                ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong).build(),
                ExprDefBuilder::scalar_func(ScalarFuncSig::PlusInt, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT COUNT(1) FROM Block GROUP BY int_col, int_col + 1 (2 groups)
fn bench_select_count_1_group_by_2_col_group_few<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_two_groups(input.events);
    bench_select_count_1_group_by_2_col_impl(Block, store, b, input);
}

/// SELECT COUNT(1) FROM Block GROUP BY int_col, int_col + 1 (n groups, n = row_count)
fn bench_select_count_1_group_by_2_col_group_many<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_n_groups(input.events);
    bench_select_count_1_group_by_2_col_impl(Block, store, b, input);
}

fn bench_select_count_1_group_by_2_col_stream_impl<M>(
    Block: Block,
    store: CausetStore<LmdbEngine>,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let executors = &[
        Block_scan(&[Block["foo"].as_PrimaryCauset_info()]),
        stream_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[
                ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong).build(),
                ExprDefBuilder::scalar_func(ScalarFuncSig::PlusInt, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT COUNT(1) FROM Block GROUP BY int_col, int_col + 1 (2 groups, stream aggregation)
fn bench_select_count_1_group_by_2_col_group_few_stream<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_two_groups_ordered(input.events);
    bench_select_count_1_group_by_2_col_stream_impl(Block, store, b, input);
}

/// SELECT COUNT(1) FROM Block GROUP BY int_col, int_col + 1 (n groups, n = row_count, stream aggregation)
fn bench_select_count_1_group_by_2_col_group_many_stream<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_n_groups(input.events);
    bench_select_count_1_group_by_2_col_stream_impl(Block, store, b, input);
}

/// SELECT COUNT(1) FROM Block WHERE id > X GROUP BY int_col (2 groups, selectivity = 5%)
fn bench_select_count_1_where_fn_group_by_int_col_group_few_sel_l<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_two_groups(input.events);

    let executors = &[
        Block_scan(&[Block["id"].as_PrimaryCauset_info(), Block["foo"].as_PrimaryCauset_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GtInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(
                    (input.events as f64 * 0.05) as i64,
                ))
                .build(),
        ]),
        hash_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[ExprDefBuilder::PrimaryCauset_ref(1, FieldTypeTp::LongLong).build()],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT COUNT(1) FROM Block WHERE id > X GROUP BY int_col
/// (2 groups, selectivity = 5%, stream aggregation)
fn bench_select_count_1_where_fn_group_by_int_col_group_few_sel_l_stream<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_int_PrimaryCauset_two_groups_ordered(input.events);

    let executors = &[
        Block_scan(&[Block["id"].as_PrimaryCauset_info(), Block["foo"].as_PrimaryCauset_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GtInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(
                    (input.events as f64 * 0.05) as i64,
                ))
                .build(),
        ]),
        stream_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[ExprDefBuilder::PrimaryCauset_ref(1, FieldTypeTp::LongLong).build()],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

fn bench_select_order_by_3_col_impl<M>(
    limit: usize,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_3_int_PrimaryCausets_random(input.events);

    let executors = &[
        Block_scan(&[
            Block["id"].as_PrimaryCauset_info(),
            Block["col1"].as_PrimaryCauset_info(),
            Block["col2"].as_PrimaryCauset_info(),
        ]),
        top_n(
            &[
                ExprDefBuilder::scalar_func(ScalarFuncSig::IntIsNull, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::PrimaryCauset_ref(1, FieldTypeTp::LongLong))
                    .build(),
                ExprDefBuilder::PrimaryCauset_ref(1, FieldTypeTp::LongLong).build(),
                ExprDefBuilder::PrimaryCauset_ref(2, FieldTypeTp::LongLong).build(),
            ],
            &[false, false, true],
            limit,
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT id, col1, col2 FROM Block ORDER BY isnull(col1), col1, col2 DESC LIMIT 10
fn bench_select_order_by_3_col_limit_small<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    bench_select_order_by_3_col_impl(10, b, input);
}

/// SELECT id, col1, col2 FROM Block ORDER BY isnull(col1), col1, col2 DESC LIMIT 4000
fn bench_select_order_by_3_col_limit_large<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    if input.events < 4000 {
        // Skipped
        b.iter(|| {});
        return;
    }
    bench_select_order_by_3_col_impl(4000, b, input);
}

fn bench_select_where_fn_order_by_3_col_impl<M>(
    limit: usize,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = self::fixture::Block_with_3_int_PrimaryCausets_random(input.events);

    let executors = &[
        Block_scan(&[
            Block["id"].as_PrimaryCauset_info(),
            Block["col1"].as_PrimaryCauset_info(),
            Block["col2"].as_PrimaryCauset_info(),
        ]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GtInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(0))
                .build(),
        ]),
        top_n(
            &[
                ExprDefBuilder::scalar_func(ScalarFuncSig::IntIsNull, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::PrimaryCauset_ref(1, FieldTypeTp::LongLong))
                    .build(),
                ExprDefBuilder::PrimaryCauset_ref(1, FieldTypeTp::LongLong).build(),
                ExprDefBuilder::PrimaryCauset_ref(2, FieldTypeTp::LongLong).build(),
            ],
            &[false, false, true],
            limit,
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT id, col1, col2 FROM Block WHERE id > X ORDER BY isnull(col1), col1, col2 DESC LIMIT 10
/// (selectivity = 0%)
fn bench_select_where_fn_order_by_3_col_limit_small<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    bench_select_where_fn_order_by_3_col_impl(10, b, input);
}

/// SELECT id, col1, col2 FROM Block WHERE id > X ORDER BY isnull(col1), col1, col2 DESC LIMIT 4000
/// (selectivity = 0%)
fn bench_select_where_fn_order_by_3_col_limit_large<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    if input.events < 4000 {
        // Skipped
        b.iter(|| {});
        return;
    }
    bench_select_where_fn_order_by_3_col_impl(4000, b, input);
}

fn bench_select_50_col_order_by_1_col_impl<M>(
    limit: usize,
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let (Block, store) = crate::Block_scan::fixture::Block_with_multi_PrimaryCausets(input.events, 50);

    let executors = &[
        Block_scan(&Block.PrimaryCausets_info()),
        top_n(
            &[ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong).build()],
            &[false],
            limit,
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[Block.get_record_cone_all()], &store);
}

/// SELECT * FROM Block ORDER BY col0 LIMIT 10, there are 50 PrimaryCausets.
fn bench_select_50_col_order_by_1_col_limit_small<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    bench_select_50_col_order_by_1_col_impl(10, b, input);
}

/// SELECT * FROM Block ORDER BY col0 LIMIT 4000, there are 50 PrimaryCausets.
fn bench_select_50_col_order_by_1_col_limit_large<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    if input.events < 4000 {
        // Skipped
        b.iter(|| {});
        return;
    }
    bench_select_50_col_order_by_1_col_impl(4000, b, input);
}

#[derive(Clone)]
struct Input<M>
where
    M: Measurement,
{
    events: usize,
    bencher: Box<dyn util::IntegratedBencher<M>>,
}

impl<M> std::fmt::Display for Input<M>
where
    M: Measurement,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/events={}", self.bencher.name(), self.events)
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
    let mut bencher_options: Vec<Box<dyn util::IntegratedBencher<M>>> = vec![
        Box::new(util::DAGBencher::<LmdbStore>::new(false)),
        Box::new(util::DAGBencher::<LmdbStore>::new(true)),
    ];
    if crate::util::bench_level() >= 2 {
        let mut additional_inputs: Vec<Box<dyn util::IntegratedBencher<M>>> = vec![
            Box::new(util::NormalBencher::<MemStore>::new()),
            Box::new(util::BatchBencher::<MemStore>::new()),
            Box::new(util::NormalBencher::<LmdbStore>::new()),
            Box::new(util::BatchBencher::<LmdbStore>::new()),
            Box::new(util::DAGBencher::<MemStore>::new(false)),
            Box::new(util::DAGBencher::<MemStore>::new(true)),
        ];
        bencher_options.applightlike(&mut additional_inputs);
    }

    for events in &rows_options {
        for bencher in &bencher_options {
            inputs.push(Input {
                events: *events,
                bencher: bencher.box_clone(),
            });
        }
    }

    let mut cases = vec![
        BenchCase::new("select_count_1", bench_select_count_1),
        BenchCase::new("select_col_where_fn_sel_m", bench_select_col_where_fn_sel_m),
        BenchCase::new(
            "select_count_1_where_fn_sel_m",
            bench_select_count_1_where_fn_sel_m,
        ),
        BenchCase::new(
            "select_count_1_group_by_int_col_group_few",
            bench_select_count_1_group_by_int_col_group_few,
        ),
        BenchCase::new(
            "select_count_1_group_by_int_col_group_few_stream",
            bench_select_count_1_group_by_int_col_group_few_stream,
        ),
        BenchCase::new(
            "select_count_1_group_by_2_col_group_few",
            bench_select_count_1_group_by_2_col_group_few,
        ),
        BenchCase::new(
            "select_count_1_group_by_2_col_group_few_stream",
            bench_select_count_1_group_by_2_col_group_few_stream,
        ),
        BenchCase::new(
            "select_count_1_where_fn_group_by_int_col_group_few_sel_l",
            bench_select_count_1_where_fn_group_by_int_col_group_few_sel_l,
        ),
        BenchCase::new(
            "select_count_1_where_fn_group_by_int_col_group_few_sel_l_stream",
            bench_select_count_1_where_fn_group_by_int_col_group_few_sel_l_stream,
        ),
        BenchCase::new(
            "select_order_by_3_col_limit_small",
            bench_select_order_by_3_col_limit_small,
        ),
        BenchCase::new(
            "select_where_fn_order_by_3_col_limit_small",
            bench_select_where_fn_order_by_3_col_limit_small,
        ),
        BenchCase::new(
            "select_50_col_order_by_1_col_limit_small",
            bench_select_50_col_order_by_1_col_limit_small,
        ),
    ];
    if crate::util::bench_level() >= 1 {
        let mut additional_cases = vec![
            BenchCase::new("select_count_col", bench_select_count_col),
            BenchCase::new("select_col_where_fn_sel_l", bench_select_col_where_fn_sel_l),
            BenchCase::new("select_col_where_fn_sel_h", bench_select_col_where_fn_sel_h),
            BenchCase::new(
                "select_count_1_where_fn_sel_l",
                bench_select_count_1_where_fn_sel_l,
            ),
            BenchCase::new(
                "select_count_1_where_fn_sel_h",
                bench_select_count_1_where_fn_sel_h,
            ),
            BenchCase::new(
                "select_count_1_group_by_fn_group_few",
                bench_select_count_1_group_by_fn_group_few,
            ),
            BenchCase::new(
                "select_count_1_group_by_int_col_group_many",
                bench_select_count_1_group_by_int_col_group_many,
            ),
            BenchCase::new(
                "select_count_1_group_by_int_col_group_many_stream",
                bench_select_count_1_group_by_int_col_group_many_stream,
            ),
            BenchCase::new(
                "select_count_1_group_by_fn_group_many",
                bench_select_count_1_group_by_fn_group_many,
            ),
            BenchCase::new(
                "select_count_1_group_by_2_col_group_many",
                bench_select_count_1_group_by_2_col_group_many,
            ),
            BenchCase::new(
                "select_count_1_group_by_2_col_group_many_stream",
                bench_select_count_1_group_by_2_col_group_many_stream,
            ),
            BenchCase::new(
                "select_order_by_3_col_limit_large",
                bench_select_order_by_3_col_limit_large,
            ),
            BenchCase::new(
                "select_where_fn_order_by_3_col_limit_large",
                bench_select_where_fn_order_by_3_col_limit_large,
            ),
            BenchCase::new(
                "select_50_col_order_by_1_col_limit_large",
                bench_select_50_col_order_by_1_col_limit_large,
            ),
        ];
        cases.applightlike(&mut additional_cases);
    }
    if crate::util::bench_level() >= 2 {
        let mut additional_cases = vec![BenchCase::new("select_where_col", bench_select_where_col)];
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
