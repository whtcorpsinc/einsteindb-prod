// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

pub mod fixture;
mod util;

use criterion::measurement::Measurement;

use crate::util::scan_bencher::ScanBencher;
use crate::util::store::*;
use crate::util::BenchCase;

const ROWS: usize = 5000;

/// 1 interested PrimaryCauset, which is PK (which is in the key)
///
/// This kind of scanner is used in SQLs like SELECT COUNT(*).
fn bench_table_scan_primary_key<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_2_PrimaryCausets(ROWS);
    input.0.bench(
        b,
        &[table["id"].as_PrimaryCauset_info()],
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 1 interested PrimaryCauset, at the front of each row. Each row contains 100 PrimaryCausets.
///
/// This kind of scanner is used in SQLs like `SELECT COUNT(PrimaryCauset)`.
fn bench_table_scan_datum_front<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_multi_PrimaryCausets(ROWS, 100);
    input.0.bench(
        b,
        &[table["col0"].as_PrimaryCauset_info()],
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 2 interested PrimaryCausets, at the front of each row. Each row contains 100 PrimaryCausets.
fn bench_table_scan_datum_multi_front<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_multi_PrimaryCausets(ROWS, 100);
    input.0.bench(
        b,
        &[
            table["col0"].as_PrimaryCauset_info(),
            table["col1"].as_PrimaryCauset_info(),
        ],
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 1 interested PrimaryCauset, at the lightlike of each row. Each row contains 100 PrimaryCausets.
fn bench_table_scan_datum_lightlike<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_multi_PrimaryCausets(ROWS, 100);
    input.0.bench(
        b,
        &[table["col99"].as_PrimaryCauset_info()],
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 100 interested PrimaryCausets, all PrimaryCausets in the row are interested (i.e. there are totally 100
/// PrimaryCausets in the row).
fn bench_table_scan_datum_all<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_multi_PrimaryCausets(ROWS, 100);
    input.0.bench(
        b,
        &table.PrimaryCausets_info(),
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 3 PrimaryCausets in the row and the last PrimaryCauset is very long but only PK is interested.
fn bench_table_scan_long_datum_primary_key<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_long_PrimaryCauset(ROWS);
    input.0.bench(
        b,
        &[table["id"].as_PrimaryCauset_info()],
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 3 PrimaryCausets in the row and the last PrimaryCauset is very long but a short PrimaryCauset is interested.
fn bench_table_scan_long_datum_normal<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_long_PrimaryCauset(ROWS);
    input.0.bench(
        b,
        &[table["foo"].as_PrimaryCauset_info()],
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 3 PrimaryCausets in the row and the last PrimaryCauset is very long and the long PrimaryCauset is interested.
fn bench_table_scan_long_datum_long<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_long_PrimaryCauset(ROWS);
    input.0.bench(
        b,
        &[table["bar"].as_PrimaryCauset_info()],
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 3 PrimaryCausets in the row and the last PrimaryCauset is very long and the all PrimaryCausets are interested.
fn bench_table_scan_long_datum_all<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_long_PrimaryCauset(ROWS);
    input.0.bench(
        b,
        &[
            table["id"].as_PrimaryCauset_info(),
            table["foo"].as_PrimaryCauset_info(),
            table["bar"].as_PrimaryCauset_info(),
        ],
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 1 interested PrimaryCauset, but the PrimaryCauset is missing from each row (i.e. it's default value is
/// used instead). Each row contains totally 10 PrimaryCausets.
fn bench_table_scan_datum_absent<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_missing_PrimaryCauset(ROWS, 10);
    input.0.bench(
        b,
        &[table["col0"].as_PrimaryCauset_info()],
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 1 interested PrimaryCauset, but the PrimaryCauset is missing from each row (i.e. it's default value is
/// used instead). Each row contains totally 100 PrimaryCausets.
fn bench_table_scan_datum_absent_large_row<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_missing_PrimaryCauset(ROWS, 100);
    input.0.bench(
        b,
        &[table["col0"].as_PrimaryCauset_info()],
        &[table.get_record_cone_all()],
        &store,
        (),
    );
}

/// 1 interested PrimaryCauset, which is PK. However the cone given are point cones.
fn bench_table_scan_point_cone<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_2_PrimaryCausets(ROWS);

    let mut cones = vec![];
    for i in 0..=1024 {
        cones.push(table.get_record_cone_one(i));
    }

    input
        .0
        .bench(b, &[table["id"].as_PrimaryCauset_info()], &cones, &store, ());
}

#[derive(Clone)]
struct Input<M>(Box<dyn ScanBencher<util::TableScanParam, M>>)
where
    M: Measurement + 'static;

impl<M> Input<M>
where
    M: Measurement + 'static,
{
    pub fn new<T: ScanBencher<util::TableScanParam, M> + 'static>(b: T) -> Self {
        Self(Box::new(b))
    }
}

impl<M> std::fmt::Display for Input<M>
where
    M: Measurement + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name())
    }
}

pub fn bench<M>(c: &mut criterion::Criterion<M>)
where
    M: Measurement + 'static,
{
    let mut inputs = vec![
        Input::new(util::NormalTableScanNext1024Bencher::<MemStore>::new()),
        Input::new(util::BatchTableScanNext1024Bencher::<MemStore>::new()),
        Input::new(util::TableScanDAGBencher::<LmdbStore>::new(false, ROWS)),
        Input::new(util::TableScanDAGBencher::<LmdbStore>::new(true, ROWS)),
    ];
    if crate::util::bench_level() >= 2 {
        let mut additional_inputs = vec![
            Input::new(util::NormalTableScanNext1024Bencher::<LmdbStore>::new()),
            Input::new(util::BatchTableScanNext1024Bencher::<LmdbStore>::new()),
            Input::new(util::NormalTableScanNext1Bencher::<MemStore>::new()),
            Input::new(util::NormalTableScanNext1Bencher::<LmdbStore>::new()),
            Input::new(util::TableScanDAGBencher::<MemStore>::new(false, ROWS)),
            Input::new(util::TableScanDAGBencher::<MemStore>::new(true, ROWS)),
        ];
        inputs.applightlike(&mut additional_inputs);
    }

    let mut cases = vec![
        BenchCase::new("table_scan_primary_key", bench_table_scan_primary_key),
        BenchCase::new("table_scan_long_datum_all", bench_table_scan_long_datum_all),
        BenchCase::new(
            "table_scan_datum_absent_large_row",
            bench_table_scan_datum_absent_large_row,
        ),
    ];
    if crate::util::bench_level() >= 1 {
        let mut additional_cases = vec![
            BenchCase::new("table_scan_datum_front", bench_table_scan_datum_front),
            BenchCase::new("table_scan_datum_all", bench_table_scan_datum_all),
            BenchCase::new("table_scan_point_cone", bench_table_scan_point_cone),
        ];
        cases.applightlike(&mut additional_cases);
    }
    if crate::util::bench_level() >= 2 {
        let mut additional_cases = vec![
            BenchCase::new(
                "table_scan_datum_multi_front",
                bench_table_scan_datum_multi_front,
            ),
            BenchCase::new("table_scan_datum_lightlike", bench_table_scan_datum_lightlike),
            BenchCase::new(
                "table_scan_long_datum_primary_key",
                bench_table_scan_long_datum_primary_key,
            ),
            BenchCase::new(
                "table_scan_long_datum_normal",
                bench_table_scan_long_datum_normal,
            ),
            BenchCase::new(
                "table_scan_long_datum_long",
                bench_table_scan_long_datum_long,
            ),
            BenchCase::new("table_scan_datum_absent", bench_table_scan_datum_absent),
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
            ); // TODO: add parameter for each bench
        }
        group.finish();
    }
}
