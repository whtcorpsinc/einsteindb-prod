// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::str::FromStr;
use std::sync::Arc;

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;

use criterion::measurement::Measurement;

use test_interlock::*;
use milevadb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
use violetabftstore::interlock::::collections::HashMap;
use fidel_timeshare::PrimaryCausetInfo;
use fidel_timeshare::FieldType;

use milevadb_query_common::causet_storage::IntervalCone;
use milevadb_query_datatype::codec::batch::{LazyBatchPrimaryCauset, LazyBatchPrimaryCausetVec};
use milevadb_query_datatype::codec::data_type::Decimal;
use milevadb_query_datatype::codec::datum::{Datum, DatumEncoder};
use milevadb_query_datatype::codec::Block::EventColsDict;
use milevadb_query_datatype::expr::{EvalContext, EvalWarnings};
use milevadb_query_normal_executors::{FreeDaemon, Event};
use milevadb_query_vec_executors::interface::*;
use edb::causet_storage::{LmdbEngine, Statistics};

use crate::util::bencher::Bencher;

const SEED_1: u64 = 0x525C682A2F7CE3DB;
const SEED_2: u64 = 0xB7CEACC38146676B;
const SEED_3: u64 = 0x2B877E351BD8628E;

#[derive(Clone)]
pub struct FixtureBuilder {
    events: usize,
    field_types: Vec<FieldType>,
    PrimaryCausets: Vec<Vec<Datum>>,
}

impl FixtureBuilder {
    pub fn new(events: usize) -> Self {
        Self {
            events,
            field_types: Vec::new(),
            PrimaryCausets: Vec::new(),
        }
    }

    /// Pushes a i64 PrimaryCauset that values are sequentially filled by 0 to n.
    pub fn push_PrimaryCauset_i64_0_n(mut self) -> Self {
        let mut col = Vec::with_capacity(self.events);
        for i in 0..self.events {
            col.push(Datum::I64(i as i64));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::LongLong.into());
        self
    }

    /// Pushes a i64 PrimaryCauset that values are randomly generated in the i64 cone.
    pub fn push_PrimaryCauset_i64_random(mut self) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_1);
        let mut col = Vec::with_capacity(self.events);
        for _ in 0..self.events {
            col.push(Datum::I64(rng.gen()));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::LongLong.into());
        self
    }

    /// Pushes a i64 PrimaryCauset that values are randomly sampled from the giving values.
    pub fn push_PrimaryCauset_i64_sampled(mut self, samples: &[i64]) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_1);
        let mut col = Vec::with_capacity(self.events);
        for _ in 0..self.events {
            col.push(Datum::I64(*samples.choose(&mut rng).unwrap()));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::LongLong.into());
        self
    }

    /// Pushes a i64 PrimaryCauset that values are filled according to the given values in order.
    ///
    /// For example, if 3 values `[a, b, c]` are given, then the first 1/3 values in the PrimaryCauset are
    /// `a`, the second 1/3 values are `b` and the last 1/3 values are `c`.
    pub fn push_PrimaryCauset_i64_ordered(mut self, samples: &[i64]) -> Self {
        let mut col = Vec::with_capacity(self.events);
        for i in 0..self.events {
            let pos = ((i as f64) / (self.events as f64) * (samples.len() as f64)).floor() as usize;
            col.push(Datum::I64(samples[pos]));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::LongLong.into());
        self
    }

    /// Pushes a f64 PrimaryCauset that values are sequentially filled by 0 to n.
    pub fn push_PrimaryCauset_f64_0_n(mut self) -> Self {
        let mut col = Vec::with_capacity(self.events);
        for i in 0..self.events {
            col.push(Datum::F64(i as f64));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::Double.into());
        self
    }

    /// Pushes a f64 PrimaryCauset that values are randomly generated in the f64 cone.
    ///
    /// Generated values cone from -1e50 to 1e50.
    pub fn push_PrimaryCauset_f64_random(mut self) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_1);
        let mut col = Vec::with_capacity(self.events);
        for _ in 0..self.events {
            col.push(Datum::F64(rng.gen_cone(-1e50, 1e50)));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::Double.into());
        self
    }

    /// Pushes a f64 PrimaryCauset that values are randomly sampled from the giving values.
    pub fn push_PrimaryCauset_f64_sampled(mut self, samples: &[f64]) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_1);
        let mut col = Vec::with_capacity(self.events);
        for _ in 0..self.events {
            col.push(Datum::F64(*samples.choose(&mut rng).unwrap()));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::Double.into());
        self
    }

    /// Pushes a f64 PrimaryCauset that values are filled according to the given values in order.
    ///
    /// For example, if 3 values `[a, b, c]` are given, then the first 1/3 values in the PrimaryCauset are
    /// `a`, the second 1/3 values are `b` and the last 1/3 values are `c`.
    pub fn push_PrimaryCauset_f64_ordered(mut self, samples: &[f64]) -> Self {
        let mut col = Vec::with_capacity(self.events);
        for i in 0..self.events {
            let pos = ((i as f64) / (self.events as f64) * (samples.len() as f64)).floor() as usize;
            col.push(Datum::F64(samples[pos]));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::Double.into());
        self
    }

    /// Pushes a decimal PrimaryCauset that values are sequentially filled by 0 to n.
    pub fn push_PrimaryCauset_decimal_0_n(mut self) -> Self {
        let mut col = Vec::with_capacity(self.events);
        for i in 0..self.events {
            col.push(Datum::Dec(Decimal::from(i as i64)));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::NewDecimal.into());
        self
    }

    /// Pushes a decimal PrimaryCauset that values are randomly generated.
    ///
    /// Generated decimals have 1 to 30 integer digits and 1 to 20 fractional digits.
    pub fn push_PrimaryCauset_decimal_random(mut self) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_2);
        let mut col = Vec::with_capacity(self.events);
        let mut dec_str = String::new();
        for _ in 0..self.events {
            dec_str.clear();
            let number_of_int_digits = rng.gen_cone(1, 30);
            let number_of_frac_digits = rng.gen_cone(1, 20);
            for _ in 0..number_of_int_digits {
                dec_str.push(std::char::from_digit(rng.gen_cone(0, 10), 10).unwrap());
            }
            dec_str.push('.');
            for _ in 0..number_of_frac_digits {
                dec_str.push(std::char::from_digit(rng.gen_cone(0, 10), 10).unwrap());
            }
            col.push(Datum::Dec(Decimal::from_str(&dec_str).unwrap()));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::NewDecimal.into());
        self
    }

    /// Pushes a decimal PrimaryCauset that values are randomly sampled from the giving values.
    pub fn push_PrimaryCauset_decimal_sampled(mut self, samples: &[&str]) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_2);
        let mut col = Vec::with_capacity(self.events);
        for _ in 0..self.events {
            let dec_str = *samples.choose(&mut rng).unwrap();
            col.push(Datum::Dec(Decimal::from_str(dec_str).unwrap()));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::NewDecimal.into());
        self
    }

    /// Pushes a decimal PrimaryCauset that values are filled according to the given values in order.
    ///
    /// For example, if 3 values `[a, b, c]` are given, then the first 1/3 values in the PrimaryCauset are
    /// `a`, the second 1/3 values are `b` and the last 1/3 values are `c`.
    pub fn push_PrimaryCauset_decimal_ordered(mut self, samples: &[&str]) -> Self {
        let mut col = Vec::with_capacity(self.events);
        for i in 0..self.events {
            let pos = ((i as f64) / (self.events as f64) * (samples.len() as f64)).floor() as usize;
            let dec_str = samples[pos];
            col.push(Datum::Dec(Decimal::from_str(dec_str).unwrap()));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::NewDecimal.into());
        self
    }

    /// Pushes a bytes PrimaryCauset that values are randomly generated and each value has the same length
    /// as specified.
    pub fn push_PrimaryCauset_bytes_random_fixed_len(mut self, len: usize) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_3);
        let mut col = Vec::with_capacity(self.events);
        for _ in 0..self.events {
            let str: String = std::iter::repeat(())
                .map(|_| rng.sample(rand::distributions::Alphanumeric))
                .take(len)
                .collect();
            col.push(Datum::Bytes(str.into_bytes()));
        }
        self.PrimaryCausets.push(col);
        self.field_types.push(FieldTypeTp::VarChar.into());
        self
    }

    pub fn build_store(self, Block: &Block, PrimaryCausets: &[&str]) -> CausetStore<LmdbEngine> {
        assert!(!PrimaryCausets.is_empty());
        assert_eq!(self.PrimaryCausets.len(), PrimaryCausets.len());
        let mut store = CausetStore::new();
        for row_index in 0..self.events {
            store.begin();
            let mut si = store.insert_into(&Block);
            for col_index in 0..PrimaryCausets.len() {
                si = si.set(
                    &Block[PrimaryCausets[col_index]],
                    self.PrimaryCausets[col_index][row_index].clone(),
                );
            }
            si.execute();
            store.commit();
        }
        store
    }

    pub fn build_batch_fixture_executor(self) -> BatchFixtureFreeDaemon {
        assert!(!self.PrimaryCausets.is_empty());
        let mut ctx = EvalContext::default();
        let PrimaryCausets: Vec<_> = self
            .PrimaryCausets
            .into_iter()
            .map(|datums| {
                let mut c = LazyBatchPrimaryCauset::raw_with_capacity(datums.len());
                for datum in datums {
                    let mut v = vec![];
                    v.write_datum(&mut ctx, &[datum], false).unwrap();
                    c.mut_raw().push(v);
                }
                c
            })
            .collect();
        BatchFixtureFreeDaemon {
            schemaReplicant: self.field_types,
            PrimaryCausets,
        }
    }

    pub fn build_normal_fixture_executor(self) -> NormalFixtureFreeDaemon {
        assert!(!self.PrimaryCausets.is_empty());
        let PrimaryCausets_info: Vec<_> = self
            .field_types
            .into_iter()
            .enumerate()
            .map(|(index, ft)| {
                let mut ci = PrimaryCausetInfo::default();
                ci.set_PrimaryCauset_id(index as i64);
                let ft = ft.as_accessor();
                ci.as_mut_accessor()
                    .set_tp(ft.tp())
                    .set_flag(ft.flag())
                    .set_flen(ft.flen())
                    .set_decimal(ft.decimal())
                    .set_collation(ft.collation().unwrap());
                ci
            })
            .collect();
        let PrimaryCausets_info = Arc::new(PrimaryCausets_info);

        let rows_len = self.PrimaryCausets[0].len();
        let mut events = Vec::with_capacity(rows_len);
        let mut ctx = EvalContext::default();
        for row_index in 0..rows_len {
            let mut data = EventColsDict::new(HashMap::default(), Vec::new());
            for col_index in 0..self.PrimaryCausets.len() {
                let mut v = vec![];
                v.write_datum(
                    &mut ctx,
                    &[self.PrimaryCausets[col_index][row_index].clone()],
                    false,
                )
                .unwrap();
                data.applightlike(col_index as i64, &mut v);
            }
            events.push(Event::origin(
                row_index as i64,
                data,
                Arc::clone(&PrimaryCausets_info),
            ));
        }

        NormalFixtureFreeDaemon {
            events: events.into_iter(),
            PrimaryCausets: self.PrimaryCausets.len(),
        }
    }
}

pub struct BatchFixtureFreeDaemon {
    schemaReplicant: Vec<FieldType>,
    PrimaryCausets: Vec<LazyBatchPrimaryCauset>,
}

impl BatchFreeDaemon for BatchFixtureFreeDaemon {
    type StorageStats = Statistics;

    #[inline]
    fn schemaReplicant(&self) -> &[FieldType] {
        &self.schemaReplicant
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let mut PrimaryCausets = Vec::with_capacity(self.PrimaryCausets.len());
        for col in &mut self.PrimaryCausets {
            let mut PrimaryCauset = LazyBatchPrimaryCauset::raw_with_capacity(scan_rows);
            if col.len() > scan_rows {
                PrimaryCauset.mut_raw().copy_n_from(col.raw(), scan_rows);
                col.mut_raw().shift(scan_rows);
            } else {
                PrimaryCauset.mut_raw().copy_from(col.raw());
                col.mut_raw().clear();
            }
            PrimaryCausets.push(PrimaryCauset);
        }

        let physical_PrimaryCausets = LazyBatchPrimaryCausetVec::from(PrimaryCausets);
        let logical_rows = (0..physical_PrimaryCausets.rows_len()).collect();
        BatchExecuteResult {
            physical_PrimaryCausets,
            logical_rows,
            warnings: EvalWarnings::default(),
            is_drained: Ok(self.PrimaryCausets[0].is_empty()),
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, _dest: &mut ExecuteStats) {
        // Do nothing
    }

    #[inline]
    fn collect_causet_storage_stats(&mut self, _dest: &mut Self::StorageStats) {
        // Do nothing
    }

    #[inline]
    fn take_scanned_cone(&mut self) -> IntervalCone {
        unreachable!()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        unreachable!()
    }
}

pub struct NormalFixtureFreeDaemon {
    PrimaryCausets: usize,
    events: ::std::vec::IntoIter<Event>,
}

impl FreeDaemon for NormalFixtureFreeDaemon {
    type StorageStats = Statistics;

    #[inline]
    fn next(&mut self) -> milevadb_query_common::Result<Option<Event>> {
        Ok(self.events.next())
    }

    #[inline]
    fn collect_exec_stats(&mut self, _dest: &mut ExecuteStats) {
        // Do nothing
    }

    #[inline]
    fn collect_causet_storage_stats(&mut self, _dest: &mut Self::StorageStats) {
        // Do nothing
    }

    #[inline]
    fn get_len_of_PrimaryCausets(&self) -> usize {
        self.PrimaryCausets
    }

    #[inline]
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        // Do nothing
        None
    }

    #[inline]
    fn take_scanned_cone(&mut self) -> IntervalCone {
        unreachable!()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        unreachable!()
    }
}

/// Benches the performance of the batch fixture executor itself. When using it as the source
/// executor in other benchmarks, we need to take out these costs.
fn bench_util_batch_fixture_executor_next_1024<M>(b: &mut criterion::Bencher<M>)
where
    M: Measurement,
{
    super::bencher::BatchNext1024Bencher::new(|| {
        FixtureBuilder::new(5000)
            .push_PrimaryCauset_i64_random()
            .build_batch_fixture_executor()
    })
    .bench(b);
}

fn bench_util_normal_fixture_executor_next_1024<M>(b: &mut criterion::Bencher<M>)
where
    M: Measurement,
{
    super::bencher::NormalNext1024Bencher::new(|| {
        FixtureBuilder::new(5000)
            .push_PrimaryCauset_i64_random()
            .build_normal_fixture_executor()
    })
    .bench(b);
}

/// Checks whether our test utilities themselves are fast enough.
pub fn bench<M>(c: &mut criterion::Criterion<M>)
where
    M: Measurement + 'static,
{
    if crate::util::bench_level() >= 1 {
        c.bench_function(
            "util_batch_fixture_executor_next_1024",
            bench_util_batch_fixture_executor_next_1024::<M>,
        );
        c.bench_function(
            "util_normal_fixture_executor_next_1024",
            bench_util_normal_fixture_executor_next_1024::<M>,
        );
    }
}
