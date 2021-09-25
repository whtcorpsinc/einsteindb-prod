//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use interlocking_directorate::ConcurrencyManager;
use criterion::{black_box, BatchSize, Bencher, Criterion};
use ekvproto::kvrpc_timeshare::Context;
use test_util::KvGenerator;
use edb::causet_storage::kv::{Engine, WriteData};
use edb::causet_storage::tail_pointer::{self, MvccTxn};
use txn_types::{Key, Mutation, TimeStamp};

use super::{BenchConfig, EngineFactory, DEFAULT_ITERATIONS};
use edb::causet_storage::txn::commit;

fn setup_prewrite<E, F>(
    engine: &E,
    config: &BenchConfig<F>,
    spacelike_ts: impl Into<TimeStamp>,
) -> Vec<Key>
where
    E: Engine,
    F: EngineFactory<E>,
{
    let ctx = Context::default();

    let snapshot = engine.snapshot(&ctx).unwrap();
    let spacelike_ts = spacelike_ts.into();
    let cm = ConcurrencyManager::new(spacelike_ts);
    let mut txn = MvccTxn::new(snapshot, spacelike_ts, true, cm);

    let kvs = KvGenerator::new(config.key_length, config.value_length).generate(DEFAULT_ITERATIONS);
    for (k, v) in &kvs {
        txn.prewrite(
            Mutation::Put((Key::from_raw(&k), v.clone())),
            &k.clone(),
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
        )
        .unwrap();
    }
    let write_data = WriteData::from_modifies(txn.into_modifies());
    let _ = engine.write(&ctx, write_data);
    let tuplespaceInstanton: Vec<Key> = kvs.iter().map(|(k, _)| Key::from_raw(&k)).collect();
    tuplespaceInstanton
}

fn txn_prewrite<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || {
            let mutations: Vec<(Mutation, Vec<u8>)> =
                KvGenerator::new(config.key_length, config.value_length)
                    .generate(DEFAULT_ITERATIONS)
                    .iter()
                    .map(|(k, v)| (Mutation::Put((Key::from_raw(&k), v.clone())), k.clone()))
                    .collect();
            mutations
        },
        |mutations| {
            for (mutation, primary) in mutations {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = tail_pointer::MvccTxn::new(snapshot, 1.into(), true, cm.clone());
                txn.prewrite(mutation, &primary, &None, false, 0, 0, TimeStamp::default())
                    .unwrap();
                let write_data = WriteData::from_modifies(txn.into_modifies());
                black_box(engine.write(&ctx, write_data)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn txn_commit<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || setup_prewrite(&engine, &config, 1),
        |tuplespaceInstanton| {
            for key in tuplespaceInstanton {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = tail_pointer::MvccTxn::new(snapshot, 1.into(), true, cm.clone());
                commit(&mut txn, key, 2.into()).unwrap();
                let write_data = WriteData::from_modifies(txn.into_modifies());
                black_box(engine.write(&ctx, write_data)).unwrap();
            }
        },
        BatchSize::SmallInput,
    );
}

fn txn_rollback_prewrote<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || setup_prewrite(&engine, &config, 1),
        |tuplespaceInstanton| {
            for key in tuplespaceInstanton {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = tail_pointer::MvccTxn::new(snapshot, 1.into(), true, cm.clone());
                txn.rollback(key).unwrap();
                let write_data = WriteData::from_modifies(txn.into_modifies());
                black_box(engine.write(&ctx, write_data)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn txn_rollback_conflict<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || setup_prewrite(&engine, &config, 2),
        |tuplespaceInstanton| {
            for key in tuplespaceInstanton {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = tail_pointer::MvccTxn::new(snapshot, 1.into(), true, cm.clone());
                txn.rollback(key).unwrap();
                let write_data = WriteData::from_modifies(txn.into_modifies());
                black_box(engine.write(&ctx, write_data)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn txn_rollback_non_prewrote<E: Engine, F: EngineFactory<E>>(
    b: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || {
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);
            let tuplespaceInstanton: Vec<Key> = kvs.iter().map(|(k, _)| Key::from_raw(&k)).collect();
            tuplespaceInstanton
        },
        |tuplespaceInstanton| {
            for key in tuplespaceInstanton {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = tail_pointer::MvccTxn::new(snapshot, 1.into(), true, cm.clone());
                txn.rollback(key).unwrap();
                let write_data = WriteData::from_modifies(txn.into_modifies());
                black_box(engine.write(&ctx, write_data)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

pub fn bench_txn<E: Engine, F: EngineFactory<E>>(c: &mut Criterion, configs: &[BenchConfig<F>]) {
    c.bench_function_over_inputs("txn_prewrite", txn_prewrite, configs.to_owned());
    c.bench_function_over_inputs("txn_commit", txn_commit, configs.to_owned());
    c.bench_function_over_inputs(
        "txn_rollback_prewrote",
        txn_rollback_prewrote,
        configs.to_owned(),
    );
    c.bench_function_over_inputs(
        "txn_rollback_conflict",
        txn_rollback_conflict,
        configs.to_owned(),
    );
    c.bench_function_over_inputs(
        "txn_rollback_non_prewrote",
        txn_rollback_non_prewrote,
        configs.to_owned(),
    );
}
