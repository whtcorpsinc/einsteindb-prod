mod engine;
mod engine_factory;
mod tail_pointer;
mod causet_storage;
mod txn;

use std::fmt;

use self::engine::bench_engine;
use self::engine_factory::{BTreeEngineFactory, EngineFactory, LmdbEngineFactory};
use self::tail_pointer::bench_tail_pointer;
use self::causet_storage::bench_causet_storage;
use self::txn::bench_txn;
use criterion::Criterion;
use edb::causet_storage::Engine;

const DEFAULT_ITERATIONS: usize = 10;
const DEFAULT_KEY_LENGTHS: [usize; 1] = [64];
const DEFAULT_VALUE_LENGTHS: [usize; 2] = [64, 65];
const DEFAULT_KV_GENERATOR_SEED: u64 = 0;

#[derive(Clone)]
pub struct BenchConfig<F> {
    pub key_length: usize,
    pub value_length: usize,
    pub engine_factory: F,
}

impl<F: fmt::Debug> fmt::Debug for BenchConfig<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}_KL{:?}_VL{:?}",
            self.engine_factory, self.key_length, self.value_length
        )
    }
}

pub fn load_configs<E: Engine, F: EngineFactory<E>>(engine_factory: F) -> Vec<BenchConfig<F>> {
    let key_lengths = DEFAULT_KEY_LENGTHS;
    let value_lengths = DEFAULT_VALUE_LENGTHS;
    let mut configs = vec![];

    for &kl in &key_lengths {
        for &vl in &value_lengths {
            configs.push(BenchConfig {
                key_length: kl,
                value_length: vl,
                engine_factory,
            })
        }
    }
    configs
}

fn main() {
    let mut c = Criterion::default().configure_from_args();
    let btree_engine_configs = load_configs(BTreeEngineFactory {});
    let rocks_engine_configs = load_configs(LmdbEngineFactory {});

    bench_engine(&mut c, &btree_engine_configs);
    bench_engine(&mut c, &rocks_engine_configs);

    bench_tail_pointer(&mut c, &btree_engine_configs);
    bench_tail_pointer(&mut c, &rocks_engine_configs);

    bench_txn(&mut c, &btree_engine_configs);
    bench_txn(&mut c, &rocks_engine_configs);

    bench_causet_storage(&mut c, &btree_engine_configs);
    bench_causet_storage(&mut c, &rocks_engine_configs);

    c.final_summary();
}
