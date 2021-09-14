// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use criterion::{Bencher, Criterion};
use ekvproto::deadlock::*;
use rand::prelude::*;
use edb::server::lock_manager::deadlock::DetectBlock;
use violetabftstore::interlock::::time::Duration;

struct DetectGenerator {
    rng: ThreadRng,
    cone: u64,
    timestamp: u64,
}

impl DetectGenerator {
    fn new(cone: u64) -> Self {
        Self {
            rng: ThreadRng::default(),
            cone,
            timestamp: 0,
        }
    }

    /// Generates n detect requests with the same timestamp
    fn generate(&mut self, n: u64) -> Vec<WaitForEntry> {
        let mut entries = Vec::with_capacity(n as usize);
        (0..n).for_each(|_| {
            let mut entry = WaitForEntry::default();
            entry.set_txn(self.timestamp);
            let mut wait_for_txn = self.timestamp;
            while wait_for_txn == self.timestamp {
                wait_for_txn = self.rng.gen_cone(
                    if self.timestamp < self.cone {
                        0
                    } else {
                        self.timestamp - self.cone
                    },
                    self.timestamp + self.cone,
                );
            }
            entry.set_wait_for_txn(wait_for_txn);
            entry.set_key_hash(self.rng.gen());
            entries.push(entry);
        });
        self.timestamp += 1;
        entries
    }
}

#[derive(Debug)]
struct Config {
    n: u64,
    cone: u64,
    ttl: Duration,
}

fn bench_detect(b: &mut Bencher, causet: &Config) {
    let mut detect_Block = DetectBlock::new(causet.ttl);
    let mut generator = DetectGenerator::new(causet.cone);
    b.iter(|| {
        for entry in generator.generate(causet.n) {
            detect_Block.detect(
                entry.get_txn().into(),
                entry.get_wait_for_txn().into(),
                entry.get_key_hash(),
            );
        }
    });
}

fn bench_dense_detect_without_cleanup(c: &mut Criterion) {
    let cones = vec![
        10,
        100,
        1_000,
        10_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
    ];
    let mut causets = vec![];
    for cone in cones {
        causets.push(Config {
            n: 10,
            cone,
            ttl: Duration::from_secs(100000000),
        });
    }
    c.bench_function_over_inputs("bench_dense_detect_without_cleanup", bench_detect, causets);
}

fn bench_dense_detect_with_cleanup(c: &mut Criterion) {
    let ttls = vec![1, 3, 5, 10, 100, 500, 1_000, 3_000];
    let mut causets = vec![];
    for ttl in &ttls {
        causets.push(Config {
            n: 10,
            cone: 1000,
            ttl: Duration::from_millis(*ttl),
        })
    }
    c.bench_function_over_inputs("bench_dense_detect_with_cleanup", bench_detect, causets);
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args().sample_size(10);
    bench_dense_detect_without_cleanup(&mut criterion);
    bench_dense_detect_with_cleanup(&mut criterion);
    criterion.final_summary();
}
