// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use criterion::black_box;
use criterion::measurement::Measurement;
use futures::executor::block_on;
use milevadb_query_normal_executors::FreeDaemon;
use milevadb_query_vec_executors::interface::*;
use edb::interlock::RequestHandler;

pub trait Bencher {
    fn bench<M>(&mut self, b: &mut criterion::Bencher<M>)
    where
        M: Measurement;
}

/// Invoke 1 next() for a normal executor.
pub struct NormalNext1Bencher<E: FreeDaemon, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: FreeDaemon, F: FnMut() -> E> NormalNext1Bencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: FreeDaemon, F: FnMut() -> E> Bencher for NormalNext1Bencher<E, F> {
    fn bench<M>(&mut self, b: &mut criterion::Bencher<M>)
    where
        M: Measurement,
    {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| {
                profiler::spacelike("./NormalNext1Bencher.profile");
                black_box(executor.next().unwrap());
                profiler::stop();
            },
            criterion::BatchSize::SmallInput,
        );
    }
}

/// Invoke 1024 next() for a normal executor.
pub struct NormalNext1024Bencher<E: FreeDaemon, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: FreeDaemon, F: FnMut() -> E> NormalNext1024Bencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: FreeDaemon, F: FnMut() -> E> Bencher for NormalNext1024Bencher<E, F> {
    fn bench<M>(&mut self, b: &mut criterion::Bencher<M>)
    where
        M: Measurement,
    {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| {
                profiler::spacelike("./NormalNext1024Bencher.profile");
                let iter_times = black_box(1024);
                for _ in 0..iter_times {
                    black_box(executor.next().unwrap());
                }
                profiler::stop();
            },
            criterion::BatchSize::SmallInput,
        );
    }
}

/// Invoke next() for a normal executor until drained.
pub struct NormalNextAllBencher<E: FreeDaemon, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: FreeDaemon, F: FnMut() -> E> NormalNextAllBencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: FreeDaemon, F: FnMut() -> E> Bencher for NormalNextAllBencher<E, F> {
    fn bench<M>(&mut self, b: &mut criterion::Bencher<M>)
    where
        M: Measurement,
    {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| {
                profiler::spacelike("./NormalNextAllBencher.profile");
                loop {
                    let r = executor.next().unwrap();
                    black_box(&r);
                    if r.is_none() {
                        break;
                    }
                }
                profiler::stop();
            },
            criterion::BatchSize::SmallInput,
        );
    }
}

/// Invoke 1 next_batch(1024) for a batch executor.
pub struct BatchNext1024Bencher<E: BatchFreeDaemon, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: BatchFreeDaemon, F: FnMut() -> E> BatchNext1024Bencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: BatchFreeDaemon, F: FnMut() -> E> Bencher for BatchNext1024Bencher<E, F> {
    fn bench<M>(&mut self, b: &mut criterion::Bencher<M>)
    where
        M: Measurement,
    {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| {
                profiler::spacelike("./BatchNext1024Bencher.profile");
                let iter_times = black_box(1024);
                let r = black_box(executor.next_batch(iter_times));
                r.is_drained.unwrap();
                profiler::stop();
            },
            criterion::BatchSize::SmallInput,
        );
    }
}

/// Invoke next_batch(1024) for a batch executor until drained.
pub struct BatchNextAllBencher<E: BatchFreeDaemon, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: BatchFreeDaemon, F: FnMut() -> E> BatchNextAllBencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: BatchFreeDaemon, F: FnMut() -> E> Bencher for BatchNextAllBencher<E, F> {
    fn bench<M>(&mut self, b: &mut criterion::Bencher<M>)
    where
        M: Measurement,
    {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| {
                profiler::spacelike("./BatchNextAllBencher.profile");
                loop {
                    let r = executor.next_batch(1024);
                    black_box(&r);
                    if r.is_drained.unwrap() {
                        break;
                    }
                }
                profiler::stop();
            },
            criterion::BatchSize::SmallInput,
        );
    }
}

/// Invoke handle request for a DAG handler.
pub struct DAGHandleBencher<F: FnMut() -> Box<dyn RequestHandler>> {
    handler_builder: F,
}

impl<F: FnMut() -> Box<dyn RequestHandler>> DAGHandleBencher<F> {
    pub fn new(handler_builder: F) -> Self {
        Self { handler_builder }
    }
}

impl<F: FnMut() -> Box<dyn RequestHandler>> Bencher for DAGHandleBencher<F> {
    fn bench<M>(&mut self, b: &mut criterion::Bencher<M>)
    where
        M: Measurement,
    {
        b.iter_batched_ref(
            &mut self.handler_builder,
            |handler| {
                profiler::spacelike("./DAGHandleBencher.profile");
                black_box(block_on(handler.handle_request()).unwrap());
                profiler::stop();
            },
            criterion::BatchSize::SmallInput,
        );
    }
}
