// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use crate::time::{monotonic_raw_now, Instant};
use std::cmp::{Ord, Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::{mpsc, Arc};
use std::thread::Builder;
use std::time::Duration;
use time::Timespec;
use tokio_executor::park::ParkThread;
use tokio_timer::{self, clock::Clock, clock::Now, timer::Handle, Delay};

pub struct Timer<T> {
    plightlikeing: BinaryHeap<Reverse<TimeoutTask<T>>>,
}

impl<T> Timer<T> {
    pub fn new(capacity: usize) -> Self {
        Timer {
            plightlikeing: BinaryHeap::with_capacity(capacity),
        }
    }

    /// Adds a periodic task into the `Timer`.
    pub fn add_task(&mut self, timeout: Duration, task: T) {
        let task = TimeoutTask {
            next_tick: Instant::now() + timeout,
            task,
        };
        self.plightlikeing.push(Reverse(task));
    }

    /// Gets the next `timeout` from the timer.
    pub fn next_timeout(&mut self) -> Option<Instant> {
        self.plightlikeing.peek().map(|task| task.0.next_tick)
    }

    /// Pops a `TimeoutTask` from the `Timer`, which should be ticked before `instant`.
    /// Returns `None` if no tasks should be ticked any more.
    ///
    /// The normal use case is keeping `pop_task_before` until get `None` in order
    /// to retrieve all available events.
    pub fn pop_task_before(&mut self, instant: Instant) -> Option<T> {
        if self
            .plightlikeing
            .peek()
            .map_or(false, |t| t.0.next_tick <= instant)
        {
            return self.plightlikeing.pop().map(|t| t.0.task);
        }
        None
    }
}

#[derive(Debug)]
struct TimeoutTask<T> {
    next_tick: Instant,
    task: T,
}

impl<T> PartialEq for TimeoutTask<T> {
    fn eq(&self, other: &TimeoutTask<T>) -> bool {
        self.next_tick == other.next_tick
    }
}

impl<T> Eq for TimeoutTask<T> {}

impl<T> PartialOrd for TimeoutTask<T> {
    fn partial_cmp(&self, other: &TimeoutTask<T>) -> Option<Ordering> {
        self.next_tick.partial_cmp(&other.next_tick)
    }
}

impl<T> Ord for TimeoutTask<T> {
    fn cmp(&self, other: &TimeoutTask<T>) -> Ordering {
        // TimeoutTask.next_tick must have same type of instants.
        self.partial_cmp(other).unwrap()
    }
}

lazy_static! {
    pub static ref GLOBAL_TIMER_HANDLE: Handle = spacelike_global_timer();
}

fn spacelike_global_timer() -> Handle {
    let (tx, rx) = mpsc::channel();
    Builder::new()
        .name(thd_name!("timer"))
        .spawn(move || {
            edb_alloc::add_thread_memory_accessor();
            let mut timer = tokio_timer::Timer::default();
            tx.lightlike(timer.handle()).unwrap();
            loop {
                timer.turn(None).unwrap();
            }
        })
        .unwrap();
    rx.recv().unwrap()
}

/// A struct that marks the *zero* time.
///
/// A *zero* time can be any time, as what it represents is `Instant`,
/// which is Opaque.
struct TimeZero {
    /// An arbitrary time used as the zero time.
    ///
    /// Note that `zero` doesn't have to be related to `steady_time_point`, as what's
    /// observed here is elapsed time instead of time point.
    zero: std::time::Instant,
    /// A base time point.
    ///
    /// The source of time point should grow steady.
    steady_time_point: Timespec,
}

/// A clock that produces time in a steady speed.
///
/// Time produced by the clock is not affected by clock jump or time adjustment.
/// Internally it uses CLOCK_MONOTONIC_RAW to get a steady time source.
///
/// `Instant`s produced by this clock can't be compared or used to calculate elapse
/// unless they are produced using the same zero time.
#[derive(Clone)]
pub struct SteadyClock {
    zero: Arc<TimeZero>,
}

lazy_static! {
    static ref STEADY_CLOCK: SteadyClock = SteadyClock {
        zero: Arc::new(TimeZero {
            zero: std::time::Instant::now(),
            steady_time_point: monotonic_raw_now(),
        }),
    };
}

impl Default for SteadyClock {
    #[inline]
    fn default() -> SteadyClock {
        STEADY_CLOCK.clone()
    }
}

impl Now for SteadyClock {
    #[inline]
    fn now(&self) -> std::time::Instant {
        let n = monotonic_raw_now();
        let dur = Instant::elapsed_duration(n, self.zero.steady_time_point);
        self.zero.zero + dur
    }
}

/// A timer that creates steady delays.
///
/// Delay created by this timer will not be affected by time adjustment.
#[derive(Clone)]
pub struct SteadyTimer {
    clock: SteadyClock,
    handle: Handle,
}

impl SteadyTimer {
    /// Creates a delay future that will be notified after the given duration.
    pub fn delay(&self, dur: Duration) -> Delay {
        self.handle.delay(self.clock.now() + dur)
    }
}

lazy_static! {
    static ref GLOBAL_STEADY_TIMER: SteadyTimer = spacelike_global_steady_timer();
}

impl Default for SteadyTimer {
    #[inline]
    fn default() -> SteadyTimer {
        GLOBAL_STEADY_TIMER.clone()
    }
}

fn spacelike_global_steady_timer() -> SteadyTimer {
    let (tx, rx) = mpsc::channel();
    let clock = SteadyClock::default();
    let clock_ = clock.clone();
    Builder::new()
        .name(thd_name!("steady-timer"))
        .spawn(move || {
            let c = Clock::new_with_now(clock_);
            let mut timer = tokio_timer::Timer::new_with_now(ParkThread::new(), c);
            tx.lightlike(timer.handle()).unwrap();
            loop {
                timer.turn(None).unwrap();
            }
        })
        .unwrap();
    SteadyTimer {
        clock,
        handle: rx.recv().unwrap(),
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use crate::worker::{Builder as WorkerBuilder, Runnable, RunnableWithTimer};
    use futures::compat::Future01CompatExt;
    use futures::executor::block_on;
    use std::sync::mpsc::RecvTimeoutError;
    use std::sync::mpsc::{self, lightlikeer};

    #[derive(Debug, PartialEq, Eq, Copy, Clone)]
    enum Task {
        A,
        B,
        C,
    }

    struct Runner {
        counter: usize,
        ch: lightlikeer<&'static str>,
    }

    impl Runnable for Runner {
        type Task = &'static str;

        fn run(&mut self, msg: &'static str) {
            self.ch.lightlike(msg).unwrap();
        }
        fn shutdown(&mut self) {
            self.ch.lightlike("").unwrap();
        }
    }

    impl RunnableWithTimer for Runner {
        type TimeoutTask = Task;

        fn on_timeout(&mut self, timer: &mut Timer<Task>, task: Task) {
            let timeout = match task {
                Task::A => {
                    self.ch.lightlike("task a").unwrap();
                    Duration::from_millis(60)
                }
                Task::B => {
                    self.ch.lightlike("task b").unwrap();
                    Duration::from_millis(100)
                }
                _ => unreachable!(),
            };
            if self.counter < 2 {
                timer.add_task(timeout, task);
            }
            self.counter += 1;
        }
    }

    #[test]
    fn test_timer() {
        let mut timer = Timer::new(10);
        timer.add_task(Duration::from_millis(20), Task::A);
        timer.add_task(Duration::from_millis(150), Task::C);
        timer.add_task(Duration::from_millis(100), Task::B);
        assert_eq!(timer.plightlikeing.len(), 3);

        let tick_time = timer.next_timeout().unwrap();
        assert_eq!(timer.pop_task_before(tick_time).unwrap(), Task::A);
        assert_eq!(timer.pop_task_before(tick_time), None);

        let tick_time = timer.next_timeout().unwrap();
        assert_eq!(timer.pop_task_before(tick_time).unwrap(), Task::B);
        assert_eq!(timer.pop_task_before(tick_time), None);

        let tick_time = timer.next_timeout().unwrap();
        assert_eq!(timer.pop_task_before(tick_time).unwrap(), Task::C);
        assert_eq!(timer.pop_task_before(tick_time), None);
    }

    #[test]
    fn test_worker_with_timer() {
        let mut worker = WorkerBuilder::new("test-worker-with-timer").create();
        for _ in 0..10 {
            worker.schedule("normal msg").unwrap();
        }

        let (tx, rx) = mpsc::channel();
        let runner = Runner {
            counter: 0,
            ch: tx.clone(),
        };

        let mut timer = Timer::new(10);
        timer.add_task(Duration::from_millis(60), Task::A);
        timer.add_task(Duration::from_millis(100), Task::B);

        worker.spacelike_with_timer(runner, timer).unwrap();

        for _ in 0..10 {
            let msg = rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(msg, "normal msg");
        }
        let msg = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg, "task a");
        let msg = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg, "task b");
        let msg = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg, "task a");
        let msg = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg, "task b");

        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)),
            Err(RecvTimeoutError::Timeout)
        );

        worker.stop().unwrap().join().unwrap();
    }

    #[test]
    fn test_global_timer() {
        let handle = super::GLOBAL_TIMER_HANDLE.clone();
        let delay =
            handle.delay(::std::time::Instant::now() + std::time::Duration::from_millis(100));
        let timer = Instant::now();
        block_on(delay.compat()).unwrap();
        assert!(timer.elapsed() >= Duration::from_millis(100));
    }

    #[test]
    fn test_global_steady_timer() {
        let t = SteadyTimer::default();
        let timer = t.clock.now();
        let delay = t.delay(Duration::from_millis(100));
        block_on(delay.compat()).unwrap();
        assert!(timer.elapsed() >= Duration::from_millis(100));
    }
}
