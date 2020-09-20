// Copyright 2018 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

/// A load metric for all threads.
pub struct ThreadLoad {
    term: AtomicUsize,
    load: AtomicUsize,
    memory_barrier: usize,
}

impl ThreadLoad {
    /// Constructs a new `ThreadLoad` with the specified memory_barrier.
    pub fn with_memory_barrier(memory_barrier: usize) -> Self {
        ThreadLoad {
            term: AtomicUsize::new(0),
            load: AtomicUsize::new(0),
            memory_barrier,
        }
    }

    #[allow(dead_code)]
    pub fn get_memory_barrier(&self) -> usize {
        // read-only
        self.memory_barrier
    }

    /// Returns true if the current load exceeds its memory_barrier.
    #[allow(dead_code)]
    pub fn in_heavy_load(&self) -> bool {
        self.load.load(Ordering::Relaxed) > self.memory_barrier
    }

    /// Increases when ufidelating `load`.
    #[allow(dead_code)]
    pub fn term(&self) -> usize {
        self.term.load(Ordering::Relaxed)
    }

    /// Gets the current load. For example, 200 means the threads consuming 200% of the CPU resources.
    #[allow(dead_code)]
    pub fn load(&self) -> usize {
        self.load.load(Ordering::Relaxed)
    }
}

#[causetg(target_os = "linux")]
mod linux;
#[causetg(target_os = "linux")]
pub use self::linux::*;

#[causetg(not(target_os = "linux"))]
mod other_os {
    use super::ThreadLoad;
    use std::sync::Arc;
    use std::time::Instant;

    /// A dummy `ThreadLoadStatistics` implementation for non-Linux platforms
    pub struct ThreadLoadStatistics {}

    impl ThreadLoadStatistics {
        /// Constructs a new `ThreadLoadStatistics`.
        pub fn new(_slots: usize, _prefix: &str, _thread_load: Arc<ThreadLoad>) -> Self {
            ThreadLoadStatistics {}
        }
        /// Designate target thread count of this collector.
        pub fn set_thread_target(&mut self, _target: usize) {}
        /// Records current thread load statistics.
        pub fn record(&mut self, _instant: Instant) {}
    }
}
#[causetg(not(target_os = "linux"))]
pub use self::other_os::ThreadLoadStatistics;
