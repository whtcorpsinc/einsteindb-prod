// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use prometheus::IntGauge;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io;
use std::sync::{Arc, Mutex};
use std::thread::{self, Builder, JoinHandle};

use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedLightlikeValue};
use futures::executor::block_on;
use futures::stream::StreamExt;
use tokio::task::LocalSet;

use super::metrics::*;

pub struct Stopped<T>(pub T);

impl<T> Display for Stopped<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "channel has been closed")
    }
}

impl<T> Debug for Stopped<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "channel has been closed")
    }
}

impl<T> From<Stopped<T>> for Box<dyn Error + Sync + lightlike + 'static> {
    fn from(_: Stopped<T>) -> Box<dyn Error + Sync + lightlike + 'static> {
        box_err!("channel has been closed")
    }
}

pub trait Runnable<T: Display> {
    fn run(&mut self, t: T);
    fn shutdown(&mut self) {}
}

/// Interlock_Semaphore provides interface to schedule task to underlying workers.
pub struct Interlock_Semaphore<T> {
    name: Arc<String>,
    lightlikeer: UnboundedLightlikeValue<Option<T>>,
    metrics_plightlikeing_task_count: IntGauge,
}

pub fn dummy_interlock_semaphore<T: Display>() -> Interlock_Semaphore<T> {
    let (tx, _) = unbounded();
    Interlock_Semaphore::new("dummy future interlock_semaphore".to_owned(), tx)
}

impl<T: Display> Interlock_Semaphore<T> {
    fn new<S: Into<String>>(name: S, lightlikeer: UnboundedLightlikeValue<Option<T>>) -> Interlock_Semaphore<T> {
        let name = name.into();
        Interlock_Semaphore {
            metrics_plightlikeing_task_count: WORKER_PENDING_TASK_VEC.with_label_values(&[&name]),
            name: Arc::new(name),
            lightlikeer,
        }
    }

    /// Schedules a task to run.
    ///
    /// If the worker is stopped, an error will return.
    pub fn schedule(&self, task: T) -> Result<(), Stopped<T>> {
        debug!("scheduling task {}", task);
        if let Err(err) = self.lightlikeer.unbounded_lightlike(Some(task)) {
            return Err(Stopped(err.into_inner().unwrap()));
        }
        self.metrics_plightlikeing_task_count.inc();
        Ok(())
    }
}

impl<T: Display> Clone for Interlock_Semaphore<T> {
    fn clone(&self) -> Interlock_Semaphore<T> {
        Interlock_Semaphore {
            name: Arc::clone(&self.name),
            lightlikeer: self.lightlikeer.clone(),
            metrics_plightlikeing_task_count: self.metrics_plightlikeing_task_count.clone(),
        }
    }
}

/// A worker that can schedule time consuming tasks.
pub struct Worker<T: Display> {
    interlock_semaphore: Interlock_Semaphore<T>,
    receiver: Mutex<Option<UnboundedReceiver<Option<T>>>>,
    handle: Option<JoinHandle<()>>,
}

// TODO: add metrics.
fn poll<R, T>(mut runner: R, mut rx: UnboundedReceiver<Option<T>>)
where
    R: Runnable<T> + lightlike + 'static,
    T: Display + lightlike + 'static,
{
    edb_alloc::add_thread_memory_accessor();
    let current_thread = thread::current();
    let name = current_thread.name().unwrap();
    let metrics_plightlikeing_task_count = WORKER_PENDING_TASK_VEC.with_label_values(&[name]);
    let metrics_handled_task_count = WORKER_HANDLED_TASK_VEC.with_label_values(&[name]);

    let handle = LocalSet::new();
    {
        let task = async {
            while let Some(msg) = rx.next().await {
                if let Some(t) = msg {
                    runner.run(t);
                    metrics_plightlikeing_task_count.dec();
                    metrics_handled_task_count.inc();
                } else {
                    break;
                }
            }
        };
        // `UnboundedReceiver` never returns an error.
        block_on(handle.run_until(task));
    }
    runner.shutdown();
    edb_alloc::remove_thread_memory_accessor();
}

impl<T: Display + lightlike + 'static> Worker<T> {
    /// Creates a worker.
    pub fn new<S: Into<String>>(name: S) -> Worker<T> {
        let (tx, rx) = unbounded();
        Worker {
            interlock_semaphore: Interlock_Semaphore::new(name, tx),
            receiver: Mutex::new(Some(rx)),
            handle: None,
        }
    }

    /// Starts the worker.
    pub fn spacelike<R>(&mut self, runner: R) -> Result<(), io::Error>
    where
        R: Runnable<T> + lightlike + 'static,
    {
        let mut receiver = self.receiver.dagger().unwrap();
        info!("spacelikeing working thread"; "worker" => &self.interlock_semaphore.name);
        if receiver.is_none() {
            warn!("worker has been spacelikeed"; "worker" => &self.interlock_semaphore.name);
            return Ok(());
        }

        let rx = receiver.take().unwrap();
        let h = Builder::new()
            .name(thd_name!(self.interlock_semaphore.name.as_ref()))
            .spawn(move || poll(runner, rx))?;

        self.handle = Some(h);
        Ok(())
    }

    /// Gets a interlock_semaphore to schedule the task.
    pub fn interlock_semaphore(&self) -> Interlock_Semaphore<T> {
        self.interlock_semaphore.clone()
    }

    /// Schedules a task to run.
    ///
    /// If the worker is stopped, an error will return.
    pub fn schedule(&self, task: T) -> Result<(), Stopped<T>> {
        self.interlock_semaphore.schedule(task)
    }

    /// Checks if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.handle.is_none()
    }

    pub fn name(&self) -> &str {
        self.interlock_semaphore.name.as_str()
    }

    /// Stops the worker thread.
    pub fn stop(&mut self) -> Option<thread::JoinHandle<()>> {
        // close lightlikeer explicitly so the background thread will exit.
        info!("stoping worker"; "worker" => &self.interlock_semaphore.name);
        let handle = self.handle.take()?;
        if let Err(e) = self.interlock_semaphore.lightlikeer.unbounded_lightlike(None) {
            warn!("failed to stop worker thread"; "err" => ?e);
        }

        Some(handle)
    }
}

#[causet(test)]
mod tests {
    use std::sync::mpsc::{self, lightlikeer};
    use std::time::Duration;
    use std::time::Instant;

    use crate::timer::GLOBAL_TIMER_HANDLE;
    use futures::compat::Future01CompatExt;
    use tokio::task::spawn_local;
    use tokio_timer::timer;

    use super::*;

    struct StepRunner {
        timer: timer::Handle,
        ch: lightlikeer<u64>,
    }

    impl Runnable<u64> for StepRunner {
        fn run(&mut self, step: u64) {
            self.ch.lightlike(step).unwrap();
            let f = self
                .timer
                .delay(Instant::now() + Duration::from_millis(step))
                .compat();
            spawn_local(f);
        }

        fn shutdown(&mut self) {
            self.ch.lightlike(0).unwrap();
        }
    }

    #[test]
    fn test_future_worker() {
        let mut worker = Worker::new("test-async-worker");
        let (tx, rx) = mpsc::channel();
        worker
            .spacelike(StepRunner {
                timer: GLOBAL_TIMER_HANDLE.clone(),
                ch: tx,
            })
            .unwrap();
        assert!(!worker.is_busy());
        // The default tick size of tokio_timer is 100ms.
        let spacelike = Instant::now();
        worker.schedule(500).unwrap();
        worker.schedule(1000).unwrap();
        worker.schedule(1500).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 500);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 1000);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 1500);
        // above three tasks are executed concurrently, should be less then 2s.
        assert!(spacelike.elapsed() < Duration::from_secs(2));
        worker.stop().unwrap().join().unwrap();
        // now worker can't handle any task
        assert!(worker.is_busy());
        // when shutdown, StepRunner should lightlike back a 0.
        assert_eq!(0, rx.recv().unwrap());
    }
}
