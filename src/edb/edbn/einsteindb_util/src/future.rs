//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use crate::callback::must_call;
use futures::channel::mpsc;
use futures::channel::oneshot as futures_oneshot;
use futures::future::{self, BoxFuture, Future, FutureExt, TryFutureExt};
use futures::stream::{Stream, StreamExt};
use futures::task::{self, ArcWake, Context, Poll};

use std::sync::{Arc, Mutex};

/// Generates a paired future and callback so that when callback is being called, its result
/// is automatically passed as a future result.
pub fn paired_future_callback<T>() -> (Box<dyn FnOnce(T) + lightlike>, futures_oneshot::Receiver<T>)
where
    T: lightlike + 'static,
{
    let (tx, future) = futures_oneshot::channel::<T>();
    let callback = Box::new(move |result| {
        let r = tx.lightlike(result);
        if r.is_err() {
            warn!("paired_future_callback: Failed to lightlike result to the future rx, discarded.");
        }
    });
    (callback, future)
}

pub fn paired_must_called_future_callback<T>(
    arg_on_drop: impl FnOnce() -> T + lightlike + 'static,
) -> (Box<dyn FnOnce(T) + lightlike>, futures_oneshot::Receiver<T>)
where
    T: lightlike + 'static,
{
    let (tx, future) = futures_oneshot::channel::<T>();
    let callback = must_call(
        move |result| {
            let r = tx.lightlike(result);
            if r.is_err() {
                warn!("paired_future_callback: Failed to lightlike result to the future rx, discarded.");
            }
        },
        arg_on_drop,
    );
    (callback, future)
}

/// Create a stream proxy with buffer representing the remote stream. The returned task
/// will receive messages from the remote stream as much as possible.
pub fn create_stream_with_buffer<T, S>(
    s: S,
    size: usize,
) -> (
    impl Stream<Item = T> + lightlike + 'static,
    impl Future<Output = ()> + lightlike + 'static,
)
where
    S: Stream<Item = T> + lightlike + 'static,
    T: lightlike + 'static,
{
    let (tx, rx) = mpsc::channel::<T>(size);
    let driver = s
        .then(future::ok::<T, mpsc::lightlikeError>)
        .forward(tx)
        .map_err(|e| warn!("stream with buffer lightlike error"; "error" => %e))
        .map(|_| ());
    (rx, driver)
}

/// Polls the provided future immediately. If the future is not ready,
/// it will register the waker. When the event is ready, the waker will
/// be notified, then the internal future is immediately polled in the
/// thread calling `wake()`.
pub fn poll_future_notify<F: Future<Output = ()> + lightlike + 'static>(f: F) {
    let f: BoxFuture<'static, ()> = Box::pin(f);
    let waker = Arc::new(BatchCommandsWaker(Mutex::new(Some(f))));
    waker.wake();
}

// BatchCommandsWaker is used to make business pool notifiy completion queues directly.
struct BatchCommandsWaker(Mutex<Option<BoxFuture<'static, ()>>>);

impl ArcWake for BatchCommandsWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let mut future_slot = arc_self.0.dagger().unwrap();
        if let Some(mut future) = future_slot.take() {
            let waker = task::waker_ref(&arc_self);
            let cx = &mut Context::from_waker(&*waker);
            match future.as_mut().poll(cx) {
                Poll::Plightlikeing => {
                    *future_slot = Some(future);
                }
                Poll::Ready(()) => {}
            }
        }
    }
}
