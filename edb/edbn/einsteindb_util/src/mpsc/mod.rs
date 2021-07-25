//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

/*!

This module provides an implementation of mpsc channel based on
crossbeam_channel. Comparing to the crossbeam_channel, this implementation
supports closed detection and try operations.

*/
pub mod batch;

use crossbeam::channel::{
    self, RecvError, RecvTimeoutError, lightlikeError, TryRecvError, TrylightlikeError,
};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct State {
    lightlikeer_cnt: AtomicIsize,
    connected: AtomicBool,
}

impl State {
    fn new() -> State {
        State {
            lightlikeer_cnt: AtomicIsize::new(1),
            connected: AtomicBool::new(true),
        }
    }

    #[inline]
    fn is_lightlikeer_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }
}

/// A lightlikeer that can be closed.
///
/// Closed means that lightlikeer can no longer lightlike out any messages after closing.
/// However, receiver may still block at receiving.
///
/// Note that a receiver should reports error in such case.
/// However, to fully implement a close mechanism, like waking up waiting
/// receivers, requires a cost of performance. And the mechanism is unnecessary
/// for current usage.
///
/// TODO: use builtin close when crossbeam-rs/crossbeam#236 is resolved.
pub struct lightlikeer<T> {
    lightlikeer: channel::lightlikeer<T>,
    state: Arc<State>,
}

impl<T> Clone for lightlikeer<T> {
    #[inline]
    fn clone(&self) -> lightlikeer<T> {
        self.state.lightlikeer_cnt.fetch_add(1, Ordering::AcqRel);
        lightlikeer {
            lightlikeer: self.lightlikeer.clone(),
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for lightlikeer<T> {
    #[inline]
    fn drop(&mut self) {
        let res = self.state.lightlikeer_cnt.fetch_add(-1, Ordering::AcqRel);
        if res == 1 {
            self.close_lightlikeer();
        }
    }
}

/// The receive lightlike of a channel.
pub struct Receiver<T> {
    receiver: channel::Receiver<T>,
    state: Arc<State>,
}

impl<T> lightlikeer<T> {
    /// Returns the number of messages in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.lightlikeer.len()
    }

    /// Returns true if the channel is empty.
    ///
    /// Note: Zero-capacity channels are always empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.lightlikeer.is_empty()
    }

    /// Blocks the current thread until a message is sent or the channel is disconnected.
    #[inline]
    pub fn lightlike(&self, t: T) -> Result<(), lightlikeError<T>> {
        if self.state.is_lightlikeer_connected() {
            self.lightlikeer.lightlike(t)
        } else {
            Err(lightlikeError(t))
        }
    }

    /// Attempts to lightlike a message into the channel without blocking.
    #[inline]
    pub fn try_lightlike(&self, t: T) -> Result<(), TrylightlikeError<T>> {
        if self.state.is_lightlikeer_connected() {
            self.lightlikeer.try_lightlike(t)
        } else {
            Err(TrylightlikeError::Disconnected(t))
        }
    }

    /// Stop the lightlikeer from lightlikeing any further messages.
    #[inline]
    pub fn close_lightlikeer(&self) {
        self.state.connected.store(false, Ordering::Release);
    }

    /// Check if the lightlikeer is still connected.
    #[inline]
    pub fn is_lightlikeer_connected(&self) -> bool {
        self.state.is_lightlikeer_connected()
    }
}

impl<T> Receiver<T> {
    /// Returns the number of messages in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.receiver.len()
    }

    /// Returns true if the channel is empty.
    ///
    /// Note: Zero-capacity channels are always empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    /// Blocks the current thread until a message is received or
    /// the channel is empty and disconnected.
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    /// Attempts to receive a message from the channel without blocking.
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    /// Waits for a message to be received from the channel,
    /// but only for a limited time.
    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }
}

impl<T> Drop for Receiver<T> {
    #[inline]
    fn drop(&mut self) {
        self.state.connected.store(false, Ordering::Release);
    }
}

/// Create an unbounded channel.
#[inline]
pub fn unbounded<T>() -> (lightlikeer<T>, Receiver<T>) {
    let state = Arc::new(State::new());
    let (lightlikeer, receiver) = channel::unbounded();
    (
        lightlikeer {
            lightlikeer,
            state: state.clone(),
        },
        Receiver { receiver, state },
    )
}

/// Create a bounded channel.
#[inline]
pub fn bounded<T>(cap: usize) -> (lightlikeer<T>, Receiver<T>) {
    let state = Arc::new(State::new());
    let (lightlikeer, receiver) = channel::bounded(cap);
    (
        lightlikeer {
            lightlikeer,
            state: state.clone(),
        },
        Receiver { receiver, state },
    )
}

const CHECK_INTERVAL: usize = 8;

/// A lightlikeer of channel that limits the maximun plightlikeing messages count loosely.
pub struct LooseBoundedlightlikeer<T> {
    lightlikeer: lightlikeer<T>,
    tried_cnt: Cell<usize>,
    limit: usize,
}

impl<T> LooseBoundedlightlikeer<T> {
    /// Returns the number of messages in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.lightlikeer.len()
    }

    /// Returns true if the channel is empty.
    ///
    /// Note: Zero-capacity channels are always empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.lightlikeer.is_empty()
    }

    /// lightlike a message regardless its capacity limit.
    #[inline]
    pub fn force_lightlike(&self, t: T) -> Result<(), lightlikeError<T>> {
        let cnt = self.tried_cnt.get();
        self.tried_cnt.set(cnt + 1);
        self.lightlikeer.lightlike(t)
    }

    /// Attempts to lightlike a message into the channel without blocking.
    #[inline]
    pub fn try_lightlike(&self, t: T) -> Result<(), TrylightlikeError<T>> {
        let cnt = self.tried_cnt.get();
        if cnt < CHECK_INTERVAL {
            self.tried_cnt.set(cnt + 1);
        } else if self.len() < self.limit {
            self.tried_cnt.set(1);
        } else {
            return Err(TrylightlikeError::Full(t));
        }

        match self.lightlikeer.lightlike(t) {
            Ok(()) => Ok(()),
            Err(lightlikeError(t)) => Err(TrylightlikeError::Disconnected(t)),
        }
    }

    /// Stop the lightlikeer from lightlikeing any further messages.
    #[inline]
    pub fn close_lightlikeer(&self) {
        self.lightlikeer.close_lightlikeer();
    }

    /// Check if the lightlikeer is still connected.
    #[inline]
    pub fn is_lightlikeer_connected(&self) -> bool {
        self.lightlikeer.state.is_lightlikeer_connected()
    }
}

impl<T> Clone for LooseBoundedlightlikeer<T> {
    #[inline]
    fn clone(&self) -> LooseBoundedlightlikeer<T> {
        LooseBoundedlightlikeer {
            lightlikeer: self.lightlikeer.clone(),
            tried_cnt: self.tried_cnt.clone(),
            limit: self.limit,
        }
    }
}

/// Create a loosely bounded channel with the given capacity.
pub fn loose_bounded<T>(cap: usize) -> (LooseBoundedlightlikeer<T>, Receiver<T>) {
    let (lightlikeer, receiver) = unbounded();
    (
        LooseBoundedlightlikeer {
            lightlikeer,
            tried_cnt: Cell::new(0),
            limit: cap,
        },
        receiver,
    )
}

#[causet(test)]
mod tests {
    use crossbeam::channel::*;
    use std::thread;
    use std::time::*;

    #[test]
    fn test_bounded() {
        let (tx, rx) = super::bounded::<u64>(10);
        tx.try_lightlike(1).unwrap();
        for i in 2..11 {
            tx.clone().lightlike(i).unwrap();
        }
        assert_eq!(tx.try_lightlike(11), Err(TrylightlikeError::Full(11)));

        assert_eq!(rx.try_recv(), Ok(1));
        for i in 2..11 {
            assert_eq!(rx.recv(), Ok(i));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        let timer = Instant::now();
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Timeout)
        );
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);

        drop(rx);
        assert_eq!(tx.lightlike(2), Err(lightlikeError(2)));
        assert_eq!(tx.try_lightlike(2), Err(TrylightlikeError::Disconnected(2)));
        assert!(!tx.is_lightlikeer_connected());

        let (tx, rx) = super::bounded::<u64>(10);
        tx.lightlike(2).unwrap();
        tx.lightlike(3).unwrap();
        drop(tx);
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.recv(), Ok(3));
        assert_eq!(rx.recv(), Err(RecvError));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Disconnected)
        );

        let (tx, rx) = super::bounded::<u64>(10);
        assert!(tx.is_empty());
        assert!(tx.is_lightlikeer_connected());
        assert_eq!(tx.len(), 0);
        assert!(rx.is_empty());
        assert_eq!(rx.len(), 0);
        tx.lightlike(2).unwrap();
        tx.lightlike(3).unwrap();
        assert_eq!(tx.len(), 2);
        assert_eq!(rx.len(), 2);
        tx.close_lightlikeer();
        assert_eq!(tx.lightlike(3), Err(lightlikeError(3)));
        assert_eq!(tx.try_lightlike(3), Err(TrylightlikeError::Disconnected(3)));
        assert!(!tx.is_lightlikeer_connected());
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.recv(), Ok(3));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        // assert_eq!(rx.recv(), Err(RecvError));

        let (tx1, rx1) = super::bounded::<u64>(10);
        let (tx2, rx2) = super::bounded::<u64>(0);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx1.lightlike(10).unwrap();
            thread::sleep(Duration::from_millis(100));
            assert_eq!(rx2.recv(), Ok(2));
        });
        let timer = Instant::now();
        assert_eq!(rx1.recv(), Ok(10));
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);
        let timer = Instant::now();
        tx2.lightlike(2).unwrap();
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(50), "{:?}", elapsed);
    }

    #[test]
    fn test_unbounded() {
        let (tx, rx) = super::unbounded::<u64>();
        tx.try_lightlike(1).unwrap();
        tx.lightlike(2).unwrap();

        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        let timer = Instant::now();
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Timeout)
        );
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);

        drop(rx);
        assert_eq!(tx.lightlike(2), Err(lightlikeError(2)));
        assert_eq!(tx.try_lightlike(2), Err(TrylightlikeError::Disconnected(2)));
        assert!(!tx.is_lightlikeer_connected());

        let (tx, rx) = super::unbounded::<u64>();
        drop(tx);
        assert_eq!(rx.recv(), Err(RecvError));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Disconnected)
        );

        let (tx, rx) = super::unbounded::<u64>();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx.lightlike(10).unwrap();
        });
        let timer = Instant::now();
        assert_eq!(rx.recv(), Ok(10));
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);

        let (tx, rx) = super::unbounded::<u64>();
        assert!(tx.is_empty());
        assert!(tx.is_lightlikeer_connected());
        assert_eq!(tx.len(), 0);
        assert!(rx.is_empty());
        assert_eq!(rx.len(), 0);
        tx.lightlike(2).unwrap();
        tx.lightlike(3).unwrap();
        assert_eq!(tx.len(), 2);
        assert_eq!(rx.len(), 2);
        tx.close_lightlikeer();
        assert_eq!(tx.lightlike(3), Err(lightlikeError(3)));
        assert_eq!(tx.try_lightlike(3), Err(TrylightlikeError::Disconnected(3)));
        assert!(!tx.is_lightlikeer_connected());
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.recv(), Ok(3));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        // assert_eq!(rx.recv(), Err(RecvError));
    }

    #[test]
    fn test_loose() {
        let (tx, rx) = super::loose_bounded(10);
        tx.try_lightlike(1).unwrap();
        for i in 2..11 {
            tx.clone().try_lightlike(i).unwrap();
        }
        for i in 1..super::CHECK_INTERVAL {
            tx.force_lightlike(i).unwrap();
        }
        assert_eq!(tx.try_lightlike(4), Err(TrylightlikeError::Full(4)));
        tx.force_lightlike(5).unwrap();
        assert_eq!(tx.try_lightlike(6), Err(TrylightlikeError::Full(6)));

        assert_eq!(rx.try_recv(), Ok(1));
        for i in 2..11 {
            assert_eq!(rx.recv(), Ok(i));
        }
        for i in 1..super::CHECK_INTERVAL {
            assert_eq!(rx.try_recv(), Ok(i));
        }
        assert_eq!(rx.try_recv(), Ok(5));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        let timer = Instant::now();
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Timeout)
        );
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);

        tx.force_lightlike(1).unwrap();
        drop(rx);
        assert_eq!(tx.force_lightlike(2), Err(lightlikeError(2)));
        assert_eq!(tx.try_lightlike(2), Err(TrylightlikeError::Disconnected(2)));
        for _ in 0..super::CHECK_INTERVAL {
            assert_eq!(tx.try_lightlike(2), Err(TrylightlikeError::Disconnected(2)));
        }

        let (tx, rx) = super::loose_bounded(10);
        tx.try_lightlike(2).unwrap();
        drop(tx);
        assert_eq!(rx.recv(), Ok(2));
        assert_eq!(rx.recv(), Err(RecvError));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Disconnected)
        );

        let (tx, rx) = super::loose_bounded(10);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx.try_lightlike(10).unwrap();
        });
        let timer = Instant::now();
        assert_eq!(rx.recv(), Ok(10));
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);
    }
}
