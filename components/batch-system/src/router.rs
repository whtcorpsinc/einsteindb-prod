// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fsm::{Fsm, FsmScheduler};
use crate::mailbox::{BasicMailbox, Mailbox};
use crossbeam::channel::{SlightlikeError, TrySlightlikeError};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use einsteindb_util::collections::HashMap;
use einsteindb_util::lru::LruCache;
use einsteindb_util::Either;

enum CheckDoResult<T> {
    NotExist,
    Invalid,
    Valid(T),
}

/// Router route messages to its target mailbox.
///
/// Every fsm has a mailbox, hence it's necessary to have an address book
/// that can deliver messages to specified fsm, which is exact router.
///
/// In our abstract model, every batch system has two different kind of
/// fsms. First is normal fsm, which does the common work like peers in a
/// violetabftstore model or apply delegate in apply model. Second is control fsm,
/// which does some work that requires a global view of resources or creates
/// missing fsm for specified address. Normal fsm and control fsm can have
/// different scheduler, but this is not required.
pub struct Router<N: Fsm, C: Fsm, Ns, Cs> {
    normals: Arc<Mutex<HashMap<u64, BasicMailbox<N>>>>,
    caches: Cell<LruCache<u64, BasicMailbox<N>>>,
    pub(super) control_box: BasicMailbox<C>,
    // TODO: These two schedulers should be unified as single one. However
    // it's not possible to write FsmScheduler<Fsm=C> + FsmScheduler<Fsm=N>
    // for now.
    pub(crate) normal_scheduler: Ns,
    control_scheduler: Cs,

    // Indicates the router is shutdown down or not.
    shutdown: Arc<AtomicBool>,
}

impl<N, C, Ns, Cs> Router<N, C, Ns, Cs>
where
    N: Fsm,
    C: Fsm,
    Ns: FsmScheduler<Fsm = N> + Clone,
    Cs: FsmScheduler<Fsm = C> + Clone,
{
    pub(super) fn new(
        control_box: BasicMailbox<C>,
        normal_scheduler: Ns,
        control_scheduler: Cs,
    ) -> Router<N, C, Ns, Cs> {
        Router {
            normals: Arc::default(),
            caches: Cell::new(LruCache::with_capacity_and_sample(1024, 7)),
            control_box,
            normal_scheduler,
            control_scheduler,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The `Router` has been already shutdown or not.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// A helper function that tries to unify a common access pattern to
    /// mailbox.
    ///
    /// Generally, when slightlikeing a message to a mailbox, cache should be
    /// check first, if not found, lock should be acquired.
    ///
    /// Returns None means there is no mailbox inside the normal registry.
    /// Some(None) means there is expected mailbox inside the normal registry
    /// but it returns None after apply the given function. Some(Some) means
    /// the given function returns Some and cache is ufidelated if it's invalid.
    #[inline]
    fn check_do<F, R>(&self, addr: u64, mut f: F) -> CheckDoResult<R>
    where
        F: FnMut(&BasicMailbox<N>) -> Option<R>,
    {
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let mut connected = true;
        if let Some(mailbox) = caches.get(&addr) {
            match f(mailbox) {
                Some(r) => return CheckDoResult::Valid(r),
                None => {
                    connected = false;
                }
            }
        }

        let (cnt, mailbox) = {
            let mut boxes = self.normals.lock().unwrap();
            let cnt = boxes.len();
            let b = match boxes.get_mut(&addr) {
                Some(mailbox) => mailbox.clone(),
                None => {
                    drop(boxes);
                    if !connected {
                        caches.remove(&addr);
                    }
                    return CheckDoResult::NotExist;
                }
            };
            (cnt, b)
        };
        if cnt > caches.capacity() || cnt < caches.capacity() / 2 {
            caches.resize(cnt);
        }

        let res = f(&mailbox);
        match res {
            Some(r) => {
                caches.insert(addr, mailbox);
                CheckDoResult::Valid(r)
            }
            None => {
                if !connected {
                    caches.remove(&addr);
                }
                CheckDoResult::Invalid
            }
        }
    }

    /// Register a mailbox with given address.
    pub fn register(&self, addr: u64, mailbox: BasicMailbox<N>) {
        let mut normals = self.normals.lock().unwrap();
        if let Some(mailbox) = normals.insert(addr, mailbox) {
            mailbox.close();
        }
    }

    pub fn register_all(&self, mailboxes: Vec<(u64, BasicMailbox<N>)>) {
        let mut normals = self.normals.lock().unwrap();
        normals.reserve(mailboxes.len());
        for (addr, mailbox) in mailboxes {
            if let Some(m) = normals.insert(addr, mailbox) {
                m.close();
            }
        }
    }

    /// Get the mailbox of specified address.
    pub fn mailbox(&self, addr: u64) -> Option<Mailbox<N, Ns>> {
        let res = self.check_do(addr, |mailbox| {
            if mailbox.is_connected() {
                Some(Mailbox::new(mailbox.clone(), self.normal_scheduler.clone()))
            } else {
                None
            }
        });
        match res {
            CheckDoResult::Valid(r) => Some(r),
            _ => None,
        }
    }

    /// Get the mailbox of control fsm.
    pub fn control_mailbox(&self) -> Mailbox<C, Cs> {
        Mailbox::new(self.control_box.clone(), self.control_scheduler.clone())
    }

    /// Try to slightlike a message to specified address.
    ///
    /// If Either::Left is returned, then the message is sent. Otherwise,
    /// it indicates mailbox is not found.
    #[inline]
    pub fn try_slightlike(
        &self,
        addr: u64,
        msg: N::Message,
    ) -> Either<Result<(), TrySlightlikeError<N::Message>>, N::Message> {
        let mut msg = Some(msg);
        let res = self.check_do(addr, |mailbox| {
            let m = msg.take().unwrap();
            match mailbox.try_slightlike(m, &self.normal_scheduler) {
                Ok(()) => Some(Ok(())),
                r @ Err(TrySlightlikeError::Full(_)) => {
                    // TODO: report channel full
                    Some(r)
                }
                Err(TrySlightlikeError::Disconnected(m)) => {
                    msg = Some(m);
                    None
                }
            }
        });
        match res {
            CheckDoResult::Valid(r) => Either::Left(r),
            CheckDoResult::Invalid => Either::Left(Err(TrySlightlikeError::Disconnected(msg.unwrap()))),
            CheckDoResult::NotExist => Either::Right(msg.unwrap()),
        }
    }

    /// Slightlike the message to specified address.
    #[inline]
    pub fn slightlike(&self, addr: u64, msg: N::Message) -> Result<(), TrySlightlikeError<N::Message>> {
        match self.try_slightlike(addr, msg) {
            Either::Left(res) => res,
            Either::Right(m) => Err(TrySlightlikeError::Disconnected(m)),
        }
    }

    /// Force slightlikeing message to specified address despite the capacity
    /// limit of mailbox.
    #[inline]
    pub fn force_slightlike(&self, addr: u64, msg: N::Message) -> Result<(), SlightlikeError<N::Message>> {
        match self.slightlike(addr, msg) {
            Ok(()) => Ok(()),
            Err(TrySlightlikeError::Full(m)) => {
                let caches = unsafe { &mut *self.caches.as_ptr() };
                caches
                    .get(&addr)
                    .unwrap()
                    .force_slightlike(m, &self.normal_scheduler)
            }
            Err(TrySlightlikeError::Disconnected(m)) => Err(SlightlikeError(m)),
        }
    }

    /// Force slightlikeing message to control fsm.
    #[inline]
    pub fn slightlike_control(&self, msg: C::Message) -> Result<(), TrySlightlikeError<C::Message>> {
        match self.control_box.try_slightlike(msg, &self.control_scheduler) {
            Ok(()) => Ok(()),
            r @ Err(TrySlightlikeError::Full(_)) => {
                // TODO: record metrics.
                r
            }
            r => r,
        }
    }

    /// Try to notify all normal fsm a message.
    pub fn broadcast_normal(&self, mut msg_gen: impl FnMut() -> N::Message) {
        let mailboxes = self.normals.lock().unwrap();
        for mailbox in mailboxes.values() {
            let _ = mailbox.force_slightlike(msg_gen(), &self.normal_scheduler);
        }
    }

    /// Try to notify all fsm that the cluster is being shutdown.
    pub fn broadcast_shutdown(&self) {
        info!("broadcasting shutdown");
        self.shutdown.store(true, Ordering::SeqCst);
        unsafe { &mut *self.caches.as_ptr() }.clear();
        let mut mailboxes = self.normals.lock().unwrap();
        for (addr, mailbox) in mailboxes.drain() {
            debug!("[brane {}] shutdown mailbox", addr);
            mailbox.close();
        }
        self.control_box.close();
        self.normal_scheduler.shutdown();
        self.control_scheduler.shutdown();
    }

    /// Close the mailbox of address.
    pub fn close(&self, addr: u64) {
        info!("[brane {}] shutdown mailbox", addr);
        unsafe { &mut *self.caches.as_ptr() }.remove(&addr);
        let mut mailboxes = self.normals.lock().unwrap();
        if let Some(mb) = mailboxes.remove(&addr) {
            mb.close();
        }
    }
}

impl<N: Fsm, C: Fsm, Ns: Clone, Cs: Clone> Clone for Router<N, C, Ns, Cs> {
    fn clone(&self) -> Router<N, C, Ns, Cs> {
        Router {
            normals: self.normals.clone(),
            caches: Cell::new(LruCache::with_capacity_and_sample(1024, 7)),
            control_box: self.control_box.clone(),
            // These two schedulers should be unified as single one. However
            // it's not possible to write FsmScheduler<Fsm=C> + FsmScheduler<Fsm=N>
            // for now.
            normal_scheduler: self.normal_scheduler.clone(),
            control_scheduler: self.control_scheduler.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}
