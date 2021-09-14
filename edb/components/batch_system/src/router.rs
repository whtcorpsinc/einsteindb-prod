// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::fsm::{Fsm, FsmInterlock_Semaphore};
use crate::mailbox::{BasicMailbox, Mailbox};
use crossbeam::channel::{lightlikeError, TrylightlikeError};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::lru::LruCache;
use violetabftstore::interlock::::Either;

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
/// violetabftstore model or apply pushdown_causet in apply model. Second is control fsm,
/// which does some work that requires a global view of resources or creates
/// missing fsm for specified address. Normal fsm and control fsm can have
/// different interlock_semaphore, but this is not required.
pub struct Router<N: Fsm, C: Fsm, Ns, Cs> {
    normals: Arc<Mutex<HashMap<u64, BasicMailbox<N>>>>,
    caches: Cell<LruCache<u64, BasicMailbox<N>>>,
    pub(super) control_box: BasicMailbox<C>,
    // TODO: These two interlock_semaphores should be unified as single one. However
    // it's not possible to write FsmInterlock_Semaphore<Fsm=C> + FsmInterlock_Semaphore<Fsm=N>
    // for now.
    pub(crate) normal_interlock_semaphore: Ns,
    control_interlock_semaphore: Cs,

    // Indicates the router is shutdown down or not.
    shutdown: Arc<AtomicBool>,
}

impl<N, C, Ns, Cs> Router<N, C, Ns, Cs>
where
    N: Fsm,
    C: Fsm,
    Ns: FsmInterlock_Semaphore<Fsm = N> + Clone,
    Cs: FsmInterlock_Semaphore<Fsm = C> + Clone,
{
    pub(super) fn new(
        control_box: BasicMailbox<C>,
        normal_interlock_semaphore: Ns,
        control_interlock_semaphore: Cs,
    ) -> Router<N, C, Ns, Cs> {
        Router {
            normals: Arc::default(),
            caches: Cell::new(LruCache::with_capacity_and_sample(1024, 7)),
            control_box,
            normal_interlock_semaphore,
            control_interlock_semaphore,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The `Router` has been already shutdown or not.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// A helper function that tries to unify a common access TuringString to
    /// mailbox.
    ///
    /// Generally, when lightlikeing a message to a mailbox, cache should be
    /// check first, if not found, dagger should be acquired.
    ///
    /// Returns None means there is no mailbox inside the normal registry.
    /// Some(None) means there is expected mailbox inside the normal registry
    /// but it returns None after apply the given function. Some(Some) means
    /// the given function returns Some and cache is fideliod if it's invalid.
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
            let mut boxes = self.normals.dagger().unwrap();
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
        let mut normals = self.normals.dagger().unwrap();
        if let Some(mailbox) = normals.insert(addr, mailbox) {
            mailbox.close();
        }
    }

    pub fn register_all(&self, mailboxes: Vec<(u64, BasicMailbox<N>)>) {
        let mut normals = self.normals.dagger().unwrap();
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
                Some(Mailbox::new(mailbox.clone(), self.normal_interlock_semaphore.clone()))
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
        Mailbox::new(self.control_box.clone(), self.control_interlock_semaphore.clone())
    }

    /// Try to lightlike a message to specified address.
    ///
    /// If Either::Left is returned, then the message is sent. Otherwise,
    /// it indicates mailbox is not found.
    #[inline]
    pub fn try_lightlike(
        &self,
        addr: u64,
        msg: N::Message,
    ) -> Either<Result<(), TrylightlikeError<N::Message>>, N::Message> {
        let mut msg = Some(msg);
        let res = self.check_do(addr, |mailbox| {
            let m = msg.take().unwrap();
            match mailbox.try_lightlike(m, &self.normal_interlock_semaphore) {
                Ok(()) => Some(Ok(())),
                r @ Err(TrylightlikeError::Full(_)) => {
                    // TODO: report channel full
                    Some(r)
                }
                Err(TrylightlikeError::Disconnected(m)) => {
                    msg = Some(m);
                    None
                }
            }
        });
        match res {
            CheckDoResult::Valid(r) => Either::Left(r),
            CheckDoResult::Invalid => Either::Left(Err(TrylightlikeError::Disconnected(msg.unwrap()))),
            CheckDoResult::NotExist => Either::Right(msg.unwrap()),
        }
    }

    /// lightlike the message to specified address.
    #[inline]
    pub fn lightlike(&self, addr: u64, msg: N::Message) -> Result<(), TrylightlikeError<N::Message>> {
        match self.try_lightlike(addr, msg) {
            Either::Left(res) => res,
            Either::Right(m) => Err(TrylightlikeError::Disconnected(m)),
        }
    }

    /// Force lightlikeing message to specified address despite the capacity
    /// limit of mailbox.
    #[inline]
    pub fn force_lightlike(&self, addr: u64, msg: N::Message) -> Result<(), lightlikeError<N::Message>> {
        match self.lightlike(addr, msg) {
            Ok(()) => Ok(()),
            Err(TrylightlikeError::Full(m)) => {
                let caches = unsafe { &mut *self.caches.as_ptr() };
                caches
                    .get(&addr)
                    .unwrap()
                    .force_lightlike(m, &self.normal_interlock_semaphore)
            }
            Err(TrylightlikeError::Disconnected(m)) => Err(lightlikeError(m)),
        }
    }

    /// Force lightlikeing message to control fsm.
    #[inline]
    pub fn lightlike_control(&self, msg: C::Message) -> Result<(), TrylightlikeError<C::Message>> {
        match self.control_box.try_lightlike(msg, &self.control_interlock_semaphore) {
            Ok(()) => Ok(()),
            r @ Err(TrylightlikeError::Full(_)) => {
                // TODO: record metrics.
                r
            }
            r => r,
        }
    }

    /// Try to notify all normal fsm a message.
    pub fn broadcast_normal(&self, mut msg_gen: impl FnMut() -> N::Message) {
        let mailboxes = self.normals.dagger().unwrap();
        for mailbox in mailboxes.values() {
            let _ = mailbox.force_lightlike(msg_gen(), &self.normal_interlock_semaphore);
        }
    }

    /// Try to notify all fsm that the cluster is being shutdown.
    pub fn broadcast_shutdown(&self) {
        info!("broadcasting shutdown");
        self.shutdown.store(true, Ordering::SeqCst);
        unsafe { &mut *self.caches.as_ptr() }.clear();
        let mut mailboxes = self.normals.dagger().unwrap();
        for (addr, mailbox) in mailboxes.drain() {
            debug!("[brane {}] shutdown mailbox", addr);
            mailbox.close();
        }
        self.control_box.close();
        self.normal_interlock_semaphore.shutdown();
        self.control_interlock_semaphore.shutdown();
    }

    /// Close the mailbox of address.
    pub fn close(&self, addr: u64) {
        info!("[brane {}] shutdown mailbox", addr);
        unsafe { &mut *self.caches.as_ptr() }.remove(&addr);
        let mut mailboxes = self.normals.dagger().unwrap();
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
            // These two interlock_semaphores should be unified as single one. However
            // it's not possible to write FsmInterlock_Semaphore<Fsm=C> + FsmInterlock_Semaphore<Fsm=N>
            // for now.
            normal_interlock_semaphore: self.normal_interlock_semaphore.clone(),
            control_interlock_semaphore: self.control_interlock_semaphore.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}
