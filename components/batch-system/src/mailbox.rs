// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::fsm::{Fsm, FsmScheduler, FsmState};
use crossbeam::channel::{SlightlikeError, TrySlightlikeError};
use std::borrow::Cow;
use std::sync::Arc;
use einsteindb_util::mpsc;

/// A basic mailbox.
///
/// Every mailbox should have one and only one owner, who will receive all
/// messages sent to this mailbox.
///
/// When a message is sent to a mailbox, its owner will be checked whether it's
/// idle. An idle owner will be scheduled via `FsmScheduler` immediately, which
/// will drive the fsm to poll for messages.
pub struct BasicMailbox<Owner: Fsm> {
    slightlikeer: mpsc::LooseBoundedSlightlikeer<Owner::Message>,
    state: Arc<FsmState<Owner>>,
}

impl<Owner: Fsm> BasicMailbox<Owner> {
    #[inline]
    pub fn new(
        slightlikeer: mpsc::LooseBoundedSlightlikeer<Owner::Message>,
        fsm: Box<Owner>,
    ) -> BasicMailbox<Owner> {
        BasicMailbox {
            slightlikeer,
            state: Arc::new(FsmState::new(fsm)),
        }
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.slightlikeer.is_slightlikeer_connected()
    }

    pub(crate) fn release(&self, fsm: Box<Owner>) {
        self.state.release(fsm)
    }

    pub(crate) fn take_fsm(&self) -> Option<Box<Owner>> {
        self.state.take_fsm()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.slightlikeer.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slightlikeer.is_empty()
    }

    /// Force slightlikeing a message despite the capacity limit on channel.
    #[inline]
    pub fn force_slightlike<S: FsmScheduler<Fsm = Owner>>(
        &self,
        msg: Owner::Message,
        scheduler: &S,
    ) -> Result<(), SlightlikeError<Owner::Message>> {
        self.slightlikeer.force_slightlike(msg)?;
        self.state.notify(scheduler, Cow::Borrowed(self));
        Ok(())
    }

    /// Try to slightlike a message to the mailbox.
    ///
    /// If there are too many plightlikeing messages, function may fail.
    #[inline]
    pub fn try_slightlike<S: FsmScheduler<Fsm = Owner>>(
        &self,
        msg: Owner::Message,
        scheduler: &S,
    ) -> Result<(), TrySlightlikeError<Owner::Message>> {
        self.slightlikeer.try_slightlike(msg)?;
        self.state.notify(scheduler, Cow::Borrowed(self));
        Ok(())
    }

    /// Close the mailbox explicitly.
    #[inline]
    pub(crate) fn close(&self) {
        self.slightlikeer.close_slightlikeer();
        self.state.clear();
    }
}

impl<Owner: Fsm> Clone for BasicMailbox<Owner> {
    #[inline]
    fn clone(&self) -> BasicMailbox<Owner> {
        BasicMailbox {
            slightlikeer: self.slightlikeer.clone(),
            state: self.state.clone(),
        }
    }
}

/// A more high level mailbox.
pub struct Mailbox<Owner, Scheduler>
where
    Owner: Fsm,
    Scheduler: FsmScheduler<Fsm = Owner>,
{
    mailbox: BasicMailbox<Owner>,
    scheduler: Scheduler,
}

impl<Owner, Scheduler> Mailbox<Owner, Scheduler>
where
    Owner: Fsm,
    Scheduler: FsmScheduler<Fsm = Owner>,
{
    pub fn new(mailbox: BasicMailbox<Owner>, scheduler: Scheduler) -> Mailbox<Owner, Scheduler> {
        Mailbox { mailbox, scheduler }
    }

    /// Force slightlikeing a message despite channel capacity limit.
    #[inline]
    pub fn force_slightlike(&self, msg: Owner::Message) -> Result<(), SlightlikeError<Owner::Message>> {
        self.mailbox.force_slightlike(msg, &self.scheduler)
    }

    /// Try to slightlike a message.
    #[inline]
    pub fn try_slightlike(&self, msg: Owner::Message) -> Result<(), TrySlightlikeError<Owner::Message>> {
        self.mailbox.try_slightlike(msg, &self.scheduler)
    }
}
