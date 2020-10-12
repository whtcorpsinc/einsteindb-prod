// Copyright 2016 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::store::{CasualMessage, PeerMsg, VioletaBftCommand, VioletaBftRouter, StoreMsg};
use crate::{DiscardReason, Error, Result};
use crossbeam::TrySlightlikeError;
use engine_promises::{KvEngine, VioletaBftEngine, Snapshot};
use ekvproto::violetabft_serverpb::VioletaBftMessage;
use std::sync::mpsc;

/// Transports messages between different VioletaBft peers.
pub trait Transport: Slightlike + Clone {
    fn slightlike(&mut self, msg: VioletaBftMessage) -> Result<()>;

    fn flush(&mut self);
}

/// Routes message to target brane.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait CasualRouter<EK>
where
    EK: KvEngine,
{
    fn slightlike(&self, brane_id: u64, msg: CasualMessage<EK>) -> Result<()>;
}

/// Routes proposal to target brane.
pub trait ProposalRouter<S>
where
    S: Snapshot,
{
    fn slightlike(&self, cmd: VioletaBftCommand<S>) -> std::result::Result<(), TrySlightlikeError<VioletaBftCommand<S>>>;
}

/// Routes message to store FSM.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait StoreRouter<EK>
where
    EK: KvEngine,
{
    fn slightlike(&self, msg: StoreMsg<EK>) -> Result<()>;
}

impl<EK, ER> CasualRouter<EK> for VioletaBftRouter<EK, ER>
where
    EK: KvEngine,
    ER: VioletaBftEngine,
{
    #[inline]
    fn slightlike(&self, brane_id: u64, msg: CasualMessage<EK>) -> Result<()> {
        match self.router.slightlike(brane_id, PeerMsg::CasualMessage(msg)) {
            Ok(()) => Ok(()),
            Err(TrySlightlikeError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrySlightlikeError::Disconnected(_)) => Err(Error::BraneNotFound(brane_id)),
        }
    }
}

impl<EK, ER> ProposalRouter<EK::Snapshot> for VioletaBftRouter<EK, ER>
where
    EK: KvEngine,
    ER: VioletaBftEngine,
{
    #[inline]
    fn slightlike(
        &self,
        cmd: VioletaBftCommand<EK::Snapshot>,
    ) -> std::result::Result<(), TrySlightlikeError<VioletaBftCommand<EK::Snapshot>>> {
        self.slightlike_violetabft_command(cmd)
    }
}

impl<EK, ER> StoreRouter<EK> for VioletaBftRouter<EK, ER>
where
    EK: KvEngine,
    ER: VioletaBftEngine,
{
    #[inline]
    fn slightlike(&self, msg: StoreMsg<EK>) -> Result<()> {
        match self.slightlike_control(msg) {
            Ok(()) => Ok(()),
            Err(TrySlightlikeError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrySlightlikeError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
        }
    }
}

impl<EK> CasualRouter<EK> for mpsc::SyncSlightlikeer<(u64, CasualMessage<EK>)>
where
    EK: KvEngine,
{
    fn slightlike(&self, brane_id: u64, msg: CasualMessage<EK>) -> Result<()> {
        match self.try_slightlike((brane_id, msg)) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySlightlikeError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
            Err(mpsc::TrySlightlikeError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
        }
    }
}

impl<S: Snapshot> ProposalRouter<S> for mpsc::SyncSlightlikeer<VioletaBftCommand<S>> {
    fn slightlike(&self, cmd: VioletaBftCommand<S>) -> std::result::Result<(), TrySlightlikeError<VioletaBftCommand<S>>> {
        match self.try_slightlike(cmd) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySlightlikeError::Disconnected(cmd)) => Err(TrySlightlikeError::Disconnected(cmd)),
            Err(mpsc::TrySlightlikeError::Full(cmd)) => Err(TrySlightlikeError::Full(cmd)),
        }
    }
}

impl<EK> StoreRouter<EK> for mpsc::Slightlikeer<StoreMsg<EK>>
where
    EK: KvEngine,
{
    fn slightlike(&self, msg: StoreMsg<EK>) -> Result<()> {
        match self.slightlike(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::SlightlikeError(_)) => Err(Error::Transport(DiscardReason::Disconnected)),
        }
    }
}
