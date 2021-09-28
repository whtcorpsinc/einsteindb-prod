// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use crate::store::{CasualMessage, PeerMsg, VioletaBftCommand, VioletaBftRouter, StoreMsg};
use crate::{DiscardReason, Error, Result};
use crossbeam::TrylightlikeError;
use edb::{CausetEngine, VioletaBftEngine, Snapshot};
use ekvproto::violetabft_server_timeshare::VioletaBftMessage;
use std::sync::mpsc;

/// Transports messages between different VioletaBft peers.
pub trait Transport: lightlike + Clone {
    fn lightlike(&mut self, msg: VioletaBftMessage) -> Result<()>;

    fn flush(&mut self);
}

/// Routes message to target brane.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait CasualRouter<EK>
where
    EK: CausetEngine,
{
    fn lightlike(&self, brane_id: u64, msg: CasualMessage<EK>) -> Result<()>;
}

/// Routes proposal to target brane.
pub trait ProposalRouter<S>
where
    S: Snapshot,
{
    fn lightlike(&self, cmd: VioletaBftCommand<S>) -> std::result::Result<(), TrylightlikeError<VioletaBftCommand<S>>>;
}

/// Routes message to store FSM.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait StoreRouter<EK>
where
    EK: CausetEngine,
{
    fn lightlike(&self, msg: StoreMsg<EK>) -> Result<()>;
}

impl<EK, ER> CasualRouter<EK> for VioletaBftRouter<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    #[inline]
    fn lightlike(&self, brane_id: u64, msg: CasualMessage<EK>) -> Result<()> {
        match self.router.lightlike(brane_id, PeerMsg::CasualMessage(msg)) {
            Ok(()) => Ok(()),
            Err(TrylightlikeError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrylightlikeError::Disconnected(_)) => Err(Error::BraneNotFound(brane_id)),
        }
    }
}

impl<EK, ER> ProposalRouter<EK::Snapshot> for VioletaBftRouter<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    #[inline]
    fn lightlike(
        &self,
        cmd: VioletaBftCommand<EK::Snapshot>,
    ) -> std::result::Result<(), TrylightlikeError<VioletaBftCommand<EK::Snapshot>>> {
        self.lightlike_violetabft_command(cmd)
    }
}

impl<EK, ER> StoreRouter<EK> for VioletaBftRouter<EK, ER>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
{
    #[inline]
    fn lightlike(&self, msg: StoreMsg<EK>) -> Result<()> {
        match self.lightlike_control(msg) {
            Ok(()) => Ok(()),
            Err(TrylightlikeError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrylightlikeError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
        }
    }
}

impl<EK> CasualRouter<EK> for mpsc::Synclightlikeer<(u64, CasualMessage<EK>)>
where
    EK: CausetEngine,
{
    fn lightlike(&self, brane_id: u64, msg: CasualMessage<EK>) -> Result<()> {
        match self.try_lightlike((brane_id, msg)) {
            Ok(()) => Ok(()),
            Err(mpsc::TrylightlikeError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
            Err(mpsc::TrylightlikeError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
        }
    }
}

impl<S: Snapshot> ProposalRouter<S> for mpsc::Synclightlikeer<VioletaBftCommand<S>> {
    fn lightlike(&self, cmd: VioletaBftCommand<S>) -> std::result::Result<(), TrylightlikeError<VioletaBftCommand<S>>> {
        match self.try_lightlike(cmd) {
            Ok(()) => Ok(()),
            Err(mpsc::TrylightlikeError::Disconnected(cmd)) => Err(TrylightlikeError::Disconnected(cmd)),
            Err(mpsc::TrylightlikeError::Full(cmd)) => Err(TrylightlikeError::Full(cmd)),
        }
    }
}

impl<EK> StoreRouter<EK> for mpsc::lightlikeer<StoreMsg<EK>>
where
    EK: CausetEngine,
{
    fn lightlike(&self, msg: StoreMsg<EK>) -> Result<()> {
        match self.lightlike(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::lightlikeError(_)) => Err(Error::Transport(DiscardReason::Disconnected)),
        }
    }
}
