// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;

use crossbeam::{lightlikeError, TrylightlikeError};
use edb::{CausetEngine, VioletaBftEngine, Snapshot};
use ekvproto::violetabft_cmd_timeshare::VioletaBftCmdRequest;
use ekvproto::violetabft_server_timeshare::VioletaBftMessage;
use violetabft::SnapshotStatus;
use violetabftstore::interlock::::time::ThreadReadId;

use crate::store::fsm::VioletaBftRouter;
use crate::store::transport::{CasualRouter, ProposalRouter, StoreRouter};
use crate::store::{
    Callback, CasualMessage, LocalReader, PeerMsg, VioletaBftCommand, SignificantMsg, StoreMsg,
};
use crate::{DiscardReason, Error as VioletaBftStoreError, Result as VioletaBftStoreResult};

/// Routes messages to the violetabftstore.
pub trait VioletaBftStoreRouter<EK>:
    StoreRouter<EK> + ProposalRouter<EK::Snapshot> + CasualRouter<EK> + lightlike + Clone
where
    EK: CausetEngine,
{
    /// lightlikes VioletaBftMessage to local store.
    fn lightlike_violetabft_msg(&self, msg: VioletaBftMessage) -> VioletaBftStoreResult<()>;

    /// lightlikes a significant message. We should guarantee that the message can't be dropped.
    fn significant_lightlike(
        &self,
        brane_id: u64,
        msg: SignificantMsg<EK::Snapshot>,
    ) -> VioletaBftStoreResult<()>;

    /// Broadcast a message generated by `msg_gen` to all VioletaBft groups.
    fn broadcast_normal(&self, msg_gen: impl FnMut() -> PeerMsg<EK>);

    /// lightlike a casual message to the given brane.
    fn lightlike_casual_msg(&self, brane_id: u64, msg: CasualMessage<EK>) -> VioletaBftStoreResult<()> {
        <Self as CasualRouter<EK>>::lightlike(self, brane_id, msg)
    }

    /// lightlike a store message to the backlightlike violetabft batch system.
    fn lightlike_store_msg(&self, msg: StoreMsg<EK>) -> VioletaBftStoreResult<()> {
        <Self as StoreRouter<EK>>::lightlike(self, msg)
    }

    /// lightlikes VioletaBftCmdRequest to local store.
    fn lightlike_command(&self, req: VioletaBftCmdRequest, cb: Callback<EK::Snapshot>) -> VioletaBftStoreResult<()> {
        let brane_id = req.get_header().get_brane_id();
        let cmd = VioletaBftCommand::new(req, cb);
        <Self as ProposalRouter<EK::Snapshot>>::lightlike(self, cmd)
            .map_err(|e| handle_lightlike_error(brane_id, e))
    }

    /// Reports the peer being unreachable to the Brane.
    fn report_unreachable(&self, brane_id: u64, to_peer_id: u64) -> VioletaBftStoreResult<()> {
        let msg = SignificantMsg::Unreachable {
            brane_id,
            to_peer_id,
        };
        self.significant_lightlike(brane_id, msg)
    }

    /// Reports the lightlikeing snapshot status to the peer of the Brane.
    fn report_snapshot_status(
        &self,
        brane_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    ) -> VioletaBftStoreResult<()> {
        let msg = SignificantMsg::SnapshotStatus {
            brane_id,
            to_peer_id,
            status,
        };
        self.significant_lightlike(brane_id, msg)
    }

    /// Broadcast an `StoreUnreachable` event to all VioletaBft groups.
    fn broadcast_unreachable(&self, store_id: u64) {
        let _ = self.lightlike_store_msg(StoreMsg::StoreUnreachable { store_id });
    }

    /// Report a `StoreResolved` event to all VioletaBft groups.
    fn report_resolved(&self, store_id: u64, group_id: u64) {
        self.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::StoreResolved { store_id, group_id })
        })
    }
}

pub trait LocalReadRouter<EK>: lightlike + Clone
where
    EK: CausetEngine,
{
    fn read(
        &self,
        read_id: Option<ThreadReadId>,
        req: VioletaBftCmdRequest,
        cb: Callback<EK::Snapshot>,
    ) -> VioletaBftStoreResult<()>;

    fn release_snapshot_cache(&self);
}

#[derive(Clone)]
pub struct VioletaBftStoreBlackHole;

impl<EK: CausetEngine> CasualRouter<EK> for VioletaBftStoreBlackHole {
    fn lightlike(&self, _: u64, _: CasualMessage<EK>) -> VioletaBftStoreResult<()> {
        Ok(())
    }
}

impl<S: Snapshot> ProposalRouter<S> for VioletaBftStoreBlackHole {
    fn lightlike(&self, _: VioletaBftCommand<S>) -> std::result::Result<(), TrylightlikeError<VioletaBftCommand<S>>> {
        Ok(())
    }
}

impl<EK> StoreRouter<EK> for VioletaBftStoreBlackHole
where
    EK: CausetEngine,
{
    fn lightlike(&self, _: StoreMsg<EK>) -> VioletaBftStoreResult<()> {
        Ok(())
    }
}

impl<EK> VioletaBftStoreRouter<EK> for VioletaBftStoreBlackHole
where
    EK: CausetEngine,
{
    /// lightlikes VioletaBftMessage to local store.
    fn lightlike_violetabft_msg(&self, _: VioletaBftMessage) -> VioletaBftStoreResult<()> {
        Ok(())
    }

    /// lightlikes a significant message. We should guarantee that the message can't be dropped.
    fn significant_lightlike(&self, _: u64, _: SignificantMsg<EK::Snapshot>) -> VioletaBftStoreResult<()> {
        Ok(())
    }

    fn broadcast_normal(&self, _: impl FnMut() -> PeerMsg<EK>) {}
}

/// A router that routes messages to the violetabftstore
pub struct ServerVioletaBftStoreRouter<EK: CausetEngine, ER: VioletaBftEngine> {
    router: VioletaBftRouter<EK, ER>,
    local_reader: RefCell<LocalReader<VioletaBftRouter<EK, ER>, EK>>,
}

impl<EK: CausetEngine, ER: VioletaBftEngine> Clone for ServerVioletaBftStoreRouter<EK, ER> {
    fn clone(&self) -> Self {
        ServerVioletaBftStoreRouter {
            router: self.router.clone(),
            local_reader: self.local_reader.clone(),
        }
    }
}

impl<EK: CausetEngine, ER: VioletaBftEngine> ServerVioletaBftStoreRouter<EK, ER> {
    /// Creates a new router.
    pub fn new(
        router: VioletaBftRouter<EK, ER>,
        reader: LocalReader<VioletaBftRouter<EK, ER>, EK>,
    ) -> ServerVioletaBftStoreRouter<EK, ER> {
        let local_reader = RefCell::new(reader);
        ServerVioletaBftStoreRouter {
            router,
            local_reader,
        }
    }
}

impl<EK: CausetEngine, ER: VioletaBftEngine> StoreRouter<EK> for ServerVioletaBftStoreRouter<EK, ER> {
    fn lightlike(&self, msg: StoreMsg<EK>) -> VioletaBftStoreResult<()> {
        StoreRouter::lightlike(&self.router, msg)
    }
}

impl<EK: CausetEngine, ER: VioletaBftEngine> ProposalRouter<EK::Snapshot> for ServerVioletaBftStoreRouter<EK, ER> {
    fn lightlike(
        &self,
        cmd: VioletaBftCommand<EK::Snapshot>,
    ) -> std::result::Result<(), TrylightlikeError<VioletaBftCommand<EK::Snapshot>>> {
        ProposalRouter::lightlike(&self.router, cmd)
    }
}

impl<EK: CausetEngine, ER: VioletaBftEngine> CasualRouter<EK> for ServerVioletaBftStoreRouter<EK, ER> {
    fn lightlike(&self, brane_id: u64, msg: CasualMessage<EK>) -> VioletaBftStoreResult<()> {
        CasualRouter::lightlike(&self.router, brane_id, msg)
    }
}

impl<EK: CausetEngine, ER: VioletaBftEngine> VioletaBftStoreRouter<EK> for ServerVioletaBftStoreRouter<EK, ER> {
    fn lightlike_violetabft_msg(&self, msg: VioletaBftMessage) -> VioletaBftStoreResult<()> {
        VioletaBftStoreRouter::lightlike_violetabft_msg(&self.router, msg)
    }

    /// lightlikes a significant message. We should guarantee that the message can't be dropped.
    fn significant_lightlike(
        &self,
        brane_id: u64,
        msg: SignificantMsg<EK::Snapshot>,
    ) -> VioletaBftStoreResult<()> {
        VioletaBftStoreRouter::significant_lightlike(&self.router, brane_id, msg)
    }

    fn broadcast_normal(&self, msg_gen: impl FnMut() -> PeerMsg<EK>) {
        self.router.broadcast_normal(msg_gen)
    }
}

impl<EK: CausetEngine, ER: VioletaBftEngine> LocalReadRouter<EK> for ServerVioletaBftStoreRouter<EK, ER> {
    fn read(
        &self,
        read_id: Option<ThreadReadId>,
        req: VioletaBftCmdRequest,
        cb: Callback<EK::Snapshot>,
    ) -> VioletaBftStoreResult<()> {
        let mut local_reader = self.local_reader.borrow_mut();
        local_reader.read(read_id, req, cb);
        Ok(())
    }

    fn release_snapshot_cache(&self) {
        let mut local_reader = self.local_reader.borrow_mut();
        local_reader.release_snapshot_cache();
    }
}

#[inline]
pub fn handle_lightlike_error<T>(brane_id: u64, e: TrylightlikeError<T>) -> VioletaBftStoreError {
    match e {
        TrylightlikeError::Full(_) => VioletaBftStoreError::Transport(DiscardReason::Full),
        TrylightlikeError::Disconnected(_) => VioletaBftStoreError::BraneNotFound(brane_id),
    }
}

impl<EK: CausetEngine, ER: VioletaBftEngine> VioletaBftStoreRouter<EK> for VioletaBftRouter<EK, ER> {
    fn lightlike_violetabft_msg(&self, msg: VioletaBftMessage) -> VioletaBftStoreResult<()> {
        let brane_id = msg.get_brane_id();
        self.lightlike_violetabft_message(msg)
            .map_err(|e| handle_lightlike_error(brane_id, e))
    }

    fn significant_lightlike(
        &self,
        brane_id: u64,
        msg: SignificantMsg<EK::Snapshot>,
    ) -> VioletaBftStoreResult<()> {
        if let Err(lightlikeError(msg)) = self
            .router
            .force_lightlike(brane_id, PeerMsg::SignificantMsg(msg))
        {
            // TODO: panic here once we can detect system is shutting down reliably.
            error!("failed to lightlike significant msg"; "msg" => ?msg);
            return Err(VioletaBftStoreError::BraneNotFound(brane_id));
        }

        Ok(())
    }

    fn broadcast_normal(&self, msg_gen: impl FnMut() -> PeerMsg<EK>) {
        batch_system::Router::broadcast_normal(self, msg_gen)
    }
}
