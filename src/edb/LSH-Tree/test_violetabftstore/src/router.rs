//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use crossbeam::channel::TrylightlikeError;
use engine_lmdb::{LmdbEngine, LmdbSnapshot};
use ekvproto::violetabft_server_timeshare::VioletaBftMessage;
use violetabftstore::errors::{Error as VioletaBftStoreError, Result as VioletaBftStoreResult};
use violetabftstore::router::{handle_lightlike_error, VioletaBftStoreRouter};
use violetabftstore::store::msg::{CasualMessage, PeerMsg, SignificantMsg};
use violetabftstore::store::{CasualRouter, ProposalRouter, VioletaBftCommand, StoreMsg, StoreRouter};
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::mpsc::{loose_bounded, LooseBoundedlightlikeer, Receiver};

#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct MockVioletaBftStoreRouter {
    lightlikeers: Arc<Mutex<HashMap<u64, LooseBoundedlightlikeer<PeerMsg<LmdbEngine>>>>>,
}

impl MockVioletaBftStoreRouter {
    pub fn new() -> MockVioletaBftStoreRouter {
        MockVioletaBftStoreRouter {
            lightlikeers: Arc::default(),
        }
    }
    pub fn add_brane(&self, brane_id: u64, cap: usize) -> Receiver<PeerMsg<LmdbEngine>> {
        let (tx, rx) = loose_bounded(cap);
        self.lightlikeers.dagger().unwrap().insert(brane_id, tx);
        rx
    }
}

impl StoreRouter<LmdbEngine> for MockVioletaBftStoreRouter {
    fn lightlike(&self, _: StoreMsg<LmdbEngine>) -> VioletaBftStoreResult<()> {
        unimplemented!();
    }
}

impl ProposalRouter<LmdbSnapshot> for MockVioletaBftStoreRouter {
    fn lightlike(
        &self,
        _: VioletaBftCommand<LmdbSnapshot>,
    ) -> std::result::Result<(), TrylightlikeError<VioletaBftCommand<LmdbSnapshot>>> {
        unimplemented!();
    }
}

impl CasualRouter<LmdbEngine> for MockVioletaBftStoreRouter {
    fn lightlike(&self, brane_id: u64, msg: CasualMessage<LmdbEngine>) -> VioletaBftStoreResult<()> {
        let mut lightlikeers = self.lightlikeers.dagger().unwrap();
        if let Some(tx) = lightlikeers.get_mut(&brane_id) {
            tx.try_lightlike(PeerMsg::CasualMessage(msg))
                .map_err(|e| handle_lightlike_error(brane_id, e))
        } else {
            Err(VioletaBftStoreError::BraneNotFound(brane_id))
        }
    }
}

impl VioletaBftStoreRouter<LmdbEngine> for MockVioletaBftStoreRouter {
    fn lightlike_violetabft_msg(&self, _: VioletaBftMessage) -> VioletaBftStoreResult<()> {
        unimplemented!()
    }

    fn significant_lightlike(
        &self,
        brane_id: u64,
        msg: SignificantMsg<LmdbSnapshot>,
    ) -> VioletaBftStoreResult<()> {
        let mut lightlikeers = self.lightlikeers.dagger().unwrap();
        if let Some(tx) = lightlikeers.get_mut(&brane_id) {
            tx.force_lightlike(PeerMsg::SignificantMsg(msg)).unwrap();
            Ok(())
        } else {
            error!("failed to lightlike significant msg"; "msg" => ?msg);
            Err(VioletaBftStoreError::BraneNotFound(brane_id))
        }
    }

    fn broadcast_normal(&self, _: impl FnMut() -> PeerMsg<LmdbEngine>) {}
}
