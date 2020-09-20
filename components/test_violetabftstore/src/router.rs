// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use crossbeam::channel::TrySlightlikeError;
use engine_lmdb::{LmdbEngine, LmdbSnapshot};
use ekvproto::raft_serverpb::VioletaBftMessage;
use violetabftstore::errors::{Error as VioletaBftStoreError, Result as VioletaBftStoreResult};
use violetabftstore::router::{handle_slightlike_error, VioletaBftStoreRouter};
use violetabftstore::store::msg::{CasualMessage, PeerMsg, SignificantMsg};
use violetabftstore::store::{CasualRouter, ProposalRouter, VioletaBftCommand, StoreMsg, StoreRouter};
use einsteindb_util::collections::HashMap;
use einsteindb_util::mpsc::{loose_bounded, LooseBoundedSlightlikeer, Receiver};

#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct MockVioletaBftStoreRouter {
    slightlikeers: Arc<Mutex<HashMap<u64, LooseBoundedSlightlikeer<PeerMsg<LmdbEngine>>>>>,
}

impl MockVioletaBftStoreRouter {
    pub fn new() -> MockVioletaBftStoreRouter {
        MockVioletaBftStoreRouter {
            slightlikeers: Arc::default(),
        }
    }
    pub fn add_brane(&self, brane_id: u64, cap: usize) -> Receiver<PeerMsg<LmdbEngine>> {
        let (tx, rx) = loose_bounded(cap);
        self.slightlikeers.lock().unwrap().insert(brane_id, tx);
        rx
    }
}

impl StoreRouter<LmdbEngine> for MockVioletaBftStoreRouter {
    fn slightlike(&self, _: StoreMsg<LmdbEngine>) -> VioletaBftStoreResult<()> {
        unimplemented!();
    }
}

impl ProposalRouter<LmdbSnapshot> for MockVioletaBftStoreRouter {
    fn slightlike(
        &self,
        _: VioletaBftCommand<LmdbSnapshot>,
    ) -> std::result::Result<(), TrySlightlikeError<VioletaBftCommand<LmdbSnapshot>>> {
        unimplemented!();
    }
}

impl CasualRouter<LmdbEngine> for MockVioletaBftStoreRouter {
    fn slightlike(&self, brane_id: u64, msg: CasualMessage<LmdbEngine>) -> VioletaBftStoreResult<()> {
        let mut slightlikeers = self.slightlikeers.lock().unwrap();
        if let Some(tx) = slightlikeers.get_mut(&brane_id) {
            tx.try_slightlike(PeerMsg::CasualMessage(msg))
                .map_err(|e| handle_slightlike_error(brane_id, e))
        } else {
            Err(VioletaBftStoreError::BraneNotFound(brane_id))
        }
    }
}

impl VioletaBftStoreRouter<LmdbEngine> for MockVioletaBftStoreRouter {
    fn slightlike_raft_msg(&self, _: VioletaBftMessage) -> VioletaBftStoreResult<()> {
        unimplemented!()
    }

    fn significant_slightlike(
        &self,
        brane_id: u64,
        msg: SignificantMsg<LmdbSnapshot>,
    ) -> VioletaBftStoreResult<()> {
        let mut slightlikeers = self.slightlikeers.lock().unwrap();
        if let Some(tx) = slightlikeers.get_mut(&brane_id) {
            tx.force_slightlike(PeerMsg::SignificantMsg(msg)).unwrap();
            Ok(())
        } else {
            error!("failed to slightlike significant msg"; "msg" => ?msg);
            Err(VioletaBftStoreError::BraneNotFound(brane_id))
        }
    }

    fn broadcast_normal(&self, _: impl FnMut() -> PeerMsg<LmdbEngine>) {}
}
