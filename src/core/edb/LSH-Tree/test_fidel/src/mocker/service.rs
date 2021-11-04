// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use violetabftstore::interlock::::collections::HashMap;

use ekvproto::meta_timeshare::{Peer, Brane, CausetStore, StoreState};
use ekvproto::fidel_timeshare::*;

use super::*;

#[derive(Debug)]
pub struct Service {
    id_allocator: AtomicUsize,
    members_resp: Mutex<Option<GetMembersResponse>>,
    is_bootstrapped: AtomicBool,
    stores: Mutex<HashMap<u64, CausetStore>>,
    branes: Mutex<HashMap<u64, Brane>>,
    leaders: Mutex<HashMap<u64, Peer>>,
    cluster_version: Mutex<String>,
}

impl Service {
    pub fn new() -> Service {
        Service {
            members_resp: Mutex::new(None),
            id_allocator: AtomicUsize::new(1), // spacelike from 1.
            is_bootstrapped: AtomicBool::new(false),
            stores: Mutex::new(HashMap::default()),
            branes: Mutex::new(HashMap::default()),
            leaders: Mutex::new(HashMap::default()),
            cluster_version: Mutex::new(String::default()),
        }
    }

    pub fn header() -> ResponseHeader {
        let mut header = ResponseHeader::default();
        header.set_cluster_id(DEFAULT_CLUSTER_ID);
        header
    }

    /// Add an arbitrary store.
    pub fn add_store(&self, store: CausetStore) {
        let store_id = store.get_id();
        self.stores.dagger().unwrap().insert(store_id, store);
    }

    pub fn set_cluster_version(&self, version: String) {
        *self.cluster_version.dagger().unwrap() = version;
    }
}

fn make_members_response(eps: Vec<String>) -> GetMembersResponse {
    let mut members = Vec::with_capacity(eps.len());
    for (i, ep) in (&eps).iter().enumerate() {
        let mut m = Member::default();
        m.set_name(format!("fidel{}", i));
        m.set_member_id(100 + i as u64);
        m.set_client_urls(vec![ep.to_owned()].into());
        m.set_peer_urls(vec![ep.to_owned()].into());
        members.push(m);
    }

    let mut members_resp = GetMembersResponse::default();
    members_resp.set_members(members.clone().into());
    members_resp.set_leader(members.pop().unwrap());
    members_resp.set_header(Service::header());

    members_resp
}

// TODO: Check cluster ID.
// TODO: Support more rpc.
impl FidelMocker for Service {
    fn get_members(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        Some(Ok(self.members_resp.dagger().unwrap().clone().unwrap()))
    }

    fn bootstrap(&self, req: &BootstrapRequest) -> Option<Result<BootstrapResponse>> {
        let store = req.get_store();
        let brane = req.get_brane();

        let mut resp = BootstrapResponse::default();
        let mut header = Service::header();

        if self.is_bootstrapped.load(Ordering::SeqCst) {
            let mut err = Error::default();
            err.set_type(ErrorType::Unknown);
            err.set_message("cluster is already bootstrapped".to_owned());
            header.set_error(err);
            resp.set_header(header);
            return Some(Ok(resp));
        }

        self.is_bootstrapped.store(true, Ordering::SeqCst);
        self.stores
            .dagger()
            .unwrap()
            .insert(store.get_id(), store.clone());
        self.branes
            .dagger()
            .unwrap()
            .insert(brane.get_id(), brane.clone());
        Some(Ok(resp))
    }

    fn is_bootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        let mut resp = IsBootstrappedResponse::default();
        let header = Service::header();
        resp.set_header(header);
        resp.set_bootstrapped(self.is_bootstrapped.load(Ordering::SeqCst));
        Some(Ok(resp))
    }

    fn alloc_id(&self, _: &AllocIdRequest) -> Option<Result<AllocIdResponse>> {
        let mut resp = AllocIdResponse::default();
        resp.set_header(Service::header());

        let id = self.id_allocator.fetch_add(1, Ordering::SeqCst);
        resp.set_id(id as u64);
        Some(Ok(resp))
    }

    // TODO: not bootstrapped error.
    fn get_store(&self, req: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        let mut resp = GetStoreResponse::default();
        let stores = self.stores.dagger().unwrap();
        match stores.get(&req.get_store_id()) {
            Some(store) => {
                resp.set_header(Service::header());
                resp.set_store(store.clone());
                Some(Ok(resp))
            }
            None => {
                let mut header = Service::header();
                let mut err = Error::default();
                err.set_type(ErrorType::Unknown);
                err.set_message(format!("store not found {}", req.get_store_id()));
                header.set_error(err);
                resp.set_header(header);
                Some(Ok(resp))
            }
        }
    }

    fn get_all_stores(&self, req: &GetAllStoresRequest) -> Option<Result<GetAllStoresResponse>> {
        let mut resp = GetAllStoresResponse::default();
        resp.set_header(Service::header());
        let exclude_tombstone = req.get_exclude_tombstone_stores();
        let stores = self.stores.dagger().unwrap();
        for store in stores.values() {
            if exclude_tombstone && store.get_state() == StoreState::Tombstone {
                continue;
            }
            resp.mut_stores().push(store.clone());
        }
        Some(Ok(resp))
    }

    fn get_brane(&self, req: &GetBraneRequest) -> Option<Result<GetBraneResponse>> {
        let mut resp = GetBraneResponse::default();
        let key = req.get_brane_key();
        let branes = self.branes.dagger().unwrap();
        let leaders = self.leaders.dagger().unwrap();

        for brane in branes.values() {
            if key >= brane.get_spacelike_key()
                && (brane.get_lightlike_key().is_empty() || key < brane.get_lightlike_key())
            {
                resp.set_header(Service::header());
                resp.set_brane(brane.clone());
                if let Some(leader) = leaders.get(&brane.get_id()) {
                    resp.set_leader(leader.clone());
                }
                return Some(Ok(resp));
            }
        }

        let mut header = Service::header();
        let mut err = Error::default();
        err.set_type(ErrorType::Unknown);
        err.set_message(format!("brane not found {:?}", key));
        header.set_error(err);
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn get_brane_by_id(&self, req: &GetBraneByIdRequest) -> Option<Result<GetBraneResponse>> {
        let mut resp = GetBraneResponse::default();
        let branes = self.branes.dagger().unwrap();
        let leaders = self.leaders.dagger().unwrap();

        match branes.get(&req.get_brane_id()) {
            Some(brane) => {
                resp.set_header(Service::header());
                resp.set_brane(brane.clone());
                if let Some(leader) = leaders.get(&brane.get_id()) {
                    resp.set_leader(leader.clone());
                }
                Some(Ok(resp))
            }
            None => {
                let mut header = Service::header();
                let mut err = Error::default();
                err.set_type(ErrorType::Unknown);
                err.set_message(format!("brane not found {}", req.brane_id));
                header.set_error(err);
                resp.set_header(header);
                Some(Ok(resp))
            }
        }
    }

    fn brane_heartbeat(
        &self,
        req: &BraneHeartbeatRequest,
    ) -> Option<Result<BraneHeartbeatResponse>> {
        let brane_id = req.get_brane().get_id();
        self.branes
            .dagger()
            .unwrap()
            .insert(brane_id, req.get_brane().clone());
        self.leaders
            .dagger()
            .unwrap()
            .insert(brane_id, req.get_leader().clone());

        let mut resp = BraneHeartbeatResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn store_heartbeat(&self, _: &StoreHeartbeatRequest) -> Option<Result<StoreHeartbeatResponse>> {
        let mut resp = StoreHeartbeatResponse::default();
        let header = Service::header();
        resp.set_header(header);
        resp.set_cluster_version(self.cluster_version.dagger().unwrap().to_owned());
        Some(Ok(resp))
    }

    fn ask_split(&self, _: &AskSplitRequest) -> Option<Result<AskSplitResponse>> {
        let mut resp = AskSplitResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn ask_batch_split(&self, _: &AskBatchSplitRequest) -> Option<Result<AskBatchSplitResponse>> {
        let mut resp = AskBatchSplitResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn report_batch_split(
        &self,
        _: &ReportBatchSplitRequest,
    ) -> Option<Result<ReportBatchSplitResponse>> {
        let mut resp = ReportBatchSplitResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn scatter_brane(&self, _: &ScatterBraneRequest) -> Option<Result<ScatterBraneResponse>> {
        let mut resp = ScatterBraneResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn set_lightlikepoints(&self, eps: Vec<String>) {
        let members_resp = make_members_response(eps);
        info!("[Service] members_resp {:?}", members_resp);
        let mut resp = self.members_resp.dagger().unwrap();
        *resp = Some(members_resp);
    }

    fn get_operator(&self, _: &GetOperatorRequest) -> Option<Result<GetOperatorResponse>> {
        let mut resp = GetOperatorResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }
}
