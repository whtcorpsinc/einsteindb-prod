// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::result;

use ekvproto::fidel_timeshare::*;

mod bootstrap;
mod incompatible;
mod leader_change;
mod retry;
mod service;
mod split;

pub use self::bootstrap::AlreadyBootstrapped;
pub use self::incompatible::Incompatible;
pub use self::leader_change::LeaderChange;
pub use self::retry::{NotRetry, Retry};
pub use self::service::Service;
pub use self::split::Split;

pub const DEFAULT_CLUSTER_ID: u64 = 42;

pub type Result<T> = result::Result<T, String>;

pub trait FidelMocker {
    fn get_members(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        None
    }

    fn tso(&self, _: &TsoRequest) -> Option<Result<TsoResponse>> {
        None
    }

    fn bootstrap(&self, _: &BootstrapRequest) -> Option<Result<BootstrapResponse>> {
        None
    }

    fn is_bootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        None
    }

    fn alloc_id(&self, _: &AllocIdRequest) -> Option<Result<AllocIdResponse>> {
        None
    }

    fn get_store(&self, _: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        None
    }

    fn put_store(&self, _: &PutStoreRequest) -> Option<Result<PutStoreResponse>> {
        None
    }

    fn get_all_stores(&self, _: &GetAllStoresRequest) -> Option<Result<GetAllStoresResponse>> {
        None
    }

    fn store_heartbeat(&self, _: &StoreHeartbeatRequest) -> Option<Result<StoreHeartbeatResponse>> {
        None
    }

    fn brane_heartbeat(
        &self,
        _: &BraneHeartbeatRequest,
    ) -> Option<Result<BraneHeartbeatResponse>> {
        None
    }

    fn get_brane(&self, _: &GetBraneRequest) -> Option<Result<GetBraneResponse>> {
        None
    }

    fn get_brane_by_id(&self, _: &GetBraneByIdRequest) -> Option<Result<GetBraneResponse>> {
        None
    }

    fn ask_split(&self, _: &AskSplitRequest) -> Option<Result<AskSplitResponse>> {
        None
    }

    fn ask_batch_split(&self, _: &AskBatchSplitRequest) -> Option<Result<AskBatchSplitResponse>> {
        None
    }

    fn report_batch_split(
        &self,
        _: &ReportBatchSplitRequest,
    ) -> Option<Result<ReportBatchSplitResponse>> {
        None
    }

    fn get_cluster_config(
        &self,
        _: &GetClusterConfigRequest,
    ) -> Option<Result<GetClusterConfigResponse>> {
        None
    }

    fn put_cluster_config(
        &self,
        _: &PutClusterConfigRequest,
    ) -> Option<Result<PutClusterConfigResponse>> {
        None
    }

    fn scatter_brane(&self, _: &ScatterBraneRequest) -> Option<Result<ScatterBraneResponse>> {
        None
    }

    fn set_lightlikepoints(&self, _: Vec<String>) {}

    fn fidelio_gc_safe_point(
        &self,
        _: &fidelioGcSafePointRequest,
    ) -> Option<Result<fidelioGcSafePointResponse>> {
        None
    }

    fn get_gc_safe_point(
        &self,
        _: &GetGcSafePointRequest,
    ) -> Option<Result<GetGcSafePointResponse>> {
        None
    }

    fn get_operator(&self, _: &GetOperatorRequest) -> Option<Result<GetOperatorResponse>> {
        None
    }
}
