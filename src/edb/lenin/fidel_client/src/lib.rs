// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.
#![feature(min_specialization)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
extern crate hex;
extern crate ekvproto;
#[macro_use(fail_point)]
extern crate fail;

#[allow(unused_extern_crates)]
extern crate edb_alloc;
#[macro_use]
extern crate violetabftstore::interlock::;

mod client;
pub mod metrics;
mod util;

mod config;
pub mod errors;
pub use self::client::{DummyFidelClient, RpcClient};
pub use self::config::Config;
pub use self::errors::{Error, Result};
pub use self::util::validate_lightlikepoints;
pub use self::util::RECONNECT_INTERVAL_SEC;

use std::ops::Deref;
use std::sync::{Arc, RwLock};

use futures::future::BoxFuture;
use ekvproto::meta_timeshare;
use ekvproto::fidel_timeshare;
use ekvproto::replication_mode_timeshare::{BraneReplicationStatus, ReplicationStatus};
use semver::{SemVerError, Version};
use violetabftstore::interlock::::time::UnixSecs;
use txn_types::TimeStamp;

pub type Key = Vec<u8>;
pub type FidelFuture<T> = BoxFuture<'static, Result<T>>;

#[derive(Default, Clone)]
pub struct BraneStat {
    pub down_peers: Vec<fidel_timeshare::PeerStats>,
    pub plightlikeing_peers: Vec<meta_timeshare::Peer>,
    pub written_bytes: u64,
    pub written_tuplespaceInstanton: u64,
    pub read_bytes: u64,
    pub read_tuplespaceInstanton: u64,
    pub approximate_size: u64,
    pub approximate_tuplespaceInstanton: u64,
    pub last_report_ts: UnixSecs,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BraneInfo {
    pub brane: meta_timeshare::Brane,
    pub leader: Option<meta_timeshare::Peer>,
}

impl BraneInfo {
    pub fn new(brane: meta_timeshare::Brane, leader: Option<meta_timeshare::Peer>) -> BraneInfo {
        BraneInfo { brane, leader }
    }
}

impl Deref for BraneInfo {
    type Target = meta_timeshare::Brane;

    fn deref(&self) -> &Self::Target {
        &self.brane
    }
}

pub const INVALID_ID: u64 = 0;

/// FidelClient communicates with Placement Driver (FIDel).
/// Because now one FIDel only supports one cluster, so it is no need to pass
/// cluster id in trait interface every time, so passing the cluster id when
/// creating the FidelClient is enough and the FidelClient will use this cluster id
/// all the time.
pub trait FidelClient: lightlike + Sync {
    /// Returns the cluster ID.
    fn get_cluster_id(&self) -> Result<u64> {
        unimplemented!();
    }

    /// Creates the cluster with cluster ID, node, stores and first Brane.
    /// If the cluster is already bootstrapped, return ClusterBootstrapped error.
    /// When a node spacelikes, if it finds nothing in the node and
    /// cluster is not bootstrapped, it begins to create node, stores, first Brane
    /// and then call bootstrap_cluster to let FIDel know it.
    /// It may happen that multi nodes spacelike at same time to try to
    /// bootstrap, but only one can succeed, while others will fail
    /// and must remove their created local Brane data themselves.
    fn bootstrap_cluster(
        &self,
        _stores: meta_timeshare::CausetStore,
        _brane: meta_timeshare::Brane,
    ) -> Result<Option<ReplicationStatus>> {
        unimplemented!();
    }

    /// Returns whether the cluster is bootstrapped or not.
    ///
    /// Cluster must be bootstrapped when we use it, so when the
    /// node spacelikes, `is_cluster_bootstrapped` must be called,
    /// and panics if cluster was not bootstrapped.
    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        unimplemented!();
    }

    /// Allocates a unique positive id.
    fn alloc_id(&self) -> Result<u64> {
        unimplemented!();
    }

    /// Informs FIDel when the store spacelikes or some store information changes.
    fn put_store(&self, _store: meta_timeshare::CausetStore) -> Result<Option<ReplicationStatus>> {
        unimplemented!();
    }

    /// We don't need to support Brane and Peer put/delete,
    /// because FIDel knows all Brane and Peers itself:
    /// - For bootstrapping, FIDel knows first Brane with `bootstrap_cluster`.
    /// - For changing Peer, FIDel determines where to add a new Peer in some store
    ///   for this Brane.
    /// - For Brane splitting, FIDel determines the new Brane id and Peer id for the
    ///   split Brane.
    /// - For Brane merging, FIDel knows which two Branes will be merged and which Brane
    ///   and Peers will be removed.
    /// - For auto-balance, FIDel determines how to move the Brane from one store to another.

    /// Gets store information if it is not a tombstone store.
    fn get_store(&self, _store_id: u64) -> Result<meta_timeshare::CausetStore> {
        unimplemented!();
    }

    /// Gets all stores information.
    fn get_all_stores(&self, _exclude_tombstone: bool) -> Result<Vec<meta_timeshare::CausetStore>> {
        unimplemented!();
    }

    /// Gets cluster meta information.
    fn get_cluster_config(&self) -> Result<meta_timeshare::Cluster> {
        unimplemented!();
    }

    /// For route.
    /// Gets Brane which the key belongs to.
    fn get_brane(&self, _key: &[u8]) -> Result<meta_timeshare::Brane> {
        unimplemented!();
    }

    /// Gets Brane info which the key belongs to.
    fn get_brane_info(&self, _key: &[u8]) -> Result<BraneInfo> {
        unimplemented!();
    }

    /// Gets Brane by Brane id.
    fn get_brane_by_id(&self, _brane_id: u64) -> FidelFuture<Option<meta_timeshare::Brane>> {
        unimplemented!();
    }

    /// Brane's Leader uses this to heartbeat FIDel.
    fn brane_heartbeat(
        &self,
        _term: u64,
        _brane: meta_timeshare::Brane,
        _leader: meta_timeshare::Peer,
        _brane_stat: BraneStat,
        _replication_status: Option<BraneReplicationStatus>,
    ) -> FidelFuture<()> {
        unimplemented!();
    }

    /// Gets a stream of Brane heartbeat response.
    ///
    /// Please note that this method should only be called once.
    fn handle_brane_heartbeat_response<F>(&self, _store_id: u64, _f: F) -> FidelFuture<()>
    where
        Self: Sized,
        F: Fn(fidel_timeshare::BraneHeartbeatResponse) + lightlike + 'static,
    {
        unimplemented!();
    }

    /// Asks FIDel for split. FIDel returns the newly split Brane id.
    fn ask_split(&self, _brane: meta_timeshare::Brane) -> FidelFuture<fidel_timeshare::AskSplitResponse> {
        unimplemented!();
    }

    /// Asks FIDel for batch split. FIDel returns the newly split Brane ids.
    fn ask_batch_split(
        &self,
        _brane: meta_timeshare::Brane,
        _count: usize,
    ) -> FidelFuture<fidel_timeshare::AskBatchSplitResponse> {
        unimplemented!();
    }

    /// lightlikes store statistics regularly.
    fn store_heartbeat(&self, _stats: fidel_timeshare::StoreStats) -> FidelFuture<fidel_timeshare::StoreHeartbeatResponse> {
        unimplemented!();
    }

    /// Reports FIDel the split Brane.
    fn report_batch_split(&self, _branes: Vec<meta_timeshare::Brane>) -> FidelFuture<()> {
        unimplemented!();
    }

    /// Scatters the Brane across the cluster.
    fn scatter_brane(&self, _: BraneInfo) -> Result<()> {
        unimplemented!();
    }

    /// Registers a handler to the client, which will be invoked after reconnecting to FIDel.
    ///
    /// Please note that this method should only be called once.
    fn handle_reconnect<F: Fn() + Sync + lightlike + 'static>(&self, _: F)
    where
        Self: Sized,
    {
    }

    fn get_gc_safe_point(&self) -> FidelFuture<u64> {
        unimplemented!();
    }

    /// Gets store state if it is not a tombstone store.
    fn get_store_stats(&self, _store_id: u64) -> Result<fidel_timeshare::StoreStats> {
        unimplemented!();
    }

    /// Gets current operator of the brane
    fn get_operator(&self, _brane_id: u64) -> Result<fidel_timeshare::GetOperatorResponse> {
        unimplemented!();
    }

    /// Gets a timestamp from FIDel.
    fn get_tso(&self) -> FidelFuture<TimeStamp> {
        unimplemented!()
    }
}

const REQUEST_TIMEOUT: u64 = 2; // 2s

/// Takes the peer address (for lightlikeing violetabft messages) from a store.
pub fn take_peer_address(store: &mut meta_timeshare::CausetStore) -> String {
    if !store.get_peer_address().is_empty() {
        store.take_peer_address()
    } else {
        store.take_address()
    }
}

#[derive(Clone, Default)]
pub struct ClusterVersion {
    version: Arc<RwLock<Option<Version>>>,
}

impl ClusterVersion {
    pub fn get(&self) -> Option<Version> {
        self.version.read().unwrap().clone()
    }

    fn set(&self, version: &str) -> std::result::Result<bool, SemVerError> {
        let new = Version::parse(version)?;
        let mut holder = self.version.write().unwrap();
        match &mut *holder {
            Some(ref mut old) if *old < new => {
                *old = new;
                Ok(true)
            }
            None => {
                *holder = Some(new);
                Ok(true)
            }
            Some(_) => Ok(false),
        }
    }

    /// Initialize a `ClusterVersion` as given `version`. Only should be used for tests.
    pub fn new(version: Version) -> Self {
        ClusterVersion {
            version: Arc::new(RwLock::new(Some(version))),
        }
    }
}
