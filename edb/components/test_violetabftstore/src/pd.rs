// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::{cmp, thread};

use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedLightlikeValue};
use futures::compat::Future01CompatExt;
use futures::executor::block_on;
use futures::future::{err, ok, FutureExt};
use futures::{stream, stream::StreamExt};
use tokio_timer::timer::Handle;

use ekvproto::meta_timeshare;
use ekvproto::fidel_timeshare;
use ekvproto::replication_mode_timeshare::{
    DrAutoSyncState, BraneReplicationStatus, ReplicationMode, ReplicationStatus,
};
use violetabft::evioletabft_timeshare;

use fail::fail_point;
use tuplespaceInstanton::{self, data_key, enc_lightlike_key, enc_spacelike_key};
use fidel_client::{Error, Key, FidelClient, FidelFuture, BraneInfo, BraneStat, Result};
use violetabftstore::store::util::{check_key_in_brane, is_learner};
use violetabftstore::store::{INIT_EPOCH_CONF_VER, INIT_EPOCH_VER};
use violetabftstore::interlock::::collections::{HashMap, HashMapEntry, HashSet};
use violetabftstore::interlock::::time::UnixSecs;
use violetabftstore::interlock::::timer::GLOBAL_TIMER_HANDLE;
use violetabftstore::interlock::::{Either, HandyRwLock};
use txn_types::TimeStamp;

use super::*;

struct CausetStore {
    store: meta_timeshare::CausetStore,
    brane_ids: HashSet<u64>,
    lightlikeer: UnboundedLightlikeValue<fidel_timeshare::BraneHeartbeatResponse>,
    receiver: Option<UnboundedReceiver<fidel_timeshare::BraneHeartbeatResponse>>,
}

impl Default for CausetStore {
    fn default() -> CausetStore {
        let (tx, rx) = mpsc::unbounded();
        CausetStore {
            store: Default::default(),
            brane_ids: Default::default(),
            lightlikeer: tx,
            receiver: Some(rx),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SchedulePolicy {
    /// Repeat an Operator.
    Repeat(isize),
    /// Repeat till succcess.
    TillSuccess,
    /// Stop immediately.
    Stop,
}

impl SchedulePolicy {
    fn schedule(&mut self) -> bool {
        match *self {
            SchedulePolicy::Repeat(ref mut c) => {
                if *c > 0 {
                    *c -= 1;
                    true
                } else {
                    false
                }
            }
            SchedulePolicy::TillSuccess => true,
            SchedulePolicy::Stop => false,
        }
    }
}

#[derive(Clone, Debug)]
enum Operator {
    AddPeer {
        // Left: to be added.
        // Right: plightlikeing peer.
        peer: Either<meta_timeshare::Peer, meta_timeshare::Peer>,
        policy: SchedulePolicy,
    },
    RemovePeer {
        peer: meta_timeshare::Peer,
        policy: SchedulePolicy,
    },
    TransferLeader {
        peer: meta_timeshare::Peer,
        policy: SchedulePolicy,
    },
    MergeBrane {
        source_brane_id: u64,
        target_brane_id: u64,
        policy: Arc<RwLock<SchedulePolicy>>,
    },
    SplitBrane {
        brane_epoch: meta_timeshare::BraneEpoch,
        policy: fidel_timeshare::CheckPolicy,
        tuplespaceInstanton: Vec<Vec<u8>>,
    },
}

impl Operator {
    fn make_brane_heartbeat_response(
        &self,
        brane_id: u64,
        cluster: &Cluster,
    ) -> fidel_timeshare::BraneHeartbeatResponse {
        match *self {
            Operator::AddPeer { ref peer, .. } => {
                if let Either::Left(ref peer) = *peer {
                    let conf_change_type = if is_learner(peer) {
                        evioletabft_timeshare::ConfChangeType::AddLearnerNode
                    } else {
                        evioletabft_timeshare::ConfChangeType::AddNode
                    };
                    new_fidel_change_peer(conf_change_type, peer.clone())
                } else {
                    fidel_timeshare::BraneHeartbeatResponse::default()
                }
            }
            Operator::RemovePeer { ref peer, .. } => {
                new_fidel_change_peer(evioletabft_timeshare::ConfChangeType::RemoveNode, peer.clone())
            }
            Operator::TransferLeader { ref peer, .. } => new_fidel_transfer_leader(peer.clone()),
            Operator::MergeBrane {
                target_brane_id, ..
            } => {
                if target_brane_id == brane_id {
                    fidel_timeshare::BraneHeartbeatResponse::default()
                } else {
                    let brane = cluster.get_brane_by_id(target_brane_id).unwrap().unwrap();
                    if cluster.check_merge_target_integrity {
                        let mut all_exist = true;
                        for peer in brane.get_peers() {
                            if cluster.plightlikeing_peers.contains_key(&peer.get_id()) {
                                all_exist = false;
                                break;
                            }
                        }
                        if all_exist {
                            new_fidel_merge_brane(brane)
                        } else {
                            fidel_timeshare::BraneHeartbeatResponse::default()
                        }
                    } else {
                        new_fidel_merge_brane(brane)
                    }
                }
            }
            Operator::SplitBrane {
                policy, ref tuplespaceInstanton, ..
            } => new_split_brane(policy, tuplespaceInstanton.clone()),
        }
    }

    fn try_finished(
        &mut self,
        cluster: &Cluster,
        brane: &meta_timeshare::Brane,
        leader: &meta_timeshare::Peer,
    ) -> bool {
        match *self {
            Operator::AddPeer {
                ref mut peer,
                ref mut policy,
            } => {
                if !policy.schedule() {
                    return true;
                }
                let pr = peer.clone();
                if let Either::Left(pr) = pr {
                    if brane.get_peers().iter().any(|p| p == &pr) {
                        // EinsteinDB is adding the peer right now,
                        // set it to Right so it will not be scheduled again.
                        *peer = Either::Right(pr);
                    } else {
                        // EinsteinDB rejects AddNode.
                        return false;
                    }
                }
                if let Either::Right(ref pr) = *peer {
                    // Still adding peer?
                    return !cluster.plightlikeing_peers.contains_key(&pr.get_id());
                }
                unreachable!()
            }
            Operator::SplitBrane {
                ref brane_epoch, ..
            } => brane.get_brane_epoch() != brane_epoch,
            Operator::RemovePeer {
                ref peer,
                ref mut policy,
            } => brane.get_peers().iter().all(|p| p != peer) || !policy.schedule(),
            Operator::TransferLeader {
                ref peer,
                ref mut policy,
            } => leader == peer || !policy.schedule(),
            Operator::MergeBrane {
                source_brane_id,
                ref mut policy,
                ..
            } => {
                if cluster
                    .get_brane_by_id(source_brane_id)
                    .unwrap()
                    .is_none()
                {
                    *policy.write().unwrap() = SchedulePolicy::Stop;
                    false
                } else {
                    !policy.write().unwrap().schedule()
                }
            }
        }
    }
}

struct Cluster {
    meta: meta_timeshare::Cluster,
    stores: HashMap<u64, CausetStore>,
    branes: BTreeMap<Key, meta_timeshare::Brane>,
    brane_id_tuplespaceInstanton: HashMap<u64, Key>,
    brane_approximate_size: HashMap<u64, u64>,
    brane_approximate_tuplespaceInstanton: HashMap<u64, u64>,
    brane_last_report_ts: HashMap<u64, UnixSecs>,
    brane_last_report_term: HashMap<u64, u64>,
    base_id: AtomicUsize,

    store_stats: HashMap<u64, fidel_timeshare::StoreStats>,
    split_count: usize,

    // brane id -> Operator
    operators: HashMap<u64, Operator>,
    enable_peer_count_check: bool,

    // brane id -> leader
    leaders: HashMap<u64, meta_timeshare::Peer>,
    down_peers: HashMap<u64, fidel_timeshare::PeerStats>,
    plightlikeing_peers: HashMap<u64, meta_timeshare::Peer>,
    is_bootstraped: bool,

    gc_safe_point: u64,

    replication_status: Option<ReplicationStatus>,
    brane_replication_status: HashMap<u64, BraneReplicationStatus>,

    // for merging
    pub check_merge_target_integrity: bool,
}

impl Cluster {
    fn new(cluster_id: u64) -> Cluster {
        let mut meta = meta_timeshare::Cluster::default();
        meta.set_id(cluster_id);
        meta.set_max_peer_count(5);

        Cluster {
            meta,
            stores: HashMap::default(),
            branes: BTreeMap::new(),
            brane_id_tuplespaceInstanton: HashMap::default(),
            brane_approximate_size: HashMap::default(),
            brane_approximate_tuplespaceInstanton: HashMap::default(),
            brane_last_report_ts: HashMap::default(),
            brane_last_report_term: HashMap::default(),
            base_id: AtomicUsize::new(1000),
            store_stats: HashMap::default(),
            split_count: 0,
            operators: HashMap::default(),
            enable_peer_count_check: true,
            leaders: HashMap::default(),
            down_peers: HashMap::default(),
            plightlikeing_peers: HashMap::default(),
            is_bootstraped: false,

            gc_safe_point: 0,
            replication_status: None,
            brane_replication_status: HashMap::default(),
            check_merge_target_integrity: true,
        }
    }

    fn bootstrap(&mut self, store: meta_timeshare::CausetStore, brane: meta_timeshare::Brane) {
        // Now, some tests use multi peers in bootstrap,
        // disable this check.
        // TODO: enable this check later.
        // assert_eq!(brane.get_peers().len(), 1);
        let store_id = store.get_id();
        let mut s = CausetStore::default();
        s.store = store;

        s.brane_ids.insert(brane.get_id());

        self.stores.insert(store_id, s);

        self.add_brane(&brane);
        self.is_bootstraped = true;
    }

    fn set_bootstrap(&mut self, is_bootstraped: bool) {
        self.is_bootstraped = is_bootstraped
    }

    // We don't care cluster id here, so any value like 0 in tests is ok.
    fn alloc_id(&self) -> Result<u64> {
        Ok(self.base_id.fetch_add(1, Ordering::Relaxed) as u64)
    }

    fn put_store(&mut self, store: meta_timeshare::CausetStore) -> Result<()> {
        let store_id = store.get_id();
        // There is a race between put_store and handle_brane_heartbeat_response. If store id is
        // 0, it means it's a placeholder created by latter, we just need to fidelio the meta.
        // Otherwise we should overwrite it.
        if self
            .stores
            .get(&store_id)
            .map_or(true, |s| s.store.get_id() != 0)
        {
            let mut s = CausetStore::default();
            s.store = store;
            self.stores.insert(store_id, s);
        } else {
            self.stores.get_mut(&store_id).unwrap().store = store;
        }
        Ok(())
    }

    fn get_store(&self, store_id: u64) -> Result<meta_timeshare::CausetStore> {
        match self.stores.get(&store_id) {
            Some(s) if s.store.get_id() != 0 => Ok(s.store.clone()),
            _ => Err(box_err!("store {} not found", store_id)),
        }
    }

    fn get_all_stores(&self) -> Result<Vec<meta_timeshare::CausetStore>> {
        Ok(self
            .stores
            .values()
            .filter_map(|s| {
                if s.store.get_id() != 0 {
                    Some(s.store.clone())
                } else {
                    None
                }
            })
            .collect())
    }

    fn get_brane(&self, key: Vec<u8>) -> Option<meta_timeshare::Brane> {
        self.branes
            .cone((Excluded(key), Unbounded))
            .next()
            .map(|(_, brane)| brane.clone())
    }

    fn get_brane_by_id(&self, brane_id: u64) -> Result<Option<meta_timeshare::Brane>> {
        Ok(self
            .brane_id_tuplespaceInstanton
            .get(&brane_id)
            .and_then(|k| self.branes.get(k).cloned()))
    }

    fn get_brane_approximate_size(&self, brane_id: u64) -> Option<u64> {
        self.brane_approximate_size.get(&brane_id).cloned()
    }

    fn get_brane_approximate_tuplespaceInstanton(&self, brane_id: u64) -> Option<u64> {
        self.brane_approximate_tuplespaceInstanton.get(&brane_id).cloned()
    }

    fn get_brane_last_report_ts(&self, brane_id: u64) -> Option<UnixSecs> {
        self.brane_last_report_ts.get(&brane_id).cloned()
    }

    fn get_brane_last_report_term(&self, brane_id: u64) -> Option<u64> {
        self.brane_last_report_term.get(&brane_id).cloned()
    }

    fn get_stores(&self) -> Vec<meta_timeshare::CausetStore> {
        self.stores
            .values()
            .filter(|s| s.store.get_id() != 0)
            .map(|s| s.store.clone())
            .collect()
    }

    fn get_branes_number(&self) -> usize {
        self.branes.len()
    }

    fn add_brane(&mut self, brane: &meta_timeshare::Brane) {
        let lightlike_key = enc_lightlike_key(brane);
        assert!(self
            .branes
            .insert(lightlike_key.clone(), brane.clone())
            .is_none());
        assert!(self
            .brane_id_tuplespaceInstanton
            .insert(brane.get_id(), lightlike_key)
            .is_none());
    }

    fn remove_brane(&mut self, brane: &meta_timeshare::Brane) {
        let lightlike_key = enc_lightlike_key(brane);
        assert!(self.branes.remove(&lightlike_key).is_some());
        assert!(self.brane_id_tuplespaceInstanton.remove(&brane.get_id()).is_some());
    }

    fn get_overlap(&self, spacelike_key: Vec<u8>, lightlike_key: Vec<u8>) -> Vec<meta_timeshare::Brane> {
        self.branes
            .cone((Excluded(spacelike_key), Unbounded))
            .map(|(_, r)| r.clone())
            .take_while(|exist_brane| lightlike_key > enc_spacelike_key(exist_brane))
            .collect()
    }

    fn check_put_brane(&mut self, brane: meta_timeshare::Brane) -> Result<Vec<meta_timeshare::Brane>> {
        let (spacelike_key, lightlike_key, incoming_epoch) = (
            enc_spacelike_key(&brane),
            enc_lightlike_key(&brane),
            brane.get_brane_epoch().clone(),
        );
        assert!(lightlike_key > spacelike_key);
        let overlaps = self.get_overlap(spacelike_key, lightlike_key);
        for r in overlaps.iter() {
            if incoming_epoch.get_version() < r.get_brane_epoch().get_version() {
                return Err(box_err!("epoch {:?} is stale.", incoming_epoch));
            }
        }
        if let Some(o) = self.get_brane_by_id(brane.get_id())? {
            let exist_epoch = o.get_brane_epoch();
            if incoming_epoch.get_version() < exist_epoch.get_version()
                || incoming_epoch.get_conf_ver() < exist_epoch.get_conf_ver()
            {
                return Err(box_err!("epoch {:?} is stale.", incoming_epoch));
            }
        }
        Ok(overlaps)
    }

    fn handle_heartbeat(
        &mut self,
        mut brane: meta_timeshare::Brane,
        leader: meta_timeshare::Peer,
    ) -> Result<fidel_timeshare::BraneHeartbeatResponse> {
        let overlaps = self.check_put_brane(brane.clone())?;
        let same_brane = {
            let (ver, conf_ver) = (
                brane.get_brane_epoch().get_version(),
                brane.get_brane_epoch().get_conf_ver(),
            );
            overlaps.len() == 1
                && overlaps[0].get_id() == brane.get_id()
                && overlaps[0].get_brane_epoch().get_version() == ver
                && overlaps[0].get_brane_epoch().get_conf_ver() == conf_ver
        };
        if !same_brane {
            // remove overlap branes
            for r in overlaps {
                self.remove_brane(&r);
            }
            // remove stale brane that have same id but different key cone
            if let Some(o) = self.get_brane_by_id(brane.get_id())? {
                self.remove_brane(&o);
            }
            self.add_brane(&brane);
        }
        let resp = self
            .poll_heartbeat_responses(brane.clone(), leader.clone())
            .unwrap_or_else(|| {
                let mut resp = fidel_timeshare::BraneHeartbeatResponse::default();
                resp.set_brane_id(brane.get_id());
                resp.set_brane_epoch(brane.take_brane_epoch());
                resp.set_target_peer(leader);
                resp
            });
        Ok(resp)
    }

    // max_peer_count check, the default operator for handling brane heartbeat.
    fn handle_heartbeat_max_peer_count(
        &mut self,
        brane: &meta_timeshare::Brane,
        leader: &meta_timeshare::Peer,
    ) -> Option<Operator> {
        let max_peer_count = self.meta.get_max_peer_count() as usize;
        let peer_count = brane.get_peers().len();
        match peer_count.cmp(&max_peer_count) {
            cmp::Ordering::Less => {
                // find the first store which the brane has not covered.
                for store_id in self.stores.tuplespaceInstanton() {
                    if brane
                        .get_peers()
                        .iter()
                        .all(|x| x.get_store_id() != *store_id)
                    {
                        let peer = Either::Left(new_peer(*store_id, self.alloc_id().unwrap()));
                        let policy = SchedulePolicy::Repeat(1);
                        return Some(Operator::AddPeer { peer, policy });
                    }
                }
            }
            cmp::Ordering::Greater => {
                // find the first peer which not leader.
                let pos = brane
                    .get_peers()
                    .iter()
                    .position(|x| x.get_store_id() != leader.get_store_id())
                    .unwrap();
                let peer = brane.get_peers()[pos].clone();
                let policy = SchedulePolicy::Repeat(1);
                return Some(Operator::RemovePeer { peer, policy });
            }
            _ => {}
        }

        None
    }

    fn poll_heartbeat_responses_for(
        &mut self,
        store_id: u64,
    ) -> Vec<fidel_timeshare::BraneHeartbeatResponse> {
        let mut resps = vec![];
        for (brane_id, leader) in self.leaders.clone() {
            if leader.get_store_id() != store_id {
                continue;
            }
            if let Ok(Some(brane)) = self.get_brane_by_id(brane_id) {
                if let Some(resp) = self.poll_heartbeat_responses(brane, leader) {
                    resps.push(resp);
                }
            }
        }

        resps
    }

    fn poll_heartbeat_responses(
        &mut self,
        mut brane: meta_timeshare::Brane,
        leader: meta_timeshare::Peer,
    ) -> Option<fidel_timeshare::BraneHeartbeatResponse> {
        let brane_id = brane.get_id();
        let mut operator = None;
        if let Some(mut op) = self.operators.remove(&brane_id) {
            if !op.try_finished(self, &brane, &leader) {
                operator = Some(op);
            };
        } else if self.enable_peer_count_check {
            // There is no on-going operator, spacelike next round.
            operator = self.handle_heartbeat_max_peer_count(&brane, &leader);
        }

        let operator = operator?;
        debug!(
            "[brane {}] schedule {:?} to {:?}, brane: {:?}",
            brane_id, operator, leader, brane
        );

        let mut resp = operator.make_brane_heartbeat_response(brane.get_id(), self);
        self.operators.insert(brane_id, operator);
        resp.set_brane_id(brane_id);
        resp.set_brane_epoch(brane.take_brane_epoch());
        resp.set_target_peer(leader);
        Some(resp)
    }

    fn brane_heartbeat(
        &mut self,
        term: u64,
        brane: meta_timeshare::Brane,
        leader: meta_timeshare::Peer,
        brane_stat: BraneStat,
        replication_status: Option<BraneReplicationStatus>,
    ) -> Result<fidel_timeshare::BraneHeartbeatResponse> {
        for peer in brane.get_peers() {
            self.down_peers.remove(&peer.get_id());
            self.plightlikeing_peers.remove(&peer.get_id());
        }
        for peer in brane_stat.down_peers {
            self.down_peers.insert(peer.get_peer().get_id(), peer);
        }
        for p in brane_stat.plightlikeing_peers {
            self.plightlikeing_peers.insert(p.get_id(), p);
        }
        self.leaders.insert(brane.get_id(), leader.clone());

        self.brane_approximate_size
            .insert(brane.get_id(), brane_stat.approximate_size);
        self.brane_approximate_tuplespaceInstanton
            .insert(brane.get_id(), brane_stat.approximate_tuplespaceInstanton);
        self.brane_last_report_ts
            .insert(brane.get_id(), brane_stat.last_report_ts);
        self.brane_last_report_term.insert(brane.get_id(), term);

        if let Some(status) = replication_status {
            self.brane_replication_status.insert(brane.id, status);
        }

        self.handle_heartbeat(brane, leader)
    }

    fn set_gc_safe_point(&mut self, safe_point: u64) {
        self.gc_safe_point = safe_point;
    }

    fn get_gc_safe_point(&self) -> u64 {
        self.gc_safe_point
    }
}

fn check_stale_brane(brane: &meta_timeshare::Brane, check_brane: &meta_timeshare::Brane) -> Result<()> {
    let epoch = brane.get_brane_epoch();
    let check_epoch = check_brane.get_brane_epoch();

    if check_epoch.get_version() < epoch.get_version()
        || check_epoch.get_conf_ver() < epoch.get_conf_ver()
    {
        return Err(box_err!(
            "epoch not match {:?}, we are now {:?}",
            check_epoch,
            epoch
        ));
    }
    Ok(())
}

// For test when a node is already bootstraped the cluster with the first brane
pub fn bootstrap_with_first_brane(fidel_client: Arc<TestFidelClient>) -> Result<()> {
    let mut brane = meta_timeshare::Brane::default();
    brane.set_id(1);
    brane.set_spacelike_key(tuplespaceInstanton::EMPTY_KEY.to_vec());
    brane.set_lightlike_key(tuplespaceInstanton::EMPTY_KEY.to_vec());
    brane.mut_brane_epoch().set_version(INIT_EPOCH_VER);
    brane.mut_brane_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
    let peer = new_peer(1, 1);
    brane.mut_peers().push(peer);
    fidel_client.add_brane(&brane);
    fidel_client.set_bootstrap(true);
    Ok(())
}

pub struct TestFidelClient {
    cluster_id: u64,
    cluster: Arc<RwLock<Cluster>>,
    timer: Handle,
    is_incompatible: bool,
    tso: AtomicU64,
    trigger_tso_failure: AtomicBool,
}

impl TestFidelClient {
    pub fn new(cluster_id: u64, is_incompatible: bool) -> TestFidelClient {
        TestFidelClient {
            cluster_id,
            cluster: Arc::new(RwLock::new(Cluster::new(cluster_id))),
            timer: GLOBAL_TIMER_HANDLE.clone(),
            is_incompatible,
            tso: AtomicU64::new(1),
            trigger_tso_failure: AtomicBool::new(false),
        }
    }

    pub fn get_stores(&self) -> Result<Vec<meta_timeshare::CausetStore>> {
        Ok(self.cluster.rl().get_stores())
    }

    fn check_bootstrap(&self) -> Result<()> {
        if !self.is_cluster_bootstrapped().unwrap() {
            return Err(Error::ClusterNotBootstrapped(self.cluster_id));
        }

        Ok(())
    }

    fn is_branes_empty(&self) -> bool {
        self.cluster.rl().branes.is_empty()
    }

    fn schedule_operator(&self, brane_id: u64, op: Operator) {
        let mut cluster = self.cluster.wl();
        match cluster.operators.entry(brane_id) {
            HashMapEntry::Occupied(mut e) => {
                debug!(
                    "[brane {}] schedule operator {:?} and remove {:?}",
                    brane_id,
                    op,
                    e.get()
                );
                e.insert(op);
            }
            HashMapEntry::Vacant(e) => {
                debug!("[brane {}] schedule operator {:?}", brane_id, op);
                e.insert(op);
            }
        }
    }

    pub fn get_brane_epoch(&self, brane_id: u64) -> meta_timeshare::BraneEpoch {
        block_on(self.get_brane_by_id(brane_id))
            .unwrap()
            .unwrap()
            .take_brane_epoch()
    }

    pub fn get_branes_number(&self) -> usize {
        self.cluster.rl().get_branes_number()
    }

    pub fn disable_default_operator(&self) {
        self.cluster.wl().enable_peer_count_check = false;
    }

    pub fn enable_default_operator(&self) {
        self.cluster.wl().enable_peer_count_check = true;
    }

    pub fn must_have_peer(&self, brane_id: u64, peer: meta_timeshare::Peer) {
        for _ in 1..500 {
            sleep_ms(10);
            let brane = match block_on(self.get_brane_by_id(brane_id)).unwrap() {
                Some(brane) => brane,
                None => continue,
            };

            if let Some(p) = find_peer(&brane, peer.get_store_id()) {
                if p == &peer {
                    return;
                }
            }
        }
        let brane = block_on(self.get_brane_by_id(brane_id)).unwrap();
        panic!("brane {:?} has no peer {:?}", brane, peer);
    }

    pub fn must_none_peer(&self, brane_id: u64, peer: meta_timeshare::Peer) {
        for _ in 1..500 {
            sleep_ms(10);
            let brane = match block_on(self.get_brane_by_id(brane_id)).unwrap() {
                Some(brane) => brane,
                None => continue,
            };
            match find_peer(&brane, peer.get_store_id()) {
                None => return,
                Some(p) if p != &peer => return,
                _ => continue,
            }
        }
        let brane = block_on(self.get_brane_by_id(brane_id)).unwrap();
        panic!("brane {:?} has peer {:?}", brane, peer);
    }

    pub fn must_none_plightlikeing_peer(&self, peer: meta_timeshare::Peer) {
        for _ in 1..500 {
            sleep_ms(10);
            if self.cluster.rl().plightlikeing_peers.contains_key(&peer.get_id()) {
                continue;
            }
            return;
        }
        panic!("peer {:?} shouldn't be plightlikeing any more", peer);
    }

    pub fn add_brane(&self, brane: &meta_timeshare::Brane) {
        self.cluster.wl().add_brane(brane)
    }

    pub fn transfer_leader(&self, brane_id: u64, peer: meta_timeshare::Peer) {
        let op = Operator::TransferLeader {
            peer,
            policy: SchedulePolicy::TillSuccess,
        };
        self.schedule_operator(brane_id, op);
    }

    pub fn add_peer(&self, brane_id: u64, peer: meta_timeshare::Peer) {
        let op = Operator::AddPeer {
            peer: Either::Left(peer),
            policy: SchedulePolicy::TillSuccess,
        };
        self.schedule_operator(brane_id, op);
    }

    pub fn remove_peer(&self, brane_id: u64, peer: meta_timeshare::Peer) {
        let op = Operator::RemovePeer {
            peer,
            policy: SchedulePolicy::TillSuccess,
        };
        self.schedule_operator(brane_id, op);
    }

    pub fn split_brane(
        &self,
        mut brane: meta_timeshare::Brane,
        policy: fidel_timeshare::CheckPolicy,
        tuplespaceInstanton: Vec<Vec<u8>>,
    ) {
        let op = Operator::SplitBrane {
            brane_epoch: brane.take_brane_epoch(),
            policy,
            tuplespaceInstanton,
        };
        self.schedule_operator(brane.get_id(), op);
    }

    pub fn must_split_brane(
        &self,
        brane: meta_timeshare::Brane,
        policy: fidel_timeshare::CheckPolicy,
        tuplespaceInstanton: Vec<Vec<u8>>,
    ) {
        let expect_brane_count = self.get_branes_number()
            + if policy == fidel_timeshare::CheckPolicy::Usekey {
                tuplespaceInstanton.len()
            } else {
                1
            };
        self.split_brane(brane.clone(), policy, tuplespaceInstanton);
        for _ in 1..500 {
            sleep_ms(10);
            if self.get_branes_number() == expect_brane_count {
                return;
            }
        }
        panic!("brane {:?} is still not split.", brane);
    }

    pub fn must_add_peer(&self, brane_id: u64, peer: meta_timeshare::Peer) {
        self.add_peer(brane_id, peer.clone());
        self.must_have_peer(brane_id, peer);
    }

    pub fn must_remove_peer(&self, brane_id: u64, peer: meta_timeshare::Peer) {
        self.remove_peer(brane_id, peer.clone());
        self.must_none_peer(brane_id, peer);
    }

    pub fn merge_brane(&self, from: u64, target: u64) {
        let op = Operator::MergeBrane {
            source_brane_id: from,
            target_brane_id: target,
            policy: Arc::new(RwLock::new(SchedulePolicy::TillSuccess)),
        };
        self.schedule_operator(from, op.clone());
        self.schedule_operator(target, op);
    }

    pub fn must_merge(&self, from: u64, target: u64) {
        self.merge_brane(from, target);

        self.check_merged_timeout(from, Duration::from_secs(5));
    }

    pub fn check_merged(&self, from: u64) -> bool {
        block_on(self.get_brane_by_id(from)).unwrap().is_none()
    }

    pub fn check_merged_timeout(&self, from: u64, duration: Duration) {
        let timer = Instant::now();
        loop {
            let brane = block_on(self.get_brane_by_id(from)).unwrap();
            if let Some(r) = brane {
                if timer.elapsed() > duration {
                    panic!("brane {:?} is still not merged.", r);
                }
            } else {
                return;
            }
            sleep_ms(10);
        }
    }

    pub fn brane_leader_must_be(&self, brane_id: u64, peer: meta_timeshare::Peer) {
        for _ in 0..500 {
            sleep_ms(10);
            if let Some(p) = self.cluster.rl().leaders.get(&brane_id) {
                if *p == peer {
                    return;
                }
            }
        }
        panic!("brane {} must have leader: {:?}", brane_id, peer);
    }

    // check whether brane is split by split_key or not.
    pub fn check_split(&self, brane: &meta_timeshare::Brane, split_key: &[u8]) -> bool {
        // E.g, 1 [a, c) -> 1 [a, b) + 2 [b, c)
        // use a to find new [a, b).
        // use b to find new [b, c)
        let left = match self.get_brane(brane.get_spacelike_key()) {
            Err(_) => return false,
            Ok(left) => left,
        };

        if left.get_lightlike_key() != split_key {
            return false;
        }

        let right = match self.get_brane(split_key) {
            Err(_) => return false,
            Ok(right) => right,
        };

        if right.get_spacelike_key() != split_key {
            return false;
        }
        left.get_brane_epoch().get_version() > brane.get_brane_epoch().get_version()
            && right.get_brane_epoch().get_version() > brane.get_brane_epoch().get_version()
    }

    pub fn get_store_stats(&self, store_id: u64) -> Option<fidel_timeshare::StoreStats> {
        self.cluster.rl().store_stats.get(&store_id).cloned()
    }

    pub fn get_split_count(&self) -> usize {
        self.cluster.rl().split_count
    }

    pub fn get_down_peers(&self) -> HashMap<u64, fidel_timeshare::PeerStats> {
        self.cluster.rl().down_peers.clone()
    }

    pub fn get_plightlikeing_peers(&self) -> HashMap<u64, meta_timeshare::Peer> {
        self.cluster.rl().plightlikeing_peers.clone()
    }

    pub fn set_bootstrap(&self, is_bootstraped: bool) {
        self.cluster.wl().set_bootstrap(is_bootstraped);
    }

    pub fn configure_dr_auto_sync(&self, label_key: &str) {
        let mut status = ReplicationStatus::default();
        status.set_mode(ReplicationMode::DrAutoSync);
        status.mut_dr_auto_sync().label_key = label_key.to_owned();
        let mut cluster = self.cluster.wl();
        status.mut_dr_auto_sync().state_id = cluster
            .replication_status
            .as_ref()
            .map(|s| s.get_dr_auto_sync().state_id + 1)
            .unwrap_or(1);
        cluster.replication_status = Some(status);
    }

    pub fn switch_replication_mode(&self, state: DrAutoSyncState) {
        let mut cluster = self.cluster.wl();
        let status = cluster.replication_status.as_mut().unwrap();
        let mut dr = status.mut_dr_auto_sync();
        dr.state_id += 1;
        dr.set_state(state);
    }

    pub fn brane_replication_status(&self, brane_id: u64) -> BraneReplicationStatus {
        self.cluster
            .rl()
            .brane_replication_status
            .get(&brane_id)
            .unwrap()
            .to_owned()
    }

    pub fn get_brane_approximate_size(&self, brane_id: u64) -> Option<u64> {
        self.cluster.rl().get_brane_approximate_size(brane_id)
    }

    pub fn get_brane_approximate_tuplespaceInstanton(&self, brane_id: u64) -> Option<u64> {
        self.cluster.rl().get_brane_approximate_tuplespaceInstanton(brane_id)
    }

    pub fn get_brane_last_report_ts(&self, brane_id: u64) -> Option<UnixSecs> {
        self.cluster.rl().get_brane_last_report_ts(brane_id)
    }

    pub fn get_brane_last_report_term(&self, brane_id: u64) -> Option<u64> {
        self.cluster.rl().get_brane_last_report_term(brane_id)
    }

    pub fn set_gc_safe_point(&self, safe_point: u64) {
        self.cluster.wl().set_gc_safe_point(safe_point);
    }

    pub fn trigger_tso_failure(&self) {
        self.trigger_tso_failure.store(true, Ordering::SeqCst);
    }

    pub fn shutdown_store(&self, store_id: u64) {
        match self.cluster.write() {
            Ok(mut c) => {
                c.stores.remove(&store_id);
            }
            Err(e) => {
                if !thread::panicking() {
                    panic!("failed to acquire write dagger: {:?}", e)
                }
            }
        }
    }

    pub fn ignore_merge_target_integrity(&self) {
        self.cluster.wl().check_merge_target_integrity = false;
    }

    pub fn set_tso(&self, ts: TimeStamp) {
        let old = self.tso.swap(ts.into_inner(), Ordering::SeqCst);
        if old > ts.into_inner() {
            panic!(
                "cannot decrease tso. current tso: {}; trying to set to: {}",
                old, ts
            );
        }
    }
}

impl FidelClient for TestFidelClient {
    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(
        &self,
        store: meta_timeshare::CausetStore,
        brane: meta_timeshare::Brane,
    ) -> Result<Option<ReplicationStatus>> {
        if self.is_cluster_bootstrapped().unwrap() || !self.is_branes_empty() {
            self.cluster.wl().set_bootstrap(true);
            return Err(Error::ClusterBootstrapped(self.cluster_id));
        }

        let mut cluster = self.cluster.wl();
        cluster.bootstrap(store, brane);
        Ok(cluster.replication_status.clone())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        Ok(self.cluster.rl().is_bootstraped)
    }

    fn alloc_id(&self) -> Result<u64> {
        self.cluster.rl().alloc_id()
    }

    fn put_store(&self, store: meta_timeshare::CausetStore) -> Result<Option<ReplicationStatus>> {
        self.check_bootstrap()?;
        let mut cluster = self.cluster.wl();
        cluster.put_store(store)?;
        Ok(cluster.replication_status.clone())
    }

    fn get_all_stores(&self, _exclude_tombstone: bool) -> Result<Vec<meta_timeshare::CausetStore>> {
        self.check_bootstrap()?;
        self.cluster.rl().get_all_stores()
    }

    fn get_store(&self, store_id: u64) -> Result<meta_timeshare::CausetStore> {
        self.check_bootstrap()?;
        self.cluster.rl().get_store(store_id)
    }

    fn get_brane(&self, key: &[u8]) -> Result<meta_timeshare::Brane> {
        self.check_bootstrap()?;
        if let Some(brane) = self.cluster.rl().get_brane(data_key(key)) {
            if check_key_in_brane(key, &brane).is_ok() {
                return Ok(brane);
            }
        }

        Err(box_err!(
            "no brane contains key {}",
            hex::encode_upper(key)
        ))
    }

    fn get_brane_info(&self, key: &[u8]) -> Result<BraneInfo> {
        let brane = self.get_brane(key)?;
        let leader = self.cluster.rl().leaders.get(&brane.get_id()).cloned();
        Ok(BraneInfo::new(brane, leader))
    }

    fn get_brane_by_id(&self, brane_id: u64) -> FidelFuture<Option<meta_timeshare::Brane>> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        match self.cluster.rl().get_brane_by_id(brane_id) {
            Ok(resp) => Box::pin(ok(resp)),
            Err(e) => Box::pin(err(e)),
        }
    }

    fn get_cluster_config(&self) -> Result<meta_timeshare::Cluster> {
        self.check_bootstrap()?;
        Ok(self.cluster.rl().meta.clone())
    }

    fn brane_heartbeat(
        &self,
        term: u64,
        brane: meta_timeshare::Brane,
        leader: meta_timeshare::Peer,
        brane_stat: BraneStat,
        replication_status: Option<BraneReplicationStatus>,
    ) -> FidelFuture<()> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        let resp = self.cluster.wl().brane_heartbeat(
            term,
            brane,
            leader.clone(),
            brane_stat,
            replication_status,
        );
        match resp {
            Ok(resp) => {
                let store_id = leader.get_store_id();
                if let Some(store) = self.cluster.wl().stores.get(&store_id) {
                    store.lightlikeer.unbounded_lightlike(resp).unwrap();
                }
                Box::pin(ok(()))
            }
            Err(e) => Box::pin(err(e)),
        }
    }

    fn handle_brane_heartbeat_response<F>(&self, store_id: u64, f: F) -> FidelFuture<()>
    where
        Self: Sized,
        F: Fn(fidel_timeshare::BraneHeartbeatResponse) + lightlike + 'static,
    {
        let cluster1 = Arc::clone(&self.cluster);
        let timer = self.timer.clone();
        let mut cluster = self.cluster.wl();
        let store = cluster
            .stores
            .entry(store_id)
            .or_insert_with(CausetStore::default);
        let rx = store.receiver.take().unwrap();
        let st1 = rx.map(|resp| vec![resp]);
        let st2 = stream::unfold(
            (timer, cluster1, store_id),
            |(timer, cluster1, store_id)| async move {
                timer
                    .delay(Instant::now() + Duration::from_millis(500))
                    .compat()
                    .await
                    .unwrap();
                let mut cluster = cluster1.wl();
                let resps = cluster.poll_heartbeat_responses_for(store_id);
                drop(cluster);
                Some((resps, (timer, cluster1, store_id)))
            },
        );
        Box::pin(
            stream::select(st1, st2)
                .for_each(move |resps| {
                    for resp in resps {
                        f(resp);
                    }
                    futures::future::ready(())
                })
                .map(|_| Ok(())),
        )
    }

    fn ask_split(&self, brane: meta_timeshare::Brane) -> FidelFuture<fidel_timeshare::AskSplitResponse> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }

        // Must ConfVer and Version be same?
        let cur_brane = self
            .cluster
            .rl()
            .get_brane_by_id(brane.get_id())
            .unwrap()
            .unwrap();
        if let Err(e) = check_stale_brane(&cur_brane, &brane) {
            return Box::pin(err(e));
        }

        let mut resp = fidel_timeshare::AskSplitResponse::default();
        resp.set_new_brane_id(self.alloc_id().unwrap());
        let mut peer_ids = vec![];
        for _ in brane.get_peers() {
            peer_ids.push(self.alloc_id().unwrap());
        }
        resp.set_new_peer_ids(peer_ids);

        Box::pin(ok(resp))
    }

    fn ask_batch_split(
        &self,
        brane: meta_timeshare::Brane,
        count: usize,
    ) -> FidelFuture<fidel_timeshare::AskBatchSplitResponse> {
        if self.is_incompatible {
            return Box::pin(err(Error::Incompatible));
        }

        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }

        // Must ConfVer and Version be same?
        let cur_brane = self
            .cluster
            .rl()
            .get_brane_by_id(brane.get_id())
            .unwrap()
            .unwrap();
        if let Err(e) = check_stale_brane(&cur_brane, &brane) {
            return Box::pin(err(e));
        }

        let mut resp = fidel_timeshare::AskBatchSplitResponse::default();
        for _ in 0..count {
            let mut id = fidel_timeshare::SplitId::default();
            id.set_new_brane_id(self.alloc_id().unwrap());
            for _ in brane.get_peers() {
                id.mut_new_peer_ids().push(self.alloc_id().unwrap());
            }
            resp.mut_ids().push(id);
        }

        Box::pin(ok(resp))
    }

    fn store_heartbeat(&self, stats: fidel_timeshare::StoreStats) -> FidelFuture<fidel_timeshare::StoreHeartbeatResponse> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }

        // Cache it directly now.
        let store_id = stats.get_store_id();
        let mut cluster = self.cluster.wl();
        cluster.store_stats.insert(store_id, stats);

        let mut resp = fidel_timeshare::StoreHeartbeatResponse::default();
        if let Some(ref status) = cluster.replication_status {
            resp.set_replication_status(status.clone());
        }
        Box::pin(ok(resp))
    }

    fn report_batch_split(&self, branes: Vec<meta_timeshare::Brane>) -> FidelFuture<()> {
        // fidel just uses this for history show, so here we just count it.
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        self.cluster.wl().split_count += branes.len() - 1;
        Box::pin(ok(()))
    }

    fn get_gc_safe_point(&self) -> FidelFuture<u64> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }

        let safe_point = self.cluster.rl().get_gc_safe_point();
        Box::pin(ok(safe_point))
    }

    fn get_store_stats(&self, store_id: u64) -> Result<fidel_timeshare::StoreStats> {
        let cluster = self.cluster.rl();
        let stats = cluster.store_stats.get(&store_id);
        match stats {
            Some(s) => Ok(s.clone()),
            None => Err(Error::StoreTombstone(format!("store_id:{}", store_id))),
        }
    }

    fn get_operator(&self, brane_id: u64) -> Result<fidel_timeshare::GetOperatorResponse> {
        let mut header = fidel_timeshare::ResponseHeader::default();
        header.set_cluster_id(self.cluster_id);
        let mut resp = fidel_timeshare::GetOperatorResponse::default();
        resp.set_header(header);
        resp.set_brane_id(brane_id);
        Ok(resp)
    }

    fn get_tso(&self) -> FidelFuture<TimeStamp> {
        fail_point!("test_violetabftstore_get_tso");
        if self.trigger_tso_failure.swap(false, Ordering::SeqCst) {
            return Box::pin(err(fidel_client::errors::Error::Grpc(
                grpcio::Error::RpcFailure(grpcio::RpcStatus::new(
                    grpcio::RpcStatusCode::UNKNOWN,
                    Some("tso error".to_owned()),
                )),
            )));
        }
        let tso = self.tso.fetch_add(1, Ordering::SeqCst);
        Box::pin(ok(TimeStamp::new(tso)))
    }
}
