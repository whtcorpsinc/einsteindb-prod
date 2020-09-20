// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use super::client::{self, Client};
use super::config::Config;
use super::metrics::*;
use super::waiter_manager::Scheduler as WaiterMgrScheduler;
use super::{Error, Result};
use crate::server::resolve::StoreAddrResolver;
use crate::persistence::lock_manager::Lock;
use engine_lmdb::LmdbEngine;
use futures::future::{self, FutureExt, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{StreamExt, TryStreamExt};
use grpcio::{
    self, DuplexSink, Environment, RequestStream, RpcContext, RpcStatus, RpcStatusCode, UnarySink,
    WriteFlags,
};
use ekvproto::deadlock::*;
use ekvproto::metapb::Brane;
use fidel_client::{FidelClient, INVALID_ID};
use violetabft::StateRole;
use violetabftstore::interlock::{
    BoxBraneChangeObserver, BoxRoleObserver, Interlock, InterlockHost, ObserverContext,
    BraneChangeEvent, BraneChangeObserver, RoleObserver,
};
use violetabftstore::store::util::is_brane_initialized;
use security::{check_common_name, SecurityManager};
use std::cell::RefCell;
use std::fmt::{self, Display, Formatter};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use einsteindb_util::collections::{HashMap, HashSet};
use einsteindb_util::future::paired_future_callback;
use einsteindb_util::time::{Duration, Instant};
use einsteindb_util::worker::{FutureRunnable, FutureScheduler, Stopped};
use tokio::task::spawn_local;
use txn_types::TimeStamp;

/// `Locks` is a set of locks belonging to one transaction.
struct Locks {
    ts: TimeStamp,
    hashes: Vec<u64>,
    last_detect_time: Instant,
}

impl Locks {
    /// Creates a new `Locks`.
    fn new(ts: TimeStamp, hash: u64, last_detect_time: Instant) -> Self {
        Self {
            ts,
            hashes: vec![hash],
            last_detect_time,
        }
    }

    /// Pushes the `hash` if not exist and ufidelates `last_detect_time`.
    fn push(&mut self, lock_hash: u64, now: Instant) {
        if !self.hashes.contains(&lock_hash) {
            self.hashes.push(lock_hash)
        }
        self.last_detect_time = now
    }

    /// Removes the `lock_hash` and returns true if the `Locks` is empty.
    fn remove(&mut self, lock_hash: u64) -> bool {
        if let Some(idx) = self.hashes.iter().position(|hash| *hash == lock_hash) {
            self.hashes.remove(idx);
        }
        self.hashes.is_empty()
    }

    /// Returns true if the `Locks` is expired.
    fn is_expired(&self, now: Instant, ttl: Duration) -> bool {
        now.duration_since(self.last_detect_time) >= ttl
    }
}

/// Used to detect the deadlock of wait-for-lock in the cluster.
pub struct DetectTable {
    /// Keeps the DAG of wait-for-lock. Every edge from `txn_ts` to `lock_ts` has a survival time -- `ttl`.
    /// When checking the deadlock, if the ttl has elpased, the corresponding edge will be removed.
    /// `last_detect_time` is the spacelike time of the edge. `Detect` requests will refresh it.
    // txn_ts => (lock_ts => Locks)
    wait_for_map: HashMap<TimeStamp, HashMap<TimeStamp, Locks>>,

    /// The ttl of every edge.
    ttl: Duration,

    /// The time of last `active_expire`.
    last_active_expire: Instant,

    now: Instant,
}

impl DetectTable {
    /// Creates a auto-expiring detect table.
    pub fn new(ttl: Duration) -> Self {
        Self {
            wait_for_map: HashMap::default(),
            ttl,
            last_active_expire: Instant::now_coarse(),
            now: Instant::now_coarse(),
        }
    }

    /// Returns the key hash which causes deadlock.
    pub fn detect(&mut self, txn_ts: TimeStamp, lock_ts: TimeStamp, lock_hash: u64) -> Option<u64> {
        let _timer = DETECT_DURATION_HISTOGRAM.spacelike_coarse_timer();
        TASK_COUNTER_METRICS.detect.inc();

        self.now = Instant::now_coarse();
        self.active_expire();

        // If `txn_ts` is waiting for `lock_ts`, it won't cause deadlock.
        if self.register_if_existed(txn_ts, lock_ts, lock_hash) {
            return None;
        }

        if let Some(deadlock_key_hash) = self.do_detect(txn_ts, lock_ts) {
            ERROR_COUNTER_METRICS.deadlock.inc();
            return Some(deadlock_key_hash);
        }
        self.register(txn_ts, lock_ts, lock_hash);
        None
    }

    /// Checks if there is an edge from `wait_for_ts` to `txn_ts`.
    fn do_detect(&mut self, txn_ts: TimeStamp, wait_for_ts: TimeStamp) -> Option<u64> {
        let now = self.now;
        let ttl = self.ttl;

        let mut stack = vec![wait_for_ts];
        // Memorize the pushed vertexes to avoid duplicate search.
        let mut pushed: HashSet<TimeStamp> = HashSet::default();
        pushed.insert(wait_for_ts);
        while let Some(wait_for_ts) = stack.pop() {
            if let Some(wait_for) = self.wait_for_map.get_mut(&wait_for_ts) {
                // Remove expired edges.
                wait_for.retain(|_, locks| !locks.is_expired(now, ttl));
                if wait_for.is_empty() {
                    self.wait_for_map.remove(&wait_for_ts);
                } else {
                    for (lock_ts, locks) in wait_for {
                        if *lock_ts == txn_ts {
                            return Some(locks.hashes[0]);
                        }
                        if !pushed.contains(lock_ts) {
                            stack.push(*lock_ts);
                            pushed.insert(*lock_ts);
                        }
                    }
                }
            }
        }
        None
    }

    /// Returns true and adds to the detect table if `txn_ts` is waiting for `lock_ts`.
    fn register_if_existed(
        &mut self,
        txn_ts: TimeStamp,
        lock_ts: TimeStamp,
        lock_hash: u64,
    ) -> bool {
        if let Some(wait_for) = self.wait_for_map.get_mut(&txn_ts) {
            if let Some(locks) = wait_for.get_mut(&lock_ts) {
                locks.push(lock_hash, self.now);
                return true;
            }
        }
        false
    }

    /// Adds to the detect table. The edge from `txn_ts` to `lock_ts` must not exist.
    fn register(&mut self, txn_ts: TimeStamp, lock_ts: TimeStamp, lock_hash: u64) {
        let wait_for = self.wait_for_map.entry(txn_ts).or_default();
        assert!(!wait_for.contains_key(&lock_ts));
        let locks = Locks::new(lock_ts, lock_hash, self.now);
        wait_for.insert(locks.ts, locks);
    }

    /// Removes the corresponding wait_for_entry.
    fn clean_up_wait_for(&mut self, txn_ts: TimeStamp, lock_ts: TimeStamp, lock_hash: u64) {
        if let Some(wait_for) = self.wait_for_map.get_mut(&txn_ts) {
            if let Some(locks) = wait_for.get_mut(&lock_ts) {
                if locks.remove(lock_hash) {
                    wait_for.remove(&lock_ts);
                    if wait_for.is_empty() {
                        self.wait_for_map.remove(&txn_ts);
                    }
                }
            }
        }
        TASK_COUNTER_METRICS.clean_up_wait_for.inc();
    }

    /// Removes the entries of the transaction.
    fn clean_up(&mut self, txn_ts: TimeStamp) {
        self.wait_for_map.remove(&txn_ts);
        TASK_COUNTER_METRICS.clean_up.inc();
    }

    /// Clears the whole detect table.
    fn clear(&mut self) {
        self.wait_for_map.clear();
    }

    /// Reset the ttl
    fn reset_ttl(&mut self, ttl: Duration) {
        self.ttl = ttl;
    }

    /// The memory_barrier of detect table size to trigger `active_expire`.
    const ACTIVE_EXPIRE_THRESHOLD: usize = 100000;
    /// The interval between `active_expire`.
    const ACTIVE_EXPIRE_INTERVAL: Duration = Duration::from_secs(3600);

    /// Iterates the whole table to remove all expired entries.
    fn active_expire(&mut self) {
        if self.wait_for_map.len() >= Self::ACTIVE_EXPIRE_THRESHOLD
            && self.now.duration_since(self.last_active_expire) >= Self::ACTIVE_EXPIRE_INTERVAL
        {
            let now = self.now;
            let ttl = self.ttl;
            for (_, wait_for) in self.wait_for_map.iter_mut() {
                wait_for.retain(|_, locks| !locks.is_expired(now, ttl));
            }
            self.wait_for_map.retain(|_, wait_for| !wait_for.is_empty());
            self.last_active_expire = self.now;
        }
    }
}

/// The role of the detector.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Role {
    /// The node is the leader of the detector.
    Leader,
    /// The node is a follower of the leader.
    Follower,
}

impl Default for Role {
    fn default() -> Role {
        Role::Follower
    }
}

impl From<StateRole> for Role {
    fn from(role: StateRole) -> Role {
        match role {
            StateRole::Leader => Role::Leader,
            _ => Role::Follower,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DetectType {
    Detect,
    CleanUpWaitFor,
    CleanUp,
}

pub enum Task {
    /// The detect request of itself.
    Detect {
        tp: DetectType,
        txn_ts: TimeStamp,
        lock: Lock,
    },
    /// The detect request of other nodes.
    DetectRpc {
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    },
    /// If the node has the leader brane and the role of the node changes,
    /// a `ChangeRole` task will be scheduled.
    ///
    /// It's the only way to change the node from leader to follower, and vice versa.
    ChangeRole(Role),
    /// Change the ttl of DetectTable
    ChangeTTL(Duration),
    // Task only used for test
    #[causetg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(u64) + Slightlike>),
    #[causetg(test)]
    GetRole(Box<dyn FnOnce(Role) + Slightlike>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Detect { tp, txn_ts, lock } => write!(
                f,
                "Detect {{ tp: {:?}, txn_ts: {}, lock: {:?} }}",
                tp, txn_ts, lock
            ),
            Task::DetectRpc { .. } => write!(f, "Detect Rpc"),
            Task::ChangeRole(role) => write!(f, "ChangeRole {{ role: {:?} }}", role),
            Task::ChangeTTL(ttl) => write!(f, "ChangeTTL {{ ttl: {:?} }}", ttl),
            #[causetg(any(test, feature = "testexport"))]
            Task::Validate(_) => write!(f, "Validate dead lock config"),
            #[causetg(test)]
            Task::GetRole(_) => write!(f, "Get role of the deadlock detector"),
        }
    }
}

/// `Scheduler` is the wrapper of the `FutureScheduler<Task>` to simplify scheduling tasks
/// to the deadlock detector.
#[derive(Clone)]
pub struct Scheduler(FutureScheduler<Task>);

impl Scheduler {
    pub fn new(scheduler: FutureScheduler<Task>) -> Self {
        Self(scheduler)
    }

    fn notify_scheduler(&self, task: Task) {
        // Only when the deadlock detector is stopped, an error will be returned.
        // So there is no need to handle the error.
        if let Err(Stopped(task)) = self.0.schedule(task) {
            error!("failed to slightlike task to deadlock_detector"; "task" => %task);
        }
    }

    pub fn detect(&self, txn_ts: TimeStamp, lock: Lock) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::Detect,
            txn_ts,
            lock,
        });
    }

    pub fn clean_up_wait_for(&self, txn_ts: TimeStamp, lock: Lock) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::CleanUpWaitFor,
            txn_ts,
            lock,
        });
    }

    pub fn clean_up(&self, txn_ts: TimeStamp) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::CleanUp,
            txn_ts,
            lock: Lock::default(),
        });
    }

    fn change_role(&self, role: Role) {
        self.notify_scheduler(Task::ChangeRole(role));
    }

    pub fn change_ttl(&self, t: Duration) {
        self.notify_scheduler(Task::ChangeTTL(t));
    }

    #[causetg(any(test, feature = "testexport"))]
    pub fn validate(&self, f: Box<dyn FnOnce(u64) + Slightlike>) {
        self.notify_scheduler(Task::Validate(f));
    }

    #[causetg(test)]
    pub fn get_role(&self, f: Box<dyn FnOnce(Role) + Slightlike>) {
        self.notify_scheduler(Task::GetRole(f));
    }
}

/// The leader brane is the brane containing the LEADER_KEY and the leader of the
/// leader brane is also the leader of the deadlock detector.
const LEADER_KEY: &[u8] = b"";

/// `RoleChangeNotifier` observes brane or role change events of violetabftstore. If the
/// brane is the leader brane and the role of this node is changed, a `ChangeRole`
/// task will be scheduled to the deadlock detector. It's the only way to change the
/// node from the leader of deadlock detector to follower, and vice versa.
#[derive(Clone)]
pub(crate) struct RoleChangeNotifier {
    /// The id of the valid leader brane.
    // violetabftstore.interlock needs it to be Sync + Slightlike.
    leader_brane_id: Arc<Mutex<u64>>,
    scheduler: Scheduler,
}

impl RoleChangeNotifier {
    fn is_leader_brane(brane: &Brane) -> bool {
        // The key cone of a new created brane is empty which misleads the leader
        // of the deadlock detector stepping down.
        //
        // If the peers of a brane is not empty, the brane info is complete.
        is_brane_initialized(brane)
            && brane.get_spacelike_key() <= LEADER_KEY
            && (brane.get_lightlike_key().is_empty() || LEADER_KEY < brane.get_lightlike_key())
    }

    pub(crate) fn new(scheduler: Scheduler) -> Self {
        Self {
            leader_brane_id: Arc::new(Mutex::new(INVALID_ID)),
            scheduler,
        }
    }

    pub(crate) fn register(self, host: &mut InterlockHost<LmdbEngine>) {
        host.registry
            .register_role_observer(1, BoxRoleObserver::new(self.clone()));
        host.registry
            .register_brane_change_observer(1, BoxBraneChangeObserver::new(self));
    }
}

impl Interlock for RoleChangeNotifier {}

impl RoleObserver for RoleChangeNotifier {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        let brane = ctx.brane();
        // A brane is created first, so the leader brane id must be valid.
        if Self::is_leader_brane(brane)
            && *self.leader_brane_id.lock().unwrap() == brane.get_id()
        {
            self.scheduler.change_role(role.into());
        }
    }
}

impl BraneChangeObserver for RoleChangeNotifier {
    fn on_brane_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: BraneChangeEvent,
        role: StateRole,
    ) {
        let brane = ctx.brane();
        if Self::is_leader_brane(brane) {
            match event {
                BraneChangeEvent::Create | BraneChangeEvent::Ufidelate => {
                    *self.leader_brane_id.lock().unwrap() = brane.get_id();
                    self.scheduler.change_role(role.into());
                }
                BraneChangeEvent::Destroy => {
                    // When one brane is merged to target brane, it will be destroyed.
                    // If the leader brane is merged to the target brane and the node
                    // is also the leader of the target brane, the RoleChangeNotifier will
                    // receive one BraneChangeEvent::Ufidelate of the target brane and one
                    // BraneChangeEvent::Destroy of the leader brane. To prevent the
                    // destroy event misleading the leader stepping down, it saves the
                    // valid leader brane id and only when the id equals to the destroyed
                    // brane id, it slightlikes a ChangeRole(Follower) task to the deadlock detector.
                    let mut leader_brane_id = self.leader_brane_id.lock().unwrap();
                    if *leader_brane_id == brane.get_id() {
                        *leader_brane_id = INVALID_ID;
                        self.scheduler.change_role(Role::Follower);
                    }
                }
            }
        }
    }
}

struct Inner {
    /// The role of the deadlock detector. Default is `Role::Follower`.
    role: Role,

    detect_table: DetectTable,
}

/// Detector is used to detect deadlocks between transactions. There is a leader
/// in the cluster which collects all `wait_for_entry` from other followers.
pub struct Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: FidelClient + 'static,
{
    /// The store id of the node.
    store_id: u64,
    /// Used to create clients to the leader.
    env: Arc<Environment>,
    /// The leader's id and address if exists.
    leader_info: Option<(u64, String)>,
    /// The connection to the leader.
    leader_client: Option<Client>,
    /// Used to get the leader of leader brane from FIDel.
    fidel_client: Arc<P>,
    /// Used to resolve store address.
    resolver: S,
    /// Used to connect other nodes.
    security_mgr: Arc<SecurityManager>,
    /// Used to schedule Deadlock msgs to the waiter manager.
    waiter_mgr_scheduler: WaiterMgrScheduler,

    inner: Rc<RefCell<Inner>>,
}

unsafe impl<S, P> Slightlike for Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: FidelClient + 'static,
{
}

impl<S, P> Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: FidelClient + 'static,
{
    pub fn new(
        store_id: u64,
        fidel_client: Arc<P>,
        resolver: S,
        security_mgr: Arc<SecurityManager>,
        waiter_mgr_scheduler: WaiterMgrScheduler,
        causetg: &Config,
    ) -> Self {
        assert!(store_id != INVALID_ID);
        Self {
            store_id,
            env: client::env(),
            leader_info: None,
            leader_client: None,
            fidel_client,
            resolver,
            security_mgr,
            waiter_mgr_scheduler,
            inner: Rc::new(RefCell::new(Inner {
                role: Role::Follower,
                detect_table: DetectTable::new(causetg.wait_for_lock_timeout.into()),
            })),
        }
    }

    /// Returns true if it is the leader of the deadlock detector.
    fn is_leader(&self) -> bool {
        self.inner.borrow().role == Role::Leader
    }

    /// Resets to the initial state.
    fn reset(&mut self, role: Role) {
        let mut inner = self.inner.borrow_mut();
        inner.detect_table.clear();
        inner.role = role;
        self.leader_client.take();
        self.leader_info.take();
    }

    /// Refreshes the leader info. Returns true if the leader exists.
    fn refresh_leader_info(&mut self) -> bool {
        assert!(!self.is_leader());
        match self.get_leader_info() {
            Ok(Some((leader_id, leader_addr))) => {
                self.ufidelate_leader_info(leader_id, leader_addr);
            }
            Ok(None) => {
                // The leader is gone, reset state.
                info!("no leader");
                self.reset(Role::Follower);
            }
            Err(e) => {
                error!("get leader info failed"; "err" => ?e);
            }
        };
        self.leader_info.is_some()
    }

    /// Gets leader info from FIDel.
    fn get_leader_info(&self) -> Result<Option<(u64, String)>> {
        let leader = self.fidel_client.get_brane_info(LEADER_KEY)?.leader;
        match leader {
            Some(leader) => {
                let leader_id = leader.get_store_id();
                let leader_addr = self.resolve_store_address(leader_id)?;
                Ok(Some((leader_id, leader_addr)))
            }

            None => {
                ERROR_COUNTER_METRICS.leader_not_found.inc();
                Ok(None)
            }
        }
    }

    /// Resolves store address.
    fn resolve_store_address(&self, store_id: u64) -> Result<String> {
        match wait_op!(|cb| self
            .resolver
            .resolve(store_id, cb)
            .map_err(|e| Error::Other(box_err!(e))))
        {
            Some(Ok(addr)) => Ok(addr),
            _ => Err(box_err!("failed to resolve store address")),
        }
    }

    /// Ufidelates the leader info.
    fn ufidelate_leader_info(&mut self, leader_id: u64, leader_addr: String) {
        match self.leader_info {
            Some((id, ref addr)) if id == leader_id && *addr == leader_addr => {
                debug!("leader not change"; "leader_id" => leader_id, "leader_addr" => %leader_addr);
            }
            _ => {
                // The leader info is stale if the leader is itself.
                if leader_id == self.store_id {
                    info!("stale leader info");
                } else {
                    info!("leader changed"; "leader_id" => leader_id, "leader_addr" => %leader_addr);
                    self.leader_client.take();
                    self.leader_info.replace((leader_id, leader_addr));
                }
            }
        }
    }

    /// Resets state if role changes.
    fn change_role(&mut self, role: Role) {
        if self.inner.borrow().role != role {
            match role {
                Role::Leader => {
                    info!("became the leader of deadlock detector!"; "self_id" => self.store_id);
                    DETECTOR_LEADER_GAUGE.set(1);
                }
                Role::Follower => {
                    info!("changed from the leader of deadlock detector to follower!"; "self_id" => self.store_id);
                    DETECTOR_LEADER_GAUGE.set(0);
                }
            }
        }
        // If the node is a follower, it will receive a `ChangeRole(Follower)` msg when the leader
        // is changed. It should reset itself even if the role of the node is not changed.
        self.reset(role);
    }

    /// Reconnects the leader. The leader info must exist.
    fn reconnect_leader(&mut self) {
        assert!(self.leader_client.is_none() && self.leader_info.is_some());
        ERROR_COUNTER_METRICS.reconnect_leader.inc();
        let (leader_id, leader_addr) = self.leader_info.as_ref().unwrap();
        // Create the connection to the leader and registers the callback to receive
        // the deadlock response.
        let mut leader_client = Client::new(
            Arc::clone(&self.env),
            Arc::clone(&self.security_mgr),
            leader_addr,
        );
        let waiter_mgr_scheduler = self.waiter_mgr_scheduler.clone();
        let (slightlike, recv) = leader_client.register_detect_handler(Box::new(move |mut resp| {
            let WaitForEntry {
                txn,
                wait_for_txn,
                key_hash,
                ..
            } = resp.take_entry();
            waiter_mgr_scheduler.deadlock(
                txn.into(),
                Lock {
                    ts: wait_for_txn.into(),
                    hash: key_hash,
                },
                resp.get_deadlock_key_hash(),
            )
        }));
        spawn_local(slightlike.map_err(|e| error!("leader client failed"; "err" => ?e)));
        // No need to log it again.
        spawn_local(recv.map_err(|_| ()));

        self.leader_client = Some(leader_client);
        info!("reconnect leader succeeded"; "leader_id" => leader_id);
    }

    /// Returns true if slightlikes successfully.
    ///
    /// If the client is None, reconnects the leader first, then slightlikes the request to the leader.
    /// If slightlikes failed, sets the client to None for retry.
    fn slightlike_request_to_leader(&mut self, tp: DetectType, txn_ts: TimeStamp, lock: Lock) -> bool {
        assert!(!self.is_leader() && self.leader_info.is_some());

        if self.leader_client.is_none() {
            self.reconnect_leader();
        }
        if let Some(leader_client) = &self.leader_client {
            let tp = match tp {
                DetectType::Detect => DeadlockRequestType::Detect,
                DetectType::CleanUpWaitFor => DeadlockRequestType::CleanUpWaitFor,
                DetectType::CleanUp => DeadlockRequestType::CleanUp,
            };
            let mut entry = WaitForEntry::default();
            entry.set_txn(txn_ts.into_inner());
            entry.set_wait_for_txn(lock.ts.into_inner());
            entry.set_key_hash(lock.hash);
            let mut req = DeadlockRequest::default();
            req.set_tp(tp);
            req.set_entry(entry);
            if leader_client.detect(req).is_ok() {
                return true;
            }
            // The client is disconnected. Take it for retry.
            self.leader_client.take();
        }
        false
    }

    fn handle_detect_locally(&self, tp: DetectType, txn_ts: TimeStamp, lock: Lock) {
        let detect_table = &mut self.inner.borrow_mut().detect_table;
        match tp {
            DetectType::Detect => {
                if let Some(deadlock_key_hash) = detect_table.detect(txn_ts, lock.ts, lock.hash) {
                    self.waiter_mgr_scheduler
                        .deadlock(txn_ts, lock, deadlock_key_hash);
                }
            }
            DetectType::CleanUpWaitFor => {
                detect_table.clean_up_wait_for(txn_ts, lock.ts, lock.hash)
            }
            DetectType::CleanUp => detect_table.clean_up(txn_ts),
        }
    }

    /// Handles detect requests of itself.
    fn handle_detect(&mut self, tp: DetectType, txn_ts: TimeStamp, lock: Lock) {
        if self.is_leader() {
            self.handle_detect_locally(tp, txn_ts, lock);
        } else {
            for _ in 0..2 {
                // TODO: If the leader hasn't been elected, it requests Fidel for
                // each detect request. Maybe need flow control here.
                //
                // Refresh leader info when the connection to the leader is disconnected.
                if self.leader_client.is_none() && !self.refresh_leader_info() {
                    break;
                }
                if self.slightlike_request_to_leader(tp, txn_ts, lock) {
                    return;
                }
                // Because the client is asynchronous, it won't be closed until failing to slightlike a
                // request. So retry to refresh the leader info and slightlike it again.
            }
            // If a request which causes deadlock is dropped, it leads to the waiter timeout.
            // MilevaDB will retry to acquire the lock and detect deadlock again.
            warn!("detect request dropped"; "tp" => ?tp, "txn_ts" => txn_ts, "lock" => ?lock);
            ERROR_COUNTER_METRICS.dropped.inc();
        }
    }

    /// Handles detect requests of other nodes.
    fn handle_detect_rpc(
        &self,
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    ) {
        if !self.is_leader() {
            let status = RpcStatus::new(
                RpcStatusCode::FAILED_PRECONDITION,
                Some("I'm not the leader of deadlock detector".to_string()),
            );
            spawn_local(sink.fail(status).map_err(|_| ()));
            ERROR_COUNTER_METRICS.not_leader.inc();
            return;
        }

        let inner = Rc::clone(&self.inner);
        let mut s = stream.map_err(Error::Grpc).filter_map(move |item| {
            if let Ok(mut req) = item {
                // It's possible the leader changes after registering this handler.
                let mut inner = inner.borrow_mut();
                if inner.role != Role::Leader {
                    ERROR_COUNTER_METRICS.not_leader.inc();
                    return future::ready(Some(Err(Error::Other(box_err!("leader changed")))));
                }
                let WaitForEntry {
                    txn,
                    wait_for_txn,
                    key_hash,
                    ..
                } = req.get_entry();
                let detect_table = &mut inner.detect_table;
                let res = match req.get_tp() {
                    DeadlockRequestType::Detect => {
                        if let Some(deadlock_key_hash) =
                            detect_table.detect(txn.into(), wait_for_txn.into(), *key_hash)
                        {
                            let mut resp = DeadlockResponse::default();
                            resp.set_entry(req.take_entry());
                            resp.set_deadlock_key_hash(deadlock_key_hash);
                            Some(Ok((resp, WriteFlags::default())))
                        } else {
                            None
                        }
                    }
                    DeadlockRequestType::CleanUpWaitFor => {
                        detect_table.clean_up_wait_for(txn.into(), wait_for_txn.into(), *key_hash);
                        None
                    }
                    DeadlockRequestType::CleanUp => {
                        detect_table.clean_up(txn.into());
                        None
                    }
                };
                future::ready(res)
            } else {
                future::ready(None)
            }
        });
        let slightlike_task = async move {
            let mut sink = sink.sink_map_err(Error::from);
            sink.slightlike_all(&mut s).await?;
            sink.close().await?;
            Result::Ok(())
        }
        .map(|_| ());
        spawn_local(slightlike_task);
    }

    fn handle_change_role(&mut self, role: Role) {
        debug!("handle change role"; "role" => ?role);
        self.change_role(role);
    }

    fn handle_change_ttl(&mut self, ttl: Duration) {
        let mut inner = self.inner.borrow_mut();
        inner.detect_table.reset_ttl(ttl);
        info!("Deadlock detector config changed"; "ttl" => ?ttl);
    }
}

impl<S, P> FutureRunnable<Task> for Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: FidelClient + 'static,
{
    fn run(&mut self, task: Task) {
        match task {
            Task::Detect { tp, txn_ts, lock } => {
                self.handle_detect(tp, txn_ts, lock);
            }
            Task::DetectRpc { stream, sink } => {
                self.handle_detect_rpc(stream, sink);
            }
            Task::ChangeRole(role) => self.handle_change_role(role),
            Task::ChangeTTL(ttl) => self.handle_change_ttl(ttl),
            #[causetg(any(test, feature = "testexport"))]
            Task::Validate(f) => f(self.inner.borrow().detect_table.ttl.as_millis() as u64),
            #[causetg(test)]
            Task::GetRole(f) => f(self.inner.borrow().role),
        }
    }
}

#[derive(Clone)]
pub struct Service {
    waiter_mgr_scheduler: WaiterMgrScheduler,
    detector_scheduler: Scheduler,
    security_mgr: Arc<SecurityManager>,
}

impl Service {
    pub fn new(
        waiter_mgr_scheduler: WaiterMgrScheduler,
        detector_scheduler: Scheduler,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        Self {
            waiter_mgr_scheduler,
            detector_scheduler,
            security_mgr,
        }
    }
}

impl Deadlock for Service {
    // TODO: remove it
    fn get_wait_for_entries(
        &mut self,
        ctx: RpcContext<'_>,
        _req: WaitForEntriesRequest,
        sink: UnarySink<WaitForEntriesResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let (cb, f) = paired_future_callback();
        if !self.waiter_mgr_scheduler.dump_wait_table(cb) {
            let status = RpcStatus::new(
                RpcStatusCode::RESOURCE_EXHAUSTED,
                Some("waiter manager has stopped".to_owned()),
            );
            ctx.spawn(sink.fail(status).map(|_| ()))
        } else {
            ctx.spawn(
                f.map_err(Error::from)
                    .map_ok(|v| {
                        let mut resp = WaitForEntriesResponse::default();
                        resp.set_entries(v.into());
                        resp
                    })
                    .and_then(|resp| sink.success(resp).map_err(Error::Grpc))
                    .unwrap_or_else(|e| debug!("get_wait_for_entries failed"; "err" => ?e)),
            );
        }
    }

    fn detect(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let task = Task::DetectRpc { stream, sink };
        if let Err(Stopped(Task::DetectRpc { sink, .. })) = self.detector_scheduler.0.schedule(task)
        {
            let status = RpcStatus::new(
                RpcStatusCode::RESOURCE_EXHAUSTED,
                Some("deadlock detector has stopped".to_owned()),
            );
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    }
}

#[causetg(test)]
pub mod tests {
    use super::*;
    use crate::server::resolve::Callback;
    use futures::executor::block_on;
    use security::SecurityConfig;
    use einsteindb_util::worker::FutureWorker;

    #[test]
    fn test_detect_table() {
        let mut detect_table = DetectTable::new(Duration::from_secs(10));

        // Deadlock: 1 -> 2 -> 1
        assert_eq!(detect_table.detect(1.into(), 2.into(), 2), None);
        assert_eq!(detect_table.detect(2.into(), 1.into(), 1).unwrap(), 2);
        // Deadlock: 1 -> 2 -> 3 -> 1
        assert_eq!(detect_table.detect(2.into(), 3.into(), 3), None);
        assert_eq!(detect_table.detect(3.into(), 1.into(), 1).unwrap(), 3);
        detect_table.clean_up(2.into());
        assert_eq!(detect_table.wait_for_map.contains_key(&2.into()), false);

        // After cycle is broken, no deadlock.
        assert_eq!(detect_table.detect(3.into(), 1.into(), 1), None);
        assert_eq!(detect_table.wait_for_map.get(&3.into()).unwrap().len(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .hashes
                .len(),
            1
        );

        // Different key_hash grows the list.
        assert_eq!(detect_table.detect(3.into(), 1.into(), 2), None);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .hashes
                .len(),
            2
        );

        // Same key_hash doesn't grow the list.
        assert_eq!(detect_table.detect(3.into(), 1.into(), 2), None);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .hashes
                .len(),
            2
        );

        // Different lock_ts grows the map.
        assert_eq!(detect_table.detect(3.into(), 2.into(), 2), None);
        assert_eq!(detect_table.wait_for_map.get(&3.into()).unwrap().len(), 2);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&2.into())
                .unwrap()
                .hashes
                .len(),
            1
        );

        // Clean up entries shrinking the map.
        detect_table.clean_up_wait_for(3.into(), 1.into(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .hashes
                .len(),
            1
        );
        detect_table.clean_up_wait_for(3.into(), 1.into(), 2);
        assert_eq!(detect_table.wait_for_map.get(&3.into()).unwrap().len(), 1);
        detect_table.clean_up_wait_for(3.into(), 2.into(), 2);
        assert_eq!(detect_table.wait_for_map.contains_key(&3.into()), false);

        // Clean up non-exist entry
        detect_table.clean_up(3.into());
        detect_table.clean_up_wait_for(3.into(), 1.into(), 1);
    }

    #[test]
    fn test_detect_table_expire() {
        let mut detect_table = DetectTable::new(Duration::from_millis(100));

        // Deadlock
        assert!(detect_table.detect(1.into(), 2.into(), 1).is_none());
        assert!(detect_table.detect(2.into(), 1.into(), 2).is_some());
        // After sleep, the expired entry has been removed. So there is no deadlock.
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(detect_table.wait_for_map.len(), 1);
        assert!(detect_table.detect(2.into(), 1.into(), 2).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 1);

        // `Detect` ufidelates the last_detect_time, so the entry won't be removed.
        detect_table.clear();
        assert!(detect_table.detect(1.into(), 2.into(), 1).is_none());
        std::thread::sleep(Duration::from_millis(500));
        assert!(detect_table.detect(1.into(), 2.into(), 1).is_none());
        assert!(detect_table.detect(2.into(), 1.into(), 2).is_some());

        // Remove expired entry shrinking the map.
        detect_table.clear();
        assert!(detect_table.detect(1.into(), 2.into(), 1).is_none());
        assert!(detect_table.detect(1.into(), 3.into(), 1).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 1);
        std::thread::sleep(Duration::from_millis(500));
        assert!(detect_table.detect(1.into(), 3.into(), 2).is_none());
        assert!(detect_table.detect(2.into(), 1.into(), 2).is_none());
        assert_eq!(detect_table.wait_for_map.get(&1.into()).unwrap().len(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&1.into())
                .unwrap()
                .get(&3.into())
                .unwrap()
                .hashes
                .len(),
            2
        );
        std::thread::sleep(Duration::from_millis(500));
        assert!(detect_table.detect(3.into(), 2.into(), 3).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 2);
        assert!(detect_table.detect(3.into(), 1.into(), 3).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 1);
    }

    pub(crate) struct MockFidelClient;

    impl FidelClient for MockFidelClient {}

    #[derive(Clone)]
    pub(crate) struct MockResolver;

    impl StoreAddrResolver for MockResolver {
        fn resolve(&self, _store_id: u64, _cb: Callback) -> Result<()> {
            Err(Error::Other(box_err!("unimplemented")))
        }
    }

    fn spacelike_deadlock_detector(
        host: &mut InterlockHost<LmdbEngine>,
    ) -> (FutureWorker<Task>, Scheduler) {
        let waiter_mgr_worker = FutureWorker::new("dummy-waiter-mgr");
        let waiter_mgr_scheduler = WaiterMgrScheduler::new(waiter_mgr_worker.scheduler());
        let mut detector_worker = FutureWorker::new("test-deadlock-detector");
        let detector_runner = Detector::new(
            1,
            Arc::new(MockFidelClient {}),
            MockResolver {},
            Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()),
            waiter_mgr_scheduler,
            &Config::default(),
        );
        let detector_scheduler = Scheduler::new(detector_worker.scheduler());
        let role_change_notifier = RoleChangeNotifier::new(detector_scheduler.clone());
        role_change_notifier.register(host);
        detector_worker.spacelike(detector_runner).unwrap();
        (detector_worker, detector_scheduler)
    }

    // Brane with non-empty peers is valid.
    fn new_brane(id: u64, spacelike_key: &[u8], lightlike_key: &[u8], valid: bool) -> Brane {
        let mut brane = Brane::default();
        brane.set_id(id);
        brane.set_spacelike_key(spacelike_key.to_vec());
        brane.set_lightlike_key(lightlike_key.to_vec());
        if valid {
            brane.set_peers(vec![ekvproto::metapb::Peer::default()].into());
        }
        brane
    }

    #[test]
    fn test_role_change_notifier() {
        let mut host = InterlockHost::default();
        let (mut worker, scheduler) = spacelike_deadlock_detector(&mut host);

        let mut brane = new_brane(1, b"", b"", true);
        let invalid = new_brane(2, b"", b"", false);
        let other = new_brane(3, b"0", b"", true);
        let follower_roles = [
            StateRole::Follower,
            StateRole::PreCandidate,
            StateRole::Candidate,
        ];
        let events = [
            BraneChangeEvent::Create,
            BraneChangeEvent::Ufidelate,
            BraneChangeEvent::Destroy,
        ];
        let check_role = |role| {
            let (tx, f) = paired_future_callback();
            scheduler.get_role(tx);
            assert_eq!(block_on(f).unwrap(), role);
        };

        // Brane changed
        for &event in &events[..2] {
            for &follower_role in &follower_roles {
                host.on_brane_changed(&brane, event, follower_role);
                check_role(Role::Follower);
                host.on_brane_changed(&invalid, event, StateRole::Leader);
                check_role(Role::Follower);
                host.on_brane_changed(&other, event, StateRole::Leader);
                check_role(Role::Follower);
                host.on_brane_changed(&brane, event, StateRole::Leader);
                check_role(Role::Leader);
                host.on_brane_changed(&invalid, event, follower_role);
                check_role(Role::Leader);
                host.on_brane_changed(&other, event, follower_role);
                check_role(Role::Leader);
                host.on_brane_changed(&brane, event, follower_role);
                check_role(Role::Follower);
            }
        }
        host.on_brane_changed(&brane, BraneChangeEvent::Create, StateRole::Leader);
        host.on_brane_changed(&invalid, BraneChangeEvent::Destroy, StateRole::Leader);
        host.on_brane_changed(&other, BraneChangeEvent::Destroy, StateRole::Leader);
        check_role(Role::Leader);
        host.on_brane_changed(&brane, BraneChangeEvent::Destroy, StateRole::Leader);
        check_role(Role::Follower);
        // Leader brane id is changed.
        brane.set_id(2);
        host.on_brane_changed(&brane, BraneChangeEvent::Ufidelate, StateRole::Leader);
        // Destroy the previous leader brane.
        brane.set_id(1);
        host.on_brane_changed(&brane, BraneChangeEvent::Destroy, StateRole::Leader);
        check_role(Role::Leader);

        // Role changed
        let brane = new_brane(1, b"", b"", true);
        host.on_brane_changed(&brane, BraneChangeEvent::Create, StateRole::Follower);
        check_role(Role::Follower);
        for &follower_role in &follower_roles {
            host.on_role_change(&brane, follower_role);
            check_role(Role::Follower);
            host.on_role_change(&invalid, StateRole::Leader);
            check_role(Role::Follower);
            host.on_role_change(&other, StateRole::Leader);
            check_role(Role::Follower);
            host.on_role_change(&brane, StateRole::Leader);
            check_role(Role::Leader);
            host.on_role_change(&invalid, follower_role);
            check_role(Role::Leader);
            host.on_role_change(&other, follower_role);
            check_role(Role::Leader);
            host.on_role_change(&brane, follower_role);
            check_role(Role::Follower);
        }

        worker.stop();
    }
}
