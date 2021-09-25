// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use interlocking_directorate::ConcurrencyManager;
use crossbeam::atomic::AtomicCell;
use engine_lmdb::{LmdbEngine, LmdbSnapshot};
use futures::compat::Future01CompatExt;
#[causet(feature = "prost-codec")]
use ekvproto::causet_context_timeshare::event::Event as Event_oneof_event;
use ekvproto::causet_context_timeshare::*;
use ekvproto::kvrpc_timeshare::ExtraOp as TxnExtraOp;
use ekvproto::meta_timeshare::Brane;
use fidel_client::FidelClient;
use violetabftstore::interlock::CmdBatch;
use violetabftstore::router::VioletaBftStoreRouter;
use violetabftstore::store::fsm::{ChangeCmd, ObserveID, StoreMeta};
use violetabftstore::store::msg::{Callback, ReadResponse, SignificantMsg};
use resolved_ts::Resolver;
use edb::config::causet_contextConfig;
use edb::causet_storage::kv::Snapshot;
use edb::causet_storage::tail_pointer::{DeltaScanner, ScannerBuilder};
use edb::causet_storage::txn::TxnEntry;
use edb::causet_storage::txn::TxnEntryScanner;
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::lru::LruCache;
use violetabftstore::interlock::::time::Instant;
use violetabftstore::interlock::::timer::{SteadyTimer, Timer};
use violetabftstore::interlock::::worker::{Runnable, RunnableWithTimer, ScheduleError, Interlock_Semaphore};
use tokio::runtime::{Builder, Runtime};
use txn_types::{
    Key, Dagger, LockType, MutationType, OldValue, TimeStamp, TxnExtra, TxnExtraInterlock_Semaphore,
};

use crate::pushdown_causet::{pushdown_causet, Downstream, DownstreamID, DownstreamState};
use crate::metrics::*;
use crate::service::{causet_contextEvent, Conn, ConnID, FeatureGate};
use crate::{causet_contextSemaphore, Error, Result};

pub enum Deregister {
    Downstream {
        brane_id: u64,
        downstream_id: DownstreamID,
        conn_id: ConnID,
        err: Option<Error>,
    },
    Brane {
        brane_id: u64,
        observe_id: ObserveID,
        err: Error,
    },
    Conn(ConnID),
}

impl fmt::Display for Deregister {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for Deregister {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("Deregister");
        match self {
            Deregister::Downstream {
                ref brane_id,
                ref downstream_id,
                ref conn_id,
                ref err,
            } => de
                .field("deregister", &"downstream")
                .field("brane_id", brane_id)
                .field("downstream_id", downstream_id)
                .field("conn_id", conn_id)
                .field("err", err)
                .finish(),
            Deregister::Brane {
                ref brane_id,
                ref observe_id,
                ref err,
            } => de
                .field("deregister", &"brane")
                .field("brane_id", brane_id)
                .field("observe_id", observe_id)
                .field("err", err)
                .finish(),
            Deregister::Conn(ref conn_id) => de
                .field("deregister", &"conn")
                .field("conn_id", conn_id)
                .finish(),
        }
    }
}

type InitCallback = Box<dyn FnOnce() + lightlike>;
pub(crate) type OldValueCallback =
    Box<dyn FnMut(Key, &mut OldValueCache) -> Option<Vec<u8>> + lightlike>;

pub struct OldValueCache {
    pub cache: LruCache<Key, (Option<OldValue>, MutationType)>,
    pub miss_count: usize,
    pub access_count: usize,
}

impl OldValueCache {
    pub fn new(size: usize) -> OldValueCache {
        OldValueCache {
            cache: LruCache::with_capacity(size),
            miss_count: 0,
            access_count: 0,
        }
    }
}

pub enum Task {
    Register {
        request: ChangeDataRequest,
        downstream: Downstream,
        conn_id: ConnID,
        version: semver::Version,
    },
    Deregister(Deregister),
    OpenConn {
        conn: Conn,
    },
    MultiBatch {
        multi: Vec<CmdBatch>,
        old_value_cb: OldValueCallback,
    },
    MinTS {
        branes: Vec<u64>,
        min_ts: TimeStamp,
    },
    ResolverReady {
        observe_id: ObserveID,
        brane: Brane,
        resolver: Resolver,
    },
    IncrementalScan {
        brane_id: u64,
        downstream_id: DownstreamID,
        entries: Vec<Option<TxnEntry>>,
    },
    RegisterMinTsEvent,
    // The result of ChangeCmd should be returned from causet_context node to ensure
    // the downstream switches to Normal after the previous commands was sunk.
    InitDownstream {
        downstream_id: DownstreamID,
        downstream_state: Arc<AtomicCell<DownstreamState>>,
        cb: InitCallback,
    },
    TxnExtra(TxnExtra),
    Validate(u64, Box<dyn FnOnce(Option<&pushdown_causet>) + lightlike>),
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("causet_contextTask");
        match self {
            Task::Register {
                ref request,
                ref downstream,
                ref conn_id,
                ref version,
                ..
            } => de
                .field("type", &"register")
                .field("register request", request)
                .field("request", request)
                .field("id", &downstream.get_id())
                .field("conn_id", conn_id)
                .field("version", version)
                .finish(),
            Task::Deregister(deregister) => de
                .field("type", &"deregister")
                .field("deregister", deregister)
                .finish(),
            Task::OpenConn { ref conn } => de
                .field("type", &"open_conn")
                .field("conn_id", &conn.get_id())
                .finish(),
            Task::MultiBatch { multi, .. } => de
                .field("type", &"multibatch")
                .field("multibatch", &multi.len())
                .finish(),
            Task::MinTS { ref min_ts, .. } => {
                de.field("type", &"mit_ts").field("min_ts", min_ts).finish()
            }
            Task::ResolverReady {
                ref observe_id,
                ref brane,
                ..
            } => de
                .field("type", &"resolver_ready")
                .field("observe_id", &observe_id)
                .field("brane_id", &brane.get_id())
                .finish(),
            Task::IncrementalScan {
                ref brane_id,
                ref downstream_id,
                ref entries,
            } => de
                .field("type", &"incremental_scan")
                .field("brane_id", &brane_id)
                .field("downstream", &downstream_id)
                .field("scan_entries", &entries.len())
                .finish(),
            Task::RegisterMinTsEvent => de.field("type", &"register_min_ts").finish(),
            Task::InitDownstream {
                ref downstream_id, ..
            } => de
                .field("type", &"init_downstream")
                .field("downstream", &downstream_id)
                .finish(),
            Task::TxnExtra(_) => de.field("type", &"txn_extra").finish(),
            Task::Validate(brane_id, _) => de.field("brane_id", &brane_id).finish(),
        }
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

pub struct node<T> {
    capture_branes: HashMap<u64, pushdown_causet>,
    connections: HashMap<ConnID, Conn>,
    interlock_semaphore: Interlock_Semaphore<Task>,
    violetabft_router: T,
    semaphore: causet_contextSemaphore,

    fidel_client: Arc<dyn FidelClient>,
    timer: SteadyTimer,
    min_ts_interval: Duration,
    scan_batch_size: usize,
    tso_worker: Runtime,
    store_meta: Arc<Mutex<StoreMeta>>,
    /// The concurrency manager for bundles. It's needed for causet_context to check locks when
    /// calculating resolved_ts.
    interlocking_directorate: ConcurrencyManager,

    workers: Runtime,

    min_resolved_ts: TimeStamp,
    min_ts_brane_id: u64,
    old_value_cache: OldValueCache,
}

impl<T: 'static + VioletaBftStoreRouter<LmdbEngine>> node<T> {
    pub fn new(
        causet: &causet_contextConfig,
        fidel_client: Arc<dyn FidelClient>,
        interlock_semaphore: Interlock_Semaphore<Task>,
        violetabft_router: T,
        semaphore: causet_contextSemaphore,
        store_meta: Arc<Mutex<StoreMeta>>,
        interlocking_directorate: ConcurrencyManager,
    ) -> node<T> {
        let workers = Builder::new()
            .threaded_interlock_semaphore()
            .thread_name("causet_contextwkr")
            .core_threads(4)
            .build()
            .unwrap();
        let tso_worker = Builder::new()
            .threaded_interlock_semaphore()
            .thread_name("tso")
            .core_threads(1)
            .build()
            .unwrap();
        let ep = node {
            capture_branes: HashMap::default(),
            connections: HashMap::default(),
            interlock_semaphore,
            fidel_client,
            tso_worker,
            timer: SteadyTimer::default(),
            workers,
            violetabft_router,
            semaphore,
            store_meta,
            interlocking_directorate,
            scan_batch_size: 1024,
            min_ts_interval: causet.min_ts_interval.0,
            min_resolved_ts: TimeStamp::max(),
            min_ts_brane_id: 0,
            old_value_cache: OldValueCache::new(causet.old_value_cache_size),
        };
        ep.register_min_ts_event();
        ep
    }

    pub fn new_timer(&self) -> Timer<()> {
        // Currently there is only one timeout for causet_context.
        let causet_context_timer_cap = 1;
        let mut timer = Timer::new(causet_context_timer_cap);
        timer.add_task(Duration::from_millis(METRICS_FLUSH_INTERVAL), ());
        timer
    }

    pub fn set_min_ts_interval(&mut self, dur: Duration) {
        self.min_ts_interval = dur;
    }

    pub fn set_scan_batch_size(&mut self, scan_batch_size: usize) {
        self.scan_batch_size = scan_batch_size;
    }

    fn on_deregister(&mut self, deregister: Deregister) {
        info!("causet_context deregister"; "deregister" => ?deregister);
        match deregister {
            Deregister::Downstream {
                brane_id,
                downstream_id,
                conn_id,
                err,
            } => {
                // The peer wants to deregister
                let mut is_last = false;
                if let Some(pushdown_causet) = self.capture_branes.get_mut(&brane_id) {
                    is_last = pushdown_causet.unsubscribe(downstream_id, err);
                }
                if let Some(conn) = self.connections.get_mut(&conn_id) {
                    if let Some(id) = conn.downstream_id(brane_id) {
                        if downstream_id == id {
                            conn.unsubscribe(brane_id);
                        }
                    }
                }
                if is_last {
                    let pushdown_causet = self.capture_branes.remove(&brane_id).unwrap();
                    if let Some(reader) = self.store_meta.dagger().unwrap().readers.get(&brane_id) {
                        reader
                            .txn_extra_op
                            .compare_and_swap(TxnExtraOp::ReadOldValue, TxnExtraOp::Noop);
                    }
                    // Do not continue to observe the events of the brane.
                    let oid = self.semaphore.unsubscribe_brane(brane_id, pushdown_causet.id);
                    assert!(
                        oid.is_some(),
                        "unsubscribe brane {} failed, ObserveID {:?}",
                        brane_id,
                        pushdown_causet.id
                    );
                }
            }
            Deregister::Brane {
                brane_id,
                observe_id,
                err,
            } => {
                // Something went wrong, deregister all downstreams of the brane.

                // To avoid ABA problem, we must check the unique ObserveID.
                let need_remove = self
                    .capture_branes
                    .get(&brane_id)
                    .map_or(false, |d| d.id == observe_id);
                if need_remove {
                    if let Some(mut pushdown_causet) = self.capture_branes.remove(&brane_id) {
                        pushdown_causet.stop(err);
                    }
                    if let Some(reader) = self.store_meta.dagger().unwrap().readers.get(&brane_id) {
                        reader.txn_extra_op.store(TxnExtraOp::Noop);
                    }
                    self.connections
                        .iter_mut()
                        .for_each(|(_, conn)| conn.unsubscribe(brane_id));
                }
                // Do not continue to observe the events of the brane.
                let oid = self.semaphore.unsubscribe_brane(brane_id, observe_id);
                assert_eq!(
                    need_remove,
                    oid.is_some(),
                    "unsubscribe brane {} failed, ObserveID {:?}",
                    brane_id,
                    observe_id
                );
            }
            Deregister::Conn(conn_id) => {
                // The connection is closed, deregister all downstreams of the connection.
                if let Some(conn) = self.connections.remove(&conn_id) {
                    conn.take_downstreams()
                        .into_iter()
                        .for_each(|(brane_id, downstream_id)| {
                            if let Some(pushdown_causet) = self.capture_branes.get_mut(&brane_id) {
                                if pushdown_causet.unsubscribe(downstream_id, None) {
                                    let pushdown_causet = self.capture_branes.remove(&brane_id).unwrap();
                                    // Do not continue to observe the events of the brane.
                                    let oid =
                                        self.semaphore.unsubscribe_brane(brane_id, pushdown_causet.id);
                                    assert!(
                                        oid.is_some(),
                                        "unsubscribe brane {} failed, ObserveID {:?}",
                                        brane_id,
                                        pushdown_causet.id
                                    );
                                }
                            }
                        });
                }
            }
        }
    }

    pub fn on_register(
        &mut self,
        mut request: ChangeDataRequest,
        mut downstream: Downstream,
        conn_id: ConnID,
        version: semver::Version,
    ) {
        let brane_id = request.brane_id;
        let downstream_id = downstream.get_id();
        let conn = match self.connections.get_mut(&conn_id) {
            Some(conn) => conn,
            None => {
                error!("register for a nonexistent connection";
                    "brane_id" => brane_id, "conn_id" => ?conn_id);
                return;
            }
        };
        downstream.set_sink(conn.get_sink());

        // TODO: Add a new task to close incompatible features.
        if let Some(e) = conn.check_version_and_set_feature(version) {
            // The downstream has not registered yet, lightlike error right away.
            downstream.sink_compatibility_error(brane_id, e);
            return;
        }
        if !conn.subscribe(request.get_brane_id(), downstream_id) {
            downstream.sink_duplicate_error(request.get_brane_id());
            error!("duplicate register";
                "brane_id" => brane_id,
                "conn_id" => ?conn_id,
                "req_id" => request.get_request_id(),
                "downstream_id" => ?downstream_id);
            return;
        }

        info!("causet_context register brane";
            "brane_id" => brane_id,
            "conn_id" => ?conn.get_id(),
            "req_id" => request.get_request_id(),
            "downstream_id" => ?downstream_id);
        let mut is_new_pushdown_causet = false;
        let pushdown_causet = self.capture_branes.entry(brane_id).or_insert_with(|| {
            let d = pushdown_causet::new(brane_id);
            is_new_pushdown_causet = true;
            d
        });

        let downstream_state = downstream.get_state();
        let checkpoint_ts = request.checkpoint_ts;
        let sched = self.interlock_semaphore.clone();
        let batch_size = self.scan_batch_size;

        if !pushdown_causet.subscribe(downstream) {
            conn.unsubscribe(request.get_brane_id());
            if is_new_pushdown_causet {
                self.capture_branes.remove(&request.get_brane_id());
            }
            return;
        }
        let change_cmd = if is_new_pushdown_causet {
            // The brane has never been registered.
            // Subscribe the change events of the brane.
            let old_id = self.semaphore.subscribe_brane(brane_id, pushdown_causet.id);
            assert!(
                old_id.is_none(),
                "brane {} must not be observed twice, old ObserveID {:?}, new ObserveID {:?}",
                brane_id,
                old_id,
                pushdown_causet.id
            );

            ChangeCmd::RegisterSemaphore {
                observe_id: pushdown_causet.id,
                brane_id,
                enabled: pushdown_causet.enabled(),
            }
        } else {
            ChangeCmd::Snapshot {
                observe_id: pushdown_causet.id,
                brane_id,
            }
        };
        let txn_extra_op = request.get_extra_op();
        if txn_extra_op != TxnExtraOp::Noop {
            pushdown_causet.txn_extra_op = request.get_extra_op();
            if let Some(reader) = self.store_meta.dagger().unwrap().readers.get(&brane_id) {
                reader.txn_extra_op.store(txn_extra_op);
            }
        }
        let init = Initializer {
            sched,
            brane_id,
            conn_id,
            downstream_id,
            batch_size,
            downstream_state: downstream_state.clone(),
            txn_extra_op: pushdown_causet.txn_extra_op,
            observe_id: pushdown_causet.id,
            checkpoint_ts: checkpoint_ts.into(),
            build_resolver: is_new_pushdown_causet,
        };

        let (cb, fut) = violetabftstore::interlock::::future::paired_future_callback();
        let interlock_semaphore = self.interlock_semaphore.clone();
        let deregister_downstream = move |err| {
            warn!("causet_context lightlike capture change cmd failed"; "brane_id" => brane_id, "error" => ?err);
            let deregister = Deregister::Downstream {
                brane_id,
                downstream_id,
                conn_id,
                err: Some(err),
            };
            if let Err(e) = interlock_semaphore.schedule(Task::Deregister(deregister)) {
                error!("schedule causet_context task failed"; "error" => ?e);
            }
        };
        let interlock_semaphore = self.interlock_semaphore.clone();
        if let Err(e) = self.violetabft_router.significant_lightlike(
            brane_id,
            SignificantMsg::CaptureChange {
                cmd: change_cmd,
                brane_epoch: request.take_brane_epoch(),
                callback: Callback::Read(Box::new(move |resp| {
                    if let Err(e) = interlock_semaphore.schedule(Task::InitDownstream {
                        downstream_id,
                        downstream_state,
                        cb: Box::new(move || {
                            cb(resp);
                        }),
                    }) {
                        error!("schedule causet_context task failed"; "error" => ?e);
                    }
                })),
            },
        ) {
            deregister_downstream(Error::Request(e.into()));
            return;
        }
        self.workers.spawn(async move {
            match fut.await {
                Ok(resp) => init.on_change_cmd(resp),
                Err(e) => deregister_downstream(Error::Other(box_err!(e))),
            }
        });
    }

    pub fn on_multi_batch(&mut self, multi: Vec<CmdBatch>, old_value_cb: OldValueCallback) {
        let old_value_cb = Rc::new(RefCell::new(old_value_cb));
        for batch in multi {
            let brane_id = batch.brane_id;
            let mut deregister = None;
            if let Some(pushdown_causet) = self.capture_branes.get_mut(&brane_id) {
                if pushdown_causet.has_failed() {
                    // Skip the batch if the pushdown_causet has failed.
                    continue;
                }
                if let Err(e) =
                    pushdown_causet.on_batch(batch, old_value_cb.clone(), &mut self.old_value_cache)
                {
                    assert!(pushdown_causet.has_failed());
                    // pushdown_causet has error, deregister the corresponding brane.
                    deregister = Some(Deregister::Brane {
                        brane_id,
                        observe_id: pushdown_causet.id,
                        err: e,
                    });
                }
            }
            if let Some(deregister) = deregister {
                self.on_deregister(deregister);
            }
        }
    }

    pub fn on_incremental_scan(
        &mut self,
        brane_id: u64,
        downstream_id: DownstreamID,
        entries: Vec<Option<TxnEntry>>,
    ) {
        if let Some(pushdown_causet) = self.capture_branes.get_mut(&brane_id) {
            pushdown_causet.on_scan(downstream_id, entries);
        } else {
            warn!("brane not found on incremental scan"; "brane_id" => brane_id);
        }
    }

    fn on_brane_ready(&mut self, observe_id: ObserveID, resolver: Resolver, brane: Brane) {
        let brane_id = brane.get_id();
        if let Some(pushdown_causet) = self.capture_branes.get_mut(&brane_id) {
            if pushdown_causet.id == observe_id {
                for downstream in pushdown_causet.on_brane_ready(resolver, brane) {
                    let conn_id = downstream.get_conn_id();
                    if !pushdown_causet.subscribe(downstream) {
                        let conn = self.connections.get_mut(&conn_id).unwrap();
                        conn.unsubscribe(brane_id);
                    }
                }
            } else {
                debug!("stale brane ready";
                    "brane_id" => brane.get_id(),
                    "observe_id" => ?observe_id,
                    "current_id" => ?pushdown_causet.id);
            }
        } else {
            debug!("brane not found on brane ready (finish building resolver)";
                "brane_id" => brane.get_id());
        }
    }

    fn on_min_ts(&mut self, branes: Vec<u64>, min_ts: TimeStamp) {
        let mut resolved_branes = Vec::with_capacity(branes.len());
        self.min_resolved_ts = TimeStamp::max();
        for brane_id in branes {
            if let Some(pushdown_causet) = self.capture_branes.get_mut(&brane_id) {
                if let Some(resolved_ts) = pushdown_causet.on_min_ts(min_ts) {
                    if resolved_ts < self.min_resolved_ts {
                        self.min_resolved_ts = resolved_ts;
                        self.min_ts_brane_id = brane_id;
                    }
                    resolved_branes.push(brane_id);
                }
            }
        }
        self.broadcast_resolved_ts(resolved_branes);
    }

    fn broadcast_resolved_ts(&self, branes: Vec<u64>) {
        let mut resolved_ts = ResolvedTs::default();
        resolved_ts.branes = branes;
        resolved_ts.ts = self.min_resolved_ts.into_inner();

        let lightlike_causet_context_event = |conn: &Conn, event| {
            if let Err(e) = conn.get_sink().try_lightlike(event) {
                match e {
                    crossbeam::TrylightlikeError::Disconnected(_) => {
                        debug!("lightlike event failed, disconnected";
                            "conn_id" => ?conn.get_id(), "downstream" => conn.get_peer());
                    }
                    crossbeam::TrylightlikeError::Full(_) => {
                        info!("lightlike event failed, full";
                            "conn_id" => ?conn.get_id(), "downstream" => conn.get_peer());
                    }
                }
            }
        };
        for conn in self.connections.values() {
            let features = if let Some(features) = conn.get_feature() {
                features
            } else {
                // None means there is no downsteam registered yet.
                continue;
            };

            if features.contains(FeatureGate::BATCH_RESOLVED_TS) {
                lightlike_causet_context_event(conn, causet_contextEvent::ResolvedTs(resolved_ts.clone()));
            } else {
                // Fallback to previous non-batch resolved ts event.
                for brane_id in &resolved_ts.branes {
                    self.broadcast_resolved_ts_compact(*brane_id, resolved_ts.ts, conn);
                }
            }
        }
    }

    fn broadcast_resolved_ts_compact(&self, brane_id: u64, resolved_ts: u64, conn: &Conn) {
        let downstream_id = match conn.downstream_id(brane_id) {
            Some(downstream_id) => downstream_id,
            // No such brane registers in the connection.
            None => return,
        };
        let pushdown_causet = match self.capture_branes.get(&brane_id) {
            Some(pushdown_causet) => pushdown_causet,
            // No such brane registers in the lightlikepoint.
            None => return,
        };
        let downstream = match pushdown_causet.downstream(downstream_id) {
            Some(downstream) => downstream,
            // No such downstream registers in the pushdown_causet.
            None => return,
        };
        let mut event = Event::default();
        event.brane_id = brane_id;
        event.event = Some(Event_oneof_event::ResolvedTs(resolved_ts));
        downstream.sink_event(event);
    }

    fn register_min_ts_event(&self) {
        let timeout = self.timer.delay(self.min_ts_interval);
        let fidel_client = self.fidel_client.clone();
        let interlock_semaphore = self.interlock_semaphore.clone();
        let violetabft_router = self.violetabft_router.clone();
        let branes: Vec<(u64, ObserveID)> = self
            .capture_branes
            .iter()
            .map(|(brane_id, pushdown_causet)| (*brane_id, pushdown_causet.id))
            .collect();
        let cm: ConcurrencyManager = self.interlocking_directorate.clone();
        let fut = async move {
            let _ = timeout.compat().await;
            // Ignore get tso errors since we will retry every `min_ts_interval`.
            let mut min_ts = fidel_client.get_tso().await.unwrap_or_default();

            // Sync with concurrency manager so that it can work correctly when optimizations
            // like async commit is enabled.
            // Note: This step must be done before scheduling `Task::MinTS` task, and the
            // resolver must be checked in or after `Task::MinTS`' execution.
            cm.fidelio_max_ts(min_ts);
            if let Some(min_mem_lock_ts) = cm.global_min_lock_ts() {
                if min_mem_lock_ts < min_ts {
                    min_ts = min_mem_lock_ts;
                }
            }

            // TODO: lightlike a message to violetabftstore would consume too much cpu time,
            // try to handle it outside violetabftstore.
            let branes: Vec<_> = branes.iter().copied().map(|(brane_id, observe_id)| {
                let interlock_semaphore_clone = interlock_semaphore.clone();
                let violetabft_router_clone = violetabft_router.clone();
                async move {
                    let (tx, rx) = futures::channel::oneshot::channel();
                    if let Err(e) = violetabft_router_clone.significant_lightlike(
                        brane_id,
                        SignificantMsg::LeaderCallback(Callback::Read(Box::new(move |resp| {
                            let resp = if resp.response.get_header().has_error() {
                                None
                            } else {
                                Some(brane_id)
                            };
                            if tx.lightlike(resp).is_err() {
                                error!("causet_context lightlike tso response failed");
                            }
                        }))),
                    ) {
                        warn!("causet_context lightlike LeaderCallback failed"; "err" => ?e, "min_ts" => min_ts);
                        let deregister = Deregister::Brane {
                            observe_id,
                            brane_id,
                            err: Error::Request(e.into()),
                        };
                        if let Err(e) = interlock_semaphore_clone.schedule(Task::Deregister(deregister)) {
                            error!("schedule causet_context task failed"; "error" => ?e);
                            return None;
                        }
                    }
                    rx.await.unwrap_or(None)
                }
            }).collect();
            let resps = futures::future::join_all(branes).await;
            let branes = resps
                .into_iter()
                .filter_map(|resp| resp)
                .collect::<Vec<u64>>();
            if !branes.is_empty() {
                match interlock_semaphore.schedule(Task::MinTS { branes, min_ts }) {
                    Ok(_) | Err(ScheduleError::Stopped(_)) => (),
                    // Must schedule `RegisterMinTsEvent` event otherwise resolved ts can not
                    // advance normally.
                    Err(err) => panic!("failed to schedule min ts event, error: {:?}", err),
                }
            }
            match interlock_semaphore.schedule(Task::RegisterMinTsEvent) {
                Ok(_) | Err(ScheduleError::Stopped(_)) => (),
                // Must schedule `RegisterMinTsEvent` event otherwise resolved ts can not
                // advance normally.
                Err(err) => panic!("failed to regiester min ts event, error: {:?}", err),
            }
        };
        self.tso_worker.spawn(fut);
    }

    fn on_open_conn(&mut self, conn: Conn) {
        self.connections.insert(conn.get_id(), conn);
    }

    fn flush_all(&self) {
        self.connections.iter().for_each(|(_, conn)| conn.flush());
    }
}

struct Initializer {
    sched: Interlock_Semaphore<Task>,

    brane_id: u64,
    observe_id: ObserveID,
    downstream_id: DownstreamID,
    downstream_state: Arc<AtomicCell<DownstreamState>>,
    conn_id: ConnID,
    checkpoint_ts: TimeStamp,
    batch_size: usize,
    txn_extra_op: TxnExtraOp,

    build_resolver: bool,
}

impl Initializer {
    fn on_change_cmd(&self, mut resp: ReadResponse<LmdbSnapshot>) {
        if let Some(brane_snapshot) = resp.snapshot {
            assert_eq!(self.brane_id, brane_snapshot.get_brane().get_id());
            let brane = brane_snapshot.get_brane().clone();
            self.async_incremental_scan(brane_snapshot, brane);
        } else {
            assert!(
                resp.response.get_header().has_error(),
                "no snapshot and no error? {:?}",
                resp.response
            );
            let err = resp.response.take_header().take_error();
            let deregister = Deregister::Brane {
                brane_id: self.brane_id,
                observe_id: self.observe_id,
                err: Error::Request(err),
            };
            if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                error!("schedule causet_context task failed"; "error" => ?e);
            }
        }
    }

    fn async_incremental_scan<S: Snapshot + 'static>(&self, snap: S, brane: Brane) {
        let downstream_id = self.downstream_id;
        let conn_id = self.conn_id;
        let brane_id = brane.get_id();
        debug!("async incremental scan";
            "brane_id" => brane_id,
            "downstream_id" => ?downstream_id,
            "observe_id" => ?self.observe_id);

        let mut resolver = if self.build_resolver {
            Some(Resolver::new(brane_id))
        } else {
            None
        };

        fail_point!("causet_context_incremental_scan_spacelike");

        let spacelike = Instant::now_coarse();
        // Time cone: (checkpoint_ts, current]
        let current = TimeStamp::max();
        let mut scanner = ScannerBuilder::new(snap, current, false)
            .cone(None, None)
            .build_delta_scanner(self.checkpoint_ts, self.txn_extra_op)
            .unwrap();
        let mut done = false;
        while !done {
            if self.downstream_state.load() != DownstreamState::Normal {
                info!("async incremental scan canceled";
                    "brane_id" => brane_id,
                    "downstream_id" => ?downstream_id,
                    "observe_id" => ?self.observe_id);
                return;
            }
            let entries = match Self::scan_batch(&mut scanner, self.batch_size, resolver.as_mut()) {
                Ok(res) => res,
                Err(e) => {
                    error!("causet_context scan entries failed"; "error" => ?e, "brane_id" => brane_id);
                    // TODO: record in metrics.
                    let deregister = Deregister::Downstream {
                        brane_id,
                        downstream_id,
                        conn_id,
                        err: Some(e),
                    };
                    if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                        error!("schedule causet_context task failed"; "error" => ?e, "brane_id" => brane_id);
                    }
                    return;
                }
            };
            // If the last element is None, it means scanning is finished.
            if let Some(None) = entries.last() {
                done = true;
            }
            debug!("causet_context scan entries"; "len" => entries.len(), "brane_id" => brane_id);
            fail_point!("before_schedule_incremental_scan");
            let scanned = Task::IncrementalScan {
                brane_id,
                downstream_id,
                entries,
            };
            if let Err(e) = self.sched.schedule(scanned) {
                error!("schedule causet_context task failed"; "error" => ?e, "brane_id" => brane_id);
                return;
            }
        }

        let takes = spacelike.elapsed();
        if let Some(resolver) = resolver {
            self.finish_building_resolver(resolver, brane, takes);
        }

        causet_context_SCAN_DURATION_HISTOGRAM.observe(takes.as_secs_f64());
    }

    fn scan_batch<S: Snapshot>(
        scanner: &mut DeltaScanner<S>,
        batch_size: usize,
        resolver: Option<&mut Resolver>,
    ) -> Result<Vec<Option<TxnEntry>>> {
        let mut entries = Vec::with_capacity(batch_size);
        while entries.len() < entries.capacity() {
            match scanner.next_entry()? {
                Some(entry) => {
                    entries.push(Some(entry));
                }
                None => {
                    entries.push(None);
                    break;
                }
            }
        }

        if let Some(resolver) = resolver {
            // Track the locks.
            for entry in &entries {
                if let Some(TxnEntry::Prewrite { dagger, .. }) = entry {
                    let (encoded_key, value) = dagger;
                    let key = Key::from_encoded_slice(encoded_key).into_raw().unwrap();
                    let dagger = Dagger::parse(value)?;
                    match dagger.lock_type {
                        LockType::Put | LockType::Delete => resolver.track_lock(dagger.ts, key),
                        _ => (),
                    };
                }
            }
        }

        Ok(entries)
    }

    fn finish_building_resolver(&self, mut resolver: Resolver, brane: Brane, takes: Duration) {
        let observe_id = self.observe_id;
        resolver.init();
        let rts = resolver.resolve(TimeStamp::zero());
        info!(
            "resolver initialized and schedule resolver ready";
            "brane_id" => brane.get_id(),
            "resolved_ts" => rts,
            "lock_count" => resolver.locks().len(),
            "observe_id" => ?observe_id,
            "takes" => ?takes,
        );

        fail_point!("before_schedule_resolver_ready");
        if let Err(e) = self.sched.schedule(Task::ResolverReady {
            observe_id,
            resolver,
            brane,
        }) {
            error!("schedule task failed"; "error" => ?e);
        }
    }
}

impl<T: 'static + VioletaBftStoreRouter<LmdbEngine>> Runnable for node<T> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("run causet_context task"; "task" => %task);
        match task {
            Task::MinTS { branes, min_ts } => self.on_min_ts(branes, min_ts),
            Task::Register {
                request,
                downstream,
                conn_id,
                version,
            } => self.on_register(request, downstream, conn_id, version),
            Task::ResolverReady {
                observe_id,
                resolver,
                brane,
            } => self.on_brane_ready(observe_id, resolver, brane),
            Task::Deregister(deregister) => self.on_deregister(deregister),
            Task::IncrementalScan {
                brane_id,
                downstream_id,
                entries,
            } => {
                self.on_incremental_scan(brane_id, downstream_id, entries);
            }
            Task::MultiBatch {
                multi,
                old_value_cb,
            } => self.on_multi_batch(multi, old_value_cb),
            Task::OpenConn { conn } => self.on_open_conn(conn),
            Task::RegisterMinTsEvent => self.register_min_ts_event(),
            Task::InitDownstream {
                downstream_id,
                downstream_state,
                cb,
            } => {
                debug!("downstream was initialized"; "downstream_id" => ?downstream_id);
                downstream_state
                    .compare_and_swap(DownstreamState::Uninitialized, DownstreamState::Normal);
                cb();
            }
            Task::TxnExtra(txn_extra) => {
                for (k, v) in txn_extra.old_values {
                    self.old_value_cache.cache.insert(k, v);
                }
            }
            Task::Validate(brane_id, validate) => {
                validate(self.capture_branes.get(&brane_id));
            }
        }
        self.flush_all();
    }
}

impl<T: 'static + VioletaBftStoreRouter<LmdbEngine>> RunnableWithTimer for node<T> {
    type TimeoutTask = ();

    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        causet_context_CAPTURED_REGION_COUNT.set(self.capture_branes.len() as i64);
        if self.min_resolved_ts != TimeStamp::max() {
            causet_context_MIN_RESOLVED_TS_REGION.set(self.min_ts_brane_id as i64);
            causet_context_MIN_RESOLVED_TS.set(self.min_resolved_ts.physical() as i64);
        }
        self.min_resolved_ts = TimeStamp::max();
        self.min_ts_brane_id = 0;

        let cache_size: usize = self
            .old_value_cache
            .cache
            .iter()
            .map(|(k, v)| k.as_encoded().len() + v.0.as_ref().map_or(0, |v| v.size()))
            .sum();
        causet_context_OLD_VALUE_CACHE_BYTES.set(cache_size as i64);
        causet_context_OLD_VALUE_CACHE_ACCESS.add(self.old_value_cache.access_count as i64);
        causet_context_OLD_VALUE_CACHE_MISS.add(self.old_value_cache.miss_count as i64);
        self.old_value_cache.access_count = 0;
        self.old_value_cache.miss_count = 0;

        timer.add_task(Duration::from_millis(METRICS_FLUSH_INTERVAL), ());
    }
}

pub struct causet_contextTxnExtraInterlock_Semaphore {
    interlock_semaphore: Interlock_Semaphore<Task>,
}

impl causet_contextTxnExtraInterlock_Semaphore {
    pub fn new(interlock_semaphore: Interlock_Semaphore<Task>) -> causet_contextTxnExtraInterlock_Semaphore {
        causet_contextTxnExtraInterlock_Semaphore { interlock_semaphore }
    }
}

impl TxnExtraInterlock_Semaphore for causet_contextTxnExtraInterlock_Semaphore {
    fn schedule(&self, txn_extra: TxnExtra) {
        if let Err(e) = self.interlock_semaphore.schedule(Task::TxnExtra(txn_extra)) {
            error!("causet_context schedule txn extra failed"; "err" => ?e);
        }
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use edb::DATA_CausetS;
    #[causet(feature = "prost-codec")]
    use ekvproto::causet_context_timeshare::event::Event as Event_oneof_event;
    use ekvproto::error_timeshare::Error as ErrorHeader;
    use ekvproto::kvrpc_timeshare::Context;
    use violetabftstore::errors::Error as VioletaBftStoreError;
    use violetabftstore::store::msg::CasualMessage;
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, lightlikeer};
    use tempfile::TempDir;
    use test_violetabftstore::MockVioletaBftStoreRouter;
    use test_violetabftstore::TestFidelClient;
    use edb::causet_storage::kv::Engine;
    use edb::causet_storage::tail_pointer::tests::*;
    use edb::causet_storage::TestEngineBuilder;
    use violetabftstore::interlock::::collections::HashSet;
    use violetabftstore::interlock::::config::ReadableDuration;
    use violetabftstore::interlock::::mpsc::batch;
    use violetabftstore::interlock::::worker::{dummy_interlock_semaphore, Builder as WorkerBuilder, Worker};

    struct ReceiverRunnable<T> {
        tx: lightlikeer<T>,
    }

    impl<T: Display> Runnable for ReceiverRunnable<T> {
        type Task = T;

        fn run(&mut self, task: T) {
            self.tx.lightlike(task).unwrap();
        }
    }

    fn new_receiver_worker<T: Display + lightlike + 'static>() -> (Worker<T>, Receiver<T>) {
        let (tx, rx) = channel();
        let runnable = ReceiverRunnable { tx };
        let mut worker = WorkerBuilder::new("test-receiver-worker").create();
        worker.spacelike(runnable).unwrap();
        (worker, rx)
    }

    fn mock_initializer() -> (Worker<Task>, Runtime, Initializer, Receiver<Task>) {
        let (receiver_worker, rx) = new_receiver_worker();

        let pool = Builder::new()
            .threaded_interlock_semaphore()
            .thread_name("test-initializer-worker")
            .core_threads(4)
            .build()
            .unwrap();
        let downstream_state = Arc::new(AtomicCell::new(DownstreamState::Normal));
        let initializer = Initializer {
            sched: receiver_worker.interlock_semaphore(),

            brane_id: 1,
            observe_id: ObserveID::new(),
            downstream_id: DownstreamID::new(),
            downstream_state,
            conn_id: ConnID::new(),
            checkpoint_ts: 1.into(),
            batch_size: 1,
            txn_extra_op: TxnExtraOp::Noop,
            build_resolver: true,
        };

        (receiver_worker, pool, initializer, rx)
    }

    #[test]
    fn test_initializer_build_resolver() {
        let (mut worker, _pool, mut initializer, rx) = mock_initializer();

        let temp = TempDir::new().unwrap();
        let engine = TestEngineBuilder::new()
            .path(temp.path())
            .causets(DATA_CausetS)
            .build()
            .unwrap();

        let mut expected_locks = BTreeMap::<TimeStamp, HashSet<Vec<u8>>>::new();

        // Pessimistic locks should not be tracked
        for i in 0..10 {
            let k = &[b'k', i];
            let ts = TimeStamp::new(i as _);
            must_acquire_pessimistic_lock(&engine, k, k, ts, ts);
        }

        for i in 10..100 {
            let (k, v) = (&[b'k', i], &[b'v', i]);
            let ts = TimeStamp::new(i as _);
            must_prewrite_put(&engine, k, v, k, ts);
            expected_locks.entry(ts).or_default().insert(k.to_vec());
        }

        let brane = Brane::default();
        let snap = engine.snapshot(&Context::default()).unwrap();

        let check_result = || loop {
            let task = rx.recv().unwrap();
            match task {
                Task::ResolverReady { resolver, .. } => {
                    assert_eq!(resolver.locks(), &expected_locks);
                    return;
                }
                Task::IncrementalScan { .. } => continue,
                t => panic!("unepxected task {} received", t),
            }
        };

        initializer.async_incremental_scan(snap.clone(), brane.clone());
        check_result();
        initializer.batch_size = 1000;
        initializer.async_incremental_scan(snap.clone(), brane.clone());
        check_result();

        initializer.batch_size = 10;
        initializer.async_incremental_scan(snap.clone(), brane.clone());
        check_result();

        initializer.batch_size = 11;
        initializer.async_incremental_scan(snap.clone(), brane.clone());
        check_result();

        initializer.build_resolver = false;
        initializer.async_incremental_scan(snap.clone(), brane.clone());

        loop {
            let task = rx.recv_timeout(Duration::from_secs(1));
            match task {
                Ok(Task::IncrementalScan { .. }) => continue,
                Ok(t) => panic!("unepxected task {} received", t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => panic!("unexpected err {:?}", e),
            }
        }

        // Test cancellation.
        initializer.downstream_state.store(DownstreamState::Stopped);
        initializer.async_incremental_scan(snap, brane);

        loop {
            let task = rx.recv_timeout(Duration::from_secs(1));
            match task {
                Ok(t) => panic!("unepxected task {} received", t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => panic!("unexpected err {:?}", e),
            }
        }

        worker.stop().unwrap().join().unwrap();
    }

    #[test]
    fn test_violetabftstore_is_busy() {
        let (task_sched, task_rx) = dummy_interlock_semaphore();
        let violetabft_router = MockVioletaBftStoreRouter::new();
        let semaphore = causet_contextSemaphore::new(task_sched.clone());
        let fidel_client = Arc::new(TestFidelClient::new(0, true));
        let mut ep = node::new(
            &causet_contextConfig::default(),
            fidel_client,
            task_sched,
            violetabft_router.clone(),
            semaphore,
            Arc::new(Mutex::new(StoreMeta::new(0))),
            ConcurrencyManager::new(1.into()),
        );
        let (tx, _rx) = batch::unbounded(1);

        // Fill the channel.
        let _violetabft_rx = violetabft_router.add_brane(1 /* brane id */, 1 /* cap */);
        loop {
            if let Err(VioletaBftStoreError::Transport(_)) =
                violetabft_router.lightlike_casual_msg(1, CasualMessage::ClearBraneSize)
            {
                break;
            }
        }
        // Make sure channel is full.
        violetabft_router
            .lightlike_casual_msg(1, CasualMessage::ClearBraneSize)
            .unwrap_err();

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_brane_id(1);
        let brane_epoch = req.get_brane_epoch().clone();
        let downstream = Downstream::new("".to_string(), brane_epoch, 0, conn_id);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_branes.len(), 1);

        for _ in 0..5 {
            if let Ok(Some(Task::Deregister(Deregister::Downstream { err, .. }))) =
                task_rx.recv_timeout(Duration::from_secs(1))
            {
                if let Some(Error::Request(err)) = err {
                    assert!(!err.has_server_is_busy());
                }
            }
        }
    }

    #[test]
    fn test_register() {
        let (task_sched, _task_rx) = dummy_interlock_semaphore();
        let violetabft_router = MockVioletaBftStoreRouter::new();
        let _violetabft_rx = violetabft_router.add_brane(1 /* brane id */, 100 /* cap */);
        let semaphore = causet_contextSemaphore::new(task_sched.clone());
        let fidel_client = Arc::new(TestFidelClient::new(0, true));
        let mut ep = node::new(
            &causet_contextConfig {
                min_ts_interval: ReadableDuration(Duration::from_secs(60)),
                ..Default::default()
            },
            fidel_client,
            task_sched,
            violetabft_router,
            semaphore,
            Arc::new(Mutex::new(StoreMeta::new(0))),
            ConcurrencyManager::new(1.into()),
        );
        let (tx, rx) = batch::unbounded(1);

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_brane_id(1);
        let brane_epoch = req.get_brane_epoch().clone();
        let downstream = Downstream::new("".to_string(), brane_epoch.clone(), 1, conn_id);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        assert_eq!(ep.capture_branes.len(), 1);

        // duplicate request error.
        let downstream = Downstream::new("".to_string(), brane_epoch.clone(), 2, conn_id);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        let causet_context_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let causet_contextEvent::Event(mut e) = causet_context_event {
            assert_eq!(e.brane_id, 1);
            assert_eq!(e.request_id, 2);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_duplicate_request());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown causet_context event {:?}", causet_context_event);
        }
        assert_eq!(ep.capture_branes.len(), 1);

        // Compatibility error.
        let downstream = Downstream::new("".to_string(), brane_epoch, 3, conn_id);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        let causet_context_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let causet_contextEvent::Event(mut e) = causet_context_event {
            assert_eq!(e.brane_id, 1);
            assert_eq!(e.request_id, 3);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_compatibility());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown causet_context event {:?}", causet_context_event);
        }
        assert_eq!(ep.capture_branes.len(), 1);
    }

    #[test]
    fn test_feature_gate() {
        let (task_sched, _task_rx) = dummy_interlock_semaphore();
        let violetabft_router = MockVioletaBftStoreRouter::new();
        let _violetabft_rx = violetabft_router.add_brane(1 /* brane id */, 100 /* cap */);
        let semaphore = causet_contextSemaphore::new(task_sched.clone());
        let fidel_client = Arc::new(TestFidelClient::new(0, true));
        let mut ep = node::new(
            &causet_contextConfig {
                min_ts_interval: ReadableDuration(Duration::from_secs(60)),
                ..Default::default()
            },
            fidel_client,
            task_sched,
            violetabft_router,
            semaphore,
            Arc::new(Mutex::new(StoreMeta::new(0))),
            ConcurrencyManager::new(1.into()),
        );

        let (tx, rx) = batch::unbounded(1);
        let mut brane = Brane::default();
        brane.set_id(1);
        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_brane_id(1);
        let brane_epoch = req.get_brane_epoch().clone();
        let downstream = Downstream::new("".to_string(), brane_epoch.clone(), 0, conn_id);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        let mut resolver = Resolver::new(1);
        resolver.init();
        let observe_id = ep.capture_branes[&1].id;
        ep.on_brane_ready(observe_id, resolver, brane.clone());
        ep.run(Task::MinTS {
            branes: vec![1],
            min_ts: TimeStamp::from(1),
        });
        let causet_context_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let causet_contextEvent::ResolvedTs(r) = causet_context_event {
            assert_eq!(r.branes, vec![1]);
            assert_eq!(r.ts, 1);
        } else {
            panic!("unknown causet_context event {:?}", causet_context_event);
        }

        // Register brane 2 to the conn.
        req.set_brane_id(2);
        let downstream = Downstream::new("".to_string(), brane_epoch.clone(), 0, conn_id);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        let mut resolver = Resolver::new(2);
        resolver.init();
        brane.set_id(2);
        let observe_id = ep.capture_branes[&2].id;
        ep.on_brane_ready(observe_id, resolver, brane);
        ep.run(Task::MinTS {
            branes: vec![1, 2],
            min_ts: TimeStamp::from(2),
        });
        let causet_context_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let causet_contextEvent::ResolvedTs(mut r) = causet_context_event {
            r.branes.as_mut_slice().sort();
            assert_eq!(r.branes, vec![1, 2]);
            assert_eq!(r.ts, 2);
        } else {
            panic!("unknown causet_context event {:?}", causet_context_event);
        }

        // Register brane 3 to another conn which is not support batch resolved ts.
        let (tx, rx2) = batch::unbounded(1);
        let mut brane = Brane::default();
        brane.set_id(3);
        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        req.set_brane_id(3);
        let downstream = Downstream::new("".to_string(), brane_epoch, 3, conn_id);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 5),
        });
        let mut resolver = Resolver::new(3);
        resolver.init();
        brane.set_id(3);
        let observe_id = ep.capture_branes[&3].id;
        ep.on_brane_ready(observe_id, resolver, brane);
        ep.run(Task::MinTS {
            branes: vec![1, 2, 3],
            min_ts: TimeStamp::from(3),
        });
        let causet_context_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let causet_contextEvent::ResolvedTs(mut r) = causet_context_event {
            r.branes.as_mut_slice().sort();
            // Although brane 3 is not register in the first conn, batch resolved ts
            // lightlikes all brane ids.
            assert_eq!(r.branes, vec![1, 2, 3]);
            assert_eq!(r.ts, 3);
        } else {
            panic!("unknown causet_context event {:?}", causet_context_event);
        }
        let causet_context_event = rx2.recv_timeout(Duration::from_millis(500)).unwrap();
        if let causet_contextEvent::Event(mut e) = causet_context_event {
            assert_eq!(e.brane_id, 3);
            assert_eq!(e.request_id, 3);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::ResolvedTs(ts) => {
                    assert_eq!(ts, 3);
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown causet_context event {:?}", causet_context_event);
        }
    }

    #[test]
    fn test_deregister() {
        let (task_sched, _task_rx) = dummy_interlock_semaphore();
        let violetabft_router = MockVioletaBftStoreRouter::new();
        let _violetabft_rx = violetabft_router.add_brane(1 /* brane id */, 100 /* cap */);
        let semaphore = causet_contextSemaphore::new(task_sched.clone());
        let fidel_client = Arc::new(TestFidelClient::new(0, true));
        let mut ep = node::new(
            &causet_contextConfig::default(),
            fidel_client,
            task_sched,
            violetabft_router,
            semaphore,
            Arc::new(Mutex::new(StoreMeta::new(0))),
            ConcurrencyManager::new(1.into()),
        );
        let (tx, rx) = batch::unbounded(1);

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_brane_id(1);
        let brane_epoch = req.get_brane_epoch().clone();
        let downstream = Downstream::new("".to_string(), brane_epoch.clone(), 0, conn_id);
        let downstream_id = downstream.get_id();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_branes.len(), 1);

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());
        let deregister = Deregister::Downstream {
            brane_id: 1,
            downstream_id,
            conn_id,
            err: Some(Error::Request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
        loop {
            let causet_context_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
            if let causet_contextEvent::Event(mut e) = causet_context_event {
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => {
                        assert!(err.has_not_leader());
                        break;
                    }
                    other => panic!("unknown event {:?}", other),
                }
            }
        }
        assert_eq!(ep.capture_branes.len(), 0);

        let downstream = Downstream::new("".to_string(), brane_epoch.clone(), 0, conn_id);
        let new_downstream_id = downstream.get_id();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_branes.len(), 1);

        let deregister = Deregister::Downstream {
            brane_id: 1,
            downstream_id,
            conn_id,
            err: Some(Error::Request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
        assert_eq!(ep.capture_branes.len(), 1);

        let deregister = Deregister::Downstream {
            brane_id: 1,
            downstream_id: new_downstream_id,
            conn_id,
            err: Some(Error::Request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
        let causet_context_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        loop {
            if let causet_contextEvent::Event(mut e) = causet_context_event {
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => {
                        assert!(err.has_not_leader());
                        break;
                    }
                    other => panic!("unknown event {:?}", other),
                }
            }
        }
        assert_eq!(ep.capture_branes.len(), 0);

        // Stale deregister should be filtered.
        let downstream = Downstream::new("".to_string(), brane_epoch, 0, conn_id);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_branes.len(), 1);
        let deregister = Deregister::Brane {
            brane_id: 1,
            // A stale ObserveID (different from the actual one).
            observe_id: ObserveID::new(),
            err: Error::Request(err_header),
        };
        ep.run(Task::Deregister(deregister));
        match rx.recv_timeout(Duration::from_millis(500)) {
            Err(_) => (),
            Ok(other) => panic!("unknown event {:?}", other),
        }
        assert_eq!(ep.capture_branes.len(), 1);
    }
}
