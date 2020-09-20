// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use ekvproto::raft_serverpb::VioletaBftMessage;
use violetabft::eraftpb::MessageType;
use std::sync::{Arc, RwLock};

use crate::server::metrics::*;
use crate::server::raft_client::VioletaBftClient;
use crate::server::resolve::StoreAddrResolver;
use crate::server::snap::Task as SnapTask;
use crate::server::Result;
use engine_lmdb::LmdbEngine;
use violetabft::SnapshotStatus;
use violetabftstore::router::VioletaBftStoreRouter;
use violetabftstore::store::Transport;
use violetabftstore::Result as VioletaBftStoreResult;
use einsteindb_util::collections::HashSet;
use einsteindb_util::worker::Scheduler;
use einsteindb_util::HandyRwLock;

pub struct ServerTransport<T, S>
where
    T: VioletaBftStoreRouter<LmdbEngine> + 'static,
    S: StoreAddrResolver + 'static,
{
    raft_client: Arc<RwLock<VioletaBftClient<T>>>,
    snap_scheduler: Scheduler<SnapTask>,
    pub raft_router: T,
    resolving: Arc<RwLock<HashSet<u64>>>,
    resolver: S,
}

impl<T, S> Clone for ServerTransport<T, S>
where
    T: VioletaBftStoreRouter<LmdbEngine> + 'static,
    S: StoreAddrResolver + 'static,
{
    fn clone(&self) -> Self {
        ServerTransport {
            raft_client: Arc::clone(&self.raft_client),
            snap_scheduler: self.snap_scheduler.clone(),
            raft_router: self.raft_router.clone(),
            resolving: Arc::clone(&self.resolving),
            resolver: self.resolver.clone(),
        }
    }
}

impl<T: VioletaBftStoreRouter<LmdbEngine> + 'static, S: StoreAddrResolver + 'static>
    ServerTransport<T, S>
{
    pub fn new(
        raft_client: Arc<RwLock<VioletaBftClient<T>>>,
        snap_scheduler: Scheduler<SnapTask>,
        raft_router: T,
        resolver: S,
    ) -> ServerTransport<T, S> {
        ServerTransport {
            raft_client,
            snap_scheduler,
            raft_router,
            resolving: Arc::new(RwLock::new(Default::default())),
            resolver,
        }
    }

    fn slightlike_store(&self, store_id: u64, msg: VioletaBftMessage) {
        // Wrapping the fail point in a closure, so we can modify
        // local variables without return,
        let transport_on_slightlike_store_fp = || {
            fail_point!("transport_on_slightlike_store", |sid| if let Some(sid) = sid {
                let sid: u64 = sid.parse().unwrap();
                if sid == store_id {
                    self.raft_client.wl().addrs.remove(&store_id);
                }
            })
        };
        transport_on_slightlike_store_fp();
        // check the corresponding token for store.
        // TODO: avoid clone
        let addr = self.raft_client.rl().addrs.get(&store_id).cloned();
        if let Some(addr) = addr {
            self.write_data(store_id, &addr, msg);
            return;
        }

        // No connection, try to resolve it.
        if self.resolving.rl().contains(&store_id) {
            RESOLVE_STORE_COUNTER_STATIC.resolving.inc();
            // If we are resolving the address, drop the message here.
            debug!(
                "store address is being resolved, msg dropped";
                "store_id" => store_id,
                "message" => ?msg
            );
            self.report_unreachable(msg);
            return;
        }

        debug!("begin to resolve store address"; "store_id" => store_id);
        RESOLVE_STORE_COUNTER_STATIC.resolve.inc();
        self.resolving.wl().insert(store_id);
        self.resolve(store_id, msg);
    }

    // TODO: remove allow unused mut.
    // Compiler warns `mut addr ` and `mut transport_on_resolve_fp`, when we disable
    // the `failpoints` feature.
    #[allow(unused_mut)]
    fn resolve(&self, store_id: u64, msg: VioletaBftMessage) {
        let trans = self.clone();
        let msg1 = msg.clone();
        let cb = Box::new(move |mut addr: Result<String>| {
            {
                // Wrapping the fail point in a closure, so we can modify
                // local variables without return.
                let mut transport_on_resolve_fp = || {
                    fail_point!(
                        "transport_snapshot_on_resolve",
                        msg.get_message().get_msg_type() == MessageType::MsgSnapshot,
                        |sid| if let Some(sid) = sid {
                            use std::mem;
                            let sid: u64 = sid.parse().unwrap();
                            if sid == store_id {
                                mem::swap(&mut addr, &mut Err(box_err!("injected failure")));
                            }
                        }
                    )
                };
                transport_on_resolve_fp();
            }

            // clear resolving.
            trans.resolving.wl().remove(&store_id);
            if let Err(e) = addr {
                RESOLVE_STORE_COUNTER_STATIC.failed.inc();
                error!("resolve store address failed"; "store_id" => store_id, "err" => ?e);
                trans.report_unreachable(msg);
                return;
            }

            RESOLVE_STORE_COUNTER_STATIC.success.inc();

            let addr = addr.unwrap();
            info!("resolve store address ok"; "store_id" => store_id, "addr" => %addr);
            trans.raft_client.wl().addrs.insert(store_id, addr.clone());
            trans.write_data(store_id, &addr, msg);
            // There may be no messages in the near future, so flush it immediately.
            trans.raft_client.wl().flush();
        });
        if let Err(e) = self.resolver.resolve(store_id, cb) {
            error!("resolve store address failed"; "store_id" => store_id, "err" => ?e);
            self.resolving.wl().remove(&store_id);
            RESOLVE_STORE_COUNTER_STATIC.failed.inc();
            self.report_unreachable(msg1);
        }
    }

    fn write_data(&self, store_id: u64, addr: &str, msg: VioletaBftMessage) {
        if msg.get_message().has_snapshot() {
            return self.slightlike_snapshot_sock(addr, msg);
        }
        if let Err(e) = self.raft_client.wl().slightlike(store_id, addr, msg) {
            error!("slightlike violetabft msg err"; "err" => ?e);
        }
    }

    fn slightlike_snapshot_sock(&self, addr: &str, msg: VioletaBftMessage) {
        let rep = self.new_snapshot_reporter(&msg);
        let cb = Box::new(move |res: Result<()>| {
            if res.is_err() {
                rep.report(SnapshotStatus::Failure);
            } else {
                rep.report(SnapshotStatus::Finish);
            }
        });
        if let Err(e) = self.snap_scheduler.schedule(SnapTask::Slightlike {
            addr: addr.to_owned(),
            msg,
            cb,
        }) {
            if let SnapTask::Slightlike { cb, .. } = e.into_inner() {
                error!(
                    "channel is unavailable, failed to schedule snapshot";
                    "to_addr" => addr
                );
                cb(Err(box_err!("failed to schedule snapshot")));
            }
        }
    }

    fn new_snapshot_reporter(&self, msg: &VioletaBftMessage) -> SnapshotReporter<T> {
        let brane_id = msg.get_brane_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let to_store_id = msg.get_to_peer().get_store_id();

        SnapshotReporter {
            raft_router: self.raft_router.clone(),
            brane_id,
            to_peer_id,
            to_store_id,
        }
    }

    pub fn report_unreachable(&self, msg: VioletaBftMessage) {
        let brane_id = msg.get_brane_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let store_id = msg.get_to_peer().get_store_id();

        // Report snapshot failure.
        if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
            self.new_snapshot_reporter(&msg)
                .report(SnapshotStatus::Failure);
        }

        if let Err(e) = self.raft_router.report_unreachable(brane_id, to_peer_id) {
            error!(?e;
                "report peer unreachable failed";
                "brane_id" => brane_id,
                "to_store_id" => store_id,
                "to_peer_id" => to_peer_id,
            );
        }
    }

    pub fn flush_raft_client(&mut self) {
        self.raft_client.wl().flush();
    }
}

impl<T, S> Transport for ServerTransport<T, S>
where
    T: VioletaBftStoreRouter<LmdbEngine> + 'static,
    S: StoreAddrResolver + 'static,
{
    fn slightlike(&mut self, msg: VioletaBftMessage) -> VioletaBftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        self.slightlike_store(to_store_id, msg);
        Ok(())
    }

    fn flush(&mut self) {
        self.flush_raft_client();
    }
}

struct SnapshotReporter<T: VioletaBftStoreRouter<LmdbEngine> + 'static> {
    raft_router: T,
    brane_id: u64,
    to_peer_id: u64,
    to_store_id: u64,
}

impl<T: VioletaBftStoreRouter<LmdbEngine> + 'static> SnapshotReporter<T> {
    pub fn report(&self, status: SnapshotStatus) {
        debug!(
            "slightlike snapshot";
            "to_peer_id" => self.to_peer_id,
            "brane_id" => self.brane_id,
            "status" => ?status
        );

        if status == SnapshotStatus::Failure {
            let store = self.to_store_id.to_string();
            REPORT_FAILURE_MSG_COUNTER
                .with_label_values(&["snapshot", &*store])
                .inc();
        };

        if let Err(e) =
            self.raft_router
                .report_snapshot_status(self.brane_id, self.to_peer_id, status)
        {
            error!(?e;
                "report snapshot to peer failes";
                "to_peer_id" => self.to_peer_id,
                "to_store_id" => self.to_store_id,
                "brane_id" => self.brane_id,
            );
        }
    }
}
