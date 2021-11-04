// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::future::{self, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{self, StreamExt, TryStreamExt};
use grpcio::{
    DuplexSink, Error as GrpcError, RequestStream, Result as GrpcResult, RpcContext, RpcStatus,
    RpcStatusCode, WriteFlags,
};
use ekvproto::causet_context_timeshare::{
    ChangeData, ChangeDataEvent, ChangeDataRequest, Compatibility, Event, ResolvedTs,
};
use protobuf::Message;
use security::{check_common_name, SecurityManager};
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::mpsc::batch::{self, BatchReceiver, lightlikeer as Batchlightlikeer, VecCollector};
use violetabftstore::interlock::::worker::*;

use crate::pushdown_causet::{Downstream, DownstreamID};
use crate::lightlikepoint::{Deregister, Task};

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

const causet_context_MSG_NOTIFY_COUNT: usize = 8;
const causet_context_MAX_RESP_SIZE: u32 = 6 * 1024 * 1024; // 6MB
const causet_context_MSG_MAX_BATCH_SIZE: usize = 128;

/// A unique identifier of a Connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnID(usize);

impl ConnID {
    pub fn new() -> ConnID {
        ConnID(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

#[derive(Clone, Debug)]
pub enum causet_contextEvent {
    ResolvedTs(ResolvedTs),
    Event(Event),
}

impl causet_contextEvent {
    pub fn size(&self) -> u32 {
        match self {
            causet_contextEvent::ResolvedTs(_) => 0,
            causet_contextEvent::Event(ref e) => e.compute_size(),
        }
    }

    pub fn event(&self) -> &Event {
        match self {
            causet_contextEvent::ResolvedTs(_) => unreachable!(),
            causet_contextEvent::Event(ref e) => e,
        }
    }

    pub fn resolved_ts(&self) -> &ResolvedTs {
        match self {
            causet_contextEvent::ResolvedTs(ref r) => r,
            causet_contextEvent::Event(_) => unreachable!(),
        }
    }
}

struct EventBatcher {
    events: Vec<ChangeDataEvent>,
    last_size: u32,
}

impl EventBatcher {
    pub fn with_capacity(cap: usize) -> EventBatcher {
        EventBatcher {
            events: Vec::with_capacity(cap),
            last_size: 0,
        }
    }

    // The size of the response should not exceed causet_context_MAX_RESP_SIZE.
    // Split the events into multiple responses by causet_context_MAX_RESP_SIZE here.
    pub fn push(&mut self, event: causet_contextEvent) {
        let size = event.size();
        if self.events.is_empty() || self.last_size + size >= causet_context_MAX_RESP_SIZE {
            self.last_size = 0;
            self.events.push(ChangeDataEvent::default());
        }
        match event {
            causet_contextEvent::Event(e) => {
                self.last_size += size;
                self.events.last_mut().unwrap().mut_events().push(e);
            }
            causet_contextEvent::ResolvedTs(r) => {
                let mut change_data_event = ChangeDataEvent::default();
                change_data_event.set_resolved_ts(r);
                self.events.push(change_data_event);
                // Set last_size to MAX-1 for lightlikeing resolved ts as an individual event.
                // '-1' is to avoid empty event when the next event is still resolved ts.
                self.last_size = causet_context_MAX_RESP_SIZE - 1;
            }
        }
    }

    pub fn build(self) -> Vec<ChangeDataEvent> {
        self.events
            .into_iter()
            .filter(|e| e.has_resolved_ts() || !e.events.is_empty())
            .collect()
    }
}

bitflags::bitflags! {
    pub struct FeatureGate: u8 {
        const BATCH_RESOLVED_TS = 0b00000001;
        // Uncomment when its ready.
        // const LargeTxn       = 0b00000010;
    }
}

pub struct Conn {
    id: ConnID,
    sink: Batchlightlikeer<causet_contextEvent>,
    downstreams: HashMap<u64, DownstreamID>,
    peer: String,
    version: Option<(semver::Version, FeatureGate)>,
}

impl Conn {
    pub fn new(sink: Batchlightlikeer<causet_contextEvent>, peer: String) -> Conn {
        Conn {
            id: ConnID::new(),
            sink,
            downstreams: HashMap::default(),
            version: None,
            peer,
        }
    }

    // TODO refactor into Error::Version.
    pub fn check_version_and_set_feature(&mut self, ver: semver::Version) -> Option<Compatibility> {
        // Assume batch resolved ts will be release in v4.0.7
        // For easy of testing (nightly CI), we lower the gate to v4.0.6
        // TODO bump the version when cherry pick to release branch.
        let v407_bacth_resoled_ts = semver::Version::new(4, 0, 6);

        match &self.version {
            Some((version, _)) => {
                if version == &ver {
                    None
                } else {
                    error!("different version on the same connection";
                        "previous version" => ?version, "version" => ?ver,
                        "downstream" => ?self.peer, "conn_id" => ?self.id);
                    let mut compat = Compatibility::default();
                    compat.required_version = version.to_string();
                    Some(compat)
                }
            }
            None => {
                let mut features = FeatureGate::empty();
                if v407_bacth_resoled_ts <= ver {
                    features.toggle(FeatureGate::BATCH_RESOLVED_TS);
                }
                info!("causet_context connection version"; "version" => ver.to_string(), "features" => ?features);
                self.version = Some((ver, features));
                None
            }
        }
        // Return Err(Compatibility) when EinsteinDB reaches the next major release,
        // so that we can remove feature gates.
    }

    pub fn get_feature(&self) -> Option<&FeatureGate> {
        self.version.as_ref().map(|(_, f)| f)
    }

    pub fn get_peer(&self) -> &str {
        &self.peer
    }

    pub fn get_id(&self) -> ConnID {
        self.id
    }

    pub fn take_downstreams(self) -> HashMap<u64, DownstreamID> {
        self.downstreams
    }

    pub fn get_sink(&self) -> Batchlightlikeer<causet_contextEvent> {
        self.sink.clone()
    }

    pub fn subscribe(&mut self, brane_id: u64, downstream_id: DownstreamID) -> bool {
        match self.downstreams.entry(brane_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(downstream_id);
                true
            }
        }
    }

    pub fn unsubscribe(&mut self, brane_id: u64) {
        self.downstreams.remove(&brane_id);
    }

    pub fn downstream_id(&self, brane_id: u64) -> Option<DownstreamID> {
        self.downstreams.get(&brane_id).copied()
    }

    pub fn flush(&self) {
        if !self.sink.is_empty() {
            if let Some(notifier) = self.sink.get_notifier() {
                notifier.notify();
            }
        }
    }
}

/// Service implements the `ChangeData` service.
///
/// It's a front-lightlike of the causet_context service, schedules requests to the `node`.
#[derive(Clone)]
pub struct Service {
    interlock_semaphore: Interlock_Semaphore<Task>,
    security_mgr: Arc<SecurityManager>,
}

impl Service {
    /// Create a ChangeData service.
    ///
    /// It requires a interlock_semaphore of an `node` in order to schedule tasks.
    pub fn new(interlock_semaphore: Interlock_Semaphore<Task>, security_mgr: Arc<SecurityManager>) -> Service {
        Service {
            interlock_semaphore,
            security_mgr,
        }
    }
}

impl ChangeData for Service {
    fn event_feed(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<ChangeDataRequest>,
        mut sink: DuplexSink<ChangeDataEvent>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        // TODO: make it a bounded channel.
        let (tx, rx) = batch::unbounded(causet_context_MSG_NOTIFY_COUNT);
        let peer = ctx.peer();
        let conn = Conn::new(tx, peer);
        let conn_id = conn.get_id();

        if let Err(status) = self
            .interlock_semaphore
            .schedule(Task::OpenConn { conn })
            .map_err(|e| RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT, Some(format!("{:?}", e))))
        {
            error!("causet_context connection initiate failed"; "error" => ?status);
            ctx.spawn(
                sink.fail(status)
                    .unwrap_or_else(|e| error!("causet_context failed to lightlike error"; "error" => ?e)),
            );
            return;
        }

        let peer = ctx.peer();
        let interlock_semaphore = self.interlock_semaphore.clone();
        let recv_req = stream.try_for_each(move |request| {
            let brane_epoch = request.get_brane_epoch().clone();
            let req_id = request.get_request_id();
            let version = match semver::Version::parse(request.get_header().get_ticauset_context_version()) {
                Ok(v) => v,
                Err(e) => {
                    warn!("empty or invalid Ticauset_context version, please upgrading Ticauset_context";
                        "version" => request.get_header().get_ticauset_context_version(),
                        "error" => ?e);
                    semver::Version::new(0, 0, 0)
                }
            };
            let downstream = Downstream::new(peer.clone(), brane_epoch, req_id, conn_id);
            let ret = interlock_semaphore
                .schedule(Task::Register {
                    request,
                    downstream,
                    conn_id,
                    version,
                })
                .map_err(|e| {
                    GrpcError::RpcFailure(RpcStatus::new(
                        RpcStatusCode::INVALID_ARGUMENT,
                        Some(format!("{:?}", e)),
                    ))
                });
            future::ready(ret)
        });

        let rx = BatchReceiver::new(rx, causet_context_MSG_MAX_BATCH_SIZE, Vec::new, VecCollector);
        let mut rx = rx
            .map(|events| {
                let mut batcher = EventBatcher::with_capacity(events.len());
                events.into_iter().for_each(|e| batcher.push(e));
                let resps = batcher
                    .build()
                    .into_iter()
                    .map(|e| (e, WriteFlags::default()));
                stream::iter(resps)
            })
            .flatten()
            .map(|item| GrpcResult::Ok(item));

        let peer = ctx.peer();
        let interlock_semaphore = self.interlock_semaphore.clone();
        ctx.spawn(async move {
            let res = recv_req.await;
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = interlock_semaphore.schedule(Task::Deregister(deregister)) {
                error!("causet_context deregister failed"; "error" => ?e, "conn_id" => ?conn_id);
            }
            match res {
                Ok(()) => {
                    info!("causet_context lightlike half closed"; "downstream" => peer, "conn_id" => ?conn_id);
                }
                Err(e) => {
                    warn!("causet_context lightlike failed"; "error" => ?e, "downstream" => peer, "conn_id" => ?conn_id);
                }
            }
        });

        let peer = ctx.peer();
        let interlock_semaphore = self.interlock_semaphore.clone();

        ctx.spawn(async move {
            let res = sink.lightlike_all(&mut rx).await;
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = interlock_semaphore.schedule(Task::Deregister(deregister)) {
                error!("causet_context deregister failed"; "error" => ?e);
            }
            match res {
                Ok(_s) => {
                    info!("causet_context lightlike half closed"; "downstream" => peer, "conn_id" => ?conn_id);
                    let _ = sink.close().await;
                }
                Err(e) => {
                    warn!("causet_context lightlike failed"; "error" => ?e, "downstream" => peer, "conn_id" => ?conn_id);
                }
            }
        });
    }
}

#[causet(test)]
mod tests {
    #[causet(feature = "prost-codec")]
    use ekvproto::causet_context_timeshare::event::{
        Entries as EventEntries, Event as Event_oneof_event, Event as EventEvent,
    };
    use ekvproto::causet_context_timeshare::{ChangeDataEvent, Event, ResolvedTs};
    #[causet(not(feature = "prost-codec"))]
    use ekvproto::causet_context_timeshare::{EventEntries, EventEvent, Event_oneof_event};

    use crate::service::{causet_contextEvent, EventBatcher, causet_context_MAX_RESP_SIZE};

    #[test]
    fn test_event_batcher() {
        let mut batcher = EventBatcher::with_capacity(1024);

        let check_events = |result: Vec<ChangeDataEvent>, expected: Vec<Vec<causet_contextEvent>>| {
            assert_eq!(result.len(), expected.len());

            for i in 0..expected.len() {
                if !result[i].has_resolved_ts() {
                    assert_eq!(result[i].events.len(), expected[i].len());
                    for j in 0..expected[i].len() {
                        assert_eq!(&result[i].events[j], expected[i][j].event());
                    }
                } else {
                    assert_eq!(expected[i].len(), 1);
                    assert_eq!(result[i].get_resolved_ts(), expected[i][0].resolved_ts());
                }
            }
        };

        let mut event_small = Event::default();
        let row_small = EventEvent::default();
        let mut event_entries = EventEntries::default();
        event_entries.entries = vec![row_small].into();
        event_small.event = Some(Event_oneof_event::Entries(event_entries));

        let mut event_big = Event::default();
        let mut row_big = EventEvent::default();
        row_big.set_key(vec![0 as u8; causet_context_MAX_RESP_SIZE as usize]);
        let mut event_entries = EventEntries::default();
        event_entries.entries = vec![row_big].into();
        event_big.event = Some(Event_oneof_event::Entries(event_entries));

        batcher.push(causet_contextEvent::Event(event_small.clone()));
        batcher.push(causet_contextEvent::ResolvedTs(ResolvedTs::default()));
        batcher.push(causet_contextEvent::ResolvedTs(ResolvedTs::default()));
        batcher.push(causet_contextEvent::Event(event_big.clone()));
        batcher.push(causet_contextEvent::Event(event_small.clone()));
        batcher.push(causet_contextEvent::Event(event_small.clone()));
        batcher.push(causet_contextEvent::Event(event_big.clone()));

        check_events(
            batcher.build(),
            vec![
                vec![causet_contextEvent::Event(event_small.clone())],
                vec![causet_contextEvent::ResolvedTs(ResolvedTs::default())],
                vec![causet_contextEvent::ResolvedTs(ResolvedTs::default())],
                vec![causet_contextEvent::Event(event_big.clone())],
                vec![causet_contextEvent::Event(event_small); 2],
                vec![causet_contextEvent::Event(event_big)],
            ],
        );
    }
}
