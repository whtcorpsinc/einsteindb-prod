// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
#[causetg(feature = "prost-codec")]
use ekvproto::cdcpb::{
    event::{
        row::OpType as EventRowOpType, Entries as EventEntries, Event as Event_oneof_event,
        LogType as EventLogType, Row as EventRow,
    },
    Compatibility, DuplicateRequest as ErrorDuplicateRequest, Error as EventError, Event,
};
#[causetg(not(feature = "prost-codec"))]
use ekvproto::cdcpb::{
    Compatibility, DuplicateRequest as ErrorDuplicateRequest, Error as EventError, Event,
    EventEntries, EventLogType, EventRow, EventRowOpType, Event_oneof_event,
};
use ekvproto::errorpb;
use ekvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use ekvproto::metapb::{Brane, BraneEpoch};
use ekvproto::violetabft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CmdType, Request};
use violetabftstore::interlock::{Cmd, CmdBatch};
use violetabftstore::store::fsm::ObserveID;
use violetabftstore::store::util::compare_brane_epoch;
use violetabftstore::Error as VioletaBftStoreError;
use resolved_ts::Resolver;
use einsteindb::causetStorage::txn::TxnEntry;
use einsteindb_util::collections::HashMap;
use einsteindb_util::mpsc::batch::Slightlikeer as BatchSlightlikeer;
use txn_types::{Key, Dagger, LockType, TimeStamp, WriteRef, WriteType};

use crate::lightlikepoint::{OldValueCache, OldValueCallback};
use crate::metrics::*;
use crate::service::{CdcEvent, ConnID};
use crate::{Error, Result};

const EVENT_MAX_SIZE: usize = 6 * 1024 * 1024; // 6MB
static DOWNSTREAM_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier of a Downstream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DownstreamID(usize);

impl DownstreamID {
    pub fn new() -> DownstreamID {
        DownstreamID(DOWNSTREAM_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DownstreamState {
    Uninitialized,
    Normal,
    Stopped,
}

impl Default for DownstreamState {
    fn default() -> Self {
        Self::Uninitialized
    }
}

#[derive(Clone)]
pub struct Downstream {
    // TODO: include cdc request.
    /// A unique identifier of the Downstream.
    id: DownstreamID,
    // The reqeust ID set by CDC to identify events corresponding different requests.
    req_id: u64,
    conn_id: ConnID,
    // The IP address of downstream.
    peer: String,
    brane_epoch: BraneEpoch,
    sink: Option<BatchSlightlikeer<CdcEvent>>,
    state: Arc<AtomicCell<DownstreamState>>,
}

impl Downstream {
    /// Create a Downsteam.
    ///
    /// peer is the address of the downstream.
    /// sink slightlikes data to the downstream.
    pub fn new(
        peer: String,
        brane_epoch: BraneEpoch,
        req_id: u64,
        conn_id: ConnID,
    ) -> Downstream {
        Downstream {
            id: DownstreamID::new(),
            req_id,
            conn_id,
            peer,
            brane_epoch,
            sink: None,
            state: Arc::new(AtomicCell::new(DownstreamState::default())),
        }
    }

    /// Sink events to the downstream.
    /// The size of `Error` and `ResolvedTS` are considered zero.
    pub fn sink_event(&self, mut event: Event) {
        event.set_request_id(self.req_id);
        if self.sink.is_none() {
            info!("drop event, no sink";
                "conn_id" => ?self.conn_id, "downstream_id" => ?self.id);
            return;
        }
        let sink = self.sink.as_ref().unwrap();
        if let Err(e) = sink.try_slightlike(CdcEvent::Event(event)) {
            match e {
                crossbeam::TrySlightlikeError::Disconnected(_) => {
                    debug!("slightlike event failed, disconnected";
                        "conn_id" => ?self.conn_id, "downstream_id" => ?self.id);
                }
                crossbeam::TrySlightlikeError::Full(_) => {
                    info!("slightlike event failed, full";
                        "conn_id" => ?self.conn_id, "downstream_id" => ?self.id);
                }
            }
        }
    }

    pub fn set_sink(&mut self, sink: BatchSlightlikeer<CdcEvent>) {
        self.sink = Some(sink);
    }

    pub fn get_id(&self) -> DownstreamID {
        self.id
    }

    pub fn get_state(&self) -> Arc<AtomicCell<DownstreamState>> {
        self.state.clone()
    }

    pub fn get_conn_id(&self) -> ConnID {
        self.conn_id
    }

    pub fn sink_duplicate_error(&self, brane_id: u64) {
        let mut change_data_event = Event::default();
        let mut cdc_err = EventError::default();
        let mut err = ErrorDuplicateRequest::default();
        err.set_brane_id(brane_id);
        cdc_err.set_duplicate_request(err);
        change_data_event.event = Some(Event_oneof_event::Error(cdc_err));
        change_data_event.brane_id = brane_id;
        self.sink_event(change_data_event);
    }

    // TODO: merge it into Delegate::error_event.
    pub fn sink_compatibility_error(&self, brane_id: u64, compat: Compatibility) {
        let mut change_data_event = Event::default();
        let mut cdc_err = EventError::default();
        cdc_err.set_compatibility(compat);
        change_data_event.event = Some(Event_oneof_event::Error(cdc_err));
        change_data_event.brane_id = brane_id;
        self.sink_event(change_data_event);
    }
}

#[derive(Default)]
struct Plightlikeing {
    pub downstreams: Vec<Downstream>,
    pub locks: Vec<PlightlikeingLock>,
    pub plightlikeing_bytes: usize,
}

impl Drop for Plightlikeing {
    fn drop(&mut self) {
        CDC_PENDING_BYTES_GAUGE.sub(self.plightlikeing_bytes as i64);
    }
}

impl Plightlikeing {
    fn take_downstreams(&mut self) -> Vec<Downstream> {
        mem::take(&mut self.downstreams)
    }

    fn take_locks(&mut self) -> Vec<PlightlikeingLock> {
        mem::take(&mut self.locks)
    }
}

enum PlightlikeingLock {
    Track {
        key: Vec<u8>,
        spacelike_ts: TimeStamp,
    },
    Untrack {
        key: Vec<u8>,
        spacelike_ts: TimeStamp,
        commit_ts: Option<TimeStamp>,
    },
}

/// A CDC delegate of a violetabftstore brane peer.
///
/// It converts violetabft commands into CDC events and broadcast to downstreams.
/// It also track trancation on the fly in order to compute resolved ts.
pub struct Delegate {
    pub id: ObserveID,
    pub brane_id: u64,
    brane: Option<Brane>,
    pub downstreams: Vec<Downstream>,
    pub resolver: Option<Resolver>,
    plightlikeing: Option<Plightlikeing>,
    enabled: Arc<AtomicBool>,
    failed: bool,
    pub txn_extra_op: TxnExtraOp,
}

impl Delegate {
    /// Create a Delegate the given brane.
    pub fn new(brane_id: u64) -> Delegate {
        Delegate {
            brane_id,
            id: ObserveID::new(),
            downstreams: Vec::new(),
            resolver: None,
            brane: None,
            plightlikeing: Some(Plightlikeing::default()),
            enabled: Arc::new(AtomicBool::new(true)),
            failed: false,
            txn_extra_op: TxnExtraOp::default(),
        }
    }

    /// Returns a shared flag.
    /// True if there are some active downstreams subscribe the brane.
    /// False if all downstreams has unsubscribed.
    pub fn enabled(&self) -> Arc<AtomicBool> {
        self.enabled.clone()
    }

    /// Return false if subscribe failed.
    pub fn subscribe(&mut self, downstream: Downstream) -> bool {
        if let Some(brane) = self.brane.as_ref() {
            if let Err(e) = compare_brane_epoch(
                &downstream.brane_epoch,
                brane,
                false, /* check_conf_ver */
                true,  /* check_ver */
                true,  /* include_brane */
            ) {
                info!("fail to subscribe downstream";
                    "brane_id" => brane.get_id(),
                    "downstream_id" => ?downstream.get_id(),
                    "conn_id" => ?downstream.get_conn_id(),
                    "req_id" => downstream.req_id,
                    "err" => ?e);
                let err = Error::Request(e.into());
                let change_data_error = self.error_event(err);
                downstream.sink_event(change_data_error);
                return false;
            }
            self.downstreams.push(downstream);
        } else {
            self.plightlikeing.as_mut().unwrap().downstreams.push(downstream);
        }
        true
    }

    pub fn downstream(&self, downstream_id: DownstreamID) -> Option<&Downstream> {
        self.downstreams.iter().find(|d| d.id == downstream_id)
    }

    pub fn downstreams(&self) -> &Vec<Downstream> {
        if self.plightlikeing.is_some() {
            &self.plightlikeing.as_ref().unwrap().downstreams
        } else {
            &self.downstreams
        }
    }

    pub fn downstreams_mut(&mut self) -> &mut Vec<Downstream> {
        if self.plightlikeing.is_some() {
            &mut self.plightlikeing.as_mut().unwrap().downstreams
        } else {
            &mut self.downstreams
        }
    }

    pub fn unsubscribe(&mut self, id: DownstreamID, err: Option<Error>) -> bool {
        let change_data_error = err.map(|err| self.error_event(err));
        let downstreams = self.downstreams_mut();
        downstreams.retain(|d| {
            if d.id == id {
                if let Some(change_data_error) = change_data_error.clone() {
                    d.sink_event(change_data_error);
                }
                d.state.store(DownstreamState::Stopped);
            }
            d.id != id
        });
        let is_last = downstreams.is_empty();
        if is_last {
            self.enabled.store(false, Ordering::SeqCst);
        }
        is_last
    }

    fn error_event(&self, err: Error) -> Event {
        let mut change_data_event = Event::default();
        let mut cdc_err = EventError::default();
        let mut err = err.extract_error_header();
        if err.has_not_leader() {
            let not_leader = err.take_not_leader();
            cdc_err.set_not_leader(not_leader);
        } else if err.has_epoch_not_match() {
            let epoch_not_match = err.take_epoch_not_match();
            cdc_err.set_epoch_not_match(epoch_not_match);
        } else {
            // TODO: Add more errors to the cdc protocol
            let mut brane_not_found = errorpb::BraneNotFound::default();
            brane_not_found.set_brane_id(self.brane_id);
            cdc_err.set_brane_not_found(brane_not_found);
        }
        change_data_event.event = Some(Event_oneof_event::Error(cdc_err));
        change_data_event.brane_id = self.brane_id;
        change_data_event
    }

    pub fn mark_failed(&mut self) {
        self.failed = true;
    }

    pub fn has_failed(&self) -> bool {
        self.failed
    }

    /// Stop the delegate
    ///
    /// This means the brane has met an unrecoverable error for CDC.
    /// It broadcasts errors to all downstream and stops.
    pub fn stop(&mut self, err: Error) {
        self.mark_failed();
        // Stop observe further events.
        self.enabled.store(false, Ordering::SeqCst);

        info!("brane met error";
            "brane_id" => self.brane_id, "error" => ?err);
        let change_data_err = self.error_event(err);
        for d in &self.downstreams {
            d.state.store(DownstreamState::Stopped);
        }
        self.broadcast(change_data_err, false);
    }

    fn broadcast(&self, change_data_event: Event, normal_only: bool) {
        let downstreams = self.downstreams();
        assert!(
            !downstreams.is_empty(),
            "brane {} miss downstream, event: {:?}",
            self.brane_id,
            change_data_event,
        );
        for i in 0..downstreams.len() - 1 {
            if normal_only && downstreams[i].state.load() != DownstreamState::Normal {
                continue;
            }
            downstreams[i].sink_event(change_data_event.clone());
        }
        downstreams.last().unwrap().sink_event(change_data_event);
    }

    /// Install a resolver and return plightlikeing downstreams.
    pub fn on_brane_ready(&mut self, mut resolver: Resolver, brane: Brane) -> Vec<Downstream> {
        assert!(
            self.resolver.is_none(),
            "brane {} resolver should not be ready",
            self.brane_id,
        );
        // Mark the delegate as initialized.
        self.brane = Some(brane);
        let mut plightlikeing = self.plightlikeing.take().unwrap();
        for dagger in plightlikeing.take_locks() {
            match dagger {
                PlightlikeingLock::Track { key, spacelike_ts } => resolver.track_lock(spacelike_ts, key),
                PlightlikeingLock::Untrack {
                    key,
                    spacelike_ts,
                    commit_ts,
                } => resolver.untrack_lock(spacelike_ts, commit_ts, key),
            }
        }
        self.resolver = Some(resolver);
        info!("brane is ready"; "brane_id" => self.brane_id);
        plightlikeing.take_downstreams()
    }

    /// Try advance and broadcast resolved ts.
    pub fn on_min_ts(&mut self, min_ts: TimeStamp) -> Option<TimeStamp> {
        if self.resolver.is_none() {
            debug!("brane resolver not ready";
                "brane_id" => self.brane_id, "min_ts" => min_ts);
            return None;
        }
        debug!("try to advance ts"; "brane_id" => self.brane_id, "min_ts" => min_ts);
        let resolver = self.resolver.as_mut().unwrap();
        let resolved_ts = match resolver.resolve(min_ts) {
            Some(rts) => rts,
            None => return None,
        };
        debug!("resolved ts ufidelated";
            "brane_id" => self.brane_id, "resolved_ts" => resolved_ts);
        CDC_RESOLVED_TS_GAP_HISTOGRAM
            .observe((min_ts.physical() - resolved_ts.physical()) as f64 / 1000f64);
        Some(resolved_ts)
    }

    pub fn on_batch(
        &mut self,
        batch: CmdBatch,
        old_value_cb: Rc<RefCell<OldValueCallback>>,
        old_value_cache: &mut OldValueCache,
    ) -> Result<()> {
        // Stale CmdBatch, drop it sliently.
        if batch.observe_id != self.id {
            return Ok(());
        }
        for cmd in batch.into_iter(self.brane_id) {
            let Cmd {
                index,
                mut request,
                mut response,
            } = cmd;
            if !response.get_header().has_error() {
                if !request.has_admin_request() {
                    self.sink_data(
                        index,
                        request.requests.into(),
                        old_value_cb.clone(),
                        old_value_cache,
                    )?;
                } else {
                    self.sink_admin(request.take_admin_request(), response.take_admin_response())?;
                }
            } else {
                let err_header = response.mut_header().take_error();
                self.mark_failed();
                return Err(Error::Request(err_header));
            }
        }
        Ok(())
    }

    pub fn on_scan(&mut self, downstream_id: DownstreamID, entries: Vec<Option<TxnEntry>>) {
        let downstreams = if let Some(plightlikeing) = self.plightlikeing.as_mut() {
            &plightlikeing.downstreams
        } else {
            &self.downstreams
        };
        let downstream = if let Some(d) = downstreams.iter().find(|d| d.id == downstream_id) {
            d
        } else {
            warn!("downstream not found"; "downstream_id" => ?downstream_id, "brane_id" => self.brane_id);
            return;
        };

        let entries_len = entries.len();
        let mut events = vec![Vec::with_capacity(entries_len)];
        let mut current_rows_size: usize = 0;
        for entry in entries {
            match entry {
                Some(TxnEntry::Prewrite {
                    default,
                    dagger,
                    old_value,
                }) => {
                    let mut row = EventRow::default();
                    let skip = decode_lock(dagger.0, &dagger.1, &mut row);
                    if skip {
                        continue;
                    }
                    decode_default(default.1, &mut row);
                    let row_size = row.key.len() + row.value.len();
                    if current_rows_size + row_size >= EVENT_MAX_SIZE {
                        events.push(Vec::with_capacity(entries_len));
                        current_rows_size = 0;
                    }
                    current_rows_size += row_size;
                    row.old_value = old_value.unwrap_or_default();
                    events.last_mut().unwrap().push(row);
                }
                Some(TxnEntry::Commit {
                    default,
                    write,
                    old_value,
                }) => {
                    let mut row = EventRow::default();
                    let skip = decode_write(write.0, &write.1, &mut row);
                    if skip {
                        continue;
                    }
                    decode_default(default.1, &mut row);

                    // This type means the row is self-contained, it has,
                    //   1. spacelike_ts
                    //   2. commit_ts
                    //   3. key
                    //   4. value
                    if row.get_type() == EventLogType::Rollback {
                        // We dont need to slightlike rollbacks to downstream,
                        // because downstream does not needs rollback to clean
                        // prewrite as it drops all previous stashed data.
                        continue;
                    }
                    set_event_row_type(&mut row, EventLogType::Committed);
                    row.old_value = old_value.unwrap_or_default();
                    let row_size = row.key.len() + row.value.len();
                    if current_rows_size + row_size >= EVENT_MAX_SIZE {
                        events.push(Vec::with_capacity(entries_len));
                        current_rows_size = 0;
                    }
                    current_rows_size += row_size;
                    events.last_mut().unwrap().push(row);
                }
                None => {
                    let mut row = EventRow::default();

                    // This type means scan has finised.
                    set_event_row_type(&mut row, EventLogType::Initialized);
                    events.last_mut().unwrap().push(row);
                }
            }
        }

        for rs in events {
            if !rs.is_empty() {
                let mut event_entries = EventEntries::default();
                event_entries.entries = rs.into();
                let mut event = Event::default();
                event.brane_id = self.brane_id;
                event.event = Some(Event_oneof_event::Entries(event_entries));
                downstream.sink_event(event);
            }
        }
    }

    fn sink_data(
        &mut self,
        index: u64,
        requests: Vec<Request>,
        old_value_cb: Rc<RefCell<OldValueCallback>>,
        old_value_cache: &mut OldValueCache,
    ) -> Result<()> {
        let mut events = HashMap::default();
        for mut req in requests {
            // CDC cares about put requests only.
            if req.get_cmd_type() != CmdType::Put {
                // Do not log delete requests because they are issued by GC
                // frequently.
                if req.get_cmd_type() != CmdType::Delete {
                    debug!(
                        "skip other command";
                        "brane_id" => self.brane_id,
                        "command" => ?req,
                    );
                }
                continue;
            }
            let mut put = req.take_put();
            match put.causet.as_str() {
                "write" => {
                    let mut row = EventRow::default();
                    let skip = decode_write(put.take_key(), put.get_value(), &mut row);
                    if skip {
                        continue;
                    }

                    // In order to advance resolved ts,
                    // we must untrack inflight txns if they are committed.
                    let commit_ts = if row.commit_ts == 0 {
                        None
                    } else {
                        Some(row.commit_ts)
                    };
                    match self.resolver {
                        Some(ref mut resolver) => resolver.untrack_lock(
                            row.spacelike_ts.into(),
                            commit_ts.map(Into::into),
                            row.key.clone(),
                        ),
                        None => {
                            assert!(self.plightlikeing.is_some(), "brane resolver not ready");
                            let plightlikeing = self.plightlikeing.as_mut().unwrap();
                            plightlikeing.locks.push(PlightlikeingLock::Untrack {
                                key: row.key.clone(),
                                spacelike_ts: row.spacelike_ts.into(),
                                commit_ts: commit_ts.map(Into::into),
                            });
                            plightlikeing.plightlikeing_bytes += row.key.len();
                            CDC_PENDING_BYTES_GAUGE.add(row.key.len() as i64);
                        }
                    }

                    let r = events.insert(row.key.clone(), row);
                    assert!(r.is_none());
                }
                "dagger" => {
                    let mut row = EventRow::default();
                    let skip = decode_lock(put.take_key(), put.get_value(), &mut row);
                    if skip {
                        continue;
                    }

                    if self.txn_extra_op == TxnExtraOp::ReadOldValue {
                        let key = Key::from_raw(&row.key).applightlike_ts(row.spacelike_ts.into());
                        row.old_value =
                            old_value_cb.borrow_mut()(key, old_value_cache).unwrap_or_default();
                    }

                    let occupied = events.entry(row.key.clone()).or_default();
                    if !occupied.value.is_empty() {
                        assert!(row.value.is_empty());
                        let mut value = vec![];
                        mem::swap(&mut occupied.value, &mut value);
                        row.value = value;
                    }

                    // In order to compute resolved ts,
                    // we must track inflight txns.
                    match self.resolver {
                        Some(ref mut resolver) => {
                            resolver.track_lock(row.spacelike_ts.into(), row.key.clone())
                        }
                        None => {
                            assert!(self.plightlikeing.is_some(), "brane resolver not ready");
                            let plightlikeing = self.plightlikeing.as_mut().unwrap();
                            plightlikeing.locks.push(PlightlikeingLock::Track {
                                key: row.key.clone(),
                                spacelike_ts: row.spacelike_ts.into(),
                            });
                            plightlikeing.plightlikeing_bytes += row.key.len();
                            CDC_PENDING_BYTES_GAUGE.add(row.key.len() as i64);
                        }
                    }

                    *occupied = row;
                }
                "" | "default" => {
                    let key = Key::from_encoded(put.take_key()).truncate_ts().unwrap();
                    let row = events.entry(key.into_raw().unwrap()).or_default();
                    decode_default(put.take_value(), row);
                }
                other => {
                    panic!("invalid causet {}", other);
                }
            }
        }
        let mut entries = Vec::with_capacity(events.len());
        for (_, v) in events {
            entries.push(v);
        }
        let mut event_entries = EventEntries::default();
        event_entries.entries = entries.into();
        let mut change_data_event = Event::default();
        change_data_event.brane_id = self.brane_id;
        change_data_event.index = index;
        change_data_event.event = Some(Event_oneof_event::Entries(event_entries));
        self.broadcast(change_data_event, true);
        Ok(())
    }

    fn sink_admin(&mut self, request: AdminRequest, mut response: AdminResponse) -> Result<()> {
        let store_err = match request.get_cmd_type() {
            AdminCmdType::Split => VioletaBftStoreError::EpochNotMatch(
                "split".to_owned(),
                vec![
                    response.mut_split().take_left(),
                    response.mut_split().take_right(),
                ],
            ),
            AdminCmdType::BatchSplit => VioletaBftStoreError::EpochNotMatch(
                "batchsplit".to_owned(),
                response.mut_splits().take_branes().into(),
            ),
            AdminCmdType::PrepareMerge
            | AdminCmdType::CommitMerge
            | AdminCmdType::RollbackMerge => {
                VioletaBftStoreError::EpochNotMatch("merge".to_owned(), vec![])
            }
            _ => return Ok(()),
        };
        self.mark_failed();
        Err(Error::Request(store_err.into()))
    }
}

fn set_event_row_type(row: &mut EventRow, ty: EventLogType) {
    #[causetg(feature = "prost-codec")]
    {
        row.r#type = ty.into();
    }
    #[causetg(not(feature = "prost-codec"))]
    {
        row.r_type = ty;
    }
}

fn decode_write(key: Vec<u8>, value: &[u8], row: &mut EventRow) -> bool {
    let write = WriteRef::parse(value).unwrap().to_owned();
    let (op_type, r_type) = match write.write_type {
        WriteType::Put => (EventRowOpType::Put, EventLogType::Commit),
        WriteType::Delete => (EventRowOpType::Delete, EventLogType::Commit),
        WriteType::Rollback => (EventRowOpType::Unknown, EventLogType::Rollback),
        other => {
            debug!("skip write record"; "write" => ?other, "key" => hex::encode_upper(key));
            return true;
        }
    };
    let key = Key::from_encoded(key);
    let commit_ts = if write.write_type == WriteType::Rollback {
        0
    } else {
        key.decode_ts().unwrap().into_inner()
    };
    row.spacelike_ts = write.spacelike_ts.into_inner();
    row.commit_ts = commit_ts;
    row.key = key.truncate_ts().unwrap().into_raw().unwrap();
    row.op_type = op_type.into();
    set_event_row_type(row, r_type);
    if let Some(value) = write.short_value {
        row.value = value;
    }

    false
}

fn decode_lock(key: Vec<u8>, value: &[u8], row: &mut EventRow) -> bool {
    let dagger = Dagger::parse(value).unwrap();
    let op_type = match dagger.lock_type {
        LockType::Put => EventRowOpType::Put,
        LockType::Delete => EventRowOpType::Delete,
        other => {
            debug!("skip dagger record";
                "type" => ?other,
                "spacelike_ts" => ?dagger.ts,
                "key" => hex::encode_upper(key),
                "for_ufidelate_ts" => ?dagger.for_ufidelate_ts);
            return true;
        }
    };
    let key = Key::from_encoded(key);
    row.spacelike_ts = dagger.ts.into_inner();
    row.key = key.into_raw().unwrap();
    row.op_type = op_type.into();
    set_event_row_type(row, EventLogType::Prewrite);
    if let Some(value) = dagger.short_value {
        row.value = value;
    }

    false
}

fn decode_default(value: Vec<u8>, row: &mut EventRow) {
    if !value.is_empty() {
        row.value = value.to_vec();
    }
}

#[causetg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::stream::StreamExt;
    use ekvproto::errorpb::Error as ErrorHeader;
    use ekvproto::metapb::Brane;
    use std::cell::Cell;
    use einsteindb::causetStorage::tail_pointer::test_util::*;
    use einsteindb_util::mpsc::batch::{self, BatchReceiver, VecCollector};

    #[test]
    fn test_error() {
        let brane_id = 1;
        let mut brane = Brane::default();
        brane.set_id(brane_id);
        brane.mut_peers().push(Default::default());
        brane.mut_brane_epoch().set_version(2);
        brane.mut_brane_epoch().set_conf_ver(2);
        let brane_epoch = brane.get_brane_epoch().clone();

        let (sink, rx) = batch::unbounded(1);
        let rx = BatchReceiver::new(rx, 1, Vec::new, VecCollector);
        let request_id = 123;
        let mut downstream =
            Downstream::new(String::new(), brane_epoch, request_id, ConnID::new());
        downstream.set_sink(sink);
        let mut delegate = Delegate::new(brane_id);
        delegate.subscribe(downstream);
        let enabled = delegate.enabled();
        assert!(enabled.load(Ordering::SeqCst));
        let mut resolver = Resolver::new(brane_id);
        resolver.init();
        for downstream in delegate.on_brane_ready(resolver, brane) {
            delegate.subscribe(downstream);
        }

        let rx_wrap = Cell::new(Some(rx));
        let receive_error = || {
            let (resps, rx) = block_on(rx_wrap.replace(None).unwrap().into_future());
            rx_wrap.set(Some(rx));
            let mut resps = resps.unwrap();
            assert_eq!(resps.len(), 1);
            for r in &resps {
                if let CdcEvent::Event(e) = r {
                    assert_eq!(e.get_request_id(), request_id);
                }
            }
            let cdc_event = &mut resps[0];
            if let CdcEvent::Event(e) = cdc_event {
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => err,
                    other => panic!("unknown event {:?}", other),
                }
            } else {
                panic!("unknown event")
            }
        };

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());
        delegate.stop(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_not_leader());
        // Enable is disabled by any error.
        assert!(!enabled.load(Ordering::SeqCst));

        let mut err_header = ErrorHeader::default();
        err_header.set_brane_not_found(Default::default());
        delegate.stop(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_brane_not_found());

        let mut err_header = ErrorHeader::default();
        err_header.set_epoch_not_match(Default::default());
        delegate.stop(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_epoch_not_match());

        // Split
        let mut brane = Brane::default();
        brane.set_id(1);
        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::Split);
        let mut response = AdminResponse::default();
        response.mut_split().set_left(brane.clone());
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        err.take_epoch_not_match()
            .current_branes
            .into_iter()
            .find(|r| r.get_id() == 1)
            .unwrap();

        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::BatchSplit);
        let mut response = AdminResponse::default();
        response.mut_splits().set_branes(vec![brane].into());
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        err.take_epoch_not_match()
            .current_branes
            .into_iter()
            .find(|r| r.get_id() == 1)
            .unwrap();

        // Merge
        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::PrepareMerge);
        let response = AdminResponse::default();
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_branes.is_empty());

        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::CommitMerge);
        let response = AdminResponse::default();
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_branes.is_empty());

        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::RollbackMerge);
        let response = AdminResponse::default();
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_branes.is_empty());
    }

    #[test]
    fn test_scan() {
        let brane_id = 1;
        let mut brane = Brane::default();
        brane.set_id(brane_id);
        brane.mut_peers().push(Default::default());
        brane.mut_brane_epoch().set_version(2);
        brane.mut_brane_epoch().set_conf_ver(2);
        let brane_epoch = brane.get_brane_epoch().clone();

        let (sink, rx) = batch::unbounded(1);
        let rx = BatchReceiver::new(rx, 1, Vec::new, VecCollector);
        let request_id = 123;
        let mut downstream =
            Downstream::new(String::new(), brane_epoch, request_id, ConnID::new());
        let downstream_id = downstream.get_id();
        downstream.set_sink(sink);
        let mut delegate = Delegate::new(brane_id);
        delegate.subscribe(downstream);
        let enabled = delegate.enabled();
        assert!(enabled.load(Ordering::SeqCst));

        let rx_wrap = Cell::new(Some(rx));
        let check_event = |event_rows: Vec<EventRow>| {
            let (resps, rx) = block_on(rx_wrap.replace(None).unwrap().into_future());
            rx_wrap.set(Some(rx));
            let mut resps = resps.unwrap();
            assert_eq!(resps.len(), 1);
            for r in &resps {
                if let CdcEvent::Event(e) = r {
                    assert_eq!(e.get_request_id(), request_id);
                }
            }
            let cdc_event = resps.remove(0);
            if let CdcEvent::Event(mut e) = cdc_event {
                assert_eq!(e.brane_id, brane_id);
                assert_eq!(e.index, 0);
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Entries(entries) => {
                        assert_eq!(entries.entries.as_slice(), event_rows.as_slice());
                    }
                    other => panic!("unknown event {:?}", other),
                }
            }
        };

        // Stashed in plightlikeing before brane ready.
        let entries = vec![
            Some(
                EntryBuilder::default()
                    .key(b"a")
                    .value(b"b")
                    .spacelike_ts(1.into())
                    .commit_ts(0.into())
                    .primary(&[])
                    .for_ufidelate_ts(0.into())
                    .build_prewrite(LockType::Put, false),
            ),
            Some(
                EntryBuilder::default()
                    .key(b"a")
                    .value(b"b")
                    .spacelike_ts(1.into())
                    .commit_ts(2.into())
                    .primary(&[])
                    .for_ufidelate_ts(0.into())
                    .build_commit(WriteType::Put, false),
            ),
            Some(
                EntryBuilder::default()
                    .key(b"a")
                    .value(b"b")
                    .spacelike_ts(3.into())
                    .commit_ts(0.into())
                    .primary(&[])
                    .for_ufidelate_ts(0.into())
                    .build_rollback(),
            ),
            None,
        ];
        delegate.on_scan(downstream_id, entries);
        // Flush all plightlikeing entries.
        let mut row1 = EventRow::default();
        row1.spacelike_ts = 1;
        row1.commit_ts = 0;
        row1.key = b"a".to_vec();
        row1.op_type = EventRowOpType::Put.into();
        set_event_row_type(&mut row1, EventLogType::Prewrite);
        row1.value = b"b".to_vec();
        let mut row2 = EventRow::default();
        row2.spacelike_ts = 1;
        row2.commit_ts = 2;
        row2.key = b"a".to_vec();
        row2.op_type = EventRowOpType::Put.into();
        set_event_row_type(&mut row2, EventLogType::Committed);
        row2.value = b"b".to_vec();
        let mut row3 = EventRow::default();
        set_event_row_type(&mut row3, EventLogType::Initialized);
        check_event(vec![row1, row2, row3]);

        let mut resolver = Resolver::new(brane_id);
        resolver.init();
        delegate.on_brane_ready(resolver, brane);
    }
}
