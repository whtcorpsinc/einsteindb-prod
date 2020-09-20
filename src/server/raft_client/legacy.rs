// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;
use std::i64;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::super::load_statistics::ThreadLoad;
use super::super::metrics::*;
use super::super::{Config, Result};
use crossbeam::channel::SlightlikeError;
use engine_lmdb::LmdbEngine;
use futures::compat::Future01CompatExt;
use futures::stream::{self, Stream, StreamExt};
use futures::task::{Context, Poll};
use futures::{FutureExt, SinkExt, TryFutureExt};
use grpcio::{ChannelBuilder, Environment, Error as GrpcError, Result as GrpcResult, WriteFlags};
use ekvproto::raft_serverpb::VioletaBftMessage;
use ekvproto::einsteindbpb::{BatchVioletaBftMessage, EINSTEINDBClient};
use violetabftstore::router::VioletaBftStoreRouter;
use security::SecurityManager;
use einsteindb_util::collections::{HashMap, HashMapEntry};
use einsteindb_util::mpsc::batch::{self, BatchCollector, Slightlikeer as BatchSlightlikeer};
use einsteindb_util::timer::GLOBAL_TIMER_HANDLE;
use tokio_timer::timer::Handle;

// When merge violetabft messages into a batch message, leave a buffer.
const GRPC_SEND_MSG_BUF: usize = 64 * 1024;

const RAFT_MSG_MAX_BATCH_SIZE: usize = 128;
const RAFT_MSG_NOTIFY_SIZE: usize = 8;

static CONN_ID: AtomicI32 = AtomicI32::new(0);

struct Conn {
    stream: BatchSlightlikeer<VioletaBftMessage>,
    _client: EINSTEINDBClient,
}

impl Conn {
    fn new<T: VioletaBftStoreRouter<LmdbEngine> + 'static>(
        env: Arc<Environment>,
        router: T,
        addr: &str,
        causetg: &Config,
        security_mgr: &SecurityManager,
        store_id: u64,
    ) -> Conn {
        info!("server: new connection with einsteindb lightlikepoint"; "addr" => addr);

        let cb = ChannelBuilder::new(env)
            .stream_initial_window_size(causetg.grpc_stream_initial_window_size.0 as i32)
            .max_slightlike_message_len(causetg.max_grpc_slightlike_msg_len)
            .keepalive_time(causetg.grpc_keepalive_time.0)
            .keepalive_timeout(causetg.grpc_keepalive_timeout.0)
            .default_compression_algorithm(causetg.grpc_compression_algorithm())
            // hack: so it's different args, grpc will always create a new connection.
            .raw_causetg_int(
                CString::new("random id").unwrap(),
                CONN_ID.fetch_add(1, Ordering::SeqCst),
            );
        let channel = security_mgr.connect(cb, addr);
        let client1 = EINSTEINDBClient::new(channel);
        let client2 = client1.clone();

        let (tx, rx) = batch::unbounded::<VioletaBftMessage>(RAFT_MSG_NOTIFY_SIZE);
        let rx = batch::BatchReceiver::new(
            rx,
            RAFT_MSG_MAX_BATCH_SIZE,
            Vec::new,
            VioletaBftMsgCollector::new(causetg.max_grpc_slightlike_msg_len as usize),
        );

        // Use a mutex to make compiler happy.
        let rx1 = Arc::new(Mutex::new(rx));
        let rx2 = Arc::clone(&rx1);
        let (addr1, addr2) = (addr.to_owned(), addr.to_owned());

        let (mut batch_sink, batch_receiver) = client1.batch_raft().unwrap();

        let batch_slightlike_or_fallback = async move {
            let mut s = Reusable(rx1).map(move |v| {
                let mut batch_msgs = BatchVioletaBftMessage::default();
                batch_msgs.set_msgs(v.into());
                GrpcResult::Ok((batch_msgs, WriteFlags::default().buffer_hint(false)))
            });
            let slightlike_res = async move {
                batch_sink.slightlike_all(&mut s).await?;
                batch_sink.close().await?;
                Ok(())
            }
            .await;
            let recv_res = batch_receiver.await;
            let res = check_rpc_result("batch_raft", &addr1, slightlike_res.err(), recv_res.err());
            if res.is_ok() {
                return Ok(());
            }
            // Don't need to fallback
            if !res.unwrap_err() {
                return Err(false);
            }
            // Fallback to violetabft RPC.
            warn!("batch_raft is unimplemented, fallback to violetabft");
            let (mut sink, receiver) = client2.violetabft().unwrap();
            let mut msgs = Reusable(rx2)
                .map(|msgs| {
                    let len = msgs.len();
                    let grpc_msgs = msgs.into_iter().enumerate().map(move |(i, v)| {
                        if i < len - 1 {
                            (v, WriteFlags::default().buffer_hint(true))
                        } else {
                            (v, WriteFlags::default())
                        }
                    });
                    stream::iter(grpc_msgs)
                })
                .flatten()
                .map(|msg| Ok(msg));
            let slightlike_res = async move {
                sink.slightlike_all(&mut msgs).await?;
                sink.close().await?;
                Ok(())
            }
            .await;
            let recv_res = receiver.await;
            check_rpc_result("violetabft", &addr2, slightlike_res.err(), recv_res.err())
        };

        client1.spawn(batch_slightlike_or_fallback.unwrap_or_else(move |_| {
            REPORT_FAILURE_MSG_COUNTER
                .with_label_values(&["unreachable", &*store_id.to_string()])
                .inc();
            router.broadcast_unreachable(store_id);
        }));

        Conn {
            stream: tx,
            _client: client1,
        }
    }
}

/// `VioletaBftClient` is used for slightlikeing violetabft messages to other stores.
pub struct VioletaBftClient<T: 'static> {
    env: Arc<Environment>,
    router: Mutex<T>,
    conns: HashMap<(String, usize), Conn>,
    pub addrs: HashMap<u64, String>,
    causetg: Arc<Config>,
    security_mgr: Arc<SecurityManager>,

    // To access CPU load of gRPC threads.
    grpc_thread_load: Arc<ThreadLoad>,
    // When message slightlikeers want to delay the notification to the gRPC client,
    // it can put a tokio_timer::Delay to the runtime.
    stats_pool: Option<tokio::runtime::Handle>,
    timer: Handle,
}

impl<T: VioletaBftStoreRouter<LmdbEngine>> VioletaBftClient<T> {
    pub fn new(
        env: Arc<Environment>,
        causetg: Arc<Config>,
        security_mgr: Arc<SecurityManager>,
        router: T,
        grpc_thread_load: Arc<ThreadLoad>,
        stats_pool: Option<tokio::runtime::Handle>,
    ) -> VioletaBftClient<T> {
        VioletaBftClient {
            env,
            router: Mutex::new(router),
            conns: HashMap::default(),
            addrs: HashMap::default(),
            causetg,
            security_mgr,
            grpc_thread_load,
            stats_pool,
            timer: GLOBAL_TIMER_HANDLE.clone(),
        }
    }

    fn get_conn(&mut self, addr: &str, brane_id: u64, store_id: u64) -> &mut Conn {
        let index = brane_id as usize % self.causetg.grpc_raft_conn_num;
        match self.conns.entry((addr.to_owned(), index)) {
            HashMapEntry::Occupied(e) => e.into_mut(),
            HashMapEntry::Vacant(e) => {
                let conn = Conn::new(
                    Arc::clone(&self.env),
                    self.router.lock().unwrap().clone(),
                    addr,
                    &self.causetg,
                    &self.security_mgr,
                    store_id,
                );
                e.insert(conn)
            }
        }
    }

    pub fn slightlike(&mut self, store_id: u64, addr: &str, msg: VioletaBftMessage) -> Result<()> {
        if let Err(SlightlikeError(msg)) = self
            .get_conn(addr, msg.brane_id, store_id)
            .stream
            .slightlike(msg)
        {
            warn!("slightlike to {} fail, the gRPC connection could be broken", addr);
            let index = msg.brane_id as usize % self.causetg.grpc_raft_conn_num;
            self.conns.remove(&(addr.to_owned(), index));

            if let Some(current_addr) = self.addrs.remove(&store_id) {
                if current_addr != *addr {
                    self.addrs.insert(store_id, current_addr);
                }
            }
            return Err(box_err!("VioletaBftClient slightlike fail"));
        }
        Ok(())
    }

    pub fn flush(&mut self) {
        let (mut counter, mut delay_counter) = (0, 0);
        for conn in self.conns.values_mut() {
            if conn.stream.is_empty() {
                continue;
            }
            if let Some(notifier) = conn.stream.get_notifier() {
                if !self.grpc_thread_load.in_heavy_load() || self.stats_pool.is_none() {
                    notifier.notify();
                    counter += 1;
                    continue;
                }
                let wait = self.causetg.heavy_load_wait_duration.0;
                self.stats_pool.as_ref().unwrap().spawn(
                    self.timer
                        .delay(Instant::now() + wait)
                        .compat()
                        .map_err(|_| warn!("VioletaBftClient delay flush error"))
                        .inspect(move |_| notifier.notify()),
                );
            }
            delay_counter += 1;
        }
        RAFT_MESSAGE_FLUSH_COUNTER.inc_by(i64::from(counter));
        RAFT_MESSAGE_DELAY_FLUSH_COUNTER.inc_by(i64::from(delay_counter));
    }
}

// Collect violetabft messages into a vector so that we can merge them into one message later.
struct VioletaBftMsgCollector {
    size: usize,
    limit: usize,
}

impl VioletaBftMsgCollector {
    fn new(limit: usize) -> Self {
        Self { size: 0, limit }
    }
}

impl BatchCollector<Vec<VioletaBftMessage>, VioletaBftMessage> for VioletaBftMsgCollector {
    fn collect(&mut self, v: &mut Vec<VioletaBftMessage>, e: VioletaBftMessage) -> Option<VioletaBftMessage> {
        let mut msg_size = e.spacelike_key.len() + e.lightlike_key.len();
        for entry in e.get_message().get_entries() {
            msg_size += entry.data.len();
        }
        if self.size > 0 && self.size + msg_size + GRPC_SEND_MSG_BUF >= self.limit as usize {
            self.size = 0;
            return Some(e);
        }
        self.size += msg_size;
        v.push(e);
        None
    }
}

// Reusable is for fallback batch_raft call to violetabft call.
struct Reusable<T>(Arc<Mutex<T>>);
impl<T: Stream + Unpin> Stream for Reusable<T> {
    type Item = T::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut t = self.0.lock().unwrap();
        Pin::new(&mut *t).poll_next(cx)
    }
}

fn check_rpc_result(
    rpc: &str,
    addr: &str,
    sink_e: Option<GrpcError>,
    recv_e: Option<GrpcError>,
) -> std::result::Result<(), bool> {
    if sink_e.is_none() && recv_e.is_none() {
        return Ok(());
    }
    warn!( "RPC {} fail", rpc; "to_addr" => addr, "sink_err" => ?sink_e, "err" => ?recv_e);
    recv_e.map_or(Ok(()), |e| Err(super::grpc_error_is_unimplemented(&e)))
}
