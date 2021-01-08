// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::{Future, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use futures::task::{Context, Poll};
use grpcio::{
    ChannelBuilder, ClientStreamingSink, Environment, RequestStream, RpcStatus, RpcStatusCode,
    WriteFlags,
};
use ekvproto::violetabft_serverpb::VioletaBftMessage;
use ekvproto::violetabft_serverpb::{Done, SnapshotSoliton};
use ekvproto::einsteindbpb::EINSTEINDBClient;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

use engine_lmdb::LmdbEngine;
use violetabftstore::router::VioletaBftStoreRouter;
use violetabftstore::store::{GenericSnapshot, SnapEntry, SnapKey, SnapManager};
use security::SecurityManager;
use einsteindb_util::worker::Runnable;
use einsteindb_util::DeferContext;

use super::metrics::*;
use super::{Config, Error, Result};

pub type Callback = Box<dyn FnOnce(Result<()>) + Slightlike>;

const DEFAULT_POOL_SIZE: usize = 4;

/// A task for either receiving Snapshot or slightlikeing Snapshot
pub enum Task {
    Recv {
        stream: RequestStream<SnapshotSoliton>,
        sink: ClientStreamingSink<Done>,
    },
    Slightlike {
        addr: String,
        msg: VioletaBftMessage,
        cb: Callback,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Recv { .. } => write!(f, "Recv"),
            Task::Slightlike {
                ref addr, ref msg, ..
            } => write!(f, "Slightlike Snap[to: {}, snap: {:?}]", addr, msg),
        }
    }
}

struct SnapSoliton {
    first: Option<SnapshotSoliton>,
    snap: Box<dyn GenericSnapshot>,
    remain_bytes: usize,
}

const SNAP_Soliton_LEN: usize = 1024 * 1024;

impl Stream for SnapSoliton {
    type Item = Result<(SnapshotSoliton, WriteFlags)>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(t) = self.first.take() {
            let write_flags = WriteFlags::default().buffer_hint(true);
            return Poll::Ready(Some(Ok((t, write_flags))));
        }

        let mut buf = match self.remain_bytes {
            0 => return Poll::Ready(None),
            n if n > SNAP_Soliton_LEN => vec![0; SNAP_Soliton_LEN],
            n => vec![0; n],
        };
        let result = self.snap.read_exact(buf.as_mut_slice());
        match result {
            Ok(_) => {
                self.remain_bytes -= buf.len();
                let mut Soliton = SnapshotSoliton::default();
                Soliton.set_data(buf);
                Poll::Ready(Some(Ok((Soliton, WriteFlags::default().buffer_hint(true)))))
            }
            Err(e) => Poll::Ready(Some(Err(box_err!("failed to read snapshot Soliton: {}", e)))),
        }
    }
}

struct SlightlikeStat {
    key: SnapKey,
    total_size: u64,
    elapsed: Duration,
}

/// Slightlike the snapshot to specified address.
///
/// It will first slightlike the normal violetabft snapshot message and then slightlike the snapshot file.
fn slightlike_snap(
    env: Arc<Environment>,
    mgr: SnapManager,
    security_mgr: Arc<SecurityManager>,
    causet: &Config,
    addr: &str,
    msg: VioletaBftMessage,
) -> Result<impl Future<Output = Result<SlightlikeStat>>> {
    assert!(msg.get_message().has_snapshot());
    let timer = Instant::now();

    let slightlike_timer = SEND_SNAP_HISTOGRAM.spacelike_coarse_timer();

    let key = {
        let snap = msg.get_message().get_snapshot();
        SnapKey::from_snap(snap)?
    };

    mgr.register(key.clone(), SnapEntry::Slightlikeing);
    let deregister = {
        let (mgr, key) = (mgr.clone(), key.clone());
        DeferContext::new(move || mgr.deregister(&key, &SnapEntry::Slightlikeing))
    };

    let s = box_try!(mgr.get_snapshot_for_slightlikeing(&key));
    if !s.exists() {
        return Err(box_err!("missing snap file: {:?}", s.path()));
    }
    let total_size = s.total_size()?;

    let mut Solitons = {
        let mut first_Soliton = SnapshotSoliton::default();
        first_Soliton.set_message(msg);

        SnapSoliton {
            first: Some(first_Soliton),
            snap: s,
            remain_bytes: total_size as usize,
        }
    };

    let cb = ChannelBuilder::new(env)
        .stream_initial_window_size(causet.grpc_stream_initial_window_size.0 as i32)
        .keepalive_time(causet.grpc_keepalive_time.0)
        .keepalive_timeout(causet.grpc_keepalive_timeout.0)
        .default_compression_algorithm(causet.grpc_compression_algorithm());

    let channel = security_mgr.connect(cb, addr);
    let client = EINSTEINDBClient::new(channel);
    let (sink, receiver) = client.snapshot()?;

    let slightlike_task = async move {
        let mut sink = sink.sink_map_err(Error::from);
        sink.slightlike_all(&mut Solitons).await?;
        sink.close().await?;
        let recv_result = receiver.map_err(Error::from).await;
        slightlike_timer.observe_duration();
        drop(deregister);
        drop(client);
        match recv_result {
            Ok(_) => {
                fail_point!("snapshot_delete_after_slightlike");
                Solitons.snap.delete();
                // TODO: improve it after rustc resolves the bug.
                // Call `info` in the closure directly will cause rustc
                // panic with `Cannot create local mono-item for DefId`.
                Ok(SlightlikeStat {
                    key,
                    total_size,
                    elapsed: timer.elapsed(),
                })
            }
            Err(e) => Err(e),
        }
    };
    Ok(slightlike_task)
}

struct RecvSnapContext {
    key: SnapKey,
    file: Option<Box<dyn GenericSnapshot>>,
    violetabft_msg: VioletaBftMessage,
}

impl RecvSnapContext {
    fn new(head_Soliton: Option<SnapshotSoliton>, snap_mgr: &SnapManager) -> Result<Self> {
        // head_Soliton is None means the stream is empty.
        let mut head = head_Soliton.ok_or_else(|| Error::Other("empty gRPC stream".into()))?;
        if !head.has_message() {
            return Err(box_err!("no violetabft message in the first Soliton"));
        }

        let meta = head.take_message();
        let key = match SnapKey::from_snap(meta.get_message().get_snapshot()) {
            Ok(k) => k,
            Err(e) => return Err(box_err!("failed to create snap key: {:?}", e)),
        };

        let snap = {
            let data = meta.get_message().get_snapshot().get_data();
            let s = match snap_mgr.get_snapshot_for_receiving(&key, data) {
                Ok(s) => s,
                Err(e) => return Err(box_err!("{} failed to create snapshot file: {:?}", key, e)),
            };

            if s.exists() {
                let p = s.path();
                info!("snapshot file already exists, skip receiving"; "snap_key" => %key, "file" => p);
                None
            } else {
                Some(s)
            }
        };

        Ok(RecvSnapContext {
            key,
            file: snap,
            violetabft_msg: meta,
        })
    }

    fn finish<R: VioletaBftStoreRouter<LmdbEngine>>(self, violetabft_router: R) -> Result<()> {
        let key = self.key;
        if let Some(mut file) = self.file {
            info!("saving snapshot file"; "snap_key" => %key, "file" => file.path());
            if let Err(e) = file.save() {
                let path = file.path();
                let e = box_err!("{} failed to save snapshot file {}: {:?}", key, path, e);
                return Err(e);
            }
        }
        if let Err(e) = violetabft_router.slightlike_violetabft_msg(self.violetabft_msg) {
            return Err(box_err!("{} failed to slightlike snapshot to violetabft: {}", key, e));
        }
        Ok(())
    }
}

fn recv_snap<R: VioletaBftStoreRouter<LmdbEngine> + 'static>(
    stream: RequestStream<SnapshotSoliton>,
    sink: ClientStreamingSink<Done>,
    snap_mgr: SnapManager,
    violetabft_router: R,
) -> impl Future<Output = Result<()>> {
    let recv_task = async move {
        let mut stream = stream.map_err(Error::from);
        let head = stream.next().await.transpose()?;
        let mut context = RecvSnapContext::new(head, &snap_mgr)?;
        if context.file.is_none() {
            return context.finish(violetabft_router);
        }
        let context_key = context.key.clone();
        snap_mgr.register(context.key.clone(), SnapEntry::Receiving);

        while let Some(item) = stream.next().await {
            let mut Soliton = item?;
            let data = Soliton.take_data();
            if data.is_empty() {
                return Err(box_err!("{} receive Soliton with empty data", context.key));
            }
            if let Err(e) = context.file.as_mut().unwrap().write_all(&data) {
                let key = &context.key;
                let path = context.file.as_mut().unwrap().path();
                let e = box_err!("{} failed to write snapshot file {}: {}", key, path, e);
                return Err(e);
            }
        }

        let res = context.finish(violetabft_router);
        snap_mgr.deregister(&context_key, &SnapEntry::Receiving);
        res
    };

    async move {
        match recv_task.await {
            Ok(()) => sink.success(Done::default()).await.map_err(Error::from),
            Err(e) => {
                let status = RpcStatus::new(RpcStatusCode::UNKNOWN, Some(format!("{:?}", e)));
                sink.fail(status).await.map_err(Error::from)
            }
        }
    }
}

pub struct Runner<R: VioletaBftStoreRouter<LmdbEngine> + 'static> {
    env: Arc<Environment>,
    snap_mgr: SnapManager,
    pool: Runtime,
    violetabft_router: R,
    security_mgr: Arc<SecurityManager>,
    causet: Arc<Config>,
    slightlikeing_count: Arc<AtomicUsize>,
    recving_count: Arc<AtomicUsize>,
}

impl<R: VioletaBftStoreRouter<LmdbEngine> + 'static> Runner<R> {
    pub fn new(
        env: Arc<Environment>,
        snap_mgr: SnapManager,
        r: R,
        security_mgr: Arc<SecurityManager>,
        causet: Arc<Config>,
    ) -> Runner<R> {
        Runner {
            env,
            snap_mgr,
            pool: RuntimeBuilder::new()
                .threaded_scheduler()
                .thread_name(thd_name!("snap-slightlikeer"))
                .core_threads(DEFAULT_POOL_SIZE)
                .on_thread_spacelike(|| einsteindb_alloc::add_thread_memory_accessor())
                .on_thread_stop(|| einsteindb_alloc::remove_thread_memory_accessor())
                .build()
                .unwrap(),
            violetabft_router: r,
            security_mgr,
            causet,
            slightlikeing_count: Arc::new(AtomicUsize::new(0)),
            recving_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<R: VioletaBftStoreRouter<LmdbEngine> + 'static> Runnable for Runner<R> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Recv { stream, sink } => {
                let task_num = self.recving_count.load(Ordering::SeqCst);
                if task_num >= self.causet.concurrent_recv_snap_limit {
                    warn!("too many recving snapshot tasks, ignore");
                    let status = RpcStatus::new(
                        RpcStatusCode::RESOURCE_EXHAUSTED,
                        Some(format!(
                            "the number of received snapshot tasks {} exceeded the limitation {}",
                            task_num, self.causet.concurrent_recv_snap_limit
                        )),
                    );
                    self.pool.spawn(sink.fail(status));
                    return;
                }
                SNAP_TASK_COUNTER_STATIC.recv.inc();

                let snap_mgr = self.snap_mgr.clone();
                let violetabft_router = self.violetabft_router.clone();
                let recving_count = Arc::clone(&self.recving_count);
                recving_count.fetch_add(1, Ordering::SeqCst);
                let task = async move {
                    let result = recv_snap(stream, sink, snap_mgr, violetabft_router).await;
                    recving_count.fetch_sub(1, Ordering::SeqCst);
                    if let Err(e) = result {
                        error!("failed to recv snapshot"; "err" => %e);
                    }
                };
                self.pool.spawn(task);
            }
            Task::Slightlike { addr, msg, cb } => {
                fail_point!("slightlike_snapshot");
                if self.slightlikeing_count.load(Ordering::SeqCst) >= self.causet.concurrent_slightlike_snap_limit
                {
                    warn!(
                        "too many slightlikeing snapshot tasks, drop Slightlike Snap[to: {}, snap: {:?}]",
                        addr, msg
                    );
                    cb(Err(Error::Other("Too many slightlikeing snapshot tasks".into())));
                    return;
                }
                SNAP_TASK_COUNTER_STATIC.slightlike.inc();

                let env = Arc::clone(&self.env);
                let mgr = self.snap_mgr.clone();
                let security_mgr = Arc::clone(&self.security_mgr);
                let slightlikeing_count = Arc::clone(&self.slightlikeing_count);
                slightlikeing_count.fetch_add(1, Ordering::SeqCst);

                let slightlike_task = slightlike_snap(env, mgr, security_mgr, &self.causet, &addr, msg);
                let task = async move {
                    let res = match slightlike_task {
                        Err(e) => Err(e),
                        Ok(f) => f.await,
                    };
                    match res {
                        Ok(stat) => {
                            info!(
                                "sent snapshot";
                                "brane_id" => stat.key.brane_id,
                                "snap_key" => %stat.key,
                                "size" => stat.total_size,
                                "duration" => ?stat.elapsed
                            );
                            cb(Ok(()));
                        }
                        Err(e) => {
                            error!("failed to slightlike snap"; "to_addr" => addr, "err" => ?e);
                            cb(Err(e));
                        }
                    };
                    slightlikeing_count.fetch_sub(1, Ordering::SeqCst);
                };

                self.pool.spawn(task);
            }
        }
    }
}
