// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use futures::channel::mpsc::UnboundedLightlikeValue;
use futures::compat::Future01CompatExt;
use futures::executor::block_on;
use futures::future;
use futures::future::TryFutureExt;
use futures::stream::Stream;
use futures::stream::TryStreamExt;
use futures::task::Context;
use futures::task::Poll;
use futures::task::Waker;

use grpcio::{
    CallOption, ChannelBuilder, ClientDuplexReceiver, ClientDuplexlightlikeer, Environment,
    Result as GrpcResult,
};
use ekvproto::fidel_timeshare::{
    ErrorType, GetMembersRequest, GetMembersResponse, Member, FidelClient as FidelClientStub,
    BraneHeartbeatRequest, BraneHeartbeatResponse, ResponseHeader,
};
use security::SecurityManager;
use violetabftstore::interlock::::collections::HashSet;
use violetabftstore::interlock::::timer::GLOBAL_TIMER_HANDLE;
use violetabftstore::interlock::::{Either, HandyRwLock};
use tokio_timer::timer::Handle;

use super::{ClusterVersion, Config, Error, FidelFuture, Result, REQUEST_TIMEOUT};

pub struct Inner {
    env: Arc<Environment>,
    pub hb_lightlikeer: Either<
        Option<ClientDuplexlightlikeer<BraneHeartbeatRequest>>,
        UnboundedLightlikeValue<BraneHeartbeatRequest>,
    >,
    pub hb_receiver: Either<Option<ClientDuplexReceiver<BraneHeartbeatResponse>>, Waker>,
    pub client_stub: FidelClientStub,
    members: GetMembersResponse,
    security_mgr: Arc<SecurityManager>,
    on_reconnect: Option<Box<dyn Fn() + Sync + lightlike + 'static>>,

    last_fidelio: Instant,

    pub cluster_version: ClusterVersion,
}

pub struct HeartbeatReceiver {
    receiver: Option<ClientDuplexReceiver<BraneHeartbeatResponse>>,
    inner: Arc<RwLock<Inner>>,
}

impl Stream for HeartbeatReceiver {
    type Item = Result<BraneHeartbeatResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ref mut receiver) = self.receiver {
                match Pin::new(receiver).poll_next(cx) {
                    Poll::Ready(Some(Ok(item))) => return Poll::Ready(Some(Ok(item))),
                    Poll::Plightlikeing => return Poll::Plightlikeing,
                    // If it's None or there's error, we need to fidelio receiver.
                    _ => {}
                }
            }

            self.receiver.take();

            let mut inner = self.inner.wl();
            let mut receiver = None;
            if let Either::Left(ref mut recv) = inner.hb_receiver {
                receiver = recv.take();
            }
            if receiver.is_some() {
                debug!("heartbeat receiver is refreshed");
                drop(inner);
                self.receiver = receiver;
            } else {
                inner.hb_receiver = Either::Right(cx.waker().clone());
                return Poll::Plightlikeing;
            }
        }
    }
}

/// A leader client doing requests asynchronous.
pub struct LeaderClient {
    timer: Handle,
    pub(crate) inner: Arc<RwLock<Inner>>,
}

impl LeaderClient {
    pub fn new(
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        client_stub: FidelClientStub,
        members: GetMembersResponse,
    ) -> LeaderClient {
        let (tx, rx) = client_stub
            .brane_heartbeat()
            .unwrap_or_else(|e| panic!("fail to request FIDel {} err {:?}", "brane_heartbeat", e));

        LeaderClient {
            timer: GLOBAL_TIMER_HANDLE.clone(),
            inner: Arc::new(RwLock::new(Inner {
                env,
                hb_lightlikeer: Either::Left(Some(tx)),
                hb_receiver: Either::Left(Some(rx)),
                client_stub,
                members,
                security_mgr,
                on_reconnect: None,

                last_fidelio: Instant::now(),
                cluster_version: ClusterVersion::default(),
            })),
        }
    }

    pub fn handle_brane_heartbeat_response<F>(&self, f: F) -> FidelFuture<()>
    where
        F: Fn(BraneHeartbeatResponse) + lightlike + 'static,
    {
        let recv = HeartbeatReceiver {
            receiver: None,
            inner: Arc::clone(&self.inner),
        };
        Box::pin(
            recv.try_for_each(move |resp| {
                f(resp);
                future::ready(Ok(()))
            })
            .map_err(|e| panic!("unexpected error: {:?}", e)),
        )
    }

    pub fn on_reconnect(&self, f: Box<dyn Fn() + Sync + lightlike + 'static>) {
        let mut inner = self.inner.wl();
        inner.on_reconnect = Some(f);
    }

    pub fn request<Req, Resp, F>(&self, req: Req, func: F, retry: usize) -> Request<Req, F>
    where
        Req: Clone + 'static,
        F: FnMut(&RwLock<Inner>, Req) -> FidelFuture<Resp> + lightlike + 'static,
    {
        Request {
            reconnect_count: retry,
            request_sent: 0,
            client: LeaderClient {
                timer: self.timer.clone(),
                inner: Arc::clone(&self.inner),
            },
            req,
            func,
        }
    }

    pub fn get_leader(&self) -> Member {
        self.inner.rl().members.get_leader().clone()
    }

    /// Re-establishes connection with FIDel leader in asynchronized fashion.
    pub async fn reconnect(&self) -> Result<()> {
        let (future, spacelike) = {
            let inner = self.inner.rl();
            if inner.last_fidelio.elapsed() < Duration::from_secs(RECONNECT_INTERVAL_SEC) {
                // Avoid unnecessary ufidelating.
                return Ok(());
            }

            let spacelike = Instant::now();

            (
                try_connect_leader(
                    Arc::clone(&inner.env),
                    Arc::clone(&inner.security_mgr),
                    inner.members.clone(),
                ),
                spacelike,
            )
        };

        let (client, members) = future.await?;
        fail_point!("leader_client_reconnect");

        {
            let mut inner = self.inner.wl();
            let (tx, rx) = client.brane_heartbeat().unwrap_or_else(|e| {
                panic!("fail to request FIDel {} err {:?}", "brane_heartbeat", e)
            });
            info!("heartbeat lightlikeer and receiver are stale, refreshing ...");

            // Try to cancel an unused heartbeat lightlikeer.
            if let Either::Left(Some(ref mut r)) = inner.hb_lightlikeer {
                debug!("cancel brane heartbeat lightlikeer");
                r.cancel();
            }
            inner.hb_lightlikeer = Either::Left(Some(tx));
            let prev_receiver = std::mem::replace(&mut inner.hb_receiver, Either::Left(Some(rx)));
            let _ = prev_receiver.right().map(|t| t.wake());
            inner.client_stub = client;
            inner.members = members;
            inner.last_fidelio = Instant::now();
            if let Some(ref on_reconnect) = inner.on_reconnect {
                on_reconnect();
            }
        }
        warn!("ufidelating FIDel client done"; "splightlike" => ?spacelike.elapsed());
        Ok(())
    }
}

pub const RECONNECT_INTERVAL_SEC: u64 = 1; // 1s

/// The context of lightlikeing requets.
pub struct Request<Req, F> {
    reconnect_count: usize,
    request_sent: usize,
    client: LeaderClient,
    req: Req,
    func: F,
}

const MAX_REQUEST_COUNT: usize = 3;

impl<Req, Resp, F> Request<Req, F>
where
    Req: Clone + lightlike + 'static,
    F: FnMut(&RwLock<Inner>, Req) -> FidelFuture<Resp> + lightlike + 'static,
{
    async fn reconnect_if_needed(&mut self) -> bool {
        debug!("reconnecting ..."; "remain" => self.reconnect_count);

        if self.request_sent < MAX_REQUEST_COUNT {
            return true;
        }

        // Ufidelating client.
        self.reconnect_count -= 1;

        // FIXME: should not block the core.
        debug!("(re)connecting FIDel client");
        match self.client.reconnect().await {
            Ok(_) => {
                self.request_sent = 0;
                true
            }
            Err(_) => {
                let _ = self
                    .client
                    .timer
                    .delay(Instant::now() + Duration::from_secs(RECONNECT_INTERVAL_SEC))
                    .compat()
                    .await;
                false
            }
        }
    }

    async fn lightlike_and_receive(&mut self) -> Result<Resp> {
        self.request_sent += 1;
        debug!("request sent: {}", self.request_sent);
        let r = self.req.clone();

        (self.func)(&self.client.inner, r).await
    }

    fn should_not_retry(resp: &Result<Resp>) -> bool {
        match resp {
            Ok(_) => true,
            // Error::Incompatible is returned by response header from FIDel, no need to retry
            Err(Error::Incompatible) => true,
            Err(err) => {
                error!(?err; "request failed, retry");
                false
            }
        }
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(mut self) -> FidelFuture<Resp> {
        Box::pin(async move {
            while self.reconnect_count != 0 {
                if self.reconnect_if_needed().await {
                    let resp = self.lightlike_and_receive().await;
                    if Self::should_not_retry(&resp) {
                        return resp;
                    }
                }
            }
            Err(box_err!("request retry exceeds limit"))
        })
    }
}

/// Do a request in synchronized fashion.
pub fn sync_request<F, R>(client: &LeaderClient, retry: usize, func: F) -> Result<R>
where
    F: Fn(&FidelClientStub) -> GrpcResult<R>,
{
    let mut err = None;
    for _ in 0..retry {
        let ret = {
            // Drop the read dagger immediately to prevent the deadlock between the caller thread
            // which may hold the read dagger and wait for FIDel client thread completing the request
            // and the FIDel client thread which may block on acquiring the write dagger.
            let client_stub = client.inner.rl().client_stub.clone();
            func(&client_stub).map_err(Error::Grpc)
        };
        match ret {
            Ok(r) => {
                return Ok(r);
            }
            Err(e) => {
                error!(?e; "request failed");
                if let Err(e) = block_on(client.reconnect()) {
                    error!(?e; "reconnect failed");
                }
                err.replace(e);
            }
        }
    }

    Err(err.unwrap_or(box_err!("fail to request")))
}

pub fn validate_lightlikepoints(
    env: Arc<Environment>,
    causet: &Config,
    security_mgr: Arc<SecurityManager>,
) -> Result<(FidelClientStub, GetMembersResponse)> {
    let len = causet.lightlikepoints.len();
    let mut lightlikepoints_set = HashSet::with_capacity_and_hasher(len, Default::default());

    let mut members = None;
    let mut cluster_id = None;
    for ep in &causet.lightlikepoints {
        if !lightlikepoints_set.insert(ep) {
            return Err(box_err!("duplicate FIDel lightlikepoint {}", ep));
        }

        let (_, resp) = match block_on(connect(Arc::clone(&env), &security_mgr, ep)) {
            Ok(resp) => resp,
            // Ignore failed FIDel node.
            Err(e) => {
                info!("FIDel failed to respond"; "lightlikepoints" => ep, "err" => ?e);
                continue;
            }
        };

        // Check cluster ID.
        let cid = resp.get_header().get_cluster_id();
        if let Some(sample) = cluster_id {
            if sample != cid {
                return Err(box_err!(
                    "FIDel response cluster_id mismatch, want {}, got {}",
                    sample,
                    cid
                ));
            }
        } else {
            cluster_id = Some(cid);
        }
        // TODO: check all fields later?

        if members.is_none() {
            members = Some(resp);
        }
    }

    match members {
        Some(members) => {
            let (client, members) =
                block_on(try_connect_leader(Arc::clone(&env), security_mgr, members))?;
            info!("all FIDel lightlikepoints are consistent"; "lightlikepoints" => ?causet.lightlikepoints);
            Ok((client, members))
        }
        _ => Err(box_err!("FIDel cluster failed to respond")),
    }
}

async fn connect(
    env: Arc<Environment>,
    security_mgr: &SecurityManager,
    addr: &str,
) -> Result<(FidelClientStub, GetMembersResponse)> {
    info!("connecting to FIDel lightlikepoint"; "lightlikepoints" => addr);
    let addr = addr
        .trim_spacelike_matches("http://")
        .trim_spacelike_matches("https://");
    let channel = {
        let cb = ChannelBuilder::new(env)
            .keepalive_time(Duration::from_secs(10))
            .keepalive_timeout(Duration::from_secs(3));
        security_mgr.connect(cb, addr)
    };
    let client = FidelClientStub::new(channel);
    let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
    let response = client
        .get_members_async_opt(&GetMembersRequest::default(), option)
        .unwrap_or_else(|e| panic!("fail to request FIDel {} err {:?}", "get_members", e))
        .await;
    match response {
        Ok(resp) => Ok((client, resp)),
        Err(e) => Err(Error::Grpc(e)),
    }
}

pub async fn try_connect_leader(
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
    previous: GetMembersResponse,
) -> Result<(FidelClientStub, GetMembersResponse)> {
    let previous_leader = previous.get_leader();
    let members = previous.get_members();
    let cluster_id = previous.get_header().get_cluster_id();
    let mut resp = None;
    // Try to connect to other members, then the previous leader.
    'outer: for m in members
        .iter()
        .filter(|m| *m != previous_leader)
        .chain(&[previous_leader.clone()])
    {
        for ep in m.get_client_urls() {
            match connect(Arc::clone(&env), &security_mgr, ep.as_str()).await {
                Ok((_, r)) => {
                    let new_cluster_id = r.get_header().get_cluster_id();
                    if new_cluster_id == cluster_id {
                        resp = Some(r);
                        break 'outer;
                    } else {
                        panic!(
                            "{} no longer belongs to cluster {}, it is in {}",
                            ep, cluster_id, new_cluster_id
                        );
                    }
                }
                Err(e) => {
                    error!(?e; "connect failed"; "lightlikepoints" => ep,);
                    continue;
                }
            }
        }
    }

    // Then try to connect the FIDel cluster leader.
    if let Some(resp) = resp {
        let leader = resp.get_leader().clone();
        for ep in leader.get_client_urls() {
            if let Ok((client, _)) = connect(Arc::clone(&env), &security_mgr, ep.as_str()).await {
                info!("connected to FIDel leader"; "lightlikepoints" => ep);
                return Ok((client, resp));
            }
        }
    }

    Err(box_err!("failed to connect to {:?}", members))
}

/// Convert a FIDel protobuf error to an `Error`.
pub fn check_resp_header(header: &ResponseHeader) -> Result<()> {
    if !header.has_error() {
        return Ok(());
    }
    let err = header.get_error();
    match err.get_type() {
        ErrorType::AlreadyBootstrapped => Err(Error::ClusterBootstrapped(header.get_cluster_id())),
        ErrorType::NotBootstrapped => Err(Error::ClusterNotBootstrapped(header.get_cluster_id())),
        ErrorType::IncompatibleVersion => Err(Error::Incompatible),
        ErrorType::StoreTombstone => Err(Error::StoreTombstone(err.get_message().to_owned())),
        ErrorType::BraneNotFound => Err(Error::BraneNotFound(vec![])),
        ErrorType::Unknown => Err(box_err!(err.get_message())),
        ErrorType::Ok => Ok(()),
    }
}
