// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::fmt;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use futures::channel::mpsc;
use futures::compat::Future01CompatExt;
use futures::executor::block_on;
use futures::future::{self, FutureExt};
use futures::sink::SinkExt;
use futures::stream::{StreamExt, TryStreamExt};

use grpcio::{CallOption, EnvBuilder, Result as GrpcResult, WriteFlags};
use ekvproto::meta_timeshare;
use ekvproto::fidel_timeshare::{self, Member};
use ekvproto::replication_mode_timeshare::{BraneReplicationStatus, ReplicationStatus};
use security::SecurityManager;
use violetabftstore::interlock::::time::duration_to_sec;
use violetabftstore::interlock::::{Either, HandyRwLock};
use txn_types::TimeStamp;

use super::metrics::*;
use super::util::{check_resp_header, sync_request, validate_lightlikepoints, Inner, LeaderClient};
use super::{ClusterVersion, Config, FidelFuture, UnixSecs};
use super::{Error, FidelClient, BraneInfo, BraneStat, Result, REQUEST_TIMEOUT};
use violetabftstore::interlock::::timer::GLOBAL_TIMER_HANDLE;

const CQ_COUNT: usize = 1;
const CLIENT_PREFIX: &str = "fidel";

pub struct RpcClient {
    cluster_id: u64,
    leader_client: Arc<LeaderClient>,
}

impl RpcClient {
    pub fn new(causet: &Config, security_mgr: Arc<SecurityManager>) -> Result<RpcClient> {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(CQ_COUNT)
                .name_prefix(thd_name!(CLIENT_PREFIX))
                .build(),
        );

        // -1 means the max.
        let retries = match causet.retry_max_count {
            -1 => std::isize::MAX,
            v => v.checked_add(1).unwrap_or(std::isize::MAX),
        };
        for i in 0..retries {
            match validate_lightlikepoints(Arc::clone(&env), causet, security_mgr.clone()) {
                Ok((client, members)) => {
                    let rpc_client = RpcClient {
                        cluster_id: members.get_header().get_cluster_id(),
                        leader_client: Arc::new(LeaderClient::new(
                            env,
                            security_mgr,
                            client,
                            members,
                        )),
                    };

                    // spawn a background future to fidelio FIDel information periodically
                    let duration = causet.fidelio_interval.0;
                    let client = Arc::downgrade(&rpc_client.leader_client);
                    let fidelio_loop = async move {
                        loop {
                            let ok = GLOBAL_TIMER_HANDLE
                                .delay(Instant::now() + duration)
                                .compat()
                                .await
                                .is_ok();

                            if !ok {
                                warn!("failed to delay with global timer");
                                continue;
                            }

                            match client.upgrade() {
                                Some(cli) => {
                                    let req = cli.reconnect().await;
                                    if req.is_err() {
                                        warn!("fidelio FIDel information failed");
                                        // will fidelio later anyway
                                    }
                                }
                                // if the client has been dropped, we can stop
                                None => break,
                            }
                        }
                    };

                    rpc_client
                        .leader_client
                        .inner
                        .rl()
                        .client_stub
                        .spawn(fidelio_loop);

                    return Ok(rpc_client);
                }
                Err(e) => {
                    if i as usize % causet.retry_log_every == 0 {
                        warn!("validate FIDel lightlikepoints failed"; "err" => ?e);
                    }
                    thread::sleep(causet.retry_interval.0);
                }
            }
        }
        Err(box_err!("lightlikepoints are invalid"))
    }

    /// Creates a new request header.
    fn header(&self) -> fidel_timeshare::RequestHeader {
        let mut header = fidel_timeshare::RequestHeader::default();
        header.set_cluster_id(self.cluster_id);
        header
    }

    /// Gets the leader of FIDel.
    pub fn get_leader(&self) -> Member {
        self.leader_client.get_leader()
    }

    /// Re-establishes connection with FIDel leader in synchronized fashion.
    pub fn reconnect(&self) -> Result<()> {
        block_on(self.leader_client.reconnect())
    }

    pub fn cluster_version(&self) -> ClusterVersion {
        self.leader_client.inner.rl().cluster_version.clone()
    }

    /// Creates a new call option with default request timeout.
    #[inline]
    fn call_option() -> CallOption {
        CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT))
    }

    /// Gets given key's Brane and Brane's leader from FIDel.
    fn get_brane_and_leader(&self, key: &[u8]) -> Result<(meta_timeshare::Brane, Option<meta_timeshare::Peer>)> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_brane"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::GetBraneRequest::default();
        req.set_header(self.header());
        req.set_brane_key(key.to_vec());

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_brane_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        let brane = if resp.has_brane() {
            resp.take_brane()
        } else {
            return Err(Error::BraneNotFound(key.to_owned()));
        };
        let leader = if resp.has_leader() {
            Some(resp.take_leader())
        } else {
            None
        };
        Ok((brane, leader))
    }
}

impl fmt::Debug for RpcClient {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("RpcClient")
            .field("cluster_id", &self.cluster_id)
            .field("leader", &self.get_leader())
            .finish()
    }
}

const LEADER_CHANGE_RETRY: usize = 10;

impl FidelClient for RpcClient {
    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(
        &self,
        stores: meta_timeshare::CausetStore,
        brane: meta_timeshare::Brane,
    ) -> Result<Option<ReplicationStatus>> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["bootstrap_cluster"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::BootstrapRequest::default();
        req.set_header(self.header());
        req.set_store(stores);
        req.set_brane(brane);

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.bootstrap_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;
        Ok(resp.replication_status.take())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["is_cluster_bootstrapped"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::IsBootstrappedRequest::default();
        req.set_header(self.header());

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.is_bootstrapped_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_bootstrapped())
    }

    fn alloc_id(&self) -> Result<u64> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["alloc_id"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::AllocIdRequest::default();
        req.set_header(self.header());

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.alloc_id_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_id())
    }

    fn put_store(&self, store: meta_timeshare::CausetStore) -> Result<Option<ReplicationStatus>> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["put_store"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::PutStoreRequest::default();
        req.set_header(self.header());
        req.set_store(store);

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.put_store_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.replication_status.take())
    }

    fn get_store(&self, store_id: u64) -> Result<meta_timeshare::CausetStore> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_store"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::GetStoreRequest::default();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_store_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        let store = resp.take_store();
        if store.get_state() != meta_timeshare::StoreState::Tombstone {
            Ok(store)
        } else {
            Err(Error::StoreTombstone(format!("{:?}", store)))
        }
    }

    fn get_all_stores(&self, exclude_tombstone: bool) -> Result<Vec<meta_timeshare::CausetStore>> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_all_stores"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::GetAllStoresRequest::default();
        req.set_header(self.header());
        req.set_exclude_tombstone_stores(exclude_tombstone);

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_all_stores_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_stores().into())
    }

    fn get_cluster_config(&self) -> Result<meta_timeshare::Cluster> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_cluster_config"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::GetClusterConfigRequest::default();
        req.set_header(self.header());

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_cluster_config_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_cluster())
    }

    fn get_brane(&self, key: &[u8]) -> Result<meta_timeshare::Brane> {
        self.get_brane_and_leader(key).map(|x| x.0)
    }

    fn get_brane_info(&self, key: &[u8]) -> Result<BraneInfo> {
        self.get_brane_and_leader(key)
            .map(|x| BraneInfo::new(x.0, x.1))
    }

    fn get_brane_by_id(&self, brane_id: u64) -> FidelFuture<Option<meta_timeshare::Brane>> {
        let timer = Instant::now();

        let mut req = fidel_timeshare::GetBraneByIdRequest::default();
        req.set_header(self.header());
        req.set_brane_id(brane_id);

        let executor = move |client: &RwLock<Inner>, req: fidel_timeshare::GetBraneByIdRequest| {
            let handler = client
                .rl()
                .client_stub
                .get_brane_by_id_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| {
                    panic!("fail to request FIDel {} err {:?}", "get_brane_by_id", e)
                });
            Box::pin(async move {
                let mut resp = handler.await?;
                FIDel_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_brane_by_id"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                if resp.has_brane() {
                    Ok(Some(resp.take_brane()))
                } else {
                    Ok(None)
                }
            }) as FidelFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn brane_heartbeat(
        &self,
        term: u64,
        brane: meta_timeshare::Brane,
        leader: meta_timeshare::Peer,
        brane_stat: BraneStat,
        replication_status: Option<BraneReplicationStatus>,
    ) -> FidelFuture<()> {
        FIDel_HEARTBEAT_COUNTER_VEC.with_label_values(&["lightlike"]).inc();

        let mut req = fidel_timeshare::BraneHeartbeatRequest::default();
        req.set_term(term);
        req.set_header(self.header());
        req.set_brane(brane);
        req.set_leader(leader);
        req.set_down_peers(brane_stat.down_peers.into());
        req.set_plightlikeing_peers(brane_stat.plightlikeing_peers.into());
        req.set_bytes_written(brane_stat.written_bytes);
        req.set_tuplespaceInstanton_written(brane_stat.written_tuplespaceInstanton);
        req.set_bytes_read(brane_stat.read_bytes);
        req.set_tuplespaceInstanton_read(brane_stat.read_tuplespaceInstanton);
        req.set_approximate_size(brane_stat.approximate_size);
        req.set_approximate_tuplespaceInstanton(brane_stat.approximate_tuplespaceInstanton);
        if let Some(s) = replication_status {
            req.set_replication_status(s);
        }
        let mut interval = fidel_timeshare::TimeInterval::default();
        interval.set_spacelike_timestamp(brane_stat.last_report_ts.into_inner());
        interval.set_lightlike_timestamp(UnixSecs::now().into_inner());
        req.set_interval(interval);

        let executor = |client: &RwLock<Inner>, req: fidel_timeshare::BraneHeartbeatRequest| {
            let mut inner = client.wl();
            if let Either::Right(ref lightlikeer) = inner.hb_lightlikeer {
                let ret = lightlikeer
                    .unbounded_lightlike(req)
                    .map_err(|e| Error::Other(Box::new(e)));
                return Box::pin(future::ready(ret)) as FidelFuture<_>;
            }

            debug!("heartbeat lightlikeer is refreshed");
            let left = inner.hb_lightlikeer.as_mut().left().unwrap();
            let lightlikeer = left.take().expect("expect brane heartbeat sink");
            let (tx, rx) = mpsc::unbounded();
            tx.unbounded_lightlike(req)
                .unwrap_or_else(|e| panic!("lightlike request to unbounded channel failed {:?}", e));
            inner.hb_lightlikeer = Either::Right(tx);
            Box::pin(async move {
                let mut lightlikeer = lightlikeer.sink_map_err(Error::Grpc);
                let result = lightlikeer
                    .lightlike_all(&mut rx.map(|r| Ok((r, WriteFlags::default()))))
                    .await;
                match result {
                    Ok(()) => {
                        lightlikeer.get_mut().cancel();
                        info!("cancel brane heartbeat lightlikeer");
                        Ok(())
                    }
                    Err(e) => {
                        error!(?e; "failed to lightlike heartbeat");
                        Err(e)
                    }
                }
            }) as FidelFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn handle_brane_heartbeat_response<F>(&self, _: u64, f: F) -> FidelFuture<()>
    where
        F: Fn(fidel_timeshare::BraneHeartbeatResponse) + lightlike + 'static,
    {
        self.leader_client.handle_brane_heartbeat_response(f)
    }

    fn ask_split(&self, brane: meta_timeshare::Brane) -> FidelFuture<fidel_timeshare::AskSplitResponse> {
        let timer = Instant::now();

        let mut req = fidel_timeshare::AskSplitRequest::default();
        req.set_header(self.header());
        req.set_brane(brane);

        let executor = move |client: &RwLock<Inner>, req: fidel_timeshare::AskSplitRequest| {
            let handler = client
                .rl()
                .client_stub
                .ask_split_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| panic!("fail to request FIDel {} err {:?}", "ask_split", e));

            Box::pin(async move {
                let resp = handler.await?;
                FIDel_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["ask_split"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp)
            }) as FidelFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn ask_batch_split(
        &self,
        brane: meta_timeshare::Brane,
        count: usize,
    ) -> FidelFuture<fidel_timeshare::AskBatchSplitResponse> {
        let timer = Instant::now();

        let mut req = fidel_timeshare::AskBatchSplitRequest::default();
        req.set_header(self.header());
        req.set_brane(brane);
        req.set_split_count(count as u32);

        let executor = move |client: &RwLock<Inner>, req: fidel_timeshare::AskBatchSplitRequest| {
            let handler = client
                .rl()
                .client_stub
                .ask_batch_split_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| panic!("fail to request FIDel {} err {:?}", "ask_batch_split", e));

            Box::pin(async move {
                let resp = handler.await?;
                FIDel_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["ask_batch_split"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp)
            }) as FidelFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn store_heartbeat(
        &self,
        mut stats: fidel_timeshare::StoreStats,
    ) -> FidelFuture<fidel_timeshare::StoreHeartbeatResponse> {
        let timer = Instant::now();

        let mut req = fidel_timeshare::StoreHeartbeatRequest::default();
        req.set_header(self.header());
        stats
            .mut_interval()
            .set_lightlike_timestamp(UnixSecs::now().into_inner());
        req.set_stats(stats);
        let executor = move |client: &RwLock<Inner>, req: fidel_timeshare::StoreHeartbeatRequest| {
            let cluster_version = client.rl().cluster_version.clone();
            let handler = client
                .rl()
                .client_stub
                .store_heartbeat_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| panic!("fail to request FIDel {} err {:?}", "store_heartbeat", e));
            Box::pin(async move {
                let resp = handler.await?;
                FIDel_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["store_heartbeat"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                match cluster_version.set(resp.get_cluster_version()) {
                    Err(_) => warn!("invalid cluster version: {}", resp.get_cluster_version()),
                    Ok(true) => info!("set cluster version to {}", resp.get_cluster_version()),
                    _ => {}
                };
                Ok(resp)
            }) as FidelFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn report_batch_split(&self, branes: Vec<meta_timeshare::Brane>) -> FidelFuture<()> {
        let timer = Instant::now();

        let mut req = fidel_timeshare::ReportBatchSplitRequest::default();
        req.set_header(self.header());
        req.set_branes(branes.into());

        let executor = move |client: &RwLock<Inner>, req: fidel_timeshare::ReportBatchSplitRequest| {
            let handler = client
                .rl()
                .client_stub
                .report_batch_split_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| {
                    panic!("fail to request FIDel {} err {:?}", "report_batch_split", e)
                });
            Box::pin(async move {
                let resp = handler.await?;
                FIDel_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["report_batch_split"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(())
            }) as FidelFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn scatter_brane(&self, mut brane: BraneInfo) -> Result<()> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["scatter_brane"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::ScatterBraneRequest::default();
        req.set_header(self.header());
        req.set_brane_id(brane.get_id());
        if let Some(leader) = brane.leader.take() {
            req.set_leader(leader);
        }
        req.set_brane(brane.brane);

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.scatter_brane_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())
    }

    fn handle_reconnect<F: Fn() + Sync + lightlike + 'static>(&self, f: F) {
        self.leader_client.on_reconnect(Box::new(f))
    }

    fn get_gc_safe_point(&self) -> FidelFuture<u64> {
        let timer = Instant::now();

        let mut req = fidel_timeshare::GetGcSafePointRequest::default();
        req.set_header(self.header());

        let executor = move |client: &RwLock<Inner>, req: fidel_timeshare::GetGcSafePointRequest| {
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            let handler = client
                .rl()
                .client_stub
                .get_gc_safe_point_async_opt(&req, option)
                .unwrap_or_else(|e| {
                    panic!("fail to request FIDel {} err {:?}", "get_gc_saft_point", e)
                });
            Box::pin(async move {
                let resp = handler.await?;
                FIDel_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_gc_safe_point"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp.get_safe_point())
            }) as FidelFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_store_stats(&self, store_id: u64) -> Result<fidel_timeshare::StoreStats> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_store"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::GetStoreRequest::default();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_store_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        let store = resp.get_store();
        if store.get_state() != meta_timeshare::StoreState::Tombstone {
            Ok(resp.take_stats())
        } else {
            Err(Error::StoreTombstone(format!("{:?}", store)))
        }
    }

    fn get_operator(&self, brane_id: u64) -> Result<fidel_timeshare::GetOperatorResponse> {
        let _timer = FIDel_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_operator"])
            .spacelike_coarse_timer();

        let mut req = fidel_timeshare::GetOperatorRequest::default();
        req.set_header(self.header());
        req.set_brane_id(brane_id);

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_operator_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp)
    }
    // TODO: The current implementation is not efficient, because it creates
    //       a RPC for every `FidelFuture<TimeStamp>`. As a duplex streaming RPC,
    //       we could use one RPC for many `FidelFuture<TimeStamp>`.
    fn get_tso(&self) -> FidelFuture<TimeStamp> {
        let timer = Instant::now();

        let mut req = fidel_timeshare::TsoRequest::default();
        req.set_count(1);
        req.set_header(self.header());
        let executor = move |client: &RwLock<Inner>, req: fidel_timeshare::TsoRequest| {
            let cli = client.read().unwrap();
            let (mut req_sink, mut resp_stream) = cli
                .client_stub
                .tso()
                .unwrap_or_else(|e| panic!("fail to request FIDel {} err {:?}", "tso", e));
            let lightlike_once = async move {
                req_sink.lightlike((req, WriteFlags::default())).await?;
                req_sink.close().await?;
                GrpcResult::Ok(())
            }
            .map(|_| ());
            cli.client_stub.spawn(lightlike_once);
            Box::pin(async move {
                let resp = resp_stream.try_next().await?;
                let resp = match resp {
                    Some(r) => r,
                    None => return Ok(TimeStamp::zero()),
                };
                FIDel_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["tso"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                let ts = resp.get_timestamp();
                let encoded = TimeStamp::compose(ts.physical as _, ts.logical as _);
                Ok(encoded)
            }) as FidelFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }
}

pub struct DummyFidelClient {
    pub next_ts: TimeStamp,
}

impl DummyFidelClient {
    pub fn new() -> DummyFidelClient {
        DummyFidelClient {
            next_ts: TimeStamp::zero(),
        }
    }
}

impl FidelClient for DummyFidelClient {
    fn get_tso(&self) -> FidelFuture<TimeStamp> {
        Box::pin(future::ok(self.next_ts))
    }
}
