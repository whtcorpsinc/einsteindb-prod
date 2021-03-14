//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use async_stream::stream;
use engine_promises::KvEngine;
use futures::compat::Compat01As03;
use futures::executor::block_on;
use futures::future::{ok, poll_fn};
use futures::prelude::*;
use hyper::client::HttpConnector;
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream};
use hyper::server::Builder as HyperBuilder;
use hyper::service::{make_service_fn, service_fn};
use hyper::{self, header, Body, Client, Method, Request, Response, Server, StatusCode, Uri};
use hyper_openssl::HttpsConnector;
use openssl::ssl::{
    SslAcceptor, SslConnector, SslConnectorBuilder, SslFiletype, SslMethod, SslVerifyMode,
};
use openssl::x509::X509;
use pin_project::pin_project;
use pprof::protos::Message;
use violetabftstore::store::{transport::CasualRouter, CasualMessage};
use regex::Regex;
use serde_json::Value;
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot::{self, Receiver, Slightlikeer};
use tokio_openssl::SslStream;

use std::error::Error as StdError;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use super::Result;
use crate::config::ConfigController;
use configuration::Configuration;
use fidel_client::RpcClient;
use security::{self, SecurityConfig};
use einsteindb_alloc::error::ProfError;
use einsteindb_util::collections::HashMap;
use einsteindb_util::metrics::dump;
use einsteindb_util::timer::GLOBAL_TIMER_HANDLE;

pub mod brane_meta;

mod profiler_guard {
    use einsteindb_alloc::error::ProfResult;
    use einsteindb_alloc::{activate_prof, deactivate_prof};

    use tokio::sync::{Mutex, MutexGuard};

    lazy_static! {
        static ref PROFILER_MUTEX: Mutex<u32> = Mutex::new(0);
    }

    pub struct ProfGuard(MutexGuard<'static, u32>);

    pub async fn new_prof() -> ProfResult<ProfGuard> {
        let guard = PROFILER_MUTEX.dagger().await;
        match activate_prof() {
            Ok(_) => Ok(ProfGuard(guard)),
            Err(e) => Err(e),
        }
    }

    impl Drop for ProfGuard {
        fn drop(&mut self) {
            // TODO: handle error here
            let _ = deactivate_prof();
        }
    }
}

const COMPONENT_REQUEST_RETRY: usize = 5;

static COMPONENT: &str = "einsteindb";

#[causet(feature = "failpoints")]
static MISSING_NAME: &[u8] = b"Missing param name";
#[causet(feature = "failpoints")]
static MISSING_ACTIONS: &[u8] = b"Missing param actions";
#[causet(feature = "failpoints")]
static FAIL_POINTS_REQUEST_PATH: &str = "/fail";

pub struct StatusServer<E, R> {
    thread_pool: Runtime,
    tx: Slightlikeer<()>,
    rx: Option<Receiver<()>>,
    addr: Option<SocketAddr>,
    advertise_addr: Option<String>,
    fidel_client: Option<Arc<RpcClient>>,
    causet_controller: ConfigController,
    router: R,
    security_config: Arc<SecurityConfig>,
    _snap: PhantomData<E>,
}

impl StatusServer<(), ()> {
    fn extract_thread_name(thread_name: &str) -> String {
        lazy_static! {
            static ref THREAD_NAME_RE: Regex =
                Regex::new(r"^(?P<thread_name>[a-z-_ :]+?)(-?\d)*$").unwrap();
            static ref THREAD_NAME_REPLACE_SEPERATOR_RE: Regex = Regex::new(r"[_ ]").unwrap();
        }

        THREAD_NAME_RE
            .captures(thread_name)
            .and_then(|cap| {
                cap.name("thread_name").map(|thread_name| {
                    THREAD_NAME_REPLACE_SEPERATOR_RE
                        .replace_all(thread_name.as_str(), "-")
                        .into_owned()
                })
            })
            .unwrap_or_else(|| thread_name.to_owned())
    }

    fn frames_post_processor() -> impl Fn(&mut pprof::Frames) {
        move |frames| {
            let name = Self::extract_thread_name(&frames.thread_name);
            frames.thread_name = name;
        }
    }

    fn err_response<T>(status_code: StatusCode, message: T) -> Response<Body>
    where
        T: Into<Body>,
    {
        Response::builder()
            .status(status_code)
            .body(message.into())
            .unwrap()
    }
}

impl<E, R> StatusServer<E, R>
where
    E: 'static,
    R: 'static + Slightlike,
{
    pub fn new(
        status_thread_pool_size: usize,
        fidel_client: Option<Arc<RpcClient>>,
        causet_controller: ConfigController,
        security_config: Arc<SecurityConfig>,
        router: R,
    ) -> Result<Self> {
        let thread_pool = Builder::new()
            .threaded_interlock_semaphore()
            .enable_all()
            .core_threads(status_thread_pool_size)
            .thread_name("status-server")
            .on_thread_spacelike(|| {
                debug!("Status server spacelikeed");
            })
            .on_thread_stop(|| {
                debug!("stopping status server");
            })
            .build()?;
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        Ok(StatusServer {
            thread_pool,
            tx,
            rx: Some(rx),
            addr: None,
            advertise_addr: None,
            fidel_client,
            causet_controller,
            router,
            security_config,
            _snap: PhantomData,
        })
    }

    pub async fn dump_prof(seconds: u64) -> std::result::Result<Vec<u8>, ProfError> {
        let guard = profiler_guard::new_prof().await?;
        info!("spacelike memory profiling {} seconds", seconds);

        let timer = GLOBAL_TIMER_HANDLE.clone();
        let _ = Compat01As03::new(timer.delay(Instant::now() + Duration::from_secs(seconds))).await;
        let tmp_dir = TempDir::new()?;
        let os_path = tmp_dir.path().join("einsteindb_dump_profile").into_os_string();
        let path = os_path
            .into_string()
            .map_err(|path| ProfError::PathEncodingError(path))?;
        einsteindb_alloc::dump_prof(&path)?;
        drop(guard);
        let mut file = tokio::fs::File::open(path).await?;
        let mut buf = Vec::new();
        file.read_to_lightlike(&mut buf).await?;
        Ok(buf)
    }

    pub async fn dump_prof_to_resp(req: Request<Body>) -> hyper::Result<Response<Body>> {
        let query = match req.uri().query() {
            Some(query) => query,
            None => {
                return Ok(StatusServer::err_response(
                    StatusCode::BAD_REQUEST,
                    "request should have the query part",
                ));
            }
        };
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let seconds: u64 = match query_pairs.get("seconds") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(_) => {
                    return Ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        "request should have seconds argument",
                    ));
                }
            },
            None => 10,
        };

        match Self::dump_prof(seconds).await {
            Ok(buf) => {
                let response = Response::builder()
                    .header("X-Content-Type-Options", "nosniff")
                    .header("Content-Disposition", "attachment; filename=\"profile\"")
                    .header("Content-Type", mime::APPLICATION_OCTET_STREAM.to_string())
                    .header("Content-Length", buf.len())
                    .body(buf.into())
                    .unwrap();
                Ok(response)
            }
            Err(err) => Ok(StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            )),
        }
    }

    async fn get_config(
        req: Request<Body>,
        causet_controller: &ConfigController,
    ) -> hyper::Result<Response<Body>> {
        let mut full = false;
        if let Some(query) = req.uri().query() {
            let query_pairs: HashMap<_, _> =
                url::form_urlencoded::parse(query.as_bytes()).collect();
            full = match query_pairs.get("full") {
                Some(val) => match val.parse() {
                    Ok(val) => val,
                    Err(err) => {
                        return Ok(StatusServer::err_response(
                            StatusCode::BAD_REQUEST,
                            err.to_string(),
                        ));
                    }
                },
                None => false,
            };
        }
        let encode_res = if full {
            // Get all config
            serde_json::to_string(&causet_controller.get_current())
        } else {
            // Filter hidden config
            serde_json::to_string(&causet_controller.get_current().get_encoder())
        };
        Ok(match encode_res {
            Ok(json) => Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))
                .unwrap(),
            Err(_) => StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error",
            ),
        })
    }

    async fn ufidelate_config(
        causet_controller: ConfigController,
        req: Request<Body>,
    ) -> hyper::Result<Response<Body>> {
        let mut body = Vec::new();
        req.into_body()
            .try_for_each(|bytes| {
                body.extlightlike(bytes);
                ok(())
            })
            .await?;
        Ok(match decode_json(&body) {
            Ok(change) => match causet_controller.ufidelate(change) {
                Err(e) => StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to ufidelate, error: {:?}", e),
                ),
                Ok(_) => {
                    let mut resp = Response::default();
                    *resp.status_mut() = StatusCode::OK;
                    resp
                }
            },
            Err(e) => StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to decode, error: {:?}", e),
            ),
        })
    }

    pub async fn dump_rsprof(seconds: u64, frequency: i32) -> pprof::Result<pprof::Report> {
        let guard = pprof::ProfilerGuard::new(frequency)?;
        info!(
            "spacelike profiling {} seconds with frequency {} /s",
            seconds, frequency
        );
        let timer = GLOBAL_TIMER_HANDLE.clone();
        let _ = Compat01As03::new(timer.delay(Instant::now() + Duration::from_secs(seconds))).await;
        guard
            .report()
            .frames_post_processor(StatusServer::frames_post_processor())
            .build()
    }

    pub async fn dump_rsperf_to_resp(req: Request<Body>) -> hyper::Result<Response<Body>> {
        let query = match req.uri().query() {
            Some(query) => query,
            None => {
                return Ok(StatusServer::err_response(StatusCode::BAD_REQUEST, ""));
            }
        };
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let seconds: u64 = match query_pairs.get("seconds") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => {
                    return Ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        err.to_string(),
                    ));
                }
            },
            None => 10,
        };

        let frequency: i32 = match query_pairs.get("frequency") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => {
                    return Ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        err.to_string(),
                    ));
                }
            },
            None => 99, // Default frequency of sampling. 99Hz to avoid coincide with special periods
        };

        let prototype_content_type: hyper::http::HeaderValue =
            hyper::http::HeaderValue::from_str("application/protobuf").unwrap();
        let report = match Self::dump_rsprof(seconds, frequency).await {
            Ok(report) => report,
            Err(err) => {
                return Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string(),
                ))
            }
        };

        let mut body: Vec<u8> = Vec::new();
        if req.headers().get("Content-Type") == Some(&prototype_content_type) {
            match report.pprof() {
                Ok(profile) => match profile.encode(&mut body) {
                    Ok(()) => {
                        info!("write report successfully");
                        Ok(StatusServer::err_response(StatusCode::OK, body))
                    }
                    Err(err) => Ok(StatusServer::err_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        err.to_string(),
                    )),
                },
                Err(err) => Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string(),
                )),
            }
        } else {
            match report.flamegraph(&mut body) {
                Ok(_) => {
                    info!("write report successfully");
                    Ok(StatusServer::err_response(StatusCode::OK, body))
                }
                Err(err) => Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string(),
                )),
            }
        }
    }

    pub fn stop(mut self) {
        // unregister the status address to fidel
        self.unregister_addr();
        let _ = self.tx.slightlike(());
        self.thread_pool.shutdown_timeout(Duration::from_secs(10));
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.addr.unwrap()
    }

    // Conveniently generate ssl connector according to SecurityConfig for https client
    // Return `None` if SecurityConfig is not set up.
    fn generate_ssl_connector(&self) -> Option<SslConnectorBuilder> {
        if !self.security_config.cert_path.is_empty()
            && !self.security_config.key_path.is_empty()
            && !self.security_config.ca_path.is_empty()
        {
            let mut ssl = SslConnector::builder(SslMethod::tls()).unwrap();
            ssl.set_ca_file(&self.security_config.ca_path).unwrap();
            ssl.set_certificate_file(&self.security_config.cert_path, SslFiletype::PEM)
                .unwrap();
            ssl.set_private_key_file(&self.security_config.key_path, SslFiletype::PEM)
                .unwrap();
            Some(ssl)
        } else {
            None
        }
    }

    fn register_addr(&mut self, advertise_addr: String) {
        if let Some(ssl) = self.generate_ssl_connector() {
            let mut connector = HttpConnector::new();
            connector.enforce_http(false);
            let https_conn = HttpsConnector::with_connector(connector, ssl).unwrap();
            self.register_addr_core(https_conn, advertise_addr);
        } else {
            self.register_addr_core(HttpConnector::new(), advertise_addr);
        }
    }

    fn register_addr_core<C>(&mut self, conn: C, advertise_addr: String)
    where
        C: hyper::client::connect::Connect + Clone + Slightlike + Sync + 'static,
    {
        if self.fidel_client.is_none() {
            return;
        }
        let fidel_client = self.fidel_client.as_ref().unwrap();
        let client = Client::builder().build::<_, Body>(conn);

        let json = {
            let mut body = std::collections::HashMap::new();
            body.insert("component".to_owned(), COMPONENT.to_owned());
            body.insert("addr".to_owned(), advertise_addr.clone());
            serde_json::to_string(&body).unwrap()
        };

        for _ in 0..COMPONENT_REQUEST_RETRY {
            for fidel_addr in fidel_client.get_leader().get_client_urls() {
                let client = client.clone();
                let uri: Uri = (fidel_addr.to_owned() + "/fidel/api/v1/component")
                    .parse()
                    .unwrap();
                let req = Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json.clone()))
                    .expect("construct post request failed");
                let req_handle = self
                    .thread_pool
                    .spawn(async move { client.request(req).await });

                match block_on(req_handle).unwrap() {
                    Ok(resp) if resp.status() == StatusCode::OK => {
                        self.advertise_addr = Some(advertise_addr);
                        return;
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        warn!("failed to register addr to fidel"; "status code" => status.as_str(), "body" => ?resp.body());
                    }
                    Err(e) => warn!("failed to register addr to fidel"; "error" => ?e),
                }
            }
            // refresh the fidel leader
            if let Err(e) = fidel_client.reconnect() {
                warn!("failed to reconnect fidel client"; "err" => ?e);
            }
        }
        warn!(
            "failed to register addr to fidel after {} tries",
            COMPONENT_REQUEST_RETRY
        );
    }

    fn unregister_addr(&mut self) {
        if let Some(ssl) = self.generate_ssl_connector() {
            let mut connector = HttpConnector::new();
            connector.enforce_http(false);
            let https_conn = HttpsConnector::with_connector(connector, ssl).unwrap();
            self.unregister_addr_core(https_conn);
        } else {
            self.unregister_addr_core(HttpConnector::new());
        }
    }

    fn unregister_addr_core<C>(&mut self, conn: C)
    where
        C: hyper::client::connect::Connect + Clone + Slightlike + Sync + 'static,
    {
        if self.fidel_client.is_none() || self.advertise_addr.is_none() {
            return;
        }
        let advertise_addr = self.advertise_addr.as_ref().unwrap().to_owned();
        let fidel_client = self.fidel_client.as_ref().unwrap();
        let client = Client::builder().build::<_, Body>(conn);

        for _ in 0..COMPONENT_REQUEST_RETRY {
            for fidel_addr in fidel_client.get_leader().get_client_urls() {
                let client = client.clone();
                let uri: Uri = (fidel_addr.to_owned()
                    + &format!("/fidel/api/v1/component/{}/{}", COMPONENT, advertise_addr))
                    .parse()
                    .unwrap();
                let req = Request::delete(uri)
                    .body(Body::empty())
                    .expect("construct delete request failed");
                let req_handle = self
                    .thread_pool
                    .spawn(async move { client.request(req).await });

                match block_on(req_handle).unwrap() {
                    Ok(resp) if resp.status() == StatusCode::OK => {
                        self.advertise_addr = None;
                        return;
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        warn!("failed to unregister addr to fidel"; "status code" => status.as_str(), "body" => ?resp.body());
                    }
                    Err(e) => warn!("failed to unregister addr to fidel"; "error" => ?e),
                }
            }
            // refresh the fidel leader
            if let Err(e) = fidel_client.reconnect() {
                warn!("failed to reconnect fidel client"; "err" => ?e);
            }
        }
        warn!(
            "failed to unregister addr to fidel after {} tries",
            COMPONENT_REQUEST_RETRY
        );
    }
}

impl<E, R> StatusServer<E, R>
where
    E: KvEngine,
    R: 'static + Slightlike + CasualRouter<E> + Clone,
{
    pub async fn dump_brane_meta(req: Request<Body>, router: R) -> hyper::Result<Response<Body>> {
        lazy_static! {
            static ref REGION: Regex = Regex::new(r"/brane/(?P<id>\d+)").unwrap();
        }

        fn err_resp(
            status_code: StatusCode,
            msg: impl Into<Body>,
        ) -> hyper::Result<Response<Body>> {
            Ok(StatusServer::err_response(status_code, msg))
        }

        fn not_found(msg: impl Into<Body>) -> hyper::Result<Response<Body>> {
            err_resp(StatusCode::NOT_FOUND, msg)
        }

        let cap = match REGION.captures(req.uri().path()) {
            Some(cap) => cap,
            None => return not_found(format!("path {} not found", req.uri().path())),
        };

        let id: u64 = match cap["id"].parse() {
            Ok(id) => id,
            Err(err) => {
                return err_resp(
                    StatusCode::BAD_REQUEST,
                    format!("invalid brane id: {}", err),
                );
            }
        };
        let (tx, rx) = oneshot::channel();
        match router.slightlike(
            id,
            CasualMessage::AccessPeer(Box::new(move |peer| {
                if let Err(meta) = tx.slightlike(brane_meta::BraneMeta::new(peer)) {
                    error!("receiver dropped, brane meta: {:?}", meta)
                }
            })),
        ) {
            Ok(_) => (),
            Err(violetabftstore::Error::BraneNotFound(_)) => {
                return not_found(format!("brane({}) not found", id));
            }
            Err(err) => {
                return err_resp(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("channel plightlikeing or disconnect: {}", err),
                );
            }
        }

        let meta = match rx.await {
            Ok(meta) => meta,
            Err(_) => {
                return Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "query cancelled",
                ))
            }
        };

        let body = match serde_json::to_vec(&meta) {
            Ok(body) => body,
            Err(err) => {
                return Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("fails to json: {}", err),
                ))
            }
        };
        match Response::builder()
            .header("content-type", "application/json")
            .body(hyper::Body::from(body))
        {
            Ok(resp) => Ok(resp),
            Err(err) => Ok(StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("fails to build response: {}", err),
            )),
        }
    }

    fn spacelike_serve<I, C>(&mut self, builder: HyperBuilder<I>)
    where
        I: Accept<Conn = C, Error = std::io::Error> + Slightlike + 'static,
        I::Error: Into<Box<dyn StdError + Slightlike + Sync>>,
        I::Conn: AsyncRead + AsyncWrite + Unpin + Slightlike + 'static,
        C: ServerConnection,
    {
        let security_config = self.security_config.clone();
        let causet_controller = self.causet_controller.clone();
        let router = self.router.clone();
        // Start to serve.
        let server = builder.serve(make_service_fn(move |conn: &C| {
            let x509 = conn.get_x509();
            let security_config = security_config.clone();
            let causet_controller = causet_controller.clone();
            let router = router.clone();
            async move {
                // Create a status service.
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                    let x509 = x509.clone();
                    let security_config = security_config.clone();
                    let causet_controller = causet_controller.clone();
                    let router = router.clone();
                    async move {
                        let path = req.uri().path().to_owned();
                        let method = req.method().to_owned();

                        #[causet(feature = "failpoints")]
                        {
                            if path.spacelikes_with(FAIL_POINTS_REQUEST_PATH) {
                                return handle_fail_points_request(req).await;
                            }
                        }

                        let should_check_cert = match (&method, path.as_ref()) {
                            (&Method::GET, "/metrics") => false,
                            (&Method::GET, "/status") => false,
                            (&Method::GET, "/config") => false,
                            (&Method::GET, "/debug/pprof/profile") => false,
                            // 1. POST "/config" will modify the configuration of EinsteinDB.
                            // 2. GET "/brane" will get spacelike key and lightlike key. These tuplespaceInstanton could be actual
                            // user data since in some cases the data itself is stored in the key.
                            _ => true,
                        };

                        if should_check_cert {
                            if !check_cert(security_config, x509) {
                                return Ok(StatusServer::err_response(
                                    StatusCode::FORBIDDEN,
                                    "certificate role error",
                                ));
                            }
                        }

                        match (method, path.as_ref()) {
                            (Method::GET, "/metrics") => Ok(Response::new(dump().into())),
                            (Method::GET, "/status") => Ok(Response::default()),
                            (Method::GET, "/debug/pprof/heap") => {
                                Self::dump_prof_to_resp(req).await
                            }
                            (Method::GET, "/config") => {
                                Self::get_config(req, &causet_controller).await
                            }
                            (Method::POST, "/config") => {
                                Self::ufidelate_config(causet_controller.clone(), req).await
                            }
                            (Method::GET, "/debug/pprof/profile") => {
                                Self::dump_rsperf_to_resp(req).await
                            }
                            (Method::GET, path) if path.spacelikes_with("/brane") => {
                                Self::dump_brane_meta(req, router).await
                            }
                            _ => Ok(StatusServer::err_response(
                                StatusCode::NOT_FOUND,
                                "path not found",
                            )),
                        }
                    }
                }))
            }
        }));

        let rx = self.rx.take().unwrap();
        let graceful = server
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .map_err(|e| error!("Status server error: {:?}", e));
        self.thread_pool.spawn(graceful);
    }

    pub fn spacelike(&mut self, status_addr: String, advertise_status_addr: String) -> Result<()> {
        let addr = SocketAddr::from_str(&status_addr)?;

        let incoming = self.thread_pool.enter(|| AddrIncoming::bind(&addr))?;
        self.addr = Some(incoming.local_addr());
        if !self.security_config.cert_path.is_empty()
            && !self.security_config.key_path.is_empty()
            && !self.security_config.ca_path.is_empty()
        {
            let mut acceptor = SslAcceptor::mozilla_modern(SslMethod::tls())?;
            acceptor.set_ca_file(&self.security_config.ca_path)?;
            acceptor.set_certificate_chain_file(&self.security_config.cert_path)?;
            acceptor.set_private_key_file(&self.security_config.key_path, SslFiletype::PEM)?;
            if !self.security_config.cert_allowed_cn.is_empty() {
                acceptor.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
            }
            let acceptor = acceptor.build();
            let tls_incoming = tls_incoming(acceptor, incoming);
            let server = Server::builder(tls_incoming);
            self.spacelike_serve(server);
        } else {
            let server = Server::builder(incoming);
            self.spacelike_serve(server);
        }
        // register the advertise status address to fidel
        self.register_addr(advertise_status_addr);
        Ok(())
    }
}

// To unify TLS/Plain connection usage in spacelike_serve function
trait ServerConnection {
    fn get_x509(&self) -> Option<X509>;
}

impl ServerConnection for SslStream<AddrStream> {
    fn get_x509(&self) -> Option<X509> {
        self.ssl().peer_certificate()
    }
}

impl ServerConnection for AddrStream {
    fn get_x509(&self) -> Option<X509> {
        None
    }
}

// Check if the peer's x509 certificate meets the requirements, this should
// be called where the access should be controlled.
//
// For now, the check only verifies the role of the peer certificate.
fn check_cert(security_config: Arc<SecurityConfig>, cert: Option<X509>) -> bool {
    // if `cert_allowed_cn` is empty, skip check and return true
    if !security_config.cert_allowed_cn.is_empty() {
        if let Some(x509) = cert {
            if let Some(name) = x509
                .subject_name()
                .entries_by_nid(openssl::nid::Nid::COMMONNAME)
                .next()
            {
                let data = name.data().as_slice();
                // Check common name in peer cert
                return security::match_peer_names(
                    &security_config.cert_allowed_cn,
                    std::str::from_utf8(data).unwrap(),
                );
            }
        }
        false
    } else {
        true
    }
}

fn tls_incoming(
    acceptor: SslAcceptor,
    mut incoming: AddrIncoming,
) -> impl Accept<Conn = SslStream<AddrStream>, Error = std::io::Error> {
    let s = stream! {
        loop {
            let stream = match poll_fn(|cx| Pin::new(&mut incoming).poll_accept(cx)).await {
                Some(Ok(stream)) => stream,
                Some(Err(e)) => {
                    yield Err(e);
                    continue;
                }
                None => break,
            };
            match tokio_openssl::accept(&acceptor, stream).await {
                Err(_) => {
                    error!("Status server error: TLS handshake error");
                    continue;
                },
                Ok(ssl_stream) => {
                    yield Ok(ssl_stream);
                },
            }
        }
    };
    TlsIncoming(s)
}

#[pin_project]
struct TlsIncoming<S>(#[pin] S);

impl<S> Accept for TlsIncoming<S>
where
    S: Stream<Item = std::io::Result<SslStream<AddrStream>>>,
{
    type Conn = SslStream<AddrStream>;
    type Error = std::io::Error;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<std::io::Result<Self::Conn>>> {
        self.project().0.poll_next(cx)
    }
}

// For handling fail points related requests
#[causet(feature = "failpoints")]
async fn handle_fail_points_request(req: Request<Body>) -> hyper::Result<Response<Body>> {
    let path = req.uri().path().to_owned();
    let method = req.method().to_owned();
    let fail_path = format!("{}/", FAIL_POINTS_REQUEST_PATH);
    let fail_path_has_sub_path: bool = path.spacelikes_with(&fail_path);

    match (method, fail_path_has_sub_path) {
        (Method::PUT, true) => {
            let mut buf = Vec::new();
            req.into_body()
                .try_for_each(|bytes| {
                    buf.extlightlike(bytes);
                    ok(())
                })
                .await?;
            let (_, name) = path.split_at(fail_path.len());
            if name.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_NAME.into())
                    .unwrap());
            };

            let actions = String::from_utf8(buf).unwrap_or_default();
            if actions.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_ACTIONS.into())
                    .unwrap());
            };

            if let Err(e) = fail::causet(name.to_owned(), &actions) {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(e.into())
                    .unwrap());
            }
            let body = format!("Added fail point with name: {}, actions: {}", name, actions);
            Ok(Response::new(body.into()))
        }
        (Method::DELETE, true) => {
            let (_, name) = path.split_at(fail_path.len());
            if name.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_NAME.into())
                    .unwrap());
            };

            fail::remove(name);
            let body = format!("Deleted fail point with name: {}", name);
            Ok(Response::new(body.into()))
        }
        (Method::GET, _) => {
            // In this scope the path must be like /fail...(/...), which spacelikes with FAIL_POINTS_REQUEST_PATH and may or may not have a sub path
            // Now we return 404 when path is neither /fail nor /fail/
            if path != FAIL_POINTS_REQUEST_PATH && path != fail_path {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap());
            }

            // From here path is either /fail or /fail/, return lists of fail points
            let list: Vec<String> = fail::list()
                .into_iter()
                .map(move |(name, actions)| format!("{}={}", name, actions))
                .collect();
            let list = list.join("\n");
            Ok(Response::new(list.into()))
        }
        _ => Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty())
            .unwrap()),
    }
}

// Decode different type of json value to string value
fn decode_json(
    data: &[u8],
) -> std::result::Result<std::collections::HashMap<String, String>, Box<dyn std::error::Error>> {
    let json: Value = serde_json::from_slice(data)?;
    if let Value::Object(map) = json {
        let mut dst = std::collections::HashMap::new();
        for (k, v) in map.into_iter() {
            let v = match v {
                Value::Bool(v) => format!("{}", v),
                Value::Number(v) => format!("{}", v),
                Value::String(v) => v,
                Value::Array(_) => return Err("array type are not supported".to_owned().into()),
                _ => return Err("wrong format".to_owned().into()),
            };
            dst.insert(k, v);
        }
        Ok(dst)
    } else {
        Err("wrong format".to_owned().into())
    }
}

#[causet(test)]
mod tests {
    use futures::executor::block_on;
    use futures::future::ok;
    use futures::prelude::*;
    use hyper::client::HttpConnector;
    use hyper::{Body, Client, Method, Request, StatusCode, Uri};
    use hyper_openssl::HttpsConnector;
    use openssl::ssl::SslFiletype;
    use openssl::ssl::{SslConnector, SslMethod};

    use std::env;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::config::{ConfigController, EINSTEINDBConfig};
    use crate::server::status_server::StatusServer;
    use configuration::Configuration;
    use engine_lmdb::LmdbEngine;
    use violetabftstore::store::transport::CasualRouter;
    use violetabftstore::store::CasualMessage;
    use security::SecurityConfig;
    use test_util::new_security_causet;
    use einsteindb_util::collections::HashSet;

    #[derive(Clone)]
    struct MockRouter;

    impl CasualRouter<LmdbEngine> for MockRouter {
        fn slightlike(&self, brane_id: u64, _: CasualMessage<LmdbEngine>) -> violetabftstore::Result<()> {
            Err(violetabftstore::Error::BraneNotFound(brane_id))
        }
    }

    #[test]
    fn test_status_service() {
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.spacelike(addr.clone(), addr);
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/metrics")
            .build()
            .unwrap();

        let handle = status_server.thread_pool.spawn(async move {
            let res = client.get(uri).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        });
        block_on(handle).unwrap();
        status_server.stop();
    }

    #[test]
    fn test_security_status_service_without_cn() {
        do_test_security_status_service(HashSet::default(), true);
    }

    #[test]
    fn test_security_status_service_with_cn() {
        let mut allowed_cn = HashSet::default();
        allowed_cn.insert("einsteindb-server".to_owned());
        do_test_security_status_service(allowed_cn, true);
    }

    #[test]
    fn test_security_status_service_with_cn_fail() {
        let mut allowed_cn = HashSet::default();
        allowed_cn.insert("invaild-cn".to_owned());
        do_test_security_status_service(allowed_cn, false);
    }

    #[test]
    fn test_config_lightlikepoint() {
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.spacelike(addr.clone(), addr);
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/config")
            .build()
            .unwrap();
        let handle = status_server.thread_pool.spawn(async move {
            let resp = client.get(uri).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let mut v = Vec::new();
            resp.into_body()
                .try_for_each(|bytes| {
                    v.extlightlike(bytes);
                    ok(())
                })
                .await
                .unwrap();
            let resp_json = String::from_utf8_lossy(&v).to_string();
            let causet = EINSTEINDBConfig::default();
            serde_json::to_string(&causet.get_encoder())
                .map(|causet_json| {
                    assert_eq!(resp_json, causet_json);
                })
                .expect("Could not convert EINSTEINDBConfig to string");
        });
        block_on(handle).unwrap();
        status_server.stop();
    }

    #[causet(feature = "failpoints")]
    #[test]
    fn test_status_service_fail_lightlikepoints() {
        let _guard = fail::FailScenario::setup();
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.spacelike(addr.clone(), addr);
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn(async move {
            // test add fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/test_fail_point_name")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("panic"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri;

            let res = client.request(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let list: Vec<String> = fail::list()
                .into_iter()
                .map(move |(name, actions)| format!("{}={}", name, actions))
                .collect();
            assert_eq!(list.len(), 1);
            let list = list.join(";");
            assert_eq!("test_fail_point_name=panic", list);

            // test add another fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/and_another_name")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("panic"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri;

            let res = client.request(req).await.unwrap();

            assert_eq!(res.status(), StatusCode::OK);

            let list: Vec<String> = fail::list()
                .into_iter()
                .map(move |(name, actions)| format!("{}={}", name, actions))
                .collect();
            assert_eq!(2, list.len());
            let list = list.join(";");
            assert!(list.contains("test_fail_point_name=panic"));
            assert!(list.contains("and_another_name=panic"));

            // test list fail points
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail")
                .build()
                .unwrap();
            let mut req = Request::default();
            *req.method_mut() = Method::GET;
            *req.uri_mut() = uri;

            let res = client.request(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let mut body = Vec::new();
            res.into_body()
                .try_for_each(|bytes| {
                    body.extlightlike(bytes);
                    ok(())
                })
                .await
                .unwrap();
            let body = String::from_utf8(body).unwrap();
            assert!(body.contains("test_fail_point_name=panic"));
            assert!(body.contains("and_another_name=panic"));

            // test delete fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/test_fail_point_name")
                .build()
                .unwrap();
            let mut req = Request::default();
            *req.method_mut() = Method::DELETE;
            *req.uri_mut() = uri;

            let res = client.request(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);

            let list: Vec<String> = fail::list()
                .into_iter()
                .map(move |(name, actions)| format!("{}={}", name, actions))
                .collect();
            assert_eq!(1, list.len());
            let list = list.join(";");
            assert_eq!("and_another_name=panic", list);
        });

        block_on(handle).unwrap();
        status_server.stop();
    }

    #[causet(feature = "failpoints")]
    #[test]
    fn test_status_service_fail_lightlikepoints_can_trigger_fails() {
        let _guard = fail::FailScenario::setup();
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.spacelike(addr.clone(), addr);
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn(async move {
            // test add fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/a_test_fail_name_nobody_else_is_using")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("return"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri;

            let res = client.request(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        });

        block_on(handle).unwrap();
        status_server.stop();

        let true_only_if_fail_point_triggered = || {
            fail_point!("a_test_fail_name_nobody_else_is_using", |_| { true });
            false
        };
        assert!(true_only_if_fail_point_triggered());
    }

    #[causet(not(feature = "failpoints"))]
    #[test]
    fn test_status_service_fail_lightlikepoints_should_give_404_when_failpoints_are_disable() {
        let _guard = fail::FailScenario::setup();
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.spacelike(addr.clone(), addr);
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn(async move {
            // test add fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/a_test_fail_name_nobody_else_is_using")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("panic"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri;

            let res = client.request(req).await.unwrap();
            // without feature "failpoints", this PUT lightlikepoint should return 404
            assert_eq!(res.status(), StatusCode::NOT_FOUND);
        });

        block_on(handle).unwrap();
        status_server.stop();
    }

    #[test]
    fn test_extract_thread_name() {
        assert_eq!(
            &StatusServer::extract_thread_name("test-name-1"),
            "test-name"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("grpc-server-5"),
            "grpc-server"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("lmdb:bg1000"),
            "lmdb:bg"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("violetabftstore-1-100"),
            "violetabftstore"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("snap slightlikeer1000"),
            "snap-slightlikeer"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("snap_slightlikeer1000"),
            "snap-slightlikeer"
        );
    }

    fn do_test_security_status_service(allowed_cn: HashSet<String>, expected: bool) {
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(new_security_causet(Some(allowed_cn))),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.spacelike(addr.clone(), addr);

        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        let mut ssl = SslConnector::builder(SslMethod::tls()).unwrap();
        ssl.set_certificate_file(
            format!(
                "{}",
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("components/test_util/data/server.pem")
                    .display()
            ),
            SslFiletype::PEM,
        )
        .unwrap();
        ssl.set_private_key_file(
            format!(
                "{}",
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("components/test_util/data/key.pem")
                    .display()
            ),
            SslFiletype::PEM,
        )
        .unwrap();
        ssl.set_ca_file(format!(
            "{}",
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("components/test_util/data/ca.pem")
                .display()
        ))
        .unwrap();

        let ssl = HttpsConnector::with_connector(connector, ssl).unwrap();
        let client = Client::builder().build::<_, Body>(ssl);

        let uri = Uri::builder()
            .scheme("https")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/brane")
            .build()
            .unwrap();

        if expected {
            let handle = status_server.thread_pool.spawn(async move {
                let res = client.get(uri).await.unwrap();
                assert_eq!(res.status(), StatusCode::NOT_FOUND);
            });
            block_on(handle).unwrap();
        } else {
            let handle = status_server.thread_pool.spawn(async move {
                let res = client.get(uri).await.unwrap();
                assert_eq!(res.status(), StatusCode::FORBIDDEN);
            });
            let _ = block_on(handle);
        }
        status_server.stop();
    }

    #[causet(feature = "mem-profiling")]
    #[test]
    #[ignore]
    fn test_pprof_heap_service() {
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.spacelike(addr.clone(), addr);
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/debug/pprof/heap?seconds=1")
            .build()
            .unwrap();
        let handle = status_server
            .thread_pool
            .spawn(async move { client.get(uri).await.unwrap() });
        let resp = block_on(handle).unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        status_server.stop();
    }

    #[test]
    fn test_pprof_profile_service() {
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.spacelike(addr.clone(), addr);
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/debug/pprof/profile?seconds=1&frequency=99")
            .build()
            .unwrap();
        let handle = status_server
            .thread_pool
            .spawn(async move { client.get(uri).await.unwrap() });
        let resp = block_on(handle).unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        status_server.stop();
    }
}