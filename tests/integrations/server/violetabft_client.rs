//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{thread, time};

use futures::{FutureExt, StreamExt, TryStreamExt};
use grpcio::{
    ClientStreamingSink, Environment, RequestStream, RpcContext, RpcStatus, RpcStatusCode, Server,
};
use ekvproto::violetabft_server_timeshare::{Done, VioletaBftMessage};
use ekvproto::edb_timeshare::BatchVioletaBftMessage;
use violetabft::evioletabft_timeshare::Entry;
use violetabftstore::router::VioletaBftStoreBlackHole;
use security::{SecurityConfig, SecurityManager};
use edb::server::{load_statistics::ThreadLoad, Config, VioletaBftClient};

use super::{mock_kv_service, MockKv, MockKvService};

pub fn get_violetabft_client(pool: &tokio::runtime::Runtime) -> VioletaBftClient<VioletaBftStoreBlackHole> {
    let env = Arc::new(Environment::new(2));
    let causet = Arc::new(Config::default());
    let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let grpc_thread_load = Arc::new(ThreadLoad::with_memory_barrier(1000));
    VioletaBftClient::new(
        env,
        causet,
        security_mgr,
        VioletaBftStoreBlackHole,
        grpc_thread_load,
        Some(pool.handle().clone()),
    )
}

#[derive(Clone)]
struct MockKvForVioletaBft {
    msg_count: Arc<AtomicUsize>,
    batch_msg_count: Arc<AtomicUsize>,
    allow_batch: bool,
}

impl MockKvForVioletaBft {
    fn new(
        msg_count: Arc<AtomicUsize>,
        batch_msg_count: Arc<AtomicUsize>,
        allow_batch: bool,
    ) -> Self {
        MockKvForVioletaBft {
            msg_count,
            batch_msg_count,
            allow_batch,
        }
    }
}

impl MockKvService for MockKvForVioletaBft {
    fn violetabft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<VioletaBftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        let counter = Arc::clone(&self.msg_count);
        ctx.spawn(async move {
            stream
                .for_each(move |_| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    futures::future::ready(())
                })
                .await;
            drop(sink);
        });
    }

    fn batch_violetabft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BatchVioletaBftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        if !self.allow_batch {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map(|_| ()));
            return;
        }
        let msg_count = Arc::clone(&self.msg_count);
        let batch_msg_count = Arc::clone(&self.batch_msg_count);
        ctx.spawn(async move {
            stream
                .try_for_each(move |msgs| {
                    batch_msg_count.fetch_add(1, Ordering::SeqCst);
                    msg_count.fetch_add(msgs.msgs.len(), Ordering::SeqCst);
                    futures::future::ok(())
                })
                .await
                .unwrap();
            drop(sink);
        });
    }
}

#[test]
fn test_batch_violetabft_fallback() {
    let pool = tokio::runtime::Builder::new()
        .threaded_interlock_semaphore()
        .core_threads(1)
        .build()
        .unwrap();
    let mut violetabft_client = get_violetabft_client(&pool);

    let msg_count = Arc::new(AtomicUsize::new(0));
    let batch_msg_count = Arc::new(AtomicUsize::new(0));
    let service = MockKvForVioletaBft::new(Arc::clone(&msg_count), Arc::clone(&batch_msg_count), false);
    let (mock_server, port) = create_mock_server(service, 60000, 60100).unwrap();

    let addr = format!("localhost:{}", port);
    (0..100).for_each(|_| {
        violetabft_client.lightlike(1, &addr, VioletaBftMessage::default()).unwrap();
        thread::sleep(time::Duration::from_millis(10));
        violetabft_client.flush();
    });

    assert!(msg_count.load(Ordering::SeqCst) > 0);
    assert_eq!(batch_msg_count.load(Ordering::SeqCst), 0);
    pool.shutdown_timeout(time::Duration::from_secs(1));
    drop(mock_server)
}

#[test]
// Test violetabft_client auto reconnect to servers after connection break.
fn test_violetabft_client_reconnect() {
    let pool = tokio::runtime::Builder::new()
        .threaded_interlock_semaphore()
        .core_threads(1)
        .build()
        .unwrap();
    let mut violetabft_client = get_violetabft_client(&pool);

    let msg_count = Arc::new(AtomicUsize::new(0));
    let batch_msg_count = Arc::new(AtomicUsize::new(0));
    let service = MockKvForVioletaBft::new(Arc::clone(&msg_count), Arc::clone(&batch_msg_count), true);
    let (mock_server, port) = create_mock_server(service, 60100, 60200).unwrap();

    // `lightlike` should success.
    let addr = format!("localhost:{}", port);
    (0..50).for_each(|_| violetabft_client.lightlike(1, &addr, VioletaBftMessage::default()).unwrap());
    violetabft_client.flush();

    check_msg_count(500, &msg_count, 50);

    // `lightlike` should fail after the mock server stopped.
    drop(mock_server);

    let lightlike = |_| {
        thread::sleep(time::Duration::from_millis(10));
        violetabft_client.lightlike(1, &addr, VioletaBftMessage::default())
    };
    assert!((0..100).map(lightlike).collect::<Result<(), _>>().is_err());

    // `lightlike` should success after the mock server respacelikeed.
    let service = MockKvForVioletaBft::new(Arc::clone(&msg_count), batch_msg_count, true);
    let mock_server = create_mock_server_on(service, port);
    (0..50).for_each(|_| violetabft_client.lightlike(1, &addr, VioletaBftMessage::default()).unwrap());
    violetabft_client.flush();

    check_msg_count(500, &msg_count, 100);

    drop(mock_server);
    pool.shutdown_timeout(time::Duration::from_secs(1));
}

#[test]
fn test_batch_size_limit() {
    let pool = tokio::runtime::Builder::new()
        .threaded_interlock_semaphore()
        .core_threads(1)
        .build()
        .unwrap();
    let mut violetabft_client = get_violetabft_client(&pool);

    let msg_count = Arc::new(AtomicUsize::new(0));
    let batch_msg_count = Arc::new(AtomicUsize::new(0));
    let service = MockKvForVioletaBft::new(Arc::clone(&msg_count), Arc::clone(&batch_msg_count), true);
    let (mock_server, port) = create_mock_server(service, 60200, 60300).unwrap();

    let addr = format!("localhost:{}", port);

    // `lightlike` should success.
    for _ in 0..10 {
        // 5M per VioletaBftMessage.
        let mut violetabft_m = VioletaBftMessage::default();
        for _ in 0..(5 * 1024) {
            let mut e = Entry::default();
            e.set_data(vec![b'a'; 1024]);
            violetabft_m.mut_message().mut_entries().push(e);
        }
        violetabft_client.lightlike(1, &addr, violetabft_m).unwrap();
    }
    violetabft_client.flush();

    check_msg_count(500, &msg_count, 10);
    // The final received message count should be 10 exactly.
    drop(violetabft_client);
    drop(mock_server);
    assert_eq!(msg_count.load(Ordering::SeqCst), 10);
}

// Try to create a mock server with `service`. The server will be binded wiht a random
// port chosen between [`min_port`, `max_port`]. Return `None` if no port is available.
fn create_mock_server<T>(service: T, min_port: u16, max_port: u16) -> Option<(Server, u16)>
where
    T: MockKvService + Clone + lightlike + 'static,
{
    for port in min_port..max_port {
        let kv = MockKv(service.clone());
        let mut mock_server = match mock_kv_service(kv, "localhost", port) {
            Ok(s) => s,
            Err(_) => continue,
        };
        mock_server.spacelike();
        return Some((mock_server, port));
    }
    None
}

// Try to create a mock server with `service` and bind it with `port`.
// Return `None` is the port is unavailable.
fn create_mock_server_on<T>(service: T, port: u16) -> Option<Server>
where
    T: MockKvService + Clone + lightlike + 'static,
{
    let mut mock_server = match mock_kv_service(MockKv(service), "localhost", port) {
        Ok(s) => s,
        Err(_) => return None,
    };
    mock_server.spacelike();
    Some(mock_server)
}

fn check_msg_count(max_delay_ms: u64, count: &AtomicUsize, expected: usize) {
    let mut got = 0;
    for _delay_ms in 0..max_delay_ms / 10 {
        got = count.load(Ordering::SeqCst);
        if got == expected {
            return;
        }
        thread::sleep(time::Duration::from_millis(10));
    }
    panic!("check_msg_count wants {}, gets {}", expected, got);
}
