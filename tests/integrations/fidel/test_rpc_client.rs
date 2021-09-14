// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use futures::executor::block_on;
use grpcio::EnvBuilder;
use ekvproto::meta_timeshare;
use ekvproto::fidel_timeshare;
use tokio::runtime::Builder;

use fidel_client::{validate_lightlikepoints, Error as FidelError, FidelClient, BraneStat, RpcClient};
use violetabftstore::store;
use security::{SecurityConfig, SecurityManager};
use semver::Version;
use violetabftstore::interlock::::config::ReadableDuration;
use txn_types::TimeStamp;

use test_fidel::{mocker::*, util::*, Server as MockServer};

#[test]
fn test_retry_rpc_client() {
    let eps_count = 1;
    let mut server = MockServer::new(eps_count);
    let eps = server.bind_addrs();
    let m_eps = eps.clone();
    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let m_mgr = mgr.clone();
    server.stop();
    let child = thread::spawn(move || {
        let causet = new_config(m_eps);
        assert_eq!(RpcClient::new(&causet, m_mgr).is_ok(), true);
    });
    thread::sleep(Duration::from_millis(500));
    server.spacelike(&mgr, eps);
    assert_eq!(child.join().is_ok(), true);
}

#[test]
fn test_rpc_client() {
    let eps_count = 1;
    let server = MockServer::new(eps_count);
    let eps = server.bind_addrs();

    let client = new_client(eps.clone(), None);
    assert_ne!(client.get_cluster_id().unwrap(), 0);

    let store_id = client.alloc_id().unwrap();
    let mut store = meta_timeshare::CausetStore::default();
    store.set_id(store_id);
    debug!("bootstrap store {:?}", store);

    let peer_id = client.alloc_id().unwrap();
    let mut peer = meta_timeshare::Peer::default();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let brane_id = client.alloc_id().unwrap();
    let mut brane = meta_timeshare::Brane::default();
    brane.set_id(brane_id);
    brane.mut_peers().push(peer.clone());
    debug!("bootstrap brane {:?}", brane);

    client
        .bootstrap_cluster(store.clone(), brane.clone())
        .unwrap();
    assert_eq!(client.is_cluster_bootstrapped().unwrap(), true);

    let tmp_stores = client.get_all_stores(false).unwrap();
    assert_eq!(tmp_stores.len(), 1);
    assert_eq!(tmp_stores[0], store);

    let tmp_store = client.get_store(store_id).unwrap();
    assert_eq!(tmp_store.get_id(), store.get_id());

    let brane_key = brane.get_spacelike_key();
    let tmp_brane = client.get_brane(brane_key).unwrap();
    assert_eq!(tmp_brane.get_id(), brane.get_id());

    let brane_info = client.get_brane_info(brane_key).unwrap();
    assert_eq!(brane_info.brane, brane);
    assert_eq!(brane_info.leader, None);

    let tmp_brane = block_on(client.get_brane_by_id(brane_id))
        .unwrap()
        .unwrap();
    assert_eq!(tmp_brane.get_id(), brane.get_id());

    let ts = block_on(client.get_tso()).unwrap();
    assert_ne!(ts, TimeStamp::zero());

    let mut prev_id = 0;
    for _ in 0..100 {
        let client = new_client(eps.clone(), None);
        let alloc_id = client.alloc_id().unwrap();
        assert!(alloc_id > prev_id);
        prev_id = alloc_id;
    }

    let poller = Builder::new()
        .threaded_interlock_semaphore()
        .thread_name(thd_name!("poller"))
        .core_threads(1)
        .build()
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let f = client.handle_brane_heartbeat_response(1, move |resp| {
        let _ = tx.lightlike(resp);
    });
    poller.spawn(f);
    poller.spawn(client.brane_heartbeat(
        store::VIOLETABFT_INIT_LOG_TERM,
        brane.clone(),
        peer.clone(),
        BraneStat::default(),
        None,
    ));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();

    let brane_info = client.get_brane_info(brane_key).unwrap();
    assert_eq!(brane_info.brane, brane);
    assert_eq!(brane_info.leader.unwrap(), peer);

    block_on(client.store_heartbeat(fidel_timeshare::StoreStats::default())).unwrap();
    block_on(client.ask_batch_split(meta_timeshare::Brane::default(), 1)).unwrap();
    block_on(client.report_batch_split(vec![meta_timeshare::Brane::default(), meta_timeshare::Brane::default()]))
        .unwrap();

    let brane_info = client.get_brane_info(brane_key).unwrap();
    client.scatter_brane(brane_info).unwrap();
}

#[test]
fn test_get_tombstone_stores() {
    let eps_count = 1;
    let server = MockServer::new(eps_count);
    let eps = server.bind_addrs();
    let client = new_client(eps, None);

    let mut all_stores = vec![];
    let store_id = client.alloc_id().unwrap();
    let mut store = meta_timeshare::CausetStore::default();
    store.set_id(store_id);
    let brane_id = client.alloc_id().unwrap();
    let mut brane = meta_timeshare::Brane::default();
    brane.set_id(brane_id);
    client.bootstrap_cluster(store.clone(), brane).unwrap();

    all_stores.push(store);
    assert_eq!(client.is_cluster_bootstrapped().unwrap(), true);
    let s = client.get_all_stores(false).unwrap();
    assert_eq!(s, all_stores);

    // Add tombstone store.
    let mut store99 = meta_timeshare::CausetStore::default();
    store99.set_id(99);
    store99.set_state(meta_timeshare::StoreState::Tombstone);
    server.default_handler().add_store(store99.clone());

    // do not include tombstone.
    let s = client.get_all_stores(true).unwrap();
    assert_eq!(s, all_stores);

    all_stores.push(store99.clone());
    all_stores.sort_by(|a, b| a.get_id().cmp(&b.get_id()));
    // include tombstone, there should be 2 stores.
    let mut s = client.get_all_stores(false).unwrap();
    s.sort_by(|a, b| a.get_id().cmp(&b.get_id()));
    assert_eq!(s, all_stores);

    // Add another tombstone store.
    let mut store199 = store99;
    store199.set_id(199);
    server.default_handler().add_store(store199.clone());

    all_stores.push(store199);
    all_stores.sort_by(|a, b| a.get_id().cmp(&b.get_id()));
    let mut s = client.get_all_stores(false).unwrap();
    s.sort_by(|a, b| a.get_id().cmp(&b.get_id()));
    assert_eq!(s, all_stores);

    client.get_store(store_id).unwrap();
    client.get_store(99).unwrap_err();
    client.get_store(199).unwrap_err();
}

#[test]
fn test_reboot() {
    let eps_count = 1;
    let server = MockServer::with_case(eps_count, Arc::new(AlreadyBootstrapped));
    let eps = server.bind_addrs();
    let client = new_client(eps, None);

    assert!(!client.is_cluster_bootstrapped().unwrap());

    match client.bootstrap_cluster(meta_timeshare::CausetStore::default(), meta_timeshare::Brane::default()) {
        Err(FidelError::ClusterBootstrapped(_)) => (),
        _ => {
            panic!("failed, should return ClusterBootstrapped");
        }
    }
}

#[test]
fn test_validate_lightlikepoints() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(Split::new()));
    let env = Arc::new(
        EnvBuilder::new()
            .cq_count(1)
            .name_prefix(thd_name!("test-fidel"))
            .build(),
    );
    let eps = server.bind_addrs();

    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    assert!(validate_lightlikepoints(env, &new_config(eps), mgr.clone()).is_err());
}

fn test_retry<F: Fn(&RpcClient)>(func: F) {
    let eps_count = 1;
    // Retry mocker returns `Err(_)` for most request, here two thirds are `Err(_)`.
    let retry = Arc::new(Retry::new(3));
    let server = MockServer::with_case(eps_count, retry);
    let eps = server.bind_addrs();

    let client = new_client(eps, None);

    for _ in 0..3 {
        func(&client);
    }
}

#[test]
fn test_retry_async() {
    let r#async = |client: &RpcClient| {
        block_on(client.get_brane_by_id(1)).unwrap();
    };
    test_retry(r#async);
}

#[test]
fn test_retry_sync() {
    let sync = |client: &RpcClient| {
        client.get_store(1).unwrap();
    };
    test_retry(sync)
}

fn test_not_retry<F: Fn(&RpcClient)>(func: F) {
    let eps_count = 1;
    // NotRetry mocker returns Ok() with error header first, and next returns Ok() without any error header.
    let not_retry = Arc::new(NotRetry::new());
    let server = MockServer::with_case(eps_count, not_retry);
    let eps = server.bind_addrs();

    let client = new_client(eps, None);

    func(&client);
}

#[test]
fn test_not_retry_async() {
    let r#async = |client: &RpcClient| {
        block_on(client.get_brane_by_id(1)).unwrap_err();
    };
    test_not_retry(r#async);
}

#[test]
fn test_not_retry_sync() {
    let sync = |client: &RpcClient| {
        client.get_store(1).unwrap_err();
    };
    test_not_retry(sync);
}

#[test]
fn test_incompatible_version() {
    let incompatible = Arc::new(Incompatible);
    let server = MockServer::with_case(1, incompatible);
    let eps = server.bind_addrs();

    let client = new_client(eps, None);

    let resp = block_on(client.ask_batch_split(meta_timeshare::Brane::default(), 2));
    assert_eq!(
        resp.unwrap_err().to_string(),
        FidelError::Incompatible.to_string()
    );
}

fn respacelike_leader(mgr: SecurityManager) {
    let mgr = Arc::new(mgr);
    // Service has only one GetMembersResponse, so the leader never changes.
    let mut server =
        MockServer::<Service>::with_configuration(&mgr, vec![("127.0.0.1".to_owned(), 0); 3], None);
    let eps = server.bind_addrs();

    let client = new_client(eps.clone(), Some(Arc::clone(&mgr)));
    // Put a brane.
    let store_id = client.alloc_id().unwrap();
    let mut store = meta_timeshare::CausetStore::default();
    store.set_id(store_id);

    let peer_id = client.alloc_id().unwrap();
    let mut peer = meta_timeshare::Peer::default();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let brane_id = client.alloc_id().unwrap();
    let mut brane = meta_timeshare::Brane::default();
    brane.set_id(brane_id);
    brane.mut_peers().push(peer);
    client.bootstrap_cluster(store, brane.clone()).unwrap();

    let brane = block_on(client.get_brane_by_id(brane.get_id()))
        .unwrap()
        .unwrap();

    // Stop servers and respacelike them again.
    server.stop();
    server.spacelike(&mgr, eps);

    // RECONNECT_INTERVAL_SEC is 1s.
    thread::sleep(Duration::from_secs(1));

    let brane = block_on(client.get_brane_by_id(brane.get_id())).unwrap();
    assert_eq!(brane.unwrap().get_id(), brane_id);
}

#[test]
fn test_respacelike_leader_insecure() {
    let mgr = SecurityManager::new(&SecurityConfig::default()).unwrap();
    respacelike_leader(mgr)
}

#[test]
fn test_respacelike_leader_secure() {
    let security_causet = test_util::new_security_causet(None);
    let mgr = SecurityManager::new(&security_causet).unwrap();
    respacelike_leader(mgr)
}

#[test]
fn test_change_leader_async() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(LeaderChange::new()));
    let eps = server.bind_addrs();

    let counter = Arc::new(AtomicUsize::new(0));
    let client = new_client(eps, None);
    let counter1 = Arc::clone(&counter);
    client.handle_reconnect(move || {
        counter1.fetch_add(1, Ordering::SeqCst);
    });
    let leader = client.get_leader();

    for _ in 0..5 {
        let brane = block_on(client.get_brane_by_id(1));
        brane.ok();

        let new = client.get_leader();
        if new != leader {
            assert!(counter.load(Ordering::SeqCst) >= 1);
            return;
        }
        thread::sleep(LeaderChange::get_leader_interval());
    }

    panic!("failed, leader should changed");
}

#[test]
fn test_brane_heartbeat_on_leader_change() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(LeaderChange::new()));
    let eps = server.bind_addrs();

    let client = new_client(eps, None);
    let poller = Builder::new()
        .threaded_interlock_semaphore()
        .thread_name(thd_name!("poller"))
        .core_threads(1)
        .build()
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let f = client.handle_brane_heartbeat_response(1, move |resp| {
        tx.lightlike(resp).unwrap();
    });
    poller.spawn(f);
    let brane = meta_timeshare::Brane::default();
    let peer = meta_timeshare::Peer::default();
    let stat = BraneStat::default();
    poller.spawn(client.brane_heartbeat(
        store::VIOLETABFT_INIT_LOG_TERM,
        brane.clone(),
        peer.clone(),
        stat.clone(),
        None,
    ));
    rx.recv_timeout(LeaderChange::get_leader_interval())
        .unwrap();

    let heartbeat_on_leader_change = |count| {
        let mut leader = client.get_leader();
        for _ in 0..count {
            loop {
                let _ = block_on(client.get_brane_by_id(1));
                let new = client.get_leader();
                if leader != new {
                    leader = new;
                    info!("leader changed!");
                    break;
                }
                thread::sleep(LeaderChange::get_leader_interval());
            }
        }
        poller.spawn(client.brane_heartbeat(
            store::VIOLETABFT_INIT_LOG_TERM,
            brane.clone(),
            peer.clone(),
            stat.clone(),
            None,
        ));
        rx.recv_timeout(LeaderChange::get_leader_interval())
            .unwrap();
    };

    // Change FIDel leader once then heartbeat FIDel.
    heartbeat_on_leader_change(1);

    // Change FIDel leader twice without fidelio the heartbeat lightlikeer, then heartbeat FIDel.
    heartbeat_on_leader_change(2);
}

#[test]
fn test_periodical_fidelio() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(LeaderChange::new()));
    let eps = server.bind_addrs();

    let counter = Arc::new(AtomicUsize::new(0));
    let client = new_client_with_fidelio_interval(eps, None, ReadableDuration::secs(3));
    let counter1 = Arc::clone(&counter);
    client.handle_reconnect(move || {
        counter1.fetch_add(1, Ordering::SeqCst);
    });
    let leader = client.get_leader();

    for _ in 0..5 {
        let new = client.get_leader();
        if new != leader {
            assert!(counter.load(Ordering::SeqCst) >= 1);
            return;
        }
        thread::sleep(LeaderChange::get_leader_interval());
    }

    panic!("failed, leader should changed");
}

#[test]
fn test_cluster_version() {
    let server = MockServer::<Service>::new(3);
    let eps = server.bind_addrs();

    let client = new_client(eps, None);
    let cluster_version = client.cluster_version();
    assert!(cluster_version.get().is_none());

    let emit_heartbeat = || {
        let req = fidel_timeshare::StoreStats::default();
        block_on(client.store_heartbeat(req)).unwrap();
    };

    let set_cluster_version = |version: &str| {
        let h = server.default_handler();
        h.set_cluster_version(version.to_owned());
    };

    // Empty version string will be treated as invalid.
    emit_heartbeat();
    assert!(cluster_version.get().is_none());

    // Explicitly invalid version string.
    set_cluster_version("invalid-version");
    emit_heartbeat();
    assert!(cluster_version.get().is_none());

    let v_500 = Version::parse("5.0.0").unwrap();
    let v_501 = Version::parse("5.0.1").unwrap();

    // Correct version string.
    set_cluster_version("5.0.0");
    emit_heartbeat();
    assert_eq!(cluster_version.get().unwrap(), v_500,);

    // Version can't go backwards.
    set_cluster_version("4.99");
    emit_heartbeat();
    assert_eq!(cluster_version.get().unwrap(), v_500,);

    // After reconnect the version should be still accessable.
    client.reconnect().unwrap();
    assert_eq!(cluster_version.get().unwrap(), v_500,);

    // Version can go forwards.
    set_cluster_version("5.0.1");
    emit_heartbeat();
    assert_eq!(cluster_version.get().unwrap(), v_501);
}
