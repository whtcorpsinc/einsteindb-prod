// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use ekvproto::meta_timeshare::*;
use fidel_client::{FidelClient, BraneInfo, BraneStat, RpcClient};
use test_fidel::{mocker::*, util::*, Server as MockServer};
use violetabftstore::interlock::::config::ReadableDuration;

use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

fn new_test_server_and_client(
    fidelio_interval: ReadableDuration,
) -> (MockServer<Service>, RpcClient) {
    let server = MockServer::new(1);
    let eps = server.bind_addrs();
    let client = new_client_with_fidelio_interval(eps, None, fidelio_interval);
    (server, client)
}

macro_rules! request {
    ($client: ident => block_on($func: tt($($arg: expr),*))) => {
        (stringify!($func), {
            let client = $client.clone();
            Box::new(move || {
                let _ = futures::executor::block_on(client.$func($($arg),*));
            })
        })
    };
    ($client: ident => $func: tt($($arg: expr),*)) => {
        (stringify!($func), {
            let client = $client.clone();
            Box::new(move || {
                let _ = client.$func($($arg),*);
            })
        })
    };
}

#[test]
fn test_fidel_client_deadlock() {
    let (_server, client) = new_test_server_and_client(ReadableDuration::millis(100));
    let client = Arc::new(client);
    let leader_client_reconnect_fp = "leader_client_reconnect";

    // It contains all interfaces of FidelClient.
    let test_funcs: Vec<(_, Box<dyn FnOnce() + lightlike>)> = vec![
        request!(client => reconnect()),
        request!(client => get_cluster_id()),
        request!(client => bootstrap_cluster(CausetStore::default(), Brane::default())),
        request!(client => is_cluster_bootstrapped()),
        request!(client => alloc_id()),
        request!(client => put_store(CausetStore::default())),
        request!(client => get_store(0)),
        request!(client => get_all_stores(false)),
        request!(client => get_cluster_config()),
        request!(client => get_brane(b"")),
        request!(client => get_brane_info(b"")),
        request!(client => block_on(get_brane_by_id(0))),
        request!(client => block_on(brane_heartbeat(0, Brane::default(), Peer::default(), BraneStat::default(), None))),
        request!(client => block_on(ask_split(Brane::default()))),
        request!(client => block_on(ask_batch_split(Brane::default(), 1))),
        request!(client => block_on(store_heartbeat(Default::default()))),
        request!(client => block_on(report_batch_split(vec![]))),
        request!(client => scatter_brane(BraneInfo::new(Brane::default(), None))),
        request!(client => block_on(get_gc_safe_point())),
        request!(client => get_store_stats(0)),
        request!(client => get_operator(0)),
        request!(client => block_on(get_tso())),
    ];

    for (name, func) in test_funcs {
        fail::causet(leader_client_reconnect_fp, "pause").unwrap();
        // Wait for the FIDel client thread blocking on the fail point.
        // The RECONNECT_INTERVAL_SEC is 1s so sleeps 2s here.
        thread::sleep(Duration::from_secs(2));

        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            func();
            tx.lightlike(()).unwrap();
        });
        // Remove the fail point to let the FIDel client thread go on.
        fail::remove(leader_client_reconnect_fp);

        let mut timeout = Duration::from_millis(500);
        if name == "brane_heartbeat" {
            // brane_heartbeat may need to retry a few times due to reconnection so increases its timeout.
            timeout = Duration::from_secs(3);
        }
        if rx.recv_timeout(timeout).is_err() {
            panic!("FidelClient::{}() hangs", name);
        }
        handle.join().unwrap();
    }
}
