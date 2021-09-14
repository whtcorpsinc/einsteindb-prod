// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::sync::Arc;

use fidel_client::FidelClient;
use test_violetabftstore::*;
use violetabftstore::interlock::::config::*;

fn check_available<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    let engine = cluster.get_engine(1);
    let violetabft_engine = cluster.get_violetabft_engine(1);

    let stats = fidel_client.get_store_stats(1).unwrap();
    assert_eq!(stats.get_brane_count(), 2);

    let value = vec![0; 1024];
    for i in 0..1000 {
        let last_available = stats.get_available();
        cluster.must_put(format!("k{}", i).as_bytes(), &value);
        violetabft_engine.flush(true).unwrap();
        engine.flush(true).unwrap();
        sleep_ms(20);

        let stats = fidel_client.get_store_stats(1).unwrap();
        // Because the available is for disk size, even we add data
        // other process may reduce data too. so here we try to
        // check available size changed.
        if stats.get_available() != last_available {
            return;
        }
    }

    panic!("available not changed")
}

fn test_simple_store_stats<T: Simulator>(cluster: &mut Cluster<T>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);

    cluster.causet.violetabft_store.fidel_store_heartbeat_tick_interval = ReadableDuration::millis(20);
    cluster.run();

    // wait store reports stats.
    for _ in 0..100 {
        sleep_ms(20);

        if fidel_client.get_store_stats(1).is_some() {
            break;
        }
    }

    let engine = cluster.get_engine(1);
    let violetabft_engine = cluster.get_violetabft_engine(1);
    violetabft_engine.flush(true).unwrap();
    engine.flush(true).unwrap();
    let last_stats = fidel_client.get_store_stats(1).unwrap();
    assert_eq!(last_stats.get_brane_count(), 1);

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let brane = fidel_client.get_brane(b"").unwrap();
    cluster.must_split(&brane, b"k2");
    violetabft_engine.flush(true).unwrap();
    engine.flush(true).unwrap();

    // wait report brane count after split
    for _ in 0..100 {
        sleep_ms(20);

        let stats = fidel_client.get_store_stats(1).unwrap();
        if stats.get_brane_count() == 2 {
            break;
        }
    }

    let stats = fidel_client.get_store_stats(1).unwrap();
    assert_eq!(stats.get_brane_count(), 2);

    check_available(cluster);
}

#[test]
fn test_node_simple_store_stats() {
    let mut cluster = new_node_cluster(0, 1);
    test_simple_store_stats(&mut cluster);
}
