// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use engine_lmdb::raw::DBStatisticsTickerType;
use edb::{MiscExt, Causet_DAGGER};
use test_violetabftstore::*;
use violetabftstore::interlock::::config::*;

fn flush<T: Simulator>(cluster: &mut Cluster<T>) {
    for engines in cluster.engines.values() {
        engines.kv.flush_causet(Causet_DAGGER, true).unwrap();
    }
}

fn flush_then_check<T: Simulator>(cluster: &mut Cluster<T>, interval: u64, written: bool) {
    flush(cluster);
    // Wait for compaction.
    sleep_ms(interval * 2);
    for engines in cluster.engines.values() {
        let compact_write_bytes = engines
            .kv
            .as_inner()
            .get_statistics_ticker_count(DBStatisticsTickerType::CompactWriteBytes);
        if written {
            assert!(compact_write_bytes > 0);
        } else {
            assert_eq!(compact_write_bytes, 0);
        }
    }
}

fn test_compact_lock_causet<T: Simulator>(cluster: &mut Cluster<T>) {
    let interval = 500;
    // Set lock_causet_compact_interval.
    cluster.causet.violetabft_store.lock_causet_compact_interval = ReadableDuration::millis(interval);
    // Set lock_causet_compact_bytes_memory_barrier.
    cluster.causet.violetabft_store.lock_causet_compact_bytes_memory_barrier = ReadableSize(100);
    cluster.run();

    // Write 40 bytes, not reach lock_causet_compact_bytes_memory_barrier, so there is no compaction.
    for i in 0..5 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        cluster.must_put_causet(Causet_DAGGER, k.as_bytes(), v.as_bytes());
    }
    // Generate one sst, if there are datas only in one memBlock, no compactions will be triggered.
    flush(cluster);

    // Write more 40 bytes, still not reach lock_causet_compact_bytes_memory_barrier,
    // so there is no compaction.
    for i in 5..10 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        cluster.must_put_causet(Causet_DAGGER, k.as_bytes(), v.as_bytes());
    }
    // Generate another sst.
    flush_then_check(cluster, interval, false);

    // Write more 50 bytes, reach lock_causet_compact_bytes_memory_barrier.
    for i in 10..15 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        cluster.must_put_causet(Causet_DAGGER, k.as_bytes(), v.as_bytes());
    }
    flush_then_check(cluster, interval, true);
}

#[test]
fn test_server_compact_lock_causet() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    test_compact_lock_causet(&mut cluster);
}
