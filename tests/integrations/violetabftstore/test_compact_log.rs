// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use ekvproto::violetabft_server_timeshare::{VioletaBftApplyState, VioletaBftTruncatedState};

use engine_lmdb::LmdbEngine;
use edb::{Engines, Peekable, Causet_VIOLETABFT};
use violetabftstore::store::*;
use test_violetabftstore::*;
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::config::*;

fn get_violetabft_msg_or_default<M: protobuf::Message + Default>(
    engines: &Engines<LmdbEngine, LmdbEngine>,
    key: &[u8],
) -> M {
    engines
        .kv
        .get_msg_causet(Causet_VIOLETABFT, key)
        .unwrap()
        .unwrap_or_default()
}

fn test_compact_log<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let mut before_states = HashMap::default();

    for (&id, engines) in &cluster.engines {
        let mut state: VioletaBftApplyState =
            get_violetabft_msg_or_default(&engines, &tuplespaceInstanton::apply_state_key(1));
        before_states.insert(id, state.take_truncated_state());
    }

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);

        if i > 100 && check_compacted(&cluster.engines, &before_states, 1) {
            return;
        }
    }

    panic!("after inserting 1000 entries, compaction is still not finished.");
}

fn check_compacted(
    all_engines: &HashMap<u64, Engines<LmdbEngine, LmdbEngine>>,
    before_states: &HashMap<u64, VioletaBftTruncatedState>,
    compact_count: u64,
) -> bool {
    // Every peer must have compacted logs, so the truncate log state index/term must > than before.
    let mut compacted_idx = HashMap::default();

    for (&id, engines) in all_engines {
        let mut state: VioletaBftApplyState =
            get_violetabft_msg_or_default(&engines, &tuplespaceInstanton::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        let term = after_state.get_term();
        if idx == before_state.get_index() || term == before_state.get_term() {
            return false;
        }
        if idx - before_state.get_index() < compact_count {
            return false;
        }
        assert!(term > before_state.get_term());
        compacted_idx.insert(id, idx);
    }

    // wait for actual deletion.
    sleep_ms(100);

    for (id, engines) in all_engines {
        for i in 0..compacted_idx[id] {
            let key = tuplespaceInstanton::violetabft_log_key(1, i);
            if engines.violetabft.get_value(&key).unwrap().is_none() {
                break;
            }
            assert!(engines.violetabft.get_value(&key).unwrap().is_none());
        }
    }
    true
}

fn test_compact_count_limit<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 100;
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 500;
    cluster.causet.violetabft_store.violetabft_log_gc_size_limit = ReadableSize::mb(20);
    cluster.run();

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::default();

    for (&id, engines) in &cluster.engines {
        must_get_equal(&engines.kv.as_inner(), b"k1", b"v1");
        let mut state: VioletaBftApplyState =
            get_violetabft_msg_or_default(&engines, &tuplespaceInstanton::apply_state_key(1));
        let state = state.take_truncated_state();
        // compact should not spacelike
        assert_eq!(VIOLETABFT_INIT_LOG_INDEX, state.get_index());
        assert_eq!(VIOLETABFT_INIT_LOG_TERM, state.get_term());
        before_states.insert(id, state);
    }

    for i in 1..60 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }

    // wait log gc.
    sleep_ms(500);

    // limit has not reached, should not gc.
    for (&id, engines) in &cluster.engines {
        let mut state: VioletaBftApplyState =
            get_violetabft_msg_or_default(&engines, &tuplespaceInstanton::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        assert_eq!(idx, before_state.get_index());
    }

    for i in 60..200 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
        let v2 = cluster.get(&k);
        assert_eq!(v2, Some(v));

        if i > 100 && check_compacted(&cluster.engines, &before_states, 1) {
            return;
        }
    }
    panic!("cluster is not compacted after inserting 200 entries.");
}

fn test_compact_many_times<T: Simulator>(cluster: &mut Cluster<T>) {
    let gc_limit: u64 = 100;
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = gc_limit;
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 500;
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(100);
    cluster.run();

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::default();

    for (&id, engines) in &cluster.engines {
        must_get_equal(&engines.kv.as_inner(), b"k1", b"v1");
        let mut state: VioletaBftApplyState =
            get_violetabft_msg_or_default(&engines, &tuplespaceInstanton::apply_state_key(1));
        let state = state.take_truncated_state();
        // compact should not spacelike
        assert_eq!(VIOLETABFT_INIT_LOG_INDEX, state.get_index());
        assert_eq!(VIOLETABFT_INIT_LOG_TERM, state.get_term());
        before_states.insert(id, state);
    }

    for i in 1..500 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
        let v2 = cluster.get(&k);
        assert_eq!(v2, Some(v));

        if i >= 200 && check_compacted(&cluster.engines, &before_states, gc_limit * 2) {
            return;
        }
    }

    panic!("compact is expected to be executed multiple times");
}

#[test]
fn test_node_compact_log() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_log(&mut cluster);
}

#[test]
fn test_node_compact_count_limit() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_count_limit(&mut cluster);
}

#[test]
fn test_node_compact_many_times() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_many_times(&mut cluster);
}

fn test_compact_size_limit<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 100000;
    cluster.causet.violetabft_store.violetabft_log_gc_size_limit = ReadableSize::mb(2);
    cluster.run();
    cluster.stop_node(1);

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::default();

    for (&id, engines) in &cluster.engines {
        if id == 1 {
            continue;
        }
        must_get_equal(&engines.kv.as_inner(), b"k1", b"v1");
        let mut state: VioletaBftApplyState =
            get_violetabft_msg_or_default(&engines, &tuplespaceInstanton::apply_state_key(1));
        let state = state.take_truncated_state();
        // compact should not spacelike
        assert_eq!(VIOLETABFT_INIT_LOG_INDEX, state.get_index());
        assert_eq!(VIOLETABFT_INIT_LOG_TERM, state.get_term());
        before_states.insert(id, state);
    }

    for i in 1..600 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
        let v2 = cluster.get(&k);
        assert_eq!(v2, Some(v));
    }

    // wait log gc.
    sleep_ms(500);

    // limit has not reached, should not gc.
    for (&id, engines) in &cluster.engines {
        if id == 1 {
            continue;
        }
        let mut state: VioletaBftApplyState =
            get_violetabft_msg_or_default(&engines, &tuplespaceInstanton::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        assert_eq!(idx, before_state.get_index());
    }

    // 600 * 10240 > 2 * 1024 * 1024
    for _ in 600..1200 {
        let k = vec![0; 1024 * 5];
        let v = k.clone();
        cluster.must_put(&k, &v);
        let v2 = cluster.get(&k);
        assert_eq!(v2, Some(v));
    }

    sleep_ms(500);

    // Size exceed max limit, every peer must have compacted logs,
    // so the truncate log state index/term must > than before.
    for (&id, engines) in &cluster.engines {
        if id == 1 {
            continue;
        }
        let mut state: VioletaBftApplyState =
            get_violetabft_msg_or_default(&engines, &tuplespaceInstanton::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        assert!(idx > before_state.get_index());

        for i in 0..idx {
            let key = tuplespaceInstanton::violetabft_log_key(1, i);
            assert!(engines.violetabft.get_value(&key).unwrap().is_none());
        }
    }
}

#[test]
fn test_node_compact_size_limit() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_size_limit(&mut cluster);
}

fn test_compact_reserve_max_ticks<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 100;
    cluster.causet.violetabft_store.violetabft_log_gc_memory_barrier = 500;
    cluster.causet.violetabft_store.violetabft_log_gc_size_limit = ReadableSize::mb(20);
    cluster.causet.violetabft_store.violetabft_log_reserve_max_ticks = 2;
    cluster.run();
    let apply_key = tuplespaceInstanton::apply_state_key(1);

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::default();
    for (&id, engines) in &cluster.engines {
        must_get_equal(&engines.kv.as_inner(), b"k1", b"v1");
        let mut state: VioletaBftApplyState = get_violetabft_msg_or_default(&engines, &apply_key);
        let state = state.take_truncated_state();
        // compact should not spacelike
        assert_eq!(VIOLETABFT_INIT_LOG_INDEX, state.get_index());
        assert_eq!(VIOLETABFT_INIT_LOG_TERM, state.get_term());
        before_states.insert(id, state);
    }

    for i in 1..60 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }

    // wait log gc.
    sleep_ms(500);

    // Should GC even if limit has not reached.
    for (&id, engines) in &cluster.engines {
        let mut state: VioletaBftApplyState = get_violetabft_msg_or_default(&engines, &apply_key);
        let after_state = state.take_truncated_state();
        let before_state = &before_states[&id];
        assert_ne!(after_state.get_index(), before_state.get_index());
    }
}

#[test]
fn test_node_compact_reserve_max_ticks() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_reserve_max_ticks(&mut cluster);
}
