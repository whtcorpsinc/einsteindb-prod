// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time;

use ekvproto::kvrpc_timeshare::Context;
use violetabft::evioletabft_timeshare::MessageType;

use edb::{CfName, IterOptions, Causet_DEFAULT};
use test_violetabftstore::*;
use edb::causet_storage::kv::*;
use edb::causet_storage::CfStatistics;
use violetabftstore::interlock::::codec::bytes;
use violetabftstore::interlock::::HandyRwLock;
use txn_types::Key;

#[test]
fn test_violetabftkv() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(b"k1"), None);

    let brane = cluster.get_brane(b"");
    let leader_id = cluster.leader_of_brane(brane.get_id()).unwrap();
    let causet_storage = cluster.sim.rl().causet_storages[&leader_id.get_id()].clone();

    let mut ctx = Context::default();
    ctx.set_brane_id(brane.get_id());
    ctx.set_brane_epoch(brane.get_brane_epoch().clone());
    ctx.set_peer(brane.get_peers()[0].clone());

    get_put(&ctx, &causet_storage);
    batch(&ctx, &causet_storage);
    seek(&ctx, &causet_storage);
    near_seek(&ctx, &causet_storage);
    causet(&ctx, &causet_storage);
    empty_write(&ctx, &causet_storage);
    wrong_context(&ctx, &causet_storage);
    // TODO: test multiple node
}

#[test]
fn test_read_leader_in_lease() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    let k1 = b"k1";
    let (k2, v2) = (b"k2", b"v2");

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(k1), None);

    let brane = cluster.get_brane(b"");
    let leader = cluster.leader_of_brane(brane.get_id()).unwrap();
    let causet_storage = cluster.sim.rl().causet_storages[&leader.get_id()].clone();

    let mut ctx = Context::default();
    ctx.set_brane_id(brane.get_id());
    ctx.set_brane_epoch(brane.get_brane_epoch().clone());
    ctx.set_peer(leader.clone());

    // write some data
    assert_none(&ctx, &causet_storage, k2);
    must_put(&ctx, &causet_storage, k2, v2);

    // isolate leader
    cluster.add_lightlike_filter(IsolationFilterFactory::new(leader.get_store_id()));

    // leader still in lease, check if can read on leader
    assert_eq!(can_read(&ctx, &causet_storage, k2, v2), true);
}

#[test]
fn test_read_index_on_replica() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    let k1 = b"k1";
    let (k2, v2) = (b"k2", b"v2");

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(k1), None);

    let brane = cluster.get_brane(b"");
    let leader = cluster.leader_of_brane(brane.get_id()).unwrap();
    let causet_storage = cluster.sim.rl().causet_storages[&leader.get_id()].clone();

    let mut ctx = Context::default();
    ctx.set_brane_id(brane.get_id());
    ctx.set_brane_epoch(brane.get_brane_epoch().clone());
    ctx.set_peer(leader.clone());

    // write some data
    let peers = brane.get_peers();
    assert_none(&ctx, &causet_storage, k2);
    must_put(&ctx, &causet_storage, k2, v2);

    // read on follower
    let mut follower_peer = None;
    for p in peers {
        if p.get_id() != leader.get_id() {
            follower_peer = Some(p.clone());
            break;
        }
    }

    assert!(follower_peer.is_some());
    ctx.set_peer(follower_peer.as_ref().unwrap().clone());
    let resp = read_index_on_peer(
        &mut cluster,
        follower_peer.unwrap(),
        brane.clone(),
        false,
        std::time::Duration::from_secs(5),
    );
    assert!(!resp.as_ref().unwrap().get_header().has_error());
    assert_ne!(
        resp.unwrap().get_responses()[0]
            .get_read_index()
            .get_read_index(),
        0
    );
}

#[test]
fn test_read_on_replica() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    cluster.causet.violetabft_store.hibernate_branes = false;
    cluster.run();

    let k1 = b"k1";
    let (k2, v2) = (b"k2", b"v2");
    let (k3, v3) = (b"k3", b"v3");
    let (k4, v4) = (b"k4", b"v4");

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(k1), None);

    let brane = cluster.get_brane(b"");
    let leader = cluster.leader_of_brane(brane.get_id()).unwrap();
    let leader_causet_storage = cluster.sim.rl().causet_storages[&leader.get_id()].clone();

    let mut leader_ctx = Context::default();
    leader_ctx.set_brane_id(brane.get_id());
    leader_ctx.set_brane_epoch(brane.get_brane_epoch().clone());
    leader_ctx.set_peer(leader.clone());

    // write some data
    let peers = brane.get_peers();
    assert_none(&leader_ctx, &leader_causet_storage, k2);
    must_put(&leader_ctx, &leader_causet_storage, k2, v2);

    // read on follower
    let mut follower_peer = None;
    let mut follower_id = 0;
    for p in peers {
        if p.get_id() != leader.get_id() {
            follower_id = p.get_id();
            follower_peer = Some(p.clone());
            break;
        }
    }

    assert!(follower_peer.is_some());
    let mut follower_ctx = Context::default();
    follower_ctx.set_brane_id(brane.get_id());
    follower_ctx.set_brane_epoch(brane.get_brane_epoch().clone());
    follower_ctx.set_peer(follower_peer.as_ref().unwrap().clone());
    follower_ctx.set_replica_read(true);
    let follower_causet_storage = cluster.sim.rl().causet_storages[&follower_id].clone();
    assert_has(&follower_ctx, &follower_causet_storage, k2, v2);

    must_put(&leader_ctx, &leader_causet_storage, k3, v3);
    assert_has(&follower_ctx, &follower_causet_storage, k3, v3);

    cluster.stop_node(follower_id);
    must_put(&leader_ctx, &leader_causet_storage, k4, v4);
    cluster.run_node(follower_id).unwrap();
    let follower_causet_storage = cluster.sim.rl().causet_storages[&follower_id].clone();
    // sleep to ensure the follower has received a heartbeat from the leader
    thread::sleep(time::Duration::from_millis(300));
    assert_has(&follower_ctx, &follower_causet_storage, k4, v4);
}

#[test]
fn test_invalid_read_index_when_no_leader() {
    // Initialize cluster
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(10), Some(6));
    cluster.causet.violetabft_store.violetabft_heartbeat_ticks = 1;
    cluster.causet.violetabft_store.hibernate_branes = false;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    // Set brane and peers
    cluster.run();
    cluster.must_put(b"k0", b"v0");
    // Transfer leader to p2
    let brane = cluster.get_brane(b"k0");
    let leader = cluster.leader_of_brane(brane.get_id()).unwrap();
    let mut follower_peers = brane.get_peers().to_vec();
    follower_peers.retain(|p| p.get_id() != leader.get_id());
    let follower = follower_peers.pop().unwrap();

    // Delay all violetabft messages on follower.
    cluster.sim.wl().add_recv_filter(
        follower.get_store_id(),
        Box::new(
            BranePacketFilter::new(brane.get_id(), follower.get_store_id())
                .direction(Direction::Recv)
                .msg_type(MessageType::MsgHeartbeat)
                .msg_type(MessageType::MsgApplightlike)
                .msg_type(MessageType::MsgRequestVoteResponse)
                .when(Arc::new(AtomicBool::new(true))),
        ),
    );

    // wait for election timeout
    thread::sleep(time::Duration::from_millis(300));
    // lightlike read index requests to follower
    let mut request = new_request(
        brane.get_id(),
        brane.get_brane_epoch().clone(),
        vec![new_read_index_cmd()],
        true,
    );
    request.mut_header().set_peer(follower.clone());
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(follower.get_store_id(), request, cb)
        .unwrap();

    let resp = rx.recv_timeout(time::Duration::from_millis(500)).unwrap();
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("can not read index due to no leader"),
        "{:?}",
        resp.get_header()
    );
}

fn must_put<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) {
    engine.put(ctx, Key::from_raw(key), value.to_vec()).unwrap();
}

fn must_put_causet<E: Engine>(ctx: &Context, engine: &E, causet: CfName, key: &[u8], value: &[u8]) {
    engine
        .put_causet(ctx, causet, Key::from_raw(key), value.to_vec())
        .unwrap();
}

fn must_delete<E: Engine>(ctx: &Context, engine: &E, key: &[u8]) {
    engine.delete(ctx, Key::from_raw(key)).unwrap();
}

fn must_delete_causet<E: Engine>(ctx: &Context, engine: &E, causet: CfName, key: &[u8]) {
    engine.delete_causet(ctx, causet, Key::from_raw(key)).unwrap();
}

fn assert_has<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap().unwrap(), value);
}

fn can_read<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) -> bool {
    if let Ok(s) = engine.snapshot(ctx) {
        assert_eq!(s.get(&Key::from_raw(key)).unwrap().unwrap(), value);
        return true;
    }
    false
}

fn assert_has_causet<E: Engine>(ctx: &Context, engine: &E, causet: CfName, key: &[u8], value: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(
        snapshot.get_causet(causet, &Key::from_raw(key)).unwrap().unwrap(),
        value
    );
}

fn assert_none<E: Engine>(ctx: &Context, engine: &E, key: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap(), None);
}

fn assert_none_causet<E: Engine>(ctx: &Context, engine: &E, causet: CfName, key: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get_causet(causet, &Key::from_raw(key)).unwrap(), None);
}

fn assert_seek<E: Engine>(ctx: &Context, engine: &E, key: &[u8], pair: (&[u8], &[u8])) {
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut cursor = snapshot
        .iter(IterOptions::default(), ScanMode::Mixed)
        .unwrap();
    let mut statistics = CfStatistics::default();
    cursor.seek(&Key::from_raw(key), &mut statistics).unwrap();
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn assert_seek_causet<E: Engine>(
    ctx: &Context,
    engine: &E,
    causet: CfName,
    key: &[u8],
    pair: (&[u8], &[u8]),
) {
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut cursor = snapshot
        .iter_causet(causet, IterOptions::default(), ScanMode::Mixed)
        .unwrap();
    let mut statistics = CfStatistics::default();
    cursor.seek(&Key::from_raw(key), &mut statistics).unwrap();
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn assert_near_seek<I: Iteron>(cursor: &mut Cursor<I>, key: &[u8], pair: (&[u8], &[u8])) {
    let mut statistics = CfStatistics::default();
    assert!(
        cursor
            .near_seek(&Key::from_raw(key), &mut statistics)
            .unwrap(),
        hex::encode_upper(key)
    );
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn assert_near_reverse_seek<I: Iteron>(cursor: &mut Cursor<I>, key: &[u8], pair: (&[u8], &[u8])) {
    let mut statistics = CfStatistics::default();
    assert!(
        cursor
            .near_reverse_seek(&Key::from_raw(key), &mut statistics)
            .unwrap(),
        hex::encode_upper(key)
    );
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn get_put<E: Engine>(ctx: &Context, engine: &E) {
    assert_none(ctx, engine, b"x");
    must_put(ctx, engine, b"x", b"1");
    assert_has(ctx, engine, b"x", b"1");
    must_put(ctx, engine, b"x", b"2");
    assert_has(ctx, engine, b"x", b"2");
}

fn batch<E: Engine>(ctx: &Context, engine: &E) {
    engine
        .write(
            ctx,
            WriteData::from_modifies(vec![
                Modify::Put(Causet_DEFAULT, Key::from_raw(b"x"), b"1".to_vec()),
                Modify::Put(Causet_DEFAULT, Key::from_raw(b"y"), b"2".to_vec()),
            ]),
        )
        .unwrap();
    assert_has(ctx, engine, b"x", b"1");
    assert_has(ctx, engine, b"y", b"2");

    engine
        .write(
            ctx,
            WriteData::from_modifies(vec![
                Modify::Delete(Causet_DEFAULT, Key::from_raw(b"x")),
                Modify::Delete(Causet_DEFAULT, Key::from_raw(b"y")),
            ]),
        )
        .unwrap();
    assert_none(ctx, engine, b"y");
    assert_none(ctx, engine, b"y");
}

fn seek<E: Engine>(ctx: &Context, engine: &E) {
    must_put(ctx, engine, b"x", b"1");
    assert_seek(ctx, engine, b"x", (b"x", b"1"));
    assert_seek(ctx, engine, b"a", (b"x", b"1"));
    must_put(ctx, engine, b"z", b"2");
    assert_seek(ctx, engine, b"y", (b"z", b"2"));
    assert_seek(ctx, engine, b"x\x00", (b"z", b"2"));
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut iter = snapshot
        .iter(IterOptions::default(), ScanMode::Mixed)
        .unwrap();
    let mut statistics = CfStatistics::default();
    assert!(!iter
        .seek(&Key::from_raw(b"z\x00"), &mut statistics)
        .unwrap());
    must_delete(ctx, engine, b"x");
    must_delete(ctx, engine, b"z");
}

fn near_seek<E: Engine>(ctx: &Context, engine: &E) {
    must_put(ctx, engine, b"x", b"1");
    must_put(ctx, engine, b"z", b"2");
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut cursor = snapshot
        .iter(IterOptions::default(), ScanMode::Mixed)
        .unwrap();
    assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
    assert_near_seek(&mut cursor, b"a", (b"x", b"1"));
    assert_near_reverse_seek(&mut cursor, b"z1", (b"z", b"2"));
    assert_near_reverse_seek(&mut cursor, b"x1", (b"x", b"1"));
    assert_near_seek(&mut cursor, b"y", (b"z", b"2"));
    assert_near_seek(&mut cursor, b"x\x00", (b"z", b"2"));
    let mut statistics = CfStatistics::default();
    assert!(!cursor
        .near_seek(&Key::from_raw(b"z\x00"), &mut statistics)
        .unwrap());
    must_delete(ctx, engine, b"x");
    must_delete(ctx, engine, b"z");
}

fn causet<E: Engine>(ctx: &Context, engine: &E) {
    assert_none_causet(ctx, engine, "default", b"key");
    must_put_causet(ctx, engine, "default", b"key", b"value");
    assert_has_causet(ctx, engine, "default", b"key", b"value");
    assert_seek_causet(ctx, engine, "default", b"k", (b"key", b"value"));
    must_delete_causet(ctx, engine, "default", b"key");
    assert_none_causet(ctx, engine, "default", b"key");
}

fn empty_write<E: Engine>(ctx: &Context, engine: &E) {
    engine.write(ctx, WriteData::default()).unwrap_err();
}

fn wrong_context<E: Engine>(ctx: &Context, engine: &E) {
    let brane_id = ctx.get_brane_id();
    let mut ctx = ctx.to_owned();
    ctx.set_brane_id(brane_id + 1);
    assert!(engine.write(&ctx, WriteData::default()).is_err());
}
