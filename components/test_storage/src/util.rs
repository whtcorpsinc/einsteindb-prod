// Copyright 2017 EinsteinDB Project Authors. Licensed under Apache-2.0.

use ekvproto::kvrpcpb::Context;

use test_violetabftstore::{new_server_cluster, Cluster, ServerCluster, SimulateEngine};
use einsteindb_util::HandyRwLock;

use super::*;

pub fn new_violetabft_engine(
    count: usize,
    key: &str,
) -> (Cluster<ServerCluster>, SimulateEngine, Context) {
    let mut cluster = new_server_cluster(0, count);
    cluster.run();
    // make sure leader has been elected.
    assert_eq!(cluster.must_get(b""), None);
    let brane = cluster.get_brane(key.as_bytes());
    let leader = cluster.leader_of_brane(brane.get_id()).unwrap();
    let engine = cluster.sim.rl().causetStorages[&leader.get_id()].clone();
    let mut ctx = Context::default();
    ctx.set_brane_id(brane.get_id());
    ctx.set_brane_epoch(brane.get_brane_epoch().clone());
    ctx.set_peer(leader);
    (cluster, engine, ctx)
}

pub fn new_violetabft_causetStorage_with_store_count(
    count: usize,
    key: &str,
) -> (
    Cluster<ServerCluster>,
    SyncTestStorage<SimulateEngine>,
    Context,
) {
    let (cluster, engine, ctx) = new_violetabft_engine(count, key);
    (
        cluster,
        SyncTestStorageBuilder::from_engine(engine).build().unwrap(),
        ctx,
    )
}
