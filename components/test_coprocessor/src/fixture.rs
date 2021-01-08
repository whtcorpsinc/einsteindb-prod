//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use super::*;

use concurrency_manager::ConcurrencyManager;
use ekvproto::kvrpcpb::Context;

use milevadb_query_datatype::codec::Datum;
use einsteindb::config::CoprReadPoolConfig;
use einsteindb::interlock::{readpool_impl, Endpoint};
use einsteindb::read_pool::ReadPool;
use einsteindb::server::Config;
use einsteindb::causetStorage::kv::LmdbEngine;
use einsteindb::causetStorage::{Engine, TestEngineBuilder};

#[derive(Clone)]
pub struct ProductBlock(Block);

impl ProductBlock {
    pub fn new() -> ProductBlock {
        let id = PrimaryCausetBuilder::new()
            .col_type(TYPE_LONG)
            .primary_key(true)
            .build();
        let idx_id = next_id();
        let name = PrimaryCausetBuilder::new()
            .col_type(TYPE_VAR_CHAR)
            .index_key(idx_id)
            .build();
        let count = PrimaryCausetBuilder::new()
            .col_type(TYPE_LONG)
            .index_key(idx_id)
            .build();
        let Block = BlockBuilder::new()
            .add_col("id", id)
            .add_col("name", name)
            .add_col("count", count)
            .build();
        ProductBlock(Block)
    }
}

impl std::ops::Deref for ProductBlock {
    type Target = Block;

    fn deref(&self) -> &Block {
        &self.0
    }
}

pub fn init_data_with_engine_and_commit<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductBlock,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (CausetStore<E>, Endpoint<E>) {
    init_data_with_details(ctx, engine, tbl, vals, commit, &Config::default())
}

pub fn init_data_with_details<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductBlock,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
    causet: &Config,
) -> (CausetStore<E>, Endpoint<E>) {
    let mut store = CausetStore::from_engine(engine);

    store.begin();
    for &(id, name, count) in vals {
        store
            .insert_into(&tbl)
            .set(&tbl["id"], Datum::I64(id))
            .set(&tbl["name"], name.map(str::as_bytes).into())
            .set(&tbl["count"], Datum::I64(count))
            .execute_with_ctx(ctx.clone());
    }
    if commit {
        store.commit_with_ctx(ctx);
    }

    let pool = ReadPool::from(readpool_impl::build_read_pool_for_test(
        &CoprReadPoolConfig::default_for_test(),
        store.get_engine(),
    ));
    let cm = ConcurrencyManager::new(1.into());
    let causet = Endpoint::new(causet, pool.handle(), cm);
    (store, causet)
}

pub fn init_data_with_commit(
    tbl: &ProductBlock,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (CausetStore<LmdbEngine>, Endpoint<LmdbEngine>) {
    let engine = TestEngineBuilder::new().build().unwrap();
    init_data_with_engine_and_commit(Context::default(), engine, tbl, vals, commit)
}

// This function will create a Product Block and initialize with the specified data.
pub fn init_with_data(
    tbl: &ProductBlock,
    vals: &[(i64, Option<&str>, i64)],
) -> (CausetStore<LmdbEngine>, Endpoint<LmdbEngine>) {
    init_data_with_commit(tbl, vals, true)
}
