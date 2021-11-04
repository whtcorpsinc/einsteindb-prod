// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

#[macro_use]
extern crate clap;
#[macro_use]
extern crate vlog;

use std::borrow::ToOwned;
use std::cmp::Ordering;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::string::ToString;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{process, str, u64};

use clap::{crate_authors, App, AppSettings, Arg, ArgMatches, SubCommand};
use futures::{executor::block_on, future, stream, Stream, StreamExt, TryStreamExt};
use grpcio::{CallOption, ChannelBuilder, Environment};
use protobuf::Message;

use encryption::{
    encryption_method_from_db_encryption_method, DataKeyManager, DecrypterReader, Iv,
};
use engine_lmdb::encryption::get_env;
use engine_lmdb::LmdbEngine;
use edb::{EncryptionKeyManager, ALL_CausetS, Causet_DEFAULT, Causet_DAGGER, Causet_WRITE};
use edb::{Engines, VioletaBftEngine};
use ekvproto::debug_timeshare::{Db as DBType, *};
use ekvproto::encryption_timeshare::EncryptionMethod;
use ekvproto::kvrpc_timeshare::{MvccInfo, SplitBraneRequest};
use ekvproto::meta_timeshare::{Peer, Brane};
use ekvproto::violetabft_cmd_timeshare::VioletaBftCmdRequest;
use ekvproto::violetabft_server_timeshare::{PeerState, SnapshotMeta};
use ekvproto::edb_timeshare::EINSTEINDBClient;
use fidel_client::{Config as FidelConfig, FidelClient, RpcClient};
use violetabft::evioletabft_timeshare::{ConfChange, Entry, EntryType};
use violetabft_log_engine::VioletaBftLogEngine;
use violetabftstore::store::INIT_EPOCH_CONF_VER;
use security::{SecurityConfig, SecurityManager};
use std::pin::Pin;
use edb::config::{ConfigController, EINSTEINDBConfig};
use edb::server::debug::{BottommostLevelCompaction, Debugger, BraneInfo};
use violetabftstore::interlock::::{escape, file::calc_crc32, unescape};
use txn_types::Key;

const METRICS_PROMETHEUS: &str = "prometheus";
const METRICS_LMDB_KV: &str = "lmdb_kv";
const METRICS_LMDB_VIOLETABFT: &str = "lmdb_violetabft";
const METRICS_JEMALLOC: &str = "jemalloc";

type MvccInfoStream = Pin<Box<dyn Stream<Item = Result<(Vec<u8>, MvccInfo), String>>>>;

fn perror_and_exit<E: Error>(prefix: &str, e: E) -> ! {
    ve1!("{}: {}", prefix, e);
    process::exit(-1);
}

fn new_debug_executor(
    db: Option<&str>,
    violetabft_db: Option<&str>,
    ignore_failover: bool,
    host: Option<&str>,
    causet: &EINSTEINDBConfig,
    mgr: Arc<SecurityManager>,
) -> Box<dyn DebugFreeDaemon> {
    match (host, db) {
        (None, Some(kv_path)) => {
            let key_manager =
                DataKeyManager::from_config(&causet.security.encryption, &causet.causet_storage.data_dir)
                    .unwrap()
                    .map(|key_manager| Arc::new(key_manager));
            let cache = causet.causet_storage.block_cache.build_shared_cache();
            let shared_block_cache = cache.is_some();
            let env = get_env(key_manager, None).unwrap();

            let mut kv_db_opts = causet.lmdb.build_opt();
            kv_db_opts.set_env(env.clone());
            kv_db_opts.set_paranoid_checks(!ignore_failover);
            let kv_causets_opts = causet.lmdb.build_causet_opts(&cache);
            let kv_path = PathBuf::from(kv_path).canonicalize().unwrap();
            let kv_path = kv_path.to_str().unwrap();
            let kv_db =
                engine_lmdb::raw_util::new_engine_opt(kv_path, kv_db_opts, kv_causets_opts).unwrap();
            let mut kv_db = LmdbEngine::from_db(Arc::new(kv_db));
            kv_db.set_shared_block_cache(shared_block_cache);

            let mut violetabft_path = violetabft_db
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("{}/../violetabft", kv_path));
            violetabft_path = PathBuf::from(violetabft_path)
                .canonicalize()
                .unwrap()
                .to_str()
                .map(ToString::to_string)
                .unwrap();

            let causet_controller = ConfigController::default();
            if !causet.violetabft_engine.enable {
                let mut violetabft_db_opts = causet.violetabftdb.build_opt();
                violetabft_db_opts.set_env(env);
                let violetabft_db_causet_opts = causet.violetabftdb.build_causet_opts(&cache);
                let violetabft_db = engine_lmdb::raw_util::new_engine_opt(
                    &violetabft_path,
                    violetabft_db_opts,
                    violetabft_db_causet_opts,
                )
                .unwrap();
                let mut violetabft_db = LmdbEngine::from_db(Arc::new(violetabft_db));
                violetabft_db.set_shared_block_cache(shared_block_cache);
                let debugger = Debugger::new(Engines::new(kv_db, violetabft_db), causet_controller);
                Box::new(debugger) as Box<dyn DebugFreeDaemon>
            } else {
                let config = causet.violetabft_engine.config();
                let violetabft_db = VioletaBftLogEngine::new(config);
                let debugger = Debugger::new(Engines::new(kv_db, violetabft_db), causet_controller);
                Box::new(debugger) as Box<dyn DebugFreeDaemon>
            }
        }
        (Some(remote), None) => Box::new(new_debug_client(remote, mgr)) as Box<dyn DebugFreeDaemon>,
        _ => unreachable!(),
    }
}

fn new_debug_client(host: &str, mgr: Arc<SecurityManager>) -> DebugClient {
    let env = Arc::new(Environment::new(1));
    let cb = ChannelBuilder::new(env)
        .max_receive_message_len(1 << 30) // 1G.
        .max_lightlike_message_len(1 << 30)
        .keepalive_time(Duration::from_secs(10))
        .keepalive_timeout(Duration::from_secs(3));

    let channel = mgr.connect(cb, host);
    DebugClient::new(channel)
}

trait DebugFreeDaemon {
    fn dump_value(&self, causet: &str, key: Vec<u8>) {
        let value = self.get_value_by_key(causet, key);
        v1!("value: {}", escape(&value));
    }

    fn dump_brane_size(&self, brane: u64, causets: Vec<&str>) -> usize {
        let sizes = self.get_brane_size(brane, causets);
        let mut total_size = 0;
        v1!("brane id: {}", brane);
        for (causet, size) in sizes {
            v1!("causet {} brane size: {}", causet, convert_gbmb(size as u64));
            total_size += size;
        }
        total_size
    }

    fn dump_all_brane_size(&self, causets: Vec<&str>) {
        let branes = self.get_all_meta_branes();
        let branes_number = branes.len();
        let mut total_size = 0;
        for brane in branes {
            total_size += self.dump_brane_size(brane, causets.clone());
        }
        v1!("total brane number: {}", branes_number);
        v1!("total brane size: {}", convert_gbmb(total_size as u64));
    }

    fn dump_brane_info(&self, brane: u64, skip_tombstone: bool) {
        let r = self.get_brane_info(brane);
        if skip_tombstone {
            let brane_state = r.brane_local_state.as_ref();
            if brane_state.map_or(false, |s| s.get_state() == PeerState::Tombstone) {
                return;
            }
        }
        let brane_state_key = tuplespaceInstanton::brane_state_key(brane);
        let violetabft_state_key = tuplespaceInstanton::violetabft_state_key(brane);
        let apply_state_key = tuplespaceInstanton::apply_state_key(brane);
        v1!("brane id: {}", brane);
        v1!("brane state key: {}", escape(&brane_state_key));
        v1!("brane state: {:?}", r.brane_local_state);
        v1!("violetabft state key: {}", escape(&violetabft_state_key));
        v1!("violetabft state: {:?}", r.violetabft_local_state);
        v1!("apply state key: {}", escape(&apply_state_key));
        v1!("apply state: {:?}", r.violetabft_apply_state);
    }

    fn dump_all_brane_info(&self, skip_tombstone: bool) {
        for brane in self.get_all_meta_branes() {
            self.dump_brane_info(brane, skip_tombstone);
        }
    }

    fn dump_violetabft_log(&self, brane: u64, index: u64) {
        let idx_key = tuplespaceInstanton::violetabft_log_key(brane, index);
        v1!("idx_key: {}", escape(&idx_key));
        v1!("brane: {}", brane);
        v1!("log index: {}", index);

        let mut entry = self.get_violetabft_log(brane, index);
        let data = entry.take_data();
        v1!("entry {:?}", entry);
        v1!("msg len: {}", data.len());

        if data.is_empty() {
            return;
        }

        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                let mut msg = VioletaBftCmdRequest::default();
                msg.merge_from_bytes(&data).unwrap();
                v1!("Normal: {:#?}", msg);
            }
            EntryType::EntryConfChange => {
                let mut msg = ConfChange::new();
                msg.merge_from_bytes(&data).unwrap();
                let ctx = msg.take_context();
                v1!("ConfChange: {:?}", msg);
                let mut cmd = VioletaBftCmdRequest::default();
                cmd.merge_from_bytes(&ctx).unwrap();
                v1!("ConfChange.VioletaBftCmdRequest: {:#?}", cmd);
            }
            EntryType::EntryConfChangeV2 => unimplemented!(),
        }
    }

    /// Dump tail_pointer infos for a given key cone. The given `from` and `to` must
    /// be raw key with `z` prefix. Both `to` and `limit` are empty value means
    /// what we want is point query instead of cone scan.
    fn dump_tail_pointers_infos(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        mut limit: u64,
        causets: Vec<&str>,
        spacelike_ts: Option<u64>,
        commit_ts: Option<u64>,
    ) {
        if !from.spacelikes_with(b"z") || (!to.is_empty() && !to.spacelikes_with(b"z")) {
            ve1!("from and to should spacelike with \"z\"");
            process::exit(-1);
        }
        if !to.is_empty() && to < from {
            ve1!("\"to\" must be greater than \"from\"");
            process::exit(-1);
        }

        let point_query = to.is_empty() && limit == 0;
        if point_query {
            limit = 1;
        }

        let scan_future =
            self.get_tail_pointer_infos(from.clone(), to, limit)
                .try_for_each(move |(key, tail_pointer)| {
                    if point_query && key != from {
                        v1!("no tail_pointer infos for {}", escape(&from));
                        return future::err::<(), String>("no tail_pointer infos".to_owned());
                    }

                    v1!("key: {}", escape(&key));
                    if causets.contains(&Causet_DAGGER) && tail_pointer.has_lock() {
                        let lock_info = tail_pointer.get_dagger();
                        if spacelike_ts.map_or(true, |ts| lock_info.get_spacelike_ts() == ts) {
                            v1!("\tdagger causet value: {:?}", lock_info);
                        }
                    }
                    if causets.contains(&Causet_DEFAULT) {
                        for value_info in tail_pointer.get_values() {
                            if commit_ts.map_or(true, |ts| value_info.get_spacelike_ts() == ts) {
                                v1!("\tdefault causet value: {:?}", value_info);
                            }
                        }
                    }
                    if causets.contains(&Causet_WRITE) {
                        for write_info in tail_pointer.get_writes() {
                            if spacelike_ts.map_or(true, |ts| write_info.get_spacelike_ts() == ts)
                                && commit_ts.map_or(true, |ts| write_info.get_commit_ts() == ts)
                            {
                                v1!("\t write causet value: {:?}", write_info);
                            }
                        }
                    }
                    v1!("");
                    future::ok::<(), String>(())
                });
        if let Err(e) = block_on(scan_future) {
            ve1!("{}", e);
            process::exit(-1);
        }
    }

    fn raw_scan(&self, from_key: &[u8], to_key: &[u8], limit: usize, causet: &str) {
        if !ALL_CausetS.contains(&causet) {
            eprintln!("Causet \"{}\" doesn't exist.", causet);
            process::exit(-1);
        }
        if !to_key.is_empty() && from_key >= to_key {
            eprintln!(
                "to_key should be greater than from_key, but got from_key: \"{}\", to_key: \"{}\"",
                escape(from_key),
                escape(to_key)
            );
            process::exit(-1);
        }
        if limit == 0 {
            eprintln!("limit should be greater than 0");
            process::exit(-1);
        }

        self.raw_scan_impl(from_key, to_key, limit, causet);
    }

    fn diff_brane(
        &self,
        brane: u64,
        db: Option<&str>,
        violetabft_db: Option<&str>,
        host: Option<&str>,
        causet: &EINSTEINDBConfig,
        mgr: Arc<SecurityManager>,
    ) {
        let rhs_debug_executor = new_debug_executor(db, violetabft_db, false, host, causet, mgr);

        let r1 = self.get_brane_info(brane);
        let r2 = rhs_debug_executor.get_brane_info(brane);
        v1!("brane id: {}", brane);
        v1!("db1 brane state: {:?}", r1.brane_local_state);
        v1!("db2 brane state: {:?}", r2.brane_local_state);
        v1!("db1 apply state: {:?}", r1.violetabft_apply_state);
        v1!("db2 apply state: {:?}", r2.violetabft_apply_state);

        match (r1.brane_local_state, r2.brane_local_state) {
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                v1!("db1 and db2 don't have same brane local_state");
            }
            (Some(brane_local_1), Some(brane_local_2)) => {
                let brane1 = brane_local_1.get_brane();
                let brane2 = brane_local_2.get_brane();
                if brane1 != brane2 {
                    v1!("db1 and db2 have different brane:");
                    v1!("db1 brane: {:?}", brane1);
                    v1!("db2 brane: {:?}", brane2);
                    return;
                }
                let spacelike_key = tuplespaceInstanton::data_key(brane1.get_spacelike_key());
                let lightlike_key = tuplespaceInstanton::data_key(brane1.get_lightlike_key());
                let mut tail_pointer_infos_1 = self.get_tail_pointer_infos(spacelike_key.clone(), lightlike_key.clone(), 0);
                let mut tail_pointer_infos_2 = rhs_debug_executor.get_tail_pointer_infos(spacelike_key, lightlike_key, 0);

                let mut has_diff = false;
                let mut key_counts = [0; 2];

                let mut take_item = |i: usize| -> Option<(Vec<u8>, MvccInfo)> {
                    let wait = match i {
                        1 => block_on(future::poll_fn(|cx| tail_pointer_infos_1.poll_next_unpin(cx))),
                        _ => block_on(future::poll_fn(|cx| tail_pointer_infos_2.poll_next_unpin(cx))),
                    };
                    match wait? {
                        Err(e) => {
                            v1!("db{} scan data in brane {} fail: {}", i, brane, e);
                            process::exit(-1);
                        }
                        Ok(s) => Some(s),
                    }
                };

                let show_only = |i: usize, k: &[u8]| {
                    v1!("only db{} has: {}", i, escape(k));
                };

                let (mut item1, mut item2) = (take_item(1), take_item(2));
                while item1.is_some() && item2.is_some() {
                    key_counts[0] += 1;
                    key_counts[1] += 1;
                    let t1 = item1.take().unwrap();
                    let t2 = item2.take().unwrap();
                    match t1.0.cmp(&t2.0) {
                        Ordering::Less => {
                            show_only(1, &t1.0);
                            has_diff = true;
                            item1 = take_item(1);
                            item2 = Some(t2);
                            key_counts[1] -= 1;
                        }
                        Ordering::Greater => {
                            show_only(2, &t2.0);
                            has_diff = true;
                            item1 = Some(t1);
                            item2 = take_item(2);
                            key_counts[0] -= 1;
                        }
                        _ => {
                            if t1.1 != t2.1 {
                                v1!("diff tail_pointer on key: {}", escape(&t1.0));
                                has_diff = true;
                            }
                            item1 = take_item(1);
                            item2 = take_item(2);
                        }
                    }
                }
                let mut item = item1.map(|t| (1, t)).or_else(|| item2.map(|t| (2, t)));
                while let Some((i, (key, _))) = item.take() {
                    key_counts[i - 1] += 1;
                    show_only(i, &key);
                    has_diff = true;
                    item = take_item(i).map(|t| (i, t));
                }
                if !has_diff {
                    v1!("db1 and db2 have same data in brane: {}", brane);
                }
                v1!(
                    "db1 has {} tuplespaceInstanton, db2 has {} tuplespaceInstanton",
                    key_counts[0],
                    key_counts[1]
                );
            }
        }
    }

    fn compact(
        &self,
        address: Option<&str>,
        db: DBType,
        causet: &str,
        from: Option<Vec<u8>>,
        to: Option<Vec<u8>>,
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        let from = from.unwrap_or_default();
        let to = to.unwrap_or_default();
        self.do_compaction(db, causet, &from, &to, threads, bottommost);
        v1!(
            "store:{:?} compact db:{:?} causet:{} cone:[{:?}, {:?}) success!",
            address.unwrap_or("local"),
            db,
            causet,
            from,
            to
        );
    }

    fn compact_brane(
        &self,
        address: Option<&str>,
        db: DBType,
        causet: &str,
        brane_id: u64,
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        let brane_local = self.get_brane_info(brane_id).brane_local_state.unwrap();
        let r = brane_local.get_brane();
        let from = tuplespaceInstanton::data_key(r.get_spacelike_key());
        let to = tuplespaceInstanton::data_lightlike_key(r.get_lightlike_key());
        self.do_compaction(db, causet, &from, &to, threads, bottommost);
        v1!(
            "store:{:?} compact_brane db:{:?} causet:{} cone:[{:?}, {:?}) success!",
            address.unwrap_or("local"),
            db,
            causet,
            from,
            to
        );
    }

    fn print_bad_branes(&self);

    fn set_brane_tombstone_after_remove_peer(
        &self,
        mgr: Arc<SecurityManager>,
        causet: &FidelConfig,
        brane_ids: Vec<u64>,
    ) {
        self.check_local_mode();
        let rpc_client =
            RpcClient::new(causet, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let branes = brane_ids
            .into_iter()
            .map(|brane_id| {
                if let Some(brane) = block_on(rpc_client.get_brane_by_id(brane_id))
                    .unwrap_or_else(|e| perror_and_exit("Get brane id from FIDel", e))
                {
                    return brane;
                }
                ve1!("no such brane in fidel: {}", brane_id);
                process::exit(-1);
            })
            .collect();
        self.set_brane_tombstone(branes);
    }

    fn set_brane_tombstone_force(&self, brane_ids: Vec<u64>) {
        self.check_local_mode();
        self.set_brane_tombstone_by_id(brane_ids);
    }

    /// Recover the cluster when given `store_ids` are failed.
    fn remove_fail_stores(&self, store_ids: Vec<u64>, brane_ids: Option<Vec<u64>>);

    /// Recreate the brane with metadata from fidel, but alloc new id for it.
    fn recreate_brane(&self, sec_mgr: Arc<SecurityManager>, fidel_causet: &FidelConfig, brane_id: u64);

    fn check_brane_consistency(&self, _: u64);

    fn check_local_mode(&self);

    fn recover_branes_tail_pointer(
        &self,
        mgr: Arc<SecurityManager>,
        causet: &FidelConfig,
        brane_ids: Vec<u64>,
        read_only: bool,
    ) {
        self.check_local_mode();
        let rpc_client =
            RpcClient::new(causet, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let branes = brane_ids
            .into_iter()
            .map(|brane_id| {
                if let Some(brane) = block_on(rpc_client.get_brane_by_id(brane_id))
                    .unwrap_or_else(|e| perror_and_exit("Get brane id from FIDel", e))
                {
                    return brane;
                }
                ve1!("no such brane in fidel: {}", brane_id);
                process::exit(-1);
            })
            .collect();
        self.recover_branes(branes, read_only);
    }

    fn recover_tail_pointer_all(&self, threads: usize, read_only: bool) {
        self.check_local_mode();
        self.recover_all(threads, read_only);
    }

    fn get_all_meta_branes(&self) -> Vec<u64>;

    fn get_value_by_key(&self, causet: &str, key: Vec<u8>) -> Vec<u8>;

    fn get_brane_size(&self, brane: u64, causets: Vec<&str>) -> Vec<(String, usize)>;

    fn get_brane_info(&self, brane: u64) -> BraneInfo;

    fn get_violetabft_log(&self, brane: u64, index: u64) -> Entry;

    fn get_tail_pointer_infos(&self, from: Vec<u8>, to: Vec<u8>, limit: u64) -> MvccInfoStream;

    fn raw_scan_impl(&self, from_key: &[u8], lightlike_key: &[u8], limit: usize, causet: &str);

    fn do_compaction(
        &self,
        db: DBType,
        causet: &str,
        from: &[u8],
        to: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    );

    fn set_brane_tombstone(&self, branes: Vec<Brane>);

    fn set_brane_tombstone_by_id(&self, branes: Vec<u64>);

    fn recover_branes(&self, branes: Vec<Brane>, read_only: bool);

    fn recover_all(&self, threads: usize, read_only: bool);

    fn modify_edb_config(&self, config_name: &str, config_value: &str);

    fn dump_metrics(&self, tags: Vec<&str>);

    fn dump_brane_properties(&self, brane_id: u64);

    fn dump_cone_properties(&self, spacelike: Vec<u8>, lightlike: Vec<u8>);

    fn dump_store_info(&self);

    fn dump_cluster_info(&self);
}

impl DebugFreeDaemon for DebugClient {
    fn check_local_mode(&self) {
        ve1!("This command is only for local mode");
        process::exit(-1);
    }

    fn get_all_meta_branes(&self) -> Vec<u64> {
        unimplemented!();
    }

    fn get_value_by_key(&self, causet: &str, key: Vec<u8>) -> Vec<u8> {
        let mut req = GetRequest::default();
        req.set_db(DBType::Kv);
        req.set_causet(causet.to_owned());
        req.set_key(key);
        self.get(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get", e))
            .take_value()
    }

    fn get_brane_size(&self, brane: u64, causets: Vec<&str>) -> Vec<(String, usize)> {
        let causets = causets.into_iter().map(ToOwned::to_owned).collect::<Vec<_>>();
        let mut req = BraneSizeRequest::default();
        req.set_causets(causets.into());
        req.set_brane_id(brane);
        self.brane_size(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::brane_size", e))
            .take_entries()
            .into_iter()
            .map(|mut entry| (entry.take_causet(), entry.get_size() as usize))
            .collect()
    }

    fn get_brane_info(&self, brane: u64) -> BraneInfo {
        let mut req = BraneInfoRequest::default();
        req.set_brane_id(brane);
        let mut resp = self
            .brane_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::brane_info", e));

        let mut brane_info = BraneInfo::default();
        if resp.has_violetabft_local_state() {
            brane_info.violetabft_local_state = Some(resp.take_violetabft_local_state());
        }
        if resp.has_violetabft_apply_state() {
            brane_info.violetabft_apply_state = Some(resp.take_violetabft_apply_state());
        }
        if resp.has_brane_local_state() {
            brane_info.brane_local_state = Some(resp.take_brane_local_state());
        }
        brane_info
    }

    fn get_violetabft_log(&self, brane: u64, index: u64) -> Entry {
        let mut req = VioletaBftLogRequest::default();
        req.set_brane_id(brane);
        req.set_log_index(index);
        self.violetabft_log(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::violetabft_log", e))
            .take_entry()
    }

    fn get_tail_pointer_infos(&self, from: Vec<u8>, to: Vec<u8>, limit: u64) -> MvccInfoStream {
        let mut req = ScanMvccRequest::default();
        req.set_from_key(from);
        req.set_to_key(to);
        req.set_limit(limit);
        Box::pin(
            self.scan_tail_pointer(&req)
                .unwrap()
                .map_err(|e| e.to_string())
                .map_ok(|mut resp| (resp.take_key(), resp.take_info())),
        )
    }

    fn raw_scan_impl(&self, _: &[u8], _: &[u8], _: usize, _: &str) {
        unimplemented!();
    }

    fn do_compaction(
        &self,
        db: DBType,
        causet: &str,
        from: &[u8],
        to: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        let mut req = CompactRequest::default();
        req.set_db(db);
        req.set_causet(causet.to_owned());
        req.set_from_key(from.to_owned());
        req.set_to_key(to.to_owned());
        req.set_threads(threads);
        req.set_bottommost_level_compaction(bottommost.into());
        self.compact(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::compact", e));
    }

    fn dump_metrics(&self, tags: Vec<&str>) {
        let mut req = GetMetricsRequest::default();
        req.set_all(true);
        if tags.len() == 1 && tags[0] == METRICS_PROMETHEUS {
            req.set_all(false);
        }
        let mut resp = self
            .get_metrics(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::metrics", e));
        for tag in tags {
            v1!("tag:{}", tag);
            let metrics = match tag {
                METRICS_LMDB_KV => resp.take_lmdb_kv(),
                METRICS_LMDB_VIOLETABFT => resp.take_lmdb_violetabft(),
                METRICS_JEMALLOC => resp.take_jemalloc(),
                METRICS_PROMETHEUS => resp.take_prometheus(),
                _ => String::from(
                    "unsupported tag, should be one of prometheus/jemalloc/lmdb_violetabft/lmdb_kv",
                ),
            };
            v1!("{}", metrics);
        }
    }

    fn set_brane_tombstone(&self, _: Vec<Brane>) {
        unimplemented!("only available for local mode");
    }

    fn set_brane_tombstone_by_id(&self, _: Vec<u64>) {
        unimplemented!("only available for local mode");
    }

    fn recover_branes(&self, _: Vec<Brane>, _: bool) {
        unimplemented!("only available for local mode");
    }

    fn recover_all(&self, _: usize, _: bool) {
        unimplemented!("only available for local mode");
    }

    fn print_bad_branes(&self) {
        unimplemented!("only available for local mode");
    }

    fn remove_fail_stores(&self, _: Vec<u64>, _: Option<Vec<u64>>) {
        self.check_local_mode();
    }

    fn recreate_brane(&self, _: Arc<SecurityManager>, _: &FidelConfig, _: u64) {
        self.check_local_mode();
    }

    fn check_brane_consistency(&self, brane_id: u64) {
        let mut req = BraneConsistencyCheckRequest::default();
        req.set_brane_id(brane_id);
        self.check_brane_consistency(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::check_brane_consistency", e));
        v1!("success!");
    }

    fn modify_edb_config(&self, config_name: &str, config_value: &str) {
        let mut req = ModifyEINSTEINDBConfigRequest::default();
        req.set_config_name(config_name.to_owned());
        req.set_config_value(config_value.to_owned());
        self.modify_edb_config(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::modify_edb_config", e));
        v1!("success");
    }

    fn dump_brane_properties(&self, brane_id: u64) {
        let mut req = GetBranePropertiesRequest::default();
        req.set_brane_id(brane_id);
        let resp = self
            .get_brane_properties(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_brane_properties", e));
        for prop in resp.get_props() {
            v1!("{}: {}", prop.get_name(), prop.get_value());
        }
    }

    fn dump_cone_properties(&self, _: Vec<u8>, _: Vec<u8>) {
        unimplemented!("only available for local mode");
    }

    fn dump_store_info(&self) {
        let req = GetStoreInfoRequest::default();
        let resp = self
            .get_store_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_store_info", e));
        v1!("{}", resp.get_store_id())
    }

    fn dump_cluster_info(&self) {
        let req = GetClusterInfoRequest::default();
        let resp = self
            .get_cluster_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_cluster_info", e));
        v1!("{}", resp.get_cluster_id())
    }
}

impl<ER: VioletaBftEngine> DebugFreeDaemon for Debugger<ER> {
    fn check_local_mode(&self) {}

    fn get_all_meta_branes(&self) -> Vec<u64> {
        self.get_all_meta_branes()
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_all_meta_branes", e))
    }

    fn get_value_by_key(&self, causet: &str, key: Vec<u8>) -> Vec<u8> {
        self.get(DBType::Kv, causet, &key)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get", e))
    }

    fn get_brane_size(&self, brane: u64, causets: Vec<&str>) -> Vec<(String, usize)> {
        self.brane_size(brane, causets)
            .unwrap_or_else(|e| perror_and_exit("Debugger::brane_size", e))
            .into_iter()
            .map(|(causet, size)| (causet.to_owned(), size as usize))
            .collect()
    }

    fn get_brane_info(&self, brane: u64) -> BraneInfo {
        self.brane_info(brane)
            .unwrap_or_else(|e| perror_and_exit("Debugger::brane_info", e))
    }

    fn get_violetabft_log(&self, brane: u64, index: u64) -> Entry {
        self.violetabft_log(brane, index)
            .unwrap_or_else(|e| perror_and_exit("Debugger::violetabft_log", e))
    }

    fn get_tail_pointer_infos(&self, from: Vec<u8>, to: Vec<u8>, limit: u64) -> MvccInfoStream {
        let iter = self
            .scan_tail_pointer(&from, &to, limit)
            .unwrap_or_else(|e| perror_and_exit("Debugger::scan_tail_pointer", e));
        let stream = stream::iter(iter).map_err(|e| e.to_string());
        Box::pin(stream)
    }

    fn raw_scan_impl(&self, from_key: &[u8], lightlike_key: &[u8], limit: usize, causet: &str) {
        let res = self
            .raw_scan(from_key, lightlike_key, limit, causet)
            .unwrap_or_else(|e| perror_and_exit("Debugger::raw_scan_impl", e));

        for (k, v) in &res {
            println!("key: \"{}\", value: \"{}\"", escape(k), escape(v));
        }
        println!();
        println!("Total scanned tuplespaceInstanton: {}", res.len());
    }

    fn do_compaction(
        &self,
        db: DBType,
        causet: &str,
        from: &[u8],
        to: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        self.compact(db, causet, from, to, threads, bottommost)
            .unwrap_or_else(|e| perror_and_exit("Debugger::compact", e));
    }

    fn set_brane_tombstone(&self, branes: Vec<Brane>) {
        let ret = self
            .set_brane_tombstone(branes)
            .unwrap_or_else(|e| perror_and_exit("Debugger::set_brane_tombstone", e));
        if ret.is_empty() {
            v1!("success!");
            return;
        }
        for (brane_id, error) in ret {
            ve1!("brane: {}, error: {}", brane_id, error);
        }
    }

    fn set_brane_tombstone_by_id(&self, brane_ids: Vec<u64>) {
        let ret = self
            .set_brane_tombstone_by_id(brane_ids)
            .unwrap_or_else(|e| perror_and_exit("Debugger::set_brane_tombstone_by_id", e));
        if ret.is_empty() {
            v1!("success!");
            return;
        }
        for (brane_id, error) in ret {
            ve1!("brane: {}, error: {}", brane_id, error);
        }
    }

    fn recover_branes(&self, branes: Vec<Brane>, read_only: bool) {
        let ret = self
            .recover_branes(branes, read_only)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recover branes", e));
        if ret.is_empty() {
            v1!("success!");
            return;
        }
        for (brane_id, error) in ret {
            ve1!("brane: {}, error: {}", brane_id, error);
        }
    }

    fn recover_all(&self, threads: usize, read_only: bool) {
        Debugger::recover_all(self, threads, read_only)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recover all", e));
    }

    fn print_bad_branes(&self) {
        let bad_branes = self
            .bad_branes()
            .unwrap_or_else(|e| perror_and_exit("Debugger::bad_branes", e));
        if !bad_branes.is_empty() {
            for (brane_id, error) in bad_branes {
                v1!("{}: {}", brane_id, error);
            }
            return;
        }
        v1!("all branes are healthy")
    }

    fn remove_fail_stores(&self, store_ids: Vec<u64>, brane_ids: Option<Vec<u64>>) {
        v1!("removing stores {:?} from configurations...", store_ids);
        self.remove_failed_stores(store_ids, brane_ids)
            .unwrap_or_else(|e| perror_and_exit("Debugger::remove_fail_stores", e));
        v1!("success");
    }

    fn recreate_brane(&self, mgr: Arc<SecurityManager>, fidel_causet: &FidelConfig, brane_id: u64) {
        let rpc_client =
            RpcClient::new(fidel_causet, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let mut brane = match block_on(rpc_client.get_brane_by_id(brane_id)) {
            Ok(Some(brane)) => brane,
            Ok(None) => {
                ve1!("no such brane {} on FIDel", brane_id);
                process::exit(-1)
            }
            Err(e) => perror_and_exit("RpcClient::get_brane_by_id", e),
        };

        let new_brane_id = rpc_client
            .alloc_id()
            .unwrap_or_else(|e| perror_and_exit("RpcClient::alloc_id", e));
        let new_peer_id = rpc_client
            .alloc_id()
            .unwrap_or_else(|e| perror_and_exit("RpcClient::alloc_id", e));

        let store_id = self.get_store_id().expect("get store id");

        brane.set_id(new_brane_id);
        let old_version = brane.get_brane_epoch().get_version();
        brane.mut_brane_epoch().set_version(old_version + 1);
        brane.mut_brane_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);

        brane.peers.clear();
        let mut peer = Peer::default();
        peer.set_id(new_peer_id);
        peer.set_store_id(store_id);
        brane.mut_peers().push(peer);

        v1!(
            "initing empty brane {} with peer_id {}...",
            new_brane_id,
            new_peer_id
        );
        self.recreate_brane(brane)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recreate_brane", e));
        v1!("success");
    }

    fn dump_metrics(&self, _tags: Vec<&str>) {
        unimplemented!("only available for online mode");
    }

    fn check_brane_consistency(&self, _: u64) {
        ve1!("only support remote mode");
        process::exit(-1);
    }

    fn modify_edb_config(&self, _: &str, _: &str) {
        ve1!("only support remote mode");
        process::exit(-1);
    }

    fn dump_brane_properties(&self, brane_id: u64) {
        let props = self
            .get_brane_properties(brane_id)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_brane_properties", e));
        for (name, value) in props {
            v1!("{}: {}", name, value);
        }
    }

    fn dump_cone_properties(&self, spacelike: Vec<u8>, lightlike: Vec<u8>) {
        let props = self
            .get_cone_properties(&spacelike, &lightlike)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_cone_properties", e));
        for (name, value) in props {
            v1!("{}: {}", name, value);
        }
    }

    fn dump_store_info(&self) {
        let store_id = self.get_store_id();
        if let Ok(id) = store_id {
            v1!("store id: {}", id);
        }
    }

    fn dump_cluster_info(&self) {
        let cluster_id = self.get_cluster_id();
        if let Ok(id) = cluster_id {
            v1!("cluster id: {}", id);
        }
    }
}

fn warning_prompt(message: &str) -> bool {
    const EXPECTED: &str = "I consent";
    println!("{}", message);
    let input: String = promptly::prompt(format!(
        "Type \"{}\" to continue, anything else to exit",
        EXPECTED
    ))
    .unwrap();
    if input == EXPECTED {
        true
    } else {
        println!("exit.");
        false
    }
}

fn main() {
    vlog::set_verbosity_level(1);

    let raw_key_hint: &'static str = "Raw key (generally spacelikes with \"z\") in escaped form";
    let version_info = edb::edb_version_info();

    let mut app = App::new("EinsteinDB Control (edb-ctl)")
        .about("A tool for interacting with EinsteinDB deployments.")
        .author(crate_authors!())
        .version(version_info.as_ref())
        .long_version(version_info.as_ref())
        .setting(AppSettings::AllowExternalSubcommands)
        .arg(
            Arg::with_name("db")
                .long("db")
                .takes_value(true)
                .help("Set the lmdb path"),
        )
        .arg(
            Arg::with_name("violetabftdb")
                .long("violetabftdb")
                .takes_value(true)
                .help("Set the violetabft lmdb path"),
        )
        .arg(
            Arg::with_name("skip-paranoid-checks")
                .required(false)
                .long("skip-paranoid-checks")
                .takes_value(false)
                .help("Skip paranoid checks when open lmdb"),
        )
        .arg(
            Arg::with_name("config")
                .long("config")
                .takes_value(true)
                .help("Set the config for lmdb"),
        )
        .arg(
            Arg::with_name("host")
                .long("host")
                .takes_value(true)
                .help("Set the remote host"),
        )
        .arg(
            Arg::with_name("ca_path")
                .required(false)
                .long("ca-path")
                .takes_value(true)
                .help("Set the CA certificate path"),
        )
        .arg(
            Arg::with_name("cert_path")
                .required(false)
                .long("cert-path")
                .takes_value(true)
                .help("Set the certificate path"),
        )
        .arg(
            Arg::with_name("key_path")
                .required(false)
                .long("key-path")
                .takes_value(true)
                .help("Set the private key path"),
        )
        .arg(
            Arg::with_name("hex-to-escaped")
                .conflicts_with("escaped-to-hex")
                .long("to-escaped")
                .takes_value(true)
                .help("Convert a hex key to escaped key"),
        )
        .arg(
            Arg::with_name("escaped-to-hex")
                .conflicts_with("hex-to-escaped")
                .long("to-hex")
                .takes_value(true)
                .help("Convert an escaped key to hex key"),
        )
        .arg(
            Arg::with_name("decode")
                .conflicts_with_all(&["hex-to-escaped", "escaped-to-hex"])
                .long("decode")
                .takes_value(true)
                .help("Decode a key in escaped format"),
        )
        .arg(
            Arg::with_name("encode")
                .conflicts_with_all(&["hex-to-escaped", "escaped-to-hex"])
                .long("encode")
                .takes_value(true)
                .help("Encode a key in escaped format"),
        )
        .arg(
            Arg::with_name("fidel")
                .long("fidel")
                .takes_value(true)
                .help("Set the address of fidel"),
        )
        .subcommand(
            SubCommand::with_name("violetabft")
                .about("Print a violetabft log entry")
                .subcommand(
                    SubCommand::with_name("log")
                        .about("Print the violetabft log entry info")
                        .arg(
                            Arg::with_name("brane")
                                .required_unless("key")
                                .conflicts_with("key")
                                .short("r")
                                .takes_value(true)
                                .help("Set the brane id"),
                        )
                        .arg(
                            Arg::with_name("index")
                                .required_unless("key")
                                .conflicts_with("key")
                                .short("i")
                                .takes_value(true)
                                .help("Set the violetabft log index"),
                        )
                        .arg(
                            Arg::with_name("key")
                                .required_unless_one(&["brane", "index"])
                                .conflicts_with_all(&["brane", "index"])
                                .short("k")
                                .takes_value(true)
                                .help(raw_key_hint)
                        ),
                )
                .subcommand(
                    SubCommand::with_name("brane")
                        .about("print brane info")
                        .arg(
                            Arg::with_name("brane")
                                .short("r")
                                .takes_value(true)
                                .help("Set the brane id, if not specified, print all branes"),
                        )
                        .arg(
                            Arg::with_name("skip-tombstone")
                                .long("skip-tombstone")
                                .takes_value(false)
                                .help("Skip tombstone branes"),
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("size")
                .about("Print brane size")
                .arg(
                    Arg::with_name("brane")
                        .short("r")
                        .takes_value(true)
                        .help("Set the brane id, if not specified, print all branes"),
                )
                .arg(
                    Arg::with_name("causet")
                        .short("c")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .default_value("default,write,dagger")
                        .help("Set the causet name, if not specified, print all causet"),
                ),
        )
        .subcommand(
            SubCommand::with_name("scan")
                .about("Print the cone db cone")
                .arg(
                    Arg::with_name("from")
                        .required(true)
                        .short("f")
                        .long("from")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .long("to")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("limit")
                        .long("limit")
                        .takes_value(true)
                        .help("Set the scan limit"),
                )
                .arg(
                    Arg::with_name("spacelike_ts")
                        .long("spacelike-ts")
                        .takes_value(true)
                        .help("Set the scan spacelike_ts as filter"),
                )
                .arg(
                    Arg::with_name("commit_ts")
                        .long("commit-ts")
                        .takes_value(true)
                        .help("Set the scan commit_ts as filter"),
                )
                .arg(
                    Arg::with_name("show-causet")
                        .long("show-causet")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .default_value(Causet_DEFAULT)
                        .help("PrimaryCauset family names, combined from default/dagger/write"),
                ),
        )
        .subcommand(
            SubCommand::with_name("raw-scan")
                .about("Print all raw tuplespaceInstanton in the cone")
                .arg(
                    Arg::with_name("from")
                        .short("f")
                        .long("from")
                        .takes_value(true)
                        .default_value("")
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .long("to")
                        .takes_value(true)
                        .default_value("")
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("limit")
                        .long("limit")
                        .takes_value(true)
                        .default_value("30")
                        .help("Limit the number of tuplespaceInstanton to scan")
                )
                .arg(
                    Arg::with_name("causet")
                        .long("causet")
                        .takes_value(true)
                        .default_value("default")
                        .possible_values(&[
                            "default", "dagger", "write"
                        ])
                        .help("The PrimaryCauset family name.")
                )
        )
        .subcommand(
            SubCommand::with_name("print")
                .about("Print the raw value")
                .arg(
                    Arg::with_name("causet")
                        .short("c")
                        .takes_value(true)
                        .default_value(Causet_DEFAULT)
                        .possible_values(&[
                            "default", "dagger", "write"
                        ])
                        .help("The PrimaryCauset family name.")
                )
                .arg(
                    Arg::with_name("key")
                        .required(true)
                        .short("k")
                        .takes_value(true)
                        .help(raw_key_hint)
                ),
        )
        .subcommand(
            SubCommand::with_name("tail_pointer")
                .about("Print the tail_pointer value")
                .arg(
                    Arg::with_name("key")
                        .required(true)
                        .short("k")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("show-causet")
                        .long("show-causet")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .default_value(Causet_DEFAULT)
                        .help("PrimaryCauset family names, combined from default/dagger/write"),
                )
                .arg(
                    Arg::with_name("spacelike_ts")
                        .long("spacelike-ts")
                        .takes_value(true)
                        .help("Set spacelike_ts as filter"),
                )
                .arg(
                    Arg::with_name("commit_ts")
                        .long("commit-ts")
                        .takes_value(true)
                        .help("Set commit_ts as filter"),
                ),
        )
        .subcommand(
            SubCommand::with_name("diff")
                .about("Calculate difference of brane tuplespaceInstanton from different dbs")
                .arg(
                    Arg::with_name("brane")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .help("Specify brane id"),
                )
                .arg(
                    Arg::with_name("to_db")
                        .required_unless("to_host")
                        .conflicts_with("to_host")
                        .long("to-db")
                        .takes_value(true)
                        .help("To which db path"),
                )
                .arg(
                    Arg::with_name("to_host")
                        .required_unless("to_db")
                        .conflicts_with("to_db")
                        .long("to-host")
                        .takes_value(true)
                        .conflicts_with("to_db")
                        .help("To which remote host"),
                ),
        )
        .subcommand(
            SubCommand::with_name("compact")
                .about("Compact a PrimaryCauset family in a specified cone")
                .arg(
                    Arg::with_name("db")
                        .short("d")
                        .takes_value(true)
                        .default_value("kv")
                        .possible_values(&[
                            "kv", "violetabft",
                        ])
                        .help("Which db to compact"),
                )
                .arg(
                    Arg::with_name("causet")
                        .short("c")
                        .takes_value(true)
                        .default_value(Causet_DEFAULT)
                        .possible_values(&[
                            "default", "dagger", "write"
                        ])
                        .help("The PrimaryCauset family name"),
                )
                .arg(
                    Arg::with_name("from")
                        .short("f")
                        .long("from")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .long("to")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("threads")
                        .short("n")
                        .long("threads")
                        .takes_value(true)
                        .default_value("8")
                        .help("Number of threads in one compaction")
                )
                .arg(
                    Arg::with_name("brane")
                        .short("r")
                        .long("brane")
                        .takes_value(true)
                        .help("Set the brane id"),
                )
                .arg(
                    Arg::with_name("bottommost")
                        .short("b")
                        .long("bottommost")
                        .takes_value(true)
                        .default_value("default")
                        .possible_values(&["skip", "force", "default"])
                        .help("Set how to compact the bottommost level"),
                ),
        )
        .subcommand(
            SubCommand::with_name("tombstone")
                .about("Set some branes on the node to tombstone by manual")
                .arg(
                    Arg::with_name("branes")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("The target branes, separated with commas if multiple"),
                )
                .arg(
                    Arg::with_name("fidel")
                        .short("p")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("FIDel lightlikepoints"),
                )
                .arg(
                    Arg::with_name("force")
                        .long("force")
                        .takes_value(false)
                        .help("force execute without fidel"),
                ),
        )
        .subcommand(
            SubCommand::with_name("recover-tail_pointer")
                .about("Recover tail_pointer data on one node by deleting corrupted tuplespaceInstanton")
                .arg(
                    Arg::with_name("all")
                        .short("a")
                        .long("all")
                        .takes_value(false)
                        .help("Recover the whole db"),
                )
                .arg(
                    Arg::with_name("branes")
                        .required_unless("all")
                        .conflicts_with("all")
                        .short("r")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("The target branes, separated with commas if multiple"),
                )
                .arg(
                    Arg::with_name("fidel")
                        .required_unless("all")
                        .short("p")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("FIDel lightlikepoints"),
                )
                .arg(
                    Arg::with_name("threads")
                        .long("threads")
                        .takes_value(true)
                        .default_value_if("all", None, "4")
                        .requires("all")
                        .help("The number of threads to do recover, only for --all mode"),
                )
                .arg(
                    Arg::with_name("read-only")
                        .short("R")
                        .long("read-only")
                        .help("Skip write Lmdb"),
                ),
        )
        .subcommand(
            SubCommand::with_name("unsafe-recover")
                .about("Unsafely recover the cluster when the majority replicas are failed")
                .subcommand(
                    SubCommand::with_name("remove-fail-stores")
                        .arg(
                            Arg::with_name("stores")
                                .required(true)
                                .short("s")
                                .takes_value(true)
                                .multiple(true)
                                .use_delimiter(true)
                                .require_delimiter(true)
                                .value_delimiter(",")
                                .help("Stores to be removed"),
                        )
                        .arg(
                            Arg::with_name("branes")
                                .required_unless("all-branes")
                                .conflicts_with("all-branes")
                                .takes_value(true)
                                .short("r")
                                .multiple(true)
                                .use_delimiter(true)
                                .require_delimiter(true)
                                .value_delimiter(",")
                                .help("Only for these branes"),
                        )
                        .arg(
                            Arg::with_name("all-branes")
                                .required_unless("branes")
                                .conflicts_with("branes")
                                .long("all-branes")
                                .takes_value(false)
                                .help("Do the command for all branes"),
                        )
                ),
        )
        .subcommand(
            SubCommand::with_name("recreate-brane")
                .about("Recreate a brane with given metadata, but alloc new id for it")
                .arg(
                    Arg::with_name("fidel")
                        .required(true)
                        .short("p")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("FIDel lightlikepoints"),
                )
                .arg(
                    Arg::with_name("brane")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .help("The origin brane id"),
                ),
        )
        .subcommand(
            SubCommand::with_name("metrics")
                .about("Print the metrics")
                .arg(
                    Arg::with_name("tag")
                        .short("t")
                        .long("tag")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .default_value(METRICS_PROMETHEUS)
                        .possible_values(&[
                            "prometheus", "jemalloc", "lmdb_violetabft", "lmdb_kv",
                        ])
                        .help(
                            "Set the metrics tag, one of prometheus/jemalloc/lmdb_violetabft/lmdb_kv, if not specified, print prometheus",
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("consistency-check")
                .about("Force a consistency-check for a specified brane")
                .arg(
                    Arg::with_name("brane")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .help("The target brane"),
                ),
        )
        .subcommand(SubCommand::with_name("bad-branes").about("Get all branes with corrupt violetabft"))
        .subcommand(
            SubCommand::with_name("modify-edb-config")
                .about("Modify edb config, eg. edb-ctl --host ip:port modify-edb-config -n lmdb.defaultcauset.disable-auto-compactions -v true")
                .arg(
                    Arg::with_name("config_name")
                        .required(true)
                        .short("n")
                        .takes_value(true)
                        .help("The config name are same as the name used on config file, eg. violetabftstore.messages-per-tick, violetabftdb.max-background-jobs"),
                )
                .arg(
                    Arg::with_name("config_value")
                        .required(true)
                        .short("v")
                        .takes_value(true)
                        .help("The config value, eg. 8, true, 1h, 8MB"),
                ),
        )
        .subcommand(
            SubCommand::with_name("dump-snap-meta")
                .about("Dump snapshot meta file")
                .arg(
                    Arg::with_name("file")
                        .required(true)
                        .short("f")
                        .long("file")
                        .takes_value(true)
                        .help("Output meta file path"),
                ),
        )
        .subcommand(
            SubCommand::with_name("compact-cluster")
                .about("Compact the whole cluster in a specified cone in one or more PrimaryCauset families")
                .arg(
                    Arg::with_name("db")
                        .short("d")
                        .takes_value(true)
                        .default_value("kv")
                        .possible_values(&["kv", "violetabft"])
                        .help("The db to use"),
                )
                .arg(
                    Arg::with_name("causet")
                        .short("c")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .default_value(Causet_DEFAULT)
                        .possible_values(&["default", "dagger", "write"])
                        .help("PrimaryCauset family names, for kv db, combine from default/dagger/write; for violetabft db, can only be default"),
                )
                .arg(
                    Arg::with_name("from")
                        .short("f")
                        .long("from")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .long("to")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("threads")
                        .short("n")
                        .long("threads")
                        .takes_value(true)
                        .default_value("8")
                        .help("Number of threads in one compaction")
                )
                .arg(
                    Arg::with_name("bottommost")
                        .short("b")
                        .long("bottommost")
                        .takes_value(true)
                        .default_value("default")
                        .possible_values(&["skip", "force", "default"])
                        .help("How to compact the bottommost level"),
                ),
        )
        .subcommand(
            SubCommand::with_name("brane-properties")
                .about("Show brane properties")
                .arg(
                    Arg::with_name("brane")
                        .short("r")
                        .required(true)
                        .takes_value(true)
                        .help("The target brane id"),
                ),
        )
        .subcommand(
            SubCommand::with_name("cone-properties")
                .about("Show cone properties")
                .arg(
                    Arg::with_name("spacelike")
                        .long("spacelike")
                        .required(true)
                        .takes_value(true)
                        .default_value("")
                        .help("hex spacelike key"),
                )
                .arg(
                    Arg::with_name("lightlike")
                        .long("lightlike")
                        .required(true)
                        .takes_value(true)
                        .default_value("")
                        .help("hex lightlike key"),
                ),
        )
        .subcommand(
            SubCommand::with_name("split-brane")
                .about("Split the brane")
                .arg(
                    Arg::with_name("brane")
                        .short("r")
                        .required(true)
                        .takes_value(true)
                        .help("The target brane id")
                )
                .arg(
                    Arg::with_name("key")
                        .short("k")
                        .required(true)
                        .takes_value(true)
                        .help("The key to split it, in unencoded escaped format")
                ),
        )
        .subcommand(
            SubCommand::with_name("fail")
                .about("Inject failures to EinsteinDB and recovery")
                .subcommand(
                    SubCommand::with_name("inject")
                        .about("Inject failures")
                        .arg(
                            Arg::with_name("args")
                                .multiple(true)
                                .takes_value(true)
                                .help(
                                    "Inject fail point and actions pairs.\
                                E.g. edb-ctl fail inject a=off b=panic",
                                ),
                        )
                        .arg(
                            Arg::with_name("file")
                                .short("f")
                                .takes_value(true)
                                .help("Read a file of fail points and actions to inject"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("recover")
                        .about("Recover failures")
                        .arg(
                            Arg::with_name("args")
                                .multiple(true)
                                .takes_value(true)
                                .help("Recover fail points. Eg. edb-ctl fail recover a b"),
                        )
                        .arg(
                            Arg::with_name("file")
                                .short("f")
                                .takes_value(true)
                                .help("Recover from a file of fail points"),
                        ),
                )
                .subcommand(SubCommand::with_name("list").about("List all fail points"))
        )
        .subcommand(
            SubCommand::with_name("store")
                .about("Print the store id"),
        )
        .subcommand(
            SubCommand::with_name("cluster")
                .about("Print the cluster id"),
        )
        .subcommand(
            SubCommand::with_name("decrypt-file")
                .about("Decrypt an encrypted file")
                .arg(
                    Arg::with_name("file")
                        .long("file")
                        .takes_value(true)
                        .required(true)
                        .help("input file path"),
                )
                .arg(
                    Arg::with_name("out-file")
                        .long("out-file")
                        .takes_value(true)
                        .required(true)
                        .help("output file path"),
                ),
        )
        .subcommand(
            SubCommand::with_name("encryption-meta")
                .about("Dump encryption metadata")
                .subcommand(
                    SubCommand::with_name("dump-key")
                        .about("Dump data tuplespaceInstanton")
                        .arg(
                            Arg::with_name("ids")
                                .long("ids")
                                .takes_value(true)
                                .use_delimiter(true)
                                .help("List of data key ids. Dump all tuplespaceInstanton if not provided."),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("dump-file")
                        .about("Dump file encryption info")
                        .arg(
                            Arg::with_name("path")
                                .long("path")
                                .takes_value(true)
                                .help("Path to the file. Dump for all files if not provided."),
                        ),
                ),
        );

    let matches = app.clone().get_matches();

    // Initialize configuration and security manager.
    let causet_path = matches.value_of("config");
    let causet = causet_path.map_or_else(EINSTEINDBConfig::default, |path| {
        let s = fs::read_to_string(&path).unwrap();
        toml::from_str(&s).unwrap()
    });
    let mgr = new_security_mgr(&matches);

    // Bypass the ldb command to Lmdb.
    if let Some(cmd) = matches.subcommand_matches("ldb") {
        run_ldb_command(&cmd, &causet);
        return;
    }

    // Deal with subcommand dump-snap-meta. This subcommand doesn't require other args, so process
    // it before checking args.
    if let Some(matches) = matches.subcommand_matches("dump-snap-meta") {
        let path = matches.value_of("file").unwrap();
        return dump_snap_meta_file(path);
    }

    if matches.args.is_empty() {
        let _ = app.print_help();
        v1!("");
        return;
    }

    // Deal with arguments about key utils.
    if let Some(hex) = matches.value_of("hex-to-escaped") {
        v1!("{}", escape(&from_hex(hex).unwrap()));
        return;
    } else if let Some(escaped) = matches.value_of("escaped-to-hex") {
        v1!("{}", hex::encode_upper(unescape(escaped)));
        return;
    } else if let Some(encoded) = matches.value_of("decode") {
        match Key::from_encoded(unescape(encoded)).into_raw() {
            Ok(k) => v1!("{}", escape(&k)),
            Err(e) => ve1!("decode meets error: {}", e),
        };
        return;
    } else if let Some(decoded) = matches.value_of("encode") {
        v1!("{}", Key::from_raw(&unescape(decoded)));
        return;
    }

    if let Some(matches) = matches.subcommand_matches("decrypt-file") {
        let message = "This action will expose sensitive data as plaintext on persistent causet_storage";
        if !warning_prompt(message) {
            return;
        }
        let infile = matches.value_of("file").unwrap();
        let outfile = matches.value_of("out-file").unwrap();
        v1!("infile: {}, outfile: {}", infile, outfile);

        let key_manager =
            match DataKeyManager::from_config(&causet.security.encryption, &causet.causet_storage.data_dir)
                .expect("DataKeyManager::from_config should success")
            {
                Some(mgr) => mgr,
                None => {
                    v1!("Encryption is disabled");
                    v1!("crc32: {}", calc_crc32(infile).unwrap());
                    return;
                }
            };

        let infile1 = Path::new(infile).canonicalize().unwrap();
        let file_info = key_manager.get_file(infile1.to_str().unwrap()).unwrap();

        let mthd = encryption_method_from_db_encryption_method(file_info.method);
        if mthd == EncryptionMethod::Plaintext {
            v1!(
                "{} is not encrypted, skip to decrypt it into {}",
                infile,
                outfile
            );
            v1!("crc32: {}", calc_crc32(infile).unwrap());
            return;
        }

        let mut outf = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(outfile)
            .unwrap();

        let iv = Iv::from_slice(&file_info.iv).unwrap();
        let f = File::open(&infile).unwrap();
        let mut reader = DecrypterReader::new(f, mthd, &file_info.key, iv).unwrap();

        io::copy(&mut reader, &mut outf).unwrap();
        v1!("crc32: {}", calc_crc32(outfile).unwrap());
        return;
    }

    if let Some(matches) = matches.subcommand_matches("encryption-meta") {
        match matches.subcommand() {
            ("dump-key", Some(matches)) => {
                let message =
                    "This action will expose encryption key(s) as plaintext. Do not output the \
                    result in file on disk.";
                if !warning_prompt(message) {
                    return;
                }
                DataKeyManager::dump_key_dict(
                    &causet.security.encryption,
                    &causet.causet_storage.data_dir,
                    matches
                        .values_of("ids")
                        .map(|ids| ids.map(|id| id.parse::<u64>().unwrap()).collect()),
                )
                .unwrap();
            }
            ("dump-file", Some(matches)) => {
                let path = matches
                    .value_of("path")
                    .map(|path| fs::canonicalize(path).unwrap().to_str().unwrap().to_owned());
                DataKeyManager::dump_file_dict(&causet.causet_storage.data_dir, path.as_deref()).unwrap();
            }
            _ => ve1!("{}", matches.usage()),
        }
        return;
    }

    // Deal with all subcommands needs FIDel.
    if let Some(fidel) = matches.value_of("fidel") {
        let fidel_client = get_fidel_rpc_client(fidel, Arc::clone(&mgr));
        if let Some(matches) = matches.subcommand_matches("compact-cluster") {
            let db = matches.value_of("db").unwrap();
            let db_type = if db == "kv" { DBType::Kv } else { DBType::VioletaBft };
            let causets = Vec::from_iter(matches.values_of("causet").unwrap());
            let from_key = matches.value_of("from").map(|k| unescape(k));
            let to_key = matches.value_of("to").map(|k| unescape(k));
            let threads = value_t_or_exit!(matches.value_of("threads"), u32);
            let bottommost = BottommostLevelCompaction::from(matches.value_of("bottommost"));
            return compact_whole_cluster(
                &fidel_client, &causet, mgr, db_type, causets, from_key, to_key, threads, bottommost,
            );
        }
        if let Some(matches) = matches.subcommand_matches("split-brane") {
            let brane_id = value_t_or_exit!(matches.value_of("brane"), u64);
            let key = unescape(matches.value_of("key").unwrap());
            return split_brane(&fidel_client, mgr, brane_id, key);
        }

        let _ = app.print_help();
        return;
    }

    // Deal with all subcommands about db or host.
    let db = matches.value_of("db");
    let ignore_failover = matches.is_present("skip-paranoid-checks");
    let violetabft_db = matches.value_of("violetabftdb");
    let host = matches.value_of("host");

    let debug_executor = new_debug_executor(
        db,
        violetabft_db,
        ignore_failover,
        host,
        &causet,
        Arc::clone(&mgr),
    );

    if let Some(matches) = matches.subcommand_matches("print") {
        let causet = matches.value_of("causet").unwrap();
        let key = unescape(matches.value_of("key").unwrap());
        debug_executor.dump_value(causet, key);
    } else if let Some(matches) = matches.subcommand_matches("violetabft") {
        if let Some(matches) = matches.subcommand_matches("log") {
            let (id, index) = if let Some(key) = matches.value_of("key") {
                tuplespaceInstanton::decode_violetabft_log_key(&unescape(key)).unwrap()
            } else {
                let id = matches.value_of("brane").unwrap().parse().unwrap();
                let index = matches.value_of("index").unwrap().parse().unwrap();
                (id, index)
            };
            debug_executor.dump_violetabft_log(id, index);
        } else if let Some(matches) = matches.subcommand_matches("brane") {
            let skip_tombstone = matches.is_present("skip-tombstone");
            if let Some(id) = matches.value_of("brane") {
                debug_executor.dump_brane_info(id.parse().unwrap(), skip_tombstone);
            } else {
                debug_executor.dump_all_brane_info(skip_tombstone);
            }
        } else {
            let _ = app.print_help();
        }
    } else if let Some(matches) = matches.subcommand_matches("size") {
        let causets = Vec::from_iter(matches.values_of("causet").unwrap());
        if let Some(id) = matches.value_of("brane") {
            debug_executor.dump_brane_size(id.parse().unwrap(), causets);
        } else {
            debug_executor.dump_all_brane_size(causets);
        }
    } else if let Some(matches) = matches.subcommand_matches("scan") {
        let from = unescape(matches.value_of("from").unwrap());
        let to = matches
            .value_of("to")
            .map_or_else(|| vec![], |to| unescape(to));
        let limit = matches
            .value_of("limit")
            .map_or(0, |s| s.parse().expect("parse u64"));
        if to.is_empty() && limit == 0 {
            ve1!(r#"please pass "to" or "limit""#);
            process::exit(-1);
        }
        let causets = Vec::from_iter(matches.values_of("show-causet").unwrap());
        let spacelike_ts = matches.value_of("spacelike_ts").map(|s| s.parse().unwrap());
        let commit_ts = matches.value_of("commit_ts").map(|s| s.parse().unwrap());
        debug_executor.dump_tail_pointers_infos(from, to, limit, causets, spacelike_ts, commit_ts);
    } else if let Some(matches) = matches.subcommand_matches("raw-scan") {
        let from = unescape(matches.value_of("from").unwrap());
        let to = unescape(matches.value_of("to").unwrap());
        let limit: usize = matches.value_of("limit").unwrap().parse().unwrap();
        let causet = matches.value_of("causet").unwrap();
        debug_executor.raw_scan(&from, &to, limit, causet);
    } else if let Some(matches) = matches.subcommand_matches("tail_pointer") {
        let from = unescape(matches.value_of("key").unwrap());
        let causets = Vec::from_iter(matches.values_of("show-causet").unwrap());
        let spacelike_ts = matches.value_of("spacelike_ts").map(|s| s.parse().unwrap());
        let commit_ts = matches.value_of("commit_ts").map(|s| s.parse().unwrap());
        debug_executor.dump_tail_pointers_infos(from, vec![], 0, causets, spacelike_ts, commit_ts);
    } else if let Some(matches) = matches.subcommand_matches("diff") {
        let brane = matches.value_of("brane").unwrap().parse().unwrap();
        let to_db = matches.value_of("to_db");
        let to_host = matches.value_of("to_host");
        debug_executor.diff_brane(brane, to_db, None, to_host, &causet, mgr);
    } else if let Some(matches) = matches.subcommand_matches("compact") {
        let db = matches.value_of("db").unwrap();
        let db_type = if db == "kv" { DBType::Kv } else { DBType::VioletaBft };
        let causet = matches.value_of("causet").unwrap();
        let from_key = matches.value_of("from").map(|k| unescape(k));
        let to_key = matches.value_of("to").map(|k| unescape(k));
        let threads = value_t_or_exit!(matches.value_of("threads"), u32);
        let bottommost = BottommostLevelCompaction::from(matches.value_of("bottommost"));
        if let Some(brane) = matches.value_of("brane") {
            debug_executor.compact_brane(
                host,
                db_type,
                causet,
                brane.parse().unwrap(),
                threads,
                bottommost,
            );
        } else {
            debug_executor.compact(host, db_type, causet, from_key, to_key, threads, bottommost);
        }
    } else if let Some(matches) = matches.subcommand_matches("tombstone") {
        let branes = matches
            .values_of("branes")
            .unwrap()
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()
            .expect("parse branes fail");
        if let Some(fidel_urls) = matches.values_of("fidel") {
            let fidel_urls = Vec::from_iter(fidel_urls.map(ToOwned::to_owned));
            let mut causet = FidelConfig::default();
            causet.lightlikepoints = fidel_urls;
            if let Err(e) = causet.validate() {
                panic!("invalid fidel configuration: {:?}", e);
            }
            debug_executor.set_brane_tombstone_after_remove_peer(mgr, &causet, branes);
        } else {
            assert!(matches.is_present("force"));
            debug_executor.set_brane_tombstone_force(branes);
        }
    } else if let Some(matches) = matches.subcommand_matches("recover-tail_pointer") {
        let read_only = matches.is_present("read-only");
        if matches.is_present("all") {
            let threads = matches
                .value_of("threads")
                .unwrap()
                .parse::<usize>()
                .unwrap();
            if threads == 0 {
                panic!("Number of threads can't be 0");
            }
            v1!(
                "Recover all, threads: {}, read_only: {}",
                threads,
                read_only
            );
            debug_executor.recover_tail_pointer_all(threads, read_only);
        } else {
            let branes = matches
                .values_of("branes")
                .unwrap()
                .map(str::parse)
                .collect::<Result<Vec<_>, _>>()
                .expect("parse branes fail");
            let fidel_urls = Vec::from_iter(matches.values_of("fidel").unwrap().map(ToOwned::to_owned));
            let mut causet = FidelConfig::default();
            v1!(
                "Recover branes: {:?}, fidel: {:?}, read_only: {}",
                branes,
                fidel_urls,
                read_only
            );
            causet.lightlikepoints = fidel_urls;
            if let Err(e) = causet.validate() {
                panic!("invalid fidel configuration: {:?}", e);
            }
            debug_executor.recover_branes_tail_pointer(mgr, &causet, branes, read_only);
        }
    } else if let Some(matches) = matches.subcommand_matches("unsafe-recover") {
        if let Some(matches) = matches.subcommand_matches("remove-fail-stores") {
            let store_ids = values_t!(matches, "stores", u64).expect("parse stores fail");
            let brane_ids = matches.values_of("branes").map(|ids| {
                ids.map(str::parse)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("parse branes fail")
            });
            debug_executor.remove_fail_stores(store_ids, brane_ids);
        } else {
            ve1!("{}", matches.usage());
        }
    } else if let Some(matches) = matches.subcommand_matches("recreate-brane") {
        let mut fidel_causet = FidelConfig::default();
        fidel_causet.lightlikepoints = Vec::from_iter(matches.values_of("fidel").unwrap().map(ToOwned::to_owned));
        let brane_id = matches.value_of("brane").unwrap().parse().unwrap();
        debug_executor.recreate_brane(mgr, &fidel_causet, brane_id);
    } else if let Some(matches) = matches.subcommand_matches("consistency-check") {
        let brane_id = matches.value_of("brane").unwrap().parse().unwrap();
        debug_executor.check_brane_consistency(brane_id);
    } else if matches.subcommand_matches("bad-branes").is_some() {
        debug_executor.print_bad_branes();
    } else if let Some(matches) = matches.subcommand_matches("modify-edb-config") {
        let config_name = matches.value_of("config_name").unwrap();
        let config_value = matches.value_of("config_value").unwrap();
        debug_executor.modify_edb_config(config_name, config_value);
    } else if let Some(matches) = matches.subcommand_matches("metrics") {
        let tags = Vec::from_iter(matches.values_of("tag").unwrap());
        debug_executor.dump_metrics(tags)
    } else if let Some(matches) = matches.subcommand_matches("brane-properties") {
        let brane_id = value_t_or_exit!(matches.value_of("brane"), u64);
        debug_executor.dump_brane_properties(brane_id)
    } else if let Some(matches) = matches.subcommand_matches("cone-properties") {
        let spacelike_key = from_hex(matches.value_of("spacelike").unwrap()).unwrap();
        let lightlike_key = from_hex(matches.value_of("lightlike").unwrap()).unwrap();
        debug_executor.dump_cone_properties(spacelike_key, lightlike_key);
    } else if let Some(matches) = matches.subcommand_matches("fail") {
        if host.is_none() {
            ve1!("command fail requires host");
            process::exit(-1);
        }
        let client = new_debug_client(host.unwrap(), mgr);
        if let Some(matches) = matches.subcommand_matches("inject") {
            let mut list = matches
                .value_of("file")
                .map_or_else(Vec::new, read_fail_file);
            if let Some(ps) = matches.values_of("args") {
                for pair in ps {
                    let mut parts = pair.split('=');
                    list.push((
                        parts.next().unwrap().to_owned(),
                        parts.next().unwrap_or("").to_owned(),
                    ))
                }
            }
            for (name, actions) in list {
                if actions.is_empty() {
                    v1!("No action for fail point {}", name);
                    continue;
                }
                let mut inject_req = InjectFailPointRequest::default();
                inject_req.set_name(name);
                inject_req.set_actions(actions);

                let option = CallOption::default().timeout(Duration::from_secs(10));
                client.inject_fail_point_opt(&inject_req, option).unwrap();
            }
        } else if let Some(matches) = matches.subcommand_matches("recover") {
            let mut list = matches
                .value_of("file")
                .map_or_else(Vec::new, read_fail_file);
            if let Some(fps) = matches.values_of("args") {
                for fp in fps {
                    list.push((fp.to_owned(), "".to_owned()))
                }
            }
            for (name, _) in list {
                let mut recover_req = RecoverFailPointRequest::default();
                recover_req.set_name(name);
                let option = CallOption::default().timeout(Duration::from_secs(10));
                client.recover_fail_point_opt(&recover_req, option).unwrap();
            }
        } else if matches.is_present("list") {
            let list_req = ListFailPointsRequest::default();
            let option = CallOption::default().timeout(Duration::from_secs(10));
            let resp = client.list_fail_points_opt(&list_req, option).unwrap();
            v1!("{:?}", resp.get_entries());
        }
    } else if matches.subcommand_matches("store").is_some() {
        debug_executor.dump_store_info();
    } else if matches.subcommand_matches("cluster").is_some() {
        debug_executor.dump_cluster_info();
    } else {
        let _ = app.print_help();
    }
}

fn from_hex(key: &str) -> Result<Vec<u8>, hex::FromHexError> {
    if key.spacelikes_with("0x") || key.spacelikes_with("0X") {
        return hex::decode(&key[2..]);
    }
    hex::decode(key)
}

fn convert_gbmb(mut bytes: u64) -> String {
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    if bytes < MB {
        return format!("{} B", bytes);
    }
    let mb = if bytes % GB == 0 {
        String::from("")
    } else {
        format!("{:.3} MB", (bytes % GB) as f64 / MB as f64)
    };
    bytes /= GB;
    let gb = if bytes == 0 {
        String::from("")
    } else {
        format!("{} GB ", bytes)
    };
    format!("{}{}", gb, mb)
}

fn new_security_mgr(matches: &ArgMatches<'_>) -> Arc<SecurityManager> {
    let ca_path = matches.value_of("ca_path");
    let cert_path = matches.value_of("cert_path");
    let key_path = matches.value_of("key_path");

    let mut causet = SecurityConfig::default();
    if ca_path.is_none() && cert_path.is_none() && key_path.is_none() {
        return Arc::new(SecurityManager::new(&causet).unwrap());
    }

    if ca_path.is_some() || cert_path.is_some() || key_path.is_some() {
        if ca_path.is_none() || cert_path.is_none() || key_path.is_none() {
            panic!("CA certificate and private key should all be set.");
        }
        causet.ca_path = ca_path.unwrap().to_owned();
        causet.cert_path = cert_path.unwrap().to_owned();
        causet.key_path = key_path.unwrap().to_owned();
    }

    Arc::new(SecurityManager::new(&causet).expect("failed to initialize security manager"))
}

fn dump_snap_meta_file(path: &str) {
    let content =
        fs::read(path).unwrap_or_else(|e| panic!("read meta file {} failed, error {:?}", path, e));

    let mut meta = SnapshotMeta::default();
    meta.merge_from_bytes(&content)
        .unwrap_or_else(|e| panic!("parse from bytes error {:?}", e));
    for causet_file in meta.get_causet_files() {
        v1!(
            "causet {}, size {}, checksum: {}",
            causet_file.causet,
            causet_file.size,
            causet_file.checksum
        );
    }
}

fn get_fidel_rpc_client(fidel: &str, mgr: Arc<SecurityManager>) -> RpcClient {
    let mut causet = FidelConfig::default();
    causet.lightlikepoints.push(fidel.to_owned());
    causet.validate().unwrap();
    RpcClient::new(&causet, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e))
}

fn split_brane(fidel_client: &RpcClient, mgr: Arc<SecurityManager>, brane_id: u64, key: Vec<u8>) {
    let brane = block_on(fidel_client.get_brane_by_id(brane_id))
        .expect("get_brane_by_id should success")
        .expect("must have the brane");

    let leader = fidel_client
        .get_brane_info(brane.get_spacelike_key())
        .expect("get_brane_info should success")
        .leader
        .expect("brane must have leader");

    let store = fidel_client
        .get_store(leader.get_store_id())
        .expect("get_store should success");

    let edb_client = {
        let cb = ChannelBuilder::new(Arc::new(Environment::new(1)));
        let channel = mgr.connect(cb, store.get_address());
        EINSTEINDBClient::new(channel)
    };

    let mut req = SplitBraneRequest::default();
    req.mut_context().set_brane_id(brane_id);
    req.mut_context()
        .set_brane_epoch(brane.get_brane_epoch().clone());
    req.set_split_key(key);

    let resp = edb_client
        .split_brane(&req)
        .expect("split_brane should success");
    if resp.has_brane_error() {
        ve1!("split_brane internal error: {:?}", resp.get_brane_error());
        return;
    }

    v1!(
        "split brane {} success, left: {}, right: {}",
        brane_id,
        resp.get_left().get_id(),
        resp.get_right().get_id(),
    );
}

fn compact_whole_cluster(
    fidel_client: &RpcClient,
    causet: &EINSTEINDBConfig,
    mgr: Arc<SecurityManager>,
    db_type: DBType,
    causets: Vec<&str>,
    from: Option<Vec<u8>>,
    to: Option<Vec<u8>>,
    threads: u32,
    bottommost: BottommostLevelCompaction,
) {
    let stores = fidel_client
        .get_all_stores(true) // Exclude tombstone stores.
        .unwrap_or_else(|e| perror_and_exit("Get all cluster stores from FIDel failed", e));

    let mut handles = Vec::new();
    for s in stores {
        let causet = causet.clone();
        let mgr = Arc::clone(&mgr);
        let addr = s.address.clone();
        let (from, to) = (from.clone(), to.clone());
        let causets: Vec<String> = causets.iter().map(|causet| (*causet).to_string()).collect();
        let h = thread::spawn(move || {
            edb_alloc::add_thread_memory_accessor();
            let debug_executor = new_debug_executor(None, None, false, Some(&addr), &causet, mgr);
            for causet in causets {
                debug_executor.compact(
                    Some(&addr),
                    db_type,
                    causet.as_str(),
                    from.clone(),
                    to.clone(),
                    threads,
                    bottommost,
                );
            }
            edb_alloc::remove_thread_memory_accessor();
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }
}

fn read_fail_file(path: &str) -> Vec<(String, String)> {
    let f = File::open(path).unwrap();
    let f = BufReader::new(f);

    let mut list = vec![];
    for line in f.lines() {
        let line = line.unwrap();
        let mut parts = line.split('=');
        list.push((
            parts.next().unwrap().to_owned(),
            parts.next().unwrap_or("").to_owned(),
        ))
    }
    list
}

fn run_ldb_command(cmd: &ArgMatches<'_>, causet: &EINSTEINDBConfig) {
    let mut args: Vec<String> = match cmd.values_of("") {
        Some(v) => v.map(ToOwned::to_owned).collect(),
        None => Vec::new(),
    };
    args.insert(0, "ldb".to_owned());
    let key_manager = DataKeyManager::from_config(&causet.security.encryption, &causet.causet_storage.data_dir)
        .unwrap()
        .map(|key_manager| Arc::new(key_manager));
    let env = get_env(key_manager, None).unwrap();
    let mut opts = causet.lmdb.build_opt();
    opts.set_env(env);

    engine_lmdb::raw::run_ldb_tool(&args, &opts);
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_hex() {
        let result = vec![0x74];
        assert_eq!(from_hex("74").unwrap(), result);
        assert_eq!(from_hex("0x74").unwrap(), result);
        assert_eq!(from_hex("0X74").unwrap(), result);
    }
}
