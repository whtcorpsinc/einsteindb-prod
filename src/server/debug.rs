// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::cmp::Ordering;
use std::iter::FromIterator;
use std::path::Path;
use std::sync::Arc;
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::{error, result};

use engine_lmdb::raw::{CompactOptions, DBBottommostLevelCompaction, DB};
use engine_lmdb::util::get_causet_handle;
use engine_lmdb::{Compat, LmdbEngine, LmdbEngineIterator, LmdbWriteBatch};
use engine_promises::{
    Engines, IterOptions, Iterable, Iteron as EngineIterator, MuBlock, Peekable, VioletaBftEngine,
    ConePropertiesExt, SeekKey, BlockProperties, BlockPropertiesCollection, BlockPropertiesExt,
    WriteOptions,
};
use engine_promises::{Cone, WriteBatchExt, CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_VIOLETABFT, CAUSET_WRITE};
use ekvproto::debugpb::{self, Db as DBType};
use ekvproto::kvrpcpb::{MvccInfo, MvccLock, MvccValue, MvccWrite, Op};
use ekvproto::metapb::Brane;
use ekvproto::violetabft_serverpb::*;
use protobuf::Message;
use violetabft::evioletabftpb::Entry;
use violetabft::{self, RawNode};

use crate::config::ConfigController;
use crate::causetStorage::tail_pointer::{Dagger, LockType, TimeStamp, Write, WriteRef, WriteType};
use engine_lmdb::properties::MvccProperties;
use violetabftstore::interlock::get_brane_approximate_middle;
use violetabftstore::store::util as violetabftstore_util;
use violetabftstore::store::PeerStorage;
use violetabftstore::store::{write_initial_apply_state, write_initial_violetabft_state, write_peer_state};
use einsteindb_util::codec::bytes;
use einsteindb_util::collections::HashSet;
use einsteindb_util::config::ReadableSize;
use einsteindb_util::keybuilder::KeyBuilder;
use einsteindb_util::worker::Worker;
use txn_types::Key;

pub type Result<T> = result::Result<T, Error>;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        InvalidArgument(msg: String) {
            display("Invalid Argument {:?}", msg)
        }
        NotFound(msg: String) {
            display("Not Found {:?}", msg)
        }
        Other(err: Box<dyn error::Error + Sync + Slightlike>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }
    }
}

/// Describes the meta information of a Brane.
#[derive(PartialEq, Debug, Default)]
pub struct BraneInfo {
    pub violetabft_local_state: Option<VioletaBftLocalState>,
    pub violetabft_apply_state: Option<VioletaBftApplyState>,
    pub brane_local_state: Option<BraneLocalState>,
}

impl BraneInfo {
    fn new(
        violetabft_local: Option<VioletaBftLocalState>,
        violetabft_apply: Option<VioletaBftApplyState>,
        brane_local: Option<BraneLocalState>,
    ) -> Self {
        BraneInfo {
            violetabft_local_state: violetabft_local,
            violetabft_apply_state: violetabft_apply,
            brane_local_state: brane_local,
        }
    }
}

/// A thin wrapper of `DBBottommostLevelCompaction`.
#[derive(Copy, Clone, Debug)]
pub struct BottommostLevelCompaction(pub DBBottommostLevelCompaction);

impl<'a> From<Option<&'a str>> for BottommostLevelCompaction {
    fn from(v: Option<&'a str>) -> Self {
        let b = match v {
            Some("skip") => DBBottommostLevelCompaction::Skip,
            Some("force") => DBBottommostLevelCompaction::Force,
            _ => DBBottommostLevelCompaction::IfHaveCompactionFilter,
        };
        BottommostLevelCompaction(b)
    }
}

impl From<debugpb::BottommostLevelCompaction> for BottommostLevelCompaction {
    fn from(v: debugpb::BottommostLevelCompaction) -> Self {
        let b = match v {
            debugpb::BottommostLevelCompaction::Skip => DBBottommostLevelCompaction::Skip,
            debugpb::BottommostLevelCompaction::Force => DBBottommostLevelCompaction::Force,
            debugpb::BottommostLevelCompaction::IfHaveCompactionFilter => {
                DBBottommostLevelCompaction::IfHaveCompactionFilter
            }
        };
        BottommostLevelCompaction(b)
    }
}

impl From<BottommostLevelCompaction> for debugpb::BottommostLevelCompaction {
    fn from(bottommost: BottommostLevelCompaction) -> debugpb::BottommostLevelCompaction {
        match bottommost.0 {
            DBBottommostLevelCompaction::Skip => debugpb::BottommostLevelCompaction::Skip,
            DBBottommostLevelCompaction::Force => debugpb::BottommostLevelCompaction::Force,
            DBBottommostLevelCompaction::IfHaveCompactionFilter => {
                debugpb::BottommostLevelCompaction::IfHaveCompactionFilter
            }
        }
    }
}

#[derive(Clone)]
pub struct Debugger<ER: VioletaBftEngine> {
    engines: Engines<LmdbEngine, ER>,
    causetg_controller: ConfigController,
}

impl<ER: VioletaBftEngine> Debugger<ER> {
    pub fn new(
        engines: Engines<LmdbEngine, ER>,
        causetg_controller: ConfigController,
    ) -> Debugger<ER> {
        Debugger {
            engines,
            causetg_controller,
        }
    }

    pub fn get_engine(&self) -> &Engines<LmdbEngine, ER> {
        &self.engines
    }

    /// Get all branes holding brane meta data from violetabft CAUSET in KV causetStorage.
    pub fn get_all_meta_branes(&self) -> Result<Vec<u64>> {
        let db = &self.engines.kv;
        let causet = CAUSET_VIOLETABFT;
        let spacelike_key = tuplespaceInstanton::REGION_META_MIN_KEY;
        let lightlike_key = tuplespaceInstanton::REGION_META_MAX_KEY;
        let mut branes = Vec::with_capacity(128);
        box_try!(db.scan_causet(causet, spacelike_key, lightlike_key, false, |key, _| {
            let (id, suffix) = box_try!(tuplespaceInstanton::decode_brane_meta_key(key));
            if suffix != tuplespaceInstanton::REGION_STATE_SUFFIX {
                return Ok(true);
            }
            branes.push(id);
            Ok(true)
        }));
        Ok(branes)
    }

    fn get_db_from_type(&self, db: DBType) -> Result<&Arc<DB>> {
        match db {
            DBType::Kv => Ok(self.engines.kv.as_inner()),
            DBType::VioletaBft => Err(box_err!("Get violetabft db is not allowed")),
            _ => Err(box_err!("invalid DBType type")),
        }
    }

    pub fn get(&self, db: DBType, causet: &str, key: &[u8]) -> Result<Vec<u8>> {
        validate_db_and_causet(db, causet)?;
        let db = self.get_db_from_type(db)?;
        match db.c().get_value_causet(causet, key) {
            Ok(Some(v)) => Ok(v.to_vec()),
            Ok(None) => Err(Error::NotFound(format!(
                "value for key {:?} in db {:?}",
                key, db
            ))),
            Err(e) => Err(box_err!(e)),
        }
    }

    pub fn violetabft_log(&self, brane_id: u64, log_index: u64) -> Result<Entry> {
        if let Some(e) = box_try!(self.engines.violetabft.get_entry(brane_id, log_index)) {
            return Ok(e);
        }
        Err(Error::NotFound(format!(
            "violetabft log for brane {} at index {}",
            brane_id, log_index
        )))
    }

    pub fn brane_info(&self, brane_id: u64) -> Result<BraneInfo> {
        let violetabft_state = box_try!(self.engines.violetabft.get_violetabft_state(brane_id));

        let apply_state_key = tuplespaceInstanton::apply_state_key(brane_id);
        let apply_state = box_try!(self
            .engines
            .kv
            .get_msg_causet::<VioletaBftApplyState>(CAUSET_VIOLETABFT, &apply_state_key));

        let brane_state_key = tuplespaceInstanton::brane_state_key(brane_id);
        let brane_state = box_try!(self
            .engines
            .kv
            .get_msg_causet::<BraneLocalState>(CAUSET_VIOLETABFT, &brane_state_key));

        match (violetabft_state, apply_state, brane_state) {
            (None, None, None) => Err(Error::NotFound(format!("info for brane {}", brane_id))),
            (violetabft_state, apply_state, brane_state) => {
                Ok(BraneInfo::new(violetabft_state, apply_state, brane_state))
            }
        }
    }

    pub fn brane_size<T: AsRef<str>>(
        &self,
        brane_id: u64,
        causets: Vec<T>,
    ) -> Result<Vec<(T, usize)>> {
        let brane_state_key = tuplespaceInstanton::brane_state_key(brane_id);
        match self
            .engines
            .kv
            .get_msg_causet::<BraneLocalState>(CAUSET_VIOLETABFT, &brane_state_key)
        {
            Ok(Some(brane_state)) => {
                let brane = brane_state.get_brane();
                let spacelike_key = &tuplespaceInstanton::data_key(brane.get_spacelike_key());
                let lightlike_key = &tuplespaceInstanton::data_lightlike_key(brane.get_lightlike_key());
                let mut sizes = vec![];
                for causet in causets {
                    let mut size = 0;
                    box_try!(self.engines.kv.scan_causet(
                        causet.as_ref(),
                        spacelike_key,
                        lightlike_key,
                        false,
                        |k, v| {
                            size += k.len() + v.len();
                            Ok(true)
                        }
                    ));
                    sizes.push((causet, size));
                }
                Ok(sizes)
            }
            Ok(None) => Err(Error::NotFound(format!("none brane {:?}", brane_id))),
            Err(e) => Err(box_err!(e)),
        }
    }

    /// Scan MVCC Infos for given cone `[spacelike, lightlike)`.
    pub fn scan_tail_pointer(&self, spacelike: &[u8], lightlike: &[u8], limit: u64) -> Result<MvccInfoIterator> {
        if !spacelike.spacelikes_with(b"z") || (!lightlike.is_empty() && !lightlike.spacelikes_with(b"z")) {
            return Err(Error::InvalidArgument(
                "spacelike and lightlike should spacelike with \"z\"".to_owned(),
            ));
        }
        if lightlike.is_empty() && limit == 0 {
            return Err(Error::InvalidArgument("no limit and to_key".to_owned()));
        }
        MvccInfoIterator::new(self.engines.kv.as_inner(), spacelike, lightlike, limit)
    }

    /// Scan raw tuplespaceInstanton for given cone `[spacelike, lightlike)` in given causet.
    pub fn raw_scan(
        &self,
        spacelike: &[u8],
        lightlike: &[u8],
        limit: usize,
        causet: &str,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = &self.engines.kv;
        let lightlike = if !lightlike.is_empty() {
            Some(KeyBuilder::from_vec(lightlike.to_vec(), 0, 0))
        } else {
            None
        };
        let iter_opt =
            IterOptions::new(Some(KeyBuilder::from_vec(spacelike.to_vec(), 0, 0)), lightlike, false);
        let mut iter = box_try!(db.Iteron_causet_opt(causet, iter_opt));
        if !iter.seek_to_first().unwrap() {
            return Ok(vec![]);
        }

        let mut res = vec![(iter.key().to_vec(), iter.value().to_vec())];
        while res.len() < limit && iter.next().unwrap() {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
        }

        Ok(res)
    }

    /// Compact the causet[spacelike..lightlike) in the db.
    pub fn compact(
        &self,
        db: DBType,
        causet: &str,
        spacelike: &[u8],
        lightlike: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) -> Result<()> {
        validate_db_and_causet(db, causet)?;
        let db = self.get_db_from_type(db)?;
        let handle = box_try!(get_causet_handle(db, causet));
        let spacelike = if spacelike.is_empty() { None } else { Some(spacelike) };
        let lightlike = if lightlike.is_empty() { None } else { Some(lightlike) };
        info!("Debugger spacelikes manual compact"; "db" => ?db, "causet" => causet);
        let mut opts = CompactOptions::new();
        opts.set_max_subcompactions(threads as i32);
        opts.set_exclusive_manual_compaction(false);
        opts.set_bottommost_level_compaction(bottommost.0);
        db.compact_cone_causet_opt(handle, &opts, spacelike, lightlike);
        info!("Debugger finishes manual compact"; "db" => ?db, "causet" => causet);
        Ok(())
    }

    /// Set branes to tombstone by manual, and apply other status(such as
    /// peers, version, and key cone) from `brane` which comes from FIDel normally.
    pub fn set_brane_tombstone(&self, branes: Vec<Brane>) -> Result<Vec<(u64, Error)>> {
        let store_id = self.get_store_id()?;
        let db = &self.engines.kv;
        let mut wb = db.write_batch();

        let mut errors = Vec::with_capacity(branes.len());
        for brane in branes {
            let brane_id = brane.get_id();
            if let Err(e) = set_brane_tombstone(db.as_inner(), store_id, brane, &mut wb) {
                errors.push((brane_id, e));
            }
        }

        if errors.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            box_try!(db.write_opt(&wb, &write_opts));
        }
        Ok(errors)
    }

    pub fn set_brane_tombstone_by_id(&self, branes: Vec<u64>) -> Result<Vec<(u64, Error)>> {
        let db = &self.engines.kv;
        let mut wb = db.write_batch();
        let mut errors = Vec::with_capacity(branes.len());
        for brane_id in branes {
            let key = tuplespaceInstanton::brane_state_key(brane_id);
            let brane_state = match db.get_msg_causet::<BraneLocalState>(CAUSET_VIOLETABFT, &key) {
                Ok(Some(state)) => state,
                Ok(None) => {
                    let error = box_err!("{} brane local state not exists", brane_id);
                    errors.push((brane_id, error));
                    continue;
                }
                Err(_) => {
                    let error = box_err!("{} gets brane local state fail", brane_id);
                    errors.push((brane_id, error));
                    continue;
                }
            };
            if brane_state.get_state() == PeerState::Tombstone {
                v1!("skip because it's already tombstone");
                continue;
            }
            let brane = &brane_state.get_brane();
            write_peer_state(&mut wb, brane, PeerState::Tombstone, None).unwrap();
        }

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        db.write_opt(&wb, &write_opts).unwrap();
        Ok(errors)
    }

    pub fn recover_branes(
        &self,
        branes: Vec<Brane>,
        read_only: bool,
    ) -> Result<Vec<(u64, Error)>> {
        let db = &self.engines.kv;

        let mut errors = Vec::with_capacity(branes.len());
        for brane in branes {
            let brane_id = brane.get_id();
            if let Err(e) = recover_tail_pointer_for_cone(
                db.as_inner(),
                brane.get_spacelike_key(),
                brane.get_lightlike_key(),
                read_only,
                0,
            ) {
                errors.push((brane_id, e));
            }
        }

        Ok(errors)
    }

    pub fn recover_all(&self, threads: usize, read_only: bool) -> Result<()> {
        let db = self.engines.kv.clone();

        v1!("Calculating split tuplespaceInstanton...");
        let split_tuplespaceInstanton = divide_db(db.as_inner(), threads)
            .unwrap()
            .into_iter()
            .map(|k| {
                let k = Key::from_encoded(tuplespaceInstanton::origin_key(&k).to_vec())
                    .truncate_ts()
                    .unwrap();
                k.as_encoded().clone()
            });

        let mut cone_borders = vec![b"".to_vec()];
        cone_borders.extlightlike(split_tuplespaceInstanton);
        cone_borders.push(b"".to_vec());

        let mut handles = Vec::new();

        for thread_index in 0..cone_borders.len() - 1 {
            let db = db.clone();
            let spacelike_key = cone_borders[thread_index].clone();
            let lightlike_key = cone_borders[thread_index + 1].clone();

            let thread = ThreadBuilder::new()
                .name(format!("tail_pointer-recover-thread-{}", thread_index))
                .spawn(move || {
                    einsteindb_alloc::add_thread_memory_accessor();
                    v1!(
                        "thread {}: spacelikeed on cone [{}, {})",
                        thread_index,
                        hex::encode_upper(&spacelike_key),
                        hex::encode_upper(&lightlike_key)
                    );

                    let result = recover_tail_pointer_for_cone(
                        db.as_inner(),
                        &spacelike_key,
                        &lightlike_key,
                        read_only,
                        thread_index,
                    );
                    einsteindb_alloc::remove_thread_memory_accessor();
                    result
                })
                .unwrap();

            handles.push(thread);
        }

        let res = handles
            .into_iter()
            .map(|h: JoinHandle<Result<()>>| h.join())
            .map(|r| {
                if let Err(e) = &r {
                    ve1!("{:?}", e);
                }
                r
            })
            .all(|r| r.is_ok());
        if res {
            Ok(())
        } else {
            Err(box_err!("Not all threads finished successfully."))
        }
    }

    pub fn bad_branes(&self) -> Result<Vec<(u64, Error)>> {
        let mut res = Vec::new();

        let from = tuplespaceInstanton::REGION_META_MIN_KEY.to_owned();
        let to = tuplespaceInstanton::REGION_META_MAX_KEY.to_owned();
        let readopts = IterOptions::new(
            Some(KeyBuilder::from_vec(from.clone(), 0, 0)),
            Some(KeyBuilder::from_vec(to, 0, 0)),
            false,
        );
        let mut iter = box_try!(self.engines.kv.Iteron_causet_opt(CAUSET_VIOLETABFT, readopts));
        iter.seek(SeekKey::from(from.as_ref())).unwrap();

        let fake_snap_worker = Worker::new("fake-snap-worker");

        let check_value = |value: &[u8]| -> Result<()> {
            let mut local_state = BraneLocalState::default();
            box_try!(local_state.merge_from_bytes(&value));

            match local_state.get_state() {
                PeerState::Tombstone | PeerState::Applying => return Ok(()),
                _ => {}
            }

            let brane = local_state.get_brane();
            let store_id = self.get_store_id()?;

            let peer_id = violetabftstore_util::find_peer(brane, store_id)
                .map(|peer| peer.get_id())
                .ok_or_else(|| {
                    Error::Other("BraneLocalState doesn't contains peer itself".into())
                })?;

            let tag = format!("[brane {}] {}", brane.get_id(), peer_id);
            let peer_causetStorage = box_try!(PeerStorage::<LmdbEngine, ER>::new(
                self.engines.clone(),
                brane,
                fake_snap_worker.scheduler(),
                peer_id,
                tag,
            ));

            let violetabft_causetg = violetabft::Config {
                id: peer_id,
                election_tick: 10,
                heartbeat_tick: 2,
                max_size_per_msg: ReadableSize::mb(1).0,
                max_inflight_msgs: 256,
                check_quorum: true,
                skip_bcast_commit: true,
                ..Default::default()
            };

            box_try!(RawNode::new(
                &violetabft_causetg,
                peer_causetStorage,
                &slog_global::get_global()
            ));
            Ok(())
        };

        while box_try!(iter.valid()) {
            let (key, value) = (iter.key(), iter.value());
            if let Ok((brane_id, suffix)) = tuplespaceInstanton::decode_brane_meta_key(&key) {
                if suffix != tuplespaceInstanton::REGION_STATE_SUFFIX {
                    box_try!(iter.next());
                    continue;
                }
                if let Err(e) = check_value(value) {
                    res.push((brane_id, e));
                }
            }
            box_try!(iter.next());
        }
        Ok(res)
    }

    pub fn remove_failed_stores(
        &self,
        store_ids: Vec<u64>,
        brane_ids: Option<Vec<u64>>,
    ) -> Result<()> {
        let store_id = self.get_store_id()?;
        if store_ids.iter().any(|&s| s == store_id) {
            let msg = format!("CausetStore {} in the failed list", store_id);
            return Err(Error::Other(msg.into()));
        }
        let mut wb = self.engines.kv.write_batch();
        let store_ids = HashSet::<u64>::from_iter(store_ids);

        {
            let remove_stores = |key: &[u8], value: &[u8], kv_wb: &mut LmdbWriteBatch| {
                let (_, suffix_type) = box_try!(tuplespaceInstanton::decode_brane_meta_key(key));
                if suffix_type != tuplespaceInstanton::REGION_STATE_SUFFIX {
                    return Ok(());
                }

                let mut brane_state = BraneLocalState::default();
                box_try!(brane_state.merge_from_bytes(value));
                if brane_state.get_state() == PeerState::Tombstone {
                    return Ok(());
                }

                let mut new_peers = brane_state.get_brane().get_peers().to_owned();
                new_peers.retain(|peer| !store_ids.contains(&peer.get_store_id()));

                let brane_id = brane_state.get_brane().get_id();
                let old_peers = brane_state.mut_brane().take_peers();
                info!(
                    "peers changed";
                    "brane_id" => brane_id,
                    "old_peers" => ?old_peers,
                    "new_peers" => ?new_peers,
                );
                // We need to leave epoch untouched to avoid inconsistency.
                brane_state.mut_brane().set_peers(new_peers.into());
                box_try!(kv_wb.put_msg_causet(CAUSET_VIOLETABFT, key, &brane_state));
                Ok(())
            };

            if let Some(brane_ids) = brane_ids {
                let kv = &self.engines.kv;
                for brane_id in brane_ids {
                    let key = tuplespaceInstanton::brane_state_key(brane_id);
                    if let Some(value) = box_try!(kv.get_value_causet(CAUSET_VIOLETABFT, &key)) {
                        box_try!(remove_stores(&key, &value, &mut wb));
                    } else {
                        let msg = format!("No such brane {} on the store", brane_id);
                        return Err(Error::Other(msg.into()));
                    }
                }
            } else {
                box_try!(self.engines.kv.scan_causet(
                    CAUSET_VIOLETABFT,
                    tuplespaceInstanton::REGION_META_MIN_KEY,
                    tuplespaceInstanton::REGION_META_MAX_KEY,
                    false,
                    |key, value| remove_stores(key, value, &mut wb).map(|_| true)
                ));
            }
        }

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        box_try!(self.engines.kv.write_opt(&wb, &write_opts));
        Ok(())
    }

    pub fn recreate_brane(&self, brane: Brane) -> Result<()> {
        let brane_id = brane.get_id();
        let kv = &self.engines.kv;
        let violetabft = &self.engines.violetabft;

        let mut kv_wb = self.engines.kv.write_batch();
        let mut violetabft_wb = self.engines.violetabft.log_batch(0);

        if brane.get_spacelike_key() >= brane.get_lightlike_key() && !brane.get_lightlike_key().is_empty() {
            return Err(box_err!("Bad brane: {:?}", brane));
        }

        box_try!(self.engines.kv.scan_causet(
            CAUSET_VIOLETABFT,
            tuplespaceInstanton::REGION_META_MIN_KEY,
            tuplespaceInstanton::REGION_META_MAX_KEY,
            false,
            |key, value| {
                let (_, suffix_type) = box_try!(tuplespaceInstanton::decode_brane_meta_key(key));
                if suffix_type != tuplespaceInstanton::REGION_STATE_SUFFIX {
                    return Ok(true);
                }

                let mut brane_state = BraneLocalState::default();
                box_try!(brane_state.merge_from_bytes(value));
                if brane_state.get_state() == PeerState::Tombstone {
                    return Ok(true);
                }
                let exists_brane = brane_state.get_brane();

                if !brane_overlap(exists_brane, &brane) {
                    return Ok(true);
                }

                if exists_brane.get_spacelike_key() == brane.get_spacelike_key()
                    && exists_brane.get_lightlike_key() == brane.get_lightlike_key()
                {
                    Err(box_err!("brane still exists {:?}", brane))
                } else {
                    Err(box_err!("brane overlap with {:?}", exists_brane))
                }
            },
        ));

        // BraneLocalState.
        let mut brane_state = BraneLocalState::default();
        brane_state.set_state(PeerState::Normal);
        brane_state.set_brane(brane);
        let key = tuplespaceInstanton::brane_state_key(brane_id);
        if box_try!(kv.get_msg_causet::<BraneLocalState>(CAUSET_VIOLETABFT, &key)).is_some() {
            return Err(Error::Other(
                "CausetStore already has the BraneLocalState".into(),
            ));
        }
        box_try!(kv_wb.put_msg_causet(CAUSET_VIOLETABFT, &key, &brane_state));

        // VioletaBftApplyState.
        let key = tuplespaceInstanton::apply_state_key(brane_id);
        if box_try!(kv.get_msg_causet::<VioletaBftApplyState>(CAUSET_VIOLETABFT, &key)).is_some() {
            return Err(Error::Other("CausetStore already has the VioletaBftApplyState".into()));
        }
        box_try!(write_initial_apply_state(&mut kv_wb, brane_id));

        // VioletaBftLocalState.
        if box_try!(violetabft.get_violetabft_state(brane_id)).is_some() {
            return Err(Error::Other("CausetStore already has the VioletaBftLocalState".into()));
        }
        box_try!(write_initial_violetabft_state(&mut violetabft_wb, brane_id));

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        box_try!(self.engines.kv.write_opt(&kv_wb, &write_opts));
        box_try!(self.engines.violetabft.consume(&mut violetabft_wb, true));
        Ok(())
    }

    pub fn get_store_id(&self) -> Result<u64> {
        let db = &self.engines.kv;
        db.get_msg::<StoreIdent>(tuplespaceInstanton::STORE_IDENT_KEY)
            .map_err(|e| box_err!(e))
            .and_then(|ident| match ident {
                Some(ident) => Ok(ident.get_store_id()),
                None => Err(Error::NotFound("No store ident key".to_owned())),
            })
    }

    pub fn get_cluster_id(&self) -> Result<u64> {
        let db = &self.engines.kv;
        db.get_msg::<StoreIdent>(tuplespaceInstanton::STORE_IDENT_KEY)
            .map_err(|e| box_err!(e))
            .and_then(|ident| match ident {
                Some(ident) => Ok(ident.get_cluster_id()),
                None => Err(Error::NotFound("No cluster ident key".to_owned())),
            })
    }

    pub fn modify_einsteindb_config(&self, config_name: &str, config_value: &str) -> Result<()> {
        if let Err(e) = self.causetg_controller.ufidelate_config(config_name, config_value) {
            return Err(Error::Other(
                format!("failed to ufidelate config, err: {:?}", e).into(),
            ));
        }
        Ok(())
    }

    fn get_brane_state(&self, brane_id: u64) -> Result<BraneLocalState> {
        let brane_state_key = tuplespaceInstanton::brane_state_key(brane_id);
        let brane_state = box_try!(self
            .engines
            .kv
            .get_msg_causet::<BraneLocalState>(CAUSET_VIOLETABFT, &brane_state_key));
        match brane_state {
            Some(v) => Ok(v),
            None => Err(Error::NotFound(format!("brane {}", brane_id))),
        }
    }

    pub fn get_brane_properties(&self, brane_id: u64) -> Result<Vec<(String, String)>> {
        let brane_state = self.get_brane_state(brane_id)?;
        let brane = brane_state.get_brane();
        let spacelike = tuplespaceInstanton::enc_spacelike_key(brane);
        let lightlike = tuplespaceInstanton::enc_lightlike_key(brane);

        let mut res = dump_tail_pointer_properties(self.engines.kv.as_inner(), &spacelike, &lightlike)?;

        let middle_key = match box_try!(get_brane_approximate_middle(&self.engines.kv, brane)) {
            Some(data_key) => {
                let mut key = tuplespaceInstanton::origin_key(&data_key);
                box_try!(bytes::decode_bytes(&mut key, false))
            }
            None => Vec::new(),
        };

        // Middle key of the cone.
        res.push((
            "middle_key_by_approximate_size".to_owned(),
            hex::encode(&middle_key),
        ));

        Ok(res)
    }

    pub fn get_cone_properties(&self, spacelike: &[u8], lightlike: &[u8]) -> Result<Vec<(String, String)>> {
        dump_tail_pointer_properties(
            self.engines.kv.as_inner(),
            &tuplespaceInstanton::data_key(spacelike),
            &tuplespaceInstanton::data_lightlike_key(lightlike),
        )
    }
}

fn dump_tail_pointer_properties(db: &Arc<DB>, spacelike: &[u8], lightlike: &[u8]) -> Result<Vec<(String, String)>> {
    let mut num_entries = 0; // number of Lmdbdb K/V entries.

    let collection = box_try!(db.c().get_cone_properties_causet(CAUSET_WRITE, &spacelike, &lightlike));
    let num_files = collection.len();

    let mut tail_pointer_properties = MvccProperties::new();
    for (_, v) in collection.iter() {
        num_entries += v.num_entries();
        let tail_pointer = box_try!(MvccProperties::decode(&v.user_collected_properties()));
        tail_pointer_properties.add(&tail_pointer);
    }

    let sst_files = collection
        .iter()
        .map(|(k, _)| {
            Path::new(&*k)
                .file_name()
                .map(|f| f.to_str().unwrap())
                .unwrap_or(&*k)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut res: Vec<(String, String)> = [
        ("tail_pointer.min_ts", tail_pointer_properties.min_ts.into_inner()),
        ("tail_pointer.max_ts", tail_pointer_properties.max_ts.into_inner()),
        ("tail_pointer.num_rows", tail_pointer_properties.num_rows),
        ("tail_pointer.num_puts", tail_pointer_properties.num_puts),
        ("tail_pointer.num_deletes", tail_pointer_properties.num_deletes),
        ("tail_pointer.num_versions", tail_pointer_properties.num_versions),
        ("tail_pointer.max_row_versions", tail_pointer_properties.max_row_versions),
    ]
    .iter()
    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
    .collect();

    // Entries and delete marks of Lmdb.
    let num_deletes = num_entries - tail_pointer_properties.num_versions;
    res.push(("num_entries".to_owned(), num_entries.to_string()));
    res.push(("num_deletes".to_owned(), num_deletes.to_string()));

    // count and list of files.
    res.push(("num_files".to_owned(), num_files.to_string()));
    res.push(("sst_files".to_owned(), sst_files));

    Ok(res)
}

fn recover_tail_pointer_for_cone(
    db: &Arc<DB>,
    spacelike_key: &[u8],
    lightlike_key: &[u8],
    read_only: bool,
    thread_index: usize,
) -> Result<()> {
    let mut tail_pointer_checker = box_try!(MvccChecker::new(Arc::clone(&db), spacelike_key, lightlike_key));
    tail_pointer_checker.thread_index = thread_index;

    let wb_limit: usize = 10240;

    loop {
        let mut wb = db.c().write_batch();
        tail_pointer_checker.check_tail_pointer(&mut wb, Some(wb_limit))?;

        let batch_size = wb.count();

        if !read_only {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            box_try!(db.c().write_opt(&wb, &write_opts));
        } else {
            v1!("thread {}: skip write {} events", thread_index, batch_size);
        }

        v1!(
            "thread {}: total fix default: {}, dagger: {}, write: {}",
            thread_index,
            tail_pointer_checker.default_fix_count,
            tail_pointer_checker.lock_fix_count,
            tail_pointer_checker.write_fix_count
        );

        if batch_size < wb_limit {
            v1!("thread {} has finished working.", thread_index);
            return Ok(());
        }
    }
}

pub struct MvccChecker {
    lock_iter: LmdbEngineIterator,
    default_iter: LmdbEngineIterator,
    write_iter: LmdbEngineIterator,
    scan_count: usize,
    lock_fix_count: usize,
    default_fix_count: usize,
    write_fix_count: usize,
    pub thread_index: usize,
}

impl MvccChecker {
    fn new(db: Arc<DB>, spacelike_key: &[u8], lightlike_key: &[u8]) -> Result<Self> {
        let spacelike_key = tuplespaceInstanton::data_key(spacelike_key);
        let lightlike_key = tuplespaceInstanton::data_lightlike_key(lightlike_key);
        let gen_iter = |causet: &str| -> Result<_> {
            let from = spacelike_key.clone();
            let to = lightlike_key.clone();
            let readopts = IterOptions::new(
                Some(KeyBuilder::from_vec(from, 0, 0)),
                Some(KeyBuilder::from_vec(to, 0, 0)),
                false,
            );
            let mut iter = box_try!(db.c().Iteron_causet_opt(causet, readopts));
            iter.seek(SeekKey::Start).unwrap();
            Ok(iter)
        };

        Ok(MvccChecker {
            write_iter: gen_iter(CAUSET_WRITE)?,
            lock_iter: gen_iter(CAUSET_DAGGER)?,
            default_iter: gen_iter(CAUSET_DEFAULT)?,
            scan_count: 0,
            lock_fix_count: 0,
            default_fix_count: 0,
            write_fix_count: 0,
            thread_index: 0,
        })
    }

    fn min_key(
        key: Option<Vec<u8>>,
        iter: &LmdbEngineIterator,
        f: fn(&[u8]) -> &[u8],
    ) -> Option<Vec<u8>> {
        let iter_key = if iter.valid().unwrap() {
            Some(f(tuplespaceInstanton::origin_key(iter.key())).to_vec())
        } else {
            None
        };
        match (key, iter_key) {
            (Some(a), Some(b)) => {
                if a < b {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    pub fn check_tail_pointer(&mut self, wb: &mut LmdbWriteBatch, limit: Option<usize>) -> Result<()> {
        loop {
            // Find min key in the 3 CAUSETs.
            let mut key = MvccChecker::min_key(None, &self.default_iter, |k| {
                Key::truncate_ts_for(k).unwrap()
            });
            key = MvccChecker::min_key(key, &self.lock_iter, |k| k);
            key = MvccChecker::min_key(key, &self.write_iter, |k| Key::truncate_ts_for(k).unwrap());

            match key {
                Some(key) => self.check_tail_pointer_key(wb, key.as_ref())?,
                None => return Ok(()),
            }

            if let Some(limit) = limit {
                if wb.count() >= limit {
                    return Ok(());
                }
            }
        }
    }

    fn check_tail_pointer_key(&mut self, wb: &mut LmdbWriteBatch, key: &[u8]) -> Result<()> {
        self.scan_count += 1;
        if self.scan_count % 1_000_000 == 0 {
            v1!(
                "thread {}: scan {} events",
                self.thread_index,
                self.scan_count
            );
        }

        let (mut default, mut write, mut dagger) = (None, None, None);
        let (mut next_default, mut next_write, mut next_lock) = (true, true, true);
        loop {
            if next_default {
                default = self.next_default(key)?;
                next_default = false;
            }
            if next_write {
                write = self.next_write(key)?;
                next_write = false;
            }
            if next_lock {
                dagger = self.next_lock(key)?;
                next_lock = false;
            }

            // If dagger exists, check whether the records in DEFAULT and WRITE
            // match it.
            if let Some(ref l) = dagger {
                // All write records's ts should be less than the for_ufidelate_ts (if the
                // dagger is from a pessimistic transaction) or the ts of the dagger.
                let (kind, check_ts) = if l.for_ufidelate_ts >= l.ts {
                    ("for_ufidelate_ts", l.for_ufidelate_ts)
                } else {
                    ("ts", l.ts)
                };

                if let Some((commit_ts, _)) = write {
                    if check_ts <= commit_ts {
                        v1!(
                            "thread {}: LOCK {} is less than WRITE ts, key: {}, {}: {}, commit_ts: {}",
                            self.thread_index,
                            kind,
                            hex::encode_upper(key),
                            kind,
                            l.ts,
                            commit_ts
                        );
                        self.delete(wb, CAUSET_DAGGER, key, None)?;
                        self.lock_fix_count += 1;
                        next_lock = true;
                        continue;
                    }
                }

                // If the dagger's type is PUT and contains no short value, there
                // should be a corresponding default record.
                if l.lock_type == LockType::Put && l.short_value.is_none() {
                    match default {
                        Some(spacelike_ts) if spacelike_ts == l.ts => {
                            next_default = true;
                        }
                        _ => {
                            v1!(
                                "thread {}: no corresponding DEFAULT record for LOCK, key: {}, lock_ts: {}",
                                self.thread_index,
                                hex::encode_upper(key),
                                l.ts
                            );
                            self.delete(wb, CAUSET_DAGGER, key, None)?;
                            self.lock_fix_count += 1;
                        }
                    }
                }
                next_lock = true;
                continue;
            }

            // For none-put write or write with short_value, no DEFAULT record
            // is needed.
            if let Some((_, ref w)) = write {
                if w.write_type != WriteType::Put || w.short_value.is_some() {
                    next_write = true;
                    continue;
                }
            }

            // The spacelike_ts of DEFAULT and WRITE should be matched.
            match (default, &write) {
                (Some(spacelike_ts), &Some((_, ref w))) if spacelike_ts == w.spacelike_ts => {
                    next_default = true;
                    next_write = true;
                    continue;
                }
                (Some(spacelike_ts), &Some((_, ref w))) if spacelike_ts < w.spacelike_ts => next_write = true,
                (Some(spacelike_ts), &Some((_, ref w))) if spacelike_ts > w.spacelike_ts => next_default = true,
                (Some(_), &Some(_)) => {} // Won't happen.
                (None, &Some(_)) => next_write = true,
                (Some(_), &None) => next_default = true,
                (None, &None) => return Ok(()),
            }

            if next_default {
                v1!(
                    "thread {}: orphan DEFAULT record, key: {}, spacelike_ts: {}",
                    self.thread_index,
                    hex::encode_upper(key),
                    default.unwrap()
                );
                self.delete(wb, CAUSET_DEFAULT, key, default)?;
                self.default_fix_count += 1;
            }

            if next_write {
                if let Some((commit_ts, ref w)) = write {
                    v1!(
                        "thread {}: no corresponding DEFAULT record for WRITE, key: {}, spacelike_ts: {}, commit_ts: {}",
                        self.thread_index,
                        hex::encode_upper(key),
                        w.spacelike_ts,
                        commit_ts
                    );
                    self.delete(wb, CAUSET_WRITE, key, Some(commit_ts))?;
                    self.write_fix_count += 1;
                }
            }
        }
    }

    fn next_lock(&mut self, key: &[u8]) -> Result<Option<Dagger>> {
        if self.lock_iter.valid().unwrap() && tuplespaceInstanton::origin_key(self.lock_iter.key()) == key {
            let dagger = box_try!(Dagger::parse(self.lock_iter.value()));
            self.lock_iter.next().unwrap();
            return Ok(Some(dagger));
        }
        Ok(None)
    }

    fn next_default(&mut self, key: &[u8]) -> Result<Option<TimeStamp>> {
        if self.default_iter.valid().unwrap()
            && box_try!(Key::truncate_ts_for(tuplespaceInstanton::origin_key(
                self.default_iter.key()
            ))) == key
        {
            let spacelike_ts = box_try!(Key::decode_ts_from(tuplespaceInstanton::origin_key(
                self.default_iter.key()
            )));
            self.default_iter.next().unwrap();
            return Ok(Some(spacelike_ts));
        }
        Ok(None)
    }

    fn next_write(&mut self, key: &[u8]) -> Result<Option<(TimeStamp, Write)>> {
        if self.write_iter.valid().unwrap()
            && box_try!(Key::truncate_ts_for(tuplespaceInstanton::origin_key(
                self.write_iter.key()
            ))) == key
        {
            let write = box_try!(WriteRef::parse(self.write_iter.value())).to_owned();
            let commit_ts = box_try!(Key::decode_ts_from(tuplespaceInstanton::origin_key(self.write_iter.key())));
            self.write_iter.next().unwrap();
            return Ok(Some((commit_ts, write)));
        }
        Ok(None)
    }

    fn delete(
        &mut self,
        wb: &mut LmdbWriteBatch,
        causet: &str,
        key: &[u8],
        ts: Option<TimeStamp>,
    ) -> Result<()> {
        match ts {
            Some(ts) => {
                let key = Key::from_encoded_slice(key).applightlike_ts(ts);
                box_try!(wb.delete_causet(causet, &tuplespaceInstanton::data_key(key.as_encoded())));
            }
            None => box_try!(wb.delete_causet(causet, &tuplespaceInstanton::data_key(key))),
        };
        Ok(())
    }
}

fn brane_overlap(r1: &Brane, r2: &Brane) -> bool {
    let (spacelike_key_1, spacelike_key_2) = (r1.get_spacelike_key(), r2.get_spacelike_key());
    let (lightlike_key_1, lightlike_key_2) = (r1.get_lightlike_key(), r2.get_lightlike_key());
    (spacelike_key_1 < lightlike_key_2 || lightlike_key_2.is_empty())
        && (spacelike_key_2 < lightlike_key_1 || lightlike_key_1.is_empty())
}

pub struct MvccInfoIterator {
    limit: u64,
    count: u64,
    lock_iter: LmdbEngineIterator,
    default_iter: LmdbEngineIterator,
    write_iter: LmdbEngineIterator,
}

pub type Kv = (Vec<u8>, Vec<u8>);

impl MvccInfoIterator {
    fn new(db: &Arc<DB>, from: &[u8], to: &[u8], limit: u64) -> Result<Self> {
        if !tuplespaceInstanton::validate_data_key(from) {
            return Err(Error::InvalidArgument(format!(
                "from non-tail_pointer area {:?}",
                from
            )));
        }

        let gen_iter = |causet: &str| -> Result<_> {
            let to = if to.is_empty() {
                None
            } else {
                Some(KeyBuilder::from_vec(to.to_vec(), 0, 0))
            };
            let readopts = IterOptions::new(None, to, false);
            let mut iter = box_try!(db.c().Iteron_causet_opt(causet, readopts));
            iter.seek(SeekKey::from(from)).unwrap();
            Ok(iter)
        };
        Ok(MvccInfoIterator {
            limit,
            count: 0,
            lock_iter: gen_iter(CAUSET_DAGGER)?,
            default_iter: gen_iter(CAUSET_DEFAULT)?,
            write_iter: gen_iter(CAUSET_WRITE)?,
        })
    }

    fn next_lock(&mut self) -> Result<Option<(Vec<u8>, MvccLock)>> {
        let iter = &mut self.lock_iter;
        if box_try!(iter.valid()) {
            let (key, value) = (iter.key().to_owned(), iter.value());
            let dagger = box_try!(Dagger::parse(&value));
            let mut lock_info = MvccLock::default();
            match dagger.lock_type {
                LockType::Put => lock_info.set_type(Op::Put),
                LockType::Delete => lock_info.set_type(Op::Del),
                LockType::Dagger => lock_info.set_type(Op::Dagger),
                LockType::Pessimistic => lock_info.set_type(Op::PessimisticLock),
            }
            lock_info.set_spacelike_ts(dagger.ts.into_inner());
            lock_info.set_primary(dagger.primary);
            lock_info.set_short_value(dagger.short_value.unwrap_or_default());
            box_try!(iter.next());
            return Ok(Some((key, lock_info)));
        };
        Ok(None)
    }

    fn next_default(&mut self) -> Result<Option<(Vec<u8>, Vec<MvccValue>)>> {
        if let Some((prefix, vec_kv)) = Self::next_grouped(&mut self.default_iter) {
            let mut values = Vec::with_capacity(vec_kv.len());
            for (key, value) in vec_kv {
                let mut value_info = MvccValue::default();
                let spacelike_ts = box_try!(Key::decode_ts_from(tuplespaceInstanton::origin_key(&key)));
                value_info.set_spacelike_ts(spacelike_ts.into_inner());
                value_info.set_value(value);
                values.push(value_info);
            }
            return Ok(Some((prefix, values)));
        }
        Ok(None)
    }

    fn next_write(&mut self) -> Result<Option<(Vec<u8>, Vec<MvccWrite>)>> {
        if let Some((prefix, vec_kv)) = Self::next_grouped(&mut self.write_iter) {
            let mut writes = Vec::with_capacity(vec_kv.len());
            for (key, value) in vec_kv {
                let write = box_try!(WriteRef::parse(&value)).to_owned();
                let mut write_info = MvccWrite::default();
                match write.write_type {
                    WriteType::Put => write_info.set_type(Op::Put),
                    WriteType::Delete => write_info.set_type(Op::Del),
                    WriteType::Dagger => write_info.set_type(Op::Dagger),
                    WriteType::Rollback => write_info.set_type(Op::Rollback),
                }
                write_info.set_spacelike_ts(write.spacelike_ts.into_inner());
                let commit_ts = box_try!(Key::decode_ts_from(tuplespaceInstanton::origin_key(&key)));
                write_info.set_commit_ts(commit_ts.into_inner());
                write_info.set_short_value(write.short_value.unwrap_or_default());
                writes.push(write_info);
            }
            return Ok(Some((prefix, writes)));
        }
        Ok(None)
    }

    fn next_grouped(iter: &mut LmdbEngineIterator) -> Option<(Vec<u8>, Vec<Kv>)> {
        if iter.valid().unwrap() {
            let prefix = Key::truncate_ts_for(iter.key()).unwrap().to_vec();
            let mut kvs = vec![(iter.key().to_vec(), iter.value().to_vec())];
            while iter.next().unwrap() && iter.key().spacelikes_with(&prefix) {
                kvs.push((iter.key().to_vec(), iter.value().to_vec()));
            }
            return Some((prefix, kvs));
        }
        None
    }

    fn next_item(&mut self) -> Result<Option<(Vec<u8>, MvccInfo)>> {
        if self.limit != 0 && self.count >= self.limit {
            return Ok(None);
        }

        let mut tail_pointer_info = MvccInfo::default();
        let mut min_prefix = Vec::new();

        let (lock_ok, writes_ok) = match (
            self.lock_iter.valid().unwrap(),
            self.write_iter.valid().unwrap(),
        ) {
            (false, false) => return Ok(None),
            (true, true) => {
                let prefix1 = self.lock_iter.key();
                let prefix2 = box_try!(Key::truncate_ts_for(self.write_iter.key()));
                match prefix1.cmp(prefix2) {
                    Ordering::Less => (true, false),
                    Ordering::Equal => (true, true),
                    _ => (false, true),
                }
            }
            valid_pair => valid_pair,
        };

        if lock_ok {
            if let Some((prefix, dagger)) = self.next_lock()? {
                tail_pointer_info.set_lock(dagger);
                min_prefix = prefix;
            }
        }
        if writes_ok {
            if let Some((prefix, writes)) = self.next_write()? {
                tail_pointer_info.set_writes(writes.into());
                min_prefix = prefix;
            }
        }
        if self.default_iter.valid().unwrap() {
            match box_try!(Key::truncate_ts_for(self.default_iter.key())).cmp(&min_prefix) {
                Ordering::Equal => {
                    if let Some((_, values)) = self.next_default()? {
                        tail_pointer_info.set_values(values.into());
                    }
                }
                Ordering::Greater => {}
                _ => {
                    let err_msg = format!(
                        "scan_tail_pointer CAUSET_DEFAULT corrupt: want {}, got {}",
                        hex::encode_upper(&min_prefix),
                        hex::encode_upper(box_try!(Key::truncate_ts_for(self.default_iter.key())))
                    );
                    return Err(box_err!(err_msg));
                }
            }
        }
        self.count += 1;
        Ok(Some((min_prefix, tail_pointer_info)))
    }
}

impl Iteron for MvccInfoIterator {
    type Item = Result<(Vec<u8>, MvccInfo)>;

    fn next(&mut self) -> Option<Result<(Vec<u8>, MvccInfo)>> {
        match self.next_item() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

fn validate_db_and_causet(db: DBType, causet: &str) -> Result<()> {
    match (db, causet) {
        (DBType::Kv, CAUSET_DEFAULT)
        | (DBType::Kv, CAUSET_WRITE)
        | (DBType::Kv, CAUSET_DAGGER)
        | (DBType::Kv, CAUSET_VIOLETABFT)
        | (DBType::VioletaBft, CAUSET_DEFAULT) => Ok(()),
        _ => Err(Error::InvalidArgument(format!(
            "invalid causet {:?} for db {:?}",
            causet, db
        ))),
    }
}

fn set_brane_tombstone(
    db: &Arc<DB>,
    store_id: u64,
    brane: Brane,
    wb: &mut LmdbWriteBatch,
) -> Result<()> {
    let id = brane.get_id();
    let key = tuplespaceInstanton::brane_state_key(id);

    let brane_state = db
        .c()
        .get_msg_causet::<BraneLocalState>(CAUSET_VIOLETABFT, &key)
        .map_err(|e| box_err!(e))
        .and_then(|s| s.ok_or_else(|| Error::Other("Can't find BraneLocalState".into())))?;
    if brane_state.get_state() == PeerState::Tombstone {
        return Ok(());
    }

    let peer_id = brane_state
        .get_brane()
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .map(|p| p.get_id())
        .ok_or_else(|| Error::Other("BraneLocalState doesn't contains the peer itself".into()))?;

    let old_conf_ver = brane_state.get_brane().get_brane_epoch().get_conf_ver();
    let new_conf_ver = brane.get_brane_epoch().get_conf_ver();
    if new_conf_ver <= old_conf_ver {
        return Err(box_err!(
            "invalid conf_ver: please make sure you have removed the peer by FIDel"
        ));
    }

    // If the store is not in peers, or it's still in but its peer_id
    // has changed, we know the peer is marked as tombstone success.
    let scheduled = brane
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .map_or(true, |p| p.get_id() != peer_id);
    if !scheduled {
        return Err(box_err!("The peer is still in target peers"));
    }

    box_try!(write_peer_state(wb, &brane, PeerState::Tombstone, None));
    Ok(())
}

fn divide_db(db: &Arc<DB>, parts: usize) -> violetabftstore::Result<Vec<Vec<u8>>> {
    // Empty spacelike and lightlike key cover all cone.
    let spacelike = tuplespaceInstanton::data_key(b"");
    let lightlike = tuplespaceInstanton::data_lightlike_key(b"");
    let cone = Cone::new(&spacelike, &lightlike);
    let brane_id = 0;
    Ok(box_try!(
        LmdbEngine::from_db(db.clone()).divide_cone(cone, brane_id, parts)
    ))
}

#[causetg(test)]
mod tests {
    use std::sync::Arc;

    use engine_lmdb::raw::{PrimaryCausetNetworkOptions, DBOptions, WriBlock};
    use ekvproto::metapb::{Peer, Brane};
    use violetabft::evioletabftpb::EntryType;
    use tempfile::Builder;

    use super::*;
    use crate::causetStorage::tail_pointer::{Dagger, LockType};
    use engine_lmdb::raw_util::{new_engine_opt, CAUSETOptions};
    use engine_lmdb::LmdbEngine;
    use engine_promises::{CAUSETHandleExt, MuBlock, SyncMuBlock};
    use engine_promises::{ALL_CAUSETS, CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_VIOLETABFT, CAUSET_WRITE};

    fn init_brane_state(engine: &Arc<DB>, brane_id: u64, stores: &[u64]) -> Brane {
        let mut brane = Brane::default();
        brane.set_id(brane_id);
        for (i, &store_id) in stores.iter().enumerate() {
            let mut peer = Peer::default();
            peer.set_id(i as u64);
            peer.set_store_id(store_id);
            brane.mut_peers().push(peer);
        }
        let mut brane_state = BraneLocalState::default();
        brane_state.set_state(PeerState::Normal);
        brane_state.set_brane(brane.clone());
        let key = tuplespaceInstanton::brane_state_key(brane_id);
        engine.c().put_msg_causet(CAUSET_VIOLETABFT, &key, &brane_state).unwrap();
        brane
    }

    fn get_brane_state(engine: &Arc<DB>, brane_id: u64) -> BraneLocalState {
        let key = tuplespaceInstanton::brane_state_key(brane_id);
        engine
            .c()
            .get_msg_causet::<BraneLocalState>(CAUSET_VIOLETABFT, &key)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn test_brane_overlap() {
        let new_brane = |spacelike: &[u8], lightlike: &[u8]| -> Brane {
            let mut brane = Brane::default();
            brane.set_spacelike_key(spacelike.to_owned());
            brane.set_lightlike_key(lightlike.to_owned());
            brane
        };

        // For normal case.
        assert!(brane_overlap(
            &new_brane(b"a", b"z"),
            &new_brane(b"b", b"y")
        ));
        assert!(brane_overlap(
            &new_brane(b"a", b"n"),
            &new_brane(b"m", b"z")
        ));
        assert!(!brane_overlap(
            &new_brane(b"a", b"m"),
            &new_brane(b"n", b"z")
        ));

        // For the first or last brane.
        assert!(brane_overlap(
            &new_brane(b"m", b""),
            &new_brane(b"a", b"n")
        ));
        assert!(brane_overlap(
            &new_brane(b"a", b"n"),
            &new_brane(b"m", b"")
        ));
        assert!(brane_overlap(
            &new_brane(b"", b""),
            &new_brane(b"m", b"")
        ));
        assert!(!brane_overlap(
            &new_brane(b"a", b"m"),
            &new_brane(b"n", b"")
        ));
    }

    #[test]
    fn test_validate_db_and_causet() {
        let valid_cases = vec![
            (DBType::Kv, CAUSET_DEFAULT),
            (DBType::Kv, CAUSET_WRITE),
            (DBType::Kv, CAUSET_DAGGER),
            (DBType::Kv, CAUSET_VIOLETABFT),
            (DBType::VioletaBft, CAUSET_DEFAULT),
        ];
        for (db, causet) in valid_cases {
            validate_db_and_causet(db, causet).unwrap();
        }

        let invalid_cases = vec![
            (DBType::VioletaBft, CAUSET_WRITE),
            (DBType::VioletaBft, CAUSET_DAGGER),
            (DBType::VioletaBft, CAUSET_VIOLETABFT),
            (DBType::Invalid, CAUSET_DEFAULT),
            (DBType::Invalid, "BAD_CAUSET"),
        ];
        for (db, causet) in invalid_cases {
            validate_db_and_causet(db, causet).unwrap_err();
        }
    }

    fn new_debugger() -> Debugger<LmdbEngine> {
        let tmp = Builder::new().prefix("test_debug").temfidelir().unwrap();
        let path = tmp.path().to_str().unwrap();
        let engine = Arc::new(
            engine_lmdb::raw_util::new_engine_opt(
                path,
                DBOptions::new(),
                vec![
                    CAUSETOptions::new(CAUSET_DEFAULT, PrimaryCausetNetworkOptions::new()),
                    CAUSETOptions::new(CAUSET_WRITE, PrimaryCausetNetworkOptions::new()),
                    CAUSETOptions::new(CAUSET_DAGGER, PrimaryCausetNetworkOptions::new()),
                    CAUSETOptions::new(CAUSET_VIOLETABFT, PrimaryCausetNetworkOptions::new()),
                ],
            )
            .unwrap(),
        );

        let engines = Engines::new(
            LmdbEngine::from_db(Arc::clone(&engine)),
            LmdbEngine::from_db(engine),
        );
        Debugger::new(engines, ConfigController::default())
    }

    impl Debugger<LmdbEngine> {
        fn get_store_ident(&self) -> Result<StoreIdent> {
            let db = &self.engines.kv;
            db.get_msg::<StoreIdent>(tuplespaceInstanton::STORE_IDENT_KEY)
                .map_err(|e| box_err!(e))
                .map(|ident| match ident {
                    Some(ident) => ident,
                    None => StoreIdent::default(),
                })
        }

        fn set_store_id(&self, store_id: u64) {
            if let Ok(mut ident) = self.get_store_ident() {
                ident.set_store_id(store_id);
                let db = &self.engines.kv;
                db.put_msg(tuplespaceInstanton::STORE_IDENT_KEY, &ident).unwrap();
            }
        }

        fn set_cluster_id(&self, cluster_id: u64) {
            if let Ok(mut ident) = self.get_store_ident() {
                ident.set_cluster_id(cluster_id);
                let db = &self.engines.kv;
                db.put_msg(tuplespaceInstanton::STORE_IDENT_KEY, &ident).unwrap();
            }
        }
    }

    #[test]
    fn test_get() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv;
        let (k, v) = (b"k", b"v");
        engine.put(k, v).unwrap();
        assert_eq!(&*engine.get_value(k).unwrap().unwrap(), v);

        let got = debugger.get(DBType::Kv, CAUSET_DEFAULT, k).unwrap();
        assert_eq!(&got, v);

        match debugger.get(DBType::Kv, CAUSET_DEFAULT, b"foo") {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }

    #[test]
    fn test_violetabft_log() {
        let debugger = new_debugger();
        let engine = &debugger.engines.violetabft;
        let (brane_id, log_index) = (1, 1);
        let key = tuplespaceInstanton::violetabft_log_key(brane_id, log_index);
        let mut entry = Entry::default();
        entry.set_term(1);
        entry.set_index(1);
        entry.set_entry_type(EntryType::EntryNormal);
        entry.set_data(vec![42]);
        engine.put_msg(&key, &entry).unwrap();
        assert_eq!(engine.get_msg::<Entry>(&key).unwrap().unwrap(), entry);

        assert_eq!(debugger.violetabft_log(brane_id, log_index).unwrap(), entry);
        match debugger.violetabft_log(brane_id + 1, log_index + 1) {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }

    #[test]
    fn test_brane_info() {
        let debugger = new_debugger();
        let violetabft_engine = &debugger.engines.violetabft;
        let kv_engine = &debugger.engines.kv;
        let brane_id = 1;

        let violetabft_state_key = tuplespaceInstanton::violetabft_state_key(brane_id);
        let mut violetabft_state = VioletaBftLocalState::default();
        violetabft_state.set_last_index(42);
        violetabft_engine.put_msg(&violetabft_state_key, &violetabft_state).unwrap();
        assert_eq!(
            violetabft_engine
                .get_msg::<VioletaBftLocalState>(&violetabft_state_key)
                .unwrap()
                .unwrap(),
            violetabft_state
        );

        let apply_state_key = tuplespaceInstanton::apply_state_key(brane_id);
        let mut apply_state = VioletaBftApplyState::default();
        apply_state.set_applied_index(42);
        kv_engine
            .put_msg_causet(CAUSET_VIOLETABFT, &apply_state_key, &apply_state)
            .unwrap();
        assert_eq!(
            kv_engine
                .get_msg_causet::<VioletaBftApplyState>(CAUSET_VIOLETABFT, &apply_state_key)
                .unwrap()
                .unwrap(),
            apply_state
        );

        let brane_state_key = tuplespaceInstanton::brane_state_key(brane_id);
        let mut brane_state = BraneLocalState::default();
        brane_state.set_state(PeerState::Tombstone);
        kv_engine
            .put_msg_causet(CAUSET_VIOLETABFT, &brane_state_key, &brane_state)
            .unwrap();
        assert_eq!(
            kv_engine
                .get_msg_causet::<BraneLocalState>(CAUSET_VIOLETABFT, &brane_state_key)
                .unwrap()
                .unwrap(),
            brane_state
        );

        assert_eq!(
            debugger.brane_info(brane_id).unwrap(),
            BraneInfo::new(Some(violetabft_state), Some(apply_state), Some(brane_state))
        );
        match debugger.brane_info(brane_id + 1) {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }

    #[test]
    fn test_brane_size() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv;

        let brane_id = 1;
        let brane_state_key = tuplespaceInstanton::brane_state_key(brane_id);
        let mut brane = Brane::default();
        brane.set_id(brane_id);
        brane.set_spacelike_key(b"a".to_vec());
        brane.set_lightlike_key(b"zz".to_vec());
        let mut state = BraneLocalState::default();
        state.set_brane(brane);
        engine
            .put_msg_causet(CAUSET_VIOLETABFT, &brane_state_key, &state)
            .unwrap();

        let causets = vec![CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_VIOLETABFT, CAUSET_WRITE];
        let (k, v) = (tuplespaceInstanton::data_key(b"k"), b"v");
        for causet in &causets {
            engine.put_causet(causet, k.as_slice(), v).unwrap();
        }

        let sizes = debugger.brane_size(brane_id, causets.clone()).unwrap();
        assert_eq!(sizes.len(), 4);
        for (causet, size) in sizes {
            causets.iter().find(|&&c| c == causet).unwrap();
            assert_eq!(size, k.len() + v.len());
        }
    }

    #[test]
    fn test_scan_tail_pointer() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv;

        let causet_default_data = vec![
            (b"k1", b"v", 5.into()),
            (b"k2", b"x", 10.into()),
            (b"k3", b"y", 15.into()),
        ];
        for &(prefix, value, ts) in &causet_default_data {
            let encoded_key = Key::from_raw(prefix).applightlike_ts(ts);
            let key = tuplespaceInstanton::data_key(encoded_key.as_encoded().as_slice());
            engine.put(key.as_slice(), value).unwrap();
        }

        let causet_lock_data = vec![
            (b"k1", LockType::Put, b"v", 5.into()),
            (b"k4", LockType::Dagger, b"x", 10.into()),
            (b"k5", LockType::Delete, b"y", 15.into()),
        ];
        for &(prefix, tp, value, version) in &causet_lock_data {
            let encoded_key = Key::from_raw(prefix);
            let key = tuplespaceInstanton::data_key(encoded_key.as_encoded().as_slice());
            let dagger = Dagger::new(
                tp,
                value.to_vec(),
                version,
                0,
                None,
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
            );
            let value = dagger.to_bytes();
            engine
                .put_causet(CAUSET_DAGGER, key.as_slice(), value.as_slice())
                .unwrap();
        }

        let causet_write_data = vec![
            (b"k2", WriteType::Put, 5.into(), 10.into()),
            (b"k3", WriteType::Put, 15.into(), 20.into()),
            (b"k6", WriteType::Dagger, 25.into(), 30.into()),
            (b"k7", WriteType::Rollback, 35.into(), 40.into()),
        ];
        for &(prefix, tp, spacelike_ts, commit_ts) in &causet_write_data {
            let encoded_key = Key::from_raw(prefix).applightlike_ts(commit_ts);
            let key = tuplespaceInstanton::data_key(encoded_key.as_encoded().as_slice());
            let write = Write::new(tp, spacelike_ts, None);
            let value = write.as_ref().to_bytes();
            engine
                .put_causet(CAUSET_WRITE, key.as_slice(), value.as_slice())
                .unwrap();
        }

        let mut count = 0;
        for key_and_tail_pointer in debugger.scan_tail_pointer(b"z", &[], 10).unwrap() {
            assert!(key_and_tail_pointer.is_ok());
            count += 1;
        }
        assert_eq!(count, 7);

        // Test scan with bad spacelike, lightlike or limit.
        assert!(debugger.scan_tail_pointer(b"z", b"", 0).is_err());
        assert!(debugger.scan_tail_pointer(b"z", b"x", 3).is_err());
    }

    #[test]
    fn test_tombstone_branes() {
        let debugger = new_debugger();
        debugger.set_store_id(11);
        let engine = &debugger.engines.kv;

        // brane 1 with peers at stores 11, 12, 13.
        let brane_1 = init_brane_state(engine.as_inner(), 1, &[11, 12, 13]);
        // Got the target brane from fidel, which doesn't contains the store.
        let mut target_brane_1 = brane_1.clone();
        target_brane_1.mut_peers().remove(0);
        target_brane_1.mut_brane_epoch().set_conf_ver(100);

        // brane 2 with peers at stores 11, 12, 13.
        let brane_2 = init_brane_state(engine.as_inner(), 2, &[11, 12, 13]);
        // Got the target brane from fidel, which has different peer_id.
        let mut target_brane_2 = brane_2.clone();
        target_brane_2.mut_peers()[0].set_id(100);
        target_brane_2.mut_brane_epoch().set_conf_ver(100);

        // brane 3 with peers at stores 21, 22, 23.
        let brane_3 = init_brane_state(engine.as_inner(), 3, &[21, 22, 23]);
        // Got the target brane from fidel but the peers are not changed.
        let mut target_brane_3 = brane_3;
        target_brane_3.mut_brane_epoch().set_conf_ver(100);

        // Test with bad target brane. No brane state in lmdb should be changed.
        let target_branes = vec![
            target_brane_1.clone(),
            target_brane_2.clone(),
            target_brane_3,
        ];
        let errors = debugger.set_brane_tombstone(target_branes).unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, 3);
        assert_eq!(
            get_brane_state(engine.as_inner(), 1).take_brane(),
            brane_1
        );
        assert_eq!(
            get_brane_state(engine.as_inner(), 2).take_brane(),
            brane_2
        );

        // After set_brane_tombstone success, all brane should be adjusted.
        let target_branes = vec![target_brane_1, target_brane_2];
        let errors = debugger.set_brane_tombstone(target_branes).unwrap();
        assert!(errors.is_empty());
        for &brane_id in &[1, 2] {
            let state = get_brane_state(engine.as_inner(), brane_id).get_state();
            assert_eq!(state, PeerState::Tombstone);
        }
    }

    #[test]
    fn test_tombstone_branes_by_id() {
        let debugger = new_debugger();
        debugger.set_store_id(11);
        let engine = &debugger.engines.kv;

        // tombstone brane 1 which currently not exists.
        let errors = debugger.set_brane_tombstone_by_id(vec![1]).unwrap();
        assert!(!errors.is_empty());

        // brane 1 with peers at stores 11, 12, 13.
        init_brane_state(engine.as_inner(), 1, &[11, 12, 13]);
        let mut expected_state = get_brane_state(engine.as_inner(), 1);
        expected_state.set_state(PeerState::Tombstone);

        // tombstone brane 1.
        let errors = debugger.set_brane_tombstone_by_id(vec![1]).unwrap();
        assert!(errors.is_empty());
        assert_eq!(get_brane_state(engine.as_inner(), 1), expected_state);

        // tombstone brane 1 again.
        let errors = debugger.set_brane_tombstone_by_id(vec![1]).unwrap();
        assert!(errors.is_empty());
        assert_eq!(get_brane_state(engine.as_inner(), 1), expected_state);
    }

    #[test]
    fn test_remove_failed_stores() {
        let debugger = new_debugger();
        debugger.set_store_id(100);
        let engine = &debugger.engines.kv;

        let get_brane_stores = |engine: &Arc<DB>, brane_id: u64| {
            get_brane_state(engine, brane_id)
                .get_brane()
                .get_peers()
                .iter()
                .map(|p| p.get_store_id())
                .collect::<Vec<_>>()
        };

        // brane 1 with peers at stores 11, 12, 13 and 14.
        init_brane_state(engine.as_inner(), 1, &[11, 12, 13, 14]);
        // brane 2 with peers at stores 21, 22 and 23.
        init_brane_state(engine.as_inner(), 2, &[21, 22, 23]);

        // Only remove specified stores from brane 1.
        debugger
            .remove_failed_stores(vec![13, 14, 21, 23], Some(vec![1]))
            .unwrap();

        // 13 and 14 should be removed from brane 1.
        assert_eq!(get_brane_stores(engine.as_inner(), 1), &[11, 12]);
        // 21 and 23 shouldn't be removed from brane 2.
        assert_eq!(get_brane_stores(engine.as_inner(), 2), &[21, 22, 23]);

        // Remove specified stores from all branes.
        debugger.remove_failed_stores(vec![11, 23], None).unwrap();

        assert_eq!(get_brane_stores(engine.as_inner(), 1), &[12]);
        assert_eq!(get_brane_stores(engine.as_inner(), 2), &[21, 22]);

        // Should fail when the store itself is in the failed list.
        init_brane_state(engine.as_inner(), 3, &[100, 31, 32, 33]);
        debugger.remove_failed_stores(vec![100], None).unwrap_err();
    }

    #[test]
    fn test_bad_branes() {
        let debugger = new_debugger();
        let kv_engine = &debugger.engines.kv;
        let violetabft_engine = &debugger.engines.violetabft;
        let store_id = 1; // It's a fake id.

        let mut wb1 = violetabft_engine.write_batch();
        let causet1 = CAUSET_DEFAULT;

        let mut wb2 = kv_engine.write_batch();
        let causet2 = CAUSET_VIOLETABFT;

        {
            let mock_brane_state = |wb: &mut LmdbWriteBatch, brane_id: u64, peers: &[u64]| {
                let brane_state_key = tuplespaceInstanton::brane_state_key(brane_id);
                let mut brane_state = BraneLocalState::default();
                brane_state.set_state(PeerState::Normal);
                {
                    let brane = brane_state.mut_brane();
                    brane.set_id(brane_id);
                    let peers = peers
                        .iter()
                        .enumerate()
                        .map(|(i, &sid)| {
                            let mut peer = Peer::default();
                            peer.id = i as u64;
                            peer.store_id = sid;
                            peer
                        })
                        .collect::<Vec<_>>();
                    brane.set_peers(peers.into());
                }
                wb.put_msg_causet(causet2, &brane_state_key, &brane_state)
                    .unwrap();
            };
            let mock_violetabft_state =
                |wb: &mut LmdbWriteBatch, brane_id: u64, last_index: u64, commit_index: u64| {
                    let violetabft_state_key = tuplespaceInstanton::violetabft_state_key(brane_id);
                    let mut violetabft_state = VioletaBftLocalState::default();
                    violetabft_state.set_last_index(last_index);
                    violetabft_state.mut_hard_state().set_commit(commit_index);
                    wb.put_msg_causet(causet1, &violetabft_state_key, &violetabft_state).unwrap();
                };
            let mock_apply_state = |wb: &mut LmdbWriteBatch, brane_id: u64, apply_index: u64| {
                let violetabft_apply_key = tuplespaceInstanton::apply_state_key(brane_id);
                let mut apply_state = VioletaBftApplyState::default();
                apply_state.set_applied_index(apply_index);
                wb.put_msg_causet(causet2, &violetabft_apply_key, &apply_state).unwrap();
            };

            for &brane_id in &[10, 11, 12] {
                mock_brane_state(&mut wb2, brane_id, &[store_id]);
            }

            // last index < commit index
            mock_violetabft_state(&mut wb1, 10, 100, 110);

            // commit index < last index < apply index, or commit index < apply index < last index.
            mock_violetabft_state(&mut wb1, 11, 100, 90);
            mock_apply_state(&mut wb2, 11, 110);
            mock_violetabft_state(&mut wb1, 12, 100, 90);
            mock_apply_state(&mut wb2, 12, 95);

            // brane state doesn't contains the peer itself.
            mock_brane_state(&mut wb2, 13, &[]);
        }

        violetabft_engine.write_opt(&wb1, &WriteOptions::new()).unwrap();
        kv_engine.write_opt(&wb2, &WriteOptions::new()).unwrap();

        let bad_branes = debugger.bad_branes().unwrap();
        assert_eq!(bad_branes.len(), 4);
        for (i, (brane_id, _)) in bad_branes.into_iter().enumerate() {
            assert_eq!(brane_id, (10 + i) as u64);
        }
    }

    #[test]
    fn test_recreate_brane() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv;

        let metadata = vec![("", "g"), ("g", "m"), ("m", "")];

        for (brane_id, (spacelike, lightlike)) in metadata.into_iter().enumerate() {
            let brane_id = brane_id as u64;
            let mut brane = Brane::default();
            brane.set_id(brane_id);
            brane.set_spacelike_key(spacelike.to_owned().into_bytes());
            brane.set_lightlike_key(lightlike.to_owned().into_bytes());

            let mut brane_state = BraneLocalState::default();
            brane_state.set_state(PeerState::Normal);
            brane_state.set_brane(brane);
            let key = tuplespaceInstanton::brane_state_key(brane_id);
            engine.put_msg_causet(CAUSET_VIOLETABFT, &key, &brane_state).unwrap();
        }

        let remove_brane_state = |brane_id: u64| {
            let key = tuplespaceInstanton::brane_state_key(brane_id);
            let causet_violetabft = engine.causet_handle(CAUSET_VIOLETABFT).unwrap();
            engine
                .as_inner()
                .delete_causet(causet_violetabft.as_inner(), &key)
                .unwrap();
        };

        let mut brane = Brane::default();
        brane.set_id(100);

        brane.set_spacelike_key(b"k".to_vec());
        brane.set_lightlike_key(b"z".to_vec());
        assert!(debugger.recreate_brane(brane.clone()).is_err());

        remove_brane_state(1);
        remove_brane_state(2);
        assert!(debugger.recreate_brane(brane.clone()).is_ok());
        assert_eq!(
            get_brane_state(engine.as_inner(), 100).get_brane(),
            &brane
        );

        brane.set_spacelike_key(b"z".to_vec());
        brane.set_lightlike_key(b"".to_vec());
        assert!(debugger.recreate_brane(brane).is_err());
    }

    #[test]
    fn test_tail_pointer_checker() {
        let (mut default, mut dagger, mut write) = (vec![], vec![], vec![]);
        enum Expect {
            Keep,
            Remove,
        };
        // test check CAUSET_DAGGER.
        default.extlightlike(vec![
            // key, spacelike_ts, check
            (b"k4", 100, Expect::Keep),
            (b"k5", 100, Expect::Keep),
        ]);
        dagger.extlightlike(vec![
            // key, spacelike_ts, for_ufidelate_ts, lock_type, short_value, check
            (b"k1", 100, 0, LockType::Put, false, Expect::Remove), // k1: remove orphan dagger.
            (b"k2", 100, 0, LockType::Delete, false, Expect::Keep), // k2: Delete doesn't need default.
            (b"k3", 100, 0, LockType::Put, true, Expect::Keep), // k3: short value doesn't need default.
            (b"k4", 100, 0, LockType::Put, false, Expect::Keep), // k4: corresponding default exists.
            (b"k5", 100, 0, LockType::Put, false, Expect::Remove), // k5: duplicated dagger and write.
        ]);
        write.extlightlike(vec![
            // key, spacelike_ts, commit_ts, write_type, short_value, check
            (b"k5", 100, 101, WriteType::Put, false, Expect::Keep),
        ]);

        // test match CAUSET_DEFAULT and CAUSET_WRITE.
        default.extlightlike(vec![
            // key, spacelike_ts
            (b"k6", 96, Expect::Remove), // extra default.
            (b"k6", 94, Expect::Keep),   // ok.
            (b"k6", 90, Expect::Remove), // Delete should not have default.
            (b"k6", 88, Expect::Remove), // Default is redundant if write has short value.
        ]);
        write.extlightlike(vec![
            // key, spacelike_ts, commit_ts, write_type, short_value
            (b"k6", 100, 101, WriteType::Put, true, Expect::Keep), // short value doesn't need default.
            (b"k6", 99, 99, WriteType::Rollback, false, Expect::Keep), // rollback doesn't need default.
            (b"k6", 97, 98, WriteType::Delete, false, Expect::Keep), // delete doesn't need default.
            (b"k6", 94, 94, WriteType::Put, false, Expect::Keep),    // ok.
            (b"k6", 92, 93, WriteType::Put, false, Expect::Remove),  // extra write.
            (b"k6", 90, 91, WriteType::Delete, false, Expect::Keep),
            (b"k6", 88, 89, WriteType::Put, true, Expect::Keep),
        ]);

        // Combine problems together.
        default.extlightlike(vec![
            // key, spacelike_ts
            (b"k7", 98, Expect::Remove), // default without dagger or write.
            (b"k7", 90, Expect::Remove), // orphan default.
        ]);
        dagger.extlightlike(vec![
            // key, spacelike_ts, for_ufidelate_ts, lock_type, short_value, check
            (b"k7", 100, 0, LockType::Put, false, Expect::Remove), // duplicated dagger and write.
        ]);
        write.extlightlike(vec![
            // key, spacelike_ts, commit_ts, write_type, short_value
            (b"k7", 99, 100, WriteType::Put, false, Expect::Remove), // write without default.
            (b"k7", 96, 97, WriteType::Put, true, Expect::Keep),
        ]);

        // Locks from pessimistic bundles
        default.extlightlike(vec![
            // key, spacelike_ts
            (b"k8", 100, Expect::Keep),
            (b"k9", 100, Expect::Keep),
        ]);
        dagger.extlightlike(vec![
            // key, spacelike_ts, for_ufidelate_ts, lock_type, short_value, check
            (b"k8", 90, 105, LockType::Pessimistic, false, Expect::Remove), // newer writes exist
            (b"k9", 90, 115, LockType::Put, true, Expect::Keep), // prewritten dagger from a pessimistic txn
        ]);
        write.extlightlike(vec![
            // key, spacelike_ts, commit_ts, write_type, short_value
            (b"k8", 100, 110, WriteType::Put, false, Expect::Keep),
            (b"k9", 100, 110, WriteType::Put, false, Expect::Keep),
        ]);

        // Out of cone.
        default.extlightlike(vec![
            // key, spacelike_ts
            (b"l0", 100, Expect::Keep),
        ]);
        dagger.extlightlike(vec![
            // key, spacelike_ts, for_ufidelate_ts, lock_type, short_value, check
            (b"l0", 101, 0, LockType::Put, false, Expect::Keep),
        ]);
        write.extlightlike(vec![
            // key, spacelike_ts, commit_ts, write_type, short_value
            (b"l0", 102, 103, WriteType::Put, false, Expect::Keep),
        ]);

        let mut kv = vec![];
        for (key, ts, expect) in default {
            kv.push((
                CAUSET_DEFAULT,
                Key::from_raw(key).applightlike_ts(ts.into()),
                b"v".to_vec(),
                expect,
            ));
        }
        for (key, ts, for_ufidelate_ts, tp, short_value, expect) in dagger {
            let v = if short_value {
                Some(b"v".to_vec())
            } else {
                None
            };
            let dagger = Dagger::new(
                tp,
                vec![],
                ts.into(),
                0,
                v,
                for_ufidelate_ts.into(),
                0,
                TimeStamp::zero(),
            );
            kv.push((CAUSET_DAGGER, Key::from_raw(key), dagger.to_bytes(), expect));
        }
        for (key, spacelike_ts, commit_ts, tp, short_value, expect) in write {
            let v = if short_value {
                Some(b"v".to_vec())
            } else {
                None
            };
            let write = Write::new(tp, spacelike_ts.into(), v);
            kv.push((
                CAUSET_WRITE,
                Key::from_raw(key).applightlike_ts(commit_ts.into()),
                write.as_ref().to_bytes(),
                expect,
            ));
        }

        let path = Builder::new()
            .prefix("test_tail_pointer_checker")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let causets_opts = ALL_CAUSETS
            .iter()
            .map(|causet| CAUSETOptions::new(causet, PrimaryCausetNetworkOptions::new()))
            .collect();
        let db = Arc::new(new_engine_opt(path_str, DBOptions::new(), causets_opts).unwrap());
        // Write initial KVs.
        let mut wb = db.c().write_batch();
        for &(causet, ref k, ref v, _) in &kv {
            wb.put_causet(causet, &tuplespaceInstanton::data_key(k.as_encoded()), v).unwrap();
        }
        db.c().write(&wb).unwrap();
        // Fix problems.
        let mut checker = MvccChecker::new(Arc::clone(&db), b"k", b"l").unwrap();
        let mut wb = db.c().write_batch();
        checker.check_tail_pointer(&mut wb, None).unwrap();
        db.c().write(&wb).unwrap();
        // Check result.
        for (causet, k, _, expect) in kv {
            let data = db
                .get_causet(
                    get_causet_handle(&db, causet).unwrap(),
                    &tuplespaceInstanton::data_key(k.as_encoded()),
                )
                .unwrap();
            match expect {
                Expect::Keep => assert!(data.is_some()),
                Expect::Remove => assert!(data.is_none()),
            }
        }
    }

    #[test]
    fn test_debug_raw_scan() {
        let tuplespaceInstanton: &[&[u8]] = &[
            b"a",
            b"a1",
            b"a2",
            b"a2\x00",
            b"a2\x00\x00",
            b"b",
            b"b1",
            b"b2",
            b"b2\x00",
            b"b2\x00\x00",
            b"c",
            b"c1",
            b"c2",
            b"c2\x00",
            b"c2\x00\x00",
        ];

        let debugger = new_debugger();

        let mut wb = debugger.engines.kv.write_batch();
        for key in tuplespaceInstanton {
            let data_key = tuplespaceInstanton::data_key(key);
            let value = key.to_vec();
            wb.put(&data_key, &value).unwrap();
        }
        debugger.engines.kv.write(&wb).unwrap();

        let check = |result: Result<_>, expected: &[&[u8]]| {
            assert_eq!(
                result.unwrap(),
                expected
                    .iter()
                    .map(|k| (tuplespaceInstanton::data_key(k), k.to_vec()))
                    .collect::<Vec<_>>()
            );
        };

        check(debugger.raw_scan(b"z", &[b'z' + 1], 100, CAUSET_DEFAULT), tuplespaceInstanton);
        check(debugger.raw_scan(b"za", b"zz", 100, CAUSET_DEFAULT), tuplespaceInstanton);
        check(debugger.raw_scan(b"za1", b"za1", 100, CAUSET_DEFAULT), &[]);
        check(
            debugger.raw_scan(b"za1", b"za2\x00\x00", 100, CAUSET_DEFAULT),
            &tuplespaceInstanton[1..4],
        );
        check(
            debugger.raw_scan(b"za2\x00", b"za2\x00\x00", 100, CAUSET_DEFAULT),
            &tuplespaceInstanton[3..4],
        );
        check(
            debugger.raw_scan(b"zb\x00", b"zb2\x00\x00", 100, CAUSET_DEFAULT),
            &tuplespaceInstanton[6..9],
        );
        check(debugger.raw_scan(b"za1", b"zz", 1, CAUSET_DEFAULT), &tuplespaceInstanton[1..2]);
        check(debugger.raw_scan(b"za1", b"zz", 3, CAUSET_DEFAULT), &tuplespaceInstanton[1..4]);
        check(
            debugger.raw_scan(b"za1", b"zb2\x00\x00", 8, CAUSET_DEFAULT),
            &tuplespaceInstanton[1..9],
        );
    }

    #[test]
    fn test_store_brane() {
        let debugger = new_debugger();
        let store_id: u64 = 42;
        let cluster_id: u64 = 4242;
        debugger.set_store_id(store_id);
        debugger.set_cluster_id(cluster_id);
        assert_eq!(store_id, debugger.get_store_id().expect("get store id"));
        assert_eq!(
            cluster_id,
            debugger.get_cluster_id().expect("get cluster id")
        );
    }
}
