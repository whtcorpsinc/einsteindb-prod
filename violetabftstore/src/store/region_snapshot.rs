// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use edb::{
    IterOptions, CausetEngine, Peekable, ReadOptions, Result as EngineResult, Snapshot,
};
use ekvproto::meta_timeshare::Brane;
use ekvproto::violetabft_server_timeshare::VioletaBftApplyState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::store::{util, PeerStorage};
use crate::{Error, Result};
use edb::util::check_key_in_cone;
use edb::VioletaBftEngine;
use edb::Causet_VIOLETABFT;
use edb::{Error as EngineError, Iterable, Iteron};
use tuplespaceInstanton::DATA_PREFIX_KEY;
use violetabftstore::interlock::::CausetLearnedKey::CausetLearnedKey;
use violetabftstore::interlock::::metrics::CRITICAL_ERROR;
use violetabftstore::interlock::::{panic_when_unexpected_key_or_data, set_panic_mark};

/// Snapshot of a brane.
///
/// Only data within a brane can be accessed.
#[derive(Debug)]
pub struct BraneSnapshot<S: Snapshot> {
    snap: Arc<S>,
    brane: Arc<Brane>,
    apply_index: Arc<AtomicU64>,
    // `None` means the snapshot does not care about max_ts
    pub max_ts_sync_status: Option<Arc<AtomicU64>>,
}

impl<S> BraneSnapshot<S>
where
    S: Snapshot,
{
    #[allow(clippy::new_ret_no_self)] // temporary until this returns BraneSnapshot<E>
    pub fn new<EK>(ps: &PeerStorage<EK, impl VioletaBftEngine>) -> BraneSnapshot<EK::Snapshot>
    where
        EK: CausetEngine,
    {
        BraneSnapshot::from_snapshot(Arc::new(ps.raw_snapshot()), Arc::new(ps.brane().clone()))
    }

    pub fn from_raw<EK>(db: EK, brane: Brane) -> BraneSnapshot<EK::Snapshot>
    where
        EK: CausetEngine,
    {
        BraneSnapshot::from_snapshot(Arc::new(db.snapshot()), Arc::new(brane))
    }

    pub fn from_snapshot(snap: Arc<S>, brane: Arc<Brane>) -> BraneSnapshot<S> {
        BraneSnapshot {
            snap,
            brane,
            // Use 0 to indicate that the apply index is missing and we need to KvGet it,
            // since apply index must be >= VIOLETABFT_INIT_LOG_INDEX.
            apply_index: Arc::new(AtomicU64::new(0)),
            max_ts_sync_status: None,
        }
    }

    #[inline]
    pub fn get_brane(&self) -> &Brane {
        &self.brane
    }

    #[inline]
    pub fn get_snapshot(&self) -> &S {
        self.snap.as_ref()
    }

    #[inline]
    pub fn get_apply_index(&self) -> Result<u64> {
        let apply_index = self.apply_index.load(Ordering::SeqCst);
        if apply_index == 0 {
            self.get_apply_index_from_causet_storage()
        } else {
            Ok(apply_index)
        }
    }

    fn get_apply_index_from_causet_storage(&self) -> Result<u64> {
        let apply_state: Option<VioletaBftApplyState> = self
            .snap
            .get_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(self.brane.get_id()))?;
        match apply_state {
            Some(s) => {
                let apply_index = s.get_applied_index();
                self.apply_index.store(apply_index, Ordering::SeqCst);
                Ok(apply_index)
            }
            None => Err(box_err!("Unable to get applied index")),
        }
    }

    pub fn iter(&self, iter_opt: IterOptions) -> BraneIterator<S> {
        BraneIterator::new(&self.snap, Arc::clone(&self.brane), iter_opt)
    }

    pub fn iter_causet(&self, causet: &str, iter_opt: IterOptions) -> Result<BraneIterator<S>> {
        Ok(BraneIterator::new_causet(
            &self.snap,
            Arc::clone(&self.brane),
            iter_opt,
            causet,
        ))
    }

    // scan scans database using an Iteron in cone [spacelike_key, lightlike_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    pub fn scan<F>(&self, spacelike_key: &[u8], lightlike_key: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let spacelike = CausetLearnedKey::from_slice(spacelike_key, DATA_PREFIX_KEY.len(), 0);
        let lightlike = CausetLearnedKey::from_slice(lightlike_key, DATA_PREFIX_KEY.len(), 0);
        let iter_opt = IterOptions::new(Some(spacelike), Some(lightlike), fill_cache);
        self.scan_impl(self.iter(iter_opt), spacelike_key, f)
    }

    // like `scan`, only on a specific PrimaryCauset family.
    pub fn scan_causet<F>(
        &self,
        causet: &str,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let spacelike = CausetLearnedKey::from_slice(spacelike_key, DATA_PREFIX_KEY.len(), 0);
        let lightlike = CausetLearnedKey::from_slice(lightlike_key, DATA_PREFIX_KEY.len(), 0);
        let iter_opt = IterOptions::new(Some(spacelike), Some(lightlike), fill_cache);
        self.scan_impl(self.iter_causet(causet, iter_opt)?, spacelike_key, f)
    }

    fn scan_impl<F>(&self, mut it: BraneIterator<S>, spacelike_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let mut it_valid = it.seek(spacelike_key)?;
        while it_valid {
            it_valid = f(it.key(), it.value())? && it.next()?;
        }
        Ok(())
    }

    #[inline]
    pub fn get_spacelike_key(&self) -> &[u8] {
        self.brane.get_spacelike_key()
    }

    #[inline]
    pub fn get_lightlike_key(&self) -> &[u8] {
        self.brane.get_lightlike_key()
    }
}

impl<S> Clone for BraneSnapshot<S>
where
    S: Snapshot,
{
    fn clone(&self) -> Self {
        BraneSnapshot {
            snap: self.snap.clone(),
            brane: Arc::clone(&self.brane),
            apply_index: Arc::clone(&self.apply_index),
            max_ts_sync_status: self.max_ts_sync_status.clone(),
        }
    }
}

impl<S> Peekable for BraneSnapshot<S>
where
    S: Snapshot,
{
    type DBVector = <S as Peekable>::DBVector;

    fn get_value_opt(
        &self,
        opts: &ReadOptions,
        key: &[u8],
    ) -> EngineResult<Option<Self::DBVector>> {
        check_key_in_cone(
            key,
            self.brane.get_id(),
            self.brane.get_spacelike_key(),
            self.brane.get_lightlike_key(),
        )
        .map_err(|e| EngineError::Other(box_err!(e)))?;
        let data_key = tuplespaceInstanton::data_key(key);
        self.snap
            .get_value_opt(opts, &data_key)
            .map_err(|e| self.handle_get_value_error(e, "", key))
    }

    fn get_value_causet_opt(
        &self,
        opts: &ReadOptions,
        causet: &str,
        key: &[u8],
    ) -> EngineResult<Option<Self::DBVector>> {
        check_key_in_cone(
            key,
            self.brane.get_id(),
            self.brane.get_spacelike_key(),
            self.brane.get_lightlike_key(),
        )
        .map_err(|e| EngineError::Other(box_err!(e)))?;
        let data_key = tuplespaceInstanton::data_key(key);
        self.snap
            .get_value_causet_opt(opts, causet, &data_key)
            .map_err(|e| self.handle_get_value_error(e, causet, key))
    }
}

impl<S> BraneSnapshot<S>
where
    S: Snapshot,
{
    #[inline(never)]
    fn handle_get_value_error(&self, e: EngineError, causet: &str, key: &[u8]) -> EngineError {
        CRITICAL_ERROR.with_label_values(&["lmdb get"]).inc();
        if panic_when_unexpected_key_or_data() {
            set_panic_mark();
            panic!(
                "failed to get value of key {} in brane {}: {:?}",
                hex::encode_upper(&key),
                self.brane.get_id(),
                e,
            );
        } else {
            error!(
                "failed to get value of key in causet";
                "key" => hex::encode_upper(&key),
                "brane" => self.brane.get_id(),
                "causet" => causet,
                "error" => ?e,
            );
            e
        }
    }
}

/// `BraneIterator` wrap a lmdb Iteron and only allow it to
/// iterate in the brane. It behaves as if underlying
/// db only contains one brane.
pub struct BraneIterator<S: Snapshot> {
    iter: <S as Iterable>::Iteron,
    brane: Arc<Brane>,
}

fn fidelio_lower_bound(iter_opt: &mut IterOptions, brane: &Brane) {
    let brane_spacelike_key = tuplespaceInstanton::enc_spacelike_key(brane);
    if iter_opt.lower_bound().is_some() && !iter_opt.lower_bound().as_ref().unwrap().is_empty() {
        iter_opt.set_lower_bound_prefix(tuplespaceInstanton::DATA_PREFIX_KEY);
        if brane_spacelike_key.as_slice() > *iter_opt.lower_bound().as_ref().unwrap() {
            iter_opt.set_vec_lower_bound(brane_spacelike_key);
        }
    } else {
        iter_opt.set_vec_lower_bound(brane_spacelike_key);
    }
}

fn fidelio_upper_bound(iter_opt: &mut IterOptions, brane: &Brane) {
    let brane_lightlike_key = tuplespaceInstanton::enc_lightlike_key(brane);
    if iter_opt.upper_bound().is_some() && !iter_opt.upper_bound().as_ref().unwrap().is_empty() {
        iter_opt.set_upper_bound_prefix(tuplespaceInstanton::DATA_PREFIX_KEY);
        if brane_lightlike_key.as_slice() < *iter_opt.upper_bound().as_ref().unwrap() {
            iter_opt.set_vec_upper_bound(brane_lightlike_key);
        }
    } else {
        iter_opt.set_vec_upper_bound(brane_lightlike_key);
    }
}

// we use engine::rocks's style Iteron, doesn't need to impl std Iteron.
impl<S> BraneIterator<S>
where
    S: Snapshot,
{
    pub fn new(snap: &S, brane: Arc<Brane>, mut iter_opt: IterOptions) -> BraneIterator<S> {
        fidelio_lower_bound(&mut iter_opt, &brane);
        fidelio_upper_bound(&mut iter_opt, &brane);
        let iter = snap
            .Iteron_opt(iter_opt)
            .expect("creating snapshot Iteron"); // FIXME error handling
        BraneIterator { iter, brane }
    }

    pub fn new_causet(
        snap: &S,
        brane: Arc<Brane>,
        mut iter_opt: IterOptions,
        causet: &str,
    ) -> BraneIterator<S> {
        fidelio_lower_bound(&mut iter_opt, &brane);
        fidelio_upper_bound(&mut iter_opt, &brane);
        let iter = snap
            .Iteron_causet_opt(causet, iter_opt)
            .expect("creating snapshot Iteron"); // FIXME error handling
        BraneIterator { iter, brane }
    }

    pub fn seek_to_first(&mut self) -> Result<bool> {
        self.iter.seek_to_first().map_err(Error::from)
    }

    pub fn seek_to_last(&mut self) -> Result<bool> {
        self.iter.seek_to_last().map_err(Error::from)
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        fail_point!("brane_snapshot_seek", |_| {
            Err(box_err!("brane seek error"))
        });
        self.should_seekable(key)?;
        let key = tuplespaceInstanton::data_key(key);
        self.iter.seek(key.as_slice().into()).map_err(Error::from)
    }

    pub fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.should_seekable(key)?;
        let key = tuplespaceInstanton::data_key(key);
        self.iter
            .seek_for_prev(key.as_slice().into())
            .map_err(Error::from)
    }

    pub fn prev(&mut self) -> Result<bool> {
        self.iter.prev().map_err(Error::from)
    }

    pub fn next(&mut self) -> Result<bool> {
        self.iter.next().map_err(Error::from)
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        tuplespaceInstanton::origin_key(self.iter.key())
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        self.iter.value()
    }

    #[inline]
    pub fn valid(&self) -> Result<bool> {
        self.iter.valid().map_err(Error::from)
    }

    #[inline]
    pub fn should_seekable(&self, key: &[u8]) -> Result<()> {
        if let Err(e) = util::check_key_in_brane_inclusive(key, &self.brane) {
            return handle_check_key_in_brane_error(e);
        }
        Ok(())
    }
}

#[inline(never)]
fn handle_check_key_in_brane_error(e: crate::Error) -> Result<()> {
    // Split out the error case to reduce hot-path code size.
    CRITICAL_ERROR
        .with_label_values(&["key not in brane"])
        .inc();
    if panic_when_unexpected_key_or_data() {
        set_panic_mark();
        panic!("key exceed bound: {:?}", e);
    } else {
        Err(e)
    }
}

#[causet(test)]
mod tests {
    use crate::store::PeerStorage;
    use crate::Result;

    use engine_lmdb::util::new_temp_engine;
    use engine_lmdb::{LmdbEngine, LmdbSnapshot};
    use edb::{CompactExt, Engines, MiscExt, Peekable, SyncMuBlock};
    use tuplespaceInstanton::data_key;
    use ekvproto::meta_timeshare::{Peer, Brane};
    use tempfile::Builder;
    use violetabftstore::interlock::::worker;

    use super::*;

    type DataSet = Vec<(Vec<u8>, Vec<u8>)>;

    fn new_peer_causet_storage(
        engines: Engines<LmdbEngine, LmdbEngine>,
        r: &Brane,
    ) -> PeerStorage<LmdbEngine, LmdbEngine> {
        let (sched, _) = worker::dummy_interlock_semaphore();
        PeerStorage::new(engines, r, sched, 0, "".to_owned()).unwrap()
    }

    fn load_default_dataset(
        engines: Engines<LmdbEngine, LmdbEngine>,
    ) -> (PeerStorage<LmdbEngine, LmdbEngine>, DataSet) {
        let mut r = Brane::default();
        r.mut_peers().push(Peer::default());
        r.set_id(10);
        r.set_spacelike_key(b"a2".to_vec());
        r.set_lightlike_key(b"a7".to_vec());

        let base_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
            (b"a9".to_vec(), b"v9".to_vec()),
        ];

        for &(ref k, ref v) in &base_data {
            engines.kv.put(&data_key(k), v).unwrap();
        }
        let store = new_peer_causet_storage(engines, &r);
        (store, base_data)
    }

    fn load_multiple_levels_dataset(
        engines: Engines<LmdbEngine, LmdbEngine>,
    ) -> (PeerStorage<LmdbEngine, LmdbEngine>, DataSet) {
        let mut r = Brane::default();
        r.mut_peers().push(Peer::default());
        r.set_id(10);
        r.set_spacelike_key(b"a04".to_vec());
        r.set_lightlike_key(b"a15".to_vec());

        let levels = vec![
            (b"a01".to_vec(), 1),
            (b"a02".to_vec(), 5),
            (b"a03".to_vec(), 3),
            (b"a04".to_vec(), 4),
            (b"a05".to_vec(), 1),
            (b"a06".to_vec(), 2),
            (b"a07".to_vec(), 2),
            (b"a08".to_vec(), 5),
            (b"a09".to_vec(), 6),
            (b"a10".to_vec(), 0),
            (b"a11".to_vec(), 1),
            (b"a12".to_vec(), 4),
            (b"a13".to_vec(), 2),
            (b"a14".to_vec(), 5),
            (b"a15".to_vec(), 3),
            (b"a16".to_vec(), 2),
            (b"a17".to_vec(), 1),
            (b"a18".to_vec(), 0),
        ];

        let mut data = vec![];
        {
            let db = &engines.kv;
            for &(ref k, level) in &levels {
                db.put(&data_key(k), k).unwrap();
                db.flush(true).unwrap();
                data.push((k.to_vec(), k.to_vec()));
                db.compact_files_in_cone(Some(&data_key(k)), Some(&data_key(k)), Some(level))
                    .unwrap();
            }
        }

        let store = new_peer_causet_storage(engines, &r);
        (store, data)
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("test-violetabftstore").temfidelir().unwrap();
        let engines = new_temp_engine(&path);
        let mut r = Brane::default();
        r.set_id(10);
        r.set_spacelike_key(b"key0".to_vec());
        r.set_lightlike_key(b"key4".to_vec());
        let store = new_peer_causet_storage(engines.clone(), &r);

        let key3 = b"key3";
        engines.kv.put_msg(&data_key(key3), &r).expect("");

        let snap = BraneSnapshot::<LmdbSnapshot>::new(&store);
        let v3 = snap.get_msg(key3).expect("");
        assert_eq!(v3, Some(r));

        let v0 = snap.get_value(b"key0").expect("");
        assert!(v0.is_none());

        let v4 = snap.get_value(b"key5");
        assert!(v4.is_err());
    }

    #[allow(clippy::type_complexity)]
    #[test]
    fn test_seek_and_seek_prev() {
        let path = Builder::new().prefix("test-violetabftstore").temfidelir().unwrap();
        let engines = new_temp_engine(&path);
        let (store, _) = load_default_dataset(engines);
        let snap = BraneSnapshot::<LmdbSnapshot>::new(&store);

        let check_seek_result = |snap: &BraneSnapshot<LmdbSnapshot>,
                                 lower_bound: Option<&[u8]>,
                                 upper_bound: Option<&[u8]>,
                                 seek_Block: &Vec<(
            &[u8],
            bool,
            Option<(&[u8], &[u8])>,
            Option<(&[u8], &[u8])>,
        )>| {
            let iter_opt = IterOptions::new(
                lower_bound.map(|v| CausetLearnedKey::from_slice(v, tuplespaceInstanton::DATA_PREFIX_KEY.len(), 0)),
                upper_bound.map(|v| CausetLearnedKey::from_slice(v, tuplespaceInstanton::DATA_PREFIX_KEY.len(), 0)),
                true,
            );
            let mut iter = snap.iter(iter_opt);
            for (seek_key, in_cone, seek_exp, prev_exp) in seek_Block.clone() {
                let check_res = |iter: &BraneIterator<LmdbSnapshot>,
                                 res: Result<bool>,
                                 exp: Option<(&[u8], &[u8])>| {
                    if !in_cone {
                        assert!(
                            res.is_err(),
                            "exp failed at {}",
                            hex::encode_upper(seek_key)
                        );
                        return;
                    }
                    if exp.is_none() {
                        assert!(!res.unwrap(), "exp none at {}", hex::encode_upper(seek_key));
                        return;
                    }

                    assert!(
                        res.unwrap(),
                        "should succeed at {}",
                        hex::encode_upper(seek_key)
                    );
                    let (exp_key, exp_val) = exp.unwrap();
                    assert_eq!(iter.key(), exp_key);
                    assert_eq!(iter.value(), exp_val);
                };
                let seek_res = iter.seek(seek_key);
                check_res(&iter, seek_res, seek_exp);
                let prev_res = iter.seek_for_prev(seek_key);
                check_res(&iter, prev_res, prev_exp);
            }
        };

        let mut seek_Block: Vec<(&[u8], bool, Option<(&[u8], &[u8])>, Option<(&[u8], &[u8])>)> = vec![
            (b"a1", false, None, None),
            (b"a2", true, Some((b"a3", b"v3")), None),
            (b"a3", true, Some((b"a3", b"v3")), Some((b"a3", b"v3"))),
            (b"a4", true, Some((b"a5", b"v5")), Some((b"a3", b"v3"))),
            (b"a6", true, None, Some((b"a5", b"v5"))),
            (b"a7", true, None, Some((b"a5", b"v5"))),
            (b"a9", false, None, None),
        ];
        check_seek_result(&snap, None, None, &seek_Block);
        check_seek_result(&snap, None, Some(b"a9"), &seek_Block);
        check_seek_result(&snap, Some(b"a1"), None, &seek_Block);
        check_seek_result(&snap, Some(b""), Some(b""), &seek_Block);
        check_seek_result(&snap, Some(b"a1"), Some(b"a9"), &seek_Block);
        check_seek_result(&snap, Some(b"a2"), Some(b"a9"), &seek_Block);
        check_seek_result(&snap, Some(b"a2"), Some(b"a7"), &seek_Block);
        check_seek_result(&snap, Some(b"a1"), Some(b"a7"), &seek_Block);

        seek_Block = vec![
            (b"a1", false, None, None),
            (b"a2", true, None, None),
            (b"a3", true, None, None),
            (b"a4", true, None, None),
            (b"a6", true, None, None),
            (b"a7", true, None, None),
            (b"a9", false, None, None),
        ];
        check_seek_result(&snap, None, Some(b"a1"), &seek_Block);
        check_seek_result(&snap, Some(b"a8"), None, &seek_Block);
        check_seek_result(&snap, Some(b"a7"), Some(b"a2"), &seek_Block);

        let path = Builder::new().prefix("test-violetabftstore").temfidelir().unwrap();
        let engines = new_temp_engine(&path);
        let (store, _) = load_multiple_levels_dataset(engines);
        let snap = BraneSnapshot::<LmdbSnapshot>::new(&store);

        seek_Block = vec![
            (b"a01", false, None, None),
            (b"a03", false, None, None),
            (b"a05", true, Some((b"a05", b"a05")), Some((b"a05", b"a05"))),
            (b"a10", true, Some((b"a10", b"a10")), Some((b"a10", b"a10"))),
            (b"a14", true, Some((b"a14", b"a14")), Some((b"a14", b"a14"))),
            (b"a15", true, None, Some((b"a14", b"a14"))),
            (b"a18", false, None, None),
            (b"a19", false, None, None),
        ];
        check_seek_result(&snap, None, None, &seek_Block);
        check_seek_result(&snap, None, Some(b"a20"), &seek_Block);
        check_seek_result(&snap, Some(b"a00"), None, &seek_Block);
        check_seek_result(&snap, Some(b""), Some(b""), &seek_Block);
        check_seek_result(&snap, Some(b"a00"), Some(b"a20"), &seek_Block);
        check_seek_result(&snap, Some(b"a01"), Some(b"a20"), &seek_Block);
        check_seek_result(&snap, Some(b"a01"), Some(b"a15"), &seek_Block);
        check_seek_result(&snap, Some(b"a00"), Some(b"a15"), &seek_Block);
    }

    #[test]
    fn test_iterate() {
        let path = Builder::new().prefix("test-violetabftstore").temfidelir().unwrap();
        let engines = new_temp_engine(&path);
        let (store, base_data) = load_default_dataset(engines.clone());

        let snap = BraneSnapshot::<LmdbSnapshot>::new(&store);
        let mut data = vec![];
        snap.scan(b"a2", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 2);
        assert_eq!(data, &base_data[1..3]);

        data.clear();
        snap.scan(b"a2", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(false)
        })
        .unwrap();

        assert_eq!(data.len(), 1);

        let mut iter = snap.iter(IterOptions::default());
        assert!(iter.seek_to_first().unwrap());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next().unwrap() {
                break;
            }
        }
        assert_eq!(res, base_data[1..3].to_vec());

        // test last brane
        let mut brane = Brane::default();
        brane.mut_peers().push(Peer::default());
        let store = new_peer_causet_storage(engines.clone(), &brane);
        let snap = BraneSnapshot::<LmdbSnapshot>::new(&store);
        data.clear();
        snap.scan(b"", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 5);
        assert_eq!(data, base_data);

        let mut iter = snap.iter(IterOptions::default());
        assert!(iter.seek(b"a1").unwrap());

        assert!(iter.seek_to_first().unwrap());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next().unwrap() {
                break;
            }
        }
        assert_eq!(res, base_data);

        // test Iteron with upper bound
        let store = new_peer_causet_storage(engines, &brane);
        let snap = BraneSnapshot::<LmdbSnapshot>::new(&store);
        let mut iter = snap.iter(IterOptions::new(
            None,
            Some(CausetLearnedKey::from_slice(b"a5", DATA_PREFIX_KEY.len(), 0)),
            true,
        ));
        assert!(iter.seek_to_first().unwrap());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next().unwrap() {
                break;
            }
        }
        assert_eq!(res, base_data[0..2].to_vec());
    }

    #[test]
    fn test_reverse_iterate_with_lower_bound() {
        let path = Builder::new().prefix("test-violetabftstore").temfidelir().unwrap();
        let engines = new_temp_engine(&path);
        let (store, test_data) = load_default_dataset(engines);

        let snap = BraneSnapshot::<LmdbSnapshot>::new(&store);
        let mut iter_opt = IterOptions::default();
        iter_opt.set_lower_bound(b"a3", 1);
        let mut iter = snap.iter(iter_opt);
        assert!(iter.seek_to_last().unwrap());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.prev().unwrap() {
                break;
            }
        }
        res.sort();
        assert_eq!(res, test_data[1..3].to_vec());
    }
}
