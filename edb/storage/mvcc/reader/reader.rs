// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causetStorage::kv::{Cursor, ScanMode, Snapshot, Statistics};
use crate::causetStorage::tail_pointer::{default_not_found_error, Result};
use engine_lmdb::properties::MvccProperties;
use engine_lmdb::LmdbBlockPropertiesCollection;
use engine_promises::{IterOptions, BlockProperties, BlockPropertiesCollection};
use engine_promises::{CAUSET_DAGGER, CAUSET_WRITE};
use ekvproto::kvrpcpb::IsolationLevel;
use std::borrow::Cow;
use txn_types::{Key, Dagger, TimeStamp, Value, Write, WriteRef, WriteType};

const GC_MAX_ROW_VERSIONS_THRESHOLD: u64 = 100;

/// The result of `get_txn_commit_record`, which is used to get the status of a specified
/// transaction from write causet.
#[derive(Debug)]
pub enum TxnCommitRecord {
    /// The commit record of the given transaction is not found. But it's possible that there's
    /// another transaction's commit record, whose `commit_ts` equals to the current transaction's
    /// `spacelike_ts`. That kind of record will be returned via the `overlapped_write` field.
    /// In this case, if the current transaction is to be rolled back, the `overlapped_write` must not
    /// be overwritten.
    None { overlapped_write: Option<Write> },
    /// Found the transaction's write record.
    SingleRecord { commit_ts: TimeStamp, write: Write },
    /// The transaction's status is found in another transaction's record's `overlapped_rollback`
    /// field. This may happen when the current transaction's `spacelike_ts` is the same as the
    /// `commit_ts` of another transaction on this key.
    OverlappedRollback { commit_ts: TimeStamp },
}

impl TxnCommitRecord {
    pub fn exist(&self) -> bool {
        match self {
            Self::None { .. } => false,
            Self::SingleRecord { .. } | Self::OverlappedRollback { .. } => true,
        }
    }

    pub fn info(&self) -> Option<(TimeStamp, WriteType)> {
        match self {
            Self::None { .. } => None,
            Self::SingleRecord { commit_ts, write } => Some((*commit_ts, write.write_type)),
            Self::OverlappedRollback { commit_ts } => Some((*commit_ts, WriteType::Rollback)),
        }
    }

    pub fn unwrap_single_record(self) -> (TimeStamp, WriteType) {
        match self {
            Self::SingleRecord { commit_ts, write } => (commit_ts, write.write_type),
            _ => panic!("not a single record: {:?}", self),
        }
    }

    pub fn unwrap_overlapped_rollback(self) -> TimeStamp {
        match self {
            Self::OverlappedRollback { commit_ts } => commit_ts,
            _ => panic!("not an overlapped rollback record: {:?}", self),
        }
    }

    pub fn unwrap_none(self) -> Option<Write> {
        match self {
            Self::None { overlapped_write } => overlapped_write,
            _ => panic!("txn record found but not expected: {:?}", self),
        }
    }
}

pub struct MvccReader<S: Snapshot> {
    snapshot: S,
    pub statistics: Statistics,
    // cursors are used for speeding up scans.
    data_cursor: Option<Cursor<S::Iter>>,
    lock_cursor: Option<Cursor<S::Iter>>,
    pub write_cursor: Option<Cursor<S::Iter>>,

    scan_mode: Option<ScanMode>,
    key_only: bool,

    fill_cache: bool,
    isolation_level: IsolationLevel,
}

impl<S: Snapshot> MvccReader<S> {
    pub fn new(
        snapshot: S,
        scan_mode: Option<ScanMode>,
        fill_cache: bool,
        isolation_level: IsolationLevel,
    ) -> Self {
        Self {
            snapshot,
            statistics: Statistics::default(),
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            scan_mode,
            isolation_level,
            key_only: false,
            fill_cache,
        }
    }

    pub fn get_statistics(&self) -> &Statistics {
        &self.statistics
    }

    pub fn collect_statistics_into(&mut self, stats: &mut Statistics) {
        stats.add(&self.statistics);
        self.statistics = Statistics::default();
    }

    pub fn set_key_only(&mut self, key_only: bool) {
        self.key_only = key_only;
    }

    pub fn load_data(&mut self, key: &Key, write: Write) -> Result<Value> {
        assert_eq!(write.write_type, WriteType::Put);
        if self.key_only {
            return Ok(vec![]);
        }
        if let Some(val) = write.short_value {
            return Ok(val);
        }
        if self.scan_mode.is_some() && self.data_cursor.is_none() {
            let iter_opt = IterOptions::new(None, None, self.fill_cache);
            self.data_cursor = Some(self.snapshot.iter(iter_opt, self.get_scan_mode(true))?);
        }

        let k = key.clone().applightlike_ts(write.spacelike_ts);
        let val = if let Some(ref mut cursor) = self.data_cursor {
            cursor
                .get(&k, &mut self.statistics.data)?
                .map(|v| v.to_vec())
        } else {
            self.statistics.data.get += 1;
            self.snapshot.get(&k)?
        };

        match val {
            Some(val) => {
                self.statistics.data.processed_tuplespaceInstanton += 1;
                Ok(val)
            }
            None => Err(default_not_found_error(key.to_raw()?, "get")),
        }
    }

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Dagger>> {
        if self.scan_mode.is_some() && self.lock_cursor.is_none() {
            let iter_opt = IterOptions::new(None, None, true);
            let iter = self
                .snapshot
                .iter_causet(CAUSET_DAGGER, iter_opt, self.get_scan_mode(true))?;
            self.lock_cursor = Some(iter);
        }

        let res = if let Some(ref mut cursor) = self.lock_cursor {
            match cursor.get(key, &mut self.statistics.dagger)? {
                Some(v) => Some(Dagger::parse(v)?),
                None => None,
            }
        } else {
            self.statistics.dagger.get += 1;
            match self.snapshot.get_causet(CAUSET_DAGGER, key)? {
                Some(v) => Some(Dagger::parse(&v)?),
                None => None,
            }
        };

        Ok(res)
    }

    fn get_scan_mode(&self, allow_backward: bool) -> ScanMode {
        match self.scan_mode {
            Some(ScanMode::Forward) => ScanMode::Forward,
            Some(ScanMode::Backward) if allow_backward => ScanMode::Backward,
            _ => ScanMode::Mixed,
        }
    }

    pub fn seek_write(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<(TimeStamp, Write)>> {
        if self.scan_mode.is_some() {
            if self.write_cursor.is_none() {
                let iter_opt = IterOptions::new(None, None, self.fill_cache);
                let iter = self
                    .snapshot
                    .iter_causet(CAUSET_WRITE, iter_opt, self.get_scan_mode(false))?;
                self.write_cursor = Some(iter);
            }
        } else {
            // use prefix bloom filter
            let iter_opt = IterOptions::default()
                .use_prefix_seek()
                .set_prefix_same_as_spacelike(true);
            let iter = self.snapshot.iter_causet(CAUSET_WRITE, iter_opt, ScanMode::Mixed)?;
            self.write_cursor = Some(iter);
        }

        let cursor = self.write_cursor.as_mut().unwrap();
        let ok = cursor.near_seek(&key.clone().applightlike_ts(ts), &mut self.statistics.write)?;
        if !ok {
            return Ok(None);
        }
        let write_key = cursor.key(&mut self.statistics.write);
        let commit_ts = Key::decode_ts_from(write_key)?;
        if !Key::is_user_key_eq(write_key, key.as_encoded()) {
            return Ok(None);
        }
        let write = WriteRef::parse(cursor.value(&mut self.statistics.write))?.to_owned();
        Ok(Some((commit_ts, write)))
    }

    /// Checks if there is a dagger which blocks reading the key at the given ts.
    /// Returns the blocking dagger as the `Err` variant.
    fn check_lock(&mut self, key: &Key, ts: TimeStamp) -> Result<()> {
        if let Some(dagger) = self.load_lock(key)? {
            if let Err(e) = Dagger::check_ts_conflict(Cow::Owned(dagger), key, ts, &Default::default())
            {
                self.statistics.dagger.processed_tuplespaceInstanton += 1;
                return Err(e.into());
            }
        }
        Ok(())
    }

    pub fn get(
        &mut self,
        key: &Key,
        ts: TimeStamp,
        skip_lock_check: bool,
    ) -> Result<Option<Value>> {
        if !skip_lock_check {
            // Check for locks that signal concurrent writes.
            match self.isolation_level {
                IsolationLevel::Si => self.check_lock(key, ts)?,
                IsolationLevel::Rc => {}
            }
        }
        if let Some(write) = self.get_write(key, ts)? {
            Ok(Some(self.load_data(key, write)?))
        } else {
            Ok(None)
        }
    }

    pub fn get_write(&mut self, key: &Key, mut ts: TimeStamp) -> Result<Option<Write>> {
        loop {
            match self.seek_write(key, ts)? {
                Some((commit_ts, write)) => match write.write_type {
                    WriteType::Put => {
                        return Ok(Some(write));
                    }
                    WriteType::Delete => {
                        return Ok(None);
                    }
                    WriteType::Dagger | WriteType::Rollback => ts = commit_ts.prev(),
                },
                None => return Ok(None),
            }
        }
    }

    pub fn get_txn_commit_record(
        &mut self,
        key: &Key,
        spacelike_ts: TimeStamp,
    ) -> Result<TxnCommitRecord> {
        // It's possible a txn with a small `spacelike_ts` has a greater `commit_ts` than a txn with
        // a greater `spacelike_ts` in pessimistic transaction.
        // I.e., txn_1.commit_ts > txn_2.commit_ts > txn_2.spacelike_ts > txn_1.spacelike_ts.
        //
        // Scan all the versions from `TimeStamp::max()` to `spacelike_ts`.
        let mut seek_ts = TimeStamp::max();
        while let Some((commit_ts, write)) = self.seek_write(key, seek_ts)? {
            if write.spacelike_ts == spacelike_ts {
                return Ok(TxnCommitRecord::SingleRecord { commit_ts, write });
            }
            if commit_ts == spacelike_ts {
                if write.has_overlapped_rollback {
                    return Ok(TxnCommitRecord::OverlappedRollback { commit_ts });
                }
                return Ok(TxnCommitRecord::None {
                    overlapped_write: Some(write),
                });
            }
            if commit_ts < spacelike_ts {
                break;
            }
            seek_ts = commit_ts.prev();
        }
        Ok(TxnCommitRecord::None {
            overlapped_write: None,
        })
    }

    fn create_data_cursor(&mut self) -> Result<()> {
        if self.data_cursor.is_none() {
            let iter_opt = IterOptions::new(None, None, true);
            let iter = self.snapshot.iter(iter_opt, self.get_scan_mode(true))?;
            self.data_cursor = Some(iter);
        }
        Ok(())
    }

    fn create_write_cursor(&mut self) -> Result<()> {
        if self.write_cursor.is_none() {
            let iter_opt = IterOptions::new(None, None, true);
            let iter = self
                .snapshot
                .iter_causet(CAUSET_WRITE, iter_opt, self.get_scan_mode(true))?;
            self.write_cursor = Some(iter);
        }
        Ok(())
    }

    fn create_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_none() {
            let iter_opt = IterOptions::new(None, None, true);
            let iter = self
                .snapshot
                .iter_causet(CAUSET_DAGGER, iter_opt, self.get_scan_mode(true))?;
            self.lock_cursor = Some(iter);
        }
        Ok(())
    }

    /// Return the first committed key for which `spacelike_ts` equals to `ts`
    pub fn seek_ts(&mut self, ts: TimeStamp) -> Result<Option<Key>> {
        assert!(self.scan_mode.is_some());
        self.create_write_cursor()?;

        let cursor = self.write_cursor.as_mut().unwrap();
        let mut ok = cursor.seek_to_first(&mut self.statistics.write);

        while ok {
            if WriteRef::parse(cursor.value(&mut self.statistics.write))?.spacelike_ts == ts {
                return Ok(Some(
                    Key::from_encoded(cursor.key(&mut self.statistics.write).to_vec())
                        .truncate_ts()?,
                ));
            }
            ok = cursor.next(&mut self.statistics.write);
        }
        Ok(None)
    }

    /// Scan locks that satisfies `filter(dagger)` returns true, from the given spacelike key `spacelike`.
    /// At most `limit` locks will be returned. If `limit` is set to `0`, it means unlimited.
    ///
    /// The return type is `(locks, is_remain)`. `is_remain` indicates whether there MAY be
    /// remaining locks that can be scanned.
    pub fn scan_locks<F>(
        &mut self,
        spacelike: Option<&Key>,
        filter: F,
        limit: usize,
    ) -> Result<(Vec<(Key, Dagger)>, bool)>
    where
        F: Fn(&Dagger) -> bool,
    {
        self.create_lock_cursor()?;
        let cursor = self.lock_cursor.as_mut().unwrap();
        let ok = match spacelike {
            Some(ref x) => cursor.seek(x, &mut self.statistics.dagger)?,
            None => cursor.seek_to_first(&mut self.statistics.dagger),
        };
        if !ok {
            return Ok((vec![], false));
        }
        let mut locks = Vec::with_capacity(limit);
        while cursor.valid()? {
            let key = Key::from_encoded_slice(cursor.key(&mut self.statistics.dagger));
            let dagger = Dagger::parse(cursor.value(&mut self.statistics.dagger))?;
            if filter(&dagger) {
                locks.push((key, dagger));
                if limit > 0 && locks.len() == limit {
                    return Ok((locks, true));
                }
            }
            cursor.next(&mut self.statistics.dagger);
        }
        self.statistics.dagger.processed_tuplespaceInstanton += locks.len();
        // If we reach here, `cursor.valid()` is `false`, so there MUST be no more locks.
        Ok((locks, false))
    }

    pub fn scan_tuplespaceInstanton(
        &mut self,
        mut spacelike: Option<Key>,
        limit: usize,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        let iter_opt = IterOptions::new(None, None, self.fill_cache);
        let scan_mode = self.get_scan_mode(false);
        let mut cursor = self.snapshot.iter_causet(CAUSET_WRITE, iter_opt, scan_mode)?;
        let mut tuplespaceInstanton = vec![];
        loop {
            let ok = match spacelike {
                Some(ref x) => cursor.near_seek(x, &mut self.statistics.write)?,
                None => cursor.seek_to_first(&mut self.statistics.write),
            };
            if !ok {
                return Ok((tuplespaceInstanton, None));
            }
            if tuplespaceInstanton.len() >= limit {
                self.statistics.write.processed_tuplespaceInstanton += tuplespaceInstanton.len();
                return Ok((tuplespaceInstanton, spacelike));
            }
            let key =
                Key::from_encoded(cursor.key(&mut self.statistics.write).to_vec()).truncate_ts()?;
            spacelike = Some(key.clone().applightlike_ts(TimeStamp::zero()));
            tuplespaceInstanton.push(key);
        }
    }

    // Get all Value of the given key in CAUSET_DEFAULT
    pub fn scan_values_in_default(&mut self, key: &Key) -> Result<Vec<(TimeStamp, Value)>> {
        self.create_data_cursor()?;
        let cursor = self.data_cursor.as_mut().unwrap();
        let mut ok = cursor.seek(key, &mut self.statistics.data)?;
        if !ok {
            return Ok(vec![]);
        }
        let mut v = vec![];
        while ok {
            let cur_key = cursor.key(&mut self.statistics.data);
            let ts = Key::decode_ts_from(cur_key)?;
            if Key::is_user_key_eq(cur_key, key.as_encoded()) {
                v.push((ts, cursor.value(&mut self.statistics.data).to_vec()));
            } else {
                break;
            }
            ok = cursor.next(&mut self.statistics.data);
        }
        Ok(v)
    }
}

// Returns true if it needs gc.
// This is for optimization purpose, does not mean to be accurate.
pub fn check_need_gc(
    safe_point: TimeStamp,
    ratio_memory_barrier: f64,
    write_properties: &LmdbBlockPropertiesCollection,
) -> bool {
    // Always GC.
    if ratio_memory_barrier < 1.0 {
        return true;
    }

    let props = match get_tail_pointer_properties(safe_point, write_properties) {
        Some(v) => v,
        None => return true,
    };

    // No data older than safe_point to GC.
    if props.min_ts > safe_point {
        return false;
    }

    // Note: Since the properties are file-based, it can be false positive.
    // For example, multiple files can have a different version of the same Evcausetidx.

    // A lot of MVCC versions to GC.
    if props.num_versions as f64 > props.num_rows as f64 * ratio_memory_barrier {
        return true;
    }
    // A lot of non-effective MVCC versions to GC.
    if props.num_versions as f64 > props.num_puts as f64 * ratio_memory_barrier {
        return true;
    }

    // A lot of MVCC versions of a single Evcausetidx to GC.
    props.max_row_versions > GC_MAX_ROW_VERSIONS_THRESHOLD
}

fn get_tail_pointer_properties(
    safe_point: TimeStamp,
    collection: &LmdbBlockPropertiesCollection,
) -> Option<MvccProperties> {
    if collection.is_empty() {
        return None;
    }
    // Aggregate MVCC properties.
    let mut props = MvccProperties::new();
    for (_, v) in collection.iter() {
        let tail_pointer = match MvccProperties::decode(&v.user_collected_properties()) {
            Ok(v) => v,
            Err(_) => return None,
        };
        // Filter out properties after safe_point.
        if tail_pointer.min_ts > safe_point {
            continue;
        }
        props.add(&tail_pointer);
    }
    Some(props)
}

#[causet(test)]
mod tests {
    use super::*;

    use crate::causetStorage::kv::Modify;
    use crate::causetStorage::tail_pointer::{MvccReader, MvccTxn};

    use crate::causetStorage::txn::commit;
    use concurrency_manager::ConcurrencyManager;
    use engine_lmdb::properties::MvccPropertiesCollectorFactory;
    use engine_lmdb::raw::DB;
    use engine_lmdb::raw::{PrimaryCausetNetworkOptions, DBOptions};
    use engine_lmdb::raw_util::CAUSETOptions;
    use engine_lmdb::{Compat, LmdbSnapshot};
    use engine_promises::{MuBlock, BlockPropertiesExt, WriteBatchExt};
    use engine_promises::{ALL_CAUSETS, CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_VIOLETABFT, CAUSET_WRITE};
    use ekvproto::kvrpcpb::IsolationLevel;
    use ekvproto::metapb::{Peer, Brane};
    use violetabftstore::store::BraneSnapshot;
    use std::ops::Bound;
    use std::sync::Arc;
    use std::u64;
    use txn_types::{LockType, Mutation};

    struct BraneEngine {
        db: Arc<DB>,
        brane: Brane,
    }

    impl BraneEngine {
        fn new(db: &Arc<DB>, brane: &Brane) -> BraneEngine {
            BraneEngine {
                db: Arc::clone(&db),
                brane: brane.clone(),
            }
        }

        fn put(
            &mut self,
            pk: &[u8],
            spacelike_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let spacelike_ts = spacelike_ts.into();
            let m = Mutation::Put((Key::from_raw(pk), vec![]));
            self.prewrite(m, pk, spacelike_ts);
            self.commit(pk, spacelike_ts, commit_ts);
        }

        fn dagger(
            &mut self,
            pk: &[u8],
            spacelike_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let spacelike_ts = spacelike_ts.into();
            let m = Mutation::Dagger(Key::from_raw(pk));
            self.prewrite(m, pk, spacelike_ts);
            self.commit(pk, spacelike_ts, commit_ts);
        }

        fn delete(
            &mut self,
            pk: &[u8],
            spacelike_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let spacelike_ts = spacelike_ts.into();
            let m = Mutation::Delete(Key::from_raw(pk));
            self.prewrite(m, pk, spacelike_ts);
            self.commit(pk, spacelike_ts, commit_ts);
        }

        fn prewrite(&mut self, m: Mutation, pk: &[u8], spacelike_ts: impl Into<TimeStamp>) {
            let snap =
                BraneSnapshot::<LmdbSnapshot>::from_raw(self.db.c().clone(), self.brane.clone());
            let spacelike_ts = spacelike_ts.into();
            let cm = ConcurrencyManager::new(spacelike_ts);
            let mut txn = MvccTxn::new(snap, spacelike_ts, true, cm);

            txn.prewrite(m, pk, &None, false, 0, 0, TimeStamp::default())
                .unwrap();
            self.write(txn.into_modifies());
        }

        fn prewrite_pessimistic_lock(
            &mut self,
            m: Mutation,
            pk: &[u8],
            spacelike_ts: impl Into<TimeStamp>,
        ) {
            let snap =
                BraneSnapshot::<LmdbSnapshot>::from_raw(self.db.c().clone(), self.brane.clone());
            let spacelike_ts = spacelike_ts.into();
            let cm = ConcurrencyManager::new(spacelike_ts);
            let mut txn = MvccTxn::new(snap, spacelike_ts, true, cm);

            txn.pessimistic_prewrite(
                m,
                pk,
                &None,
                true,
                0,
                TimeStamp::default(),
                0,
                TimeStamp::default(),
                false,
            )
            .unwrap();
            self.write(txn.into_modifies());
        }

        fn acquire_pessimistic_lock(
            &mut self,
            k: Key,
            pk: &[u8],
            spacelike_ts: impl Into<TimeStamp>,
            for_ufidelate_ts: impl Into<TimeStamp>,
        ) {
            let snap =
                BraneSnapshot::<LmdbSnapshot>::from_raw(self.db.c().clone(), self.brane.clone());
            let for_ufidelate_ts = for_ufidelate_ts.into();
            let cm = ConcurrencyManager::new(for_ufidelate_ts);
            let mut txn = MvccTxn::new(snap, spacelike_ts.into(), true, cm);
            txn.acquire_pessimistic_lock(k, pk, false, 0, for_ufidelate_ts, false, TimeStamp::zero())
                .unwrap();
            self.write(txn.into_modifies());
        }

        fn commit(
            &mut self,
            pk: &[u8],
            spacelike_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let snap =
                BraneSnapshot::<LmdbSnapshot>::from_raw(self.db.c().clone(), self.brane.clone());
            let spacelike_ts = spacelike_ts.into();
            let cm = ConcurrencyManager::new(spacelike_ts);
            let mut txn = MvccTxn::new(snap, spacelike_ts, true, cm);
            commit(&mut txn, Key::from_raw(pk), commit_ts.into()).unwrap();
            self.write(txn.into_modifies());
        }

        fn rollback(&mut self, pk: &[u8], spacelike_ts: impl Into<TimeStamp>) {
            let snap =
                BraneSnapshot::<LmdbSnapshot>::from_raw(self.db.c().clone(), self.brane.clone());
            let spacelike_ts = spacelike_ts.into();
            let cm = ConcurrencyManager::new(spacelike_ts);
            let mut txn = MvccTxn::new(snap, spacelike_ts, true, cm);
            txn.collapse_rollback(false);
            txn.rollback(Key::from_raw(pk)).unwrap();
            self.write(txn.into_modifies());
        }

        fn rollback_protected(&mut self, pk: &[u8], spacelike_ts: impl Into<TimeStamp>) {
            let snap =
                BraneSnapshot::<LmdbSnapshot>::from_raw(self.db.c().clone(), self.brane.clone());
            let spacelike_ts = spacelike_ts.into();
            let cm = ConcurrencyManager::new(spacelike_ts);
            let mut txn = MvccTxn::new(snap, spacelike_ts, true, cm);
            txn.collapse_rollback(false);
            txn.cleanup(Key::from_raw(pk), TimeStamp::zero(), true)
                .unwrap();
            self.write(txn.into_modifies());
        }

        fn gc(&mut self, pk: &[u8], safe_point: impl Into<TimeStamp> + Copy) {
            let cm = ConcurrencyManager::new(safe_point.into());
            loop {
                let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(
                    self.db.c().clone(),
                    self.brane.clone(),
                );
                let mut txn = MvccTxn::new(snap, safe_point.into(), true, cm.clone());
                txn.gc(Key::from_raw(pk), safe_point.into()).unwrap();
                let modifies = txn.into_modifies();
                if modifies.is_empty() {
                    return;
                }
                self.write(modifies);
            }
        }

        fn write(&mut self, modifies: Vec<Modify>) {
            let db = &self.db;
            let mut wb = db.c().write_batch();
            for rev in modifies {
                match rev {
                    Modify::Put(causet, k, v) => {
                        let k = tuplespaceInstanton::data_key(k.as_encoded());
                        wb.put_causet(causet, &k, &v).unwrap();
                    }
                    Modify::Delete(causet, k) => {
                        let k = tuplespaceInstanton::data_key(k.as_encoded());
                        wb.delete_causet(causet, &k).unwrap();
                    }
                    Modify::DeleteCone(causet, k1, k2, notify_only) => {
                        if !notify_only {
                            let k1 = tuplespaceInstanton::data_key(k1.as_encoded());
                            let k2 = tuplespaceInstanton::data_key(k2.as_encoded());
                            wb.delete_cone_causet(causet, &k1, &k2).unwrap();
                        }
                    }
                }
            }
            db.c().write(&wb).unwrap();
        }

        fn flush(&mut self) {
            for causet in ALL_CAUSETS {
                let causet = engine_lmdb::util::get_causet_handle(&self.db, causet).unwrap();
                self.db.flush_causet(causet, true).unwrap();
            }
        }

        fn compact(&mut self) {
            for causet in ALL_CAUSETS {
                let causet = engine_lmdb::util::get_causet_handle(&self.db, causet).unwrap();
                self.db.compact_cone_causet(causet, None, None);
            }
        }
    }

    fn open_db(path: &str, with_properties: bool) -> Arc<DB> {
        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_write_buffer_size(32 * 1024 * 1024);
        if with_properties {
            let f = Box::new(MvccPropertiesCollectorFactory::default());
            causet_opts.add_Block_properties_collector_factory("einsteindb.test-collector", f);
        }
        let causets_opts = vec![
            CAUSETOptions::new(CAUSET_DEFAULT, PrimaryCausetNetworkOptions::new()),
            CAUSETOptions::new(CAUSET_VIOLETABFT, PrimaryCausetNetworkOptions::new()),
            CAUSETOptions::new(CAUSET_DAGGER, PrimaryCausetNetworkOptions::new()),
            CAUSETOptions::new(CAUSET_WRITE, causet_opts),
        ];
        Arc::new(engine_lmdb::raw_util::new_engine_opt(path, db_opts, causets_opts).unwrap())
    }

    fn make_brane(id: u64, spacelike_key: Vec<u8>, lightlike_key: Vec<u8>) -> Brane {
        let mut peer = Peer::default();
        peer.set_id(id);
        peer.set_store_id(id);
        let mut brane = Brane::default();
        brane.set_id(id);
        brane.set_spacelike_key(spacelike_key);
        brane.set_lightlike_key(lightlike_key);
        brane.mut_peers().push(peer);
        brane
    }

    fn get_tail_pointer_properties_and_check_gc(
        db: Arc<DB>,
        brane: Brane,
        safe_point: impl Into<TimeStamp>,
        need_gc: bool,
    ) -> Option<MvccProperties> {
        let safe_point = safe_point.into();

        let spacelike = tuplespaceInstanton::data_key(brane.get_spacelike_key());
        let lightlike = tuplespaceInstanton::data_lightlike_key(brane.get_lightlike_key());
        let collection = db
            .c()
            .get_cone_properties_causet(CAUSET_WRITE, &spacelike, &lightlike)
            .unwrap();
        assert_eq!(check_need_gc(safe_point, 1.0, &collection), need_gc);

        get_tail_pointer_properties(safe_point, &collection)
    }

    #[test]
    fn test_need_gc() {
        let path = tempfile::Builder::new()
            .prefix("test_causetStorage_tail_pointer_reader")
            .temfidelir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let brane = make_brane(1, vec![0], vec![10]);
        test_without_properties(path, &brane);
        test_with_properties(path, &brane);
    }

    fn test_without_properties(path: &str, brane: &Brane) {
        let db = open_db(path, false);
        let mut engine = BraneEngine::new(&db, &brane);

        // Put 2 tuplespaceInstanton.
        engine.put(&[1], 1, 1);
        engine.put(&[4], 2, 2);
        assert!(
            get_tail_pointer_properties_and_check_gc(Arc::clone(&db), brane.clone(), 10, true).is_none()
        );
        engine.flush();
        // After this flush, we have a SST file without properties.
        // Without properties, we always need GC.
        assert!(
            get_tail_pointer_properties_and_check_gc(Arc::clone(&db), brane.clone(), 10, true).is_none()
        );
    }

    #[test]
    fn test_ts_filter() {
        let path = tempfile::Builder::new()
            .prefix("test_ts_filter")
            .temfidelir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let brane = make_brane(1, vec![0], vec![13]);

        let db = open_db(path, true);
        let mut engine = BraneEngine::new(&db, &brane);

        engine.put(&[2], 1, 2);
        engine.put(&[4], 3, 4);
        engine.flush();
        engine.put(&[6], 5, 6);
        engine.put(&[8], 7, 8);
        engine.flush();
        engine.put(&[10], 9, 10);
        engine.put(&[12], 11, 12);
        engine.flush();

        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane);

        let tests = vec![
            // set nothing.
            (
                Bound::Unbounded,
                Bound::Unbounded,
                vec![2u64, 4, 6, 8, 10, 12],
            ),
            // test set both hint_min_ts and hint_max_ts.
            (Bound::Included(6), Bound::Included(8), vec![6u64, 8]),
            (Bound::Excluded(5), Bound::Included(8), vec![6u64, 8]),
            (Bound::Included(6), Bound::Excluded(9), vec![6u64, 8]),
            (Bound::Excluded(5), Bound::Excluded(9), vec![6u64, 8]),
            // test set only hint_min_ts.
            (Bound::Included(10), Bound::Unbounded, vec![10u64, 12]),
            (Bound::Excluded(9), Bound::Unbounded, vec![10u64, 12]),
            // test set only hint_max_ts.
            (Bound::Unbounded, Bound::Included(7), vec![2u64, 4, 6, 8]),
            (Bound::Unbounded, Bound::Excluded(8), vec![2u64, 4, 6, 8]),
        ];

        for (_, &(min, max, ref res)) in tests.iter().enumerate() {
            let mut iopt = IterOptions::default();
            iopt.set_hint_min_ts(min);
            iopt.set_hint_max_ts(max);

            let mut iter = snap.iter_causet(CAUSET_WRITE, iopt).unwrap();

            for (i, expect_ts) in res.iter().enumerate() {
                if i == 0 {
                    assert_eq!(iter.seek_to_first().unwrap(), true);
                } else {
                    assert_eq!(iter.next().unwrap(), true);
                }

                let ts = Key::decode_ts_from(iter.key()).unwrap();
                assert_eq!(ts.into_inner(), *expect_ts);
            }

            assert_eq!(iter.next().unwrap(), false);
        }
    }

    #[test]
    fn test_ts_filter_lost_delete() {
        let dir = tempfile::Builder::new()
            .prefix("test_ts_filter_lost_deletion")
            .temfidelir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let brane = make_brane(1, vec![0], vec![]);

        let db = open_db(&path, true);
        let mut engine = BraneEngine::new(&db, &brane);

        let key1 = &[1];
        engine.put(key1, 2, 3);
        engine.flush();
        engine.compact();

        // Delete key 1 commit ts@5 and GC@6
        // Put key 2 commit ts@7
        let key2 = &[2];
        engine.put(key2, 6, 7);
        engine.delete(key1, 4, 5);
        engine.gc(key1, 6);
        engine.flush();

        // Scan kv with ts filter [1, 6].
        let mut iopt = IterOptions::default();
        iopt.set_hint_min_ts(Bound::Included(1));
        iopt.set_hint_max_ts(Bound::Included(6));

        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane);
        let mut iter = snap.iter_causet(CAUSET_WRITE, iopt).unwrap();

        // Must not omit the latest deletion of key1 to prevent seeing outdated record.
        assert_eq!(iter.seek_to_first().unwrap(), true);
        assert_eq!(
            Key::from_encoded_slice(iter.key())
                .to_raw()
                .unwrap()
                .as_slice(),
            key2
        );
        assert_eq!(iter.next().unwrap(), false);
    }

    fn test_with_properties(path: &str, brane: &Brane) {
        let db = open_db(path, true);
        let mut engine = BraneEngine::new(&db, &brane);

        // Put 2 tuplespaceInstanton.
        engine.put(&[2], 3, 3);
        engine.put(&[3], 4, 4);
        engine.flush();
        // After this flush, we have a SST file w/ properties, plus the SST
        // file w/o properties from previous flush. We always need GC as
        // long as we can't get properties from any SST files.
        assert!(
            get_tail_pointer_properties_and_check_gc(Arc::clone(&db), brane.clone(), 10, true).is_none()
        );
        engine.compact();
        // After this compact, the two SST files are compacted into a new
        // SST file with properties. Now all SST files have properties and
        // all tuplespaceInstanton have only one version, so we don't need gc.
        let props =
            get_tail_pointer_properties_and_check_gc(Arc::clone(&db), brane.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 4.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // Put 2 more tuplespaceInstanton and delete them.
        engine.put(&[5], 5, 5);
        engine.put(&[6], 6, 6);
        engine.delete(&[5], 7, 7);
        engine.delete(&[6], 8, 8);
        engine.flush();
        // After this flush, tuplespaceInstanton 5,6 in the new SST file have more than one
        // versions, so we need gc.
        let props =
            get_tail_pointer_properties_and_check_gc(Arc::clone(&db), brane.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 8.into());
        assert_eq!(props.num_rows, 6);
        assert_eq!(props.num_puts, 6);
        assert_eq!(props.num_versions, 8);
        assert_eq!(props.max_row_versions, 2);
        // But if the `safe_point` is older than all versions, we don't need gc too.
        let props =
            get_tail_pointer_properties_and_check_gc(Arc::clone(&db), brane.clone(), 0, false).unwrap();
        assert_eq!(props.min_ts, TimeStamp::max());
        assert_eq!(props.max_ts, TimeStamp::zero());
        assert_eq!(props.num_rows, 0);
        assert_eq!(props.num_puts, 0);
        assert_eq!(props.num_versions, 0);
        assert_eq!(props.max_row_versions, 0);

        // We gc the two deleted tuplespaceInstanton manually.
        engine.gc(&[5], 10);
        engine.gc(&[6], 10);
        engine.compact();
        // After this compact, all versions of tuplespaceInstanton 5,6 are deleted,
        // no tuplespaceInstanton have more than one versions, so we don't need gc.
        let props =
            get_tail_pointer_properties_and_check_gc(Arc::clone(&db), brane.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 4.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // A single dagger version need gc.
        engine.dagger(&[7], 9, 9);
        engine.flush();
        let props =
            get_tail_pointer_properties_and_check_gc(Arc::clone(&db), brane.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 9.into());
        assert_eq!(props.num_rows, 5);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 5);
        assert_eq!(props.max_row_versions, 1);
    }

    #[test]
    fn test_get_txn_commit_record() {
        let path = tempfile::Builder::new()
            .prefix("_test_causetStorage_tail_pointer_reader_get_txn_commit_record")
            .temfidelir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let brane = make_brane(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = BraneEngine::new(&db, &brane);

        let (k, v) = (b"k", b"v");
        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 1);
        engine.commit(k, 1, 10);

        engine.rollback(k, 5);
        engine.rollback(k, 20);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 25);
        engine.commit(k, 25, 30);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 35);
        engine.commit(k, 35, 40);

        // Overlapped rollback on the commit record at 40.
        engine.rollback_protected(k, 40);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 45, 45);
        engine.prewrite_pessimistic_lock(m, k, 45);
        engine.commit(k, 45, 50);

        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        // Let's assume `50_45 PUT` means a commit version with spacelike ts is 45 and commit ts
        // is 50.
        // Commit versions: [50_45 PUT, 45_40 PUT, 40_35 PUT, 30_25 PUT, 20_20 Rollback, 10_1 PUT, 5_5 Rollback].
        let key = Key::from_raw(k);
        let overlapped_write = reader
            .get_txn_commit_record(&key, 55.into())
            .unwrap()
            .unwrap_none();
        assert!(overlapped_write.is_none());

        // When no such record is found but a record of another txn has a write record with
        // its commit_ts equals to current spacelike_ts, it
        let overlapped_write = reader
            .get_txn_commit_record(&key, 50.into())
            .unwrap()
            .unwrap_none()
            .unwrap();
        assert_eq!(overlapped_write.spacelike_ts, 45.into());
        assert_eq!(overlapped_write.write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 45.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 50.into());
        assert_eq!(write_type, WriteType::Put);

        let commit_ts = reader
            .get_txn_commit_record(&key, 40.into())
            .unwrap()
            .unwrap_overlapped_rollback();
        assert_eq!(commit_ts, 40.into());

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 35.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 40.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 25.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 30.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 20.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 20.into());
        assert_eq!(write_type, WriteType::Rollback);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 1.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 10.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 5.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 5.into());
        assert_eq!(write_type, WriteType::Rollback);

        let seek_old = reader.get_statistics().write.seek;
        assert!(!reader
            .get_txn_commit_record(&key, 30.into())
            .unwrap()
            .exist());
        let seek_new = reader.get_statistics().write.seek;

        // `get_txn_commit_record(&key, 30)` stopped at `30_25 PUT`.
        assert_eq!(seek_new - seek_old, 3);
    }

    #[test]
    fn test_get_txn_commit_record_of_pessimistic_txn() {
        let path = tempfile::Builder::new()
            .prefix("_test_causetStorage_tail_pointer_reader_get_txn_commit_record_of_pessimistic_txn")
            .temfidelir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let brane = make_brane(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = BraneEngine::new(&db, &brane);

        let (k, v) = (b"k", b"v");
        let key = Key::from_raw(k);
        let m = Mutation::Put((key.clone(), v.to_vec()));

        // txn: spacelike_ts = 2, commit_ts = 3
        engine.acquire_pessimistic_lock(key.clone(), k, 2, 2);
        engine.prewrite_pessimistic_lock(m.clone(), k, 2);
        engine.commit(k, 2, 3);
        // txn: spacelike_ts = 1, commit_ts = 4
        engine.acquire_pessimistic_lock(key.clone(), k, 1, 3);
        engine.prewrite_pessimistic_lock(m, k, 1);
        engine.commit(k, 1, 4);

        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 2.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 3.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 1.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 4.into());
        assert_eq!(write_type, WriteType::Put);
    }

    #[test]
    fn test_seek_write() {
        let path = tempfile::Builder::new()
            .prefix("_test_causetStorage_tail_pointer_reader_seek_write")
            .temfidelir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let brane = make_brane(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = BraneEngine::new(&db, &brane);

        let (k, v) = (b"k", b"v");
        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m.clone(), k, 1);
        engine.commit(k, 1, 5);

        engine.rollback(k, 3);
        engine.rollback(k, 7);

        engine.prewrite(m.clone(), k, 15);
        engine.commit(k, 15, 17);

        // Timestamp overlap with the previous transaction.
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 10, 18);
        engine.prewrite_pessimistic_lock(Mutation::Dagger(Key::from_raw(k)), k, 10);
        engine.commit(k, 10, 20);

        engine.prewrite(m, k, 23);
        engine.commit(k, 23, 25);

        // Let's assume `2_1 PUT` means a commit version with spacelike ts is 1 and commit ts
        // is 2.
        // Commit versions: [25_23 PUT, 20_10 PUT, 17_15 PUT, 7_7 Rollback, 5_1 PUT, 3_3 Rollback].
        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        let k = Key::from_raw(k);
        let (commit_ts, write) = reader.seek_write(&k, 30.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 25.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 23.into(), Some(v.to_vec()))
        );

        let (commit_ts, write) = reader.seek_write(&k, 25.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 25.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 23.into(), Some(v.to_vec()))
        );

        let (commit_ts, write) = reader.seek_write(&k, 20.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 20.into());
        assert_eq!(write, Write::new(WriteType::Dagger, 10.into(), None));

        let (commit_ts, write) = reader.seek_write(&k, 19.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 17.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 15.into(), Some(v.to_vec()))
        );

        let (commit_ts, write) = reader.seek_write(&k, 3.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 3.into());
        assert_eq!(write, Write::new_rollback(3.into(), false));

        let (commit_ts, write) = reader.seek_write(&k, 16.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 7.into());
        assert_eq!(write, Write::new_rollback(7.into(), false));

        let (commit_ts, write) = reader.seek_write(&k, 6.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 5.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 1.into(), Some(v.to_vec()))
        );

        assert!(reader.seek_write(&k, 2.into()).unwrap().is_none());

        // Test seek_write should not see the next key.
        let (k2, v2) = (b"k2", b"v2");
        let m2 = Mutation::Put((Key::from_raw(k2), v2.to_vec()));
        engine.prewrite(m2, k2, 1);
        engine.commit(k2, 1, 2);

        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        let (commit_ts, write) = reader
            .seek_write(&Key::from_raw(k2), 3.into())
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 2.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 1.into(), Some(v2.to_vec()))
        );

        assert!(reader.seek_write(&k, 2.into()).unwrap().is_none());

        // Test seek_write touches brane's lightlike.
        let brane1 = make_brane(1, vec![], Key::from_raw(b"k1").into_encoded());
        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane1);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        assert!(reader.seek_write(&k, 2.into()).unwrap().is_none());
    }

    #[test]
    fn test_get_write() {
        let path = tempfile::Builder::new()
            .prefix("_test_causetStorage_tail_pointer_reader_get_write")
            .temfidelir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let brane = make_brane(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = BraneEngine::new(&db, &brane);

        let (k, v) = (b"k", b"v");
        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 1);
        engine.commit(k, 1, 2);

        engine.rollback(k, 5);

        engine.dagger(k, 6, 7);

        engine.delete(k, 8, 9);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 12);
        engine.commit(k, 12, 14);

        let m = Mutation::Dagger(Key::from_raw(k));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 13, 15);
        engine.prewrite_pessimistic_lock(m, k, 13);
        engine.commit(k, 13, 15);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 18, 18);
        engine.prewrite_pessimistic_lock(m, k, 18);
        engine.commit(k, 18, 20);

        let m = Mutation::Dagger(Key::from_raw(k));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 17, 21);
        engine.prewrite_pessimistic_lock(m, k, 17);
        engine.commit(k, 17, 21);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 24);

        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        // Let's assume `2_1 PUT` means a commit version with spacelike ts is 1 and commit ts
        // is 2.
        // Commit versions: [21_17 LOCK, 20_18 PUT, 15_13 LOCK, 14_12 PUT, 9_8 DELETE, 7_6 LOCK,
        //                   5_5 Rollback, 2_1 PUT].
        let key = Key::from_raw(k);

        assert!(reader.get_write(&key, 1.into()).unwrap().is_none());

        let write = reader.get_write(&key, 2.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.spacelike_ts, 1.into());

        let write = reader.get_write(&key, 5.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.spacelike_ts, 1.into());

        let write = reader.get_write(&key, 7.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.spacelike_ts, 1.into());

        assert!(reader.get_write(&key, 9.into()).unwrap().is_none());

        let write = reader.get_write(&key, 14.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.spacelike_ts, 12.into());

        let write = reader.get_write(&key, 16.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.spacelike_ts, 12.into());

        let write = reader.get_write(&key, 20.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.spacelike_ts, 18.into());

        let write = reader.get_write(&key, 24.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.spacelike_ts, 18.into());

        assert!(reader
            .get_write(&Key::from_raw(b"j"), 100.into())
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_check_lock() {
        let path = tempfile::Builder::new()
            .prefix("_test_causetStorage_tail_pointer_reader_check_lock")
            .temfidelir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let brane = make_brane(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = BraneEngine::new(&db, &brane);

        let (k1, k2, k3, k4, v) = (b"k1", b"k2", b"k3", b"k4", b"v");
        engine.prewrite(Mutation::Put((Key::from_raw(k1), v.to_vec())), k1, 5);
        engine.prewrite(Mutation::Put((Key::from_raw(k2), v.to_vec())), k1, 5);
        engine.prewrite(Mutation::Dagger(Key::from_raw(k3)), k1, 5);

        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        // Ignore the dagger if read ts is less than the dagger version
        assert!(reader.check_lock(&Key::from_raw(k1), 4.into()).is_ok());
        assert!(reader.check_lock(&Key::from_raw(k2), 4.into()).is_ok());
        // Returns the dagger if read ts >= dagger version
        assert!(reader.check_lock(&Key::from_raw(k1), 6.into()).is_err());
        assert!(reader.check_lock(&Key::from_raw(k2), 6.into()).is_err());
        // Read locks don't block any read operation
        assert!(reader.check_lock(&Key::from_raw(k3), 6.into()).is_ok());
        // Ignore the primary dagger when reading the latest committed version by setting TimeStamp::max() as ts
        assert!(reader
            .check_lock(&Key::from_raw(k1), TimeStamp::max())
            .is_ok());
        // Should not ignore the secondary dagger even though reading the latest version
        assert!(reader
            .check_lock(&Key::from_raw(k2), TimeStamp::max())
            .is_err());

        // Commit the primary dagger only
        engine.commit(k1, 5, 7);
        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        // Then reading the primary key should succeed
        assert!(reader.check_lock(&Key::from_raw(k1), 6.into()).is_ok());
        // Reading secondary tuplespaceInstanton should still fail
        assert!(reader.check_lock(&Key::from_raw(k2), 6.into()).is_err());
        assert!(reader
            .check_lock(&Key::from_raw(k2), TimeStamp::max())
            .is_err());

        // Pessimistic locks
        engine.acquire_pessimistic_lock(Key::from_raw(k4), k4, 9, 9);
        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        // Pessimistic locks don't block any read operation
        assert!(reader.check_lock(&Key::from_raw(k4), 10.into()).is_ok());
    }

    #[test]
    fn test_scan_locks() {
        let path = tempfile::Builder::new()
            .prefix("_test_causetStorage_tail_pointer_reader_scan_locks")
            .temfidelir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let brane = make_brane(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = BraneEngine::new(&db, &brane);

        // Put some locks to the db.
        engine.prewrite(
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            b"k1",
            5,
        );
        engine.prewrite(
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            b"k1",
            10,
        );
        engine.prewrite(Mutation::Delete(Key::from_raw(b"k3")), b"k1", 10);
        engine.prewrite(Mutation::Dagger(Key::from_raw(b"k3\x00")), b"k1", 10);
        engine.prewrite(Mutation::Delete(Key::from_raw(b"k4")), b"k1", 12);
        engine.acquire_pessimistic_lock(Key::from_raw(b"k5"), b"k1", 10, 12);
        engine.acquire_pessimistic_lock(Key::from_raw(b"k6"), b"k1", 12, 12);

        // All locks whose ts <= 10.
        let visible_locks: Vec<_> = vec![
            // key, lock_type, short_value, ts, for_ufidelate_ts
            (
                b"k1".to_vec(),
                LockType::Put,
                Some(b"v1".to_vec()),
                5.into(),
                TimeStamp::zero(),
            ),
            (
                b"k2".to_vec(),
                LockType::Put,
                Some(b"v2".to_vec()),
                10.into(),
                TimeStamp::zero(),
            ),
            (
                b"k3".to_vec(),
                LockType::Delete,
                None,
                10.into(),
                TimeStamp::zero(),
            ),
            (
                b"k3\x00".to_vec(),
                LockType::Dagger,
                None,
                10.into(),
                TimeStamp::zero(),
            ),
            (
                b"k5".to_vec(),
                LockType::Pessimistic,
                None,
                10.into(),
                12.into(),
            ),
        ]
        .into_iter()
        .map(|(k, lock_type, short_value, ts, for_ufidelate_ts)| {
            (
                Key::from_raw(&k),
                Dagger::new(
                    lock_type,
                    b"k1".to_vec(),
                    ts,
                    0,
                    short_value,
                    for_ufidelate_ts,
                    0,
                    TimeStamp::zero(),
                ),
            )
        })
        .collect();

        // Creates a reader and scan locks,
        let check_scan_lock =
            |spacelike_key: Option<Key>, limit, expect_res: &[_], expect_is_remain| {
                let snap =
                    BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane.clone());
                let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
                let res = reader
                    .scan_locks(spacelike_key.as_ref(), |l| l.ts <= 10.into(), limit)
                    .unwrap();
                assert_eq!(res.0, expect_res);
                assert_eq!(res.1, expect_is_remain);
            };

        check_scan_lock(None, 6, &visible_locks, false);
        check_scan_lock(None, 5, &visible_locks, true);
        check_scan_lock(None, 4, &visible_locks[0..4], true);
        check_scan_lock(Some(Key::from_raw(b"k2")), 3, &visible_locks[1..4], true);
        check_scan_lock(
            Some(Key::from_raw(b"k3\x00")),
            1,
            &visible_locks[3..4],
            true,
        );
        check_scan_lock(
            Some(Key::from_raw(b"k3\x00")),
            10,
            &visible_locks[3..],
            false,
        );
        // limit = 0 means unlimited.
        check_scan_lock(None, 0, &visible_locks, false);
    }

    #[test]
    fn test_get() {
        let path = tempfile::Builder::new()
            .prefix("_test_causetStorage_tail_pointer_reader_get_write")
            .temfidelir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let brane = make_brane(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = BraneEngine::new(&db, &brane);

        let (k, short_value, long_value) = (
            b"k",
            b"v",
            "v".repeat(txn_types::SHORT_VALUE_MAX_LEN + 1).into_bytes(),
        );
        let m = Mutation::Put((Key::from_raw(k), short_value.to_vec()));
        engine.prewrite(m, k, 1);
        engine.commit(k, 1, 2);

        engine.rollback(k, 5);

        engine.dagger(k, 6, 7);

        engine.delete(k, 8, 9);

        let m = Mutation::Put((Key::from_raw(k), long_value.to_vec()));
        engine.prewrite(m, k, 10);

        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        let key = Key::from_raw(k);

        for &skip_lock_check in &[false, true] {
            assert_eq!(
                reader.get(&key, 2.into(), skip_lock_check).unwrap(),
                Some(short_value.to_vec())
            );
            assert_eq!(
                reader.get(&key, 5.into(), skip_lock_check).unwrap(),
                Some(short_value.to_vec())
            );
            assert_eq!(
                reader.get(&key, 7.into(), skip_lock_check).unwrap(),
                Some(short_value.to_vec())
            );
            assert_eq!(reader.get(&key, 9.into(), skip_lock_check).unwrap(), None);
        }
        assert!(reader.get(&key, 11.into(), false).is_err());
        assert_eq!(reader.get(&key, 9.into(), true).unwrap(), None);

        // Commit the long value
        engine.commit(k, 10, 11);
        let snap = BraneSnapshot::<LmdbSnapshot>::from_raw(db.c().clone(), brane);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        for &skip_lock_check in &[false, true] {
            assert_eq!(
                reader.get(&key, 11.into(), skip_lock_check).unwrap(),
                Some(long_value.to_vec())
            );
        }
    }
}
