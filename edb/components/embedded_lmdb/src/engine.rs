// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use edb::{
    Error, IterOptions, Iterable, CausetEngine, Peekable, ReadOptions, Result, SyncMuBlock,
};
use lmdb::{DBIterator, WriBlock, DB};

use crate::db_vector::LmdbDBVector;
use crate::options::LmdbReadOptions;
use crate::rocks_metrics::{
    flush_engine_histogram_metrics, flush_engine_iostall_properties, flush_engine_properties,
    flush_engine_ticker_metrics,
};
use crate::rocks_metrics_defs::{
    ENGINE_HIST_TYPES, ENGINE_TICKER_TYPES, TITAN_ENGINE_HIST_TYPES, TITAN_ENGINE_TICKER_TYPES,
};
use crate::util::get_causet_handle;
use crate::{LmdbEngineIterator, LmdbSnapshot};

#[derive(Clone, Debug)]
pub struct LmdbEngine {
    db: Arc<DB>,
    shared_block_cache: bool,
}

impl LmdbEngine {
    pub fn from_db(db: Arc<DB>) -> Self {
        LmdbEngine {
            db,
            shared_block_cache: false,
        }
    }

    pub fn from_ref(db: &Arc<DB>) -> &Self {
        unsafe { &*(db as *const Arc<DB> as *const LmdbEngine) }
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        &self.db
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.db.clone()
    }

    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }

        // If path is not an empty directory, we say db exists. If path is not an empty directory
        // but db has not been created, `DB::list_PrimaryCauset_families` fails and we can clean up
        // the directory by this indication.
        fs::read_dir(&path).unwrap().next().is_some()
    }

    pub fn set_shared_block_cache(&mut self, enable: bool) {
        self.shared_block_cache = enable;
    }
}

impl CausetEngine for LmdbEngine {
    type Snapshot = LmdbSnapshot;

    fn snapshot(&self) -> LmdbSnapshot {
        LmdbSnapshot::new(self.db.clone())
    }

    fn sync(&self) -> Result<()> {
        self.db.sync_wal().map_err(Error::Engine)
    }

    fn flush_metrics(&self, instance: &str) {
        for t in ENGINE_TICKER_TYPES {
            let v = self.db.get_and_reset_statistics_ticker_count(*t);
            flush_engine_ticker_metrics(*t, v, instance);
        }
        for t in ENGINE_HIST_TYPES {
            if let Some(v) = self.db.get_statistics_histogram(*t) {
                flush_engine_histogram_metrics(*t, v, instance);
            }
        }
        if self.db.is_titan() {
            for t in TITAN_ENGINE_TICKER_TYPES {
                let v = self.db.get_and_reset_statistics_ticker_count(*t);
                flush_engine_ticker_metrics(*t, v, instance);
            }
            for t in TITAN_ENGINE_HIST_TYPES {
                if let Some(v) = self.db.get_statistics_histogram(*t) {
                    flush_engine_histogram_metrics(*t, v, instance);
                }
            }
        }
        flush_engine_properties(&self.db, instance, self.shared_block_cache);
        flush_engine_iostall_properties(&self.db, instance);
    }

    fn reset_statistics(&self) {
        self.db.reset_statistics();
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        let e: &dyn Any = &self.db;
        e.downcast_ref().expect("bad engine downcast")
    }
}

impl Iterable for LmdbEngine {
    type Iteron = LmdbEngineIterator;

    fn Iteron_opt(&self, opts: IterOptions) -> Result<Self::Iteron> {
        let opt: LmdbReadOptions = opts.into();
        Ok(LmdbEngineIterator::from_raw(DBIterator::new(
            self.db.clone(),
            opt.into_raw(),
        )))
    }

    fn Iteron_causet_opt(&self, causet: &str, opts: IterOptions) -> Result<Self::Iteron> {
        let handle = get_causet_handle(&self.db, causet)?;
        let opt: LmdbReadOptions = opts.into();
        Ok(LmdbEngineIterator::from_raw(DBIterator::new_causet(
            self.db.clone(),
            handle,
            opt.into_raw(),
        )))
    }
}

impl Peekable for LmdbEngine {
    type DBVector = LmdbDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<LmdbDBVector>> {
        let opt: LmdbReadOptions = opts.into();
        let v = self.db.get_opt(key, &opt.into_raw())?;
        Ok(v.map(LmdbDBVector::from_raw))
    }

    fn get_value_causet_opt(
        &self,
        opts: &ReadOptions,
        causet: &str,
        key: &[u8],
    ) -> Result<Option<LmdbDBVector>> {
        let opt: LmdbReadOptions = opts.into();
        let handle = get_causet_handle(&self.db, causet)?;
        let v = self.db.get_causet_opt(handle, key, &opt.into_raw())?;
        Ok(v.map(LmdbDBVector::from_raw))
    }
}

impl SyncMuBlock for LmdbEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value).map_err(Error::Engine)
    }

    fn put_causet(&self, causet: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_causet_handle(&self.db, causet)?;
        self.db.put_causet(handle, key, value).map_err(Error::Engine)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key).map_err(Error::Engine)
    }

    fn delete_causet(&self, causet: &str, key: &[u8]) -> Result<()> {
        let handle = get_causet_handle(&self.db, causet)?;
        self.db.delete_causet(handle, key).map_err(Error::Engine)
    }

    fn delete_cone_causet(&self, causet: &str, begin_key: &[u8], lightlike_key: &[u8]) -> Result<()> {
        let handle = get_causet_handle(&self.db, causet)?;
        self.db
            .delete_cone_causet(handle, begin_key, lightlike_key)
            .map_err(Error::Engine)
    }
}

#[causet(test)]
mod tests {
    use crate::raw_util;
    use edb::{Iterable, CausetEngine, Peekable, SyncMuBlock};
    use ekvproto::meta_timeshare::Brane;
    use std::sync::Arc;
    use tempfile::Builder;

    use crate::{LmdbEngine, LmdbSnapshot};

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").temfidelir().unwrap();
        let causet = "causet";
        let engine = LmdbEngine::from_db(Arc::new(
            raw_util::new_engine(path.path().to_str().unwrap(), None, &[causet], None).unwrap(),
        ));

        let mut r = Brane::default();
        r.set_id(10);

        let key = b"key";
        engine.put_msg(key, &r).unwrap();
        engine.put_msg_causet(causet, key, &r).unwrap();

        let snap = engine.snapshot();

        let mut r1: Brane = engine.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_causet: Brane = engine.get_msg_causet(causet, key).unwrap().unwrap();
        assert_eq!(r, r1_causet);

        let mut r2: Brane = snap.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r2);
        let r2_causet: Brane = snap.get_msg_causet(causet, key).unwrap().unwrap();
        assert_eq!(r, r2_causet);

        r.set_id(11);
        engine.put_msg(key, &r).unwrap();
        r1 = engine.get_msg(key).unwrap().unwrap();
        r2 = snap.get_msg(key).unwrap().unwrap();
        assert_ne!(r1, r2);

        let b: Option<Brane> = engine.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("var").temfidelir().unwrap();
        let causet = "causet";
        let engine = LmdbEngine::from_db(Arc::new(
            raw_util::new_engine(path.path().to_str().unwrap(), None, &[causet], None).unwrap(),
        ));

        engine.put(b"k1", b"v1").unwrap();
        engine.put_causet(causet, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get_value(b"k1").unwrap().unwrap(), b"v1");
        assert!(engine.get_value_causet("foo", b"k1").is_err());
        assert_eq!(&*engine.get_value_causet(causet, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = Builder::new().prefix("var").temfidelir().unwrap();
        let causet = "causet";
        let engine = LmdbEngine::from_db(Arc::new(
            raw_util::new_engine(path.path().to_str().unwrap(), None, &[causet], None).unwrap(),
        ));

        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();
        engine.put_causet(causet, b"a1", b"v1").unwrap();
        engine.put_causet(causet, b"a2", b"v22").unwrap();

        let mut data = vec![];
        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v2".to_vec()),
            ]
        );
        data.clear();

        engine
            .scan_causet(causet, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v22".to_vec()),
            ]
        );
        data.clear();

        let pair = engine.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek(b"a3").unwrap().is_none());
        let pair_causet = engine.seek_causet(causet, b"a1").unwrap().unwrap();
        assert_eq!(pair_causet, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek_causet(causet, b"a3").unwrap().is_none());

        let mut index = 0;
        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                index += 1;
                Ok(index != 1)
            })
            .unwrap();

        assert_eq!(data.len(), 1);

        let snap = LmdbSnapshot::new(engine.get_sync_db());

        engine.put(b"a3", b"v3").unwrap();
        assert!(engine.seek(b"a3").unwrap().is_some());

        let pair = snap.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(snap.seek(b"a3").unwrap().is_none());

        data.clear();

        snap.scan(b"", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 2);
    }
}
