// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;
use std::fmt::{self, Debug, Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engine_lmdb::raw::{PrimaryCausetNetworkOptions, DBIterator, SeekKey as DBSeekKey, DB};
use engine_lmdb::raw_util::CAUSETOptions;
use engine_lmdb::{LmdbEngine as BaseLmdbEngine, LmdbEngineIterator};
use engine_promises::{CfName, CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_RAFT, CAUSET_WRITE};
use engine_promises::{
    Engines, IterOptions, Iterable, Iteron, KvEngine, Mutable, Peekable, ReadOptions, SeekKey,
    WriteBatchExt,
};
use ekvproto::kvrpcpb::Context;
use tempfile::{Builder, TempDir};
use txn_types::{Key, Value};

use crate::persistence::config::BlockCacheConfig;
use einsteindb_util::escape;
use einsteindb_util::time::ThreadReadId;
use einsteindb_util::worker::{Runnable, Scheduler, Worker};

use super::{
    Callback, CbContext, Cursor, Engine, Error, ErrorInner, Iteron as EngineIterator, Modify,
    Result, ScanMode, Snapshot, WriteData,
};

pub use engine_lmdb::LmdbSnapshot;

const TEMP_DIR: &str = "";

enum Task {
    Write(Vec<Modify>, Callback<()>),
    Snapshot(Callback<Arc<LmdbSnapshot>>),
    Pause(Duration),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Write(..) => write!(f, "write task"),
            Task::Snapshot(_) => write!(f, "snapshot task"),
            Task::Pause(_) => write!(f, "pause"),
        }
    }
}

struct Runner(Engines<BaseLmdbEngine, BaseLmdbEngine>);

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, t: Task) {
        match t {
            Task::Write(modifies, cb) => {
                cb((CbContext::new(), write_modifies(&self.0.kv, modifies)))
            }
            Task::Snapshot(cb) => cb((CbContext::new(), Ok(Arc::new(self.0.kv.snapshot())))),
            Task::Pause(dur) => std::thread::sleep(dur),
        }
    }
}

struct LmdbEngineCore {
    // only use for memory mode
    temp_dir: Option<TempDir>,
    worker: Worker<Task>,
}

impl Drop for LmdbEngineCore {
    fn drop(&mut self) {
        if let Some(h) = self.worker.stop() {
            if let Err(e) = h.join() {
                safe_panic!("LmdbEngineCore engine thread panicked: {:?}", e);
            }
        }
    }
}

/// The LmdbEngine is based on `Lmdb`.
///
/// This is intlightlikeed for **testing use only**.
#[derive(Clone)]
pub struct LmdbEngine {
    core: Arc<Mutex<LmdbEngineCore>>,
    sched: Scheduler<Task>,
    engines: Engines<BaseLmdbEngine, BaseLmdbEngine>,
    not_leader: Arc<AtomicBool>,
}

impl LmdbEngine {
    pub fn new(
        path: &str,
        causets: &[CfName],
        causets_opts: Option<Vec<CAUSETOptions<'_>>>,
        shared_block_cache: bool,
    ) -> Result<LmdbEngine> {
        info!("LmdbEngine: creating for path"; "path" => path);
        let (path, temp_dir) = match path {
            TEMP_DIR => {
                let td = Builder::new().prefix("temp-lmdb").temfidelir().unwrap();
                (td.path().to_str().unwrap().to_owned(), Some(td))
            }
            _ => (path.to_owned(), None),
        };
        let mut worker = Worker::new("engine-lmdb");
        let db = Arc::new(engine_lmdb::raw_util::new_engine(
            &path, None, causets, causets_opts,
        )?);
        // It does not use the raft_engine, so it is ok to fill with the same
        // lmdb.
        let mut kv_engine = BaseLmdbEngine::from_db(db.clone());
        let mut raft_engine = BaseLmdbEngine::from_db(db);
        kv_engine.set_shared_block_cache(shared_block_cache);
        raft_engine.set_shared_block_cache(shared_block_cache);
        let engines = Engines::new(kv_engine, raft_engine);
        box_try!(worker.spacelike(Runner(engines.clone())));
        Ok(LmdbEngine {
            sched: worker.scheduler(),
            core: Arc::new(Mutex::new(LmdbEngineCore { temp_dir, worker })),
            not_leader: Arc::new(AtomicBool::new(false)),
            engines,
        })
    }

    pub fn trigger_not_leader(&self) {
        self.not_leader.store(true, Ordering::SeqCst);
    }

    pub fn pause(&self, dur: Duration) {
        self.sched.schedule(Task::Pause(dur)).unwrap();
    }

    pub fn engines(&self) -> Engines<BaseLmdbEngine, BaseLmdbEngine> {
        self.engines.clone()
    }

    pub fn get_lmdb(&self) -> BaseLmdbEngine {
        self.engines.kv.clone()
    }

    pub fn stop(&self) {
        let mut core = self.core.lock().unwrap();
        if let Some(h) = core.worker.stop() {
            h.join().unwrap();
        }
    }
}

impl Display for LmdbEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Lmdb")
    }
}

impl Debug for LmdbEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Lmdb [is_temp: {}]",
            self.core.lock().unwrap().temp_dir.is_some()
        )
    }
}

/// A builder to build a temporary `LmdbEngine`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestEngineBuilder {
    path: Option<PathBuf>,
    causets: Option<Vec<CfName>>,
}

impl TestEngineBuilder {
    pub fn new() -> Self {
        Self {
            path: None,
            causets: None,
        }
    }

    /// Customize the data directory of the temporary engine.
    ///
    /// By default, TEMP_DIR will be used.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Customize the CAUSETs that engine will have.
    ///
    /// By default, engine will have all CAUSETs.
    pub fn causets(mut self, causets: impl AsRef<[CfName]>) -> Self {
        self.causets = Some(causets.as_ref().to_vec());
        self
    }

    /// Build a `LmdbEngine`.
    pub fn build(self) -> Result<LmdbEngine> {
        let causetg_lmdb = crate::config::DbConfig::default();
        self.build_with_causetg(&causetg_lmdb)
    }

    pub fn build_with_causetg(self, causetg_lmdb: &crate::config::DbConfig) -> Result<LmdbEngine> {
        let path = match self.path {
            None => TEMP_DIR.to_owned(),
            Some(p) => p.to_str().unwrap().to_owned(),
        };
        let causets = self.causets.unwrap_or_else(|| crate::persistence::ALL_CAUSETS.to_vec());
        let cache = BlockCacheConfig::default().build_shared_cache();
        let causets_opts = causets
            .iter()
            .map(|causet| match *causet {
                CAUSET_DEFAULT => CAUSETOptions::new(CAUSET_DEFAULT, causetg_lmdb.defaultcauset.build_opt(&cache)),
                CAUSET_DAGGER => CAUSETOptions::new(CAUSET_DAGGER, causetg_lmdb.lockcauset.build_opt(&cache)),
                CAUSET_WRITE => CAUSETOptions::new(CAUSET_WRITE, causetg_lmdb.writecauset.build_opt(&cache)),
                CAUSET_RAFT => CAUSETOptions::new(CAUSET_RAFT, causetg_lmdb.raftcauset.build_opt(&cache)),
                _ => CAUSETOptions::new(*causet, PrimaryCausetNetworkOptions::new()),
            })
            .collect();
        LmdbEngine::new(&path, &causets, Some(causets_opts), cache.is_some())
    }
}

/// Write modifications into a `BaseLmdbEngine` instance.
pub fn write_modifies(kv_engine: &BaseLmdbEngine, modifies: Vec<Modify>) -> Result<()> {
    fail_point!("rockskv_write_modifies", |_| Err(box_err!("write failed")));

    let mut wb = kv_engine.write_batch();
    for rev in modifies {
        let res = match rev {
            Modify::Delete(causet, k) => {
                if causet == CAUSET_DEFAULT {
                    trace!("LmdbEngine: delete"; "key" => %k);
                    wb.delete(k.as_encoded())
                } else {
                    trace!("LmdbEngine: delete_causet"; "causet" => causet, "key" => %k);
                    wb.delete_causet(causet, k.as_encoded())
                }
            }
            Modify::Put(causet, k, v) => {
                if causet == CAUSET_DEFAULT {
                    trace!("LmdbEngine: put"; "key" => %k, "value" => escape(&v));
                    wb.put(k.as_encoded(), &v)
                } else {
                    trace!("LmdbEngine: put_causet"; "causet" => causet, "key" => %k, "value" => escape(&v));
                    wb.put_causet(causet, k.as_encoded(), &v)
                }
            }
            Modify::DeleteCone(causet, spacelike_key, lightlike_key, notify_only) => {
                trace!(
                    "LmdbEngine: delete_cone_causet";
                    "causet" => causet,
                    "spacelike_key" => %spacelike_key,
                    "lightlike_key" => %lightlike_key,
                    "notify_only" => notify_only,
                );
                if !notify_only {
                    wb.delete_cone_causet(causet, spacelike_key.as_encoded(), lightlike_key.as_encoded())
                } else {
                    Ok(())
                }
            }
        };
        // TODO: turn the error into an engine error.
        if let Err(msg) = res {
            return Err(box_err!("{}", msg));
        }
    }
    kv_engine.write(&wb)?;
    Ok(())
}

impl Engine for LmdbEngine {
    type Snap = Arc<LmdbSnapshot>;
    type Local = BaseLmdbEngine;

    fn kv_engine(&self) -> BaseLmdbEngine {
        self.engines.kv.clone()
    }

    fn snapshot_on_kv_engine(&self, _: &[u8], _: &[u8]) -> Result<Self::Snap> {
        self.snapshot(&Context::default())
    }

    fn modify_on_kv_engine(&self, modifies: Vec<Modify>) -> Result<()> {
        write_modifies(&self.engines.kv, modifies)
    }

    fn async_write(&self, _: &Context, batch: WriteData, cb: Callback<()>) -> Result<()> {
        fail_point!("rockskv_async_write", |_| Err(box_err!("write failed")));

        if batch.modifies.is_empty() {
            return Err(Error::from(ErrorInner::EmptyRequest));
        }
        box_try!(self.sched.schedule(Task::Write(batch.modifies, cb)));
        Ok(())
    }

    fn async_snapshot(
        &self,
        _: &Context,
        _: Option<ThreadReadId>,
        cb: Callback<Self::Snap>,
    ) -> Result<()> {
        fail_point!("rockskv_async_snapshot", |_| Err(box_err!(
            "snapshot failed"
        )));
        let not_leader = {
            let mut header = ekvproto::errorpb::Error::default();
            header.mut_not_leader().set_brane_id(100);
            header
        };
        fail_point!("rockskv_async_snapshot_not_leader", |_| {
            Err(Error::from(ErrorInner::Request(not_leader.clone())))
        });
        if self.not_leader.load(Ordering::SeqCst) {
            return Err(Error::from(ErrorInner::Request(not_leader)));
        }
        box_try!(self.sched.schedule(Task::Snapshot(cb)));
        Ok(())
    }
}

impl Snapshot for Arc<LmdbSnapshot> {
    type Iter = LmdbEngineIterator;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        trace!("LmdbSnapshot: get"; "key" => %key);
        let v = box_try!(self.get_value(key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_causet(&self, causet: CfName, key: &Key) -> Result<Option<Value>> {
        trace!("LmdbSnapshot: get_causet"; "causet" => causet, "key" => %key);
        let v = box_try!(self.get_value_causet(causet, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_causet_opt(&self, opts: ReadOptions, causet: CfName, key: &Key) -> Result<Option<Value>> {
        trace!("LmdbSnapshot: get_causet"; "causet" => causet, "key" => %key);
        let v = box_try!(self.get_value_causet_opt(&opts, causet, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter(&self, iter_opt: IterOptions, mode: ScanMode) -> Result<Cursor<Self::Iter>> {
        trace!("LmdbSnapshot: create Iteron");
        let iter = self.Iteron_opt(iter_opt)?;
        Ok(Cursor::new(iter, mode))
    }

    fn iter_causet(
        &self,
        causet: CfName,
        iter_opt: IterOptions,
        mode: ScanMode,
    ) -> Result<Cursor<Self::Iter>> {
        trace!("LmdbSnapshot: create causet Iteron");
        let iter = self.Iteron_causet_opt(causet, iter_opt)?;
        Ok(Cursor::new(iter, mode))
    }
}

impl EngineIterator for LmdbEngineIterator {
    fn next(&mut self) -> Result<bool> {
        Iteron::next(self).map_err(Error::from)
    }

    fn prev(&mut self) -> Result<bool> {
        Iteron::prev(self).map_err(Error::from)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        Iteron::seek(self, key.as_encoded().as_slice().into()).map_err(Error::from)
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        Iteron::seek_for_prev(self, key.as_encoded().as_slice().into()).map_err(Error::from)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        Iteron::seek(self, SeekKey::Start).map_err(Error::from)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        Iteron::seek(self, SeekKey::End).map_err(Error::from)
    }

    fn valid(&self) -> Result<bool> {
        Iteron::valid(self).map_err(Error::from)
    }

    fn key(&self) -> &[u8] {
        Iteron::key(self)
    }

    fn value(&self) -> &[u8] {
        Iteron::value(self)
    }
}

impl<D: Borrow<DB> + Slightlike> EngineIterator for DBIterator<D> {
    fn next(&mut self) -> Result<bool> {
        DBIterator::next(self).map_err(|e| box_err!(e))
    }

    fn prev(&mut self) -> Result<bool> {
        DBIterator::prev(self).map_err(|e| box_err!(e))
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        DBIterator::seek(self, key.as_encoded().as_slice().into()).map_err(|e| box_err!(e))
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        DBIterator::seek_for_prev(self, key.as_encoded().as_slice().into()).map_err(|e| box_err!(e))
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        DBIterator::seek(self, DBSeekKey::Start).map_err(|e| box_err!(e))
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        DBIterator::seek(self, DBSeekKey::End).map_err(|e| box_err!(e))
    }

    fn valid(&self) -> Result<bool> {
        DBIterator::valid(self).map_err(|e| box_err!(e))
    }

    fn key(&self) -> &[u8] {
        DBIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        DBIterator::value(self)
    }
}

#[causetg(test)]
mod tests {
    use super::super::perf_context::PerfStatisticsInstant;
    use super::super::tests::*;
    use super::super::CfStatistics;
    use super::*;

    #[test]
    fn test_lmdb() {
        let engine = TestEngineBuilder::new()
            .causets(TEST_ENGINE_CAUSETS)
            .build()
            .unwrap();
        test_base_curd_options(&engine)
    }

    #[test]
    fn test_lmdb_linear() {
        let engine = TestEngineBuilder::new()
            .causets(TEST_ENGINE_CAUSETS)
            .build()
            .unwrap();
        test_linear(&engine);
    }

    #[test]
    fn test_lmdb_statistic() {
        let engine = TestEngineBuilder::new()
            .causets(TEST_ENGINE_CAUSETS)
            .build()
            .unwrap();
        test_causets_statistics(&engine);
    }

    #[test]
    fn lmdb_reopen() {
        let dir = tempfile::Builder::new()
            .prefix("lmdb_test")
            .temfidelir()
            .unwrap();
        {
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .causets(TEST_ENGINE_CAUSETS)
                .build()
                .unwrap();
            must_put_causet(&engine, "causet", b"k", b"v1");
        }
        {
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .causets(TEST_ENGINE_CAUSETS)
                .build()
                .unwrap();
            assert_has_causet(&engine, "causet", b"k", b"v1");
        }
    }

    #[test]
    fn test_lmdb_perf_statistics() {
        let engine = TestEngineBuilder::new()
            .causets(TEST_ENGINE_CAUSETS)
            .build()
            .unwrap();
        test_perf_statistics(&engine);
    }

    fn test_perf_statistics<E: Engine>(engine: &E) {
        must_put(engine, b"foo", b"bar1");
        must_put(engine, b"foo2", b"bar2");
        must_put(engine, b"foo3", b"bar3"); // deleted
        must_put(engine, b"foo4", b"bar4");
        must_put(engine, b"foo42", b"bar42"); // deleted
        must_put(engine, b"foo5", b"bar5"); // deleted
        must_put(engine, b"foo6", b"bar6");
        must_delete(engine, b"foo3");
        must_delete(engine, b"foo42");
        must_delete(engine, b"foo5");

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut iter = snapshot
            .iter(IterOptions::default(), ScanMode::Forward)
            .unwrap();

        let mut statistics = CfStatistics::default();

        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(&Key::from_raw(b"foo30"), &mut statistics)
            .unwrap();
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.near_seek(&Key::from_raw(b"foo55"), &mut statistics)
            .unwrap();
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 2);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 2);

        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 3);

        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 3);
    }
}
