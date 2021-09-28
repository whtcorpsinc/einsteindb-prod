// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::error;
use std::fmt::{self, Display, Formatter};
use std::time::Instant;

use edb::CausetEngine;
use edb::Causet_WRITE;
use violetabftstore::interlock::::worker::Runnable;

use super::metrics::COMPACT_RANGE_Causet;
use engine_lmdb::properties::get_cone_entries_and_versions;

type Key = Vec<u8>;

pub enum Task {
    Compact {
        causet_name: String,
        spacelike_key: Option<Key>, // None means smallest key
        lightlike_key: Option<Key>,   // None means largest key
    },

    CheckAndCompact {
        causet_names: Vec<String>,         // PrimaryCauset families need to compact
        cones: Vec<Key>,              // Cones need to check
        tombstones_num_memory_barrier: u64, // The minimum Lmdb tombstones a cone that need compacting has
        tombstones_percent_memory_barrier: u64,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Compact {
                ref causet_name,
                ref spacelike_key,
                ref lightlike_key,
            } => f
                .debug_struct("Compact")
                .field("causet_name", causet_name)
                .field(
                    "spacelike_key",
                    &spacelike_key.as_ref().map(|k| hex::encode_upper(k)),
                )
                .field("lightlike_key", &lightlike_key.as_ref().map(|k| hex::encode_upper(k)))
                .finish(),
            Task::CheckAndCompact {
                ref causet_names,
                ref cones,
                tombstones_num_memory_barrier,
                tombstones_percent_memory_barrier,
            } => f
                .debug_struct("CheckAndCompact")
                .field("causet_names", causet_names)
                .field(
                    "cones",
                    &(
                        cones.first().as_ref().map(|k| hex::encode_upper(k)),
                        cones.last().as_ref().map(|k| hex::encode_upper(k)),
                    ),
                )
                .field("tombstones_num_memory_barrier", &tombstones_num_memory_barrier)
                .field(
                    "tombstones_percent_memory_barrier",
                    &tombstones_percent_memory_barrier,
                )
                .finish(),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<dyn error::Error + Sync + lightlike>) {
            from()
            cause(err.as_ref())
            display("compact failed {:?}", err)
        }
    }
}

pub struct Runner<E> {
    engine: E,
}

impl<E> Runner<E>
where
    E: CausetEngine,
{
    pub fn new(engine: E) -> Runner<E> {
        Runner { engine }
    }

    /// lightlikes a compact cone command to Lmdb to compact the cone of the causet.
    pub fn compact_cone_causet(
        &mut self,
        causet_name: &str,
        spacelike_key: Option<&[u8]>,
        lightlike_key: Option<&[u8]>,
    ) -> Result<(), Error> {
        let timer = Instant::now();
        let compact_cone_timer = COMPACT_RANGE_Causet
            .with_label_values(&[causet_name])
            .spacelike_coarse_timer();
        box_try!(self
            .engine
            .compact_cone(causet_name, spacelike_key, lightlike_key, false, 1 /* threads */,));
        compact_cone_timer.observe_duration();
        info!(
            "compact cone finished";
            "cone_spacelike" => spacelike_key.map(::log_wrappers::Key),
            "cone_lightlike" => lightlike_key.map(::log_wrappers::Key),
            "causet" => causet_name,
            "time_takes" => ?timer.elapsed(),
        );
        Ok(())
    }
}

impl<E> Runnable for Runner<E>
where
    E: CausetEngine,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Compact {
                causet_name,
                spacelike_key,
                lightlike_key,
            } => {
                let causet = &causet_name;
                if let Err(e) = self.compact_cone_causet(causet, spacelike_key.as_deref(), lightlike_key.as_deref())
                {
                    error!("execute compact cone failed"; "causet" => causet, "err" => %e);
                }
            }
            Task::CheckAndCompact {
                causet_names,
                cones,
                tombstones_num_memory_barrier,
                tombstones_percent_memory_barrier,
            } => match collect_cones_need_compact(
                &self.engine,
                cones,
                tombstones_num_memory_barrier,
                tombstones_percent_memory_barrier,
            ) {
                Ok(mut cones) => {
                    for (spacelike, lightlike) in cones.drain(..) {
                        for causet in &causet_names {
                            if let Err(e) = self.compact_cone_causet(causet, Some(&spacelike), Some(&lightlike)) {
                                error!(
                                    "compact cone failed";
                                    "cone_spacelike" => log_wrappers::Key(&spacelike),
                                    "cone_lightlike" => log_wrappers::Key(&lightlike),
                                    "causet" => causet,
                                    "err" => %e,
                                );
                            }
                        }
                        fail_point!("violetabftstore::compact::CheckAndCompact:AfterCompact");
                    }
                }
                Err(e) => warn!("check cones need reclaim failed"; "err" => %e),
            },
        }
    }
}

fn need_compact(
    num_entires: u64,
    num_versions: u64,
    tombstones_num_memory_barrier: u64,
    tombstones_percent_memory_barrier: u64,
) -> bool {
    if num_entires <= num_versions {
        return false;
    }

    // When the number of tombstones exceed memory_barrier and ratio, this cone need compacting.
    let estimate_num_del = num_entires - num_versions;
    estimate_num_del >= tombstones_num_memory_barrier
        && estimate_num_del * 100 >= tombstones_percent_memory_barrier * num_entires
}

fn collect_cones_need_compact(
    engine: &impl CausetEngine,
    cones: Vec<Key>,
    tombstones_num_memory_barrier: u64,
    tombstones_percent_memory_barrier: u64,
) -> Result<VecDeque<(Key, Key)>, Error> {
    // Check the SST properties for each cone, and EinsteinDB will compact a cone if the cone
    // contains too many Lmdb tombstones. EinsteinDB will merge multiple neighboring cones
    // that need compacting into a single cone.
    let mut cones_need_compact = VecDeque::new();
    let causet = box_try!(engine.causet_handle(Causet_WRITE));
    let mut compact_spacelike = None;
    let mut compact_lightlike = None;
    for cone in cones.windows(2) {
        // Get total entries and total versions in this cone and checks if it needs to be compacted.
        if let Some((num_ent, num_ver)) =
            get_cone_entries_and_versions(engine, causet, &cone[0], &cone[1])
        {
            if need_compact(
                num_ent,
                num_ver,
                tombstones_num_memory_barrier,
                tombstones_percent_memory_barrier,
            ) {
                if compact_spacelike.is_none() {
                    // The previous cone doesn't need compacting.
                    compact_spacelike = Some(cone[0].clone());
                }
                compact_lightlike = Some(cone[1].clone());
                // Move to next cone.
                continue;
            }
        }

        // Current cone doesn't need compacting, save previous cone that need compacting.
        if compact_spacelike.is_some() {
            assert!(compact_lightlike.is_some());
        }
        if let (Some(cs), Some(ce)) = (compact_spacelike, compact_lightlike) {
            cones_need_compact.push_back((cs, ce));
        }
        compact_spacelike = None;
        compact_lightlike = None;
    }

    // Save the last cone that needs to be compacted.
    if compact_spacelike.is_some() {
        assert!(compact_lightlike.is_some());
    }
    if let (Some(cs), Some(ce)) = (compact_spacelike, compact_lightlike) {
        cones_need_compact.push_back((cs, ce));
    }

    Ok(cones_need_compact)
}

#[causet(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use engine_lmdb::raw::WriBlock;
    use engine_lmdb::raw::DB;
    use engine_lmdb::raw::{PrimaryCausetNetworkOptions, DBOptions};
    use engine_lmdb::raw_util::{new_engine, new_engine_opt, CausetOptions};
    use engine_lmdb::util::get_causet_handle;
    use engine_lmdb::Compat;
    use edb::{CausetHandleExt, MuBlock, WriteBatchExt};
    use edb::{Causet_DEFAULT, Causet_DAGGER, Causet_VIOLETABFT, Causet_WRITE};
    use tempfile::Builder;

    use engine_lmdb::get_cone_entries_and_versions;
    use engine_lmdb::MvccPropertiesCollectorFactory;
    use tuplespaceInstanton::data_key;
    use std::sync::Arc;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::*;

    const LMDB_TOTAL_SST_FILES_SIZE: &str = "lmdb.total-sst-files-size";

    #[test]
    fn test_compact_cone() {
        let path = Builder::new()
            .prefix("compact-cone-test")
            .temfidelir()
            .unwrap();
        let db = new_engine(path.path().to_str().unwrap(), None, &[Causet_DEFAULT], None).unwrap();
        let db = Arc::new(db);

        let mut runner = Runner::new(db.c().clone());

        let handle = get_causet_handle(&db, Causet_DEFAULT).unwrap();

        // Generate the first SST file.
        let mut wb = db.c().write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_causet(Causet_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        db.c().write(&wb).unwrap();
        db.flush_causet(handle, true).unwrap();

        // Generate another SST file has the same content with first SST file.
        let mut wb = db.c().write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_causet(Causet_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        db.c().write(&wb).unwrap();
        db.flush_causet(handle, true).unwrap();

        // Get the total SST files size.
        let old_sst_files_size = db
            .get_property_int_causet(handle, LMDB_TOTAL_SST_FILES_SIZE)
            .unwrap();

        // Schedule compact cone task.
        runner.run(Task::Compact {
            causet_name: String::from(Causet_DEFAULT),
            spacelike_key: None,
            lightlike_key: None,
        });
        sleep(Duration::from_secs(5));

        // Get the total SST files size after compact cone.
        let new_sst_files_size = db
            .get_property_int_causet(handle, LMDB_TOTAL_SST_FILES_SIZE)
            .unwrap();
        assert!(old_sst_files_size > new_sst_files_size);
    }

    fn tail_pointer_put(db: &DB, k: &[u8], v: &[u8], spacelike_ts: TimeStamp, commit_ts: TimeStamp) {
        let causet = get_causet_handle(db, Causet_WRITE).unwrap();
        let k = Key::from_encoded(data_key(k)).applightlike_ts(commit_ts);
        let w = Write::new(WriteType::Put, spacelike_ts, Some(v.to_vec()));
        db.put_causet(causet, k.as_encoded(), &w.as_ref().to_bytes())
            .unwrap();
    }

    fn delete(db: &DB, k: &[u8], commit_ts: TimeStamp) {
        let causet = get_causet_handle(db, Causet_WRITE).unwrap();
        let k = Key::from_encoded(data_key(k)).applightlike_ts(commit_ts);
        db.delete_causet(causet, k.as_encoded()).unwrap();
    }

    fn open_db(path: &str) -> Arc<DB> {
        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_level_zero_file_num_compaction_trigger(8);
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.test-collector", f);
        let causets_opts = vec![
            CausetOptions::new(Causet_DEFAULT, PrimaryCausetNetworkOptions::new()),
            CausetOptions::new(Causet_VIOLETABFT, PrimaryCausetNetworkOptions::new()),
            CausetOptions::new(Causet_DAGGER, PrimaryCausetNetworkOptions::new()),
            CausetOptions::new(Causet_WRITE, causet_opts),
        ];
        Arc::new(new_engine_opt(path, db_opts, causets_opts).unwrap())
    }

    #[test]
    fn test_check_space_redundancy() {
        let tmp_dir = Builder::new().prefix("test").temfidelir().unwrap();
        let engine = open_db(tmp_dir.path().to_str().unwrap());
        let causet = get_causet_handle(&engine, Causet_WRITE).unwrap();
        let causet2 = engine.c().causet_handle(Causet_WRITE).unwrap();

        // tail_pointer_put 0..5
        for i in 0..5 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            tail_pointer_put(&engine, k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        }
        engine.flush_causet(causet, true).unwrap();

        // gc 0..5
        for i in 0..5 {
            let k = format!("k{}", i);
            delete(&engine, k.as_bytes(), 2.into());
        }
        engine.flush_causet(causet, true).unwrap();

        let (spacelike, lightlike) = (data_key(b"k0"), data_key(b"k5"));
        let (entries, version) =
            get_cone_entries_and_versions(engine.c(), causet2, &spacelike, &lightlike).unwrap();
        assert_eq!(entries, 10);
        assert_eq!(version, 5);

        // tail_pointer_put 5..10
        for i in 5..10 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            tail_pointer_put(&engine, k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        }
        engine.flush_causet(causet, true).unwrap();

        let (s, e) = (data_key(b"k5"), data_key(b"k9"));
        let (entries, version) = get_cone_entries_and_versions(engine.c(), causet2, &s, &e).unwrap();
        assert_eq!(entries, 5);
        assert_eq!(version, 5);

        let cones_need_to_compact = collect_cones_need_compact(
            engine.c(),
            vec![data_key(b"k0"), data_key(b"k5"), data_key(b"k9")],
            1,
            50,
        )
        .unwrap();
        let (s, e) = (data_key(b"k0"), data_key(b"k5"));
        let mut expected_cones = VecDeque::new();
        expected_cones.push_back((s, e));
        assert_eq!(cones_need_to_compact, expected_cones);

        // gc 5..10
        for i in 5..10 {
            let k = format!("k{}", i);
            delete(&engine, k.as_bytes(), 2.into());
        }
        engine.flush_causet(causet, true).unwrap();

        let (s, e) = (data_key(b"k5"), data_key(b"k9"));
        let (entries, version) = get_cone_entries_and_versions(engine.c(), causet2, &s, &e).unwrap();
        assert_eq!(entries, 10);
        assert_eq!(version, 5);

        let cones_need_to_compact = collect_cones_need_compact(
            engine.c(),
            vec![data_key(b"k0"), data_key(b"k5"), data_key(b"k9")],
            1,
            50,
        )
        .unwrap();
        let (s, e) = (data_key(b"k0"), data_key(b"k9"));
        let mut expected_cones = VecDeque::new();
        expected_cones.push_back((s, e));
        assert_eq!(cones_need_to_compact, expected_cones);
    }
}
