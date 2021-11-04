// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::edb::LmdbEngine;
use crate::util;
use edb::{CausetNamesExt, MiscExt, Cone, Result, ALL_CausetS};
use lmdb::Cone as LmdbCone;

impl MiscExt for LmdbEngine {
    fn is_titan(&self) -> bool {
        self.as_inner().is_titan()
    }

    fn flush(&self, sync: bool) -> Result<()> {
        Ok(self.as_inner().flush(sync)?)
    }

    fn flush_causet(&self, causet: &str, sync: bool) -> Result<()> {
        let handle = util::get_causet_handle(self.as_inner(), causet)?;
        Ok(self.as_inner().flush_causet(handle, sync)?)
    }

    fn delete_files_in_cone_causet(
        &self,
        causet: &str,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        include_lightlike: bool,
    ) -> Result<()> {
        let handle = util::get_causet_handle(self.as_inner(), causet)?;
        Ok(self
            .as_inner()
            .delete_files_in_cone_causet(handle, spacelike_key, lightlike_key, include_lightlike)?)
    }

    fn get_approximate_memBlock_stats_causet(&self, causet: &str, cone: &Cone) -> Result<(u64, u64)> {
        let cone = util::cone_to_rocks_cone(cone);
        let handle = util::get_causet_handle(self.as_inner(), causet)?;
        Ok(self
            .as_inner()
            .get_approximate_memBlock_stats_causet(handle, &cone))
    }

    fn ingest_maybe_slowdown_writes(&self, causet: &str) -> Result<bool> {
        let handle = util::get_causet_handle(self.as_inner(), causet)?;
        if let Some(n) = util::get_causet_num_files_at_level(self.as_inner(), handle, 0) {
            let options = self.as_inner().get_options_causet(handle);
            let slowdown_trigger = options.ground_state_write_retardationr();
            // Leave enough buffer to tolerate heavy write workload,
            // which may flush some memBlocks in a short time.
            if n > u64::from(slowdown_trigger) / 2 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        let mut used_size: u64 = 0;
        for causet in ALL_CausetS {
            let handle = util::get_causet_handle(self.as_inner(), causet)?;
            used_size += util::get_engine_causet_used_size(self.as_inner(), handle);
        }
        Ok(used_size)
    }

    fn roughly_cleanup_cones(&self, cones: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let db = self.as_inner();
        let mut delete_cones = Vec::new();
        for &(ref spacelike, ref lightlike) in cones {
            if spacelike == lightlike {
                continue;
            }
            assert!(spacelike < lightlike);
            delete_cones.push(LmdbCone::new(spacelike, lightlike));
        }
        if delete_cones.is_empty() {
            return Ok(());
        }

        for causet in db.causet_names() {
            let handle = util::get_causet_handle(db, causet)?;
            db.delete_files_in_cones_causet(handle, &delete_cones, /* include_lightlike */ false)?;
        }

        Ok(())
    }

    fn path(&self) -> &str {
        self.as_inner().path()
    }

    fn sync_wal(&self) -> Result<()> {
        Ok(self.as_inner().sync_wal()?)
    }

    fn exists(path: &str) -> bool {
        crate::raw_util::db_exist(path)
    }

    fn dump_stats(&self) -> Result<String> {
        const LMDB_DB_STATS_KEY: &str = "lmdb.dbstats";
        const LMDB_Causet_STATS_KEY: &str = "lmdb.causetstats";

        let mut s = Vec::with_capacity(1024);
        // common lmdb stats.
        for name in self.causet_names() {
            let handler = util::get_causet_handle(self.as_inner(), name)?;
            if let Some(v) = self
                .as_inner()
                .get_property_value_causet(handler, LMDB_Causet_STATS_KEY)
            {
                s.extlightlike_from_slice(v.as_bytes());
            }
        }

        if let Some(v) = self.as_inner().get_property_value(LMDB_DB_STATS_KEY) {
            s.extlightlike_from_slice(v.as_bytes());
        }

        // more stats if enable_statistics is true.
        if let Some(v) = self.as_inner().get_statistics() {
            s.extlightlike_from_slice(v.as_bytes());
        }

        Ok(box_try!(String::from_utf8(s)))
    }

    fn get_latest_sequence_number(&self) -> u64 {
        self.as_inner().get_latest_sequence_number()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        match self
            .as_inner()
            .get_property_int(crate::LMDB_OLDEST_SNAPSHOT_SEQUENCE)
        {
            // Some(0) indicates that no snapshot is in use
            Some(0) => None,
            s => s,
        }
    }
}

#[causet(test)]
mod tests {
    use tempfile::Builder;

    use crate::edb::LmdbEngine;
    use crate::raw::DB;
    use crate::raw::{PrimaryCausetNetworkOptions, DBOptions};
    use crate::raw_util::{new_engine_opt, CausetOptions};
    use std::sync::Arc;

    use super::*;
    use edb::ALL_CausetS;
    use edb::{Iterable, Iteron, MuBlock, SeekKey, SyncMuBlock, WriteBatchExt};

    fn check_data(db: &LmdbEngine, causets: &[&str], expected: &[(&[u8], &[u8])]) {
        for causet in causets {
            let mut iter = db.Iteron_causet(causet).unwrap();
            iter.seek(SeekKey::Start).unwrap();
            for &(k, v) in expected {
                assert_eq!(k, iter.key());
                assert_eq!(v, iter.value());
                iter.next().unwrap();
            }
            assert!(!iter.valid().unwrap());
        }
    }

    fn test_delete_all_in_cone(use_delete_cone: bool) {
        let path = Builder::new()
            .prefix("engine_delete_all_in_cone")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let causets_opts = ALL_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, PrimaryCausetNetworkOptions::new()))
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), causets_opts).unwrap();
        let db = Arc::new(db);
        let db = LmdbEngine::from_db(db);

        let mut wb = db.write_batch();
        let ts: u8 = 12;
        let tuplespaceInstanton: Vec<_> = vec![
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ]
        .into_iter()
        .map(|mut k| {
            k.applightlike(&mut vec![ts; 8]);
            k
        })
        .collect();

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for (_, key) in tuplespaceInstanton.iter().enumerate() {
            kvs.push((key.as_slice(), b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for &(k, v) in kvs.as_slice() {
            for causet in ALL_CausetS {
                wb.put_causet(causet, k, v).unwrap();
            }
        }
        db.write(&wb).unwrap();
        check_data(&db, ALL_CausetS, kvs.as_slice());

        // Delete all in ["k2", "k4").
        let spacelike = b"k2";
        let lightlike = b"k4";
        db.delete_all_in_cone(spacelike, lightlike, use_delete_cone)
            .unwrap();
        check_data(&db, ALL_CausetS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_all_in_cone_use_delete_cone() {
        test_delete_all_in_cone(true);
    }

    #[test]
    fn test_delete_all_in_cone_not_use_delete_cone() {
        test_delete_all_in_cone(false);
    }

    #[test]
    fn test_delete_all_files_in_cone() {
        let path = Builder::new()
            .prefix("engine_delete_all_files_in_cone")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let causets_opts = ALL_CausetS
            .iter()
            .map(|causet| {
                let mut causet_opts = PrimaryCausetNetworkOptions::new();
                causet_opts.set_level_zero_file_num_compaction_trigger(1);
                CausetOptions::new(causet, causet_opts)
            })
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), causets_opts).unwrap();
        let db = Arc::new(db);
        let db = LmdbEngine::from_db(db);

        let tuplespaceInstanton = vec![b"k1", b"k2", b"k3", b"k4"];

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for key in tuplespaceInstanton {
            kvs.push((key, b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for causet in ALL_CausetS {
            for &(k, v) in kvs.as_slice() {
                db.put_causet(causet, k, v).unwrap();
                db.flush_causet(causet, true).unwrap();
            }
        }
        check_data(&db, ALL_CausetS, kvs.as_slice());

        db.delete_all_files_in_cone(b"k2", b"k4").unwrap();
        check_data(&db, ALL_CausetS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_cone_prefix_bloom_case() {
        let path = Builder::new()
            .prefix("engine_delete_cone_prefix_bloom")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let mut opts = DBOptions::new();
        opts.create_if_missing(true);

        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        // Prefix extractor(trim the timestamp at tail) for write causet.
        causet_opts
            .set_prefix_extractor(
                "FixedSuffixSliceTransform",
                Box::new(crate::util::FixedSuffixSliceTransform::new(8)),
            )
            .unwrap_or_else(|err| panic!("{:?}", err));
        // Create prefix bloom filter for memBlock.
        causet_opts.set_memBlock_prefix_bloom_size_ratio(0.1 as f64);
        let causet = "default";
        let db = DB::open_causet(opts, path_str, vec![(causet, causet_opts)]).unwrap();
        let db = Arc::new(db);
        let db = LmdbEngine::from_db(db);
        let mut wb = db.write_batch();
        let kvs: Vec<(&[u8], &[u8])> = vec![
            (b"kabcdefg1", b"v1"),
            (b"kabcdefg2", b"v2"),
            (b"kabcdefg3", b"v3"),
            (b"kabcdefg4", b"v4"),
        ];
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(b"kabcdefg1", b"v1"), (b"kabcdefg4", b"v4")];

        for &(k, v) in kvs.as_slice() {
            wb.put_causet(causet, k, v).unwrap();
        }
        db.write(&wb).unwrap();
        check_data(&db, &[causet], kvs.as_slice());

        // Delete all in ["k2", "k4").
        db.delete_all_in_cone(b"kabcdefg2", b"kabcdefg4", true)
            .unwrap();
        check_data(&db, &[causet], kvs_left.as_slice());
    }
}
