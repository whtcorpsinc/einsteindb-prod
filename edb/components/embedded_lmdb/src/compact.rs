// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::edb::LmdbEngine;
use crate::util;
use edb::*;
use lmdb::{CompactOptions, CompactionOptions, DBCompressionType};
use std::cmp;

impl CompactExt for LmdbEngine {
    type CompactedEvent = crate::compact_listener::LmdbCompactedEvent;

    fn auto_compactions_is_disabled(&self) -> Result<bool> {
        for causet_name in self.causet_names() {
            let causet = util::get_causet_handle(self.as_inner(), causet_name)?;
            if self
                .as_inner()
                .get_options_causet(causet)
                .get_disable_auto_compactions()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn compact_cone(
        &self,
        causet: &str,
        spacelike_key: Option<&[u8]>,
        lightlike_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subcompactions: u32,
    ) -> Result<()> {
        let db = self.as_inner();
        let handle = util::get_causet_handle(db, causet)?;
        let mut compact_opts = CompactOptions::new();
        // `exclusive_manual == false` means manual compaction can
        // concurrently run with other background compactions.
        compact_opts.set_exclusive_manual_compaction(exclusive_manual);
        compact_opts.set_max_subcompactions(max_subcompactions as i32);
        db.compact_cone_causet_opt(handle, &compact_opts, spacelike_key, lightlike_key);
        Ok(())
    }

    fn compact_files_in_cone(
        &self,
        spacelike: Option<&[u8]>,
        lightlike: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        for causet_name in self.causet_names() {
            self.compact_files_in_cone_causet(causet_name, spacelike, lightlike, output_level)?;
        }
        Ok(())
    }

    fn compact_files_in_cone_causet(
        &self,
        causet_name: &str,
        spacelike: Option<&[u8]>,
        lightlike: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        let db = self.as_inner();
        let causet = util::get_causet_handle(db, causet_name)?;
        let causet_opts = db.get_options_causet(causet);
        let output_level = output_level.unwrap_or(causet_opts.get_num_levels() as i32 - 1);
        let output_compression = causet_opts
            .get_compression_per_level()
            .get(output_level as usize)
            .cloned()
            .unwrap_or(DBCompressionType::No);
        let output_file_size_limit = causet_opts.get_target_file_size_base() as usize;

        let mut input_files = Vec::new();
        let causet_meta = db.get_PrimaryCauset_family_meta_data(causet);
        for (i, level) in causet_meta.get_levels().iter().enumerate() {
            if i as i32 >= output_level {
                break;
            }
            for f in level.get_files() {
                if lightlike.is_some() && lightlike.unwrap() <= f.get_smallestkey() {
                    continue;
                }
                if spacelike.is_some() && spacelike.unwrap() > f.get_largestkey() {
                    continue;
                }
                input_files.push(f.get_name());
            }
        }
        if input_files.is_empty() {
            return Ok(());
        }

        let mut opts = CompactionOptions::new();
        opts.set_compression(output_compression);
        let max_subcompactions = num_cpus::get();
        let max_subcompactions = cmp::min(max_subcompactions, 32);
        opts.set_max_subcompactions(max_subcompactions as i32);
        opts.set_output_file_size_limit(output_file_size_limit);
        db.compact_files_causet(causet, &opts, &input_files, output_level)?;

        Ok(())
    }
}

#[causet(test)]
mod tests {
    use crate::raw_util::{new_engine, CausetOptions};
    use crate::Compat;
    use edb::CompactExt;
    use lmdb::{PrimaryCausetNetworkOptions, WriBlock};
    use std::sync::Arc;
    use tempfile::Builder;

    #[test]
    fn test_compact_files_in_cone() {
        let temp_dir = Builder::new()
            .prefix("test_compact_files_in_cone")
            .temfidelir()
            .unwrap();

        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_disable_auto_compactions(true);
        let causets_opts = vec![
            CausetOptions::new("default", causet_opts.clone()),
            CausetOptions::new("test", causet_opts),
        ];
        let db = new_engine(
            temp_dir.path().to_str().unwrap(),
            None,
            &["default", "test"],
            Some(causets_opts),
        )
        .unwrap();
        let db = Arc::new(db);

        for causet_name in db.causet_names() {
            let causet = db.causet_handle(causet_name).unwrap();
            for i in 0..5 {
                db.put_causet(causet, &[i], &[i]).unwrap();
                db.put_causet(causet, &[i + 1], &[i + 1]).unwrap();
                db.flush_causet(causet, true).unwrap();
            }
            let causet_meta = db.get_PrimaryCauset_family_meta_data(causet);
            let causet_levels = causet_meta.get_levels();
            assert_eq!(causet_levels.first().unwrap().get_files().len(), 5);
        }

        // # Before
        // Level-0: [4-5], [3-4], [2-3], [1-2], [0-1]
        // # After
        // Level-0: [4-5]
        // Level-1: [0-4]
        db.c()
            .compact_files_in_cone(None, Some(&[4]), Some(1))
            .unwrap();

        for causet_name in db.causet_names() {
            let causet = db.causet_handle(causet_name).unwrap();
            let causet_meta = db.get_PrimaryCauset_family_meta_data(causet);
            let causet_levels = causet_meta.get_levels();
            let level_0 = causet_levels[0].get_files();
            assert_eq!(level_0.len(), 1);
            assert_eq!(level_0[0].get_smallestkey(), &[4]);
            assert_eq!(level_0[0].get_largestkey(), &[5]);
            let level_1 = causet_levels[1].get_files();
            assert_eq!(level_1.len(), 1);
            assert_eq!(level_1[0].get_smallestkey(), &[0]);
            assert_eq!(level_1[0].get_largestkey(), &[4]);
        }

        // # Before
        // Level-0: [4-5]
        // Level-1: [0-4]
        // # After
        // Level-0: [4-5]
        // Level-N: [0-4]
        db.c()
            .compact_files_in_cone(Some(&[2]), Some(&[4]), None)
            .unwrap();

        for causet_name in db.causet_names() {
            let causet = db.causet_handle(causet_name).unwrap();
            let causet_opts = db.get_options_causet(causet);
            let causet_meta = db.get_PrimaryCauset_family_meta_data(causet);
            let causet_levels = causet_meta.get_levels();
            let level_0 = causet_levels[0].get_files();
            assert_eq!(level_0.len(), 1);
            assert_eq!(level_0[0].get_smallestkey(), &[4]);
            assert_eq!(level_0[0].get_largestkey(), &[5]);
            let level_n = causet_levels[causet_opts.get_num_levels() - 1].get_files();
            assert_eq!(level_n.len(), 1);
            assert_eq!(level_n[0].get_smallestkey(), &[0]);
            assert_eq!(level_n[0].get_largestkey(), &[4]);
        }
    }
}
