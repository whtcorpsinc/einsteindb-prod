// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::edb::LmdbEngine;
use edb::ImportExt;
use edb::IngestExternalFileOptions;
use edb::Result;
use lmdb::set_external_sst_file_global_seq_no;
use lmdb::IngestExternalFileOptions as RawIngestExternalFileOptions;
use std::fs::File;
use std::path::Path;

impl ImportExt for LmdbEngine {
    type IngestExternalFileOptions = LmdbIngestExternalFileOptions;

    fn ingest_external_file_causet(
        &self,
        causet: &Self::CausetHandle,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        let causet = causet.as_inner();
        // This is calling a specially optimized version of
        // ingest_external_file_causet. In cases where the memBlock needs to be
        // flushed it avoids blocking writers while doing the flush. The unused
        // return value here just indicates whether the fallback path requiring
        // the manual memBlock flush was taken.
        let _did_nonblocking_memBlock_flush = self
            .as_inner()
            .ingest_external_file_optimized(&causet, &opts.0, files)?;
        Ok(())
    }

    // TODO: rename it to `reset_global_seq`.
    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        causet: &Self::CausetHandle,
        path: P,
        _expected_size: u64,
        _expected_checksum: u32,
    ) -> Result<()> {
        let path = path.as_ref().to_str().unwrap();
        let f = File::open(path)?;

        // Lmdb may have modified the global seqno.
        let causet = causet.as_inner();
        set_external_sst_file_global_seq_no(&self.as_inner(), causet, path, 0)?;
        f.sync_all()
            .map_err(|e| format!("sync {}: {:?}", path, e))?;

        Ok(())
    }
}

pub struct LmdbIngestExternalFileOptions(RawIngestExternalFileOptions);

impl IngestExternalFileOptions for LmdbIngestExternalFileOptions {
    fn new() -> LmdbIngestExternalFileOptions {
        LmdbIngestExternalFileOptions(RawIngestExternalFileOptions::new())
    }

    fn move_files(&mut self, f: bool) {
        self.0.move_files(f);
    }
}
