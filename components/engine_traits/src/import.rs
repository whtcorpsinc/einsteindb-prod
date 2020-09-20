// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_handle::CAUSETHandleExt;
use crate::errors::Result;
use std::path::Path;

pub trait ImportExt: CAUSETHandleExt {
    type IngestExternalFileOptions: IngestExternalFileOptions;

    fn ingest_external_file_causet(
        &self,
        causet: &Self::CAUSETHandle,
        opt: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()>;

    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        causet: &Self::CAUSETHandle,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()>;
}

pub trait IngestExternalFileOptions {
    fn new() -> Self;

    fn move_files(&mut self, f: bool);
}
