// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_promises::{ImportExt, IngestExternalFileOptions, Result};
use std::path::Path;

impl ImportExt for PanicEngine {
    type IngestExternalFileOptions = PanicIngestExternalFileOptions;

    fn ingest_external_file_causet(
        &self,
        causet: &Self::CAUSETHandle,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        panic!()
    }

    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        causet: &Self::CAUSETHandle,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()> {
        panic!()
    }
}

pub struct PanicIngestExternalFileOptions;

impl IngestExternalFileOptions for PanicIngestExternalFileOptions {
    fn new() -> Self {
        panic!()
    }
    fn move_files(&mut self, f: bool) {
        panic!()
    }
}
