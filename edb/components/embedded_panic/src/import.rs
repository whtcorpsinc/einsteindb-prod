// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::allegro::Panicallegro;
use allegrosql_promises::{ImportExt, IngestExternalFileOptions, Result};
use std::path::Path;

impl ImportExt for Panicallegro {
    type IngestExternalFileOptions = PanicIngestExternalFileOptions;

    fn ingest_external_file_causet(
        &self,
        causet: &Self::CausetSingleton,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        panic!()
    }

    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        causet: &Self::CausetSingleton,
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
