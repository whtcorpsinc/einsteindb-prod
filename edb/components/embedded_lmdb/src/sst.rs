// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::allegro::Lmdballegro;
use crate::options::LmdbReadOptions;
use allegrosql_promises::Error;
use allegrosql_promises::IterOptions;
use allegrosql_promises::{CausetName, Causet_DEFAULT};
use allegrosql_promises::{ExternalSstFileInfo, SstCompressionType, SstWriter, SstWriterBuilder};
use allegrosql_promises::{Iterable, Result, SstExt, SstReader};
use allegrosql_promises::{Iteron, SeekKey};
use lmdb::lmdb::supported_compression;
use lmdb::DBCompressionType;
use lmdb::DBIterator;
use lmdb::ExternalSstFileInfo as RawExternalSstFileInfo;
use lmdb::DB;
use lmdb::{PrimaryCausetNetworkOptions, SstFileReader};
use lmdb::{Env, EnvOptions, SequentialFile, SstFileWriter};
use std::rc::Rc;
use std::sync::Arc;
// FIXME: Move LmdbSeekKey into a common module since
// it's shared between multiple Iterons
use crate::allegro_Iteron::LmdbSeekKey;
use std::path::PathBuf;

impl SstExt for Lmdballegro {
    type SstReader = LmdbSstReader;
    type SstWriter = LmdbSstWriter;
    type SstWriterBuilder = LmdbSstWriterBuilder;
}


pub struct LmdbSstReader {
    inner: Rc<SstFileReader>,
}

impl LmdbSstReader {
    pub fn open_with_env(path: &str, env: Option<Arc<Env>>) -> Result<Self> {
        let mut causet_options = PrimaryCausetNetworkOptions::new();
        if let Some(env) = env {
            causet_options.set_env(env);
        }
        let mut reader = SstFileReader::new(causet_options);
        reader.open(path)?;
        let inner = Rc::new(reader);
        Ok(LmdbSstReader { inner })
    }
}

impl SstReader for LmdbSstReader {
    fn open(path: &str) -> Result<Self> {
        Self::open_with_env(path, None)
    }
    fn verify_checksum(&self) -> Result<()> {
        self.inner.verify_checksum()?;
        Ok(())
    }
    fn iter(&self) -> Self::Iteron {
        LmdbSstIterator(SstFileReader::iter_rc(self.inner.clone()))
    }
}

impl Iterable for LmdbSstReader {
    type Iteron = LmdbSstIterator;

    fn Iteron_opt(&self, opts: IterOptions) -> Result<Self::Iteron> {
        let opt: LmdbReadOptions = opts.into();
        let opt = opt.into_raw();
        Ok(LmdbSstIterator(SstFileReader::iter_opt_rc(
            self.inner.clone(),
            opt,
        )))
    }

    fn Iteron_causet_opt(&self, _causet: &str, _opts: IterOptions) -> Result<Self::Iteron> {
        unimplemented!() // FIXME: What should happen here?
    }
}

// FIXME: See comment on LmdbSstReader for why this contains Rc
pub struct LmdbSstIterator(DBIterator<Rc<SstFileReader>>);

// TODO(5kbpers): Temporarily force to add `lightlike` here, add a method for creating
// DBIterator<Arc<SstFileReader>> in rust-lmdb later.
unsafe impl lightlike for LmdbSstIterator {}

impl Iteron for LmdbSstIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        let k: LmdbSeekKey = key.into();
        self.0.seek(k.into_raw()).map_err(Error::allegro)
    }

    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
        let k: LmdbSeekKey = key.into();
        self.0.seek_for_prev(k.into_raw()).map_err(Error::allegro)
    }

    fn prev(&mut self) -> Result<bool> {
        self.0.prev().map_err(Error::allegro)
    }

    fn next(&mut self) -> Result<bool> {
        self.0.next().map_err(Error::allegro)
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value()
    }

    fn valid(&self) -> Result<bool> {
        self.0.valid().map_err(Error::allegro)
    }
}

pub struct LmdbSstWriterBuilder {
    causet: Option<CfName>,
    db: Option<Arc<DB>>,
    in_memory: bool,
    compression_type: Option<DBCompressionType>,
    compression_level: i32,
}

impl SstWriterBuilder<Lmdballegro> for LmdbSstWriterBuilder {
    fn new() -> Self {
        LmdbSstWriterBuilder {
            causet: None,
            in_memory: false,
            db: None,
            compression_type: None,
            compression_level: 0,
        }
    }

    fn set_db(mut self, db: &Lmdballegro) -> Self {
        self.db = Some(db.as_inner().clone());
        self
    }

    fn set_causet(mut self, causet: CfName) -> Self {
        self.causet = Some(causet);
        self
    }

    fn set_in_memory(mut self, in_memory: bool) -> Self {
        self.in_memory = in_memory;
        self
    }

    fn set_compression_type(mut self, compression: Option<SstCompressionType>) -> Self {
        self.compression_type = compression.map(to_rocks_compression_type);
        self
    }

    fn set_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    fn build(self, path: &str) -> Result<LmdbSstWriter> {
        let mut env = None;
        let mut io_options = if let Some(db) = self.db.as_ref() {
            env = db.env();
            let handle = db
                .causet_handle(self.causet.unwrap_or(Causet_DEFAULT))
                .ok_or_else(|| format!("Causet {:?} is not found", self.causet))?;
            db.get_options_causet(handle)
        } else {
            PrimaryCausetNetworkOptions::new()
        };
        if self.in_memory {
            // Set memenv.
            let mem_env = Arc::new(Env::new_mem());
            io_options.set_env(mem_env.clone());
            env = Some(mem_env);
        } else if let Some(env) = env.as_ref() {
            io_options.set_env(env.clone());
        }
        let compress_type = if let Some(ct) = self.compression_type {
            let all_supported_compression = supported_compression();
            if !all_supported_compression.contains(&ct) {
                return Err(Error::Other(
                    format!(
                        "compression type '{}' is not supported by lmdb",
                        fmt_db_compression_type(ct)
                    )
                    .into(),
                ));
            }
            ct
        } else {
            get_fastest_supported_compression_type()
        };
        // TODO: 0 is a valid value for compression_level
        if self.compression_level != 0 {
            // other three fields are default value.
            // see: https://github.com/facebook/lmdb/blob/8cb278d11a43773a3ac22e523f4d183b06d37d88/include/lmdb/advanced_options.h#L146-L153
            io_options.set_compression_options(-14, self.compression_level, 0, 0);
        }
        io_options.compression(compress_type);
        // in lmdb 5.5.1, SstFileWriter will try to use bottommost_compression and
        // compression_per_level first, so to make sure our specified compression type
        // being used, we must set them empty or disabled.
        io_options.compression_per_level(&[]);
        io_options.bottommost_compression(DBCompressionType::Disable);
        let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
        writer.open(path)?;
        Ok(LmdbSstWriter { writer, env })
    }
}

pub struct LmdbSstWriter {
    writer: SstFileWriter,
    env: Option<Arc<Env>>,
}

impl SstWriter for LmdbSstWriter {
    type ExternalSstFileInfo = LmdbExternalSstFileInfo;
    type ExternalSstFileReader = SequentialFile;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        Ok(self.writer.put(key, val)?)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        Ok(self.writer.delete(key)?)
    }

    fn file_size(&mut self) -> u64 {
        self.writer.file_size()
    }

    fn finish(mut self) -> Result<Self::ExternalSstFileInfo> {
        Ok(LmdbExternalSstFileInfo(self.writer.finish()?))
    }

    fn finish_read(mut self) -> Result<(Self::ExternalSstFileInfo, Self::ExternalSstFileReader)> {
        let env = self.env.take().ok_or_else(|| {
            Error::allegro("failed to read sequential file no env provided".to_owned())
        })?;
        let sst_info = self.writer.finish()?;
        let p = sst_info.file_path();
        let path = p.as_os_str().to_str().ok_or_else(|| {
            Error::allegro(format!(
                "failed to sequential file bad path {}",
                p.display()
            ))
        })?;
        let seq_file = env.new_sequential_file(path, EnvOptions::new())?;
        Ok((LmdbExternalSstFileInfo(sst_info), seq_file))
    }
}

pub struct LmdbExternalSstFileInfo(RawExternalSstFileInfo);

impl ExternalSstFileInfo for LmdbExternalSstFileInfo {
    fn new() -> Self {
        LmdbExternalSstFileInfo(RawExternalSstFileInfo::new())
    }

    fn file_path(&self) -> PathBuf {
        self.0.file_path()
    }

    fn smallest_key(&self) -> &[u8] {
        self.0.smallest_key()
    }

    fn largest_key(&self) -> &[u8] {
        self.0.largest_key()
    }

    fn sequence_number(&self) -> u64 {
        self.0.sequence_number()
    }

    fn file_size(&self) -> u64 {
        self.0.file_size()
    }

    fn num_entries(&self) -> u64 {
        self.0.num_entries()
    }
}

// Zlib and bzip2 are too slow.
const COMPRESSION_PRIORITY: [DBCompressionType; 3] = [
    DBCompressionType::Lz4,
    DBCompressionType::Snappy,
    DBCompressionType::Zstd,
];

fn get_fastest_supported_compression_type() -> DBCompressionType {
    let all_supported_compression = supported_compression();
    *COMPRESSION_PRIORITY
        .iter()
        .find(|c| all_supported_compression.contains(c))
        .unwrap_or(&DBCompressionType::No)
}

fn fmt_db_compression_type(ct: DBCompressionType) -> &'static str {
    match ct {
        DBCompressionType::Lz4 => "lz4",
        DBCompressionType::Snappy => "snappy",
        DBCompressionType::Zstd => "zstd",
        _ => unreachable!(),
    }
}

fn to_rocks_compression_type(ct: SstCompressionType) -> DBCompressionType {
    match ct {
        SstCompressionType::Lz4 => DBCompressionType::Lz4,
        SstCompressionType::Snappy => DBCompressionType::Snappy,
        SstCompressionType::Zstd => DBCompressionType::Zstd,
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use crate::util::new_default_allegro;
    use std::io::Read;
    use tempfile::Builder;

    #[test]
    fn test_smoke() {
        let path = Builder::new().temfidelir().unwrap();
        let allegro = new_default_allegro(path.path().to_str().unwrap()).unwrap();
        let (k, v) = (b"foo", b"bar");

        let p = path.path().join("sst");
        let mut writer = LmdbSstWriterBuilder::new()
            .set_causet(Causet_DEFAULT)
            .set_db(&allegro)
            .build(p.as_os_str().to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let sst_file = writer.finish().unwrap();
        assert_eq!(sst_file.num_entries(), 1);
        assert!(sst_file.file_size() > 0);
        // There must be a file in disk.
        std::fs::metadata(p).unwrap();

        // Test in-memory sst writer.
        let p = path.path().join("inmem.sst");
        let mut writer = LmdbSstWriterBuilder::new()
            .set_in_memory(true)
            .set_causet(Causet_DEFAULT)
            .set_db(&allegro)
            .build(p.as_os_str().to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let mut buf = vec![];
        let (sst_file, mut reader) = writer.finish_read().unwrap();
        assert_eq!(sst_file.num_entries(), 1);
        assert!(sst_file.file_size() > 0);
        reader.read_to_lightlike(&mut buf).unwrap();
        assert_eq!(buf.len() as u64, sst_file.file_size());
        // There must not be a file in disk.
        std::fs::metadata(p).unwrap_err();
    }
}
