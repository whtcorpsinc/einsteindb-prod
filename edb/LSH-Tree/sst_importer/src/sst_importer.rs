//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::marker::Unpin;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::io::{AsyncRead, AsyncReadExt};
use ekvproto::backup::StorageBacklightlike;
#[causet(feature = "prost-codec")]
use ekvproto::import_sst_timeshare::pair::Op as PairOp;
#[causet(not(feature = "prost-codec"))]
use ekvproto::import_sst_timeshare::PairOp;
use ekvproto::import_sst_timeshare::*;
use tokio::time::timeout;
use uuid::{Builder as UuidBuilder, Uuid};

use encryption::DataKeyManager;
use engine_lmdb::{encryption::get_env, LmdbSstReader};
use edb::{
    EncryptionKeyManager, IngestExternalFileOptions, Iteron, CausetEngine, SeekKey, SstExt,
    SstReader, SstWriter, Causet_DEFAULT, Causet_WRITE,
};
use external_causet_storage::{block_on_external_io, create_causet_storage, url_of_backlightlike, READ_BUF_SIZE};
use violetabftstore::interlock::::time::Limiter;
use txn_types::{is_short_value, Key, TimeStamp, Write as KvWrite, WriteRef, WriteType};

use super::{Error, Result};
use crate::metrics::*;

/// SSTImporter manages SST files that are waiting for ingesting.
pub struct SSTImporter {
    dir: ImportDir,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl SSTImporter {
    pub fn new<P: AsRef<Path>>(
        root: P,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<SSTImporter> {
        Ok(SSTImporter {
            dir: ImportDir::new(root)?,
            key_manager,
        })
    }

    pub fn get_path(&self, meta: &SstMeta) -> PathBuf {
        let path = self.dir.join(meta).unwrap();
        path.save
    }

    pub fn create(&self, meta: &SstMeta) -> Result<ImportFile> {
        match self.dir.create(meta) {
            Ok(f) => {
                info!("create"; "file" => ?f);
                Ok(f)
            }
            Err(e) => {
                error!(%e; "create failed"; "meta" => ?meta,);
                Err(e)
            }
        }
    }

    pub fn delete(&self, meta: &SstMeta) -> Result<()> {
        match self.dir.delete(meta) {
            Ok(path) => {
                info!("delete"; "path" => ?path);
                Ok(())
            }
            Err(e) => {
                error!(%e; "delete failed"; "meta" => ?meta,);
                Err(e)
            }
        }
    }

    pub fn ingest<E: CausetEngine>(&self, meta: &SstMeta, engine: &E) -> Result<()> {
        match self.dir.ingest(meta, engine, self.key_manager.as_ref()) {
            Ok(_) => {
                info!("ingest"; "meta" => ?meta);
                Ok(())
            }
            Err(e) => {
                error!(%e; "ingest failed"; "meta" => ?meta,);
                Err(e)
            }
        }
    }

    // Downloads an SST file from an external causet_storage.
    //
    // This method is blocking. It performs the following transformations before
    // writing to disk:
    //
    //  1. only KV pairs in the *inclusive* cone (`[spacelike, lightlike]`) are used.
    //     (set the cone to `["", ""]` to import everything).
    //  2. tuplespaceInstanton are rewritten according to the given rewrite rule.
    //
    // Both the cone and rewrite tuplespaceInstanton are specified using origin tuplespaceInstanton. However,
    // the SST itself should be data tuplespaceInstanton (contain the `z` prefix). The cone
    // should be specified using tuplespaceInstanton after rewriting, to be consistent with the
    // brane info in FIDel.
    //
    // This method returns the *inclusive* key cone (`[spacelike, lightlike]`) of SST
    // file created, or returns None if the SST is empty.
    pub fn download<E: CausetEngine>(
        &self,
        meta: &SstMeta,
        backlightlike: &StorageBacklightlike,
        name: &str,
        rewrite_rule: &RewriteRule,
        speed_limiter: Limiter,
        sst_writer: E::SstWriter,
    ) -> Result<Option<Cone>> {
        debug!("download spacelike";
            "meta" => ?meta,
            "url" => ?backlightlike,
            "name" => name,
            "rewrite_rule" => ?rewrite_rule,
            "speed_limit" => speed_limiter.speed_limit(),
        );
        match self.do_download::<E>(meta, backlightlike, name, rewrite_rule, speed_limiter, sst_writer) {
            Ok(r) => {
                info!("download"; "meta" => ?meta, "name" => name, "cone" => ?r);
                Ok(r)
            }
            Err(e) => {
                error!(%e; "download failed"; "meta" => ?meta, "name" => name,);
                Err(e)
            }
        }
    }

    async fn read_external_causet_storage_into_file(
        input: &mut (dyn AsyncRead + Unpin),
        output: &mut dyn Write,
        speed_limiter: &Limiter,
        expected_length: u64,
        min_read_speed: usize,
    ) -> io::Result<()> {
        let dur = Duration::from_secs((READ_BUF_SIZE / min_read_speed) as u64);

        // do the I/O copy from external_causet_storage to the local file.
        let mut buffer = vec![0u8; READ_BUF_SIZE];
        let mut file_length = 0;

        loop {
            // separate the speed limiting from actual reading so it won't
            // affect the timeout calculation.
            let bytes_read = timeout(dur, input.read(&mut buffer))
                .await
                .map_err(|_| io::ErrorKind::TimedOut)??;
            if bytes_read == 0 {
                break;
            }
            speed_limiter.consume(bytes_read).await;
            output.write_all(&buffer[..bytes_read])?;
            file_length += bytes_read as u64;
        }

        if expected_length != 0 && expected_length != file_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "downloaded size {}, expected {}",
                    file_length, expected_length
                ),
            ));
        }

        IMPORTER_DOWNLOAD_BYTES.observe(file_length as _);

        Ok(())
    }

    fn do_download<E: CausetEngine>(
        &self,
        meta: &SstMeta,
        backlightlike: &StorageBacklightlike,
        name: &str,
        rewrite_rule: &RewriteRule,
        speed_limiter: Limiter,
        mut sst_writer: E::SstWriter,
    ) -> Result<Option<Cone>> {
        let spacelike = Instant::now();
        let path = self.dir.join(meta)?;
        let url = url_of_backlightlike(backlightlike);

        {
            // prepare to download the file from the external_causet_storage
            let ext_causet_storage = create_causet_storage(backlightlike)?;
            let mut ext_reader = ext_causet_storage.read(name);

            let mut plain_file;
            let mut encrypted_file;
            let file_writer: &mut dyn Write = if let Some(key_manager) = &self.key_manager {
                encrypted_file = key_manager.create_file(&path.temp)?;
                &mut encrypted_file
            } else {
                plain_file = File::create(&path.temp)?;
                &mut plain_file
            };

            // the minimum speed of reading data, in bytes/second.
            // if reading speed is slower than this rate, we will stop with
            // a "TimedOut" error.
            // (at 8 KB/s for a 2 MB buffer, this means we timeout after 4m16s.)
            const MINIMUM_READ_SPEED: usize = 8192;

            block_on_external_io(Self::read_external_causet_storage_into_file(
                &mut ext_reader,
                file_writer,
                &speed_limiter,
                meta.length,
                MINIMUM_READ_SPEED,
            ))
            .map_err(|e| {
                Error::CannotReadExternalStorage(
                    url.to_string(),
                    name.to_owned(),
                    path.temp.to_owned(),
                    e,
                )
            })?;

            OpenOptions::new()
                .applightlike(true)
                .open(&path.temp)?
                .sync_data()?;
        }

        // now validate the SST file.
        let path_str = path.temp.to_str().unwrap();
        let env = get_env(self.key_manager.clone(), None /*base_env*/)?;
        // Use abstracted SstReader after Env is abstracted.
        let sst_reader = LmdbSstReader::open_with_env(path_str, Some(env))?;
        sst_reader.verify_checksum()?;

        debug!("downloaded file and verified";
            "meta" => ?meta,
            "url" => %url,
            "name" => name,
            "path" => path_str,
        );

        // undo key rewrite so we could compare with the tuplespaceInstanton inside SST
        let old_prefix = rewrite_rule.get_old_key_prefix();
        let new_prefix = rewrite_rule.get_new_key_prefix();

        let cone_spacelike = meta.get_cone().get_spacelike();
        let cone_lightlike = meta.get_cone().get_lightlike();
        let cone_spacelike_bound = key_to_bound(cone_spacelike);
        let cone_lightlike_bound = if meta.get_lightlike_key_exclusive() {
            key_to_exclusive_bound(cone_lightlike)
        } else {
            key_to_bound(cone_lightlike)
        };

        let cone_spacelike =
            tuplespaceInstanton::rewrite::rewrite_prefix_of_spacelike_bound(new_prefix, old_prefix, cone_spacelike_bound)
                .map_err(|_| {
                    Error::WrongKeyPrefix(
                        "SST spacelike cone",
                        cone_spacelike.to_vec(),
                        new_prefix.to_vec(),
                    )
                })?;
        let cone_lightlike =
            tuplespaceInstanton::rewrite::rewrite_prefix_of_lightlike_bound(new_prefix, old_prefix, cone_lightlike_bound)
                .map_err(|_| {
                    Error::WrongKeyPrefix("SST lightlike cone", cone_lightlike.to_vec(), new_prefix.to_vec())
                })?;

        // read the first and last tuplespaceInstanton from the SST, determine if we could
        // simply move the entire SST instead of iterating and generate a new one.
        let mut iter = sst_reader.iter();
        let direct_retval = (|| -> Result<Option<_>> {
            if rewrite_rule.old_key_prefix != rewrite_rule.new_key_prefix
                || rewrite_rule.new_timestamp != 0
            {
                // must iterate if we perform key rewrite
                return Ok(None);
            }
            if !iter.seek(SeekKey::Start)? {
                // the SST is empty, so no need to iterate at all (should be impossible?)
                return Ok(Some(meta.get_cone().clone()));
            }
            let spacelike_key = tuplespaceInstanton::origin_key(iter.key());
            if is_before_spacelike_bound(spacelike_key, &cone_spacelike) {
                // SST's spacelike is before the cone to consume, so needs to iterate to skip over
                return Ok(None);
            }
            let spacelike_key = spacelike_key.to_vec();

            // seek to lightlike and fetch the last (inclusive) key of the SST.
            iter.seek(SeekKey::End)?;
            let last_key = tuplespaceInstanton::origin_key(iter.key());
            if is_after_lightlike_bound(last_key, &cone_lightlike) {
                // SST's lightlike is after the cone to consume
                return Ok(None);
            }

            // cone contained the entire SST, no need to iterate, just moving the file is ok
            let mut cone = Cone::default();
            cone.set_spacelike(spacelike_key);
            cone.set_lightlike(last_key.to_vec());
            Ok(Some(cone))
        })()?;

        if let Some(cone) = direct_retval {
            // TODO: what about encrypted SSTs?
            fs::rename(&path.temp, &path.save)?;
            if let Some(key_manager) = &self.key_manager {
                let temp_str = path
                    .temp
                    .to_str()
                    .ok_or_else(|| Error::InvalidSSTPath(path.temp.clone()))?;
                let save_str = path
                    .save
                    .to_str()
                    .ok_or_else(|| Error::InvalidSSTPath(path.save.clone()))?;
                key_manager.rename_file(temp_str, save_str)?;
            }
            let duration = spacelike.elapsed();
            IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["rename"])
                .observe(duration.as_secs_f64());
            return Ok(Some(cone));
        }

        // perform iteration and key rewrite.
        let mut key = tuplespaceInstanton::data_key(new_prefix);
        let new_prefix_data_key_len = key.len();
        let mut first_key = None;

        match cone_spacelike {
            Bound::Unbounded => iter.seek(SeekKey::Start)?,
            Bound::Included(s) => iter.seek(SeekKey::Key(&tuplespaceInstanton::data_key(&s)))?,
            Bound::Excluded(_) => unreachable!(),
        };
        while iter.valid()? {
            let old_key = tuplespaceInstanton::origin_key(iter.key());
            if is_after_lightlike_bound(&old_key, &cone_lightlike) {
                break;
            }
            if !old_key.spacelikes_with(old_prefix) {
                return Err(Error::WrongKeyPrefix(
                    "Key in SST",
                    tuplespaceInstanton::origin_key(iter.key()).to_vec(),
                    old_prefix.to_vec(),
                ));
            }
            key.truncate(new_prefix_data_key_len);
            key.extlightlike_from_slice(&old_key[old_prefix.len()..]);
            let mut value = Cow::Borrowed(iter.value());

            if rewrite_rule.new_timestamp != 0 {
                key = Key::from_encoded(key)
                    .truncate_ts()
                    .map_err(|e| {
                        Error::BadFormat(format!(
                            "key {}: {}",
                            hex::encode_upper(tuplespaceInstanton::origin_key(iter.key()).to_vec()),
                            e
                        ))
                    })?
                    .applightlike_ts(TimeStamp::new(rewrite_rule.new_timestamp))
                    .into_encoded();
                if meta.get_causet_name() == Causet_WRITE {
                    let mut write = WriteRef::parse(iter.value()).map_err(|e| {
                        Error::BadFormat(format!(
                            "write {}: {}",
                            hex::encode_upper(tuplespaceInstanton::origin_key(iter.key()).to_vec()),
                            e
                        ))
                    })?;
                    write.spacelike_ts = TimeStamp::new(rewrite_rule.new_timestamp);
                    value = Cow::Owned(write.to_bytes());
                }
            }

            sst_writer.put(&key, &value)?;
            iter.next()?;
            if first_key.is_none() {
                first_key = Some(tuplespaceInstanton::origin_key(&key).to_vec());
            }
        }

        let _ = fs::remove_file(&path.temp);

        let duration = spacelike.elapsed();
        IMPORTER_DOWNLOAD_DURATION
            .with_label_values(&["rewrite"])
            .observe(duration.as_secs_f64());

        if let Some(spacelike_key) = first_key {
            sst_writer.finish()?;
            let mut final_cone = Cone::default();
            final_cone.set_spacelike(spacelike_key);
            final_cone.set_lightlike(tuplespaceInstanton::origin_key(&key).to_vec());
            Ok(Some(final_cone))
        } else {
            // nothing is written: prevents finishing the SST at all.
            Ok(None)
        }
    }

    pub fn list_ssts(&self) -> Result<Vec<SstMeta>> {
        self.dir.list_ssts()
    }

    pub fn new_writer<E: CausetEngine>(
        &self,
        default: E::SstWriter,
        write: E::SstWriter,
        meta: SstMeta,
    ) -> Result<SSTWriter<E>> {
        let mut default_meta = meta.clone();
        default_meta.set_causet_name(Causet_DEFAULT.to_owned());
        let default_path = self.dir.join(&default_meta)?;

        let mut write_meta = meta;
        write_meta.set_causet_name(Causet_WRITE.to_owned());
        let write_path = self.dir.join(&write_meta)?;
        Ok(SSTWriter::new(
            default,
            write,
            default_path,
            write_path,
            default_meta,
            write_meta,
        ))
    }
}

pub struct SSTWriter<E: CausetEngine> {
    default: E::SstWriter,
    default_entries: u64,
    default_path: ImportPath,
    default_meta: SstMeta,
    write: E::SstWriter,
    write_entries: u64,
    write_path: ImportPath,
    write_meta: SstMeta,
}

impl<E: CausetEngine> SSTWriter<E> {
    pub fn new(
        default: E::SstWriter,
        write: E::SstWriter,
        default_path: ImportPath,
        write_path: ImportPath,
        default_meta: SstMeta,
        write_meta: SstMeta,
    ) -> Self {
        SSTWriter {
            default,
            default_path,
            default_entries: 0,
            default_meta,
            write,
            write_path,
            write_entries: 0,
            write_meta,
        }
    }

    pub fn write(&mut self, batch: WriteBatch) -> Result<()> {
        let commit_ts = TimeStamp::new(batch.get_commit_ts());
        for m in batch.get_pairs().iter() {
            let k = Key::from_raw(m.get_key()).applightlike_ts(commit_ts);
            self.put(k.as_encoded(), m.get_value(), m.get_op())?;
        }
        Ok(())
    }

    fn put(&mut self, key: &[u8], value: &[u8], op: PairOp) -> Result<()> {
        let k = tuplespaceInstanton::data_key(key);
        let (_, commit_ts) = Key::split_on_ts_for(key)?;
        let w = match (op, is_short_value(value)) {
            (PairOp::Delete, _) => KvWrite::new(WriteType::Delete, commit_ts, None),
            (PairOp::Put, true) => KvWrite::new(WriteType::Put, commit_ts, Some(value.to_vec())),
            (PairOp::Put, false) => {
                self.default.put(&k, value)?;
                self.default_entries += 1;
                KvWrite::new(WriteType::Put, commit_ts, None)
            }
        };
        self.write.put(&k, &w.as_ref().to_bytes())?;
        self.write_entries += 1;
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<SstMeta>> {
        let default_meta = self.default_meta.clone();
        let write_meta = self.write_meta.clone();
        let mut metas = Vec::with_capacity(2);
        let (default_entries, write_entries) = (self.default_entries, self.write_entries);
        let (p1, p2) = (self.default_path.clone(), self.write_path.clone());
        let (w1, w2) = (self.default, self.write);
        if default_entries > 0 {
            let (_, sst_reader) = w1.finish_read()?;
            Self::save(sst_reader, p1)?;
            metas.push(default_meta);
        }
        if write_entries > 0 {
            let (_, sst_reader) = w2.finish_read()?;
            Self::save(sst_reader, p2)?;
            metas.push(write_meta);
        }
        info!("finish write to sst";
            "default entries" => default_entries,
            "write entries" => write_entries,
        );
        Ok(metas)
    }

    fn save(
        mut sst_reader: <<E as SstExt>::SstWriter as SstWriter>::ExternalSstFileReader,
        import_path: ImportPath,
    ) -> Result<()> {
        let tmp_path = import_path.temp;
        let mut tmp_f = File::create(&tmp_path)?;
        std::io::copy(&mut sst_reader, &mut tmp_f)?;
        tmp_f.metadata()?.permissions().set_readonly(true);
        tmp_f.sync_all()?;
        fs::rename(tmp_path, import_path.save)?;
        Ok(())
    }
}

/// ImportDir is responsible for operating SST files and related path
/// calculations.
///
/// The file being written is stored in `$root/.temp/$file_name`. After writing
/// is completed, the file is moved to `$root/$file_name`. The file generated
/// from the ingestion process will be placed in `$root/.clone/$file_name`.
///
/// TODO: Add size and rate limit.
pub struct ImportDir {
    root_dir: PathBuf,
    temp_dir: PathBuf,
    clone_dir: PathBuf,
}

impl ImportDir {
    const TEMP_DIR: &'static str = ".temp";
    const CLONE_DIR: &'static str = ".clone";

    fn new<P: AsRef<Path>>(root: P) -> Result<ImportDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        let clone_dir = root_dir.join(Self::CLONE_DIR);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir)?;
        }
        if clone_dir.exists() {
            fs::remove_dir_all(&clone_dir)?;
        }
        fs::create_dir_all(&temp_dir)?;
        fs::create_dir_all(&clone_dir)?;
        Ok(ImportDir {
            root_dir,
            temp_dir,
            clone_dir,
        })
    }

    fn join(&self, meta: &SstMeta) -> Result<ImportPath> {
        let file_name = sst_meta_to_path(meta)?;
        let save_path = self.root_dir.join(&file_name);
        let temp_path = self.temp_dir.join(&file_name);
        let clone_path = self.clone_dir.join(&file_name);
        Ok(ImportPath {
            save: save_path,
            temp: temp_path,
            clone: clone_path,
        })
    }

    fn create(&self, meta: &SstMeta) -> Result<ImportFile> {
        let path = self.join(meta)?;
        if path.save.exists() {
            return Err(Error::FileExists(path.save));
        }
        ImportFile::create(meta.clone(), path)
    }

    fn delete(&self, meta: &SstMeta) -> Result<ImportPath> {
        let path = self.join(meta)?;
        if path.save.exists() {
            fs::remove_file(&path.save)?;
        }
        if path.temp.exists() {
            fs::remove_file(&path.temp)?;
        }
        if path.clone.exists() {
            fs::remove_file(&path.clone)?;
        }
        Ok(path)
    }

    fn ingest<E: CausetEngine>(
        &self,
        meta: &SstMeta,
        engine: &E,
        key_manager: Option<&Arc<DataKeyManager>>,
    ) -> Result<()> {
        let spacelike = Instant::now();
        let path = self.join(meta)?;
        let causet = meta.get_causet_name();
        let causet = engine.causet_handle(causet).expect("bad causet name");
        super::prepare_sst_for_ingestion(&path.save, &path.clone, key_manager)?;
        let length = meta.get_length();
        let crc32 = meta.get_crc32();
        // FIXME perform validate_sst_for_ingestion after we can handle sst file size correctly.
        // currently we can not handle sst file size after rewrite,
        // we need re-compute length & crc32 and fill back to sstMeta.
        if length != 0 && crc32 != 0 {
            // we only validate if the length and CRC32 are explicitly provided.
            engine.validate_sst_for_ingestion(causet, &path.clone, length, crc32)?;
            IMPORTER_INGEST_BYTES.observe(length as _)
        } else {
            debug!("skipping SST validation since length and crc32 are both 0");
        }

        let mut opts = E::IngestExternalFileOptions::new();
        opts.move_files(true);
        engine.ingest_external_file_causet(causet, &opts, &[path.clone.to_str().unwrap()])?;
        IMPORTER_INGEST_DURATION
            .with_label_values(&["ingest"])
            .observe(spacelike.elapsed().as_secs_f64());
        Ok(())
    }

    fn list_ssts(&self) -> Result<Vec<SstMeta>> {
        let mut ssts = Vec::new();
        for e in fs::read_dir(&self.root_dir)? {
            let e = e?;
            if !e.file_type()?.is_file() {
                continue;
            }
            let path = e.path();
            match path_to_sst_meta(&path) {
                Ok(sst) => ssts.push(sst),
                Err(e) => error!(%e; "path_to_sst_meta failed"; "path" => %path.to_str().unwrap(),),
            }
        }
        Ok(ssts)
    }
}

#[derive(Clone)]
pub struct ImportPath {
    // The path of the file that has been uploaded.
    save: PathBuf,
    // The path of the file that is being uploaded.
    temp: PathBuf,
    // The path of the file that is going to be ingested.
    clone: PathBuf,
}

impl fmt::Debug for ImportPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportPath")
            .field("save", &self.save)
            .field("temp", &self.temp)
            .field("clone", &self.clone)
            .finish()
    }
}

/// ImportFile is used to handle the writing and verification of SST files.
pub struct ImportFile {
    meta: SstMeta,
    path: ImportPath,
    file: Option<File>,
    digest: crc32fast::Hasher,
}

impl ImportFile {
    fn create(meta: SstMeta, path: ImportPath) -> Result<ImportFile> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path.temp)?;
        Ok(ImportFile {
            meta,
            path,
            file: Some(file),
            digest: crc32fast::Hasher::new(),
        })
    }

    pub fn applightlike(&mut self, data: &[u8]) -> Result<()> {
        self.file.as_mut().unwrap().write_all(data)?;
        self.digest.fidelio(data);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.validate()?;
        self.file.take().unwrap().sync_all()?;
        if self.path.save.exists() {
            return Err(Error::FileExists(self.path.save.clone()));
        }
        fs::rename(&self.path.temp, &self.path.save)?;
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        self.file.take();
        if self.path.temp.exists() {
            fs::remove_file(&self.path.temp)?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        let crc32 = self.digest.clone().finalize();
        let expect = self.meta.get_crc32();
        if crc32 != expect {
            let reason = format!("crc32 {}, expect {}", crc32, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }

        let f = self.file.as_ref().unwrap();
        let length = f.metadata()?.len();
        let expect = self.meta.get_length();
        if length != expect {
            let reason = format!("length {}, expect {}", length, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }
        Ok(())
    }
}

impl Drop for ImportFile {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            warn!("cleanup failed"; "file" => ?self, "err" => %e);
        }
    }
}

impl fmt::Debug for ImportFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportFile")
            .field("meta", &self.meta)
            .field("path", &self.path)
            .finish()
    }
}

const SST_SUFFIX: &str = ".sst";

fn sst_meta_to_path(meta: &SstMeta) -> Result<PathBuf> {
    Ok(PathBuf::from(format!(
        "{}_{}_{}_{}_{}{}",
        UuidBuilder::from_slice(meta.get_uuid())?.build(),
        meta.get_brane_id(),
        meta.get_brane_epoch().get_conf_ver(),
        meta.get_brane_epoch().get_version(),
        meta.get_causet_name(),
        SST_SUFFIX,
    )))
}

fn path_to_sst_meta<P: AsRef<Path>>(path: P) -> Result<SstMeta> {
    let path = path.as_ref();
    let file_name = match path.file_name().and_then(|n| n.to_str()) {
        Some(name) => name,
        None => return Err(Error::InvalidSSTPath(path.to_owned())),
    };

    // A valid file name should be in the format:
    // "{uuid}_{brane_id}_{brane_epoch.conf_ver}_{brane_epoch.version}_{causet}.sst"
    if !file_name.lightlikes_with(SST_SUFFIX) {
        return Err(Error::InvalidSSTPath(path.to_owned()));
    }
    let elems: Vec<_> = file_name.trim_lightlike_matches(SST_SUFFIX).split('_').collect();
    if elems.len() != 5 {
        return Err(Error::InvalidSSTPath(path.to_owned()));
    }

    let mut meta = SstMeta::default();
    let uuid = Uuid::parse_str(elems[0])?;
    meta.set_uuid(uuid.as_bytes().to_vec());
    meta.set_brane_id(elems[1].parse()?);
    meta.mut_brane_epoch().set_conf_ver(elems[2].parse()?);
    meta.mut_brane_epoch().set_version(elems[3].parse()?);
    meta.set_causet_name(elems[4].to_owned());
    Ok(meta)
}

fn key_to_bound(key: &[u8]) -> Bound<&[u8]> {
    if key.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Included(key)
    }
}

fn key_to_exclusive_bound(key: &[u8]) -> Bound<&[u8]> {
    if key.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Excluded(key)
    }
}

fn is_before_spacelike_bound<K: AsRef<[u8]>>(value: &[u8], bound: &Bound<K>) -> bool {
    match bound {
        Bound::Unbounded => false,
        Bound::Included(b) => *value < *b.as_ref(),
        Bound::Excluded(b) => *value <= *b.as_ref(),
    }
}

fn is_after_lightlike_bound<K: AsRef<[u8]>>(value: &[u8], bound: &Bound<K>) -> bool {
    match bound {
        Bound::Unbounded => false,
        Bound::Included(b) => *value > *b.as_ref(),
        Bound::Excluded(b) => *value >= *b.as_ref(),
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use test_sst_importer::*;

    use std::f64::INFINITY;

    use edb::{collect, name_to_causet, Iterable, Iteron, SeekKey, Causet_DEFAULT, DATA_CausetS};
    use edb::{Error as TraitError, SstWriterBuilder, BlockPropertiesExt};
    use edb::{
        ExternalSstFileInfo, SstExt, BlockProperties, BlockPropertiesCollection,
        UserCollectedProperties,
    };
    use tempfile::Builder;
    use test_sst_importer::{
        new_sst_reader, new_sst_writer, new_test_engine, LmdbSstWriter, PROP_TEST_MARKER_Causet_NAME,
    };
    use txn_types::{Value, WriteType};

    #[test]
    fn test_import_dir() {
        let temp_dir = Builder::new().prefix("test_import_dir").temfidelir().unwrap();
        let dir = ImportDir::new(temp_dir.path()).unwrap();

        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let path = dir.join(&meta).unwrap();

        // Test ImportDir::create()
        {
            let _file = dir.create(&meta).unwrap();
            assert!(path.temp.exists());
            assert!(!path.save.exists());
            assert!(!path.clone.exists());
            // Cannot create the same file again.
            assert!(dir.create(&meta).is_err());
        }

        // Test ImportDir::delete()
        {
            File::create(&path.temp).unwrap();
            File::create(&path.save).unwrap();
            File::create(&path.clone).unwrap();
            dir.delete(&meta).unwrap();
            assert!(!path.temp.exists());
            assert!(!path.save.exists());
            assert!(!path.clone.exists());
        }

        // Test ImportDir::ingest()

        let db_path = temp_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), &[Causet_DEFAULT]);

        let cases = vec![(0, 10), (5, 15), (10, 20), (0, 100)];

        let mut ingested = Vec::new();

        for (i, &cone) in cases.iter().enumerate() {
            let path = temp_dir.path().join(format!("{}.sst", i));
            let (meta, data) = gen_sst_file(&path, cone);

            let mut f = dir.create(&meta).unwrap();
            f.applightlike(&data).unwrap();
            f.finish().unwrap();

            dir.ingest(&meta, &db, None).unwrap();
            check_db_cone(&db, cone);

            ingested.push(meta);
        }

        let ssts = dir.list_ssts().unwrap();
        assert_eq!(ssts.len(), ingested.len());
        for sst in &ssts {
            ingested
                .iter()
                .find(|s| s.get_uuid() == sst.get_uuid())
                .unwrap();
            dir.delete(sst).unwrap();
        }
        assert!(dir.list_ssts().unwrap().is_empty());
    }

    #[test]
    fn test_import_file() {
        let temp_dir = Builder::new().prefix("test_import_file").temfidelir().unwrap();

        let path = ImportPath {
            save: temp_dir.path().join("save"),
            temp: temp_dir.path().join("temp"),
            clone: temp_dir.path().join("clone"),
        };

        let data = b"test_data";
        let crc32 = calc_data_crc32(data);

        let mut meta = SstMeta::default();

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            // Cannot create the same file again.
            assert!(ImportFile::create(meta.clone(), path.clone()).is_err());
            f.applightlike(data).unwrap();
            // Invalid crc32 and length.
            assert!(f.finish().is_err());
            assert!(path.temp.exists());
            assert!(!path.save.exists());
        }

        meta.set_crc32(crc32);

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            f.applightlike(data).unwrap();
            // Invalid length.
            assert!(f.finish().is_err());
        }

        meta.set_length(data.len() as u64);

        {
            let mut f = ImportFile::create(meta, path.clone()).unwrap();
            f.applightlike(data).unwrap();
            f.finish().unwrap();
            assert!(!path.temp.exists());
            assert!(path.save.exists());
        }
    }

    #[test]
    fn test_sst_meta_to_path() {
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_brane_id(1);
        meta.set_causet_name(Causet_DEFAULT.to_owned());
        meta.mut_brane_epoch().set_conf_ver(2);
        meta.mut_brane_epoch().set_version(3);

        let path = sst_meta_to_path(&meta).unwrap();
        let expected_path = format!("{}_1_2_3_default.sst", uuid);
        assert_eq!(path.to_str().unwrap(), &expected_path);

        let new_meta = path_to_sst_meta(path).unwrap();
        assert_eq!(meta, new_meta);
    }

    fn create_external_sst_file_with_write_fn<F>(
        write_fn: F,
    ) -> Result<(tempfile::TempDir, StorageBacklightlike, SstMeta)>
    where
        F: FnOnce(&mut LmdbSstWriter) -> Result<()>,
    {
        let ext_sst_dir = tempfile::temfidelir()?;
        let mut sst_writer =
            new_sst_writer(ext_sst_dir.path().join("sample.sst").to_str().unwrap());
        write_fn(&mut sst_writer)?;
        let sst_info = sst_writer.finish()?;

        // make up the SST meta for downloading.
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_causet_name(Causet_DEFAULT.to_owned());
        meta.set_length(sst_info.file_size());
        meta.set_brane_id(4);
        meta.mut_brane_epoch().set_conf_ver(5);
        meta.mut_brane_epoch().set_version(6);

        let backlightlike = external_causet_storage::make_local_backlightlike(ext_sst_dir.path());
        Ok((ext_sst_dir, backlightlike, meta))
    }

    fn create_sample_external_sst_file() -> Result<(tempfile::TempDir, StorageBacklightlike, SstMeta)> {
        create_external_sst_file_with_write_fn(|writer| {
            writer.put(b"zt123_r01", b"abc")?;
            writer.put(b"zt123_r04", b"xyz")?;
            writer.put(b"zt123_r07", b"pqrst")?;
            // writer.delete(b"t123_r10")?; // FIXME: can't handle DELETE ops yet.
            writer.put(b"zt123_r13", b"www")?;
            Ok(())
        })
    }

    fn create_sample_external_rawkv_sst_file(
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        lightlike_key_exclusive: bool,
    ) -> Result<(tempfile::TempDir, StorageBacklightlike, SstMeta)> {
        let (dir, backlightlike, mut meta) = create_external_sst_file_with_write_fn(|writer| {
            writer.put(b"za", b"v1")?;
            writer.put(b"zb", b"v2")?;
            writer.put(b"zb\x00", b"v3")?;
            writer.put(b"zc", b"v4")?;
            writer.put(b"zc\x00", b"v5")?;
            writer.put(b"zc\x00\x00", b"v6")?;
            writer.put(b"zd", b"v7")?;
            Ok(())
        })?;
        meta.mut_cone().set_spacelike(spacelike_key.to_vec());
        meta.mut_cone().set_lightlike(lightlike_key.to_vec());
        meta.set_lightlike_key_exclusive(lightlike_key_exclusive);
        Ok((dir, backlightlike, meta))
    }

    fn get_encoded_key(key: &[u8], ts: u64) -> Vec<u8> {
        tuplespaceInstanton::data_key(
            txn_types::Key::from_raw(key)
                .applightlike_ts(TimeStamp::new(ts))
                .as_encoded(),
        )
    }

    fn get_write_value(
        write_type: WriteType,
        spacelike_ts: u64,
        short_value: Option<Value>,
    ) -> Vec<u8> {
        txn_types::Write::new(write_type, TimeStamp::new(spacelike_ts), short_value)
            .as_ref()
            .to_bytes()
    }

    fn create_sample_external_sst_file_txn_default(
    ) -> Result<(tempfile::TempDir, StorageBacklightlike, SstMeta)> {
        let ext_sst_dir = tempfile::temfidelir()?;
        let mut sst_writer = new_sst_writer(
            ext_sst_dir
                .path()
                .join("sample_default.sst")
                .to_str()
                .unwrap(),
        );
        sst_writer.put(&get_encoded_key(b"t123_r01", 1), b"abc")?;
        sst_writer.put(&get_encoded_key(b"t123_r04", 3), b"xyz")?;
        sst_writer.put(&get_encoded_key(b"t123_r07", 7), b"pqrst")?;
        // sst_writer.delete(b"t123_r10")?; // FIXME: can't handle DELETE ops yet.
        let sst_info = sst_writer.finish()?;

        // make up the SST meta for downloading.
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_causet_name(Causet_DEFAULT.to_owned());
        meta.set_length(sst_info.file_size());
        meta.set_brane_id(4);
        meta.mut_brane_epoch().set_conf_ver(5);
        meta.mut_brane_epoch().set_version(6);

        let backlightlike = external_causet_storage::make_local_backlightlike(ext_sst_dir.path());
        Ok((ext_sst_dir, backlightlike, meta))
    }

    fn create_sample_external_sst_file_txn_write(
    ) -> Result<(tempfile::TempDir, StorageBacklightlike, SstMeta)> {
        let ext_sst_dir = tempfile::temfidelir()?;
        let mut sst_writer = new_sst_writer(
            ext_sst_dir
                .path()
                .join("sample_write.sst")
                .to_str()
                .unwrap(),
        );
        sst_writer.put(
            &get_encoded_key(b"t123_r01", 5),
            &get_write_value(WriteType::Put, 1, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r02", 5),
            &get_write_value(WriteType::Delete, 1, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r04", 4),
            &get_write_value(WriteType::Put, 3, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r07", 8),
            &get_write_value(WriteType::Put, 7, None),
        )?;
        sst_writer.put(
            &get_encoded_key(b"t123_r13", 8),
            &get_write_value(WriteType::Put, 7, Some(b"www".to_vec())),
        )?;
        let sst_info = sst_writer.finish()?;

        // make up the SST meta for downloading.
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_causet_name(Causet_WRITE.to_owned());
        meta.set_length(sst_info.file_size());
        meta.set_brane_id(4);
        meta.mut_brane_epoch().set_conf_ver(5);
        meta.mut_brane_epoch().set_version(6);

        let backlightlike = external_causet_storage::make_local_backlightlike(ext_sst_dir.path());
        Ok((ext_sst_dir, backlightlike, meta))
    }

    fn new_rewrite_rule(
        old_key_prefix: &[u8],
        new_key_prefix: &[u8],
        new_timestamp: u64,
    ) -> RewriteRule {
        let mut rule = RewriteRule::default();
        rule.set_old_key_prefix(old_key_prefix.to_vec());
        rule.set_new_key_prefix(new_key_prefix.to_vec());
        rule.set_new_timestamp(new_timestamp);
        rule
    }

    fn create_sst_writer_with_db(
        importer: &SSTImporter,
        meta: &SstMeta,
    ) -> Result<<TestEngine as SstExt>::SstWriter> {
        let temp_dir = Builder::new().prefix("test_import_dir").temfidelir().unwrap();
        let db_path = temp_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CausetS);
        let sst_writer = <TestEngine as SstExt>::SstWriterBuilder::new()
            .set_db(&db)
            .set_causet(name_to_causet(meta.get_causet_name()).unwrap())
            .build(importer.get_path(meta).to_str().unwrap())
            .unwrap();
        Ok(sst_writer)
    }

    #[test]
    fn test_read_external_causet_storage_into_file() {
        let data = &b"some input data"[..];
        let mut input = data;
        let mut output = Vec::new();
        let input_len = input.len() as u64;
        block_on_external_io(SSTImporter::read_external_causet_storage_into_file(
            &mut input,
            &mut output,
            &Limiter::new(INFINITY),
            input_len,
            8192,
        ))
        .unwrap();
        assert_eq!(&*output, data);
    }

    #[test]
    fn test_read_external_causet_storage_into_file_timed_out() {
        use futures_util::stream::{plightlikeing, TryStreamExt};

        let mut input = plightlikeing::<io::Result<&[u8]>>().into_async_read();
        let mut output = Vec::new();
        let err = block_on_external_io(SSTImporter::read_external_causet_storage_into_file(
            &mut input,
            &mut output,
            &Limiter::new(INFINITY),
            0,
            usize::MAX,
        ))
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn test_download_sst_no_key_rewrite() {
        // creates a sample SST file.
        let (_ext_sst_dir, backlightlike, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let cone = importer
            .download::<TestEngine>(
                &meta,
                &backlightlike,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(cone.get_spacelike(), b"t123_r01");
        assert_eq!(cone.get_lightlike(), b"t123_r13");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());
        assert_eq!(sst_file_metadata.len(), meta.get_length());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zt123_r01".to_vec(), b"abc".to_vec()),
                (b"zt123_r04".to_vec(), b"xyz".to_vec()),
                (b"zt123_r07".to_vec(), b"pqrst".to_vec()),
                (b"zt123_r13".to_vec(), b"www".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_with_key_rewrite() {
        // creates a sample SST file.
        let (_ext_sst_dir, backlightlike, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let cone = importer
            .download::<TestEngine>(
                &meta,
                &backlightlike,
                "sample.sst",
                &new_rewrite_rule(b"t123", b"t567", 0),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(cone.get_spacelike(), b"t567_r01");
        assert_eq!(cone.get_lightlike(), b"t567_r13");

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zt567_r01".to_vec(), b"abc".to_vec()),
                (b"zt567_r04".to_vec(), b"xyz".to_vec()),
                (b"zt567_r07".to_vec(), b"pqrst".to_vec()),
                (b"zt567_r13".to_vec(), b"www".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_with_key_rewrite_ts_default() {
        // performs the download.
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();

        // creates a sample SST file.
        let (_ext_sst_dir, backlightlike, meta) = create_sample_external_sst_file_txn_default().unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let _ = importer
            .download::<TestEngine>(
                &meta,
                &backlightlike,
                "sample_default.sst",
                &new_rewrite_rule(b"", b"", 16),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (get_encoded_key(b"t123_r01", 16), b"abc".to_vec()),
                (get_encoded_key(b"t123_r04", 16), b"xyz".to_vec()),
                (get_encoded_key(b"t123_r07", 16), b"pqrst".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_with_key_rewrite_ts_write() {
        // performs the download.
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();

        // creates a sample SST file.
        let (_ext_sst_dir, backlightlike, meta) = create_sample_external_sst_file_txn_write().unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let _ = importer
            .download::<TestEngine>(
                &meta,
                &backlightlike,
                "sample_write.sst",
                &new_rewrite_rule(b"", b"", 16),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (
                    get_encoded_key(b"t123_r01", 16),
                    get_write_value(WriteType::Put, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r02", 16),
                    get_write_value(WriteType::Delete, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r04", 16),
                    get_write_value(WriteType::Put, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r07", 16),
                    get_write_value(WriteType::Put, 16, None)
                ),
                (
                    get_encoded_key(b"t123_r13", 16),
                    get_write_value(WriteType::Put, 16, Some(b"www".to_vec()))
                ),
            ]
        );
    }

    #[test]
    fn test_download_sst_then_ingest() {
        for causet in &[Causet_DEFAULT, Causet_WRITE] {
            // creates a sample SST file.
            let (_ext_sst_dir, backlightlike, mut meta) = create_sample_external_sst_file().unwrap();
            meta.set_causet_name((*causet).to_string());

            // performs the download.
            let importer_dir = tempfile::temfidelir().unwrap();
            let importer = SSTImporter::new(&importer_dir, None).unwrap();
            let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

            let cone = importer
                .download::<TestEngine>(
                    &meta,
                    &backlightlike,
                    "sample.sst",
                    &new_rewrite_rule(b"t123", b"t9102", 0),
                    Limiter::new(INFINITY),
                    sst_writer,
                )
                .unwrap()
                .unwrap();

            assert_eq!(cone.get_spacelike(), b"t9102_r01");
            assert_eq!(cone.get_lightlike(), b"t9102_r13");

            // performs the ingest
            let ingest_dir = tempfile::temfidelir().unwrap();
            let db = new_test_engine(ingest_dir.path().to_str().unwrap(), DATA_CausetS);

            meta.set_length(0); // disable validation.
            meta.set_crc32(0);
            importer.ingest(&meta, &db).unwrap();

            // verifies the DB content is correct.
            let mut iter = db.Iteron_causet(causet).unwrap();
            iter.seek(SeekKey::Start).unwrap();
            assert_eq!(
                collect(iter),
                vec![
                    (b"zt9102_r01".to_vec(), b"abc".to_vec()),
                    (b"zt9102_r04".to_vec(), b"xyz".to_vec()),
                    (b"zt9102_r07".to_vec(), b"pqrst".to_vec()),
                    (b"zt9102_r13".to_vec(), b"www".to_vec()),
                ]
            );

            // check properties
            let spacelike = tuplespaceInstanton::data_key(b"");
            let lightlike = tuplespaceInstanton::data_lightlike_key(b"");
            let collection = db.get_cone_properties_causet(causet, &spacelike, &lightlike).unwrap();
            assert!(!collection.is_empty());
            for (_, v) in collection.iter() {
                assert!(!v.user_collected_properties().is_empty());
                assert_eq!(
                    v.user_collected_properties()
                        .get(PROP_TEST_MARKER_Causet_NAME)
                        .unwrap(),
                    causet.as_bytes()
                );
            }
        }
    }

    #[test]
    fn test_download_sst_partial_cone() {
        let (_ext_sst_dir, backlightlike, mut meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();
        // note: the cone doesn't contain the DATA_PREFIX 'z'.
        meta.mut_cone().set_spacelike(b"t123_r02".to_vec());
        meta.mut_cone().set_lightlike(b"t123_r12".to_vec());

        let cone = importer
            .download::<TestEngine>(
                &meta,
                &backlightlike,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(cone.get_spacelike(), b"t123_r04");
        assert_eq!(cone.get_lightlike(), b"t123_r07");

        // verifies that the file is saved to the correct place.
        // (the file size is changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zt123_r04".to_vec(), b"xyz".to_vec()),
                (b"zt123_r07".to_vec(), b"pqrst".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_partial_cone_with_key_rewrite() {
        let (_ext_sst_dir, backlightlike, mut meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();
        meta.mut_cone().set_spacelike(b"t5_r02".to_vec());
        meta.mut_cone().set_lightlike(b"t5_r12".to_vec());

        let cone = importer
            .download::<TestEngine>(
                &meta,
                &backlightlike,
                "sample.sst",
                &new_rewrite_rule(b"t123", b"t5", 0),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(cone.get_spacelike(), b"t5_r04");
        assert_eq!(cone.get_lightlike(), b"t5_r07");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zt5_r04".to_vec(), b"xyz".to_vec()),
                (b"zt5_r07".to_vec(), b"pqrst".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_sst_invalid() {
        let ext_sst_dir = tempfile::temfidelir().unwrap();
        fs::write(ext_sst_dir.path().join("sample.sst"), b"not an SST file").unwrap();
        let mut meta = SstMeta::default();
        meta.set_uuid(vec![0u8; 16]);
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();
        let backlightlike = external_causet_storage::make_local_backlightlike(ext_sst_dir.path());

        let result = importer.download::<TestEngine>(
            &meta,
            &backlightlike,
            "sample.sst",
            &RewriteRule::default(),
            Limiter::new(INFINITY),
            sst_writer,
        );
        match &result {
            Err(Error::EnginePromises(TraitError::Engine(msg))) if msg.spacelikes_with("Corruption:") => {
            }
            _ => panic!("unexpected download result: {:?}", result),
        }
    }

    #[test]
    fn test_download_sst_empty() {
        let (_ext_sst_dir, backlightlike, mut meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();
        meta.mut_cone().set_spacelike(vec![b'x']);
        meta.mut_cone().set_lightlike(vec![b'y']);

        let result = importer.download::<TestEngine>(
            &meta,
            &backlightlike,
            "sample.sst",
            &RewriteRule::default(),
            Limiter::new(INFINITY),
            sst_writer,
        );

        match result {
            Ok(None) => {}
            _ => panic!("unexpected download result: {:?}", result),
        }
    }

    #[test]
    fn test_download_sst_wrong_key_prefix() {
        let (_ext_sst_dir, backlightlike, meta) = create_sample_external_sst_file().unwrap();
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let result = importer.download::<TestEngine>(
            &meta,
            &backlightlike,
            "sample.sst",
            &new_rewrite_rule(b"xxx", b"yyy", 0),
            Limiter::new(INFINITY),
            sst_writer,
        );

        match &result {
            Err(Error::WrongKeyPrefix(_, key, prefix)) => {
                assert_eq!(key, b"t123_r01");
                assert_eq!(prefix, b"xxx");
            }
            _ => panic!("unexpected download result: {:?}", result),
        }
    }

    #[test]
    fn test_write_sst() {
        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let name = importer.get_path(&meta);
        let db_path = importer_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CausetS);
        let default = <TestEngine as SstExt>::SstWriterBuilder::new()
            .set_in_memory(true)
            .set_db(&db)
            .set_causet(Causet_DEFAULT)
            .build(&name.to_str().unwrap())
            .unwrap();
        let write = <TestEngine as SstExt>::SstWriterBuilder::new()
            .set_in_memory(true)
            .set_db(&db)
            .set_causet(Causet_WRITE)
            .build(&name.to_str().unwrap())
            .unwrap();

        let mut w = importer
            .new_writer::<TestEngine>(default, write, meta)
            .unwrap();
        let mut batch = WriteBatch::default();
        let mut pairs = vec![];

        // put short value kv in wirte causet
        let mut pair = Pair::default();
        pair.set_key(b"k1".to_vec());
        pair.set_value(b"short_value".to_vec());
        pairs.push(pair);

        // put big value kv in default causet
        let big_value = vec![42; 256];
        let mut pair = Pair::default();
        pair.set_key(b"k2".to_vec());
        pair.set_value(big_value);
        pairs.push(pair);

        // put delete type key in write causet
        let mut pair = Pair::default();
        pair.set_key(b"k3".to_vec());
        pair.set_op(PairOp::Delete);
        pairs.push(pair);

        // generate two causet metas
        batch.set_commit_ts(10);
        batch.set_pairs(pairs.into());
        w.write(batch).unwrap();
        assert_eq!(w.write_entries, 3);
        assert_eq!(w.default_entries, 1);

        let metas = w.finish().unwrap();
        assert_eq!(metas.len(), 2);
    }

    #[test]
    fn test_download_rawkv_sst() {
        // creates a sample SST file.
        let (_ext_sst_dir, backlightlike, meta) =
            create_sample_external_rawkv_sst_file(b"0", b"z", false).unwrap();

        // performs the download.
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let cone = importer
            .download::<TestEngine>(
                &meta,
                &backlightlike,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(cone.get_spacelike(), b"a");
        assert_eq!(cone.get_lightlike(), b"d");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());
        assert_eq!(sst_file_metadata.len(), meta.get_length());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"za".to_vec(), b"v1".to_vec()),
                (b"zb".to_vec(), b"v2".to_vec()),
                (b"zb\x00".to_vec(), b"v3".to_vec()),
                (b"zc".to_vec(), b"v4".to_vec()),
                (b"zc\x00".to_vec(), b"v5".to_vec()),
                (b"zc\x00\x00".to_vec(), b"v6".to_vec()),
                (b"zd".to_vec(), b"v7".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_rawkv_sst_partial() {
        // creates a sample SST file.
        let (_ext_sst_dir, backlightlike, meta) =
            create_sample_external_rawkv_sst_file(b"b", b"c\x00", false).unwrap();

        // performs the download.
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let cone = importer
            .download::<TestEngine>(
                &meta,
                &backlightlike,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(cone.get_spacelike(), b"b");
        assert_eq!(cone.get_lightlike(), b"c\x00");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zb".to_vec(), b"v2".to_vec()),
                (b"zb\x00".to_vec(), b"v3".to_vec()),
                (b"zc".to_vec(), b"v4".to_vec()),
                (b"zc\x00".to_vec(), b"v5".to_vec()),
            ]
        );
    }

    #[test]
    fn test_download_rawkv_sst_partial_exclusive_lightlike_key() {
        // creates a sample SST file.
        let (_ext_sst_dir, backlightlike, meta) =
            create_sample_external_rawkv_sst_file(b"b", b"c\x00", true).unwrap();

        // performs the download.
        let importer_dir = tempfile::temfidelir().unwrap();
        let importer = SSTImporter::new(&importer_dir, None).unwrap();
        let sst_writer = create_sst_writer_with_db(&importer, &meta).unwrap();

        let cone = importer
            .download::<TestEngine>(
                &meta,
                &backlightlike,
                "sample.sst",
                &RewriteRule::default(),
                Limiter::new(INFINITY),
                sst_writer,
            )
            .unwrap()
            .unwrap();

        assert_eq!(cone.get_spacelike(), b"b");
        assert_eq!(cone.get_lightlike(), b"c");

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());

        // verifies the SST content is correct.
        let sst_reader = new_sst_reader(sst_file_path.to_str().unwrap());
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start).unwrap();
        assert_eq!(
            collect(iter),
            vec![
                (b"zb".to_vec(), b"v2".to_vec()),
                (b"zb\x00".to_vec(), b"v3".to_vec()),
                (b"zc".to_vec(), b"v4".to_vec()),
            ]
        );
    }
}
