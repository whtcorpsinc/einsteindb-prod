// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::cmp::{Ordering as CmpOrdering, Reverse};
use std::f64::INFINITY;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, ErrorKind, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use std::{error, result, str, thread, time, u64};

use encryption::{
    create_aes_ctr_crypter, encryption_method_from_db_encryption_method, DataKeyManager, Iv,
};
use edb::{CfName, Causet_DEFAULT, Causet_DAGGER, Causet_WRITE};
use edb::{EncryptionKeyManager, CausetEngine};
use futures::executor::block_on;
use futures_util::io::{AllowStdIo, AsyncWriteExt};
use ekvproto::encryption_timeshare::EncryptionMethod;
use ekvproto::meta_timeshare::Brane;
use ekvproto::violetabft_server_timeshare::VioletaBftSnapshotData;
use ekvproto::violetabft_server_timeshare::{SnapshotCfFile, SnapshotMeta};
use protobuf::Message;
use violetabft::evioletabft_timeshare::Snapshot as VioletaBftSnapshot;

use error_code::{self, ErrorCode, ErrorCodeExt};
use tuplespaceInstanton::{enc_lightlike_key, enc_spacelike_key};
use openssl::symm::{Cipher, Crypter, Mode};
use violetabftstore::interlock::::collections::{HashMap, HashMapEntry as Entry};
use violetabftstore::interlock::::file::{
    calc_crc32, calc_crc32_and_size, delete_file_if_exist, file_exists, get_file_size, sync_dir,
};
use violetabftstore::interlock::::time::{duration_to_sec, Limiter};
use violetabftstore::interlock::::HandyRwLock;

use crate::interlock::InterlockHost;
use crate::store::metrics::{
    CfNames, INGEST_SST_DURATION_SECONDS, SNAPSHOT_BUILD_TIME_HISTOGRAM, SNAPSHOT_Causet_KV_COUNT,
    SNAPSHOT_Causet_SIZE,
};
use crate::store::peer_causet_storage::JOB_STATUS_CANCELLING;
use crate::{Error as VioletaBftStoreError, Result as VioletaBftStoreResult};

#[path = "snap/io.rs"]
pub mod snap_io;

// Data in Causet_VIOLETABFT should be excluded for a snapshot.
pub const SNAPSHOT_CausetS: &[CfName] = &[Causet_DEFAULT, Causet_DAGGER, Causet_WRITE];
pub const SNAPSHOT_CausetS_ENUM_PAIR: &[(CfNames, CfName)] = &[
    (CfNames::default, Causet_DEFAULT),
    (CfNames::dagger, Causet_DAGGER),
    (CfNames::write, Causet_WRITE),
];
pub const SNAPSHOT_VERSION: u64 = 2;

/// Name prefix for the self-generated snapshot file.
const SNAP_GEN_PREFIX: &str = "gen";
/// Name prefix for the received snapshot file.
const SNAP_REV_PREFIX: &str = "rev";

const TMP_FILE_SUFFIX: &str = ".tmp";
const SST_FILE_SUFFIX: &str = ".sst";
const CLONE_FILE_SUFFIX: &str = ".clone";
const META_FILE_SUFFIX: &str = ".meta";

const DELETE_RETRY_MAX_TIMES: u32 = 6;
const DELETE_RETRY_TIME_MILLIS: u64 = 500;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Abort {
            display("abort")
        }
        TooManySnapshots {
            display("too many snapshots")
        }
        Other(err: Box<dyn error::Error + Sync + lightlike>) {
            from()
            cause(err.as_ref())
            display("snap failed {:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Abort => error_code::violetabftstore::SNAP_ABORT,
            Error::TooManySnapshots => error_code::violetabftstore::SNAP_TOO_MANY,
            Error::Other(_) => error_code::violetabftstore::SNAP_UNKNOWN,
        }
    }
}

// Causet_DAGGER is relatively small, so we use plain file for performance issue.
#[inline]
pub fn plain_file_used(causet: &str) -> bool {
    causet == Causet_DAGGER
}

#[inline]
pub fn check_abort(status: &AtomicUsize) -> Result<()> {
    if status.load(Ordering::Relaxed) == JOB_STATUS_CANCELLING {
        return Err(Error::Abort);
    }
    Ok(())
}

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SnapKey {
    pub brane_id: u64,
    pub term: u64,
    pub idx: u64,
}

impl SnapKey {
    #[inline]
    pub fn new(brane_id: u64, term: u64, idx: u64) -> SnapKey {
        SnapKey {
            brane_id,
            term,
            idx,
        }
    }

    pub fn from_brane_snap(brane_id: u64, snap: &VioletaBftSnapshot) -> SnapKey {
        let index = snap.get_metadata().get_index();
        let term = snap.get_metadata().get_term();
        SnapKey::new(brane_id, term, index)
    }

    pub fn from_snap(snap: &VioletaBftSnapshot) -> io::Result<SnapKey> {
        let mut snap_data = VioletaBftSnapshotData::default();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            return Err(io::Error::new(ErrorKind::Other, e));
        }

        Ok(SnapKey::from_brane_snap(
            snap_data.get_brane().get_id(),
            snap,
        ))
    }
}

impl Display for SnapKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}_{}", self.brane_id, self.term, self.idx)
    }
}

#[derive(Default)]
pub struct SnapshotStatistics {
    pub size: u64,
    pub kv_count: usize,
}

impl SnapshotStatistics {
    pub fn new() -> SnapshotStatistics {
        SnapshotStatistics {
            ..Default::default()
        }
    }
}

pub struct ApplyOptions<EK>
where
    EK: CausetEngine,
{
    pub db: EK,
    pub brane: Brane,
    pub abort: Arc<AtomicUsize>,
    pub write_batch_size: usize,
    pub interlock_host: InterlockHost<EK>,
}

/// `Snapshot` is a trait for snapshot.
/// It's used in these scenarios:
///   1. build local snapshot
///   2. read local snapshot and then replicate it to remote violetabftstores
///   3. receive snapshot from remote violetabftstore and write it to local causet_storage
///   4. apply snapshot
///   5. snapshot gc
pub trait Snapshot<EK: CausetEngine>: GenericSnapshot {
    fn build(
        &mut self,
        engine: &EK,
        kv_snap: &EK::Snapshot,
        brane: &Brane,
        snap_data: &mut VioletaBftSnapshotData,
        stat: &mut SnapshotStatistics,
    ) -> VioletaBftStoreResult<()>;
    fn apply(&mut self, options: ApplyOptions<EK>) -> Result<()>;
}

/// `GenericSnapshot` is a snapshot not tied to any KV engines.
pub trait GenericSnapshot: Read + Write + lightlike {
    fn path(&self) -> &str;
    fn exists(&self) -> bool;
    fn delete(&self);
    fn meta(&self) -> io::Result<Metadata>;
    fn total_size(&self) -> io::Result<u64>;
    fn save(&mut self) -> io::Result<()>;
}

// A helper function to copy snapshot.
// Only used in tests.
pub fn copy_snapshot(
    mut from: Box<dyn GenericSnapshot>,
    mut to: Box<dyn GenericSnapshot>,
) -> io::Result<()> {
    if !to.exists() {
        io::copy(&mut from, &mut to)?;
        to.save()?;
    }
    Ok(())
}

// Try to delete the specified snapshot, return true if the deletion is done.
fn retry_delete_snapshot(mgr: &SnapManagerCore, key: &SnapKey, snap: &dyn GenericSnapshot) -> bool {
    let d = time::Duration::from_millis(DELETE_RETRY_TIME_MILLIS);
    for _ in 0..DELETE_RETRY_MAX_TIMES {
        if mgr.delete_snapshot(key, snap, true) {
            return true;
        }
        thread::sleep(d);
    }
    false
}

fn gen_snapshot_meta(causet_files: &[CfFile]) -> VioletaBftStoreResult<SnapshotMeta> {
    let mut meta = Vec::with_capacity(causet_files.len());
    for causet_file in causet_files {
        if SNAPSHOT_CausetS.iter().find(|&causet| causet_file.causet == *causet).is_none() {
            return Err(box_err!(
                "failed to encode invalid snapshot causet {}",
                causet_file.causet
            ));
        }

        let mut causet_file_meta = SnapshotCfFile::new();
        causet_file_meta.set_causet(causet_file.causet.to_owned());
        causet_file_meta.set_size(causet_file.size);
        causet_file_meta.set_checksum(causet_file.checksum);
        meta.push(causet_file_meta);
    }
    let mut snapshot_meta = SnapshotMeta::default();
    snapshot_meta.set_causet_files(meta.into());
    Ok(snapshot_meta)
}

fn calc_checksum_and_size(
    path: &PathBuf,
    encryption_key_manager: Option<&Arc<DataKeyManager>>,
) -> VioletaBftStoreResult<(u32, u64)> {
    let (checksum, size) = if let Some(mgr) = encryption_key_manager {
        // Crc32 and file size need to be calculated based on decrypted contents.
        let file_name = path.to_str().unwrap();
        let mut r = snap_io::get_decrypter_reader(file_name, &mgr)?;
        calc_crc32_and_size(&mut r)?
    } else {
        (calc_crc32(path)?, get_file_size(path)?)
    };
    Ok((checksum, size))
}

fn check_file_size(got_size: u64, expected_size: u64, path: &PathBuf) -> VioletaBftStoreResult<()> {
    if got_size != expected_size {
        return Err(box_err!(
            "invalid size {} for snapshot causet file {}, expected {}",
            got_size,
            path.display(),
            expected_size
        ));
    }
    Ok(())
}

fn check_file_checksum(
    got_checksum: u32,
    expected_checksum: u32,
    path: &PathBuf,
) -> VioletaBftStoreResult<()> {
    if got_checksum != expected_checksum {
        return Err(box_err!(
            "invalid checksum {} for snapshot causet file {}, expected {}",
            got_checksum,
            path.display(),
            expected_checksum
        ));
    }
    Ok(())
}

fn check_file_size_and_checksum(
    path: &PathBuf,
    expected_size: u64,
    expected_checksum: u32,
    encryption_key_manager: Option<&Arc<DataKeyManager>>,
) -> VioletaBftStoreResult<()> {
    let (checksum, size) = calc_checksum_and_size(path, encryption_key_manager)?;
    check_file_size(size, expected_size, path)?;
    check_file_checksum(checksum, expected_checksum, path)?;
    Ok(())
}

struct CfFileForRecving {
    file: File,
    encrypter: Option<(Cipher, Crypter)>,
    written_size: u64,
    write_digest: crc32fast::Hasher,
}

#[derive(Default)]
pub struct CfFile {
    pub causet: CfName,
    pub path: PathBuf,
    pub tmp_path: PathBuf,
    pub clone_path: PathBuf,
    file_for_lightlikeing: Option<Box<dyn Read + lightlike>>,
    file_for_recving: Option<CfFileForRecving>,
    pub kv_count: u64,
    pub size: u64,
    pub checksum: u32,
}

#[derive(Default)]
struct MetaFile {
    pub meta: SnapshotMeta,
    pub path: PathBuf,
    pub file: Option<File>,

    // for writing snapshot
    pub tmp_path: PathBuf,
}

pub struct Snap {
    key: SnapKey,
    display_path: String,
    dir_path: PathBuf,
    causet_files: Vec<CfFile>,
    causet_index: usize,
    meta_file: MetaFile,
    hold_tmp_files: bool,

    mgr: SnapManagerCore,
}

impl Snap {
    fn new<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        is_lightlikeing: bool,
        to_build: bool,
        mgr: &SnapManagerCore,
    ) -> VioletaBftStoreResult<Self> {
        let dir_path = dir.into();
        if !dir_path.exists() {
            fs::create_dir_all(dir_path.as_path())?;
        }
        let snap_prefix = if is_lightlikeing {
            SNAP_GEN_PREFIX
        } else {
            SNAP_REV_PREFIX
        };
        let prefix = format!("{}_{}", snap_prefix, key);
        let display_path = Self::get_display_path(&dir_path, &prefix);

        let mut causet_files = Vec::with_capacity(SNAPSHOT_CausetS.len());
        for causet in SNAPSHOT_CausetS {
            let filename = format!("{}_{}{}", prefix, causet, SST_FILE_SUFFIX);
            let path = dir_path.join(&filename);
            let tmp_path = dir_path.join(format!("{}{}", filename, TMP_FILE_SUFFIX));
            let clone_path = dir_path.join(format!("{}{}", filename, CLONE_FILE_SUFFIX));
            let causet_file = CfFile {
                causet,
                path,
                tmp_path,
                clone_path,
                ..Default::default()
            };
            causet_files.push(causet_file);
        }

        let meta_filename = format!("{}{}", prefix, META_FILE_SUFFIX);
        let meta_path = dir_path.join(&meta_filename);
        let meta_tmp_path = dir_path.join(format!("{}{}", meta_filename, TMP_FILE_SUFFIX));
        let meta_file = MetaFile {
            path: meta_path,
            tmp_path: meta_tmp_path,
            ..Default::default()
        };

        let mut s = Snap {
            key: key.clone(),
            display_path,
            dir_path,
            causet_files,
            causet_index: 0,
            meta_file,
            hold_tmp_files: false,
            mgr: mgr.clone(),
        };

        // load snapshot meta if meta_file exists
        if file_exists(&s.meta_file.path) {
            if let Err(e) = s.load_snapshot_meta() {
                if !to_build {
                    return Err(e);
                }
                warn!(
                    "failed to load existent snapshot meta when try to build snapshot";
                    "snapshot" => %s.path(),
                    "err" => ?e,
                    "error_code" => %e.error_code(),
                );
                if !retry_delete_snapshot(mgr, key, &s) {
                    warn!(
                        "failed to delete snapshot because it's already registered elsewhere";
                        "snapshot" => %s.path(),
                    );
                    return Err(e);
                }
            }
        }
        Ok(s)
    }

    fn new_for_building<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        mgr: &SnapManagerCore,
    ) -> VioletaBftStoreResult<Self> {
        let mut s = Self::new(dir, key, true, true, mgr)?;
        s.init_for_building()?;
        Ok(s)
    }

    fn new_for_lightlikeing<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        mgr: &SnapManagerCore,
    ) -> VioletaBftStoreResult<Self> {
        let mut s = Self::new(dir, key, true, false, mgr)?;
        s.mgr.limiter = Limiter::new(INFINITY);

        if !s.exists() {
            // Skip the initialization below if it doesn't exists.
            return Ok(s);
        }
        for causet_file in &mut s.causet_files {
            // initialize causet file size and reader
            if causet_file.size > 0 {
                let file = File::open(&causet_file.path)?;
                causet_file.file_for_lightlikeing = Some(Box::new(file) as Box<dyn Read + lightlike>);
            }
        }
        Ok(s)
    }

    fn new_for_receiving<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        mgr: &SnapManagerCore,
        snapshot_meta: SnapshotMeta,
    ) -> VioletaBftStoreResult<Self> {
        let mut s = Self::new(dir, key, false, false, mgr)?;
        s.set_snapshot_meta(snapshot_meta)?;
        if s.exists() {
            return Ok(s);
        }

        let f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&s.meta_file.tmp_path)?;
        s.meta_file.file = Some(f);
        s.hold_tmp_files = true;

        for causet_file in &mut s.causet_files {
            if causet_file.size == 0 {
                continue;
            }
            let f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&causet_file.tmp_path)?;
            causet_file.file_for_recving = Some(CfFileForRecving {
                file: f,
                encrypter: None,
                written_size: 0,
                write_digest: crc32fast::Hasher::new(),
            });

            if let Some(mgr) = &s.mgr.encryption_key_manager {
                let path = causet_file.path.to_str().unwrap();
                let enc_info = mgr.new_file(path)?;
                let mthd = encryption_method_from_db_encryption_method(enc_info.method);
                if mthd != EncryptionMethod::Plaintext {
                    let file_for_recving = causet_file.file_for_recving.as_mut().unwrap();
                    file_for_recving.encrypter = Some(
                        create_aes_ctr_crypter(
                            mthd,
                            &enc_info.key,
                            Mode::Encrypt,
                            Iv::from_slice(&enc_info.iv)?,
                        )
                        .map_err(|e| VioletaBftStoreError::Snapshot(box_err!(e)))?,
                    );
                }
            }
        }
        Ok(s)
    }

    fn new_for_applying<T: Into<PathBuf>>(
        dir: T,
        key: &SnapKey,
        mgr: &SnapManagerCore,
    ) -> VioletaBftStoreResult<Self> {
        let mut s = Self::new(dir, key, false, false, mgr)?;
        s.mgr.limiter = Limiter::new(INFINITY);
        Ok(s)
    }

    // If all files of the snapshot exist, return `Ok` directly. Otherwise create a new file at
    // the temporary meta file path, so that all other try will fail.
    fn init_for_building(&mut self) -> VioletaBftStoreResult<()> {
        if self.exists() {
            return Ok(());
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.meta_file.tmp_path)?;
        self.meta_file.file = Some(file);
        self.hold_tmp_files = true;
        Ok(())
    }

    fn read_snapshot_meta(&mut self) -> VioletaBftStoreResult<SnapshotMeta> {
        let buf = fs::read(&self.meta_file.path)?;
        let mut snapshot_meta = SnapshotMeta::default();
        snapshot_meta.merge_from_bytes(&buf)?;
        Ok(snapshot_meta)
    }

    fn set_snapshot_meta(&mut self, snapshot_meta: SnapshotMeta) -> VioletaBftStoreResult<()> {
        if snapshot_meta.get_causet_files().len() != self.causet_files.len() {
            return Err(box_err!(
                "invalid causet number of snapshot meta, expect {}, got {}",
                SNAPSHOT_CausetS.len(),
                snapshot_meta.get_causet_files().len()
            ));
        }
        for (i, causet_file) in self.causet_files.iter_mut().enumerate() {
            let meta = snapshot_meta.get_causet_files().get(i).unwrap();
            if meta.get_causet() != causet_file.causet {
                return Err(box_err!(
                    "invalid {} causet in snapshot meta, expect {}, got {}",
                    i,
                    causet_file.causet,
                    meta.get_causet()
                ));
            }
            causet_file.size = meta.get_size();
            causet_file.checksum = meta.get_checksum();
            if file_exists(&causet_file.path) {
                let mgr = self.mgr.encryption_key_manager.as_ref();
                let (_, size) = calc_checksum_and_size(&causet_file.path, mgr)?;
                check_file_size(size, causet_file.size, &causet_file.path)?;
            }
        }
        self.meta_file.meta = snapshot_meta;
        Ok(())
    }

    fn load_snapshot_meta(&mut self) -> VioletaBftStoreResult<()> {
        let snapshot_meta = self.read_snapshot_meta()?;
        self.set_snapshot_meta(snapshot_meta)?;
        // check if there is a data corruption when the meta file exists
        // but causet files are deleted.
        if !self.exists() {
            return Err(box_err!(
                "snapshot {} is corrupted, some causet file is missing",
                self.path()
            ));
        }
        Ok(())
    }

    fn get_display_path(dir_path: impl AsRef<Path>, prefix: &str) -> String {
        let causet_names = "(".to_owned() + &SNAPSHOT_CausetS.join("|") + ")";
        format!(
            "{}/{}_{}{}",
            dir_path.as_ref().display(),
            prefix,
            causet_names,
            SST_FILE_SUFFIX
        )
    }

    fn validate(&self, engine: &impl CausetEngine, for_lightlike: bool) -> VioletaBftStoreResult<()> {
        for causet_file in &self.causet_files {
            if causet_file.size == 0 {
                // Skip empty file. The checksum of this causet file should be 0 and
                // this is checked when loading the snapshot meta.
                continue;
            }

            if !plain_file_used(causet_file.causet) {
                // Reset global seq number.
                let causet = engine.causet_handle(causet_file.causet)?;
                engine.validate_sst_for_ingestion(
                    &causet,
                    &causet_file.path,
                    causet_file.size,
                    causet_file.checksum,
                )?;
            }
            check_file_size_and_checksum(
                &causet_file.path,
                causet_file.size,
                causet_file.checksum,
                self.mgr.encryption_key_manager.as_ref(),
            )?;

            if !for_lightlike && !plain_file_used(causet_file.causet) {
                sst_importer::prepare_sst_for_ingestion(
                    &causet_file.path,
                    &causet_file.clone_path,
                    self.mgr.encryption_key_manager.as_ref(),
                )?;
            }
        }
        Ok(())
    }

    fn switch_to_causet_file(&mut self, causet: &str) -> io::Result<()> {
        match self.causet_files.iter().position(|x| x.causet == causet) {
            Some(index) => {
                self.causet_index = index;
                Ok(())
            }
            None => Err(io::Error::new(
                ErrorKind::Other,
                format!("fail to find causet {}", causet),
            )),
        }
    }

    // Only called in `do_build`.
    fn save_meta_file(&mut self) -> VioletaBftStoreResult<()> {
        let v = box_try!(self.meta_file.meta.write_to_bytes());
        if let Some(mut f) = self.meta_file.file.take() {
            // `meta_file` could be None for this case: in `init_for_building` the snapshot exists
            // so no temporary meta file is created, and this field is None. However in `do_build`
            // it's deleted so we build it again, and then call `save_meta_file` with `meta_file`
            // as None.
            // FIXME: We can fix it later by introducing a better snapshot delete mechanism.
            f.write_all(&v[..])?;
            f.flush()?;
            f.sync_all()?;
            fs::rename(&self.meta_file.tmp_path, &self.meta_file.path)?;
            self.hold_tmp_files = false;
            Ok(())
        } else {
            Err(box_err!(
                "save meta file without metadata for {:?}",
                self.key
            ))
        }
    }

    fn do_build<EK: CausetEngine>(
        &mut self,
        engine: &EK,
        kv_snap: &EK::Snapshot,
        brane: &Brane,
        stat: &mut SnapshotStatistics,
    ) -> VioletaBftStoreResult<()>
    where
        EK: CausetEngine,
    {
        fail_point!("snapshot_enter_do_build");
        if self.exists() {
            match self.validate(engine, true) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    error!(?e;
                        "snapshot is corrupted, will rebuild";
                        "brane_id" => brane.get_id(),
                        "snapshot" => %self.path(),
                    );
                    if !retry_delete_snapshot(&self.mgr, &self.key, self) {
                        error!(
                            "failed to delete corrupted snapshot because it's \
                             already registered elsewhere";
                            "brane_id" => brane.get_id(),
                            "snapshot" => %self.path(),
                        );
                        return Err(e);
                    }
                    self.init_for_building()?;
                }
            }
        }

        let (begin_key, lightlike_key) = (enc_spacelike_key(brane), enc_lightlike_key(brane));
        for (causet_enum, causet) in SNAPSHOT_CausetS_ENUM_PAIR {
            self.switch_to_causet_file(causet)?;
            let causet_file = &mut self.causet_files[self.causet_index];
            let path = causet_file.tmp_path.to_str().unwrap();
            let causet_stat = if plain_file_used(causet_file.causet) {
                let key_mgr = self.mgr.encryption_key_manager.as_ref();
                snap_io::build_plain_causet_file::<EK>(
                    path, key_mgr, kv_snap, causet_file.causet, &begin_key, &lightlike_key,
                )?
            } else {
                snap_io::build_sst_causet_file::<EK>(
                    path,
                    engine,
                    kv_snap,
                    causet_file.causet,
                    &begin_key,
                    &lightlike_key,
                    &self.mgr.limiter,
                )?
            };
            causet_file.kv_count = causet_stat.key_count as u64;
            if causet_file.kv_count > 0 {
                // Use `kv_count` instead of file size to check empty files because encrypted sst files
                // contain some metadata so their sizes will never be 0.
                self.mgr.rename_tmp_causet_file_for_lightlike(causet_file)?;
                self.mgr.snap_size.fetch_add(causet_file.size, Ordering::SeqCst);
            } else {
                delete_file_if_exist(&causet_file.tmp_path).unwrap();
            }

            SNAPSHOT_Causet_KV_COUNT
                .get(*causet_enum)
                .observe(causet_stat.key_count as f64);
            SNAPSHOT_Causet_SIZE
                .get(*causet_enum)
                .observe(causet_stat.total_size as f64);
            info!(
                "scan snapshot of one causet";
                "brane_id" => brane.get_id(),
                "snapshot" => self.path(),
                "causet" => causet,
                "key_count" => causet_stat.key_count,
                "size" => causet_stat.total_size,
            );
        }

        stat.kv_count = self.causet_files.iter().map(|causet| causet.kv_count as usize).sum();
        // save snapshot meta to meta file
        let snapshot_meta = gen_snapshot_meta(&self.causet_files[..])?;
        self.meta_file.meta = snapshot_meta;
        self.save_meta_file()?;
        Ok(())
    }
}

impl fmt::Debug for Snap {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snap")
            .field("key", &self.key)
            .field("display_path", &self.display_path)
            .finish()
    }
}

impl<EK> Snapshot<EK> for Snap
where
    EK: CausetEngine,
{
    fn build(
        &mut self,
        engine: &EK,
        kv_snap: &EK::Snapshot,
        brane: &Brane,
        snap_data: &mut VioletaBftSnapshotData,
        stat: &mut SnapshotStatistics,
    ) -> VioletaBftStoreResult<()> {
        let t = Instant::now();
        self.do_build::<EK>(engine, kv_snap, brane, stat)?;

        let total_size = self.total_size()?;
        stat.size = total_size;
        // set snapshot meta data
        snap_data.set_file_size(total_size);
        snap_data.set_version(SNAPSHOT_VERSION);
        snap_data.set_meta(self.meta_file.meta.clone());

        SNAPSHOT_BUILD_TIME_HISTOGRAM.observe(duration_to_sec(t.elapsed()) as f64);
        info!(
            "scan snapshot";
            "brane_id" => brane.get_id(),
            "snapshot" => self.path(),
            "key_count" => stat.kv_count,
            "size" => total_size,
            "takes" => ?t.elapsed(),
        );

        Ok(())
    }

    fn apply(&mut self, options: ApplyOptions<EK>) -> Result<()> {
        box_try!(self.validate(&options.db, false));

        let abort_checker = ApplyAbortChecker(options.abort);
        let interlock_host = options.interlock_host;
        let brane = options.brane;
        let key_mgr = self.mgr.encryption_key_manager.as_ref();
        for causet_file in &mut self.causet_files {
            if causet_file.size == 0 {
                // Skip empty causet file.
                continue;
            }
            let causet = causet_file.causet;
            if plain_file_used(causet_file.causet) {
                let path = causet_file.path.to_str().unwrap();
                let batch_size = options.write_batch_size;
                let cb = |kv: &[(Vec<u8>, Vec<u8>)]| {
                    interlock_host.post_apply_plain_kvs_from_snapshot(&brane, causet, kv)
                };
                snap_io::apply_plain_causet_file(
                    path,
                    key_mgr,
                    &abort_checker,
                    &options.db,
                    causet,
                    batch_size,
                    cb,
                )?;
            } else {
                let _timer = INGEST_SST_DURATION_SECONDS.spacelike_coarse_timer();
                let path = causet_file.clone_path.to_str().unwrap();
                snap_io::apply_sst_causet_file(path, &options.db, causet)?;
                interlock_host.post_apply_sst_from_snapshot(&brane, causet, path);
            }
        }
        Ok(())
    }
}

impl GenericSnapshot for Snap {
    fn path(&self) -> &str {
        &self.display_path
    }

    fn exists(&self) -> bool {
        self.causet_files
            .iter()
            .all(|causet_file| causet_file.size == 0 || file_exists(&causet_file.path))
            && file_exists(&self.meta_file.path)
    }

    // TODO: It's very hard to handle key manager correctly without dagger `SnapManager`.
    // Let's do it later.
    fn delete(&self) {
        debug!(
            "deleting snapshot file";
            "snapshot" => %self.path(),
        );
        for causet_file in &self.causet_files {
            // Delete cloned files.
            delete_file_if_exist(&causet_file.clone_path).unwrap();

            // Delete temp files.
            if self.hold_tmp_files {
                delete_file_if_exist(&causet_file.tmp_path).unwrap();
            }

            // Delete causet files.
            if delete_file_if_exist(&causet_file.path).unwrap() {
                self.mgr.snap_size.fetch_sub(causet_file.size, Ordering::SeqCst);
            }
        }
        delete_file_if_exist(&self.meta_file.path).unwrap();
        if self.hold_tmp_files {
            delete_file_if_exist(&self.meta_file.tmp_path).unwrap();
        }
    }

    fn meta(&self) -> io::Result<Metadata> {
        fs::metadata(&self.meta_file.path)
    }

    fn total_size(&self) -> io::Result<u64> {
        Ok(self.causet_files.iter().fold(0, |acc, x| acc + x.size))
    }

    fn save(&mut self) -> io::Result<()> {
        debug!(
            "saving to snapshot file";
            "snapshot" => %self.path(),
        );
        for causet_file in &mut self.causet_files {
            if causet_file.size == 0 {
                // Skip empty causet file.
                continue;
            }

            // Check each causet file has been fully written, and the checksum matches.
            let mut file_for_recving = causet_file.file_for_recving.take().unwrap();
            file_for_recving.file.flush()?;
            file_for_recving.file.sync_all()?;

            if file_for_recving.written_size != causet_file.size {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "snapshot file {} for causet {} size mismatches, \
                         real size {}, expected size {}",
                        causet_file.path.display(),
                        causet_file.causet,
                        file_for_recving.written_size,
                        causet_file.size
                    ),
                ));
            }

            let checksum = file_for_recving.write_digest.finalize();
            if checksum != causet_file.checksum {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "snapshot file {} for causet {} checksum \
                         mismatches, real checksum {}, expected \
                         checksum {}",
                        causet_file.path.display(),
                        causet_file.causet,
                        checksum,
                        causet_file.checksum
                    ),
                ));
            }

            fs::rename(&causet_file.tmp_path, &causet_file.path)?;
            self.mgr.snap_size.fetch_add(causet_file.size, Ordering::SeqCst);
        }
        sync_dir(&self.dir_path)?;

        // write meta file
        let v = self.meta_file.meta.write_to_bytes()?;
        {
            let mut meta_file = self.meta_file.file.take().unwrap();
            meta_file.write_all(&v[..])?;
            meta_file.sync_all()?;
        }
        fs::rename(&self.meta_file.tmp_path, &self.meta_file.path)?;
        sync_dir(&self.dir_path)?;
        self.hold_tmp_files = false;
        Ok(())
    }
}

// To check whether a procedure about apply snapshot aborts or not.
struct ApplyAbortChecker(Arc<AtomicUsize>);
impl snap_io::StaleDetector for ApplyAbortChecker {
    fn is_stale(&self) -> bool {
        self.0.load(Ordering::Relaxed) == JOB_STATUS_CANCELLING
    }
}

impl Read for Snap {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        while self.causet_index < self.causet_files.len() {
            let causet_file = &mut self.causet_files[self.causet_index];
            if causet_file.size == 0 {
                self.causet_index += 1;
                continue;
            }
            let reader = causet_file.file_for_lightlikeing.as_mut().unwrap();
            match reader.read(buf) {
                Ok(0) => {
                    // EOF. Switch to next file.
                    self.causet_index += 1;
                }
                Ok(n) => return Ok(n),
                e => return e,
            }
        }
        Ok(0)
    }
}

impl Write for Snap {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let (mut next_buf, mut written_bytes) = (buf, 0);
        while self.causet_index < self.causet_files.len() {
            let causet_file = &mut self.causet_files[self.causet_index];
            if causet_file.size == 0 {
                self.causet_index += 1;
                continue;
            }

            let mut file_for_recving = causet_file.file_for_recving.as_mut().unwrap();

            let left = (causet_file.size - file_for_recving.written_size) as usize;
            assert!(left > 0 && !next_buf.is_empty());
            let (write_len, switch, finished) = match next_buf.len().cmp(&left) {
                CmpOrdering::Greater => (left, true, false),
                CmpOrdering::Equal => (left, true, true),
                CmpOrdering::Less => (next_buf.len(), false, true),
            };

            file_for_recving
                .write_digest
                .fidelio(&next_buf[0..write_len]);
            file_for_recving.written_size += write_len as u64;
            written_bytes += write_len;

            let file = AllowStdIo::new(&mut file_for_recving.file);
            let mut file = self.mgr.limiter.clone().limit(file);
            if file_for_recving.encrypter.is_none() {
                block_on(file.write_all(&next_buf[0..write_len]))?;
            } else {
                let (cipher, crypter) = file_for_recving.encrypter.as_mut().unwrap();
                let mut encrypt_buffer = vec![0; write_len + cipher.block_size()];
                let mut bytes = crypter.fidelio(&next_buf[0..write_len], &mut encrypt_buffer)?;
                if switch {
                    bytes += crypter.finalize(&mut encrypt_buffer)?;
                }
                encrypt_buffer.truncate(bytes);
                block_on(file.write_all(&encrypt_buffer))?;
            }

            if switch {
                self.causet_index += 1;
                next_buf = &next_buf[write_len..]
            }
            if finished {
                break;
            }
        }
        Ok(written_bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(causet_file) = self.causet_files.get_mut(self.causet_index) {
            let file_for_recving = causet_file.file_for_recving.as_mut().unwrap();
            file_for_recving.file.flush()?;
        }
        Ok(())
    }
}

impl Drop for Snap {
    fn drop(&mut self) {
        // cleanup if some of the causet files and meta file is partly written
        if self
            .causet_files
            .iter()
            .any(|causet_file| file_exists(&causet_file.tmp_path))
            || file_exists(&self.meta_file.tmp_path)
        {
            self.delete();
            return;
        }
        // cleanup if data corruption happens and any file goes missing
        if !self.exists() {
            self.delete();
            return;
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum SnapEntry {
    Generating = 1,
    lightlikeing = 2,
    Receiving = 3,
    Applying = 4,
}

/// `SnapStats` is for snapshot statistics.
pub struct SnapStats {
    pub lightlikeing_count: usize,
    pub receiving_count: usize,
}

#[derive(Clone)]
struct SnapManagerCore {
    // directory to store snapfile.
    base: String,

    registry: Arc<RwLock<HashMap<SnapKey, Vec<SnapEntry>>>>,
    limiter: Limiter,
    snap_size: Arc<AtomicU64>,
    encryption_key_manager: Option<Arc<DataKeyManager>>,
}

/// `SnapManagerCore` trace all current processing snapshots.
pub struct SnapManager {
    core: SnapManagerCore,
    max_total_size: u64,
}

impl Clone for SnapManager {
    fn clone(&self) -> Self {
        SnapManager {
            core: self.core.clone(),
            max_total_size: self.max_total_size,
        }
    }
}

impl SnapManager {
    pub fn new<T: Into<String>>(path: T) -> Self {
        SnapManagerBuilder::default().build(path)
    }

    pub fn init(&self) -> io::Result<()> {
        let enc_enabled = self.core.encryption_key_manager.is_some();
        info!(
            "Initializing SnapManager, encryption is enabled: {}",
            enc_enabled
        );

        // Use write dagger so only one thread initialize the directory at a time.
        let _lock = self.core.registry.wl();
        let path = Path::new(&self.core.base);
        if !path.exists() {
            fs::create_dir_all(path)?;
            return Ok(());
        }
        if !path.is_dir() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("{} should be a directory", path.display()),
            ));
        }
        for f in fs::read_dir(path)? {
            let p = f?;
            if p.file_type()?.is_file() {
                if let Some(s) = p.file_name().to_str() {
                    if s.lightlikes_with(TMP_FILE_SUFFIX) {
                        fs::remove_file(p.path())?;
                    } else if s.lightlikes_with(SST_FILE_SUFFIX) {
                        let len = p.metadata()?.len();
                        self.core.snap_size.fetch_add(len, Ordering::SeqCst);
                    }
                }
            }
        }
        Ok(())
    }

    // Return all snapshots which is idle not being used.
    pub fn list_idle_snap(&self) -> io::Result<Vec<(SnapKey, bool)>> {
        // Use a dagger to protect the directory when scanning.
        let registry = self.core.registry.rl();
        let read_dir = fs::read_dir(Path::new(&self.core.base))?;
        // Remove the duplicate snap tuplespaceInstanton.
        let mut v: Vec<_> = read_dir
            .filter_map(|p| {
                let p = match p {
                    Err(e) => {
                        error!(
                            "failed to list content of directory";
                            "directory" => %&self.core.base,
                            "err" => ?e,
                        );
                        return None;
                    }
                    Ok(p) => p,
                };
                match p.file_type() {
                    Ok(t) if t.is_file() => {}
                    _ => return None,
                }
                let file_name = p.file_name();
                let name = match file_name.to_str() {
                    None => return None,
                    Some(n) => n,
                };
                let is_lightlikeing = name.spacelikes_with(SNAP_GEN_PREFIX);
                let numbers: Vec<u64> = name.split('.').next().map_or_else(
                    || vec![],
                    |s| {
                        s.split('_')
                            .skip(1)
                            .filter_map(|s| s.parse().ok())
                            .collect()
                    },
                );
                if numbers.len() != 3 {
                    error!(
                        "failed to parse snapkey";
                        "snap_key" => %name,
                    );
                    return None;
                }
                let snap_key = SnapKey::new(numbers[0], numbers[1], numbers[2]);
                if registry.contains_key(&snap_key) {
                    // Skip those registered snapshot.
                    return None;
                }
                Some((snap_key, is_lightlikeing))
            })
            .collect();
        v.sort();
        v.dedup();
        Ok(v)
    }

    #[inline]
    pub fn has_registered(&self, key: &SnapKey) -> bool {
        self.core.registry.rl().contains_key(key)
    }

    pub fn get_snapshot_for_building<EK: CausetEngine>(
        &self,
        key: &SnapKey,
    ) -> VioletaBftStoreResult<Box<dyn Snapshot<EK>>> {
        let mut old_snaps = None;
        while self.get_total_snap_size() > self.max_total_snap_size() {
            if old_snaps.is_none() {
                let snaps = self.list_idle_snap()?;
                let mut key_and_snaps = Vec::with_capacity(snaps.len());
                for (key, is_lightlikeing) in snaps {
                    if !is_lightlikeing {
                        continue;
                    }
                    let snap = match self.get_snapshot_for_lightlikeing(&key) {
                        Ok(snap) => snap,
                        Err(_) => continue,
                    };
                    if let Ok(modified) = snap.meta().and_then(|m| m.modified()) {
                        key_and_snaps.push((key, snap, modified));
                    }
                }
                key_and_snaps.sort_by_key(|&(_, _, modified)| Reverse(modified));
                old_snaps = Some(key_and_snaps);
            }
            match old_snaps.as_mut().unwrap().pop() {
                Some((key, snap, _)) => self.delete_snapshot(&key, snap.as_ref(), false),
                None => return Err(VioletaBftStoreError::Snapshot(Error::TooManySnapshots)),
            };
        }

        let base = &self.core.base;
        let f = Snap::new_for_building(base, key, &self.core)?;
        Ok(Box::new(f))
    }

    pub fn get_snapshot_for_lightlikeing(
        &self,
        key: &SnapKey,
    ) -> VioletaBftStoreResult<Box<dyn GenericSnapshot>> {
        let _lock = self.core.registry.rl();
        let base = &self.core.base;
        let mut s = Snap::new_for_lightlikeing(base, key, &self.core)?;
        let key_manager = match self.core.encryption_key_manager.as_ref() {
            Some(m) => m,
            None => return Ok(Box::new(s)),
        };
        for causet_file in &mut s.causet_files {
            if causet_file.size == 0 {
                continue;
            }
            let p = causet_file.path.to_str().unwrap();
            let reader = snap_io::get_decrypter_reader(p, key_manager)?;
            causet_file.file_for_lightlikeing = Some(reader);
        }
        Ok(Box::new(s))
    }

    pub fn get_snapshot_for_receiving(
        &self,
        key: &SnapKey,
        data: &[u8],
    ) -> VioletaBftStoreResult<Box<dyn GenericSnapshot>> {
        let _lock = self.core.registry.rl();
        let mut snapshot_data = VioletaBftSnapshotData::default();
        snapshot_data.merge_from_bytes(data)?;
        let base = &self.core.base;
        let f = Snap::new_for_receiving(base, key, &self.core, snapshot_data.take_meta())?;
        Ok(Box::new(f))
    }

    fn get_concrete_snapshot_for_applying(&self, key: &SnapKey) -> VioletaBftStoreResult<Box<Snap>> {
        let _lock = self.core.registry.rl();
        let base = &self.core.base;
        let s = Snap::new_for_applying(base, key, &self.core)?;
        if !s.exists() {
            return Err(VioletaBftStoreError::Other(From::from(format!(
                "snapshot of {:?} not exists.",
                key
            ))));
        }
        Ok(Box::new(s))
    }

    pub fn get_snapshot_for_applying(
        &self,
        key: &SnapKey,
    ) -> VioletaBftStoreResult<Box<dyn GenericSnapshot>> {
        Ok(self.get_concrete_snapshot_for_applying(key)?)
    }

    pub fn get_snapshot_for_applying_to_engine<EK: CausetEngine>(
        &self,
        key: &SnapKey,
    ) -> VioletaBftStoreResult<Box<dyn Snapshot<EK>>> {
        Ok(self.get_concrete_snapshot_for_applying(key)?)
    }

    /// Get the approximate size of snap file exists in snap directory.
    ///
    /// Return value is not guaranteed to be accurate.
    pub fn get_total_snap_size(&self) -> u64 {
        self.core.snap_size.load(Ordering::SeqCst)
    }

    pub fn max_total_snap_size(&self) -> u64 {
        self.max_total_size
    }

    pub fn register(&self, key: SnapKey, entry: SnapEntry) {
        debug!(
            "register snapshot";
            "key" => %key,
            "entry" => ?entry,
        );
        match self.core.registry.wl().entry(key) {
            Entry::Occupied(mut e) => {
                if e.get().contains(&entry) {
                    warn!(
                        "snap key is registered more than once!";
                        "key" => %e.key(),
                    );
                    return;
                }
                e.get_mut().push(entry);
            }
            Entry::Vacant(e) => {
                e.insert(vec![entry]);
            }
        }
    }

    pub fn deregister(&self, key: &SnapKey, entry: &SnapEntry) {
        debug!(
            "deregister snapshot";
            "key" => %key,
            "entry" => ?entry,
        );
        let mut need_clean = false;
        let mut handled = false;
        let registry = &mut self.core.registry.wl();
        if let Some(e) = registry.get_mut(key) {
            let last_len = e.len();
            e.retain(|e| e != entry);
            need_clean = e.is_empty();
            handled = last_len > e.len();
        }
        if need_clean {
            registry.remove(key);
        }
        if handled {
            return;
        }
        warn!(
            "stale deregister snapshot";
            "key" => %key,
            "entry" => ?entry,
        );
    }

    pub fn stats(&self) -> SnapStats {
        // lightlike_count, generating_count, receiving_count, applying_count
        let (mut lightlikeing_cnt, mut receiving_cnt) = (0, 0);
        for v in self.core.registry.rl().values() {
            let (mut is_lightlikeing, mut is_receiving) = (false, false);
            for s in v {
                match *s {
                    SnapEntry::lightlikeing | SnapEntry::Generating => is_lightlikeing = true,
                    SnapEntry::Receiving | SnapEntry::Applying => is_receiving = true,
                }
            }
            if is_lightlikeing {
                lightlikeing_cnt += 1;
            }
            if is_receiving {
                receiving_cnt += 1;
            }
        }

        SnapStats {
            lightlikeing_count: lightlikeing_cnt,
            receiving_count: receiving_cnt,
        }
    }

    pub fn delete_snapshot(
        &self,
        key: &SnapKey,
        snap: &dyn GenericSnapshot,
        check_entry: bool,
    ) -> bool {
        self.core.delete_snapshot(key, snap, check_entry)
    }
}

impl SnapManagerCore {
    // Return true if it successfully delete the specified snapshot.
    fn delete_snapshot(
        &self,
        key: &SnapKey,
        snap: &dyn GenericSnapshot,
        check_entry: bool,
    ) -> bool {
        let registry = self.registry.rl();
        if check_entry {
            if let Some(e) = registry.get(key) {
                if e.len() > 1 {
                    info!(
                        "skip to delete snapshot since it's registered more than once";
                        "snapshot" => %snap.path(),
                        "registered_entries" => ?e,
                    );
                    return false;
                }
            }
        } else if registry.contains_key(key) {
            info!(
                "skip to delete snapshot since it's registered";
                "snapshot" => %snap.path(),
            );
            return false;
        }
        snap.delete();
        true
    }

    fn rename_tmp_causet_file_for_lightlike(&self, causet_file: &mut CfFile) -> VioletaBftStoreResult<()> {
        fs::rename(&causet_file.tmp_path, &causet_file.path)?;
        let mgr = self.encryption_key_manager.as_ref();
        if let Some(mgr) = &mgr {
            let src = causet_file.tmp_path.to_str().unwrap();
            let dst = causet_file.path.to_str().unwrap();
            // It's ok that the causet file is moved but machine fails before `mgr.rename_file`
            // because without metadata file, saved causet files are nothing.
            mgr.rename_file(src, dst)?;
        }
        let (checksum, size) = calc_checksum_and_size(&causet_file.path, mgr)?;
        causet_file.checksum = checksum;
        causet_file.size = size;
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct SnapManagerBuilder {
    max_write_bytes_per_sec: i64,
    max_total_size: u64,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl SnapManagerBuilder {
    pub fn max_write_bytes_per_sec(mut self, bytes: i64) -> SnapManagerBuilder {
        self.max_write_bytes_per_sec = bytes;
        self
    }
    pub fn max_total_size(mut self, bytes: u64) -> SnapManagerBuilder {
        self.max_total_size = bytes;
        self
    }
    pub fn encryption_key_manager(mut self, m: Option<Arc<DataKeyManager>>) -> SnapManagerBuilder {
        self.key_manager = m;
        self
    }
    pub fn build<T: Into<String>>(self, path: T) -> SnapManager {
        let limiter = Limiter::new(if self.max_write_bytes_per_sec > 0 {
            self.max_write_bytes_per_sec as f64
        } else {
            INFINITY
        });
        let max_total_size = if self.max_total_size > 0 {
            self.max_total_size
        } else {
            u64::MAX
        };
        SnapManager {
            core: SnapManagerCore {
                base: path.into(),
                registry: Arc::new(RwLock::new(map![])),
                limiter,
                snap_size: Arc::new(AtomicU64::new(0)),
                encryption_key_manager: self.key_manager,
            },
            max_total_size,
        }
    }
}

#[causet(test)]
pub mod tests {
    use std::cmp;
    use std::f64::INFINITY;
    use std::fs::{self, File, OpenOptions};
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    use engine_lmdb::raw::{DBOptions, Env, DB};
    use engine_lmdb::raw_util::CausetOptions;
    use engine_lmdb::{Compat, LmdbEngine, LmdbSnapshot};
    use edb::Engines;
    use edb::{Iterable, Peekable, SyncMuBlock};
    use edb::{ALL_CausetS, Causet_DEFAULT, Causet_DAGGER, Causet_VIOLETABFT, Causet_WRITE};
    use ekvproto::meta_timeshare::{Peer, Brane};
    use ekvproto::violetabft_server_timeshare::{
        VioletaBftApplyState, VioletaBftSnapshotData, BraneLocalState, SnapshotMeta,
    };
    use violetabft::evioletabft_timeshare::Entry;

    use protobuf::Message;
    use tempfile::{Builder, TempDir};
    use violetabftstore::interlock::::time::Limiter;

    use super::{
        ApplyOptions, GenericSnapshot, Snap, SnapEntry, SnapKey, SnapManager, SnapManagerBuilder,
        SnapManagerCore, Snapshot, SnapshotStatistics, META_FILE_SUFFIX, SNAPSHOT_CausetS,
        SNAP_GEN_PREFIX,
    };

    use crate::interlock::InterlockHost;
    use crate::store::peer_causet_storage::JOB_STATUS_RUNNING;
    use crate::store::{INIT_EPOCH_CONF_VER, INIT_EPOCH_VER};
    use crate::Result;

    const TEST_STORE_ID: u64 = 1;
    const TEST_KEY: &[u8] = b"akey";
    const TEST_WRITE_BATCH_SIZE: usize = 10 * 1024 * 1024;
    const TEST_META_FILE_BUFFER_SIZE: usize = 1000;
    const BYTE_SIZE: usize = 1;

    type DBBuilder = fn(
        p: &Path,
        db_opt: Option<DBOptions>,
        causet_opts: Option<Vec<CausetOptions<'_>>>,
    ) -> Result<Arc<DB>>;

    pub fn open_test_empty_db(
        path: &Path,
        db_opt: Option<DBOptions>,
        causet_opts: Option<Vec<CausetOptions<'_>>>,
    ) -> Result<Arc<DB>> {
        let p = path.to_str().unwrap();
        let db = engine_lmdb::raw_util::new_engine(p, db_opt, ALL_CausetS, causet_opts).unwrap();
        Ok(Arc::new(db))
    }

    pub fn open_test_db(
        path: &Path,
        db_opt: Option<DBOptions>,
        causet_opts: Option<Vec<CausetOptions<'_>>>,
    ) -> Result<Arc<DB>> {
        let p = path.to_str().unwrap();
        let db = engine_lmdb::raw_util::new_engine(p, db_opt, ALL_CausetS, causet_opts).unwrap();
        let db = Arc::new(db);
        let key = tuplespaceInstanton::data_key(TEST_KEY);
        // write some data into each causet
        for (i, causet) in db.causet_names().into_iter().enumerate() {
            let mut p = Peer::default();
            p.set_store_id(TEST_STORE_ID);
            p.set_id((i + 1) as u64);
            db.c().put_msg_causet(causet, &key[..], &p)?;
        }
        Ok(db)
    }

    pub fn get_test_db_for_branes(
        path: &TempDir,
        violetabft_db_opt: Option<DBOptions>,
        violetabft_causet_opt: Option<CausetOptions<'_>>,
        kv_db_opt: Option<DBOptions>,
        kv_causet_opts: Option<Vec<CausetOptions<'_>>>,
        branes: &[u64],
    ) -> Result<Engines<LmdbEngine, LmdbEngine>> {
        let p = path.path();
        let kv = open_test_db(p.join("kv").as_path(), kv_db_opt, kv_causet_opts)?;
        let violetabft = open_test_db(
            p.join("violetabft").as_path(),
            violetabft_db_opt,
            violetabft_causet_opt.map(|opt| vec![opt]),
        )?;
        for &brane_id in branes {
            // Put apply state into kv engine.
            let mut apply_state = VioletaBftApplyState::default();
            let mut apply_entry = Entry::default();
            apply_state.set_applied_index(10);
            apply_entry.set_index(10);
            apply_entry.set_term(0);
            apply_state.mut_truncated_state().set_index(10);
            kv.c()
                .put_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(brane_id), &apply_state)?;
            violetabft.c()
                .put_msg(&tuplespaceInstanton::violetabft_log_key(brane_id, 10), &apply_entry)?;

            // Put brane info into kv engine.
            let brane = gen_test_brane(brane_id, 1, 1);
            let mut brane_state = BraneLocalState::default();
            brane_state.set_brane(brane);
            kv.c()
                .put_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(brane_id), &brane_state)?;
        }
        Ok(Engines {
            kv: kv.c().clone(),
            violetabft: violetabft.c().clone(),
        })
    }

    pub fn get_kv_count(snap: &LmdbSnapshot) -> usize {
        let mut kv_count = 0;
        for causet in SNAPSHOT_CausetS {
            snap.scan_causet(
                causet,
                &tuplespaceInstanton::data_key(b"a"),
                &tuplespaceInstanton::data_key(b"z"),
                false,
                |_, _| {
                    kv_count += 1;
                    Ok(true)
                },
            )
            .unwrap();
        }
        kv_count
    }

    pub fn gen_test_brane(brane_id: u64, store_id: u64, peer_id: u64) -> Brane {
        let mut peer = Peer::default();
        peer.set_store_id(store_id);
        peer.set_id(peer_id);
        let mut brane = Brane::default();
        brane.set_id(brane_id);
        brane.set_spacelike_key(b"a".to_vec());
        brane.set_lightlike_key(b"z".to_vec());
        brane.mut_brane_epoch().set_version(INIT_EPOCH_VER);
        brane.mut_brane_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
        brane.mut_peers().push(peer);
        brane
    }

    pub fn assert_eq_db(expected_db: &Arc<DB>, db: &Arc<DB>) {
        let key = tuplespaceInstanton::data_key(TEST_KEY);
        for causet in SNAPSHOT_CausetS {
            let p1: Option<Peer> = expected_db.c().get_msg_causet(causet, &key[..]).unwrap();
            if let Some(p1) = p1 {
                let p2: Option<Peer> = db.c().get_msg_causet(causet, &key[..]).unwrap();
                if let Some(p2) = p2 {
                    if p2 != p1 {
                        panic!(
                            "causet {}: key {:?}, value {:?}, expected {:?}",
                            causet, key, p2, p1
                        );
                    }
                } else {
                    panic!("causet {}: expect key {:?} has value", causet, key);
                }
            }
        }
    }

    fn create_manager_core(path: &str) -> SnapManagerCore {
        SnapManagerCore {
            base: path.to_owned(),
            registry: Arc::new(RwLock::new(map![])),
            limiter: Limiter::new(INFINITY),
            snap_size: Arc::new(AtomicU64::new(0)),
            encryption_key_manager: None,
        }
    }

    pub fn gen_db_options_with_encryption() -> DBOptions {
        let env = Arc::new(Env::new_default_ctr_encrypted_env(b"abcd").unwrap());
        let mut db_opt = DBOptions::new();
        db_opt.set_env(env);
        db_opt
    }

    #[test]
    fn test_gen_snapshot_meta() {
        let mut causet_file = Vec::with_capacity(super::SNAPSHOT_CausetS.len());
        for (i, causet) in super::SNAPSHOT_CausetS.iter().enumerate() {
            let f = super::CfFile {
                causet,
                size: 100 * (i + 1) as u64,
                checksum: 1000 * (i + 1) as u32,
                ..Default::default()
            };
            causet_file.push(f);
        }
        let meta = super::gen_snapshot_meta(&causet_file).unwrap();
        for (i, causet_file_meta) in meta.get_causet_files().iter().enumerate() {
            if causet_file_meta.get_causet() != causet_file[i].causet {
                panic!(
                    "{}: expect causet {}, got {}",
                    i,
                    causet_file[i].causet,
                    causet_file_meta.get_causet()
                );
            }
            if causet_file_meta.get_size() != causet_file[i].size {
                panic!(
                    "{}: expect causet size {}, got {}",
                    i,
                    causet_file[i].size,
                    causet_file_meta.get_size()
                );
            }
            if causet_file_meta.get_checksum() != causet_file[i].checksum {
                panic!(
                    "{}: expect causet checksum {}, got {}",
                    i,
                    causet_file[i].checksum,
                    causet_file_meta.get_checksum()
                );
            }
        }
    }

    #[test]
    fn test_display_path() {
        let dir = Builder::new()
            .prefix("test-display-path")
            .temfidelir()
            .unwrap();
        let key = SnapKey::new(1, 1, 1);
        let prefix = format!("{}_{}", SNAP_GEN_PREFIX, key);
        let display_path = Snap::get_display_path(dir.path(), &prefix);
        assert_ne!(display_path, "");
    }

    #[test]
    fn test_empty_snap_file() {
        test_snap_file(open_test_empty_db, None);
        test_snap_file(open_test_empty_db, Some(gen_db_options_with_encryption()));
    }

    #[test]
    fn test_non_empty_snap_file() {
        test_snap_file(open_test_db, None);
        test_snap_file(open_test_db, Some(gen_db_options_with_encryption()));
    }

    fn test_snap_file(get_db: DBBuilder, db_opt: Option<DBOptions>) {
        let brane_id = 1;
        let brane = gen_test_brane(brane_id, 1, 1);
        let src_db_dir = Builder::new()
            .prefix("test-snap-file-db-src")
            .temfidelir()
            .unwrap();
        let db = get_db(&src_db_dir.path(), db_opt.clone(), None).unwrap();
        let snapshot = LmdbSnapshot::new(Arc::clone(&db));

        let src_dir = Builder::new()
            .prefix("test-snap-file-db-src")
            .temfidelir()
            .unwrap();

        let key = SnapKey::new(brane_id, 1, 1);

        let mgr_core = create_manager_core(src_dir.path().to_str().unwrap());

        let mut s1 = Snap::new_for_building(src_dir.path(), &key, &mgr_core).unwrap();

        // Ensure that this snapshot file doesn't exist before being built.
        assert!(!s1.exists());
        assert_eq!(mgr_core.snap_size.load(Ordering::SeqCst), 0);

        let mut snap_data = VioletaBftSnapshotData::default();
        snap_data.set_brane(brane.clone());
        let mut stat = SnapshotStatistics::new();
        Snapshot::<LmdbEngine>::build(
            &mut s1,
            db.c(),
            &snapshot,
            &brane,
            &mut snap_data,
            &mut stat,
        )
        .unwrap();

        // Ensure that this snapshot file does exist after being built.
        assert!(s1.exists());
        let total_size = s1.total_size().unwrap();
        // Ensure the `size_track` is modified correctly.
        let size = mgr_core.snap_size.load(Ordering::SeqCst);
        assert_eq!(size, total_size);
        assert_eq!(stat.size as u64, size);
        assert_eq!(stat.kv_count, get_kv_count(&snapshot));

        // Ensure this snapshot could be read for lightlikeing.
        let mut s2 = Snap::new_for_lightlikeing(src_dir.path(), &key, &mgr_core).unwrap();
        assert!(s2.exists());

        // TODO check meta data correct.
        let _ = s2.meta().unwrap();

        let dst_dir = Builder::new()
            .prefix("test-snap-file-dst")
            .temfidelir()
            .unwrap();

        let mut s3 =
            Snap::new_for_receiving(dst_dir.path(), &key, &mgr_core, snap_data.take_meta())
                .unwrap();
        assert!(!s3.exists());

        // Ensure snapshot data could be read out of `s2`, and write into `s3`.
        let copy_size = io::copy(&mut s2, &mut s3).unwrap();
        assert_eq!(copy_size, size);
        assert!(!s3.exists());
        s3.save().unwrap();
        assert!(s3.exists());

        // Ensure the tracked size is handled correctly after receiving a snapshot.
        assert_eq!(mgr_core.snap_size.load(Ordering::SeqCst), size * 2);

        // Ensure `delete()` works to delete the source snapshot.
        s2.delete();
        assert!(!s2.exists());
        assert!(!s1.exists());
        assert_eq!(mgr_core.snap_size.load(Ordering::SeqCst), size);

        // Ensure a snapshot could be applied to DB.
        let mut s4 = Snap::new_for_applying(dst_dir.path(), &key, &mgr_core).unwrap();
        assert!(s4.exists());

        let dst_db_dir = Builder::new()
            .prefix("test-snap-file-dst")
            .temfidelir()
            .unwrap();
        let dst_db_path = dst_db_dir.path().to_str().unwrap();
        // Change arbitrarily the causet order of ALL_CausetS at destination db.
        let dst_causets = [Causet_WRITE, Causet_DEFAULT, Causet_DAGGER, Causet_VIOLETABFT];
        let dst_db = Arc::new(
            engine_lmdb::raw_util::new_engine(dst_db_path, db_opt, &dst_causets, None).unwrap(),
        );
        let options = ApplyOptions {
            db: dst_db.c().clone(),
            brane,
            abort: Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)),
            write_batch_size: TEST_WRITE_BATCH_SIZE,
            interlock_host: InterlockHost::<LmdbEngine>::default(),
        };
        // Verify thte snapshot applying is ok.
        assert!(s4.apply(options).is_ok());

        // Ensure `delete()` works to delete the dest snapshot.
        s4.delete();
        assert!(!s4.exists());
        assert!(!s3.exists());
        assert_eq!(mgr_core.snap_size.load(Ordering::SeqCst), 0);

        // Verify the data is correct after applying snapshot.
        assert_eq_db(&db, &dst_db);
    }

    #[test]
    fn test_empty_snap_validation() {
        test_snap_validation(open_test_empty_db);
    }

    #[test]
    fn test_non_empty_snap_validation() {
        test_snap_validation(open_test_db);
    }

    fn test_snap_validation(get_db: DBBuilder) {
        let brane_id = 1;
        let brane = gen_test_brane(brane_id, 1, 1);
        let db_dir = Builder::new()
            .prefix("test-snap-validation-db")
            .temfidelir()
            .unwrap();
        let db = get_db(&db_dir.path(), None, None).unwrap();
        let snapshot = LmdbSnapshot::new(Arc::clone(&db));

        let dir = Builder::new()
            .prefix("test-snap-validation")
            .temfidelir()
            .unwrap();
        let key = SnapKey::new(brane_id, 1, 1);
        let mgr_core = create_manager_core(dir.path().to_str().unwrap());

        let mut s1 = Snap::new_for_building(dir.path(), &key, &mgr_core).unwrap();
        assert!(!s1.exists());

        let mut snap_data = VioletaBftSnapshotData::default();
        snap_data.set_brane(brane.clone());
        let mut stat = SnapshotStatistics::new();
        Snapshot::<LmdbEngine>::build(
            &mut s1,
            db.c(),
            &snapshot,
            &brane,
            &mut snap_data,
            &mut stat,
        )
        .unwrap();
        assert!(s1.exists());

        let mut s2 = Snap::new_for_building(dir.path(), &key, &mgr_core).unwrap();
        assert!(s2.exists());

        Snapshot::<LmdbEngine>::build(
            &mut s2,
            db.c(),
            &snapshot,
            &brane,
            &mut snap_data,
            &mut stat,
        )
        .unwrap();
        assert!(s2.exists());
    }

    // Make all the snapshot in the specified dir corrupted to have incorrect size.
    fn corrupt_snapshot_size_in<T: Into<PathBuf>>(dir: T) {
        let dir_path = dir.into();
        let read_dir = fs::read_dir(dir_path).unwrap();
        for p in read_dir {
            if p.is_ok() {
                let e = p.as_ref().unwrap();
                if !e
                    .file_name()
                    .into_string()
                    .unwrap()
                    .lightlikes_with(META_FILE_SUFFIX)
                {
                    let mut f = OpenOptions::new().applightlike(true).open(e.path()).unwrap();
                    f.write_all(b"xxxxx").unwrap();
                }
            }
        }
    }

    // Make all the snapshot in the specified dir corrupted to have incorrect checksum.
    fn corrupt_snapshot_checksum_in<T: Into<PathBuf>>(dir: T) -> Vec<SnapshotMeta> {
        let dir_path = dir.into();
        let mut res = Vec::new();
        let read_dir = fs::read_dir(dir_path).unwrap();
        for p in read_dir {
            if p.is_ok() {
                let e = p.as_ref().unwrap();
                if e.file_name()
                    .into_string()
                    .unwrap()
                    .lightlikes_with(META_FILE_SUFFIX)
                {
                    let mut snapshot_meta = SnapshotMeta::default();
                    let mut buf = Vec::with_capacity(TEST_META_FILE_BUFFER_SIZE);
                    {
                        let mut f = OpenOptions::new().read(true).open(e.path()).unwrap();
                        f.read_to_lightlike(&mut buf).unwrap();
                    }

                    snapshot_meta.merge_from_bytes(&buf).unwrap();

                    for causet in snapshot_meta.mut_causet_files().iter_mut() {
                        let corrupted_checksum = causet.get_checksum() + 100;
                        causet.set_checksum(corrupted_checksum);
                    }

                    let buf = snapshot_meta.write_to_bytes().unwrap();
                    {
                        let mut f = OpenOptions::new()
                            .write(true)
                            .truncate(true)
                            .open(e.path())
                            .unwrap();
                        f.write_all(&buf[..]).unwrap();
                        f.flush().unwrap();
                    }

                    res.push(snapshot_meta);
                }
            }
        }
        res
    }

    // Make all the snapshot meta files in the specified corrupted to have incorrect content.
    fn corrupt_snapshot_meta_file<T: Into<PathBuf>>(dir: T) -> usize {
        let mut total = 0;
        let dir_path = dir.into();
        let read_dir = fs::read_dir(dir_path).unwrap();
        for p in read_dir {
            if p.is_ok() {
                let e = p.as_ref().unwrap();
                if e.file_name()
                    .into_string()
                    .unwrap()
                    .lightlikes_with(META_FILE_SUFFIX)
                {
                    let mut f = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(e.path())
                        .unwrap();
                    // Make the last byte of the meta file corrupted
                    // by turning over all bits of it
                    let pos = SeekFrom::End(-(BYTE_SIZE as i64));
                    f.seek(pos).unwrap();
                    let mut buf = [0; BYTE_SIZE];
                    f.read_exact(&mut buf[..]).unwrap();
                    buf[0] ^= u8::max_value();
                    f.seek(pos).unwrap();
                    f.write_all(&buf[..]).unwrap();
                    total += 1;
                }
            }
        }
        total
    }

    fn copy_snapshot(
        from_dir: &TempDir,
        to_dir: &TempDir,
        key: &SnapKey,
        mgr: &SnapManagerCore,
        snapshot_meta: SnapshotMeta,
    ) {
        let mut from = Snap::new_for_lightlikeing(from_dir.path(), key, mgr).unwrap();
        assert!(from.exists());

        let mut to = Snap::new_for_receiving(to_dir.path(), key, mgr, snapshot_meta).unwrap();

        assert!(!to.exists());
        let _ = io::copy(&mut from, &mut to).unwrap();
        to.save().unwrap();
        assert!(to.exists());
    }

    #[test]
    fn test_snap_corruption_on_size_or_checksum() {
        let brane_id = 1;
        let brane = gen_test_brane(brane_id, 1, 1);
        let db_dir = Builder::new()
            .prefix("test-snap-corruption-db")
            .temfidelir()
            .unwrap();
        let db = open_test_db(&db_dir.path(), None, None).unwrap();
        let snapshot = LmdbSnapshot::new(db.clone());

        let dir = Builder::new()
            .prefix("test-snap-corruption")
            .temfidelir()
            .unwrap();
        let key = SnapKey::new(brane_id, 1, 1);
        let mgr_core = create_manager_core(dir.path().to_str().unwrap());
        let mut s1 = Snap::new_for_building(dir.path(), &key, &mgr_core).unwrap();
        assert!(!s1.exists());

        let mut snap_data = VioletaBftSnapshotData::default();
        snap_data.set_brane(brane.clone());
        let mut stat = SnapshotStatistics::new();
        Snapshot::<LmdbEngine>::build(
            &mut s1,
            db.c(),
            &snapshot,
            &brane,
            &mut snap_data,
            &mut stat,
        )
        .unwrap();
        assert!(s1.exists());

        corrupt_snapshot_size_in(dir.path());

        assert!(Snap::new_for_lightlikeing(dir.path(), &key, &mgr_core,).is_err());

        let mut s2 = Snap::new_for_building(dir.path(), &key, &mgr_core).unwrap();
        assert!(!s2.exists());
        Snapshot::<LmdbEngine>::build(
            &mut s2,
            db.c(),
            &snapshot,
            &brane,
            &mut snap_data,
            &mut stat,
        )
        .unwrap();
        assert!(s2.exists());

        let dst_dir = Builder::new()
            .prefix("test-snap-corruption-dst")
            .temfidelir()
            .unwrap();
        copy_snapshot(
            &dir,
            &dst_dir,
            &key,
            &mgr_core,
            snap_data.get_meta().clone(),
        );

        let mut metas = corrupt_snapshot_checksum_in(dst_dir.path());
        assert_eq!(1, metas.len());
        let snap_meta = metas.pop().unwrap();

        let mut s5 = Snap::new_for_applying(dst_dir.path(), &key, &mgr_core).unwrap();
        assert!(s5.exists());

        let dst_db_dir = Builder::new()
            .prefix("test-snap-corruption-dst-db")
            .temfidelir()
            .unwrap();
        let dst_db = open_test_empty_db(&dst_db_dir.path(), None, None).unwrap();
        let options = ApplyOptions {
            db: dst_db.c().clone(),
            brane,
            abort: Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)),
            write_batch_size: TEST_WRITE_BATCH_SIZE,
            interlock_host: InterlockHost::<LmdbEngine>::default(),
        };
        assert!(s5.apply(options).is_err());

        corrupt_snapshot_size_in(dst_dir.path());
        assert!(Snap::new_for_receiving(dst_dir.path(), &key, &mgr_core, snap_meta,).is_err());
        assert!(Snap::new_for_applying(dst_dir.path(), &key, &mgr_core).is_err());
    }

    #[test]
    fn test_snap_corruption_on_meta_file() {
        let brane_id = 1;
        let brane = gen_test_brane(brane_id, 1, 1);
        let db_dir = Builder::new()
            .prefix("test-snapshot-corruption-meta-db")
            .temfidelir()
            .unwrap();
        let db = open_test_db(&db_dir.path(), None, None).unwrap();
        let snapshot = LmdbSnapshot::new(db.clone());

        let dir = Builder::new()
            .prefix("test-snap-corruption-meta")
            .temfidelir()
            .unwrap();
        let key = SnapKey::new(brane_id, 1, 1);
        let mgr_core = create_manager_core(dir.path().to_str().unwrap());
        let mut s1 = Snap::new_for_building(dir.path(), &key, &mgr_core).unwrap();
        assert!(!s1.exists());

        let mut snap_data = VioletaBftSnapshotData::default();
        snap_data.set_brane(brane.clone());
        let mut stat = SnapshotStatistics::new();
        Snapshot::<LmdbEngine>::build(
            &mut s1,
            db.c(),
            &snapshot,
            &brane,
            &mut snap_data,
            &mut stat,
        )
        .unwrap();
        assert!(s1.exists());

        assert_eq!(1, corrupt_snapshot_meta_file(dir.path()));

        assert!(Snap::new_for_lightlikeing(dir.path(), &key, &mgr_core,).is_err());

        let mut s2 = Snap::new_for_building(dir.path(), &key, &mgr_core).unwrap();
        assert!(!s2.exists());
        Snapshot::<LmdbEngine>::build(
            &mut s2,
            db.c(),
            &snapshot,
            &brane,
            &mut snap_data,
            &mut stat,
        )
        .unwrap();
        assert!(s2.exists());

        let dst_dir = Builder::new()
            .prefix("test-snap-corruption-meta-dst")
            .temfidelir()
            .unwrap();
        copy_snapshot(
            &dir,
            &dst_dir,
            &key,
            &mgr_core,
            snap_data.get_meta().clone(),
        );

        assert_eq!(1, corrupt_snapshot_meta_file(dst_dir.path()));

        assert!(Snap::new_for_applying(dst_dir.path(), &key, &mgr_core,).is_err());
        assert!(
            Snap::new_for_receiving(dst_dir.path(), &key, &mgr_core, snap_data.take_meta(),)
                .is_err()
        );
    }

    #[test]
    fn test_snap_mgr_create_dir() {
        // Ensure `mgr` creates the specified directory when it does not exist.
        let temp_dir = Builder::new()
            .prefix("test-snap-mgr-create-dir")
            .temfidelir()
            .unwrap();
        let temp_path = temp_dir.path().join("snap1");
        let path = temp_path.to_str().unwrap().to_owned();
        assert!(!temp_path.exists());
        let mut mgr = SnapManager::new(path);
        mgr.init().unwrap();
        assert!(temp_path.exists());

        // Ensure `init()` will return an error if specified target is a file.
        let temp_path2 = temp_dir.path().join("snap2");
        let path2 = temp_path2.to_str().unwrap().to_owned();
        File::create(temp_path2).unwrap();
        mgr = SnapManager::new(path2);
        assert!(mgr.init().is_err());
    }

    #[test]
    fn test_snap_mgr_v2() {
        let temp_dir = Builder::new().prefix("test-snap-mgr-v2").temfidelir().unwrap();
        let path = temp_dir.path().to_str().unwrap().to_owned();
        let mgr = SnapManager::new(path.clone());
        mgr.init().unwrap();
        assert_eq!(mgr.get_total_snap_size(), 0);

        let db_dir = Builder::new()
            .prefix("test-snap-mgr-delete-temp-files-v2-db")
            .temfidelir()
            .unwrap();
        let db = open_test_db(&db_dir.path(), None, None).unwrap();
        let snapshot = LmdbSnapshot::new(db.clone());
        let key1 = SnapKey::new(1, 1, 1);
        let mgr_core = create_manager_core(&path);
        let mut s1 = Snap::new_for_building(&path, &key1, &mgr_core).unwrap();
        let mut brane = gen_test_brane(1, 1, 1);
        let mut snap_data = VioletaBftSnapshotData::default();
        snap_data.set_brane(brane.clone());
        let mut stat = SnapshotStatistics::new();
        Snapshot::<LmdbEngine>::build(
            &mut s1,
            db.c(),
            &snapshot,
            &brane,
            &mut snap_data,
            &mut stat,
        )
        .unwrap();
        let mut s = Snap::new_for_lightlikeing(&path, &key1, &mgr_core).unwrap();
        let expected_size = s.total_size().unwrap();
        let mut s2 =
            Snap::new_for_receiving(&path, &key1, &mgr_core, snap_data.get_meta().clone()).unwrap();
        let n = io::copy(&mut s, &mut s2).unwrap();
        assert_eq!(n, expected_size);
        s2.save().unwrap();

        let key2 = SnapKey::new(2, 1, 1);
        brane.set_id(2);
        snap_data.set_brane(brane);
        let s3 = Snap::new_for_building(&path, &key2, &mgr_core).unwrap();
        let s4 = Snap::new_for_receiving(&path, &key2, &mgr_core, snap_data.take_meta()).unwrap();

        assert!(s1.exists());
        assert!(s2.exists());
        assert!(!s3.exists());
        assert!(!s4.exists());

        let mgr = SnapManager::new(path);
        mgr.init().unwrap();
        assert_eq!(mgr.get_total_snap_size(), expected_size * 2);

        assert!(s1.exists());
        assert!(s2.exists());
        assert!(!s3.exists());
        assert!(!s4.exists());

        mgr.get_snapshot_for_lightlikeing(&key1).unwrap().delete();
        assert_eq!(mgr.get_total_snap_size(), expected_size);
        mgr.get_snapshot_for_applying(&key1).unwrap().delete();
        assert_eq!(mgr.get_total_snap_size(), 0);
    }

    fn check_registry_around_deregister(mgr: SnapManager, key: &SnapKey, entry: &SnapEntry) {
        let snap_tuplespaceInstanton = mgr.list_idle_snap().unwrap();
        assert!(snap_tuplespaceInstanton.is_empty());
        assert!(mgr.has_registered(key));
        mgr.deregister(key, entry);
        let mut snap_tuplespaceInstanton = mgr.list_idle_snap().unwrap();
        assert_eq!(snap_tuplespaceInstanton.len(), 1);
        let snap_key = snap_tuplespaceInstanton.pop().unwrap().0;
        assert_eq!(snap_key, *key);
        assert!(!mgr.has_registered(&snap_key));
    }

    #[test]
    fn test_snap_deletion_on_registry() {
        let src_temp_dir = Builder::new()
            .prefix("test-snap-deletion-on-registry-src")
            .temfidelir()
            .unwrap();
        let src_path = src_temp_dir.path().to_str().unwrap().to_owned();
        let src_mgr = SnapManager::new(src_path);
        src_mgr.init().unwrap();

        let src_db_dir = Builder::new()
            .prefix("test-snap-deletion-on-registry-src-db")
            .temfidelir()
            .unwrap();
        let db = open_test_db(&src_db_dir.path(), None, None).unwrap();
        let snapshot = LmdbSnapshot::new(db.clone());

        let key = SnapKey::new(1, 1, 1);
        let brane = gen_test_brane(1, 1, 1);

        // Ensure the snapshot being built will not be deleted on GC.
        src_mgr.register(key.clone(), SnapEntry::Generating);
        let mut s1 = src_mgr.get_snapshot_for_building(&key).unwrap();
        let mut snap_data = VioletaBftSnapshotData::default();
        snap_data.set_brane(brane.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(db.c(), &snapshot, &brane, &mut snap_data, &mut stat)
            .unwrap();
        let v = snap_data.write_to_bytes().unwrap();

        check_registry_around_deregister(src_mgr.clone(), &key, &SnapEntry::Generating);

        // Ensure the snapshot being sent will not be deleted on GC.
        src_mgr.register(key.clone(), SnapEntry::lightlikeing);
        let mut s2 = src_mgr.get_snapshot_for_lightlikeing(&key).unwrap();
        let expected_size = s2.total_size().unwrap();

        let dst_temp_dir = Builder::new()
            .prefix("test-snap-deletion-on-registry-dst")
            .temfidelir()
            .unwrap();
        let dst_path = dst_temp_dir.path().to_str().unwrap().to_owned();
        let dst_mgr = SnapManager::new(dst_path);
        dst_mgr.init().unwrap();

        // Ensure the snapshot being received will not be deleted on GC.
        dst_mgr.register(key.clone(), SnapEntry::Receiving);
        let mut s3 = dst_mgr.get_snapshot_for_receiving(&key, &v[..]).unwrap();
        let n = io::copy(&mut s2, &mut s3).unwrap();
        assert_eq!(n, expected_size);
        s3.save().unwrap();

        check_registry_around_deregister(src_mgr.clone(), &key, &SnapEntry::lightlikeing);
        check_registry_around_deregister(dst_mgr.clone(), &key, &SnapEntry::Receiving);

        // Ensure the snapshot to be applied will not be deleted on GC.
        let mut snap_tuplespaceInstanton = dst_mgr.list_idle_snap().unwrap();
        assert_eq!(snap_tuplespaceInstanton.len(), 1);
        let snap_key = snap_tuplespaceInstanton.pop().unwrap().0;
        assert_eq!(snap_key, key);
        assert!(!dst_mgr.has_registered(&snap_key));
        dst_mgr.register(key.clone(), SnapEntry::Applying);
        let s4 = dst_mgr.get_snapshot_for_applying(&key).unwrap();
        let s5 = dst_mgr.get_snapshot_for_applying(&key).unwrap();
        dst_mgr.delete_snapshot(&key, s4.as_ref(), false);
        assert!(s5.exists());
    }

    #[test]
    fn test_snapshot_max_total_size() {
        let branes: Vec<u64> = (0..20).collect();
        let kv_path = Builder::new()
            .prefix("test-snapshot-max-total-size-db")
            .temfidelir()
            .unwrap();
        let engine = get_test_db_for_branes(&kv_path, None, None, None, None, &branes).unwrap();

        let snapfiles_path = Builder::new()
            .prefix("test-snapshot-max-total-size-snapshots")
            .temfidelir()
            .unwrap();
        let max_total_size = 10240;
        let snap_mgr = SnapManagerBuilder::default()
            .max_total_size(max_total_size)
            .build::<_>(snapfiles_path.path().to_str().unwrap());
        let snapshot = LmdbSnapshot::new(engine.kv.as_inner().clone());

        // Add an oldest snapshot for receiving.
        let recv_key = SnapKey::new(100, 100, 100);
        let recv_head = {
            let mut stat = SnapshotStatistics::new();
            let mut snap_data = VioletaBftSnapshotData::default();
            let mut s = snap_mgr.get_snapshot_for_building(&recv_key).unwrap();
            s.build(
                &engine.kv,
                &snapshot,
                &gen_test_brane(100, 1, 1),
                &mut snap_data,
                &mut stat,
            )
            .unwrap();
            snap_data.write_to_bytes().unwrap()
        };
        let recv_remain = {
            let mut data = Vec::with_capacity(1024);
            let mut s = snap_mgr.get_snapshot_for_lightlikeing(&recv_key).unwrap();
            s.read_to_lightlike(&mut data).unwrap();
            assert!(snap_mgr.delete_snapshot(&recv_key, s.as_ref(), true));
            data
        };
        let mut s = snap_mgr
            .get_snapshot_for_receiving(&recv_key, &recv_head)
            .unwrap();
        s.write_all(&recv_remain).unwrap();
        s.save().unwrap();

        for (i, brane_id) in branes.into_iter().enumerate() {
            let key = SnapKey::new(brane_id, 1, 1);
            let brane = gen_test_brane(brane_id, 1, 1);
            let mut s = snap_mgr.get_snapshot_for_building(&key).unwrap();
            let mut snap_data = VioletaBftSnapshotData::default();
            let mut stat = SnapshotStatistics::new();
            s.build(&engine.kv, &snapshot, &brane, &mut snap_data, &mut stat)
                .unwrap();

            // TODO: this size may change in different Lmdb version.
            let snap_size = 1658;
            let max_snap_count = (max_total_size + snap_size - 1) / snap_size;
            // The first snap_size is for brane 100.
            // That snapshot won't be deleted because it's not for generating.
            assert_eq!(
                snap_mgr.get_total_snap_size(),
                snap_size * cmp::min(max_snap_count, (i + 2) as u64)
            );
        }
    }
}
