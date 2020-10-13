// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

//! Multi-version concurrency control functionality.

pub(super) mod metrics;
mod reader;
pub(super) mod txn;

pub use self::metrics::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM};
pub use self::reader::*;
pub use self::txn::{GcInfo, MvccTxn, ReleasedLock, SecondaryLockStatus, MAX_TXN_WRITE_SIZE};
pub use txn_types::{
    Key, Dagger, LockType, Mutation, TimeStamp, Value, Write, WriteRef, WriteType,
    SHORT_VALUE_MAX_LEN,
};

use error_code::{self, ErrorCode, ErrorCodeExt};
use std::error;
use std::fmt;
use std::io;
use einsteindb_util::metrics::CRITICAL_ERROR;
use einsteindb_util::{panic_when_unexpected_key_or_data, set_panic_mark};

quick_error! {
    #[derive(Debug)]
    pub enum ErrorInner {
        Engine(err: crate::causetStorage::kv::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        Io(err: io::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        Codec(err: einsteindb_util::codec::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        KeyIsLocked(info: ekvproto::kvrpcpb::LockInfo) {
            display("key is locked (backoff or cleanup) {:?}", info)
        }
        BadFormat(err: txn_types::Error ) {
            cause(err)
            display("{}", err)
        }
        Committed { commit_ts: TimeStamp } {
            display("txn already committed @{}", commit_ts)
        }
        PessimisticLockRolledBack { spacelike_ts: TimeStamp, key: Vec<u8> } {
            display("pessimistic dagger already rollbacked, spacelike_ts:{}, key:{}", spacelike_ts, hex::encode_upper(key))
        }
        TxnLockNotFound { spacelike_ts: TimeStamp, commit_ts: TimeStamp, key: Vec<u8> } {
            display("txn dagger not found {}-{} key:{}", spacelike_ts, commit_ts, hex::encode_upper(key))
        }
        TxnNotFound { spacelike_ts:  TimeStamp, key: Vec<u8> } {
            display("txn not found {} key: {}", spacelike_ts, hex::encode_upper(key))
        }
        LockTypeNotMatch { spacelike_ts: TimeStamp, key: Vec<u8>, pessimistic: bool } {
            display("dagger type not match, spacelike_ts:{}, key:{}, pessimistic:{}", spacelike_ts, hex::encode_upper(key), pessimistic)
        }
        WriteConflict { spacelike_ts: TimeStamp, conflict_spacelike_ts: TimeStamp, conflict_commit_ts: TimeStamp, key: Vec<u8>, primary: Vec<u8> } {
            display("write conflict, spacelike_ts:{}, conflict_spacelike_ts:{}, conflict_commit_ts:{}, key:{}, primary:{}",
                    spacelike_ts, conflict_spacelike_ts, conflict_commit_ts, hex::encode_upper(key), hex::encode_upper(primary))
        }
        Deadlock { spacelike_ts: TimeStamp, lock_ts: TimeStamp, lock_key: Vec<u8>, deadlock_key_hash: u64 } {
            display("deadlock occurs between txn:{} and txn:{}, lock_key:{}, deadlock_key_hash:{}",
                    spacelike_ts, lock_ts, hex::encode_upper(lock_key), deadlock_key_hash)
        }
        AlreadyExist { key: Vec<u8> } {
            display("key {} already exists", hex::encode_upper(key))
        }
        DefaultNotFound { key: Vec<u8> } {
            display("default not found: key:{}, maybe read truncated/dropped table data?", hex::encode_upper(key))
        }
        CommitTsExpired { spacelike_ts: TimeStamp, commit_ts: TimeStamp, key: Vec<u8>, min_commit_ts: TimeStamp } {
            display("try to commit key {} with commit_ts {} but min_commit_ts is {}", hex::encode_upper(key), commit_ts, min_commit_ts)
        }
        KeyVersion { display("bad format key(version)") }
        PessimisticLockNotFound { spacelike_ts: TimeStamp, key: Vec<u8> } {
            display("pessimistic dagger not found, spacelike_ts:{}, key:{}", spacelike_ts, hex::encode_upper(key))
        }
        Other(err: Box<dyn error::Error + Sync + Slightlike>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }
    }
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match self {
            ErrorInner::Engine(e) => e.maybe_clone().map(ErrorInner::Engine),
            ErrorInner::Codec(e) => e.maybe_clone().map(ErrorInner::Codec),
            ErrorInner::KeyIsLocked(info) => Some(ErrorInner::KeyIsLocked(info.clone())),
            ErrorInner::BadFormat(e) => e.maybe_clone().map(ErrorInner::BadFormat),
            ErrorInner::TxnLockNotFound {
                spacelike_ts,
                commit_ts,
                key,
            } => Some(ErrorInner::TxnLockNotFound {
                spacelike_ts: *spacelike_ts,
                commit_ts: *commit_ts,
                key: key.to_owned(),
            }),
            ErrorInner::TxnNotFound { spacelike_ts, key } => Some(ErrorInner::TxnNotFound {
                spacelike_ts: *spacelike_ts,
                key: key.to_owned(),
            }),
            ErrorInner::LockTypeNotMatch {
                spacelike_ts,
                key,
                pessimistic,
            } => Some(ErrorInner::LockTypeNotMatch {
                spacelike_ts: *spacelike_ts,
                key: key.to_owned(),
                pessimistic: *pessimistic,
            }),
            ErrorInner::WriteConflict {
                spacelike_ts,
                conflict_spacelike_ts,
                conflict_commit_ts,
                key,
                primary,
            } => Some(ErrorInner::WriteConflict {
                spacelike_ts: *spacelike_ts,
                conflict_spacelike_ts: *conflict_spacelike_ts,
                conflict_commit_ts: *conflict_commit_ts,
                key: key.to_owned(),
                primary: primary.to_owned(),
            }),
            ErrorInner::Deadlock {
                spacelike_ts,
                lock_ts,
                lock_key,
                deadlock_key_hash,
            } => Some(ErrorInner::Deadlock {
                spacelike_ts: *spacelike_ts,
                lock_ts: *lock_ts,
                lock_key: lock_key.to_owned(),
                deadlock_key_hash: *deadlock_key_hash,
            }),
            ErrorInner::AlreadyExist { key } => Some(ErrorInner::AlreadyExist { key: key.clone() }),
            ErrorInner::DefaultNotFound { key } => Some(ErrorInner::DefaultNotFound {
                key: key.to_owned(),
            }),
            ErrorInner::CommitTsExpired {
                spacelike_ts,
                commit_ts,
                key,
                min_commit_ts,
            } => Some(ErrorInner::CommitTsExpired {
                spacelike_ts: *spacelike_ts,
                commit_ts: *commit_ts,
                key: key.clone(),
                min_commit_ts: *min_commit_ts,
            }),
            ErrorInner::KeyVersion => Some(ErrorInner::KeyVersion),
            ErrorInner::Committed { commit_ts } => Some(ErrorInner::Committed {
                commit_ts: *commit_ts,
            }),
            ErrorInner::PessimisticLockRolledBack { spacelike_ts, key } => {
                Some(ErrorInner::PessimisticLockRolledBack {
                    spacelike_ts: *spacelike_ts,
                    key: key.to_owned(),
                })
            }
            ErrorInner::PessimisticLockNotFound { spacelike_ts, key } => {
                Some(ErrorInner::PessimisticLockNotFound {
                    spacelike_ts: *spacelike_ts,
                    key: key.to_owned(),
                })
            }
            ErrorInner::Io(_) | ErrorInner::Other(_) => None,
        }
    }
}

pub struct Error(pub Box<ErrorInner>);

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        self.0.maybe_clone().map(Error::from)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.0)
    }
}

impl From<ErrorInner> for Error {
    #[inline]
    fn from(e: ErrorInner) -> Self {
        Error(Box::new(e))
    }
}

impl<T: Into<ErrorInner>> From<T> for Error {
    #[inline]
    default fn from(err: T) -> Self {
        let err = err.into();
        err.into()
    }
}

impl From<codec::Error> for ErrorInner {
    fn from(err: codec::Error) -> Self {
        box_err!("{}", err)
    }
}

impl From<::fidel_client::Error> for ErrorInner {
    fn from(err: ::fidel_client::Error) -> Self {
        box_err!("{}", err)
    }
}

impl From<txn_types::Error> for ErrorInner {
    fn from(err: txn_types::Error) -> Self {
        match err {
            txn_types::Error(box txn_types::ErrorInner::Io(e)) => ErrorInner::Io(e),
            txn_types::Error(box txn_types::ErrorInner::Codec(e)) => ErrorInner::Codec(e),
            txn_types::Error(box txn_types::ErrorInner::BadFormatLock)
            | txn_types::Error(box txn_types::ErrorInner::BadFormatWrite) => {
                ErrorInner::BadFormat(err)
            }
            txn_types::Error(box txn_types::ErrorInner::KeyIsLocked(lock_info)) => {
                ErrorInner::KeyIsLocked(lock_info)
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Engine(e) => e.error_code(),
            ErrorInner::Io(_) => error_code::causetStorage::IO,
            ErrorInner::Codec(e) => e.error_code(),
            ErrorInner::KeyIsLocked(_) => error_code::causetStorage::KEY_IS_LOCKED,
            ErrorInner::BadFormat(e) => e.error_code(),
            ErrorInner::Committed { .. } => error_code::causetStorage::COMMITTED,
            ErrorInner::PessimisticLockRolledBack { .. } => {
                error_code::causetStorage::PESSIMISTIC_LOCK_ROLLED_BACK
            }
            ErrorInner::TxnLockNotFound { .. } => error_code::causetStorage::TXN_LOCK_NOT_FOUND,
            ErrorInner::TxnNotFound { .. } => error_code::causetStorage::TXN_NOT_FOUND,
            ErrorInner::LockTypeNotMatch { .. } => error_code::causetStorage::LOCK_TYPE_NOT_MATCH,
            ErrorInner::WriteConflict { .. } => error_code::causetStorage::WRITE_CONFLICT,
            ErrorInner::Deadlock { .. } => error_code::causetStorage::DEADLOCK,
            ErrorInner::AlreadyExist { .. } => error_code::causetStorage::ALREADY_EXIST,
            ErrorInner::DefaultNotFound { .. } => error_code::causetStorage::DEFAULT_NOT_FOUND,
            ErrorInner::CommitTsExpired { .. } => error_code::causetStorage::COMMIT_TS_EXPIRED,
            ErrorInner::KeyVersion => error_code::causetStorage::KEY_VERSION,
            ErrorInner::PessimisticLockNotFound { .. } => {
                error_code::causetStorage::PESSIMISTIC_LOCK_NOT_FOUND
            }
            ErrorInner::Other(_) => error_code::causetStorage::UNKNOWN,
        }
    }
}

/// Generates `DefaultNotFound` error or panic directly based on config.
#[inline(never)]
pub fn default_not_found_error(key: Vec<u8>, hint: &str) -> Error {
    CRITICAL_ERROR
        .with_label_values(&["default value not found"])
        .inc();
    if panic_when_unexpected_key_or_data() {
        set_panic_mark();
        panic!(
            "default value not found for key {:?} when {}",
            hex::encode_upper(&key),
            hint,
        );
    } else {
        error!(
            "default value not found";
            "key" => log_wrappers::Key(&key),
            "hint" => hint,
        );
        Error::from(ErrorInner::DefaultNotFound { key })
    }
}

pub mod tests {
    use super::*;
    use crate::causetStorage::kv::{Engine, Modify, ScanMode, Snapshot, WriteData};
    use concurrency_manager::ConcurrencyManager;
    use engine_promises::CAUSET_WRITE;
    use ekvproto::kvrpcpb::{Context, IsolationLevel};
    use txn_types::Key;

    pub fn write<E: Engine>(engine: &E, ctx: &Context, modifies: Vec<Modify>) {
        if !modifies.is_empty() {
            engine
                .write(ctx, WriteData::from_modifies(modifies))
                .unwrap();
        }
    }

    pub fn must_get<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>, expect: &[u8]) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        assert_eq!(
            reader
                .get(&Key::from_raw(key), ts.into(), false)
                .unwrap()
                .unwrap(),
            expect
        );
    }

    pub fn must_get_rc<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>, expect: &[u8]) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Rc);
        assert_eq!(
            reader
                .get(&Key::from_raw(key), ts.into(), false)
                .unwrap()
                .unwrap(),
            expect
        );
    }

    pub fn must_get_none<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        assert!(reader
            .get(&Key::from_raw(key), ts.into(), false)
            .unwrap()
            .is_none());
    }

    pub fn must_get_err<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        assert!(reader.get(&Key::from_raw(key), ts.into(), false).is_err());
    }

    // Insert has a constraint that key should not exist
    pub fn try_prewrite_insert<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        txn.prewrite(
            Mutation::Insert((Key::from_raw(key), value.to_vec())),
            pk,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
        )?;
        write(engine, &ctx, txn.into_modifies());
        Ok(())
    }

    pub fn try_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        txn.prewrite(
            Mutation::CheckNotExists(Key::from_raw(key)),
            pk,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
        )?;
        Ok(())
    }

    pub fn try_pessimistic_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        txn.pessimistic_prewrite(
            Mutation::CheckNotExists(Key::from_raw(key)),
            pk,
            &None,
            false,
            0,
            TimeStamp::default(),
            0,
            TimeStamp::default(),
            false,
        )?;
        Ok(())
    }

    pub fn must_prewrite_put_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        secondary_tuplespaceInstanton: &Option<Vec<Vec<u8>>>,
        ts: TimeStamp,
        is_pessimistic_lock: bool,
        lock_ttl: u64,
        for_ufidelate_ts: TimeStamp,
        txn_size: u64,
        min_commit_ts: TimeStamp,
        pipelined_pessimistic_lock: bool,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        let mutation = Mutation::Put((Key::from_raw(key), value.to_vec()));
        if for_ufidelate_ts.is_zero() {
            txn.prewrite(
                mutation,
                pk,
                &secondary_tuplespaceInstanton,
                false,
                lock_ttl,
                txn_size,
                min_commit_ts,
            )
            .unwrap();
        } else {
            txn.pessimistic_prewrite(
                mutation,
                pk,
                &secondary_tuplespaceInstanton,
                is_pessimistic_lock,
                lock_ttl,
                for_ufidelate_ts,
                txn_size,
                min_commit_ts,
                pipelined_pessimistic_lock,
            )
            .unwrap();
        }
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_prewrite_put<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        must_prewrite_put_impl(
            engine,
            key,
            value,
            pk,
            &None,
            ts.into(),
            false,
            0,
            TimeStamp::default(),
            0,
            TimeStamp::default(),
            false,
        );
    }

    pub fn must_pessimistic_prewrite_put<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        must_prewrite_put_impl(
            engine,
            key,
            value,
            pk,
            &None,
            ts.into(),
            is_pessimistic_lock,
            0,
            for_ufidelate_ts.into(),
            0,
            TimeStamp::default(),
            false,
        );
    }

    pub fn must_pipelined_pessimistic_prewrite_put<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        must_prewrite_put_impl(
            engine,
            key,
            value,
            pk,
            &None,
            ts.into(),
            is_pessimistic_lock,
            0,
            for_ufidelate_ts.into(),
            0,
            TimeStamp::default(),
            true,
        );
    }

    pub fn must_pessimistic_prewrite_put_with_ttl<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
        lock_ttl: u64,
    ) {
        must_prewrite_put_impl(
            engine,
            key,
            value,
            pk,
            &None,
            ts.into(),
            is_pessimistic_lock,
            lock_ttl,
            for_ufidelate_ts.into(),
            0,
            TimeStamp::default(),
            false,
        );
    }

    pub fn must_prewrite_put_for_large_txn<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        ttl: u64,
        for_ufidelate_ts: impl Into<TimeStamp>,
    ) {
        let lock_ttl = ttl;
        let ts = ts.into();
        let min_commit_ts = (ts.into_inner() + 1).into();
        let for_ufidelate_ts = for_ufidelate_ts.into();
        must_prewrite_put_impl(
            engine,
            key,
            value,
            pk,
            &None,
            ts,
            !for_ufidelate_ts.is_zero(),
            lock_ttl,
            for_ufidelate_ts,
            0,
            min_commit_ts,
            false,
        );
    }

    pub fn must_prewrite_put_async_commit<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        secondary_tuplespaceInstanton: &Option<Vec<Vec<u8>>>,
        ts: impl Into<TimeStamp>,
        min_commit_ts: impl Into<TimeStamp>,
    ) {
        assert!(secondary_tuplespaceInstanton.is_some());
        must_prewrite_put_impl(
            engine,
            key,
            value,
            pk,
            secondary_tuplespaceInstanton,
            ts.into(),
            false,
            0,
            TimeStamp::default(),
            0,
            min_commit_ts.into(),
            false,
        );
    }

    pub fn must_pessimistic_prewrite_put_async_commit<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        secondary_tuplespaceInstanton: &Option<Vec<Vec<u8>>>,
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
        min_commit_ts: impl Into<TimeStamp>,
    ) {
        assert!(secondary_tuplespaceInstanton.is_some());
        must_prewrite_put_impl(
            engine,
            key,
            value,
            pk,
            secondary_tuplespaceInstanton,
            ts.into(),
            is_pessimistic_lock,
            0,
            for_ufidelate_ts.into(),
            0,
            min_commit_ts.into(),
            false,
        );
    }

    fn must_prewrite_put_err_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
        pipelined_pessimistic_lock: bool,
    ) -> Error {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let for_ufidelate_ts = for_ufidelate_ts.into();
        let cm = ConcurrencyManager::new(for_ufidelate_ts);
        let mut txn = MvccTxn::new(snapshot, ts.into(), true, cm);
        let mutation = Mutation::Put((Key::from_raw(key), value.to_vec()));

        if for_ufidelate_ts.is_zero() {
            txn.prewrite(mutation, pk, &None, false, 0, 0, TimeStamp::default())
                .unwrap_err()
        } else {
            txn.pessimistic_prewrite(
                mutation,
                pk,
                &None,
                is_pessimistic_lock,
                0,
                for_ufidelate_ts,
                0,
                TimeStamp::default(),
                pipelined_pessimistic_lock,
            )
            .unwrap_err()
        }
    }

    pub fn must_prewrite_put_err<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Error {
        must_prewrite_put_err_impl(engine, key, value, pk, ts, TimeStamp::zero(), false, false)
    }

    pub fn must_pessimistic_prewrite_put_err<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) -> Error {
        must_prewrite_put_err_impl(
            engine,
            key,
            value,
            pk,
            ts,
            for_ufidelate_ts,
            is_pessimistic_lock,
            false,
        )
    }

    pub fn must_pipelined_pessimistic_prewrite_put_err<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) -> Error {
        must_prewrite_put_err_impl(
            engine,
            key,
            value,
            pk,
            ts,
            for_ufidelate_ts,
            is_pessimistic_lock,
            true,
        )
    }

    fn must_prewrite_delete_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let for_ufidelate_ts = for_ufidelate_ts.into();
        let cm = ConcurrencyManager::new(for_ufidelate_ts);
        let mut txn = MvccTxn::new(snapshot, ts.into(), true, cm);
        let mutation = Mutation::Delete(Key::from_raw(key));

        if for_ufidelate_ts.is_zero() {
            txn.prewrite(mutation, pk, &None, false, 0, 0, TimeStamp::default())
                .unwrap();
        } else {
            txn.pessimistic_prewrite(
                mutation,
                pk,
                &None,
                is_pessimistic_lock,
                0,
                for_ufidelate_ts,
                0,
                TimeStamp::default(),
                false,
            )
            .unwrap();
        }
        engine
            .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
            .unwrap();
    }

    pub fn must_prewrite_delete<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        must_prewrite_delete_impl(engine, key, pk, ts, TimeStamp::zero(), false);
    }

    pub fn must_pessimistic_prewrite_delete<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        must_prewrite_delete_impl(engine, key, pk, ts, for_ufidelate_ts, is_pessimistic_lock);
    }

    fn must_prewrite_lock_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let for_ufidelate_ts = for_ufidelate_ts.into();
        let cm = ConcurrencyManager::new(for_ufidelate_ts);
        let mut txn = MvccTxn::new(snapshot, ts.into(), true, cm);

        let mutation = Mutation::Dagger(Key::from_raw(key));
        if for_ufidelate_ts.is_zero() {
            txn.prewrite(mutation, pk, &None, false, 0, 0, TimeStamp::default())
                .unwrap();
        } else {
            txn.pessimistic_prewrite(
                mutation,
                pk,
                &None,
                is_pessimistic_lock,
                0,
                for_ufidelate_ts,
                0,
                TimeStamp::default(),
                false,
            )
            .unwrap();
        }
        engine
            .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
            .unwrap();
    }

    pub fn must_prewrite_lock<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        must_prewrite_lock_impl(engine, key, pk, ts, TimeStamp::zero(), false);
    }

    pub fn must_prewrite_lock_err<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        assert!(txn
            .prewrite(
                Mutation::Dagger(Key::from_raw(key)),
                pk,
                &None,
                false,
                0,
                0,
                TimeStamp::default(),
            )
            .is_err());
    }

    pub fn must_pessimistic_prewrite_lock<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        must_prewrite_lock_impl(engine, key, pk, ts, for_ufidelate_ts, is_pessimistic_lock);
    }

    pub fn must_acquire_pessimistic_lock_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        lock_ttl: u64,
        for_ufidelate_ts: impl Into<TimeStamp>,
        need_value: bool,
        min_commit_ts: impl Into<TimeStamp>,
    ) -> Option<Value> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let min_commit_ts = min_commit_ts.into();
        let cm = ConcurrencyManager::new(min_commit_ts);
        let mut txn = MvccTxn::new(snapshot, spacelike_ts.into(), true, cm);
        let res = txn
            .acquire_pessimistic_lock(
                Key::from_raw(key),
                pk,
                false,
                lock_ttl,
                for_ufidelate_ts.into(),
                need_value,
                min_commit_ts,
            )
            .unwrap();
        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            engine
                .write(&ctx, WriteData::from_modifies(modifies))
                .unwrap();
        }
        res
    }

    pub fn must_acquire_pessimistic_lock<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
    ) {
        must_acquire_pessimistic_lock_with_ttl(engine, key, pk, spacelike_ts, for_ufidelate_ts, 0);
    }

    pub fn must_acquire_pessimistic_lock_return_value<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
    ) -> Option<Value> {
        must_acquire_pessimistic_lock_impl(
            engine,
            key,
            pk,
            spacelike_ts,
            0,
            for_ufidelate_ts.into(),
            true,
            TimeStamp::zero(),
        )
    }

    pub fn must_acquire_pessimistic_lock_with_ttl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        ttl: u64,
    ) {
        assert!(must_acquire_pessimistic_lock_impl(
            engine,
            key,
            pk,
            spacelike_ts,
            ttl,
            for_ufidelate_ts.into(),
            false,
            TimeStamp::zero(),
        )
        .is_none());
    }

    pub fn must_acquire_pessimistic_lock_for_large_txn<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        lock_ttl: u64,
    ) {
        let for_ufidelate_ts = for_ufidelate_ts.into();
        let min_commit_ts = for_ufidelate_ts.next();
        must_acquire_pessimistic_lock_impl(
            engine,
            key,
            pk,
            spacelike_ts,
            lock_ttl,
            for_ufidelate_ts,
            false,
            min_commit_ts,
        );
    }

    pub fn must_acquire_pessimistic_lock_err<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
    ) -> Error {
        must_acquire_pessimistic_lock_err_impl(
            engine,
            key,
            pk,
            spacelike_ts,
            for_ufidelate_ts,
            false,
            TimeStamp::zero(),
        )
    }

    pub fn must_acquire_pessimistic_lock_return_value_err<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
    ) -> Error {
        must_acquire_pessimistic_lock_err_impl(
            engine,
            key,
            pk,
            spacelike_ts,
            for_ufidelate_ts,
            true,
            TimeStamp::zero(),
        )
    }

    pub fn must_acquire_pessimistic_lock_err_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
        need_value: bool,
        min_commit_ts: impl Into<TimeStamp>,
    ) -> Error {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let min_commit_ts = min_commit_ts.into();
        let cm = ConcurrencyManager::new(min_commit_ts);
        let mut txn = MvccTxn::new(snapshot, spacelike_ts.into(), true, cm);
        txn.acquire_pessimistic_lock(
            Key::from_raw(key),
            pk,
            false,
            0,
            for_ufidelate_ts.into(),
            need_value,
            min_commit_ts,
        )
        .unwrap_err()
    }

    pub fn must_rollback<E: Engine>(engine: &E, key: &[u8], spacelike_ts: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let spacelike_ts = spacelike_ts.into();
        let cm = ConcurrencyManager::new(spacelike_ts);
        let mut txn = MvccTxn::new(snapshot, spacelike_ts, true, cm);
        txn.collapse_rollback(false);
        txn.rollback(Key::from_raw(key)).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_rollback_collapsed<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let spacelike_ts = spacelike_ts.into();
        let cm = ConcurrencyManager::new(spacelike_ts);
        let mut txn = MvccTxn::new(snapshot, spacelike_ts, true, cm);
        txn.rollback(Key::from_raw(key)).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_rollback_err<E: Engine>(engine: &E, key: &[u8], spacelike_ts: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let spacelike_ts = spacelike_ts.into();
        let cm = ConcurrencyManager::new(spacelike_ts);
        let mut txn = MvccTxn::new(snapshot, spacelike_ts, true, cm);
        assert!(txn.rollback(Key::from_raw(key)).is_err());
    }

    pub fn must_cleanup<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let mut txn = MvccTxn::new(snapshot, spacelike_ts.into(), true, cm);
        txn.cleanup(Key::from_raw(key), current_ts, true).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_cleanup_err<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) -> Error {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let mut txn = MvccTxn::new(snapshot, spacelike_ts.into(), true, cm);
        txn.cleanup(Key::from_raw(key), current_ts, true)
            .unwrap_err()
    }

    pub fn must_gc<E: Engine>(engine: &E, key: &[u8], safe_point: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let cm = ConcurrencyManager::new(1.into());
        let mut txn = MvccTxn::for_scan(
            snapshot,
            Some(ScanMode::Forward),
            TimeStamp::zero(),
            true,
            cm,
        );
        txn.gc(Key::from_raw(key), safe_point.into()).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_locked<E: Engine>(engine: &E, key: &[u8], spacelike_ts: impl Into<TimeStamp>) -> Dagger {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let dagger = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(dagger.ts, spacelike_ts.into());
        assert_ne!(dagger.lock_type, LockType::Pessimistic);
        dagger
    }

    pub fn must_locked_with_ttl<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        ttl: u64,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let dagger = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(dagger.ts, spacelike_ts.into());
        assert_ne!(dagger.lock_type, LockType::Pessimistic);
        assert_eq!(dagger.ttl, ttl);
    }

    pub fn must_large_txn_locked<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        ttl: u64,
        min_commit_ts: impl Into<TimeStamp>,
        is_pessimistic: bool,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let dagger = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(dagger.ts, spacelike_ts.into());
        assert_eq!(dagger.ttl, ttl);
        assert_eq!(dagger.min_commit_ts, min_commit_ts.into());
        if is_pessimistic {
            assert_eq!(dagger.lock_type, LockType::Pessimistic);
        } else {
            assert_ne!(dagger.lock_type, LockType::Pessimistic);
        }
    }

    pub fn must_pessimistic_locked<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        for_ufidelate_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let dagger = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(dagger.ts, spacelike_ts.into());
        assert_eq!(dagger.for_ufidelate_ts, for_ufidelate_ts.into());
        assert_eq!(dagger.lock_type, LockType::Pessimistic);
    }

    pub fn must_unlocked<E: Engine>(engine: &E, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        assert!(reader.load_lock(&Key::from_raw(key)).unwrap().is_none());
    }

    pub fn must_written<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        tp: WriteType,
    ) -> Write {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let k = Key::from_raw(key).applightlike_ts(commit_ts.into());
        let v = snapshot.get_causet(CAUSET_WRITE, &k).unwrap().unwrap();
        let write = WriteRef::parse(&v).unwrap();
        assert_eq!(write.spacelike_ts, spacelike_ts.into());
        assert_eq!(write.write_type, tp);
        write.to_owned()
    }

    pub fn must_seek_write_none<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        assert!(reader
            .seek_write(&Key::from_raw(key), ts.into())
            .unwrap()
            .is_none());
    }

    pub fn must_seek_write<E: Engine>(
        engine: &E,
        key: &[u8],
        ts: impl Into<TimeStamp>,
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        write_type: WriteType,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let (t, write) = reader
            .seek_write(&Key::from_raw(key), ts.into())
            .unwrap()
            .unwrap();
        assert_eq!(t, commit_ts.into());
        assert_eq!(write.spacelike_ts, spacelike_ts.into());
        assert_eq!(write.write_type, write_type);
    }

    pub fn must_get_commit_ts<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let (ts, write_type) = reader
            .get_txn_commit_record(&Key::from_raw(key), spacelike_ts.into())
            .unwrap()
            .info()
            .unwrap();
        assert_ne!(write_type, WriteType::Rollback);
        assert_eq!(ts, commit_ts.into());
    }

    pub fn must_get_commit_ts_none<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);

        let ret = reader.get_txn_commit_record(&Key::from_raw(key), spacelike_ts.into());
        assert!(ret.is_ok());
        match ret.unwrap().info() {
            None => {}
            Some((_, write_type)) => {
                assert_eq!(write_type, WriteType::Rollback);
            }
        }
    }

    pub fn must_get_rollback_ts<E: Engine>(engine: &E, key: &[u8], spacelike_ts: impl Into<TimeStamp>) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);

        let spacelike_ts = spacelike_ts.into();
        let (ts, write_type) = reader
            .get_txn_commit_record(&Key::from_raw(key), spacelike_ts)
            .unwrap()
            .info()
            .unwrap();
        assert_eq!(ts, spacelike_ts);
        assert_eq!(write_type, WriteType::Rollback);
    }

    pub fn must_get_rollback_ts_none<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);

        let ret = reader
            .get_txn_commit_record(&Key::from_raw(key), spacelike_ts.into())
            .unwrap()
            .info();
        assert_eq!(ret, None);
    }

    pub fn must_get_rollback_protected<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        protected: bool,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);

        let spacelike_ts = spacelike_ts.into();
        let (ts, write) = reader
            .seek_write(&Key::from_raw(key), spacelike_ts)
            .unwrap()
            .unwrap();
        assert_eq!(ts, spacelike_ts);
        assert_eq!(write.write_type, WriteType::Rollback);
        assert_eq!(write.as_ref().is_protected(), protected);
    }

    pub fn must_get_overlapped_rollback<E: Engine>(
        engine: &E,
        key: &[u8],
        spacelike_ts: impl Into<TimeStamp>,
        overlapped_spacelike_ts: impl Into<TimeStamp>,
        overlapped_write_type: WriteType,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);

        let spacelike_ts = spacelike_ts.into();
        let overlapped_spacelike_ts = overlapped_spacelike_ts.into();
        let (ts, write) = reader
            .seek_write(&Key::from_raw(key), spacelike_ts)
            .unwrap()
            .unwrap();
        assert_eq!(ts, spacelike_ts);
        assert!(write.has_overlapped_rollback);
        assert_eq!(write.spacelike_ts, overlapped_spacelike_ts);
        assert_eq!(write.write_type, overlapped_write_type);
    }

    pub fn must_scan_tuplespaceInstanton<E: Engine>(
        engine: &E,
        spacelike: Option<&[u8]>,
        limit: usize,
        tuplespaceInstanton: Vec<&[u8]>,
        next_spacelike: Option<&[u8]>,
    ) {
        let expect = (
            tuplespaceInstanton.into_iter().map(Key::from_raw).collect(),
            next_spacelike.map(|x| Key::from_raw(x).applightlike_ts(TimeStamp::zero())),
        );
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader =
            MvccReader::new(snapshot, Some(ScanMode::Mixed), false, IsolationLevel::Si);
        assert_eq!(
            reader.scan_tuplespaceInstanton(spacelike.map(Key::from_raw), limit).unwrap(),
            expect
        );
    }
}
