// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

mod btree_engine;
mod cursor;
mod perf_context;
mod lmdb_engine;
mod stats;

use std::cell::UnsafeCell;
use std::fmt;
use std::time::Duration;
use std::{error, ptr, result};

use engine_lmdb::LmdbTablePropertiesCollection;
use engine_promises::{CfName, CAUSET_DEFAULT};
use engine_promises::{IterOptions, KvEngine as LocalEngine, ReadOptions};
use futures::prelude::*;
use ekvproto::errorpb::Error as ErrorHeader;
use ekvproto::kvrpcpb::{Context, ExtraOp as TxnExtraOp};
use txn_types::{Key, TxnExtra, Value};

pub use self::btree_engine::{BTreeEngine, BTreeEngineIterator, BTreeEngineSnapshot};
pub use self::cursor::{Cursor, CursorBuilder};
pub use self::perf_context::{PerfStatisticsDelta, PerfStatisticsInstant};
pub use self::lmdb_engine::{write_modifies, LmdbEngine, LmdbSnapshot, TestEngineBuilder};
pub use self::stats::{
    CfStatistics, FlowStatistics, FlowStatsReporter, Statistics, StatisticsSummary,
};
use error_code::{self, ErrorCode, ErrorCodeExt};
use into_other::IntoOther;
use einsteindb_util::time::ThreadReadId;

pub const SEEK_BOUND: u64 = 8;
const DEFAULT_TIMEOUT_SECS: u64 = 5;

pub type Callback<T> = Box<dyn FnOnce((CbContext, Result<T>)) + Slightlike>;
pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub struct CbContext {
    pub term: Option<u64>,
    pub txn_extra_op: TxnExtraOp,
}

impl CbContext {
    pub fn new() -> CbContext {
        CbContext {
            term: None,
            txn_extra_op: TxnExtraOp::Noop,
        }
    }
}

#[derive(Debug)]
pub enum Modify {
    Delete(CfName, Key),
    Put(CfName, Key, Value),
    // causet_name, spacelike_key, lightlike_key, notify_only
    DeleteCone(CfName, Key, Key, bool),
}

impl Modify {
    pub fn size(&self) -> usize {
        let causet = match self {
            Modify::Delete(causet, _) => causet,
            Modify::Put(causet, ..) => causet,
            Modify::DeleteCone(..) => unreachable!(),
        };
        let causet_size = if causet == &CAUSET_DEFAULT { 0 } else { causet.len() };

        match self {
            Modify::Delete(_, k) => causet_size + k.as_encoded().len(),
            Modify::Put(_, k, v) => causet_size + k.as_encoded().len() + v.len(),
            Modify::DeleteCone(..) => unreachable!(),
        }
    }
}

#[derive(Default)]
pub struct WriteData {
    pub modifies: Vec<Modify>,
    pub extra: TxnExtra,
}

impl WriteData {
    pub fn new(modifies: Vec<Modify>, extra: TxnExtra) -> Self {
        Self { modifies, extra }
    }

    pub fn from_modifies(modifies: Vec<Modify>) -> Self {
        Self::new(modifies, TxnExtra::default())
    }
}

pub trait Engine: Slightlike + Clone + 'static {
    type Snap: Snapshot;
    type Local: LocalEngine;

    /// Local persistence engine.
    fn kv_engine(&self) -> Self::Local;

    fn snapshot_on_kv_engine(&self, spacelike_key: &[u8], lightlike_key: &[u8]) -> Result<Self::Snap>;

    /// Write modifications into internal local engine directly.
    fn modify_on_kv_engine(&self, modifies: Vec<Modify>) -> Result<()>;

    fn async_snapshot(
        &self,
        ctx: &Context,
        read_id: Option<ThreadReadId>,
        cb: Callback<Self::Snap>,
    ) -> Result<()>;

    fn async_write(&self, ctx: &Context, batch: WriteData, callback: Callback<()>) -> Result<()>;

    fn write(&self, ctx: &Context, batch: WriteData) -> Result<()> {
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
        match wait_op!(|cb| self.async_write(ctx, batch, cb), timeout) {
            Some((_, res)) => res,
            None => Err(Error::from(ErrorInner::Timeout(timeout))),
        }
    }

    fn release_snapshot(&self) {}

    fn snapshot(&self, ctx: &Context) -> Result<Self::Snap> {
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
        match wait_op!(|cb| self.async_snapshot(ctx, None, cb), timeout) {
            Some((_, res)) => res,
            None => Err(Error::from(ErrorInner::Timeout(timeout))),
        }
    }

    fn put(&self, ctx: &Context, key: Key, value: Value) -> Result<()> {
        self.put_causet(ctx, CAUSET_DEFAULT, key, value)
    }

    fn put_causet(&self, ctx: &Context, causet: CfName, key: Key, value: Value) -> Result<()> {
        self.write(
            ctx,
            WriteData::from_modifies(vec![Modify::Put(causet, key, value)]),
        )
    }

    fn delete(&self, ctx: &Context, key: Key) -> Result<()> {
        self.delete_causet(ctx, CAUSET_DEFAULT, key)
    }

    fn delete_causet(&self, ctx: &Context, causet: CfName, key: Key) -> Result<()> {
        self.write(ctx, WriteData::from_modifies(vec![Modify::Delete(causet, key)]))
    }

    fn get_properties(&self, spacelike: &[u8], lightlike: &[u8]) -> Result<LmdbTablePropertiesCollection> {
        self.get_properties_causet(CAUSET_DEFAULT, spacelike, lightlike)
    }

    fn get_properties_causet(
        &self,
        _: CfName,
        _spacelike: &[u8],
        _lightlike: &[u8],
    ) -> Result<LmdbTablePropertiesCollection> {
        Err(box_err!("no user properties"))
    }
}

pub trait Snapshot: Sync + Slightlike + Clone {
    type Iter: Iteron;

    fn get(&self, key: &Key) -> Result<Option<Value>>;
    fn get_causet(&self, causet: CfName, key: &Key) -> Result<Option<Value>>;
    fn get_causet_opt(&self, opts: ReadOptions, causet: CfName, key: &Key) -> Result<Option<Value>>;
    fn iter(&self, iter_opt: IterOptions, mode: ScanMode) -> Result<Cursor<Self::Iter>>;
    fn iter_causet(
        &self,
        causet: CfName,
        iter_opt: IterOptions,
        mode: ScanMode,
    ) -> Result<Cursor<Self::Iter>>;
    // The minimum key this snapshot can retrieve.
    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        None
    }
    // The maximum key can be fetched from the snapshot should less than the upper bound.
    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        None
    }

    /// Retrieves a version that represents the modification status of the underlying data.
    /// Version should be changed when underlying data is changed.
    ///
    /// If the engine does not support data version, then `None` is returned.
    #[inline]
    fn get_data_version(&self) -> Option<u64> {
        None
    }

    fn is_max_ts_synced(&self) -> bool {
        // If the snapshot does not come from a multi-violetabft engine, max ts
        // needn't be ufidelated.
        true
    }
}

pub trait Iteron: Slightlike {
    fn next(&mut self) -> Result<bool>;
    fn prev(&mut self) -> Result<bool>;
    fn seek(&mut self, key: &Key) -> Result<bool>;
    fn seek_for_prev(&mut self, key: &Key) -> Result<bool>;
    fn seek_to_first(&mut self) -> Result<bool>;
    fn seek_to_last(&mut self) -> Result<bool>;
    fn valid(&self) -> Result<bool>;

    fn validate_key(&self, _: &Key) -> Result<()> {
        Ok(())
    }

    /// Only be called when `self.valid() == Ok(true)`.
    fn key(&self) -> &[u8];
    /// Only be called when `self.valid() == Ok(true)`.
    fn value(&self) -> &[u8];
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ScanMode {
    Forward,
    Backward,
    Mixed,
}

quick_error! {
    #[derive(Debug)]
    pub enum ErrorInner {
        Request(err: ErrorHeader) {
            from()
            display("{:?}", err)
        }
        Timeout(d: Duration) {
            display("timeout after {:?}", d)
        }
        EmptyRequest {
            display("an empty request")
        }
        Other(err: Box<dyn error::Error + Slightlike + Sync>) {
            from()
            cause(err.as_ref())
            display("unknown error {:?}", err)
        }
    }
}

impl From<engine_promises::Error> for ErrorInner {
    fn from(err: engine_promises::Error) -> ErrorInner {
        ErrorInner::Request(err.into_other())
    }
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match *self {
            ErrorInner::Request(ref e) => Some(ErrorInner::Request(e.clone())),
            ErrorInner::Timeout(d) => Some(ErrorInner::Timeout(d)),
            ErrorInner::EmptyRequest => Some(ErrorInner::EmptyRequest),
            ErrorInner::Other(_) => None,
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

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Request(e) => e.error_code(),
            ErrorInner::Timeout(_) => error_code::persistence::TIMEOUT,
            ErrorInner::EmptyRequest => error_code::persistence::EMPTY_REQUEST,
            ErrorInner::Other(_) => error_code::persistence::UNKNOWN,
        }
    }
}

thread_local! {
    // A pointer to thread local engine. Use raw pointer and `UnsafeCell` to reduce runtime check.
    static TLS_ENGINE_ANY: UnsafeCell<*mut ()> = UnsafeCell::new(ptr::null_mut());
}

/// Execute the closure on the thread local engine.
///
/// # Safety
///
/// Precondition: `TLS_ENGINE_ANY` is non-null.
pub unsafe fn with_tls_engine<E: Engine, F, R>(f: F) -> R
where
    F: FnOnce(&E) -> R,
{
    TLS_ENGINE_ANY.with(|e| {
        let engine = &*(*e.get() as *const E);
        f(engine)
    })
}

/// Set the thread local engine.
///
/// Postcondition: `TLS_ENGINE_ANY` is non-null.
pub fn set_tls_engine<E: Engine>(engine: E) {
    // Safety: we check that `TLS_ENGINE_ANY` is null to ensure we don't leak an existing
    // engine; we ensure there are no other references to `engine`.
    TLS_ENGINE_ANY.with(move |e| unsafe {
        if (*e.get()).is_null() {
            let engine = Box::into_raw(Box::new(engine)) as *mut ();
            *e.get() = engine;
        }
    });
}

/// Destroy the thread local engine.
///
/// Postcondition: `TLS_ENGINE_ANY` is null.
///
/// # Safety
///
/// The current tls engine must have the same type as `E` (or at least
/// there destructors must be compatible).
pub unsafe fn destroy_tls_engine<E: Engine>() {
    // Safety: we check that `TLS_ENGINE_ANY` is non-null, we must ensure that references
    // to `TLS_ENGINE_ANY` can never be stored outside of `TLS_ENGINE_ANY`.
    TLS_ENGINE_ANY.with(|e| {
        let ptr = *e.get();
        if !ptr.is_null() {
            drop(Box::from_raw(ptr as *mut E));
            *e.get() = ptr::null_mut();
        }
    });
}

/// Get a snapshot of `engine`.
pub fn snapshot<E: Engine>(
    engine: &E,
    read_id: Option<ThreadReadId>,
    ctx: &Context,
) -> impl std::future::Future<Output = Result<E::Snap>> {
    let (callback, future) =
        einsteindb_util::future::paired_must_called_future_callback(drop_snapshot_callback::<E>);
    let val = engine.async_snapshot(ctx, read_id, callback);
    // make engine not cross yield point
    async move {
        val?; // propagate error
        let (_ctx, result) = future
            .map_err(|cancel| Error::from(ErrorInner::Other(box_err!(cancel))))
            .await?;
        result
    }
}

pub fn drop_snapshot_callback<E: Engine>() -> (CbContext, Result<E::Snap>) {
    let bt = backtrace::Backtrace::new();
    warn!("async snapshot callback is dropped"; "backtrace" => ?bt);
    let mut err = ErrorHeader::default();
    err.set_message("async snapshot callback is dropped".to_string());
    (CbContext::new(), Err(Error::from(ErrorInner::Request(err))))
}

#[causetg(test)]
pub mod tests {
    use super::*;
    use einsteindb_util::codec::bytes;

    pub const TEST_ENGINE_CAUSETS: &[CfName] = &["causet"];

    pub fn must_put<E: Engine>(engine: &E, key: &[u8], value: &[u8]) {
        engine
            .put(&Context::default(), Key::from_raw(key), value.to_vec())
            .unwrap();
    }

    pub fn must_put_causet<E: Engine>(engine: &E, causet: CfName, key: &[u8], value: &[u8]) {
        engine
            .put_causet(&Context::default(), causet, Key::from_raw(key), value.to_vec())
            .unwrap();
    }

    pub fn must_delete<E: Engine>(engine: &E, key: &[u8]) {
        engine
            .delete(&Context::default(), Key::from_raw(key))
            .unwrap();
    }

    pub fn must_delete_causet<E: Engine>(engine: &E, causet: CfName, key: &[u8]) {
        engine
            .delete_causet(&Context::default(), causet, Key::from_raw(key))
            .unwrap();
    }

    pub fn assert_has<E: Engine>(engine: &E, key: &[u8], value: &[u8]) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap().unwrap(), value);
    }

    pub fn assert_has_causet<E: Engine>(engine: &E, causet: CfName, key: &[u8], value: &[u8]) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        assert_eq!(
            snapshot.get_causet(causet, &Key::from_raw(key)).unwrap().unwrap(),
            value
        );
    }

    pub fn assert_none<E: Engine>(engine: &E, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap(), None);
    }

    pub fn assert_none_causet<E: Engine>(engine: &E, causet: CfName, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        assert_eq!(snapshot.get_causet(causet, &Key::from_raw(key)).unwrap(), None);
    }

    fn assert_seek<E: Engine>(engine: &E, key: &[u8], pair: (&[u8], &[u8])) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = snapshot
            .iter(IterOptions::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CfStatistics::default();
        cursor.seek(&Key::from_raw(key), &mut statistics).unwrap();
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    fn assert_reverse_seek<E: Engine>(engine: &E, key: &[u8], pair: (&[u8], &[u8])) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = snapshot
            .iter(IterOptions::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CfStatistics::default();
        cursor
            .reverse_seek(&Key::from_raw(key), &mut statistics)
            .unwrap();
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    fn assert_near_seek<I: Iteron>(cursor: &mut Cursor<I>, key: &[u8], pair: (&[u8], &[u8])) {
        let mut statistics = CfStatistics::default();
        assert!(
            cursor
                .near_seek(&Key::from_raw(key), &mut statistics)
                .unwrap(),
            hex::encode_upper(key)
        );
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    fn assert_near_reverse_seek<I: Iteron>(
        cursor: &mut Cursor<I>,
        key: &[u8],
        pair: (&[u8], &[u8]),
    ) {
        let mut statistics = CfStatistics::default();
        assert!(
            cursor
                .near_reverse_seek(&Key::from_raw(key), &mut statistics)
                .unwrap(),
            hex::encode_upper(key)
        );
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    pub fn test_base_curd_options<E: Engine>(engine: &E) {
        test_get_put(engine);
        test_batch(engine);
        test_empty_seek(engine);
        test_seek(engine);
        test_near_seek(engine);
        test_causet(engine);
        test_empty_write(engine);
    }

    fn test_get_put<E: Engine>(engine: &E) {
        assert_none(engine, b"x");
        must_put(engine, b"x", b"1");
        assert_has(engine, b"x", b"1");
        must_put(engine, b"x", b"2");
        assert_has(engine, b"x", b"2");
    }

    fn test_batch<E: Engine>(engine: &E) {
        engine
            .write(
                &Context::default(),
                WriteData::from_modifies(vec![
                    Modify::Put(CAUSET_DEFAULT, Key::from_raw(b"x"), b"1".to_vec()),
                    Modify::Put(CAUSET_DEFAULT, Key::from_raw(b"y"), b"2".to_vec()),
                ]),
            )
            .unwrap();
        assert_has(engine, b"x", b"1");
        assert_has(engine, b"y", b"2");

        engine
            .write(
                &Context::default(),
                WriteData::from_modifies(vec![
                    Modify::Delete(CAUSET_DEFAULT, Key::from_raw(b"x")),
                    Modify::Delete(CAUSET_DEFAULT, Key::from_raw(b"y")),
                ]),
            )
            .unwrap();
        assert_none(engine, b"y");
        assert_none(engine, b"y");
    }

    fn test_seek<E: Engine>(engine: &E) {
        must_put(engine, b"x", b"1");
        assert_seek(engine, b"x", (b"x", b"1"));
        assert_seek(engine, b"a", (b"x", b"1"));
        assert_reverse_seek(engine, b"x1", (b"x", b"1"));
        must_put(engine, b"z", b"2");
        assert_seek(engine, b"y", (b"z", b"2"));
        assert_seek(engine, b"x\x00", (b"z", b"2"));
        assert_reverse_seek(engine, b"y", (b"x", b"1"));
        assert_reverse_seek(engine, b"z", (b"x", b"1"));
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut iter = snapshot
            .iter(IterOptions::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CfStatistics::default();
        assert!(!iter
            .seek(&Key::from_raw(b"z\x00"), &mut statistics)
            .unwrap());
        assert!(!iter
            .reverse_seek(&Key::from_raw(b"x"), &mut statistics)
            .unwrap());
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }

    fn test_near_seek<E: Engine>(engine: &E) {
        must_put(engine, b"x", b"1");
        must_put(engine, b"z", b"2");
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = snapshot
            .iter(IterOptions::default(), ScanMode::Mixed)
            .unwrap();
        assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"a", (b"x", b"1"));
        assert_near_reverse_seek(&mut cursor, b"z1", (b"z", b"2"));
        assert_near_reverse_seek(&mut cursor, b"x1", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"y", (b"z", b"2"));
        assert_near_seek(&mut cursor, b"x\x00", (b"z", b"2"));
        let mut statistics = CfStatistics::default();
        assert!(!cursor
            .near_seek(&Key::from_raw(b"z\x00"), &mut statistics)
            .unwrap());
        // Insert many key-values between 'x' and 'z' then near_seek will fallback to seek.
        for i in 0..super::SEEK_BOUND {
            let key = format!("y{}", i);
            must_put(engine, key.as_bytes(), b"3");
        }
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = snapshot
            .iter(IterOptions::default(), ScanMode::Mixed)
            .unwrap();
        assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"z", (b"z", b"2"));

        must_delete(engine, b"x");
        must_delete(engine, b"z");
        for i in 0..super::SEEK_BOUND {
            let key = format!("y{}", i);
            must_delete(engine, key.as_bytes());
        }
    }

    fn test_empty_seek<E: Engine>(engine: &E) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = snapshot
            .iter(IterOptions::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CfStatistics::default();
        assert!(!cursor
            .near_reverse_seek(&Key::from_raw(b"x"), &mut statistics)
            .unwrap());
        assert!(!cursor
            .near_reverse_seek(&Key::from_raw(b"z"), &mut statistics)
            .unwrap());
        assert!(!cursor
            .near_reverse_seek(&Key::from_raw(b"w"), &mut statistics)
            .unwrap());
        assert!(!cursor
            .near_seek(&Key::from_raw(b"x"), &mut statistics)
            .unwrap());
        assert!(!cursor
            .near_seek(&Key::from_raw(b"z"), &mut statistics)
            .unwrap());
        assert!(!cursor
            .near_seek(&Key::from_raw(b"w"), &mut statistics)
            .unwrap());
    }

    macro_rules! assert_seek {
        ($cursor:ident, $func:ident, $k:expr, $res:ident) => {{
            let mut statistics = CfStatistics::default();
            assert_eq!(
                $cursor.$func(&$k, &mut statistics).unwrap(),
                $res.is_some(),
                "assert_seek {} failed exp {:?}",
                $k,
                $res
            );
            if let Some((ref k, ref v)) = $res {
                assert_eq!(
                    $cursor.key(&mut statistics),
                    bytes::encode_bytes(k.as_bytes()).as_slice()
                );
                assert_eq!($cursor.value(&mut statistics), v.as_bytes());
            }
        }};
    }

    #[derive(PartialEq, Eq, Clone, Copy)]
    enum SeekMode {
        Normal,
        Reverse,
        ForPrev,
    }

    // use step to control the distance between target key and current key in cursor.
    fn test_linear_seek<S: Snapshot>(
        snapshot: &S,
        mode: ScanMode,
        seek_mode: SeekMode,
        spacelike_idx: usize,
        step: usize,
    ) {
        let mut cursor = snapshot.iter(IterOptions::default(), mode).unwrap();
        let mut near_cursor = snapshot.iter(IterOptions::default(), mode).unwrap();
        let limit = (SEEK_BOUND as usize * 10 + 50 - 1) * 2;

        for (_, mut i) in (spacelike_idx..(SEEK_BOUND as usize * 30))
            .enumerate()
            .filter(|&(i, _)| i % step == 0)
        {
            if seek_mode != SeekMode::Normal {
                i = SEEK_BOUND as usize * 30 - 1 - i;
            }
            let key = format!("key_{:03}", i);
            let seek_key = Key::from_raw(key.as_bytes());
            let exp_kv = if i <= 100 {
                match seek_mode {
                    SeekMode::Reverse => None,
                    SeekMode::ForPrev if i < 100 => None,
                    SeekMode::Normal | SeekMode::ForPrev => {
                        Some(("key_100".to_owned(), "value_50".to_owned()))
                    }
                }
            } else if i <= limit {
                if seek_mode == SeekMode::Reverse {
                    Some((
                        format!("key_{}", (i - 1) / 2 * 2),
                        format!("value_{}", (i - 1) / 2),
                    ))
                } else if seek_mode == SeekMode::ForPrev {
                    Some((format!("key_{}", i / 2 * 2), format!("value_{}", i / 2)))
                } else {
                    Some((
                        format!("key_{}", (i + 1) / 2 * 2),
                        format!("value_{}", (i + 1) / 2),
                    ))
                }
            } else if seek_mode != SeekMode::Normal {
                Some((
                    format!("key_{:03}", limit),
                    format!("value_{:03}", limit / 2),
                ))
            } else {
                None
            };

            match seek_mode {
                SeekMode::Reverse => {
                    assert_seek!(cursor, reverse_seek, seek_key, exp_kv);
                    assert_seek!(near_cursor, near_reverse_seek, seek_key, exp_kv);
                }
                SeekMode::Normal => {
                    assert_seek!(cursor, seek, seek_key, exp_kv);
                    assert_seek!(near_cursor, near_seek, seek_key, exp_kv);
                }
                SeekMode::ForPrev => {
                    assert_seek!(cursor, seek_for_prev, seek_key, exp_kv);
                    assert_seek!(near_cursor, near_seek_for_prev, seek_key, exp_kv);
                }
            }
        }
    }

    pub fn test_linear<E: Engine>(engine: &E) {
        for i in 50..50 + SEEK_BOUND * 10 {
            let key = format!("key_{}", i * 2);
            let value = format!("value_{}", i);
            must_put(engine, key.as_bytes(), value.as_bytes());
        }
        let snapshot = engine.snapshot(&Context::default()).unwrap();

        for step in 1..SEEK_BOUND as usize * 3 {
            for spacelike in 0..10 {
                test_linear_seek(
                    &snapshot,
                    ScanMode::Forward,
                    SeekMode::Normal,
                    spacelike * SEEK_BOUND as usize,
                    step,
                );
                test_linear_seek(
                    &snapshot,
                    ScanMode::Backward,
                    SeekMode::Reverse,
                    spacelike * SEEK_BOUND as usize,
                    step,
                );
                test_linear_seek(
                    &snapshot,
                    ScanMode::Backward,
                    SeekMode::ForPrev,
                    spacelike * SEEK_BOUND as usize,
                    step,
                );
            }
        }
        for &seek_mode in &[SeekMode::Reverse, SeekMode::Normal, SeekMode::ForPrev] {
            for step in 1..SEEK_BOUND as usize * 3 {
                for spacelike in 0..10 {
                    test_linear_seek(
                        &snapshot,
                        ScanMode::Mixed,
                        seek_mode,
                        spacelike * SEEK_BOUND as usize,
                        step,
                    );
                }
            }
        }
    }

    fn test_causet<E: Engine>(engine: &E) {
        assert_none_causet(engine, "causet", b"key");
        must_put_causet(engine, "causet", b"key", b"value");
        assert_has_causet(engine, "causet", b"key", b"value");
        must_delete_causet(engine, "causet", b"key");
        assert_none_causet(engine, "causet", b"key");
    }

    fn test_empty_write<E: Engine>(engine: &E) {
        engine
            .write(&Context::default(), WriteData::default())
            .unwrap_err();
    }

    pub fn test_causets_statistics<E: Engine>(engine: &E) {
        must_put(engine, b"foo", b"bar1");
        must_put(engine, b"foo2", b"bar2");
        must_put(engine, b"foo3", b"bar3"); // deleted
        must_put(engine, b"foo4", b"bar4");
        must_put(engine, b"foo42", b"bar42"); // deleted
        must_put(engine, b"foo5", b"bar5"); // deleted
        must_put(engine, b"foo6", b"bar6");
        must_delete(engine, b"foo3");
        must_delete(engine, b"foo42");
        must_delete(engine, b"foo5");

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut iter = snapshot
            .iter(IterOptions::default(), ScanMode::Forward)
            .unwrap();

        let mut statistics = CfStatistics::default();
        iter.seek(&Key::from_raw(b"foo30"), &mut statistics)
            .unwrap();

        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo4"));
        assert_eq!(iter.value(&mut statistics), b"bar4");
        assert_eq!(statistics.seek, 1);

        let mut statistics = CfStatistics::default();
        iter.near_seek(&Key::from_raw(b"foo55"), &mut statistics)
            .unwrap();

        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo6"));
        assert_eq!(iter.value(&mut statistics), b"bar6");
        assert_eq!(statistics.seek, 0);
        assert_eq!(statistics.next, 1);

        let mut statistics = CfStatistics::default();
        iter.prev(&mut statistics);

        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo4"));
        assert_eq!(iter.value(&mut statistics), b"bar4");
        assert_eq!(statistics.prev, 1);

        iter.prev(&mut statistics);
        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo2"));
        assert_eq!(iter.value(&mut statistics), b"bar2");
        assert_eq!(statistics.prev, 2);

        iter.prev(&mut statistics);
        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo"));
        assert_eq!(iter.value(&mut statistics), b"bar1");
        assert_eq!(statistics.prev, 3);
    }
}
