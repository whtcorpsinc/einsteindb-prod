//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use crate::causet_storage;
use crate::causet_storage::kv::{Error as KvError, ErrorInner as KvErrorInner};
use crate::causet_storage::tail_pointer::{Error as MvccError, ErrorInner as MvccErrorInner};
use crate::causet_storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};

use error_code::{self, ErrorCode, ErrorCodeExt};

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "Brane error (will back off and retry) {:?}", _0)]
    Brane(ekvproto::error_timeshare::Error),

    #[fail(display = "Key is locked (will clean up) {:?}", _0)]
    Locked(ekvproto::kvrpc_timeshare::LockInfo),

    #[fail(display = "Interlock task terminated due to exceeding the deadline")]
    DeadlineExceeded,

    #[fail(display = "Interlock task canceled due to exceeding max plightlikeing tasks")]
    MaxPlightlikeingTasksExceeded,

    #[fail(display = "{}", _0)]
    Other(String),
}

impl From<Box<dyn std::error::Error + lightlike + Sync>> for Error {
    #[inline]
    fn from(err: Box<dyn std::error::Error + lightlike + Sync>) -> Self {
        Error::Other(err.to_string())
    }
}

impl From<Error> for milevadb_query_common::error::StorageError {
    fn from(err: Error) -> Self {
        failure::Error::from(err).into()
    }
}

impl From<milevadb_query_common::error::StorageError> for Error {
    fn from(err: milevadb_query_common::error::StorageError) -> Self {
        match err.0.downcast::<Error>() {
            Ok(e) => e,
            Err(e) => box_err!("Unknown causet_storage error: {}", e),
        }
    }
}

impl From<milevadb_query_common::error::EvaluateError> for Error {
    fn from(err: milevadb_query_common::error::EvaluateError) -> Self {
        Error::Other(err.to_string())
    }
}

impl From<milevadb_query_common::Error> for Error {
    fn from(err: milevadb_query_common::Error) -> Self {
        use milevadb_query_common::error::ErrorInner;

        match *err.0 {
            ErrorInner::causet_storage(err) => err.into(),
            ErrorInner::Evaluate(err) => err.into(),
        }
    }
}

impl From<KvError> for Error {
    fn from(err: KvError) -> Self {
        match err {
            KvError(box KvErrorInner::Request(e)) => Error::Brane(e),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<MvccError> for Error {
    fn from(err: MvccError) -> Self {
        match err {
            MvccError(box MvccErrorInner::KeyIsLocked(info)) => Error::Locked(info),
            MvccError(box MvccErrorInner::Engine(engine_error)) => Error::from(engine_error),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<TxnError> for Error {
    fn from(err: causet_storage::txn::Error) -> Self {
        match err {
            TxnError(box TxnErrorInner::Mvcc(tail_pointer_error)) => Error::from(tail_pointer_error),
            TxnError(box TxnErrorInner::Engine(engine_error)) => Error::from(engine_error),
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<violetabftstore::interlock::::deadline::DeadlineError> for Error {
    fn from(_: violetabftstore::interlock::::deadline::DeadlineError) -> Self {
        Error::DeadlineExceeded
    }
}

impl From<milevadb_query_datatype::DataTypeError> for Error {
    fn from(err: milevadb_query_datatype::DataTypeError) -> Self {
        Error::Other(err.to_string())
    }
}

impl From<milevadb_query_datatype::codec::Error> for Error {
    fn from(err: milevadb_query_datatype::codec::Error) -> Self {
        Error::Other(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Brane(e) => e.error_code(),
            Error::Locked(_) => error_code::interlock::LOCKED,
            Error::DeadlineExceeded => error_code::interlock::DEADLINE_EXCEEDED,
            Error::MaxPlightlikeingTasksExceeded => error_code::interlock::MAX_PENDING_TASKS_EXCEEDED,
            Error::Other(_) => error_code::UNKNOWN,
        }
    }
}
