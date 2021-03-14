// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

//! CausetStorage bundles

pub mod commands;
pub mod sched_pool;
pub mod interlock_semaphore;

mod actions;

pub use actions::commit::commit;

mod latch;
mod store;

use crate::causetStorage::{
    types::{MvccInfo, PessimisticLockRes, PrewriteResult, SecondaryLocksStatus, TxnStatus},
    Error as StorageError, Result as StorageResult,
};
use error_code::{self, ErrorCode, ErrorCodeExt};
use ekvproto::kvrpcpb::LockInfo;
use std::error;
use std::fmt;
use std::io::Error as IoError;
use txn_types::{Key, TimeStamp};

pub use self::commands::{Command, RESOLVE_LOCK_BATCH_SIZE};
pub use self::interlock_semaphore::Interlock_Semaphore;
pub use self::store::{
    EntryBatch, FixtureStore, FixtureStoreScanner, Scanner, SnapshotStore, CausetStore, TxnEntry,
    TxnEntryScanner, TxnEntryStore,
};

/// Process result of a command.
#[derive(Debug)]
pub enum ProcessResult {
    Res,
    MultiRes {
        results: Vec<StorageResult<()>>,
    },
    PrewriteResult {
        result: PrewriteResult,
    },
    MvccKey {
        tail_pointer: MvccInfo,
    },
    MvccStartTs {
        tail_pointer: Option<(Key, MvccInfo)>,
    },
    Locks {
        locks: Vec<LockInfo>,
    },
    TxnStatus {
        txn_status: TxnStatus,
    },
    NextCommand {
        cmd: Command,
    },
    Failed {
        err: StorageError,
    },
    PessimisticLockRes {
        res: StorageResult<PessimisticLockRes>,
    },
    SecondaryLocksStatus {
        status: SecondaryLocksStatus,
    },
}

impl ProcessResult {
    pub fn maybe_clone(&self) -> Option<ProcessResult> {
        match self {
            ProcessResult::PessimisticLockRes { res: Ok(r) } => {
                Some(ProcessResult::PessimisticLockRes { res: Ok(r.clone()) })
            }
            _ => None,
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum ErrorInner {
        Engine(err: crate::causetStorage::kv::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        Codec(err: einsteindb_util::codec::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        ProtoBuf(err: protobuf::error::ProtobufError) {
            from()
            cause(err)
            display("{}", err)
        }
        Mvcc(err: crate::causetStorage::tail_pointer::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        Other(err: Box<dyn error::Error + Sync + Slightlike>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }
        Io(err: IoError) {
            from()
            cause(err)
            display("{}", err)
        }
        InvalidTxnTso {spacelike_ts: TimeStamp, commit_ts: TimeStamp} {
            display("Invalid transaction tso with spacelike_ts:{},commit_ts:{}",
                        spacelike_ts,
                        commit_ts)
        }
        InvalidReqCone {spacelike: Option<Vec<u8>>,
                        lightlike: Option<Vec<u8>>,
                        lower_bound: Option<Vec<u8>>,
                        upper_bound: Option<Vec<u8>>} {
            display("Request cone exceeds bound, request cone:[{}, lightlike:{}), physical bound:[{}, {})",
                        spacelike.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        lightlike.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        lower_bound.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        upper_bound.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()))
        }
        MaxTimestampNotSynced { brane_id: u64, spacelike_ts: TimeStamp } {
            display("Prewrite for async commit fails due to potentially stale max timestamp, spacelike_ts: {}, brane_id: {}",
                        spacelike_ts,
                        brane_id)
        }
    }
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match *self {
            ErrorInner::Engine(ref e) => e.maybe_clone().map(ErrorInner::Engine),
            ErrorInner::Codec(ref e) => e.maybe_clone().map(ErrorInner::Codec),
            ErrorInner::Mvcc(ref e) => e.maybe_clone().map(ErrorInner::Mvcc),
            ErrorInner::InvalidTxnTso {
                spacelike_ts,
                commit_ts,
            } => Some(ErrorInner::InvalidTxnTso {
                spacelike_ts,
                commit_ts,
            }),
            ErrorInner::InvalidReqCone {
                ref spacelike,
                ref lightlike,
                ref lower_bound,
                ref upper_bound,
            } => Some(ErrorInner::InvalidReqCone {
                spacelike: spacelike.clone(),
                lightlike: lightlike.clone(),
                lower_bound: lower_bound.clone(),
                upper_bound: upper_bound.clone(),
            }),
            ErrorInner::MaxTimestampNotSynced {
                brane_id,
                spacelike_ts,
            } => Some(ErrorInner::MaxTimestampNotSynced {
                brane_id,
                spacelike_ts,
            }),
            ErrorInner::Other(_) | ErrorInner::ProtoBuf(_) | ErrorInner::Io(_) => None,
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

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Engine(e) => e.error_code(),
            ErrorInner::Codec(e) => e.error_code(),
            ErrorInner::ProtoBuf(_) => error_code::causetStorage::PROTOBUF,
            ErrorInner::Mvcc(e) => e.error_code(),
            ErrorInner::Other(_) => error_code::causetStorage::UNKNOWN,
            ErrorInner::Io(_) => error_code::causetStorage::IO,
            ErrorInner::InvalidTxnTso { .. } => error_code::causetStorage::INVALID_TXN_TSO,
            ErrorInner::InvalidReqCone { .. } => error_code::causetStorage::INVALID_REQ_RANGE,
            ErrorInner::MaxTimestampNotSynced { .. } => {
                error_code::causetStorage::MAX_TIMESTAMP_NOT_SYNCED
            }
        }
    }
}

pub mod tests {
    use super::*;
    pub use actions::commit::tests::{must_err as must_commit_err, must_succeed as must_commit};
}
