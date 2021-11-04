// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::{error, result};

use edb::Error as EnginePromisesError;
use ekvproto::error_timeshare::Error as ErrorHeader;
use edb::causet_storage::kv::{Error as EngineError, ErrorInner as EngineErrorInner};
use edb::causet_storage::tail_pointer::{Error as MvccError, ErrorInner as MvccErrorInner};
use edb::causet_storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};
use txn_types::Error as TxnTypesError;

/// The error type for causet_context.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Other error {}", _0)]
    Other(Box<dyn error::Error + Sync + lightlike>),
    #[fail(display = "Lmdb error {}", _0)]
    Lmdb(String),
    #[fail(display = "IO error {}", _0)]
    Io(IoError),
    #[fail(display = "Engine error {}", _0)]
    Engine(EngineError),
    #[fail(display = "Transaction error {}", _0)]
    Txn(TxnError),
    #[fail(display = "Mvcc error {}", _0)]
    Mvcc(MvccError),
    #[fail(display = "Request error {:?}", _0)]
    Request(ErrorHeader),
    #[fail(display = "Engine promises error {}", _0)]
    EnginePromises(EnginePromisesError),
}

macro_rules! impl_from {
    ($($inner:ty => $container:ident,)+) => {
        $(
            impl From<$inner> for Error {
                fn from(inr: $inner) -> Error {
                    Error::$container(inr.into())
                }
            }
        )+
    };
}

impl_from! {
    Box<dyn error::Error + Sync + lightlike> => Other,
    String => Lmdb,
    IoError => Io,
    EngineError => Engine,
    TxnError => Txn,
    MvccError => Mvcc,
    TxnTypesError => Mvcc,
    EnginePromisesError => EnginePromises,
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    pub fn extract_error_header(self) -> ErrorHeader {
        match self {
            Error::Engine(EngineError(box EngineErrorInner::Request(e)))
            | Error::Txn(TxnError(box TxnErrorInner::Engine(EngineError(
                box EngineErrorInner::Request(e),
            ))))
            | Error::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::Engine(EngineError(box EngineErrorInner::Request(e))),
            ))))
            | Error::Request(e) => e,
            other => {
                let mut e = ErrorHeader::default();
                e.set_message(format!("{:?}", other));
                e
            }
        }
    }
}
