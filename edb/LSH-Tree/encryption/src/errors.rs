// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use error_code::{self, ErrorCode, ErrorCodeExt};
use openssl::error::ErrorStack as CrypterError;
use protobuf::ProtobufError;
use std::io::{Error as IoError, ErrorKind};
use std::{error, result};

/// The error type for encryption.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Other error {}", _0)]
    Other(Box<dyn error::Error + Sync + lightlike>),
    #[fail(display = "Lmdb error {}", _0)]
    Lmdb(String),
    #[fail(display = "IO error {}", _0)]
    Io(IoError),
    #[fail(display = "OpenSSL error {}", _0)]
    Crypter(CrypterError),
    #[fail(display = "Protobuf error {}", _0)]
    Proto(ProtobufError),
    #[fail(display = "Unknown encryption error")]
    UnknownEncryption,
    #[fail(display = "Wrong master key error {}", _0)]
    WrongMasterKey(Box<dyn error::Error + Sync + lightlike>),
    #[fail(
        display = "Both master key failed, current key {}, previous key {}.",
        _0, _1
    )]
    BothMasterKeyFail(
        Box<dyn error::Error + Sync + lightlike>,
        Box<dyn error::Error + Sync + lightlike>,
    ),
}

macro_rules! impl_from {
    ($($inner:ty => $container:ident,)+) => {
        $(
            impl From<$inner> for Error {
                fn from(inr: $inner) -> Error {
                    Error::$container(inr)
                }
            }
        )+
    };
}

impl_from! {
    Box<dyn error::Error + Sync + lightlike> => Other,
    String => Lmdb,
    IoError => Io,
    CrypterError => Crypter,
    ProtobufError => Proto,
}

impl From<Error> for IoError {
    fn from(err: Error) -> IoError {
        match err {
            Error::Io(e) => e,
            other => IoError::new(ErrorKind::Other, format!("{}", other)),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Lmdb(_) => error_code::encryption::ROCKS,
            Error::Io(_) => error_code::encryption::IO,
            Error::Crypter(_) => error_code::encryption::CRYPTER,
            Error::Proto(_) => error_code::encryption::PROTO,
            Error::UnknownEncryption => error_code::encryption::UNKNOWN_ENCRYPTION,
            Error::WrongMasterKey(_) => error_code::encryption::WRONG_MASTER_KEY,
            Error::BothMasterKeyFail(_, _) => error_code::encryption::BOTH_MASTER_KEY_FAIL,
            Error::Other(_) => error_code::UNKNOWN,
        }
    }
}
