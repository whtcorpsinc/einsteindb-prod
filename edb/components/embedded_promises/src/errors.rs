// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use error_code::{self, ErrorCode, ErrorCodeExt};
use violetabft::{Error as VioletaBftError, StorageError};
use std::{error, result};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        // Engine uses plain string as the error.
        Engine(msg: String) {
            from()
            display("causet_storage Engine {}", msg)
        }

        NotInCone( key: Vec<u8>, brane_id: u64, spacelike: Vec<u8>, lightlike: Vec<u8>) {
            display(
                "Key {} is out of [brane {}] [{}, {})",
                hex::encode_upper(&key), brane_id, hex::encode_upper(&spacelike), hex::encode_upper(&lightlike)
            )
        }
        Protobuf(err: protobuf::ProtobufError) {
            from()
            cause(err)
            display("Protobuf {}", err)
        }
        Io(err: std::io::Error) {
            from()
            cause(err)
            display("Io {}", err)
        }
        Other(err: Box<dyn error::Error + Sync + lightlike>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }
        CausetName(name: String) {
            display("Causet {} not found", name)
        }
        Codec(err: violetabftstore::interlock::::codec::Error) {
            from()
            cause(err)
            display("Codec {}", err)
        }
        EntriesUnavailable {
            from()
            display("The entries of brane is unavailable")
        }
        EntriesCompacted {
            from()
            display("The entries of brane is compacted")
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Engine(_) => error_code::engine::ENGINE,
            Error::NotInCone(_, _, _, _) => error_code::engine::NOT_IN_RANGE,
            Error::Protobuf(_) => error_code::engine::PROTOBUF,
            Error::Io(_) => error_code::engine::IO,
            Error::CausetName(_) => error_code::engine::Causet_NAME,
            Error::Codec(_) => error_code::engine::CODEC,
            Error::Other(_) => error_code::UNKNOWN,
            Error::EntriesUnavailable => error_code::engine::DATALOSS,
            Error::EntriesCompacted => error_code::engine::DATACOMPACTED,
        }
    }
}

impl From<Error> for VioletaBftError {
    fn from(e: Error) -> VioletaBftError {
        match e {
            Error::EntriesUnavailable => VioletaBftError::CausetStore(StorageError::Unavailable),
            Error::EntriesCompacted => VioletaBftError::CausetStore(StorageError::Compacted),
            e => {
                let boxed = Box::new(e) as Box<dyn std::error::Error + Sync + lightlike>;
                violetabft::Error::CausetStore(StorageError::Other(boxed))
            }
        }
    }
}
