// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use error_code::{self, ErrorCode, ErrorCodeExt};
use std::error;
use std::result;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: std::io::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        ClusterBootstrapped(cluster_id: u64) {
            display("cluster {} is already bootstrapped", cluster_id)
        }
        ClusterNotBootstrapped(cluster_id: u64) {
            display("cluster {} is not bootstrapped", cluster_id)
        }
        Incompatible {
            display("feature is not supported in other cluster components")
        }
        Grpc(err: grpcio::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        Other(err: Box<dyn error::Error + Sync + lightlike>) {
            from()
            cause(err.as_ref())
            display("unknown error {:?}", err)
        }
        BraneNotFound(key: Vec<u8>) {
            display("brane is not found for key {}", hex::encode_upper(key))
        }
        StoreTombstone(msg: String) {
            display("store is tombstone {:?}", msg)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Io(_) => error_code::fidel::IO,
            Error::ClusterBootstrapped(_) => error_code::fidel::CLUSTER_BOOTSTRAPPED,
            Error::ClusterNotBootstrapped(_) => error_code::fidel::CLUSTER_NOT_BOOTSTRAPPED,
            Error::Incompatible => error_code::fidel::INCOMPATIBLE,
            Error::Grpc(_) => error_code::fidel::GRPC,
            Error::BraneNotFound(_) => error_code::fidel::REGION_NOT_FOUND,
            Error::StoreTombstone(_) => error_code::fidel::STORE_TOMBSTONE,
            Error::Other(_) => error_code::fidel::UNKNOWN,
        }
    }
}
