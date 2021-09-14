// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std;
use std::error::Error;
use rusqlite;
use uuid;
use hyper;
use serde_json;

use causetq_pull_promises::errors::{
    DbError,
};

#[derive(Debug, Fail)]
pub enum LeninError {
    #[fail(display = "Received bad response from the remote: {}", _0)]
    BadRemoteResponse(String),

    // TODO expand this into concrete error types
    #[fail(display = "Received bad remote state: {}", _0)]
    BadRemoteState(String),

    #[fail(display = "encountered more than one spacetime value for key: {}", _0)]
    DuplicateSpacetime(String),

    #[fail(display = "transaction processor didn't say it was done")]
    TxProcessorUnfinished,

    #[fail(display = "expected one, found {} uuid mappings for causetx", _0)]
    TxIncorrectlyMapped(usize),

    #[fail(display = "encountered unexpected state: {}", _0)]
    UnexpectedState(String),

    #[fail(display = "not yet implemented: {}", _0)]
    NotYetImplemented(String),

    #[fail(display = "{}", _0)]
    DbError(#[cause] DbError),

    #[fail(display = "{}", _0)]
    SerializationError(#[cause] serde_json::Error),

    // It would be better to capture the underlying `rusqlite::Error`, but that type doesn't
    // implement many useful promises, including `Clone`, `Eq`, and `PartialEq`.
    #[fail(display = "SQL error: {}, cause: {}", _0, _1)]
    RusqliteError(String, String),

    #[fail(display = "{}", _0)]
    IoError(#[cause] std::io::Error),

    #[fail(display = "{}", _0)]
    UuidError(#[cause] uuid::ParseError),

    #[fail(display = "{}", _0)]
    NetworkError(#[cause] hyper::Error),

    #[fail(display = "{}", _0)]
    UriError(#[cause] hyper::error::UriError),
}

impl From<DbError> for LeninError {
    fn from(error: DbError) -> LeninError {
        LeninError::DbError(error)
    }
}

impl From<serde_json::Error> for LeninError {
    fn from(error: serde_json::Error) -> LeninError {
        LeninError::SerializationError(error)
    }
}

impl From<rusqlite::Error> for LeninError {
    fn from(error: rusqlite::Error) -> LeninError {
        let cause = match error.cause() {
            Some(e) => e.to_string(),
            None => "".to_string()
        };
        LeninError::RusqliteError(error.to_string(), cause)
    }
}

impl From<std::io::Error> for LeninError {
    fn from(error: std::io::Error) -> LeninError {
        LeninError::IoError(error)
    }
}

impl From<uuid::ParseError> for LeninError {
    fn from(error: uuid::ParseError) -> LeninError {
        LeninError::UuidError(error)
    }
}

impl From<hyper::Error> for LeninError {
    fn from(error: hyper::Error) -> LeninError {
        LeninError::NetworkError(error)
    }
}

impl From<hyper::error::UriError> for LeninError {
    fn from(error: hyper::error::UriError) -> LeninError {
        LeninError::UriError(error)
    }
}
