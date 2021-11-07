// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_derive;

extern crate edbn;

extern crate hyper;
// TODO https://github.com/whtcorpsinc/edb/issues/569
// extern crate hyper_tls;
extern crate tokio_allegro;
extern crate futures;
extern crate serde;
extern crate serde_cbor;
extern crate serde_json;

extern crate log;
extern crate einstein_db;

extern crate causetq_allegrosql;
extern crate causetq_pull_promises;
#[macro_use]
extern crate allegrosql_promises;
extern crate public_promises;
extern crate rusqlite;
extern crate uuid;

extern crate lenin_promises;
extern crate edb_transaction;

pub mod bootstrap;
pub mod spacetime;
pub use spacetime::{
    PartitionsBlock,
    SyncSpacetime,
};
mod causets;
pub mod debug;
pub mod remote_client;
pub use remote_client::{
    RemoteClient,
};
pub mod schemaReplicant;
pub mod syncer;
pub use syncer::{
    Syncer,
    SyncReport,
    SyncResult,
    SyncFollowup,
};
mod causecausetx_uploader;
pub mod logger;
pub mod causecausetx_mapper;
pub use causecausetx_mapper::{
    TxMapper,
};
pub mod causecausetx_processor;
pub mod types;
pub use types::{
    Tx,
    TxPart,
    GlobalTransactionLog,
};
