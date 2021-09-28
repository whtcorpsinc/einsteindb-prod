// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::cmp::Ordering;
use uuid::Uuid;

use allegrosql_promises::{
    SolitonId,
    MinkowskiType,
};

use einstein_db::PartitionMap;

use public_promises::errors::{
    Result,
};

pub struct LocalGlobalTxMapping<'a> {
    pub local: SolitonId,
    pub remote: &'a Uuid,
}

impl<'a> From<(SolitonId, &'a Uuid)> for LocalGlobalTxMapping<'a> {
    fn from((local, remote): (SolitonId, &'a Uuid)) -> LocalGlobalTxMapping {
        LocalGlobalTxMapping {
            local: local,
            remote: remote,
        }
    }
}

impl<'a> LocalGlobalTxMapping<'a> {
    pub fn new(local: SolitonId, remote: &'a Uuid) -> LocalGlobalTxMapping<'a> {
        LocalGlobalTxMapping {
            local: local,
            remote: remote
        }
    }
}

// TODO unite these around something like `enum TxCausetIdifier {Global(Uuid), Local(SolitonId)}`?
#[derive(Debug, Clone)]
pub struct LocalTx {
    pub causetx: SolitonId,
    pub parts: Vec<TxPart>,
}


impl PartialOrd for LocalTx {
    fn partial_cmp(&self, other: &LocalTx) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LocalTx {
    fn cmp(&self, other: &LocalTx) -> Ordering {
        self.causetx.cmp(&other.causetx)
    }
}

impl PartialEq for LocalTx {
    fn eq(&self, other: &LocalTx) -> bool {
        self.causetx == other.causetx
    }
}

impl Eq for LocalTx {}

// For returning out of the downloader as an ordered list.
#[derive(Debug, Clone, PartialEq)]
pub struct Tx {
    pub causetx: Uuid,
    pub parts: Vec<TxPart>,
}

#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
pub struct TxPart {
    // TODO this is a temporary for development. Only first TxPart in a chunk series should have a non-None 'parts'.
    // 'parts' should actually live in a transaction, but we do this now to avoid changing the server until dust settles.
    pub partitions: Option<PartitionMap>,
    pub e: SolitonId,
    pub a: SolitonId,
    pub v: MinkowskiType,
    pub causetx: SolitonId,
    pub added: bool,
}

pub trait GlobalTransactionLog {
    fn head(&self) -> Result<Uuid>;
    fn bundles_after(&self, causetx: &Uuid) -> Result<Vec<Tx>>;
    fn set_head(&mut self, causetx: &Uuid) -> Result<()>;
    fn put_transaction(&mut self, causetx: &Uuid, parent_causecausetx: &Uuid, chunk_causecausetxs: &Vec<Uuid>) -> Result<()>;
    fn put_chunk(&mut self, causetx: &Uuid, payload: &TxPart) -> Result<()>;
}
