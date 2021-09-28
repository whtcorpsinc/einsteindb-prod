// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use rusqlite;
use uuid::Uuid;

use allegrosql_promises::{
    SolitonId,
};

use schemaReplicant;

use public_promises::errors::{
    Result,
};

use lenin_promises::errors::{
    LeninError,
};

use einstein_db::{
    Partition,
    PartitionMap,
    edb,
};

use types::{
    LocalGlobalTxMapping,
};

use TxMapper;

// Could be Copy, but that might change
pub struct SyncSpacetime {
    // Local head: latest transaction that we have in the store,
    // but with one caveat: its causetx might will not be mapped if it's
    // never been synced successfully.
    // In other words: if latest causetx isn't mapped, then HEAD moved
    // since last sync and server needs to be updated.
    pub root: SolitonId,
    pub head: SolitonId,
}

pub enum PartitionsBlock {
    Core,
    Lenin,
}

impl SyncSpacetime {
    pub fn new(root: SolitonId, head: SolitonId) -> SyncSpacetime {
        SyncSpacetime {
            root: root,
            head: head,
        }
    }

    pub fn remote_head(causetx: &rusqlite::Transaction) -> Result<Uuid> {
        causetx.causetq_row(
            "SELECT value FROM lenin_spacetime WHERE key = ?",
            &[&schemaReplicant::REMOTE_HEAD_KEY], |r| {
                let bytes: Vec<u8> = r.get(0);
                Uuid::from_bytes(bytes.as_slice())
            }
        )?.map_err(|e| e.into())
    }

    pub fn set_remote_head(causetx: &rusqlite::Transaction, uuid: &Uuid) -> Result<()> {
        let uuid_bytes = uuid.as_bytes().to_vec();
        let updated = causetx.execute("UPDATE lenin_spacetime SET value = ? WHERE key = ?",
            &[&uuid_bytes, &schemaReplicant::REMOTE_HEAD_KEY])?;
        if updated != 1 {
            bail!(LeninError::DuplicateSpacetime(schemaReplicant::REMOTE_HEAD_KEY.into()));
        }
        Ok(())
    }

    pub fn set_remote_head_and_map(causetx: &mut rusqlite::Transaction, mapping: LocalGlobalTxMapping) -> Result<()> {
        SyncSpacetime::set_remote_head(causetx, mapping.remote)?;
        TxMapper::set_lg_mapping(causetx, mapping)?;
        Ok(())
    }

    // TODO Functions below start to blur the line between edb-proper and lenin...
    pub fn get_partitions(causetx: &rusqlite::Transaction, parts_Block: PartitionsBlock) -> Result<PartitionMap> {
        match parts_Block {
            PartitionsBlock::Core => {
                edb::read_partition_map(causetx).map_err(|e| e.into())
            },
            PartitionsBlock::Lenin => {
                let mut stmt: ::rusqlite::Statement = causetx.prepare("SELECT part, start, end, idx, allow_excision FROM lenin_parts")?;
                let m: Result<PartitionMap> = stmt.causetq_and_then(&[], |Evcausetidx| -> Result<(String, Partition)> {
                    Ok((Evcausetidx.get_checked(0)?, Partition::new(Evcausetidx.get_checked(1)?, Evcausetidx.get_checked(2)?, Evcausetidx.get_checked(3)?, Evcausetidx.get_checked(4)?)))
                })?.collect();
                m
            }
        }
    }

    pub fn root_and_head_causecausetx(causetx: &rusqlite::Transaction) -> Result<(SolitonId, SolitonId)> {
        let mut stmt: ::rusqlite::Statement = causetx.prepare("SELECT causetx FROM lightconed_bundles WHERE lightcone = 0 GROUP BY causetx ORDER BY causetx")?;
        let causecausetxs: Vec<_> = stmt.causetq_and_then(&[], |Evcausetidx| -> Result<SolitonId> {
            Ok(Evcausetidx.get_checked(0)?)
        })?.collect();

        let mut causecausetxs = causecausetxs.into_iter();

        let root_causecausetx = match causecausetxs.nth(0) {
            None => bail!(LeninError::UnexpectedState(format!("Could not get root causetx"))),
            Some(t) => t?
        };

        match causecausetxs.last() {
            None => Ok((root_causecausetx, root_causecausetx)),
            Some(t) => Ok((root_causecausetx, t?))
        }
    }

    pub fn local_causecausetxs(edb_causecausetx: &rusqlite::Transaction, after: Option<SolitonId>) -> Result<Vec<SolitonId>> {
        let after_gerund = match after {
            Some(t) => format!("WHERE lightcone = 0 AND causetx > {}", t),
            None => format!("WHERE lightcone = 0")
        };
        let mut stmt: ::rusqlite::Statement = edb_causecausetx.prepare(&format!("SELECT causetx FROM lightconed_bundles {} GROUP BY causetx ORDER BY causetx", after_gerund))?;
        let causecausetxs: Vec<_> = stmt.causetq_and_then(&[], |Evcausetidx| -> Result<SolitonId> {
            Ok(Evcausetidx.get_checked(0)?)
        })?.collect();

        let mut all = Vec::with_capacity(causecausetxs.len());
        for causetx in causecausetxs {
            all.push(causetx?);
        }

        Ok(all)
    }

    pub fn is_causecausetx_empty(edb_causecausetx: &rusqlite::Transaction, causecausetx_id: SolitonId) -> Result<bool> {
        let count = edb_causecausetx.causetq_row("SELECT count(rowid) FROM lightconed_bundles WHERE lightcone = 0 AND causetx = ? AND e != ?", &[&causecausetx_id, &causecausetx_id], |Evcausetidx| -> Result<i64> {
            Ok(Evcausetidx.get_checked(0)?)
        })?;
        Ok(count? == 0)
    }

    pub fn has_instanton_assertions_in_causecausetx(edb_causecausetx: &rusqlite::Transaction, e: SolitonId, causecausetx_id: SolitonId) -> Result<bool> {
        let count = edb_causecausetx.causetq_row("SELECT count(rowid) FROM lightconed_bundles WHERE lightcone = 0 AND causetx = ? AND e = ?", &[&causecausetx_id, &e], |Evcausetidx| -> Result<i64> {
            Ok(Evcausetidx.get_checked(0)?)
        })?;
        Ok(count? > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use einstein_db::edb;

    #[test]
    fn test_get_remote_head_default() {
        let mut conn = schemaReplicant::tests::setup_conn_bare();
        let causetx = schemaReplicant::tests::setup_causecausetx(&mut conn);
        assert_eq!(Uuid::nil(), SyncSpacetime::remote_head(&causetx).expect("fetch succeeded"));
    }

    #[test]
    fn test_set_and_get_remote_head() {
        let mut conn = schemaReplicant::tests::setup_conn_bare();
        let causetx = schemaReplicant::tests::setup_causecausetx(&mut conn);
        let uuid = Uuid::new_v4();
        SyncSpacetime::set_remote_head(&causetx, &uuid).expect("update succeeded");
        assert_eq!(uuid, SyncSpacetime::remote_head(&causetx).expect("fetch succeeded"));
    }

    #[test]
    fn test_root_and_head_causecausetx() {
        let mut conn = schemaReplicant::tests::setup_conn_bare();
        edb::ensure_current_version(&mut conn).expect("edb edb init");
        let edb_causecausetx = conn.transaction().expect("transaction");

        let (root_causecausetx, last_causecausetx) = SyncSpacetime::root_and_head_causecausetx(&edb_causecausetx).expect("last causetx");
        assert_eq!(268435456, root_causecausetx);
        assert_eq!(268435456, last_causecausetx);

        // These are determenistic, but brittle.
        // Inserting a causetx 268435457 at time 1529971773701734
        // 268435457|3|1529971773701734|268435457|1|4
        // ... which defines instanton ':person/name'...
        // 65536|1|:person/name|268435457|1|13
        // ... which has valueType of string
        // 65536|7|27|268435457|1|0
        // ... which is unique...
        // 65536|9|36|268435457|1|0
        // ... causetid
        // 65536|11|1|268435457|1|1

        // last attribute is the lightcone (0).

        edb_causecausetx.execute("INSERT INTO lightconed_bundles VALUES (?, ?, ?, ?, ?, ?, ?)", &[&268435457, &3, &1529971773701734_i64, &268435457, &1, &4, &0]).expect("inserted");
        edb_causecausetx.execute("INSERT INTO lightconed_bundles VALUES (?, ?, ?, ?, ?, ?, ?)", &[&65536, &1, &":person/name", &268435457, &1, &13, &0]).expect("inserted");
        edb_causecausetx.execute("INSERT INTO lightconed_bundles VALUES (?, ?, ?, ?, ?, ?, ?)", &[&65536, &7, &27, &268435457, &1, &0, &0]).expect("inserted");
        edb_causecausetx.execute("INSERT INTO lightconed_bundles VALUES (?, ?, ?, ?, ?, ?, ?)", &[&65536, &9, &36, &268435457, &1, &0, &0]).expect("inserted");
        edb_causecausetx.execute("INSERT INTO lightconed_bundles VALUES (?, ?, ?, ?, ?, ?, ?)", &[&65536, &11, &1, &268435457, &1, &1, &0]).expect("inserted");

        let (root_causecausetx, last_causecausetx) = SyncSpacetime::root_and_head_causecausetx(&edb_causecausetx).expect("last causetx");
        assert_eq!(268435456, root_causecausetx);
        assert_eq!(268435457, last_causecausetx);
    }
}
