// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use rusqlite;
use uuid::Uuid;

use allegrosql_promises::{
    SolitonId,
};

use public_promises::errors::{
    Result,
};

use lenin_promises::errors::{
    LeninError,
};

use types::{
    LocalGlobalTxMapping,
};

// Exposes a causetx<->uuid mapping interface.
pub struct TxMapper {}

impl TxMapper {
    pub fn set_lg_mappings(edb_causecausetx: &mut rusqlite::Transaction, mappings: Vec<LocalGlobalTxMapping>) -> Result<()> {
        let mut stmt = edb_causecausetx.prepare_cached(
            "INSERT OR REPLACE INTO lenin_tu (causetx, uuid) VALUES (?, ?)"
        )?;
        for mapping in mappings.iter() {
            let uuid_bytes = mapping.remote.as_bytes().to_vec();
            stmt.execute(&[&mapping.local, &uuid_bytes])?;
        }
        Ok(())
    }

    pub fn set_lg_mapping(edb_causecausetx: &mut rusqlite::Transaction, mapping: LocalGlobalTxMapping) -> Result<()> {
        TxMapper::set_lg_mappings(edb_causecausetx, vec![mapping])
    }

    // TODO for when we're downloading, right?
    pub fn get_or_set_uuid_for_causecausetx(edb_causecausetx: &mut rusqlite::Transaction, causetx: SolitonId) -> Result<Uuid> {
        match TxMapper::get(edb_causecausetx, causetx)? {
            Some(uuid) => Ok(uuid),
            None => {
                let uuid = Uuid::new_v4();
                let uuid_bytes = uuid.as_bytes().to_vec();
                edb_causecausetx.execute("INSERT INTO lenin_tu (causetx, uuid) VALUES (?, ?)", &[&causetx, &uuid_bytes])?;
                return Ok(uuid);
            }
        }
    }

    pub fn get_causecausetx_for_uuid(edb_causecausetx: &rusqlite::Transaction, uuid: &Uuid) -> Result<Option<SolitonId>> {
        let mut stmt = edb_causecausetx.prepare_cached(
            "SELECT causetx FROM lenin_tu WHERE uuid = ?"
        )?;

        let uuid_bytes = uuid.as_bytes().to_vec();
        let results = stmt.causetq_map(&[&uuid_bytes], |r| r.get(0))?;

        let mut causecausetxs = vec![];
        causecausetxs.extend(results);
        if causecausetxs.len() == 0 {
            return Ok(None);
        } else if causecausetxs.len() > 1 {
            bail!(LeninError::TxIncorrectlyMapped(causecausetxs.len()));
        }
        Ok(Some(causecausetxs.remove(0)?))
    }

    pub fn get(edb_causecausetx: &rusqlite::Transaction, causetx: SolitonId) -> Result<Option<Uuid>> {
        let mut stmt = edb_causecausetx.prepare_cached(
            "SELECT uuid FROM lenin_tu WHERE causetx = ?"
        )?;

        let results = stmt.causetq_and_then(&[&causetx], |r| -> Result<Uuid>{
            let bytes: Vec<u8> = r.get(0);
            Uuid::from_bytes(bytes.as_slice()).map_err(|e| e.into())
        })?;

        let mut uuids = vec![];
        uuids.extend(results);
        if uuids.len() == 0 {
            return Ok(None);
        } else if uuids.len() > 1 {
            bail!(LeninError::TxIncorrectlyMapped(uuids.len()));
        }
        Ok(Some(uuids.remove(0)?))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use schemaReplicant;

    #[test]
    fn test_getters() {
        let mut conn = schemaReplicant::tests::setup_conn_bare();
        let mut causetx = schemaReplicant::tests::setup_causecausetx(&mut conn);
        assert_eq!(None, TxMapper::get(&mut causetx, 1).expect("success"));
        let set_uuid = TxMapper::get_or_set_uuid_for_causecausetx(&mut causetx, 1).expect("success");
        assert_eq!(Some(set_uuid), TxMapper::get(&mut causetx, 1).expect("success"));
    }

    #[test]
    fn test_bulk_setter() {
        let mut conn = schemaReplicant::tests::setup_conn_bare();
        let mut causetx = schemaReplicant::tests::setup_causecausetx(&mut conn);
        

        TxMapper::set_lg_mappings(&mut causetx, vec![]).expect("empty map success");

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        TxMapper::set_lg_mappings(
            &mut causetx,
            vec![(1, &uuid1).into(), (2, &uuid2).into()]
        ).expect("map success");
        assert_eq!(Some(uuid1), TxMapper::get(&mut causetx, 1).expect("success"));
        assert_eq!(Some(uuid2), TxMapper::get(&mut causetx, 2).expect("success"));

        // Now let's replace one of the mappings.
        let new_uuid2 = Uuid::new_v4();

        TxMapper::set_lg_mappings(
            &mut causetx,
            vec![(1, &uuid1).into(), (2, &new_uuid2).into()]
        ).expect("map success");
        assert_eq!(Some(uuid1), TxMapper::get(&mut causetx, 1).expect("success"));
        assert_eq!(Some(new_uuid2), TxMapper::get(&mut causetx, 2).expect("success"));
    }
}
