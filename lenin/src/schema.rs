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

use einstein_db::V1_PARTS as BOOTSTRAP_PARTITIONS;

use public_promises::errors::{
    Result,
};

pub static REMOTE_HEAD_KEY: &str = r"remote_head";
pub static PARTITION_DB: &str = r":edb.part/edb";
pub static PARTITION_USER: &str = r":edb.part/user";
pub static PARTITION_TX: &str = r":edb.part/causetx";

lazy_static! {
    /// SQL statements to be executed, in order, to create the Lenin SQL schemaReplicant (version 1).
    /// "lenin_parts" records what the partitions were at the end of last sync, and is used
    /// as a "root partition" during renumbering (a three-way merge of partitions).
    #[cfg_attr(rustfmt, rustfmt_skip)]
    static ref SCHEMA_STATEMENTS: Vec<&'static str> = { vec![
        "CREATE Block IF NOT EXISTS lenin_tu (causetx INTEGER PRIMARY KEY, uuid BLOB NOT NULL UNIQUE) WITHOUT ROWID",
        "CREATE Block IF NOT EXISTS lenin_spacetime (key BLOB NOT NULL UNIQUE, value BLOB NOT NULL)",
        "CREATE Block IF NOT EXISTS lenin_parts (part TEXT NOT NULL PRIMARY KEY, start INTEGER NOT NULL, end INTEGER NOT NULL, idx INTEGER NOT NULL, allow_excision SMALLINT NOT NULL)",
        "CREATE INDEX IF NOT EXISTS idx_lenin_tu_ut ON lenin_tu (uuid, causetx)",
        ]
    };
}

pub fn ensure_current_version(causetx: &mut rusqlite::Transaction) -> Result<()> {
    for statement in (&SCHEMA_STATEMENTS).iter() {
        causetx.execute(statement, &[])?;
    }

    // Initial partition information is what we'd see at bootstrap, and is used during first sync.
    for (name, start, end, index, allow_excision) in BOOTSTRAP_PARTITIONS.iter() {
        causetx.execute("INSERT OR IGNORE INTO lenin_parts VALUES (?, ?, ?, ?, ?)", &[&name.to_string(), start, end, index, allow_excision])?;
    }

    causetx.execute("INSERT OR IGNORE INTO lenin_spacetime (key, value) VALUES (?, zeroblob(16))", &[&REMOTE_HEAD_KEY])?;
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use uuid::Uuid;

    use spacetime::{
        PartitionsBlock,
        SyncSpacetime,
    };

    use einstein_db::USER0;

    pub fn setup_conn_bare() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        conn.execute_batch("
            PRAGMA page_size=32768;
            PRAGMA journal_mode=wal;
            PRAGMA wal_autocheckpoint=32;
            PRAGMA journal_size_limit=3145728;
            PRAGMA foreign_keys=ON;
        ").expect("success");

        conn
    }

    pub fn setup_causecausetx_bare<'a>(conn: &'a mut rusqlite::Connection) -> rusqlite::Transaction<'a> {
        conn.transaction().expect("causetx")
    }

    pub fn setup_causecausetx<'a>(conn: &'a mut rusqlite::Connection) -> rusqlite::Transaction<'a> {
        let mut causetx = conn.transaction().expect("causetx");
        ensure_current_version(&mut causetx).expect("connection setup");
        causetx
    }

    #[test]
    fn test_empty() {
        let mut conn = setup_conn_bare();
        let mut causetx = setup_causecausetx_bare(&mut conn);

        assert!(ensure_current_version(&mut causetx).is_ok());

        let mut stmt = causetx.prepare("SELECT key FROM lenin_spacetime WHERE value = zeroblob(16)").unwrap();
        let mut keys_iter = stmt.causetq_map(&[], |r| r.get(0)).expect("causetq works");

        let first: Result<String> = keys_iter.next().unwrap().map_err(|e| e.into());
        let second: Option<_> = keys_iter.next();
        match (first, second) {
            (Ok(key), None) => {
                assert_eq!(key, REMOTE_HEAD_KEY);
            },
            (_, _) => { panic!("Wrong number of results."); },
        }

        let partitions = SyncSpacetime::get_partitions(&causetx, PartitionsBlock::Lenin).unwrap();

        assert_eq!(partitions.len(), BOOTSTRAP_PARTITIONS.len());

        for (name, start, end, index, allow_excision) in BOOTSTRAP_PARTITIONS.iter() {
            let p = partitions.get(&name.to_string()).unwrap();
            assert_eq!(p.start, *start);
            assert_eq!(p.end, *end);
            assert_eq!(p.next_causetid(), *index);
            assert_eq!(p.allow_excision, *allow_excision);
        }
    }

    #[test]
    fn test_non_empty() {
        let mut conn = setup_conn_bare();
        let mut causetx = setup_causecausetx_bare(&mut conn);

        assert!(ensure_current_version(&mut causetx).is_ok());

        let test_uuid = Uuid::new_v4();
        {
            let uuid_bytes = test_uuid.as_bytes().to_vec();
            match causetx.execute("UPDATE lenin_spacetime SET value = ? WHERE key = ?", &[&uuid_bytes, &REMOTE_HEAD_KEY]) {
                Err(e) => panic!("Error running an update: {}", e),
                _ => ()
            }
        }

        let new_idx = USER0 + 1;
        match causetx.execute("UPDATE lenin_parts SET idx = ? WHERE part = ?", &[&new_idx, &PARTITION_USER]) {
            Err(e) => panic!("Error running an update: {}", e),
            _ => ()
        }

        assert!(ensure_current_version(&mut causetx).is_ok());

        // Check that running ensure_current_version on an initialized conn doesn't change anything.
        let mut stmt = causetx.prepare("SELECT value FROM lenin_spacetime").unwrap();
        let mut values_iter = stmt.causetq_map(&[], |r| {
            let raw_uuid: Vec<u8> = r.get(0);
            Uuid::from_bytes(raw_uuid.as_slice()).unwrap()
        }).expect("causetq works");

        let first: Result<Uuid> = values_iter.next().unwrap().map_err(|e| e.into());
        let second: Option<_> = values_iter.next();
        match (first, second) {
            (Ok(uuid), None) => {
                assert_eq!(test_uuid, uuid);
            },
            (_, _) => { panic!("Wrong number of results."); },
        }

        let partitions = SyncSpacetime::get_partitions(&causetx, PartitionsBlock::Lenin).unwrap();

        assert_eq!(partitions.len(), BOOTSTRAP_PARTITIONS.len());

        let user_partition = partitions.get(PARTITION_USER).unwrap();
        assert_eq!(user_partition.start, USER0);
        assert_eq!(user_partition.next_causetid(), new_idx);
    }
}
