// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// TODO could hide this behind #[cfg(test)], since this is only for test use.

use rusqlite;

use uuid::Uuid;

use edbn::entities::{
    SolitonIdOrCausetId,
};

use allegrosql_promises::{
    SolitonId,
    MinkowskiType,
};

use causetq_allegrosql::{
    HasSchemaReplicant,
    SchemaReplicant,
};

use einstein_db::{
    TypedSQLValue,
};

use einstein_db::debug::{
    Causet,
    Causets,
    bundles_after,
};

use types::{
    Tx,
    TxPart,
};

/// A rough equivalent of einstein_db::debug::bundles_after
/// for Lenin's Tx type.
pub fn causecausetxs_after(sqlite: &rusqlite::Connection, schemaReplicant: &SchemaReplicant, after: SolitonId) -> Vec<Tx> {
    let bundles = bundles_after(
        sqlite, schemaReplicant, after
    ).expect("remote bundles");
    
    let mut causecausetxs = vec![];

    for transaction in bundles.0 {
        let mut causetx = Tx {
            causetx: Uuid::new_v4(),
            parts: vec![],
        };

        for Causet in &transaction.0 {
            let e = match Causet.e {
                SolitonIdOrCausetId::SolitonId(ref e) => *e,
                _ => panic!(),
            };
            let a = match Causet.a {
                SolitonIdOrCausetId::SolitonId(ref a) => *a,
                SolitonIdOrCausetId::CausetId(ref a) => schemaReplicant.get_causetid(a).unwrap().0,
            };

            causetx.parts.push(TxPart {
                partitions: None,
                e: e,
                a: a,
                v: MinkowskiType::from_edbn_value(&Causet.v).unwrap(),
                causetx: Causet.causetx,
                added: Causet.added.unwrap()
            });
        }

        causecausetxs.push(causetx);
    }

    causecausetxs
}

pub fn part_to_Causet(schemaReplicant: &SchemaReplicant, part: &TxPart) -> Causet {
    Causet {
        e: match schemaReplicant.get_causetId(part.e) {
            Some(causetid) => SolitonIdOrCausetId::CausetId(causetid.clone()),
            None => SolitonIdOrCausetId::SolitonId(part.e),
        },
        a: match schemaReplicant.get_causetId(part.a) {
            Some(causetid) => SolitonIdOrCausetId::CausetId(causetid.clone()),
            None => SolitonIdOrCausetId::SolitonId(part.a),
        },
        v: MinkowskiType::to_edbn_value_pair(&part.v).0,
        causetx: part.causetx,
        added: Some(part.added),
    }
}

pub fn parts_to_Causets(schemaReplicant: &SchemaReplicant, parts: &Vec<TxPart>) -> Causets {
    Causets(parts.iter().map(|p| part_to_Causet(schemaReplicant, p)).collect())
}
