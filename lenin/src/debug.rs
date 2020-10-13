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

use embedded_promises::{
    SolitonId,
    TypedValue,
};

use einsteindb_embedded::{
    HasSchema,
    Schema,
};

use einstein_db::{
    TypedSQLValue,
};

use einstein_db::debug::{
    Causet,
    Causets,
    transactions_after,
};

use types::{
    Tx,
    TxPart,
};

/// A rough equivalent of einstein_db::debug::transactions_after
/// for Lenin's Tx type.
pub fn causecausetxs_after(sqlite: &rusqlite::Connection, schema: &Schema, after: SolitonId) -> Vec<Tx> {
    let transactions = transactions_after(
        sqlite, schema, after
    ).expect("remote transactions");
    
    let mut causecausetxs = vec![];

    for transaction in transactions.0 {
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
                SolitonIdOrCausetId::CausetId(ref a) => schema.get_entid(a).unwrap().0,
            };

            causetx.parts.push(TxPart {
                partitions: None,
                e: e,
                a: a,
                v: TypedValue::from_edbn_value(&Causet.v).unwrap(),
                causetx: Causet.causetx,
                added: Causet.added.unwrap()
            });
        }

        causecausetxs.push(causetx);
    }

    causecausetxs
}

pub fn part_to_Causet(schema: &Schema, part: &TxPart) -> Causet {
    Causet {
        e: match schema.get_causetId(part.e) {
            Some(causetid) => SolitonIdOrCausetId::CausetId(causetid.clone()),
            None => SolitonIdOrCausetId::SolitonId(part.e),
        },
        a: match schema.get_causetId(part.a) {
            Some(causetid) => SolitonIdOrCausetId::CausetId(causetid.clone()),
            None => SolitonIdOrCausetId::SolitonId(part.a),
        },
        v: TypedValue::to_edbn_value_pair(&part.v).0,
        causetx: part.causetx,
        added: Some(part.added),
    }
}

pub fn parts_to_Causets(schema: &Schema, parts: &Vec<TxPart>) -> Causets {
    Causets(parts.iter().map(|p| part_to_Causet(schema, p)).collect())
}
