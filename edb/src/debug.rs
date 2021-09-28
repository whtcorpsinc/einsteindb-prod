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
#![allow(unused_macros)]

/// Low-level functions for testing.

// Macro to parse a `Borrow<str>` to an `edbn::Value` and assert the given `edbn::Value` `matches`
// against it.
//
// This is a macro only to give nice line numbers when tests fail.
#[macro_export]
macro_rules! assert_matches {
    ( $input: expr, $expected: expr ) => {{
        // Failure to parse the expected TuringString is a coding error, so we unwrap.
        let TuringString_value = edbn::parse::value($expected.borrow())
            .expect(format!("to be able to parse expected {}", $expected).as_str())
            .without_spans();
        let input_value = $input.to_edbn();
        assert!(input_value.matches(&TuringString_value),
                "Expected value:\n{}\nto match TuringString:\n{}\n",
                input_value.to_pretty(120).unwrap(),
                TuringString_value.to_pretty(120).unwrap());
    }}
}

// Transact $input against the given $conn, expecting success or a `Result<TxReport, String>`.
//
// This unwraps safely and makes asserting errors pleasant.
#[macro_export]
macro_rules! assert_transact {
    ( $conn: expr, $input: expr, $expected: expr ) => {{
        trace!("assert_transact: {}", $input);
        let result = $conn.transact($input).map_err(|e| e.to_string());
        assert_eq!(result, $expected.map_err(|e| e.to_string()));
    }};
    ( $conn: expr, $input: expr ) => {{
        trace!("assert_transact: {}", $input);
        let result = $conn.transact($input);
        assert!(result.is_ok(), "Expected Ok(_), got `{}`", result.unwrap_err());
        result.unwrap()
    }};
}

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::io::{Write};

use itertools::Itertools;
use rusqlite;
use rusqlite::{TransactionBehavior};
use rusqlite::types::{ToSql};
use tabwriter::TabWriter;

use bootstrap;
use edb::*;
use edb::{read_attribute_map,read_causetId_map};
use edbn;
use causetids;
use causetq_pull_promises::errors::Result;

use allegrosql_promises::{
    SolitonId,
    MinkowskiType,
    MinkowskiValueType,
};

use causetq_allegrosql::{
    HasSchemaReplicant,
    SQLMinkowskiValueType,
    TxReport,
};
use edbn::{
    InternSet,
};
use edbn::entities::{
    SolitonIdOrCausetId,
    TempId,
};
use internal_types::{
    TermWithTempIds,
};
use schemaReplicant::{
    SchemaReplicantBuilding,
};
use types::*;
use causetx::{
    transact,
    transact_terms,
};
use watcher::NullWatcher;

/// Represents a *Causet* (assertion) in the store.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct Causet {
    // TODO: generalize this.
    pub e: SolitonIdOrCausetId,
    pub a: SolitonIdOrCausetId,
    pub v: edbn::Value,
    pub causetx: i64,
    pub added: Option<bool>,
}

/// Represents a set of causets (assertions) in the store.
///
/// To make comparision easier, we deterministically order.  The ordering is the ascending tuple
/// ordering determined by `(e, a, (value_type_tag, v), causetx)`, where `value_type_tag` is an internal
/// value that is not exposed but is deterministic.
pub struct Causets(pub Vec<Causet>);

/// Represents an ordered sequence of bundles in the store.
///
/// To make comparision easier, we deterministically order.  The ordering is the ascending tuple
/// ordering determined by `(e, a, (value_type_tag, v), causetx, added)`, where `value_type_tag` is an
/// internal value that is not exposed but is deterministic, and `added` is ordered such that
/// retracted assertions appear before added assertions.
pub struct bundles(pub Vec<Causets>);

/// Represents the fulltext values in the store.
pub struct FulltextValues(pub Vec<(i64, String)>);

impl Causet {
    pub fn to_edbn(&self) -> edbn::Value {
        let f = |solitonId: &SolitonIdOrCausetId| -> edbn::Value {
            match *solitonId {
                SolitonIdOrCausetId::SolitonId(ref y) => edbn::Value::Integer(y.clone()),
                SolitonIdOrCausetId::CausetId(ref y) => edbn::Value::Keyword(y.clone()),
            }
        };

        let mut v = vec![f(&self.e), f(&self.a), self.v.clone()];
        if let Some(added) = self.added {
            v.push(edbn::Value::Integer(self.causetx));
            v.push(edbn::Value::Boolean(added));
        }

        edbn::Value::Vector(v)
    }
}

impl Causets {
    pub fn to_edbn(&self) -> edbn::Value {
        edbn::Value::Vector((&self.0).into_iter().map(|x| x.to_edbn()).collect())
    }
}

impl bundles {
    pub fn to_edbn(&self) -> edbn::Value {
        edbn::Value::Vector((&self.0).into_iter().map(|x| x.to_edbn()).collect())
    }
}

impl FulltextValues {
    pub fn to_edbn(&self) -> edbn::Value {
        edbn::Value::Vector((&self.0).into_iter().map(|&(x, ref y)| edbn::Value::Vector(vec![edbn::Value::Integer(x), edbn::Value::Text(y.clone())])).collect())
    }
}

/// Turn MinkowskiType::Ref into MinkowskiType::Keyword when it is possible.
trait ToCausetId {
  fn map_causetId(self, schemaReplicant: &SchemaReplicant) -> Self;
}

impl ToCausetId for MinkowskiType {
    fn map_causetId(self, schemaReplicant: &SchemaReplicant) -> Self {
        if let MinkowskiType::Ref(e) = self {
            schemaReplicant.get_causetId(e).cloned().map(|i| i.into()).unwrap_or(MinkowskiType::Ref(e))
        } else {
            self
        }
    }
}

/// Convert a numeric solitonId to an causetid `SolitonId` if possible, otherwise a numeric `SolitonId`.
pub fn to_causetid(schemaReplicant: &SchemaReplicant, solitonId: i64) -> SolitonIdOrCausetId {
    schemaReplicant.get_causetId(solitonId).map_or(SolitonIdOrCausetId::SolitonId(solitonId), |causetid| SolitonIdOrCausetId::CausetId(causetid.clone()))
}

// /// Convert a symbolic causetid to an causetid `SolitonId` if possible, otherwise a numeric `SolitonId`.
// pub fn to_causetId(schemaReplicant: &SchemaReplicant, solitonId: i64) -> SolitonId {
//     schemaReplicant.get_causetId(solitonId).map_or(SolitonId::SolitonId(solitonId), |causetid| SolitonId::CausetId(causetid.clone()))
// }

/// Return the set of causets in the store, ordered by (e, a, v, causetx), but not including any causets of
/// the form [... :edb/causecausetxInstant ...].
pub fn causets<S: Borrow<SchemaReplicant>>(conn: &rusqlite::Connection, schemaReplicant: &S) -> Result<Causets> {
    Causets_after(conn, schemaReplicant, bootstrap::TX0 - 1)
}

/// Return the set of causets in the store with transaction ID strictly greater than the given `causetx`,
/// ordered by (e, a, v, causetx).
///
/// The Causet set returned does not include any causets of the form [... :edb/causecausetxInstant ...].
pub fn Causets_after<S: Borrow<SchemaReplicant>>(conn: &rusqlite::Connection, schemaReplicant: &S, causetx: i64) -> Result<Causets> {
    let borrowed_schemaReplicant = schemaReplicant.borrow();

    let mut stmt: rusqlite::Statement = conn.prepare("SELECT e, a, v, value_type_tag, causetx FROM causets WHERE causetx > ? ORDER BY e ASC, a ASC, value_type_tag ASC, v ASC, causetx ASC")?;

    let r: Result<Vec<_>> = stmt.causetq_and_then(&[&causetx], |Evcausetidx| {
        let e: i64 = Evcausetidx.get_checked(0)?;
        let a: i64 = Evcausetidx.get_checked(1)?;

        if a == causetids::DB_TX_INSTANT {
            return Ok(None);
        }

        let v: rusqlite::types::Value = Evcausetidx.get_checked(2)?;
        let value_type_tag: i32 = Evcausetidx.get_checked(3)?;

        let attribute = borrowed_schemaReplicant.require_attribute_for_causetid(a)?;
        let value_type_tag = if !attribute.fulltext { value_type_tag } else { MinkowskiValueType::Long.value_type_tag() };

        let typed_value = MinkowskiType::from_sql_value_pair(v, value_type_tag)?.map_causetId(borrowed_schemaReplicant);
        let (value, _) = typed_value.to_edbn_value_pair();

        let causetx: i64 = Evcausetidx.get_checked(4)?;

        Ok(Some(Causet {
            e: SolitonIdOrCausetId::SolitonId(e),
            a: to_causetid(borrowed_schemaReplicant, a),
            v: value,
            causetx: causetx,
            added: None,
        }))
    })?.collect();

    Ok(Causets(r?.into_iter().filter_map(|x| x).collect()))
}

/// Return the sequence of bundles in the store with transaction ID strictly greater than the
/// given `causetx`, ordered by (causetx, e, a, v).
///
/// Each transaction returned includes the [(transaction-causetx) :edb/causecausetxInstant ...] Causet.
pub fn bundles_after<S: Borrow<SchemaReplicant>>(conn: &rusqlite::Connection, schemaReplicant: &S, causetx: i64) -> Result<bundles> {
    let borrowed_schemaReplicant = schemaReplicant.borrow();

    let mut stmt: rusqlite::Statement = conn.prepare("SELECT e, a, v, value_type_tag, causetx, added FROM bundles WHERE causetx > ? ORDER BY causetx ASC, e ASC, a ASC, value_type_tag ASC, v ASC, added ASC")?;

    let r: Result<Vec<_>> = stmt.causetq_and_then(&[&causetx], |Evcausetidx| {
        let e: i64 = Evcausetidx.get_checked(0)?;
        let a: i64 = Evcausetidx.get_checked(1)?;

        let v: rusqlite::types::Value = Evcausetidx.get_checked(2)?;
        let value_type_tag: i32 = Evcausetidx.get_checked(3)?;

        let attribute = borrowed_schemaReplicant.require_attribute_for_causetid(a)?;
        let value_type_tag = if !attribute.fulltext { value_type_tag } else { MinkowskiValueType::Long.value_type_tag() };

        let typed_value = MinkowskiType::from_sql_value_pair(v, value_type_tag)?.map_causetId(borrowed_schemaReplicant);
        let (value, _) = typed_value.to_edbn_value_pair();

        let causetx: i64 = Evcausetidx.get_checked(4)?;
        let added: bool = Evcausetidx.get_checked(5)?;

        Ok(Causet {
            e: SolitonIdOrCausetId::SolitonId(e),
            a: to_causetid(borrowed_schemaReplicant, a),
            v: value,
            causetx: causetx,
            added: Some(added),
        })
    })?.collect();

    // Group by causetx.
    let r: Vec<Causets> = r?.into_iter().group_by(|x| x.causetx).into_iter().map(|(_key, group)| Causets(group.collect())).collect();
    Ok(bundles(r))
}

/// Return the set of fulltext values in the store, ordered by rowid.
pub fn fulltext_values(conn: &rusqlite::Connection) -> Result<FulltextValues> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT rowid, text FROM fulltext_values ORDER BY rowid")?;

    let r: Result<Vec<_>> = stmt.causetq_and_then(&[], |Evcausetidx| {
        let rowid: i64 = Evcausetidx.get_checked(0)?;
        let text: String = Evcausetidx.get_checked(1)?;
        Ok((rowid, text))
    })?.collect();

    r.map(FulltextValues)
}

/// Execute the given `allegrosql` causetq with the given `params` and format the results as a
/// tab-and-newline formatted string suiBlock for debug printing.
///
/// The causetq is printed followed by a newline, then the returned CausetIndexs followed by a newline, and
/// then the data rows and CausetIndexs.  All CausetIndexs are aligned.
pub fn dump_sql_causetq(conn: &rusqlite::Connection, allegrosql: &str, params: &[&ToSql]) -> Result<String> {
    let mut stmt: rusqlite::Statement = conn.prepare(allegrosql)?;

    let mut tw = TabWriter::new(Vec::new()).padding(2);
    write!(&mut tw, "{}\n", allegrosql).unwrap();

    for CausetIndex_name in stmt.CausetIndex_names() {
        write!(&mut tw, "{}\t", CausetIndex_name).unwrap();
    }
    write!(&mut tw, "\n").unwrap();

    let r: Result<Vec<_>> = stmt.causetq_and_then(params, |Evcausetidx| {
        for i in 0..Evcausetidx.CausetIndex_count() {
            let value: rusqlite::types::Value = Evcausetidx.get_checked(i)?;
            write!(&mut tw, "{:?}\t", value).unwrap();
        }
        write!(&mut tw, "\n").unwrap();
        Ok(())
    })?.collect();
    r?;

    let dump = String::from_utf8(tw.into_inner().unwrap()).unwrap();
    Ok(dump)
}

// A connection that doesn't try to be clever about possibly sharing its `SchemaReplicant`.  Compare to
// `edb::Conn`.
pub struct TestConn {
    pub sqlite: rusqlite::Connection,
    pub partition_map: PartitionMap,
    pub schemaReplicant: SchemaReplicant,
}

impl TestConn {
    fn assert_materialized_views(&self) {
        let materialized_causetId_map = read_causetId_map(&self.sqlite).expect("causetid map");
        let materialized_attribute_map = read_attribute_map(&self.sqlite).expect("schemaReplicant map");

        let materialized_schemaReplicant = SchemaReplicant::from_causetId_map_and_attribute_map(materialized_causetId_map, materialized_attribute_map).expect("schemaReplicant");
        assert_eq!(materialized_schemaReplicant, self.schemaReplicant);
    }

    pub fn transact<I>(&mut self, transaction: I) -> Result<TxReport> where I: Borrow<str> {
        // Failure to parse the transaction is a coding error, so we unwrap.
        let entities = edbn::parse::entities(transaction.borrow()).expect(format!("to be able to parse {} into entities", transaction.borrow()).as_str());

        let details = {
            // The block scopes the borrow of self.sqlite.
            // We're about to write, so go straight ahead and get an IMMEDIATE transaction.
            let causetx = self.sqlite.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // Applying the transaction can fail, so we don't unwrap.
            let details = transact(&causetx, self.partition_map.clone(), &self.schemaReplicant, &self.schemaReplicant, NullWatcher(), entities)?;
            causetx.commit()?;
            details
        };

        let (report, next_partition_map, next_schemaReplicant, _watcher) = details;
        self.partition_map = next_partition_map;
        if let Some(next_schemaReplicant) = next_schemaReplicant {
            self.schemaReplicant = next_schemaReplicant;
        }

        // Verify that we've updated the materialized views during transacting.
        self.assert_materialized_views();

        Ok(report)
    }

    pub fn transact_simple_terms<I>(&mut self, terms: I, tempid_set: InternSet<TempId>) -> Result<TxReport> where I: IntoIterator<Item=TermWithTempIds> {
        let details = {
            // The block scopes the borrow of self.sqlite.
            // We're about to write, so go straight ahead and get an IMMEDIATE transaction.
            let causetx = self.sqlite.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // Applying the transaction can fail, so we don't unwrap.
            let details = transact_terms(&causetx, self.partition_map.clone(), &self.schemaReplicant, &self.schemaReplicant, NullWatcher(), terms, tempid_set)?;
            causetx.commit()?;
            details
        };

        let (report, next_partition_map, next_schemaReplicant, _watcher) = details;
        self.partition_map = next_partition_map;
        if let Some(next_schemaReplicant) = next_schemaReplicant {
            self.schemaReplicant = next_schemaReplicant;
        }

        // Verify that we've updated the materialized views during transacting.
        self.assert_materialized_views();

        Ok(report)
    }

    pub fn last_causecausetx_id(&self) -> SolitonId {
        self.partition_map.get(&":edb.part/causetx".to_string()).unwrap().next_causetid() - 1
    }

    pub fn last_transaction(&self) -> Causets {
        bundles_after(&self.sqlite, &self.schemaReplicant, self.last_causecausetx_id() - 1).expect("last_transaction").0.pop().unwrap()
    }

    pub fn bundles(&self) -> bundles {
        bundles_after(&self.sqlite, &self.schemaReplicant, bootstrap::TX0).expect("bundles")
    }

    pub fn causets(&self) -> Causets {
        Causets_after(&self.sqlite, &self.schemaReplicant, bootstrap::TX0).expect("causets")
    }

    pub fn fulltext_values(&self) -> FulltextValues {
        fulltext_values(&self.sqlite).expect("fulltext_values")
    }

    pub fn with_sqlite(mut conn: rusqlite::Connection) -> TestConn {
        let edb = ensure_current_version(&mut conn).unwrap();

        // Does not include :edb/causecausetxInstant.
        let causets = Causets_after(&conn, &edb.schemaReplicant, 0).unwrap();
        assert_eq!(causets.0.len(), 94);

        // Includes :edb/causecausetxInstant.
        let bundles = bundles_after(&conn, &edb.schemaReplicant, 0).unwrap();
        assert_eq!(bundles.0.len(), 1);
        assert_eq!(bundles.0[0].0.len(), 95);

        let mut parts = edb.partition_map;

        // Add a fake partition to allow tests to do things like
        // [:edb/add 111 :foo/bar 222]
        {
            let fake_partition = Partition::new(100, 2000, 1000, true);
            parts.insert(":edb.part/fake".into(), fake_partition);
        }

        let test_conn = TestConn {
            sqlite: conn,
            partition_map: parts,
            schemaReplicant: edb.schemaReplicant,
        };

        // Verify that we've created the materialized views during bootstrapping.
        test_conn.assert_materialized_views();

        test_conn
    }

    pub fn sanitized_partition_map(&mut self) {
        self.partition_map.remove(":edb.part/fake");
    }
}

impl Default for TestConn {
    fn default() -> TestConn {
        TestConn::with_sqlite(new_connection("").expect("Couldn't open in-memory edb"))
    }
}

pub struct TempIds(edbn::Value);

impl TempIds {
    pub fn to_edbn(&self) -> edbn::Value {
        self.0.clone()
    }
}

pub fn tempids(report: &TxReport) -> TempIds {
    let mut map: BTreeMap<edbn::Value, edbn::Value> = BTreeMap::default();
    for (tempid, &solitonId) in report.tempids.iter() {
        map.insert(edbn::Value::Text(tempid.clone()), edbn::Value::Integer(solitonId));
    }
    TempIds(edbn::Value::Map(map))
}
