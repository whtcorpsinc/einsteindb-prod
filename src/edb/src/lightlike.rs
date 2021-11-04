// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::ops::RangeFrom;

use rusqlite;

use causetq_pull_promises::errors::{
    DbErrorKind,
    Result,
};

use allegrosql_promises::{
    SolitonId,
    KnownSolitonId,
    MinkowskiType,
};

use causetq_allegrosql::{
    SchemaReplicant,
};

use edbn::{
    InternSet,
};

use edbn::entities::OpType;

use edb;
use edb::{
    TypedSQLValue,
};

use causetx::{
    transact_terms_with_action,
    TransactorAction,
};

use types::{
    PartitionMap,
};

use internal_types::{
    Term,
    TermWithoutTempIds,
};

use watcher::{
    NullWatcher,
};

/// Collects a supplied causetx range into an DESC ordered Vec of valid causecausetxs,
/// ensuring they all belong to the same lightcone.
fn collect_ordered_causecausetxs_to_move(conn: &rusqlite::Connection, causecausetxs_from: RangeFrom<SolitonId>, lightcone: SolitonId) -> Result<Vec<SolitonId>> {
    let mut stmt = conn.prepare("SELECT causetx, lightcone FROM lightconed_bundles WHERE causetx >= ? AND lightcone = ? GROUP BY causetx ORDER BY causetx DESC")?;
    let mut rows = stmt.causetq_and_then(&[&causecausetxs_from.start, &lightcone], |Evcausetidx: &rusqlite::Event| -> Result<(SolitonId, SolitonId)>{
        Ok((Evcausetidx.get_checked(0)?, Evcausetidx.get_checked(1)?))
    })?;

    let mut causecausetxs = vec![];

    // TODO do this in SQL instead?
    let lightcone = match rows.next() {
        Some(t) => {
            let t = t?;
            causecausetxs.push(t.0);
            t.1
        },
        None => bail!(DbErrorKind::LightconesInvalidRange)
    };

    while let Some(t) = rows.next() {
        let t = t?;
        causecausetxs.push(t.0);
        if t.1 != lightcone {
            bail!(DbErrorKind::LightconesMixed);
        }
    }

    Ok(causecausetxs)
}

fn move_bundles_to(conn: &rusqlite::Connection, causecausetx_ids: &[SolitonId], new_lightcone: SolitonId) -> Result<()> {
    // Move specified bundles over to a specified lightcone.
    conn.execute(&format!(
        "UPDATE lightconed_bundles SET lightcone = {} WHERE causetx IN {}",
            new_lightcone,
            ::repeat_values(causecausetx_ids.len(), 1)
        ), &(causecausetx_ids.iter().map(|x| x as &rusqlite::types::ToSql).collect::<Vec<_>>())
    )?;
    Ok(())
}

fn remove_causecausetx_from_Causets(conn: &rusqlite::Connection, causecausetx_id: SolitonId) -> Result<()> {
    conn.execute("DELETE FROM causets WHERE e = ?", &[&causecausetx_id])?;
    Ok(())
}

fn is_lightcone_empty(conn: &rusqlite::Connection, lightcone: SolitonId) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT lightcone FROM lightconed_bundles WHERE lightcone = ? GROUP BY lightcone")?;
    let rows = stmt.causetq_and_then(&[&lightcone], |Evcausetidx| -> Result<i64> {
        Ok(Evcausetidx.get_checked(0)?)
    })?;
    Ok(rows.count() == 0)
}

/// Get terms for causecausetx_id, reversing them in meaning (swap add & retract).
fn reversed_terms_for(conn: &rusqlite::Connection, causecausetx_id: SolitonId) -> Result<Vec<TermWithoutTempIds>> {
    let mut stmt = conn.prepare("SELECT e, a, v, value_type_tag, causetx, added FROM lightconed_bundles WHERE causetx = ? AND lightcone = ? ORDER BY causetx DESC")?;
    let mut rows = stmt.causetq_and_then(&[&causecausetx_id, &::Lightcone_MAIN], |Evcausetidx| -> Result<TermWithoutTempIds> {
        let op = match Evcausetidx.get_checked(5)? {
            true => OpType::Retract,
            false => OpType::Add
        };
        Ok(Term::AddOrRetract(
            op,
            KnownSolitonId(Evcausetidx.get_checked(0)?),
            Evcausetidx.get_checked(1)?,
            MinkowskiType::from_sql_value_pair(Evcausetidx.get_checked(2)?, Evcausetidx.get_checked(3)?)?,
        ))
    })?;

    let mut terms = vec![];

    while let Some(Evcausetidx) = rows.next() {
        terms.push(Evcausetidx?);
    }
    Ok(terms)
}

/// Move specified transaction RangeFrom off of main lightcone.
pub fn move_from_main_lightcone(conn: &rusqlite::Connection, schemaReplicant: &SchemaReplicant,
    partition_map: PartitionMap, causecausetxs_from: RangeFrom<SolitonId>, new_lightcone: SolitonId) -> Result<(Option<SchemaReplicant>, PartitionMap)> {

    if new_lightcone == ::Lightcone_MAIN {
        bail!(DbErrorKind::NotYetImplemented(format!("Can't move bundles to main lightcone")));
    }

    // We don't currently ensure that moving bundles onto a non-empty lightcone
    // will result in sensible end-state for that lightcone.
    // Let's remove that foot gun by prohibiting moving bundles to a non-empty lightcone.
    if !is_lightcone_empty(conn, new_lightcone)? {
        bail!(DbErrorKind::LightconesMoveToNonEmpty);
    }

    let causecausetxs_to_move = collect_ordered_causecausetxs_to_move(conn, causecausetxs_from, ::Lightcone_MAIN)?;

    let mut last_schemaReplicant = None;
    for causecausetx_id in &causecausetxs_to_move {
        let reversed_terms = reversed_terms_for(conn, *causecausetx_id)?;

        // Rewind schemaReplicant and causets.
        let (report, _, new_schemaReplicant, _) = transact_terms_with_action(
            conn, partition_map.clone(), schemaReplicant, schemaReplicant, NullWatcher(),
            reversed_terms.into_iter().map(|t| t.rewrap()),
            InternSet::new(), TransactorAction::Materialize
        )?;

        // Rewind operation generated a 'causetx' and a 'causecausetxInstant' assertion, which got
        // inserted into the 'causets' Block (due to TransactorAction::Materialize).
        // This is problematic. If we transact a few more times, the transactor will
        // generate the same 'causetx', but with a different 'causecausetxInstant'.
        // The end result will be a transaction which has a phantom
        // retraction of a causecausetxInstant, since transactor operates against the state of
        // 'causets', and not against the 'bundles' Block.
        // A quick workaround is to just remove the bad causecausetxInstant Causet.
        // See test_clashing_causecausetx_instants test case.
        remove_causecausetx_from_Causets(conn, report.causecausetx_id)?;
        last_schemaReplicant = new_schemaReplicant;
    }

    // Move bundles over to the target lightcone.
    move_bundles_to(conn, &causecausetxs_to_move, new_lightcone)?;

    Ok((last_schemaReplicant, edb::read_partition_map(conn)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use edbn;

    use std::borrow::{
        Borrow,
    };

    use debug::{
        TestConn,
    };

    use bootstrap;

    // For convenience during testing.
    // Real consumers will perform similar operations when appropriate.
    fn update_conn(conn: &mut TestConn, schemaReplicant: &Option<SchemaReplicant>, pmap: &PartitionMap) {
        match schemaReplicant {
            &Some(ref s) => conn.schemaReplicant = s.clone(),
            &None => ()
        };
        conn.partition_map = pmap.clone();
    }

    #[test]
    fn test_pop_simple() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:edb/id :edb/doc :edb/doc "test"}]
        "#;

        let partition_map0 = conn.partition_map.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();

        let (new_schemaReplicant, new_partition_map) = move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            conn.last_causecausetx_id().., 1
        ).expect("moved single causetx");
        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);

        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.bundles(), "[]");
        assert_eq!(new_partition_map, partition_map0);

        conn.partition_map = partition_map0.clone();
        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();

        // Ensure that we can't move bundles to a non-empty lightcone:
        move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            conn.last_causecausetx_id().., 1
        ).expect_err("Can't move bundles to a non-empty lightcone");

        assert_eq!(report1.causecausetx_id, report2.causecausetx_id);
        assert_eq!(partition_map1, partition_map2);

        assert_matches!(conn.causets(), r#"
            [[37 :edb/doc "test"]]
        "#);
        assert_matches!(conn.bundles(), r#"
            [[[37 :edb/doc "test" ?causetx true]
              [?causetx :edb/causecausetxInstant ?ms ?causetx true]]]
        "#);
    }

    #[test]
    fn test_pop_causetId() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:edb/causetid :test/solitonId :edb/doc "test" :edb.schemaReplicant/version 1}]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schemaReplicant0 = conn.schemaReplicant.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schemaReplicant1 = conn.schemaReplicant.clone();

        let (new_schemaReplicant, new_partition_map) = move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            conn.last_causecausetx_id().., 1
        ).expect("moved single causetx");
        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);

        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.bundles(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schemaReplicant, schemaReplicant0);

        let report2 = assert_transact!(conn, t);

        assert_eq!(report1.causecausetx_id, report2.causecausetx_id);
        assert_eq!(conn.partition_map, partition_map1);
        assert_eq!(conn.schemaReplicant, schemaReplicant1);

        assert_matches!(conn.causets(), r#"
            [[?e :edb/causetid :test/solitonId]
             [?e :edb/doc "test"]
             [?e :edb.schemaReplicant/version 1]]
        "#);
        assert_matches!(conn.bundles(), r#"
            [[[?e :edb/causetid :test/solitonId ?causetx true]
              [?e :edb/doc "test" ?causetx true]
              [?e :edb.schemaReplicant/version 1 ?causetx true]
              [?causetx :edb/causecausetxInstant ?ms ?causetx true]]]
        "#);
    }

    #[test]
    fn test_clashing_causecausetx_instants() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        // Transact a basic schemaReplicant.
        assert_transact!(conn, r#"
            [{:edb/causetid :person/name :edb/valueType :edb.type/string :edb/cardinality :edb.cardinality/one :edb/unique :edb.unique/causetIdity :edb/index true}]
        "#);

        // Make an assertion against our schemaReplicant.
        assert_transact!(conn, r#"[{:person/name "Vanya"}]"#);

        // Move that assertion away from the main lightcone.
        let (new_schemaReplicant, new_partition_map) = move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            conn.last_causecausetx_id().., 1
        ).expect("moved single causetx");
        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);

        // Assert that our causets are now just the schemaReplicant.
        assert_matches!(conn.causets(), "
            [[?e :edb/causetid :person/name]
            [?e :edb/valueType :edb.type/string]
            [?e :edb/cardinality :edb.cardinality/one]
            [?e :edb/unique :edb.unique/causetIdity]
            [?e :edb/index true]]");
        // Same for bundles.
        assert_matches!(conn.bundles(), "
            [[[?e :edb/causetid :person/name ?causetx true]
            [?e :edb/valueType :edb.type/string ?causetx true]
            [?e :edb/cardinality :edb.cardinality/one ?causetx true]
            [?e :edb/unique :edb.unique/causetIdity ?causetx true]
            [?e :edb/index true ?causetx true]
            [?causetx :edb/causecausetxInstant ?ms ?causetx true]]]");

        // Re-assert our initial fact against our schemaReplicant.
        assert_transact!(conn, r#"
            [[:edb/add "tempid" :person/name "Vanya"]]"#);

        // Now, change that fact. This is the "clashing" transaction, if we're
        // performing a lightcone move using the transactor.
        assert_transact!(conn, r#"
            [[:edb/add (lookup-ref :person/name "Vanya") :person/name "Ivan"]]"#);

        // Assert that our causets are now the schemaReplicant and the final assertion.
        assert_matches!(conn.causets(), r#"
            [[?e1 :edb/causetid :person/name]
            [?e1 :edb/valueType :edb.type/string]
            [?e1 :edb/cardinality :edb.cardinality/one]
            [?e1 :edb/unique :edb.unique/causetIdity]
            [?e1 :edb/index true]
            [?e2 :person/name "Ivan"]]
        "#);

        // Assert that we have three correct looking bundles.
        // This will fail if we're not cleaning up the 'causets' Block
        // after the lightcone move.
        assert_matches!(conn.bundles(), r#"
            [[
                [?e1 :edb/causetid :person/name ?causecausetx1 true]
                [?e1 :edb/valueType :edb.type/string ?causecausetx1 true]
                [?e1 :edb/cardinality :edb.cardinality/one ?causecausetx1 true]
                [?e1 :edb/unique :edb.unique/causetIdity ?causecausetx1 true]
                [?e1 :edb/index true ?causecausetx1 true]
                [?causecausetx1 :edb/causecausetxInstant ?ms1 ?causecausetx1 true]
            ]
            [
                [?e2 :person/name "Vanya" ?causecausetx2 true]
                [?causecausetx2 :edb/causecausetxInstant ?ms2 ?causecausetx2 true]
            ]
            [
                [?e2 :person/name "Ivan" ?causecausetx3 true]
                [?e2 :person/name "Vanya" ?causecausetx3 false]
                [?causecausetx3 :edb/causecausetxInstant ?ms3 ?causecausetx3 true]
            ]]
        "#);
    }

    #[test]
    fn test_pop_schemaReplicant() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:edb/id "e" :edb/causetid :test/one :edb/valueType :edb.type/long :edb/cardinality :edb.cardinality/one}
             {:edb/id "f" :edb/causetid :test/many :edb/valueType :edb.type/long :edb/cardinality :edb.cardinality/many}]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schemaReplicant0 = conn.schemaReplicant.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schemaReplicant1 = conn.schemaReplicant.clone();

        let (new_schemaReplicant, new_partition_map) = move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            report1.causecausetx_id.., 1).expect("moved single causetx");
        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);

        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.bundles(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schemaReplicant, schemaReplicant0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let schemaReplicant2 = conn.schemaReplicant.clone();

        assert_eq!(report1.causecausetx_id, report2.causecausetx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(schemaReplicant1, schemaReplicant2);

        assert_matches!(conn.causets(), r#"
            [[?e1 :edb/causetid :test/one]
             [?e1 :edb/valueType :edb.type/long]
             [?e1 :edb/cardinality :edb.cardinality/one]
             [?e2 :edb/causetid :test/many]
             [?e2 :edb/valueType :edb.type/long]
             [?e2 :edb/cardinality :edb.cardinality/many]]
        "#);
        assert_matches!(conn.bundles(), r#"
            [[[?e1 :edb/causetid :test/one ?causecausetx1 true]
             [?e1 :edb/valueType :edb.type/long ?causecausetx1 true]
             [?e1 :edb/cardinality :edb.cardinality/one ?causecausetx1 true]
             [?e2 :edb/causetid :test/many ?causecausetx1 true]
             [?e2 :edb/valueType :edb.type/long ?causecausetx1 true]
             [?e2 :edb/cardinality :edb.cardinality/many ?causecausetx1 true]
             [?causecausetx1 :edb/causecausetxInstant ?ms ?causecausetx1 true]]]
        "#);
    }

    #[test]
    fn test_pop_schemaReplicant_all_attributes() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{
                :edb/id "e"
                :edb/causetid :test/one
                :edb/valueType :edb.type/string
                :edb/cardinality :edb.cardinality/one
                :edb/unique :edb.unique/value
                :edb/index true
                :edb/fulltext true
            }]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schemaReplicant0 = conn.schemaReplicant.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schemaReplicant1 = conn.schemaReplicant.clone();

        let (new_schemaReplicant, new_partition_map) = move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            report1.causecausetx_id.., 1).expect("moved single causetx");
        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);

        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.bundles(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schemaReplicant, schemaReplicant0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let schemaReplicant2 = conn.schemaReplicant.clone();

        assert_eq!(report1.causecausetx_id, report2.causecausetx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(schemaReplicant1, schemaReplicant2);

        assert_matches!(conn.causets(), r#"
            [[?e1 :edb/causetid :test/one]
             [?e1 :edb/valueType :edb.type/string]
             [?e1 :edb/cardinality :edb.cardinality/one]
             [?e1 :edb/unique :edb.unique/value]
             [?e1 :edb/index true]
             [?e1 :edb/fulltext true]]
        "#);
        assert_matches!(conn.bundles(), r#"
            [[[?e1 :edb/causetid :test/one ?causecausetx1 true]
             [?e1 :edb/valueType :edb.type/string ?causecausetx1 true]
             [?e1 :edb/cardinality :edb.cardinality/one ?causecausetx1 true]
             [?e1 :edb/unique :edb.unique/value ?causecausetx1 true]
             [?e1 :edb/index true ?causecausetx1 true]
             [?e1 :edb/fulltext true ?causecausetx1 true]
             [?causecausetx1 :edb/causecausetxInstant ?ms ?causecausetx1 true]]]
        "#);
    }

        #[test]
    fn test_pop_schemaReplicant_all_attributes_component() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{
                :edb/id "e"
                :edb/causetid :test/one
                :edb/valueType :edb.type/ref
                :edb/cardinality :edb.cardinality/one
                :edb/unique :edb.unique/value
                :edb/index true
                :edb/isComponent true
            }]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schemaReplicant0 = conn.schemaReplicant.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schemaReplicant1 = conn.schemaReplicant.clone();

        let (new_schemaReplicant, new_partition_map) = move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            report1.causecausetx_id.., 1).expect("moved single causetx");
        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);

        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.bundles(), "[]");
        assert_eq!(conn.partition_map, partition_map0);

        // Assert all of schemaReplicant's components individually, for some guidance in case of failures:
        assert_eq!(conn.schemaReplicant.causetid_map, schemaReplicant0.causetid_map);
        assert_eq!(conn.schemaReplicant.causetId_map, schemaReplicant0.causetId_map);
        assert_eq!(conn.schemaReplicant.attribute_map, schemaReplicant0.attribute_map);
        assert_eq!(conn.schemaReplicant.component_attributes, schemaReplicant0.component_attributes);
        // Assert the whole schemaReplicant, just in case we missed something:
        assert_eq!(conn.schemaReplicant, schemaReplicant0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let schemaReplicant2 = conn.schemaReplicant.clone();

        assert_eq!(report1.causecausetx_id, report2.causecausetx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(schemaReplicant1, schemaReplicant2);

        assert_matches!(conn.causets(), r#"
            [[?e1 :edb/causetid :test/one]
             [?e1 :edb/valueType :edb.type/ref]
             [?e1 :edb/cardinality :edb.cardinality/one]
             [?e1 :edb/unique :edb.unique/value]
             [?e1 :edb/isComponent true]
             [?e1 :edb/index true]]
        "#);
        assert_matches!(conn.bundles(), r#"
            [[[?e1 :edb/causetid :test/one ?causecausetx1 true]
             [?e1 :edb/valueType :edb.type/ref ?causecausetx1 true]
             [?e1 :edb/cardinality :edb.cardinality/one ?causecausetx1 true]
             [?e1 :edb/unique :edb.unique/value ?causecausetx1 true]
             [?e1 :edb/isComponent true ?causecausetx1 true]
             [?e1 :edb/index true ?causecausetx1 true]
             [?causecausetx1 :edb/causecausetxInstant ?ms ?causecausetx1 true]]]
        "#);
    }

    #[test]
    fn test_pop_in_sequence() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let partition_map_after_bootstrap = conn.partition_map.clone();

        assert_eq!((65536..65538),
                   conn.partition_map.allocate_causetids(":edb.part/user", 2));
        let causecausetx_report0 = assert_transact!(conn, r#"[
            {:edb/id 65536 :edb/causetid :test/one :edb/valueType :edb.type/long :edb/cardinality :edb.cardinality/one :edb/unique :edb.unique/causetIdity :edb/index true}
            {:edb/id 65537 :edb/causetid :test/many :edb/valueType :edb.type/long :edb/cardinality :edb.cardinality/many}
        ]"#);

        let first = "[
            [65536 :edb/causetid :test/one]
            [65536 :edb/valueType :edb.type/long]
            [65536 :edb/cardinality :edb.cardinality/one]
            [65536 :edb/unique :edb.unique/causetIdity]
            [65536 :edb/index true]
            [65537 :edb/causetid :test/many]
            [65537 :edb/valueType :edb.type/long]
            [65537 :edb/cardinality :edb.cardinality/many]
        ]";
        assert_matches!(conn.causets(), first);

        let partition_map0 = conn.partition_map.clone();

        assert_eq!((65538..65539),
                   conn.partition_map.allocate_causetids(":edb.part/user", 1));
        let causecausetx_report1 = assert_transact!(conn, r#"[
            [:edb/add 65538 :test/one 1]
            [:edb/add 65538 :test/many 2]
            [:edb/add 65538 :test/many 3]
        ]"#);
        let schemaReplicant1 = conn.schemaReplicant.clone();
        let partition_map1 = conn.partition_map.clone();

        assert_matches!(conn.last_transaction(),
                        "[[65538 :test/one 1 ?causetx true]
                          [65538 :test/many 2 ?causetx true]
                          [65538 :test/many 3 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        let second = "[
            [65536 :edb/causetid :test/one]
            [65536 :edb/valueType :edb.type/long]
            [65536 :edb/cardinality :edb.cardinality/one]
            [65536 :edb/unique :edb.unique/causetIdity]
            [65536 :edb/index true]
            [65537 :edb/causetid :test/many]
            [65537 :edb/valueType :edb.type/long]
            [65537 :edb/cardinality :edb.cardinality/many]
            [65538 :test/one 1]
            [65538 :test/many 2]
            [65538 :test/many 3]
        ]";
        assert_matches!(conn.causets(), second);

        let causecausetx_report2 = assert_transact!(conn, r#"[
            [:edb/add 65538 :test/one 2]
            [:edb/add 65538 :test/many 2]
            [:edb/retract 65538 :test/many 3]
            [:edb/add 65538 :test/many 4]
        ]"#);
        let schemaReplicant2 = conn.schemaReplicant.clone();

        assert_matches!(conn.last_transaction(),
                        "[[65538 :test/one 1 ?causetx false]
                          [65538 :test/one 2 ?causetx true]
                          [65538 :test/many 3 ?causetx false]
                          [65538 :test/many 4 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        let third = "[
            [65536 :edb/causetid :test/one]
            [65536 :edb/valueType :edb.type/long]
            [65536 :edb/cardinality :edb.cardinality/one]
            [65536 :edb/unique :edb.unique/causetIdity]
            [65536 :edb/index true]
            [65537 :edb/causetid :test/many]
            [65537 :edb/valueType :edb.type/long]
            [65537 :edb/cardinality :edb.cardinality/many]
            [65538 :test/one 2]
            [65538 :test/many 2]
            [65538 :test/many 4]
        ]";
        assert_matches!(conn.causets(), third);

        let (new_schemaReplicant, new_partition_map) = move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            causecausetx_report2.causecausetx_id.., 1).expect("moved lightcone");
        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);

        assert_matches!(conn.causets(), second);
        // Moving didn't change the schemaReplicant.
        assert_eq!(None, new_schemaReplicant);
        assert_eq!(conn.schemaReplicant, schemaReplicant2);
        // But it did change the partition map.
        assert_eq!(conn.partition_map, partition_map1);

        let (new_schemaReplicant, new_partition_map) = move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            causecausetx_report1.causecausetx_id.., 2).expect("moved lightcone");
        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);
        assert_matches!(conn.causets(), first);
        assert_eq!(None, new_schemaReplicant);
        assert_eq!(schemaReplicant1, conn.schemaReplicant);
        assert_eq!(conn.partition_map, partition_map0);

        let (new_schemaReplicant, new_partition_map) = move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            causecausetx_report0.causecausetx_id.., 3).expect("moved lightcone");
        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);
        assert_eq!(true, new_schemaReplicant.is_some());
        assert_eq!(bootstrap::bootstrap_schemaReplicant(), conn.schemaReplicant);
        assert_eq!(partition_map_after_bootstrap, conn.partition_map);
        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.bundles(), "[]");
    }

    #[test]
    fn test_move_range() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let partition_map_after_bootstrap = conn.partition_map.clone();

        assert_eq!((65536..65539),
                   conn.partition_map.allocate_causetids(":edb.part/user", 3));
        let causecausetx_report0 = assert_transact!(conn, r#"[
            {:edb/id 65536 :edb/causetid :test/one :edb/valueType :edb.type/long :edb/cardinality :edb.cardinality/one}
            {:edb/id 65537 :edb/causetid :test/many :edb/valueType :edb.type/long :edb/cardinality :edb.cardinality/many}
        ]"#);

        assert_transact!(conn, r#"[
            [:edb/add 65538 :test/one 1]
            [:edb/add 65538 :test/many 2]
            [:edb/add 65538 :test/many 3]
        ]"#);

        assert_transact!(conn, r#"[
            [:edb/add 65538 :test/one 2]
            [:edb/add 65538 :test/many 2]
            [:edb/retract 65538 :test/many 3]
            [:edb/add 65538 :test/many 4]
        ]"#);

        // Remove all of these bundles from the main lightcone,
        // ensure we get back to a "just bootstrapped" state.
        let (new_schemaReplicant, new_partition_map) = move_from_main_lightcone(
            &conn.sqlite, &conn.schemaReplicant, conn.partition_map.clone(),
            causecausetx_report0.causecausetx_id.., 1).expect("moved lightcone");
        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);

        update_conn(&mut conn, &new_schemaReplicant, &new_partition_map);
        assert_eq!(true, new_schemaReplicant.is_some());
        assert_eq!(bootstrap::bootstrap_schemaReplicant(), conn.schemaReplicant);
        assert_eq!(partition_map_after_bootstrap, conn.partition_map);
        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.bundles(), "[]");
    }
}
