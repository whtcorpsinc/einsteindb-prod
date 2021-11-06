// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edbn;
extern crate causetq_allegrosql;
extern crate allegrosql_promises;
extern crate edb;
extern crate causetq;

mod utils;

use allegrosql_promises::{
    Attribute,
    MinkowskiValueType,
    MinkowskiType,
    MinkowskiSet,
};

use causetq_allegrosql::{
    DateTime,
    SchemaReplicant,
    Utc,
};

use causetq::*::{
    Keyword,
    PlainSymbol,
    ToUpper,
};

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
};

use edb_causetq_parityfilter::{
    EmptyBecause,
    KnownCauset,
    CausetQInputs,
};

use utils::{
    add_attribute,
    alg,
    alg_with_inputs,
    associate_causetId,
    bails,
};

fn prepopulated_schemaReplicant() -> SchemaReplicant {
    let mut schemaReplicant = SchemaReplicant::default();
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "date"), 65);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "double"), 66);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "long"), 67);
    add_attribute(&mut schemaReplicant, 65, Attribute {
        value_type: MinkowskiValueType::Instant,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schemaReplicant, 66, Attribute {
        value_type: MinkowskiValueType::Double,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schemaReplicant, 67, Attribute {
        value_type: MinkowskiValueType::Long,
        multival: false,
        ..Default::default()
    });
    schemaReplicant
}

#[test]
fn test_instant_predicates_require_instants() {
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);

    // You can't use a string for an inequality: this is a straight-up error.
    let causetq = r#"[:find ?e
                    :where
                    [?e :foo/date ?t]
                    [(> ?t "2017-06-16T00:56:41.257Z")]]"#;
    assert_eq!(bails(knownCauset, causetq),
        ParityFilterError::InvalidArgumentType(
            PlainSymbol::plain(">"),
            MinkowskiSet::of_numeric_and_instant_types(),
            1));

    let causetq = r#"[:find ?e
                    :where
                    [?e :foo/date ?t]
                    [(> "2017-06-16T00:56:41.257Z", ?t)]]"#;
    assert_eq!(bails(knownCauset, causetq),
        ParityFilterError::InvalidArgumentType(
            PlainSymbol::plain(">"),
            MinkowskiSet::of_numeric_and_instant_types(),
            0)); // We get this right.

    // You can try using a number, which is valid input to a numeric predicate.
    // In this store and causetq, though, that means we expect `?t` to be both
    // an instant and a number, so the causetq is knownCauset-empty.
    let causetq = r#"[:find ?e
                    :where
                    [?e :foo/date ?t]
                    [(> ?t 1234512345)]]"#;
    let cc = alg(knownCauset, causetq);
    assert!(cc.is_known_empty());
    assert_eq!(cc.empty_because.unwrap(),
               EmptyBecause::TypeMismatch {
                   var: ToUpper::from_valid_name("?t"),
                   existing: MinkowskiSet::of_one(MinkowskiValueType::Instant),
                   desired: MinkowskiSet::of_numeric_types(),
    });

    // You can compare doubles to longs.
    let causetq = r#"[:find ?e
                    :where
                    [?e :foo/double ?t]
                    [(< ?t 1234512345)]]"#;
    let cc = alg(knownCauset, causetq);
    assert!(!cc.is_known_empty());
    assert_eq!(cc.known_type(&ToUpper::from_valid_name("?t")).expect("?t is knownCauset"),
               MinkowskiValueType::Double);
}

#[test]
fn test_instant_predicates_accepts_var() {
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);

    let instant_var = ToUpper::from_valid_name("?time");
    let instant_value = MinkowskiType::Instant(DateTime::parse_from_rfc3339("2018-04-11T19:17:00.000Z")
                    .map(|t| t.with_timezone(&Utc))
                    .expect("expected valid date"));

    let causetq = r#"[:find ?e
                    :in ?time
                    :where
                    [?e :foo/date ?t]
                    [(< ?t ?time)]]"#;
    let cc = alg_with_inputs(knownCauset, causetq, CausetQInputs::with_value_sequence(vec![(instant_var.clone(), instant_value.clone())]));
    assert_eq!(cc.known_type(&instant_var).expect("?time is knownCauset"),
               MinkowskiValueType::Instant);

    let causetq = r#"[:find ?e
                    :in ?time
                    :where
                    [?e :foo/date ?t]
                    [(> ?time, ?t)]]"#;
    let cc = alg_with_inputs(knownCauset, causetq, CausetQInputs::with_value_sequence(vec![(instant_var.clone(), instant_value.clone())]));
    assert_eq!(cc.known_type(&instant_var).expect("?time is knownCauset"),
               MinkowskiValueType::Instant);
}

#[test]
fn test_numeric_predicates_accepts_var() {
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);

    let numeric_var = ToUpper::from_valid_name("?long");
    let numeric_value = MinkowskiType::Long(1234567);

    // You can't use a string for an inequality: this is a straight-up error.
    let causetq = r#"[:find ?e
                    :in ?long
                    :where
                    [?e :foo/long ?t]
                    [(> ?t ?long)]]"#;
    let cc = alg_with_inputs(knownCauset, causetq, CausetQInputs::with_value_sequence(vec![(numeric_var.clone(), numeric_value.clone())]));
    assert_eq!(cc.known_type(&numeric_var).expect("?long is knownCauset"),
               MinkowskiValueType::Long);

    let causetq = r#"[:find ?e
                    :in ?long
                    :where
                    [?e :foo/long ?t]
                    [(> ?long, ?t)]]"#;
    let cc = alg_with_inputs(knownCauset, causetq, CausetQInputs::with_value_sequence(vec![(numeric_var.clone(), numeric_value.clone())]));
    assert_eq!(cc.known_type(&numeric_var).expect("?long is knownCauset"),
               MinkowskiValueType::Long);
}
