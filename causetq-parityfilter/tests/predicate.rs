// Copyright 2016 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edbn;
extern crate einsteindb_embedded;
extern crate embedded_promises;
extern crate einsteindb_causetq_parityfilter;
extern crate causetq_parityfilter_promises;

mod utils;

use embedded_promises::{
    Attribute,
    ValueType,
    TypedValue,
    ValueTypeSet,
};

use einsteindb_embedded::{
    DateTime,
    Schema,
    Utc,
};

use edbn::causetq::{
    Keyword,
    PlainSymbol,
    Variable,
};

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
};

use einsteindb_causetq_parityfilter::{
    EmptyBecause,
    Known,
    CausetQInputs,
};

use utils::{
    add_attribute,
    alg,
    alg_with_inputs,
    associate_causetId,
    bails,
};

fn prepopulated_schema() -> Schema {
    let mut schema = Schema::default();
    associate_causetId(&mut schema, Keyword::namespaced("foo", "date"), 65);
    associate_causetId(&mut schema, Keyword::namespaced("foo", "double"), 66);
    associate_causetId(&mut schema, Keyword::namespaced("foo", "long"), 67);
    add_attribute(&mut schema, 65, Attribute {
        value_type: ValueType::Instant,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schema, 66, Attribute {
        value_type: ValueType::Double,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schema, 67, Attribute {
        value_type: ValueType::Long,
        multival: false,
        ..Default::default()
    });
    schema
}

#[test]
fn test_instant_predicates_require_instants() {
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);

    // You can't use a string for an inequality: this is a straight-up error.
    let causetq = r#"[:find ?e
                    :where
                    [?e :foo/date ?t]
                    [(> ?t "2017-06-16T00:56:41.257Z")]]"#;
    assert_eq!(bails(known, causetq),
        ParityFilterError::InvalidArgumentType(
            PlainSymbol::plain(">"),
            ValueTypeSet::of_numeric_and_instant_types(),
            1));

    let causetq = r#"[:find ?e
                    :where
                    [?e :foo/date ?t]
                    [(> "2017-06-16T00:56:41.257Z", ?t)]]"#;
    assert_eq!(bails(known, causetq),
        ParityFilterError::InvalidArgumentType(
            PlainSymbol::plain(">"),
            ValueTypeSet::of_numeric_and_instant_types(),
            0)); // We get this right.

    // You can try using a number, which is valid input to a numeric predicate.
    // In this store and causetq, though, that means we expect `?t` to be both
    // an instant and a number, so the causetq is known-empty.
    let causetq = r#"[:find ?e
                    :where
                    [?e :foo/date ?t]
                    [(> ?t 1234512345)]]"#;
    let cc = alg(known, causetq);
    assert!(cc.is_known_empty());
    assert_eq!(cc.empty_because.unwrap(),
               EmptyBecause::TypeMismatch {
                   var: Variable::from_valid_name("?t"),
                   existing: ValueTypeSet::of_one(ValueType::Instant),
                   desired: ValueTypeSet::of_numeric_types(),
    });

    // You can compare doubles to longs.
    let causetq = r#"[:find ?e
                    :where
                    [?e :foo/double ?t]
                    [(< ?t 1234512345)]]"#;
    let cc = alg(known, causetq);
    assert!(!cc.is_known_empty());
    assert_eq!(cc.known_type(&Variable::from_valid_name("?t")).expect("?t is known"),
               ValueType::Double);
}

#[test]
fn test_instant_predicates_accepts_var() {
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);

    let instant_var = Variable::from_valid_name("?time");
    let instant_value = TypedValue::Instant(DateTime::parse_from_rfc3339("2018-04-11T19:17:00.000Z")
                    .map(|t| t.with_timezone(&Utc))
                    .expect("expected valid date"));

    let causetq = r#"[:find ?e
                    :in ?time
                    :where
                    [?e :foo/date ?t]
                    [(< ?t ?time)]]"#;
    let cc = alg_with_inputs(known, causetq, CausetQInputs::with_value_sequence(vec![(instant_var.clone(), instant_value.clone())]));
    assert_eq!(cc.known_type(&instant_var).expect("?time is known"),
               ValueType::Instant);

    let causetq = r#"[:find ?e
                    :in ?time
                    :where
                    [?e :foo/date ?t]
                    [(> ?time, ?t)]]"#;
    let cc = alg_with_inputs(known, causetq, CausetQInputs::with_value_sequence(vec![(instant_var.clone(), instant_value.clone())]));
    assert_eq!(cc.known_type(&instant_var).expect("?time is known"),
               ValueType::Instant);
}

#[test]
fn test_numeric_predicates_accepts_var() {
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);

    let numeric_var = Variable::from_valid_name("?long");
    let numeric_value = TypedValue::Long(1234567);

    // You can't use a string for an inequality: this is a straight-up error.
    let causetq = r#"[:find ?e
                    :in ?long
                    :where
                    [?e :foo/long ?t]
                    [(> ?t ?long)]]"#;
    let cc = alg_with_inputs(known, causetq, CausetQInputs::with_value_sequence(vec![(numeric_var.clone(), numeric_value.clone())]));
    assert_eq!(cc.known_type(&numeric_var).expect("?long is known"),
               ValueType::Long);

    let causetq = r#"[:find ?e
                    :in ?long
                    :where
                    [?e :foo/long ?t]
                    [(> ?long, ?t)]]"#;
    let cc = alg_with_inputs(known, causetq, CausetQInputs::with_value_sequence(vec![(numeric_var.clone(), numeric_value.clone())]));
    assert_eq!(cc.known_type(&numeric_var).expect("?long is known"),
               ValueType::Long);
}
