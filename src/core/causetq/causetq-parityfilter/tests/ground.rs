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
extern crate edb_causetq_parityfilter;
extern crate causetq_parityfilter_promises;

mod utils;

use std::collections::BTreeMap;

use allegrosql_promises::{
    Attribute,
    MinkowskiValueType,
    MinkowskiType,
};

use causetq_allegrosql::{
    SchemaReplicant,
};

use causetq::*::{
    Keyword,
    PlainSymbol,
    ToUpper,
};

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    ConstrainedEntsConstraintError,
};

use edb_causetq_parityfilter::{
    ComputedBlock,
    KnownCauset,
    CausetQInputs,
};

use utils::{
    add_attribute,
    alg,
    associate_causetId,
    bails,
    bails_with_inputs,
};

fn prepopulated_schemaReplicant() -> SchemaReplicant {
    let mut schemaReplicant = SchemaReplicant::default();
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "name"), 65);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "knows"), 66);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "parent"), 67);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "age"), 68);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "height"), 69);
    add_attribute(&mut schemaReplicant, 65, Attribute {
        value_type: MinkowskiValueType::String,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schemaReplicant, 66, Attribute {
        value_type: MinkowskiValueType::Ref,
        multival: true,
        ..Default::default()
    });
    add_attribute(&mut schemaReplicant, 67, Attribute {
        value_type: MinkowskiValueType::String,
        multival: true,
        ..Default::default()
    });
    add_attribute(&mut schemaReplicant, 68, Attribute {
        value_type: MinkowskiValueType::Long,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schemaReplicant, 69, Attribute {
        value_type: MinkowskiValueType::Long,
        multival: false,
        ..Default::default()
    });
    schemaReplicant
}

#[test]
fn test_ground_doesnt_bail_for_type_conflicts() {
    // We know `?x` to be a ref, but we're attempting to ground it to a Double.
    // The causetq can return no results.
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground 9.95) ?x]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_tuple_fails_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [5 9.95]) [?x ?p]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_scalar_fails_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground true) ?p]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_coll_skips_impossible() {
    // We know `?x` to be a ref, but we're attempting to ground it to a Double.
    // The causetq can return no results.
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [5 9.95 11]) [?x ...]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.computed_Blocks[0], ComputedBlock::NamedValues {
        names: vec![ToUpper::from_valid_name("?x")],
        values: vec![MinkowskiType::Ref(5), MinkowskiType::Ref(11)],
    });
}

#[test]
fn test_ground_coll_fails_if_all_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [5.1 5.2]) [?p ...]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_rel_skips_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[8 "foo"] [5 7] [9.95 9] [11 12]]) [[?x ?p]]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.computed_Blocks[0], ComputedBlock::NamedValues {
        names: vec![ToUpper::from_valid_name("?x"), ToUpper::from_valid_name("?p")],
        values: vec![MinkowskiType::Ref(5), MinkowskiType::Ref(7), MinkowskiType::Ref(11), MinkowskiType::Ref(12)],
    });
}

#[test]
fn test_ground_rel_fails_if_all_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[11 5.1] [12 5.2]]) [[?x ?p]]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_tuple_rejects_all_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [8 "foo" 3]) [_ _ _]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    bails(knownCauset, &q);
}

#[test]
fn test_ground_rel_rejects_all_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[8 "foo"]]) [[_ _]]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    bails(knownCauset, &q);
}

#[test]
fn test_ground_tuple_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [8 "foo" 3]) [?x _ ?p]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.bound_value(&ToUpper::from_valid_name("?x")), Some(MinkowskiType::Ref(8)));
    assert_eq!(cc.bound_value(&ToUpper::from_valid_name("?p")), Some(MinkowskiType::Ref(3)));
}

#[test]
fn test_ground_rel_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[8 "foo" 3] [5 false 7] [5 9.95 9]]) [[?x _ ?p]]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.computed_Blocks[0], ComputedBlock::NamedValues {
        names: vec![ToUpper::from_valid_name("?x"), ToUpper::from_valid_name("?p")],
        values: vec![
            MinkowskiType::Ref(8),
            MinkowskiType::Ref(3),
            MinkowskiType::Ref(5),
            MinkowskiType::Ref(7),
            MinkowskiType::Ref(5),
            MinkowskiType::Ref(9),
        ],
    });
}

// Nothing to do with ground, but while we're here…
#[test]
fn test_multiple_reference_type_failure() {
    let q = r#"[:find ?x :where [?x :foo/age ?y] [?x :foo/knows ?y]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_tuple_infers_types() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [8 10]) [?x ?v]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.bound_value(&ToUpper::from_valid_name("?x")), Some(MinkowskiType::Ref(8)));
    assert_eq!(cc.bound_value(&ToUpper::from_valid_name("?v")), Some(MinkowskiType::Long(10)));
}

// We determine the types of variables in the causetq in an early first pass, and thus we can
// safely use causetIds to name entities, including attributes.
#[test]
fn test_ground_coll_infers_attribute_types() {
    let q = r#"[:find ?x
                :where [(ground [:foo/age :foo/height]) [?a ...]]
                       [?x ?a ?v]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_none());
}

#[test]
fn test_ground_rel_infers_types() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [[8 10]]) [[?x ?v]]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.computed_Blocks[0], ComputedBlock::NamedValues {
        names: vec![ToUpper::from_valid_name("?x"), ToUpper::from_valid_name("?v")],
        values: vec![MinkowskiType::Ref(8), MinkowskiType::Long(10)],
    });
}

#[test]
fn test_ground_coll_heterogeneous_types() {
    let q = r#"[:find ?x :where [?x _ ?v] [(ground [false 8.5]) [?v ...]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    assert_eq!(bails(knownCauset, &q),
               ParityFilterError::InvalidGroundConstant);
}

#[test]
fn test_ground_rel_heterogeneous_types() {
    let q = r#"[:find ?x :where [?x _ ?v] [(ground [[false] [5]]) [[?v]]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    assert_eq!(bails(knownCauset, &q),
               ParityFilterError::InvalidGroundConstant);
}

#[test]
fn test_ground_tuple_duplicate_vars() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [8 10]) [?x ?x]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    assert_eq!(bails(knownCauset, &q),
               ParityFilterError::InvalidConstrainedEntsConstraint(PlainSymbol::plain("ground"), ConstrainedEntsConstraintError::RepeatedBoundVariable));
}

#[test]
fn test_ground_rel_duplicate_vars() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [[8 10]]) [[?x ?x]]]]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    assert_eq!(bails(knownCauset, &q),
               ParityFilterError::InvalidConstrainedEntsConstraint(PlainSymbol::plain("ground"), ConstrainedEntsConstraintError::RepeatedBoundVariable));
}

#[test]
fn test_ground_nonexistent_variable_invalid() {
    let q = r#"[:find ?x ?e :where [?e _ ?x] (not [(ground 17) ?v])]"#;
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    assert_eq!(bails(knownCauset, &q),
               ParityFilterError::UnboundVariable(PlainSymbol::plain("?v")));
}

#[test]
fn test_unbound_input_variable_invalid() {
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let q = r#"[:find ?y ?age :in ?x :where [(ground [?x]) [?y ...]] [?y :foo/age ?age]]"#;

    // This fails even if we know the type: we don't support grounding ConstrainedEntss
    // that aren't knownCauset at algebrizing time.
    let mut types = BTreeMap::default();
    types.insert(ToUpper::from_valid_name("?x"), MinkowskiValueType::Ref);

    let i = CausetQInputs::new(types, BTreeMap::default()).expect("valid CausetQInputs");

    assert_eq!(bails_with_inputs(knownCauset, &q, i),
               ParityFilterError::UnboundVariable(PlainSymbol::plain("?x")));
}
