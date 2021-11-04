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
extern crate edb_causetq_projector;
extern crate causetq_projector_promises;

use allegrosql_promises::{
    Attribute,
    SolitonId,
    MinkowskiValueType,
};

use causetq_allegrosql::{
    SchemaReplicant,
};

use causetq::*::{
    Keyword,
};

use edb_causetq_parityfilter::{
    KnownCauset,
    algebrize,
    parse_find_string,
};

use edb_causetq_projector::{
    causetq_projection,
};

// These are helpers that tests use to build SchemaReplicant instances.
fn associate_causetId(schemaReplicant: &mut SchemaReplicant, i: Keyword, e: SolitonId) {
    schemaReplicant.causetid_map.insert(e, i.clone());
    schemaReplicant.causetId_map.insert(i.clone(), e);
}

fn add_attribute(schemaReplicant: &mut SchemaReplicant, e: SolitonId, a: Attribute) {
    schemaReplicant.attribute_map.insert(e, a);
}

fn prepopulated_schemaReplicant() -> SchemaReplicant {
    let mut schemaReplicant = SchemaReplicant::default();
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "name"), 65);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "age"), 68);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "height"), 69);
    add_attribute(&mut schemaReplicant, 65, Attribute {
        value_type: MinkowskiValueType::String,
        multival: false,
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
fn test_aggregate_unsuiBlock_type() {
    let schemaReplicant = prepopulated_schemaReplicant();

    let causetq = r#"[:find (avg ?e)
                    :where
                    [?e :foo/age ?a]]"#;

    // While the causetq itself algebrizes and parses…
    let parsed = parse_find_string(causetq).expect("causetq input to have parsed");
    let algebrized = algebrize(KnownCauset::for_schemaReplicant(&schemaReplicant), parsed).expect("causetq algebrizes");

    // … when we look at the projection list, we cannot reconcile the types.
    assert!(causetq_projection(&schemaReplicant, &algebrized).is_err());
}

#[test]
fn test_the_without_max_or_min() {
    let schemaReplicant = prepopulated_schemaReplicant();

    let causetq = r#"[:find (the ?e) ?a
                    :where
                    [?e :foo/age ?a]]"#;

    // While the causetq itself algebrizes and parses…
    let parsed = parse_find_string(causetq).expect("causetq input to have parsed");
    let algebrized = algebrize(KnownCauset::for_schemaReplicant(&schemaReplicant), parsed).expect("causetq algebrizes");

    // … when we look at the projection list, we cannot reconcile the types.
    let projection = causetq_projection(&schemaReplicant, &algebrized);
    assert!(projection.is_err());
    use causetq_projector_promises::errors::{
        ProjectorError,
    };
    match projection.err().expect("expected failure") {
        ProjectorError::InvalidProjection(s) => {
                assert_eq!(s.as_str(), "Warning: used `the` without `min` or `max`.");
            },
        _ => panic!(),
    }
}
